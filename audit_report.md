# auto_squid 代码审计报告

对 `auto_squid/` 全部源码(`router.py` / `selector.py` / `pools.py` / `sticky.py` / `http_cache.py` / `digest.py` / `cluster.py` / `auth.py` / `tuner.py` / `config_schema.py` / `api.py`)做了逐行通读与行为核验。以下按严重度分组。所有结论均已对照源码核实,并排查过相反的候选(如 API 侧认证大小写已修复、路由哈希冲突已排除)。

> **评审修订(第二遍)**:全部 8 项发现已逐条对照源码复核确认,行号与行为描述准确。
> 本遍修订:① P2.6 降级为 P3(与自身描述"非资源泄漏"匹配);② 精化 P1.1 的修复
> 定位(无界窗口只在首行/头部/body 读取阶段,relay 阶段已有 300s 读超时;该加的是
> 并发连接数上限而非 `_running_tasks` 上限);③ 补录 3 个第一遍漏项(9/10/已知风险);
> ④ 末尾新增修复优先级建议。

---

## P1 高

### 1. 客户端连接无读/空闲超时 —— 慢速客户端可耗尽文件描述符(DoS)
- **位置**:`router.py:3628`(`asyncio.start_server(self.handle_client, ...)` 无任何 `limit`/超时参数),`router.py:2532-2630`(`handle_client` 的首行/请求头/请求体读取全程无 `asyncio.wait_for`)。
- **描述**:代理端口(默认 `0.0.0.0:10808`,认证默认关闭)对每一条客户端连接的 `reader.readline()`(首行 router.py:2548、请求头逐行 2560)、`reader.readexactly(cl)`(请求体 2616)、`reader.read()`(无 Content-Length 的分块读取 2623)都没有设读/空闲超时。上游侧的 `_upstream_timeout`(3s)只作用于 `httpx.AsyncClient`(**上游→代理**这一段),与**客户端→代理**这一跳无关。
- **边界澄清(评审补充)**:头部阶段已有**字节/行数**上限(`_MAX_REQUEST_HEADER_LINES=100` / `_MAX_REQUEST_HEADER_BYTES=64KB`,router.py:2567),隧道 relay 阶段的 `_pipe` 也有 300s 读超时——但首行/头部/body 的**读取时间**无任何上限:攻击者每分钟发 1 字节即可无限期占住连接,字节上限防不住时间型占用。
- **影响**:任意攻击者(或断连的浏览器)建立 TCP 连接后不发/慢发数据,每一条连接无限期占用一个 socket 与一个 `handle_client` task;并发连接数无上限。批量发起即可耗尽进程 fd/内存,造成拒绝服务。这是典型的 slow-loris。认证开启也无法缓解:认证校验在读完首行头之后才进行(router.py:2582),攻击者提交前即可挂住连接。默认开放代理使其更容易被利用。
- **修复建议**:对首行/每次头部行/body 读取包一层 `asyncio.wait_for`(建议:首行+头部 30s、body 120s;**注意不要给 relay 阶段加更紧的超时**,避免破坏 keep-alive 长连接与慢下载),超时即关闭连接;同时给**并发客户端连接数**设上限(超限回 503 并关闭)。注意:不是给 `_running_tasks` set 加上限——该 set 本身有自清理(`handle_client` finally 移除;预热/探路 task 用 `add_done_callback(discard)`),不会泄漏,缺的是并发度约束。

---

## P2 中

### 2. HTTP 响应缓存键仅 `method:url`,跨客户端共享个性化内容
- **位置**:`http_cache.py:80-82`(`_http_cache_key` 返回 `f"{method}:{url}"`)。
- **描述**:缓存键不区分客户端 IP、请求头(Cookie/Authorization)、查询参数之外的任何差异。`_handle_http_request` 在读取请求头后直接查缓存(`router.py:2737`)并整包回写(`router.py:2738-2745`)。
- **影响**:配置了 `auth_enabled` 的多客户端共享代理下,若源站对同一 URL 返回随客户端 Cookie/请求头变化的响应且未标 `Cache-Control: private`(代码已尊重 `private/no-store/no-cache`,见 `http_cache.py:143-145`,可缓解),后到的客户端会拿到先到客户端的个性化响应,造成跨用户数据串读。也属于共享代理缓存语义上的监听面问题。HTTPS(CONNECT 裸隧道)不进此缓存,故主要波及明文 HTTP 路径。
- **修复建议**:将缓存键扩展为 `method:url:client` 或至少在 `Cache-Control` 含 `private` / 请求头带认证或 Cookie 时跳过共享缓存;对含 `Set-Cookie`/`Authorization` 的请求不命中共享缓存。

### 3. 粘性后台探路节流表 `_sticky_probe_last` 无上限、无清理
- **位置**:`sticky.py:51`(定义)、`router.py:1897`(写入)、`sticky.py:184`(读取)。
- **描述**:`_sticky_probe_last[client_ip|domain]` 每次进入 `_sticky_probe_race` 写入一次,只有 `sticky_probe_due` 读,全工程无任何 prune/expire/容量淘汰。与之对照,`_sticky_cache` 有大量清理(`_prune_sticky`、`max_entries`)。
- **影响**:仅当 `stickiness.probe_interval_sec > 0`(杠杆A,默认关闭的实验特性)时才会增长,且受探路冷却节流(每 (client,domain) 每间隔至多 1 条);但一旦开启,键集合(客户端 IP × 域名)理论上无界,时间一长缓慢累积内存。属潜在的内存无界增长缺陷。
- **修复建议**:复用 `_sticky_cache` 的清理机制(周期 prune + `max_entries` 上限),或在写入时顺带淘汰超期条目。

### 4. 客户端认证头大小写敏感 —— fail-closed 兼容性问题
- **位置**:`auth.py:45`(`headers.get('Proxy-Authorization') or headers.get('Authorization')`)、`router.py:2575-2579`(头解析`dict`保留原始大小写)。
- **描述**:HTTP 头按 RFC 应大小写不敏感;但 `req_headers` 是普通 dict,`check_auth` 用固定键名 `get`。客户端若发 `proxy-authorization:`/`authorization:`(小写)即使凭据正确也会被拒。注意管理 API 侧(api.py)已在中间件把键补回标准大小写,代理端口侧未同步处理。
- **影响**:**fail-closed(拒绝而非绕过)**,即合法客户端因大小写被拒,不是安全绕过;但会破坏与某些只发小写头客户端的兼容,并已证实是整个代码库中"一处修复、另一处未修"的不一致。
- **修复建议**:在解析处统一小写化键,或 `check_auth` 用 `next((v for k,v in headers.items() if k.lower()=='proxy-authorization'), None)` 等方式做大小写不敏感读取(一行修)。

### 5. 重复同名请求头被 `dict` 折叠(后者覆盖前者)
- **位置**:`router.py:2575-2579`(`req_headers[k.strip()] = v.strip()`)、`router.py:2720`(`hdrs = {k: v for k, v in headers.items() ...}`)。
- **描述**:客户端发多个 `Cookie:` 或 `Host:` 头时,只有最后一个保留;转发上游(httpx `build_request(headers=hdrs)` 接受 dict)无法表达重复头。
- **影响**:转发给上游时可能丢失首个(常为关键)的 Cookie / 重复头,导致会话/鉴权部分失效或请求语义改变。上游响应侧已用 `multi_items()` 正确处理重复头(如多个 `Set-Cookie`),请求侧则不一致。
- **修复建议**:请求头也改为 `list[(name, value)]` 结构并在转发时保持重复头(与 `_write_cached_response` / `_stream_upstream_response` 的响应侧一致),或至少对 Cookie 类头特殊处理。注意改造时认证检查(`check_auth` 读 dict)可保留 dict 副本,两结构并存即可。

---

## P3 低

### 6. `_relay_tunnel` 对取消的 pipe task 只 `cancel()` 未 `await`
- **位置**:`router.py:3478-3479`。
- **描述**:`asyncio.wait(FIRST_COMPLETED)` 返回后,`for t in pending: t.cancel()`,随后只 `await` 了 `done` 中已完成的任务,`pending` 中被取消的任务没有 `await asyncio.gather(*pending, return_exceptions=True)` 等待其退出。
- **影响**:被取消的 `_pipe` task(CancelledError 是其自身 `except Exception` 捕获不到的 BaseException)未等待即被丢弃,可能在 GC 时触发 `"Task was destroyed but it is pending"` 警告,并存在"客户端→上游 方向的 pipe 尚未冲刷完缓冲即被中断"的时序毛刺。非资源泄漏,但属清理不彻底。(评审定级说明:被取消方向的数据本就随隧道拆除丢弃,实际影响是警告与理论上的截断毛刺,故从 P2 降为 P3。)
- **修复建议**:取消后用 `await asyncio.gather(*pending, return_exceptions=True)` 排空。

### 7. CONNECT 200 判定用子串匹配而非状态码解析
- **位置**:`router.py:2488`(`if '200' not in status_text`)。
- **描述**:对上游 CONNECT 响应状态行只检查是否包含子串 `'200'`,而非解析数值状态码。`HTTP/1.1 2000 ...` 或状态行原因短语恰好含 `200` 会被误判为成功。
- **影响**:实际上游代理不会返回这种非法状态行,影响极低;但判定方式不够严谨。
- **修复建议**:按 `status_text.split(' ', 2)[1] == '200'` 严格解析,或至少匹配 `b' 200 '` / 行首 `HTTP/1.x 200`。

### 8. 请求行/`Content-Length` 解析异常未单独处理
- **位置**:`router.py:2609`(`cl = int(v)`)、`router.py:2594`(`target = first.split(' ')[1]`)。
- **描述**:客户端发非数值 `Content-Length`(如 `abc`)时 `int()` 抛 `ValueError`;CONNECT 首行只有单个 token 时 `split(' ')[1]` 抛 `IndexError`。两者都被 `handle_client` 外层 `except Exception` 捕获并记日志、关闭连接。(评审补充:原报告只列了 `int()`,同一族的畸形 CONNECT 首行 `IndexError` 一并并入。)
- **影响**:**不崩溃、已优雅降级**,只是返回的是直接断连而非明确的 `400 Bad Request`。属健壮性瑕疵。
- **修复建议**:各包一层 `try/except` 并显式回 400。

### 9. CONNECT target 未做格式校验 —— 上游请求行注入候选(评审补录)
- **位置**:`router.py:2594`(`target = first.split(' ')[1]`,无校验)、`router.py:2480`(`f"CONNECT {target} HTTP/1.1\r\n..."` 直接内插)。
- **描述**:`_handle_connect` 对 `target` 无任何格式校验。`readline()` 只以 `\n` 断行,内嵌裸 `\r` 可存活于 `target`(`strip()` 只去首尾),形如 `CONNECT a\rb HTTP/1.1` 的请求会把注入内容带进发给上游的请求行/头区(`_hb(target)` 也不过滤)。
- **影响**:潜在的上游请求走私/头注入——可注入伪造请求行使上游实际隧道目的地与 `target`(域名缓存/粘性/统计的键)不一致,或注入额外头。缓解因素:上游代理解析通常严格(裸 `\r` 多按 RFC 7230 当空格或拒绝),且攻击者只能打自己配置的上游,故实际风险有限;但属典型的校验缺口,值得一行修掉。
- **修复建议**:target 白名单正则 `^[A-Za-z0-9.\-_]+(:\d+)?$`,不匹配回 400。

### 10. `POST /cost` 权重只钳负值、无上限(评审补录)
- **位置**:`api.py` `_validate_cost_update`(`max(0.0, float(...))`)。
- **描述**:热更新端点对三个 `cost_weight_*` 只做非负钳制,无上界;手滑传 `1e9` 会让排序畸形(自动调参器有硬边界,手动端点没有)。
- **影响**:仅管理面、需 `api.auth` 鉴权,属自伤型操作;但与调参器"有界"的设计意图不一致。
- **修复建议**:与 `tuner._WEIGHT_BOUNDS` 对齐,`/cost` 侧做同样钳制(或超界回 422)。

---

## 已知接受的风险(非缺陷,显式记录)

- **管理 API(18080)默认无鉴权**:README Limitations 已声明,属部署 posture 决策。暴露到不可信网络前必须开启 `api.auth`。此处显式记录,避免后人当成审计遗漏。

---

## 已核验并排除的候选(非问题,供对照)
- **API 侧认证大小写**:`api.py:41-45` 中间件已把 Starlette 小写化键补回标准大小写,`/health` 豁免 → 无漏洞。
- **路由哈希导致的选择偏差**:候选键为代理 id / proxy_url,mapping 唯一 → 无碰撞。
- **`record_ttfb`/`record_failure` 全局与域名双写**:`domain=None` 时两桶是同一 dict,已去重只写一次 → 无重复计数。
- **配置 `extra="forbid"`**:key 拼错启动即硬报错 → 无静默降级。
- **`_drain_losers` / `_pending_cleanups` 泄漏**:有 `_MAX_PENDING_CLEANUPS` 软上限 + `done_callback` 自清理 → 已兜底(soak 曾观测 fd_peak 569 后修复)。
- **`_running_tasks` 泄漏**:`handle_client` 在 finally 中自移除;预热/探路 task 用 `add_done_callback(discard)` → set 本身不泄漏(P1 的问题是**并发度**无上限,不是集合无界)。

---

## 修复优先级建议

1. **#1 slow-loris**(连接入口超时 + 并发上限,影响面最大)
2. **#9 CONNECT target 校验**(一行修,与 #1 同在连接入口,顺手)
3. **#5 重复头**(正确性,改动面中等)
4. **#4 认证大小写**(一行修)
5. **#2 缓存键**(语义/隐私,需权衡共享缓存收益)
6. **#7 / #8**(一行修×2,可并入 #9 的 400 化改造)
7. **#10 / #6**(小修)
8. **#3**(复用 sticky 清理;杠杆A开启前修即可)
