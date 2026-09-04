# auto_squid 代码审计报告

对 `auto_squid/` 全部源码(`router.py` / `selector.py` / `pools.py` / `sticky.py` / `http_cache.py` / `digest.py` / `cluster.py` / `auth.py` / `tuner.py` / `config_schema.py` / `api.py`)做了逐行通读与行为核验。以下按严重度分组。所有结论均已对照源码核实,并排查过相反的候选(如 API 侧认证大小写已修复、路由哈希冲突已排除)。

---

## P1 高

### 1. 客户端连接无读/空闲超时 —— 慢速客户端可耗尽文件描述符(DoS)
- **位置**:`router.py:3628`(`asyncio.start_server(self.handle_client, ...)` 无任何 `limit`/超时参数),`router.py:2548-2623`(`handle_client` 全程无 `asyncio.wait_for`)。
- **描述**:代理端口(默认 `0.0.0.0:10808`,认证默认关闭)对每一条客户端连接的 `reader.readline()`(首行、请求头逐行)、`reader.readexactly(cl)`(请求体)、`reader.read()`(无 Content-Length 的分块读取)都没有设读/空闲超时。上游侧的 `_upstream_timeout`(3s)只作用于 `httpx.AsyncClient`(**上游→代理**这一段),与**客户端→代理**这一跳无关。
- **影响**:任意攻击者(或断连的浏览器)建立 TCP 连接后不发/慢发数据,每一条连接可以无限期占用一个 socket 与一个 `handle_client` task。`handle_client` 又把 task 加入 `_running_tasks`(无上限)。批量发起即可耗尽进程 fd/内存,造成拒绝服务。这是典型的 slow-loris。认证开启也无法缓解:认证校验在读完首行头之后才进行,攻击者提交前即可挂住连接。默认开放代理使其更容易被利用。
- **修复建议**:给 `handle_client` 整体或对每次 `readline`/body 读取包一层 `asyncio.wait_for`(如头部 30s、body 120s),超时即关闭连接并移除 task;或对 `start_server` 的连接级空闲做跟踪(定期清扫长时间无进展的连接),并给 `_running_tasks` 加上限。

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
- **影响**:仅当 `stickiness.probe_interval_sec > 0`(杠杆A,默认关闭的实验特性)时才会增长;但一旦开启,键集合(客户端 IP × 域名)理论上无界,时间一长缓慢累积内存。属潜在的内存无界增长缺陷。
- **修复建议**:复用 `_sticky_cache` 的清理机制(周期 prune + `max_entries` 上限),或在写入时顺带淘汰超期条目。

### 4. 客户端认证头大小写敏感 —— fail-closed 兼容性问题
- **位置**:`auth.py:45`(`headers.get('Proxy-Authorization') or headers.get('Authorization')`)、`router.py:2575-2579`(头解析`dict`保留原始大小写)。
- **描述**:HTTP 头按 RFC 应大小写不敏感;但 `req_headers` 是普通 dict,`check_auth` 用固定键名 `get`。客户端若发 `proxy-authorization:`/`authorization:`(小写)即使凭据正确也会被拒。注意管理 API 侧(api.py)已在中间件把键补回标准大小写,代理端口侧未同步处理。
- **影响**:**fail-closed(拒绝而非绕过)**,即合法客户端因大小写被拒,不是安全绕过;但会破坏与某些只发小写头客户端的兼容,并已证实是整个代码库中"一处修复、另一处未修"的不一致。
- **修复建议**:在解析处统一小写化键,或 `check_auth` 用 `next((v for k,v in headers.items() if k.lower()=='proxy-authorization'), None)` 等方式做大小写不敏感读取。

### 5. 重复同名请求头被 `dict` 折叠(后者覆盖前者)
- **位置**:`router.py:2575-2579`(`req_headers[k.strip()] = v.strip()`)、`router.py:2720`(`hdrs = {k: v for k, v in headers.items() ...}`)。
- **描述**:客户端发多个 `Cookie:` 或 `Host:` 头时,只有最后一个保留;转发上游(httpx `build_request(headers=hdrs)` 接受 dict)无法表达重复头。
- **影响**:转发给上游时可能丢失首个(常为关键)的 Cookie / 重复头,导致会话/鉴权部分失效或请求语义改变。上游响应侧已用 `multi_items()` 正确处理重复头(如多个 `Set-Cookie`),请求侧则不一致。
- **修复建议**:请求头也改为 `list[(name, value)]` 结构并在转发时保持重复头(与 `_write_cached_response` / `_stream_upstream_response` 的响应侧一致),或至少对 Cookie 类头特殊处理。

### 6. `_relay_tunnel` 对取消的 pipe task 只 `cancel()` 未 `await`
- **位置**:`router.py:3478-3479`。
- **描述**:`asyncio.wait(FIRST_COMPLETED)` 返回后,`for t in pending: t.cancel()`,随后只 `await` 了 `done` 中已完成的任务,`pending` 中被取消的任务没有 `await asyncio.gather(*pending, return_exceptions=True)` 等待其退出。
- **影响**:被取消的 `_pipe` task(CancelledError 是其自身 `except Exception` 捕获不到的 BaseException)未等待即被丢弃,可能在 GC 时触发 `"Task was destroyed but it is pending"` 警告,并存在"客户端→上游 方向的 pipe 尚未冲刷完缓冲即被中断"的时序毛刺。非资源泄漏,但属清理不彻底。
- **修复建议**:取消后用 `await asyncio.gather(*pending, return_exceptions=True)` 排空。

---

## P3 低

### 7. CONNECT 200 判定用子串匹配而非状态码解析
- **位置**:`router.py:2488`(`if '200' not in status_text`)。
- **描述**:对上游 CONNECT 响应状态行只检查是否包含子串 `'200'`,而非解析数值状态码。`HTTP/1.1 2000 ...` 或状态行原因短语恰好含 `200` 会被误判为成功。
- **影响**:实际上游代理不会返回这种非法状态行,影响极低;但判定方式不够严谨。
- **修复建议**:按 `status_text.split(' ', 2)[1] == '200'` 严格解析,或至少匹配 `b' 200 '` / 行首 `HTTP/1.x 200`。

### 8. `Content-Length` 解析异常未单独处理
- **位置**:`router.py:2609`(`cl = int(v)`)。
- **描述**:客户端发非数值 `Content-Length`(如 `abc`)时 `int()` 抛 `ValueError`,被 `handle_client` 外层 `except Exception` 捕获并记日志、关闭连接。
- **影响**:**不崩溃、已优雅降级**,只是返回的是直接断连而非明确的 `400 Bad Request`。属健壮性瑕疵。
- **修复建议**:包一层 `try/except` 并显式回 400。

---

## 已核验并排除的候选(非问题,供对照)
- **API 侧认证大小写**:`api.py:41-45` 中间件已把 Starlette 小写化键补回标准大小写,`/health` 豁免 → 无漏洞。
- **路由哈希导致的选择偏差**:候选键为代理 id / proxy_url,mapping 唯一 → 无碰撞。
- **`record_ttfb`/`record_failure` 全局与域名双写**:`domain=None` 时两桶是同一 dict,已去重只写一次 → 无重复计数。
- **配置 `extra="forbid"`**:key 拼错启动即硬报错 → 无静默降级。
- **`_drain_losers` / `_pending_cleanups` 泄漏**:有 `_MAX_PENDING_CLEANUPS` 软上限 + `done_callback` 自清理 → 已兜底(soak 曾观测 fd_peak 569 后修复)。
