# TODO — Implementation roadmap (prioritized)

目标：把 auto_squid 从设计骨架完善为能在 B 上运行、按域名竞速转发到最优代理的可用原型。

> 架构演进说明：早期路线图基于 ProbeEngine（TCP 探测 + 打分 + 每域选优）。现已用**竞速路由**替换：
> 请求同时发给多个上游，取最先成功者，胜出代理按域名缓存复用（cache_ttl）。ProbeEngine /
> scoring / policy_engine 相关模块已移除，不再适用。

## 已完成 — 核心转发（P0，MVP）

- [x] HTTP/CONNECT 转发路由器（auto_squid/router）
  - 解析 Host/CONNECT 目标，竞速选代理，转发请求/流式响应。
- [x] ProxyStore 持久化（proxies.yaml 加载 + 运行时 CRUD API）
- [x] 竞速路由（racing）：首批 max_retries 并行，全失败兜底批，首字节判胜，败者取消并释放连接
- [x] 域名缓存（domain cache）：胜出代理按域名复用至 cache_ttl，过期自动回退竞速
- [x] HTTP 响应缓存：幂等 GET 内存缓存（TTL 60s，遵循 Cache-Control；含非 2xx 幂等状态码）
- [x] 写方法（POST/PUT/DELETE/PATCH）按域名失效 GET 响应缓存（二级索引 O(K)）
- [x] 并发 GET 去重聚合（cache stampede protection，_AGG_WAIT_TIMEOUT 超时保护）
- [x] 流式响应转发 + 上游连接池化（每代理长驻 httpx.AsyncClient，keep-alive 复用）
- [x] SQLite 持久化（domain_stats / domain_meta）：内存镜像 + 后台批量落盘，WAL 模式
- [x] 客户端认证（可选 HTTP Basic，auth.py，默认关闭，失败 407）
- [x] 管理 API + 单页 Web UI（域名统计/默认代理/胜出计数，卡片点击按默认代理筛选）
- [x] 管理 API HTTP Basic 认证（`api.auth`，默认关闭，除 /health 外全部端点需凭据）
- [x] 端到端测试：HTTP/CONNECT 转发、HTTP/域名缓存、竞速、ProxyStore、API、二进制安全

## 已完成 — 健壮性与特性（P1）

- [x] 并发控制 / 资源上限：MAX_BODY（10 MiB，超限 413）、连接池上限、败者清理积压上限（64）
- [x] 超时治理：上游 connect/pool/read/write 超时、CONNECT 建连/读首字节超时、聚合等待超时
- [x] 优雅关闭：在途连接取消排空、_pending_cleanups 排空后再关 DB/连接池
- [x] hop-by-hop 请求头剔除（Proxy-Authorization 等不透传上游）；响应头剔除 + Content-Length 重写
- [x] 日志分级（每请求 DEBUG，启动/认证拒绝 INFO）、uvloop 事件循环
- [x] 压测工具链（bench/）：mock 上游集群 + 进程隔离压测（server_proc 子进程 + 跨进程计数器）
  - 模式：staircase / rate / mixed / soak（含 --open-loop）/ conn-reuse / all
  - 指标：吞吐/延迟分位、缓存命中率/竞速放大率（服务端计数器）、资源采样、正确性校验
- [x] docker / docker-compose 容器化（`Dockerfile` + `examples/docker/docker-compose.yml`，非 root、数据卷、健康检查）

## 已完成 — 速度提升计划（P0-P3）

- [x] 每代理 EWMA 首字节延迟跟踪 + 按 RTT 排序竞速（/quality API）
- [x] RFC 8305 错峰启动（stagger_start）：先发最优 stagger_initial 个，间隔补发，首字节成功即取消其余
- [x] 全局熔断器 + 指数退避 + slow-start 恢复（/circuit API，真实请求与探活共享失败计数）
- [x] 后台探活（probe_interval_sec 对 enabled 代理做轻量 CONNECT 到 canary）
  - 多 canary / 按标签探活（probe_canaries）
  - canary 本机不可达时整轮跳过（计 probes_skipped），避免误熔断健康代理
- [x] 加权 least-request 选择（ewma × (1 + active)^lb_bias，在途计数保护慢代理）
- [x] 单发降级（质量感知确定性探路）：域名缓存/粘性命中代理连续失败 ≥ 阈值或 EWMA 恶化 ratio 倍 → 降级回竞速
- [x] 策略路由（policies）：按域名/标签收窄竞速候选集（/policies API）
- [x] 自适应域名缓存 TTL（adaptive_ttl）：稳定域名 TTL 上浮、抖动回落
- [x] 域名赢家切换阻尼（switch_damping）：新赢家需连续胜出/显著更优才替换，降出口 IP 抖动
- [x] 自适应并发限制（concurrency_limit）：每代理并发上限成功增/失败乘性降
- [x] CONNECT 上游 TCP 预热池（conn_pool）：每代理维护空闲连接，省建连 TTFB
  - [x] 第二阶段·目标半预连接（conn_pool.target_prewarm）：命中域名缓存/粘性或竞速胜出的高频 CONNECT target 后台预建"到上游"TCP（每条补 2 条、取走仍留 1 条备用），按 (proxy, target) 键区分，取用优先于通用池，共享 fd 预算/空闲超时
  - [x] 第三阶段·已建握手隧道复用（conn_pool.established_reuse）：隧道结束若连接干净（上游无残留缓冲）则归还 `_established_pool` 而非关闭，下次同 (proxy, target) 复用已 CONNECT 握手的连接、跳过握手，省掉慢线路上一次完整往返（github 场景）。`_relay_tunnel` 改用 wait(FIRST_COMPLETED) 任一端结束即取消另一端，客户端断开后立即归还不等上游挂起。严格验证：有残留即丢弃不复用，宁可不复用也不污染
  - [x] 已建握手池缺陷修复（2026-08-26）：① fd 预算：归还路径加 `conn_pool_total` 全局预算 + per-key cap=2（`_ESTABLISHED_KEY_CAP`）检查，超限 close 不复用；两处 refill 预算快照纳入 `_established_pool`（三池同口径）。② 死隧道赢得竞速：新增 `_established_alive`（read(1)+50ms 探测）在复用前判死（FIN→b''/RST→异常 均回落新建），杜绝"复用路径无 I/O、1 tick 赢竞速"。③ 半开兜底：归还连接设 `SO_KEEPALIVE`（`_set_pool_keepalive`，KEEPIDLE 60s）由 OS 判死。171 测试全过（新增探测三态/cap 并发/预算注入/keepalive 4 个）
  - [x] 全失败 5xx resp 泄漏修复（2026-08-26，#3）：`_race_staggered` 全体候选返回 5xx（无赢家）走到底时，`completed` 里持有 5xx resp 的任务从未 aclose → 累积耗尽 httpx 连接池。修复：`return winner` 前 `if completed and cleanup: self._spawn_cleanup(completed, cleanup)`。回归 2 个（全 5xx 清理 + 赢家路径不破坏）
  - [x] _forward_single aclose 收进 finally（2026-08-26，#4）：`_stream_upstream_response` 抛 BaseException 时 `resp.aclose()` 被跳过 → 池化连接泄漏。修复：stream/缓存/return 收进 try，aclose 收进 finally。回归 1 个（stream 抛 CancelledError → aclose 仍执行）。174 全过
- [x] 请求头行数/总字节无上限修复（2026-08-26，#6）：`handle_client` 读请求头循环加 `_MAX_REQUEST_HEADER_LINES=100`（行数）与 `_MAX_REQUEST_HEADER_BYTES=64KB`（累计字节）双上限,超限 log warning + 拒连（finally 统一关闭）。慢速 loris 式攻击发大量小 header 行不再让 bytearray 无界增长。回归 2 个（行数超限 / 字节先于行数触发）。
- [x] 截断上游响应检测（2026-08-26，#7）：`_stream_upstream_response` 累计流式字节 `streamed`,循环结束（含 aiter_raw 抛异常的 RemoteProtocolError 截断）统一比对:non-chunked 且 `streamed != int(upstream_cl)` → `logger.warning`（"truncated upstream response" 带 URL/承诺/实际长度）+ `aclose()` 掐断残留传输。客户端已拿 200 头无法撤回,靠 warn 暴露问题。客户端断开不误报（断开后仍排空上游 body）。回归 1 个（declared=1000/sent=5 → warn）。
- [x] 聚合去重超时提升（2026-08-26，#8）：`_AGG_WAIT_TIMEOUT` 0.1s → 3.0s——0.1s 比典型上游 TTFB 还短,waiter 几乎每次都超时回退竞速,并发同 URL 时去重等于死代码且扇出误触熔断;提到秒级让聚合在真实 TTFB 窗口内生效,保留有界等待。`test_coalescing_timeout_falls_back` 改述为新语义;新增常量回归 `test_aggregation_wait_timeout_is_second_scale`。178 全过
- [x] 配置模型 `extra="forbid"` + 跨字段校验（2026-08-26，#12）：新增 `ConfigBase`（`ConfigDict(extra="forbid")`）,全部配置模型改继承它——拼错键（`stagger_inital`）在启动即硬报错,不再静默落默认;数据模型（ProxyInfo/ProbeCanaryConfig）保持宽松不误伤。`@model_validator`:stagger_initial>=1 且<=max_retries、adaptive_ttl.min<=max（enabled 时）、concurrency_limit.min<=initial<=max（enabled 时）、conn_pool.target_prewarm/established_reuse 依赖 enabled=True、logging.level 合法性。回归 14 个。196 全过
- [x] `logging.level` 生效 + 配置加载友好退出（2026-08-26，#13）：`setup_logging` 文件 handler 级别改用 cfg.logging.level（默认 INFO 行为不变;配 DEBUG 开 per-request 日志）,auto_squid logger 同步跟随。`_load_config` 包 try/except:YAML 缺失/语法错/pydantic 校验失败→打印 `config error: ...` 并 `sys.exit(2)`,不再抛裸 traceback。回归 5 个。196 全过
- [x] 会话粘性（per-client+domain，滑动 TTL）：redispatch、5xx 驱逐、recheck 重竞速、容量上限
- [x] bench 透传全部速度特性开关（--conn-pool / --adaptive-ttl / --switch-damping / --concurrency-limit / --policies / --http-cache-max-*）

## 已完成 — 修复

- [x] UnicodeEncodeError：上游返回非 ASCII（如中文）响应头时，httpx utf-8 解码后 latin-1 重编码崩溃。新增 `_hb()`（latin-1 + utf-8 回退），应用到全部上游派生编码点（流式/缓存头、reason phrase、Content-Length、CONNECT Host/target）
- [x] Python 3.12 预热连接关闭挂起：3.12 的 `StreamWriter.wait_closed()`/`Server.wait_closed()` 变严格——等待对端 FIN / 活跃 handler 协程。预热池"半连接"（只建 TCP 未发数据）对端永不关闭，导致关闭/测试收尾死锁。修复：router 侧 `_conn_pool_close_all`/`_pool_prune` 的 `wait_closed()` 加 0.5s 超时；测试 mock 上游对空闲连接 5s 超时关闭（模拟真实上游 idle 超时）。仅 CI 3.12 矩阵暴露，3.11 无此问题（CI 双版本矩阵的必要性实证）
- [x] 深夜空闲期预热池空转浪费：opt.log 分析发现 01:00-06:59 零请求时段通用池仍按 refill 周期"建连→空闲过期→重建"，6 代理 6h 白建 ~1400 条连接（100% 被清，约 233 条/小时）。修复：`conn_pool.refill_pause_minutes`（默认 60）——连续 N 分钟无客户端请求则挂起 refill/目标预热，新请求到来立即恢复；暂停期间仍照常 prune 清理过期连接。0=不暂停（向后兼容）。已在 `_handle_client` 认证放行处刷新活动时间戳，探活/预热不算请求活动
- [x] 后台心跳使空闲暂停失效：生产实测发现夜间并非零请求——GitHub Desktop 的 alive.github.com（207 次/夜）/ Windows 的 client.wns.windows.com / Edge 云消息每 3-10 分钟一轮，把"距上次请求"持续拉近，refill_pause_minutes 永不触发（gen7 深夜 conn_pool creates 1855 条 76% 过期、target_pool 95% 过期）。先以 `refill_pause_silence_sec`（间隔一刀切）修复，验证深夜归零后暴露新缺陷：**间隔 > 窗口的真实孤立请求被误当心跳忽略**，refill 在活跃期也从不恢复（gen8 每天 1600-2458 次真实 attempts 期间 refill 依然不恢复）。最终改为**簇度计数**：`refill_pause_activity_window`（默认 120s）+ `refill_pause_min_requests`（默认 3）——窗口内请求数 ≥ 阈值才刷新活动时间戳。真实流量是簇（一次页面加载数秒内多 hostname 并发 CONNECT，计数 5-30），心跳是孤例（计数 1-2），据此区分：既免疫心跳、又不误伤真实孤立请求。窗口=0 或阈值≤1 退化为任意请求都刷新（向后兼容）。**空闲暂停只挂起后台预建（refill/目标预热），永不卡请求路径**（取池/新建/复用已握手连接均不检查空闲态）——新增回归测试钉死此安全属性

## 已完成 — 运维与安全（P2）

- [x] CI（`.github/workflows/test.yml`）：GitHub Actions，push master / PR 触发，Python 3.10 + 3.11 + 3.12 三版本跑 160 测试，带 pytest-timeout(60s) 防挂起；uv.lock 已纳入版本管理，`uv sync --frozen` 锁定依赖

## 待办 — 运维与安全（P2）

- [ ] systemd unit（当前仅 docker-compose 示例，无 systemd 服务文件）
- [ ] 与真实 Squid 的集成测试（bench 已支持 --upstream real，但缺 CI/集成用例）

## 工作流备注

- 每个组件配单元测试；测试保持确定性（mock 网络调用）。
- 生产配置调参记录：`idle_timeout` 30→120→180（目标池命中率 5%→25%）、`single_send_degrade_ratio` 3.0→2.0（降级收敛）、`refill_pause_minutes` 60（深夜空转暂停）→ **0（2026-08-26 废弃：夜间心跳 2-5 分钟一簇，暂停永不触发；空转成本实测可接受，池峰值 12/64）**。`refill_pause_activity_window` 120 + `refill_pause_min_requests` 3（簇度活动判定）保留但随 pause=0 停用。脱敏样例见 `config_xxh_example.yaml`。

## 当前测试状态

- `tests/test_end_to_end.py` 等：**196** 个用例，覆盖转发/缓存/竞速/聚合/认证/熔断/探活/粘性/计数/DB 持久化/UTF-8 头安全/连接预热（通用池 + target 半预连接 + 已建握手隧道复用，含竞速胜出触发预热）+ refill 空闲暂停（refill_pause_minutes + 簇度活动判定 refill_pause_activity_window/min_requests，含"空闲暂停不卡请求路径"回归）+ 健壮性（请求头行数/字节上限、截断响应检测、聚合超时秒级）+ 配置层（extra="forbid" 拼错键拒绝/跨字段校验/cli 退出码 2/logging.level 生效）。
- 运行：`.venv/bin/python -m pytest -q`；CI 三版本（3.10/3.11/3.12）全部通过。
