# 代码审查与性能优化报告

审查目标：以客户端端到端响应速度为主要指标；可以接受适度增加上游请求、连接和 CPU 消耗。静态审查日期：2026-09-05。

## 高优先级缺陷

### P0：GET 聚合超时会取消共享 Future

`auto_squid/router.py:2810` 对共享的在途 Future 直接调用 `asyncio.wait_for(existing, ...)`。当 waiter 超时时，`wait_for` 会取消该 Future；首个请求随后不能再写入结果，后续请求 await 已取消 Future 时可能直接抛出 `CancelledError` 并断开。

修复为 `await asyncio.wait_for(asyncio.shield(existing), timeout=...)`，并增加“首请求超过聚合等待上限、多个 waiter 回退”的回归测试。

### P0：已承载业务数据的 CONNECT 隧道被复用

`auto_squid/router.py:3509` 在客户端断开时停止双向转发，`router.py:3579` 又将到上游的既有隧道归还 `_established_pool`。该隧道可能已经承载过 TLS/应用字节；目标端不知道旧会话结束，下个客户端向同一字节流发送新的 TLS ClientHello 会破坏协议边界，并可能造成数据串扰。

不要归还正常业务隧道。只允许复用从未转发应用数据的竞速败者和预握手隧道；`conn_pool.established_reuse` 应维持默认关闭，直到实现该约束。

### P1：域名缓存单发的 HTTP 5xx 不会回退竞速

`auto_squid/router.py:3203` 的域名缓存命中会直接流式写出 5xx，并记录该代理为粘性代理。竞速路径则会排除 5xx，因此两条路径语义不一致，故障代理会持续向客户端返回错误。

在写响应头前检查单发响应状态；若为 5xx，关闭响应、失效/降级该代理并进入竞速。新增“缓存代理 5xx、备用代理 2xx”的端到端测试。

### P1：共享 HTTP 缓存忽略 `Vary` 和 `Set-Cookie`

缓存键仅是 `method:url`（`auto_squid/http_cache.py:80`）。不同的 `Accept-Encoding`、语言等请求可收到错误表示；带 `Set-Cookie` 的可缓存响应还可能被共享给其他客户端。

至少跳过带 `Set-Cookie`、`Vary: *` 或未支持 `Vary` 字段的响应；更完整的方案是将允许的 Vary 请求头规范化后纳入缓存键。

### P2：下游 HTTP/1.1 连接没有 keep-alive

`auto_squid/router.py:2670` 的 `finally` 无条件关闭客户端连接，导致每个普通 HTTP 请求都重新建立客户端到代理的 TCP 连接。对顺序 API 调用和多资源页面加载会增加响应延迟。

将请求解析和分发置于顺序处理循环中，响应后仅在 `Connection: close`、协议要求关闭或异常时断开。初版可以明确不支持 HTTP pipelining，但应支持顺序 keep-alive。

## 响应速度优先的优化建议

1. 修复上述 P0/P1 后再做性能调参，避免用不正确的复用掩盖问题。
2. 将错峰竞速改为 `stagger_initial: 2`、`stagger_interval_ms: 100`。这会增加上游扇出，但可降低首选代理突发变慢时的 P95/P99。
3. 将热路径的池命中、MISS、归还等 `INFO` 日志改为 `DEBUG` 或周期聚合指标，避免同步日志 I/O 阻塞事件循环。
4. 流式响应不要每个 chunk 都调用 `drain()`；累计多个 chunk 或在 transport 写缓冲到高水位时再 drain，以降低系统调用与调度开销，同时保持背压。
5. 将 httpx 每代理连接上限、keep-alive 数量和 HTTP/2 开关配置化。延迟优先的部署可提高连接上限，避免连接池排队占用 5 秒超时窗口；是否启用 HTTP/2 应通过真实上游压测确认。

## 验证状态

- `python3 -m compileall -q auto_squid`：通过。
- 使用最小 asyncio 验证确认：`asyncio.wait_for()` 超时会取消未屏蔽的 Future。
- 完整测试未运行：当前环境没有 `uv`（`uv: command not found`）。可在依赖齐备后执行 `uv run pytest -q --timeout=60`。
