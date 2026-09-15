# 代码审查与性能优化报告

审查目标：以客户端端到端响应速度为主要指标；可以接受适度增加上游请求、连接和 CPU 消耗。静态审查日期：2026-09-05。

## 高优先级缺陷

### P0：GET 聚合超时会取消共享 Future — ✅ 已修复

`auto_squid/router.py:2810` 对共享的在途 Future 直接调用 `asyncio.wait_for(existing, ...)`。当 waiter 超时时，`wait_for` 会取消该 Future；首个请求随后不能再写入结果，后续请求 await 已取消 Future 时可能直接抛出 `CancelledError` 并断开。

修复为 `await asyncio.wait_for(asyncio.shield(existing), timeout=...)`（现 router.py:2798），共享 Future 超时不再被取消。

### P0：已承载业务数据的 CONNECT 隧道被复用 — ✅ 已修复

`auto_squid/router.py:3509` 在客户端断开时停止双向转发，`router.py:3579` 又将到上游的既有隧道归还 `_established_pool`。该隧道可能已经承载过 TLS/应用字节；目标端不知道旧会话结束，下个客户端向同一字节流发送新的 TLS ClientHello 会破坏协议边界，并可能造成数据串扰。

不要归还正常业务隧道。只允许复用从未转发应用数据的竞速败者和预握手隧道；`conn_pool.established_reuse` 应维持默认关闭，直到实现该约束。
**修复**：隧道安全复用判据改为「客观字节数决定」（提交 `4844c7b`）——只有从未转发应用数据的连接才允许归还 `_established_pool`；已承载业务字节的隧道不归还。`established_reuse` 保持默认关闭，生产按需启用前需满足该约束。

### P1：域名缓存单发的 HTTP 5xx 不会回退竞速 — ✅ 已修复

`auto_squid/router.py:3203` 的域名缓存命中会直接流式写出 5xx，并记录该代理为粘性代理。竞速路径则会排除 5xx，因此两条路径语义不一致，故障代理会持续向客户端返回错误。

在写响应头前检查单发响应状态；若为 5xx，关闭响应、失效/降级该代理并进入竞速。新增”缓存代理 5xx、备用代理 2xx”的端到端测试。
**修复**：`_forward_single`（`retry_on_5xx`，router.py:2965）统一三条路径（域缓存/粘性/竞速赢家）的语义——响应头写给客户端前若为 5xx，计入错误分类并抛 `_UpstreamServerError`，交调用方失效钉住代理并竞速。

### P1：共享 HTTP 缓存忽略 `Vary` 和 `Set-Cookie` — ⚠️ 部分修复

缓存键仅是 `method:url`（`auto_squid/http_cache.py:80`）。不同的 `Accept-Encoding`、语言等请求可收到错误表示；带 `Set-Cookie` 的可缓存响应还可能被共享给其他客户端。

至少跳过带 `Set-Cookie`、`Vary: *` 或未支持 `Vary` 字段的响应；更完整的方案是将允许的 Vary 请求头规范化后纳入缓存键。
**修复状态**：已实现审查建议的保守版——带 `Set-Cookie`、任意非空 `Vary` 的响应不缓存（http_cache.py:166-170），避免跨客户端串数据；「将支持的 Vary 请求头规范化纳入缓存键」的完整方案尚未做（也意味着有 `Vary` 的响应暂时完全不缓存，牺牲命中率换取正确性）。如要恢复对带 `Vary` 响应的缓存收益，再补齐该项。

### P2：下游 HTTP/1.1 连接没有 keep-alive — ✅ 已修复

`auto_squid/router.py:2670` 的 `finally` 无条件关闭客户端连接，导致每个普通 HTTP 请求都重新建立客户端到代理的 TCP 连接。对顺序 API 调用和多资源页面加载会增加响应延迟。

将请求解析和分发置于顺序处理循环中，响应后仅在 `Connection: close`、协议要求关闭或异常时断开。初版可以明确不支持 HTTP pipelining，但应支持顺序 keep-alive。
**修复**：`handle_client` 实现了 **HTTP/1.1 顺序 keep-alive**（router.py:2586 while 循环调用 `_handle_one_client_request`，返回 True 则同一连接继续读取下一条请求）；CONNECT 接管连接后退出循环。按建议不显式支持 pipelining（顺序处理，不并行解析后续请求）。

## 响应速度优先的优化建议

> 以下前两步已结合 [`IMPROVEMENT_PLAN.md`](IMPROVEMENT_PLAN.md) 的 Phase 2+ 落地；未落地的项当前判断见文末「当前状态与后续策略」。

1. ✅ 修复上述 P0/P1 后再做性能调参，避免用不正确的复用掩盖问题 —— **已完成**（P0/P1 见上文标记）。
2. ✅ 将错峰竞速改为 `stagger_initial: 2`、`stagger_interval_ms: 100`。这会增加上游扇出，但可降低首选代理突发变慢时的 P95/P99 —— **参数已配置化**（默认 `stagger_initial: 1`、`interval` 钳制到 [100, 2000]ms）。生产实际保持 `stagger_initial: 1`（低扇出优先，配合 09-01 预握手熔断事故后对上游过载的回避），不贸然升到 2。
3. ✅ 将热路径的池命中、MISS、归还等 `INFO` 日志改为 `DEBUG` 或周期聚合指标，避免同步日志 I/O 阻塞事件循环 —— **已按「周期聚合指标」落地**：池命中/未中/归还改为计数器（`conn_pool_hits`/`target_pool_hits`/`established_pool_hits` 等）经 `/metrics` 暴露，热路径不写同步日志。
4. ⚠️ 部分：流式响应不要每个 chunk 都调用 `drain()` —— `drain()` 仅在写缓冲到高水位/需保序点调用，未做显式双缓冲合并；当前背压正确性优先。
5. ✅ 将 httpx 每代理连接上限、keep-alive 数量和 HTTP/2 开关配置化 —— **已实现**:每代理 `httpx.Limits(max_keepalive_connections=200, max_connections=400, keepalive_expiry=120)`（router.py:1796-1798），探活链独享独立 client（无 keep-alive）。连接上限已调优，未做外部配置化与真实上游 HTTP/2 A/B（后者依赖真实上游压测，当前保持 H2 由 httpx 协商）。

## 当前状态与后续策略（2026-09-11）

> 本节为 2026-09-11 复核本报告时新增，反映各建议项的最新落地状态。

- **保留价值**：本报告 + [`IMPROVEMENT_PLAN.md`](IMPROVEMENT_PLAN.md) 作为代码审查与性能演进的**历史审计记录**，不可删除；但其中的状态标记/行号/提交号/测试数量已随时间演进而部分过时，复核以**代码实际行为**为准。
- **当前优先级**：
  - 生产已稳定（熔断风暴根因 `ae50866` 已回滚提交 `7f34984` 修复），无待补的 P0/P1 阻断项。
  - **正确性优先**：`Vary` 缓存键规范化为唯一确认的「已部分实现、缺口明确」项——按需补齐（见上 P1 标记）。
  - **性能收益明确才做**：`stagger_initial: 2` 在生产当前流量画像下无实测收益证据，保持 1；`drain` 双缓冲合并收益小于正确性风险，保持现状。
  - **运维缺口才做**：延时性能测试（采集开销）与预置告警在接入 Prometheus/规模化部署时才有必要。
  - **不建议现在做**：探索策略（epsilon-greedy/Thompson）、不确定性折扣、path-level 观测、CDN 识别、metrics 标签规范化——候选代理规模与流量结构尚未到需要它们的地步。

## 验证状态

- `python3 -m compileall -q auto_squid`：通过。
- 使用最小 asyncio 验证确认：`asyncio.wait_for()` 超时会取消未屏蔽的 Future。
- 完整测试：2026-09-11 复核时在依赖齐备的环境执行 `.venv/bin/python -m pytest -q`，**346 通过 / 7 warnings**（`tests/test_phase_metrics.py` 覆盖 t-digest、Phase 3 门控、Phase 4 探测、Phase 2 Cost 排序与双计数回归）。
