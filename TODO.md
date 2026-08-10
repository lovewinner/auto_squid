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
- [x] 会话粘性（per-client+domain，滑动 TTL）：redispatch、5xx 驱逐、recheck 重竞速、容量上限
- [x] bench 透传全部速度特性开关（--conn-pool / --adaptive-ttl / --switch-damping / --concurrency-limit / --policies / --http-cache-max-*）

## 已完成 — 修复

- [x] UnicodeEncodeError：上游返回非 ASCII（如中文）响应头时，httpx utf-8 解码后 latin-1 重编码崩溃。新增 `_hb()`（latin-1 + utf-8 回退），应用到全部上游派生编码点（流式/缓存头、reason phrase、Content-Length、CONNECT Host/target）

## 待办 — 运维与安全（P2）

- [ ] systemd unit（当前仅 docker-compose 示例，无 systemd 服务文件）
- [ ] 与真实 Squid 的集成测试（bench 已支持 --upstream real，但缺 CI/集成用例）
- [ ] CI（当前无 .github/workflows，测试仅本地运行）

## 工作流备注

- 每个组件配单元测试；测试保持确定性（mock 网络调用）。

## 当前测试状态

- `tests/test_end_to_end.py` 等：133 个用例，覆盖转发/缓存/竞速/聚合/认证/熔断/探活/粘性/计数/DB 持久化/UTF-8 头安全。
- 运行：`.venv/bin/python -m pytest -q`
