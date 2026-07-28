# auto_squid — 设计文档（概要）

目标概述
- 提供一个在计算机 B 上运行的代理入口（forwarding proxy），按域名为来自计算机 A 的每个请求选择最优的出站代理节点（可在 B/C/D 等运行），并把流量转发到该节点以访问目标网站。

高层架构组件
- Listener / Router
  - 功能：作为客户端（A）的上游代理入口，支持 HTTP 查询与 CONNECT 隧道（HTTPS）。
  - 责任：解析请求的目标域名，查找 DomainIndex，选择最佳代理（通过 ProxySelector），并把请求转发到所选代理（建立 TCP 或 HTTP 连接）。

- ProxyStore
  - 管理已知代理实例（id, host, port, protocol, enabled, tags）。
  - 提供 CRUD 操作与持久化（YAML/JSON 加载/保存）。

- DomainIndex
  - 跟踪近实时活跃域名与访问频率，用于探测优先级调度与缓存选择结果。

- ProbeEngine
  - 周期性对每个 (domain, proxy) 对运行探测：
    - TCP connect 时间（快速）
    - HTTP GET 到 probe_url（204 或短页面）以测 RTT
    - 可选小文件下载以估测吞吐（8KB）
  - 数据聚合：对最近 history_minutes 的样本做时衰加权（半衰期 half_life_minutes），过滤异常值（IQR），并输出三项组件分数：L_score（延迟）、T_score（吞吐）、R_score（可靠性）。
  - 合成总分：score = α * L_score + β * T_score + γ * R_score（可配置）。
  - 并发控制：全局 semaphore 与 per-domain/per-proxy 限制。

- ProxySelector
  - 利用 ProbeEngine 提供的 score 查询（或缓存）按域名返回优先代理列表（带置信度/状态：warming, normal）。

- Management API (FastAPI)
  - /health, /metrics, /proxies (list/add/remove), /score?domain=, /probe/status
  - 提供运行时观察与管理接口（建议：token 验证中间件）。

- Observability
  - 日志（请求路由、探测结果摘要、错误）与基本指标（探测延迟分布、选择次数）。

请求转发流程（简化）
1. 客户端 A 发起 HTTP 请求或 CONNECT 到 B:10808。
2. Router 解析 Host/Authority（CONNECT 使用目标 host:port，HTTP 使用 Host header）。
3. DomainIndex.touch(domain) 更新活跃性。
4. ProxySelector.query(domain) 返回优先代理（基于最新 score 缓存或即时计算）。
5. Router 建立到选中代理的连接：
   - 对 HTTP：代理协议（代理握手 -> 将原始请求写入到代理 -> 等待响应 -> 转回客户端）。
   - 对 HTTPS (CONNECT)：建立到选中代理的 CONNECT 隧道（发送 CONNECT 到代理），并在隧道建立后在二进制层转发客户端与代理的数据流。
6. 在代理失败或超时情况下，Router 依次尝试次优代理或返回 502/504，并记录事件用于评分与监控。

打分与时间加权示意
- 每个样本带权重 w = exp(-ln2 * age_minutes / half_life_minutes)
- 使用 IQR 去除异常样本
- latency_score = map_latency_to_0_100(latency_ms, latency_max)
- throughput_score = map_throughput_to_0_100(kbps, throughput_max)
- reliability_score = recent_successes / attempts * 100
- total_score = latency_weight * latency_score + throughput_weight * throughput_score + reliability_weight * reliability_score

冷启动与最小样本策略
- min_samples = 3：若样本不足，标记为 warming，优先使用全局或基线测量（TCP connect）并降低权重。

故障处理
- 对单请求：在 proxy 连接建立失败时使用短重试（切换到下一个最佳代理），记录并触发快速探测（提升优先级）。
- 对探测：若探测频繁失败，暂时将该 proxy 标记为 degraded 并降低对其的选择权重。

配置与部署
- 配置文件（YAML）包含 listen、api、probe、score、logging、proxies。
- 启动：systemd unit 示例，或 docker-compose（容器化部署可将 auto-squid 与 Squid 节点分离）。

安全建议
- 管理 API 使用 token 或 TLS + 客户端 IP 白名单。
- 建议在可信网络或通过防火墙限制 10808/18080 的入站访问。
- 不在仓库中包含代理凭据，使用运行时 secret 管理。

扩展点
- 权重自动学习（基于真实请求成功率与延迟历史）
- 本地缓存最优路由以减少探测频率
- 支持基于地理/标签的策略（例如优先同地域节点）

附录：最小可交付版本（MVP）
- 支持 HTTP/CONNECT 转发、ProxyStore 加载 proxies.yaml、ProbeEngine 最小探测（TCP + HTTP GET）、简单 scoring、/proxies 与 /score API、单元测试与 smoke tests。