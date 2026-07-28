# TODO — Implementation roadmap (prioritized)

目标：把 auto_squid 从设计骨架完善为能在 B 上运行、按域名选择并转发到最优代理的可用原型。

优先级 P0 — MVP
- [ ] Implement HTTP/CONNECT forwarding router (auto_squid/router)
  - Parse Host/CONNECT target, perform proxy selection, forward request/stream.
- [ ] Implement ProxyStore persistence (load from examples/proxies.yaml) and runtime CRUD API.
- [ ] Implement ProbeEngine minimal probes
  - TCP connect timing
  - HTTP GET to probe_url (204) for RTT
  - Store timestamped samples in memory (history window)
- [ ] Implement scoring (latency/throughput/reliability) with time decay and IQR outlier removal.
- [ ] Expose Management API endpoints: /health, /proxies, /score?domain=, /probe/status.
- [ ] End-to-end smoke test: client -> B (auto_squid) -> selected proxy -> website

优先级 P1 — Robustness & features
- [ ] Add concurrency controls (global semaphore, per-domain/per-proxy limits) in probe engine.
- [ ] Implement cold-start strategy and warming state for proxies/domains.
- [ ] Add logging, metrics endpoints (/metrics) and store probe history for debugging.
- [ ] Add retry/failover policy for request forwarding.
- [ ] Add configuration file parsing and CLI flags.

优先级 P2 — Ops & Security
- [ ] Add token-based auth to management API and optional TLS support.
- [ ] Add systemd unit and docker-compose examples.
- [ ] Add integration tests with mocked proxy backends and end-to-end integration with Squid.

优先级 P3 — Enhancements
- [ ] Throughput probe implementation and measurement aggregation.
- [ ] Policy engine: per-domain static rules, tag-based routing, geolocation awareness.
- [ ] Admin UI to display scores and choose manual override for routing.

Workflow notes
- Work in short iterative branches named feature/<area> (e.g., feature/probe-engine).
- Add unit tests for each component; keep probe engine tests deterministic by mocking network calls.

If helpful, add these TODOs into the repository issue tracker or create tickets from the P0 list. For immediate next step, implement router and proxy_store (P0 first two bullets).