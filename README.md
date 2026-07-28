# auto_squid

Lightweight MVP for domain-based outbound proxy selection.

Overview
- Run on a gateway host (machine B) to accept HTTP/HTTPS client proxy traffic and forward each request to the best outbound proxy (nodes on B/C/D).
- Periodically probes configured proxies and computes per-proxy scores (latency, throughput, reliability) to drive selection.

Features
- HTTP and HTTPS (CONNECT) forwarding via selected upstream proxy
- Probe engine: TCP connect + HTTP GET, throughput measurement, IQR outlier filtering, time-decay scoring
- Runtime ProxyStore with simple CRUD via Management API
- Management API: /health, /proxies, /score, /probe/status, /probe/history, /probe/states, /metrics
- CLI to start router, probe loop and API server

Quickstart (local)
1. Create a project virtualenv (recommended: uv):
   uv venv .venv --seed && uv sync

2. Prepare a proxies YAML (example format):

```yaml
- id: squid-beijing-01
  name: beijing-01
  host: 10.14.25.86
  port: 3128
  protocol: http
  enabled: true
```

3. Start the service:
   python -m auto_squid.cli start --config ./config.yaml --proxies ./proxies.yaml

4. Verify:
   curl http://127.0.0.1:18080/health
   curl http://127.0.0.1:18080/score

Usage notes
- Configure your client (machine A) to use B:10808 as HTTP/HTTPS proxy (or redirect traffic to that port).
- The Router forwards requests to the chosen upstream proxy; upstream proxies must support CONNECT for HTTPS.

Limitations / MVP notes
- Minimal HTTP parsing and no full persistent connection handling — suitable for prototyping only.
- Management API has no authentication; protect 18080 with firewall or add auth before production.
- Scoring uses simple heuristics; further tuning and more robust probes are recommended.

Development
- Tests: pytest (tests/test_end_to_end.py)
- Main modules: auto_squid/router.py, probe_engine.py, proxy_store.py, api.py, cli.py

Next steps
- Improve HTTP streaming, persistent connections and TLS handling
- Add management API auth and Prometheus metrics
- Add integration tests with Squid and container examples

License: MIT
