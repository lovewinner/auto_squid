# Observability

This document lists recommended metrics, logging fields, and alerts.

Prometheus metrics (suggested names & types)
- auto_squid_probe_latency_ms_bucket{domain,proxy_id} (histogram)
- auto_squid_probe_success_total{domain,proxy_id} (counter)
- auto_squid_probe_fail_total{domain,proxy_id} (counter)
- auto_squid_route_requests_total{domain,proxy_id} (counter)
- auto_squid_active_connections (gauge)
- auto_squid_domain_queue_length (gauge)
- auto_squid_db_write_errors_total (counter)

Logging (structured JSON fields)
- timestamp
- level
- module
- domain
- proxy_id
- client_ip
- action (probe, route, api)
- duration_ms
- status
- error

Example alert rules (Prometheus style)
- Alert: High probe failure rate
  - expr: sum by (domain)(rate(auto_squid_probe_fail_total[5m])) / sum by(domain)(rate(auto_squid_probe_success_total[5m])) > 0.3
- Alert: DB write errors
  - expr: increase(auto_squid_db_write_errors_total[5m]) > 0

/metrics endpoint
- Expose `/metrics` using `prometheus_client` or equivalent. Include basic process metrics (memory, cpu) and custom app metrics.

Grafana
- Panels:
  - Probe latency heatmap (domain × proxy)
  - Top 10 domains by request count
  - Probe success rate over time
  - DB write errors and probe queue length

