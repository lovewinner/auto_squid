# Squid integration guide for auto_squid

Summary
- The auto_squid project provides management, probing, and routing decisions (management API at port 18080).
- Squid (or another forward proxy) is the actual HTTP/HTTPS proxy that terminates client connections and forwards traffic.
- auto_squid probes and scores proxy instances (Squid servers) and can be used to choose the best outbound proxy per-domain.

1) Quick answer to a common question
- http://<host>:18080/ is the management API (FastAPI). It is NOT an HTTP forwarding proxy.
- To access websites via a proxy on this host, run Squid (or another proxy) and use its listen port (commonly 3128 or 8080).

2) Minimal Squid config snippet (example /etc/squid/squid.conf)

# listen on all interfaces, port 3128
http_port 3128 accel vhost

# allow local network and management host (adjust network to your environment)
acl localnet src 10.14.25.0/24
acl manager src 127.0.0.1/32 10.14.25.86/32
http_access allow manager
http_access allow localnet
http_access deny all

# enable logging & optimizations (production tune as needed)
access_log /var/log/squid/access.log squid
cache_log /var/log/squid/cache.log

Restart Squid after changes: sudo systemctl restart squid

3) Registering Squid instances with auto_squid
- Format for proxies configuration (examples/proxies.yaml below) follows the ProxyInfo schema in config_schema.py.
- Put one entry per Squid instance (id, name, host, port, protocol).

4) Example auto_squid proxies file (examples/proxies.yaml)
- See repository examples/proxies.yaml

5) Example auto_squid config (examples/config.yaml)
- See repository examples/config.yaml. Key points:
  - api.bind_remote: true (allow management API to bind remote interfaces)
  - probe.url: endpoint used for HTTP probing (e.g., https://www.gstatic.com/generate_204)

6) How to test the proxy from a client machine
- HTTP test through Squid:
  curl -x http://10.14.25.86:3128 -I http://example.com
- HTTPS test (CONNECT):
  curl -x http://10.14.25.86:3128 -I https://example.com

7) How auto_squid uses Squid
- auto_squid's probe engine (when implemented) will run probes against the Squid host:port pairs and compute latency/throughput/reliability scores per (domain, proxy).
- The management API allows querying scores (/score) and controlling the proxy registry (future endpoints).

8) Security & network recommendations
- Only open Squid listen ports to trusted subnets; prefer firewall rules to limit access.
- If exposing the management API (18080) remotely, protect it with firewall rules and API authentication (token / TLS).
- For production, place Squid behind TLS-terminating reverse proxies or use iptables rules to allow trusted clients only.

9) Next steps (suggested)
- Add proxy entries to examples/proxies.yaml and enable auto_squid to read them on startup.
- Implement authenticated management endpoints and ensure probe_engine uses the configured proxies for probing.
- Optionally add an admin UI to show current best proxy per domain.

If you want, auto_squid can: generate the proxies file in the repo, or implement the small admin endpoints to add/remove proxies at runtime.