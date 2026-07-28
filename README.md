# auto_squid

Lightweight forward proxy with parallel racing, domain-based stats, and SQLite persistence.

## Overview

- Runs on a gateway host, accepts HTTP/HTTPS proxy traffic, and forwards each request through upstream proxies
- **Parallel racing**: sends each request to all upstream proxies simultaneously, uses the first successful response
- **Probe engine**: periodically probes proxies, computes scores (latency, throughput, reliability) for selection
- **Domain stats**: per-domain win counts tracked in SQLite, survives restarts

## Features

- HTTP and HTTPS (CONNECT) forwarding with parallel racing across upstream proxies
- Weighted random proxy ordering via probe scores
- Hop-by-hop header filtering (`transfer-encoding`, `content-encoding`, etc.) + `Content-Length` rewrite
- Runtime ProxyStore with YAML persistence, CRUD via Management API
- Probe engine: TCP connect + HTTP GET, throughput measurement, IQR outlier filtering, time-decay scoring
- Domain-level win statistics persisted to SQLite (`auto_squid.db`)
- Management API: `/health`, `/proxies`, `/score`, `/probe/*`, `/stats`, `/domains`, `/metrics`
- CLI to start router, probe loop and API server

## Quickstart

1. Create a virtualenv:

   ```bash
   uv venv .venv --seed && uv sync
   ```

2. Prepare `proxies.yaml`:

   ```yaml
   - id: squid-01
     name: beijing-01
     host: 10.14.25.86
     port: 3128
     protocol: http
     auth:
       username: "user"
       password: "pass"
     enabled: true
   ```

3. Start:

   ```bash
   python -m auto_squid.cli
   ```

   Options: `--proxies ./proxies.yaml` `--db ./auto_squid.db` `--config ./config.yaml`

4. Verify:

   ```bash
   curl http://127.0.0.1:18080/health
   curl http://127.0.0.1:18080/proxies
   curl http://127.0.0.1:18080/stats
   curl http://127.0.0.1:18080/domains
   ```

5. Use as proxy:

   ```bash
   curl -x http://127.0.0.1:10808 http://www.baidu.com
   curl -x http://127.0.0.1:10808 https://www.baidu.com
   ```

## Architecture

```
Client ──HTTP/S──> auto_squid (B:10808)
                      │
                      ├── parallel ──> upstream proxy 1 (squid)
                      ├── parallel ──> upstream proxy 2 (squid)
                      └── parallel ──> upstream proxy 3 (squid)
                      │
                      ▼ fastest response wins, rest cancelled
```

- **HTTP requests**: raced via `httpx.AsyncClient` with per-request clients
- **CONNECT requests**: raced via raw `asyncio.open_connection` tunnels
- **Scoring**: probe engine periodically tests each proxy; `ProxySelector.ordered_proxies()` returns weighted random ordering (ignored in racing mode since all proxies are tried)
- **Stats**: `request_counts` (wins), `attempted_counts` (total attempts) per proxy; `domain_stats` (wins per domain per proxy) in SQLite

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Health check |
| `GET /proxies` | List configured proxies |
| `POST /proxies` | Add a proxy (JSON body) |
| `GET /score` | Current probe scores per proxy |
| `GET /probe/status` | Probe loop running status |
| `GET /probe/history` | Probe history samples |
| `GET /probe/states` | Proxy states (warming/normal/degraded) |
| `GET /stats` | `request_counts` + `attempted_counts` |
| `GET /domains` | Per-domain win stats from SQLite |
| `GET /metrics` | Combined scores, states, counts, domain stats |

## Config

CLI accepts `--config` YAML with structure defined in `config_schema.py`:

```yaml
listen:
  host: "0.0.0.0"
  port: 10808
api:
  host: "0.0.0.0"
  port: 18080
probe:
  url: "http://www.baidu.com"
  interval: 60
  timeout: 10
  concurrency: 5
  history_minutes: 60
  min_samples: 10
logging:
  file: "auto_squid.log"
```

## Limitations

- HTTP parsing is MVP-level; large streaming responses may have edge cases
- Management API has no auth — protect port 18080 with firewall before production
- CONNECT tunnel uses raw pipes (no TLS interception)

## License

MIT
