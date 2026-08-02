# auto_squid

Lightweight forward proxy with parallel racing, domain-based caching, an HTTP response cache, and SQLite-persisted stats.

> [中文说明 →](README_CN.md)

## Overview

- Runs on a gateway host, accepts HTTP/HTTPS proxy traffic, and forwards each request through upstream proxies
- **Parallel racing**: sends each request to several upstream proxies simultaneously and uses the first successful response; the rest are cancelled and their connections released
- **Domain cache**: once a proxy wins a race for a domain, it is reused for that domain until `cache_ttl` expires — avoids racing every request
- **HTTP response cache**: idempotent `GET` responses are cached in memory (TTL 60s, respects `Cache-Control`)
- **Local racing**: optionally lets the gateway host itself race as a proxy node (direct, no upstream)
- **Domain stats**: per-domain win counts tracked in SQLite, survive restarts
- **Web UI**: a built-in dashboard at `/` for browsing domain stats, default proxies, and win counts with auto-refresh; clicking a stat card filters the domain table to the domains using that proxy as Default Proxy

## Features

- HTTP and HTTPS (`CONNECT`) forwarding with parallel racing across upstream proxies
- Domain-level caching (`cache_ttl`) of the winning proxy per domain
- In-memory HTTP `GET` response cache with `Cache-Control` awareness
- Optional local racing node (gateway races directly alongside upstreams)
- Hop-by-hop header filtering in both directions: request headers (`proxy-authorization`, `connection`, etc.) are stripped before forwarding upstream so client-to-proxy credentials never leak to the next hop; response headers (`transfer-encoding`, `content-encoding`, `content-length`, etc.) are stripped and `Content-Length` is rewritten to the actual body length
- Request body handling with a 10 MB cap (returns `413` on overflow); `Content-Length: 0` is handled correctly (no hang)
- CONNECT tunnels with connect/read timeouts so a stuck upstream cannot hold a race slot forever
- SQLite access serialized with a lock (safe under the FastAPI/uvicorn thread pool)
- Graceful shutdown: in-flight connections are cancelled and drained before the DB is closed
- Runtime `ProxyStore` with YAML persistence; CRUD via the Management API
- Domain-level win statistics persisted to SQLite (`auto_squid.db`)
- Management API + single-page web UI

## Client authentication

The proxy port (`:10808`) can require HTTP Basic auth from clients. It is **off by default** — enable it in `config.yaml`:

```yaml
router:
  auth:
    enabled: true
    username: "admin"
    password: "secret"
```

When enabled, every request (HTTP and `CONNECT`) must carry a `Proxy-Authorization: Basic <base64(user:pass)>` header (clients fall back to `Authorization`). Missing or wrong credentials get a `407 Proxy Authentication Required` with `Proxy-Authenticate: Basic realm="auto_squid"`, and **no upstream work happens**.

```bash
# rejected (no credentials)
curl -x http://127.0.0.1:10808 http://example.com        # → 407
# accepted
curl -x http://admin:secret@127.0.0.1:10808 http://example.com
```

> Auth gates the **proxy port only**. The management API on `:18080` stays open — protect it with a firewall as noted in Limitations.

## Quickstart

1. Create a virtualenv and install dependencies:

   ```bash
   uv venv .venv --seed && uv sync
   ```

   Runtime dependencies: `fastapi`, `uvicorn[standard]`, `httpx`, `pydantic`, `typer`, `pyyaml`.
   Dev: `pytest`, `pytest-asyncio` (`asyncio_mode = "auto"`).

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
   # or the installed entry point:
   auto-squid
   ```

   Options: `--proxies ./proxies.yaml` `--db ./auto_squid.db` `--config ./config.yaml`

4. Verify:

   ```bash
   curl http://127.0.0.1:18080/health
   curl http://127.0.0.1:18080/proxies
   curl http://127.0.0.1:18080/stats
   curl http://127.0.0.1:18080/domains
   ```

   Open the dashboard in a browser: `http://127.0.0.1:18080/`

5. Use as a proxy:

   ```bash
   curl -x http://127.0.0.1:10808 http://www.baidu.com
   curl -x http://127.0.0.1:10808 https://www.baidu.com
   ```

## Architecture

```
Client ──HTTP/S──> auto_squid (proxy :10808)
                      │
                      ├── race ──> upstream proxy 1 (squid)
                      ├── race ──> upstream proxy 2 (squid)
                      ├── race ──> upstream proxy 3 (squid)
                      └── race ──> local (optional, direct)
                      │
                      ▼ fastest success wins; losers cancelled + closed
                      │
                      ▼ default proxy cached per domain (cache_ttl)
```

- **HTTP requests**: raced via `httpx.AsyncClient` (one client per attempt); winning response is written back, loser clients are closed
- **CONNECT requests**: raced via raw `asyncio.open_connection` tunnels with connect/read timeouts
- **Selection**: `ProxySelector.ordered_proxies()` returns a randomly shuffled list of enabled proxies; the first `max_retries` race, then any remaining proxies race as a fallback
- **Domain cache**: after a win, the winning proxy is recorded in `domain_meta` and reused for the domain until `cache_ttl` expires
- **Stats**: `request_counts` (wins), `attempted_counts` (total attempts) per proxy; `domain_stats` (wins per domain per proxy) in SQLite

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /` | Web UI dashboard (domain stats, default proxies, auto-refresh; click a stat card to filter domains by Default Proxy) |
| `GET /health` | Health check |
| `GET /proxies` | List configured proxies |
| `POST /proxies` | Add a proxy (JSON body) |
| `GET /stats` | `request_counts` + `attempted_counts` |
| `GET /metrics` | `request_counts`, `attempted_counts`, and domain stats |
| `GET /config` | Router config (`enable_local_racing`) |
| `GET /domains` | Per-domain win stats from SQLite |
| `GET /domains/meta` | Per-domain default proxy + last-updated time |

## Config

CLI accepts `--config` YAML with structure defined in `config_schema.py`:

```yaml
listen:
  host: "0.0.0.0"
  port: 10808
api:
  host: "0.0.0.0"
  port: 18080
router:
  enable_local_racing: false   # let the gateway host race as a proxy node
  cache_ttl: 600               # domain cache TTL in seconds
logging:
  file: "auto_squid.log"
```

## Testing

```bash
.venv/bin/python -m pytest -q
```

The suite covers HTTP/CONNECT forwarding, the HTTP response cache, the domain cache, local racing, `ProxyStore` CRUD, the API, and binary-safe request body handling.

## Benchmarking

A controlled, repeatable, attributable benchmark harness lives in `bench/`. It spins up **mock upstream proxies** (with configurable latency / response size / chunked / failure rate, each carrying a hit counter) so results are not dominated by real-network jitter, then drives the `Router` under load and reports throughput, latency percentiles, cache hit rate, racing amplification, and resource usage.

> See [`bench/README.md`](bench/README.md) for full details. Quick reference:

```bash
# Default: mock upstreams, concurrency staircase (find the saturation point)
python -m bench.stress

# Smoke run (~10s, small scale)
python -m bench.stress --quick

# Disable the HTTP response cache to measure raw routing performance
# (run with and without to isolate the cache's benefit)
python -m bench.stress --no-http-cache

# All four modes (staircase / rate / mixed / soak)
python -m bench.stress --mode all

# Long-run stability + leak check (default 60s)
python -m bench.stress --mode soak --duration 120

# Profile with cProfile (writes bench_profile.txt)
python -m bench.stress --profile

# Point at real upstream proxies instead of the mock cluster
python -m bench.stress --upstream real --proxies proxies.yaml

# Run N rounds of the SAME condition (fresh subprocess + SQLite + caches per round)
# and report mean±stddev to cancel out environment noise (default 3)
python -m bench.stress --rounds 5
```

Modes:

| Mode | Load shape | Answers |
|------|-----------|---------|
| `staircase` | concurrency 1→200, fixed requests per level | throughput/latency vs. concurrency → **saturation point** |
| `rate` | target RPS 100→2000, sustained | latency/error vs. load → **capacity ceiling** |
| `mixed` | 30% hot + 20% large + 20% chunked + 20% cold + 10% CONNECT | a **realistic mixed profile** |
| `soak` | fixed concurrency, sustained (default 60s) | **stability / resource leaks** |

Key metrics: throughput (req/s), TTFB & total at P50/P95/P99, error rate by category, **cache hit rate** and **racing amplification** (derived from the mock hit counters), and resource samples (RSS, fd count, connection-pool size, HTTP-cache entries). Results print to the terminal and are written to `bench_report.json` (tagged with the git revision, so runs are diffable across versions).

**Multi-round (`--rounds N`, default 3):** each round runs the same scenario on a fresh `server_proc` subprocess with a fresh SQLite DB, caches, and mock upstreams, so inter-round variance is pure environment noise. The report then gives per-metric **mean ± stddev** (plus `round_results` for per-round data and `aggregates` for min/max/mean/stddev); `--rounds 1` keeps the report byte-identical to the single-round schema.

## Limitations

- HTTP parsing is MVP-level; large streaming responses may have edge cases
- Management API has no auth — protect port 18080 with a firewall before production
- CONNECT tunnel uses raw pipes (no TLS interception)

## License

MIT
