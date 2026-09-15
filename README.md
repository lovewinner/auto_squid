# auto_squid

A lightweight forward proxy with **parallel racing**, **multi-objective Cost ranking**, **domain caching**, an **HTTP response cache**, **session stickiness**, and **SQLite-persisted stats**. Runs on a gateway host and forwards each HTTP/HTTPS request through a pool of upstream proxies — the fastest winner takes all, losers are cancelled and released.

> [中文说明 →](README_CN.md)

---

## What this is

Every client request (HTTP or `CONNECT`) is raced across upstream proxies: candidates are sorted by a weighted Cost of latency + success rate + throughput, the best 1–2 launch first (RFC 8305 §5 staggered start), and the rest refill at ~250 ms intervals. The first first-byte response wins; the rest are cancelled. The result is lower TTFB than a single proxy, built-in failover when proxies degrade, and no manual server-configuration required on the upstream side.

Beyond racing, auto_squid gives you: domain-level caching of race winners, optional session stickiness (stable egress IP), an in-memory HTTP GET response cache, three layers of TCP connection pooling for CONNECT, a built-in management API with a dashboard, and per-proxy/per-domain **windowed (recent 256) and lifetime (t-digest) metrics** with error classification.

## Quickstart

```bash
# 1. Install
uv venv .venv --seed && uv sync          # runtime: fastapi, uvicorn[standard], httpx, pydantic, typer, pyyaml

# 2. Prepare upstream proxies (see proxies.yaml for the schema)
cp proxies.yaml examples/proxies.yaml   # placeholder — fill in your own

# 3. Start
python -m auto_squid.cli
# or: auto-squid            # installed entry point (pyproject.toml)
# Options: --proxies ./proxies.yaml --db ./auto_squid.db --config ./config.yaml

# 4. Verify
curl http://127.0.0.1:18080/health        # → {"status":"ok"}
curl http://127.0.0.1:18080/proxies
curl http://127.0.0.1:18080/stats

# 5. Use as a proxy
curl -x http://127.0.0.1:10808 http://www.baidu.com
curl -x http://127.0.0.1:10808 https://www.baidu.com
```

Then open the dashboard: [http://127.0.0.1:18080/](http://127.0.0.1:18080/)

## Features

- **Parallel racing + staggered start** — candidates EWMA-sorted; best 1–2 launch first, rest refill at ~250 ms (RFC 8305 §5); first first-byte wins, losers cancelled & closed
- **Multi-objective Cost ranking** (default on) — racing order = `w_lat·norm(latency) + w_sr·norm(1−success_rate) + w_tp·norm(1−throughput)` with **min–max normalization inside the candidate set** (weights directly comparable, scale-free). The latency term uses the **lifetime TTFB P99** (t-digest) with EWMA fallback; success rate is Laplace-smoothed (domain-level preferred); throughput needs a 1 MB byte floor. `cost_sort_enabled: false` rolls back to pure EWMA ranking instantly
- **Cost hot-reload + auto-tuner** (P1) — `GET/POST /cost` reads/updates all cost parameters at runtime (effective on the very next race, no restart); `POST /tuner` toggles a conservative hill-climbing auto-tuner that adopts only ≥5% winner-TTFB improvements with no success-rate regression, with automatic rollback on degradation
- **Domain cache** — once a proxy wins a race for a domain, it is reused until `cache_ttl` expires
- **Session stickiness** — same client IP + domain/target reuses the same proxy (stable egress IP); failing or 5xx-returning sticky proxies are evicted and re-raced; periodic re-check (`recheck_hits`); in-memory table with a hard capacity cap
- **HTTP response cache** — idempotent `GET` responses cached in memory (TTL 60 s, respects `Cache-Control`; POST/PUT/DELETE/PATCH invalidate the domain's cached GETs)
- **In-flight GET coalescing** — concurrent requests to the same URL share one upstream request (bounded wait, falls back to racing)
- **Local racing** — optionally let the gateway host itself race as a proxy node (direct, no upstream)
- **Three-layer CONNECT TCP pooling** — generic upstream pool, target half-preconnection (`target_prewarm`), and established-handshake reuse (`established_reuse`); plus **request-cluster predictive pre-warm** (`cluster_predict`) that pre-builds "to-upstream" TCP for targets predicted to co-occur in the next page-load window
- **Business-aligned probing** (default off) — `probe_with_get` follows the CONNECT liveness probe with a lightweight GET against a whitelist, recording TTFB/protocol/throughput into the real domain metric buckets
- **Observability** — per-proxy & per-domain **windowed** (last 256) **and** lifetime (t-digest, cross-restart) percentiles; error classification (timeout/connect/5xx/HTTP-status/TLS/protocol/cancelled/other); HTTP protocol-version distribution; per-proxy **Cost breakdown** showing which component dominates the ranking; Bayesian-smoothed success rates with low-confidence flags
- **Domain stats** — per-domain win counts persisted to SQLite, survive restarts
- **Web UI** — built-in dashboard at `/` for browsing domain stats, default proxies, and win counts with auto-refresh; click a stat card to filter domains by that proxy as Default Proxy
- **Config-driven everything** — all knobs live in `config.yaml` (`router.circuit.*`, `router.stickiness`, `router.conn_pool`, etc.), validated at load time (`extra="forbid"`, cross-field validation, clean exit code 2 with readable messages)
- **Client auth** — optional HTTP Basic auth on the proxy port (`:10808`) and/or the management API (`:18080`), off by default
- **Hop-by-hop header filtering** — `proxy-authorization` is stripped before forwarding upstream (client credentials never leak); `Content-Length` is rewritten to the actual body length after stripping transfer/encoding headers
- **Graceful shutdown** — in-flight connections cancelled and drained before the DB closes

## Ports

| Port | Purpose |
|------|---------|
| `10808` | Proxy port — HTTP/HTTPS traffic in (`-x http://127.0.0.1:10808 ...`) |
| `18080` | Management API + Web UI (`/health`, `/proxies`, `/metrics`, `/`) |

## Request pipeline

Every connection goes through the same decision chain (all in `auto_squid/router.py`):

1. **Connection & auth** — `handle_client` reads the request head; enforces `Proxy-Authorization` (407 before any upstream work). `CONNECT` → `_handle_connect`, plain HTTP → `_handle_http_request`.
2. **Short-circuits** — `local_direct_domains` whitelist (dial origin directly, no upstream); in-flight GET coalescing.
3. **Dispatch decision** (`_dispatch_single`) — in priority order:
   - **Session stickiness** → single-send on the pinned proxy (no racing)
   - **Domain cache** → single-send on the domain's last race winner within `cache_ttl`
   - **Racing** (`_race_staggered`) — Cost-sorted candidates, staggered start; first first-byte wins
4. **Degradation feedback** — a failing single-send triggers `_degrade_send_proxy` so the *next* request for that domain skips stickiness/cache and re-races; immediate-degrade markers clear when a new race winner is established.
5. **Failure accounting** — racing losers call `record_failure` per attempt, feeding the circuit breaker in `selector.py`: `circuit_threshold` consecutive failures open a circuit with exponential backoff (8 s → up to 300 s). Business rejections (`http_status`, e.g. CONNECT upstream returning non-200) do **not** trip the circuit — only proxy-quality signals do.
6. **Connection reuse** — three pooling layers sit beneath everything: generic upstream TCP pool, target half-preconnection, and established-handshake reuse; stale pooled connections are detected on handshake and retried fresh without penalizing the circuit breaker.

### Where each knob acts

| Knob | Acts at | Effect |
|---|---|---|
| `cache_ttl` | Domain cache (step 3.2) | How long a race winner is reused |
| `stickiness_*` | Sticky lookup (step 3.1) | Enable pinning / TTL / recheck / capacity |
| `max_retries` | Racing (step 3.3) | Initial race fan-out width |
| `cost_sort_enabled` | Candidate sort | Multi-objective ranking vs pure EWMA |
| `circuit_threshold` | Failure accounting (step 5) | Consecutive failures before circuit opens |
| `single_send_degrade_*` | Degrade gate (step 4) | Unpin a failing single-send before it breaks the circuit |

## Configuration

All configuration lives in `config.yaml`. Here is the full structure with defaults (the sensitive parts are omitted):

```yaml
listen:                        # host/port for the proxy port (default 0.0.0.0:10808)
api:
  host: "0.0.0.0"              # management API host
  port: 18080                    # management API port
  auth:                          # optional HTTP Basic auth for the management API (off by default)
    enabled: true
    username: "..."
    password: "..."
router:
  enable_local_racing: false     # let the gateway host race as a proxy node
  cache_ttl: 600                 # domain cache TTL (seconds)
  max_retries: 3                 # racing fan-out: first N candidates race in the first batch
  stagger_start: true            # RFC 8305 staggered start (best first, refill at interval)
  stagger_initial: 2             # initial batch size when no EWMA history (min 1, max max_retries)
  stagger_interval_ms: 100       # RFC 8305 refill interval (ms; minimum allowed: 100)
  local_direct_domains: []       # targets dialed directly (no upstream) — for internal services
  circuit:                       # probe + circuit-breaker + single-send signals
    probe_interval_sec: 20
    probe_canary: "www.baidu.com:443"
    circuit_threshold: 3         # consecutive failures → circuit open
    circuit_max_backoff: 300     # exponential backoff cap (seconds)
    slow_start_window: 60        # consecutive successes to exit slow-start
    slow_start_success: 3
    lb_bias: 0.5                 # how much in-flight backlog penalizes a proxy's race order
    single_send_degrade_fail: 2      # early-warning threshold (consecutive failures; 0=off)
    single_send_degrade_ratio: 1.5   # EWMA-degradation ratio vs pin-time baseline (0=off)
    single_send_degrade_slack_ms: 10   # absolute floor (ms) against false positives
    single_send_degrade_success_rate: 0.0  # domain success-rate drop → demote (needs ≥8 samples)
    single_send_degrade_p99_ms: 0.0        # domain P99 spike → demote (needs ≥4 samples)
    single_send_degrade_min_throughput: 0.0 # domain throughput drop → demote (needs ≥4 samples)
    single_send_slow_log_ms: 1500  # slow single-send sampling log (ms; 0=off): logs client IP
    connect_tunnel_timeout_sec: 3.0  # CONNECT connect/read timeout (s)
    http_read_timeout_sec: 3.0       # HTTP single-send read timeout (s)
    probe_with_get: false          # follow CONNECT probe with a GET (Phase 4, default off)
    probe_get_targets: []          # whitelist of URLs for GET probing
    probe_get_interval_sec: 60.0   # min interval per (proxy, target)
    probe_get_timeout_sec: 5.0
    probe_get_max_bytes: 65536
    cost_sort_enabled: true        # multi-objective Cost ranking (false = pure EWMA)
    cost_latency_metric: "p99"     # latency term: "p99" (tail-first) or "ewma"
    cost_weight_latency: 1.0
    cost_weight_success_rate: 0.6
    cost_weight_throughput: 0.1    # tiny: tunnel traffic rarely yields per-body throughput
    cost_latency_min_samples: 1
    cost_throughput_min_bytes: 1000000
  stickiness:                    # session stickiness (off by default)
    enabled: false
    ttl: 1800
    recheck_hits: 100
    max_entries: 100000
  conn_pool:                     # CONNECT TCP pooling (Phase 1, default on)
    enabled: true
    per_proxy: 4
    total: 64
    idle_timeout: 30.0
    refill_interval: 5.0
    refill_target: 2
    connect_timeout: 10.0
    target_prewarm: true         # Phase 2: pre-open "upstream → target" TCP for hot targets
    established_reuse: true      # re-use already-CONNECTed idle tunnels
    established_idle_timeout: 300.0
    prehandshake: false          # P1: extra CONNECT pre-builds established tunnels (throttled!)
    prehandshake_throttle_window_sec: 2.0
    prehandshake_throttle_max_per_window: 6
    cluster_predict: false       # P3: pre-warm predicted co-targets for next page load
    cluster_window_sec: 2.0
    cluster_predict_topk: 3
    cluster_min_support: 2
    cluster_graph_ttl_sec: 86400
    cluster_graph_max_entries: 100000
    cluster_predict_throttle_sec: 30.0
  policies: []                   # policy routing: narrow racing candidates per domain/tag
  adaptive_ttl: {enabled: false, ...}  # per-domain TTL by stability
  switch_damping: {enabled: false, ...}  # damp proxy switching when close
  concurrency_limit: {enabled: false, ...}  # per-proxy adaptive concurrency cap
logging:
  level: INFO                    # file-log level (console always WARNING+ to avoid spam)
  file: "auto_squid.log"
```

For production-ready snippets, see `examples/config.yaml` and the [Speed tuning](#speed-tuning) section below.

## Config validation at load time

`config_schema.py` defines the configuration as pydantic models with `extra="forbid"`. Typos, unknown fields, and cross-field violations are caught at load time with a readable message and **exit code 2** (no traceback):

```bash
$ python -m auto_squid.cli
config error: invalid configuration:
  1 validation error for Config
  router.circuit.local_direct_domains
    Extra inputs are not permitted
```

## Proxy store

Upstream proxies are declared in `proxies.yaml` (a list, gitignored in production):

```yaml
- id: squid-01
  name: beijing-01
  host: 10.14.25.86
  port: 3128
  protocol: http              # http | https | socks5
  auth:                        # optional; omitted = no auth
    username: "..."
    password: "..."
  enabled: true
  tags:                        # optional, for policy routing
    region: "cn"
```

The `ProxyStore` supports runtime CRUD through the management API — changes are persisted back to `proxies.yaml`.

## API Endpoints

All on the management port `:18080`. `POST` bodies are JSON; unknown fields are rejected (`422`). When `api.auth` is enabled, every endpoint except `/health` requires HTTP Basic credentials (via `Authorization` or `Proxy-Authorization`).

### Proxy & stats

| Endpoint | Description |
|----------|-------------|
| `GET /` | Web UI dashboard (domain stats, default proxies, win counts, auto-refresh) |
| `GET /health` | Health check (always open) |
| `GET /proxies` | List configured proxies |
| `POST /proxies` | Add a proxy (`ProxyIn` JSON); persisted to `proxies.yaml` |
| `GET /stats` | `request_counts` (wins) + `attempted_counts` (total attempts) per proxy |
| `GET /domains` | Per-domain win stats from SQLite |
| `GET /domains/meta` | Per-domain default proxy + last-updated time (+ TTL fields when adaptive TTL is on) |
| `GET /stickiness` | Session stickiness table (client_ip\|domain → sticky proxy) |
| `GET /policies` | Policy-routing config snapshot |
| `GET /config` | Router config snapshot |

### Metrics & quality

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | `request_counts`, `attempted_counts`, domain stats, server perf counters (cache hits, racing fan-out, probe counters, pool sizes, etc.) |
| `GET /metrics/per-destination` | Per (domain, proxy) metrics: windowed percentiles, success rates, error classification, plus a `cumulative` sub-object (lifetime means, smoothed success rate, t-digest percentiles, total bytes) |
| `GET /quality` | Per-proxy EWMA first-byte latency (s) — the legacy racing-order basis |
| `GET /quality/meta` | Enhanced per-proxy metrics: windowed P50/P95/P99, smoothed success rate, error breakdown, HTTP protocol version counts, `cumulative` lifetime object, and a `cost_breakdown` per proxy |
| `POST /quality/reset` | Clear all proxy EWMA quality |
| `GET /circuit` | Circuit-breaker + probe state per proxy (`open`, backoff, `probes_sent/ok/skipped`, `single_send_degrades`, slow-send log counts) |
| `POST /circuit/reset` | Un-break all circuits, keep EWMA quality |
| `GET /server-stats` | Server resource sampling (CPU %, event-loop lag) filled by the bench subprocess |

### Cost ranking & auto-tuning (P1)

| Endpoint | Description |
|----------|-------------|
| `GET /cost` | Current cost parameters (7 fields), the auto-tuner state, and safety bounds |
| `POST /cost` | Hot-update any subset of cost fields — **effective on the next race, no restart** |
| `POST /tuner` | `{"enabled": true\|false}` — toggle the auto-tuner at runtime |

```bash
# Which component dominates each proxy's ranking?
curl http://127.0.0.1:18080/quality/meta | jq '.["239-192"].cost_breakdown'
# → {"rank":2,"cost":0.31,"latency":{"contrib":0.21},
#     "success_rate":{"contrib":0.05},"throughput":{...}}

# Raise the throughput weight — no restart needed
curl -X POST http://127.0.0.1:18080/cost \
     -H 'Content-Type: application/json' \
     -d '{"cost_weight_throughput": 0.4}'

# Instant rollback without touching the process
curl -X POST http://127.0.0.1:18080/cost -d '{"cost_sort_enabled": false}'
```

## Speed tuning

Tuning depends on your traffic. Three starting profiles (see `examples/config.yaml` for the full YAML):

**Stability-first** (stable egress IP, login/bot-protection-sensitive sites):

```yaml
router:
  stickiness: {enabled: true, ttl: 1800, recheck_hits: 100}
  circuit:
    probe_interval_sec: 30
    single_send_degrade_fail: 2
```

**Speed-first** (lowest TTFB, tolerant of egress switching):

```yaml
router:
  cache_ttl: 900
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 100   # RFC 8305 floor
```

**Low-fan-out first** (CONNECT-heavy, minimize racing amplification):

```yaml
router:
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 200
  circuit:
    probe_interval_sec: 30
    single_send_degrade_fail: 1   # unpin before the circuit breaker trips
```

> **Tuning tips**
> - `probe_canary` must be reachable both locally and through every upstream — watch `GET /circuit` for growing `probes_skipped`.
> - `single_send_degrade_fail` is an early warning for the circuit breaker — set it to `circuit_threshold - 1`.
> - `single_send_slow_log_ms` (default 0=off) is the only anchor to attribute "won't open / needs refresh" stalls to a specific client IP (the success path emits no IP log).
> - `http_read_timeout_sec` default is 3 s; before tightening further, watch p99/fd in production — see the [Limitations](#limitations) note.
> - **Policy routing** (`router.policies`) narrows the racing candidate set per domain/tag — see `examples/config.yaml` for the shape.
> - `conn_pool.target_prewarm` requires `conn_pool.enabled`; `conn_pool.prehandshake` requires `enabled` + `target_prewarm` + `established_reuse` — set all or none.
> - `conn_pool.cluster_predict` requires `conn_pool.enabled` **and** `conn_pool.target_prewarm`.
> - **Escape corridor (fail-open):** when every external proxy is circuit-open and the domain is reachable from the host, the router silently falls back to dialing `local` directly — no config required.

## Container deployment (Docker / docker compose)

A multi-stage image and a compose example let you run auto_squid in one command, with a persistent data volume and a health check:

```bash
docker compose -f examples/docker/docker-compose.yml build
docker compose -f examples/docker/docker-compose.yml up -d
curl http://127.0.0.1:18080/health
curl -x http://127.0.0.1:10808 http://www.baidu.com
```

- The default upstreams inside the image are placeholders for bootstrap verification — see [`examples/docker/README.md`](examples/docker/README.md) to attach real upstreams (mount your own `proxies.yaml` or inject node ids at build time).
- SQLite stats persist in the `./data` volume; logs go to stdout (`docker compose logs -f`).
- The image runs as a non-root user and exposes ports `10808` and `18080`.

## Testing

```bash
.venv/bin/python -m pytest -q
```

The suite covers HTTP/CONNECT forwarding, the HTTP response cache (LRU/eviction, in-flight coalescing), the domain cache, racing/aggregation timeouts, client auth, circuit breaker / probing / EWMA selection / in-flight weighting, session stickiness, per-domain stats + SQLite persistence, UTF-8 / binary-safe body handling, connection warm pools + established-handshake reuse + idle pause, robustness (header limits, truncated responses), and the config layer (`extra="forbid"`, cross-field validation, exit code 2). The Phase-2+ metric/ranking layer has its own suites: `tests/test_phase_metrics.py` and `tests/test_tuner.py`.

CI runs the suite on **Python 3.10, 3.11 and 3.12** via GitHub Actions (`.github/workflows/test.yml`) with a per-test 60 s timeout.

> **Python 3.12 note:** `StreamWriter.wait_closed()` and `Server.wait_closed()` are stricter in 3.12 (they wait for peer FIN). The router bounds half-open pooled connections with a short timeout; the mock upstreams close idle connections after 5 s. 3.11 passes without these.

## Benchmarking

A controlled, repeatable, attributable benchmark harness lives in `bench/`. Mock upstream proxies (configurable latency / response size / chunked / failure rate, with hit counters) eliminate real-network jitter. See [`bench/README.md`](bench/README.md) for details. Quick reference:

```bash
.venv/bin/python -m bench.stress --quick            # ~10 s smoke run
.venv/bin/python -m bench.stress --mode staircase   # concurrency staircase → saturation point
.venv/bin/python -m bench.stress --mode soak        # stability / resource-leak check
.venv/bin/python -m bench.stress --rounds 5         # mean ± stddev across fresh subprocesses
.venv/bin/python -m bench.stress --profile          # cProfile → bench_profile.txt
```

Key metrics: throughput, TTFB & total at P50/P95/P99, error rate by category, **cache hit rate** and **racing amplification** (derived from server-side `/metrics` counters — unified across mock and real upstreams), plus resource samples (RSS, fd count, pool sizes, CPU %, event-loop lag). Results are written to `bench_report.json` tagged with the git revision.

Standalone routing inspector: `test_routing.py` — analyze the decision chain (sticky/domain-cache hit, policy routing, proxy ordering, circuit state) for a given URL without making real requests (`--test-request` to actually forward one).

## Limitations

- HTTP parsing is MVP-level; large streaming responses may have edge cases
- The management API is **open by default** — enable `api.auth` (HTTP Basic) before exposing port 18080 beyond a trusted network
- CONNECT tunnels use raw pipes (no TLS interception)
- `http_read_timeout_sec` (3 s by default) may timeout slow but healthy origins over high-latency links — increase carefully and verify p99/fd before wide rollout
- In-memory tables (stickiness, domain cache, HTTP response cache) are cleared on restart; SQLite persists domain win counts and t-digest lifetime metrics only

## License

MIT
