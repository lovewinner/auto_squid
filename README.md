# auto_squid

A lightweight forward proxy that **races every request across a pool of upstream proxies** and keeps the fastest winner. It adds multi-objective Cost ranking, domain caching, session stickiness, an HTTP response cache, CONNECT connection pre-warming, and SQLite-persisted lifetime metrics — all behind two ports and one YAML file.

> [中文说明 →](README_CN.md)

---

## What this is

A client points its system- or app-level proxy at auto_squid. Every request — plain HTTP or `CONNECT` — is dispatched to a set of upstream proxies **in parallel**: candidates are ordered by a weighted Cost, the best few launch first, the rest are refilled on a timer (RFC 8305 §5 staggered start). The first response to produce a first byte wins; the losers are cancelled and closed.

You get lower TTFB than pinning one proxy, automatic failover when a proxy degrades, and no configuration changes on the upstream side.

Once a race is won the result is remembered: per-domain caching, optional session stickiness (stable egress IP), pre-warmed TCP to the upstream, and per-proxy/per-domain statistics that feed the next ordering decision.

## Quickstart

```bash
# 1. Install (runtime deps: fastapi, uvicorn[standard], httpx, pydantic, typer, pyyaml)
uv venv .venv --seed && uv sync

# 2. Declare your upstream proxies (proxies.yaml is gitignored)
cp examples/proxies.yaml proxies.yaml     # fill in host/port/auth

# 3. Optional: tune behaviour (config.yaml is gitignored too)
cp examples/config.yaml config.yaml

# 4. Start
python -m auto_squid.cli
# or: auto-squid                          # entry point registered in pyproject.toml
# Flags: --config ./config.yaml --proxies ./proxies.yaml --db ./auto_squid.db

# 5. Verify
curl http://127.0.0.1:18080/health        # → {"status":"ok"}
curl http://127.0.0.1:18080/proxies
curl -x http://127.0.0.1:10808 http://www.baidu.com
curl -x http://127.0.0.1:10808 https://www.baidu.com
```

Dashboard: <http://127.0.0.1:18080/> (redirects to `/panel/domains`).

**Defaults are conservative.** Racing, Cost ranking, the domain cache and the HTTP response cache work out of the box; stickiness, connection pooling, active probing, cluster prediction, adaptive TTL and the auto-tuner are **off** until you switch them on. The management API binds to loopback only and is unauthenticated by default — see [Security model](#security-model).

## Ports

| Port | Purpose | Binds by default |
|------|---------|------------------|
| `10808` | Proxy port — HTTP/`CONNECT` in (`-x http://127.0.0.1:10808 …`), optional client Basic auth | `0.0.0.0` |
| `18080` | Management API + dashboard + `/docs` | `127.0.0.1` |

## Features

**Racing & selection**
- **Staggered parallel racing** — a first batch of `stagger_initial` candidates, remaining candidates refilled every `stagger_interval_ms` (floor 100 ms); first first-byte wins, losers cancelled.
- **Multi-objective Cost** — `w_lat·norm(latency) + w_sr·norm(1−success_rate) + w_tp·norm(1−throughput)`, min–max normalized *inside the candidate set* so the weights are directly comparable. Latency defaults to lifetime TTFB **P99** (t-digest) with an EWMA fallback; success rate is Laplace-smoothed and prefers the domain-level bucket. `cost_sort_enabled: false` rolls back to pure EWMA ordering instantly.
  The throughput term is fed **only by plaintext response bodies**: a tunnel's "bytes ÷ wall-clock tunnel lifetime" measures how long the *client* idled, not how fast the proxy relays, so it never enters Cost (it stays observable as `tunnel_relay_ewma_mbps`).
- **Least-request bias** (`lb_bias`) and a consecutive-failure penalty (`fail_penalty_weight`) steer load away from backed-up or flaky proxies.
- **Circuit breaker + slow-start** — `circuit_threshold` consecutive transport failures open a circuit with exponential backoff up to `circuit_max_backoff`; recovery is rate-limited through slow-start. Business rejections (a non-200 answer to the upstream's `CONNECT`, HTTP 5xx) are classified separately and do **not** open a circuit.
- **Active probing** — `probe_canary` (or a `probe_canaries` list) re-tests open circuits without waiting for client traffic. `probe_with_get` can follow the CONNECT probe with a whitelisted GET so real TTFB/body metrics keep updating.
- **Early-warning degrade** (`single_send_degrade_*`) — unpins a sticky/cached proxy at the first sign of degradation, before the circuit would trip.
- **Local racing / fail-open** — the gateway host can race as its own node (`enable_local_racing`); `local_direct_domains` dials origins directly, bypassing upstreams.

**Caching & stickiness**
- **Domain cache** — a race winner is reused for that domain until `cache_ttl` expires; `adaptive_ttl` grows or resets that TTL based on domain stability.
- **Session stickiness** — `client_ip + domain` pins a proxy for a stable egress IP; failing or degraded pins are evicted and re-raced; `recheck_hits` periodically re-verifies a pin. Bounded table (`max_entries`, LRU).
- **HTTP response cache** — idempotent `GET` bodies cached in memory (`ttl`, respects `Cache-Control`); `POST/PUT/DELETE/PATCH` invalidate that domain's cached GETs. A single response larger than `max_bytes/2` is deliberately not cached.
- **In-flight GET coalescing** — concurrent requests for the same URL share one upstream fetch.

**Connection pooling (CONNECT)**
- **Phase 1** — a pool of idle TCP connections to each upstream proxy.
- **Phase 2** (`target_prewarm`) — the same kind of bare TCP, bucketed by `(proxy, target)` and pre-built when a domain-cache/sticky hit or race win predicts a repeat.
- **Cross-phase steal** — when the Phase-1 bucket is empty, a connection is borrowed from a Phase-2 bucket of the *same* proxy (the TCP is target-agnostic; `target` is only a bucket label). Guardrails: only buckets holding ≥2 connections are robbed, oldest-first, so every target keeps one for itself.
- **Cluster prediction** (`cluster_predict`) — learns which targets co-occur inside a short page-load window and pre-warms predicted co-targets before the next request arrives.
- **Stale detection** — a connection the peer already half-closed (CLOSE_WAIT) still looks perfectly healthy to `is_closing()`. Taking one burns a whole CONNECT round trip and returns "no response", so EOF/stray-bytes are checked on the reader side and the connection is dropped on the spot.

**Operations**
- **Management API + dashboard** — proxy list, per-domain stats, stickiness table, Cost breakdown, circuit/probe state, auto-tuner control.
- **Runtime hot-reload** of Cost parameters (`POST /cost`) — effective on the very next race, no restart.
- **Auto-tuner** (`auto_tune`, off by default) — conservative hill climbing that adopts a weight change only when winner TTFB improves by ≥ `hysteresis` with no success-rate regression, and rolls back when things get worse.
- **Lifetime metrics in SQLite** — per-proxy and per-domain windowed percentiles (last 256 samples) *plus* t-digest lifetime rollups, error classification, HTTP-version distribution, and smoothed success rates with low-confidence flags.
- **Config validated at load** — `extra="forbid"`, cross-field checks, readable message, exit code 2.
- **Size-based log rotation**, graceful shutdown, Docker image + compose example.

## Request pipeline

The decision chain (`auto_squid/router.py`):

1. **Accept & parse** — `handle_client` reads the request head, enforces client Basic auth (`407` before any upstream work), and rejects over-limit request heads with a single INFO line instead of a traceback. `CONNECT` → `_handle_connect`; plain HTTP → `_handle_http_request`.
2. **Short-circuits** — `local_direct_domains` (dial the origin directly), in-flight GET coalescing, HTTP response cache hit.
3. **Dispatch** (`_dispatch_single`) — in priority order:
   1. **session stickiness** → single-send on the pinned proxy;
   2. **domain cache** → single-send on the last winner within its TTL;
   3. **racing** (`_race_staggered`) → Cost-ordered candidates, staggered launch.
4. **Connection acquisition** — an established tunnel is never reused; per request the router takes a TCP in the order **Phase 2 → Phase 1 → cross-phase steal → new connection**, then sends `CONNECT`. A stale pooled connection is detected at take time, discarded and retried fresh — and does **not** count against the circuit breaker.
5. **Response framing** — hop-by-hop headers are stripped (`proxy-authorization` never reaches the upstream), `Content-Length` is recomputed after decoding a chunked request body, and `Transfer-Encoding` takes precedence over `Content-Length` (RFC 9112 §6.1). HTTP/1.0 clients get close-delimited bodies instead of chunked framing (RFC 9112 §2.7); `HEAD`, `204`, `304` and 1xx never carry a body. Truncated upstream responses are detected and reported rather than silently forwarded.
6. **Feedback** — the winner updates EWMA/TTFB/OFB, protocol version, throughput (plaintext bodies) and the domain cache; failures call `record_failure`, feeding the circuit breaker in `selector.py`. A degraded single-send triggers `_degrade_send_proxy`, so the *next* request for that domain skips stickiness/cache and re-races.

### Where each knob acts

| Knob | Stage | Effect |
|---|---|---|
| `stickiness.*` | dispatch 3.1 | pinning on/off, TTL, re-check cadence, capacity |
| `cache_ttl`, `adaptive_ttl.*` | dispatch 3.2 | how long a race winner is reused |
| `max_retries`, `stagger_initial`, `stagger_interval_ms` | racing 3.3 | fan-out width and refill cadence |
| `cost_*` | candidate sort | multi-objective ranking vs pure EWMA |
| `lb_bias`, `fail_penalty_weight` | candidate sort | backlog and flakiness penalties |
| `conn_pool.*` | acquisition 4 | pool depth, idle TTL, pre-warm, prediction |
| `circuit_threshold`, `circuit_max_backoff`, `slow_start_*` | failure accounting 5–6 | open / backoff / recovery |
| `probe_*` | background | re-test open circuits, keep metrics warm |
| `single_send_degrade_*` | degrade gate 6 | unpin before the breaker trips |
| `policies` | candidate set | narrow which proxies may serve a domain |
| `concurrency_limit.*` | admission | per-proxy adaptive in-flight cap |

## Configuration

Everything lives in `config.yaml`. Every key is optional; the values below are the **code defaults**. `examples/config.yaml` shows a production-shaped alternative.

```yaml
listen:
  host: "0.0.0.0"                  # proxy port bind
  port: 10808

api:
  host: "127.0.0.1"                # loopback; binding 0.0.0.0 with auth off logs a WARNING
  port: 18080
  auth: {enabled: false, username: "", password: ""}   # HTTP Basic on every route but /health

logging:
  level: "INFO"                    # FILE level only (console is pinned to WARNING+)
  file: null                       # null → "auto_squid.log", resolved against the startup CWD
  max_bytes: 33554432              # 32 MiB per file, then rotate
  backup_count: 5                  # keep .1 … .5 (0 = truncate in place)

router:
  enable_local_racing: false
  local_direct_domains: []         # bare host/IP (no port) — dial the origin directly
  cache_ttl: 600
  max_retries: 3                   # first racing batch size; also clamps stagger_initial
  stagger_start: true
  stagger_initial: 2               # 1..max_retries (out-of-range values are clamped)
  stagger_interval_ms: 100         # RFC 8305 refill interval, clamped to [100, 2000]
  auth: {enabled: false, username: "", password: ""}   # client-facing Basic auth (:10808)

  policies: []                     # match{domain_suffix|domain_exact|domain_regex} → proxies{tags|ids}
  stickiness:
    enabled: false
    ttl: 1800                      # sliding on hit
    recheck_hits: 100              # 0 = never re-race a pinned proxy
    max_entries: 100000
    probe_interval_sec: 0.0        # 0 = background re-probing off
    probe_fanout: 2
  http_cache:
    enabled: true
    ttl: 60                        # seconds, sliding on hit
    max_entries: 10000             # LRU
    max_bytes: 268435456           # 256 MiB budget; a body > max_bytes/2 is never cached
    stream_cache_limit: 1048576    # per-response buffer still eligible for caching
  adaptive_ttl: {enabled: false, min_sec: 60.0, max_sec: 1800.0}
  switch_damping: {enabled: false, min_wins: 2, ratio: 0.8, abs_ms: 30.0}
  concurrency_limit: {enabled: false, min: 2, initial: 16, max: 128,
                      add_on_success: 4, mult_on_failure: 0.5, failure_window: 20}
  conn_pool:
    enabled: false                 # both pools are OFF by default
    per_proxy: 4
    total: 64                      # fd budget shared by Phase 1 + Phase 2
    idle_timeout: 30.0             # MUST stay below the upstream's own idle close
    refill_interval: 5.0           # 0 = take-only, never refill
    refill_target: 2               # Phase-1 water level per proxy (clamped to per_proxy)
    connect_timeout: 10.0
    target_prewarm: false          # Phase 2; requires enabled: true
    refill_pause_minutes: 60.0     # 0 = never pause when idle
    refill_pause_silence_sec: 120.0      # DEPRECATED, kept for compatibility; use the two knobs below
    refill_pause_activity_window: null   # null → max(30, refill_pause_silence_sec/4)
    refill_pause_min_requests: 3
    cluster_predict: false         # requires enabled: true AND target_prewarm: true
    cluster_window_sec: 2.0
    cluster_predict_topk: 3
    cluster_predict_throttle_sec: 30.0
    cluster_min_support: 2
    cluster_graph_ttl_sec: 86400
    cluster_graph_max_entries: 100000
    cluster_proxy_fanout: 2        # pre-warm across the N most-likely winner proxies
    cluster_probe_decay_sec: 3600.0
    cluster_pool_idle_timeout: 600.0    # predicted pre-warms live longer than passive ones
  circuit:
    probe_interval_sec: 30.0       # 0 = no active probing
    probe_canary: "1.1.1.1:443"    # superseded by probe_canaries when that list is non-empty
    probe_canaries: []             # [{name, target, tags}] — probe per candidate group
    circuit_threshold: 3
    circuit_max_backoff: 300.0
    slow_start_window: 60.0
    slow_start_success: 3
    lb_bias: 1.0                   # weight = ewma × (1 + in_flight)^bias; 0 = pure EWMA
    fail_penalty_weight: 4.0       # 0 = off
    single_send_degrade_fail: 0    # 0 = off; sensible value = circuit_threshold - 1
    single_send_degrade_ratio: 0.0        # also drives sticky slow-probe eviction
    single_send_degrade_slack_ms: 10.0    # absolute floor guarding the ratio rule
    single_send_degrade_success_rate: 0.0 # needs ≥ 8 domain samples
    single_send_degrade_p99_ms: 0.0       # needs ≥ 4 domain samples
    single_send_degrade_min_throughput: 0.0  # plaintext bodies only; needs ≥ 4 samples
    single_send_slow_log_ms: 0.0   # the only log that ties a stall to a client IP
    connect_tunnel_timeout_sec: 3.0
    http_read_timeout_sec: 3.0
    local_direct_timeout_sec: 10.0 # applies to the local_direct_domains path only
    probe_with_get: false          # a no-op unless probe_get_targets is non-empty
    probe_get_targets: []
    probe_get_interval_sec: 60.0
    probe_get_timeout_sec: 5.0
    probe_get_max_bytes: 65536
    cost_sort_enabled: true
    cost_latency_metric: "p99"     # "p99" | "ewma"
    cost_weight_latency: 1.0
    cost_weight_success_rate: 0.6
    cost_weight_throughput: 0.1
    cost_latency_min_samples: 1
    cost_throughput_min_bytes: 1000000   # measured RESPONSE-BODY bytes, not lifetime relay
  auto_tune:
    enabled: false
    window_sec: 900.0
    min_samples: 50
    step: 0.25                     # ±25% per probe
    hysteresis: 0.05               # adopt only ≥5% winner-TTFB gains
    sr_guard: 0.005                # reject if success rate drops more than this
    persist: true                  # state kept in SQLite (tuner_state)
```

### Load-time validation

`config_schema.py` models are `extra="forbid"` (except the deliberately lenient data models `ProxyInfo` and `ProbeCanaryConfig`). Typos and cross-field violations abort startup with **exit code 2** and a readable message instead of a traceback:

```text
config error: invalid configuration:
  1 validation error for Config
  router.stagger_inital
    Extra inputs are not permitted
```

Enforced constraints: `logging.level` ∈ {DEBUG, INFO, WARNING, ERROR, CRITICAL}; `max_bytes > 0`; `backup_count ≥ 0`; an enabled `auth` block must have both username and password non-empty (otherwise `Basic Og==` would compare equal); `stagger_initial ≥ 1` and `≤ max_retries`; `adaptive_ttl.min_sec ≤ max_sec` when enabled; `concurrency_limit.min ≤ initial ≤ max` when enabled; `target_prewarm` and `cluster_predict` require `conn_pool.enabled`, and `cluster_predict` additionally requires `target_prewarm`.

Two values are **not** validated and fail quietly rather than loudly — worth knowing when a change appears to do nothing:

- `circuit.cost_latency_metric` in YAML: anything other than `"p99"`/`"ewma"` is coerced to `"p99"` (the `POST /cost` API *does* reject it with 400).
- `stagger_interval_ms`: out-of-range values are clamped into `[100, 2000]` rather than rejected.

## Upstream proxy list

Proxies live in a separate `proxies.yaml` (a plain list, gitignored in production):

```yaml
- id: squid-01                 # required, unique — the key used by every metric
  name: beijing-01             # optional display name
  host: 10.14.25.86            # required: bare hostname / IPv4 / [IPv6] — no scheme, port, @ or /
  port: 3128
  protocol: http               # only "http" is implemented; see the note below
  auth:                        # optional upstream credentials
    username: "..."
    password: "..."
  enabled: true
  tags: {region: cn}           # optional, consumed by router.policies
```

> **`protocol` is currently decorative.** Nothing in the codebase branches on it: every upstream is used as an HTTP/`CONNECT` proxy, and `https` or `socks5` would be silently treated as `http`. The write path (`POST /proxies`) therefore refuses anything but `http` outright so you cannot add a node that misbehaves — but a hand-edited `proxies.yaml` is not validated and will still be used as HTTP.

Nodes can be added at runtime with `POST /proxies`, which rewrites the whole bound `proxies.yaml` (comments are lost). There is **no delete or patch endpoint** — updating a node means POSTing the complete object under the same `id`.

## Management API

All on `:18080`. With `api.auth.enabled`, everything except `GET /health` requires HTTP Basic — accepted in either `Authorization` or `Proxy-Authorization`, compared with `hmac.compare_digest` on both fields. Failure returns `401` + `WWW-Authenticate: Basic realm="auto_squid"`.

### Read

| Endpoint | Description |
|---|---|
| `GET /` | 307 → `/panel/domains` |
| `GET /panel/{domains,stickiness,metrics}` | the dashboard itself; unknown view → 404 |
| `GET /static/panel.js` | dashboard script, served from memory |
| `GET /health` | liveness; exempt from auth |
| `GET /proxies` | node list; `auth` values masked as `***` unless `?include_secrets=true` (the key names stay visible) |
| `GET /stats` | `request_counts` (wins) and `attempted_counts` (attempts) per proxy |
| `GET /domains` | per-domain win counts — top-N **by wins** (`?limit=`, default 2000, `0` = all) |
| `GET /domains/meta` | per-domain default proxy + `updated_at` (+ TTL fields when `adaptive_ttl` is on) — top-N **most recently updated** (`?limit=`) |
| `GET /stickiness` | pin table `client_ip|domain → proxy` — top-N **most recently used** (`?limit=`) |
| `GET /policies` | policy-routing snapshot |
| `GET /config` | currently only `{"enable_local_racing": …}` |
| `GET /metrics` | `request_counts`, `attempted_counts`, **full** `domain_stats`, ~80 `counters`, `proxy_metrics` |
| `GET /metrics/per-destination?domain=` | per `(domain, proxy)` percentiles, error classes and the lifetime `cumulative` object; omit `domain` for the whole grid (raw samples stripped), pass one for a cheap drill-down |
| `GET /quality` | per-proxy EWMA first-byte latency (s) — the legacy ordering basis |
| `GET /quality/meta` | per-proxy: windowed TTFB/OFB percentiles, `throughput_ewma_mbps` (bodies only), `tunnel_relay_ewma_mbps` (observation), `body_bytes_transferred`, windowed + lifetime success rates, error breakdown, HTTP-version counts, the `cumulative` lifetime object, and `cost_breakdown` |
| `GET /circuit` | `circuit_open_count`, probe counters, `single_send_degrades`, `degraded_single_send`, and per-proxy `{open, open_until, backoff, consec_fail, slow_start}` |
| `GET /server-stats` | CPU % and event-loop lag — only populated when run under `bench/`; `{}` for a normal CLI start |
| `GET /cost` | current Cost parameters, auto-tuner state, safety bounds |
| `GET /docs` | OpenAPI UI (auth-gated like everything else) |

`/domains`, `/domains/meta` and `/stickiness` default to 2000 rows because those tables can hold ~100k entries and the payload is built on the same event loop that forwards your traffic. `GET /metrics` is **not** truncated.

### Write

| Endpoint | Effect | Persists? |
|---|---|---|
| `POST /proxies` | add/overwrite a node by `id`; immediately raceable | rewrites `proxies.yaml`; the response reports `persisted` (+ `persist_error`) — with `persisted: false` the node serves until restart and then disappears |
| `POST /cost` | hot-update any subset of the 7 Cost fields — effective on the next race | no (restart restores `router.circuit.cost_*`), but it also resets the tuner baseline |
| `POST /tuner` | `{"enabled": true\|false}`; enabling restarts the evaluation loop, **disabling rolls weights back** to the last accepted baseline | no |
| `POST /quality/reset` | clear EWMA, domain metrics, circuits, in-flight and degrade markers → everything re-races as unknown quality | **also wipes the persisted lifetime rows in SQLite** — destructive |
| `POST /circuit/reset` | close all circuits, clear backoff and consecutive-failure counters, rearm slow-start; latency history untouched | no |

Unknown JSON fields are rejected with `422` on `/cost` and `/tuner` (`extra="forbid"`), but **silently ignored** by `POST /proxies`. `POST /proxies` additionally rejects `host` values containing a scheme/port/`@`/`/`, requires `protocol == "http"`, and refuses an `auth` block whose values are all `***` — you cannot round-trip a masked GET.

### Security model

- The proxy port binds `0.0.0.0` by default with client auth **off** — that is an open proxy for anything that can reach it. Set `router.auth.enabled`, or restrict the bind/ACL at the firewall.
- The API binds loopback by default. A non-loopback bind with `api.auth` disabled logs a startup WARNING but does not refuse to start.
- **Cross-site writes are blocked.** `POST/PUT/PATCH/DELETE` are checked *before* authentication, and the check applies even when `api.auth` is off: `Sec-Fetch-Site: cross-site` → `403 {"detail":"cross-site admin write blocked"}`; otherwise an `Origin` whose host doesn't match this host is rejected (the scheme is ignored so an HTTPS-terminating reverse proxy still works). A missing `Origin` (curl, scripts) and `Origin: null` are allowed. This matters because a browser re-sends cached Basic credentials automatically — "the attacker's page can't read the response" does not stop the write from landing.
- `proxy-authorization` is stripped before forwarding, so client proxy credentials never reach the upstream.

## Metrics worth watching

`GET /metrics` → `counters`. The ones that actually tell you how the box is behaving:

| Counter | Meaning | What healthy looks like |
|---|---|---|
| `http_cache_hits` / `_misses`, `domain_cache_hits`, `sticky_cache_hits` | short-circuit rates | high hit rates mean fewer races |
| `racing_invocations`, `upstream_attempts` | racing amplification = attempts ÷ requests | ≤ ~1.5 for CONNECT-heavy traffic |
| `conn_pool_hits` / `_misses` / `_steals` / `_expired`, `target_pool_*` | pool effectiveness — `steals` are cross-phase borrows, and `misses` now means "both pools empty → had to build a TCP" | few `expired` relative to `creates` |
| `pool_stale_discards` | half-closed connections caught at take time | should fall together with `stale_conn_retries` once `idle_timeout` is low enough |
| `stale_conn_retries` | `pooled conn STALE` retries, each costing a CONNECT round trip | **≈ 0**; a rising rate means the pool's idle TTL is longer than the upstream's |
| `single_send_degrades` | early-warning unpins | low and flat |
| `circuit_open_count`, `probes_sent/ok/skipped/failed` | breaker + probe activity | growing `probes_skipped` = the canary is unreachable through some proxy |
| `cluster_pool_bucket_miss` / `_timing_miss` | why a prediction missed (wrong winner bucket / arrived too late) | only meaningful with `cluster_predict: true` |
| `http_cache_evictions`, `sticky_cache_size`, `cluster_graph_size` | table pressure against their caps | steady, not pinned at the cap |

Windowed values cover the last 256 samples and reset on restart; `cumulative` and the t-digest percentiles are lifetime and SQLite-backed. Don't compare the two.

## Tuning

Three starting profiles (full annotated version in `examples/config.yaml`):

```yaml
# Stability first — stable egress IP, login/bot-protection sensitive
router:
  stickiness: {enabled: true, ttl: 1800, recheck_hits: 100}
  circuit: {probe_interval_sec: 30, single_send_degrade_fail: 2}
```

```yaml
# Speed first — lowest TTFB, egress switching acceptable
router:
  cache_ttl: 900
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 100        # RFC 8305 floor
```

```yaml
# Fan-out first — CONNECT-heavy, minimise racing amplification
router:
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 200
  circuit:
    probe_interval_sec: 30
    single_send_degrade_fail: 1   # unpin before the breaker trips
```

Notes that only show up in production:

- `conn_pool.idle_timeout` must be **below the upstream proxy's own idle close**, otherwise pooled connections sit half-closed and every reuse costs a wasted CONNECT. Watch `pool_stale_discards` and `stale_conn_retries` after changing it.
- `probe_canary` must be reachable both from the gateway and through every upstream, or whole probe rounds get counted as `probes_skipped`.
- `single_send_slow_log_ms` (default off) is the only log line that ties a "won't open / needs refresh" stall to a specific client IP — the success path logs no IP.
- `http_read_timeout_sec` is 3 s; before tightening further, check p99 and fd usage under real load.
- `conn_pool.enabled` and `target_prewarm` default to **off**. Turning on `target_prewarm` without enough repeat traffic shows up as `target_pool_expired` (connections built and then retired unused) — the cross-phase steal reclaims most of that inventory, but the ratio is what to watch.
- `conn_pool.refill_pause_minutes: 0` disables the idle pause. With bursty heartbeats (a cluster every 2–5 min) no window/threshold combination ever triggers a pause, and the idle churn is cheap enough to leave alone.
- `cluster_predict` is off here for measured reasons, not caution: over ~4 days of production it hit only 29.5%, and 63.8% of its misses were `bucket_miss` (the predicted winner was not the actual winner). `cluster_proxy_fanout: 2` did not fix that.
- Fail-open: if every external proxy is circuit-open and the host can reach the origin, the router silently dials `local` directly. No configuration required.

## Logging

File logging uses a single-process `RotatingFileHandler`: `max_bytes` per file, `backup_count` history files, and a startup banner printing the resolved absolute path plus the active policy:

```text
INFO:auto_squid.cli:file log: /srv/auto_squid/auto_squid.log (level=INFO, rotate=33554432 bytes, keep=5 backups)
```

Caveats:

- **Do not also run logrotate against this file.** logrotate renames it out from under the process (which keeps writing to the old inode while the "new" file stays empty) or, with `copytruncate`, discards the lines written during the race. In-app rotation and logrotate are mutually exclusive.
- Hand-cut history (`auto_squid.log.261011`) is outside the `.1 … .N` chain — never reused, never deleted. Clean it up manually.
- `logging.level` sets the **file** level only; the console handler is pinned to WARNING+ so forwarding volume doesn't flood it. Under systemd or Docker the stdout side is therefore small and belongs to journald / the Docker log driver.
- `RotatingFileHandler` is not multiprocess-safe: two instances sharing one `logging.file` clobber each other's rotation chain. Give each its own path.

## Container deployment

```bash
docker compose -f examples/docker/docker-compose.yml build
docker compose -f examples/docker/docker-compose.yml up -d
curl http://127.0.0.1:18080/health
curl -x http://127.0.0.1:10808 http://www.baidu.com
```

Multi-stage image, non-root user, health check, and a `./data` volume for SQLite. Two log decisions matter, and `examples/docker/` makes both: point `logging.file` at `/data/…` (the default relative path lands in the container's writable layer and is lost on rebuild), and cap the **stdout** stream with the compose `logging:` options — the json-file driver is unbounded by default and `max_bytes` does not apply to it. See [`examples/docker/README.md`](examples/docker/README.md) for attaching real upstreams.

## Testing

```bash
uv run pytest -q --timeout=60      # 489 tests, ~85 s
```

Coverage spans HTTP/`CONNECT` forwarding and framing (HTTP/1.0 downgrade, `HEAD`, truncation detection), the HTTP cache (LRU order, eviction, coalescing), the domain cache, racing and aggregation timeouts, client and API auth, the CSRF guard, circuit breaker / probing / EWMA / in-flight weighting, stickiness, per-domain stats and SQLite persistence, UTF-8 and binary-safe bodies, both pools plus the cross-phase steal, idle pause and budget accounting, config-layer validation and exit codes, and event-loop hygiene tests that fail if a background task leaves an orphan DNS future. `tests/test_phase_metrics.py` covers the metric and ranking layer, `tests/test_tuner.py` the auto-tuner.

CI runs the same suite on Python **3.10, 3.11 and 3.12** (`.github/workflows/test.yml`) with a 60 s per-test timeout; coverage (~87%) is measured on the 3.11 leg only, and there is deliberately no `fail_under` because async timing makes coverage jitter by ~0.2%.

> **Python 3.12 note:** `StreamWriter.wait_closed()` and `Server.wait_closed()` became strict about peer FIN. The router bounds those waits; mock upstreams close idle connections after 5 s.

## Benchmarking

`bench/` is a controlled harness: a mock upstream cluster with configurable latency/size/chunking/failure rates removes real-network jitter, and the router under test runs in its own subprocess so client work never inflates the measured server cost.

```bash
uv run python -m bench.stress --quick                 # ~10 s smoke
uv run python -m bench.stress --mode staircase        # saturation point
uv run python -m bench.stress --mode soak --duration 600 --open-loop
uv run python -m bench.stress --mode conn-reuse       # keep-alive reuse ratio (mock only)
uv run python -m bench.stress --upstream real --proxies proxies.yaml
uv run python -m bench.github --mode tunnel --duration 60   # CONNECT against real targets
uv run python -m bench.stress --rounds 5              # mean ± stddev, fresh subprocess per round
```

Reports print throughput, TTFB/total at P50/P95/P99, error classes, cache hit rate, racing amplification, pool hits/steals/misses, and server-side CPU and event-loop lag; `bench_report.json` is tagged with the git revision. See [`bench/README.md`](bench/README.md).

`test_routing.py` inspects the decision chain for a URL (sticky/domain-cache hit, policy narrowing, candidate order, circuit state) without sending traffic — `--test-request` performs one real request.

## Limitations

- HTTP parsing is MVP-level: headers and framing are handled carefully, but very large streaming responses can still hit edge cases.
- Upstream `protocol` is HTTP-only in practice (`https`/`socks5` have no implementation).
- `CONNECT` tunnels are raw byte pipes — no TLS interception, so per-tunnel "throughput" cannot describe origin transfer speed.
- In-memory state (stickiness, domain cache, HTTP response cache, windowed samples) is cleared on restart; only lifetime counters, t-digests and domain win counts survive in SQLite.
- The proxy port is an **open proxy** unless you enable `router.auth` or restrict it at the network layer.
- `POST /proxies` rewrites `proxies.yaml` in full and drops comments; there is no delete endpoint.
- `http_read_timeout_sec` (3 s) can time out slow-but-healthy origins on high-latency links; raise deliberately and verify p99/fd.

## Further reading

[`docs/README.md`](docs/README.md) indexes the design, audit and post-mortem write-ups (`性能优化总纲`, `改进方案_响应速度评估`, `代码审计报告`, the 2026-09 circuit-breaker incident analysis).

## License

MIT
