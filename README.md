# auto_squid

Lightweight forward proxy with parallel racing, domain-based caching, an HTTP response cache, and SQLite-persisted stats.

> [中文说明 →](README_CN.md)

## Overview

- Runs on a gateway host, accepts HTTP/HTTPS proxy traffic, and forwards each request through upstream proxies
- **Parallel racing + staggered start**: races upstreams sorted by per-proxy EWMA latency, launching the best 1–2 first (RFC 8305 §5) and refilling at ~250ms intervals; the first first-byte response wins and the rest are cancelled. Staggering cuts CONNECT tunnel fan-out and HTTP double-write traffic dramatically
- **Multi-objective Cost ranking (Phase 2)**: racing order is a weighted Cost of **latency (P99-tail-first) + success rate + throughput**, min-max normalized within the candidate set — a fast-but-flaky proxy no longer outranks a slightly slower but reliable one. On by default, `cost_sort_enabled: false` rolls back to pure-EWMA ranking instantly
- **Self-tuning (P1)**: Cost weights can be changed at runtime (`POST /cost`, effective on the very next race, no restart), and an optional conservative auto-tuner (`router.auto_tune`) hill-climbs the weights against measured winner-TTFB with a success-rate guard and automatic rollback
- **Domain cache**: once a proxy wins a race for a domain, it is reused for that domain until `cache_ttl` expires — avoids racing every request
- **Session stickiness**: optional; the same client IP + domain/target reuses the same proxy (keeps the egress IP stable); a failing or 5xx-returning sticky proxy evicts its entry and falls back to racing (redispatch), and sticky entries are re-raced periodically (`recheck_hits`)
- **HTTP response cache**: idempotent `GET` responses are cached in memory (TTL 60s, respects `Cache-Control`)
- **Local racing**: optionally lets the gateway host itself race as a proxy node (direct, no upstream)
- **Observability**: per-proxy & per-domain windowed (recent 256) **and** lifetime (t-digest, cross-restart) percentiles, error classification, HTTP protocol version distribution, a per-proxy **Cost breakdown** that shows which component dominates the ranking, and Bayesian-smoothed success rates with low-confidence flags
- **Domain stats**: per-domain win counts tracked in SQLite, survive restarts
- **Web UI**: a built-in dashboard at `/` for browsing domain stats, default proxies, and win counts with auto-refresh; clicking a stat card filters the domain table to the domains using that proxy as Default Proxy

## Features

- HTTP and HTTPS (`CONNECT`) forwarding with parallel racing across upstream proxies (EWMA-sorted + staggered start)
- **Multi-objective Cost ranking** (Phase 2, default on): candidate order = `w_lat·norm(latency) + w_sr·norm(1−success_rate) + w_tp·norm(1−throughput)` with min–max normalization inside the candidate set (weights directly comparable, scale-free). The latency term uses the **lifetime TTFB P99** (t-digest, tail-first) with EWMA fallback; success rate is Laplace-smoothed (domain-level preferred); throughput only participates once cumulative bytes pass a floor (tunnel traffic rarely yields per-body throughput, so its weight is tiny). Missing data is neutral (0.5). `cost_sort_enabled: false` = instant rollback to pure-EWMA ranking
- **Cost hot-reload + auto-tuner** (P1): `GET/POST /cost` reads/updates all cost parameters at runtime (next race picks them up, no restart); `POST /tuner` toggles the auto-tuner, which hill-climbs the three weights one ±25% step per evaluation window, adopting only improvements ≥5% in winner-TTFB mean that do not trade away success rate (guard), rolling back degradations immediately, and persisting the adopted baseline to SQLite
- **Single-send degrade gating** (Phase 3, all thresholds default off): besides consecutive-fail and EWMA-degradation signals, a pinned (sticky/domain-cache) proxy is demoted back to racing when its domain-level success rate, P99 latency, or throughput crosses a configured threshold — closing the "fast handshake but flaky/slow transfer" blind spot
- **Business-aligned probing** (Phase 4, `probe_with_get`, default off): optionally follow the CONNECT liveness probe with a lightweight GET through the upstream against a whitelist of URLs (rate-limited per proxy+target, isolated short-lived client), recording TTFB / protocol / throughput into the *real* domain metric buckets — probe latency finally matches business latency
- **Metrics robustness**: lifetime percentiles via a self-contained t-digest (bounded memory, JSON-persisted, survives restarts) alongside the recent-256 window; `low_confidence` flags on percentiles with few samples; Laplace-smoothed success rates so a 1/1 proxy never shows a perfect 1.0
- **HTTP protocol version stats** (Phase 1.4): per-proxy/per-domain `HTTP/1.1`/`HTTP/2`/`HTTP/3` counters from the HTTP path (H2 share doubles as a connection-reuse proxy signal; CONNECT tunnel reuse/TLS resumption is architecturally invisible to a forward proxy and is not fabricated)
- Domain-level caching (`cache_ttl`) of the winning proxy per domain
- Session stickiness (per-client+domain, in-memory, sliding TTL) with redispatch on sticky-proxy failure, 5xx eviction, periodic re-race, and a capacity cap
- CONNECT upstream TCP warm pool (Phase 1, `router.conn_pool`): keeps a few idle TCP connections per upstream so CONNECTs skip the "this host → upstream proxy" connect; target half-preconnection (Phase 2, `conn_pool.target_prewarm`) pre-opens "to upstream" TCP for hot CONNECT targets on domain-cache/sticky hits **or racing wins** (warmed in pairs so a peek leaves a spare), shared fd budget + idle timeout
- Request-cluster predictive pre-warm (Phase 3, `conn_pool.cluster_predict`): learns which CONNECT targets co-occur within a client's page-load window and, at the next window's opening request, pre-builds bare "to-upstream" TCP for the predicted co-targets (never CONNECTing the origin) — cutting the subresource burst's connect TTFB
- Slow single-send sampling log (`router.circuit.single_send_slow_log_ms`): when a sticky/domain-cache single-send (the paths that skip racing) takes longer than the threshold from request to first byte, logs a line carrying the client IP — the only IP-attribution anchor for "won't open / needs refresh", since the success path emits no IP log
- In-memory HTTP `GET` response cache with `Cache-Control` awareness
- In-flight GET coalescing: concurrent requests to the same URL await the in-flight upstream request instead of racing a duplicate (bounded wait, falls back to racing on timeout)
- Write-method cache invalidation: `POST`/`PUT`/`DELETE`/`PATCH` evict all cached `GET` responses for that domain before forwarding, so subsequent `GET`s don't serve stale content
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

> Auth gates the **proxy port only** by default. The management API on `:18080` has its own optional HTTP Basic auth (`api.auth`, off by default) — when enabled, every management endpoint except `/health` requires credentials.

### Management API authentication

The management API on `:18080` is **open by default**. To protect it, enable Basic auth in `config.yaml`:

```yaml
api:
  auth:
    enabled: true
    username: "admin"
    password: "secret"
```

When enabled, every endpoint except `/health` returns `401` without credentials:

```bash
curl http://127.0.0.1:18080/proxies        # → 401
curl -u admin:secret http://127.0.0.1:18080/proxies   # → 200
curl http://127.0.0.1:18080/health          # always open
```

The built-in dashboard at `/` uses the same protection: open it in a browser, enter the credentials in the prompt, and the auto-refresh fetches reuse them (browsers cache Basic credentials per origin). `/health` stays open for load balancers and monitoring.

## Session stickiness

**Disabled by default.** When enabled, requests from the same client IP to the same domain/target reuse the proxy that last won for that key (single-send, no racing), keeping the **egress IP stable** — origin sites often bind login state / bot-protection / CAPTCHA to the egress IP, so switching proxies mid-session can drop logins or trigger bot flags.

```yaml
router:
  stickiness:
    enabled: true
    ttl: 1800          # stickiness TTL (s); refreshed on hit (sliding), active sessions never expire
    recheck_hits: 100  # re-race after N sticky hits (0=off), default 100
    max_entries: 100000 # hard capacity cap; evicts the oldest entry when exceeded
```

Behavior notes:

- Key is `client_ip|domain` (URL hostname for HTTP, `host:port` for CONNECT); **takes precedence over the domain cache**. Hit → single-send; miss → domain cache → racing.
- **Redispatch**: a failing sticky proxy evicts its entry and falls back to the domain cache/racing; the racing winner repopulates the sticky table, so the next request automatically switches proxies.
- **5xx eviction**: a sticky single-send that returns HTTP 5xx also evicts the entry (the response is already streamed, so the next request re-races) instead of letting a sick proxy hold the egress forever.
- **Periodic recheck**: after `recheck_hits` sticky hits, the entry is evicted and, **skipping the domain cache**, re-raced so a newer winner replaces a sticky proxy that may have slowed down; the new winner restarts the hit counter.
- **Local racing**: with `enable_local_racing` on, a local win is also pinned in the sticky table and stays sticky (direct connection) instead of being mistaken for a dead proxy.
- **Capacity cap**: `max_entries` hard limit; before inserting, expired entries are pruned and, if still over, the entry with the oldest `updated_at` is evicted — bounding memory when the client-IP set grows large.
- In-memory only (HAProxy-style table), cleared on restart; a background sweep prunes expired entries so the client-IP set cannot grow unbounded.
- Entries pointing at a deleted/disabled proxy are validated and evicted on use.
- Inspect the current table: `curl http://127.0.0.1:18080/stickiness`; the management dashboard has a "Session stickiness" view showing the full table plus size / hits / evictions counters.

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

- **HTTP requests**: raced via `httpx.AsyncClient` (streaming, one long-lived pooled client per upstream); winning response is written back, loser responses are closed
- **CONNECT requests**: raced via raw `asyncio.open_connection` tunnels with connect/read timeouts
- **Selection**: `ProxySelector` produces the racing order — enabled proxies, circuit-open excluded, sorted by weighted least-request (`ewma × (1 + active)^bias`, fast-and-idle first) with slow-start-recovering and unknown-quality proxies demoted; the first `max_retries` form the first (staggered) batch, then any remaining proxies race as a fallback
- **Domain cache**: after a win, the winning proxy is recorded in `domain_meta` and reused for the domain until `cache_ttl` expires
- **Session stickiness**: same client IP + domain/target reuses the last winning proxy (single-send, beats the domain cache); a failing sticky proxy evicts its entry and falls back to racing, and the winner repopulates the table
- **Stats**: `request_counts` (wins), `attempted_counts` (total attempts) per proxy; `domain_stats` (wins per domain per proxy) in SQLite

### Module layout

The router is split into a thin orchestration shell plus focused collaborator modules. Each collaborator owns its state and methods; `Router` forwards the hot-path member names to them via whitelist `__getattr__`/`__setattr__`, so callers (`api.py`, tests, benches) keep using the same attribute names.

| File | Holds |
|------|-------|
| `router.py` | `Router` — client handling, racing, domain cache, single-send degrade, policy routing, SQLite persistence, and the forwarding shim to the collaborators |
| `selector.py` | `ProxySelector` — per-proxy EWMA latency, circuit breaker + slow-start, adaptive concurrency limit, multi-objective Cost ranking + breakdown, warm-up order |
| `tuner.py` | `AutoTuner` — conservative hill-climb auto-tuner for the three cost weights (P1, off by default) |
| `digest.py` | `TDigest` — self-contained t-digest (dict subclass, JSON-persistable) behind the lifetime percentiles |
| `pools.py` | `ConnectionPools` — the three CONNECT warm pools (generic, target prewarm, established-handshake reuse) |
| `sticky.py` | `StickyCache` — per-client+domain session stickiness table |
| `http_cache.py` | `HttpCache` — GET response cache (LRU, in-flight coalescing, write-method invalidation) |
| `cluster.py` | `ClusterGraph` — request-cluster co-occurrence graph (per-client windows → global edges); predicts the next targets in a page's cluster and pre-warms "to-upstream" TCP for them |
| `config_schema.py` | `Config` / `RouterConfig` / … pydantic models (`extra="forbid"`, cross-field validation) |
| `api.py` / `cli.py` | Management API + dashboard; entry point (uvloop, config loading, uvicorn) |

The domain-cache cluster (`_meta_cache`, adaptive TTL, switch damping, quality-driven single-send degrade, SQLite persistence) stays in `Router` — it is entangled with the win-recording decision chain and read directly by `api.py`.

## API Endpoints

All on the management port `:18080`. `POST` bodies are JSON; unknown fields are rejected (`422`). When `api.auth` is enabled, everything except `/health` requires HTTP Basic credentials.

### Proxy & stats

| Endpoint | Description |
|----------|-------------|
| `GET /` | Web UI dashboard (domain stats, default proxies, auto-refresh; click a stat card to filter domains by Default Proxy) |
| `GET /health` | Health check (always open, even with `api.auth`) |
| `GET /proxies` | List configured proxies |
| `POST /proxies` | Add a proxy (`ProxyIn` JSON: `id`, `host`, `port`, `protocol`, `auth`, `enabled`, `tags`); persisted to `proxies.yaml` |
| `GET /stats` | `request_counts` (wins) + `attempted_counts` (total attempts) per proxy |
| `GET /domains` | Per-domain win stats from SQLite |
| `GET /domains/meta` | Per-domain default proxy + last-updated time (+ `ttl`/`expires_at`/`switch_count` when adaptive TTL is enabled) |
| `GET /stickiness` | Session stickiness table (client_ip\|domain → sticky proxy + updated time) |
| `GET /policies` | Policy-routing config snapshot (match + allowed proxy subset) |
| `GET /config` | Router config (`enable_local_racing`) |

### Metrics & quality

| Endpoint | Description |
|----------|-------------|
| `GET /metrics` | `request_counts`, `attempted_counts`, domain stats, server perf counters (cache hits, racing fan-out, probe counters incl. `probe_get_sent/ok/failed/throttled`) |
| `GET /metrics/per-destination` | Per (domain, proxy) metrics: windowed percentiles, success rates, error classification, plus a `cumulative` sub-object (lifetime means, smoothed success rate, `ttfb_percentiles`/`ofb_percentiles` from the t-digest, total bytes) |
| `GET /quality` | Per-proxy EWMA first-byte latency (s) — the legacy racing-order basis |
| `GET /quality/meta` | **Enhanced per-proxy metrics**: windowed P50/P95/P99, smoothed windowed success rate, error breakdown, HTTP protocol version counts, a `cumulative` lifetime object, and a `cost_breakdown` per proxy (see [Cost ranking & auto-tuning](#cost-ranking--auto-tuning)) |
| `POST /quality/reset` | Clear all proxy EWMA quality (call after network changes) |
| `GET /circuit` | Circuit-breaker + probe state per proxy (`open`, backoff, `probes_sent/ok/skipped`, `single_send_degrades`, `single_send_slow_log_ms`/`logged`) |
| `POST /circuit/reset` | Un-break all circuits, keep EWMA quality |
| `GET /server-stats` | Server resource sampling (CPU %, event-loop lag) filled by the bench subprocess; empty in normal runs |

### Cost ranking & auto-tuning (P1)

| Endpoint | Description |
|----------|-------------|
| `GET /cost` | Current cost parameters (7 fields), the auto-tuner state (`enabled`, bounds, `baseline`, window samples, `last_decision`), and the safety bounds |
| `POST /cost` | Hot-update any subset of `cost_sort_enabled`, `cost_latency_metric` (`"p99"`/`"ewma"`), `cost_weight_latency`, `cost_weight_success_rate`, `cost_weight_throughput`, `cost_latency_min_samples`, `cost_throughput_min_bytes` — effective on the **next race**, no restart. Negative weights are clamped to 0; if the auto-tuner is on, a manual update re-baselines it (its next window re-measures the manual values as the new known-good point) |
| `POST /tuner` | `{"enabled": true|false}` — toggle the auto-tuner at runtime. Enabling starts fresh (baseline re-measured); disabling reverts the weights to the last adopted baseline. See [Cost ranking & auto-tuning](#cost-ranking--auto-tuning) |

Example session:

```bash
# What does the ranking look like right now, and which component dominates each proxy?
curl http://127.0.0.1:18080/quality/meta | jq '.["239-192"].cost_breakdown'
# → {"rank":2,"cost":0.31,"latency":{"raw":0.124,...,"contrib":0.21},
#     "success_rate":{"failure":0.026,...,"contrib":0.05},"throughput":{...},"load_mult":1.0}

# Success rate is fine but transfers are slow → raise the throughput weight, no restart
curl -X POST http://127.0.0.1:18080/cost \
     -H 'Content-Type: application/json' \
     -d '{"cost_weight_throughput": 0.4}'

# Something looks wrong → instant rollback, without touching the process
curl -X POST http://127.0.0.1:18080/cost -d '{"cost_sort_enabled": false}'

# Turn the auto-tuner on/off at runtime
curl -X POST http://127.0.0.1:18080/tuner -d '{"enabled": true}'
```

`cost_breakdown` per proxy contains: `rank` (same sort key as the racer: slow-start tier → unknown-quality → cost), `cost` (total), the three components' `raw`/`norm` (0 = best, 1 = worst, 0.5 = no data) /`contrib` (weight × norm — the largest contrib is the dominant ranking factor), `load_mult` (consecutive-fail × in-flight penalty folded into the latency), and `slow_start_rank`/`unknown_quality` markers so "ranked last" can be told apart from "recovering". Proxies outside the current candidate set (circuit-open/disabled/concurrency-capped) have `cost_breakdown: null`.

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
  stickiness:
    enabled: false             # session stickiness (per-client+domain)
    ttl: 1800                  # stickiness TTL in seconds, sliding
    recheck_hits: 100          # re-race after N sticky hits (0=off)
    max_entries: 100000        # hard capacity cap (evict oldest when exceeded)
  circuit:
    single_send_degrade_fail: 2     # single-send demote: consec-fail threshold (early warn, 0=off)
    single_send_degrade_ratio: 3.0  # single-send demote: EWMA vs pin-time baseline ratio (0=off)
    single_send_degrade_slack_ms: 10  # absolute floor (ms) against false positives at tiny latencies
    # ── Phase 3: domain-level single-send demote signals (all 0 = off) ──
    single_send_degrade_success_rate: 0.0   # demote when domain success/total < this (needs >=8 samples)
    single_send_degrade_p99_ms: 0.0         # demote when max(TTFB,OFB) P99 > this ms (needs >=4 samples)
    single_send_degrade_min_throughput: 0.0 # demote when domain throughput EWMA < this MB/s (needs >=4 samples)
    single_send_slow_log_ms: 1500     # slow single-send sampling log (ms, 0=off): when a sticky/domain-cache single-send "request → first byte" exceeds this, log one line carrying the client IP (the only IP-attribution anchor for "won't open / needs refresh" — success path logs no IP). Single-send FAILURES (connect-timeout / handshake failure past the threshold) log a `slow single send FAILED` line too (counted under single_send_fail_logged), closing the IP-attribution blind spot for connect-failure stalls.
    connect_tunnel_timeout_sec: 3.0   # CONNECT tunnel connect/read-response timeout (s, default 3; was hardcoded 15): upper bound for _try_tunnel's CONNECT to origin, stops one proxy's egress→origin connect/handshake stall from dragging a request to 10s+. Measured CDN first byte ≈ 0.6s, so 3s gives 5× headroom.
    http_read_timeout_sec: 3.0        # HTTP single-send first-byte read timeout (s, default 3; was 10): _upstream_timeout.read. NOTE: tightening header wait was once found non-net-win (soak p99 + fd buildup, reverted), so watch p99/fd during production gray.
    # ── Phase 4: business-aligned probing (default off) ──
    probe_with_get: false             # follow the CONNECT probe with a lightweight GET
    probe_get_targets: []             # whitelist, e.g. ["https://api.github.com/"]; rotated per proxy
    probe_get_interval_sec: 60.0      # min interval per (proxy, target) — never hammer the origin
    probe_get_timeout_sec: 5.0
    probe_get_max_bytes: 65536        # enough to compute throughput, no big downloads
    # ── Phase 2: multi-objective Cost ranking (default ON) ──
    cost_sort_enabled: true           # false = instant rollback to pure-EWMA ranking
    cost_latency_metric: "p99"        # latency term: "p99" (tail-first) or "ewma"
    cost_weight_latency: 1.0
    cost_weight_success_rate: 0.6
    cost_weight_throughput: 0.1       # keep tiny: tunnel traffic rarely yields per-body throughput
    cost_latency_min_samples: 1       # min digest samples for the P99 term (1 = same as EWMA obs>=1)
    cost_throughput_min_bytes: 1000000  # throughput term needs >=1MB cumulative bytes
  # ── P1: Cost-weight auto-tuner (default off) ──
  # auto_tune:
  #   enabled: true        # conservative hill-climb on the three weights
  #   window_sec: 900      # evaluation window (15 min)
  #   min_samples: 50      # min winner-TTFB samples per window (else extends, max 3x)
  #   step: 0.25           # single-dimension ±25% perturbation per window
  #   hysteresis: 0.05     # adopt only >=5% winner-TTFB improvement; beyond +5% = degrade
  #   sr_guard: 0.005      # reject any trial whose success rate drops >0.5pp
  #   persist: true        # persist the adopted baseline to SQLite, restored across restarts
  # optional speed features (all off by default):
  # policies:                        # narrow the racing candidate set per domain/tag
  #   - match: {domain_suffix: [".cn", "baidu.com"]}
  #     proxies: {tags: {region: "cn"}}
  # adaptive_ttl: {enabled: true, min_sec: 60, max_sec: 1800}   # per-domain TTL by stability
  # switch_damping: {enabled: true, min_wins: 2, ratio: 0.8, abs_ms: 30}  # stable egress
  # concurrency_limit: {enabled: true, initial: 16, min: 2, max: 128, add_on_success: 4, mult_on_failure: 0.5, failure_window: 20}
  # conn_pool: {enabled: true, per_proxy: 4, total: 64, idle_timeout: 30.0, refill_interval: 5.0, refill_target: 2, connect_timeout: 10.0, target_prewarm: true, refill_pause_minutes: 60, refill_pause_activity_window: 120, refill_pause_min_requests: 3, established_reuse: true, cluster_predict: true, cluster_window_sec: 2.0, cluster_predict_topk: 3, cluster_min_support: 2, cluster_graph_ttl_sec: 86400, cluster_graph_max_entries: 100000, cluster_predict_throttle_sec: 30.0}
logging:
  file: "auto_squid.log"
```

## Cost ranking & auto-tuning

Phase 2 replaced the single-signal (EWMA) racing order with a **multi-objective Cost** — the core fix for "fast handshake but flaky or slow transfer" proxies getting picked. Weights were derived from production data: success rates cluster tightly (0.94–0.98, low discrimination), TTFB spans widely (66–151ms, the main signal), and tunnel traffic rarely yields per-body throughput (near-zero → tiny weight + a 1 MB byte floor).

**Observation workflow** (this is the loop the features are built around):

1. `python test_routing.py --metrics` (or `GET /quality/meta`) — every proxy shows a `cost_breakdown` line like `rank=3 cost=1.65 [延迟1.00 成功率0.60 吞吐0.05] (p99=800ms 失败率=50.0%)`. The **largest contribution is the dominant ranking factor** — that's the weight to consider tuning.
2. Watch it for a few days. Proxies switching ranks on P99 tail spikes? That's `cost_latency_metric: "p99"` doing its job; if too twitchy, switch to `"ewma"`.
3. Tune either by hand (`POST /cost`, next race picks it up) or let the auto-tuner do it (`auto_tune.enabled: true` / `POST /tuner`).

**Auto-tuner guarantees** (conservative hill-climb, `(1+1)`-ES style):

- One window (default 15 min) tests exactly **one** ±25% perturbation of one weight, rotating dimensions/directions; hard safety bounds (`latency ∈ [0.2, 4.0]`, `success_rate ∈ [0, 2.0]`, `throughput ∈ [0, 1.0]`).
- The objective is the **winner-TTFB mean** (pure winners only, captured at the race win point via a task side-channel — `record_ttfb` alone would include non-cancelled losers).
- Adopt requires **≥5% improvement** *and* no success-rate drop beyond 0.5pp; degradation ≥5% (or a guard breach) **reverts immediately**; everything in between is treated as noise and retried with the next perturbation.
- Every 10 windows the baseline is re-measured (guards against traffic drift); low-traffic windows auto-extend (max 3×) instead of deciding on noise; the adopted baseline persists to SQLite (`tuner_state`) and survives restarts.
- The tuner only ever touches the three weights — never stagger/max_retries/timeouts. Triple kill-switch: `POST /tuner {"enabled": false}` → `POST /cost {"cost_sort_enabled": false}` → config + restart.

## Speed tuning

The proxy already ships with most latency levers wired in (`examples/config.yaml` shows every field). Which settings to pick depends on your traffic. Three recommended profiles:

**Stability-first** (egress-IP stability, login/bot-protection-sensitive sites):

```yaml
router:
  stickiness:
    enabled: true
    ttl: 1800
    recheck_hits: 100
  circuit:
    probe_interval_sec: 30
    probe_canary: "www.baidu.com:443"
    single_send_degrade_fail: 2
    single_send_degrade_ratio: 3.0
    single_send_degrade_slack_ms: 10
```

**Speed-first** (low TTFB, tolerant of egress switching):

```yaml
router:
  cache_ttl: 900
  stagger_start: true
  stagger_initial: 1        # race the best proxy first, refill at interval
  stagger_interval_ms: 100  # shortest legal refill interval (RFC 8305 floor)
  circuit:
    probe_interval_sec: 20
    lb_bias: 0.5            # stop de-prioritizing the fast proxy too eagerly
    single_send_degrade_fail: 2
    single_send_degrade_ratio: 3.0
    single_send_degrade_slack_ms: 10
```

**Low-fan-out first** (CONNECT-heavy, want minimal racing amplification):

```yaml
router:
  stagger_start: true
  stagger_initial: 1
  stagger_interval_ms: 200
  circuit:
    probe_interval_sec: 30
    lb_bias: 1.0
    single_send_degrade_fail: 1   # pin faster than the circuit breaker
    single_send_degrade_ratio: 2.0
    single_send_degrade_slack_ms: 10
```

Tuning notes:

- **`probe_canary` must be reachable both locally and through every upstream.** After deploying, watch `GET /circuit` — if `probes_skipped` keeps growing, the canary is wrong for your network and probes are silently being skipped (no mis-trips, but no quality signal either).
- **`single_send_degrade_fail` is an early warning for the circuit breaker**: set it to `circuit_threshold - 1` (2 with the default 3) so a pinned proxy that starts failing demotes to racing *before* it breaks the circuit.
- **`single_send_slow_log_ms`** (default 0=off): samples the "request → first byte" latency of sticky/domain-cache single-sends (the paths that skip racing) and, when it exceeds the threshold, logs one line per dirty request carrying the client IP (`slow single send client=<ip> … ttfb=<ms>`), plus counts it under `single_send_slow_logged`. Single-send **failures** (connect-timeout / handshake failure) that take past the threshold also log a `slow single send FAILED` line carrying the IP under a separate `single_send_fail_logged` counter — production stalls were mostly *connect-failure* type (10s+ builds waiting on a pinned proxy's egress→origin), which the success-only sampler couldn't see. The success path logs no IP at all, so this is the only anchor to attribute "won't open / needs multiple refreshes" to a specific client IP on this host. Keep the threshold loose (e.g. 1500 = 1.5 s) to catch genuine stalls without flooding the log; monitor `single_send_slow_logged` / `single_send_fail_logged` in `/metrics` (or `opt.log`) to see how often each trips.
- **`connect_tunnel_timeout_sec`** (default 3.0, configurable; was hardcoded 15): the upper bound on a single CONNECT attempt — both the `open_connection` to the upstream and the CONNECT-to-origin `readline` for the 200. When a pinned upstream's egress to the origin (e.g. a `githubusercontent` Fastly CDN) occasionally stalls on connect/handshake, this cap is what turns that into a fixed "卡 3s then fall back to racing" instead of "卡 10–15s". Measured CDN first byte ≈ 0.6s, so 3s is a 5× safety margin. Lower only if real CDN first bytes are consistently under the new bound; the failed single-send falls back to racing.
- **`http_read_timeout_sec`** (default 3.0, configurable; was 10): the `read` bound of `_upstream_timeout` for plain-HTTP single-sends. ⚠️ A past attempt to tighten the HTTP header wait (`_RACE_HEADER_TIMEOUT` + `asyncio.wait_for`) was reverted as non-net-win (5s config blew up soak p99 + fd buildup), so carry this carefully: verify no p99/fd regression in gray before deploying wide.
- **`lb_bias`** controls how much in-flight backlog penalizes a proxy's race order (`ewma × (1 + active)^bias`). Raise it if slow proxies get hammered; lower it if the fastest proxy is being deprioritized.
- **Policy routing** (`router.policies`) narrows the racing candidate set per domain/tag, cutting TTFB and `racing.amplification` — see `examples/config.yaml` for the shape.
- **`conn_pool.target_prewarm`** (Phase 2) needs `conn_pool.enabled`; it pre-opens "to-upstream" TCP for hot CONNECT targets on domain-cache/sticky hits **or when a racing proxy wins** (the dominant path for most CONNECT traffic — without it prewarm only served the few cache-hit requests). Each target is warmed to 2 connections so a peek leaves a spare. `conn_pool_total` bounds the combined fd budget of both pools. Watch `/metrics` `target_pool_hits` vs `target_pool_misses` to confirm hot targets actually reuse prewarmed connections.
- **`conn_pool.refill_pause_minutes`** (default 60): when no client request has arrived for N consecutive minutes (e.g. overnight), the background refill and target-prewarm **pause** so they stop churning "connect → idle-expire → reconnect" with zero traffic. Production measured ~233 wasted connects/hour per 6 proxies during a 6h idle stretch (100% expired). The pool still drains stale connections while paused (prune runs), and any new request immediately resumes refilling. Set `0` to keep the old always-refill behavior.
- **`conn_pool.refill_pause_activity_window`** (default 120) and **`conn_pool.refill_pause_min_requests`** (default 3): activity is judged as **clustered requests** — real traffic is a cluster (a page load fires CONNECTs to multiple hostnames within seconds, so window counts run 5-30), while background heartbeats (GitHub Desktop's `alive.github.com`, Windows' `client.wns.windows.com`, Edge cloud-messaging — every 3-10 min) are isolated single requests (window count 1, rarely 2). The activity timestamp refreshes only when the count inside the window reaches the threshold, so heartbeats can never defeat the idle pause — while real isolated requests are no longer misclassified (the old `refill_pause_silence_sec` interval cutoff wrongly ignored any request spaced >120s, and refill never resumed during the day). Set the window to `0` or threshold ≤ 1 to keep the old refresh-on-any-request behavior. Note: **idle pause only suspends background prewarm (refill / target-prewarm), never the request path** — a real request always takes/creates/reuses connections normally even while paused.
- **`conn_pool.established_reuse`** (default false): reuses *already-CONNECT-handshaked* tunnels. When a tunnel ends cleanly (no residual buffered data on the upstream side), the connection is returned to `_established_pool` instead of closed; the next request for the same `(proxy, target)` reuses it directly, skipping the CONNECT send + 200 check — saving a full round-trip over slow lines (e.g. github). Strict verification discards dirty connections rather than risk data pollution. The pool is bounded by the global `conn_pool.total` budget (counted alongside the other two pools) plus a per-key cap of 2; before reuse, a 50ms liveness probe (`read(1)`) drops connections whose peer closed (FIN/RST) and falls back to a fresh CONNECT — a dead tunnel can never win a race on zero I/O. Returned connections get `SO_KEEPALIVE` so the OS clears half-open peers while pooled. Watch `/metrics` `established_pool_hits` vs `established_pool_misses` to confirm reuse. Requires `conn_pool.enabled`.
- **`conn_pool.cluster_predict`** (default false, requires `conn_pool.enabled` **and** `conn_pool.target_prewarm`): learns, from each client's page-load window (default 2s group of CONNECT targets), which targets co-occur globally (a cross-client co-occurrence graph) and, at the *next* window's **opening request** — the HTML request while its js/css/CDN burst is still to come — pre-warms bare "local → upstream proxy" TCP for the top-K predicted co-targets (never sending a CONNECT to the origin). It is *predictive* pre-warm, complementing the *reactive* `target_prewarm`: the co-targets' TCP is already connected by the time the subresources arrive, and `_target_pool` hands it out on the take-ladder. A wrong prediction costs one idle TCP that the 30s idle-expiry reaps, and all predictions share the `conn_pool.total` fd budget. `cluster_window_sec` groups targets into clusters; `cluster_predict_topk` caps predicted co-targets per opening; `cluster_min_support` (minimum co-occurrence windows) filters out one-off co-occurrence; `cluster_graph_max_entries` + `cluster_graph_ttl_sec` bound the graph; `cluster_predict_throttle_sec` stops a reload from re-predicting the same pair too often. Watch `/metrics` `cluster_windows_learned` / `cluster_predictions` / `cluster_prewarm_spawned` downstream of `cluster_predict`.

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
- The image runs as a non-root user and `EXPOSE`s ports `10808 18080`.

## Testing

```bash
.venv/bin/python -m pytest -q
```

The suite (328 tests) covers HTTP/CONNECT forwarding, the HTTP response cache (incl. LRU/eviction and in-flight coalescing), the domain cache, racing/aggregation timeouts, client auth, circuit breaker / probing / EWMA selection / in-flight weighting, session stickiness (incl. quality-driven single-send degrade **and slow single-send sampling logs**), per-domain stats + SQLite persistence, UTF-8 header safety, binary-safe request body handling, connection warm pools + established-handshake reuse + idle pause (with the "pause never blocks the request path" guarantee), robustness (request-header limits, truncated-response detection), the config layer (`extra="forbid"` rejects typos, cross-field validation, exit code 2), and the module-split regressions (router_cfg= vs kwarg equivalence and pool/cache/sticky forwarding identity).

The Phase-2+ metric/ranking layer has its own focused suites: `tests/test_phase_metrics.py` (t-digest bounds/roundtrip/accuracy, protocol-version stats, dual-scope double-count regression, Phase-3 demote gating, Phase-4 GET probing, Phase-2 Cost ordering incl. EWMA-equivalence and rollback equivalence, cost breakdown) and `tests/test_tuner.py` (adopt/reject/rollback decisions, SR guard, perturbation rotation + bounds, the win-TTFB task side-channel, baseline persistence/recovery, and the `/cost` + `/tuner` hot-reload endpoints).

CI runs the suite on **Python 3.10, 3.11 and 3.12** via GitHub Actions (`.github/workflows/test.yml`), with a per-test timeout (`pytest --timeout=60`) so a hanging test fails fast instead of blocking the job.

> **Python 3.12 compatibility note**: `StreamWriter.wait_closed()` and `Server.wait_closed()` became stricter in 3.12 — they wait for the peer FIN / active handler coroutines. Prewarm pool connections are "half-open" (TCP established, no data sent), so their peers never close; the router now bounds these with a short timeout, and the mock upstreams in the test suite close idle connections after 5s (mirroring a real upstream's idle timeout). This only surfaced under CI's 3.12 matrix — 3.11 passes without it.

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
| `staircase` | concurrency 1→800, fixed requests per level | throughput/latency vs. concurrency → **saturation point** |
| `rate` | target RPS 100→2000, sustained | latency/error vs. load → **capacity ceiling** |
| `mixed` | 30% hot + 20% large + 20% chunked + 20% cold + 10% CONNECT | a **realistic mixed profile** |
| `soak` | fixed concurrency, sustained (default 60s) | **stability / resource leaks** |

Key metrics: throughput (req/s), TTFB & total at P50/P95/P99, error rate by category, **cache hit rate** and **racing amplification** (derived from the server-side `/metrics` counters, unified across mock and real upstreams), and resource samples (RSS, fd count, connection-pool size, HTTP-cache entries, server CPU % and event-loop lag). Results print to the terminal and are written to `bench_report.json` (tagged with the git revision, so runs are diffable across versions).

**Multi-round (`--rounds N`, default 3):** each round runs the same scenario on a fresh `server_proc` subprocess with a fresh SQLite DB, caches, and mock upstreams, so inter-round variance is pure environment noise. The report then gives per-metric **mean ± stddev** (plus `round_results` for per-round data and `aggregates` for min/max/mean/stddev); `--rounds 1` keeps the report byte-identical to the single-round schema.

## Limitations

- HTTP parsing is MVP-level; large streaming responses may have edge cases
- Management API is open by default — enable `api.auth` (HTTP Basic) before exposing port 18080 beyond a trusted network
- CONNECT tunnel uses raw pipes (no TLS interception)

## License

MIT
