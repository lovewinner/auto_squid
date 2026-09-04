# CODEBUDDY.md

This file provides guidance to CodeBuddy Code when working with code in this repository.

## Overview

`auto_squid` is a lightweight **forward proxy** that forwards client HTTP/HTTPS traffic through a set of upstream proxies using **parallel racing** (RFC 8305 staggered start): it races the best 1–2 upstreams first, refills ~250ms apart, and the first to return a first byte wins; losers are cancelled. It adds domain caching, session stickiness, an HTTP GET response cache, CONNECT TCP warm pools, and SQLite-persisted domain stats, plus a management API + web dashboard.

Two ports: proxy `:10808` (client traffic) and management API `:18080` (dashboard + JSON endpoints, open by default).

## Common commands

Environment uses a checked-in `.venv` (Python 3.10+). `uv.lock` is frozen; CI runs `uv sync --frozen`.

```bash
# Install deps (first time)
uv venv .venv --seed && uv sync

# Run the full test suite (CI uses --timeout=60 to avoid hangs)
.venv/bin/python -m pytest tests/ -q --timeout=60
# or, with uv:
uv run pytest -q --timeout=60

# Run a single test / class / file
.venv/bin/python -m pytest tests/test_end_to_end.py::TestDispatchSingleUnified::test_local_real_failure_feeds_circuit -q
.venv/bin/python -m pytest tests/test_end_to_end.py -q -k "circuit"

# Lint: none configured (no ruff/flake8 in pyproject). Tests are the gate.

# Start the proxy + API (reads ./config.yaml and ./proxies.yaml if present)
.venv/bin/python -m auto_squid.cli                # or installed entry point: auto-squid
.venv/bin/python -m auto_squid.cli --config config.yaml --proxies proxies.yaml --db auto_squid.db

# Live metrics / routing introspection (reads a RUNNING instance's API, not the DB)
python test_routing.py --metrics                  # full quality + per-destination view
python test_routing.py --metrics --api-url http://host:18080
python test_routing.py https://github.com         # analyze single-URL routing decision
```

`pyproject.toml` sets `asyncio_mode = "auto"` (no `@pytest.mark.asyncio` needed) and requires `pytest-timeout`. Tests live in `tests/test_end_to_end.py` and a few sibling modules; `278` tests is the current baseline.

## Architecture (the big picture)

The router is a thin **orchestration shell** (`Router` in `router.py`) plus focused **collaborator** modules that own their own state. `api.py` and `test_routing.py` call `router.<attr>` directly; `Router` forwards those names to collaborators via whitelist `__getattr__`/`__setattr__` (`_POOL_FORWARD`, `_CACHE_FORWARD`, `_STICKY_FORWARD`, `_CLUSTER_FORWARD` at the bottom of `router.py`). So a name like `router.stickiness_enabled` or `router._conn_pool` resolves to a sub-object transparently — don't look for it as a real `Router` attribute.

| Module | Responsibility |
|--------|----------------|
| `router.py` | `Router` shell: client handling, the racing loop (`_race`/`_try_http`/`_try_tunnel`), domain cache, single-send degrade, policy routing, SQLite persistence, win-recording decision chain. Largest file; hot path. |
| `selector.py` | `ProxySelector`: per-proxy EWMA first-byte latency, circuit breaker + slow-start, adaptive concurrency, racing-order ranking, and all **metrics** (windowed + cumulative, see below). |
| `pools.py` | `ConnectionPools`: three CONNECT warm pools (generic, target prewarm, established-handshake reuse). |
| `sticky.py` | `StickyCache`: per-client+domain session stickiness (in-memory, sliding TTL). |
| `http_cache.py` | `HttpCache`: GET response cache (LRU, in-flight coalescing, write-method invalidation). |
| `cluster.py` | `ClusterGraph`: request co-occurrence graph → predicts & pre-warms next CONNECT targets. |
| `config_schema.py` | pydantic `Config`/`RouterConfig` (`extra="forbid"` + cross-field validation). Config errors exit code 2 with a readable message, never a raw traceback. |
| `api.py` | FastAPI management API + server-rendered dashboard (HTML/JS). Mounted via `mount_api(proxy_store, router, api_auth)`. |
| `cli.py` | Typer entry point: loads config, installs uvloop, starts `Router` + uvicorn. |
| `proxy_store.py` | `ProxyStore`: upstream proxy list with YAML persistence + CRUD. |

Key flows:
- **HTTP**: raced via one long-lived pooled `httpx.AsyncClient` per upstream; winner's response is streamed back, losers closed (`_stream_upstream_response` / `_tee_to_cache`).
- **HTTPS (CONNECT)**: raced via raw `asyncio.open_connection` byte tunnels (`_relay_tunnel` / `_pipe`); throughput for HTTPS is recorded via `record_complete` in `_relay_tunnel` (bidirectional bytes over tunnel lifetime, not per-response body timing).
- **Selection order**: `ProxySelector` ranks enabled, non-circuit-open proxies by weighted least-request (`ewma × (1+active)^bias`); first `max_retries` form the staggered first batch, remainder race as fallback.
- **Persistence**: `domain_stats` (per-domain wins) and `domain_meta` (default proxy per domain) are the permanent SQLite-backed pieces. Hot path only touches memory; a background `_flush_loop` (every `FLUSH_INTERVAL = 5.0s`) batches writes under `_db_lock`. API threads and the flush thread are the only DB writers.

### Metrics: windowed EWMA vs cumulative (lifetime)

This is the non-obvious part future edits touch most. `selector.py` keeps **two parallel metric families** per `(proxy, [domain])` bucket:

- **Windowed** (`_OBS_WINDOW = 256` samples): TTFB (proxy-side handshake) and OFB (origin first byte) percentile windows, EWMA latency, `success_rate`, `throughput_ewma`, error classification. Reflects *recent* behavior; what the racing order uses. The `quality`/`percentiles`/EWMA fields in API output are these.
- **Cumulative** (`cum_*` counters → `_cumulative_view`): lifetime `cum_success` / `cum_failure_transport` / `cum_failure_5xx`, `cum_ttfb_sum/n`, `cum_ofb_sum/n`, and monotonic `total_bytes` / `transfer_time`. Derived into true lifetime `success_rate`, `avg_ttfb_ms`, `avg_ofb_ms`, `throughput_mbps`. Persisted to SQLite (`proxy_metrics` / `domain_metrics` JSON) and is the only value that survives restarts — the real "permanent" number.

Both are written in the same scope loop inside `record_ttfb` / `record_failure` / `record_http_error` / `record_complete`. The loop uses `(g, m) if m is not g else (g,)` to process global + domain buckets; when `domain=None`, the global dict is processed twice (pre-existing, ratio stays correct — do not "fix" without rethinking `success`/`total` semantics).

**Consistency rule for the UI / CLI**: `test_routing.py --metrics` and the `api.py` dashboard both surface the **cumulative** value as primary (success rate / throughput), with the windowed EWMA shown as a secondary `(近期 …)` / `累/近` annotation. The dashboard and `--metrics` read the **live in-memory** API (`/quality/meta`, `/metrics/per-destination`) — *not* the SQLite file directly. Only `domain_stats` (wins) is permanently meaningful from the DB. Do not reintroduce a mismatch where the main table shows EWMA while a "累计" line shows cumulative.

## API surface (consumed by dashboard + test_routing.py)

`GET /` dashboard · `/health` · `/proxies` (CRUD) · `/stats` · `/metrics` · `/domains` · `/domains/meta` · `/stickiness` · `/quality` · `/quality/reset` (clears EWMA, persists) · `/circuit` · `/circuit/reset` · `/policies` · `/config`. The dashboard HTML/JS in `api.py` reads `get_pid_quality_v2()` / `get_domain_metrics()` which now include a `cumulative` sub-object.

## Conventions worth knowing

- Comments are in Chinese and dense (they explain *why*, not *what*). Preserve this style; match it for new code.
- `MAX_BODY = 10 MiB` (413 on overflow), `STREAM_CACHE_LIMIT = 1 MiB`, `FLUSH_INTERVAL = 5.0s` are module constants in `router.py`.
- The management API is **open by default**; protect with `api.auth` in `config.yaml`. Proxy port auth is `router.auth` (also off by default).
- `bench/` is a load-test harness, deliberately excluded from the packaged `auto_squid` distribution (`pyproject.toml` `packages.find` is scoped to `auto_squid*`).
