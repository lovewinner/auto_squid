# AGENTS.md

## Setup

```bash
uv venv .venv --seed && uv sync
```

## Test

```bash
uv run pytest -q --timeout=60
```

- `asyncio_mode = "auto"` — no `@pytest.mark.asyncio` needed.
- Single test: `uv run pytest tests/test_end_to_end.py::TestFoo::test_bar -q`
- By keyword: `uv run pytest -q -k "circuit"`
- Per-test timeout 60s (CI hangs were real).
- No linter/formatter configured — tests are the gate.

## Architecture

`Router` (`router.py`) is a thin shell; real state lives in collaborators: `ProxySelector` (`selector.py`), `ConnectionPools` (`pools.py`), `StickyCache` (`sticky.py`), `HttpCache` (`http_cache.py`), `ClusterGraph` (`cluster.py`). `Router` forwards attribute names to them via `__getattr__`/`__setattr__` — callers use `router.stickiness_enabled` etc. without noticing the delegation.

Two ports: proxy `:10808`, management API `:18080`.

## Conventions

- Comments are Chinese and explain *why*. Match this style.
- Config models use `extra="forbid"` — typos in `config.yaml` crash at startup with a clear message, never a traceback.
- `bench/` is a load-test harness, excluded from the `auto_squid` package.
- `proxies.yaml` and `config.yaml` are `.gitignore`'d. Use `examples/config.yaml` and `examples/proxies.yaml` as templates.
- Metrics: windowed (recent 256 samples) and cumulative (lifetime, SQLite-persisted). Dashboard shows cumulative as primary; windowed as annotation. Don't mix them up.
- Entry points: `python -m auto_squid.cli` (starts proxy + API), `python test_routing.py` (routing introspection against a running instance).

## Bench

```bash
python -m bench.stress --quick          # ~10s smoke
python -m bench.stress                  # default staircase
python -m bench.stress --mode all       # all four modes
python -m bench.stress --rounds 3       # multi-round mean±stddev
```
