from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

from .proxy_store import ProxyStore
from .probe_engine import ProbeEngine
from .router import Router
from .config_schema import ProxyInfo

app = FastAPI(title="auto_squid API")

# these will be set by CLI on startup
_proxy_store: ProxyStore | None = None
_probe_engine: ProbeEngine | None = None
_router: Router | None = None


class ProxyIn(BaseModel):
    id: str
    name: str | None = None
    host: str
    port: int = 3128
    protocol: str = "http"


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/proxies")
async def add_proxy(p: ProxyIn):
    if not _proxy_store:
        raise HTTPException(status_code=500, detail="proxy store not initialized")
    proxy = ProxyInfo(**p.model_dump())
    _proxy_store.add(proxy)
    return {"added": proxy.id}


@app.get("/proxies", response_model=List[ProxyIn])
async def list_proxies():
    if not _proxy_store:
        return []
    return [ProxyIn(**p.model_dump()) for p in _proxy_store.list()]


@app.get("/score")
async def scores():
    if not _probe_engine:
        return {}
    return _probe_engine.get_scores()


@app.get("/probe/status")
async def probe_status():
    if not _probe_engine:
        return {"running": False}
    return {"running": getattr(_probe_engine, '_running', False)}


@app.get("/probe/history")
async def probe_history():
    if not _probe_engine:
        return {}
    return _probe_engine.get_history()


@app.get("/probe/states")
async def probe_states():
    if not _probe_engine:
        return {}
    return _probe_engine.get_states()


@app.get("/metrics")
async def metrics():
    if not _probe_engine:
        return {}
    scores = _probe_engine.get_scores()
    states = _probe_engine.get_states()
    counts = _router.request_counts if _router else {}
    attempts = _router.attempted_counts if _router else {}
    domain_stats = _router.domain_stats if _router else {}
    return {"scores_count": len(scores), "states": states, "request_counts": counts, "attempted_counts": attempts, "domain_stats": domain_stats}


@app.get("/stats")
async def stats():
    counts = _router.request_counts if _router else {}
    attempts = _router.attempted_counts if _router else {}
    return {"request_counts": counts, "attempted_counts": attempts}


@app.get("/domains")
async def domains():
    if not _router:
        return {}
    return dict(_router.domain_stats)


def mount(proxy_store: ProxyStore, probe_engine: ProbeEngine, router: Router | None = None):
    global _proxy_store, _probe_engine, _router
    _proxy_store = proxy_store
    _probe_engine = probe_engine
    _router = router
