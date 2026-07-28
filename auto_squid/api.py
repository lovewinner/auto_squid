from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List

from .proxy_store import ProxyStore
from .probe_engine import ProbeEngine
from .config_schema import ProxyInfo

app = FastAPI(title="auto_squid API")

# these will be set by CLI on startup
_proxy_store: ProxyStore | None = None
_probe_engine: ProbeEngine | None = None


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


def mount(proxy_store: ProxyStore, probe_engine: ProbeEngine):
    global _proxy_store, _probe_engine
    _proxy_store = proxy_store
    _probe_engine = probe_engine
