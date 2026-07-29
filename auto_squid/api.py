from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
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
    domain_stats = _router.get_domain_stats_from_db() if _router else {}
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
    return _router.get_domain_stats_from_db()


@app.get("/")
async def domains_ui():
    return HTMLResponse("""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>auto_squid - Domain Stats</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;background:#1a1a2e;color:#e0e0e0;padding:24px;max-width:1200px;margin:0 auto}
h1{font-size:22px;margin-bottom:16px;color:#e94560}
.toolbar{display:flex;gap:10px;margin-bottom:16px;align-items:center}
.toolbar input{flex:1;padding:8px 12px;border:1px solid #333;border-radius:6px;background:#16213e;color:#e0e0e0;font-size:14px;outline:none}
.toolbar input:focus{border-color:#e94560}
.toolbar button{padding:8px 20px;border:none;border-radius:6px;background:#e94560;color:#fff;font-size:14px;cursor:pointer}
.toolbar button:hover{background:#d63850}
table{width:100%;border-collapse:collapse;font-size:13px}
thead{position:sticky;top:0;z-index:1}
th{background:#0f3460;padding:10px 12px;text-align:left;cursor:pointer;user-select:none;white-space:nowrap}
th:hover{background:#1a5276}
th .arrow{color:#e94560;margin-left:4px}
td{padding:8px 12px;border-bottom:1px solid #222}
tr:hover td{background:rgba(233,69,96,0.08)}
tr:last-child td{border-bottom:none}
td.domain{font-weight:500;color:#a8d8ea}
td.num{text-align:center;font-variant-numeric:tabular-nums}
td.best{font-weight:600;color:#e94560}
.no-data{padding:40px;text-align:center;color:#666}
.footer{margin-top:12px;font-size:12px;color:#555}
</style>
</head>
<body>
<h1>Domain Stats</h1>
<div class="toolbar">
<input id="filter" placeholder="Filter domains..." oninput="render()">
<button onclick="fetchData()">Refresh</button>
</div>
<div id="table-wrap"></div>
<div id="footer" class="footer"></div>
<script>
let data = {}, proxyIds = [];

async function fetchData() {
  const r = await fetch('/domains');
  data = await r.json();
  proxyIds = [...new Set(Object.values(data).flatMap(v => Object.keys(v)))].sort();
  render();
}

function render() {
  const q = document.getElementById('filter').value.toLowerCase();
  const wrap = document.getElementById('table-wrap');
  const entries = Object.entries(data).filter(([d]) => d.includes(q));
  if (!entries.length) { wrap.innerHTML = '<div class="no-data">No data</div>'; document.getElementById('footer').textContent = ''; return; }
  let html = '<table><thead><tr><th onclick="sortBy(0)">Domain <span class="arrow">↕</span></th>';
  for (const pid of proxyIds) html += `<th onclick="sortBy(${proxyIds.indexOf(pid)+1})">${pid} <span class="arrow">↕</span></th>`;
  html += '<th onclick="sortBy('+(proxyIds.length+1)+')">Total <span class="arrow">↕</span></th></tr></thead><tbody id="tbody"></tbody></table>';
  wrap.innerHTML = html;
  const tbody = document.getElementById('tbody');
  for (const [domain, wins] of entries) {
    const total = Object.values(wins).reduce((a,b) => a+b, 0);
    const best = Math.max(...Object.values(wins));
    let row = `<tr><td class="domain">${domain}</td>`;
    for (const pid of proxyIds) {
      const v = wins[pid] || 0;
      row += `<td class="num${v === best && v > 0 ? ' best' : ''}">${v}</td>`;
    }
    row += `<td class="num best">${total}</td></tr>`;
    tbody.innerHTML += row;
  }
  document.getElementById('footer').textContent = entries.length + ' domains · ' + proxyIds.length + ' proxies';
}

let sortDir = {}, sortCol = 0;
function sortBy(col) {
  sortDir[col] = sortDir[col] === 'asc' ? 'desc' : 'asc';
  sortCol = col;
  const entries = Object.entries(data);
  const pidKey = i => proxyIds[i-1] || null;
  entries.sort((a,b) => {
    const va = col === 0 ? a[0] : col > proxyIds.length ? Object.values(a[1]).reduce((x,y)=>x+y,0) : (a[1][pidKey(col)]||0);
    const vb = col === 0 ? b[0] : col > proxyIds.length ? Object.values(b[1]).reduce((x,y)=>x+y,0) : (b[1][pidKey(col)]||0);
    if (typeof va === 'string') return sortDir[col] === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    return sortDir[col] === 'asc' ? va - vb : vb - va;
  });
  data = Object.fromEntries(entries);
  render();
}

fetchData();
</script>
</body>
</html>""")

def mount(proxy_store: ProxyStore, probe_engine: ProbeEngine, router: Router | None = None):
    global _proxy_store, _probe_engine, _router
    _proxy_store = proxy_store
    _probe_engine = probe_engine
    _router = router
