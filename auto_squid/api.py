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


@app.get("/domains/meta")
async def domains_meta():
    if not _router:
        return {}
    return _router.get_domain_meta_from_db()


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
td.domain{font-weight:500;color:#a8d8ea;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:280px;min-width:280px;width:280px}
th:first-child{width:280px;min-width:280px;max-width:280px}
td.num{text-align:center;font-variant-numeric:tabular-nums}
td.best{font-weight:600;color:#e94560}
td.default-proxy{text-align:center;font-weight:600;color:#e94560}
td.updated-at{font-size:11px;color:#888;white-space:nowrap}
.no-data{padding:40px;text-align:center;color:#666}
.pager{display:flex;gap:8px;margin-top:12px;align-items:center;justify-content:center;flex-wrap:wrap}
.pager button{padding:6px 14px;border:1px solid #333;border-radius:4px;background:#16213e;color:#e0e0e0;font-size:13px;cursor:pointer}
.pager button:hover{border-color:#e94560}
.pager button.active{background:#e94560;color:#fff;border-color:#e94560;font-weight:600}
.pager button:disabled{opacity:.35;cursor:default}
.pager span{font-size:13px;color:#888;margin:0 4px}
.footer{margin-top:8px;font-size:12px;color:#555;text-align:center}
select{padding:6px 10px;border:1px solid #333;border-radius:6px;background:#16213e;color:#e0e0e0;font-size:13px;outline:none;cursor:pointer}
select:focus{border-color:#e94560}
.autorefresh-label{font-size:12px;color:#555;min-width:70px;text-align:right}
</style>
</head>
<body>
<h1>Domain Stats</h1>
<div class="toolbar">
<input id="filter" placeholder="Filter domains..." oninput="onFilter()">
<select id="interval" onchange="onIntervalChange(this)">
<option value="0">关闭</option>
<option value="10">10s</option>
<option value="30" selected>30s</option>
<option value="60">60s</option>
<option value="180">3m</option>
<option value="300">5m</option>
<option value="600">10m</option>
</select>
<button onclick="fetchData()">Refresh</button>
<span id="autorefresh-label" class="autorefresh-label"></span>
</div>
<div id="table-wrap"></div>
<div id="pager" class="pager"></div>
<div id="footer" class="footer"></div>
<script>
let data = {}, meta = {}, proxyIds = [];
let page = 0, pageSize = 20;
let refreshTimer = null;
let refreshInterval = 30;

function onIntervalChange(sel) {
  refreshInterval = parseInt(sel.value);
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = null;
  const label = document.getElementById('autorefresh-label');
  if (refreshInterval > 0) {
    label.textContent = '每 ' + sel.options[sel.selectedIndex].text + ' 自动刷新';
    refreshTimer = setInterval(fetchData, refreshInterval * 1000);
  } else {
    label.textContent = '';
  }
}

async function fetchData() {
  const [r1, r2] = await Promise.all([fetch('/domains'), fetch('/domains/meta')]);
  data = await r1.json();
  meta = await r2.json();
  proxyIds = [...new Set(Object.values(data).flatMap(v => Object.keys(v)))].sort();
  const entries = Object.entries(data);
  entries.sort((a,b) => {
    const ua = (meta[a[0]]||{}).updated_at||'';
    const ub = (meta[b[0]]||{}).updated_at||'';
    return ub.localeCompare(ua);
  });
  data = Object.fromEntries(entries);
  page = 0;
  render();
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = setInterval(fetchData, refreshInterval * 1000); }
}

function copyDomain(el) {
  const text = el.title;
  const ta = document.createElement('textarea');
  ta.value = text;
  ta.style.position = 'fixed';
  ta.style.opacity = '0';
  document.body.appendChild(ta);
  ta.select();
  try {
    document.execCommand('copy');
    const orig = el.textContent;
    el.textContent = '✓ Copied!';
    el.style.color = '#4caf50';
    setTimeout(() => { el.textContent = orig; el.style.color = ''; }, 800);
  } catch (e) {}
  document.body.removeChild(ta);
}

function toBeijing(isoStr) {
  if (!isoStr) return '-';
  return new Date(isoStr).toLocaleString('zh-CN', {timeZone:'Asia/Shanghai', hour12:false});
}

function getFiltered() {
  const q = document.getElementById('filter').value.toLowerCase();
  return Object.entries(data).filter(([d]) => d.includes(q));
}

function onFilter() { page = 0; render(); }

function render() {
  const entries = getFiltered();
  const wrap = document.getElementById('table-wrap');
  if (!entries.length) {
    wrap.innerHTML = '<div class="no-data">No data</div>';
    document.getElementById('pager').innerHTML = '';
    document.getElementById('footer').textContent = '';
    return;
  }
  const pageCount = Math.ceil(entries.length / pageSize);
  if (page >= pageCount) page = pageCount - 1;
  const start = page * pageSize;
  const pageEntries = entries.slice(start, start + pageSize);
  const offset = 2; // cols before proxy columns: domain, default-proxy, updated-at

  let html = '<table><thead><tr><th onclick="sortBy(0)">Domain <span class="arrow">↕</span></th>';
  html += '<th onclick="sortBy(1)">Default Proxy <span class="arrow">↕</span></th>';
  html += '<th onclick="sortBy(2)">Updated At <span class="arrow">↕</span></th>';
  for (const pid of proxyIds) html += `<th onclick="sortBy(${proxyIds.indexOf(pid)+offset})">${pid} <span class="arrow">↕</span></th>`;
  html += '<th onclick="sortBy('+(proxyIds.length+offset)+')">Total <span class="arrow">↕</span></th></tr></thead><tbody>';
  for (const [domain, wins] of pageEntries) {
    const m = meta[domain] || {};
    const total = Object.values(wins).reduce((a,b) => a+b, 0);
    const best = Math.max(...Object.values(wins));
    let row = `<tr><td class="domain" title="${domain}" onclick="copyDomain(this)" style="cursor:pointer">${domain}</td>`;
    row += `<td class="default-proxy">${m.default_proxy || '-'}</td>`;
    row += `<td class="updated-at">${toBeijing(m.updated_at)}</td>`;
    for (const pid of proxyIds) {
      const v = wins[pid] || 0;
      row += `<td class="num${v === best && v > 0 ? ' best' : ''}">${v}</td>`;
    }
    row += `<td class="num best">${total}</td></tr>`;
    html += row;
  }
  html += '</tbody></table>';
  wrap.innerHTML = html;

  let pagerHtml = '';
  pagerHtml += `<button onclick="goPage(0)"${page===0?' disabled':''}>&laquo;</button>`;
  pagerHtml += `<button onclick="goPage(${page-1})"${page===0?' disabled':''}>&lsaquo;</button>`;
  const rangeStart = Math.max(0, page - 2);
  const rangeEnd = Math.min(pageCount, page + 3);
  if (rangeStart > 0) pagerHtml += '<button onclick="goPage(0)">1</button><span>...</span>';
  for (let i = rangeStart; i < rangeEnd; i++) {
    pagerHtml += `<button onclick="goPage(${i})"${i===page?' class="active"':''}>${i+1}</button>`;
  }
  if (rangeEnd < pageCount) pagerHtml += '<span>...</span>';
  pagerHtml += `<button onclick="goPage(${page+1})"${page>=pageCount-1?' disabled':''}>&rsaquo;</button>`;
  pagerHtml += `<button onclick="goPage(${pageCount-1})"${page>=pageCount-1?' disabled':''}>&raquo;</button>`;
  document.getElementById('pager').innerHTML = pagerHtml;
  document.getElementById('footer').textContent = entries.length + ' domains \u00b7 ' + proxyIds.length + ' proxies \u00b7 page ' + (page+1) + '/' + pageCount;
}

function goPage(n) { page = n; render(); }

let sortDir = {}, sortCol = 2;
sortDir[2] = 'desc';
function sortBy(col) {
  sortDir[col] = sortDir[col] === 'asc' ? 'desc' : 'asc';
  sortCol = col;
  const entries = Object.entries(data);
  const offset = 2;
  const pidKey = i => proxyIds[i-offset] || null;
  entries.sort((a,b) => {
    const m1 = meta[a[0]]||{}, m2 = meta[b[0]]||{};
    let va, vb;
    if (col === 0) { va = a[0]; vb = b[0]; }
    else if (col === 1) { va = m1.default_proxy||''; vb = m2.default_proxy||''; }
    else if (col === 2) { va = m1.updated_at||''; vb = m2.updated_at||''; }
    else if (col >= offset && col < proxyIds.length+offset) {
      va = a[1][pidKey(col)]||0; vb = b[1][pidKey(col)]||0;
    } else {
      va = Object.values(a[1]).reduce((x,y)=>x+y,0); vb = Object.values(b[1]).reduce((x,y)=>x+y,0);
    }
    if (typeof va === 'string') return sortDir[col] === 'asc' ? va.localeCompare(vb) : vb.localeCompare(va);
    return sortDir[col] === 'asc' ? va - vb : vb - va;
  });
  data = Object.fromEntries(entries);
  page = 0;
  render();
}

fetchData();
onIntervalChange(document.getElementById('interval'));
</script>
</body>
</html>""")

def mount(proxy_store: ProxyStore, probe_engine: ProbeEngine, router: Router | None = None):
    global _proxy_store, _probe_engine, _router
    _proxy_store = proxy_store
    _probe_engine = probe_engine
    _router = router
