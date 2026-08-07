from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List

from .config_schema import ProxyInfo
from .proxy_store import ProxyStore
from .router import Router

app = FastAPI(title="auto_squid API")

# 由 CLI 在启动时注入
_proxy_store: ProxyStore | None = None
_router: Router | None = None

# 由 bench 压测子进程(server_proc)周期填充:服务端 CPU 与事件循环延迟采样。
# 非压测启动(CLI 正常运行)时为空 dict,/server-stats 返回空快照。
_server_stats: dict = {}


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


@app.get("/quality")
async def quality():
    """返回每代理 EWMA 首字节延迟(秒),供仪表盘/运维观察竞速排序依据。"""
    if not _router:
        return {}
    return _router.selector.get_quality()


@app.post("/quality/reset")
async def quality_reset():
    """清空全部代理 EWMA 质量数据(网络切换/代理分组变化后调用)。

    RFC 8305 §4:历史 RTT 不可跨网络沿用,换网络后应清空重学。
    """
    if not _router:
        raise HTTPException(status_code=500, detail="router not initialized")
    _router.reset_proxy_quality()
    return {"reset": True}


@app.get("/metrics")
async def metrics():
    counts = _router.request_counts if _router else {}
    attempts = _router.attempted_counts if _router else {}
    domain_stats = _router.get_domain_stats_from_db() if _router else {}
    # 服务端性能计数器(缓存命中/竞速扇出),供压测跨进程读取算命中率/放大率。
    counters = _router.snapshot_counters() if _router else {}
    return {"request_counts": counts, "attempted_counts": attempts, "domain_stats": domain_stats,
            "counters": counters}


@app.get("/server-stats")
async def server_stats():
    """压测子进程的服务端资源采样:CPU 占用与事件循环延迟(由 server_proc 填充)。

    非压测启动时返回空快照。压测主进程周期拉取,记进场景的资源指标——
    反映被测 Router 自身(而非客户端)的 CPU 饱和度与同步阻塞情况。
    """
    return _server_stats


@app.get("/stats")
async def stats():
    counts = _router.request_counts if _router else {}
    attempts = _router.attempted_counts if _router else {}
    return {"request_counts": counts, "attempted_counts": attempts}


@app.get("/config")
async def router_config():
    if not _router:
        return {"enable_local_racing": False}
    return {"enable_local_racing": _router.enable_local_racing}


@app.get("/domains")
async def domains():
    """返回各域名在各代理上的获胜次数"""
    if not _router:
        return {}
    return _router.get_domain_stats_from_db()


@app.get("/domains/meta")
async def domains_meta():
    """返回域名缓存元数据（当前默认代理、更新时间）"""
    if not _router:
        return {}
    return _router.get_domain_meta_from_db()


@app.get("/stickiness")
async def stickiness():
    """返回会话粘性表（客户端IP|域名 → 粘性代理、更新时间）"""
    if not _router:
        return {}
    return _router.get_sticky_cache()


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
.toolbar button.view-btn{background:#0f3460;color:#a8d8ea;border:1px solid #333;padding:8px 14px}
.toolbar button.view-btn:hover{background:#1a5276;border-color:#e94560}
.toolbar button.view-btn.active{border-color:#e94560;color:#fff;background:#1a5276}
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
.stats{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.stat-card{padding:10px 18px;border-radius:8px;background:#0f3460;text-align:center;min-width:100px;cursor:pointer;border:2px solid transparent;transition:border-color .15s,transform .15s}
.stat-card:hover{border-color:#e94560;transform:translateY(-2px)}
.stat-card.active{border-color:#e94560;background:#e94560}
.stat-card.active .pid,.stat-card.active .count{color:#fff}
.stat-card.active .label{color:#f8d7dd}
.filter-banner{display:none;margin-bottom:16px;padding:10px 14px;border:1px solid #333;border-radius:6px;background:#16213e;font-size:13px;align-items:center;gap:10px}
.filter-banner .fb-close{cursor:pointer;color:#e94560;font-weight:700;margin-left:auto;border:none;background:none;font-size:15px;padding:2px 6px}
.filter-banner .fb-close:hover{background:rgba(233,69,96,0.15);border-radius:4px}
.stat-card .pid{font-size:15px;font-weight:600;color:#a8d8ea}
.stat-card .count{font-size:20px;font-weight:700;color:#e94560;margin-top:2px}
.stat-card .label{font-size:10px;color:#666;margin-top:1px}
</style>
</head>
<body>
<h1>auto_squid 管理面板</h1>
<div class="toolbar">
<button id="view-domains" class="view-btn active" onclick="setView('domains')">域名统计</button>
<button id="view-stickiness" class="view-btn" onclick="setView('stickiness')">会话粘性</button>
<input id="filter" placeholder="Filter..." oninput="onFilter()">
<select id="interval" onchange="onIntervalChange(this)">
<option value="0">关闭</option>
<option value="3">3s</option>
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
<div id="stats" class="stats"></div>
<div id="filter-banner" class="filter-banner"></div>
<div id="table-wrap"></div>
<div id="pager" class="pager"></div>
<div id="footer" class="footer"></div>
<script>
let data = {}, meta = {}, proxyIds = [];
let page = 0, pageSize = 20;
let refreshTimer = null;
let refreshInterval = 30;
let cfg = {};
let view = 'domains';
let sticky = [], stickyStats = {size: 0, hits: 0, evictions: 0};
let activeProxy = new URLSearchParams(location.search).get('default_proxy') || null;

function onIntervalChange(sel) {
  refreshInterval = parseInt(sel.value);
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = null;
  const label = document.getElementById('autorefresh-label');
  if (refreshInterval > 0) {
    label.textContent = '每 ' + sel.options[sel.selectedIndex].text + ' 自动刷新';
    refreshTimer = setInterval(() => { view === 'stickiness' ? renderStickiness() : fetchData(); }, refreshInterval * 1000);
  } else {
    label.textContent = '';
  }
}

function setView(v) {
  view = v;
  document.getElementById('view-domains').classList.toggle('active', v === 'domains');
  document.getElementById('view-stickiness').classList.toggle('active', v === 'stickiness');
  document.getElementById('filter').placeholder = v === 'stickiness' ? 'Filter client|domain / proxy...' : 'Filter domains...';
  document.getElementById('stats').innerHTML = '';
  document.getElementById('filter-banner').style.display = 'none';
  if (v === 'stickiness') { renderStickiness(); return; }
  page = 0;
  render();
  renderBanner();
  renderStats();
}

async function fetchData() {
  const [r1, r2, r3] = await Promise.all([fetch('/domains'), fetch('/domains/meta'), fetch('/config')]);
  data = await r1.json();
  meta = await r2.json();
  cfg = await r3.json();
  proxyIds = [...new Set(Object.values(data).flatMap(v => Object.keys(v)))].sort();
  // 如果开启本机竞速但尚无数据，仍然显示 local 列
  if (cfg.enable_local_racing && !proxyIds.includes('local')) proxyIds.unshift('local');
  const entries = Object.entries(data);
  // 默认按 Updated At 降序排列
  entries.sort((a,b) => {
    const ua = (meta[a[0]]||{}).updated_at||'';
    const ub = (meta[b[0]]||{}).updated_at||'';
    return ub.localeCompare(ua);
  });
  data = Object.fromEntries(entries);
  if (activeProxy && !proxyIds.includes(activeProxy)) { activeProxy = null; history.replaceState({}, '', location.pathname); }
  page = 0;
  render();
  renderBanner();
  renderStats();
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = setInterval(() => { view === 'stickiness' ? renderStickiness() : fetchData(); }, refreshInterval * 1000); }
}

function selectProxy(pid) {
  activeProxy = pid || null;
  history.replaceState({}, '', activeProxy ? '?default_proxy=' + encodeURIComponent(activeProxy) : location.pathname);
  page = 0;
  scrollTo(0, 0);
  renderBanner();
  render();
  renderStats();
}

function clearFilter() {
  selectProxy(null);
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
  return Object.entries(data).filter(([d]) => {
    if (q && !d.includes(q)) return false;
    if (activeProxy) return (meta[d] || {}).default_proxy === activeProxy;
    return true;
  });
}

function renderBanner() {
  const banner = document.getElementById('filter-banner');
  if (!activeProxy) { banner.style.display = 'none'; banner.innerHTML = ''; return; }
  banner.style.display = 'flex';
  banner.innerHTML = '已筛选：Default Proxy 为 <strong>' + activeProxy + '</strong> 的域名（' +
    Object.keys(data).filter(d => (meta[d] || {}).default_proxy === activeProxy).length + ' 个）' +
    '<button class="fb-close" onclick="clearFilter()" title="清除筛选">✕</button>';
}

function onFilter() { page = 0; if (view === 'stickiness') renderStickyTable(); else render(); }

async function renderStickiness() {
  const [r1, r2] = await Promise.all([fetch('/stickiness'), fetch('/metrics')]);
  const raw = await r1.json();
  const counters = (await r2.json()).counters || {};
  stickyStats = {
    size: counters.sticky_cache_size || 0,
    hits: counters.sticky_cache_hits || 0,
    evictions: counters.sticky_evictions || 0
  };
  const q = document.getElementById('filter').value.toLowerCase();
  sticky = Object.entries(raw).map(([k, v]) => {
    const i = k.indexOf('|');
    return { client: k.slice(0, i), domain: k.slice(i + 1), pid: v.proxy_id || '-', hits: v.hits || 0, updatedAt: v.updated_at || '' };
  });
  if (q) sticky = sticky.filter(s => (s.client + '|' + s.domain).toLowerCase().includes(q) || s.pid.toLowerCase().includes(q));
  sticky.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  document.getElementById('stats').innerHTML =
    '<div class="stat-card" style="cursor:default"><div class="pid">条目</div><div class="count">' + stickyStats.size + '</div><div class="label">sticky cache</div></div>' +
    '<div class="stat-card" style="cursor:default"><div class="pid">命中</div><div class="count">' + stickyStats.hits + '</div><div class="label">sticky hits</div></div>' +
    '<div class="stat-card" style="cursor:default"><div class="pid">驱逐</div><div class="count">' + stickyStats.evictions + '</div><div class="label">sticky evictions</div></div>';
  page = 0;
  renderStickyTable();
}

function renderStickyTable() {
  const wrap = document.getElementById('table-wrap');
  if (!sticky.length) {
    wrap.innerHTML = '<div class="no-data">No sticky entries</div>';
    document.getElementById('pager').innerHTML = '';
    document.getElementById('footer').textContent = '';
    return;
  }
  const pageCount = Math.ceil(sticky.length / pageSize);
  if (page >= pageCount) page = pageCount - 1;
  const start = page * pageSize;
  const pageEntries = sticky.slice(start, start + pageSize);
  let html = '<table><thead><tr><th>Client</th><th>Domain / Target</th><th>Sticky Proxy</th><th>Hits</th><th>Updated At</th></tr></thead><tbody>';
  for (const s of pageEntries) {
    html += `<tr><td class="domain" title="${s.client}" style="max-width:280px;min-width:180px">${s.client}</td>`;
    html += `<td class="domain" title="${s.domain}" style="max-width:280px;min-width:180px">${s.domain}</td>`;
    html += `<td class="default-proxy">${s.pid}</td>`;
    html += `<td class="num">${s.hits}</td>`;
    html += `<td class="updated-at">${toBeijing(s.updatedAt)}</td></tr>`;
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
  document.getElementById('footer').textContent = sticky.length + ' sticky entries \u00b7 page ' + (page+1) + '/' + pageCount;
}

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
  const offset = 2; // 代理数据列之前的列数：domain, default-proxy, updated-at

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

function goPage(n) { page = n; if (view === 'stickiness') renderStickyTable(); else render(); }

function renderStats() {
  const cnt = {};
  for (const d in meta) { const p = meta[d].default_proxy; if (p) cnt[p] = (cnt[p]||0) + 1; }
  const sorted = Object.entries(cnt).sort((a,b) => b[1]-a[1]);
  const total = sorted.reduce((s,[,n]) => s+n, 0);
  let html = '';
  if (total > 0) html += `<div class="stat-card${activeProxy===null?' active':''}" title="全部域名" onclick="selectProxy('')"><div class="pid">全部</div><div class="count">${total}</div><div class="label">default proxy</div></div>`;
  for (const [pid, n] of sorted) {
    html += `<div class="stat-card${pid===activeProxy?' active':''}" title="${pid}" onclick="selectProxy('${pid.replace(/'/g, "\\'")}')"><div class="pid">${pid}</div><div class="count">${n}</div><div class="label">default proxy</div></div>`;
  }
  document.getElementById('stats').innerHTML = html || '<div class="stat-card" style="color:#666;font-size:13px;padding:10px 18px">No data</div>';
}

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

def mount(proxy_store: ProxyStore, router: Router | None = None):
    global _proxy_store, _router
    _proxy_store = proxy_store
    _router = router
