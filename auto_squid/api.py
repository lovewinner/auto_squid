from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from typing import List

from .proxy_store import ProxyStore
from .router import Router
from .config_schema import ProxyInfo, PolicyRuleIn
from .policy_engine import PolicyEngine

app = FastAPI(title="auto_squid API")

# 由 CLI 在启动时注入
_proxy_store: ProxyStore | None = None
_router: Router | None = None
_policy_engine: PolicyEngine | None = None


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


@app.get("/metrics")
async def metrics():
    counts = _router.request_counts if _router else {}
    attempts = _router.attempted_counts if _router else {}
    domain_stats = _router.get_domain_stats_from_db() if _router else {}
    return {"request_counts": counts, "attempted_counts": attempts, "domain_stats": domain_stats}


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
.tabs{display:flex;gap:0;margin-bottom:16px;border-bottom:2px solid #0f3460}
.tabs button{padding:8px 20px;border:none;background:none;color:#888;font-size:14px;cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-2px}
.tabs button:hover{color:#e0e0e0}
.tabs button.active{color:#e94560;border-bottom-color:#e94560}
.tab-panel{display:none}
.tab-panel.active{display:block}
.form-row{display:flex;gap:8px;margin-bottom:12px;align-items:center;flex-wrap:wrap}
.form-row input,.form-row select{padding:6px 10px;border:1px solid #333;border-radius:4px;background:#16213e;color:#e0e0e0;font-size:13px;outline:none}
.form-row input:focus,.form-row select:focus{border-color:#e94560}
.form-row input[type=number]{width:70px}
.rule-badge{display:inline-block;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:600;text-transform:uppercase}
.rule-badge.force{background:#e94560;color:#fff}
.rule-badge.prefer{background:#f0a500;color:#1a1a2e}
.rule-badge.deny{background:#444;color:#e0e0e0}
td.actions{white-space:nowrap}
td.actions button{padding:3px 10px;border:1px solid #e94560;border-radius:4px;background:0;color:#e94560;font-size:12px;cursor:pointer}
td.actions button:hover{background:#e94560;color:#fff}
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
.stats{display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap}
.stat-card{padding:10px 18px;border-radius:8px;background:#0f3460;text-align:center;min-width:100px}
.stat-card .pid{font-size:15px;font-weight:600;color:#a8d8ea}
.stat-card .count{font-size:20px;font-weight:700;color:#e94560;margin-top:2px}
.stat-card .label{font-size:10px;color:#666;margin-top:1px}
</style>
</head>
<body>
<h1>auto_squid</h1>
<div class="tabs">
<button id="tab-domains" class="active" onclick="switchTab('domains')">Domain Stats</button>
<button id="tab-policy" onclick="switchTab('policy')">Policy Rules</button>
</div>
<div id="panel-domains" class="tab-panel active">
<input id="filter" placeholder="Filter domains..." oninput="onFilter()">
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
<div id="table-wrap"></div>
<div id="pager" class="pager"></div>
<div id="footer" class="footer"></div>
</div><!-- /panel-domains -->
<div id="panel-policy" class="tab-panel">
<div class="toolbar">
<button onclick="fetchRules()">Refresh Rules</button>
</div>
<div id="policy-table-wrap"></div>
<hr style="border-color:#222;margin:20px 0">
<div class="form-row">
<select id="new-rule-type">
<option value="force">force</option>
<option value="prefer">prefer</option>
<option value="deny">deny</option>
</select>
<select id="new-target-type" onchange="onTargetTypeChange()">
<option value="proxy_id">proxy_id</option>
<option value="tag">tag</option>
</select>
<input id="new-domain-pattern" placeholder="domain pattern (e.g. *.example.com)" style="flex:1">
<input id="new-target-proxy" placeholder="proxy id" style="flex:0.5">
<input id="new-tag-key" placeholder="tag key" style="flex:0.3;display:none">
<input id="new-tag-value" placeholder="tag value" style="flex:0.3;display:none">
<input id="new-priority" type="number" value="0" placeholder="priority">
<button onclick="addRule()">Add Rule</button>
</div>
<div id="policy-footer" class="footer"></div>
</div><!-- /panel-policy -->
<script>
let data = {}, meta = {}, proxyIds = [];
let page = 0, pageSize = 20;
let refreshTimer = null;
let refreshInterval = 30;
let cfg = {};

// ── Tab switching ──
function switchTab(tab) {
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tabs button').forEach(b => b.classList.remove('active'));
  document.getElementById('panel-' + tab).classList.add('active');
  document.getElementById('tab-' + tab).classList.add('active');
  if (tab === 'policy') fetchPolicyData();
  if (tab === 'domains') fetchData();
}

// ── Policy Rules ──
let policyRules = [];
async function fetchPolicyData() {
  const r = await fetch('/policy/rules');
  policyRules = await r.json();
  renderPolicyTable();
}
function onTargetTypeChange() {
  const v = document.getElementById('new-target-type').value;
  document.getElementById('new-target-proxy').style.display = v === 'proxy_id' ? '' : 'none';
  document.getElementById('new-tag-key').style.display = v === 'tag' ? '' : 'none';
  document.getElementById('new-tag-value').style.display = v === 'tag' ? '' : 'none';
}
async function addRule() {
  const body = {
    rule_type: document.getElementById('new-rule-type').value,
    domain_pattern: document.getElementById('new-domain-pattern').value,
    target_type: document.getElementById('new-target-type').value,
    priority: parseInt(document.getElementById('new-priority').value) || 0,
    enabled: true
  };
  if (body.target_type === 'proxy_id') body.target_proxy = document.getElementById('new-target-proxy').value;
  else { body.tag_key = document.getElementById('new-tag-key').value; body.tag_value = document.getElementById('new-tag-value').value; }
  const r = await fetch('/policy/rules', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  if (r.ok) { await fetchPolicyData(); document.getElementById('new-domain-pattern').value = ''; }
}
async function deleteRule(id) {
  if (!confirm('Delete rule ' + id + '?')) return;
  await fetch('/policy/rules/' + id, {method:'DELETE'});
  await fetchPolicyData();
}
function renderPolicyTable() {
  let html = '<table><thead><tr><th>ID</th><th>Type</th><th>Domain Pattern</th><th>Target</th><th>Priority</th><th>Actions</th></tr></thead><tbody>';
  if (!policyRules.length) html += '<tr><td colspan="6" class="no-data">No rules</td></tr>';
  for (const r of policyRules) {
    let target = '';
    if (r.target_type === 'proxy_id') target = 'proxy: ' + (r.target_proxy || '-');
    else target = 'tag: ' + (r.tag_key || '') + '=' + (r.tag_value || '');
    html += '<tr><td>' + r.id + '</td><td><span class="rule-badge ' + r.rule_type + '">' + r.rule_type + '</span></td><td>' + r.domain_pattern + '</td><td>' + target + '</td><td class="num">' + r.priority + '</td><td class="actions"><button onclick="deleteRule(' + r.id + ')">Delete</button></td></tr>';
  }
  html += '</tbody></table>';
  document.getElementById('policy-table-wrap').innerHTML = html;
  document.getElementById('policy-footer').textContent = policyRules.length + ' rules';
}

function onIntervalChange(sel) {
  refreshInterval = parseInt(sel.value);
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = null;
  const label = document.getElementById('autorefresh-label');
  if (refreshInterval > 0) {
    label.textContent = '每 ' + sel.options[sel.selectedIndex].text + ' 自动刷新';
    refreshTimer = setInterval(fetchAll, refreshInterval * 1000);
  } else {
    label.textContent = '';
  }
}

function fetchAll() {
  if (document.getElementById('panel-domains').classList.contains('active')) fetchData();
  if (document.getElementById('panel-policy').classList.contains('active')) fetchPolicyData();
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
  page = 0;
  render();
  renderStats();
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = setInterval(fetchAll, refreshInterval * 1000); }
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

function goPage(n) { page = n; render(); }

function renderStats() {
  const cnt = {};
  for (const d in meta) { const p = meta[d].default_proxy; if (p) cnt[p] = (cnt[p]||0) + 1; }
  const sorted = Object.entries(cnt).sort((a,b) => b[1]-a[1]);
  let html = '';
  for (const [pid, n] of sorted) html += `<div class="stat-card"><div class="pid">${pid}</div><div class="count">${n}</div><div class="label">default proxy</div></div>`;
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

# ── Policy Rules API ─────────────────────────────────────────

@app.get("/policy/rules")
async def list_policy_rules():
    if not _policy_engine:
        raise HTTPException(status_code=500, detail="policy engine not initialized")
    rules = _policy_engine.load_rules()
    return [r.model_dump() for r in rules]


@app.post("/policy/rules")
async def create_policy_rule(rule_in: PolicyRuleIn):
    if not _policy_engine:
        raise HTTPException(status_code=500, detail="policy engine not initialized")
    from .config_schema import PolicyRule
    rule = PolicyRule(**rule_in.model_dump())
    created = _policy_engine.add_rule(rule)
    return created.model_dump()


@app.delete("/policy/rules/{rule_id}")
async def delete_policy_rule(rule_id: int):
    if not _policy_engine:
        raise HTTPException(status_code=500, detail="policy engine not initialized")
    deleted = _policy_engine.delete_rule(rule_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="rule not found")
    return {"deleted": rule_id}


def mount(proxy_store: ProxyStore, router: Router | None = None):
    global _proxy_store, _router, _policy_engine
    _proxy_store = proxy_store
    _router = router
    if router:
        _policy_engine = router.policy_engine
