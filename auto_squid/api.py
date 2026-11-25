from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from pydantic import BaseModel, Field, field_validator
from typing import Dict, List
import asyncio
import json
import logging
import os
import time
from collections import deque

from .auth import check_auth
from .config_schema import AuthConfig, ProxyInfo, is_valid_host
from .proxy_store import ProxyStore
from .router import Router

logger = logging.getLogger(__name__)

app = FastAPI(title="auto_squid API")


# ══════════════════════════════════════════════════════════════════════
# 管理面板共享资产(拆分 3 独立页 /panel/domains|stickiness|metrics 用)
#   _PANEL_CSS : 三页共用的整套样式(表 / 卡片 / 分页 / metrics 两表)。
#   _PANEL_JS  : 三页共用的 JS(格式化工具 + 刷新基础设施)。经
#                /static/panel.js 路由返回,三页 <script src> 引用。
# 页面独有逻辑(域名统计渲染 / 粘性渲染 / 监控渲染)内嵌各页 HTML。
# ══════════════════════════════════════════════════════════════════════
_PANEL_CSS = """\
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
.nav{display:flex;gap:8px;margin-bottom:16px;flex-wrap:wrap}
.nav a{padding:6px 14px;border:1px solid #333;border-radius:6px;background:#0f3460;color:#a8d8ea;font-size:13px;text-decoration:none}
.nav a:hover{border-color:#e94560;background:#1a5276}
.nav a.active{border-color:#e94560;color:#fff;background:#1a5276}
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
.metrics-tabs{display:flex;gap:8px;margin-bottom:12px}
.metrics-tabs .view-btn{padding:6px 16px}
.metric-cell-num{text-align:center;font-variant-numeric:tabular-nums}
.metric-cell-muted{color:#555;text-align:center;font-variant-numeric:tabular-nums}
td.err-cell{font-size:11px;color:#a8d8ea}
.ewma-sub{color:#888;font-size:0.82em}
.low-conf{color:#e9a23b;font-size:0.82em;font-weight:500}
tr.cum-row td{font-size:11px;color:#888;padding-top:0;padding-bottom:8px;font-variant-numeric:tabular-nums}
/* 监控指标:窗口 / 累计 两表并排,列对齐直接对比 */
.metrics-split{display:flex;flex-direction:column;gap:24px}
.metrics-pane .pane-title{margin:0 0 8px;font-size:13px;color:#a8d8ea;font-weight:500;text-align:center}
.metrics-table{table-layout:fixed;width:100%}
.metrics-table th:first-child,.metrics-table td:first-child{width:90px;min-width:90px;max-width:90px}
/* 监控指标页专用(深色仪表盘):body.panel-dark 作用域,不影响其它两页 */
body.panel-dark{background:radial-gradient(1100px 520px at 18% -8%,#182331 0,#0b0f16 58%) fixed #0b0f16;color:#c9d1d9;max-width:1400px}
body.panel-dark h1{color:#e6edf3;font-size:20px;font-weight:600}
body.panel-dark .nav a{background:#111823;border-color:#263041;color:#9fb0c3;border-radius:6px}
body.panel-dark .nav a.active{background:#1b2636;border-color:#4c9aff;color:#dbe9ff}
body.panel-dark .toolbar select{background:#111823;border-color:#263041;color:#c9d1d9}
body.panel-dark .toolbar button{background:#1f6feb}
body.panel-dark .toolbar button:hover{background:#2b7fff}
.panel-dark .metrics-tabs .view-btn{background:#111823;border:1px solid #263041;color:#9fb0c3;border-radius:6px;padding:6px 14px}
.panel-dark .metrics-tabs .view-btn.active{background:#1b2636;border-color:#4c9aff;color:#dbe9ff}
.panel-dark .status-panel{display:flex;align-items:stretch;gap:18px;background:linear-gradient(180deg,#141c28,#101722);border:1px solid #263041;border-radius:8px;padding:14px 18px;margin-bottom:16px;box-shadow:inset 0 1px 0 rgba(255,255,255,.03)}
.panel-dark .status-main{display:flex;align-items:center;gap:12px;min-width:240px;padding-right:18px;border-right:1px solid #263041}
.panel-dark .status-dot{width:11px;height:11px;border-radius:50%;flex:0 0 auto}
.panel-dark .status-panel.ok .status-dot{background:#3fb950;box-shadow:0 0 0 4px rgba(63,185,80,.14),0 0 16px rgba(63,185,80,.5)}
.panel-dark .status-panel.warn .status-dot{background:#d29922;box-shadow:0 0 0 4px rgba(210,153,34,.14),0 0 16px rgba(210,153,34,.45)}
.panel-dark .status-panel.bad .status-dot{background:#f85149;box-shadow:0 0 0 4px rgba(248,81,73,.14),0 0 16px rgba(248,81,73,.5)}
.panel-dark .status-text{font-size:16px;font-weight:600;color:#e6edf3}
.panel-dark .status-sub{font-size:11px;color:#8b98a9;margin-top:3px}
.panel-dark .kpi-grid{display:grid;grid-template-columns:repeat(6,minmax(86px,1fr));flex:1}
.panel-dark .kpi{padding:0 14px;border-left:1px solid #1f2937}
.panel-dark .kpi:first-child{border-left:0;padding-left:2px}
.panel-dark .kpi .k{font-size:11px;color:#8b98a9;white-space:nowrap}
.panel-dark .kpi .v{font-size:19px;font-weight:650;margin-top:4px;font-variant-numeric:tabular-nums;color:#e6edf3}
.panel-dark .kpi.ok .v{color:#3fb950}
.panel-dark .kpi.warn .v{color:#d29922}
.panel-dark .kpi.bad .v{color:#f85149}
.panel-dark #table-wrap{background:#111823;border:1px solid #263041;border-radius:8px;overflow:hidden}
.panel-dark th{background:#0f1620;color:#8b98a9;font-size:11px;font-weight:600;letter-spacing:.3px;border-bottom:1px solid #263041;padding:11px 12px}
.panel-dark th:hover{background:#131c29}
.panel-dark td{border-bottom:1px solid #1a2331;padding:11px 12px;font-variant-numeric:tabular-nums}
.panel-dark tr:hover td{background:#151f2d}
.panel-dark tr.node-bad td{background:rgba(248,81,73,.055)}
.panel-dark .pill{display:inline-flex;align-items:center;gap:6px;padding:2px 9px;border-radius:6px;font-size:11px;border:1px solid transparent;white-space:nowrap}
.panel-dark .pill.ok{color:#3fb950;background:rgba(63,185,80,.10);border-color:rgba(63,185,80,.28)}
.panel-dark .pill.warn{color:#d29922;background:rgba(210,153,34,.10);border-color:rgba(210,153,34,.30)}
.panel-dark .pill.bad{color:#f85149;background:rgba(248,81,73,.10);border-color:rgba(248,81,73,.30)}
.panel-dark .pill .dot{width:6px;height:6px;margin:0}
.panel-dark .dot.ok{background:#3fb950}
.panel-dark .dot.warn{background:#d29922}
.panel-dark .dot.bad{background:#f85149}
.panel-dark .node-note{display:block;font-size:11px;color:#7b8798;margin-top:4px;max-width:260px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.panel-dark .ok-text{color:#3fb950}
.panel-dark .bad-text{color:#f85149}
.panel-dark .warn-text{color:#d29922}
.panel-dark .trend-card{margin-bottom:14px}
.panel-dark .trend-legend{float:right;font-weight:400;color:#8b98a9;font-size:11px}
.panel-dark .trend-legend .lg{display:inline-block;width:8px;height:8px;border-radius:2px;margin:0 4px 0 10px}
.panel-dark .trend-legend .lg.ok{background:#3fb950}
.panel-dark .trend-legend .lg.bad{background:#f85149}
.panel-dark .trend-legend .lg.accent{background:#4c9aff}
.panel-dark .trend-empty{color:#6b7787;font-size:12px;padding:18px 0;text-align:center}
.panel-dark #trend-canvas{width:100%;display:block}
/* 域名详情:左列表(搜索/分页)+ 右明细 */
.panel-dark .domain-picker{display:grid;grid-template-columns:340px minmax(0,1fr);gap:12px;align-items:start}
.panel-dark .domain-list-card{background:#111823;border:1px solid #263041;border-radius:8px;padding:10px;position:sticky;top:8px}
.panel-dark #domain-search{width:100%;padding:8px 10px;border:1px solid #263041;border-radius:6px;background:#0b1119;color:#c9d1d9;outline:none;font-size:13px}
.panel-dark #domain-search:focus{border-color:#4c9aff}
.panel-dark .domain-list{margin-top:8px;max-height:520px;overflow:auto}
.panel-dark .domain-row{display:flex;justify-content:space-between;gap:8px;padding:7px 8px;border-radius:6px;cursor:pointer;font-size:12px;color:#a9c1e0}
.panel-dark .domain-row:hover{background:#16202e}
.panel-dark .domain-row.active{background:#16233a;color:#dbe9ff;box-shadow:inset 2px 0 0 #4c9aff}
.panel-dark .domain-row .d{overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.panel-dark .domain-row .w{color:#7b8798;font-variant-numeric:tabular-nums;white-space:nowrap}
.panel-dark .domain-pager{display:flex;gap:6px;align-items:center;justify-content:center;margin-top:8px;font-size:12px;color:#7b8798}
.panel-dark .domain-pager button{background:#111823;border:1px solid #263041;color:#9fb0c3;border-radius:6px;padding:3px 10px;cursor:pointer}
.panel-dark .domain-pager button:hover{border-color:#4c9aff}
.panel-dark .domain-pager button:disabled{opacity:.35;cursor:default}
.panel-dark .domain-detail{min-width:0}
.panel-dark .domain-head{display:flex;gap:14px;align-items:baseline;flex-wrap:wrap;margin-bottom:10px;font-size:12px;color:#8b98a9}
.panel-dark .domain-head b{color:#e6edf3;font-size:15px}
.panel-dark .domain-detail .metrics-table{background:#111823;border:1px solid #263041;border-radius:8px;border-collapse:separate;border-spacing:0;overflow:hidden}
.panel-dark tr.default-row td{background:rgba(76,154,255,.06)}
@media (max-width:900px){.panel-dark .domain-picker{grid-template-columns:1fr}}
.panel-dark .sec-title{margin:18px 0 8px;font-size:12px;color:#8b98a9;font-weight:600;letter-spacing:.4px}
.panel-dark .sys-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;margin-bottom:14px}
.panel-dark .sys-card{background:linear-gradient(180deg,#141c28,#101722);border:1px solid #263041;border-radius:8px;padding:14px}
.panel-dark .sys-card h4{margin:0 0 8px;font-size:12px;color:#9fb0c3;font-weight:600}
.panel-dark .sys-card .row{display:flex;justify-content:space-between;font-size:12px;padding:5px 0;border-bottom:1px dashed #1d2735;color:#9fb0c3}
.panel-dark .sys-card .row:last-child{border-bottom:0}
.panel-dark .sys-card .row b{color:#dbe4ee;font-weight:600;font-variant-numeric:tabular-nums}
.panel-dark .meter{height:4px;background:#1d2735;border-radius:1px;overflow:hidden;margin:4px 0 2px}
.panel-dark .meter i{display:block;height:100%;border-radius:1px;background:#3fb950}
.panel-dark .meter i.warn{background:#d29922}
.panel-dark .meter i.bad{background:#f85149}
.panel-dark .footer{color:#6b7787}
/* 域名统计 / 会话粘性:与监控页共用同一套视觉 */
body.panel-dark .toolbar input{background:#111823;border-color:#263041;color:#c9d1d9}
body.panel-dark .toolbar input:focus{border-color:#4c9aff}
body.panel-dark .autorefresh-label{color:#6b7787}
body.panel-dark .stat-card{background:linear-gradient(180deg,#141c28,#101722);border:1px solid #263041;border-radius:8px;padding:10px 18px}
body.panel-dark .stat-card:hover{border-color:#4c9aff;transform:none}
body.panel-dark .stat-card.active{border-color:#4c9aff;background:#16233a}
body.panel-dark .stat-card .pid{color:#dbe9ff}
body.panel-dark .stat-card .count{color:#4c9aff}
body.panel-dark .stat-card .label{color:#8b98a9}
body.panel-dark .stat-card.active .pid,body.panel-dark .stat-card.active .count{color:#fff}
body.panel-dark .filter-banner{background:#111823;border-color:#263041;color:#c9d1d9}
body.panel-dark .filter-banner .fb-close{color:#f85149}
body.panel-dark td.domain{color:#a9c1e0}
body.panel-dark td.best{color:#4c9aff}
body.panel-dark td.default-proxy{color:#4c9aff}
body.panel-dark .pager button{background:#111823;border-color:#263041;color:#9fb0c3}
body.panel-dark .pager button:hover{border-color:#4c9aff}
body.panel-dark .pager button.active{background:#1f6feb;border-color:#1f6feb;color:#fff}
body.panel-dark .pager span{color:#6b7787}
body.panel-dark .no-data{color:#6b7787}
body.panel-dark .low-conf{color:#d29922}
body.panel-dark .ewma-sub{color:#7b8798}
body.panel-dark tr.cum-row td{color:#7b8798}
body.panel-dark .metrics-pane .pane-title{color:#8b98a9}
body.panel-dark td.err-cell{color:#9fb0c3}
@media (max-width:900px){.panel-dark .status-panel{flex-direction:column}.panel-dark .status-main{border-right:0;padding-right:0}.panel-dark .kpi-grid{grid-template-columns:repeat(3,1fr);gap:10px}.panel-dark .kpi{border-left:0;padding:0}}
"""

_PANEL_JS = """\
// ═══ auto_squid 管理面板 共享 JS ═══
// 被 /panel/domains|stickiness|metrics 三页 <script src="/static/panel.js">
// 引用。含:分页/刷新状态基础、格式化工具(纯函数)、刷新基础设施。
// 页面独有变量(data/sticky/qmeta 等)与渲染函数留在各页内嵌 JS。

// ── 跨页共享状态(每页内嵌 JS 复用)──
let refreshTimer = null;
let refreshInterval = 30;
let page = 0;
let pageSize = 20;
let cfg = {};

// ── 错误分类标签(监控页 errStr 复用)──
const ERR_LABELS = {timeout:'超时', connect:'连接', http_5xx:'5xx', http_status:'HTTP状态', tls:'TLS', protocol:'协议', cancelled:'取消', other:'其他'};

// ── 格式化工具(纯函数,所有页共用)──
function fmtPct(p) { return p == null ? '—' : (p * 100).toFixed(1) + '%'; }
function fmtMs(v) { return v == null ? '—' : Math.round(v * 1000) + 'ms'; }
function fmtBytes(b) {
  if (b == null) return '—';
  if (b >= 1e9) return (b/1e9).toFixed(2) + ' GB';
  if (b >= 1e6) return (b/1e6).toFixed(1) + ' MB';
  if (b >= 1e3) return (b/1e3).toFixed(1) + ' KB';
  return b + ' B';
}
function toBeijing(isoStr) {
  if (!isoStr) return '-';
  return new Date(isoStr).toLocaleString('zh-CN', {timeZone:'Asia/Shanghai', hour12:false});
}

// 累计(永久值)为主, 窗口化 EWMA 作为括号里的次要列。无累计时回退到 EWMA。
function cumCell(cumVal, ewmaVal, fmtFn) {
  const c = (cumVal != null) ? fmtFn(cumVal) : null;
  const e = (ewmaVal != null) ? fmtFn(ewmaVal) : null;
  if (c == null && e == null) return '—';
  if (c != null && e != null) return c + ' <span class="ewma-sub">(' + e + ')</span>';
  return c != null ? c : e;
}
// 分位数单元格: 'p50/p95/p99(n=样本数)'; 低样本(obs < 8)追加「⚠低样本」置信度标识。
function pctCell(p) {
  if (!p || !p.samples) return '—';
  let s = Math.round((p.p50||0)*1000) + '/' + Math.round((p.p95||0)*1000) + '/'
        + Math.round((p.p99||0)*1000) + 'ms(n=' + p.samples + ')';
  if (p.low_confidence) s += ' <span class="low-conf">⚠低样本</span>';
  return s;
}
// 累计明细行(监控页累计表下方补平均时延 + 失败细分)。
function cumLine(cum) {
  if (!cum || !cum.samples) return '累计(永久值): — 暂无样本';
  const at = (cum.avg_ttfb_ms != null) ? Math.round(cum.avg_ttfb_ms) + 'ms' : '—';
  const ob = (cum.avg_ofb_ms != null) ? Math.round(cum.avg_ofb_ms) + 'ms' : '—';
  return '累计(永久值, n=' + cum.samples + '): 平均握手(代理)=' + at
       + ' 平均源站首字节=' + ob
       + ' | 失败:' + (cum.cum_failure_transport + cum.cum_failure_5xx)
       + '(传输' + cum.cum_failure_transport + '+5xx' + cum.cum_failure_5xx + ')';
}
function errStr(errs) {
  const items = Object.entries(errs || {}).filter(([,v]) => v > 0).map(([k,v]) => (ERR_LABELS[k]||k) + ':' + v);
  return items.length ? items.join(', ') : '—';
}
// 终身分位数明细行(t-digest rollup):窗口表给"近 256 次",这行给"全历史"分位。
function lifePctLine(cum) {
  const t = cum.ttfb_percentiles, o = cum.ofb_percentiles;
  if (!t || !t.samples) return '';
  const f = (p) => (p && p.samples)
    ? Math.round(p.p50*1000) + '/' + Math.round(p.p95*1000) + '/' + Math.round(p.p99*1000) + 'ms'
    : '—';
  return '终身分位(全历史 n=' + t.samples + '): 握手 P50/P95/P99=' + f(t)
       + ' 源站首字节=' + f(o);
}
// 协议版本累计计数 {版本串: n} → 紧凑展示,如 "H2 62% / H1 38%";无数据显 "—"。
function protoStr(hv) {
  if (!hv || !Object.keys(hv).length) return '—';
  const tot = Object.values(hv).reduce((a, b) => a + b, 0);
  const parts = Object.keys(hv).sort().map(k => {
    const pct = tot ? Math.round(hv[k] / tot * 100) : 0;
    return k.replace('HTTP/', 'H') + ' ' + pct + '%';
  });
  return parts.join(' / ');
}

// ── 动态文本转义 ──
// 面板把域名 / 代理 id / 客户端 IP 直接拼进 innerHTML,而这些字符串的来源并不
// 可信:域名统计键取自客户端请求的 URL origin-form(`GET /<svg onload=…> HTTP/1.1`
// 会整段成为 key),`?default_proxy=` 查询参数也会反射进横幅。未经转义的拼接就是
// 存储型 + 反射型 XSS —— :10808 的普通客户端种一次,管理员打开面板即执行,并可借
// 浏览器缓存的 Basic 凭据回手改配置。所有进入 HTML 的动态值必须过 esc()。
function esc(s) {
  if (s === null || s === undefined) return '';
  return String(s).replace(/[&<>"']/g, function (c) {
    return {'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[c];
  });
}

// ── 刷新基础设施(回调风格:每页注入自己的 refreshFn)──
function onIntervalChange(sel, refreshFn) {
  refreshInterval = parseInt(sel.value);
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = null;
  const label = document.getElementById('autorefresh-label');
  if (refreshInterval > 0) {
    label.textContent = '每 ' + sel.options[sel.selectedIndex].text + ' 自动刷新';
    refreshTimer = setInterval(() => { refreshFn(); }, refreshInterval * 1000);
  } else {
    label.textContent = '';
  }
}
"""


# 由 CLI 在启动时注入
_proxy_store: ProxyStore | None = None
_router: Router | None = None
# 管理 API 的 HTTP Basic 认证配置(默认 None = 关闭)。由 mount() 注入。
_api_auth: AuthConfig | None = None

# 由 bench 压测子进程(server_proc)周期填充:服务端 CPU 与事件循环延迟采样。
# 非压测启动(CLI 正常运行)时为空 dict,/server-stats 返回空快照。
_server_stats: dict = {}


_UNSAFE_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})


def _cross_site_write(request: Request) -> bool:
    """判断一个"会改状态"的请求是否来自别的站点(CSRF 形态)。

    为什么需要:Basic 凭据一旦被浏览器按 origin 缓存,第三方页面的
    `fetch('http://127.0.0.1:18080/proxies', {method:'POST'})` 会**自动附带**这套
    凭据 —— 跨域请求照样发得出去(只是读不回包)。运维开着面板再点开别的网页,
    就可能被静默改走上游代理 / 熔断 / cost 配置。而 api.auth 默认关闭,那时
    本地任意页面更是直接可写 —— 所以这道检查与是否启用认证无关,必须前置。

    判据(按可靠度):
    - `Sec-Fetch-Site: cross-site` —— 浏览器自己给出的权威答案,直接拒;
    - 否则比 `Origin` 与请求 host:不同则拒。Origin 缺失(curl/脚本/同源表单)
      放行,保持命令行用法不变。
    """
    if request.headers.get("sec-fetch-site", "").lower() == "cross-site":
        return True
    origin = request.headers.get("origin")
    if not origin or origin.lower() == "null":
        return False
    # 只比 host[:port]:反向代理改写了 scheme(外部 https、内部 http)时,
    # 连 scheme 一起比会把合法的同站管理请求判成跨站。
    host = origin.split("://", 1)[-1].rstrip("/")
    request_host = request.url.netloc
    return host != request_host


@app.middleware("http")
async def api_auth_middleware(request: Request, call_next):
    """管理 API 的 HTTP Basic 认证(经 config api.auth 开启,默认关闭)。

    开启后除 /health(健康检查/负载均衡探活)外,全部端点(含内嵌仪表盘 /
    以及 FastAPI 自动生成的 /docs、/openapi.json)均需凭据。凭据经标准
    Authorization 头传入,复用 auth.check_auth(常量时间比较)。认证失败回
    401 + WWW-Authenticate,浏览器据此弹出原生凭据框;成功后浏览器按 origin
    缓存 Basic 凭据,页面内同源 fetch 自动附带 Authorization,仪表盘无需改动。
    """
    if request.method in _UNSAFE_METHODS and _cross_site_write(request):
        return JSONResponse(status_code=403,
                            content={"detail": "cross-site admin write blocked"})
    if not _api_auth or not _api_auth.enabled:
        return await call_next(request)
    if request.url.path == "/health":
        return await call_next(request)
    # Starlette 的 Headers 会把键小写化(dict(request.headers) 的键为
    # "authorization"),而 check_auth 读取字面量 "Authorization"/"Proxy-Authorization",
    # 这里补回标准大小写,否则认证永不通过。
    hdrs = dict(request.headers)
    if "authorization" in hdrs and "Authorization" not in hdrs:
        hdrs["Authorization"] = hdrs["authorization"]
    if "proxy-authorization" in hdrs and "Proxy-Authorization" not in hdrs:
        hdrs["Proxy-Authorization"] = hdrs["proxy-authorization"]
    ok, _ = check_auth(hdrs, True, _api_auth.username, _api_auth.password)
    if not ok:
        return JSONResponse(status_code=401, content={"detail": "Authentication required"},
                            headers={"WWW-Authenticate": 'Basic realm="auto_squid"'})
    return await call_next(request)


# GET /proxies 默认把凭据值替换成这个占位符(见 list_proxies 的说明)。
#: 管理端点单次返回的条目上限:表规模上限是 10 万量级,全量快照的拷贝与序列化
#: 都在事件循环上跑,而面板是按几秒一次自动刷新的 —— 默认截断到最近/最相关的
#: N 条,要全量显式传 limit=0(见各端点 docstring)。
_ADMIN_DEFAULT_LIMIT = 2000

_AUTH_MASK = "***"


class ProxyIn(BaseModel):
    """POST /proxies 的入参模型:与 ProxyInfo 字段对齐。

    历史上曾只含 id/name/host/port/protocol,导致通过管理 API 添加代理时
    `enabled`(禁用状态)与 `auth`(上游认证)字段被 pydantic 静默丢弃——
    ProxyInfo(**model_dump()) 拿到缺字段的 dict,添加出的代理总是 enabled=True、
    无认证。这里显式列出全部可写字段(与 config_schema.ProxyInfo 对齐),
    `auth` 复用 ProxyInfo 的 Dict[str,str] 结构(如 {"username": .., "password": ..})。
    """
    id: str
    name: str | None = None
    host: str
    port: int = Field(3128, ge=1, le=65535)
    protocol: str = "http"
    auth: Dict[str, str] | None = None
    enabled: bool = True
    tags: Dict[str, str] | None = None

    @field_validator("host")
    @classmethod
    def _check_host(cls, v: str):
        """host 只接受裸主机名 / IPv4 / 方括号 IPv6(见 config_schema.is_valid_host)。

        上游代理 URL 是 f-string 拼出来的(proxy_store.proxy_url →
        `http://{host}:{port}`,凭据版为 `http://u:p@{host}:{port}`),所以
        '@' '/' ':' 这类字符会改写实际连接的上游(把流量导向第三个主机)。
        管理 API 的写入路径必须挡住,否则一条 POST 就是定向 SSRF/流量劫持。
        """
        v = v.strip()
        if not is_valid_host(v):
            raise ValueError(
                f"host {v!r} 不合法:只允许主机名 / IPv4 / [IPv6],"
                "不接受含 scheme、端口、'@' 或 '/' 的写法(会改写上游目标)")
        return v

    @field_validator("protocol")
    @classmethod
    def _check_protocol(cls, v: str):
        # ProxyInfo.protocol 全仓无消费点:声明 socks5/https 也会被当作 HTTP
        # CONNECT 代理使用(静默错配)。与其静默误用,不如在写入点拒绝。
        if v != "http":
            raise ValueError(
                f"protocol {v!r} 未实现:当前只支持 HTTP CONNECT 上游代理('http')")
        return v


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/proxies")
async def add_proxy(p: ProxyIn):
    if not _proxy_store:
        raise HTTPException(status_code=500, detail="proxy store not initialized")
    if p.auth and all(v == _AUTH_MASK for v in p.auth.values()):
        # "GET 脱敏 → 改字段 → POST 回去"是常见改配置流程,把 '***' 当真实口令
        # 写回去会静默打坏这个节点(上游返回 407)。要求带 include_secrets 重取。
        raise HTTPException(
            status_code=422,
            detail="auth 值是 GET /proxies 的脱敏占位符;请先 GET ?include_secrets=true "
                   "取回真实凭据再提交,或整体省略 auth 字段")
    proxy = ProxyInfo(**p.model_dump())
    _proxy_store.add(proxy)
    # 落盘(README 承诺"添加代理会持久化到 proxies.yaml"):此前 save() 全仓无调用点,
    # 经 API 加的节点重启即丢。落盘失败(只读挂载、目录不存在)**不回滚内存态**——
    # 节点此刻已在服务流量,回滚会让正在工作的路由与响应互相矛盾;改为如实回报
    # persisted=false,让调用方知道这是"本次进程有效、重启后消失"的临时改动。
    persisted = False
    persist_error = None
    if _proxy_store.path is None:
        persist_error = "store 未绑定 proxies.yaml 路径,节点仅在本次进程内有效"
    else:
        try:
            _proxy_store.save()
            persisted = True
        except OSError as e:
            persist_error = f"{type(e).__name__}: {e}"
            logger.warning("POST /proxies: node %s added in-memory but persist to %s failed: %s",
                           proxy.id, _proxy_store.path, e)
    out = {"added": proxy.id, "persisted": persisted}
    if persist_error:
        out["persist_error"] = persist_error
    return out


@app.get("/proxies", response_model=List[ProxyIn])
async def list_proxies(include_secrets: bool = False):
    """列出上游代理。默认**脱敏** `auth` 的值(保留键名,值替换为 '***')。

    凭据只是"配置代理"这一必需目的的副产物,而一次 GET 就把全部上游口令
    拖走(认证关闭时更是匿名可读),代价与收益完全不对称。保留键名让调用方
    仍能判断"该代理是否配了认证、用的是 username/password 还是别的字段"。
    确需读原文时显式带 ?include_secrets=true。
    """
    if not _proxy_store:
        return []
    items = []
    for p in _proxy_store.list():
        d = p.model_dump()
        if not include_secrets and d.get("auth"):
            d["auth"] = {k: _AUTH_MASK for k in d["auth"]}
        items.append(ProxyIn(**d))
    return items


@app.get("/quality")
async def quality():
    """返回每代理 EWMA 首字节延迟(秒),供仪表盘/运维观察竞速排序依据。"""
    if not _router:
        return {}
    return _router.selector.get_quality()


@app.get("/quality/meta")
async def quality_meta():
    """返回每代理的增强指标(Phase 1,IMPROVEMENT_PLAN.md)。

    在原有 EWMA TTFB 之外补充握手/源站首字节分位数(P50/P95/P99)、成功率、错误
    分类、吞吐与累计字节。供运维/仪表盘评估"特定 URL 实测速度"(握手=代理侧、
    源站首字节=源站侧,两维度覆盖"代理→源站"整条链路)。返回 {pid: metric},见
    selector.get_pid_quality_v2()。
    """
    if not _router:
        return {}
    return _router.selector.get_pid_quality_v2()


@app.post("/quality/reset")
async def quality_reset():
    """清空全部代理 EWMA 质量数据(网络切换/代理分组变化后调用)。

    RFC 8305 §4:历史 RTT 不可跨网络沿用,换网络后应清空重学。
    """
    if not _router:
        raise HTTPException(status_code=500, detail="router not initialized")
    _router.reset_proxy_quality()
    return {"reset": True}


@app.get("/policies")
async def policies():
    """返回策略路由配置快照(匹配条件 + 允许的代理子集)。

    供运维确认策略路由已生效、校验命中顺序。读 _router._policies,无锁。
    """
    if not _router:
        return []
    return [
        {
            "match": p.match.model_dump(exclude_none=True),
            "proxies": p.proxies.model_dump(exclude_none=True),
        }
        for p in _router._policies
    ]


@app.get("/circuit")
async def circuit():
    """返回每代理熔断器状态(是否熔断、退避剩余、连续失败、slow-start 中)。

    供仪表盘/运维观察熔断活动。数据来自 selector.get_circuit_state()。
    """
    if not _router:
        return {}
    states = _router.selector.get_circuit_state()
    # get_circuit_state 只为"出现过失败"的代理建条目;面板/探活展示需要每台上游
    # 都有行,这里按 store 里 enabled 的代理补默认态(local 按需)。
    known = {p.id for p in _proxy_store.list() if p.enabled} if _proxy_store else set()
    if _router.enable_local_racing:
        known.add("local")
    for pid in known:
        states.setdefault(pid, {"open": False, "open_until": None, "backoff": 0.0,
                                "consec_fail": 0, "slow_start": False})
    now_m = time.monotonic()
    probe = _router.probe_state_snapshot()
    inflight = _router.selector.get_in_flight()
    for pid, st in states.items():
        # open_until 是 monotonic 时钟,面板侧算不出剩余时间,这里直接换算成秒。
        st["backoff_remaining_sec"] = (
            round(max(0.0, float(st.get("open_until") or 0.0) - now_m), 1)
            if st.get("open") else 0.0)
        st["in_flight"] = inflight.get(pid, 0)
        st["probe"] = probe.get(pid)
        st["last_failure"] = _router.last_failure_snapshot(pid)
    return {
        "circuit_open_count": _router.selector.circuit_open_count,
        # 被归因到"目标不可达"而没有加重于代理的失败数:整池同时跳闸时,这个数
        # 与 circuit_open_count 的比例就是"目标问题 vs 代理问题"的分界线。
        "failures_target_level": _router.selector.failures_target_level,
        "target_attribution_domains": len(_router.selector._domain_fail),
        "probes_sent": _router.probes_sent,
        "probes_ok": _router.probes_ok,
        "probes_skipped": _router.probes_skipped,
        "probes_failed": _router.probes_failed,
        "single_send_degrades": _router.single_send_degrades,
        "degraded_single_send": _router.get_degraded_single_send(),
        # 目标级不可达判定:熔断按代理记账,但"目标谁也到不了"的失败会把整池一起
        # 打黑(RFC1918 目标在本部署里是正常业务,故判据是"全池试过且目标无成功观测",
        # 不是地址段)。这里给出窗口内的目标,便于区分"代理坏了" vs "目标死了"。
        "dead_target_ttl_sec": _router.dead_target_ttl_sec,
        "dead_target_marked": _router.dead_target_marked,
        "dead_target_fastfails": _router.dead_target_fastfails,
        "dead_targets": _router._snapshot_dead_targets(),
        "proxies": states,
    }


@app.post("/circuit/reset")
async def circuit_reset():
    """手动解除全部代理熔断并清空连续失败计数(运维介入后调用)。

    与 /quality/reset 的区别:不动 EWMA(延迟历史仍有效),只清熔断状态,
    让代理立刻重新参与竞速。
    """
    if not _router:
        raise HTTPException(status_code=500, detail="router not initialized")
    _router.reset_proxy_circuits()
    return {"reset": True}


@app.get("/metrics")
async def metrics():
    counts = _router.request_counts if _router else {}
    attempts = _router.attempted_counts if _router else {}
    domain_stats = _router.get_domain_stats_from_db() if _router else {}
    # 服务端性能计数器(缓存命中/竞速扇出),供压测跨进程读取算命中率/放大率。
    counters = _router.snapshot_counters() if _router else {}
    # Phase 1:每代理增强指标(成功率/错误分类/握手+源站首字节/吞吐分位数),见 /quality/meta。
    proxy_metrics = _router.selector.get_proxy_metrics() if _router else {}
    return {"request_counts": counts, "attempted_counts": attempts, "domain_stats": domain_stats,
            "counters": counters, "proxy_metrics": proxy_metrics}


@app.get("/metrics/summary")
async def metrics_summary():
    """只返回 counters(不带域名统计与每代理分位),供面板周期轮询。

    /metrics 会在事件循环上拷贝并序列化全部域名统计(生产 4k+ 域名,~390KB),
    面板每 30s 拉一次就是周期性停顿;这里只回 counters(几 KB)。
    """
    return {"counters": _router.snapshot_counters() if _router else {}}


@app.get("/metrics/trend")
async def metrics_trend():
    """近 3 小时的分钟级趋势桶(增量口径),供面板画折线与后续告警。

    每桶字段均为该周期的增量:probe_ok/probe_fail/probe_skip、biz_ok/biz_fail、
    requests、circuit_opens;另带桶结束时的 circuit_open_now。进程重启后清空。
    """
    if not _router:
        return {"interval_sec": 0, "buckets": []}
    return {"interval_sec": _router.trend_interval_sec,
            "buckets": _router.trend_snapshot()}


@app.get("/metrics/per-destination")
async def metrics_per_destination(domain: str | None = None):
    """每分钟度(域名,代理)的增强指标(Phase 1,IMPROVEMENT_PLAN.md)。

    评估"特定 URL(如 https://github.com 的 domain key github.com:443)实测速度":
    返回 {domain: {pid: {ttfb/ofb 分位数, 成功率, 错误分类, 吞吐}}}。见
    selector.get_domain_metrics()。

    domain 可选:非 None 只返回该域名的 {pid: ...}(监控页明细按需拉单域名,
    避开全量 1175 域名网格的序列化/传输);缺省返回全部。走版本化快照缓存
    (use_cache=True):面板 30s 轮询无新样本时直接复用快照,避免重复重算分位
    (近式单次 1.3s/6.7MB);面板口径剔除原始样本数组,载荷大幅缩减。
    落盘路径(router._flush_to_db)用 use_cache=False 不全缓存/不减样本。

    **全量查询不能跑在事件循环上**:它与转发共用同一个 loop,单次重算约 1.3s,
    且真实流量下缓存版本几乎每次都在变(等于每次重算)。此前只要有人反复拉这个
    URL,代理侧所有隧道就跟着停摆 —— 一个小请求换全站中断。现把重算与序列化
    一起挪进工作线程:循环只负责 await,不再被占用。序列化也一并挪走是必须的,
    否则 6.7MB 的 json.dumps 仍然卡在循环里。
    """
    if not _router:
        return {}
    if domain is not None:
        # 单域名明细是 O(代理数) 的小计算,留在循环内(避免为几 KB 起线程切换)。
        return _router.selector.get_domain_metrics(use_cache=True, domain=domain)
    loop = asyncio.get_running_loop()
    payload = await loop.run_in_executor(
        None,
        lambda: json.dumps(_router.selector.get_domain_metrics(use_cache=True),
                           default=list))
    return Response(content=payload.encode('utf-8'), media_type="application/json")


@app.get("/server-stats")
async def server_stats():
    """压测子进程的服务端资源采样:CPU 占用与事件循环延迟(由 server_proc 填充)。

    非压测启动时返回空快照。压测主进程周期拉取,记进场景的资源指标——
    反映被测 Router 自身(而非客户端)的 CPU 饱和度与同步阻塞情况。
    """
    return _server_stats


# ── 常开进程/事件循环观测(P1)────────────────────────────────
# 事件循环延迟用 0.5s 定时器的"多睡了多久"近似:连续几十 ms 说明有同步阻塞
# (大 JSON 序列化 / 同步 DB 调用)在拖尾。只在有人访问端点后启动采样。
_lag_samples: deque = deque(maxlen=120)
_lag_task = None


def _proc_stats() -> dict:
    out: dict = {}
    try:
        with open("/proc/self/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    out["rss_mb"] = round(int(line.split()[1]) / 1024.0, 1)
                elif line.startswith("Threads:"):
                    out["threads"] = int(line.split()[1])
    except OSError:
        pass
    try:
        out["fds"] = len(os.listdir("/proc/self/fd"))
    except OSError:
        pass
    return out


async def _lag_loop() -> None:
    loop = asyncio.get_running_loop()
    while True:
        t0 = loop.time()
        await asyncio.sleep(0.5)
        _lag_samples.append(max(0.0, loop.time() - t0 - 0.5))


def _ensure_lag_sampler() -> None:
    global _lag_task
    if _lag_task is None or _lag_task.done():
        try:
            _lag_task = asyncio.get_running_loop().create_task(_lag_loop())
        except RuntimeError:
            pass


@app.get("/system-stats")
async def system_stats():
    """服务端资源与事件循环健康:压测 CPU 采样 + 常开的 RSS/FD/线程/loop 延迟。

    /panel/metrics 的系统卡片用它展示"系统自身是否健康";压测字段与常开字段
    合并返回,互不覆盖。
    """
    _ensure_lag_sampler()
    out = dict(_server_stats)
    out.update(_proc_stats())
    vals = sorted(_lag_samples)
    if vals:
        out["event_loop_lag_ms"] = {
            "p50": round(vals[len(vals) // 2] * 1000.0, 1),
            "p95": round(vals[min(len(vals) - 1, int(len(vals) * 0.95))] * 1000.0, 1),
            "max": round(vals[-1] * 1000.0, 1),
            "samples": len(vals),
        }
    else:
        out["event_loop_lag_ms"] = {"p50": None, "p95": None, "max": None, "samples": 0}
    return out


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


# ── Cost 权重热更新 + 自动调参器控制(P1) ──────────────────────

class CostUpdateRequest(BaseModel):
    """POST /cost 入参:Cost 排序参数的运行时更新。

    extra="forbid":拼错的键名直接 422,而不是静默忽略造成"以为改了其实没改"。
    全部字段可选——只传要改的。权重经 max(0.0, ·) 钳制,与 selector 构造语义一致。
    """
    model_config = {"extra": "forbid"}

    cost_sort_enabled: bool | None = None
    cost_latency_metric: str | None = None
    cost_weight_latency: float | None = None
    cost_weight_success_rate: float | None = None
    cost_weight_throughput: float | None = None
    cost_latency_min_samples: int | None = None
    cost_throughput_min_bytes: int | None = None


class TunerToggleRequest(BaseModel):
    """POST /tuner 入参:自动调参器运行时启停。"""
    model_config = {"extra": "forbid"}
    enabled: bool


def _validate_cost_update(data: dict) -> dict:
    """校验并钳制 Cost 更新值(语义与 ProxySelector.__init__ 一致),非法即 422。"""
    out = {}
    if "cost_latency_metric" in data:
        if data["cost_latency_metric"] not in ("p99", "ewma"):
            raise HTTPException(status_code=422,
                                detail="cost_latency_metric must be 'p99' or 'ewma'")
        out["cost_latency_metric"] = data["cost_latency_metric"]
    if "cost_sort_enabled" in data:
        out["cost_sort_enabled"] = bool(data["cost_sort_enabled"])
    if "cost_weight_latency" in data:
        out["cost_weight_latency"] = max(0.0, float(data["cost_weight_latency"]))
    if "cost_weight_success_rate" in data:
        out["cost_weight_success_rate"] = max(0.0, float(data["cost_weight_success_rate"]))
    if "cost_weight_throughput" in data:
        out["cost_weight_throughput"] = max(0.0, float(data["cost_weight_throughput"]))
    if "cost_latency_min_samples" in data:
        out["cost_latency_min_samples"] = max(1, int(data["cost_latency_min_samples"]))
    if "cost_throughput_min_bytes" in data:
        out["cost_throughput_min_bytes"] = max(0, int(data["cost_throughput_min_bytes"]))
    return out


@app.get("/cost")
async def get_cost():
    """Cost 排序参数 + 自动调参器状态快照(只读)。"""
    if not _router:
        raise HTTPException(status_code=503, detail="router not ready")
    snap = _router.tuner.snapshot()
    snap["cost_sort_enabled"] = _router.selector.cost_sort_enabled
    snap["cost_latency_metric"] = _router.selector.cost_latency_metric
    snap["cost_latency_min_samples"] = _router.selector.cost_latency_min_samples
    snap["cost_throughput_min_bytes"] = _router.selector.cost_throughput_min_bytes
    return snap


@app.post("/cost")
async def update_cost(req: CostUpdateRequest):
    """运行时更新 Cost 排序参数(热更新,下一次排序即生效,无需重启)。

    自动调参器开启时,手动更新会置位 pending_baseline:调参器下一窗口把手动值
    重测为新基线,避免旧基线与人类决策打架(见 AutoTuner.manual_override)。
    """
    if not _router:
        raise HTTPException(status_code=503, detail="router not ready")
    updates = _validate_cost_update(req.model_dump(exclude_none=True))
    if not updates:
        raise HTTPException(status_code=422, detail="no updatable fields provided")
    sel = _router.selector
    for k, v in updates.items():
        setattr(sel, k, v)
    _router.tuner.manual_override()
    return {"updated": updates, "snapshot": _router.tuner.snapshot()}


@app.post("/tuner")
async def toggle_tuner(req: TunerToggleRequest):
    """运行时启停自动调参器。关闭时回滚到最近已采纳的基线权重(若有)。"""
    if not _router:
        raise HTTPException(status_code=503, detail="router not ready")
    await _router.tuner.set_enabled(req.enabled)
    return _router.tuner.snapshot()


@app.get("/domains")
async def domains(limit: int = Query(_ADMIN_DEFAULT_LIMIT, ge=0,
                                     description="返回条数上限,0=不限")):
    """返回各域名在各代理上的获胜次数(默认只回 top-N,见 limit)。"""
    if not _router:
        return {}
    return _router.get_domain_stats_from_db(limit=limit or None)


@app.get("/domains/meta")
async def domains_meta(limit: int = Query(_ADMIN_DEFAULT_LIMIT, ge=0,
                                          description="返回条数上限,0=不限")):
    """返回域名缓存元数据(当前默认代理、更新时间;自适应 TTL 开启时含
    ttl/expires_at/switch_count)。

    注意:不再附加 `proxy_metrics`。旧版给每个域名算并返回逐域名逐代理的
    `get_domain_metrics()` 分位快照(实测 ~6.7MB / ~1.3s 的 CPU 重算),但
    域名面板 JS 只读 `default_proxy` + `updated_at`,`proxy_metrics` 完全未被
    前端使用。为消除这部分死重(域名分位明细如需,请走
    `/metrics/per-destination` 或 `/quality/meta`),这里只返回纯元数据。
    见 Phase 1 IMPROVEMENT_PLAN 的评估目标,现改由独占端点承担。
    """
    if not _router:
        return {}
    return _router.get_domain_meta_enriched(limit=limit or None)


@app.get("/stickiness")
async def stickiness(limit: int = Query(_ADMIN_DEFAULT_LIMIT, ge=0,
                                        description="返回条数上限,0=不限")):
    """返回会话粘性表(客户端IP|域名 → 粘性代理、更新时间;默认只回最近 N 条)。"""
    if not _router:
        return {}
    return _router.get_sticky_cache(limit=limit or None)


# ══════════════════════════════════════════════════════════════════════
# 管理面板:拆分为 3 个独立 URL 页面(2026-09-12)
#   /panel/domains    → 域名统计
#   /panel/stickiness → 会话粘性
#   /panel/metrics    → 监控指标
#   /                → 307 重定向到 /panel/domains(兼容旧书签)
# 三页共享 _PANEL_CSS 样式 与 /static/panel.js(_PANEL_JS 含格式化工具/
# 刷新基础设施);页面独有渲染 JS 内嵌各页。页内不再单页三视图互切。
# ══════════════════════════════════════════════════════════════════════

def _panel_nav(active: str) -> str:
    """三页顶部导航条(当前视图高亮)。active ∈ domains|stickiness|metrics。"""
    items = [
        ("domains", "域名统计"),
        ("stickiness", "会话粘性"),
        ("metrics", "监控指标"),
    ]
    links = "".join(
        '<a href="/panel/{k}"{cls}>{t}</a>'.format(
            k=k, t=t, cls=' class="active"' if k == active else ""
        )
        for k, t in items
    )
    return f'<div class="nav">{links}</div>'


def _panel_toolbar(placeholder: str, show_filter: bool = True) -> str:
    """共享顶部工具栏:导航 + (可选筛选框) + 刷新间隔 + 手动刷新。

    监控页(show_filter=False)隐藏筛选框,metrics 自身用 tabs/下拉。
    """
    filter_html = (
        f'<input id="filter" placeholder="{placeholder}" oninput="onFilter()">'
        if show_filter else ""
    )
    return f"""
<div class="toolbar">
{filter_html}
<select id="interval" onchange="onIntervalChange(this, doRefresh)">
<option value="0">关闭</option>
<option value="3">3s</option>
<option value="10">10s</option>
<option value="30" selected>30s</option>
<option value="60">60s</option>
<option value="180">3m</option>
<option value="300">5m</option>
<option value="600">10m</option>
</select>
<button onclick="doRefresh()">Refresh</button>
<span id="autorefresh-label" class="autorefresh-label"></span>
</div>
"""


_PANEL_SHELL = """\
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>auto_squid - {title}</title>
<style>
{css}
</style>
</head>
<body{class_attr}>
<h1>auto_squid 管理面板</h1>
{nav}
{toolbar}
{body}
<script src="/static/panel.js"></script>
{script}
</body>
</html>"""


def _panel_domains_html() -> str:
    """域名统计页:filter + default-proxy stats 卡片 + 分页表 + 排序 + 复制。
    数据: /domains(域名→各代理胜出数), /domains/meta(默认代理/更新时间),
          /config(enable_local_racing)。
    """
    body = """\
<div id="stats" class="stats"></div>
<div id="filter-banner" class="filter-banner"></div>
<div id="table-wrap"></div>
<div id="pager" class="pager"></div>
<div id="footer" class="footer"></div>"""
    script = """\
let data = {}, meta = {}, proxyIds = [];
let activeProxy = new URLSearchParams(location.search).get('default_proxy') || null;

async function fetchData() {
  const [r1, r2, r3] = await Promise.all([fetch('/domains'), fetch('/domains/meta'), fetch('/config')]);
  data = await r1.json();
  meta = await r2.json();
  cfg = await r3.json();
  proxyIds = [...new Set(Object.values(data).flatMap(v => Object.keys(v)))].sort();
  if (cfg.enable_local_racing && !proxyIds.includes('local')) proxyIds.unshift('local');
  const entries = Object.entries(data);
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
}

function doRefresh() { fetchData(); }

function getFiltered() {
  const q = document.getElementById('filter').value.toLowerCase();
  return Object.entries(data).filter(([d]) => {
    if (q && !d.includes(q)) return false;
    if (activeProxy) return (meta[d] || {}).default_proxy === activeProxy;
    return true;
  });
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
  const offset = 2;
  let html = '<table><thead><tr><th onclick="sortBy(0)">Domain <span class="arrow">\\u2195</span></th>';
  html += '<th onclick="sortBy(1)">Default Proxy <span class="arrow">\\u2195</span></th>';
  html += '<th onclick="sortBy(2)">Updated At <span class="arrow">\\u2195</span></th>';
  for (const pid of proxyIds) html += `<th onclick="sortBy(${proxyIds.indexOf(pid)+offset})">${esc(pid)} <span class="arrow">\\u2195</span></th>`;
  html += '<th onclick="sortBy('+(proxyIds.length+offset)+')">Total <span class="arrow">\\u2195</span></th></tr></thead><tbody>';
  for (const [domain, wins] of pageEntries) {
    const m = meta[domain] || {};
    const total = Object.values(wins).reduce((a,b) => a+b, 0);
    const best = Math.max(...Object.values(wins));
    let row = `<tr><td class="domain" title="${esc(domain)}" onclick="copyDomain(this)" style="cursor:pointer">${esc(domain)}</td>`;
    row += `<td class="default-proxy">${esc(m.default_proxy || '-')}</td>`;
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
  document.getElementById('footer').textContent = entries.length + ' domains · ' + proxyIds.length + ' proxies · page ' + (page+1) + '/' + pageCount;
}

function goPage(n) { page = n; render(); }

function renderStats() {
  const cnt = {};
  for (const d in meta) { const p = meta[d].default_proxy; if (p) cnt[p] = (cnt[p]||0) + 1; }
  const sorted = Object.entries(cnt).sort((a,b) => b[1]-a[1]);
  const total = sorted.reduce((s,[,n]) => s+n, 0);
  let html = '';
  if (total > 0) html += `<div class="stat-card${activeProxy===null?' active':''}" title="全部域名" data-pid=""><div class="pid">全部</div><div class="count">${total}</div><div class="label">default proxy</div></div>`;
  for (const [pid, n] of sorted) {
    html += `<div class="stat-card${pid===activeProxy?' active':''}" title="${esc(pid)}" data-pid="${esc(pid)}"><div class="pid">${esc(pid)}</div><div class="count">${n}</div><div class="label">default proxy</div></div>`;
  }
  document.getElementById('stats').innerHTML = html || '<div class="stat-card" style="color:#666;font-size:13px;padding:10px 18px">No data</div>';
}

// 事件委托代替内联 onclick:pid 曾被拼进 `selectProxy('${pid}')` 的 JS 字符串字面量,
// 手工 replace 单引号并不能防住(反斜杠/换行同样能逃出字面量),而 dataset 读到的
// 值是浏览器解码后的原文 —— 动态数据不该进入可执行代码位置。
document.getElementById('stats').addEventListener('click', function (e) {
  const card = e.target.closest('[data-pid]');
  if (card) selectProxy(card.dataset.pid || null);
});

function selectProxy(pid) {
  activeProxy = pid || null;
  history.replaceState({}, '', activeProxy ? '?default_proxy=' + encodeURIComponent(activeProxy) : location.pathname);
  page = 0;
  scrollTo(0, 0);
  renderBanner();
  render();
  renderStats();
}

function clearFilter() { selectProxy(null); }

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

function renderBanner() {
  const banner = document.getElementById('filter-banner');
  if (!activeProxy) { banner.style.display = 'none'; banner.innerHTML = ''; return; }
  banner.style.display = 'flex';
  banner.innerHTML = '已筛选：Default Proxy 为 <strong>' + esc(activeProxy) + '</strong> 的域名（' +
    Object.keys(data).filter(d => (meta[d] || {}).default_proxy === activeProxy).length + ' 个）' +
    '<button class="fb-close" onclick="clearFilter()" title="清除筛选">✕</button>';
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
onIntervalChange(document.getElementById('interval'), doRefresh);
"""
    return _PANEL_SHELL.format(
        title="域名统计",
        class_attr=' class="panel-dark"',
        css=_PANEL_CSS,
        nav=_panel_nav("domains"),
        toolbar=_panel_toolbar("Filter domains...", show_filter=True),
        body=body,
        script=f"<script>{script}</script>",
    )


def _panel_stickiness_html() -> str:
    """会话粘性页:filter + 条目/命中/驱逐三卡片 + 分页表。
    数据: /stickiness(粘性表), /metrics(取 counters 的 sticky 统计)。
    """
    body = """\
<div id="stats" class="stats"></div>
<div id="table-wrap"></div>
<div id="pager" class="pager"></div>
<div id="footer" class="footer"></div>"""
    script = """\
let sticky = [], stickyStats = {size: 0, hits: 0, evictions: 0};

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

function doRefresh() { renderStickiness(); }

function onFilter() { page = 0; renderStickyTable(); }

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
    html += `<tr><td class="domain" title="${esc(s.client)}" style="max-width:280px;min-width:180px">${esc(s.client)}</td>`;
    html += `<td class="domain" title="${esc(s.domain)}" style="max-width:280px;min-width:180px">${esc(s.domain)}</td>`;
    html += `<td class="default-proxy">${esc(s.pid)}</td>`;
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
  // 服务端默认只回最近 2000 条(_ADMIN_DEFAULT_LIMIT):表内真实条数取自
  // /metrics 的 sticky_cache_size,两者不等时明确告诉运维"这是截断视图"。
  let note = sticky.length + ' sticky entries · page ' + (page+1) + '/' + pageCount;
  if (!document.getElementById('filter').value && stickyStats.size > sticky.length) {
    note += ' · 粘性表共 ' + stickyStats.size + ' 条,此处仅最近 ' + sticky.length + ' 条(?limit=0 取全量)';
  }
  document.getElementById('footer').textContent = note;
}

function goPage(n) { page = n; renderStickyTable(); }

renderStickiness();
onIntervalChange(document.getElementById('interval'), doRefresh);
"""
    return _PANEL_SHELL.format(
        title="会话粘性",
        class_attr=' class="panel-dark"',
        css=_PANEL_CSS,
        nav=_panel_nav("stickiness"),
        toolbar=_panel_toolbar("Filter client|domain / proxy...", show_filter=True),
        body=body,
        script=f"<script>{script}</script>",
    )


def _panel_metrics_html() -> str:
    """监控指标页:全局/域名详情 tabs + (域名详情时)域名下拉 + 双表(窗口/累计)。
    无 filter / pager。数据: /quality/meta(每代理增强指标),
                              /metrics/per-destination(域名×代理指标)。
    """
    body = """\
<div id="metrics-tabs" class="metrics-tabs">
<button id="metricsub-global" class="view-btn active" onclick="setMetricsSub('global')">全局概览</button>
<button id="metricsub-domain" class="view-btn" onclick="setMetricsSub('domain')">域名详情</button>
</div>
<div id="domain-pane" style="display:none">
<div class="domain-picker">
  <div class="domain-list-card">
    <input id="domain-search" placeholder="搜索域名…" oninput="onDomainSearch()">
    <div id="domain-list" class="domain-list"></div>
    <div id="domain-pager" class="domain-pager"></div>
  </div>
  <div id="domain-detail" class="domain-detail"></div>
</div>
</div>
<div id="health"></div>
<div id="table-wrap"></div>
<div id="sys-title" class="sec-title" style="display:none">系统运行</div>
<div id="systems"></div>
<div id="trend"></div>
<div id="pager" class="pager"></div>
<div id="footer" class="footer"></div>"""
    script = """\
let metricsSub = 'global';
let metricsDomain = '';
let qmeta = {}, perDest = {}, circ = {}, counters = {}, counts = {}, attempts = {};
let sysStats = {}, trendData = {interval_sec: 60, buckets: []};
let domainData = {}, domainMeta = {}, domainKeys = [], domainFilter = '';
let dpage = 0;
const DSIZE = 20;
let enabledIds = [];
let _prevWin = null, _prevWinAt = 0;

function doRefresh() { fetchMetrics(); }

// 全局概览:只需 /quality/meta(每代理增强指标,轻量)。域名下拉的 keys 来自
// /domains(域名统计,60KB),不拉全量 /metrics/per-destination(1175 域名 6.7MB)。
// 域名明细在切到域名 tab / 选中域名时才按需 /metrics/per-destination?domain=X 拉单域名。
// 系统卡片/趋势用 /metrics/summary(仅 counters)、/system-stats 与 /metrics/trend,
// 都不带域名统计,可随面板周期刷新。
async function fetchMetrics() {
  const [r1, r2, r3, r4, r5, r6, r7, r8, r9] = await Promise.all([
    fetch('/quality/meta'), fetch('/domains'), fetch('/circuit'), fetch('/stats'),
    fetch('/proxies'), fetch('/metrics/summary'), fetch('/system-stats'),
    fetch('/metrics/trend'), fetch('/domains/meta')]);
  qmeta = await r1.json();
  domainData = (await r2.json()) || {};
  circ = (await r3.json()) || {};
  const st = (await r4.json()) || {};
  counts = st.request_counts || {};
  attempts = st.attempted_counts || {};
  try {
    const px = await r5.json();
    enabledIds = (px || []).filter(p => p && p.enabled).map(p => p.id || p.name);
  } catch (e) { enabledIds = []; }
  try { counters = ((await r6.json()) || {}).counters || {}; } catch (e) { counters = {}; }
  try { sysStats = (await r7.json()) || {}; } catch (e) { sysStats = {}; }
  try { trendData = (await r8.json()) || {buckets: []}; } catch (e) { trendData = {buckets: []}; }
  try { domainMeta = (await r9.json()) || {}; } catch (e) { domainMeta = {}; }
  // 域名列表:按最近更新时间倒序(与域名统计页一致),当前选中不存在时回到首个。
  domainKeys = Object.keys(domainData).sort((a, b) => {
    const ua = (domainMeta[a] || {}).updated_at || '', ub = (domainMeta[b] || {}).updated_at || '';
    return ub.localeCompare(ua) || a.localeCompare(b);
  });
  if (!metricsDomain || !domainData[metricsDomain]) metricsDomain = domainKeys[0] || '';
  renderMetrics();
}

class _metric_tracker {
  constructor() { this.last = {}; }
  need(domain) {
    const key = domain || '';
    const t = performance.now();
    // 30s 内不重复拉同一域名明细;强刷(Refresh)会换对象强制重拉
    if (this.last[key] && t - this.last[key] < 30000) return false;
    this.last[key] = t; return true;
  }
  reset() { this.last = {}; }
}
window._metricFetchT = new _metric_tracker();

async function fetchDomain(domain) {
  const rp = await fetch('/metrics/per-destination?domain=' + encodeURIComponent(domain));
  const d = await rp.json();
  perDest = d[domain] || {};
}

async function renderMetrics() {
  const wrap = document.getElementById('table-wrap');
  document.getElementById('pager').innerHTML = '';
  const isGlobal = metricsSub === 'global';
  document.getElementById('health').style.display = isGlobal ? '' : 'none';
  document.getElementById('sys-title').style.display = isGlobal ? '' : 'none';
  document.getElementById('systems').style.display = isGlobal ? '' : 'none';
  document.getElementById('trend').style.display = isGlobal ? '' : 'none';
  document.getElementById('domain-pane').style.display = isGlobal ? 'none' : '';
  wrap.style.display = isGlobal ? '' : 'none';
  if (isGlobal) { renderMetricsGlobal(wrap); return; }
  renderDomainList();
  if (!metricsDomain) {
    document.getElementById('domain-detail').innerHTML = '<div class="no-data">选择左侧域名查看各上游指标</div>';
    document.getElementById('footer').textContent = '';
    return;
  }
  // 域名明细:timer 内已有缓存则直接用;否则按需拉单域名
  if (window._metricFetchT.need(metricsDomain)) {
    await fetchDomain(metricsDomain);
  }
  renderMetricsDomain(document.getElementById('domain-detail'));
}

// ── 域名列表:搜索 + 分页(替代原先 2000+ 项的大下拉)──────────────
function visibleDomains() {
  const q = domainFilter.trim().toLowerCase();
  return q ? domainKeys.filter(k => k.toLowerCase().includes(q)) : domainKeys;
}

function renderDomainList() {
  const listEl = document.getElementById('domain-list');
  const all = visibleDomains();
  const pages = Math.max(1, Math.ceil(all.length / DSIZE));
  if (dpage >= pages) dpage = pages - 1;
  const slice = all.slice(dpage * DSIZE, dpage * DSIZE + DSIZE);
  listEl.innerHTML = slice.map(k => {
    const wins = Object.values(domainData[k] || {}).reduce((a, b) => a + b, 0);
    return '<div class="domain-row' + (k === metricsDomain ? ' active' : '') + '" data-key="' + esc(k) + '">'
      + '<span class="d" title="' + esc(k) + '">' + esc(k) + '</span>'
      + '<span class="w">' + wins + '</span></div>';
  }).join('') || '<div class="no-data">无匹配域名</div>';
  if (!listEl._bound) {
    // 事件委托 + data-key:域名可能含引号/尖括号,不能把值拼进 onclick。
    listEl.addEventListener('click', ev => {
      const row = ev.target.closest('.domain-row');
      if (row && row.dataset.key) selectDomain(row.dataset.key);
    });
    listEl._bound = true;
  }
  document.getElementById('domain-pager').innerHTML =
      '<button onclick="domainPage(-1)"' + (dpage <= 0 ? ' disabled' : '') + '>上一页</button>'
    + '<span>' + (dpage + 1) + ' / ' + pages + ' · ' + all.length + ' 个域名</span>'
    + '<button onclick="domainPage(1)"' + (dpage >= pages - 1 ? ' disabled' : '') + '>下一页</button>';
}

function domainPage(d) { dpage = Math.max(0, dpage + d); renderDomainList(); }

function onDomainSearch() {
  domainFilter = document.getElementById('domain-search').value || '';
  dpage = 0;
  renderDomainList();
}

function selectDomain(dom) {
  if (!dom) return;
  metricsDomain = dom;
  window._metricFetchT.reset();   // 立即拉新域名指标,不等 30s 缓存
  renderDomainList();
  renderMetrics();
}

function setMetricsSub(s) {
  metricsSub = s;
  document.getElementById('metricsub-global').classList.toggle('active', s === 'global');
  document.getElementById('metricsub-domain').classList.toggle('active', s === 'domain');
  renderMetrics();
}

// ── P0:健康条 / 上游节点表 / 系统运行 ──────────────────────────
// 口径:成功率与失败来自窗口 outcome(0/1),不含竞速取消(CancelledError
// 由调用方跳过,不进样本);探活 skip 单列,不进成功率分母。
function msPair(pr) {
  if (!pr || pr.p50 == null) return '—';
  return Math.round(pr.p50 * 1000) + '/' + Math.round((pr.p95 != null ? pr.p95 : pr.p50) * 1000) + 'ms';
}

function ago(ts) {
  if (!ts) return '—';
  const d = Math.max(0, Date.now() / 1000 - ts);
  if (d < 90) return Math.round(d) + 's前';
  if (d < 5400) return Math.round(d / 60) + 'm前';
  return (d / 3600).toFixed(1) + 'h前';
}

function probeCell(pid) {
  const p = (((circ.proxies || {})[pid] || {}).probe) || null;
  if (!p) return '<span class="metric-cell-muted">—</span>';
  if (p.ok === true) {
    const lat = p.latency_ms != null ? Math.round(p.latency_ms) + 'ms' : 'ok';
    return '<span class="ok-text">✓ ' + lat + '</span> <span class="metric-cell-muted">' + ago(p.at) + '</span>';
  }
  if (p.ok === false) {
    const why = p.stage === 'connect' ? '连不上' : '握手失败';
    return '<span class="bad-text">✗ ' + why + '</span> <span class="metric-cell-muted">' + ago(p.at) + '</span>';
  }
  return '<span class="warn-text">跳过</span> <span class="metric-cell-muted">' + ago(p.at) + '</span>';
}

function nodeState(pid) {
  const s = (circ.proxies || {})[pid] || {};
  const m = qmeta[pid] || {};
  let note = '';
  const lf = s.last_failure;
  if (lf && lf.at) note = '最近失败 ' + ago(lf.at) + ' ' + (lf.err || '');
  if (s.open) {
    const rem = s.backoff_remaining_sec != null
      ? Math.max(0, Math.ceil(s.backoff_remaining_sec)) + 's'
      : Math.round(s.backoff || 0) + 's';
    return {cls:'bad', label:'熔断', note:'剩余 ' + rem + ' · 连续失败 ' + (s.consec_fail || 0)
            + (note ? ' · ' + note : '')};
  }
  if ((s.consec_fail || 0) > 0) return {cls:'warn', label:'抖动', note:'连续失败 ' + s.consec_fail
            + (note ? ' · ' + note : '')};
  const wr = m.window_success_rate, n = m.window_total || 0;
  if (wr != null && n >= 20 && wr < 0.9) return {cls:'warn', label:'质量差', note:'近窗成功率 ' + fmtPct(wr)};
  return {cls:'ok', label:'在线', note:note};
}

function renderHealth() {
  const el = document.getElementById('health');
  const enabled = enabledIds.length ? enabledIds : Object.keys(qmeta).filter(p => p !== 'local');
  const open = enabled.filter(p => ((circ.proxies || {})[p] || {}).open);
  const okN = circ.probes_ok || 0, failN = circ.probes_failed || 0, skipN = circ.probes_skipped || 0;
  const prb = (okN + failN) ? okN / (okN + failN) : null;
  let wf = 0, wt = 0;
  for (const pid of Object.keys(qmeta)) {
    const m = qmeta[pid] || {};
    wt += m.window_total || 0;
    wf += (m.window_total || 0) - (m.window_success_count || 0);
  }
  const biz = wt ? wf / wt : null;
  let cls = 'ok';
  if (open.length > 1 || (prb != null && prb < 0.9)) cls = 'bad';
  else if (open.length === 1 || (prb != null && prb < 0.99) || (biz != null && biz > 0.02)) cls = 'warn';
  const title = cls === 'bad' ? '运行异常' : (cls === 'warn' ? '部分降级' : '运行正常');
  const sub = enabled.length + ' 台上游 · ' + (open.length ? open.length + ' 台熔断（' + open.map(esc).join('、') + '）' : '全部可用')
            + ' · 数据截止 ' + new Date().toLocaleTimeString();
  const kpi = (k, v, c) => '<div class="kpi ' + (c || '') + '"><div class="k">' + k + '</div><div class="v">' + v + '</div></div>';
  const availCls = open.length === 0 ? 'ok' : (open.length <= 1 ? 'warn' : 'bad');
  const prbCls = prb == null ? '' : (prb >= 0.99 ? 'ok' : (prb >= 0.95 ? 'warn' : 'bad'));
  const bizCls = biz == null ? '' : (biz <= 0.02 ? 'ok' : (biz <= 0.1 ? 'warn' : 'bad'));
  el.innerHTML = '<div class="status-panel ' + cls + '">'
    + '<div class="status-main"><span class="status-dot"></span><div>'
      + '<div class="status-text">' + title + '</div>'
      + '<div class="status-sub">' + sub + '</div></div></div>'
    + '<div class="kpi-grid">'
      + kpi('可用上游', (enabled.length - open.length) + ' / ' + enabled.length, availCls)
      + kpi('熔断中', open.length, availCls === 'ok' ? 'ok' : 'bad')
      + kpi('探活成功率', fmtPct(prb), prbCls)
      + kpi('近窗业务失败', wf + ' / ' + wt, bizCls)
      + kpi('探活跳过', skipN, skipN ? '' : 'ok')
      + kpi('WS 握手', (counters.ws_upgrades || 0) + (counters.ws_upgrade_failed ? ' / 失败 ' + counters.ws_upgrade_failed : ''), '')
    + '</div></div>';
}

function sysRow(k, v) { return '<div class="row"><span>' + k + '</span><b>' + v + '</b></div>'; }

function renderSystems() {
  const el = document.getElementById('systems');
  const c = counters || {};
  const lag = sysStats.event_loop_lag_ms || {};
  const ratio = (a, b) => (a != null && b) ? fmtPct(a / b) : '—';
  const meter = (p, goodHigh) => {
    if (p == null) return '';
    const cls = goodHigh ? (p >= 0.7 ? 'ok' : (p >= 0.5 ? 'warn' : 'bad'))
                         : (p < 0.5 ? 'ok' : (p < 0.7 ? 'warn' : 'bad'));
    return '<div class="meter"><i class="' + cls + '" style="width:' + Math.round(Math.min(1, Math.max(0, p)) * 100) + '%"></i></div>';
  };
  const hitP = (c.conn_pool_hits != null && (c.conn_pool_hits + c.conn_pool_misses))
    ? c.conn_pool_hits / (c.conn_pool_hits + c.conn_pool_misses) : null;
  const expP = (c.conn_pool_expired != null && c.conn_pool_creates)
    ? c.conn_pool_expired / c.conn_pool_creates : null;
  let winN = 0, atN = 0;
  for (const pid of Object.keys(counts)) winN += counts[pid] || 0;
  for (const pid of Object.keys(attempts)) atN += attempts[pid] || 0;
  const now = Date.now();
  let rps = null;
  if (_prevWin != null && now > _prevWinAt) {
    const dt = (now - _prevWinAt) / 1000;
    if (dt >= 1) rps = Math.max(0, (winN - _prevWin) / dt);
  }
  _prevWin = winN; _prevWinAt = now;
  const fanout = (winN && atN) ? (atN / winN).toFixed(2) + '×' : '—';
  el.innerHTML = '<div class="sys-grid">'
    + '<div class="sys-card"><h4>探活（累计）</h4>'
      + sysRow('已发 / 成功', (circ.probes_sent || 0) + ' / ' + (circ.probes_ok || 0))
      + sysRow('失败 / 跳过', (circ.probes_failed || 0) + ' / ' + (circ.probes_skipped || 0))
      + sysRow('熔断开启次数', c.circuit_open_count != null ? c.circuit_open_count : '—') + '</div>'
    + '<div class="sys-card"><h4>目标侧</h4>'
      + sysRow('目标级失败', circ.failures_target_level != null ? circ.failures_target_level : '—')
      + sysRow('归因域名', circ.target_attribution_domains != null ? circ.target_attribution_domains : '—')
      + sysRow('死目标 / 快速失败', Object.keys(circ.dead_targets || {}).length + ' / ' + (circ.dead_target_fastfails || 0)) + '</div>'
    + '<div class="sys-card"><h4>连接池</h4>'
      + sysRow('命中率', ratio(c.conn_pool_hits, (c.conn_pool_hits || 0) + (c.conn_pool_misses || 0))) + meter(hitP, true)
      + sysRow('过期率', ratio(c.conn_pool_expired, c.conn_pool_creates)) + meter(expP, false)
      + sysRow('steal / stale 丢弃', (c.conn_pool_steals != null ? c.conn_pool_steals : '—') + ' / ' + (c.pool_stale_discards != null ? c.pool_stale_discards : '—'))
      + sysRow('池大小（连接 / 目标）', (c.conn_pool_size != null ? c.conn_pool_size : '—') + ' / ' + (c.target_pool_size != null ? c.target_pool_size : '—')) + '</div>'
    + '<div class="sys-card"><h4>通道与降级</h4>'
      + sysRow('WS 升级 / 失败 / 拒绝', (c.ws_upgrades || 0) + ' / ' + (c.ws_upgrade_failed || 0) + ' / ' + (c.ws_upgrade_rejected || 0))
      + sysRow('本机直连 成功 / 失败', (c.local_direct_hits != null ? c.local_direct_hits : '—') + ' / ' + (c.local_direct_failures != null ? c.local_direct_failures : '—'))
      + sysRow('单发降级（累计）', c.single_send_degrades != null ? c.single_send_degrades : '—') + '</div>'
    + '<div class="sys-card"><h4>流量</h4>'
      + sysRow('获胜请求 / 尝试', winN + ' / ' + atN)
      + sysRow('扇出比', fanout)
      + sysRow('获胜速率', rps == null ? '—' : rps.toFixed(1) + ' req/s') + '</div>'
    + '<div class="sys-card"><h4>进程资源</h4>'
      + sysRow('RSS / FD / 线程', (sysStats.rss_mb != null ? sysStats.rss_mb + ' MB' : '—') + ' / '
              + (sysStats.fds != null ? sysStats.fds : '—') + ' / '
              + (sysStats.threads != null ? sysStats.threads : '—'))
      + sysRow('事件循环 p50 / p95', lag.p50 != null ? lag.p50 + ' / ' + lag.p95 + ' ms' : '采样中')
      + sysRow('loop 最大延迟', lag.max != null ? lag.max + ' ms' : '—') + '</div>'
    + '</div>';
}

function renderTrend() {
  const el = document.getElementById('trend');
  if (!el) return;
  const bs = trendData.buckets || [];
  const iv = trendData.interval_sec || 60;
  if (bs.length < 2) {
    el.innerHTML = '<div class="sys-card trend-card"><h4>近 3 小时趋势</h4>'
      + '<div class="trend-empty">数据积累中 · 每 ' + iv + 's 一个点</div></div>';
    return;
  }
  el.innerHTML = '<div class="sys-card trend-card"><h4>近 3 小时趋势'
    + '<span class="trend-legend"><i class="lg ok"></i>探活成功率 <i class="lg bad"></i>业务失败率 <i class="lg accent"></i>熔断数</span></h4>'
    + '<canvas id="trend-canvas" height="150"></canvas></div>';
  const cv = document.getElementById('trend-canvas');
  if (!cv || !cv.getContext) return;   // 无 canvas 环境(如无头测试)时只留图例
  const dpr = window.devicePixelRatio || 1;
  const w = cv.clientWidth || 900, h = 150;
  cv.width = Math.round(w * dpr);
  cv.height = Math.round(h * dpr);
  const ctx = cv.getContext('2d');
  if (!ctx) return;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, w, h);
  ctx.strokeStyle = '#1d2735';
  ctx.lineWidth = 1;
  for (const p of [0, 0.5, 1]) {
    const gy = 10 + (1 - p) * (h - 30);
    ctx.beginPath(); ctx.moveTo(10, gy); ctx.lineTo(w - 10, gy); ctx.stroke();
  }
  const n = bs.length;
  const X = i => 10 + i * (w - 20) / (n - 1);
  const Y = v => 10 + (1 - Math.max(0, Math.min(1, v))) * (h - 30);
  const line = (vals, color) => {
    ctx.strokeStyle = color; ctx.lineWidth = 1.6; ctx.beginPath();
    let started = false;
    for (let i = 0; i < n; i++) {
      const v = vals[i];
      if (v == null) { started = false; continue; }
      if (!started) { ctx.moveTo(X(i), Y(v)); started = true; }
      else ctx.lineTo(X(i), Y(v));
    }
    ctx.stroke();
  };
  const probe = bs.map(b => (b.probe_ok + b.probe_fail) ? b.probe_ok / (b.probe_ok + b.probe_fail) : null);
  const bizFail = bs.map(b => (b.biz_ok + b.biz_fail) ? b.biz_fail / (b.biz_ok + b.biz_fail) : null);
  const denom = Math.max(1, enabledIds.length || 1);
  const circ = bs.map(b => (b.circuit_open_now || 0) / denom);
  line(probe, '#3fb950');
  line(bizFail, '#f85149');
  line(circ, '#4c9aff');
  ctx.fillStyle = '#6b7787';
  ctx.font = '10px system-ui';
  ctx.fillText('100%', 12, 9);
  ctx.fillText('0', 12, h - 12);
  ctx.textAlign = 'right';
  ctx.fillText(new Date(bs[0].ts * 1000).toLocaleTimeString(), w - 10, h - 4);
  ctx.fillText('now', w - 10, 9);
  ctx.textAlign = 'left';
}

function renderMetricsGlobal(wrap) {
  const pids = Object.keys(qmeta);
  if (!pids.length) { wrap.innerHTML = '<div class="no-data">No data</div>'; document.getElementById('footer').textContent=''; return; }
  page = 0;
  renderHealth();
  renderSystems();
  renderTrend();
  const order = {bad: 0, warn: 1, ok: 2};
  const rows = pids.map(pid => ({pid: pid, st: nodeState(pid)}))
    .sort((a, b) => (order[a.st.cls] - order[b.st.cls]) || a.pid.localeCompare(b.pid));
  let t = '<table class="metrics-table"><thead><tr><th>状态</th><th>代理</th><th>探活</th><th>握手 P50/P95</th><th>首字节 P50/P95</th><th>成功率（近）</th><th>成功/失败（近）</th><th>在途</th><th>累计请求</th><th>累计字节</th></tr></thead><tbody>';
  for (const r of rows) {
    const pid = r.pid, st = r.st;
    const s = (circ.proxies || {})[pid] || {};
    const m = qmeta[pid] || {};
    const c = m.cumulative || {};
    const tot = m.window_total || 0, succ = m.window_success_count || 0;
    t += '<tr class="' + (st.cls === 'bad' ? 'node-bad' : '') + '">';
    t += '<td><span class="pill ' + st.cls + '"><span class="dot ' + st.cls + '"></span>' + st.label + '</span>'
       + (st.note ? '<span class="node-note">' + st.note + '</span>' : '') + '</td>';
    t += '<td class="default-proxy">' + esc(pid) + '</td>';
    t += '<td class="metric-cell-num">' + probeCell(pid) + '</td>';
    t += '<td class="metric-cell-num">' + msPair(m.ttfb) + '</td>';
    t += '<td class="metric-cell-num">' + msPair(m.ofb) + '</td>';
    t += '<td class="metric-cell-num" title="' + errStr(m.errors) + '">' + fmtPct(m.window_success_rate) + '</td>';
    t += '<td class="metric-cell-num">' + succ + '/' + (tot - succ) + '</td>';
    t += '<td class="metric-cell-num">' + (s.in_flight != null ? s.in_flight : '—') + '</td>';
    t += '<td class="metric-cell-num" title="' + protoStr(m.http_versions) + '">' + (c.samples != null ? c.samples : (m.total_attempts || 0)) + '</td>';
    t += '<td class="metric-cell-num">' + fmtBytes(c.total_bytes || m.total_bytes_transferred || 0) + '</td></tr>';
  }
  t += '</tbody></table>';
  wrap.innerHTML = t;
  document.getElementById('footer').textContent =
    pids.length + ' 个代理 · 窗口=近 256 样本（路由决策口径），累计=跨重启 · 失败不含竞速取消 · 数据截止 ' +
    new Date().toLocaleTimeString();
}

function renderMetricsDomain(wrap) {
  const per = perDest || {};
  const pids = Object.keys(per).sort((a, b) => (per[b].total || 0) - (per[a].total || 0));
  const meta = domainMeta[metricsDomain] || {};
  const wins = Object.values(domainData[metricsDomain] || {}).reduce((a, b) => a + b, 0);
  const head = '<div class="domain-head"><b>' + esc(metricsDomain) + '</b>'
    + '<span>默认代理 ' + esc(meta.default_proxy || '—') + '</span>'
    + '<span>胜出 ' + wins + '</span>'
    + '<span>更新 ' + (meta.updated_at ? toBeijing(meta.updated_at) : '—') + '</span>'
    + '<span>' + pids.length + ' 个上游有样本</span></div>';
  if (!pids.length) {
    wrap.innerHTML = head + '<div class="no-data">该域名暂无代理指标</div>';
    document.getElementById('footer').textContent = '';
    return;
  }
  let win = '<table class="metrics-table"><thead><tr><th>代理</th><th>握手 P50/P95/P99</th><th>源站首字节 P50/P95/P99</th><th>成功率(近)</th><th>成功/失败(近)</th><th>错误分类(近 256)</th></tr></thead><tbody>';
  let cum = '<table class="metrics-table"><thead><tr><th>代理</th><th>握手 均值</th><th>源站首字节 均值</th><th>成功率</th><th>总请求</th><th>累计字节</th><th>协议(累计)</th></tr></thead><tbody>';
  for (const pid of pids) {
    const m = per[pid] || {};
    const pt = m.percentiles || {};
    const c = m.cumulative || {};
    const tot = m.window_total || 0, succ = m.window_success_count || 0;
    const rowCls = pid === meta.default_proxy ? ' class="default-row"' : '';
    win += `<tr${rowCls}><td class="default-proxy">${esc(pid)}</td>`;
    win += `<td class="metric-cell-num">${pctCell(pt.ttfb)}</td>`;
    win += `<td class="metric-cell-num">${pctCell(pt.ofb)}</td>`;
    win += `<td class="metric-cell-num">${fmtPct(m.window_success_rate)}</td>`;
    win += `<td class="metric-cell-num">${succ}/${tot - succ}</td>`;
    win += `<td class="metric-cell-num err-cell">${errStr(m.errors)}</td></tr>`;
    cum += `<tr${rowCls}><td class="default-proxy">${esc(pid)}</td>`;
    cum += `<td class="metric-cell-num">${c.avg_ttfb_ms != null ? Math.round(c.avg_ttfb_ms) + 'ms' : '—'}</td>`;
    cum += `<td class="metric-cell-num">${c.avg_ofb_ms != null ? Math.round(c.avg_ofb_ms) + 'ms' : '—'}</td>`;
    cum += `<td class="metric-cell-num">${fmtPct(c.success_rate)}</td>`;
    cum += `<td class="metric-cell-num">${c.samples || 0}</td>`;
    cum += `<td class="metric-cell-num">${fmtBytes(c.total_bytes || 0)}</td>`;
    cum += `<td class="metric-cell-num">${protoStr(m.http_versions)}</td></tr>`;
    const lp = lifePctLine(c);
    if (lp) cum += `<tr class="cum-row"><td colspan="7">${lp}</td></tr>`;
  }
  win += '</tbody></table>';
  cum += '</tbody></table>';
  wrap.innerHTML = head
    + '<div class="metrics-split">'
    + '<div class="metrics-pane"><h3 class="pane-title">窗口(近期 _OBS_WINDOW=256 · 用于路由决策)</h3>' + win + '</div>'
    + '<div class="metrics-pane"><h3 class="pane-title">累计(永久值,跨重启 · 历史可追溯)</h3>' + cum + '</div>'
    + '</div>';
  document.getElementById('footer').textContent = pids.length + ' 个上游 · 域名: ' + metricsDomain
    + ' · 蓝底行为该域名当前默认代理 · 数据截止 ' + new Date().toLocaleTimeString();
}

fetchMetrics();
onIntervalChange(document.getElementById('interval'), doRefresh);
"""
    return _PANEL_SHELL.format(
        title="监控指标",
        class_attr=' class="panel-dark"',
        css=_PANEL_CSS,
        nav=_panel_nav("metrics"),
        toolbar=_panel_toolbar("", show_filter=False),
        body=body,
        script=f"<script>{script}</script>",
    )


# 面板视图注册表(键 = URL 路径段;html 构建惰性执行一次)
_PANEL_VIEWS = {
    "domains": _panel_domains_html(),
    "stickiness": _panel_stickiness_html(),
    "metrics": _panel_metrics_html(),
}


@app.get("/panel/{view}")
async def panel(view: str):
    """渲染管理面板的某一独立视图页(域名统计/会话粘性/监控指标)。

    view ∈ {domains, stickiness, metrics}。三页各自独立 URL,共享
    _PANEL_CSS 样式 与 /static/panel.js(格式化/刷新基础设施)。未知
    view 返回 404。数据由页面内 JS 再 fetch,此路由只出 HTML 骨架。
    """
    html = _PANEL_VIEWS.get(view)
    if html is None:
        raise HTTPException(status_code=404, detail="unknown panel view")
    return HTMLResponse(html)


@app.get("/static/panel.js")
async def panel_js():
    """共享面板 JS(格式化工具 + 刷新基础设施)。受 api_auth_middleware
    保护(除 /health 外全需 Basic 认证)。纯内存常量化,不落盘。
    """
    return Response(content=_PANEL_JS, media_type="application/javascript")


@app.get("/")
async def root_redirect():
    """根路径 307 重定向到域名统计面板页(兼容旧书签:原本 / 即域名统计)。"""
    return RedirectResponse(url="/panel/domains", status_code=307)
def mount(proxy_store: ProxyStore, router: Router | None = None,
          api_auth: AuthConfig | None = None):
    global _proxy_store, _router, _api_auth
    _proxy_store = proxy_store
    _router = router
    # api_auth 为 None 或 AuthConfig(enabled 默认 False)→ 认证关闭。
    _api_auth = api_auth
