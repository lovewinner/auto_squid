from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
from typing import Dict, List

from .auth import check_auth
from .config_schema import AuthConfig, ProxyInfo
from .proxy_store import ProxyStore
from .router import Router

app = FastAPI(title="auto_squid API")

# 由 CLI 在启动时注入
_proxy_store: ProxyStore | None = None
_router: Router | None = None
# 管理 API 的 HTTP Basic 认证配置(默认 None = 关闭)。由 mount() 注入。
_api_auth: AuthConfig | None = None

# 由 bench 压测子进程(server_proc)周期填充:服务端 CPU 与事件循环延迟采样。
# 非压测启动(CLI 正常运行)时为空 dict,/server-stats 返回空快照。
_server_stats: dict = {}


@app.middleware("http")
async def api_auth_middleware(request: Request, call_next):
    """管理 API 的 HTTP Basic 认证(经 config api.auth 开启,默认关闭)。

    开启后除 /health(健康检查/负载均衡探活)外,全部端点(含内嵌仪表盘 /
    以及 FastAPI 自动生成的 /docs、/openapi.json)均需凭据。凭据经标准
    Authorization 头传入,复用 auth.check_auth(常量时间比较)。认证失败回
    401 + WWW-Authenticate,浏览器据此弹出原生凭据框;成功后浏览器按 origin
    缓存 Basic 凭据,页面内同源 fetch 自动附带 Authorization,仪表盘无需改动。
    """
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
    port: int = 3128
    protocol: str = "http"
    auth: Dict[str, str] | None = None
    enabled: bool = True
    tags: Dict[str, str] | None = None


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
    return {
        "circuit_open_count": _router.selector.circuit_open_count,
        "probes_sent": _router.probes_sent,
        "probes_ok": _router.probes_ok,
        "probes_skipped": _router.probes_skipped,
        "probes_failed": _router.probes_failed,
        "single_send_degrades": _router.single_send_degrades,
        "degraded_single_send": _router.get_degraded_single_send(),
        "proxies": _router.selector.get_circuit_state(),
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


@app.get("/metrics/per-destination")
async def metrics_per_destination():
    """每分钟度(域名,代理)的增强指标(Phase 1,IMPROVEMENT_PLAN.md)。

    评估"特定 URL(如 https://github.com 的 domain key github.com:443)实测速度":
    返回 {domain: {pid: {ttfb/ofb 分位数, 成功率, 错误分类, 吞吐}}}。见
    selector.get_domain_metrics()。
    """
    if not _router:
        return {}
    return _router.selector.get_domain_metrics()


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
    _router.tuner.set_enabled(req.enabled)
    return _router.tuner.snapshot()


@app.get("/domains")
async def domains():
    """返回各域名在各代理上的获胜次数"""
    if not _router:
        return {}
    return _router.get_domain_stats_from_db()


@app.get("/domains/meta")
async def domains_meta():
    """返回域名缓存元数据（当前默认代理、更新时间；自适应 TTL 开启时含
    ttl/expires_at/switch_count）。

    Phase 1 增强:每个域名追加 proxy_metrics = {pid: {ttfb/ofb 分位数, 成功率,
    错误分类, 吞吐}}——用于评估"特定 URL(如 github.com:443)在各代理上的实测
    速度差异"(握手+源站首字节双维度)。见 selector.get_domain_metrics()。
    """
    if not _router:
        return {}
    out = _router.get_domain_meta_enriched()
    per_dest = _router.selector.get_domain_metrics()
    for d, per_pid in per_dest.items():
        out.setdefault(d, {})["proxy_metrics"] = per_pid
    return out


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
</style>
</head>
<body>
<h1>auto_squid 管理面板</h1>
<div class="toolbar">
<button id="view-domains" class="view-btn active" onclick="setView('domains')">域名统计</button>
<button id="view-stickiness" class="view-btn" onclick="setView('stickiness')">会话粘性</button>
<button id="view-metrics" class="view-btn" onclick="setView('metrics')">监控指标</button>
<select id="metrics-domain" style="display:none" onchange="metricsDomain = this.value; renderMetrics();"></select>
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
<button onclick="doRefresh()">Refresh</button>
<span id="autorefresh-label" class="autorefresh-label"></span>
</div>
<div id="stats" class="stats"></div>
<div id="filter-banner" class="filter-banner"></div>
<div id="metrics-tabs" class="metrics-tabs" style="display:none">
<button id="metricsub-global" class="view-btn active" onclick="setMetricsSub('global')">全局概览</button>
<button id="metricsub-domain" class="view-btn" onclick="setMetricsSub('domain')">域名详情</button>
</div>
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
let metricsSub = 'global';
let metricsDomain = '';
let qmeta = {}, perDest = {};
let stickMetrics = false;
let sticky = [], stickyStats = {size: 0, hits: 0, evictions: 0};
let activeProxy = new URLSearchParams(location.search).get('default_proxy') || null;

function onIntervalChange(sel) {
  refreshInterval = parseInt(sel.value);
  if (refreshTimer) clearInterval(refreshTimer);
  refreshTimer = null;
  const label = document.getElementById('autorefresh-label');
  if (refreshInterval > 0) {
    label.textContent = '每 ' + sel.options[sel.selectedIndex].text + ' 自动刷新';
    refreshTimer = setInterval(() => {
      if (view === 'stickiness') renderStickiness();
      else if (view === 'metrics') fetchMetrics();
      else fetchData();
    }, refreshInterval * 1000);
  } else {
    label.textContent = '';
  }
}

function setView(v) {
  view = v;
  document.getElementById('view-domains').classList.toggle('active', v === 'domains');
  document.getElementById('view-stickiness').classList.toggle('active', v === 'stickiness');
  document.getElementById('view-metrics').classList.toggle('active', v === 'metrics');
  document.getElementById('metrics-domain').style.display = (v === 'metrics' && metricsSub === 'domain') ? '' : 'none';
  document.getElementById('metrics-tabs').style.display = v === 'metrics' ? '' : 'none';
  document.getElementById('filter').style.display = v === 'metrics' ? 'none' : '';
  document.getElementById('filter').placeholder = v === 'stickiness' ? 'Filter client|domain / proxy...' : 'Filter domains...';
  document.getElementById('stats').innerHTML = '';
  document.getElementById('filter-banner').style.display = 'none';
  if (v === 'stickiness') { renderStickiness(); return; }
  if (v === 'metrics') { fetchMetrics(); return; }
  page = 0;
  render();
  renderBanner();
  renderStats();
}

function setMetricsSub(s) {
  metricsSub = s;
  document.getElementById('metricsub-global').classList.toggle('active', s === 'global');
  document.getElementById('metricsub-domain').classList.toggle('active', s === 'domain');
  document.getElementById('metrics-domain').style.display = s === 'domain' ? '' : 'none';
  renderMetrics();
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
  if (refreshTimer) { clearInterval(refreshTimer); refreshTimer = setInterval(() => { view === 'stickiness' ? renderStickiness() : (view === 'metrics' ? fetchMetrics() : fetchData()); }, refreshInterval * 1000); }
}

function doRefresh() {
  if (view === 'stickiness') renderStickiness();
  else if (view === 'metrics') fetchMetrics();
  else fetchData();
}

const ERR_LABELS = {timeout:'超时', connect:'连接', http_5xx:'5xx', tls:'TLS', protocol:'协议', cancelled:'取消', other:'其他'};

async function fetchMetrics() {
  const [r1, r2] = await Promise.all([fetch('/quality/meta'), fetch('/metrics/per-destination')]);
  qmeta = await r1.json();
  perDest = await r2.json();
  // 数据变更时保持域名下拉同步
  const sel = document.getElementById('metrics-domain');
  const keys = Object.keys(perDest).sort();
  if (sel.options.length !== keys.length) {
    sel.innerHTML = keys.map(k => `<option value="${k.replace(/"/g,'&quot;')}">${k}</option>`).join('');
    sel.value = (metricsDomain && keys.includes(metricsDomain)) ? metricsDomain : '';
    metricsDomain = sel.value;
  }
  renderMetrics();
}

function renderMetrics() {
  const wrap = document.getElementById('table-wrap');
  document.getElementById('stats').innerHTML = '';
  document.getElementById('filter-banner').style.display = 'none';
  if (metricsSub === 'global') { renderMetricsGlobal(wrap); return; }
  renderMetricsDomain(wrap);
}

function fmtPct(p) { return p == null ? '—' : (p * 100).toFixed(1) + '%'; }
function fmtMs(v) { return v == null ? '—' : Math.round(v * 1000) + 'ms'; }
function fmtMbps(v) { return v == null ? '—' : v.toFixed(2) + ' MB/s'; }
function fmtBytes(b) {
  if (b == null) return '—';
  if (b >= 1e9) return (b/1e9).toFixed(2) + ' GB';
  if (b >= 1e6) return (b/1e6).toFixed(1) + ' MB';
  if (b >= 1e3) return (b/1e3).toFixed(1) + ' KB';
  return b + ' B';
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

// 累计明细行, 措辞与 test_routing.py --metrics 的 "累计(永久值...)" 行保持一致
// (分位数无法累计: 累计只存 sum/n, 故这里给平均时延 + 失败细分)。
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

function renderMetricsGlobal(wrap) {
  const pids = Object.keys(qmeta).sort();
  if (!pids.length) { wrap.innerHTML = '<div class="no-data">No data</div>'; document.getElementById('pager').innerHTML=''; document.getElementById('footer').textContent=''; return; }
  page = 0;
  // 两表列错开各展所长:窗口表重在 EWMA 趋势与近期计数(代理近况);
  // 累计表重在永久累计与均值(代理历史表现,跨重启可追溯)。
  let win = '<table class="metrics-table"><thead><tr><th>代理</th><th>握手 P50/P95/P99</th><th>源站首字节 P50/P95/P99</th><th>成功率(近)</th><th>成功/总数(近)</th><th>错误分类(近 256)</th></tr></thead><tbody>';
  let cum = '<table class="metrics-table"><thead><tr><th>代理</th><th>握手 均值</th><th>源站首字节 均值</th><th>吞吐 累计</th><th>成功率</th><th>成功/总数</th><th>累计字节</th><th>协议(累计)</th></tr></thead><tbody>';
  for (const pid of pids) {
    const m = qmeta[pid] || {};
    const c = m.cumulative || {};
    // 窗口行
    win += `<tr><td class="default-proxy">${pid}</td>`;
    win += `<td class="metric-cell-num">${pctCell(m.ttfb)}</td>`;
    win += `<td class="metric-cell-num">${pctCell(m.ofb)}</td>`;
    win += `<td class="metric-cell-num">${fmtPct(m.window_success_rate)}</td>`;
    win += `<td class="metric-cell-num">${m.window_success_count||0}/${m.window_total||0}</td>`;
    win += `<td class="metric-cell-num err-cell">${errStr(m.errors)}</td></tr>`;
    // 累计行(同列同代理,便于左右对比)
    cum += `<tr><td class="default-proxy">${pid}</td>`;
    cum += `<td class="metric-cell-num">${c.avg_ttfb_ms != null ? Math.round(c.avg_ttfb_ms)+'ms' : '\u2014'}</td>`;
    cum += `<td class="metric-cell-num">${c.avg_ofb_ms != null ? Math.round(c.avg_ofb_ms)+'ms' : '\u2014'}</td>`;
    cum += `<td class="metric-cell-num">${fmtMbps(c.throughput_mbps)}</td>`;
    cum += `<td class="metric-cell-num">${fmtPct(c.success_rate)}</td>`;
    cum += `<td class="metric-cell-num">${c.success||0}/${c.samples||0}</td>`;
    cum += `<td class="metric-cell-num">${fmtBytes(c.total_bytes || 0)}</td>`;
    cum += `<td class="metric-cell-num">${protoStr(m.http_versions)}</td></tr>`;
    const lp = lifePctLine(c);
    if (lp) cum += `<tr class="cum-row"><td colspan="8">${lp}</td></tr>`;
  }
  win += '</tbody></table>';
  cum += '</tbody></table>';
  let html = '<div class="metrics-split">'
           + '<div class="metrics-pane"><h3 class="pane-title">窗口(近期 _OBS_WINDOW=256 · 用于路由决策)</h3>' + win + '</div>'
           + '<div class="metrics-pane"><h3 class="pane-title">累计(永久值,跨重启 · 历史可追溯)</h3>' + cum + '</div>'
           + '</div>';
  wrap.innerHTML = html;
  document.getElementById('pager').innerHTML = '';
  document.getElementById('footer').textContent = pids.length + ' proxies \u00b7 全局每代理概览(跨域名聚合)';
}

function renderMetricsDomain(wrap) {
  if (!metricsDomain || !perDest[metricsDomain]) {
    const keys = Object.keys(perDest);
    if (!keys.length) { wrap.innerHTML = '<div class="no-data">No per-domain metrics</div>'; document.getElementById('pager').innerHTML=''; document.getElementById('footer').textContent=''; return; }
    wrap.innerHTML = '<div class="no-data">请在上方选择域名</div>';
    document.getElementById('pager').innerHTML=''; document.getElementById('footer').textContent='';
    return;
  }
  const per = perDest[metricsDomain];
  const pids = Object.keys(per).sort((a,b) => (per[b].total||0)-(per[a].total||0));
  let banner = `<div class="filter-banner" style="display:flex"><strong>${metricsDomain}</strong>&nbsp;各代理实测指标</div>`;
  // 两表列错开:窗口表 P50/P95/P99 + EWMA 吞吐(近况);累计表 均值 + 永久计数 + 累计字节(历史)。
  let win = '<table class="metrics-table"><thead><tr><th>代理</th><th>握手 P50/P95/P99</th><th>源站首字节 P50/P95/P99</th><th>成功率(近)</th><th>成功/总数(近)</th><th>错误分类(近 256)</th></tr></thead><tbody>';
  let cum = '<table class="metrics-table"><thead><tr><th>代理</th><th>握手 均值</th><th>源站首字节 均值</th><th>吞吐 累计</th><th>成功率</th><th>总请求</th><th>累计字节</th><th>协议(累计)</th></tr></thead><tbody>';
  for (const pid of pids) {
    const m = per[pid] || {};
    const per_t = m.percentiles || {};
    const total = m.total || 0;
    const rate = total ? m.success / total : null;
    const c = m.cumulative || {};
    // 窗口行
    win += `<tr><td class="default-proxy">${pid}</td>`;
    win += `<td class="metric-cell-num">${pctCell(per_t.ttfb)}</td>`;
    win += `<td class="metric-cell-num">${pctCell(per_t.ofb)}</td>`;
    win += `<td class="metric-cell-num">${fmtPct(m.window_success_rate)}</td>`;
    win += `<td class="metric-cell-num">${m.window_success_count||0}/${m.window_total||0}</td>`;
    win += `<td class="metric-cell-num err-cell">${errStr(m.errors)}</td></tr>`;
    // 累计行(同列同代理,便于左右对比)
    cum += `<tr><td class="default-proxy">${pid}</td>`;
    cum += `<td class="metric-cell-num">${c.avg_ttfb_ms != null ? Math.round(c.avg_ttfb_ms)+'ms' : '\u2014'}</td>`;
    cum += `<td class="metric-cell-num">${c.avg_ofb_ms != null ? Math.round(c.avg_ofb_ms)+'ms' : '\u2014'}</td>`;
    cum += `<td class="metric-cell-num">${fmtMbps(c.throughput_mbps)}</td>`;
    cum += `<td class="metric-cell-num">${fmtPct(c.success_rate)}</td>`;
    cum += `<td class="metric-cell-num">${c.samples||0}</td>`;
    cum += `<td class="metric-cell-num">${fmtBytes(c.total_bytes || 0)}</td>`;
    cum += `<td class="metric-cell-num">${protoStr(m.http_versions)}</td></tr>`;
    const lp = lifePctLine(c);
    if (lp) cum += `<tr class="cum-row"><td colspan="8">${lp}</td></tr>`;
  }
  win += '</tbody></table>';
  cum += '</tbody></table>';
  let html = banner
           + '<div class="metrics-split">'
           + '<div class="metrics-pane"><h3 class="pane-title">窗口(近期 _OBS_WINDOW=256 · 用于路由决策)</h3>' + win + '</div>'
           + '<div class="metrics-pane"><h3 class="pane-title">累计(永久值,跨重启 · 历史可追溯)</h3>' + cum + '</div>'
           + '</div>';
  wrap.innerHTML = html;
  document.getElementById('pager').innerHTML = '';
  document.getElementById('footer').textContent = pids.length + ' proxies \u00b7 域名: ' + metricsDomain;
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

def mount(proxy_store: ProxyStore, router: Router | None = None,
          api_auth: AuthConfig | None = None):
    global _proxy_store, _router, _api_auth
    _proxy_store = proxy_store
    _router = router
    # api_auth 为 None 或 AuthConfig(enabled 默认 False)→ 认证关闭。
    _api_auth = api_auth
