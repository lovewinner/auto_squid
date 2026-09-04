"""Phase 1.4 / 3 / 4 / 6 的新增指标逻辑测试。

覆盖:
  - t-digest 摘要(digest.py):质心硬上限、分位单调、JSON 往返、精度容忍度
  - Phase 1.4 协议版本采集:双作用域计数、旧行兼容
  - Phase 3 单发降级门控:成功率 / P99 / 吞吐 三信号与默认关闭
  - Phase 4 探测对齐:轻量 GET 探活的限速率、目标轮转、指标记录、默认关闭
"""

import asyncio
import json
from unittest import mock

import httpx
import pytest

from auto_squid.config_schema import CircuitConfig, RouterConfig
from auto_squid.digest import TDigest
from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.selector import ProxySelector


# ── t-digest ────────────────────────────────────────────
def test_digest_centroid_count_is_bounded():
    """质心数必须恒 <= MAX_CENTROIDS(内存 O(1) 的保证)。"""
    d = TDigest()
    for i in range(50000):
        d.add(float(i % 1000) + (i % 7) * 0.001)
    d._compress()
    assert len(d["c"]) <= TDigest.MAX_CENTROIDS


def test_digest_percentiles_monotonic_and_bounded():
    """p50 <= p95 <= p99,且都落在 [min, max] 内。"""
    d = TDigest()
    for i in range(20000):
        d.add(float(i % 500) / 7.0)
    pc = d.percentiles()
    assert pc["p50"] <= pc["p95"] <= pc["p99"]
    assert pc["min"] <= pc["p50"] and pc["p99"] <= pc["max"]
    assert pc["samples"] == 20000


def test_digest_survives_json_roundtrip():
    """DB 落盘(json.dumps)后恢复为普通 dict,重新包装后分位数不变。"""
    d = TDigest()
    for i in range(1000):
        d.add(float(i) / 3.0)
    before = d.percentiles()
    restored = TDigest(json.loads(json.dumps(d)))
    assert restored.percentiles() == before


def test_digest_accuracy_within_tolerance():
    """长尾分布下 p50/p95 误差应在 5% 内(t-digest 是近似算法)。"""
    import random

    random.seed(11)
    vals = [random.lognormvariate(-2.0, 1.0) for _ in range(20000)]
    d = TDigest()
    for v in vals:
        d.add(v)
    pc = d.percentiles()

    def exact(p):
        s = sorted(vals)
        k = (len(s) - 1) * (p / 100.0)
        lo = int(k)
        hi = min(lo + 1, len(s) - 1)
        return s[lo] * (1 - (k - lo)) + s[hi] * (k - lo)

    for p in (50, 95):
        e = exact(p)
        assert abs(pc[f"p{p}"] - e) / e < 0.05, f"p{p} 误差过大"


# ── Phase 1.4 协议版本 ──────────────────────────────────
def _selector():
    store = ProxyStore.__new__(ProxyStore)
    return ProxySelector(store)


def test_record_protocol_dual_scope():
    """域名桶与全局桶双写,且 domain=None 时不重复计数。"""
    sel = _selector()
    sel.record_protocol("p1", "HTTP/2", domain="gh:443")
    sel.record_protocol("p1", "HTTP/2")
    sel.record_protocol("p1", "HTTP/1.1")
    assert sel._proxy_metrics["p1"]["metrics"]["http_versions"] == {
        "HTTP/2": 2, "HTTP/1.1": 1}
    assert sel._domain_metrics["gh:443"]["p1"]["metrics"]["http_versions"] == {
        "HTTP/2": 1}


def test_protocol_backfilled_for_old_db_rows():
    """旧 DB 行没有 http_versions 时惰性补全,热路径不 KeyError。"""
    sel = _selector()
    old = {"p9": {"ttfb_samples": [], "throughput_ewma": None, "success": 1,
                  "total": 1, "errors": {}, "total_bytes": 0.0,
                  "transfer_time": 0.0}}
    sel.set_proxy_metrics(old)
    sel.record_protocol("p9", "HTTP/2")  # 不应抛异常
    assert sel._proxy_metrics["p9"]["metrics"]["http_versions"] == {"HTTP/2": 1}


# ── Phase 3 单发降级门控 ────────────────────────────────
def _degrade_router(sel, **kw):
    r = Router.__new__(Router)
    r.selector = sel
    r.single_send_degrades = 0
    r.single_send_degrade_fail = 0
    r.single_send_degrade_ratio = 0.0
    r.single_send_degrade_slack_ms = 0.0
    r.single_send_degrade_success_rate = kw.get("success_rate", 0.0)
    r.single_send_degrade_p99_ms = kw.get("p99_ms", 0.0)
    r.single_send_degrade_min_throughput = kw.get("min_throughput", 0.0)
    return r


def _seed(sel, total, success, ttfb=(), ofb=(), throughput=None):
    scopes = (sel._metrics_for("p1", "gh:443"), sel._metrics_for("p1", None))
    for sc in scopes:
        sc["total"] = total
        sc["success"] = success
        sc["ttfb_samples"] = list(ttfb)
        sc["ofb_samples"] = list(ofb)
        sc["throughput_ewma"] = throughput


def test_degrade_by_low_success_rate():
    sel = _selector()
    _seed(sel, 10, 7)  # 0.7 < 0.95
    r = _degrade_router(sel, success_rate=0.95)
    assert r._single_send_degraded("gh:443", "p1", None) is True


def test_degrade_ignored_when_samples_insufficient():
    """样本 <8 时不按成功率降级,避免偶发失败被误判。"""
    sel = _selector()
    _seed(sel, 6, 3)
    r = _degrade_router(sel, success_rate=0.95)
    assert r._single_send_degraded("gh:443", "p1", None) is False


def test_degrade_by_p99_latency():
    sel = _selector()
    _seed(sel, 10, 10, ttfb=[0.1] * 9 + [2.1])  # p99 ≈ 2.1s,阈 500ms
    r = _degrade_router(sel, p99_ms=500.0)
    assert r._single_send_degraded("gh:443", "p1", None) is True


def test_degrade_by_low_throughput():
    sel = _selector()
    _seed(sel, 10, 10, throughput=0.5)  # 0.5 < 2.0 MB/s
    r = _degrade_router(sel, min_throughput=2.0)
    assert r._single_send_degraded("gh:443", "p1", None) is True


def test_degrade_defaults_off_is_backward_compatible():
    """阈值全为 0(默认)时,即便指标很差也不降级——零行为变化。"""
    sel = _selector()
    _seed(sel, 10, 7, ttfb=[0.1] * 9 + [2.1], throughput=0.5)
    r = _degrade_router(sel)
    assert r._single_send_degraded("gh:443", "p1", None) is False


def test_degrade_thresholds_reachable_from_config():
    """新阈值挂在 CircuitConfig 上,router 经 router_cfg.circuit 读取。"""
    cc = CircuitConfig(single_send_degrade_success_rate=0.9,
                       single_send_degrade_p99_ms=800.0,
                       single_send_degrade_min_throughput=1.5)
    assert RouterConfig().circuit.single_send_degrade_success_rate == 0.0
    assert cc.single_send_degrade_p99_ms == 800.0
    assert cc.single_send_degrade_min_throughput == 1.5


# ── Phase 4 探测对齐 ────────────────────────────────────
class _FakeResp:
    status_code = 200
    http_version = "HTTP/2"

    async def aiter_bytes(self):
        for _ in range(4):
            await asyncio.sleep(0.001)  # 保证 body_dur > 0,使 record_complete 必然触发
            yield b"x" * 1024

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


class _FakeClient:
    def __init__(self, **kw):
        self.kw = kw

    def stream(self, method, url):
        return _FakeResp()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False


def _probe_router(sel, **kw):
    r = Router.__new__(Router)
    r.proxy_store = ProxyStore.__new__(ProxyStore)
    r.selector = sel
    r.probe_with_get = kw.get("enabled", True)
    r.probe_get_targets = kw.get("targets", ["https://api.github.com/"])
    r.probe_get_interval_sec = kw.get("interval", 60.0)
    r.probe_get_timeout_sec = 5.0
    r.probe_get_max_bytes = 65536
    r._probe_get_last = {}
    r._probe_get_rr = 0
    r.probe_get_sent = r.probe_get_ok = 0
    r.probe_get_failed = r.probe_get_throttled = 0
    return r


@pytest.mark.asyncio
async def test_probe_get_records_business_metrics():
    """GET 探活按业务口径记录 TTFB/协议/吞吐,域名键为 host:port。"""
    sel = _selector()
    r = _probe_router(sel)
    seen = []
    for name in ("record_ttfb", "record_protocol", "record_complete"):
        orig = getattr(sel, name)

        def wrap(n=name, o=orig):
            def f(*a, **k):
                seen.append((n, k.get("domain")))
                return o(*a, **k)
            return f
        setattr(sel, name, wrap())

    proxy = mock.Mock(id="p1")
    with mock.patch.object(httpx, "AsyncClient", _FakeClient), \
            mock.patch.object(r.proxy_store, "proxy_url", lambda pid: "http://127.0.0.1:1"):
        await r._maybe_probe_get(proxy)

    assert r.probe_get_ok == 1
    assert seen == [("record_ttfb", "api.github.com:443"),
                    ("record_protocol", "api.github.com:443"),
                    ("record_complete", "api.github.com:443")]


@pytest.mark.asyncio
async def test_probe_get_is_rate_limited_per_proxy_target():
    """同一(代理,目标)在间隔内重复探活应被限流,不重复记账。"""
    sel = _selector()
    r = _probe_router(sel)
    proxy = mock.Mock(id="p1")
    with mock.patch.object(httpx, "AsyncClient", _FakeClient), \
            mock.patch.object(r.proxy_store, "proxy_url", lambda pid: "http://127.0.0.1:1"):
        await r._maybe_probe_get(proxy)
        await r._maybe_probe_get(proxy)
    assert r.probe_get_ok == 1
    assert r.probe_get_throttled == 1


@pytest.mark.asyncio
async def test_probe_get_rotates_targets():
    """多目标时每轮只测一个并轮转,避免一次性放大流量。"""
    sel = _selector()
    r = _probe_router(sel, targets=["https://a.example/", "https://b.example/"],
                      interval=0.0)
    proxy = mock.Mock(id="p1")
    with mock.patch.object(httpx, "AsyncClient", _FakeClient), \
            mock.patch.object(r.proxy_store, "proxy_url", lambda pid: "http://127.0.0.1:1"):
        await r._maybe_probe_get(proxy)
        await r._maybe_probe_get(proxy)
    assert r.probe_get_ok == 2
    assert set(sel._domain_metrics) == {"a.example:443", "b.example:443"}


@pytest.mark.asyncio
async def test_probe_get_disabled_by_default():
    """默认关闭时完全无副作用。"""
    sel = _selector()
    r = _probe_router(sel, enabled=False)
    with mock.patch.object(httpx, "AsyncClient", _FakeClient), \
            mock.patch.object(r.proxy_store, "proxy_url", lambda pid: "http://127.0.0.1:1"):
        await r._maybe_probe_get(mock.Mock(id="p1"))
    assert r.probe_get_sent == 0
    assert sel._domain_metrics == {}
