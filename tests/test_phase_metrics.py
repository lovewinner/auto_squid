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
from auto_squid.proxy_store import ProxyInfo, ProxyStore
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


# ── 双作用域双写:domain=None 不得重复计数 ──────────────
def test_metrics_not_double_counted_when_domain_none():
    """domain=None 时两个作用域是同一全局 dict,必须只写一次(既有 bug 回归)。

    修复前 record_ttfb/record_failure/record_ofb/record_http_error 在 domain=None
    时对该 dict 写两遍 → 非域名流量的 success/total/样本/digest 全部翻倍,
    使全局成功率向非域名流量倾斜(相对域名流量被放大 2 倍)。
    """
    sel = _selector()
    sel.record_ttfb("p1", 0.10)
    m = sel._proxy_metrics["p1"]["metrics"]
    assert m["success"] == 1 and m["total"] == 1
    assert len(m["ttfb_samples"]) == 1
    assert m["cum_ttfb_digest"]["n"] == 1
    assert m["cum_success"] == 1 and m["cum_ttfb_n"] == 1

    sel.record_failure("p1")
    assert m["total"] == 2 and m["cum_failure_transport"] == 1

    sel.record_origin_first_byte("p1", 0.05)
    assert len(m["ofb_samples"]) == 1
    assert m["cum_ofb_n"] == 1 and m["cum_ofb_digest"]["n"] == 1


def test_metrics_dual_written_when_domain_given():
    """带 domain 时域名桶与全局桶各写一次(双写语义必须保留)。"""
    sel = _selector()
    sel.record_ttfb("p1", 0.10, domain="gh:443")
    dm = sel._domain_metrics["gh:443"]["p1"]["metrics"]
    gm = sel._proxy_metrics["p1"]["metrics"]
    assert dm["success"] == 1 and dm["total"] == 1
    assert gm["success"] == 1 and gm["total"] == 1
    assert dm is not gm  # 两个不同的桶


def test_http_error_rollback_not_double_counted():
    """5xx 回退 cum_success 只扣一次(修复前双写会双扣,触发 cum_success>0 守卫)。"""
    sel = _selector()
    sel.record_ttfb("p1", 0.10)
    sel.record_http_error("p1", 500)
    m = sel._proxy_metrics["p1"]["metrics"]
    assert m["cum_success"] == 0
    assert m["cum_failure_5xx"] == 1


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


# ── Phase 2: 多目标 Cost 排序 ────────────────────────────
def _store_with(*pids):
    """建一个含指定 pid 的真实 ProxyStore(排序需要 proxy_store.list())。"""
    store = ProxyStore()
    for i, pid in enumerate(pids):
        store.add(ProxyInfo(id=pid, host=f"h{i}", port=3128))
    return store


def test_cost_sort_default_enabled():
    """默认开启(canary 默认 ON;关闭即回滚纯 EWMA)。"""
    sel = ProxySelector(_store_with("p1"))
    assert sel.cost_sort_enabled is True
    assert sel.cost_latency_metric == "p99"


def test_cost_sort_equivalent_to_ewma_when_only_latency():
    """仅延迟差异时,Cost 排序与纯 EWMA 顺序一致(负载因子已折进延迟再归一化)。"""
    sel = ProxySelector(_store_with("fast", "mid", "slow"))
    sel.record_ttfb("fast", 0.02)
    sel.record_ttfb("mid", 0.10)
    sel.record_ttfb("slow", 0.80)
    cost_order = sel.ordered_proxies()
    sel.cost_sort_enabled = False
    ewma_order = sel.ordered_proxies()
    assert cost_order == ["fast", "mid", "slow"]
    assert ewma_order == cost_order


def test_cost_sort_prefers_high_success_rate():
    """延迟相等、成功率差异大 → 高成功率者靠前(纯 EWMA 下会随机)。"""
    sel = ProxySelector(_store_with("A", "B"))
    sel.record_ttfb("A", 0.10)
    sel.record_ttfb("B", 0.10)
    sel._proxy_metrics["A"]["metrics"].update(success=99, total=100)
    sel._proxy_metrics["B"]["metrics"].update(success=50, total=100)
    for _ in range(20):
        assert sel.ordered_proxies()[0] == "A"


def test_cost_sort_disabled_equals_legacy():
    """cost_sort_enabled=False 时排序与 _weighted_rank(纯 EWMA)完全一致。"""
    sel = ProxySelector(_store_with("p1", "p2", "p3"), cost_sort_enabled=False)
    sel.record_ttfb("p1", 0.30)
    sel.record_ttfb("p2", 0.01)
    sel.record_ttfb("p3", 0.10)
    order = sel.ordered_proxies()
    legacy = sorted(["p1", "p2", "p3"], key=sel._weighted_rank)
    assert order == legacy == ["p2", "p3", "p1"]


def test_cost_sort_missing_data_neutral():
    """全缺指标的代理:cost 取中性值(各项 0.5),不崩且未知质量仍垫底。"""
    sel = ProxySelector(_store_with("known", "unknown"))
    sel.record_ttfb("known", 0.05)
    scores = sel._cost_scores(["known", "unknown"], None)
    w_sum = (sel.cost_weight_latency + sel.cost_weight_success_rate
             + sel.cost_weight_throughput)
    assert scores["unknown"] == pytest.approx(0.5 * w_sum)
    assert scores["known"] < scores["unknown"]
    assert sel.ordered_proxies()[-1] == "unknown"


def test_cost_sort_throughput_ignored_when_bytes_low():
    """累计字节低于 cost_throughput_min_bytes 时吞吐项不参与(隧道零吞吐噪声防护)。"""
    sel = ProxySelector(_store_with("p1", "p2"), cost_throughput_min_bytes=1_000_000)
    sel.record_ttfb("p1", 0.10)
    sel.record_ttfb("p2", 0.20)
    m1 = sel._proxy_metrics["p1"]["metrics"]
    m2 = sel._proxy_metrics["p2"]["metrics"]
    # 字节不足:吞吐近 0 / 极高都应被忽略 → 两代理 cost 差仅来自延迟
    m1.update(throughput_ewma=0.0001, total_bytes=100)
    m2.update(throughput_ewma=5.0, total_bytes=100)
    diff_low = sel._cost_scores(["p1", "p2"], None)
    # 字节充足:吞吐项生效,p2 的高吞吐抵消部分延迟劣势 → 差距缩小
    m1["total_bytes"] = m2["total_bytes"] = 2_000_000
    diff_high = sel._cost_scores(["p1", "p2"], None)
    assert (diff_low["p2"] - diff_low["p1"]) > (diff_high["p2"] - diff_high["p1"])


def test_cost_latency_metric_ewma_vs_p99():
    """切换主延迟项会改变排序:均值口径选 spiky,尾部(P99)口径选 steady。"""
    sel = ProxySelector(_store_with("spiky", "steady"), cost_latency_metric="ewma")
    # spiky: 极快 + 极慢 → 均值小、尾部大;steady: 两次中等。
    sel.record_ttfb("spiky", 0.02)
    sel.record_ttfb("spiky", 0.30)
    sel.record_ttfb("steady", 0.10)
    sel.record_ttfb("steady", 0.20)
    assert sel.ordered_proxies()[0] == "spiky"      # 均值口径
    sel.cost_latency_metric = "p99"
    assert sel.ordered_proxies()[0] == "steady"     # 尾部口径(P99 尾部优先)


def test_cost_thresholds_reachable_from_config():
    """Cost 配置挂在 CircuitConfig 上,router 经 router_cfg.circuit 读取。"""
    cc = CircuitConfig(cost_sort_enabled=False, cost_latency_metric="ewma",
                       cost_weight_success_rate=0.9)
    assert cc.cost_sort_enabled is False
    assert cc.cost_latency_metric == "ewma"
    assert cc.cost_weight_success_rate == 0.9
    assert RouterConfig().circuit.cost_sort_enabled is True  # 默认开


# ── P1: Cost 分解输出(观测/调参) ─────────────────────────
def _store4():
    store = ProxyStore()
    for i, pid in enumerate(("fast", "mid", "slow", "dead")):
        store.add(ProxyInfo(id=pid, host=f"h{i}", port=3128))
    return store


def test_cost_breakdown_components_and_rank():
    """分解含三分量原始值/归一化/贡献,贡献之和=总 cost,rank 与 cost 同序。"""
    sel = ProxySelector(_store4())
    sel.record_ttfb("fast", 0.02)
    sel.record_ttfb("mid", 0.10)
    sel.record_ttfb("slow", 0.80)
    sel._proxy_metrics["slow"]["metrics"].update(success=50, total=100)
    bd = sel.cost_breakdown()
    assert bd["cost_sort_enabled"] is True
    assert bd["latency_metric"] == "p99"
    assert bd["weights"] == {"latency": 1.0, "success_rate": 0.6,
                             "throughput": 0.1}
    cands = bd["candidates"]
    assert set(cands) == {"fast", "mid", "slow", "dead"}
    for pid, d in cands.items():
        total = (d["latency"]["contrib"] + d["success_rate"]["contrib"]
                 + d["throughput"]["contrib"])
        assert d["cost"] == pytest.approx(total)
        assert 0.0 <= d["latency"]["norm"] <= 1.0
        assert d["rank"] >= 1
    # fast 延迟最优 → 延迟贡献 0、rank 1;slow 延迟最差且成功率差 → 双高贡献
    assert cands["fast"]["latency"]["contrib"] == pytest.approx(0.0)
    assert cands["fast"]["rank"] == 1
    assert cands["slow"]["latency"]["norm"] == pytest.approx(1.0)
    assert cands["slow"]["success_rate"]["failure"] == pytest.approx(0.5)
    # 无数据代理:各分量中性 0.5
    dead = cands["dead"]
    assert dead["latency"]["raw"] is None
    assert dead["latency"]["norm"] == pytest.approx(0.5)
    assert dead["success_rate"]["failure"] is None
    # rank 按 (slow-start, 未知质量, cost) 与 ordered_proxies 同键:
    # 已知质量代理内 cost 升序;无观测的 dead 即使 cost 更低也垫底(未知质量)。
    assert cands["dead"]["unknown_quality"] == 1
    assert cands["dead"]["rank"] == 4
    known_costs = [cands[p]["cost"] for p in ("fast", "mid", "slow")]
    assert known_costs == sorted(known_costs)


def test_cost_breakdown_load_mult_visible():
    """连续失败惩罚折进延迟:load_mult 反映在分解里且抬高有效延迟。"""
    sel = ProxySelector(_store4())
    sel.record_ttfb("fast", 0.10)
    sel.record_ttfb("mid", 0.10)
    sel.record_failure("mid")  # mid 连失一次 → fail_mult = 1+1*4 = 5
    bd = sel.cost_breakdown()["candidates"]
    assert bd["mid"]["load_mult"] == pytest.approx(5.0)
    assert bd["mid"]["latency"]["effective"] == pytest.approx(0.5)
    assert bd["fast"]["load_mult"] == pytest.approx(1.0)


def test_cost_breakdown_excludes_non_candidates():
    """熔断代理不在候选集:v2 里其 cost_breakdown 为 None(指标仍在窗口/累计)。"""
    sel = ProxySelector(_store4())
    sel.record_ttfb("fast", 0.02)
    for _ in range(3):
        sel.record_failure("slow")  # 熔断
    v2 = sel.get_pid_quality_v2()
    assert v2["fast"]["cost_breakdown"] is not None
    assert v2["slow"]["cost_breakdown"] is None
    assert v2["slow"]["cumulative"] is not None  # 指标本身仍可见
