"""P1 自动调参器(AutoTuner)与 Cost 热更新 API 测试。

覆盖:
  - cycle() 判定:基线测量 / 改进采纳 / 恶化回滚 / 噪声带拒绝 / SR 守卫 / 扩窗
  - 扰动轮转:三维度方向穷尽、边界钳制跳过
  - 蓄水池窗口与 observe() 开关
  - 胜点侧信道:_stash_attempt_ttfb / _observe_win
  - 持久化:tuner_state 落库与恢复(含越界/缺统计行拒绝)
  - API:GET/POST /cost 热更新即时生效、未知键 422、POST /tuner 启停回滚
"""

import asyncio
import json
import tempfile
from unittest import mock

import pytest
from fastapi.testclient import TestClient

from auto_squid.api import app
import auto_squid.api as api_module
from auto_squid.config_schema import AutoTuneConfig
from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.tuner import AutoTuner, _WEIGHT_BOUNDS, _WEIGHT_KEYS


# ── 测试脚手架 ──────────────────────────────────────────
class _Cfg:
    """AutoTuneConfig 的轻量替身(避免 pydantic 校验拖慢表驱动用例)。"""

    def __init__(self, **kw):
        self.enabled = kw.get("enabled", True)
        self.window_sec = kw.get("window_sec", 900.0)
        self.min_samples = kw.get("min_samples", 3)
        self.step = kw.get("step", 0.25)
        self.hysteresis = kw.get("hysteresis", 0.05)
        self.sr_guard = kw.get("sr_guard", 0.005)
        self.persist = kw.get("persist", False)


class _Sel:
    """ProxySelector 的轻量替身:只暴露 tuner 读写的东西。"""

    def __init__(self):
        self.cost_weight_latency = 1.0
        self.cost_weight_success_rate = 0.6
        self.cost_weight_throughput = 0.1
        self._sr = (9, 10)  # global_window_success 返回值

    def global_window_success(self):
        return self._sr


def _tuner(**cfgkw):
    sel = _Sel()
    return AutoTuner(sel, None, None, _Cfg(**cfgkw)), sel


def _measure(tuner, mean_ttfb, sr=(9, 10)):
    """跑一个"基线测量"窗口(等价于首轮 cycle)。"""
    tuner.baseline = None
    tuner.pending_baseline = True
    for v in [mean_ttfb] * 10:
        tuner.observe(v)
    sel_sr, tuner.selector._sr = tuner.selector._sr, sr
    tuner._cycle()
    tuner.selector._sr = sel_sr


# ── cycle() 判定 ────────────────────────────────────────
def test_first_cycle_measures_baseline_and_applies_trial():
    tuner, sel = _tuner()
    for _ in range(10):
        tuner.observe(0.10)
    tuner._cycle()
    # 基线=当前权重;随后立即应用第一个试跑扰动(默认轮转先 latency +25%)
    assert tuner.baseline is not None
    assert tuner.baseline[1] == pytest.approx(0.10)
    assert sel.cost_weight_latency == pytest.approx(1.25)


def test_trial_improvement_adopted():
    tuner, sel = _tuner()
    _measure(tuner, mean_ttfb=0.10, sr=(10, 10))
    trial_lat = sel.cost_weight_latency  # _measure 后 selector 已应用试跑权重
    # 试跑窗口:均值 0.05,远好于基线 0.10(改进 50% > 滞回 5%),SR 不降
    for _ in range(10):
        tuner.observe(0.05)
    tuner.selector._sr = (10, 10)
    tuner._cycle()
    assert tuner.last_decision["type"] == "adopted"
    assert tuner.baseline[0]["cost_weight_latency"] == pytest.approx(trial_lat)
    # 采纳后立即应用下一个试跑:轮转游标已消耗 latency(+),下一个是 success_rate(+)
    assert sel.cost_weight_latency == pytest.approx(trial_lat)
    assert sel.cost_weight_success_rate == pytest.approx(0.6 * 1.25)


def test_trial_degradation_reverted():
    tuner, sel = _tuner()
    _measure(tuner, mean_ttfb=0.10, sr=(10, 10))
    base_w = dict(tuner.baseline[0])
    base_lat = base_w["cost_weight_latency"]
    for _ in range(10):
        tuner.observe(0.50)  # 恶化 5 倍
    tuner.selector._sr = (10, 10)
    tuner._cycle()
    assert tuner.last_decision["type"] == "rejected_degraded"
    # selector 权重已回滚到基线
    assert sel.cost_weight_latency == pytest.approx(base_lat)
    assert tuner.baseline[0]["cost_weight_latency"] == pytest.approx(base_lat)


def test_noise_band_rejected_without_penalty():
    """噪声带(改进/恶化都 < 滞回):拒绝但不判恶化,基线不变。"""
    tuner, sel = _tuner()
    _measure(tuner, mean_ttfb=0.10, sr=(10, 10))
    base_w = dict(tuner.baseline[0])
    for _ in range(10):
        tuner.observe(0.098)  # 改进 2% < 5% 滞回
    tuner.selector._sr = (10, 10)
    tuner._cycle()
    assert tuner.last_decision["type"] == "rejected_noise"
    assert tuner.baseline[0] == base_w
    assert sel.cost_weight_latency == pytest.approx(base_w["cost_weight_latency"])


def test_success_rate_guard_blocks_adoption():
    """试跑再快,成功率跌破守卫也必须拒绝(不允许拿成功率换延迟)。"""
    tuner, sel = _tuner()
    _measure(tuner, mean_ttfb=0.10, sr=(10, 10))
    base_w = dict(tuner.baseline[0])
    for _ in range(10):
        tuner.observe(0.01)  # 延迟大幅改进
    tuner.selector._sr = (8, 10)  # 但成功率 0.8,较基线 1.0 跌 0.2 >> 0.005
    tuner._cycle()
    assert tuner.last_decision["type"] == "rejected_degraded"
    assert sel.cost_weight_latency == pytest.approx(base_w["cost_weight_latency"])


def test_undersampled_window_extends_then_discards():
    tuner, _ = _tuner(min_samples=10)
    _measure(tuner, mean_ttfb=0.10)
    for _ in range(3):  # 只有 3 个样本 < 10
        tuner.observe(0.5)
    for _ in range(4):
        tuner._cycle()
    # 扩窗 3 次后(第 4 次 cycle)放弃:窗口被清空、无采纳决策
    assert tuner.last_decision is None or tuner.last_decision["type"] != "adopted"
    assert len(tuner._win_ttfb) == 0


def test_disabled_tuner_is_noop():
    tuner, sel = _tuner(enabled=False)
    for _ in range(20):
        tuner.observe(0.10)
    tuner._cycle()
    assert tuner.baseline is None
    assert tuner.last_decision is None


# ── 扰动轮转与边界 ──────────────────────────────────────
def test_perturbation_round_robin_covers_all_dims():
    tuner, _ = _tuner()
    tuner.baseline = ({"cost_weight_latency": 1.0,
                       "cost_weight_success_rate": 0.6,
                       "cost_weight_throughput": 0.1}, 0.1, 0.9)
    seen = []
    for _ in range(6):
        trial = tuner._next_trial()
        changed = [k for k in _WEIGHT_KEYS
                   if trial[k] != pytest.approx(tuner.baseline[0][k])]
        assert len(changed) == 1  # 每次只动一个维度
        seen.append((changed[0], trial[changed[0]] > tuner.baseline[0][changed[0]]))
    # 6 次覆盖 3 维度 × 2 方向
    assert len({(k, d) for k, d in seen}) == 6
    assert {k for k, _ in seen} == set(_WEIGHT_KEYS)


def test_perturbation_clamped_at_bounds():
    tuner, _ = _tuner()
    lo_latency = _WEIGHT_BOUNDS["cost_weight_latency"][0]
    tuner.baseline = ({"cost_weight_latency": lo_latency,
                       "cost_weight_success_rate": 0.6,
                       "cost_weight_throughput": 0.1}, 0.1, 0.9)
    # 前两个扰动是 latency +25%(合法)与下一维度;持续轮转不会产生越界权重
    for _ in range(12):
        trial = tuner._next_trial()
        for k, (lo, hi) in _WEIGHT_BOUNDS.items():
            assert lo <= trial[k] <= hi


# ── 胜点侧信道 ──────────────────────────────────────────
@pytest.mark.asyncio
async def test_win_ttfb_side_channel_roundtrip():
    router = _router()
    router.tuner.enabled = True  # 侧信道只在 tuner 开启时入窗
    async def attempt():
        router._stash_attempt_ttfb(0.123)  # 模拟 _try_http/_try_tunnel 内的 stash
    task = asyncio.create_task(attempt())
    await task
    router._observe_win(task)
    assert router.tuner._win_ttfb and router.tuner._win_ttfb[0] == pytest.approx(0.123)


@pytest.mark.asyncio
async def test_observe_win_without_stash_is_noop():
    router = _router()
    router.tuner.enabled = True
    async def attempt():
        return 1
    task = asyncio.create_task(attempt())
    await task
    router._observe_win(task)  # 无 _squid_ttfb 属性:不抛、不记
    router._observe_win(None)
    assert len(router.tuner._win_ttfb) == 0


def _router(**kw):
    """构造带真实 DB 与默认(关闭)调参器的 Router(不监听)。"""
    ps = ProxyStore()
    db_path = kw.pop("db_path", None) or tempfile.mktemp(suffix=".db")
    return Router(ps, listen_host="127.0.0.1", listen_port=10909,
                  db_path=db_path, probe_interval_sec=0.0, **kw)


# ── 持久化 ──────────────────────────────────────────────
def test_baseline_persisted_and_restored():
    db_path = tempfile.mktemp(suffix=".db")
    r1 = _router(db_path=db_path)
    r1.tuner.enabled = True
    r1.tuner.baseline = ({"cost_weight_latency": 2.0,
                          "cost_weight_success_rate": 0.5,
                          "cost_weight_throughput": 0.05}, 0.08, 0.95)
    r1.tuner._save_state()
    # 新 Router 从同一 DB 恢复
    r2 = _router(db_path=db_path, auto_tune=AutoTuneConfig(enabled=True))
    assert r2.tuner.baseline is not None
    assert r2.tuner.baseline[0]["cost_weight_latency"] == pytest.approx(2.0)
    assert r2.selector.cost_weight_latency == pytest.approx(2.0)  # 已写回 selector
    r2.tuner.enabled = False  # 防泄漏 task


def test_restore_rejects_out_of_bounds_weights():
    db_path = tempfile.mktemp(suffix=".db")
    r1 = _router(db_path=db_path)
    r1.tuner.enabled = True
    bad = {"cost_weight_latency": 999.0,
           "cost_weight_success_rate": 0.5,
           "cost_weight_throughput": 0.05}
    with r1._db_lock:
        r1._db.execute(
            "INSERT INTO tuner_state (key, value_json, updated_at) VALUES ('baseline', ?, 't')",
            (json.dumps({"weights": bad, "mean_ttfb": 0.1, "sr": 0.9}),))
        r1._db.commit()
    r2 = _router(db_path=db_path, auto_tune=AutoTuneConfig(enabled=True))
    assert r2.tuner.baseline is None  # 越界行被拒,按无历史处理
    assert r2.selector.cost_weight_latency == pytest.approx(1.0)  # 保持 config 默认
    r2.tuner.enabled = False


def test_restore_rejects_missing_mean_ttfb():
    db_path = tempfile.mktemp(suffix=".db")
    r1 = _router(db_path=db_path)
    r1.tuner.enabled = True
    with r1._db_lock:
        r1._db.execute(
            "INSERT INTO tuner_state (key, value_json, updated_at) VALUES ('baseline', ?, 't')",
            (json.dumps({"weights": {"cost_weight_latency": 1.0,
                                     "cost_weight_success_rate": 0.6,
                                     "cost_weight_throughput": 0.1}}),))
        r1._db.commit()
    r2 = _router(db_path=db_path, auto_tune=AutoTuneConfig(enabled=True))
    assert r2.tuner.baseline is None
    r2.tuner.enabled = False


# ── API 热更新 ──────────────────────────────────────────
@pytest.fixture()
def client():
    r = _router()
    api_module._router = r
    with TestClient(app) as c:
        yield c, r


def test_get_cost_snapshot(client):
    c, r = client
    data = c.get("/cost").json()
    assert data["enabled"] is False
    assert data["current_weights"] == {"cost_weight_latency": 1.0,
                                       "cost_weight_success_rate": 0.6,
                                       "cost_weight_throughput": 0.1}
    assert data["bounds"]["cost_weight_latency"] == list(_WEIGHT_BOUNDS["cost_weight_latency"])


def test_post_cost_updates_selector_immediately(client):
    c, r = client
    resp = c.post("/cost", json={"cost_weight_latency": 2.0,
                                 "cost_sort_enabled": False})
    assert resp.status_code == 200
    assert r.selector.cost_weight_latency == pytest.approx(2.0)
    assert r.selector.cost_sort_enabled is False
    # 快照同步反映
    assert c.get("/cost").json()["current_weights"]["cost_weight_latency"] == pytest.approx(2.0)


def test_post_cost_rejects_unknown_keys_and_bad_values(client):
    c, r = client
    assert c.post("/cost", json={"cost_weight_bad": 1}).status_code == 422
    assert c.post("/cost", json={"cost_latency_metric": "h3"}).status_code == 422
    assert c.post("/cost", json={}).status_code == 422
    # 负权重被钳到 0
    c.post("/cost", json={"cost_weight_latency": -5})
    assert r.selector.cost_weight_latency == pytest.approx(0.0)


def test_post_cost_sets_pending_baseline_when_tuner_on(client):
    c, r = client
    r.tuner.enabled = True
    assert r.tuner.pending_baseline is False
    c.post("/cost", json={"cost_weight_latency": 1.5})
    assert r.tuner.pending_baseline is True  # 下一窗口把手动值重测为新基线
    r.tuner.enabled = False


def test_post_tuner_toggle_reverts_on_disable(client):
    """开启→(模拟一次采纳)→关闭:关闭即回滚到最近采纳的基线权重。

    注意 set_enabled(True) 按设计清零基线(全新开始),故基线在开启后模拟。"""
    c, r = client
    c.post("/tuner", json={"enabled": True})
    assert r.tuner.enabled is True
    # 模拟调参器已采纳 0.8 为基线
    r.tuner.baseline = ({"cost_weight_latency": 0.8,
                         "cost_weight_success_rate": 0.6,
                         "cost_weight_throughput": 0.1}, 0.1, 0.9)
    c.post("/tuner", json={"enabled": False})
    assert r.tuner.enabled is False
    assert r.selector.cost_weight_latency == pytest.approx(0.8)  # 回滚基线


@pytest.mark.asyncio
async def test_router_start_stop_runs_tuner_loop():
    """集成:enabled 时 start() 启动循环、stop() 干净取消(不悬挂)。"""
    r = _router(auto_tune=AutoTuneConfig(enabled=True, window_sec=0.2, min_samples=3))
    await r.start()
    try:
        assert r.tuner._task is not None and not r.tuner._task.done()
        for _ in range(5):
            r.tuner.observe(0.1)
        await asyncio.sleep(0.35)  # 一个窗口过去 → 基线已测
        assert r.tuner.baseline is not None
    finally:
        await r.stop()
    assert r.tuner._task is None or r.tuner._task.done()
