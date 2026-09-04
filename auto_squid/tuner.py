"""Cost 权重自动调参器(P1)。

目标:在不重启、不改路由代码的前提下,依据生产流量自动微调 Phase 2 多目标
Cost 的三个权重(latency / success_rate / throughput),让**赢家 TTFB 均值**
更短;成功率作硬守卫——不允许为降延迟牺牲成功率。

## 算法:单维扰动的保守爬山((1+1)-ES 风格)

- 每个「评估窗口」(window_sec,默认 15min)只试跑**一个**扰动:对基线权重
  按 维度×方向 轮转做 ±step(默认 ±25%)相对扰动。
- 窗口结束时把试跑统计与基线统计对比,双门槛判定:
  - 改进:试跑均值 < 基线×(1−hysteresis) **且** 成功率跌幅 ≤ sr_guard → 采纳
  - 恶化:试跑均值 > 基线×(1+hysteresis) 或 成功率跌破守卫 → 立即回滚基线权重
  - 两者之间:噪声带 → 拒绝(不采纳,也不视作恶化,换下一个扰动)
- 每窗口最多动一个维度的一步,收敛慢但几乎不可能被噪声或时段性流量漂移带偏
  ——这是"保守"风格刻意付出的代价(用户选定)。

## objective 的采集:赢家 TTFB 侧信道

`Router._try_http/_try_tunnel` 在算出 TTFB 处调用 `_stash_attempt_ttfb()` 把
值挂到 `asyncio.current_task()` 的自定义属性上;竞速 harness 在**胜出点**与
单发路径在拿到结果后调用 `_observe_win()`,从当前 task 取回该值喂给
`observe()`。这样窗口里只有**真正的赢家样本**(record_ttfb 会混入未被取消的
败者握手,不能直接当 objective),且零元组手术——`_cleanup_tunnel_result`
用 `result[-1]` 取 writer,给结果元组追加元素会炸清理路径,故绝不改元组形状。

## 防陈化

基线统计会随流量结构漂移而过时,故每 `_BASELINE_REMEASURE_EVERY` 个窗口强制
重测一次基线(该窗口跑基线权重本身,刷新 mean/sr)。

## 线程/并发模型

observe() 与 _cycle() 都在事件循环线程(API 端点为 async def,亦同循环),
无锁;唯一例外是 DB 读写——与后台 flush 线程共享连接,持 Router 的 _db_lock。
"""

import asyncio
import json
import logging
import random
from collections import deque
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# 三权重的安全边界(硬编码)。调参器的价值在于"不会调坏":边界收窄一点点,
# 换来的是即便判定逻辑被极端流量骗了,路由行为也仍在人为验证过的区间内。
# 需要更宽的取值请走手动 POST /cost(那是人类决策,不经本表钳制)。
_WEIGHT_BOUNDS = {
    "cost_weight_latency": (0.2, 4.0),
    "cost_weight_success_rate": (0.0, 2.0),
    "cost_weight_throughput": (0.0, 1.0),
}
_WEIGHT_KEYS = tuple(_WEIGHT_BOUNDS)   # 固定轮转顺序(保证可复现)
_WINDOW_CAP = 20_000                   # 窗口样本上限(蓄水池采样,内存有界)
_MAX_EXTENSIONS = 3                    # 窗口样本不足时的最大扩窗次数
_BASELINE_REMEASURE_EVERY = 10         # 每 N 个试跑窗口强制重测一次基线(防统计陈化)


class AutoTuner:
    """Cost 权重保守爬山调参器。自管后台 task(照 pools.py 模式)。"""

    def __init__(self, selector, db, db_lock, config):
        self.selector = selector
        self._db = db
        self._db_lock = db_lock
        self.enabled = bool(config.enabled)
        self.window_sec = max(0.05, float(config.window_sec))
        self.min_samples = max(1, int(config.min_samples))
        self.step = min(1.0, max(0.01, float(config.step)))
        self.hysteresis = min(0.5, max(0.0, float(config.hysteresis)))
        self.sr_guard = max(0.0, float(config.sr_guard))
        self.persist = bool(config.persist)

        self._task: asyncio.Task | None = None
        # 当前评估窗口的赢家 TTFB 样本(蓄水池,均值无偏)
        self._win_ttfb: deque = deque()
        self._win_count = 0
        # 基线 = (weights dict, mean_ttfb, success_rate);None 表示尚未测过
        self.baseline: tuple[dict, float, float] | None = None
        self.pending_baseline = False   # 手动 POST /cost 后置位:下一窗口重测基线
        self._extensions = 0
        self._trial_idx = 0             # 维度×方向 轮转游标
        self._since_measure = 0         # 距上次基线(重)测量的试跑窗口数
        self.last_decision: dict | None = None

        if self.enabled and self.persist:
            self._restore()

    # ── 生命周期(照 pools.py 自管 task 模式) ──────────────────
    def start(self) -> None:
        if not self.enabled or (self._task and not self._task.done()):
            return
        self._task = asyncio.create_task(self._loop())
        logger.info("auto tuner started: window=%ss min_samples=%d step=%s hysteresis=%s",
                    self.window_sec, self.min_samples, self.step, self.hysteresis)

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    def set_enabled(self, enabled: bool) -> None:
        """运行时启停(POST /tuner)。开启=全新开始(基线清零重测);关闭=回滚到
        已知好的基线权重(若有),避免停在试跑到一半的权重上。"""
        enabled = bool(enabled)
        if enabled == self.enabled:
            return
        self.enabled = enabled
        if enabled:
            self.baseline = None
            self.pending_baseline = False
            self._extensions = 0
            self._since_measure = 0
            self._win_ttfb.clear()
            self._win_count = 0
            self.start()
            logger.info("auto tuner enabled at runtime")
        else:
            # 关闭即回滚:停在基线权重上(确定性已知好点),不留在试跑权重。
            if self.baseline is not None:
                self._apply(self.baseline[0])
                logger.info("auto tuner disabled at runtime; reverted to baseline weights %s",
                            self.baseline[0])
            else:
                logger.info("auto tuner disabled at runtime (no baseline measured)")
            # stop 需在事件循环里 await;本方法由 async 端点调用(有运行循环),
            # 兜底:无循环时直接丢弃 task 引用(测试/异常上下文)。
            try:
                asyncio.get_running_loop().create_task(self._stop_async())
            except RuntimeError:
                self._task = None

    async def _stop_async(self) -> None:
        await self.stop()

    # ── 热路径采样(必须极便宜) ─────────────────────────────────
    def observe(self, ttfb: float) -> None:
        """记录一个赢家 TTFB(蓄水池采样,窗口内均匀,均值无偏)。"""
        if not self.enabled:
            return
        self._win_count += 1
        if len(self._win_ttfb) < _WINDOW_CAP:
            self._win_ttfb.append(ttfb)
        else:
            i = random.randrange(self._win_count)
            if i < _WINDOW_CAP:
                self._win_ttfb[i] = ttfb

    def manual_override(self) -> None:
        """手动 POST /cost 改过权重后调用:置位重测标记,下一窗口把当前权重
        (手动值)测成新基线,避免调参器的旧基线与人类决策打架。"""
        if self.enabled:
            self.pending_baseline = True
            logger.info("auto tuner: manual override detected, next window re-measures baseline")

    # ── 主循环 ────────────────────────────────────────────────
    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.window_sec)
            try:
                self._cycle()
            except Exception:
                logger.exception("auto tuner cycle failed")

    def _cycle(self) -> None:
        """窗口结束时的判定(事件循环线程内,无 await,原子执行)。"""
        n = len(self._win_ttfb)
        cur_weights = self._current_weights()
        if n < self.min_samples:
            self._extensions += 1
            if self._extensions <= _MAX_EXTENSIONS:
                logger.info("auto tuner: window under-sampled (%d < %d), extending (%d/%s)",
                            n, self.min_samples, self._extensions, _MAX_EXTENSIONS)
                return  # 不清窗,继续累计
            logger.warning("auto tuner: window still under-sampled after %d extensions, "
                           "discarding and waiting for traffic", _MAX_EXTENSIONS)
            self._reset_window()
            return

        mean_ttfb = sum(self._win_ttfb) / n
        succ, total = self.selector.global_window_success()
        sr = (succ / total) if total else None
        self._reset_window()

        # ── 本窗口是基线测量(启动首轮 / 手动覆盖后 / 周期性重测) ──
        if self.pending_baseline or self.baseline is None or self._since_measure >= _BASELINE_REMEASURE_EVERY:
            self.baseline = (cur_weights, mean_ttfb, sr)
            self.pending_baseline = False
            self._since_measure = 0
            self.last_decision = {
                "type": "baseline_measured", "weights": cur_weights,
                "mean_ttfb": mean_ttfb, "sr": sr, "samples": n,
            }
            logger.info("auto tuner baseline measured: weights=%s mean_ttfb=%.4fs sr=%s (n=%d)",
                        cur_weights, mean_ttfb, sr, n)
            self._apply(self._next_trial())
            return

        # ── 本窗口是试跑:与基线对比,双门槛判定 ──────────────────
        base_w, base_mean, base_sr = self.baseline
        self._since_measure += 1
        sr_ok = (sr is None or base_sr is None
                 or sr >= base_sr - self.sr_guard)
        improved = mean_ttfb < base_mean * (1.0 - self.hysteresis)
        degraded = mean_ttfb > base_mean * (1.0 + self.hysteresis)

        if improved and sr_ok:
            self.baseline = (cur_weights, mean_ttfb, sr)
            self.last_decision = {
                "type": "adopted", "weights": cur_weights,
                "mean_ttfb": mean_ttfb, "base_mean_ttfb": base_mean,
                "sr": sr, "base_sr": base_sr, "samples": n,
            }
            logger.info("auto tuner ADOPT weights=%s mean_ttfb=%.4fs (base %.4fs) sr=%s",
                        cur_weights, mean_ttfb, base_mean, sr)
            if self.persist:
                self._save_state()
        else:
            # 恶化或噪声带:立即回滚基线权重(拒绝本扰动,换下一个继续试)。
            self._apply(base_w)
            kind = "rejected_degraded" if (degraded or not sr_ok) else "rejected_noise"
            self.last_decision = {
                "type": kind, "weights": cur_weights, "reverted_to": base_w,
                "mean_ttfb": mean_ttfb, "base_mean_ttfb": base_mean,
                "sr": sr, "base_sr": base_sr, "samples": n, "sr_ok": sr_ok,
            }
            logger.info("auto tuner %s: trial=%s mean_ttfb=%.4fs (base %.4fs) sr=%s (base %s)",
                        kind, cur_weights, mean_ttfb, base_mean, sr, base_sr)

        self._apply(self._next_trial())

    # ── 扰动生成:维度×方向 轮转,越界跳过 ──────────────────────
    def _next_trial(self) -> dict:
        base_w = self.baseline[0] if self.baseline else self._current_weights()
        for _ in range(len(_WEIGHT_KEYS) * 2):
            key = _WEIGHT_KEYS[self._trial_idx % len(_WEIGHT_KEYS)]
            sign = 1.0 if (self._trial_idx // len(_WEIGHT_KEYS)) % 2 == 0 else -1.0
            self._trial_idx += 1
            lo, hi = _WEIGHT_BOUNDS[key]
            v = base_w[key] * (1.0 + sign * self.step)
            if v < lo or v > hi:
                continue  # 越界:该维度该方向已到边,轮转到下一个扰动
            trial = dict(base_w)
            trial[key] = v
            return trial
        # 三个维度两个方向全部越界(理论上到不了:边界内总有一个可行方向)
        logger.warning("auto tuner: all perturbations out of bounds, staying on baseline")
        return dict(base_w)

    # ── selector 权重读写 ─────────────────────────────────────
    def _current_weights(self) -> dict:
        return {
            "cost_weight_latency": float(self.selector.cost_weight_latency),
            "cost_weight_success_rate": float(self.selector.cost_weight_success_rate),
            "cost_weight_throughput": float(self.selector.cost_weight_throughput),
        }

    def _apply(self, weights: dict) -> None:
        """把权重写到 selector(普通属性,下一次排序即生效)。"""
        self.selector.cost_weight_latency = weights["cost_weight_latency"]
        self.selector.cost_weight_success_rate = weights["cost_weight_success_rate"]
        self.selector.cost_weight_throughput = weights["cost_weight_throughput"]

    def _reset_window(self) -> None:
        self._win_ttfb.clear()
        self._win_count = 0
        self._extensions = 0

    # ── 持久化(SQLite tuner_state 单行 JSON,持 _db_lock) ──────
    def _save_state(self) -> None:
        if not self.persist or self.baseline is None:
            return
        payload = {
            "weights": self.baseline[0],
            "mean_ttfb": self.baseline[1],
            "sr": self.baseline[2],
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        try:
            with self._db_lock:
                self._db.execute(
                    "INSERT INTO tuner_state (key, value_json, updated_at) VALUES ('baseline', ?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, updated_at=excluded.updated_at",
                    (json.dumps(payload), payload["ts"]))
                self._db.commit()
        except Exception:
            logger.exception("auto tuner: failed to persist baseline")

    def _restore(self) -> None:
        """启动时恢复持久化的基线权重(覆盖 config 值,日志注明)。"""
        try:
            with self._db_lock:
                row = self._db.execute(
                    "SELECT value_json FROM tuner_state WHERE key='baseline'").fetchone()
        except Exception:
            return  # 表还不存在(首次启动)或 DB 异常:按无历史处理
        if not row:
            return
        try:
            payload = json.loads(row[0])
            w = payload["weights"]
            # 只接受键齐全、都在安全边界内、且带有效基线统计的历史值
            # (防止旧版本/手改 DB 注入越界权重或缺统计导致判定失真)。
            if set(w) != set(_WEIGHT_KEYS):
                logger.warning("auto tuner: persisted weights keys mismatch, ignoring")
                return
            for k, (lo, hi) in _WEIGHT_BOUNDS.items():
                if not (lo <= w[k] <= hi):
                    logger.warning("auto tuner: persisted %s=%s out of bounds, ignoring", k, w[k])
                    return
            mean_ttfb = payload.get("mean_ttfb")
            if not isinstance(mean_ttfb, (int, float)) or mean_ttfb <= 0:
                logger.warning("auto tuner: persisted baseline has no valid mean_ttfb, ignoring")
                return
            sr = payload.get("sr")
            self.baseline = (w, float(mean_ttfb),
                             float(sr) if isinstance(sr, (int, float)) else None)
            self._apply(w)
            logger.info("auto tuner: restored persisted baseline weights %s (measured %s)",
                        w, payload.get("ts"))
        except Exception:
            logger.exception("auto tuner: failed to restore persisted baseline")

    # ── 快照(供 GET /cost) ───────────────────────────────────
    def snapshot(self) -> dict:
        return {
            "enabled": self.enabled,
            "window_sec": self.window_sec,
            "min_samples": self.min_samples,
            "step": self.step,
            "hysteresis": self.hysteresis,
            "sr_guard": self.sr_guard,
            "current_weights": self._current_weights(),
            "baseline": ({"weights": self.baseline[0], "mean_ttfb": self.baseline[1],
                          "sr": self.baseline[2]} if self.baseline else None),
            "window_samples": len(self._win_ttfb),
            "pending_baseline": self.pending_baseline,
            "last_decision": self.last_decision,
            "bounds": {k: list(v) for k, v in _WEIGHT_BOUNDS.items()},
        }
