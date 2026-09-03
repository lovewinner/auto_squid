"""代理选择子系统(从 Router 拆出,见 #14 selector.py)。

`ProxySelector` 从 ProxyStore 产出代理 id 的有序列表,供竞速使用:

- 取所有 enabled 代理,剔除熔断中的代理,按加权 least-request 权重排序
  ——权重 = ewma × (1 + active)^lb_bias,快速者靠前、在途积压多者被压低
  (least-active 语义),同权重代理间随机打乱以均衡负载;slow-start 恢复期
  (熔断退避刚结束)的代理垫底。
- 同时维护每代理质量 EWMA(_quality)、熔断/slow-start 状态(_circuit)、
  在途计数(_in_flight)与自适应并发限制(_conc)。

状态全部由事件循环单线程读写(Router 的 _try_http/_try_tunnel 在竞速与单发
两条路径经 _inflight_start/_inflight_finish 增减在途数)。本模块完全自包含:
只依赖 ProxyStore(类型 import)、random、time 与下列常量,零 Router 引用。
"""

import logging
import random
import time
from typing import List, Optional

from .proxy_store import ProxyStore

logger = logging.getLogger(__name__)


# 熔断器默认参数。连续失败 _CIRCUIT_THRESHOLD 次后熔断,退避期 circuit_until 按
# 指数增长(初始 1s,每次翻倍,上限 _CIRCUIT_MAX_BACKOFF)。退避期内该代理不参与
# 竞速/单发。退避到期后置 started_at=now 进入 slow-start:排序垫底、低权重,累计
# _SLOW_START_SUCCESS 次成功(或窗口期满)才恢复完整权重,防冷启动被打懵。
_CIRCUIT_THRESHOLD = 3
_CIRCUIT_MAX_BACKOFF = 300.0
_SLOW_START_WINDOW = 60.0
_SLOW_START_SUCCESS = 3

# 加权 least-request 的在途积压惩罚指数(bias,默认 1.0)。
# 排序权重 = ewma × (1 + active)^bias,即分析 doc 2.2 的 "peak EWMA"(最近 RTT ×
# 在途数):在途积压多的代理即使延迟历史最快,有效权重也被抬高、排序靠后,竞速选批
# 天然避开——保护慢代理不被打爆(Envoy LeastRequest 的 weight/(active+1)^bias
# 公式的对偶,此处以乘法形式作用于延迟权重)。bias=0 时退化为纯 EWMA 排序。
_LB_BIAS_DEFAULT = 1.0

# 近期连续失败对排序权重的惩罚倍数(见 _failure_penalty_mult)。默认 4.0 →
# 连失 1 次权重 ×5、连失 2 次 ×9,把恒失败代理从"首批竞速"挤走(比 3 次熔断
# 早一步反应);成功清零 consec_fail 后自动回归。0=关闭(熔断兜底)。
_FAIL_PENALTY_DEFAULT = 4.0


class ProxySelector:
    """从 ProxyStore 产出代理 id 的有序列表,供竞速使用。

    策略:取所有 enabled 代理,剔除熔断中的代理,按加权 least-request 权重排序
    ——权重 = ewma × (1 + active)^lb_bias,快速者靠前、在途积压多者被压低
    (least-active 语义),同权重代理间随机打乱以均衡负载;slow-start 恢复期
    (熔断退避刚结束)的代理垫底。竞速顺序决定"谁先到/是否白占竞速槽"——把扇出
    集中到快且空闲的优质代理,减少失败/积压代理在竞速中的无谓参与。
    """

    # EWMA 平滑系数:新观测占 0.3,历史占 0.7。取值参考 Finagle 峰值 EWMA 的
    # 常见做法,兼顾对新网络状况的响应速度与对抖动的抑制。
    EWMA_ALPHA = 0.3

    # 熔断退避指数:每次熔断退避期 * _CIRCUIT_BACKOFF_MULT,下一周期翻倍。
    _CIRCUIT_BACKOFF_MULT = 2.0

    def __init__(self, proxy_store: ProxyStore,
                 circuit_threshold: int = _CIRCUIT_THRESHOLD,
                 circuit_max_backoff: float = _CIRCUIT_MAX_BACKOFF,
                 slow_start_window: float = _SLOW_START_WINDOW,
                 slow_start_success: int = _SLOW_START_SUCCESS,
                 lb_bias: float = _LB_BIAS_DEFAULT,
                 fail_penalty_weight: float = _FAIL_PENALTY_DEFAULT,
                 concurrency_limit_enabled: bool = False,
                 concurrency_limit_initial: int = 16,
                 concurrency_limit_min: int = 2,
                 concurrency_limit_max: int = 128,
                 concurrency_add_on_success: int = 4,
                 concurrency_mult_on_failure: float = 0.5,
                 concurrency_failure_window: int = 20):
        self.proxy_store = proxy_store
        self.circuit_threshold = max(1, circuit_threshold)
        self.circuit_max_backoff = max(1.0, circuit_max_backoff)
        self.slow_start_window = max(1.0, slow_start_window)
        self.slow_start_success = max(1, slow_start_success)
        # 加权 least-request 的在途惩罚指数(见 _LB_BIAS_DEFAULT)。
        # 排序权重 = ewma × (1 + active)^bias;bias=0 退化为纯 EWMA 排序。
        self.lb_bias = max(0.0, lb_bias)
        # 近期连续失败惩罚权重(见 _FAIL_PENALTY_DEFAULT):把 consec_fail 折算成
        # 排序权重抬升,让恒失败代理提前移出前排竞速,不必等熔断阈值。
        self.fail_penalty_weight = max(0.0, fail_penalty_weight)
        # ── 自适应并发限制(P3)────────────────────────────────
        # 每代理并发上限,成功加性增/失败乘性降,防慢代理被请求堆死。
        self.concurrency_enabled = concurrency_limit_enabled
        self._conc_initial = max(1, concurrency_limit_initial)
        self._conc_min = max(1, min(concurrency_limit_min, self._conc_initial))
        self._conc_max = max(self._conc_initial, concurrency_limit_max)
        self._conc_add = max(1, concurrency_add_on_success)
        self._conc_mult = max(0.0, min(1.0, concurrency_mult_on_failure))
        self._conc_win = max(1, concurrency_failure_window)
        # {pid: {"limit": int, "ok": int, "fail": int}} —— ok/fail 为最近窗口计数。
        self._conc: dict[str, dict[str, float]] = {}
        # 每代理质量: {pid: {"ewma_ttfb": float(秒), "obs": int}}。
        #   obs = 成功观测计数(EWMA 样本数),供单发降级判定把"当前 EWMA"与该代理
        #   被钉住时的基线 EWMA 对比(见 Router._single_send_degraded)。obs==1 时
        #   EWMA 直接等于该次观测,可直接参与对比;仅存有观测的代理,无观测的代理
        #   在排序时视为"未知质量"(排在新手区)。
        self._quality: dict[str, dict[str, float]] = {}
        # 每域名×代理质量: {domain: {pid: {"ewma_ttfb": float(秒), "obs": int, "ts": float}}}。
        #   ts = time.monotonic() 最近观测时刻,供周期淘汰(prune_domain_quality)。
        #   域名级数据存在时,降级判定优先用它(见 Router._single_send_degraded /
        #   _worse_than_best),避免全局 EWMA 跨域名平均掩盖"某代理对该域名其实
        #   很快"的事实(生产案例:247-246 全局 EWMA 快但被其他域名拖累,对
        #   github.com 其实很快,却因全局 EWMA 恶化被降级剔除)。
        self._domain_quality: dict[str, dict[str, dict[str, float]]] = {}
        # 每代理熔断/慢启动状态(与 _quality 分开维护,含未观测过的新代理):
        #   {pid: {"consec_fail": int, "open_until": float(monotonic 秒), "backoff": float}}
        self._circuit: dict[str, dict[str, float]] = {}
        # 每代理当前在途请求数(P2C/least-active 选批依据)。
        # 由 Router._try_http/_try_tunnel 在候选"发起→结束(成功/失败/取消)"的生命
        # 周期内 self._inflight_start/_inflight_finish 增减,含竞速与单发两条路径。
        self._in_flight: dict[str, int] = {}
        # 在途数高水位(历史峰值,供 /metrics 观察选批负载压力)。
        self.max_in_flight = 0
        # 累计熔断开启次数(供 /metrics /circuit 观察熔断活动)。
        self.circuit_open_count = 0

    def get_quality(self) -> dict[str, dict[str, float]]:
        """返回质量表快照(供 /metrics / 仪表盘展示,读内存无锁)。"""
        return {pid: dict(q) for pid, q in self._quality.items()}

    def has_quality(self) -> bool:
        """是否已有任何代理质量观测(冷启动判定用,无需整表快照拷贝)。

        _stagger_initial 的冷启动翻倍只需知道"空/非空",get_quality 返回整表
        dict 拷贝,竞速热路径没必要为一次布尔判断付出 O(n) 拷贝。
        """
        return bool(self._quality)

    # ── in-flight 计数(P2C / least-active 选批依据)──────────────

    def _inflight_start(self, pid: str):
        """发起一次上游尝试:在途数 +1,并推进高水位。热路径,O(1),无锁。"""
        n = self._in_flight.get(pid, 0) + 1
        self._in_flight[pid] = n
        if n > self.max_in_flight:
            self.max_in_flight = n

    def _inflight_finish(self, pid: str):
        """结束一次上游尝试(成功/失败/被取消):在途数 -1。

        由 _try_http/_try_tunnel 的 finally 调用,保证竞速取消也释放计数。
        只在确有发起(started)时递减;防御性下限 0,防止并发异常路径下计数漂移。
        """
        n = max(0, self._in_flight.get(pid, 0) - 1)
        if n == 0:
            self._in_flight.pop(pid, None)
        else:
            self._in_flight[pid] = n

    def get_in_flight(self) -> dict[str, int]:
        """返回当前在途数快照 {pid: n}(供 /metrics / 仪表盘,读内存无锁)。"""
        return dict(self._in_flight)

    # ── 自适应并发限制(P3)────────────────────────────────────

    def _conc_state(self, pid: str) -> dict[str, float]:
        """惰性取(或建)某代理的并发限制状态。"""
        s = self._conc.get(pid)
        if s is None:
            s = {"limit": float(self._conc_initial), "ok": 0.0, "fail": 0.0}
            self._conc[pid] = s
        return s

    def _at_concurrency_limit(self, pid: str) -> bool:
        """该代理当前在途数是否达到并发上限。"""
        if not self.concurrency_enabled:
            return False
        s = self._conc_state(pid)
        return self._in_flight.get(pid, 0) >= int(s["limit"])

    def _conc_observe_success(self, pid: str):
        """成功观测:窗口内累计成功;达标(成功 ≥ 窗口)且稳定 → 加性提升上限。

        只增不降,上限封顶 _conc_max。用 EWMA 作为稳定度参考:仅当当前观测数
        >= 窗口时才提升,避免冷启动即打满。
        """
        if not self.concurrency_enabled:
            return
        s = self._conc_state(pid)
        s["ok"] = int(s.get("ok", 0)) + 1
        s["fail"] = 0  # 成功清零失败窗口
        if s["ok"] >= self._conc_win and int(s["limit"]) < self._conc_max:
            s["limit"] = min(float(self._conc_max), int(s["limit"]) + self._conc_add)
            s["ok"] = 0

    def _conc_observe_failure(self, pid: str):
        """失败观测:乘性降低上限(触底 _conc_min),并清成功窗口。"""
        if not self.concurrency_enabled:
            return
        s = self._conc_state(pid)
        s["fail"] = int(s.get("fail", 0)) + 1
        if s["fail"] >= 1:
            new_limit = max(float(self._conc_min), int(s["limit"]) * self._conc_mult)
            s["limit"] = new_limit
            s["fail"] = 0
            s["ok"] = 0

    def get_concurrency_limits(self) -> dict[str, int]:
        """返回当前每代理并发上限快照 {pid: limit}(供 /metrics / 仪表盘)。"""
        return {pid: int(s["limit"]) for pid, s in self._conc.items()}

    def record_ttfb(self, pid: str, ttfb: float, domain: Optional[str] = None):
        """记录一次成功请求的首字节耗时(秒),更新该代理的 EWMA。

        EWMA 公式:无历史时直接取当前值;有历史时 ewma = (1-alpha)*old + alpha*new。
        obs 计数随每次观测 +1(EWMA 样本数),供单发降级判定读取。
        domain 非 None 时同步更新该域名的独立 EWMA 桶(见 _domain_quality)——降级
        判定域名级优先(见 Router._single_send_degraded),probe 探活无目标域名不传。
        """
        self._apply_ewma(self._quality, pid, ttfb)
        self._conc_observe_success(pid)
        if domain:
            per_pid = self._domain_quality.setdefault(domain, {})
            entry = self._apply_ewma(per_pid, pid, ttfb)
            if entry is not None:
                entry["ts"] = time.monotonic()

    @staticmethod
    def _apply_ewma(table: dict, pid: str, ttfb: float) -> Optional[dict]:
        """在 table[pid] 上应用 EWMA_ALPHA 公式,返回更新后的条目(首次建新)。

        全局质量表与域名质量表共用同一公式(见 _quality/_domain_quality 注释)。
        """
        q = table.get(pid)
        if q is None:
            q = {"ewma_ttfb": ttfb, "obs": 1}
            table[pid] = q
            return q
        q["ewma_ttfb"] = ((1.0 - ProxySelector.EWMA_ALPHA) * q["ewma_ttfb"]
                          + ProxySelector.EWMA_ALPHA * ttfb)
        q["obs"] = int(q.get("obs", 0)) + 1
        return q

    def _domain_quality_for(self, domain: str, pid: str) -> Optional[dict]:
        """热路径定向读:某域名下某代理的质量条目(不存在返回 None)。

        不要在判定热路径用 get_domain_quality()(全量拷贝),用它逐域名读取。
        """
        return self._domain_quality.get(domain, {}).get(pid)

    def get_domain_quality(self) -> dict[str, dict[str, dict[str, float]]]:
        """返回域名级质量表快照 {domain: {pid: dict}}(供 /metrics / 仪表盘)。

        读内存无锁;返回深一层拷贝,调用方改返回值不影响内部表。
        """
        return {d: {pid: dict(q) for pid, q in per_pid.items()}
                for d, per_pid in self._domain_quality.items()}

    def best_domain_ewma(self, domain: str, exclude: Optional[str] = None) -> tuple:
        """该域名下有观测且可用(未熔断/未被禁用/仍存在)的代理中 EWMA 最小者。

        返回 (pid, ewma);无观测或全部不可用返回 (None, None)。exclude 给定则
        跳过该代理(方向 A 回填门比较赢家与其余候选,自己不比自己)。
        过滤与 ordered_proxies 同构:熔断/已删/禁用代理残留的域名观测不能成为
        "域名 best",否则会把健康赢家误拦或放错基准。
        """
        per_pid = self._domain_quality.get(domain)
        if not per_pid:
            return None, None
        best_pid, best_ewma = None, None
        for pid, q in per_pid.items():
            if pid == exclude:
                continue
            ewma = self._proxy_quality_ewma(q)
            if ewma is None:
                continue
            if self.is_circuit_open(pid):
                continue
            p = self.proxy_store.get(pid)
            if p is None or not p.enabled:
                continue
            if best_ewma is None or ewma < best_ewma:
                best_pid, best_ewma = pid, ewma
        return best_pid, best_ewma

    @staticmethod
    def _proxy_quality_ewma(q: Optional[dict]) -> Optional[float]:
        """从质量表条目取出 EWMA(秒);无条目/缺字段返回 None。

        与 Router._proxy_quality_ewma 同语义,域名桶条目结构一致。
        """
        if not q:
            return None
        ewma = q.get("ewma_ttfb")
        return float(ewma) if isinstance(ewma, (int, float)) else None

    def prune_domain_quality(self, max_entries: int = 10_000):
        """域名级质量表容量保护:条目超上限时按最近观测 ts 淘汰最旧条目。

        由 Router._flush_loop 周期调用(与 _prune_sticky/cluster.prune 同位置),
        防止域名×代理数组合无界增长。max_entries=0 时清空整表。
        """
        if max_entries <= 0:
            self._domain_quality.clear()
            return
        total = sum(len(per_pid) for per_pid in self._domain_quality.values())
        if total <= max_entries:
            return
        flat: list[tuple[float, str, str]] = []
        for d, per_pid in self._domain_quality.items():
            for pid, q in per_pid.items():
                flat.append((float(q.get("ts", 0.0)), d, pid))
        flat.sort(key=lambda t: t[0])
        for _, d, pid in flat[: total - max_entries]:
            per_pid = self._domain_quality.get(d)
            if per_pid is not None:
                per_pid.pop(pid, None)
                if not per_pid:
                    self._domain_quality.pop(d, None)

    def reset_quality(self):
        """清空全部质量数据(RFC 8305 §4:历史 RTT 不可跨网络沿用)。

        网络切换/代理分组变化后调用,让排序回到无偏状态重新学习。熔断/慢启动
        状态一并清空(旧网络的连续失败计数对当前网络无意义)。域名级质量表
        (跨域名分离的 EWMA)同样清空——旧网络的域名级历史对当前网络无意义。
        """
        self._quality.clear()
        self._domain_quality.clear()
        self._circuit.clear()
        self._in_flight.clear()
        self._conc.clear()  # 并发上限随质量重学(P3)

    def reset_circuits(self):
        """手动解除全部代理的熔断并清空连续失败计数(运维介入后调用)。

        与 reset_quality 的区别:不动 EWMA(延迟历史仍有效),只清熔断状态。
        """
        self._circuit.clear()

    # ── 熔断器 / slow-start ────────────────────────────────────

    def _circuit_state(self, pid: str) -> dict[str, float]:
        """惰性取(或建)某代理的熔断状态 dict,避免在 __init__ 枚举代理。"""
        s = self._circuit.get(pid)
        if s is None:
            s = {"consec_fail": 0, "open_until": 0.0, "backoff": 0.0}
            self._circuit[pid] = s
        return s

    def record_failure(self, pid: str):
        """记录一次上游失败(连接失败/超时/5xx)。连续失败达阈值即熔断。

        退避期指数增长:首熔断 backoff=1s,此后每次新熔断翻倍(上限
        circuit_max_backoff)。退避期内 open_until 未到,该代理不参与竞速/单发。
        """
        self._conc_observe_failure(pid)  # 自适应并发:失败 → 乘性降低上限(P3)
        s = self._circuit_state(pid)
        s["consec_fail"] = int(s.get("consec_fail", 0)) + 1
        if s["consec_fail"] >= self.circuit_threshold:
            backoff = (float(s.get("backoff", 0.0)) or 1.0) * self._CIRCUIT_BACKOFF_MULT
            s["backoff"] = min(self.circuit_max_backoff, backoff)
            s["open_until"] = time.monotonic() + s["backoff"]
            s["consec_fail"] = 0  # 熔断后计数清零,恢复后的失败重新累计
            self.circuit_open_count += 1
            logger.warning("circuit opened for proxy %s, backoff=%.1fs", pid, s["backoff"])

    def record_success(self, pid: str):
        """记录一次上游成功,连续失败计数归零。

        若正处于 slow-start 恢复期(backoff 期满但未完成爬升),累计成功次数,
        达标即恢复完整权重。若此前无慢启动标记,本方法无副作用。
        """
        s = self._circuit.get(pid)
        if s is None:
            return
        s["consec_fail"] = 0
        if self._in_slow_start(pid, s):
            s["slow_start_ok"] = int(s.get("slow_start_ok", 0)) + 1

    def _in_slow_start(self, pid: str, s: Optional[dict] = None) -> bool:
        """是否处于 slow-start 恢复期:退避刚结束(started_at 新鲜)且累计成功不足。

        慢启动只在"熔断退避到期后的恢复阶段"触发,与冷启动(从未观测)无关。
        判断依据:慢启动窗口(默认 60s)内且成功数未达阈值。窗口期内任一时刻
        累计 slow_start_ok 达标即退出(由 record_success 判断)。
        """
        s = s or self._circuit.get(pid)
        if not s:
            return False
        started_at = s.get("started_at")
        if not started_at:
            return False
        if time.monotonic() - started_at >= self.slow_start_window:
            return False
        return int(s.get("slow_start_ok", 0)) < self.slow_start_success

    def _rearm_slow_start(self, pid: str):
        """熔断退避到期后,把代理置入 slow-start 恢复期(权重垫底,爬升中)。

        重置 started_at=now、累计成功归零。在退避期满的下一次排序/取用路径上
        触发一次,不单独起 task。
        """
        s = self._circuit_state(pid)
        s["started_at"] = time.monotonic()
        s["slow_start_ok"] = 0
        s["consec_fail"] = 0

    def is_circuit_open(self, pid: str) -> bool:
        """该代理是否处于熔断退避期(open_until 未到)。已过期自动解除。

        退避到期后清 open_until 并置入 slow-start 恢复期(started_at=now),此
        后返回 False(可再次参与排序,但垫底)。整个过程在排序/取用路径上惰性
        触发,不单独起 task。
        """
        s = self._circuit.get(pid)
        if not s:
            return False
        open_until = s.get("open_until", 0.0)
        if open_until and time.monotonic() < open_until:
            return True
        if open_until:  # 退避刚到期:进入 slow-start 恢复期(仅一次,清 open_until)。
            s["open_until"] = 0.0
            self._rearm_slow_start(pid)
        return False

    def get_circuit_state(self) -> dict[str, dict]:
        """返回全部熔断状态快照(供 /circuit API / 仪表盘),含退避剩余时间。

        返回结构:{pid: {"open": bool, "open_until": float 或 None, "backoff": float,
                        "consec_fail": int, "slow_start": bool}}。
        """
        now = time.monotonic()
        out = {}
        for pid in self._circuit:
            s = self._circuit[pid]
            open_until = s.get("open_until", 0.0)
            open_now = bool(open_until and now < open_until)
            out[pid] = {
                "open": open_now,
                "open_until": open_until or None,
                "backoff": s.get("backoff", 0.0),
                "consec_fail": int(s.get("consec_fail", 0)),
                "slow_start": self._in_slow_start(pid, s),
            }
        return out

    # ── 排序 ───────────────────────────────────────────────────

    def _quality_rank(self, pid: str) -> tuple:
        """排序键:无观测(未知质量)的代理排最后,EWMA 小者靠前。

        返回 (未知标记, ewma) 二元组,让排序稳定:未知质量统一放尾部。
        """
        q = self._quality.get(pid)
        if q is None:
            return (1, 0.0)
        return (0, q["ewma_ttfb"])

    def _failure_penalty_mult(self, pid: str) -> float:
        """近期连续失败对排序权重的惩罚倍数。

        竞速/单发排序只按 EWMA(成功耗时)排,失败代理的 EWMA 不会因失败而降——
        一个"恒 0ms 失败"的代理能靠老 EWMA 赖在前排持续陪跑(2026-09 事故的金
        属型:247-246 单日 400+ 次竞速失败)。这里把当前连续失败次数(record_failure
        累加的 consec_fail)折算成权重惩罚:连续失败越多,权重被抬得越高(排序越靠后)。

        只在 **退避期外** 生效(熔断退避期由 is_circuit_open 过滤,退避结束后进入
        slow-start 已由 _slow_start_rank 垫底);成功 record_success 会清零
        consec_fail,惩罚随之消失——"间歇可用"代理恢复一次成功即中断惩罚,不误伤。
        """
        s = self._circuit.get(pid)
        if not s:
            return 1.0
        k = int(s.get("consec_fail", 0))
        return 1.0 + k * self.fail_penalty_weight

    def _weighted_rank(self, pid: str) -> float:
        """加权 least-request 排序权重:ewma × (1 + active)^lb_bias。

        无观测(未知质量)的代理排新手区、由 _quality_rank 的未知标记兜底,
        此处不参与加权(不曾在途/无 EWMA 的新代理直接给 0 权重,靠前尝试)。
        在途积压惩罚随 active 指数放大:fast 代理同时背上大量在途请求时,
        (1+active)^bias 的权重把它从"首批竞速"的位置挤下去,让轻载代理顶上
        ——正是"保护慢代理不被打爆"的 least-active 语义(Envoy LeastRequest
        的 weight/(active+1)^bias 对偶,见 _LB_BIAS_DEFAULT)。bias=0 时恒为
        ewma,退化为纯 EWMA 排序(RFC 8305 §4 RTT 排序)。
        """
        q = self._quality.get(pid)
        if q is None:
            return 0.0
        # 近期连续失败惩罚:一个代理连失 N 次,权重 ×(1 + N*fail_penalty) 抬升,
        # 从"首批竞速"位置被挤下去(比熔断早一步反应——0ms 失败一次就该降温,
        # 不必等 3 次熔断)。成功清零 consec_fail 后自动回归。
        w = q["ewma_ttfb"] * self._failure_penalty_mult(pid)
        if self.lb_bias > 0:
            active = self._in_flight.get(pid, 0)
            if active:
                w *= (1.0 + active) ** self.lb_bias
        return w

    def _slow_start_rank(self, pid: str) -> int:
        """slow-start 排序档位:恢复期代理垫底(档 1),正常代理靠前(档 0)。

        slow-start 是恢复阶段的**临时低权重**——把恢复中的代理从"排第一被首批
        竞速/被单发抢打"的位置挪开,先让一两个请求试水,成功若干次后再回到
        正常排序。若此时恢复代理碰巧排在前 max_retries 内仍可能被竞速选中,但
        竞速首字节判胜 + 扇出集中在健康代理,试水代价被天然稀释。
        """
        return 1 if self._in_slow_start(pid) else 0

    def ordered_proxies(self) -> List[str]:
        """返回按加权 least-request 权重(快且不忙者靠前)排序的已启用代理列表。

        - 剔除熔断退避期内的代理(open_until 未到);
        - 按 slow-start 档位分层(恢复期代理垫底),层内按加权权重排:
          权重 = ewma × (1 + active)^lb_bias——快而空闲的代理靠前,背上大量
          在途积压的代理被压低(least-active 语义,保护慢代理不被打爆);
        - 同权重段随机打乱,均衡负载的同时保持"快且不忙者先竞速"。
        退避到期的代理在此被解熔断并置入 slow-start(is_circuit_open 副作用)。
        """
        proxies = self.proxy_store.list()
        enabled = [p for p in proxies if p.enabled]
        # 过滤熔断中的代理(is_circuit_open 同时处理退避到期解熔断)。
        enabled = [p for p in enabled if not self.is_circuit_open(p.id)]
        # 自适应并发限制(P3):在途已达上限的代理不参与候选(防慢代理被堆死)。
        if self.concurrency_enabled:
            enabled = [p for p in enabled if not self._at_concurrency_limit(p.id)]
        random.shuffle(enabled)
        enabled.sort(key=lambda p: (self._slow_start_rank(p.id),
                                    self._quality_rank(p.id)[0],  # 未知质量垫底
                                    self._weighted_rank(p.id)))
        return [p.id for p in enabled]

    def _domain_quality_rank(self, domain_obs: Optional[dict], pid: str) -> tuple:
        """域名维度未知质量排序键:该域名无观测的代理统一垫底(标记 1)。

        domain_obs 为 None(域名无任何观测桶)时委托全局 _quality_rank——
        即整体回退全局排序。域名有观测桶时,只对"该域名下被测过"的代理按
        域名 EWMA 排,该域名下从未观测的代理垫底(首字节判胜竞速里不该死磕
        未知质量的代理,与全局 _quality_rank 的"未知垫底"语义一致)。
        """
        if domain_obs is None:
            return self._quality_rank(pid)
        q = domain_obs.get(pid)
        if q is None:
            return (1, 0.0)
        return (0, self._proxy_quality_ewma(q) or 0.0)

    def _domain_weighted_rank(self, domain_obs: Optional[dict], pid: str) -> float:
        """域名维度加权 least-request 权重:域名 EWMA × (1 + active)^lb_bias。

        与 _weighted_rank 同构,只是延迟基准从全局 EWMA 换成该域名 EWMA。
        在途积压 active 仍取全局(并发压力是全局的,与域名无关),保留
        least-active 语义。domain_obs 为 None 时委托全局 _weighted_rank(回退)。
        该域名无观测的代理给权重 0(靠前尝试,与 _weighted_rank 一致)。
        """
        if domain_obs is None:
            return self._weighted_rank(pid)
        q = domain_obs.get(pid)
        if q is None:
            return 0.0
        w = self._proxy_quality_ewma(q)
        if w is None:
            return 0.0
        # 近期连续失败惩罚注入(与全局 _weighted_rank 同构),见 _failure_penalty_mult。
        w *= self._failure_penalty_mult(pid)
        if self.lb_bias > 0:
            active = self._in_flight.get(pid, 0)
            if active:
                w *= (1.0 + active) ** self.lb_bias
        return w

    def ordered_for_domain(self, domain: Optional[str] = None) -> List[str]:
        """域名级竞速候选排序:该域名下有观测时,该域名快代理(域名 EWMA 小者)
        靠前,该域名无观测的代理垫底;整个域名无任何观测时完全回退全局排序。

        与 ordered_proxies 的差异只在排序基准:过滤(熔断/禁用/并发上限)、
        slow-start 恢复期垫底、同权重随机打乱、least-active 在途惩罚全部保留,
        只是把"全局 EWMA"换成"该域名 EWMA(缺失时用全局)"。这样竞速首批
        (stagger_initial 个)发的是**该域名下最快的代理**,而非全局最快的——
        修复全局 EWMA 跨域名平均掩盖"代理对该域名真实快慢"的污染。

        domain=None 时与 ordered_proxies() 逐位等价(冷启动/无域名上下文零变化)。
        """
        domain_obs = None
        if domain is not None:
            per = self._domain_quality.get(domain)
            if per:
                domain_obs = per
        proxies = self.proxy_store.list()
        enabled = [p for p in proxies if p.enabled]
        # 过滤熔断中的代理(is_circuit_open 同时处理退避到期解熔断)。
        enabled = [p for p in enabled if not self.is_circuit_open(p.id)]
        # 自适应并发限制(P3):在途已达上限的代理不参与候选(防慢代理被堆死)。
        if self.concurrency_enabled:
            enabled = [p for p in enabled if not self._at_concurrency_limit(p.id)]
        random.shuffle(enabled)
        enabled.sort(key=lambda p: (self._slow_start_rank(p.id),
                                    self._domain_quality_rank(domain_obs, p.id)[0],
                                    self._domain_weighted_rank(domain_obs, p.id)))
        return [p.id for p in enabled]

    def best_proxy(self) -> Optional[str]:
        """返回按 EWMA 排序后的首个代理 id(无代理时返回 None)。"""
        lst = self.ordered_proxies()
        return lst[0] if lst else None