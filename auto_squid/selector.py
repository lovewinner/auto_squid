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

    def record_ttfb(self, pid: str, ttfb: float):
        """记录一次成功请求的首字节耗时(秒),更新该代理的 EWMA。

        EWMA 公式:无历史时直接取当前值;有历史时 ewma = (1-alpha)*old + alpha*new。
        obs 计数随每次观测 +1(EWMA 样本数),供单发降级判定读取。
        """
        q = self._quality.get(pid)
        if q is None:
            self._quality[pid] = {"ewma_ttfb": ttfb, "obs": 1}
            self._conc_observe_success(pid)
            return
        old = q["ewma_ttfb"]
        q["ewma_ttfb"] = (1.0 - self.EWMA_ALPHA) * old + self.EWMA_ALPHA * ttfb
        q["obs"] = int(q.get("obs", 0)) + 1
        self._conc_observe_success(pid)

    def reset_quality(self):
        """清空全部质量数据(RFC 8305 §4:历史 RTT 不可跨网络沿用)。

        网络切换/代理分组变化后调用,让排序回到无偏状态重新学习。熔断/慢启动
        状态一并清空(旧网络的连续失败计数对当前网络无意义)。
        """
        self._quality.clear()
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
        w = q["ewma_ttfb"]
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

    def best_proxy(self) -> Optional[str]:
        """返回按 EWMA 排序后的首个代理 id(无代理时返回 None)。"""
        lst = self.ordered_proxies()
        return lst[0] if lst else None