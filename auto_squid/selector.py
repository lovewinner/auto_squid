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

# ── 可观测性增强指标(Phase 1,见 IMPROVEMENT_PLAN.md)────────────
# 这些指标只用于观测/暴露(Phase 5)与未来的排序加权(Phase 2),**不改变**现有
# 选择逻辑(_quality/_domain_quality 的 ewma_ttfb/obs 原样保留供排序)。每个
# (pid) 或 (domain, pid) 维护一个滑动窗口:
#   - ttfb_samples: 有界样本列表,供 P50/P95/P99 分位数(隧道握手时延,代理侧)。
#   - ofb_samples:  有界样本列表,供 P50/P95/P99 分位数(源站首字节,源站侧)。
#   - success / total: 成功率。
#   - errors: 错误分类计数(timeout/connect/http_5xx/tls/protocol/other)。
#   - throughput_ewma: 吞吐的 EWMA(Phase 2 加权输入)。
#   - WINDOW 界定样本上限(环形截断,存最近 N 个,内存有界)。
_OBS_WINDOW = 256          # 每个序列保留的最近样本数(分位数用)
_THROUGHPUT_ALPHA = 0.3

# 终身累计指标字段(随每次观测单调增长,经 DB 恢复后继续累加,是唯一跨重启
# 的"永久值")。与窗口化指标(ttfb_samples 分位 / *_ewma)并存:后者反映近期
# 瞬时,本组反映该 (代理[,域名]) 自首次观测(含 DB 恢复)以来的累计表现。
# 计数字段放同一 metric_dict,由 _flush_to_db 随 metrics_json 落盘、set_*_metrics
# 恢复;旧 DB 行缺这些字段时由 set_*_metrics 用本默认值补齐。
_CUM_FIELDS = {
    "cum_success": 0,                 # 累计成功(收到非 5xx 响应头)
    "cum_failure_transport": 0,       # 累计传输层失败(连接/超时/TLS/协议/其他)
    "cum_failure_5xx": 0,             # 累计业务层失败(HTTP 5xx)
    "cum_ttfb_sum": 0.0,              # 累计隧道握手耗时和(秒),供平均握手延迟(代理侧)
    "cum_ttfb_n": 0,                  # 累计隧道握手观测次数
    "cum_ofb_sum": 0.0,               # 累计源站首字节耗时和(秒),供平均源站首字节(源站侧)
    "cum_ofb_n": 0,                   # 累计源站首字节观测次数
}

# 错误分类键(统一枚举,供 _try_http/_try_tunnel 的 except 归类,见 record_failure)。
ERROR_TIMEOUT = "timeout"
ERROR_CONNECT = "connect"
ERROR_HTTP_5XX = "http_5xx"
ERROR_TLS = "tls"
ERROR_PROTOCOL = "protocol"
ERROR_CANCELLED = "cancelled"
ERROR_OTHER = "other"
_ERROR_KEYS = (ERROR_TIMEOUT, ERROR_CONNECT, ERROR_HTTP_5XX,
               ERROR_TLS, ERROR_PROTOCOL, ERROR_CANCELLED, ERROR_OTHER)


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
        # ── 可观测性增强指标(Phase 1)────────────────────────────
        # 与 _quality/_domain_quality 并行的独立结构:排序继续用 ewma_ttfb/obs,
        # 此处收集完整耗时/吞吐/成功率/错误分类/分位数,仅供观测与 Phase 5 暴露
        # (详见 IMPROVEMENT_PLAN.md). 结构:
        #   _proxy_metrics:   {pid: metric_dict}
        #   _domain_metrics:  {domain: {pid: metric_dict}}
        # metric_dict 字段见 _ensure_metrics 注释。
        self._proxy_metrics: dict[str, dict] = {}
        self._domain_metrics: dict[str, dict[str, dict]] = {}
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
        # ── 可观测性增强(Phase 1):记录 ttfb 样本 + success/total ──
        # 收到响应头即视为一次"成功观测"(与选择逻辑一致);ttfb 样本入分位数窗口。
        # total 在成功(success+1)与失败(record_failure,total+1)两侧同步累加。
        for scope in (self._metrics_for(pid, domain), self._metrics_for(pid, None)):
            scope["success"] += 1
            scope["total"] += 1
            self._append_sample(scope["ttfb_samples"], ttfb)
            # ── 终身累计(跨重启永久):成功数 + TTFB 累加和/计数(算平均首字节)。
            #   5xx 响应也会走到本分支(record_ttfb 在收到响应头即记),但随后
            #   record_http_error 会把 cum_success 回退并计入 cum_failure_5xx,
            #   故 cum_success 最终只含非 5xx 成功(见 record_http_error)。
            scope["cum_success"] += 1
            scope["cum_ttfb_sum"] += ttfb
            scope["cum_ttfb_n"] += 1

    def record_origin_first_byte(self, pid: str, ofb: float,
                                 domain: Optional[str] = None):
        """记录一次 HTTPS 隧道的「源站首字节」耗时(秒,源站侧延迟)。

        盲 CONNECT 隧道(不解密)里唯一能观测到的源站侧延迟:计时起点是隧道
        建立(_relay_tunnel 开始透传),终点是上游→客户端方向收到**第一个字节**。
        该字节即源站回的 TLS ServerHello,故本指标 ≈ 源站 TCP 建连 + TLS 握手
        首字节,补上 record_ttfb(隧道握手,代理侧)覆盖不到的那一半链路。

        注意它不是 HTTP 响应首字节——后者埋在加密流里,不解密无从分辨(TLS1.3
        0-RTT / 会话复用 / HTTP2 都会改变往返形态,做突发分析很脆)。

        只在**新建隧道**上记录:命中已握手隧道复用(_established_reused)时不再
        有 TLS 握手,首个上行字节其实是应用响应数据,语义不同,混入会失真——
        故该情形由调用方直接跳过(见 router._relay_tunnel)。

        双作用域写入与 record_ttfb 一致:域名桶 + 全局桶。domain 为 None 时两
        次取到同一个全局桶(既有行为,比值类指标不受影响)。
        """
        if ofb <= 0:
            return
        for scope in (self._metrics_for(pid, domain), self._metrics_for(pid, None)):
            self._append_sample(scope["ofb_samples"], ofb)
            scope["cum_ofb_sum"] += ofb
            scope["cum_ofb_n"] += 1

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

    # ── 可观测性增强指标(Phase 1)───────────────────────────────
    # 这些结构仅用于观测/暴露,不参与现有排序(排序仍读 _quality/_domain_quality
    # 的 ewma_ttfb/obs)。见 IMPROVEMENT_PLAN.md Phase 1 / Phase 5。

    @staticmethod
    def _ensure_metrics(m: dict) -> dict:
        """惰性初始化一个 metric_dict(幂等,热路径 O(1) 获取字段)。

        字段:
          ttfb_samples:  最近 _OBS_WINDOW 个隧道握手耗时(秒,代理侧),算 P50/P95/P99。
          ofb_samples:   最近 _OBS_WINDOW 个源站首字节耗时(秒,源站侧),算 P50/P95/P99。
          throughput_ewma: 吞吐 EWMA(MB/s),初始 None。
          success / total: 成功数与总尝试数(成功率 = success/total)。
          errors:        错误分类计数 {key: n}。
          total_bytes:  累计转发 body 字节数(吞吐分母之一)。
          transfer_time: 累计"转发 body"耗时(秒)。
        """
        if "metrics" not in m:
            m["metrics"] = {
                "ttfb_samples": [],
                "ofb_samples": [],
                "throughput_ewma": None,
                "success": 0,
                "total": 0,
                "errors": {k: 0 for k in _ERROR_KEYS},
                "total_bytes": 0.0,
                "transfer_time": 0.0,
                **_CUM_FIELDS,
            }
        return m["metrics"]

    def _metrics_for(self, pid: str, domain: Optional[str]) -> dict:
        """取 (自适应 domain 存在时) 域名级或全局级 metric_dict,带惰性初始化。"""
        if domain is not None:
            per = self._domain_metrics.setdefault(domain, {})
            return self._ensure_metrics(per.setdefault(pid, {}))
        return self._ensure_metrics(self._proxy_metrics.setdefault(pid, {}))

    @staticmethod
    def _append_sample(samples: list, value: float):
        """向有界样本列表追加一个值,超出 _OBS_WINDOW 丢弃最旧(环形截断)。"""
        samples.append(value)
        if len(samples) > _OBS_WINDOW:
            del samples[0]

    @staticmethod
    def _percentile(samples: list, p: float) -> Optional[float]:
        """返回样本列表的 p 分位数(0-100);空列表返回 None。"""
        if not samples:
            return None
        s = sorted(samples)
        if len(s) == 1:
            return s[0]
        k = (len(s) - 1) * (p / 100.0)
        lo = int(k)
        hi = min(lo + 1, len(s) - 1)
        frac = k - lo
        return s[lo] * (1.0 - frac) + s[hi] * frac

    @staticmethod
    def _percentiles(samples: list) -> dict:
        """返回 {p50, p95, p99, min, max, mean} 汇总;空列表返回空。"""
        if not samples:
            return {}
        return {
            "p50": ProxySelector._percentile(samples, 50),
            "p95": ProxySelector._percentile(samples, 95),
            "p99": ProxySelector._percentile(samples, 99),
            "min": min(samples),
            "max": max(samples),
            "mean": sum(samples) / len(samples),
            "samples": len(samples),
        }

    @staticmethod
    def _cumulative_view(m: dict) -> dict:
        """从 metric_dict 计算终身累计(跨重启持久)派生指标。

        与窗口化指标(ttfb_samples 分位 / *_ewma)并存:后者反映近期瞬时,本视图
        反映该 (代理[,域名]) 自首次观测(含 DB 恢复)以来的累计表现。计数字段
        (cum_*) 随每次观测单调增长,经 selector.set_*_metrics 从 DB 恢复后继续
        累加,因此是唯一跨重启真正的"永久值"——这正是 test_routing.py --metrics
        想看到的"数据库里的永久值"。

        派生:
          - samples:        累计总样本 = 成功 + 失败(传输层 + 5xx)。
          - success_rate:   端到端累计成功率 = cum_success / samples(5xx 计失败)。
          - avg_ttfb_ms:    累计平均隧道握手延迟 = cum_ttfb_sum / cum_ttfb_n(ms,代理侧)。
          - avg_ofb_ms:     累计平均源站首字节 = cum_ofb_sum / cum_ofb_n(ms,源站侧,仅
                            HTTPS 新建隧道;复用隧道无 TLS 握手,不计)。
          - throughput_mbps:累计平均吞吐 = total_bytes / transfer_time(MB/s,HTTP+HTTPS)。
        """
        cum_success = m.get("cum_success", 0)
        cum_fail_t = m.get("cum_failure_transport", 0)
        cum_fail_5 = m.get("cum_failure_5xx", 0)
        cum_fail = cum_fail_t + cum_fail_5
        cum_samples = cum_success + cum_fail
        success_rate = (cum_success / cum_samples) if cum_samples else None
        n_ttfb = m.get("cum_ttfb_n", 0)
        avg_ttfb_ms = (m.get("cum_ttfb_sum", 0.0) / n_ttfb * 1000.0) if n_ttfb else None
        n_ofb = m.get("cum_ofb_n", 0)
        avg_ofb_ms = (m.get("cum_ofb_sum", 0.0) / n_ofb * 1000.0) if n_ofb else None
        transfer_time = m.get("transfer_time", 0.0)
        total_bytes = m.get("total_bytes", 0.0)
        throughput_mbps = ((total_bytes / 1024.0 / 1024.0) / transfer_time) if transfer_time > 0 else None
        return {
            "success": cum_success,
            "failure_transport": cum_fail_t,
            "failure_5xx": cum_fail_5,
            "failure": cum_fail,
            "samples": cum_samples,
            "success_rate": success_rate,
            "avg_ttfb_ms": avg_ttfb_ms,
            "avg_ofb_ms": avg_ofb_ms,
            "throughput_mbps": throughput_mbps,
            "total_bytes": total_bytes,
        }

    def record_complete(self, pid: str, body_bytes: int, body_duration: float,
                        body_ttfb: float = 0.0, domain: Optional[str] = None):
        """记录一次 **body 转发/隧道透传完成**的吞吐与累计字节(Phase 1.1)。

        HTTP:body_duration 是 body 从首个数据块到读完全部的耗时;
        HTTPS:是整条 CONNECT 隧道的存活时长(见 router._relay_tunnel)。两者
        都用于吞吐与累计字节——这是代理对"该 target 中转速率"的可观测量。

        注意:这里**不再**产生任何"完整响应耗时"分位指标。原 TTLB(body 下载
        时间)维度已移除——它是 HTTP-only 指标,在 HTTPS 主导的负载下绝大多数
        代理恒为 n/a;且对 CONNECT 而言"隧道寿命"受 keep-alive 影响,语义与
        "body 下载时间"完全不同,混在一起会毁掉分位数。HTTPS 的延迟画像改由
        隧道握手时延(TTFB,代理侧)+ 源站首字节(OFB,源站侧)两个维度刻画。

        - body_bytes:实际转发/透传的字节数。
        - body_ttfb:本响应的 TTFB(秒),仅用于吞吐分母修正;若调用方未提供则
          视为 0(即吞吐 = body_bytes / body_duration)。

        产出:
          - throughput_ewma:吞吐 EWMA(MB/s)= body_bytes / body_duration。
          - total_bytes / transfer_time:累计值(平均吞吐直接相除)。
        """
        m = self._metrics_for(pid, domain)
        g = self._metrics_for(pid, None)
        # 全局桶与(存在时)域名桶都更新:全局供 /quality/meta 跨域名聚合,域名桶
        # 单独保留便于"特定 URL 实测"(与 record_ttfb 的双作用域写入一致)。
        for sc in (g, m) if m is not g else (g,):
            if body_duration > 0:
                if body_bytes > 0:
                    transfer = max(body_duration - max(body_ttfb, 0.0), 0.0)
                    denom = transfer if transfer > 0 else body_duration
                    mbps = (body_bytes / 1024.0 / 1024.0) / denom
                    old = sc["throughput_ewma"]
                    sc["throughput_ewma"] = mbps if old is None else (
                        (1.0 - _THROUGHPUT_ALPHA) * old + _THROUGHPUT_ALPHA * mbps)
                sc["total_bytes"] += body_bytes
                sc["transfer_time"] += body_duration

    def record_http_error(self, pid: str, status_code: int,
                          domain: Optional[str] = None):
        """记录一次 HTTP 5xx 响应(header 已收到但为 5xx)。

        5xx 不抛异常(_race 判为"非可接受胜出"),record_failure 捕获不到,故单独
        计入 error 分类(IMPROVEMENT_PLAN.md Phase 1.3)。
        """
        if status_code >= 500:
            for scope in (self._metrics_for(pid, domain), self._metrics_for(pid, None)):
                scope["errors"][ERROR_HTTP_5XX] += 1
                # 终身累计:5xx 视作业务层失败。record_ttfb 在收到响应头时已将本
                # 请求计入 cum_success(非 5xx 假设),此处回退并计入 cum_failure_5xx,
                # 使 cum_success 只含非 5xx 成功、累计成功率 = cum_success/样本数
                # 真正反映端到端成功率(5xx 算失败)。
                scope["cum_failure_5xx"] += 1
                if scope["cum_success"] > 0:
                    scope["cum_success"] -= 1

    def get_proxy_metrics(self) -> dict:
        """返回全局(跨域名聚合)代理指标快照 {pid: metric_dict},供 /metrics 展示。

        注意全局桶的 ttfb/ofb 样本是**所有域名合并**的;域名级明细见
        get_domain_metrics(见下)。返回拷贝,调用方改不动内部。
        """
        import copy
        return {pid: copy.deepcopy(m["metrics"]) for pid, m in self._proxy_metrics.items()}

    def get_domain_metrics(self) -> dict:
        """返回域名级代理指标快照 {domain: {pid: metric_dict}},供 /domains/meta。

        增加 per-pid 的 P50/P95/P99 汇总(分位数从该域名样本窗口算)。
        返回拷贝,调用方改不动内部。
        """
        out = {}
        for d, per_pid in self._domain_metrics.items():
            for pid, m in per_pid.items():
                mm = m["metrics"]
                out.setdefault(d, {})[pid] = dict(mm)
                out[d][pid]["percentiles"] = {
                    "ttfb": ProxySelector._percentiles(mm.get("ttfb_samples", [])),
                    "ofb": ProxySelector._percentiles(mm.get("ofb_samples", [])),
                }
                # 终身累计(跨重启永久):与窗口化分位并存,供 --metrics 展示"永久值"。
                out[d][pid]["cumulative"] = ProxySelector._cumulative_view(mm)
        return out

    def get_pid_quality_v2(self) -> dict:
        """返回每代理的增强指标摘要(Phase 5 /quality 暴露)。

        含 P50/P95/P99(ttfb 隧道握手 / ofb 源站首字节)、成功率、错误分类、吞吐。
        内部 _append_sample 把样本留存在内存供分位,此处只读不拷贝样本(避免大对象)。
        """
        out = {}
        for pid, m in self._proxy_metrics.items():
            mm = m["metrics"]
            per = {
                "ttfb": ProxySelector._percentiles(mm.get("ttfb_samples", [])),
                "ofb": ProxySelector._percentiles(mm.get("ofb_samples", [])),
                "throughput_ewma_mbps": mm.get("throughput_ewma"),
                "success_count": mm.get("success", 0),
                "total_attempts": mm.get("total", 0),
                "success_rate": (mm["success"] / mm["total"]) if mm.get("total", 0) else None,
                "errors": dict(mm.get("errors", {})),
                "total_bytes_transferred": mm.get("total_bytes", 0.0),
                # 终身累计(跨重启永久):成功率/平均首字节/吞吐,与窗口化指标并存。
                "cumulative": ProxySelector._cumulative_view(mm),
            }
            out[pid] = per
        return out

    def prune_domain_metrics(self, max_entries: int = 10_000):
        """域名级指标表容量保护:条目超上限按字典序淘汰最旧域名(不排序,由
        domain_metrics 本身按域名字典序;保守额度保守,不被常用)。

        目前域名级指标与 _domain_quality 同规模,由 _flush_loop 经
        prune_domain_quality 一并治理;此处提供独立清理入口,防阶段性超量。
        """
        if max_entries <= 0:
            self._domain_metrics.clear()
            return
        total = sum(len(per_pid) for per_pid in self._domain_metrics.values())
        if total <= max_entries:
            return
        # 均匀地删:按域名遍历砍掉多余条目(简单、无偏的容量保护)。
        to_free = total - max_entries
        for d, per_pid in list(self._domain_metrics.items()):
            if to_free <= 0:
                break
            drop = min(len(per_pid), to_free)
            for pid in list(per_pid.keys())[:drop]:
                per_pid.pop(pid, None)
                self._domain_quality.get(d, {}).pop(pid, None)
            to_free -= drop
            if not per_pid:
                self._domain_metrics.pop(d, None)
                self._domain_quality.pop(d, None)

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
        状态一并清空(旧网络的连续失败对当前网络无意义);单发降级失效集合
        一并清空(旧网络的降级标记不可沿用)。
        """
        self._quality.clear()
        self._domain_quality.clear()
        self._circuit.clear()
        self._in_flight.clear()
        self._conc.clear()
        self._proxy_metrics.clear()
        self._domain_metrics.clear()

    def set_proxy_metrics(self, data: dict):
        """从 DB 恢复 proxy 级全局指标。

        覆盖内存中的 _proxy_metrics:JSON 里存的 metric_dict 映射到
        {"metrics": metric_dict} 结构(_metrics_for 约定)。
        跳过缺失字段的行,保证内部结构完整。
        """
        for pid, m in data.items():
            if not isinstance(m, dict):
                continue
            needed = {"ttfb_samples", "throughput_ewma", "success", "total",
                      "errors", "total_bytes", "transfer_time"}
            if not needed.issubset(m):
                logger.debug("proxy_metrics %s 缺少字段,跳过: %s", pid, set(m.keys()))
                continue
            # ofb_samples 为后加字段(TTLB 移除后新增的源站首字节窗口):旧 DB 行
            # 没有,补空列表,否则热路径 _append_sample 会 KeyError。
            if "ofb_samples" not in m:
                m["ofb_samples"] = []
            # 惰性补全终身累计字段(_CUM_FIELDS):旧 DB 行缺这些键时补默认值,使
            # record_ttfb/record_http_error 的 scope[...] 读写不抛 KeyError(09-04
            # 事故:旧行无 cum_* → 热路径 KeyError → 全请求失败 → 熔断全开)。
            for k, v in _CUM_FIELDS.items():
                if k not in m:
                    m[k] = v
            self._proxy_metrics[pid] = {"metrics": m}

    def set_domain_metrics(self, data: dict):
        """从 DB 恢复域名 × 代理 实测指标。

        data = {domain: {pid: metric_dict}}。
        覆盖内存中的 _domain_metrics:JSON 里存的 metric_dict 映射到
        {"metrics": metric_dict} 结构(_metrics_for 约定)。
        跳过缺失字段的行,保证内部结构完整。
        """
        for d, per_pid in data.items():
            if not isinstance(per_pid, dict):
                continue
            for pid, m in per_pid.items():
                if not isinstance(m, dict):
                    continue
                needed = {"ttfb_samples", "throughput_ewma", "success", "total",
                          "errors", "total_bytes", "transfer_time"}
                if not needed.issubset(m):
                    logger.debug("domain_metrics %s %s 缺少字段,跳过", d, pid)
                    continue
                # 同 set_proxy_metrics:旧 DB 行补 ofb_samples 空列表。
                if "ofb_samples" not in m:
                    m["ofb_samples"] = []
                # 惰性补全终身累计字段,同 set_proxy_metrics。
                for k, v in _CUM_FIELDS.items():
                    if k not in m:
                        m[k] = v
                self._domain_metrics.setdefault(d, {})[pid] = {"metrics": m}

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

    def record_failure(self, pid: str, error_type: Optional[str] = None,
                       domain: Optional[str] = None):
        """记录一次上游失败(连接失败/超时/5xx)。连续失败达阈值即熔断。

        退避期指数增长:首熔断 backoff=1s,此后每次新熔断翻倍(上限
        circuit_max_backoff)。退避期内 open_until 未到,该代理不参与竞速/单发。

        Phase 1:error_type 为错误分类键(_ERROR_KEYS 之一,默认 'other'),
        domain 为域名(HTTP 的 domain key / CONNECT 的 target)时,同步记录到
        域名级与全局级指标的错误分类与 total。注意**被竞速取消的败者
        (CancelledError)不应喂 record_failure**(由调用方跳过),故此处不再引入
        error_type='cancelled' 入口——取消在调用方(_try_http/_try_tunnel)判断。
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
        # ── 可观测性增强(Phase 1):错误分类 + total ──
        etype = error_type if error_type in _ERROR_KEYS else ERROR_OTHER
        for scope in (self._metrics_for(pid, domain), self._metrics_for(pid, None)):
            scope["total"] += 1
            scope["errors"][etype] += 1
            # 终身累计:传输层失败(连接/超时/TLS/协议/其他),与 cum_failure_5xx
            # 一并构成累计失败总数(见 _cumulative_view)。
            scope["cum_failure_transport"] += 1

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