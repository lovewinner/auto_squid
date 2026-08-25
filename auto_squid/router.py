"""代理路由核心:并行竞速转发 + 域名/响应缓存 + 客户端认证。

本模块实现一个正向代理:接受客户端的 HTTP 与 HTTPS(CONNECT)请求,经一组
上游代理并行竞速,取最先成功的响应回写客户端。核心机制:

- 并行竞速:同一请求同时发往多个上游,最先成功者获胜,其余取消并释放资源
  (_race / _try_http / _try_tunnel)。
- 流式转发 + 首字节判胜:HTTP 响应经 httpx 流式拉取,收到响应头即判胜,
  随后边收边转发 body 给客户端,降低首字节延迟(TTFB);落败者在判胜后即被
  取消,不再下载整包,省带宽(_stream_upstream_response / _tee_to_cache)。
- 上游连接池化:每个上游代理维护一个长驻 httpx.AsyncClient,跨请求复用
  keep-alive 连接,避免每请求重建 TCP/CONNECT(_get_client / _client_pool)。
- 域名缓存:某代理为某域名胜出后,在 cache_ttl 内复用该代理,避免每请求竞速
  (内存镜像 _meta_cache + _get_fresh_proxy)。
- 会话粘性:同一客户端 IP + 域名/目标复用上次胜出的代理单发,保持 egress IP
  稳定(纯内存 _sticky_cache,滑动 TTL);粘性代理失败或返回 5xx 则驱逐并回落
  竞速(redispatch),赢家回填粘性表。优先级高于域名缓存。粘性命中 N 次
  (recheck_hits)后触发探路重竞速,用新赢家替换可能已变慢的代理;粘性表有
  容量硬上限(stickiness_max_entries),超限驱逐最旧条目。
- HTTP 响应缓存:幂等 GET 的成功响应在内存缓存 60s(_http_cache_*),遵循
  Cache-Control;流式转发时边转边缓冲(带上限),缓冲满或响应过大则放弃缓存。
  写方法(POST/PUT/DELETE/PATCH)在转发前失效该域名的所有 GET 缓存
  (_http_cache_invalidate),使变更后的 GET 回源拿新内容,而非 60s 内返回旧响应。
- 数据持久化用 SQLite(domain_stats / domain_meta):内存累加 + 后台周期批量
  落盘(_stats_cache / _meta_dirty / _flush_loop),热路径无逐请求 fsync。
- 客户端认证:可选 HTTP Basic,在 handle_client 分流前统一校验(auth.check_auth)。
- 优雅关闭:stop() 先停服务、flush 残留统计、关闭连接池、再取消并等待在途连接,
  最后关 DB(_running_tasks)。

跨线程 DB 访问经 _db_lock 串行化(仅后台 flush 线程与 API 线程会触达);
事件循环热路径(转发)只读写内存,不经锁、不经 fsync。
"""

import asyncio
import base64
import logging
import random
import re
import socket
import sqlite3
import threading
import time
import urllib.parse
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Any, Dict, Tuple
import httpx

from .proxy_store import ProxyStore
from .auth import check_auth
from .config_schema import PolicyConfig

logger = logging.getLogger(__name__)

# 单个 HTTP 请求体的最大字节数。超过则返回 413，避免无 Content-Length 的
# 请求靠 read(-1) 读到 EOF 才返回（会破坏 keep-alive）及无界内存占用。
MAX_BODY = 10 * 1024 * 1024

# 流式转发时为响应缓存缓冲的 body 上限。超过此大小不再缓冲(放弃缓存该响应),
# 避免大响应把内存撑爆。缓存的目的是命中幂等小响应,大文件本就不该缓存。
STREAM_CACHE_LIMIT = 1 * 1024 * 1024  # 1 MiB

# 后台 flush 周期(秒):把内存里累积的胜出统计/元数据批量落盘。
FLUSH_INTERVAL = 5.0

# 可缓存的响应状态码。除 2xx 外,纳入幂等的 3xx 重定向(301/302/304)与
# 404/410——这些对象在真实流量中占比高且高度幂等:真实源站对随机路径常回
# 301/302/404,原"仅 2xx"策略使 HTTP 响应缓存对真实流量几乎完全失效
# (压测见 http_cache_entries_end 恒为 0)。5xx/4xx 中仅 404/410 安全可缓存
# (其余如 502 可能瞬态,缓存会放大故障)。
CACHEABLE_STATUS = frozenset({200, 203, 300, 301, 302, 304, 404, 410})

# 会改写资源的请求方法。命中即失效该 URL 的 GET 响应缓存,使随后的 GET
# 回源拿到变更后的内容,而不是 60s TTL 内返回变更前的旧响应体。缓存键为
# "GET:<url>",写方法不会被缓存(_http_cache_set 对非 GET 直接 return),
# 故失效只需按同一 URL 删 GET 条目;无需前缀扫描。
_INVALIDATING_METHODS = frozenset({'POST', 'PUT', 'DELETE', 'PATCH'})

# 在途 GET 去重聚合的等待超时(秒)。waiter 发现同 URL 有在途请求时 await 其
# Future;超过该阈值仍未完成(首个请求上游慢)则放弃聚合、自行竞速,避免在
# 慢上游下 waiter 长时间挂住连接导致 fd 堆积(压测曾观测 fd_peak 冲到 300+)。
# 200ms→100ms:降低超时开销对 P99 长尾的影响(rate 场景 P99 均值 275ms 中
# 约 200ms 来自超时等待),代价是聚合窗口更短、高频并发下聚合率略降。
_AGG_WAIT_TIMEOUT = 0.1

# 败者清理后台 task 的软上限。超过则在 _race 里就地排空一次,防止持续高吞吐
# 下 _pending_cleanups 无界堆积(soak 模式曾观测 fd_peak 冲到 569)。
_MAX_PENDING_CLEANUPS = 64

# 错峰启动(staggered start,RFC 8305 §5)的配置下限。
# 默认间隔 250ms,下限 100ms(绝对值下限 10ms,防止丢包率高时拥塞崩溃),上限 2s。
# stagger_interval 由 __init__ 钳制到此区间,配置传 0/负值时落到默认。
_STAGGER_DEFAULT_MS = 250
_STAGGER_MIN_MS = 100
_STAGGER_ABS_MIN_MS = 10
_STAGGER_MAX_MS = 2000

# 熔断器默认参数。连续失败 _CIRCUIT_THRESHOLD 次后熔断,退避期 circuit_until 按
# 指数增长(初始 1s,每次翻倍,上限 _CIRCUIT_MAX_BACKOFF)。退避期内该代理不参与
# 竞速/单发。退避到期后置 started_at=now 进入 slow-start:排序垫底、低权重,累计
# _SLOW_START_SUCCESS 次成功(或窗口期满)才恢复完整权重,防冷启动被打懵。
_CIRCUIT_THRESHOLD = 3
_CIRCUIT_MAX_BACKOFF = 300.0
_SLOW_START_WINDOW = 60.0
_SLOW_START_SUCCESS = 3
# 后台探活周期(秒)与 canary 目标。0=关闭主动探活(仅真实请求驱动熔断)。
_PROBE_INTERVAL_DEFAULT = 30.0
_PROBE_CANARY_DEFAULT = "1.1.1.1:443"
_PROBE_TIMEOUT = 4.0

# 加权 least-request 的在途积压惩罚指数(bias,默认 1.0)。
# 排序权重 = ewma × (1 + active)^bias,即分析 doc 2.2 的 "peak EWMA"(最近 RTT ×
# 在途数):在途积压多的代理即使延迟历史最快,有效权重也被抬高、排序靠后,竞速选批
# 天然避开——保护慢代理不被打爆(Envoy LeastRequest 的 weight/(active+1)^bias
# 公式的对偶,此处以乘法形式作用于延迟权重)。bias=0 时退化为纯 EWMA 排序。
_LB_BIAS_DEFAULT = 1.0

# Hop-by-hop 请求头：只服务于"客户端→本代理"这一跳，绝不能转发给上游。
# 特别是 Proxy-Authorization——若把客户端访问本代理的凭据透传到上游，
# 上游 Squid 会用它校验缓存对象访问权限（ERR_CACHE_ACCESS_DENIED），
# 误返回 407 + Proxy-Authenticate，导致浏览器弹用户名密码框。
_HOP_BY_HOP_REQUEST_HEADERS = frozenset({
    'proxy-authorization', 'connection', 'proxy-connection', 'keep-alive',
    'te', 'trailer', 'transfer-encoding', 'upgrade',
})

# 响应侧需剔除/重写的头:hop-by-hop 头由代理自身管理;content-length 因流式
# 转发按实际写入字节数重算而剔除;content-encoding 保留(aiter_raw 给的是
# 已编码的原始字节,与上游 Content-Length 语义一致,故保留编码头更安全)。
_HOP_BY_HOP_RESPONSE_HEADERS = frozenset({
    'transfer-encoding', 'content-length', 'connection', 'keep-alive',
    'proxy-connection', 'te', 'trailer', 'upgrade',
})


def _hb(v: str) -> bytes:
    """编码一个响应头字符串为 HTTP 线上的原始字节(lossless)。

    httpx 的 Headers.encoding 启发式按 ascii → utf-8 → iso-8859-1 依次尝试,
    把上游的原始字节解码成 str:纯 ASCII 走 ascii,含合法 UTF-8 多字节的走
    utf-8(如响应头里的中文字符串),其余按 iso-8859-1(每字节映射一码点)。
    latin-1 与 iso-8859-1 完全等价,但 utf-8 解码出的字符串含 >255 的码点,
    直接 .encode('latin-1') 会抛 UnicodeEncodeError(生产实测:上游带中文的
    Server/Set-Cookie 头 → 竞速胜出后整请求失败)。先按 latin-1 编码(覆盖
    iso-8859-1 分支),失败则回退 utf-8(覆盖 utf-8 分支,字节可无损还原)。
    """
    try:
        return v.encode('latin-1')
    except UnicodeEncodeError:
        return v.encode('utf-8')


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


class Router:
    """代理路由器:监听端口、处理客户端连接、竞速转发、维护统计与缓存。

    生命周期:start() 开始监听 → handle_client 处理每个连接 → stop() 优雅关闭。
    """

    def __init__(self, proxy_store: ProxyStore, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3, db_path: str = "auto_squid.db", cache_ttl: int = 600, enable_local_racing: bool = False, auth_enabled: bool = False, auth_username: str = "", auth_password: str = "", enable_http_cache: bool = True, http_cache_ttl: int = 60, http_cache_max_entries: int = 10_000, http_cache_max_bytes: int = 256 * 1024 * 1024, http_cache_stream_limit: int = 1 * 1024 * 1024, stickiness_enabled: bool = False, stickiness_ttl: int = 1800, stickiness_recheck_hits: int = 100, stickiness_max_entries: int = 100_000, stagger_start: bool = True, stagger_initial: int = 1, stagger_interval_ms: int = _STAGGER_DEFAULT_MS, probe_interval_sec: float = _PROBE_INTERVAL_DEFAULT, probe_canary: str = _PROBE_CANARY_DEFAULT, probe_canaries: Optional[List[Dict[str, Any]]] = None, circuit_threshold: int = _CIRCUIT_THRESHOLD, circuit_max_backoff: float = _CIRCUIT_MAX_BACKOFF, slow_start_window: float = _SLOW_START_WINDOW, slow_start_success: int = _SLOW_START_SUCCESS, lb_bias: float = _LB_BIAS_DEFAULT, single_send_degrade_fail: int = 0, single_send_degrade_ratio: float = 0.0, single_send_degrade_slack_ms: float = 0.0, policies: Optional[List[PolicyConfig]] = None, adaptive_ttl: bool = False, adaptive_ttl_min: float = 60.0, adaptive_ttl_max: float = 1800.0, switch_damping: bool = False, switch_damping_min_wins: int = 2, switch_damping_ratio: float = 0.8, switch_damping_abs_ms: float = 30.0, concurrency_limit_enabled: bool = False, concurrency_limit_initial: int = 16, concurrency_limit_min: int = 2, concurrency_limit_max: int = 128, concurrency_add_on_success: int = 4, concurrency_mult_on_failure: float = 0.5, concurrency_failure_window: int = 20, conn_pool_enabled: bool = False, conn_pool_per_proxy: int = 4, conn_pool_total: int = 64, conn_pool_idle_timeout: float = 30.0, conn_pool_refill_interval: float = 5.0, conn_pool_refill_target: int = 2, conn_pool_connect_timeout: float = 10.0, conn_pool_target_prewarm: bool = False, conn_pool_refill_pause_minutes: float = 60.0, conn_pool_refill_pause_silence_sec: float = 120.0, conn_pool_established_reuse: bool = False):
        """构造路由器。

        参数:
            proxy_store:         上游代理注册表。
            listen_host/port:    代理监听地址/端口(面向客户端)。
            max_retries:         竞速首批并行的代理数量;失败后对剩余代理再竞速。
            db_path:             SQLite 文件路径(域名统计/元数据持久化)。
            cache_ttl:           域名缓存有效期(秒)。
            enable_local_racing: 让本机作为代理节点直接参与竞速。
            auth_enabled:        是否要求客户端 HTTP Basic 认证。
            auth_username/password: 客户端认证的预期凭据。
            stickiness_enabled:  是否启用会话粘性(同客户端+域名复用同一代理)。
            stickiness_ttl:      会话粘性有效期(秒),粘性命中成功滑动刷新。
            stickiness_recheck_hits: 粘性命中 N 次后触发探路重竞速(0=关闭)。
            stickiness_max_entries: 粘性表最大条目数,超出驱逐最旧(内存保护)。
            stagger_start:       是否启用错峰启动(RFC 8305 §5)。竞速首批不再同时全发,
                                 先发最优 stagger_initial 个,间隔 stagger_interval_ms
                                 补发下一个;首个首字节成功即取消其余。显著减少 CONNECT
                                 隧道扇出与 HTTP 双写流量。默认 True(启用错峰)。
            stagger_initial:     错峰首批并发数(必须 >= 1;经 max_retries 钳制)。
                                 有历史 RTT 时可设 2 同时赌两个最优者(RFC 8305 §5 允许)。
            stagger_interval_ms: 相邻候选的启动间隔(毫秒),钳制到 [100, 2000]
                                 (RFC 8305 §5 下限 100ms/绝对值 10ms、上限 2s)。
            probe_interval_sec: 后台探活周期(秒)。每周期对 enabled 代理做轻量
                                CONNECT 到 probe_canary + 关闭,计延迟/成败 →
                                更新 EWMA 与熔断计数。0=关闭主动探活(仅真实请求
                                驱动熔断)。默认 30。
            probe_canary:       探活目标 "host:port"。轻量 CONNECT 只验证上游可达
                                与建连延迟,域名级最终仍由竞速决定。
            circuit_threshold:  连续失败多少次触发熔断(默认 3)。真实请求失败与
                                探活失败共享计数。
            circuit_max_backoff: 熔断退避上限(秒,默认 300)。退避指数增长:1s → 2s
                                → 4s → ... 直到此上限。
            slow_start_window:  slow-start 爬升窗口(秒,默认 60)。熔断退避到期后
                                该代理在此窗口内低权重垫底。
            slow_start_success: slow-start 恢复期内累计成功多少次后恢复完整权重
                                (默认 3)。
            lb_bias:            加权 least-request 的在途惩罚指数(默认 1.0)。竞速
                                排序权重 = ewma × (1 + active)^bias,在途积压多的
                                代理即使延迟历史最快也被压低排序,保护慢代理不被打爆
                                (Envoy LeastRequest / Dubbo LeastActive)。bias=0
                                退化为纯 EWMA 排序。
            single_send_degrade_fail: 单发降级:连续失败阈值(默认 0=关闭)。域名缓存/
                                粘性命中的代理连续失败达该值,即使未到熔断阈值也
                                视作"不稳定",单发路径主动降级回竞速。
            single_send_degrade_ratio: 单发降级:EWMA 恶化阈值(默认 0=关闭)。被钉住
                                代理的当前 EWMA 相对钉住时基线的比值超过该值(如 3.0
                                = 延迟恶化 3 倍)即降级回竞速。0=只按连续失败降级。
            single_send_degrade_slack_ms: EWMA 降级的绝对下限(毫秒)。基线与当前值
                                都极小时(如 0.2ms→0.9ms,比值 4.5 但绝对差距 <1ms)
                                用纯比值会误判剧烈恶化——绝对差值低于该 slack 时
                                即使比值超阈值也不降级(默认 10)。
            policies:           策略路由:按目标域名(后缀/精确/正则)命中第一条
                                策略,把候选代理集收窄到该策略允许的 tags/ids 子集
                                (作用于竞速、域名缓存、粘性,三者一致)。
            http_cache_ttl:     HTTP 响应缓存条目有效期(秒),命中滑动刷新。
            http_cache_max_entries: 缓存条目数硬上限,超限按 LRU 淘汰最久未访问。
            http_cache_max_bytes:   缓存总字节(body)上限,超限按 LRU 淘汰。
            http_cache_stream_limit: 单条响应 body 缓冲上限(字节),超过放弃缓存。
            adaptive_ttl:       启用自适应域名缓存 TTL(默认关闭)。开启后每域名
                                TTL 按稳定度升降:连续同代理胜出 → TTL 上浮
                                (上限 adaptive_ttl_max);单发降级/换赢家/熔断类
                                故障 → TTL 回落(下限 adaptive_ttl_min)。
            adaptive_ttl_min:   自适应 TTL 下限(秒,默认 60)。
            adaptive_ttl_max:   自适应 TTL 上限(秒,默认 1800)。
            switch_damping:     启用域名赢家切换阻尼(默认关闭)。新赢家不能因单次
                                竞速抖动就替换稳定域名赢家,需连续胜出
                                switch_damping_min_wins 次,或 EWMA 显著优于旧赢家
                                (switch_damping_ratio 比例 / switch_damping_abs_ms
                                绝对毫秒)才立即替换。降低出口 IP 抖动。
            switch_damping_min_wins: 新赢家需连续胜出次数(默认 2)。
            switch_damping_ratio: 新赢家 EWMA ≤ 旧×该比例即立即切换(默认 0.8)。
            switch_damping_abs_ms: 新赢家快 ≥ 该毫秒即立即切换(默认 30)。
            concurrency_limit_enabled: 启用自适应并发限制(默认关闭)。每代理
                                并发上限成功加性增/失败乘性降,在途达上限的代理
                                不参与竞速候选,防慢代理被请求堆死。
            concurrency_limit_initial/min/max: 每代理并发上限的初始/下限/上限。
            concurrency_add_on_success: 成功且稳定时加性提升上限(默认 +4)。
            concurrency_mult_on_failure: 失败时乘性降低上限(默认 0.5)。
            concurrency_failure_window: 成功观测窗口(达标才提升上限)。
            conn_pool_enabled:   启用 CONNECT 上游 TCP 预热池(默认关闭)。为每
                                上游维护少量空闲 TCP,CONNECT 到来优先取池中
                                socket 再发 CONNECT target,省"本机→上游"建连。
            conn_pool_per_proxy: 每代理预热连接数上限。
            conn_pool_total:     全局预热连接数上限(fd 预算)。
            conn_pool_idle_timeout: 空闲连接超时(秒),超时未取用则关闭。
            conn_pool_refill_interval: 后台补充周期(秒),0=只取不补。
            conn_pool_refill_target: 每代理保持的空闲连接数目标。
            conn_pool_connect_timeout: 预热/取用建连超时(秒)。
            conn_pool_target_prewarm: 第二阶段(CONNECT 目标半预连接)。命中域名
                                缓存/粘性的高频 CONNECT target 在后台提前建立
                                "到上游代理"的 TCP(不提前 CONNECT 到目标),按
                                (proxy, target) 键区分,下次命中直接复用该 TCP
                                发 CONNECT,进一步压低 HTTPS 短连接 TTFB。与
                                第一阶段共享 per-proxy/全局 fd 预算/空闲超时。
            conn_pool_refill_pause_minutes: 空闲暂停(分钟,默认 60)。连续 N 分钟
                                无客户端请求时,挂起后台 refill/目标预热,避免
                                深夜空闲期"建了又过期"的空转浪费(生产实测:6 代理
                                深夜 6h 白建 ~1400 条连接,100% 超时被清)。新请求
                                到来立即恢复补充。
            conn_pool_refill_pause_silence_sec: 活动判定静默窗口(秒,默认 120)。
                                生产实测后台心跳(GitHub Desktop 的 alive.github.com
                                等,间隔 3-10 分钟)会持续刷新"距上次请求",使空闲
                                暂停永不触发。活动判定改为"密集请求":仅当距上次
                                请求 ≤ 本窗口才视为活动并刷新时间戳;间隔更大的
                                孤立请求(心跳)不刷新。0=任意请求都刷新(旧行为)。
            conn_pool_established_reuse: 已建握手隧道复用(默认关闭)。隧道结束
                                时若连接干净(无残留数据),归还 _established_pool
                                而非关闭;下次同 (proxy, target) 请求直接复用已
                                CONNECT 握手的连接,跳过 CONNECT 发送+200 校验,
                                省掉重建。仅当 conn_pool_enabled 时生效。
        """
        self.proxy_store = proxy_store
        self.selector = ProxySelector(
            proxy_store,
            circuit_threshold=circuit_threshold,
            circuit_max_backoff=circuit_max_backoff,
            slow_start_window=slow_start_window,
            slow_start_success=slow_start_success,
            lb_bias=lb_bias,
            concurrency_limit_enabled=concurrency_limit_enabled,
            concurrency_limit_initial=concurrency_limit_initial,
            concurrency_limit_min=concurrency_limit_min,
            concurrency_limit_max=concurrency_limit_max,
            concurrency_add_on_success=concurrency_add_on_success,
            concurrency_mult_on_failure=concurrency_mult_on_failure,
            concurrency_failure_window=concurrency_failure_window)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.max_retries = max_retries
        # ── 熔断器 + 探活 + slow-start ────────────────────────────
        # 连续失败达阈值 → 指数退避熔断;退避期内不参与竞速/单发。真实请求失败
        # 与后台探活共享同一连续失败计数(见 selector.record_failure)。探活每
        # probe_interval_sec 对 enabled 代理做轻量 CONNECT 到 canary + 关闭,
        # 喂 EWMA 与熔断计数;0=关闭主动探活。退避到期 → slow-start 低权重爬升。
        self.probe_interval_sec = probe_interval_sec
        self.probe_canary = probe_canary
        # ── 多 canary / 按标签探活(P2)─────────────────────────
        # probe_canaries 配置后替代单 canary:每个代理按 tags 命中第一条匹配的
        # canary(无 tags 的 canary 为兜底),未命中任何 → 用全局 probe_canary。
        # 结构:[{"name": str, "target": "host:port", "tags": {k:v}},...]
        self.probe_canaries: List[Dict[str, Any]] = [
            dict(c) for c in (probe_canaries or []) if c.get("target")
        ]
        self._probe_task: Optional[asyncio.Task] = None
        self.probes_sent = 0
        self.probes_ok = 0
        self.probes_skipped = 0  # 因本机→canary 不可达(环境原因)而跳过的探活次数
        self.probes_failed = 0   # 经上游 CONNECT 失败的探活次数(上游侧真故障)
        # 熔断开启计数归 ProxySelector 维护(开启时刻在 record_failure 内),经
        # snapshot_counters 经 selector.circuit_open_count 读取。
        self.enable_local_racing = enable_local_racing
        self.enable_http_cache = enable_http_cache
        # ── 错峰启动(RFC 8305 §5)──
        # 竞速首批不再同时全发:先发最优 stagger_initial 个,间隔 stagger_interval_ms
        # 补发下一个,首个首字节成功即取消其余。interval 钳制到 RFC 8305 参数区间
        # (默认 250ms、下限 100ms、绝对值下限 10ms、上限 2s),防配置越界破坏竞速。
        self.stagger_start = stagger_start
        self.stagger_initial = max(1, min(max_retries, stagger_initial))
        if stagger_interval_ms <= 0:
            stagger_interval_ms = _STAGGER_DEFAULT_MS
        self.stagger_interval = max(_STAGGER_MIN_MS,
                                    min(_STAGGER_MAX_MS, stagger_interval_ms)) / 1000.0
        self.auth_enabled = auth_enabled
        self.auth_username = auth_username
        self.auth_password = auth_password
        self._server: Optional[asyncio.AbstractServer] = None
        # 跟踪所有正在处理的客户端连接 task，供 stop() 在关闭 DB 前取消并等待。
        self._running_tasks: set[asyncio.Task] = set()
        self.request_counts: dict[str, int] = {}
        self.attempted_counts: dict[str, int] = {}
        self.cache_ttl = cache_ttl
        # 自适应域名缓存 TTL(P2):开启后每域名 TTL 按稳定度升降。
        self.adaptive_ttl_enabled = adaptive_ttl
        self.adaptive_ttl_min = max(1.0, adaptive_ttl_min)
        self.adaptive_ttl_max = max(self.adaptive_ttl_min, adaptive_ttl_max)
        # ── 域名赢家切换阻尼(P3)──────────────────────────────
        # 开启后新赢家不能因单次竞速抖动就替换稳定域名赢家:需连续胜出
        # switch_damping_min_wins 次,或 EWMA 显著优于旧赢家(比例/绝对毫秒)
        # 才立即替换。对 5xx/熔断类故障跳过阻尼立即切换。降低出口 IP 抖动。
        self.switch_damping_enabled = switch_damping
        self.switch_damping_min_wins = max(1, int(switch_damping_min_wins))
        self.switch_damping_ratio = max(0.0, float(switch_damping_ratio))
        self.switch_damping_abs_ms = max(0.0, float(switch_damping_abs_ms))
        # 每域名"新赢家候选连续胜出计数"与"被阻尼的替换次数(可观测)"。
        self._damping_wins: dict[str, dict[str, int]] = {}   # domain -> {pid: consecutive}
        self.switch_damping_blocks = 0   # 被阻尼挡下的替换次数
        self.switch_damping_fast_swaps = 0  # 因 EWMA 显著优势直接切换的次数
        # ── 会话粘性 ────────────────────────────────────────────
        # 键 = "{client_ip}|{domain}",值 = {"proxy_id": pid, "updated_at": ts}。
        # 纯内存、滑动 TTL:同一客户端+域名复用上次胜出的代理,保持 egress IP
        # 稳定;粘性代理失败则驱逐并回落竞速(redispatch)。仿 _meta_cache 模式,
        # 但不落盘(粘性是瞬态,重启即清)。
        self.stickiness_enabled = stickiness_enabled
        self.stickiness_ttl = stickiness_ttl
        self.stickiness_recheck_hits = stickiness_recheck_hits
        self.stickiness_max_entries = stickiness_max_entries
        self._sticky_cache: dict[str, dict[str, object]] = {}
        self.sticky_cache_hits = 0
        self.sticky_evictions = 0       # 粘性表驱逐次数(5xx/失败/超容量)
        # 单发降级触发次数(Goal #6,供 /metrics / 仪表盘观察降级活动)。
        self.single_send_degrades = 0
        # ── 单发降级(质量感知的确定性探路,Goal #6)─────────────────
        # 域名缓存/粘性命中单发时,若被钉住代理"最近失败率上升(连续失败)"
        # 或"EWMA 恶化(相对钉住时基线)",主动降级回竞速——把确定性探路从
        # recheck_hits 的纯命中计数升级为 EWMA 感知的"不稳定即重竞速"。
        # 任一阈值为 0(默认)即关闭对应维度的降级。见 _single_send_degraded。
        self.single_send_degrade_fail = max(0, int(single_send_degrade_fail))
        self.single_send_degrade_ratio = max(0.0, float(single_send_degrade_ratio))
        self.single_send_degrade_slack_ms = max(0.0, float(single_send_degrade_slack_ms))
        # "降级中"代理集合(可观测,非门控):被单发降级判定命中的代理记录于此。
        # 注意真正的门控是每次选择时实时重估 _single_send_degraded(代理恢复后立即
        # 重新可单发,无需冷却),此集合只供 /metrics /circuit 展示"当前被判定降级的
        # 代理";由 _record_win_meta(新赢家接管)或 reset_proxy_quality 清除。
        self._degraded_single_send: set[str] = set()
        # ── 策略路由(P1)───────────────────────────────────────
        # 按目标域名收窄候选代理集。不配置(policies 为空)→ 对所有 enabled
        # 代理统一竞速,等价旧行为。预编译正则避免每请求重编译;条目为
        # (policy_index, compiled_regex),匹配时按该索引取对应策略。
        self._policies = [p for p in (policies or []) if p.match is not None]
        self._policy_regexes: List[Tuple[int, re.Pattern]] = []
        for idx, pol in enumerate(self._policies):
            for pat in pol.match.domain_regex or []:
                try:
                    self._policy_regexes.append((idx, re.compile(pat)))
                except re.error:
                    logger.warning("ignoring invalid domain_regex %r in policy %d", pat, idx)
        # ── 服务端性能计数器 ────────────────────────────────────
        # 供压测经 /metrics 跨进程读取,在两种上游模式(mock/real)下统一计算
        # 缓存命中率与竞速放大率——不再依赖 mock 上游的 hit_count(那只对 mock
        # 模式有效)。纯内存整数,热路径 +1,无锁无 I/O。
        self.http_cache_hits = 0       # 响应缓存命中(完全不经上游)
        self.http_cache_misses = 0     # 进入 HTTP 处理但未命中响应缓存
        self.domain_cache_hits = 0     # 域名缓存命中(单发上游,跳过竞速)
        self.racing_invocations = 0    # 触发竞速的请求数(含首批 + 兜底批)
        self.upstream_attempts = 0     # 竞速扇出总尝试数(每个 _try_http/_try_tunnel +1)

        # ── 上游连接池 ──────────────────────────────────────────
        # 每个"代理标识"维护一个长驻 httpx.AsyncClient,跨请求复用 keep-alive
        # 连接。键为 pid(含 'local'),故同一上游代理在所有请求间共享一个池。
        # 连接以 check_same_thread=False 跨线程共享,但实际只在事件循环线程
        # 读写(_flush_loop 是另一个 task,只碰 DB 缓存,不碰 client 池)。
        self._client_pool: dict[str, httpx.AsyncClient] = {}
        # 每请求整体超时(秒):连接/池获取设短以快速判负,读首字节给 10s。
        # 注:曾尝试用 _RACE_HEADER_TIMEOUT + asyncio.wait_for 包裹 send 来独立
        # 收紧 header 等待,但 real-upstream 压测四份对比证明它在 p50 与 p95 间是
        # 权衡而非净赢(且有坏点:5s 配置引爆 soak p99 + fd 堆积),故回退,保留
        # 原超时。尾延迟治理改由 Phase 2a(败者清理下放后台)承担,不带超时权衡。
        self._upstream_timeout = httpx.Timeout(10.0, connect=5.0, pool=5.0, read=10.0, write=10.0)

        # ── CONNECT 上游 TCP 预热池(P1)────────────────────────
        # 每个上游维护少量空闲 TCP 连接,CONNECT 请求到来时优先取池中 socket
        # 再发 CONNECT target,省掉"本机→上游代理"的建连 TTFB。隧道结束后该
        # 连接不可复用到新 target(CONNECT 后 socket 已被隧道占用),但池可提前
        # 补充下一条空闲连接。资源约束:per-proxy 上限 + 全局 fd 预算(total)+
        # 空闲超时,防泄漏。后台 refill task 周期性补充到目标水位。
        self.conn_pool_enabled = conn_pool_enabled
        self.conn_pool_per_proxy = max(1, conn_pool_per_proxy)
        self.conn_pool_total = max(1, conn_pool_total)
        self.conn_pool_idle_timeout = max(1.0, conn_pool_idle_timeout)
        self.conn_pool_refill_interval = max(0.0, conn_pool_refill_interval)
        self.conn_pool_refill_target = max(0, min(self.conn_pool_per_proxy, conn_pool_refill_target))
        self.conn_pool_connect_timeout = max(1.0, conn_pool_connect_timeout)
        # 空闲暂停:连续 N 分钟无客户端请求则挂起 refill/目标预热,避免深夜空闲
        # 期"建了又过期"的空转。0=不暂停。新请求到来立即恢复。
        self.conn_pool_refill_pause_minutes = max(0.0, conn_pool_refill_pause_minutes)
        # 活动判定静默窗口(秒):仅当距上次请求 ≤ 该窗口才视为"密集活动"并刷新
        # 活动时间戳。后台心跳(GitHub Desktop 的 alive.github.com / Windows 的
        # client.wns.windows.com 等,间隔 3-10 分钟)间隔大于窗口,不会刷新——
        # 防止心跳阻止空闲暂停触发。0=任意请求都刷新(旧行为)。
        self.conn_pool_refill_pause_silence_sec = max(0.0, conn_pool_refill_pause_silence_sec)
        # 最近一次"密集客户端请求"到达时间(monotonic 秒)。由 _record_request_
        # activity() 在每个有效请求(通过认证的 HTTP/CONNECT 首行)上按静默窗口
        # 判定后更新。初始为当前时刻:新路由器 60 分钟内不暂停(refill 照常),
        # 与旧语义一致;进程长时间运行后,后台心跳(间隔 3-10 分钟)不刷新活动
        # 时间戳,自然进入空闲暂停。
        self._last_request_activity = time.monotonic()
        # 第二阶段(CONNECT 目标半预连接):需 conn_pool_enabled 且显式开启。
        self.conn_pool_target_prewarm = bool(conn_pool_target_prewarm)
        # 已建握手隧道复用:隧道结束时连接干净则归还而非关闭,同 (proxy,target)
        # 复用跳过 CONNECT 握手。需 conn_pool_enabled 且显式开启。
        self.conn_pool_established_reuse = bool(conn_pool_established_reuse)
        # {proxy_url: [StreamWriter,...]} —— 空闲预热连接。只由事件循环线程读写。
        self._conn_pool: dict[str, list] = {}
        self.conn_pool_creates = 0      # 累计预热建连次数
        self.conn_pool_hits = 0         # 池中取用成功次数
        self.conn_pool_misses = 0       # 取池未中需新建的次数
        self.conn_pool_expired = 0      # 空闲超时关闭次数
        self._conn_pool_task: Optional[asyncio.Task] = None
        # {proxy_host:port|target: [StreamWriter,...]} —— 按 CONNECT target 半预
        # 连接的"到上游代理"TCP(P2,见 _target_pool_*)。命中域名缓存/粘性的
        # target 后台预热,取用时优先于第一阶段通用池。共享 fd 预算/空闲超时。
        self._target_pool: dict[str, list] = {}
        self.target_pool_creates = 0    # 累计 target 半预建连次数
        self.target_pool_hits = 0       # target 预连接取用成功次数
        self.target_pool_misses = 0     # 取 target 池未中需新建的次数
        self.target_pool_expired = 0    # target 预连接空闲超时关闭次数
        self.target_prewarm_dispatched = 0  # 后台预热协程发起次数
        self.target_prewarm_success = 0     # 预热建连成功次数
        self.target_prewarm_failed = 0      # 预热建连失败次数
        # 已建握手隧道池(P3-6):{ "proxy_host:port|target": [(reader, writer)] }。
        # 存"已发 CONNECT 且收到 200"的隧道连接,隧道结束若连接干净则归还,
        # 下次同 (proxy, target) 直接复用,跳过 CONNECT 握手。与 _target_pool
        # 区分:后者是"未发 CONNECT 的裸 TCP",取用后必须重新握手。
        self._established_pool: dict[str, list] = {}
        self.established_pool_hits = 0      # 已握手连接取用成功次数
        self.established_pool_misses = 0    # 取已握手池未中需新建/新建握手的次数
        self.established_pool_expired = 0   # 已握手连接空闲超时关闭次数
        self.established_pool_returned = 0  # 隧道结束归还次数
        # 观测(P1 先观测后实现):CONNECT 到上游的新建 TCP 连接计数(不含预热池
        # 命中)。供压测算 HTTPS 建链成本、验证预热池收益。
        self.connect_new_conns = 0

        # ── 数据持久化 ──────────────────────────────────────────
        self._db_path = db_path
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        # WAL 模式 + synchronous=NORMAL:热路径已不 commit,后台 flush 是低频
        # 单写者;WAL 让 commit 只追加 -wal 文件、把 fsync 推迟到 checkpoint,
        # 缩短 _flush_to_db 持锁时长。NORMAL 在 WAL 下安全(仅断电可能丢最后
        # 一次 flush,而 flush 是幂等全量覆盖,下次启动可补齐)。
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        # 后台 flush task 与 FastAPI 线程池都可能触达 DB;同一连接的并发使用
        # 非线程安全,用锁串行化所有 DB 写入,避免 "database is locked"。
        # 热路径(转发)只读写下方内存缓存,不经此锁。
        self._db_lock = threading.Lock()
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS domain_stats (
                domain TEXT NOT NULL,
                proxy_id TEXT NOT NULL,
                wins INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (domain, proxy_id)
            )
        """)
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS domain_meta (
                domain TEXT NOT NULL PRIMARY KEY,
                default_proxy TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                ref_ewma REAL
            )
        """)
        # 迁移:老库的 domain_meta 无 ref_ewma 列(GOAL #6 之前)。CREATE TABLE IF NOT
        # EXISTS 不会给已存在的表补列,这里检查 PRAGMA 并 ALTER ADD COLUMN,保证
        # 既有部署升级后启动不崩(老行 ref_ewma 为 NULL,降级判定按"无基线"处理)。
        cols = {row[1] for row in self._db.execute("PRAGMA table_info(domain_meta)")}
        if "ref_ewma" not in cols:
            self._db.execute("ALTER TABLE domain_meta ADD COLUMN ref_ewma REAL")
        self._db.commit()

        # 内存镜像:热路径(每请求查域名缓存)只读这两份内存,不经 DB/锁。
        # _meta_cache: {domain: {'default_proxy': pid, 'updated_at': ts}}
        # _stats_cache: {domain: {pid: wins}}(内存累加,后台 flush 落盘)
        self._meta_cache: dict[str, dict[str, str]] = {}
        self._stats_cache: dict[str, dict[str, int]] = {}
        # ── 自适应域名缓存 TTL(P2)──────────────────────────────
        # 每域名独立 TTL,按稳定度升降(见 _domain_ttl)。状态与 _meta_cache
        # 并列维护:meta 负责"当前赢家/时间",这里负责"该域名缓存多久过期"。
        # 稳定域名 TTL 上浮(减少竞速),抖动域名 TTL 下调(更快换路)。
        self._domain_ttl_cache: dict[str, float] = {}      # domain -> 当前 TTL(秒)
        self._domain_switch_count: dict[str, int] = {}     # domain -> 切换赢家次数
        self._domain_last_pid: dict[str, str] = {}         # domain -> 上次赢家 pid
        self.domain_ttl_grows = 0       # TTL 上调次数(可观测)
        self.domain_ttl_resets = 0      # TTL 下调/重置次数(可观测)
        self._load_caches_from_db()
        # _stats_dirty / _meta_dirty 标记自上次 flush 后是否有变更。
        self._stats_dirty = False
        self._meta_dirty = False
        self._flush_task: Optional[asyncio.Task] = None
        # 竞速中"败者清理"(aclose 流式 resp / 关上游裸连接)被下放到后台 task,
        # 不阻塞赢家首字节(见 _race / _drain_losers)。stop() 收尾时排空,防泄漏。
        self._pending_cleanups: set = set()

        # ── HTTP 响应缓存 ───────────────────────────────────────
        # P2:LRU + 容量上限。普通 dict 升级为 OrderedDict + 访问时间戳:
        #   - _http_cache_get 命中时刷新 last_access(滑动 TTL + LRU 顺序);
        #   - _http_cache_set 写入前检查 max_entries / max_bytes,超限淘汰
        #     last_access 最旧的条目(避免高基数 URL 下内存无界增长)。
        # 二级索引 _http_cache_domain_index 与淘汰同步维护,防漏删。
        self._http_cache: dict[str, dict] = {}
        self._http_cache_ttl = max(1, http_cache_ttl)
        self._http_cache_max_entries = max(1, http_cache_max_entries)
        self._http_cache_max_bytes = max(1, http_cache_max_bytes)
        # 流式转发时响应 body 的缓冲上限(超过放弃缓存该响应)。原为模块常量
        # STREAM_CACHE_LIMIT,现由配置 http_cache.stream_cache_limit 控制。
        self._stream_cache_limit = max(1024, http_cache_stream_limit)
        self._http_cache_bytes = 0           # 当前缓存 body 总字节数
        self.http_cache_evictions = 0        # 累计淘汰次数(LRU 或 TTL)
        # 二级索引: domain → set[缓存键]。使 _http_cache_invalidate 从 O(N) 降为
        # O(K)(K=该域名条目数)。_http_cache_set 写入时同步更新,_http_cache_get
        # 过期清除时同步删除。索引与主 dict 无锁(均在同一个 asyncio 线程)。
        self._http_cache_domain_index: dict[str, set[str]] = {}
        # 在途 GET 去重聚合(Cache Stampede Protection): key = _http_cache_key('GET', url)
        # → 该 URL 首个转发上游的请求持有的 Future。同 URL 并发 GET 发现已有在途请求
        # 则 await 该 Future 拿首个请求的结果,不重复发上游。Future 结果为
        # (status_code, reason_phrase, headers, content) 或 None(上游失败,waiter 自行竞速)。
        self._inflight_futures: dict[str, asyncio.Future] = {}

    # ── DB helpers ──────────────────────────────────────────────

    @staticmethod
    def _now_utc() -> str:
        """当前 UTC 时间的 ISO-8601 字符串(用于 domain_meta.updated_at)。"""
        return datetime.now(timezone.utc).isoformat()

    def _load_caches_from_db(self):
        """启动时一次性把 domain_stats / domain_meta 载入内存镜像。

        之后热路径只读写内存,不再每请求查 DB。载入在构造期同步完成,此时
        事件循环尚未启动,无需异步化。
        """
        with self._db_lock:
            stats_rows = self._db.execute(
                "SELECT domain, proxy_id, wins FROM domain_stats").fetchall()
            meta_rows = self._db.execute(
                "SELECT domain, default_proxy, updated_at, ref_ewma FROM domain_meta").fetchall()
        self._stats_cache = {}
        for domain, pid, wins in stats_rows:
            self._stats_cache.setdefault(domain, {})[pid] = wins
        self._meta_cache = {
            domain: {"default_proxy": dp, "updated_at": ua,
                     "ref_ewma": (float(ewma) if ewma is not None else None)}
            for domain, dp, ua, ewma in meta_rows
        }

    def _record_attempt(self, domain: str, pid: str):
        """记录一次"代理 pid 对域名 domain 的尝试"(竞速扇出统计)。

        每个竞速候选每次尝试都调:统计的是上游命中扇出,不是"胜出"。仅更新
        内存镜像 _stats_cache 并置脏,由后台 _flush_loop 周期批量落盘。热路径
        无逐请求 INSERT/commit,避免 fsync 阻塞事件循环。不动 _meta_cache——
        meta 只应由 _record_win_meta 在确认赢家后写一次(见下),否则竞速中
        多个候选都收到响应头时会互相覆写,把域名缓存污染成被取消的败者。
        """
        per_domain = self._stats_cache.setdefault(domain, {})
        per_domain[pid] = per_domain.get(pid, 0) + 1
        self._stats_dirty = True

    def _damping_allows_switch(self, domain: str, new_pid: str) -> bool:
        """切换阻尼判定(P3):新赢家 new_pid 能否替换该域名当前赢家。

        关闭(默认)→ 恒 True(旧行为)。开启时:
          - 无当前赢家 / 同代理 / 旧赢家熔断或已删除 → 立即允许(首次钉住不算
            切换;对故障类跳过阻尼立即换路)。
          - EWMA 显著优势 → 立即允许:新 EWMA ≤ 旧 × ratio,或新 EWMA 快 ≥
            abs_ms 毫秒(fast_swap 计数)。
          - 否则需连续胜出 switch_damping_min_wins 次才允许(计数经 _damping_wins
            维护,换候选清零);未达阈值 → 阻止(switch_damping_blocks 计数)。
        """
        if not self.switch_damping_enabled:
            return True
        old_entry = self._meta_cache.get(domain)
        if not old_entry:
            return True
        old_pid = old_entry["default_proxy"]
        if old_pid == new_pid:
            return True
        # 旧赢家故障(熔断)/已删除 → 跳过阻尼立即切换。
        if old_pid != 'local':
            old_proxy = self.proxy_store.get(old_pid)
            if not old_proxy or self.selector.is_circuit_open(old_pid):
                return True
        # EWMA 显著优势 → 立即切换。
        if self.switch_damping_ratio > 0 or self.switch_damping_abs_ms > 0:
            q = self.selector.get_quality()
            new_ewma = self._proxy_quality_ewma(q.get(new_pid))
            old_ewma = self._proxy_quality_ewma(q.get(old_pid))
            if new_ewma is not None and old_ewma is not None and old_ewma > 0:
                if self.switch_damping_ratio > 0 and new_ewma <= old_ewma * self.switch_damping_ratio:
                    self.switch_damping_fast_swaps += 1
                    return True
                if self.switch_damping_abs_ms > 0 \
                        and new_ewma + self.switch_damping_abs_ms / 1000.0 <= old_ewma:
                    self.switch_damping_fast_swaps += 1
                    return True
        # 需连续胜出:维护每域名的候选胜出计数。
        per = self._damping_wins.setdefault(domain, {})
        if per.get(new_pid, 0) + 1 >= self.switch_damping_min_wins:
            self._damping_wins[domain] = {new_pid: 0}  # 已通过,清计数
            return True
        per.clear()
        per[new_pid] = 1
        self.switch_damping_blocks += 1
        return False

    def _worse_than_best(self, pid: str) -> bool:
        """方向 A:赢家回填前的质量闸——代理 EWMA 是否显著差于当前可用最优代理。

        判定:pid 有 EWMA 观测(不要求 obs>=2——竞速赢家常是刚起步的新代理,
        首胜时往往只有 1 次观测,若这里苛求 obs 数会让新慢代理绕过闸门被钉住),
        且当前可用最优代理(ordered_proxies 首位的 EWMA)有观测且 >0 时,若
        pid EWMA > 最优 EWMA × ratio(默认 2.0) 且差量 > slack
        (single_send_degrade_slack_ms,防极低延迟误判)→ 视为"显著更慢"。
        最优代理可能正是 pid 自己(此时不触发);无最优/无观测 → False。

        与 Goal #6 degrade 的语义对比:degrade 是"相对**钉住时刻**的自身基线恶化",
        这里问"相对**当前最优代理**差多少"。前者兜底"钉住的代理自己变差",后者
        兜底"赢家本就显著劣于其他代理"(竞速偶发让慢代理赢,却把它回填钉住)。
        ratio 复用 single_send_degrade_ratio(生产 2.0),语义一致:EWMA 慢 2 倍
        就认为不该单发。
        """
        q = self.selector.get_quality().get(pid)
        cur = self._proxy_quality_ewma(q)
        if cur is None:
            return False
        best = self.selector.best_proxy()
        if not best or best == pid:
            return False
        best_q = self.selector.get_quality().get(best)
        best_ewma = self._proxy_quality_ewma(best_q)
        if best_ewma is None or best_ewma <= 0:
            return False
        if self.single_send_degrade_ratio <= 0:
            return False
        slack = self.single_send_degrade_slack_ms / 1000.0
        return cur > best_ewma * self.single_send_degrade_ratio and (cur - best_ewma) > slack

    def _record_win_meta(self, domain: str, pid: str):
        """记录某域名确认的"赢家代理",更新 _meta_cache(域名→首选代理)。

        仅在竞速判定赢家(或域名缓存命中复用)后调一次。这样 _meta_cache 反映
        真正被采用的上游,而非竞速中"最后收到响应头的候选"(可能被取消)。
        更新内存镜像并置脏,由后台 _flush_loop 落盘。

        Goal #6:此处是域名缓存钉住时刻——捕获 pid 当前 EWMA 作为 ref_ewma 基线
        (供 _get_fresh_proxy 判定"相对钉住时是否恶化");同时清除 _degraded_single_send
        标记(新赢家已接管,该代理可再次被单发)。

        方向 A:竞速赢家若显著差于当前最优代理(_worse_than_best)→ **不更新**
        域名缓存。竞速偶发让慢代理赢(其他代理熔断/并发上限被踢出候选)时,避免
        把劣质赢家钉进域名缓存,让它继续走竞速直到沉淀到健康代理。

        P2 自适应 TTL:每次确认赢家时按稳定度演化该域名 TTL——
          - 同代理连续胜出(稳定)→ TTL 上浮,步进 1.5×,封顶 adaptive_ttl_max;
          - 赢家切换(抖动)→ switch_count+1,TTL 下调至 adaptive_ttl_min。
        _get_fresh_proxy 在命中时若检测到代理开始恶化(降级/熔断)也会把 TTL
        打回下限(见 _domain_ttl)。

        P3 切换阻尼:开启时若 _damping_allows_switch 判定新赢家不可替换旧赢家,
        则**不更新** _meta_cache(保持旧赢家钉住,降低出口 IP 抖动),仅记录尝试。
        """
        if not self._damping_allows_switch(domain, pid):
            return  # 阻尼拦截:保持旧赢家,不替换
        # 方向 A:竞速赢家显著差于当前最优代理 → 不钉进域名缓存,继续竞速。
        # (慢代理偶发胜出时,回填会把劣质赢家钉住导致"钉住→降级→回填"循环。)
        if self._worse_than_best(pid):
            return
        self._meta_cache[domain] = {
            "default_proxy": pid,
            "updated_at": self._now_utc(),
            "ref_ewma": self._proxy_quality_ewma(self.selector.get_quality().get(pid)),
        }
        if self.adaptive_ttl_enabled:
            prev = self._domain_last_pid.get(domain)
            if prev == pid:
                # 同代理连续胜出 → TTL 上浮(步进 1.5×,封顶)。
                new_ttl = min(self.adaptive_ttl_max,
                              self._domain_ttl_cache.get(domain, self.cache_ttl) * 1.5)
                if new_ttl > self._domain_ttl_cache.get(domain, self.cache_ttl):
                    self.domain_ttl_grows += 1
                self._domain_ttl_cache[domain] = new_ttl
            else:
                # 赢家切换(抖动)→ 计数 +1,TTL 回落下限。首次钉住(prev 为空)
                # 不算切换,只有"从某代理换成另一代理"才算。
                if prev is not None:
                    self._domain_switch_count[domain] = self._domain_switch_count.get(domain, 0) + 1
                    self.domain_ttl_resets += 1
                self._domain_ttl_cache[domain] = self.adaptive_ttl_min
            self._domain_last_pid[domain] = pid
        if pid in self._degraded_single_send:
            self._degraded_single_send.remove(pid)
        self._meta_dirty = True

    def _flush_to_db(self):
        """把内存里累积的统计/元数据一次性落盘(单事务)。

        由后台 _flush_loop 周期调用,以及 stop() 收尾调用。持 _db_lock 写库。
        注意:这里是幂等的全量覆盖——把内存当前值写回,而非增量累加,因此
        多次 flush 结果一致;即使中间 flush 丢失,下一次 flush 仍能补齐。
        """
        if not (self._stats_dirty or self._meta_dirty):
            return
        with self._db_lock:
            if self._stats_dirty:
                # 全量重建 domain_stats:内存是权威源(已含历史累加)。
                self._db.execute("DELETE FROM domain_stats")
                self._db.executemany(
                    "INSERT INTO domain_stats (domain, proxy_id, wins) VALUES (?, ?, ?)",
                    [(d, pid, w) for d, m in self._stats_cache.items()
                     for pid, w in m.items()],
                )
                self._stats_dirty = False
            if self._meta_dirty:
                self._db.execute("DELETE FROM domain_meta")
                self._db.executemany(
                    "INSERT INTO domain_meta (domain, default_proxy, updated_at, ref_ewma)"
                    " VALUES (?, ?, ?, ?)",
                    [(d, m["default_proxy"], m["updated_at"], m.get("ref_ewma"))
                     for d, m in self._meta_cache.items()],
                )
                self._meta_dirty = False
            self._db.commit()

    async def _flush_loop(self):
        """后台周期 flush:把内存统计批量落盘,周期 FLUSH_INTERVAL 秒。

        捕获异常不退出循环(单次 flush 失败不影响后续);被取消时静默退出
        (stop() 会做最终 flush)。
        """
        try:
            while True:
                await asyncio.sleep(FLUSH_INTERVAL)
                try:
                    self._flush_to_db()
                    self._prune_sticky()
                except Exception:
                    logger.exception("background flush failed")
        except asyncio.CancelledError:
            pass

    # ── 后台探活(仿 _flush_loop)────────────────────────────────

    async def _probe_loop(self):
        """后台周期探活:每 probe_interval_sec 对 enabled 代理做轻量 CONNECT 探活。

        探活只验证"上游代理本身可达"——建连 + CONNECT 握手,不拉取任何业务
        数据;成功则更新 EWMA(粗延迟观测),失败则累计连续失败(与真实请求
        共享,达阈值即熔断)。捕获异常不退出循环;被取消时静默退出(stop()
        会做最终清理)。probe_interval_sec<=0 时 start() 不启动本循环。
        """
        try:
            while True:
                await asyncio.sleep(self.probe_interval_sec)
                try:
                    await self._probe_all()
                except Exception:
                    logger.exception("background probe failed")
        except asyncio.CancelledError:
            pass

    @staticmethod
    def _parse_target(target: str) -> Tuple[str, int]:
        """解析 "host:port" / "[ipv6]:port" 为目标 (host, port)。"""
        if target.startswith('['):
            host_end = target.find(']')
            host = target[1:host_end]
            port = int(target[host_end + 2:])
        else:
            host, port_str = target.rsplit(':', 1)
            port = int(port_str)
        return host, port

    def _canary_for_proxy(self, proxy) -> str:
        """返回该代理应探活的 canary 目标:按 tags 命中第一条,无匹配用全局。

        多 canary 配置(probe_canaries)下:遍历 canary,若 canary 有 tags 且
        代理 tags 全命中 → 选它;若无 tags 的 canary(兜底)遇到即选。未配置
        多 canary 或全未命中 → 回退 self.probe_canary(单 canary)。
        """
        if self.probe_canaries:
            ptags = proxy.tags or {}
            for c in self.probe_canaries:
                ctags = c.get("tags") or {}
                if not ctags:
                    return c["target"]  # 兜底 canary
                if all(ptags.get(k) == v for k, v in ctags.items()):
                    return c["target"]
        return self.probe_canary

    async def _canary_reachable(self, target: str) -> bool:
        """本机直连 canary 探可达性(短超时;失败仅跳过本轮,不算任何上游失败)。

        探活结果只有在"本机→canary"路径可达时才有意义:不可达(选错 canary /
        校网防火墙 DROP 出口)时经上游 CONNECT 的应答永远收不到,会把健康上游
        误判为故障。返回 False 表示该 canary 不适合本机网络,本轮跳过。
        """
        try:
            c_host, c_port = self._parse_target(target)
        except (ValueError, IndexError):
            logger.warning("invalid probe canary target %r", target)
            return False
        try:
            c_reader, c_writer = await asyncio.wait_for(
                asyncio.open_connection(c_host, c_port), timeout=_PROBE_TIMEOUT)
            c_writer.close()
            await c_writer.wait_closed()
            return True
        except (asyncio.TimeoutError, OSError, ConnectionError):
            return False

    async def _probe_all(self):
        """对全部 enabled 代理各做一次轻量探活(并发,单个失败不影响其余)。

        探活目标:单 canary(probe_canary)或按代理标签选的多 canary
        (probe_canaries,见 _canary_for_proxy)。CONNECT 到 canary 只验证上游
        存活与建连延迟;成功/失败分别喂 EWMA 与熔断计数。

        关键预检:每代理按其 canary 先直连一次,确认"本机→canary"可达。不可达
        时该代理探活跳过(计 probes_skipped),避免把健康上游误判为故障并误熔断。
        直连超时 _PROBE_TIMEOUT;即便直连可达,经上游的 CONNECT 仍可能因上游侧
        不通而失败(该情况照常累计 record_failure)。
        """
        proxies = [p for p in self.proxy_store.list() if p.enabled]
        if not proxies:
            return
        tasks = [asyncio.create_task(self._probe_proxy(p)) for p in proxies]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _probe_proxy(self, proxy):
        """对单个代理发起一次 CONNECT 探活:经该上游 CONNECT 到其 canary 后关闭。

        成功:记录 EWMA + 成功观测(连续失败归零);失败/超时:记录失败(累计连续
        失败,达阈值即熔断,与真实请求失败同源)。'local' 是本机直连,无上游可
        探,跳过。超时 _PROBE_TIMEOUT 防半开上游长期占用。

        探活前先本机直连 canary(该代理选的 canary):不可达表示"探活目标在本机
        网络里不可达"——探活结果无法反映上游真实健康(上游可能很好,只是本机到
        canary 的路由/防火墙挡了)。跳过本轮,计入 probes_skipped,而不是累计
        record_failure。
        """
        if proxy.id == 'local':
            return
        canary = self._canary_for_proxy(proxy)
        if not await self._canary_reachable(canary):
            self.probes_skipped += 1
            return
        self.probes_sent += 1
        t0 = time.perf_counter()
        try:
            up_reader, up_writer = await asyncio.wait_for(
                asyncio.open_connection(proxy.host, proxy.port), timeout=_PROBE_TIMEOUT)
        except (asyncio.TimeoutError, OSError, ConnectionError):
            self.selector.record_failure(proxy.id)
            self.probes_failed += 1
            return
        try:
            auth_hdr = ""
            if proxy.auth:
                raw = f"{proxy.auth['username']}:{proxy.auth['password']}"
                encoded = base64.b64encode(raw.encode()).decode()
                auth_hdr = f"Proxy-Authorization: Basic {encoded}\r\n"
            up_writer.write(
                f"CONNECT {canary} HTTP/1.1\r\nHost: ".encode('latin-1') + _hb(canary) + f"\r\n{auth_hdr}\r\n".encode('latin-1'))
            await up_writer.drain()
            status = await asyncio.wait_for(up_reader.readline(), timeout=_PROBE_TIMEOUT)
            if not status or b'200' not in status:
                raise RuntimeError('probe CONNECT failed')
            self.selector.record_ttfb(proxy.id, time.perf_counter() - t0)
            self.selector.record_success(proxy.id)
            self.probes_ok += 1
        except (asyncio.TimeoutError, OSError, ConnectionError, RuntimeError):
            self.selector.record_failure(proxy.id)
            self.probes_failed += 1
        finally:
            try:
                up_writer.close()
                await up_writer.wait_closed()
            except Exception:
                pass

    def get_domain_stats_from_db(self) -> dict[str, dict[str, int]]:
        """读取全量域名胜出统计,组织为 {domain: {proxy_id: wins}}。

        读内存镜像(权威源),供管理 API / 仪表盘使用。无需触 DB/锁。
        """
        return {d: dict(m) for d, m in self._stats_cache.items()}

    def get_domain_meta_from_db(self) -> dict[str, dict[str, str]]:
        """读取全量域名元数据 {domain: {default_proxy, updated_at}}。

        读内存镜像(权威源),供管理 API / 仪表盘使用。无需触 DB/锁。
        """
        return {d: dict(m) for d, m in self._meta_cache.items()}

    def get_domain_meta_enriched(self) -> dict[str, dict]:
        """读取域名元数据 + 自适应 TTL 状态(P2 验收:/domains/meta 展示
        ttl/expires_at/switch_count)。仅自适应 TTL 开启时附加字段,否则与
        get_domain_meta_from_db 同构(兼容旧消费方)。
        """
        out = {d: dict(m) for d, m in self._meta_cache.items()}
        if self.adaptive_ttl_enabled:
            now = datetime.now(timezone.utc)
            for d, m in out.items():
                ttl = self._domain_ttl(d)
                m["ttl"] = ttl
                try:
                    dt = datetime.fromisoformat(m["updated_at"])
                    m["expires_at"] = (dt + timedelta(seconds=ttl)).isoformat()
                    m["ttl_remaining"] = max(0.0, (dt + timedelta(seconds=ttl) - now).total_seconds())
                except Exception:
                    m["expires_at"] = None
                    m["ttl_remaining"] = None
                m["switch_count"] = self._domain_switch_count.get(d, 0)
        return out

    def snapshot_counters(self) -> dict:
        """快照服务端性能计数器 + 池/缓存规模,供 /metrics 跨进程读取。

        压测在每个场景开始/结束各取一次快照,差值即该场景的缓存命中/竞速扇出。
        纯内存读取,无锁无 I/O。返回 dict 可直接 JSON 序列化。
        """
        return {
            "http_cache_hits": self.http_cache_hits,
            "http_cache_misses": self.http_cache_misses,
            "domain_cache_hits": self.domain_cache_hits,
            "sticky_cache_hits": self.sticky_cache_hits,
            "sticky_evictions": self.sticky_evictions,
            "racing_invocations": self.racing_invocations,
            "upstream_attempts": self.upstream_attempts,
            "http_cache_entries": len(self._http_cache),
            "http_cache_bytes": self._http_cache_bytes,
            "http_cache_evictions": self.http_cache_evictions,
            "client_pool_size": len(self._client_pool),
            "sticky_cache_size": len(self._sticky_cache),
            "request_counts": dict(self.request_counts),
            "attempted_counts": dict(self.attempted_counts),
            "proxy_quality": self.selector.get_quality(),
            "proxy_in_flight": self.selector.get_in_flight(),
            "max_in_flight": self.selector.max_in_flight,
            "proxy_concurrency_limits": self.selector.get_concurrency_limits(),
            "concurrency_limit_enabled": self.selector.concurrency_enabled,
            "conn_pool_enabled": self.conn_pool_enabled,
            "conn_pool_creates": self.conn_pool_creates,
            "conn_pool_hits": self.conn_pool_hits,
            "conn_pool_misses": self.conn_pool_misses,
            "conn_pool_expired": self.conn_pool_expired,
            "conn_pool_size": sum(len(v) for v in self._conn_pool.values()),
            "conn_pool_target_prewarm": self.conn_pool_target_prewarm,
            "conn_pool_refill_pause_minutes": self.conn_pool_refill_pause_minutes,
            "conn_pool_refill_pause_silence_sec": self.conn_pool_refill_pause_silence_sec,
            "conn_pool_idle_paused": self._conn_pool_idle(),
            "target_pool_creates": self.target_pool_creates,
            "target_pool_hits": self.target_pool_hits,
            "target_pool_misses": self.target_pool_misses,
            "target_pool_expired": self.target_pool_expired,
            "target_pool_size": sum(len(v) for v in self._target_pool.values()),
            "target_prewarm_dispatched": self.target_prewarm_dispatched,
            "target_prewarm_success": self.target_prewarm_success,
            "target_prewarm_failed": self.target_prewarm_failed,
            "conn_pool_established_reuse": self.conn_pool_established_reuse,
            "established_pool_hits": self.established_pool_hits,
            "established_pool_misses": self.established_pool_misses,
            "established_pool_expired": self.established_pool_expired,
            "established_pool_returned": self.established_pool_returned,
            "established_pool_size": sum(len(v) for v in self._established_pool.values()),
            "connect_new_conns": self.connect_new_conns,
            "probes_sent": self.probes_sent,
            "probes_ok": self.probes_ok,
            "probes_skipped": self.probes_skipped,
            "probes_failed": self.probes_failed,
            "circuit_open_count": self.selector.circuit_open_count,
            "circuit_state": self.selector.get_circuit_state(),
            "single_send_degrades": self.single_send_degrades,
            "domain_ttl_grows": self.domain_ttl_grows,
            "domain_ttl_resets": self.domain_ttl_resets,
            "adaptive_ttl_enabled": self.adaptive_ttl_enabled,
            "switch_damping_blocks": self.switch_damping_blocks,
            "switch_damping_fast_swaps": self.switch_damping_fast_swaps,
            "switch_damping_enabled": self.switch_damping_enabled,
        }

    def get_degraded_single_send(self) -> list[str]:
        """返回当前"被单发降级判定命中的代理"集合(供 /metrics / 仪表盘展示)。

        仅用于可观测——真正的门控是每次选择的实时重估,此集合由新赢家接管
        (_record_win_meta)或 reset_proxy_quality 清除。读内存无锁。
        """
        return sorted(self._degraded_single_send)

    def reset_proxy_quality(self):
        """清空全部代理 EWMA 质量数据(网络切换/代理分组变化时调用)。

        RFC 8305 §4:历史 RTT 数据不可跨网络接口使用,换网络后应清空重学。
        熔断/慢启动状态一并清空(旧网络的连续失败对当前网络无意义);
        单发降级失效集合一并清空(旧网络的降级标记不可沿用)。
        """
        self.selector.reset_quality()
        self._degraded_single_send.clear()

    def reset_proxy_circuits(self):
        """手动解除全部代理熔断并清空连续失败计数(运维介入后调用)。

        与 reset_proxy_quality 的区别:不动 EWMA(延迟历史仍有效),只清熔断
        状态,让代理立刻重新参与竞速。
        """
        self.selector.reset_circuits()

    @staticmethod
    def _normalize_host(host: str) -> str:
        """规范化目标 host 用于策略匹配:小写、去尾部点、去 IPv6 括号、剥端口。

        输入可能是纯域名、域名:port(CONNECT target)、[ipv6]:port 或 [ipv6]。
        仅当末尾段为纯数字才剥端口,避免误伤裸 IPv6(裸 IPv6 不以 '[' 开头时
        rpartition 后末段恰是十六进制,isdigit 为 False,不剥)。
        """
        h = host.strip().lower()
        if h.startswith('[') and ']' in h:
            h = h[1:h.find(']')]
        elif ':' in h:
            head, _, tail = h.rpartition(':')
            if tail.isdigit():
                h = head
        return h.rstrip('.')

    def _policy_matches(self, host: str) -> Optional[PolicyConfig]:
        """返回命中的第一条策略(匹配条件 OR);无命中返回 None。

        顺序语义:按配置的 policies 列表顺序,第一条命中即返回(可配置
        覆盖/优先级)。命中后调用方用 _policy_allows_proxy 校验单个代理。
        """
        if not self._policies:
            return None
        h = self._normalize_host(host)
        # 精确匹配与后缀匹配(直接比较,无正则开销;host 已小写化)。
        for pol in self._policies:
            m = pol.match
            if h in (m.domain_exact or []):
                return pol
            if any(h.endswith(suf.lower()) for suf in (m.domain_suffix or [])):
                return pol
        # 正则匹配(数量少,预编译后逐条 re.search)。
        for idx, rx in self._policy_regexes:
            if rx.search(h):
                return self._policies[idx]
        return None

    def _policy_allows_proxy(self, pol: PolicyConfig, proxy: Optional[Any]) -> bool:
        """策略是否允许该代理参与候选:tags 或 ids 任一命中即允许(并集)。

        代理不存在(如 'local' 直连)→ 仅当策略无 tags 且无 ids(未限制)才允许;
        有 tags/ids 限制时 local 不属于任何显式子集,排除(防御:不直连绕过
        策略)。防御性:策略限制为空(异常配置)视为不限制,不阻断流量。
        """
        if pol is None:
            return True
        tags = pol.proxies.tags or {}
        ids = set(pol.proxies.ids or [])
        if not tags and not ids:
            return True  # 未限制:全量候选(防御,不阻断流量)
        if proxy is None:
            return False  # local 直连不在任何显式子集内
        if proxy.id in ids:
            return True
        ptags = proxy.tags or {}
        return any(ptags.get(k) == v for k, v in tags.items())

    def _policy_candidate_pids(self, host: str, proxies: List[str]) -> List[str]:
        """按 host 命中的策略过滤有序候选 pid 列表(保持顺序)。"""
        pol = self._policy_matches(host)
        if pol is None:
            return proxies
        return [pid for pid in proxies
                if self._policy_allows_proxy(pol, self.proxy_store.get(pid))]

    def _policy_allows_sticky(self, host: str, pid: str) -> bool:
        """粘性/域名缓存取用时的策略校验:命中策略但 pid 不在子集 → 视为 miss。

        与竞速候选收窄保持同一套策略,防止旧缓存/粘性条目绕过新策略
        (文档 §8:策略路由必须同时作用于粘性、域名缓存与竞速)。
        'local' 直连由 _policy_allows_proxy 处理(受限时排除)。
        """
        pol = self._policy_matches(host)
        if pol is None:
            return True
        return self._policy_allows_proxy(pol, self.proxy_store.get(pid))

    @staticmethod
    def _proxy_quality_ewma(q: Optional[dict]) -> Optional[float]:
        """从质量表条目取出 EWMA(秒);无条目/缺字段返回 None。"""
        if not q:
            return None
        ewma = q.get("ewma_ttfb")
        return float(ewma) if isinstance(ewma, (int, float)) else None

    def _single_send_degraded(self, pid: str, ref_ewma: Optional[float]) -> bool:
        """被钉住代理在"单发选择"时是否已恶化,应降级回竞速(Goal #6)。

        两条独立信号,任一命中即判定不稳定(与熔断解耦——熔断是"连续失败达阈值
        直接剔除",这里是"尚未熔断但已开始变差,别再确定性单发,交给竞速选路"):
          1) 连续失败:selector 的连续失败计数 ≥ single_send_degrade_fail
             (熔断阈值 3 的早告警,默认 2)。被钉住代理最近在真实请求/探活中
             连续失败,说明它在变差——单发命中它只会放大失败路径,降级回竞速
             让有序候选/兜底批自动绕开它。
          2) EWMA 恶化:当前 EWMA ≥ ref_ewma × single_send_degrade_ratio。
             与熔断器解耦——该代理可能仍整体健康(EWMA 未到"差"的绝对档),但
             相比被钉住时显著变慢,应重竞速换新赢家。EWMA 相对基线恶化
             (envoy 风格连续失败剔除 + 基线比对,见分析 doc P2-6)。

        防御:代理已熔断(open)→ 由调用方的 is_circuit_open 处理,此处不重复;
        无 EWMA 观测/无基线 → 不触发 EWMA 信号(失败信号仍可独立触发)。
        EWMA 信号要求观测数 obs>=2:obs==1 时当前 EWMA 即钉住时的单次观测,
        尚无"趋势"可言,任何更新都会把它误判为恶化,故不触发。
        'local'(本机直连)跳过本机直连路径的特殊处理由调用方负责。
        """
        if self.single_send_degrade_fail > 0:
            st = self.selector.get_circuit_state().get(pid)
            consec = int(st["consec_fail"]) if st else 0
            if consec >= self.single_send_degrade_fail:
                self.single_send_degrades += 1
                return True
        if self.single_send_degrade_ratio > 0 and ref_ewma is not None and ref_ewma > 0:
            q = self.selector.get_quality().get(pid)
            cur = self._proxy_quality_ewma(q)
            if cur is not None and int(q.get("obs", 0)) >= 2 \
                    and cur >= ref_ewma * self.single_send_degrade_ratio:
                slack = self.single_send_degrade_slack_ms / 1000.0
                if (cur - ref_ewma) > slack:
                    self.single_send_degrades += 1
                    return True
        return False

    def _get_fresh_proxy(self, domain: str) -> Optional[str]:
        """返回某域名在 cache_ttl 内的缓存代理 id;过期或无记录返回 None。

        用于域名缓存:命中则直接复用该代理,跳过竞速。熔断中的代理视为未命中
        (退回竞速找健康代理,竞速赢家会刷新 meta)。单发降级判定(Goal #6)命中
        的代理也视为未命中——被钉住代理最近失败率上升或 EWMA 恶化时,主动降级
        回竞速,不再确定性单发。纯内存读取,无 DB/锁。
        """
        entry = self._meta_cache.get(domain)
        if not entry:
            return None
        pid = entry["default_proxy"]
        # 策略路由(P1):命中策略但缓存代理不在允许子集内 → 视为 miss 退回竞速
        # (防旧缓存绕过新策略;竞速赢家会经 _record_win_meta 重新钉住)。
        if self._policies and not self._policy_allows_sticky(domain, pid):
            return None
        if self.selector.is_circuit_open(pid):
            return None
        # Goal #6:质量感知单发。基线 ref_ewma 在钉住时刻捕获(见 _record_win_meta),
        # 已是浮点 EWMA 值(非质量 dict)。
        # 命中降级 → 记入降级集合(可观测)并视为未命中退回竞速;竞速新赢家会经
        # _record_win_meta 清除标记。
        if self._single_send_degraded(pid, entry.get("ref_ewma")):
            self._degraded_single_send.add(pid)
            # 自适应 TTL:被钉住代理开始恶化 → TTL 打回下限,让竞速新赢家接管。
            if self.adaptive_ttl_enabled:
                self._domain_ttl_cache[domain] = self.adaptive_ttl_min
                self.domain_ttl_resets += 1
            return None
        updated_at_str = entry["updated_at"]
        ttl = self._domain_ttl(domain)
        try:
            dt = datetime.fromisoformat(updated_at_str)
            if (datetime.now(timezone.utc) - dt).total_seconds() < ttl:
                return pid
        except Exception:
            pass
        return None

    def _domain_ttl(self, domain: str) -> float:
        """该域名缓存当前有效期:自适应 TTL 开启时取 per-domain 值,否则全局值。"""
        if not self.adaptive_ttl_enabled:
            return self.cache_ttl
        return self._domain_ttl_cache.get(domain, self.cache_ttl)

    # ── 会话粘性 ────────────────────────────────────────────────

    def get_sticky_cache(self) -> dict[str, dict[str, object]]:
        """返回全量会话粘性表快照 {key: {proxy_id, updated_at, hits}}。

        供管理 API / 仪表盘展示。读内存镜像,无锁无 I/O。
        """
        return {k: dict(v) for k, v in self._sticky_cache.items()}

    @staticmethod
    def _sticky_key(client_ip: str, domain: str) -> str:
        """会话粘性键:"客户端IP|域名"。hostname/IP 均不含 '|',分隔安全。"""
        return f"{client_ip}|{domain}"

    def _evict_sticky_key(self, key: str):
        """按 key 驱逐粘性条目并计入驱逐统计(所有驱逐路径共用)。"""
        if self._sticky_cache.pop(key, None) is not None:
            self.sticky_evictions += 1

    def _get_sticky_proxy(self, client_ip: str, domain: str) -> Optional[str]:
        """返回客户端+域名在 stickiness_ttl 内的粘性代理 id;未启用/过期/代理
        失效/重评估到期 返回 None(并把失效/过期条目就地驱逐)。

        纯内存读取。TTL 为滑动制:命中后由 _bump_sticky 刷新 updated_at 并累加
        hits,活跃会话不过期;到期后重新走竞速。取回时校验代理仍在 ProxyStore
        且 enabled——内存-only 的表可能在代理被删除/停用后残留,必须就地驱逐。
        本机竞速胜者('local')不经过 proxy_store(直连),仅当 enable_local_racing
        时才视为有效,否则视作失效条目驱逐(A1)。
        """
        if not self.stickiness_enabled:
            return None
        key = self._sticky_key(client_ip, domain)
        entry = self._sticky_cache.get(key)
        if not entry:
            return None
        pid = entry["proxy_id"]
        if pid == 'local':
            if not self.enable_local_racing:
                self._evict_sticky_key(key)
                return None
        else:
            proxy = self.proxy_store.get(pid)
            if not proxy or not proxy.enabled:
                self._evict_sticky_key(key)
                return None
        try:
            dt = datetime.fromisoformat(entry["updated_at"])
            if (datetime.now(timezone.utc) - dt).total_seconds() >= self.stickiness_ttl:
                self._evict_sticky_key(key)
                return None
        except Exception:
            self._evict_sticky_key(key)
            return None
        # 熔断中的代理不作粘性单发:直接驱逐(退回竞速找健康代理),避免对
        # 已确认故障的代理持续单发。local 不经 selector,跳过该检查(A1)。
        if pid != 'local' and self.selector.is_circuit_open(pid):
            self._evict_sticky_key(key)
            return None
        # 策略路由(P1):命中策略但粘性代理不在允许子集内 → 视为 miss 驱逐并
        # 回落竞速(防旧粘性绕过新策略;与域名缓存同一套策略校验)。
        if self._policies and not self._policy_allows_sticky(domain, pid):
            self._evict_sticky_key(key)
            return None
        # B2:命中次数达到阈值 → 触发探路重竞速(不驱逐,由调用方依据
        # _sticky_recheck_due 决定跳过域名缓存直接竞速)。
        if self._sticky_recheck_due(client_ip, domain):
            return None
        # Goal #6:质量感知粘性。被钉住代理最近失败率上升 / EWMA 恶化 → 驱逐
        # 并回落竞速(调用方 _evict_sticky + 跳过域名缓存直接竞速)。local 直连
        # 不经 selector,跳过降级判定(A1)。
        if pid != 'local' and self._sticky_degrade_due(client_ip, domain):
            return None
        return pid

    def _sticky_recheck_due(self, client_ip: str, domain: str) -> bool:
        """该客户端+域名是否到了"探路重竞速"时机(粘性命中 recheck_hits 次)。

        仅当 sticky_recheck_hits > 0 且条目仍处于 TTL 内且 hits 达到阈值时为真。
        调用方发现为真后应驱逐该条目并跳过域名缓存直接竞速,用新赢家替换可能
        已变慢的粘性代理。
        """
        if not self.stickiness_enabled or self.stickiness_recheck_hits <= 0:
            return False
        entry = self._sticky_cache.get(self._sticky_key(client_ip, domain))
        if not entry:
            return False
        try:
            if int(entry.get("hits", 0)) < self.stickiness_recheck_hits:
                return False
            dt = datetime.fromisoformat(entry["updated_at"])
            return (datetime.now(timezone.utc) - dt).total_seconds() < self.stickiness_ttl
        except Exception:
            return False

    def _sticky_degrade_due(self, client_ip: str, domain: str) -> bool:
        """粘性单发是否该因"代理质量恶化"降级回竞速(Goal #6)。

        与 _sticky_recheck_due(B2,命中计数触发)互补:B2 是"达到 N 次命中后周期
        性重探路",这里是"被钉住代理已被质量模型判定不稳定"——两者任一命中都
        应放弃粘性单发,驱逐条目并跳过域名缓存直接竞速,让竞速赢家重新钉住。
        基线 ref_ewma 在钉住时刻捕获(见 _record_sticky),粘性命中仅滑动 TTL
        不刷新基线,保证"恶化"是相对钉住时的初始状态,而非相对最近一次命中。
        """
        if not self.stickiness_enabled:
            return False
        entry = self._sticky_cache.get(self._sticky_key(client_ip, domain))
        if not entry:
            return False
        pid = entry["proxy_id"]
        if pid == 'local':
            return False  # 本机直连不经 selector,跳过降级判定(A1)
        if not self._single_send_degraded(pid, entry.get("ref_ewma")):
            return False
        self._degraded_single_send.add(pid)
        return True

    def _record_sticky(self, client_ip: str, domain: str, pid: str):
        """记录客户端+域名的粘性代理(刷新 updated_at,hits 归零)。

        仅由确认的赢家(粘性单发成功 / 竞速赢家 / 域名缓存单发成功)调用,新
        赢家从 0 开始重新计数。未启用时为空操作。写前检查容量上限(B1):超限
        先清过期条目,仍超则驱逐 updated_at 最旧的一条。
        """
        if not self.stickiness_enabled:
            return
        # 方向 A:新赢家显著差于当前最优代理 → 不回填粘性表(继续竞速),避免
        # 慢代理被钉住进入"钉住→降级→回填→再钉住"循环。
        if self._worse_than_best(pid):
            return
        key = self._sticky_key(client_ip, domain)
        if key not in self._sticky_cache and len(self._sticky_cache) >= self.stickiness_max_entries:
            self._prune_sticky()
            if len(self._sticky_cache) >= self.stickiness_max_entries:
                self._evict_oldest_sticky()
        self._sticky_cache[key] = {
            "proxy_id": pid,
            "updated_at": self._now_utc(),
            "hits": 0,
            # Goal #6:钉住时刻的 EWMA 基线,供 _sticky_degrade_due 判定"相对钉住
            # 时是否恶化"。粘性命中(_bump_sticky)只滑动 TTL,不刷新基线。
            "ref_ewma": self._proxy_quality_ewma(self.selector.get_quality().get(pid)),
        }

    def _bump_sticky(self, client_ip: str, domain: str, pid: str):
        """粘性命中成功:刷新 updated_at(滑动 TTL)并累加 hits(B2)。

        区别于 _record_sticky:hits 只增不减(新赢家才归零),保证 recheck_hits
        阈值可被持续命中累计触发。条目被并发驱逐时退化为重新记录。
        """
        key = self._sticky_key(client_ip, domain)
        entry = self._sticky_cache.get(key)
        if entry is None:
            self._record_sticky(client_ip, domain, pid)
            return
        entry["proxy_id"] = pid
        entry["updated_at"] = self._now_utc()
        entry["hits"] = int(entry.get("hits", 0)) + 1

    def _evict_sticky(self, client_ip: str, domain: str):
        """驱逐客户端+域名的粘性条目(粘性代理单发失败/5xx 时调用)。"""
        self._evict_sticky_key(self._sticky_key(client_ip, domain))

    def _evict_oldest_sticky(self):
        """容量保护:驱逐 updated_at 最旧的一条粘性条目(计入驱逐统计)。

        ISO-8601 UTC 时间戳同格式下按字典序比较即时间序,无需解析。
        """
        if not self._sticky_cache:
            return
        oldest_key = min(self._sticky_cache, key=lambda k: self._sticky_cache[k].get("updated_at", ""))
        self._evict_sticky_key(oldest_key)

    def _prune_sticky(self):
        """清扫过期/指向失效代理的粘性条目,限制内存无界增长。

        由后台 _flush_loop 周期调用,也由 _record_sticky 在超容量时先调用。
        粘性键集合(客户端 IP)可能远大于域名集合,若放任不管会缓慢累积;
        过期清扫把表规模收敛到"最近 TTL 内活跃的客户端+域名"。
        """
        if not self._sticky_cache:
            return
        now = datetime.now(timezone.utc)
        stale = []
        for key, entry in self._sticky_cache.items():
            pid = entry["proxy_id"]
            if pid == 'local':
                if not self.enable_local_racing:
                    stale.append(key)
                continue
            proxy = self.proxy_store.get(pid)
            if not proxy or not proxy.enabled:
                stale.append(key)
                continue
            try:
                dt = datetime.fromisoformat(entry["updated_at"])
                if (now - dt).total_seconds() >= self.stickiness_ttl:
                    stale.append(key)
            except Exception:
                stale.append(key)
        for key in stale:
            self._evict_sticky_key(key)

    # ── TCP 调优 ────────────────────────────────────────────────

    @staticmethod
    def _set_nodelay(writer):
        """对连接设置 TCP_NODELAY(禁用 Nagle)与 TCP_QUICKACK,降低转发延迟。

        代理是中转,小包延迟敏感,禁用 Nagle 让数据立即发出。失败静默忽略
        (某些平台不支持 TCP_QUICKACK)。
        """
        sock = writer.get_extra_info('socket')
        if sock:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
            except (OSError, AttributeError):
                pass

    # ── HTTP GET 缓存 ──────────────────────────────────────────

    def _http_cache_key(self, method: str, url: str) -> str:
        """响应缓存键:"方法:URL"。仅 GET 缓存,故方法实际恒为 GET。"""
        return f"{method}:{url}"

    def _http_cache_get(self, method: str, url: str) -> Optional[dict]:
        """取 GET 的缓存响应;非 GET 或未命中或已过期返回 None。过期项顺便清除。

        enable_http_cache=False 时一律未命中(用于压测隔离缓存层,测纯路由性能)。
        P2:命中刷新 last_access(滑动 TTL 兼作 LRU 顺序);过期项按 LRU 淘汰
        路径清除(同步维护 _http_cache_bytes 与二级索引)。
        """
        if not self.enable_http_cache or method != 'GET':
            return None
        key = self._http_cache_key(method, url)
        entry = self._http_cache.get(key)
        if not entry:
            return None
        now = time.time()
        if now - entry['cached_at'] > self._http_cache_ttl:
            self._http_cache_remove(key)
            return None
        entry['last_access'] = now  # 滑动 TTL + LRU 顺序
        return entry

    def _http_cache_remove(self, key: str) -> None:
        """从 _http_cache 删除 key,同步维护字节计数与 _http_cache_domain_index。

        所有删除路径统一走这里(过期清除、LRU 淘汰、写方法按域名失效),确保
        _http_cache_bytes 与二级索引不漏不重。返回是否删除了条目。
        """
        entry = self._http_cache.pop(key, None)
        if entry is None:
            return False
        self._http_cache_bytes -= len(entry.get('content', b'') or b'')
        cached_url = key[len('GET:'):] if key.startswith('GET:') else key
        cached_host = urllib.parse.urlparse(cached_url).hostname or cached_url
        idx = self._http_cache_domain_index.get(cached_host)
        if idx:
            idx.discard(key)
            if not idx:
                del self._http_cache_domain_index[cached_host]
        return True

    def _http_cache_set(self, method: str, url: str, status_code, reason_phrase, headers, content) -> None:
        """缓存一个 GET 可缓存响应(状态码、原因、头、body、时间戳)。

        可缓存状态码由调用方按 CACHEABLE_STATUS 判断。无论上游是否给出
        Content-Length,都遵循 Cache-Control 的 no-store/no-cache/private:
        本代理是共享缓存(为多客户端服务),private 明确禁止共享缓存存储,
        no-store/no-cache 同理。原实现仅在缺 Content-Length 时查 Cache-Control,
        扩展到 3xx/404 后必须无条件查,否则会把源站标 private 的 302 也缓存。

        P2:写入前按 max_entries / max_bytes 做 LRU 淘汰(淘汰 last_access 最旧
        的条目),并维护 _http_cache_bytes。单一超大响应(>max_bytes 的一半)不缓存,
        避免单条即打满总预算。更新已有 key 时先归还旧字节再计入新字节。
        """
        if method != 'GET':
            return
        # 共享缓存必须尊重源站的 Cache-Control 禁存指令(无论是否有
        # Content-Length)。no-cache 在此保守按"不存"处理:本代理不做再校验
        # (发条件请求),存了也只是徒增一次过期清除,不如直接不存。
        # headers 为 list[(name, value)](保留重复头)或 dict,两种都按 (k, v) 迭代。
        items = headers.items() if isinstance(headers, dict) else headers
        cc = next((v for k, v in items if k.lower() == 'cache-control'), '')
        if 'no-store' in cc or 'no-cache' in cc or 'private' in cc:
            return
        size = len(content or b'')
        if size > self._http_cache_max_bytes // 2:
            return  # 单一超大响应不缓存
        now = time.time()
        key = self._http_cache_key(method, url)
        # 更新已有 key:先归还旧字节,避免重复计数。
        old = self._http_cache.get(key)
        if old is not None:
            self._http_cache_bytes -= len(old.get('content', b'') or b'')
        self._http_cache[key] = {
            'status_code': status_code,
            'reason_phrase': reason_phrase,
            'headers': headers,
            'content': content,
            'cached_at': now,
            'last_access': now,
        }
        self._http_cache_bytes += size
        # 容量保护(LRU):条目数或字节数超限 → 淘汰 last_access 最旧的条目,
        # 直至回到上限以内。单次 O(N),写入远低于读取频率,可接受。
        while len(self._http_cache) > self._http_cache_max_entries \
                or self._http_cache_bytes > self._http_cache_max_bytes:
            oldest = min(self._http_cache, key=lambda k: self._http_cache[k].get('last_access', 0.0))
            self._http_cache_remove(oldest)
            self.http_cache_evictions += 1
        # 同步更新二级索引:缓存键 -> 域名,供 O(1) 域名级批量失效。
        cached_host = urllib.parse.urlparse(url).hostname or url
        self._http_cache_domain_index.setdefault(cached_host, set()).add(key)

    def _http_cache_invalidate(self, domain: str) -> None:
        """清空某域名下所有 GET 响应缓存条目(利用二级索引 O(K),K=该域名条目数)。

        写方法(POST/PUT/DELETE/PATCH)改写资源后调用。按域名而非按 URL 失效:
        添加动作常打 POST /api/items,而刷新的列表页是 GET /,两者 URL 不同,
        按 URL 精确失效会漏掉列表页缓存,导致刷新仍返回旧内容。整域清空可覆盖
        同站任意路径的 GET。利用 _http_cache_domain_index 直接取该域名下所有
        缓存键,避免 O(N) 遍历全量缓存与逐条重复 urlparse。
        提前(转发前)失效:即便写请求最终失败,后果也仅是下次 GET 多回源一次,
        不会返回错误内容。enable_http_cache=False 时缓存本就空,此处为空操作。
        """
        stale = self._http_cache_domain_index.pop(domain, set())
        for key in stale:
            self._http_cache_remove(key)

    # ── 上游连接池 ──────────────────────────────────────────────

    def _client_key(self, pid: str, proxy_url: Optional[str]) -> str:
        """连接池键:用 proxy_url 区分"同一 pid 不同上游凭据/地址"的情形。

        local(无上游)用固定键 'local';走上游的用 proxy_url(已含凭据)。
        实际上 pid↔proxy_url 一一对应,但用 proxy_url 作键更稳健。
        """
        if proxy_url is None:
            return 'local'
        return proxy_url

    async def _get_client(self, key: str, proxy_url: Optional[str]) -> httpx.AsyncClient:
        """从池中取(或按需创建)某上游的长驻 httpx.AsyncClient。

        池化跨请求复用 keep-alive 连接,避免每请求重建到上游代理的 TCP
        (HTTPS 经 CONNECT 还多一次握手)。client 不随单请求关闭,仅在
        stop() 时统一 aclose。
        """
        client = self._client_pool.get(key)
        if client is not None and not client.is_closed:
            return client
        kw: dict[str, Any] = {
            "timeout": self._upstream_timeout,
            # 连接池上限按"单代理"计。压测 staircase 在 concurrency=200 时,
            # 冷请求(30%)向 4 个代理竞速 + 热请求单发,瞬时并发上游 socket
            # ~380(fd_peak 实测 358≈池打满)。原 max_connections=100/代理虽
            # 总量够,但 max_keepalive=20 偏小:突发过后大部分连接被回收,
            # 下一突发又得重建到上游代理的 CONNECT 隧道(含 TLS 握手),正是
            # staircase p95≈1300ms 长尾的主因。调大 keepalive 与总量,并把
            # 过期延长到 120s,让突发间复用连接、减少隧道重建。
            "limits": httpx.Limits(
                max_keepalive_connections=50, max_connections=200,
                keepalive_expiry=120),
        }
        if proxy_url:
            kw['proxy'] = proxy_url
        client = httpx.AsyncClient(**kw)
        self._client_pool[key] = client
        return client

    async def _aclose_all_clients(self):
        """关闭所有长驻上游 client(仅在 stop 时调用)。"""
        clients = list(self._client_pool.values())
        self._client_pool.clear()
        for c in clients:
            try:
                await c.aclose()
            except Exception:
                pass

    # ── CONNECT 上游 TCP 预热池(P1)+ 目标半预连接(P2)────────────

    @staticmethod
    def _discard_conn(writer: asyncio.StreamWriter):
        """fire-and-forget 关闭一条废弃连接(同步路径不能用 await 时的清理)。

        已握手池复用前发现脏缓冲 → 丢弃该连接。close 放入后台 task 执行,不阻塞
        取用热路径;3.12 的 wait_closed() 严格等对端 FIN,用 0.5s 超时保护。
        """
        async def _close():
            try:
                writer.close()
                await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
            except Exception:
                pass
        try:
            asyncio.create_task(_close())
        except RuntimeError:
            # 事件循环未运行(如构造期)时直接 close,不等待。
            try:
                writer.close()
            except Exception:
                pass

    @staticmethod
    def _pool_peek(pool: dict, key: str) -> Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """从预热池取一条空闲连接 (reader, writer);无则返回 None。

        连接在取用后由调用方发 CONNECT,隧道结束即关闭(不归还——CONNECT 后
        socket 已被隧道占用)。池中存 (reader, writer) 对:asyncio 的
        get_extra_info('reader') 不可靠,必须由建连处成对保存。
        """
        stack = pool.get(key)
        while stack:
            reader, writer = stack.pop()
            if writer.is_closing():
                continue  # 已关闭的废弃连接直接丢弃
            return reader, writer
        if stack is not None and not stack:
            pool.pop(key, None)
        return None

    def _conn_pool_peek(self, proxy_host: str, proxy_port: int) -> Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """从预热池取一条到该上游的空闲连接 (reader, writer);无则返回 None。

        取用成功计 hits,需新建计 misses。连接在取用后由调用方发 CONNECT,
        隧道结束即关闭(不归还——CONNECT 后 socket 已被隧道占用)。
        """
        got = Router._pool_peek(self._conn_pool, f"{proxy_host}:{proxy_port}")
        if got is None:
            self.conn_pool_misses += 1
        else:
            self.conn_pool_hits += 1
        return got

    def _target_pool_peek(self, proxy_host: str, proxy_port: int, target: str) -> Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """从 target 半预连接池取一条到该上游的空闲连接 (reader, writer)。

        键为 "proxy_host:proxy_port|target"——只预连"到上游代理"的 TCP,未发
        CONNECT,可安全复用于同 target。取用成功计 target_pool_hits,需新建计
        target_pool_misses。上层优先用此池,其次才回退第一阶段通用池。
        命中/miss 均记 INFO(低频诊断,判断热 target 是否真的复用了预热连接)。
        """
        got = Router._pool_peek(self._target_pool, f"{proxy_host}:{proxy_port}|{target}")
        if got is None:
            self.target_pool_misses += 1
            logger.info("target pool MISS %s via %s:%s (misses=%d)",
                        target, proxy_host, proxy_port, self.target_pool_misses)
        else:
            self.target_pool_hits += 1
            logger.info("target pool HIT  %s via %s:%s (hits=%d)",
                        target, proxy_host, proxy_port, self.target_pool_hits)
        return got

    def _established_pool_peek(self, proxy_host: str, proxy_port: int, target: str) -> Optional[Tuple[asyncio.StreamReader, asyncio.StreamWriter]]:
        """从已建握手隧道池取一条到该上游的连接 (reader, writer);无则返回 None。

        键为 "proxy_host:proxy_port|target"。存的是"已发 CONNECT 且收到 200"的
        隧道连接,复用时**跳过 CONNECT 握手**(连接已处于可透传状态)。取用成功
        计 established_pool_hits,未中计 established_pool_misses。复用前验证连接
        干净(无残留缓冲),有残留则视为过期丢弃,避免脏数据污染下一个客户端。
        仅当 conn_pool_enabled 且 conn_pool_established_reuse 时由调用方使用。
        """
        got = Router._pool_peek(self._established_pool, f"{proxy_host}:{proxy_port}|{target}")
        if got is None:
            self.established_pool_misses += 1
            return None
        reader, writer = got
        # 严格验证:上游缓冲残留数据 → 连接已脏,丢弃而非复用(宁可不复用也不污染)。
        # 本方法是同步热路径,关闭用 fire-and-forget 后台任务(不阻塞取用)。
        if reader.at_eof() or (reader._buffer and len(reader._buffer) > 0):
            self.established_pool_expired += 1
            logger.info("established pool DISCARD %s via %s:%s (dirty buffer)", target, proxy_host, proxy_port)
            _discard_conn(writer)
            return None
        # 标记:该连接已建握手,调用方 _try_tunnel 据此跳过 CONNECT 发送/200 校验。
        writer._established_reused = True
        self.established_pool_hits += 1
        logger.info("established pool HIT  %s via %s:%s (hits=%d)",
                    target, proxy_host, proxy_port, self.established_pool_hits)
        return reader, writer

    def _record_request_activity(self):
        """刷新"密集请求"时间戳(refill 空闲感知的活动信号)。

        在每个通过认证的 HTTP/CONNECT 首行上调用(见 _handle_client,认证放行后)。
        **活动判定为"密集请求"**:仅当距上次活动时间戳 ≤ 静默窗口
        (conn_pool_refill_pause_silence_sec)时才视为真实活动并刷新时间戳;间隔
        更大的孤立请求(如后台心跳 alive.github.com 每 3-10 分钟一次)不刷新——
        否则心跳会持续拉近"距上次请求",使空闲暂停(refill_pause_minutes)永不触发。
        若此前处于"空闲暂停"态且本次判定为活动,则立刻解除暂停并在 logger 留一条
        INFO,便于从日志确认恢复时刻。仅在有实际客户端请求的路径调用——探活/预热/
        本机自连都不算"请求",不应恢复预热。
        """
        now = time.monotonic()
        silence = self.conn_pool_refill_pause_silence_sec
        # 静默窗口>0:仅密集请求(间隔 ≤ 窗口)刷新;孤立请求(心跳)忽略。
        # 静默窗口=0:任意请求都刷新(旧行为)。
        if silence > 0 and (now - self._last_request_activity) > silence:
            return
        was_idle = self._conn_pool_idle()
        self._last_request_activity = now
        if was_idle:
            logger.info("conn pool refill resumed by client request (was idle >= %.0fmin)",
                        self.conn_pool_refill_pause_minutes)

    def _conn_pool_idle(self) -> bool:
        """空闲暂停判定:距上次"密集请求"已超过 conn_pool_refill_pause_minutes。

        活动判定见 _record_request_activity——后台心跳(间隔 3-10 分钟)不会刷新
        活动时间戳,因此即使深夜每分钟都有心跳,只要没有"密集请求"持续到来,
        本判定在距最后密集请求超过阈值后返回 True,refill/目标预热挂起。
        仅当预热池开启且配置了暂停时长才可能返回 True;0(默认关闭该特性)恒
        False——refill/目标预热行为与未加本特性时完全一致,不改变默认语义。
        """
        if not self.conn_pool_enabled or self.conn_pool_refill_pause_minutes <= 0:
            return False
        return (time.monotonic() - self._last_request_activity) >= self.conn_pool_refill_pause_minutes * 60.0

    async def _conn_pool_refill(self):
        """补充第一阶段预热连接到目标水位(后台 refill task 周期调用)。

        每代理目标 conn_pool_refill_target 条;全局受 conn_pool_total 钳制。
        建连失败静默(上游临时不可达时下次再补)。空代理/未启用跳过。
        空闲暂停期间直接返回(不建新连,已有空闲连接照常可用/可过期)。
        """
        if not self.conn_pool_enabled:
            return
        # 空闲暂停:深夜无请求时挂起补充,避免"建了又过期"的空转。
        if self._conn_pool_idle():
            return
        # 快照当前空闲总数,防止并发补充超过全局预算(两阶段共享 conn_pool_total)。
        total_idle = sum(len(v) for v in self._conn_pool.values()) \
            + sum(len(v) for v in self._target_pool.values())
        for proxy in self.proxy_store.list():
            if not proxy.enabled:
                continue
            key = f"{proxy.host}:{proxy.port}"
            have = len(self._conn_pool.get(key, []))
            need = self.conn_pool_refill_target - have
            if need <= 0 or total_idle >= self.conn_pool_total:
                continue
            need = min(need, self.conn_pool_total - total_idle,
                       self.conn_pool_per_proxy - have)
            for _ in range(max(0, need)):
                try:
                    reader, writer = await asyncio.wait_for(
                        asyncio.open_connection(proxy.host, proxy.port),
                        timeout=self.conn_pool_connect_timeout)
                except (asyncio.TimeoutError, OSError, ConnectionError):
                    break
                writer._conn_pool_created = time.monotonic()
                self._conn_pool.setdefault(key, []).append((reader, writer))
                self.conn_pool_creates += 1
                total_idle += 1
                if total_idle >= self.conn_pool_total:
                    break

    async def _target_pool_refill(self, proxy_host: str, proxy_port: int, target: str, cap: int = 2):
        """为某 (proxy, target) 键补充半预连接到目标水位(一次调用补到 cap)。

        只建立"到上游代理"的 TCP(不提前 CONNECT 到目标),可安全复用于同
        target。全局受 conn_pool_total 钳制,单键受 cap 钳制(默认 2 条:取走
        1 条仍留 1 条备用,降低"取走即空→周期 miss";避免为单个 target 占用
        过多 fd)。建连失败静默(下次命中再试)。单事件循环线程调用,无需加锁。
        cap 从 1 提升到 2:生产实测 cap=1 时 target_pool_hits=1 / misses=71,
        单条预热被取走即空,下一条同 target 请求只能回退 miss。
        """
        if not self.conn_pool_enabled:
            return 0
        # 空闲暂停:无请求期间不发起目标预热(省建连;已有半连接照常可复用)。
        if self._conn_pool_idle():
            return 0
        key = f"{proxy_host}:{proxy_port}|{target}"
        # 全局 fd 预算:第一阶段 + 第二阶段共享,超限则不补。
        total_idle = sum(len(v) for v in self._conn_pool.values()) \
            + sum(len(v) for v in self._target_pool.values())
        made = 0
        while len(self._target_pool.get(key, [])) < cap and total_idle < self.conn_pool_total:
            try:
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(proxy_host, proxy_port),
                    timeout=self.conn_pool_connect_timeout)
            except (asyncio.TimeoutError, OSError, ConnectionError):
                self.target_prewarm_failed += 1
                logger.info("target prewarm CONNECT-FAIL %s via %s:%s (failed=%d)",
                            target, proxy_host, proxy_port, self.target_prewarm_failed)
                break
            writer._conn_pool_created = time.monotonic()
            self._target_pool.setdefault(key, []).append((reader, writer))
            self.target_pool_creates += 1
            self.target_prewarm_success += 1
            made += 1
            total_idle += 1
        if made:
            logger.info("target prewarm CREATED %d conn(s) for %s via %s:%s (creates=%d, size=%d)",
                        made, target, proxy_host, proxy_port,
                        self.target_pool_creates, len(self._target_pool.get(key, [])))
        return made

    async def _target_pool_prewarm(self, proxy_host: str, proxy_port: int, target: str, cap: int = 2):
        """后台协程:为 (proxy, target) 键预热半连接,失败静默(被取消/超时/建连
        失败都只是少一条预热,不影响主请求)。由命中域名缓存/粘性或竞速胜出的
        CONNECT 触发,是"fire-and-forget",主请求不 await 本协程。cap=2 与
        _target_pool_refill 一致(见其 docstring)。
        """
        try:
            await self._target_pool_refill(proxy_host, proxy_port, target, cap=cap)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.info("target prewarm FAILED %s via %s:%s", target, proxy_host, proxy_port)

    def _spawn_target_prewarm(self, proxy_host: Optional[str], proxy_port: Optional[int], target: str):
        """命中域名缓存/粘性或竞速胜出的 CONNECT → 后台预热 (proxy, target) 半连接。

        仅在第二阶段开启且经上游代理(非本机直连)时触发;计数并记录到
        _running_tasks(供 stop() 排空),不阻塞主请求路径。预热条数由
        _target_pool_prewarm 默认 cap=2 控制(取走 1 条仍留 1 条备用,降低
        "取走即空→周期 miss";生产实测 cap=1 时 target_pool_hits=1 / misses=71)。
        """
        if not (self.conn_pool_enabled and self.conn_pool_target_prewarm):
            return
        if proxy_host is None:
            return  # 本机直连路径无"上游代理"可预热
        self.target_prewarm_dispatched += 1
        logger.info("target prewarm SPAWN %s via %s:%s (dispatched=%d)",
                    target, proxy_host, proxy_port, self.target_prewarm_dispatched)
        task = asyncio.create_task(self._target_pool_prewarm(proxy_host, proxy_port, target))
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    async def _pool_prune(self):
        """关闭空闲超时的预热连接(两阶段池,每 refill 周期顺带清理,防 fd 泄漏)。

        用 writer._conn_pool_created 记录建连时间戳(两阶段建连处统一打)。
        """
        now = time.monotonic()
        stale = []
        # 先收集待关连接,再统一重建 dict —— 迭代中 pop 会触发
        # "dictionary changed size during iteration"。
        for pool, expired_counter in ((self._conn_pool, 'conn_pool_expired'),
                                      (self._target_pool, 'target_pool_expired'),
                                      (self._established_pool, 'established_pool_expired')):
            for key in list(pool.keys()):
                stack = pool[key]
                alive = []
                for item in stack:
                    reader, writer = item
                    if writer.is_closing():
                        continue
                    last = getattr(writer, '_conn_pool_created', 0)
                    if now - last > self.conn_pool_idle_timeout:
                        stale.append(writer)
                        setattr(self, expired_counter, getattr(self, expired_counter) + 1)
                        continue
                    alive.append(item)
                if alive:
                    pool[key] = alive
                else:
                    if expired_counter == 'target_pool_expired':
                        logger.info("target prewarm EXPIRED %s (%s conn(s))",
                                    key, len(stack))
                    elif expired_counter == 'established_pool_expired':
                        logger.info("established pool EXPIRED %s (%s conn(s))",
                                    key, len(stack))
                    pool.pop(key, None)
        for w in stale:
            try:
                w.close()
                # 同 _conn_pool_close_all:预热连接对端可能挂起不关,3.12 的
                # wait_closed() 会严格等对端 FIN 而挂死,用超时保护。
                await asyncio.wait_for(w.wait_closed(), timeout=0.5)
            except (asyncio.TimeoutError, Exception):
                pass

    async def _conn_pool_prune(self):
        """兼容旧调用:第一阶段池清理(委托给 _pool_prune)。"""
        await self._pool_prune()

    async def _conn_pool_loop(self):
        """后台预热循环:周期补充到目标水位并清理过期连接。

        捕获异常不退出;被取消静默退出(stop() 会收尾)。refill_interval<=0
        时 start() 不启动本循环(只取不补)。空闲暂停(refill_pause_minutes)期间
        只清不补:expired 照常关闭、池渐空,避免深夜空转建连。
        """
        try:
            while True:
                await asyncio.sleep(self.conn_pool_refill_interval)
                try:
                    await self._conn_pool_refill()
                    await self._pool_prune()
                except Exception:
                    logger.exception("conn pool refill failed")
        except asyncio.CancelledError:
            pass

    async def _conn_pool_close_all(self):
        """关闭全部预热连接(stop 时调用),含 target 半预连接池。

        预热连接是"半连接"(只建 TCP 未发数据),对端(mock/真实上游)可能一直
        挂起等待客户端数据而不主动关闭。Python 3.12 的 StreamWriter.wait_closed()
        会严格等待对端 FIN 确认,此时会无限挂起(本地 3.11 立即返回,CI 3.12
        卡死)。故用超时保护:close() 后最多等 0.5s,超时即放弃等待,避免阻塞。
        """
        stacks = list(self._conn_pool.values()) + list(self._target_pool.values()) \
            + list(self._established_pool.values())
        self._conn_pool.clear()
        self._target_pool.clear()
        self._established_pool.clear()
        for stack in stacks:
            for item in stack:
                reader, writer = item
                try:
                    writer.close()
                    await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
                except (asyncio.TimeoutError, Exception):
                    pass

    # ── 通用竞速 / pipe / 响应写入 ──────────────────────────────

    @staticmethod
    def _is_acceptable_win(result) -> bool:
        """竞速赢家过滤:HTTP 5xx 不算胜出,CONNECT 一律算。

        HTTP 候选返回 5xx(500 内部错误/503 过载)说明上游已应答但业务失败,
        不该作为竞速赢家——否则错峰首批单发时,坏的先应答即胜,吞掉好代理,
        还会污染域名缓存与粘性表。CONNECT 候选拿到 200 才返回(见 _try_tunnel),
        故无需在此检查。
        """
        if not result:
            return False
        if len(result) >= 5:
            # HTTP 结果元组 (pid, method, url, resp, client);CONNECT 为 (pid, r, w)。
            return result[3].status_code < 500
        return True

    async def _race(self, tasks: set, cleanup=None) -> Optional[Any]:
        """取最先成功完成的 task 的结果;败者清理下放后台,立即返回赢家。

        竞速判胜取 FIRST_COMPLETED:某 task 返回结果即判其获胜。注意同一 tick
        可能有多个 task 完成(asyncio.wait 的 done 集合可含多个),此时取遍历到的
        第一个非异常者为 winner,其余**已完成但未获胜**的 task 连同尚未完成的
        task 一起作为败者。

        关键:败者清理(对已完成者调 cleanup 释放流式 resp / 关上游裸连接;
        对未完成者 cancel 后由其自身 except 分支关资源)被打包成后台 task
        (_drain_losers),_race 不等待其完成即返回赢家——这把败者清理移出
        首字节关键路径,降低赢家 TTFB。后台 task 存入 _pending_cleanups,
        stop() 收尾排空,防连接泄漏。

        cleanup(result) 仅对"已完成且非取消"的败者调用(它们持有需要显式释放
        的资源,如流式 resp);被 cancel 的败者由其 _try_http/_try_tunnel 的
        except BaseException 分支自行关闭。
        """
        winner = None
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            winner_task = None
            for t in done:
                try:
                    winner = t.result()
                    # HTTP 5xx 不算胜出(见 _is_acceptable_win):跳过,保持 winner
                    # 为 None,让本批继续等待其他候选/兜底;该 t 仍是败者(由下方
                    # losers 收集并经 cleanup 释放 resp)。
                    if not self._is_acceptable_win(winner):
                        winner = None
                        continue
                    winner_task = t
                    break
                except Exception:
                    pass
            if winner:
                # 败者 = 未完成者(tasks) ∪ 已完成但未获胜者(done 去掉 winner_task)。
                # 旧实现只清理 tasks,漏掉 done 里的其余完成者 → 它们的 resp 泄漏。
                losers = set(tasks)
                for t in done:
                    if t is not winner_task:
                        losers.add(t)
                # 立即取消未完成者(停止读 body、释放竞速槽/连接池);已完成者
                # 无需 cancel,直接进 _drain_losers 由 cleanup 释放资源。
                for t in tasks:
                    t.cancel()
                if losers and cleanup is not None:
                    # 软上限:持续高吞吐下败者清理 task 会堆积(soak 曾观测
                    # fd_peak 569)。超过阈值则就地排空已完成的清理 task,
                    # 释放其持有的流式 resp / 上游连接,避免无界增长。就地
                    # gather 只等已完成的清理(多为秒级 aclose),不阻塞赢家
                    # 首字节——此刻赢家早已返回,这是下一轮竞速前的间隙。
                    if len(self._pending_cleanups) >= _MAX_PENDING_CLEANUPS:
                        stale = self._pending_cleanups
                        self._pending_cleanups = set()
                        await asyncio.gather(*stale, return_exceptions=True)
                    cleanup_task = asyncio.create_task(
                        self._drain_losers(losers, cleanup))
                    self._pending_cleanups.add(cleanup_task)
                    cleanup_task.add_done_callback(self._pending_cleanups.discard)
                break
        return winner

    async def _race_staggered(self, places, cleanup=None,
                              initial: int = 1, interval: float = 0.25,
                              method: str = "", url: str = "",
                              headers: Optional[dict] = None, body: Optional[bytes] = None) -> Optional[Any]:
        """错峰启动竞速(RFC 8305 §5):先发最优 initial 个,间隔 interval 补发,首字节成功即取消其余。

        与 _race 的差异只在**候选的启动时机**:
        - _race 首批同时全发,赢家由首字节最快者决定;
        - 本方法首批只发 initial 个(默认 1 个),此后**按 interval 定时补发**下一个,
          首个候选拿到响应头/CONNECT 200 即判胜,取消其余未完成/未开始的候选,
          败者清理下放 _drain_losers(同 _race)。

        定时补发是 RFC 8305 §5 的关键:补发**不等待**上一候选失败——若最优者恰好
        半开挂起,后发者仍能按 interval 及时顶上,竞速的"慢时兜底"能力得以保留。
        相比 _race 同时全发,错峰让先发的优质代理先到,劣质代理大概率根本不发;
        HTTP 候选不发就不双写上游流量,CONNECT 候选不发就不必建好隧道再关(最浪费),
        扇出与败者清理成本随未发候选数线性下降。代价:若先发者慢,TTFB 最坏多等
        一个 interval(默认 250ms,RFC 8305 容限内);EWMA 排序保证先发的几乎总是
        历史最快者,此代价只在网络突变时出现。

        places 是**有序候选占位**(pid 或 (pid, target)),按"最优在前"排列;真 task
        只在补发时经 _make_race_task 惰性创建。若急切 create_task,事件循环立刻
        调度,错峰退化为同时全发。前 initial 个占位首批同时发出,其后每个 interval
        从前方 pop 一个补发(保证"下一个最优者"先补)。

        `winner` 为 None 只表示"已发候选全部失败",不表示"未胜出就中止"——循环会
        把未发候选按 interval 逐一补发完才结束,让调用方据此走兜底批。
        """
        headers = headers or {}
        places = list(places)
        initial = max(1, min(initial, len(places)))
        running: set = set()
        for p in places[:initial]:
            running.add(self._make_race_task(p, method, url, headers, body))
        # 未发候选:后补发的先 pop 先发,故反转成"从最优端 pop"。
        unlaunched = places[initial:]
        unlaunched.reverse()
        # 已完成的候选累积:失败候选的异常需在收尾时 retrieval(_drain_losers 的
        # gather + result),否则 asyncio 报 "Task exception was never retrieved"。
        completed: set = set()
        winner = None
        while running or unlaunched:
            # 等待首字节;interval 超时无候选完成则返回(未完成者仍在 running 里),
            # 用于定时补发下一个。有候选完成则 done 含该候选。
            done, running = await asyncio.wait(
                running, return_when=asyncio.FIRST_COMPLETED, timeout=interval)
            # 判胜:任一候选拿到结果(响应头/CONNECT 200)即获胜;HTTP 5xx 不算胜出
            # (见 _is_acceptable_win),跳过并继续补发/等待其他候选。
            winner_task = None
            for t in done:
                completed.add(t)
                try:
                    winner = t.result()
                    if not self._is_acceptable_win(winner):
                        winner = None
                        continue
                    winner_task = t
                    break
                except Exception:
                    pass
            if winner is not None:
                losers = set(running)
                for t in completed:
                    if t is not winner_task:
                        losers.add(t)
                for t in running:
                    t.cancel()
                if losers:
                    self._spawn_cleanup(losers, cleanup)
                return winner
            # 无胜者(完成候选均失败/被取消):定时补发下一个候选(若有)。
            if unlaunched:
                running.add(self._make_race_task(unlaunched.pop(), method, url, headers, body))
        return winner

    def _spawn_cleanup(self, losers: set, cleanup):
        """把竞速败者清理下放后台 task(_drain_losers),带软上限就地排空。

        _race / _race_staggered 共用:败者清理不阻塞赢家首字节。软上限阈值
        _MAX_PENDING_CLEANUPS 下,持续高吞吐时先就地 gather 已完成的清理 task,
        释放其持有的流式 resp / 上游连接,避免 _pending_cleanups 无界堆积。
        """
        if not losers:
            return
        if cleanup is not None:
            if len(self._pending_cleanups) >= _MAX_PENDING_CLEANUPS:
                stale = self._pending_cleanups
                self._pending_cleanups = set()
                asyncio.get_running_loop().create_task(
                    asyncio.gather(*stale, return_exceptions=True))
            cleanup_task = asyncio.create_task(self._drain_losers(losers, cleanup))
            self._pending_cleanups.add(cleanup_task)
            cleanup_task.add_done_callback(self._pending_cleanups.discard)

    async def _drain_losers(self, losers: set, cleanup):
        """后台清理竞速败者:等未完成者取消结束,对已完成者调 cleanup。

        由 _race 下放,不阻塞赢家首字节。完成后从 _pending_cleanups 自移除
        (经 add_done_callback)。任何异常静默——败者清理失败不影响赢家。
        """
        try:
            await asyncio.gather(*losers, return_exceptions=True)
            for t in losers:
                if t.done() and not t.cancelled():
                    try:
                        await cleanup(t.result())
                    except Exception:
                        pass
        except Exception:
            pass

    def _make_race_task(self, place, method: str, url: str, headers: dict,
                        body: Optional[bytes]) -> asyncio.Task:
        """把一个候选占位(pid 或 (pid, target))惰性创建为竞速 task。

        统一工厂供 _race_staggered 补发候选:place 为字符串 pid 时建 HTTP task
        (_try_http,经上游代理转发;pid='local' 直连);place 为 (pid, target) 时建
        CONNECT 隧道 task(_try_tunnel,经上游 CONNECT)。延迟到调用时才 create_task,
        保证"未发候选不启动"——这是错峰与 _race 同时全发的本质区别。
        """
        if isinstance(place, tuple):
            pid, target = place
            proxy = self.proxy_store.get(pid)
            if proxy is None:
                # 本机直连路径:pid 为 'local' 时 proxy 不存在,proxy_host 置 None。
                return asyncio.create_task(self._try_tunnel(pid, target, None, None, None))
            return asyncio.create_task(
                self._try_tunnel(pid, target, proxy.host, proxy.port, proxy.auth))
        pid = place
        proxy = self.proxy_store.get(pid)
        if proxy is None:
            return asyncio.create_task(self._try_http('local', None, method, url, headers, body))
        return asyncio.create_task(
            self._try_http(pid, self._build_proxy_url(proxy), method, url, headers, body))

    @staticmethod
    async def _cleanup_http_result(result):
        """关闭竞速中已完成但未获胜的 HTTP task 持有的流式 resp。

        池化后 client 不关闭(留给后续请求复用),只 aclose 流式响应,释放
        其占用的上游连接归还到池中。
        """
        if not result:
            return
        # result = (pid, method, url, resp, client)
        resp = result[3]
        try:
            await resp.aclose()
        except Exception:
            pass

    @staticmethod
    async def _cleanup_tunnel_result(result):
        """关闭竞速中已完成但未获胜的 CONNECT task 持有的上游连接。

        CONNECT 走裸 socket(无连接池),连接不可跨请求复用,故直接关闭。
        """
        if not result:
            return
        up_writer = result[-1]
        try:
            up_writer.close()
            await up_writer.wait_closed()
        except Exception:
            pass

    @staticmethod
    async def _pipe(reader, writer, close_writer: bool = True):
        """把 reader 的数据单向搬运到 writer,直至 EOF 或超时/异常。

        用于 CONNECT 隧道的双向透传(两个 _pipe 反向组合)。300s 读超时
        防止半开连接永久占用;任何异常都静默关闭 writer。
        close_writer=False 时结束不关 writer——供隧道归还场景:客户端断开后
        保留上游连接,由 _relay_tunnel 判断是否归还已握手池。
        """
        try:
            while True:
                data = await asyncio.wait_for(reader.read(65536), timeout=300)
                if not data:
                    break
                writer.write(data)
                await writer.drain()
        except Exception:
            pass
        if close_writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    @staticmethod
    async def _write_cached_response(writer, status_code, reason_phrase, headers, body):
        """把缓存的整包响应写回客户端(状态行+头+body 已在内存)。

        与流式路径不同:缓存命中时 body 已完整在内存,直接整体写出即可。
        """
        hop_by_hop = _HOP_BY_HOP_RESPONSE_HEADERS
        # 缓存命中路径传入的是 list[(name, value)](保留重复头如多个 Set-Cookie);
        # 内部错误响应(407/502 等)传入 dict,两种都按 (k, v) 迭代即可。
        items = headers.items() if isinstance(headers, dict) else headers
        try:
            writer.write(f"HTTP/1.1 {status_code} ".encode('latin-1') + _hb(reason_phrase) + b"\r\n")
            for k, v in items:
                if k.lower() not in hop_by_hop:
                    writer.write(f"{k}: ".encode('latin-1') + _hb(v) + b"\r\n")
            writer.write(f"Content-Length: {len(body)}\r\n".encode('latin-1'))
            writer.write(b"\r\n")
            writer.write(body)
            await writer.drain()
        except (BrokenPipeError, ConnectionError, OSError):
            pass

    # ── HTTP 请求 ──────────────────────────────────────────────

    @staticmethod
    def _build_proxy_url(proxy) -> Optional[str]:
        """构造 httpx 代理 URL:`http://[user:pw@]host:port`。

        有上游认证时把凭据 URL 编码后嵌入(凭据含特殊字符也安全)。
        proxy 为 None 返回 None(表示不走上游,如本机竞速)。
        """
        if not proxy:
            return None
        if proxy.auth:
            user = urllib.parse.quote(proxy.auth['username'], safe='')
            pw = urllib.parse.quote(proxy.auth['password'], safe='')
            return f"http://{user}:{pw}@{proxy.host}:{proxy.port}"
        return f"http://{proxy.host}:{proxy.port}"

    async def _try_http(self, pid: str, proxy_url: Optional[str], method: str, url: str, headers: dict, body: Optional[bytes]):
        """经某上游代理尝试一次 HTTP 请求,作为竞速的一个候选(流式)。

        从连接池取长驻 client,以 stream=True 发送——收到响应头即返回(resp
        尚未读 body)。这是"首字节判胜"的基础:_race 在某候选返回响应头时
        即判其获胜,其余候选随即取消、其流式 resp 被 aclose(见 _cleanup),
        不再下载整包。获胜者的 body 由调用方在 _stream_upstream_response 中
        边收边转发,client 用完归还连接池(不关闭)。

        成功返回 (pid, method, url, resp, client);失败(BaseException,含
        CancelledError)关闭 resp 并向上抛出,让 _race 的清理逻辑处理。
        """
        key = self._client_key(pid, proxy_url)
        client = await self._get_client(key, proxy_url)
        resp = None
        # 计入该代理在途数:从"发起尝试"到"收到响应头/失败/被取消"的整个窗口,
        # 供加权 least-request 选批避开积压代理。finally 中无论何种出口都释放。
        self.selector._inflight_start(pid)
        try:
            self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
            self.upstream_attempts += 1  # 聚合竞速扇出总数(供 /metrics 算放大率)
            # 首字节计时:从发起到收到响应头。用于 EWMA 质量跟踪(竞速排序)。
            t0 = time.perf_counter()
            resp = await client.send(
                client.build_request(method, url, headers=headers, content=body),
                stream=True)
            self.selector.record_ttfb(pid, time.perf_counter() - t0)
            self.request_counts[pid] = self.request_counts.get(pid, 0) + 1
            domain = urllib.parse.urlparse(url).hostname or url
            # 仅记尝试统计(竞速扇出);meta 由 _handle_http_request 在确认赢家后
            # 调 _record_win_meta 写一次,避免败者覆写域名缓存。
            self._record_attempt(domain, pid)
            # 收到响应头即视为一次成功观测(EWMA + 连续失败归零)。
            self.selector.record_success(pid)
            return pid, method, url, resp, client
        except BaseException as ex:
            # 仅在确实取得流式 resp 时才 aclose;client.build_request / client.send
            # 在赋值前抛错时 resp 仍为 None,无条件 aclose 会抛 UnboundLocalError
            # 被吞掉并掩盖根因。client 始终留在连接池,不在此关闭。
            if resp is not None:
                try:
                    await resp.aclose()
                except Exception:
                    pass
            # 竞速落败被取消(CancelledError)不算失败——健康慢代理每次竞速都会
            # 被快代理抢先取消,若计入会误熔断。真失败(连接/超时/上游错误)
            # 才累计连续失败并可能触发熔断。
            if not isinstance(ex, asyncio.CancelledError) and pid != 'local':
                self.selector.record_failure(pid)
            raise
        finally:
            self.selector._inflight_finish(pid)

    async def _try_tunnel(self, pid: str, target: str, proxy_host: Optional[str], proxy_port: Optional[int], proxy_auth: Optional[dict]):
        """尝试建立一条 CONNECT 隧道,作为竞速的一个候选。

        - proxy_host 给定:经该上游代理发起 CONNECT(带上游 Proxy-Authorization)。
        - proxy_host 为 None:直连 target(本机竞速路径)。target 形如
          "host:port" 或 "[ipv6]:port"。
        建连与读响应均设 connect_timeout(15s),防止挂死上游长期占用竞速槽。
        成功返回 (pid, up_reader, up_writer);失败/被取消则关闭上游连接并抛出。
        """
        # 建立 CONNECT 与读取响应均设超时，避免挂死的上游无限占用竞速 task 与连接。
        connect_timeout = 15
        try:
            if proxy_host is not None:
                # CONNECT 预热池(P1)+ 目标半预连接(P2):取用顺序为——
                # 1) target 半预连接池(按 proxy|target 键,只预连"到上游代理"的
                #    TCP,未发 CONNECT,可安全复用);2) 第一阶段通用池;3) 新建。
                # 取用成功即省掉"本机→上游"建连 TTFB。
                if self.conn_pool_enabled:
                    up_reader, up_writer = None, None
                    # 已握手隧道复用:优先取"已发 CONNECT 且收到 200"的连接,命中则
                    # 跳过下方 CONNECT 握手,直接返回(连接已处于可透传状态)。
                    if self.conn_pool_established_reuse:
                        up_reader, up_writer = self._established_pool_peek(proxy_host, proxy_port, target) or (None, None)
                    if up_reader is None and self.conn_pool_target_prewarm:
                        up_reader, up_writer = self._target_pool_peek(proxy_host, proxy_port, target) or (None, None)
                    if up_reader is None:
                        pooled = self._conn_pool_peek(proxy_host, proxy_port)
                        if pooled is not None:
                            up_reader, up_writer = pooled
                    if up_reader is None:
                        self.connect_new_conns += 1  # 观测:池未中需新建
                        up_reader, up_writer = await asyncio.wait_for(
                            asyncio.open_connection(proxy_host, proxy_port), timeout=connect_timeout)
                else:
                    self.connect_new_conns += 1  # 观测:无池路径每次新建
                    up_reader, up_writer = await asyncio.wait_for(
                        asyncio.open_connection(proxy_host, proxy_port), timeout=connect_timeout)
            else:
                if ':' not in target:
                    raise ValueError(f'Invalid CONNECT target: {target}')
                if target.startswith('['):
                    host_end = target.find(']')
                    host = target[1:host_end]
                    port = int(target[host_end + 2:])
                else:
                    host, port_str = target.rsplit(':', 1)
                    port = int(port_str)
                up_reader, up_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=connect_timeout)
        except (asyncio.TimeoutError, OSError, ConnectionError) as e:
            raise RuntimeError(f'connect to {proxy_host or target} timed out or failed: {e}') from e
        # 首字节计时:从 CONNECT 发出到收到 200。用于 EWMA 质量跟踪(竞速排序)。
        # 复用已握手隧道时无 CONNECT 往返,计时为 0(不做 EWMA 观测)。
        reused_established = (self.conn_pool_established_reuse
                              and proxy_host is not None
                              and up_reader is not None
                              and getattr(up_writer, '_established_reused', False))
        t0 = time.perf_counter()
        # 计入该代理在途数(从 CONNECT 发起到拿到 200/失败/被取消),finally 释放。
        self.selector._inflight_start(pid)
        try:
            if not reused_established:
                auth_hdr = ""
                if proxy_auth:
                    raw = f"{proxy_auth['username']}:{proxy_auth['password']}"
                    encoded = base64.b64encode(raw.encode()).decode()
                    auth_hdr = f"Proxy-Authorization: Basic {encoded}\r\n"
                up_writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: ".encode('latin-1') + _hb(target) + f"\r\n{auth_hdr}\r\n".encode('latin-1'))
                await up_writer.drain()
                self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
                self.upstream_attempts += 1  # 聚合竞速扇出总数(供 /metrics 算放大率)
                status = await asyncio.wait_for(up_reader.readline(), timeout=connect_timeout)
                if not status:
                    raise RuntimeError('no response from upstream')
                status_text = status.decode('latin-1')
                if '200' not in status_text:
                    while True:
                        h = await up_reader.readline()
                        if not h or h in (b"\r\n", b"\n"):
                            break
                    raise RuntimeError(f'upstream returned non-200 for CONNECT: {status_text.strip()}')
                while True:
                    h = await up_reader.readline()
                    if not h or h in (b"\r\n", b"\n"):
                        break
                self.request_counts[pid] = self.request_counts.get(pid, 0) + 1
                self.selector.record_ttfb(pid, time.perf_counter() - t0)
            # 仅记尝试统计;meta 由 _handle_connect 在确认赢家后调 _record_win_meta。
            self._record_attempt(target, pid)
            # CONNECT 拿到 200 即视为一次成功观测(EWMA + 连续失败归零)。
            self.selector.record_success(pid)
            return pid, up_reader, up_writer
        except BaseException as ex:
            try:
                up_writer.close()
                await up_writer.wait_closed()
            except Exception:
                pass
            # 同 _try_http:被竞速取消(CancelledError)不算失败;真失败才累计熔断。
            if not isinstance(ex, asyncio.CancelledError) and pid != 'local':
                self.selector.record_failure(pid)
            raise
        finally:
            self.selector._inflight_finish(pid)

    # ── 客户端入口 ──────────────────────────────────────────────

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        """客户端连接入口:读首行+请求头,认证,再分流到 HTTP 或 CONNECT 处理。

        这是 HTTP 与 CONNECT 的唯一公共入口,客户端认证在此统一校验(分流前),
        因此未认证客户端不会触达任何上游。finally 中无论正常返回还是异常,
        都从 _running_tasks 移除当前 task 并关闭客户端连接。
        """
        task = asyncio.current_task()
        self._running_tasks.add(task)
        peer = writer.get_extra_info('peername')
        # 会话粘性的客户端键:仅取 IP(不带端口),同一客户端复用;无 peer 时
        # 退化为空串(粘性关闭时不影响,开启时该请求只走域名缓存/竞速)。
        client_ip = peer[0] if peer else ""
        logger.debug("client connected %s", peer)
        self._set_nodelay(writer)
        try:
            line = await reader.readline()
            if not line:
                return
            first = line.decode('latin-1').strip()
            headers = bytearray()
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
                headers.extend(h)
            logger.debug("first line: %s", first)
            # 一次性把请求头字节解析成 dict(键保留原大小写),auth 与 body
            # 长度判定及下游转发共用此 dict,不再各自重新 decode+split 头部。
            # HTTP 头字段为 ASCII,latin-1 解码安全;body 不在此解码(见下)。
            req_headers = {}
            for h in headers.decode('latin-1').split('\r\n'):
                if ':' in h:
                    k, v = h.split(':', 1)
                    req_headers[k.strip()] = v.strip()
            # 客户端认证：在 CONNECT/HTTP 分流前统一校验，未通过则返回 407，
            # 不进行任何上游连接/竞速/DB 写入。auth_enabled=False 时放行。
            if self.auth_enabled:
                ok, reason = check_auth(req_headers, self.auth_enabled, self.auth_username, self.auth_password)
                if not ok:
                    logger.info("auth rejected for %s: %s", peer, reason)
                    await self._write_cached_response(writer, 407, 'Proxy Authentication Required',
                                               {'Proxy-Authenticate': 'Basic realm="auto_squid"',
                                                'Content-Type': 'text/plain'},
                                               _hb(reason or 'Authentication required'))
                    return
            # 有效客户端请求(认证通过):刷新 refill 空闲感知的活动时间戳,解除深夜暂停。
            self._record_request_activity()
            if first.upper().startswith('CONNECT'):
                target = first.split(' ')[1]
                await self._handle_connect(target, reader, writer, client_ip)
            else:
                # 首行合法性提前校验(原由 _handle_http_request 做):缺方法/URL
                # 直接 400,不必再拼包传下去重新解析。
                parts = first.split(' ')
                if len(parts) < 3:
                    writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\n\r\nBad Request")
                    await writer.drain()
                    return
                method, url = parts[0], parts[1]
                body = b''
                cl = None
                for k, v in req_headers.items():
                    if k.lower() == 'content-length':
                        cl = int(v)
                        break
                if cl is not None and cl > 0:
                    if cl > MAX_BODY:
                        writer.write(b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 15\r\n\r\nPayload Too Large")
                        await writer.drain()
                        return
                    body = await reader.readexactly(cl)
                elif cl is None and method.upper() in ('POST', 'PUT', 'PATCH'):
                    # 无 Content-Length 头：分块读取至上限，避免 read(-1) 阻塞到
                    # 客户端关闭连接而破坏 HTTP keep-alive。注意 cl is None 与
                    # cl == 0 不同——后者表示头部存在但 body 为空，应直接用 b''。
                    body = bytearray()
                    while len(body) < MAX_BODY:
                        chunk = await reader.read(MAX_BODY - len(body))
                        if not chunk:
                            break
                        body.extend(chunk)
                    if len(body) >= MAX_BODY:
                        writer.write(b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 15\r\n\r\nPayload Too Large")
                        await writer.drain()
                        return
                # 直接传已解析的 method/url/headers/body,不再拼回 request_bytes
                # 让下游重新 find+decode+split(消除双重解析)。
                await self._handle_http_request(method, url, req_headers, bytes(body) if isinstance(body, bytearray) else body, writer, client_ip)
        except Exception:
            logger.exception("error handling client")
        finally:
            self._running_tasks.discard(task)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    # ── HTTP 请求处理 ──────────────────────────────────────────

    def _build_racing_tasks_http(self, proxies: List[str], host: str = "") -> set:
        """为 HTTP 竞速产出候选占位集合(前 max_retries 个 pid + 本机 local)。

        N 由 max_retries 限制(本批只竞速前 N 个)。返回的 set 交给 _race(真 task)
        或 _race_staggered(惰性占位,补发时才创建)。占位为 pid 字符串,
        _make_race_task 据此建 _try_http task。host 给策略路由:命中策略时
        proxies 已由调用方按策略收窄;local 仅当策略放行时加入。
        """
        places = {pid for pid in proxies[:self.max_retries] if self.proxy_store.get(pid)}
        if self.enable_local_racing and self._policy_allows_sticky(host, 'local'):
            places.add('local')
        return places

    def _stagger_initial(self) -> int:
        """首批并发数:冷启动(无任何 EWMA 历史)时翻倍,其余用配置值。

        RFC 8305 §5 允许有历史 RTT 时首批发多个。冷启动时排序等于均匀随机,
        只发 1 个会概率性丢掉快代理(随机首抽到慢者即败)——翻倍到 2 个同时赌两个
        最优者,等价于旧 _race 的兜底能力;一旦学得任一 EWMA 即回落到 stagger_initial
        (历史排序可信,首批单发即可)。与 _race 的差异只在候选启动时机,不影响
        max_retries 的候选总数上限。
        """
        if not self.selector.get_quality():
            return min(self.max_retries, max(2, self.stagger_initial))
        return self.stagger_initial

    def _prep_http(self, proxies: List[str], host: str = "") -> tuple:
        """HTTP 竞速的启动参数:首批/补发按 stagger 配置取占位,返回 (initial_places, remaining)。

        供 _forward_upstream 统一拼接 _race_staggered 的调用。`initial_places` 是
        首批要同时发出的**有序**占位列表(最优先发出,保持 proxies 的 EWMA 排序);
        `remaining` 是待定时补发的**有序**占位列表。本机竞速开启时 local 优先
        (直连,常最快)。占位为 pid 字符串,_make_race_task 据此建 _try_http task。
        host 给策略路由:local 仅当策略放行时参与。
        """
        n_initial = self._stagger_initial()
        initial_pids = proxies[:n_initial]
        if self.enable_local_racing and 'local' not in initial_pids \
                and self._policy_allows_sticky(host, 'local'):
            initial_pids = ['local'] + initial_pids
        initial_places = [pid for pid in initial_pids
                          if pid == 'local' or self.proxy_store.get(pid)]
        remaining = [pid for pid in proxies
                     if pid not in initial_places and (pid == 'local' or self.proxy_store.get(pid))]
        return initial_places, remaining

    async def _handle_http_request(self, method: str, url: str, headers: dict, body: bytes, writer: asyncio.StreamWriter, client_ip: str = ""):
        """处理一个完整 HTTP 请求(已解析好的 method/url/headers/body),按优先级回写响应。

        决策顺序(命中即返回):
        1. HTTP 响应缓存命中 → 直接回写缓存响应(整包在内存)。
        2. 会话粘性命中(客户端+域名) → 用该代理单发请求(不竞速);失败则继续。
        3. 域名缓存命中 → 用该代理单发请求(不竞速);失败则继续。
        4. 竞速:首批 max_retries 个代理并行,全失败且有剩余则对剩余再竞速。
        5. 全失败 → 502。成功 2xx 顺带写入响应缓存(流式边转边缓冲)。

        竞速采用首字节判胜:某候选拿到响应头即获胜,其余取消;获胜者 body
        由 _stream_upstream_response 边收边转发。请求头转发前剔除
        hop-by-hop 头(下方),避免把客户端访问本代理的凭据
        (Proxy-Authorization)等透传给上游。

        解析已在 handle_client 一次性完成并传入,此处不再重复 find+decode+split。
        body 为原始字节(未解码),保留二进制安全。
        """
        domain = urllib.parse.urlparse(url).hostname or url
        # 剔除 hop-by-hop 请求头:只服务"客户端→本代理"这一跳,不透传上游。
        hdrs = {k: v for k, v in headers.items() if k.lower() not in _HOP_BY_HOP_REQUEST_HEADERS}
        body = body or None

        # 计数:进入 HTTP 处理先记一次 miss;响应缓存命中分支会把它翻成 hit。
        # 入口先记 miss 是为避免漏计多条 return 路径(竞速成功/全失败/域名缓存命中)。
        self.http_cache_misses += 1

        # 0) 写方法失效:POST/PUT/DELETE/PATCH 改写资源,提前清掉该域名的所有
        #    GET 缓存条目,使随后的 GET 回源拿新内容(否则 60s TTL 内会返回变更
        #    前旧响应)。按域名失效而非按 URL:添加动作常打 POST /api/items,而
        #    刷新的列表页是 GET /,URL 不同,按 URL 精确失效会漏掉列表页。放在
        #    缓存读取前、转发前,覆盖所有后续 return 路径;写请求即便最终失败,
        #    后果也只是下次 GET 多回源一次。
        if method.upper() in _INVALIDATING_METHODS:
            self._http_cache_invalidate(domain)

        # 1) HTTP 响应缓存:GET 幂等响应直接命中,完全不经上游。
        cached_entry = self._http_cache_get(method, url)
        if cached_entry:
            # 翻转:命中响应缓存,把入口记的 miss 撤回、改记 hit。
            self.http_cache_misses -= 1
            self.http_cache_hits += 1
            logger.debug("HTTP cache hit %s %s", method, url)
            await self._write_cached_response(writer, cached_entry['status_code'], cached_entry['reason_phrase'],
                                       cached_entry['headers'], cached_entry['content'])
            return

        # 1.5) 在途 GET 去重聚合:同 URL 并发 GET 命中未命中缓存时,若已有在途
        #      请求(首个请求正在转发上游),则 await 其结果,不再重复打上游。
        #      首个请求完成后把结果 set 进 Future,waiter 据此回写客户端。仅
        #      GET 适用——非 GET 方法不缓存也不聚合。结果 None 表示首个请求
        #      失败,waiter 需自行走域名缓存/竞速路径。
        #      超时保护:waiter 等待未来 _AGG_WAIT_TIMEOUT 未完成则放弃聚合、
        #      自行竞速,避免慢上游下 waiter 挂住连接导致 fd 堆积(压测观测
        #      rate 场景 fd_peak 冲到 300+)。放弃后该 Future 仍由首个请求在
        #      finally 中 resolve,waiter 不再 await,无副作用。
        agg_key = self._http_cache_key(method, url)
        agg_fut = None
        if method == 'GET':
            existing = self._inflight_futures.get(agg_key)
            if existing is not None:
                try:
                    logger.debug("coalescing %s %s (in-flight)", method, url)
                    agg_result = await asyncio.wait_for(existing, timeout=_AGG_WAIT_TIMEOUT)
                except asyncio.TimeoutError:
                    logger.debug("coalescing timeout %s %s, fall back to racing", method, url)
                    existing = None
                else:
                    if agg_result is not None:
                        status_code, reason_phrase, headers, content = agg_result
                        await self._write_cached_response(writer, status_code, reason_phrase, headers, content)
                        return
                    existing = None
            if existing is None:
                agg_fut = asyncio.get_running_loop().create_future()
                self._inflight_futures[agg_key] = agg_fut
        try:
            await self._forward_upstream(writer, method, url, hdrs, body, domain, client_ip)
        finally:
            # 仅 GET 且本请求持有在途 Future 时 resolve(成功→结果,失败→None 让
            # waiter 自行竞速),并从在途表移除。若上方缓存/聚合命中则本请求不
            # 持有 Future,此为空操作。非 GET 永远不进聚合表,同样为空操作。
            if agg_fut is not None:
                self._inflight_futures.pop(agg_key, None)
                if not agg_fut.done():
                    # 从响应缓存取刚写入的条目作为聚合结果(内容可能超
                    # STREAM_CACHE_LIMIT 未入缓存 → 无条目 → 回 None,waiter 自行竞速)。
                    entry = self._http_cache_get(method, url)
                    if entry is not None:
                        agg_fut.set_result((entry['status_code'], entry['reason_phrase'],
                                            entry['headers'], entry['content']))
                    else:
                        agg_fut.set_result(None)

    async def _forward_single(self, writer, method: str, url: str, hdrs: dict, body, domain: str,
                             pid: str | None = None, instantiated=None, sticky: bool = False):
        """流式转发一个已取得胜利的响应并视情写入响应缓存,作为统一收尾。

        供域名缓存命中单发、会话粘性命中单发 与 竞速赢家三条路径共用:流式
        转发 body → 关闭上游流式 resp(内存),2xx/可缓存 且 body 未超上限则
        写响应缓存。
        instantiated=(pid, resp) 表示已由竞速拿到的流式响应(不再 _try_http);
        pid 非 None 表示域名缓存/会话粘性单发的代理 id(内部 _try_http,失败
        抛出让调用方回退)。sticky=True 时单发成功计入 sticky_cache_hits(否则
        计入 domain_cache_hits)。
        """
        if pid is not None:
            try:
                proxy = self.proxy_store.get(pid)
                _pid, method, url, resp, client = await self._try_http(
                    pid, self._build_proxy_url(proxy), method, url, hdrs, body)
            except Exception:
                raise  # 单发失败:让调用方回退到竞速
        else:
            pid, resp = instantiated
        # 单发成功 → 记一次(单发失败回退竞速的不算);竞速赢家路径(instantiated)
        # 不计——竞速命中率只统计"未竞速即命中"的单发。
        if instantiated is None:
            if sticky:
                self.sticky_cache_hits += 1
            else:
                self.domain_cache_hits += 1
        buffered = await self._stream_upstream_response(writer, resp, method, url)
        try:
            await resp.aclose()
        except Exception:
            pass
        if buffered is not None and resp.status_code in CACHEABLE_STATUS:
            self._http_cache_set(method, url, resp.status_code, resp.reason_phrase,
                                 list(resp.headers.multi_items()), buffered)
        return resp.status_code

    async def _forward_upstream(self, writer, method: str, url: str, hdrs: dict, body, domain: str, client_ip: str = ""):
        """把请求转发上游(会话粘性单发 → 域名缓存单发 → 竞速 → 兜底竞速 → 502)。

        从 _handle_http_request 提取,供在途 GET 去重聚合统一在 finally 中
        resolve Future。优先级:同一客户端+域名的会话粘性 > 全局域名缓存 >
        竞速。粘性/域名缓存命中即单发(失败回退下一级),竞速失败有剩余则
        对剩余再竞速。赢家同时回填粘性表与域名缓存 meta。
        """
        # 1) 会话粘性:同一客户端+域名复用上次胜出的代理单发(滑动 TTL)。
        #    失败则驱逐该条目,回落到域名缓存/竞速(redispatch)。单发成功但
        #    返回 5xx 同样驱逐(A2:响应已流式发出无法重试,下一请求竞速换新)。
        #    非 5xx 成功则滑动 TTL 并累加命中次数(B2)。
        skip_domain_cache = False
        if domain and self.stickiness_enabled:
            sticky_pid = self._get_sticky_proxy(client_ip, domain)
            if sticky_pid:
                try:
                    status = await self._forward_single(
                        writer, method, url, hdrs, body, domain, sticky_pid, sticky=True)
                    if status is not None and status >= 500:
                        self._evict_sticky(client_ip, domain)
                    else:
                        self._bump_sticky(client_ip, domain, sticky_pid)
                    return
                except Exception:
                    logger.debug("sticky proxy %s failed for %s", sticky_pid, domain)
                    self._evict_sticky(client_ip, domain)
            elif self._sticky_recheck_due(client_ip, domain):
                # B2:探路重评估到期——驱逐并跳过域名缓存,直接竞速换新赢家。
                self._evict_sticky(client_ip, domain)
                skip_domain_cache = True

        # 2) 域名缓存:用上次胜出的代理单发请求(不重复更新 meta——_try_http
        #    内部只记尝试统计),失败则回退到竞速。单发路径同样流式转发。
        #    成功时也回填粘性表:粘性可能因上一轮 redispatch 被驱逐,而域名
        #    缓存仍有效;若不回填,该客户端+域名会一直丢粘性直到域名缓存过期。
        if domain and not skip_domain_cache:
            cached_pid = self._get_fresh_proxy(domain)
            if cached_pid:
                try:
                    result = await self._forward_single(
                        writer, method, url, hdrs, body, domain, cached_pid)
                    self._record_sticky(client_ip, domain, cached_pid)
                    return result
                except Exception:
                    logger.debug("cached proxy %s failed for %s", cached_pid, domain)

        # 3) 竞速:首批并行 max_retries 个代理,全失败且还有剩余则对剩余再竞速。
        #    错峰启动(stagger_start)时首批只发 stagger_initial 个(默认 1 个),
        #    补发剩余占位交 _race_staggered 按 interval 定时补发;否则同时全发。
        #    策略路由(P1):按目标域名收窄候选集,命中策略的域只在该子集内竞速。
        proxies = self.selector.ordered_proxies()
        if self._policies:
            proxies = self._policy_candidate_pids(domain, proxies)
        if not proxies and not self.enable_local_racing:
            await self._write_cached_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')
            return

        # 计数:进入竞速(首批)。兜底批单独再 +1,故 invocations 可能 > 请求数。
        self.racing_invocations += 1
        if self.stagger_start:
            initial_places, remaining = self._prep_http(proxies, domain)
            winner_resp = await self._race_staggered(
                initial_places + remaining, cleanup=self._cleanup_http_result,
                initial=len(initial_places), interval=self.stagger_interval,
                method=method, url=url, headers=hdrs, body=body)
        else:
            # 非错峰(_race):需真 task,占位经 _make_race_task 急切创建。
            places = self._build_racing_tasks_http(proxies, domain)
            tasks = {self._make_race_task(p, method, url, hdrs, body) for p in places}
            winner_resp = await self._race(tasks, cleanup=self._cleanup_http_result)

            # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
            if not winner_resp and len(proxies) > self.max_retries:
                self.racing_invocations += 1
                remaining = proxies[self.max_retries:]
                places = self._build_racing_tasks_http(remaining)
                tasks = {self._make_race_task(p, method, url, hdrs, body) for p in places}
                winner_resp = await self._race(tasks, cleanup=self._cleanup_http_result)

        if winner_resp:
            pid, method, url, resp, client = winner_resp
            logger.debug("proxy %s racing win %s %s", pid, method, url)
            # 仅赢家更新域名缓存 meta:竞速中败者只记了 _record_attempt,不会反被
            # 覆写 _meta_cache;domain 在上方已算好(同一 urlparse)。
            self._record_win_meta(domain, pid)
            if domain:
                self._record_sticky(client_ip, domain, pid)
            return await self._forward_single(writer, method, url, hdrs, body, domain, instantiated=(pid, resp))

        logger.error("all proxies failed for HTTP request")
        await self._write_cached_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')

    async def _stream_upstream_response(self, client_writer, resp, method: str, url: str) -> Optional[bytes]:
        """把上游流式响应转发给客户端,同时边收边缓冲(供响应缓存)。

        关键:首字节判胜后,获胜者的 body 在这里逐块转发,客户端无需等待
        整包到达代理即可拿到首字节(TTFB 下降)。同时把已转发的字节缓冲到
        内存(上限 self._stream_cache_limit),收齐且为 2xx 时写入响应缓存——这样
        流式路径仍能命中缓存,无需把整包读进内存才缓存。

        长度策略:若上游提供 content-length,转发头时剔除它(避免与 chunked
        重复)但单独按上游原值重写一条 content-length(aiter_raw 给的是已编码
        原始字节,与该值语义一致,长度正确);否则用 HTTP/1.1 chunked 传输编码
        逐块写出。两种方式都保证客户端能正确界定 body 边界,且不破坏流式收益。

        返回缓冲的 body(若未超上限);超过上限返回 None 表示放弃缓存。
        客户端断开时静默,但仍尽量把已读字节丢弃以释放上游连接。
        """
        client_disconnected = False
        # 先决定 body 的定界方式。
        upstream_cl = resp.headers.get('content-length')
        use_chunked = upstream_cl is None
        try:
            # 状态行 + 转发头(剔除 hop-by-hop,含 content-length)。
            # 用 multi_items():httpx 的 items() 会把同名头(如多个 Set-Cookie)合并成
            # 逗号拼接的单行值,浏览器据此只解析出第一个 cookie,其余(如 Django 的
            # sessionid)被当未知属性丢弃,导致登录会话丢失。逐条写回保留重复头。
            client_writer.write(f"HTTP/1.1 {resp.status_code} ".encode('latin-1') + _hb(resp.reason_phrase) + b"\r\n")
            for k, v in resp.headers.multi_items():
                if k.lower() in _HOP_BY_HOP_RESPONSE_HEADERS:
                    continue
                client_writer.write(f"{k}: ".encode('latin-1') + _hb(v) + b"\r\n")
            if use_chunked:
                client_writer.write(b"Transfer-Encoding: chunked\r\n")
            else:
                # 上游给了 content-length:按其原值重写(aiter_raw 字节数与之等长)。
                client_writer.write(f"Content-Length: {upstream_cl}\r\n".encode('latin-1'))
            client_writer.write(b"\r\n")
            await client_writer.drain()
        except (BrokenPipeError, ConnectionError, OSError):
            client_disconnected = True

        buffered = bytearray()
        buffering = True
        try:
            async for chunk in resp.aiter_raw():
                if buffering:
                    if len(buffered) + len(chunk) <= self._stream_cache_limit:
                        buffered.extend(chunk)
                    else:
                        # 超过缓存上限:放弃缓存,丢弃已缓冲的部分省内存。
                        buffering = False
                        buffered = bytearray()
                if not client_disconnected:
                    try:
                        if use_chunked:
                            client_writer.write(f"{len(chunk):X}\r\n".encode('latin-1'))
                            client_writer.write(chunk)
                            client_writer.write(b"\r\n")
                        else:
                            client_writer.write(chunk)
                        await client_writer.drain()
                    except (BrokenPipeError, ConnectionError, OSError):
                        client_disconnected = True
            if use_chunked and not client_disconnected:
                try:
                    client_writer.write(b"0\r\n\r\n")
                    await client_writer.drain()
                except (BrokenPipeError, ConnectionError, OSError):
                    client_disconnected = True
        except Exception:
            # 上游读取异常:尽量关闭,客户端会得到截断响应。
            client_disconnected = True
        # 返回缓冲:仅当未超上限且仍在 buffering 状态。
        if buffering:
            return bytes(buffered)
        return None

    # ── CONNECT 处理 ──────────────────────────────────────────

    def _build_racing_tasks_connect(self, proxies: List[str], target: str) -> set:
        """为 CONNECT 竞速产出候选占位集合(前 max_retries 个上游 + 本机 local)。

        占位为 (pid, target) 元组,交由 _race(真 task)/ _race_staggered(惰性占位,
        补发时才创建)执行;本机竞速时追加 (local, target) 直连占位。target 给
        策略路由:proxies 已由调用方收窄,local 仅当策略放行时参与。
        """
        places = set()
        for pid in proxies[:self.max_retries]:
            if self.proxy_store.get(pid):
                places.add((pid, target))
        if self.enable_local_racing and self._policy_allows_sticky(target, 'local'):
            places.add(('local', target))
        return places

    def _prep_connect(self, proxies: List[str], target: str) -> tuple:
        """CONNECT 竞速的启动参数:首批/补发按 stagger 配置取占位,返回 (initial_places, remaining)。

        与 _prep_http 同构:首批取前 stagger_initial 个最优代理,本机竞速时 local
        优先(直连,常最快)。占位为 (pid, target) 元组,_make_race_task 据此建
        _try_tunnel task。返回的两个列表均保持 proxies 的 EWMA 排序(最优在前)。
        target 给策略路由:local 仅当策略放行时参与。
        """
        n_initial = self._stagger_initial()
        initial_pids = proxies[:n_initial]
        if self.enable_local_racing and 'local' not in initial_pids \
                and self._policy_allows_sticky(target, 'local'):
            initial_pids = ['local'] + initial_pids
        initial_places = [(pid, target) for pid in initial_pids
                          if pid == 'local' or self.proxy_store.get(pid)]
        remaining = [(pid, target) for pid in proxies
                     if (pid, target) not in initial_places and (pid == 'local' or self.proxy_store.get(pid))]
        return initial_places, remaining

    async def _connect_established(self, client_writer, up_writer):
        """回写 CONNECT 200 并对客户端与上游连接设 TCP_NODELAY。"""
        self._set_nodelay(client_writer)
        self._set_nodelay(up_writer)
        client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await client_writer.drain()

    async def _relay_tunnel(self, client_reader, up_writer, up_reader, client_writer,
                            proxy_host=None, proxy_port=None, target=None):
        """双向透传一个已建立的隧道。

        任一方向结束即结束透传。若启用了已握手隧道复用(conn_pool_established_reuse
        且经上游代理)、且上游连接健康(未关闭、无残留缓冲),则归还 _established_pool
        供下一条同 (proxy, target) 请求复用;否则关闭上游连接。客户端侧连接始终关闭。
        """
        # 客户端→上游 方向的 _pipe 结束时**不关上游**(close_writer=False),让
        # _relay_tunnel 在结束时统一判断归还/关闭;上游→客户端 方向照常关客户端。
        # wait(FIRST_COMPLETED):任一端结束(客户端断开 EOF / 上游结束 / 超时)
        # 即返回并取消另一端——客户端断开后不等待上游挂起(上游可能长连不关),
        # 立即进入 finally 决定归还/关闭。原 gather 会等两个 pipe 都结束,
        # 若上游长连不关则归还延迟到其 idle 超时。
        try:
            done, pending = await asyncio.wait(
                {asyncio.create_task(Router._pipe(client_reader, up_writer, close_writer=False)),
                 asyncio.create_task(Router._pipe(up_reader, client_writer))},
                return_when=asyncio.FIRST_COMPLETED)
            for t in pending:
                t.cancel()
            for t in done:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
        finally:
            # 归还条件:启用复用 + 经上游代理(非本机直连) + 上游连接未关闭。
            can_reuse = (self.conn_pool_enabled and self.conn_pool_established_reuse
                         and proxy_host is not None and target is not None
                         and not up_writer.is_closing())
            # 严格验证:上游残留缓冲 → 连接已脏,不归还(宁可不复用也不污染)。
            if can_reuse and up_reader._buffer and len(up_reader._buffer) > 0:
                logger.info("established pool NOT-RETURN %s via %s:%s (dirty buffer)",
                            target, proxy_host, proxy_port)
                can_reuse = False
            if can_reuse:
                up_writer._conn_pool_created = time.monotonic()
                self._established_pool.setdefault(
                    f"{proxy_host}:{proxy_port}|{target}", []).append((up_reader, up_writer))
                self.established_pool_returned += 1
                logger.info("established pool RETURN %s via %s:%s (returned=%d)",
                            target, proxy_host, proxy_port, self.established_pool_returned)
            else:
                try:
                    up_writer.close()
                    await asyncio.wait_for(up_writer.wait_closed(), timeout=0.5)
                except Exception:
                    pass

    async def _handle_connect(self, target: str, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter, client_ip: str = ""):
        """处理 CONNECT 请求:建立到 target 的隧道并双向透传数据。

        决策顺序与 HTTP 类似:会话粘性命中 → 单发隧道;域名缓存命中 → 单发
        隧道;否则竞速(首批 max_retries,失败对剩余兜底)。胜出后回 200,用
        两个反向 _pipe 双向透传,任一方向结束即关闭。全失败回写 502。认证
        已在 handle_client 完成,此处不再校验。
        """
        # 1) 会话粘性:同一客户端+target 复用上次胜出的代理单发隧道,失败则
        #    驱逐该条目并回落到域名缓存/竞速(redispatch)。本机胜者('local')
        #    走直连(None 代理),无需 proxy_store 校验(A1)。策略路由(P1):
        #    粘性取用也须满足策略——命中策略但 pid 不在子集内 → 视为 miss
        #    驱逐并回落(防旧粘性绕过新策略)。
        skip_domain_cache = False
        if self.stickiness_enabled:
            sticky_pid = self._get_sticky_proxy(client_ip, target)
            if sticky_pid:
                proxy = None if sticky_pid == 'local' else self.proxy_store.get(sticky_pid)
                try:
                    if proxy is None:
                        pid, up_reader, up_writer = await self._try_tunnel(sticky_pid, target, None, None, None)
                    else:
                        pid, up_reader, up_writer = await self._try_tunnel(sticky_pid, target, proxy.host, proxy.port, proxy.auth)
                        # CONNECT 目标半预连接(P2):粘性命中说明该 target 高频,
                        # 后台预热下一条到上游代理的 TCP(不阻塞本请求)。
                        self._spawn_target_prewarm(proxy.host, proxy.port, target)
                    logger.debug("proxy %s sticky hit CONNECT %s", pid, target)
                    self.sticky_cache_hits += 1
                    self._bump_sticky(client_ip, target, sticky_pid)
                    await self._connect_established(client_writer, up_writer)
                    await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                             proxy.host if proxy else None,
                                             proxy.port if proxy else None,
                                             target)
                    return
                except Exception:
                    logger.debug("sticky proxy %s failed CONNECT %s", sticky_pid, target)
                    self._evict_sticky(client_ip, target)
            elif self._sticky_recheck_due(client_ip, target):
                # B2:探路重评估到期——驱逐并跳过域名缓存,直接竞速换新赢家。
                self._evict_sticky(client_ip, target)
                skip_domain_cache = True

        # 2) 域名缓存命中:用上次胜出的代理单发隧道(只记尝试统计),失败回退竞速。
        #    成功时也回填粘性表(见 _forward_upstream 同名说明)。
        cached_pid = None if skip_domain_cache else self._get_fresh_proxy(target)
        if cached_pid:
            proxy = None if cached_pid == 'local' else self.proxy_store.get(cached_pid)
            try:
                if proxy is None:
                    pid, up_reader, up_writer = await self._try_tunnel(cached_pid, target, None, None, None)
                else:
                    pid, up_reader, up_writer = await self._try_tunnel(cached_pid, target, proxy.host, proxy.port, proxy.auth)
                    # CONNECT 目标半预连接(P2):域名缓存命中说明该 target 高频,
                    # 后台预热下一条到上游代理的 TCP(不阻塞本请求)。
                    self._spawn_target_prewarm(proxy.host, proxy.port, target)
                logger.debug("proxy %s cache hit CONNECT %s", pid, target)
                self._record_sticky(client_ip, target, cached_pid)
                await self._connect_established(client_writer, up_writer)
                await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                         proxy.host if proxy else None,
                                         proxy.port if proxy else None,
                                         target)
                return
            except Exception:
                logger.debug("cached proxy %s failed CONNECT %s", cached_pid, target)

        # 3) 竞速:首批并行 max_retries 个,全失败且还有剩余则对剩余再竞速。
        #    策略路由(P1):按 target 收窄候选集,命中策略的 target 只在该子集内竞速。
        proxies = self.selector.ordered_proxies()
        if self._policies:
            proxies = self._policy_candidate_pids(target, proxies)
        if not proxies and not self.enable_local_racing:
            try:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await client_writer.drain()
            except Exception:
                pass
            return

        # 错峰启动(stagger_start)时首批只发 stagger_initial 个,补发占位交
        # _race_staggered 按 interval 定时补发;否则同时全发(全失败再兜底批)。
        if self.stagger_start:
            initial_places, remaining = self._prep_connect(proxies, target)
            winner = await self._race_staggered(
                initial_places + remaining, cleanup=self._cleanup_tunnel_result,
                initial=len(initial_places), interval=self.stagger_interval)
        else:
            # 非错峰(_race):需真 task,占位经 _make_race_task 急切创建。
            places = self._build_racing_tasks_connect(proxies, target)
            tasks = {self._make_race_task(p, '', '', None, None) for p in places}
            winner = await self._race(tasks, cleanup=self._cleanup_tunnel_result)

            # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
            if not winner and len(proxies) > self.max_retries:
                remaining = proxies[self.max_retries:]
                places = self._build_racing_tasks_connect(remaining, target)
                tasks = {self._make_race_task(p, '', '', None, None) for p in places}
                winner = await self._race(tasks, cleanup=self._cleanup_tunnel_result)

        if winner:
            pid, up_reader, up_writer = winner
            client_peer = client_writer.get_extra_info('peername')
            logger.debug("proxy %s racing CONNECT to %s for client %s", pid, target, client_peer)
            # 仅赢家更新域名缓存 meta 与会话粘性表(用 target 作 domain key);
            # 败者只记了尝试统计。
            self._record_win_meta(target, pid)
            self._record_sticky(client_ip, target, pid)
            # CONNECT 目标半预连接(P2):竞速胜出说明该 target 高频且最优代理已
            # 确定,后台为 (proxy, target) 预热下一条到上游代理的 TCP(不阻塞本
            # 请求)。注意 pid 可能是 'local'(enable_local_racing 直连),此时无
            # 上游代理可预热,交由 _spawn_target_prewarm 内部跳过。
            if pid != 'local':
                win_proxy = self.proxy_store.get(pid)
                if win_proxy is not None:
                    self._spawn_target_prewarm(win_proxy.host, win_proxy.port, target)
            await self._connect_established(client_writer, up_writer)
            await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                     win_proxy.host if pid != 'local' and win_proxy is not None else None,
                                     win_proxy.port if pid != 'local' and win_proxy is not None else None,
                                     target)
            return

        # 4) 全失败:回写 502 并关闭客户端连接。
        logger.error("all proxies failed for CONNECT to %s", target)
        try:
            client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await client_writer.drain()
        except Exception:
            pass
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass

    async def start(self):
        """开始监听代理端口,接受客户端连接(非阻塞,返回后服务在后台运行)。

        同时启动后台 flush task(周期把内存统计批量落盘)与探活 task
        (probe_interval_sec>0 时,周期对 enabled 代理做轻量 CONNECT 探活)。
        """
        self._server = await asyncio.start_server(self.handle_client, host=self.listen_host, port=self.listen_port)
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_loop())
        if self.probe_interval_sec > 0 and (self._probe_task is None or self._probe_task.done()):
            self._probe_task = asyncio.create_task(self._probe_loop())
        # CONNECT 预热池(P1):refill_interval>0 时启动后台补充循环。
        if self.conn_pool_enabled and self.conn_pool_refill_interval > 0 \
                and (self._conn_pool_task is None or self._conn_pool_task.done()):
            self._conn_pool_task = asyncio.create_task(self._conn_pool_loop())
        logger.info("Router listening on %s:%s", self.listen_host, self.listen_port)

    async def stop(self):
        """优雅关闭:停止接受新连接 → 最终 flush 落盘 → 关闭连接池 →
        取消并等待在途连接 → 取消 flush task → 关闭 DB。

        先关 _server(不再接受新连接),做一次最终 flush 把残留统计落盘,
        关闭上游连接池;再取消所有正在处理的 handle_client task 并等待其退出
        (此时它们已无法再写库);最后停止 flush task 并 _db.close()。
        """
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        # 最终 flush:把内存里尚未落盘的统计/元数据写库。
        try:
            self._flush_to_db()
        except Exception:
            logger.exception("final flush failed")
        # 关闭上游连接池(归还所有 keep-alive 连接)。
        await self._aclose_all_clients()
        # 关闭 CONNECT 预热池(P1):停补充循环,关闭全部预热连接。
        if self._conn_pool_task and not self._conn_pool_task.done():
            self._conn_pool_task.cancel()
            try:
                await self._conn_pool_task
            except (asyncio.CancelledError, Exception):
                pass
            self._conn_pool_task = None
        await self._conn_pool_close_all()
        # 排空竞速败者的后台清理 task:它们正在 aclose 流式 resp / 关上游裸连接,
        # 必须在 _db.close() 前完成,否则连接泄漏(ResourceWarning)。
        if self._pending_cleanups:
            await asyncio.gather(*self._pending_cleanups, return_exceptions=True)
            self._pending_cleanups.clear()
        # 停止接受新连接后，取消仍在处理的客户端连接 task 并等待它们退出，
        # 避免在 _db.close() 之后还有在途请求尝试写库而报错。
        for t in list(self._running_tasks):
            t.cancel()
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)
            self._running_tasks.clear()
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except (asyncio.CancelledError, Exception):
                pass
            self._flush_task = None
        if self._probe_task and not self._probe_task.done():
            self._probe_task.cancel()
            try:
                await self._probe_task
            except (asyncio.CancelledError, Exception):
                pass
            self._probe_task = None
        self._db.close()
