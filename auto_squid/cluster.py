"""请求簇预测预热子系统(ClusterGraph 协作类)。

根据真实流量的"簇"形态做**传输层预测预热**:一次页面加载会在数秒内对一组
子资源域名并发 CONNECT(js/css/CDN,窗口内计数 5-30,与 pools.py 的
refill_pause 簇度活动判定同源)。本子系统记录这些簇,学习"同簇目标"的全局
共现规律,并在下一次页面加载开口就为即将到来的 co-target **提前预建
"本机→上游代理"的裸 TCP**(不 CONNECT 到源站——不发请求体,不污染源站),省掉
那些目标真实到达时的建连 TTFB。

与 `conn_pool.target_prewarm`(被动预建:target 每次胜出后才补)互补:本特性做
**预测**预建(窗口开口即预同簇其余 target),覆盖"页面首访之后的子资源
突发"——HTML 正在竞速时 js/css 的 TCP 已在路上。

设计要点:
- **学习作用域为全局共现图**(跨所有客户端),不保存 per-client 长期行为:
  `_active_windows` 只存瞬态窗口(`client_ip → 窗口`),窗口关闭即学进全局图并
  丢弃;图按 TTL + LRU 上限修剪。隐私面小,新客户端冷启动直接继承全局知识。
- **键格式**:直接用观察到的完整 target 字符串(CONNECT 的 "host:port";
  HTTP 的裸 hostname)。不做端口剥离——consistency 优先,预测时用记忆串原样预热。
- **窗口分组**:per-client_ip 墙钟窗口(默认 2s)。批量关闭规则——某客户端新
  请求距上一请求 > 窗口宽,则先关闭并学习上一窗口再开新窗口;同一突发内几乎
  同时到达的并发 CONNECT 归属同一窗口。无 per-window 定时器(避免定时器爆炸)。
- **预测触发**:窗口开口首个带赢家(pid 非空)的请求;其 co-target 正是浏览器
  随后要连接的目标,lead time 最大。取 top-K(默认 3),跳过当前窗口已观察到的
  目标,并受同 (src→co) 对节流(默认 30s)约束。预测只通过注入的 prewarm_spawn
  (Router._spawn_target_prewarm)发射——它受 conn_pool 全局 fd 预算 + 空闲暂停
  门天然约束;错预测的代价只是一条 30s 空闲后被淘汰的 TCP。
- 无 import 环:本模块只依赖 stdlib + ProxyStore(类型);Router 侧把绑定方法
  `_spawn_target_prewarm` 作为 `prewarm_spawn` 注入。
"""

import logging
import time
from typing import Callable, Optional

from .proxy_store import ProxyStore

logger = logging.getLogger(__name__)


class _Window:
    """一个客户端的一段簇窗口(瞬态,窗口关闭即学并被丢弃)。

    - `opened_mono`:窗口打开的时刻(time.monotonic)。
    - `predicted`:本窗口是否已做过预测(每窗口至多预测一次)。
    - `observed`:本窗口内观察到的 `(target, pid)` 序列(保序)。pid 为决定代理
      id 或 None(竞速全失败等无赢家情况)。
    """

    __slots__ = ('opened_mono', 'predicted', 'observed')

    def __init__(self, opened_mono: float):
        self.opened_mono: float = opened_mono
        self.predicted: bool = False
        self.observed: list = []


class _CoEntry:
    """共现图的一条边:`src_target → co_target` 的统计。

    边只在(src, co)同窗口**共现**时计数。`last_seen` 为 monotonic 时间
    (`prune` 判 TTL 与 LRU 淘汰);`probe_pid` 为 co 最近一次带赢家出现时胜出的
    代理 id(预测时经 ProxyStore 解析为 host:port)。
    """

    __slots__ = ('hits', 'probe_pid', 'last_seen')

    def __init__(self, pid: Optional[str], now: float):
        self.hits: int = 1
        self.probe_pid: Optional[str] = pid
        self.last_seen: float = now


class ClusterGraph:
    """请求簇共现图 + 窗口分组(ClusterGraph 协作类,Router 持有 self.cluster)。

    全部状态由事件循环单线程读写。Router 经类尾 `_CLUSTER_FORWARD` 白名单
    __getattr__/__setattr__ 把本类的状态/计数器/方法转发到 self.cluster,
    使观察点与快照的 `self.cluster_*` / `self._cluster_*` 引用原样解析。
    """

    def __init__(self, store: ProxyStore, enabled: bool = False, window_sec: float = 2.0,
                 predict_topk: int = 3, min_support: int = 2, ttl_sec: int = 86400,
                 max_entries: int = 100_000, throttle_sec: float = 30.0,
                 prewarm_spawn: Optional[Callable] = None):
        self._store = store
        # 总闸与门:仅当 conn_pool.enabled + target_prewarm 时 Router 才开启并调用;
        # enabled=False 时 observe/prune 全部近乎空操作(状态零分配)。
        self.enabled = enabled
        self.window_sec = max(0.1, float(window_sec))
        self.cluster_predict_topk = max(0, int(predict_topk))
        self.cluster_min_support = max(1, int(min_support))
        self.cluster_graph_ttl_sec = max(1, int(ttl_sec))
        self.cluster_graph_max_entries = max(1, int(max_entries))
        self.cluster_predict_throttle_sec = max(0.1, float(throttle_sec))
        # 发射通道:Router 绑定方法 _spawn_target_prewarm(proxy_host, proxy_port,
        # target)。本模块不 import Router(避免环);spawn 内部已含 conn_pool 门、
        # 本机直连跳过、task 注册与 fd 预算——预测只是"多一个 caller"。
        # 注意:同一方法同时是"被动预建"的入口(域缓存/粘性/竞速胜出后直接调用)。
        # ClusterGraph 经 _fire 时时携带 source='cluster' 调用,以便 pools 侧把
        # 预测预建的连接打上 cluster 专属标签(_target_pool_refill 的 source 参数)。
        self._prewarm_spawn = prewarm_spawn
        # 瞬态窗口:client_ip → _Window。事件后即弃,stop() 清空;仅事件循环线程读写。
        self._active_windows: dict[str, _Window] = {}
        # 全局共现图:src_target → {co_target: _CoEntry}。
        self._cooccur: dict[str, dict[str, _CoEntry]] = {}
        # 预测节流:(src, co) → 上次发射的 monotonic 时间(防 reload 反复预建)。
        self._last_predict: dict[tuple, float] = {}
        # 计数器 / 快照。
        self.cluster_windows_learned = 0
        self.cluster_predictions = 0
        self.cluster_prewarm_spawned = 0

    # ── 观察 / 学习 ─────────────────────────────────────────

    def observe(self, client_ip: str, target: str, pid: Optional[str], now: Optional[float] = None):
        """把一个已解析(target, pid)的目标记入客户端的簇窗口。

        批量关闭规则:该客户端已有窗口且 `now - opened > window_sec` → 先关闭并
        学习旧窗口,再开新窗口;否则(窗口不存在,或仍在窗口宽内——同一次页面
        加载的并发 CONNECT)追加进当前窗口。窗口开口的**首个带赢家**(pid 非空)
        请求触发 `maybe_predict`(顶部只在 Router 开启本特性时被调用)。

        pid 为 None(该目标未赢得代理,竞速全失败):仍记录进窗口(浏览器可能再次
        连接),但它的共现边 `probe_pid` 为 None —— 预测时被 top-K/支持度自然跳过。
        """
        if not self.enabled:
            return
        now = now if now is not None else time.monotonic()
        win = self._active_windows.get(client_ip)
        if win is not None and now - win.opened_mono > self.window_sec:
            self._close_and_learn(client_ip, win, now)
            win = None
        if win is None:
            win = _Window(now)
            self._active_windows[client_ip] = win
        win.observed.append((target, pid))
        # 预测只在"本窗口还没预测过、且首个 pid 已确定"时尝试:窗口前后缀内
        # 任一请求都可能成为第一个带赢家的目标,这里统一在首 pid 出现时触发。
        if pid is not None and not win.predicted:
            self.maybe_predict(client_ip, target, now)

    def maybe_predict(self, client_ip: str, target: str, now: Optional[float] = None):
        """为某客户端的当前窗口(尚未预测过)发起预测:src=target 的 top co-target
        逐个经 `_prewarm_spawn` 发射预建。节流/skip 当前窗口内已观察的目标。
        """
        if not self.enabled:
            return
        now = now if now is not None else time.monotonic()
        win = self._active_windows.get(client_ip)
        if win is None or win.predicted:
            return
        if target not in self._cooccur:
            return
        win.predicted = True
        out = self._cooccur[target]
        top = sorted(out.items(),
                     key=lambda kv: (kv[1].hits, kv[1].last_seen),
                     reverse=True)[:self.cluster_predict_topk]
        observed = {t for t, _p in win.observed}
        fired = False
        for co, entry in top:
            if co in observed:
                continue  # 已在当前窗口出现,无需预建(browser 正要连)
            if entry.hits < self.cluster_min_support:
                continue  # 偶然共现,支持度不够,不预测
            pair = (target, co)
            if now - self._last_predict.get(pair, 0.0) < self.cluster_predict_throttle_sec:
                continue  # 同对节流:防 reload 反复预建
            proxy = self._resolve(entry.probe_pid)
            if proxy is None:
                continue  # co 无可用 record 的代理(被删/停用)——宁可跳过不预建
            self._last_predict[pair] = now
            self._fire(co, proxy)
            fired = True
        if fired:
            self.cluster_predictions += 1

    # ── 修剪 / 重置 ─────────────────────────────────────────

    def prune(self, now: Optional[float] = None):
        """周期修剪:关闭并学习刚落定的窗口;TTL + LRU 上限约束图的体积。

        Router._flush_loop 周期调用(每 FLUSH_INTERVAL 秒)。空转期开销可忽略。
        `now` 仅供测试注入合成时钟;生产经 flush 调用时取 time.monotonic()。
        """
        if not self.enabled:
            return
        now = now if now is not None else time.monotonic()
        # 关闭并学习停滞的窗口:窗口安静超过 window_sec 的客户端,若以后还有请求
        # 会由 observe 的批量关闭接手;这里兜底把已尘埃落定的窗口尽早学掉,防
        # 低流量客户端窗口悬挂过久把学习推迟到下一次请求。
        for client_ip, win in list(self._active_windows.items()):
            if now - win.opened_mono > self.window_sec and win.observed:
                self._close_and_learn(client_ip, win, now)
        # 图:TTL 边淘汰(超过 ttl 未再共现的边)。
        for src, out in list(self._cooccur.items()):
            stale = [co for co, e in out.items() if now - e.last_seen > self.cluster_graph_ttl_sec]
            for co in stale:
                del out[co]
            if not out:
                del self._cooccur[src]
        # 图:LRU 上限(边数超 max_entries → 淘汰 last_seen 最旧的一条)。
        self._enforce_cap()

    def reset(self):
        """清空全部状态(stop() 与强制重启学习时调用)。"""
        self._active_windows.clear()
        self._cooccur.clear()
        self._last_predict.clear()

    def graph_size(self) -> int:
        """共现图总边数(src→co 对)。"""
        return self._edge_count()

    def get_cluster_cache(self) -> dict:
        """只读快照:src → {co: (hits, probe_pid)}。供测试/管理面板诊断。"""
        return {src: {co: (e.hits, e.probe_pid) for co, e in out.items()}
                for src, out in self._cooccur.items()}

    # ── 内部 ───────────────────────────────────────────────

    def _close_and_learn(self, client_ip: str, win: _Window, now: float):
        """关闭一个窗口并学习其簇到全局图(同时从 _active_windows 移除)。"""
        self._active_windows.pop(client_ip, None)
        self._learn_window(win, now)
        self._enforce_cap()

    def _learn_window(self, win: _Window, now: float):
        """从窗口 observed 序列学习无向共现:(a,b) 同窗则双侧计数。

        每条边存"co 最近一次带赢家时的胜出 pid"(b 的 probe_pid 取本窗口中
        b 最后一次出现时的 pid;窗口内同目标可能换代理,取最近一次)。
        """
        seen: list[str] = []
        pid_of: dict[str, Optional[str]] = {}
        for target, pid in win.observed:
            if target not in seen:
                seen.append(target)
            if pid is not None:
                pid_of[target] = pid
        if len(seen) < 2:
            return  # 单目标窗口无可学共现
        for i, a in enumerate(seen):
            for b in seen[i + 1:]:
                self._bump_edge(a, b, pid_of.get(b), now)
                self._bump_edge(b, a, pid_of.get(a), now)
        self.cluster_windows_learned += 1

    def _bump_edge(self, src: str, co: str, pid: Optional[str], now: float):
        """记一条 (src→co) 共现边:命中 +1、刷新 last_seen、更新 probe_pid。"""
        out = self._cooccur.setdefault(src, {})
        entry = out.get(co)
        if entry is None:
            out[co] = _CoEntry(pid, now)
        else:
            entry.hits += 1
            entry.last_seen = now
            if pid is not None:
                entry.probe_pid = pid

    def _resolve(self, pid: Optional[str]) -> Optional[tuple]:
        """把代理 id 解析为 (host, port) 供预建;'local'/被删/停用 返回 None。"""
        if pid is None or pid == 'local':
            return None
        proxy = self._store.get(pid)
        if proxy is None:
            return None
        return (proxy.host, proxy.port)

    def _fire(self, co_target: str, proxy: tuple):
        """经注入的 prewarm_spawn 发射一条预测预建(host, port, target)。"""
        if self._prewarm_spawn is None:
            return
        host, port = proxy
        self.cluster_prewarm_spawned += 1
        try:
            self._prewarm_spawn(host, port, co_target)
        except Exception:
            logger.info("cluster prewarm spawn failed for %s", co_target, exc_info=True)

    def _edge_count(self) -> int:
        return sum(len(out) for out in self._cooccur.values())

    def _enforce_cap(self):
        """边数超 max_entries → LRU 淘汰 last_seen 最旧的边(仿 sticky _evict_oldest)。"""
        while self._edge_count() > self.cluster_graph_max_entries:
            self._evict_oldest_edge()

    def _evict_oldest_edge(self):
        """LRU 淘汰:找全图 last_seen 最旧的一条边并删除。"""
        target = None
        best = None
        for src, out in self._cooccur.items():
            for co, e in out.items():
                if best is None or e.last_seen < best:
                    best, target = e.last_seen, (src, co)
        if target is None:
            return
        src, co = target
        out = self._cooccur[src]
        del out[co]
        if not out:
            del self._cooccur[src]