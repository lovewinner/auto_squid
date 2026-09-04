"""会话粘性子系统(从 Router 拆出,见 #14 sticky.py)。

`StickyCache` 持有"客户端 IP + 域名 → 上次胜出代理"的纯内存粘性表:

- 同一客户端+域名复用上次胜出的代理单发,保持 egress IP 稳定;粘性代理失败
  /5xx 则驱逐并回落竞速(redispatch),赢家回填粘性表。优先级高于域名缓存。
- TTL 为滑动制:粘性命中成功刷新 updated_at(滑动 TTL)并累加 hits;hits 达
  recheck_hits 后触发探路重竞速(不驱逐,由调用方跳过域名缓存直接竞速)。
- 质量感知降级(Goal #6):被钉住代理在"单发选择"时经 Router._single_send_degraded
  (连续失败 / EWMA 相对钉住基线恶化)判定不稳定 → 驱逐回竞速;降级计数写入
  Router 共享的 `_degraded_single_send`(与 Router 侧 remove/clear 共享同一 set)。
- 容量硬上限(max_entries):超限先清过期条目,仍超则驱逐 updated_at 最旧一条。

依赖 Router 的决策链成员(selector/策略/单发降级/质量判优)通过 `self.router`
背引用(runtime 对象引用,非 import,避免 import 环)。本类不落盘
(粘性是瞬态,重启即清)。
"""

import time
from datetime import datetime, timezone
from typing import Optional


class StickyCache:
    """会话粘性表(StickyCache 协作类,Router 持有 self.sticky)。

    Router 经类尾 `_STICKY_FORWARD` 白名单 __getattr__/__setattr__ 转发本类
    的配置字段/计数/方法,使热路径(_forward_upstream/_handle_connect/
    _forward_single/_flush_loop)与测试的 `self._sticky_cache` 等引用原样解析。
    决策链成员经 `self.router` 背引用读取(对象引用,非 import)。
    """

    def __init__(self, router, enable_local_racing: bool = False,
                 enabled: bool = False, ttl: int = 1800,
                 recheck_hits: int = 100, max_entries: int = 100_000,
                 probe_interval_sec: float = 0.0, probe_fanout: int = 2):
        # 背引用:Router→StickyCache→Router 的运行时对象引用,不构成 import 环。
        # 依赖的决策链成员(selector/策略/单发降级/质量判优)统一经 router 读取。
        self.router = router
        self.stickiness_enabled = enabled
        self.stickiness_ttl = ttl
        self.stickiness_recheck_hits = recheck_hits
        self.stickiness_max_entries = max_entries
        # 本机竞速开关('local' 直连是否参与粘性判定)。Router 同名单字段同步读本值。
        self.enable_local_racing = enable_local_racing
        # 粘性命中后台探路(杠杆A):命中后 fire-and-forget 对竞争代理做 CONNECT-only
        # 探路,探路显著快于粘性代理则驱逐。probe_interval_sec<=0 关闭(默认)。
        self.stickiness_probe_interval_sec = probe_interval_sec
        self.stickiness_probe_fanout = probe_fanout
        # 节流状态:sticky_key -> monotonic 最后实际探路时刻(供 sticky_probe_due 判定)。
        self._sticky_probe_last: dict[str, float] = {}
        # 键 = "{client_ip}|{domain}",值 = {"proxy_id": pid, "updated_at": ts}。
        # 纯内存、滑动 TTL:同一客户端+域名复用上次胜出的代理,保持 egress IP
        # 稳定;粘性代理失败则驱逐并回落竞速(redispatch)。仿 Router._meta_cache
        # 模式,但不落盘(粘性是瞬态,重启即清)。
        self._sticky_cache: dict[str, dict[str, object]] = {}
        self.sticky_cache_hits = 0
        self.sticky_evictions = 0       # 粘性表驱逐次数(5xx/失败/超容量)
        # 方案C:慢探路驱逐次数。sticky 代理的域名级 EWMA 显著差于同域名最优
        # 可用代理时主动驱逐(见 _sticky_slow_probe_due),与 Goal #6 的
        # _sticky_degrade_due(相对自身钉住基线)互补——后者只在代理"自己变差"
        # 或"失败"时触发,对"钉住时就慢→基线高→永远不超 ratio"的盲区无效。
        self.sticky_slow_probes = 0
        # 杠杆A:粘性后台探路实际发射次数 / 探路驱逐次数(探路发现显著更快代理)。
        self.sticky_probes_fired = 0
        self.sticky_probe_evictions = 0
        # "降级中"代理集合与 Router 共享同一 set 实例(Router._degraded_single_send):
        # 本类在 _sticky_degrade_due 判定命中时 add,Router 侧 remove/clear 同对象
        # 生效(由 _record_win_meta 新赢家接管 / reset_proxy_quality 清除)。
        self._degraded_single_send = router._degraded_single_send

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
            proxy = self.router.proxy_store.get(pid)
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
        if pid != 'local' and self.router.selector.is_circuit_open(pid):
            self._evict_sticky_key(key)
            return None
        # 策略路由(P1):命中策略但粘性代理不在允许子集内 → 视为 miss 驱逐并
        # 回落竞速(防旧粘性绕过新策略;与域名缓存同一套策略校验)。
        if self.router._policies and not self.router._policy_allows_sticky(domain, pid):
            self._evict_sticky_key(key)
            return None
        # B2:命中次数达到阈值 → 触发探路重竞速(不驱逐,由调用方依据
        # _sticky_recheck_due 决定跳过域名缓存直接竞速)。
        if self._sticky_recheck_due(client_ip, domain):
            return None
        # 方案C:慢探路——sticky 代理显著差于该域名最优可用代理 → 驱逐并退回
        # 竞速。与 _sticky_degrade_due 互补:后者比较"相对自身钉住基线恶化",
        # 这里比较"相对同域名其他代理更慢",解决"钉住时就慢→基线高→无法
        # 驱逐"的盲区。local 直连不经 selector,跳过该检查(A1)。
        if pid != 'local' and self._sticky_slow_probe_due(client_ip, domain, pid):
            self._degraded_single_send.add(pid)
            self.sticky_slow_probes += 1
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

    def sticky_probe_due(self, client_ip: str, domain: str) -> bool:
        """该 sticky key 是否到了后台探路时机(杠杆A 节流)。

        仅读判定、不刷新时间戳(时间戳由探路协程 _sticky_probe_race 启动时
        刷新——被"无域名观测/无候选"前置门提前 return 的分支不消耗节流)。
        probe_interval_sec<=0(关闭)恒返回 False;从未探过恒放行(不依赖单调钟
        与 interval 的绝对关系——fresh 环境单调钟从容器启动计,uptime<interval
        时旧实现会把首次探路误判为冷却内);冷却 interval 内返回 False。
        """
        if self.stickiness_probe_interval_sec <= 0:
            return False
        key = self._sticky_key(client_ip, domain)
        last = self._sticky_probe_last.get(key)
        if last is None:
            return True
        return (time.monotonic() - last) >= self.stickiness_probe_interval_sec

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
        if not self.router._single_send_degraded(domain, pid, entry.get("ref_ewma")):
            return False
        self._degraded_single_send.add(pid)
        return True

    def _sticky_slow_probe_due(self, client_ip: str, domain: str, pid: str) -> bool:
        """方案C:粘性代理是否显著慢于该域名最优可用代理。

        与 _sticky_degrade_due(相对自身钉住基线)互补:这里比较的是**同域名下
        其他代理**,解决"钉住时就慢→基线高→永远不超 ratio→无法驱逐"的盲区。
        复用 single_send_degrade_ratio/slack_ms,无需新增配置。
        要求:单发降级已启用(degrade_ratio>0)且该代理与 best_domain 有足够
        差距(差值>slack)。best_domain 无观测/自己是唯一→不驱逐(无更好选择)。
        """
        if self.router.single_send_degrade_ratio <= 0:
            return False
        # 与 best_domain_ewma(exclude=pid)比较:找同域名下**其他**最优代理。
        best_pid, best_ewma = self.router.selector.best_domain_ewma(domain, exclude=pid)
        if best_pid is None:
            return False  # 无更好选择(自己是唯一观测者或全部不可用)
        dq = self.router.selector._domain_quality_for(domain, pid)
        if dq is None:
            return False
        cur = self.router.selector._proxy_quality_ewma(dq)
        if cur is None:
            return False
        slack = self.router.single_send_degrade_slack_ms / 1000.0
        if cur > best_ewma * self.router.single_send_degrade_ratio \
                and (cur - best_ewma) > slack:
            return True
        return False

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
        if self.router._worse_than_best(domain, pid):
            return
        key = self._sticky_key(client_ip, domain)
        if key not in self._sticky_cache and len(self._sticky_cache) >= self.stickiness_max_entries:
            self._prune_sticky()
            if len(self._sticky_cache) >= self.stickiness_max_entries:
                self._evict_oldest_sticky()
        self._sticky_cache[key] = {
            "proxy_id": pid,
            "updated_at": self.router._now_utc(),
            "hits": 0,
            # Goal #6:钉住时刻的 EWMA 基线,供 _sticky_degrade_due 判定"相对钉住
            # 时是否恶化"。粘性命中(_bump_sticky)只滑动 TTL,不刷新基线。
            "ref_ewma": self.router._ref_ewma_for(domain, pid),
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
        entry["updated_at"] = self.router._now_utc()
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

        由后台 Router._flush_loop 周期调用,也由 _record_sticky 在超容量时先调用。
        粘性键集合(客户端 IP)可能远大于域名集合,若放任不管会缓慢累积;
        过期清扫把表规模收敛到"最近 TTL 内活跃的客户端+域名"。
        """
        # 先做探路节流表清扫(审计 P2#3):_sticky_probe_last 只在探路间隔>0 时
        # 写入,键集合理论上随"客户端×域名"无界增长;若粘性表为空就提前返回,
        # 会连这个独立表也一起漏清,故把该清扫放在空表早退**之前**。按
        # "距最后探路超过若干倍探路间隔(至少 TTL)"淘汰,让表规模收敛到活跃键;
        # 探路间隔<=0(特性关闭)时表本就为空,零开销。用"较长空闲即淘汰",
        # 避免与 active session 的探路节流冲突。
        if self.stickiness_probe_interval_sec > 0 and self._sticky_probe_last:
            idle_cutoff = time.monotonic() - max(
                float(self.stickiness_ttl), 8.0 * self.stickiness_probe_interval_sec)
            for key in [k for k, last in self._sticky_probe_last.items()
                        if last < idle_cutoff]:
                self._sticky_probe_last.pop(key, None)

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
            proxy = self.router.proxy_store.get(pid)
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