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
import socket
import sqlite3
import threading
import time
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, List, Any, Dict, Tuple
import httpx

from .proxy_store import ProxyStore
from .auth import check_auth

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


class ProxySelector:
    """从 ProxyStore 产出代理 id 的有序列表,供竞速使用。

    当前策略:取所有 enabled 代理,随机打乱后返回 id 列表。竞速模式下
    排序不影响结果(所有代理都会被尝试),随机化只为均衡负载。
    """

    def __init__(self, proxy_store: ProxyStore):
        self.proxy_store = proxy_store

    def ordered_proxies(self) -> List[str]:
        """返回随机打乱后的已启用代理 id 列表。"""
        proxies = self.proxy_store.list()
        enabled = [p for p in proxies if p.enabled]
        random.shuffle(enabled)
        return [p.id for p in enabled]

    def best_proxy(self) -> Optional[str]:
        """返回打乱后的首个代理 id(无代理时返回 None)。"""
        lst = self.ordered_proxies()
        return lst[0] if lst else None


class Router:
    """代理路由器:监听端口、处理客户端连接、竞速转发、维护统计与缓存。

    生命周期:start() 开始监听 → handle_client 处理每个连接 → stop() 优雅关闭。
    """

    def __init__(self, proxy_store: ProxyStore, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3, db_path: str = "auto_squid.db", cache_ttl: int = 600, enable_local_racing: bool = False, auth_enabled: bool = False, auth_username: str = "", auth_password: str = "", enable_http_cache: bool = True, stickiness_enabled: bool = False, stickiness_ttl: int = 1800, stickiness_recheck_hits: int = 100, stickiness_max_entries: int = 100_000):
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
        """
        self.proxy_store = proxy_store
        self.selector = ProxySelector(proxy_store)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.max_retries = max_retries
        self.enable_local_racing = enable_local_racing
        self.enable_http_cache = enable_http_cache
        self.auth_enabled = auth_enabled
        self.auth_username = auth_username
        self.auth_password = auth_password
        self._server: Optional[asyncio.AbstractServer] = None
        # 跟踪所有正在处理的客户端连接 task，供 stop() 在关闭 DB 前取消并等待。
        self._running_tasks: set[asyncio.Task] = set()
        self.request_counts: dict[str, int] = {}
        self.attempted_counts: dict[str, int] = {}
        self.cache_ttl = cache_ttl
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
                updated_at TEXT NOT NULL
            )
        """)
        self._db.commit()

        # 内存镜像:热路径(每请求查域名缓存)只读这两份内存,不经 DB/锁。
        # _meta_cache: {domain: {'default_proxy': pid, 'updated_at': ts}}
        # _stats_cache: {domain: {pid: wins}}(内存累加,后台 flush 落盘)
        self._meta_cache: dict[str, dict[str, str]] = {}
        self._stats_cache: dict[str, dict[str, int]] = {}
        self._load_caches_from_db()
        # _stats_dirty / _meta_dirty 标记自上次 flush 后是否有变更。
        self._stats_dirty = False
        self._meta_dirty = False
        self._flush_task: Optional[asyncio.Task] = None
        # 竞速中"败者清理"(aclose 流式 resp / 关上游裸连接)被下放到后台 task,
        # 不阻塞赢家首字节(见 _race / _drain_losers)。stop() 收尾时排空,防泄漏。
        self._pending_cleanups: set = set()

        # ── HTTP 响应缓存 ───────────────────────────────────────
        self._http_cache: dict[str, dict] = {}
        self._http_cache_ttl = 60
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
                "SELECT domain, default_proxy, updated_at FROM domain_meta").fetchall()
        self._stats_cache = {}
        for domain, pid, wins in stats_rows:
            self._stats_cache.setdefault(domain, {})[pid] = wins
        self._meta_cache = {
            domain: {"default_proxy": dp, "updated_at": ua}
            for domain, dp, ua in meta_rows
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

    def _record_win_meta(self, domain: str, pid: str):
        """记录某域名确认的"赢家代理",更新 _meta_cache(域名→首选代理)。

        仅在竞速判定赢家(或域名缓存命中复用)后调一次。这样 _meta_cache 反映
        真正被采用的上游,而非竞速中"最后收到响应头的候选"(可能被取消)。
        更新内存镜像并置脏,由后台 _flush_loop 落盘。
        """
        self._meta_cache[domain] = {
            "default_proxy": pid,
            "updated_at": self._now_utc(),
        }
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
                    "INSERT INTO domain_meta (domain, default_proxy, updated_at) VALUES (?, ?, ?)",
                    [(d, m["default_proxy"], m["updated_at"])
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
            "client_pool_size": len(self._client_pool),
            "sticky_cache_size": len(self._sticky_cache),
            "request_counts": dict(self.request_counts),
            "attempted_counts": dict(self.attempted_counts),
        }

    def _get_fresh_proxy(self, domain: str) -> Optional[str]:
        """返回某域名在 cache_ttl 内的缓存代理 id;过期或无记录返回 None。

        用于域名缓存:命中则直接复用该代理,跳过竞速。纯内存读取,无 DB/锁。
        """
        entry = self._meta_cache.get(domain)
        if not entry:
            return None
        pid = entry["default_proxy"]
        updated_at_str = entry["updated_at"]
        try:
            dt = datetime.fromisoformat(updated_at_str)
            if (datetime.now(timezone.utc) - dt).total_seconds() < self.cache_ttl:
                return pid
        except Exception:
            pass
        return None

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
        # B2:命中次数达到阈值 → 触发探路重竞速(不驱逐,由调用方依据
        # _sticky_recheck_due 决定跳过域名缓存直接竞速)。
        if self._sticky_recheck_due(client_ip, domain):
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

    def _record_sticky(self, client_ip: str, domain: str, pid: str):
        """记录客户端+域名的粘性代理(刷新 updated_at,hits 归零)。

        仅由确认的赢家(粘性单发成功 / 竞速赢家 / 域名缓存单发成功)调用,新
        赢家从 0 开始重新计数。未启用时为空操作。写前检查容量上限(B1):超限
        先清过期条目,仍超则驱逐 updated_at 最旧的一条。
        """
        if not self.stickiness_enabled:
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
        """
        if not self.enable_http_cache or method != 'GET':
            return None
        key = self._http_cache_key(method, url)
        entry = self._http_cache.get(key)
        if not entry:
            return None
        if time.time() - entry['cached_at'] > self._http_cache_ttl:
            self._http_cache_del_with_index(key)
            return None
        return entry

    def _http_cache_del_with_index(self, key: str) -> None:
        """从 _http_cache 删除 key,并同步清除 _http_cache_domain_index 中的引用。"""
        entry = self._http_cache.pop(key, None)
        if entry is not None:
            cached_url = key[len('GET:'):] if key.startswith('GET:') else key
            cached_host = urllib.parse.urlparse(cached_url).hostname or cached_url
            idx = self._http_cache_domain_index.get(cached_host)
            if idx:
                idx.discard(key)
                if not idx:
                    del self._http_cache_domain_index[cached_host]

    def _http_cache_set(self, method: str, url: str, status_code, reason_phrase, headers, content) -> None:
        """缓存一个 GET 可缓存响应(状态码、原因、头、body、时间戳)。

        可缓存状态码由调用方按 CACHEABLE_STATUS 判断。无论上游是否给出
        Content-Length,都遵循 Cache-Control 的 no-store/no-cache/private:
        本代理是共享缓存(为多客户端服务),private 明确禁止共享缓存存储,
        no-store/no-cache 同理。原实现仅在缺 Content-Length 时查 Cache-Control,
        扩展到 3xx/404 后必须无条件查,否则会把源站标 private 的 302 也缓存。
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
        key = self._http_cache_key(method, url)
        self._http_cache[key] = {
            'status_code': status_code,
            'reason_phrase': reason_phrase,
            'headers': headers,
            'content': content,
            'cached_at': time.time(),
        }
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
            self._http_cache.pop(key, None)

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

    # ── 通用竞速 / pipe / 响应写入 ──────────────────────────────

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
    async def _pipe(reader, writer):
        """把 reader 的数据单向搬运到 writer,直至 EOF 或超时/异常。

        用于 CONNECT 隧道的双向透传(两个 _pipe 反向组合)。300s 读超时
        防止半开连接永久占用;任何异常都静默关闭 writer。
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
            writer.write(f"HTTP/1.1 {status_code} {reason_phrase}\r\n".encode('latin-1'))
            for k, v in items:
                if k.lower() not in hop_by_hop:
                    writer.write(f"{k}: {v}\r\n".encode('latin-1'))
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
        try:
            self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
            self.upstream_attempts += 1  # 聚合竞速扇出总数(供 /metrics 算放大率)
            resp = await client.send(
                client.build_request(method, url, headers=headers, content=body),
                stream=True)
            self.request_counts[pid] = self.request_counts.get(pid, 0) + 1
            domain = urllib.parse.urlparse(url).hostname or url
            # 仅记尝试统计(竞速扇出);meta 由 _handle_http_request 在确认赢家后
            # 调 _record_win_meta 写一次,避免败者覆写域名缓存。
            self._record_attempt(domain, pid)
            return pid, method, url, resp, client
        except BaseException:
            # 仅在确实取得流式 resp 时才 aclose;client.build_request / client.send
            # 在赋值前抛错时 resp 仍为 None,无条件 aclose 会抛 UnboundLocalError
            # 被吞掉并掩盖根因。client 始终留在连接池,不在此关闭。
            if resp is not None:
                try:
                    await resp.aclose()
                except Exception:
                    pass
            raise

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
        try:
            auth_hdr = ""
            if proxy_auth:
                raw = f"{proxy_auth['username']}:{proxy_auth['password']}"
                encoded = base64.b64encode(raw.encode()).decode()
                auth_hdr = f"Proxy-Authorization: Basic {encoded}\r\n"
            up_writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n{auth_hdr}\r\n".encode('latin-1'))
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
            # 仅记尝试统计;meta 由 _handle_connect 在确认赢家后调 _record_win_meta。
            self._record_attempt(target, pid)
            return pid, up_reader, up_writer
        except BaseException:
            try:
                up_writer.close()
                await up_writer.wait_closed()
            except Exception:
                pass
            raise

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
                                               (reason or 'Authentication required').encode('latin-1'))
                    return
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

    async def _build_racing_tasks_http(self, proxies: List[str], method: str, url: str, headers: dict, body: Optional[bytes]) -> set:
        """为一组代理创建竞速 task 集合:前 N 个上游各一个 _try_http task。

        N 由 max_retries 限制(本批只竞速前 N 个)。开启本机竞速时追加一个
        不走上游的 local task。返回的 set 交给 _race 执行。
        """
        tasks = set()
        for pid in proxies[:self.max_retries]:
            proxy = self.proxy_store.get(pid)
            if not proxy:
                continue
            tasks.add(asyncio.create_task(self._try_http(pid, self._build_proxy_url(proxy), method, url, headers, body)))
        if self.enable_local_racing:
            tasks.add(asyncio.create_task(self._try_http('local', None, method, url, headers, body)))
        return tasks

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
        proxies = self.selector.ordered_proxies()
        if not proxies and not self.enable_local_racing:
            await self._write_cached_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')
            return

        # 计数:进入竞速(首批)。兜底批单独再 +1,故 invocations 可能 > 请求数。
        self.racing_invocations += 1
        tasks = await self._build_racing_tasks_http(proxies, method, url, hdrs, body)
        winner_resp = await self._race(tasks, cleanup=self._cleanup_http_result)

        # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
        if not winner_resp and len(proxies) > self.max_retries:
            self.racing_invocations += 1
            remaining = proxies[self.max_retries:]
            tasks = await self._build_racing_tasks_http(remaining, method, url, hdrs, body)
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
        内存(上限 STREAM_CACHE_LIMIT),收齐且为 2xx 时写入响应缓存——这样
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
            client_writer.write(f"HTTP/1.1 {resp.status_code} {resp.reason_phrase}\r\n".encode('latin-1'))
            for k, v in resp.headers.multi_items():
                if k.lower() in _HOP_BY_HOP_RESPONSE_HEADERS:
                    continue
                client_writer.write(f"{k}: {v}\r\n".encode('latin-1'))
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
                    if len(buffered) + len(chunk) <= STREAM_CACHE_LIMIT:
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

    async def _build_racing_tasks_connect(self, proxies: List[str], target: str) -> set:
        """为一组代理创建 CONNECT 竞速 task 集合(前 N 个上游各一个 _try_tunnel)。

        本机竞速时追加一个直连 target 的 local task(proxy_host=None)。
        """
        tasks = set()
        for pid in proxies[:self.max_retries]:
            proxy = self.proxy_store.get(pid)
            if not proxy:
                continue
            tasks.add(asyncio.create_task(self._try_tunnel(pid, target, proxy.host, proxy.port, proxy.auth)))
        if self.enable_local_racing:
            tasks.add(asyncio.create_task(self._try_tunnel('local', target, None, None, None)))
        return tasks

    async def _connect_established(self, client_writer, up_writer):
        """回写 CONNECT 200 并对客户端与上游连接设 TCP_NODELAY。"""
        self._set_nodelay(client_writer)
        self._set_nodelay(up_writer)
        client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
        await client_writer.drain()

    @staticmethod
    async def _relay_tunnel(client_reader, up_writer, up_reader, client_writer):
        """双向透传一个已建立的隧道,任一方向结束即关闭上游连接。"""
        try:
            await asyncio.gather(
                Router._pipe(client_reader, up_writer),
                Router._pipe(up_reader, client_writer))
        finally:
            try:
                up_writer.close()
                await up_writer.wait_closed()
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
        #    走直连(None 代理),无需 proxy_store 校验(A1)。
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
                    logger.debug("proxy %s sticky hit CONNECT %s", pid, target)
                    self.sticky_cache_hits += 1
                    self._bump_sticky(client_ip, target, sticky_pid)
                    await self._connect_established(client_writer, up_writer)
                    await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer)
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
                logger.debug("proxy %s cache hit CONNECT %s", pid, target)
                self._record_sticky(client_ip, target, cached_pid)
                await self._connect_established(client_writer, up_writer)
                await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer)
                return
            except Exception:
                logger.debug("cached proxy %s failed CONNECT %s", cached_pid, target)

        # 3) 竞速:首批并行 max_retries 个,全失败且还有剩余则对剩余再竞速。
        proxies = self.selector.ordered_proxies()
        if not proxies and not self.enable_local_racing:
            try:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await client_writer.drain()
            except Exception:
                pass
            return

        tasks = await self._build_racing_tasks_connect(proxies, target)
        winner = await self._race(tasks, cleanup=self._cleanup_tunnel_result)

        # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
        if not winner and len(proxies) > self.max_retries:
            remaining = proxies[self.max_retries:]
            tasks = await self._build_racing_tasks_connect(remaining, target)
            winner = await self._race(tasks, cleanup=self._cleanup_tunnel_result)

        if winner:
            pid, up_reader, up_writer = winner
            client_peer = client_writer.get_extra_info('peername')
            logger.debug("proxy %s racing CONNECT to %s for client %s", pid, target, client_peer)
            # 仅赢家更新域名缓存 meta 与会话粘性表(用 target 作 domain key);
            # 败者只记了尝试统计。
            self._record_win_meta(target, pid)
            self._record_sticky(client_ip, target, pid)
            await self._connect_established(client_writer, up_writer)
            await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer)
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

        同时启动后台 flush task,周期把内存统计批量落盘。
        """
        self._server = await asyncio.start_server(self.handle_client, host=self.listen_host, port=self.listen_port)
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._flush_loop())
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
        self._db.close()
