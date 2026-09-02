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
import functools
import logging
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
from .config_schema import PolicyConfig, RouterConfig
from .pools import ConnectionPools, _discard_conn, _ESTABLISHED_KEY_CAP, _ESTABLISHED_PROBE_TIMEOUT
from .selector import (ProxySelector, _CIRCUIT_THRESHOLD, _CIRCUIT_MAX_BACKOFF,
                       _SLOW_START_WINDOW, _SLOW_START_SUCCESS, _LB_BIAS_DEFAULT)
from .http_cache import HttpCache, CACHEABLE_STATUS, _INVALIDATING_METHODS
from .sticky import StickyCache
from .cluster import ClusterGraph

logger = logging.getLogger(__name__)

# 单个 HTTP 请求体的最大字节数。超过则返回 413，避免无 Content-Length 的
# 请求靠 read(-1) 读到 EOF 才返回（会破坏 keep-alive）及无界内存占用。
MAX_BODY = 10 * 1024 * 1024

# 流式转发时为响应缓存缓冲的 body 上限。超过此大小不再缓冲(放弃缓存该响应),
# 避免大响应把内存撑爆。缓存的目的是命中幂等小响应,大文件本就不该缓存。
STREAM_CACHE_LIMIT = 1 * 1024 * 1024  # 1 MiB

# 后台 flush 周期(秒):把内存里累积的胜出统计/元数据批量落盘。
FLUSH_INTERVAL = 5.0

# 可缓存状态码(CACHEABLE_STATUS)与写方法(_INVALIDATING_METHODS)随 http_cache
# 拆分搬入 http_cache.py,顶部 re-import(http_cache 方法与 _handle_http_request/
# _forward_single 两端共用)。

# 在途 GET 去重聚合的等待超时(秒)。waiter 发现同 URL 有在途请求时 await 其
# Future;超过该阈值仍未完成(首个请求上游慢)则放弃聚合、自行竞速,避免在
# 慢上游下 waiter 长时间挂住连接导致 fd 堆积(压测曾观测 fd_peak 冲到 300+)。
# 2026-08-26(#8):0.1s → 3.0s。0.1s 比典型上游 TTFB 还短,waiter 几乎总是
# 超时回退竞速,并发同 URL 时对上游形成 stampede(且 _record_attempt 还会扇出
# 误触熔断),聚合等于死代码——提升到秒级让聚合在真实 TTFB 窗口内真正生效,
# 同时保留有界等待(3s 后仍放弃,连接不长期挂死)。
_AGG_WAIT_TIMEOUT = 3.0

# 客户端单请求头数量与总字节上限(#6)。readline 每行 64KB,行数无上限——
# 慢速 loris 式攻击发大量小 header 行会让 bytearray 无界增长。超限拒绝并关闭
# 连接(log warning)。100 行/64KB 对正常客户端(含各浏览器/工具)远宽裕。
_MAX_REQUEST_HEADER_LINES = 100
_MAX_REQUEST_HEADER_BYTES = 64 * 1024

# 败者清理后台 task 的软上限。超过则在 _race 里就地排空一次,防止持续高吞吐
# 下 _pending_cleanups 无界堆积(soak 模式曾观测 fd_peak 冲到 569)。
_MAX_PENDING_CLEANUPS = 64

# 已建握手隧道池(_established_pool)的 per-key 驻池上限 / 复用前活性探测超时,
# #14 拆 pools.py 后统一定义在 pools.py(ConnectionPools 使用)。
_ESTABLISHED_KEY_CAP, _ESTABLISHED_PROBE_TIMEOUT = (
    _ESTABLISHED_KEY_CAP, _ESTABLISHED_PROBE_TIMEOUT)

# 错峰启动(staggered start,RFC 8305 §5)的配置下限。
# 默认间隔 250ms,下限 100ms(绝对值下限 10ms,防止丢包率高时拥塞崩溃),上限 2s。
# stagger_interval 由 __init__ 钳制到此区间,配置传 0/负值时落到默认。
_STAGGER_DEFAULT_MS = 250
_STAGGER_MIN_MS = 100
_STAGGER_ABS_MIN_MS = 10
_STAGGER_MAX_MS = 2000

# 后台探活周期(秒)与 canary 目标。0=关闭主动探活(仅真实请求驱动熔断)。
_PROBE_INTERVAL_DEFAULT = 30.0
_PROBE_CANARY_DEFAULT = "1.1.1.1:443"
_PROBE_TIMEOUT = 4.0

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


# ProxySelector 已随 #14 拆分搬入 auto_squid/selector.py(顶部 re-import);
# Router 构造在此经 self.selector = ProxySelector(...) 持有其协作对象。
class Router:
    """代理路由器:监听端口、处理客户端连接、竞速转发、维护统计与缓存。

    生命周期:start() 开始监听 → handle_client 处理每个连接 → stop() 优雅关闭。
    """

    def __init__(self, proxy_store: ProxyStore, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3, db_path: str = "auto_squid.db", cache_ttl: int = 600, enable_local_racing: bool = False, auth_enabled: bool = False, auth_username: str = "", auth_password: str = "", enable_http_cache: bool = True, http_cache_ttl: int = 60, http_cache_max_entries: int = 10_000, http_cache_max_bytes: int = 256 * 1024 * 1024, http_cache_stream_limit: int = 1 * 1024 * 1024, stickiness_enabled: bool = False, stickiness_ttl: int = 1800, stickiness_recheck_hits: int = 100, stickiness_max_entries: int = 100_000, sticky_probe_interval_sec: float = 0.0, sticky_probe_fanout: int = 2, stagger_start: bool = True, stagger_initial: int = 1, stagger_interval_ms: int = _STAGGER_DEFAULT_MS, probe_interval_sec: float = _PROBE_INTERVAL_DEFAULT, probe_canary: str = _PROBE_CANARY_DEFAULT, probe_canaries: Optional[List[Dict[str, Any]]] = None, circuit_threshold: int = _CIRCUIT_THRESHOLD, circuit_max_backoff: float = _CIRCUIT_MAX_BACKOFF, slow_start_window: float = _SLOW_START_WINDOW, slow_start_success: int = _SLOW_START_SUCCESS, lb_bias: float = _LB_BIAS_DEFAULT, single_send_degrade_fail: int = 0, single_send_degrade_ratio: float = 0.0, single_send_degrade_slack_ms: float = 0.0, single_send_slow_log_ms: float = 0.0, connect_tunnel_timeout_sec: float = 3.0, http_read_timeout_sec: float = 3.0, local_direct_domains: Optional[List[str]] = None, local_direct_timeout_sec: float = 10.0, policies: Optional[List[PolicyConfig]] = None, adaptive_ttl: bool = False, adaptive_ttl_min: float = 60.0, adaptive_ttl_max: float = 1800.0, switch_damping: bool = False, switch_damping_min_wins: int = 2, switch_damping_ratio: float = 0.8, switch_damping_abs_ms: float = 30.0, concurrency_limit_enabled: bool = False, concurrency_limit_initial: int = 16, concurrency_limit_min: int = 2, concurrency_limit_max: int = 128, concurrency_add_on_success: int = 4, concurrency_mult_on_failure: float = 0.5, concurrency_failure_window: int = 20, conn_pool_enabled: bool = False, conn_pool_per_proxy: int = 4, conn_pool_total: int = 64, conn_pool_idle_timeout: float = 30.0, conn_pool_refill_interval: float = 5.0, conn_pool_refill_target: int = 2, conn_pool_connect_timeout: float = 10.0, conn_pool_target_prewarm: bool = False, conn_pool_refill_pause_minutes: float = 60.0, conn_pool_refill_pause_silence_sec: float = 120.0, conn_pool_refill_pause_activity_window: Optional[float] = None, conn_pool_refill_pause_min_requests: int = 3, conn_pool_established_reuse: bool = False, conn_pool_established_idle_timeout: Optional[float] = None, conn_pool_prehandshake: bool = False, conn_pool_prehandshake_throttle_window_sec: float = 0.0, conn_pool_prehandshake_throttle_max_per_window: int = 0, cluster_predict: bool = False, cluster_window_sec: float = 2.0, cluster_predict_topk: int = 3, cluster_min_support: int = 2, cluster_graph_ttl_sec: int = 86400, cluster_graph_max_entries: int = 100_000, cluster_predict_throttle_sec: float = 30.0, cluster_proxy_fanout: int = 2, cluster_probe_decay_sec: float = 3600.0, cluster_pool_idle_timeout: float = 600.0, router_cfg: Optional[RouterConfig] = None):
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
            conn_pool_refill_pause_silence_sec: [已弃用,仅兼容] 旧版"间隔一刀切"
                                活动判定(默认 120),误伤真实孤立请求。已由窗口计数
                                取代,本参数仅对旧配置兼容。
            conn_pool_refill_pause_activity_window: 活动判定窗口(秒,默认 None=
                                旧 silence_sec 换算或 120s)。窗口内请求数 ≥
                                min_requests 才算"密集活动"并刷新时间戳;真实流量
                                是簇(一次页面加载多 hostname 并发,计数高),后台
                                心跳是孤例(窗口内计数低)——据此区分,既不误伤真实
                                孤立请求,又免疫心跳。0=不启用窗口计数(任意请求都刷新)。
            conn_pool_refill_pause_min_requests: 活动判定窗口阈值(默认 3)。
                                窗口内请求数 ≥ 此值才刷新活动时间戳;≤1 时退化为
                                "任意请求都刷新"。
            conn_pool_established_reuse: 已建握手隧道复用(默认关闭)。隧道结束
                                时若连接干净(无残留数据),归还 _established_pool
                                而非关闭;下次同 (proxy, target) 请求直接复用已
                                CONNECT 握手的连接,跳过 CONNECT 发送+200 校验,
                                省掉重建。仅当 conn_pool_enabled 时生效。
            router_cfg:       #15 配置整体入口(RouterConfig)。给出时用它解析出与
                                上述 kwarg 同名的局部变量,后续 __init__ body 原样
                                消费;未给出时局部变量即各 kwarg 默认值(测试/bench
                                的 Router(**kwargs) 构造不受影响)。两者都给时
                                router_cfg 优先。
        """
        # #15:配置整体入口。给出 router_cfg 时覆盖同名 kwarg 局部变量,后续 body
        # (selector/stagger/circuit/http_cache/conn_pool 构造)原样消费,无重排。
        if router_cfg is not None:
            c, cc, auth, stick = router_cfg, router_cfg.circuit, router_cfg.auth, router_cfg.stickiness
            hc, at, sd, cl, pc = (router_cfg.http_cache, router_cfg.adaptive_ttl,
                                  router_cfg.switch_damping, router_cfg.concurrency_limit,
                                  router_cfg.conn_pool)
            max_retries = c.max_retries
            cache_ttl = c.cache_ttl
            enable_local_racing = c.enable_local_racing
            local_direct_domains = list(c.local_direct_domains)
            local_direct_timeout_sec = cc.local_direct_timeout_sec
            stagger_start, stagger_initial, stagger_interval_ms = c.stagger_start, c.stagger_initial, c.stagger_interval_ms
            probe_interval_sec = cc.probe_interval_sec
            probe_canary = cc.probe_canary
            probe_canaries = [x.model_dump() for x in cc.probe_canaries]
            circuit_threshold, circuit_max_backoff = cc.circuit_threshold, cc.circuit_max_backoff
            slow_start_window, slow_start_success = cc.slow_start_window, cc.slow_start_success
            lb_bias = cc.lb_bias
            single_send_degrade_fail, single_send_degrade_ratio, single_send_degrade_slack_ms = (
                cc.single_send_degrade_fail, cc.single_send_degrade_ratio, cc.single_send_degrade_slack_ms)
            single_send_slow_log_ms = cc.single_send_slow_log_ms
            connect_tunnel_timeout_sec, http_read_timeout_sec = cc.connect_tunnel_timeout_sec, cc.http_read_timeout_sec
            auth_enabled, auth_username, auth_password = auth.enabled, auth.username, auth.password
            enable_http_cache, http_cache_ttl = hc.enabled, hc.ttl
            http_cache_max_entries, http_cache_max_bytes, http_cache_stream_limit = (
                hc.max_entries, hc.max_bytes, hc.stream_cache_limit)
            stickiness_enabled, stickiness_ttl = stick.enabled, stick.ttl
            stickiness_recheck_hits, stickiness_max_entries = stick.recheck_hits, stick.max_entries
            sticky_probe_interval_sec, sticky_probe_fanout = stick.probe_interval_sec, stick.probe_fanout
            adaptive_ttl, adaptive_ttl_min, adaptive_ttl_max = at.enabled, at.min_sec, at.max_sec
            switch_damping, switch_damping_min_wins = sd.enabled, sd.min_wins
            switch_damping_ratio, switch_damping_abs_ms = sd.ratio, sd.abs_ms
            concurrency_limit_enabled, concurrency_limit_initial = cl.enabled, cl.initial
            concurrency_limit_min, concurrency_limit_max = cl.min, cl.max
            concurrency_add_on_success, concurrency_mult_on_failure = cl.add_on_success, cl.mult_on_failure
            concurrency_failure_window = cl.failure_window
            conn_pool_enabled, conn_pool_per_proxy = pc.enabled, pc.per_proxy
            conn_pool_total, conn_pool_idle_timeout = pc.total, pc.idle_timeout
            conn_pool_refill_interval, conn_pool_refill_target = pc.refill_interval, pc.refill_target
            conn_pool_connect_timeout, conn_pool_target_prewarm = pc.connect_timeout, pc.target_prewarm
            conn_pool_refill_pause_minutes = pc.refill_pause_minutes
            conn_pool_refill_pause_silence_sec = pc.refill_pause_silence_sec
            conn_pool_refill_pause_activity_window = pc.refill_pause_activity_window
            conn_pool_refill_pause_min_requests = pc.refill_pause_min_requests
            conn_pool_established_reuse = pc.established_reuse
            conn_pool_established_idle_timeout = pc.established_idle_timeout
            conn_pool_prehandshake = pc.prehandshake
            conn_pool_prehandshake_throttle_window_sec = pc.prehandshake_throttle_window_sec
            conn_pool_prehandshake_throttle_max_per_window = pc.prehandshake_throttle_max_per_window
            cluster_predict = pc.cluster_predict
            cluster_window_sec = pc.cluster_window_sec
            cluster_predict_topk = pc.cluster_predict_topk
            cluster_min_support = pc.cluster_min_support
            cluster_graph_ttl_sec = pc.cluster_graph_ttl_sec
            cluster_graph_max_entries = pc.cluster_graph_max_entries
            cluster_predict_throttle_sec = pc.cluster_predict_throttle_sec
            cluster_proxy_fanout = pc.cluster_proxy_fanout
            cluster_probe_decay_sec = pc.cluster_probe_decay_sec
            cluster_pool_idle_timeout = pc.cluster_pool_idle_timeout
            policies = list(c.policies)
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
        # 粘性表(_sticky_cache/配置/方法)已随 #14 拆分搬入 StickyCache(self.sticky),
        # 经类尾 _STICKY_FORWARD 白名单 __getattr__/__setattr__ 转发。构造放在
        # _degraded_single_send 之后(sticky 的 _sticky_degrade_due 与其共享同一 set)。
        # 本行仅保留"单发降级触发次数"计数器(Router 决策链自增,snapshot 直读)。
        self.single_send_degrades = 0
        # ── 单发降级(质量感知的确定性探路,Goal #6)─────────────────
        # 域名缓存/粘性命中单发时,若被钉住代理"最近失败率上升(连续失败)"
        # 或"EWMA 恶化(相对钉住时基线)",主动降级回竞速——把确定性探路从
        # recheck_hits 的纯命中计数升级为 EWMA 感知的"不稳定即重竞速"。
        # 任一阈值为 0(默认)即关闭对应维度的降级。见 _single_send_degraded。
        self.single_send_degrade_fail = max(0, int(single_send_degrade_fail))
        self.single_send_degrade_ratio = max(0.0, float(single_send_degrade_ratio))
        self.single_send_degrade_slack_ms = max(0.0, float(single_send_degrade_slack_ms))
        # 慢单发采样日志(毫秒):粘性/域名缓存命中的单发"发起到首字节"耗时超阈值
        # 即记一条带 client_ip 的日志(见 HTTP _forward_single / CONNECT 单发)。默认
        # 0=关闭。同时记录采样日志条数供 opt.log 观测阈值触发频率。
        # single_send_fail_logged:失败型卡顿(建连超时/握手失败)按同一阈值观测,
        # 独立计数以便 opt.log 区分"成功但慢" vs "建连失败"两类。
        self.single_send_slow_log_ms = max(0.0, float(single_send_slow_log_ms))
        self.single_send_slow_logged = 0
        self.single_send_fail_logged = 0
        # 请求路径超时(秒):CONNECT 隧道建连/读响应、HTTP 单发读首字节的统一上限。
        # 防某代理 egress→源站建连/握手偶发卡死把请求拖成 10s+(原 CONNECT 硬编码
        # 15s / HTTP read 10s)。测得 CDN 首字节实际 0.6s,默认 3s 给 5 倍余量。
        # 公开同名字段供 /metrics 与测试观测;_try_tunnel/_upstream_timeout 用带 _ 的
        # 钳制后的值。
        self.connect_tunnel_timeout_sec = max(1.0, float(connect_tunnel_timeout_sec))
        self.http_read_timeout_sec = max(1.0, float(http_read_timeout_sec))
        self._tunnel_timeout_sec = self.connect_tunnel_timeout_sec
        self._http_read_timeout_sec = self.http_read_timeout_sec
        # 本地域名白名单强制直连(local_direct_domains):命中的目标(裸 host/IP)强制
        # 走本机直连(local),不经任何远端代理——本机/内网管理服务(如 10.14.25.86:20128)
        # 不被全局 http_read_timeout_sec/connect_tunnel_timeout_sec(3s)掐断。直连失败
        # 直接回 502 不绕远端(用户决策)。白名单条目不依赖 enable_local_racing 开关。
        # local_direct_timeout_sec:白名单直连的放宽超时(默认 10s),仅白名单路径用
        # (relaxed=True),竞速/粘性的 local 单发仍走全局 3s 零行为变化。
        self._local_direct_domains = frozenset(
            self._norm_host(d) for d in (local_direct_domains or []) if d)
        self.local_direct_timeout_sec = max(1.0, float(local_direct_timeout_sec))
        self._local_direct_timeout = self.local_direct_timeout_sec
        self.local_direct_hits = 0        # 白名单命中(强制本机直连)次数
        self.local_direct_failures = 0    # 白名单直连失败(回 502)次数
        # "降级中"代理集合(可观测,非门控):被单发降级判定命中的代理记录于此。
        # 注意真正的门控是每次选择时实时重估 _single_send_degraded(代理恢复后立即
        # 重新可单发,无需冷却),此集合只供 /metrics /circuit 展示"当前被判定降级的
        # 代理";由 _record_win_meta(新赢家接管)或 reset_proxy_quality 清除。
        self._degraded_single_send: set[str] = set()
        # ── 会话粘性协作类(StickyCache,#14)──────────────────────
        # 键 = "{client_ip}|{domain}",值 = {"proxy_id": pid, "updated_at": ts}。
        # 纯内存、滑动 TTL:同一客户端+域名复用上次胜出的代理,保持 egress IP
        # 稳定;粘性代理失败则驱逐并回落竞速(redispatch)。仿 _meta_cache 模式,
        # 但不落盘(粘性是瞬态,重启即清)。决策链成员经 sticky 背引用(router)
        # 读取;self._degraded_single_send 以引用共享给 sticky 的降级判定。
        self.sticky = StickyCache(
            self,
            enable_local_racing=enable_local_racing,
            enabled=stickiness_enabled,
            ttl=stickiness_ttl,
            recheck_hits=stickiness_recheck_hits,
            max_entries=stickiness_max_entries,
            probe_interval_sec=sticky_probe_interval_sec,
            probe_fanout=sticky_probe_fanout)
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
        # 每请求整体超时(秒):连接/池获取设短以快速判负,读首字节给
        # http_read_timeout_sec(默认 3s,原 10s)。
        # 注:曾尝试用 _RACE_HEADER_TIMEOUT + asyncio.wait_for 包裹 send 来独立
        # 收紧 header 等待,但 real-upstream 压测四份对比证明它在 p50 与 p95 间是
        # 权衡而非净赢(且有坏点:5s 配置引爆 soak p99 + fd 堆积),故回退,保留
        # 原超时。尾延迟治理改由 Phase 2a(败者清理下放后台)承担,不带超时权衡。
        # 2026-09 因生产 github CDN 首字节偶发 10s 卡顿,read 收紧到可配置
        # http_read_timeout_sec(默认 3s),生产灰度须盯 p99/fd 是否复现旧坏点。
        self._upstream_timeout = httpx.Timeout(10.0, connect=5.0, pool=5.0,
                                               read=self._http_read_timeout_sec, write=10.0)

        # ── CONNECT 上游 TCP 预热池(P1)+ 目标半预连接(P2)+ 已建握手复用(P3)───
        # 三池统一成 ConnectionPools(见 pools.py,#14):共享单个全局 fd 预算 +
        # 空闲超时 + 空闲暂停(refill_pause)。Router 对本对象做白名单转发
        # (__getattr__/__setattr__),使本构造函数 deleted 区域之外的
        # self._conn_pool / self.conn_pool_creates 等原样解析到 pools。
        self.pools = ConnectionPools(
            proxy_store,
            enabled=conn_pool_enabled, per_proxy=conn_pool_per_proxy, total=conn_pool_total,
            idle_timeout=conn_pool_idle_timeout, refill_interval=conn_pool_refill_interval,
            refill_target=conn_pool_refill_target, connect_timeout=conn_pool_connect_timeout,
            target_prewarm=conn_pool_target_prewarm, established_reuse=conn_pool_established_reuse,
            prehandshake=conn_pool_prehandshake,
            pause_minutes=conn_pool_refill_pause_minutes, pause_silence_sec=conn_pool_refill_pause_silence_sec,
            pause_activity_window=conn_pool_refill_pause_activity_window,
            pause_min_requests=conn_pool_refill_pause_min_requests,
            idle_timeout_cluster=cluster_pool_idle_timeout,
            idle_timeout_established=conn_pool_established_idle_timeout,
            prehandshake_throttle_window_sec=conn_pool_prehandshake_throttle_window_sec,
            prehandshake_throttle_max_per_window=conn_pool_prehandshake_throttle_max_per_window)

        # ── 请求簇预测预热(ClusterGraph,#新增:观察见 _cluster_observe)──────
        # 全局共现图 + 客户端瞬态窗口(不超过 window_sec)。总闸 = conn_pool 第二
        # 阶段开启且启用 cluster_predict(未启用时 observe 为近乎空操作,零状态)。
        # prewarm_spawn 注入 Router._spawn_target_prewarm(绑定方法)的 cluster 变体：
        # 预测只走既有预建通道,受 conn_pool 门/fd 预算/空闲暂停约束,错预建 30s
        # 自动回收;并用 source='cluster' 给预测预建打上专属归因标签(被动预建
        # 调用的裸 _spawn_target_prewarm 走默认 source='passive',见其签名)。
        self.cluster = ClusterGraph(
            proxy_store,
            enabled=(cluster_predict and conn_pool_enabled and conn_pool_target_prewarm),
            window_sec=cluster_window_sec,
            predict_topk=cluster_predict_topk,
            min_support=cluster_min_support,
            ttl_sec=cluster_graph_ttl_sec,
            max_entries=cluster_graph_max_entries,
            throttle_sec=cluster_predict_throttle_sec,
            proxy_fanout=cluster_proxy_fanout,
            probe_decay_sec=cluster_probe_decay_sec,
            prewarm_spawn=lambda h, p, t: self._spawn_target_prewarm(h, p, t, source='cluster'),
            # 熔断感知:摊桶跳过熔断退避期内的代理(退避期内连不上,预建白建 → bucket_miss)。
            # 与竞速路径同一判定(is_circuit_open),保持"预测桶=竞速可用桶"一致。
            is_circuit_open=self.selector.is_circuit_open)

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

        # ── HTTP 响应缓存(HttpCache,#14)────────────────────────
        # 缓存(_http_cache/配置/二级索引/LRU/在途聚合)已随 #14 拆分搬入
        # HttpCache(self.httpcache),经类尾 _CACHE_FORWARD 白名单 __getattr__/
        # __setattr__ 转发。http_cache_hits/misses 计数留在 Router(在上方初始化,
        # flow 直接自增,snapshot_counters 直读)。enable_http_cache 门随类单源。
        self.httpcache = HttpCache(
            enable_http_cache=enable_http_cache,
            ttl=http_cache_ttl,
            max_entries=http_cache_max_entries,
            max_bytes=http_cache_max_bytes,
            stream_limit=http_cache_stream_limit)

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
        # DB 冷启动载入:updated_at 为 ISO 字符串,解析一次换算成单调时钟浮点
        # (等价于 _record_win_meta 写入的 _updated_mono),此后热路径免解析。
        for domain, dp, ua, ewma in meta_rows:
            mono = None
            try:
                dt = datetime.fromisoformat(ua)
                # ISO(UTC) → 与 time.monotonic 对齐的单调基线:用墙上时钟差倒推。
                # 单调时钟重启清零,此处取"距现在秒数"等价于 _get_fresh_proxy 的
                # (now - dt) 判定;用 time.time() 与单调时钟的相对关系换算即可。
                age = (datetime.now(timezone.utc) - dt).total_seconds()
                mono = time.monotonic() - age
            except Exception:
                pass
            self._meta_cache[domain] = {
                "default_proxy": dp,
                "updated_at": ua,
                "_updated_mono": mono,
                "ref_ewma": (float(ewma) if ewma is not None else None),
            }

    @staticmethod
    def _norm_host(host: str) -> str:
        """归一化 hostname/IP 用于白名单匹配:去尾点、小写、IPv6 去括号。

        不拆端口(白名单条目是裸 host;HTTP 的 domain 已由 urlparse.hostname 剥掉
        端口,CONNECT 的 host 由调用方按 _try_tunnel 同逻辑解析)。空串原样返回。
        """
        h = (host or "").strip()
        if h.startswith('[') and h.endswith(']'):
            h = h[1:-1]
        if h.endswith('.'):
            h = h[:-1]
        return h.lower()

    def _host_in_local_direct(self, host: str) -> bool:
        """目标 host 是否命中本地白名单(强制本机直连)。空白名单恒 False。"""
        if not self._local_direct_domains or not host:
            return False
        return self._norm_host(host) in self._local_direct_domains

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

    def _worse_than_best(self, domain: str, pid: str) -> bool:
        """方向 A:赢家回填前的质量闸——代理 EWMA 是否显著差于当前可用最优代理。

        判定:pid 有 EWMA 观测(不要求 obs>=2——竞速赢家常是刚起步的新代理,
        首胜时往往只有 1 次观测,若这里苛求 obs 数会让新慢代理绕过闸门被钉住),
        且当前可用最优代理(ordered_proxies 首位的 EWMA)有观测且 >0 时,若
        pid EWMA > 最优 EWMA × ratio(默认 2.0) 且差量 > slack
        (single_send_degrade_slack_ms,防极低延迟误判)→ 视为"显著更慢"。
        最优代理可能正是 pid 自己(此时不触发);无最优/无观测 → False。

        **域名级优先**:该域名有 pid 的观测时,与**同域名下的最优可用代理**
        (best_domain_ewma,过滤熔断/禁用/删除)比较;pid 是唯一观测者 → 不拦
        (与全局 `best == pid → False` 同构,防"全局慢/该域名唯一观测"的代理被
        全局最优误拦)。域名级数据不足(该域名无 pid 观测)→ 回退全局逻辑,与
        现状一致(重启后/冷启动平滑过渡)。

        与 Goal #6 degrade 的语义对比:degrade 是"相对**钉住时刻**的自身基线恶化",
        这里问"相对**当前最优代理**差多少"。前者兜底"钉住的代理自己变差",后者
        兜底"赢家本就显著劣于其他代理"(竞速偶发让慢代理赢,却把它回填钉住)。
        ratio 复用 single_send_degrade_ratio(生产 2.0),语义一致:EWMA 慢 2 倍
        就认为不该单发。
        """
        if self.single_send_degrade_ratio <= 0:
            return False
        slack = self.single_send_degrade_slack_ms / 1000.0
        dq = self.selector._domain_quality_for(domain, pid)
        if dq is not None:
            cur = self.selector._proxy_quality_ewma(dq)
            if cur is None:
                return False
            best, best_ewma = self.selector.best_domain_ewma(domain, exclude=pid)
            if best is None:
                return False  # 自己是该域名唯一有观测的可用代理,无比较对象不拦
            return cur > best_ewma * self.single_send_degrade_ratio \
                and (cur - best_ewma) > slack
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
        if self._worse_than_best(domain, pid):
            return
        self._meta_cache[domain] = {
            "default_proxy": pid,
            "updated_at": self._now_utc(),
            # 单调时钟 TTL 判定用的浮点时间戳:_get_fresh_proxy 用它判断
            # cache_ttl 是否过期,避免每次检查都 fromisoformat + tz 计算。
            # updated_at(ISO)保留给 API 展示与 DB 持久化,两者不冲突。
            "_updated_mono": time.monotonic(),
            "ref_ewma": self._ref_ewma_for(domain, pid),
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
                    self.sticky._prune_sticky()
                    self.cluster.prune()
                    self.selector.prune_domain_quality()
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
        剔除内部字段(_updated_mono 单调时钟 TTL 判定专用,不外露)。
        """
        return {d: {k: v for k, v in m.items() if k != '_updated_mono'}
                for d, m in self._meta_cache.items()}

    def get_domain_meta_enriched(self) -> dict[str, dict]:
        """读取域名元数据 + 自适应 TTL 状态(P2 验收:/domains/meta 展示
        ttl/expires_at/switch_count)。仅自适应 TTL 开启时附加字段,否则与
        get_domain_meta_from_db 同构(兼容旧消费方)。
        """
        out = {d: {k: v for k, v in m.items() if k != '_updated_mono'}
               for d, m in self._meta_cache.items()}
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
            "sticky_cache_hits": self.sticky.sticky_cache_hits,
            "sticky_evictions": self.sticky.sticky_evictions,
            "sticky_slow_probes": self.sticky.sticky_slow_probes,
            "sticky_probes_fired": self.sticky.sticky_probes_fired,
            "sticky_probe_evictions": self.sticky.sticky_probe_evictions,
            "racing_invocations": self.racing_invocations,
            "upstream_attempts": self.upstream_attempts,
            "http_cache_entries": len(self.httpcache._http_cache),
            "http_cache_bytes": self.httpcache._http_cache_bytes,
            "http_cache_evictions": self.httpcache.http_cache_evictions,
            "client_pool_size": len(self._client_pool),
            "sticky_cache_size": len(self.sticky._sticky_cache),
            "request_counts": dict(self.request_counts),
            "attempted_counts": dict(self.attempted_counts),
            "proxy_quality": self.selector.get_quality(),
            "proxy_in_flight": self.selector.get_in_flight(),
            "max_in_flight": self.selector.max_in_flight,
            "proxy_concurrency_limits": self.selector.get_concurrency_limits(),
            "concurrency_limit_enabled": self.selector.concurrency_enabled,
            "conn_pool_enabled": self.pools.conn_pool_enabled,
            "conn_pool_creates": self.pools.conn_pool_creates,
            "conn_pool_hits": self.pools.conn_pool_hits,
            "conn_pool_misses": self.pools.conn_pool_misses,
            "conn_pool_expired": self.pools.conn_pool_expired,
            "conn_pool_size": sum(len(v) for v in self.pools._conn_pool.values()),
            "conn_pool_target_prewarm": self.pools.conn_pool_target_prewarm,
            "conn_pool_refill_pause_minutes": self.pools.conn_pool_refill_pause_minutes,
            "conn_pool_refill_pause_activity_window": self.pools.conn_pool_refill_pause_activity_window,
            "conn_pool_refill_pause_min_requests": self.pools.conn_pool_refill_pause_min_requests,
            "conn_pool_idle_paused": self.pools._conn_pool_idle(),
            "target_pool_creates": self.pools.target_pool_creates,
            "target_pool_hits": self.pools.target_pool_hits,
            "target_pool_misses": self.pools.target_pool_misses,
            "target_pool_expired": self.pools.target_pool_expired,
            "cluster_pool_creates": self.pools.cluster_pool_creates,
            "cluster_pool_hits": self.pools.cluster_pool_hits,
            "cluster_pool_expired": self.pools.cluster_pool_expired,
            "cluster_pool_timing_miss": self.pools.cluster_pool_timing_miss,
            "cluster_pool_bucket_miss": self.pools.cluster_pool_bucket_miss,
            "cluster_pool_consumed_expired": self.pools.cluster_pool_consumed_expired,
            "cluster_pool_idle_timeout": self.pools.cluster_pool_idle_timeout,
            "target_pool_size": sum(len(v) for v in self.pools._target_pool.values()),
            "target_prewarm_dispatched": self.pools.target_prewarm_dispatched,
            "target_prewarm_success": self.pools.target_prewarm_success,
            "target_prewarm_failed": self.pools.target_prewarm_failed,
            "cluster_predict": self.cluster.enabled,
            "cluster_windows_learned": self.cluster.cluster_windows_learned,
            "cluster_predictions": self.cluster.cluster_predictions,
            "cluster_prewarm_spawned": self.cluster.cluster_prewarm_spawned,
            "cluster_bucket_spawns": self.cluster.cluster_bucket_spawns,
            "cluster_graph_size": self.cluster.graph_size(),
            "conn_pool_established_reuse": self.pools.conn_pool_established_reuse,
            "conn_pool_established_idle_timeout": self.pools.established_pool_idle_timeout,
            "conn_pool_prehandshake": self.pools.prehandshake_enabled,
            "established_pool_prehandshook": self.pools.established_pool_prehandshook,
            "established_pool_prewarm_failed": self.pools.established_pool_prewarm_failed,
            "prehandshake_throttled_skips": self.pools.prehandshake_throttled_skips,
            "prehandshake_throttle_window_sec": self.pools.prehandshake_throttle_window_sec,
            "prehandshake_throttle_max_per_window": self.pools.prehandshake_throttle_max_per_window,
            "established_pool_hits": self.pools.established_pool_hits,
            "established_pool_misses": self.pools.established_pool_misses,
            "established_pool_expired": self.pools.established_pool_expired,
            "established_pool_returned": self.pools.established_pool_returned,
            "established_pool_size": sum(len(v) for v in self.pools._established_pool.values()),
            "connect_new_conns": self.pools.connect_new_conns,
            "probes_sent": self.probes_sent,
            "probes_ok": self.probes_ok,
            "probes_skipped": self.probes_skipped,
            "probes_failed": self.probes_failed,
            "circuit_open_count": self.selector.circuit_open_count,
            "circuit_state": self.selector.get_circuit_state(),
            "single_send_degrades": self.single_send_degrades,
            "single_send_slow_logged": self.single_send_slow_logged,
            "single_send_fail_logged": self.single_send_fail_logged,
            "single_send_slow_log_ms": self.single_send_slow_log_ms,
            "local_direct_hits": self.local_direct_hits,
            "local_direct_failures": self.local_direct_failures,
            "connect_tunnel_timeout_sec": self.connect_tunnel_timeout_sec,
            "http_read_timeout_sec": self.http_read_timeout_sec,
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

    def _ref_ewma_for(self, domain: str, pid: str) -> Optional[float]:
        """钉住时刻的 EWMA 基线捕获(供 _record_win_meta / _record_sticky)。

        域名级优先:该域名有 pid 的观测(obs>0)时取域名级 EWMA——这样恶化判定
        相对"该代理对该域名的真实延迟",而非被其他域名拖累的全局平均;域名级
        数据缺失(重启后/冷启动)回退全局,与旧行为一致。
        """
        dq = self.selector._domain_quality_for(domain, pid)
        if dq is not None and int(dq.get("obs", 0)) > 0:
            return self.selector._proxy_quality_ewma(dq)
        return self._proxy_quality_ewma(self.selector.get_quality().get(pid))

    def _single_send_degraded(self, domain: str, pid: str, ref_ewma: Optional[float]) -> bool:
        """被钉住代理在"单发选择"时是否已恶化,应降级回竞速(Goal #6)。

        两条独立信号,任一命中即判定不稳定(与熔断解耦——熔断是"连续失败达阈值
        直接剔除",这里是"尚未熔断但已开始变差,别再确定性单发,交给竞速选路"):
          1) 连续失败:selector 的连续失败计数 ≥ single_send_degrade_fail
             (熔断阈值 3 的早告警,默认 2)。被钉住代理最近在真实请求/探活中
             连续失败,说明它在变差——单发命中它只会放大失败路径,降级回竞速
             让有序候选/兜底批自动绕开它。此信号保持全局(proxy 级健康信号,
             跨域名共享)。
          2) EWMA 恶化:当前 EWMA ≥ ref_ewma × single_send_degrade_ratio。
             与熔断器解耦——该代理可能仍整体健康(EWMA 未到"差"的绝对档),但
             相比被钉住时显著变慢,应重竞速换新赢家。EWMA 相对基线恶化
             (envoy 风格连续失败剔除 + 基线比对,见分析 doc P2-6)。
             **域名级优先**:该域名有 pid 的观测时用域名级 EWMA 与域名级 obs
             (避免全局 EWMA 跨域名平均掩盖"该代理对这个域名其实很快",生产案例
             247-246);域名级数据缺失则回退全局(重启后/冷启动平滑过渡)。

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
            dq = self.selector._domain_quality_for(domain, pid)
            if dq is not None:
                cur = self.selector._proxy_quality_ewma(dq)
                obs = int(dq.get("obs", 0))
            else:
                q = self.selector.get_quality().get(pid)
                cur = self._proxy_quality_ewma(q)
                obs = int(q.get("obs", 0)) if q else 0
            if cur is not None and obs >= 2 \
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
        if self._single_send_degraded(domain, pid, entry.get("ref_ewma")):
            self._degraded_single_send.add(pid)
            # 自适应 TTL:被钉住代理开始恶化 → TTL 打回下限,让竞速新赢家接管。
            if self.adaptive_ttl_enabled:
                self._domain_ttl_cache[domain] = self.adaptive_ttl_min
                self.domain_ttl_resets += 1
            return None
        ttl = self._domain_ttl(domain)
        # 优先用单调时钟浮点时间戳判定 TTL(热路径:无字符串解析/时区计算)。
        # 仅 DB 冷启动载入的条目缺 _updated_mono,回退 ISO 解析(启动期一次)。
        mono = entry.get("_updated_mono")
        if mono is not None:
            if time.monotonic() - mono < ttl:
                return pid
            return None
        updated_at_str = entry["updated_at"]
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

    # ── 会话粘性(StickyCache,#14)───────────────────────────
    # get_sticky_cache/_get_sticky_proxy/_record_sticky/_prune_sticky 等方法
    # 已随 #14 拆分搬入 StickyCache(self.sticky),经类尾 _STICKY_FORWARD
    # 白名单 __getattr__ 转发。决策链成员经 sticky 背引用(router)读取。
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

    @staticmethod
    def _set_pool_keepalive(writer):
        """对驻池连接设 SO_KEEPALIVE:OS 在 TCP_KEEPIDLE 后探测半开连接。

        已握手隧道池(_established_pool)的复用前活性探测(read(1)+超时)只能查出对端
        已发 FIN/RST 的死连接;纯半开(对端静默、连接仍 ESTABLISHED)会让 read 一直
        阻塞到超时而误判"活"。启用 SO_KEEPALIVE 后 OS 在 KEEPIDLE(60s)后发探测包,
        对端不可达即判死(RST/错误),进程内 read 随即收到异常——在驻池期间就把半开
        清掉,而不是等到复用后首包才暴露。失败静默(部分平台/socket 阶段无 socket)。
        """
        try:
            sock = writer.get_extra_info('socket')
            if sock is None:
                return
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            if hasattr(socket, 'TCP_KEEPIDLE'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
            if hasattr(socket, 'TCP_KEEPINTVL'):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        except (OSError, AttributeError):
            pass

    # ── HTTP GET 缓存(HttpCache,#14)────────────────────────
    # _http_cache_key/_get/_remove/_set/_invalidate 已随 #14 拆分搬入
    # HttpCache(self.httpcache),经类尾 _CACHE_FORWARD 白名单 __getattr__
    # 转发。_write_cached_response/流式缓冲/flow 编排留 Router。
    # ── 上游连接池 ──────────────────────────────────────────────

    def _client_key(self, pid: str, proxy_url: Optional[str]) -> str:
        """连接池键:用 proxy_url 区分"同一 pid 不同上游凭据/地址"的情形。

        local(无上游)用固定键 'local';走上游的用 proxy_url(已含凭据)。
        实际上 pid↔proxy_url 一一对应,但用 proxy_url 作键更稳健。
        """
        if proxy_url is None:
            return 'local'
        return proxy_url

    async def _get_client(self, key: str, proxy_url: Optional[str],
                          relaxed: bool = False) -> httpx.AsyncClient:
        """从池中取(或按需创建)某上游的长驻 httpx.AsyncClient。

        池化跨请求复用 keep-alive 连接,避免每请求重建到上游代理的 TCP
        (HTTPS 经 CONNECT 还多一次握手)。client 不随单请求关闭,仅在
        stop() 时统一 aclose。

        relaxed=True:用 local_direct_timeout_sec 替代全局 _upstream_timeout
        (仅本地白名单强制直连路径传)。其余(竞速/粘性/域名缓存的 local 单发)
        保持默认 relaxed=False 用全局 3s,零行为变化。
        """
        client = self._client_pool.get(key)
        if client is not None and not client.is_closed:
            return client
        kw: dict[str, Any] = {
            "timeout": (httpx.Timeout(self._local_direct_timeout,
                                      connect=self._local_direct_timeout,
                                      pool=5.0, read=self._local_direct_timeout,
                                      write=self._local_direct_timeout)
                        if relaxed else self._upstream_timeout),
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

    # ── CONNECT 预热池(P1)+ 目标半预连接(P2)+ 已建握手复用(P3)────────
    # 三池状态与方法 #14 后统一在 pools.py 的 ConnectionPools(self.pools),
    # Router 经 _POOL_FORWARD 白名单转发(见类尾 __getattr__/__setattr__),
    # 热路径原有 self._conn_pool / self.conn_pool_creates 等引用原样解析到 pools。
    # Router 侧只剩"触发预热"的一处编排 —— task 注册/排空进 _running_tasks。

    def _spawn_target_prewarm(self, proxy_host: Optional[str], proxy_port: Optional[int],
                              target: str, source: str = 'passive', *,
                              proxy_auth: Optional[dict] = None):
        """命中域名缓存/粘性或竞速胜出的 CONNECT → 后台预热 (proxy, target) 连接。

        仅在第二阶段开启且经上游代理(非本机直连)时触发;计数并登记到
        self._running_tasks(供 stop() 排空)。实际预热协程在 pools 的
        _target_pool_prewarm(idle-pause/预算/超时都在 pools 侧决定)。
        预热条数由 _target_pool_prewarm 默认 cap=2 控制(取走 1 条仍留 1 条备用,
        降低"取走即空→周期 miss";生产实测 cap=1 时 target_pool_hits=1/misses=71)。

        `source` 归因标签:域缓存/粘性/竞速胜后的被动预建走默认 'passive';
        ClusterGraph 预测预建经注入的 prewarm_spawn lambda 以 source='cluster'
        进入(见 __init__ 接线)。pools 据此给 cluster 连接打上 _cluster_prewarmed
        标签,算 cluster 专属命中率(cluster_pool_hits / cluster_pool_creates)。

        **预握手升级(prehandshake)**:`proxy_auth` 提供访问该上游代理的凭据
        (调用点已拿到 proxy.auth,带进 pools 免二次反查)。pools 在预握手开启且
        established_reuse 开启时,对**被动**预建(self 发起的(proxy, target))
        额外建一条**自建 TCP + CONNECT 预握手**隧道(CONNECT 拿 200 即进
        established 池,skip 下次握手)——注意是**新建** TCP,不复用本次请求的
        赢家隧道(那隧道正被 _relay_tunnel 透传,复用会与业务数据交织竞态)。
        预握手库存打 _prehandshook 标签走 established 超时(600s),并计
        established_pool_prehandshook 专属命中率。cluster 预测保持只建 TCP
        (预测命中率 ~3%,预握手浪费面大,v1 不做)。未开启 established_reuse 或
        预握手失败时回退只建 TCP 进 target 池(现状,零行为变化)。
        """
        if not (self.pools.conn_pool_enabled and self.pools.conn_pool_target_prewarm):
            return
        if proxy_host is None:
            return  # 本机直连路径无"上游代理"可预热
        self.pools.target_prewarm_dispatched += 1
        logger.info("target prewarm SPAWN %s via %s:%s (dispatched=%d)",
                    target, proxy_host, proxy_port, self.pools.target_prewarm_dispatched)
        task = asyncio.create_task(
            self.pools._target_pool_prewarm(proxy_host, proxy_port, target,
                                            source=source, proxy_auth=proxy_auth))
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    def _spawn_sticky_probe(self, client_ip: str, domain: str, sticky_pid: str):
        """粘性命中后 fire-and-forget 后台探路(杠杆A):对竞争代理做轻量 CONNECT
        竞速,探路结果显示有代理显著快于粘性代理时驱逐该粘性条目。

        domain 是 sticky 记账桶:CONNECT 为原始 target("host:port"),HTTP 为裸
        hostname(与 record_ttfb 的 domain 桶同键)。探路统一走 CONNECT-only:
        HTTP 的 probe_target 拼成 "host:443"(HTTPS 短连接是压 TTFB 主场景)。
        探路候选的 EWMA 记入 probe_target 桶(CONNECT 时与 domain 同桶;HTTP 时
        与 sticky 的裸 hostname 桶不同——比较时 cur 读 domain 桶、best 读
        probe_target 桶,两桶独立,见 _sticky_probe_race)。

        仿 _spawn_target_prewarm 的任务登记/排空模式:asyncio.create_task 注册进
        _running_tasks + add_done_callback(discard),stop() 统一收尾。探路本身
        CONNECT-only、不拉业务数据,首个 CONNECT 200 即停;失败静默(不算失败
        观测,不喂熔断)。节流由 sticky 侧 cooldown 状态保证(sticky_probe_due)。
        """
        if not self.sticky.stickiness_enabled or self.sticky.stickiness_probe_interval_sec <= 0:
            return
        if sticky_pid == 'local':
            return  # 直连路径无上游可探
        # 探路 target:CONNECT 的 domain 已是 "host:port";HTTP 裸 hostname → 拼 443。
        probe_target = domain if ':' in domain else f"{domain}:443"
        if not self.selector._domain_quality.get(domain):
            return  # sticky 记账桶无观测,无从比较快慢(探路无基准)
        if not self.sticky.sticky_probe_due(client_ip, domain):
            return  # 冷却内:不重复探(时间戳由探路协程启动时刷新,不在此消耗)
        task = asyncio.create_task(
            self._sticky_probe_race(client_ip, domain, probe_target, sticky_pid))
        self._running_tasks.add(task)
        task.add_done_callback(self._running_tasks.discard)

    async def _sticky_probe_race(self, client_ip: str, domain: str, probe_target: str,
                                 sticky_pid: str):
        """单次探路:对竞争代理发起 CONNECT-only 错峰竞速,收集最快者 EWMA,显著
        快于粘性代理则驱逐粘性条目(复用 _sticky_slow_probe_due 的 ratio/slack)。

        cur 读 domain 桶(sticky 代理的真实请求 EWMA);best 读 probe_target 桶
        (探路赢家的 CONNECT 握手 EWMA)。CONNECT 时两桶合一(都是 target);
        HTTP 时不同桶(裸 hostname vs host:443),比较是"真实请求延迟 vs 探路
        握手延迟",只对"显著更快"才驱逐,粗比值下误差可容忍。

        探路赢家的 EWMA 由 _try_tunnel 内部 record_ttfb 自动喂入 probe_target
        桶——但**探路只握手不拉数据**,单次握手延迟混入域名 EWMA 有噪声。设计上
        接受:探路频率低(节流 interval),且只影响该桶的 best 基准,不参与 sticky
        驱逐的 cur(那吃真实请求 TTFB)。探路建出的隧道经 _cleanup_tunnel_result
        归还 _established_pool(杠杆C,探路即备货)。
        """
        try:
            # 探路时间戳在此刷新:只有真正进入探路才消耗节流。
            self.sticky._sticky_probe_last[self.sticky._sticky_key(client_ip, domain)] = time.monotonic()
            exclude = sticky_pid if sticky_pid != 'local' else None
            candidates = self.selector.ordered_for_domain(probe_target)
            candidates = [p for p in candidates if p != exclude and p != 'local']
            candidates = candidates[:self.sticky.stickiness_probe_fanout]
            places = [(pid, probe_target) for pid in candidates if self.proxy_store.get(pid)]
            if not places:
                return
            race_cleanup = functools.partial(self._cleanup_tunnel_result, target=probe_target)
            winner = await self._race_staggered(
                places, cleanup=race_cleanup,
                initial=1, interval=self.stagger_interval)
            if winner is None:
                return
            win_pid = winner[0]
            self.sticky.sticky_probes_fired += 1
            # 判定:探路赢家是"候选里最快的",若它显著快于 sticky 代理 → 驱逐。
            dq = self.selector._domain_quality_for(domain, sticky_pid)
            if dq is None:
                return
            cur = self.selector._proxy_quality_ewma(dq)
            if cur is None:
                return
            best_ewma = self.selector._proxy_quality_ewma(
                self.selector._domain_quality_for(probe_target, win_pid)) or cur
            slack = self.single_send_degrade_slack_ms / 1000.0
            if cur > best_ewma * self.single_send_degrade_ratio \
                    and (cur - best_ewma) > slack:
                self.sticky.sticky_probe_evictions += 1
                self.sticky._evict_sticky(client_ip, domain)
                logger.info("sticky probe EVICT %s pid=%s cur=%.3fs best=%.3fs",
                            domain, sticky_pid, cur, best_ewma)
        except Exception:
            logger.debug("sticky probe failed", exc_info=True)

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
                              headers: Optional[dict] = None, body: Optional[bytes] = None,
                              domain: Optional[str] = None) -> Optional[Any]:
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
            running.add(self._make_race_task(p, method, url, headers, body, domain))
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
                running.add(self._make_race_task(unlaunched.pop(), method, url, headers, body, domain))
        # 全部候选耗尽仍无胜者:completed 里那些"拿到 5xx 响应头但被判非胜"的任务
        # 持有流式 resp(占 httpx 连接池连接),必须下放清理,否则反复全失败累积到
        # 连接池耗尽。winner 分支已在上面处理了 completed(经 losers),_race 的
        # 竞速任务失败路径也会自清——这里只兜底走到底仍无胜者这个唯一出口。
        if completed and cleanup:
            self._spawn_cleanup(completed, cleanup)
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
                        body: Optional[bytes], domain: Optional[str] = None) -> asyncio.Task:
        """把一个候选占位(pid 或 (pid, target))惰性创建为竞速 task。

        统一工厂供 _race_staggered 补发候选:place 为字符串 pid 时建 HTTP task
        (_try_http,经上游代理转发;pid='local' 直连);place 为 (pid, target) 时建
        CONNECT 隧道 task(_try_tunnel,经上游 CONNECT)。延迟到调用时才 create_task,
        保证"未发候选不启动"——这是错峰与 _race 同时全发的本质区别。

        domain: HTTP 路径已由调用方算好的域名 key,透传 _try_http 避免重复
        urlparse;CONNECT 路径用 target 作 key,忽略此参。
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
            return asyncio.create_task(self._try_http('local', None, method, url, headers, body, domain))
        return asyncio.create_task(
            self._try_http(pid, self.proxy_store.proxy_url(pid), method, url, headers, body, domain))

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

    async def _cleanup_tunnel_result(self, result, target=None):
        """竞速败者 CONNECT 隧道的统一清理:可归还时归还 _established_pool,
        否则关闭上游连接。result = (pid, up_reader, up_writer)。

        target 由 CONNECT 竞速调用点经 functools.partial 部分绑定(缺省 None 时
        退化为旧行为——只关闭连接,用于 HTTP 竞速等无 target 上下文路径)。
        上游地址按 result[0] 的 pid 反查 proxy_store(避免 partial 在 mixed
        local/上游批次里绑定错 host;local 直连 proxy 为 None → 不归还)。
        竞速败者 CONNECT 200 后 _try_tunnel 已 record_ttfb;竞速 harness 只读
        响应头不读数据字节,败者连接无脏 buffer,满足 _maybe_return_established
        的干净判定。开启 established_reuse 后,竞速浪费的已握手隧道变库存。
        """
        if not result:
            return
        pid = result[0]
        proxy = None if pid == 'local' else self.proxy_store.get(pid)
        if (self.pools.conn_pool_established_reuse and proxy is not None and target):
            await self._maybe_return_established(
                result[2], result[1], proxy.host, proxy.port, target)
        else:
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

    async def _try_http(self, pid: str, proxy_url: Optional[str], method: str, url: str, headers: dict, body: Optional[bytes], domain: Optional[str] = None, relaxed: bool = False):
        """经某上游代理尝试一次 HTTP 请求,作为竞速的一个候选(流式)。

        从连接池取长驻 client,以 stream=True 发送——收到响应头即返回(resp
        尚未读 body)。这是"首字节判胜"的基础:_race 在某候选返回响应头时
        即判其获胜,其余候选随即取消、其流式 resp 被 aclose(见 _cleanup),
        不再下载整包。获胜者的 body 由调用方在 _stream_upstream_response 中
        边收边转发,client 用完归还连接池(不关闭)。

        domain: 已由调用方(handle_client→_handle_http_request)算好的域名 key,
        竞速多候选共用同一 URL → 传入避免每个候选重复 urlparse;为 None 时
        此处回退解析(单发等路径),保证与 _record_attempt / _record_win_meta
        用同一解析表达式,域名级 EWMA 的 key 与 meta/sticky 取用 key 完全一致。

        relaxed: 本地白名单强制直连路径传 True → _get_client 用
        local_direct_timeout_sec 放宽超时;其余调用保持 False 用全局 3s。

        成功返回 (pid, method, url, resp, client);失败(BaseException,含
        CancelledError)关闭 resp 并向上抛出,让 _race 的清理逻辑处理。
        """
        key = self._client_key(pid, proxy_url)
        client = await self._get_client(key, proxy_url, relaxed=relaxed)
        resp = None
        # 计入该代理在途数:从"发起尝试"到"收到响应头/失败/被取消"的整个窗口,
        # 供加权 least-request 选批避开积压代理。finally 中无论何种出口都释放。
        self.selector._inflight_start(pid)
        try:
            self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
            self.upstream_attempts += 1  # 聚合竞速扇出总数(供 /metrics 算放大率)
            # 首字节计时:从发起到收到响应头。用于 EWMA 质量跟踪(竞速排序)。
            # 调用方通常已算出 domain(见 docstring),仅兜底时才 urlparse。
            if domain is None:
                domain = urllib.parse.urlparse(url).hostname or url
            t0 = time.perf_counter()
            resp = await client.send(
                client.build_request(method, url, headers=headers, content=body),
                stream=True)
            self.selector.record_ttfb(pid, time.perf_counter() - t0, domain)
            self.request_counts[pid] = self.request_counts.get(pid, 0) + 1
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

    @staticmethod
    def _try_tunnel_host(target: str) -> str:
        """从 CONNECT target("host:port" 或 "[ipv6]:port")解析出裸 host。

        供 _try_tunnel 直连分支与 _handle_connect 白名单判定共用,保证两处解析
        一致。非法 target 原样返回(由 _try_tunnel 报错处理)。
        """
        if not target or ':' not in target:
            return target or ''
        if target.startswith('['):
            host_end = target.find(']')
            if host_end > 0:
                return target[1:host_end]
            return target
        return target.rsplit(':', 1)[0]

    async def _try_tunnel(self, pid: str, target: str, proxy_host: Optional[str], proxy_port: Optional[int], proxy_auth: Optional[dict], relaxed: bool = False):
        """尝试建立一条 CONNECT 隧道,作为竞速的一个候选。

        - proxy_host 给定:经该上游代理发起 CONNECT(带上游 Proxy-Authorization)。
        - proxy_host 为 None:直连 target(本机竞速路径)。target 形如
          "host:port" 或 "[ipv6]:port"。
        建连与读响应均设 connect_timeout(self._tunnel_timeout_sec,默认 3s),
        防止挂死上游长期占用竞速槽。
        relaxed: 本地白名单强制直连路径传 True → connect_timeout 用
        local_direct_timeout_sec(默认 10s);其余调用保持 False 用全局 3s。
        成功返回 (pid, up_reader, up_writer);失败/被取消则关闭上游连接并抛出。
        """
        # 建立 CONNECT 与读取响应均设超时，避免挂死的上游无限占用竞速 task 与连接。
        connect_timeout = self._local_direct_timeout if relaxed else self._tunnel_timeout_sec
        try:
            if proxy_host is not None:
                # CONNECT 预热池(P1)+ 目标半预连接(P2):取用顺序为——
                # 1) target 半预连接池(按 proxy|target 键,只预连"到上游代理"的
                #    TCP,未发 CONNECT,可安全复用);2) 第一阶段通用池;3) 新建。
                # 取用成功即省掉"本机→上游"建连 TTFB。
                if self.pools.conn_pool_enabled:
                    up_reader, up_writer = None, None
                    # 已握手隧道复用:优先取"已发 CONNECT 且收到 200"的连接,命中则
                    # 跳过下方 CONNECT 握手,直接返回(连接已处于可透传状态)。
                    if self.pools.conn_pool_established_reuse:
                        up_reader, up_writer = self.pools._established_pool_peek(proxy_host, proxy_port, target) or (None, None)
                        if up_reader is not None and not await self.pools._established_alive(up_reader, up_writer):
                            # 死/脏连接:丢弃(peek 已计 hits),不跳过多余 I/O、不影响下
                            # 一候选判定——回落后续池/新建。避免一个 tick 赢竞速。
                            self.pools.established_pool_expired += 1
                            logger.info("established pool DEAD-ON-PROBE %s via %s:%s",
                                        target, proxy_host, proxy_port)
                            _discard_conn(up_writer)
                            up_reader = up_writer = None
                    if up_reader is None and self.pools.conn_pool_target_prewarm:
                        up_reader, up_writer = self.pools._target_pool_peek(proxy_host, proxy_port, target) or (None, None)
                    if up_reader is None:
                        pooled = self.pools._conn_pool_peek(proxy_host, proxy_port)
                        if pooled is not None:
                            up_reader, up_writer = pooled
                    if up_reader is None:
                        self.pools.connect_new_conns += 1  # 观测:池未中需新建
                        up_reader, up_writer = await asyncio.wait_for(
                            asyncio.open_connection(proxy_host, proxy_port), timeout=connect_timeout)
                else:
                    self.pools.connect_new_conns += 1  # 观测:无池路径每次新建
                    up_reader, up_writer = await asyncio.wait_for(
                        asyncio.open_connection(proxy_host, proxy_port), timeout=connect_timeout)
            else:
                host = self._try_tunnel_host(target)
                if not host:
                    raise ValueError(f'Invalid CONNECT target: {target}')
                # 端口从 target 解析(host:port);_try_tunnel_host 只回裸 host。
                if target.startswith('['):
                    host_end = target.find(']')
                    port = int(target[host_end + 2:])
                else:
                    port = int(target.rsplit(':', 1)[1])
                up_reader, up_writer = await asyncio.wait_for(
                    asyncio.open_connection(host, port), timeout=connect_timeout)
        except (asyncio.TimeoutError, OSError, ConnectionError) as e:
            raise RuntimeError(f'connect to {proxy_host or target} timed out or failed: {e}') from e
        # 首字节计时:从 CONNECT 发出到收到 200。用于 EWMA 质量跟踪(竞速排序)。
        # 复用已握手隧道时无 CONNECT 往返,计时为 0(不做 EWMA 观测)。
        reused_established = (self.pools.conn_pool_established_reuse
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
                # CONNECT 域名 key = 原始 target("host:port"),与 _record_attempt /
                # _get_fresh_proxy(target) / sticky key(client_ip|target)一致。
                self.selector.record_ttfb(pid, time.perf_counter() - t0, target)
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
            # 客户端请求头有界读:每行受 readline 64KB 限制,但行数无上限——慢速
            # loris 式攻击发大量小 header 行会让 headers bytearray 无界增长。
            # 双上限:header 数量(100)与累计总字节(64KB),任何一项超限拒绝并关连接
            # (拒绝而非容忍慢读——请求不合法,不必然回 431)。字节上限取 64KB,
            # 单行已被主机背压限制,blocking 客户端最多消耗 64KB×100,有界。
            headers = bytearray()
            header_lines = 0
            while True:
                h = await reader.readline()
                if not h:
                    break
                if h in (b"\r\n", b"\n"):
                    break
                headers.extend(h)
                header_lines += 1
                if header_lines > _MAX_REQUEST_HEADER_LINES or len(headers) > _MAX_REQUEST_HEADER_BYTES:
                    logger.warning("rejecting client %s: header limit exceeded (lines=%d bytes=%d)",
                                   peer, header_lines, len(headers))
                    raise ConnectionError('request header limit exceeded')
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
            self.pools._record_request_activity()
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

        冷启动判定保持**全局**键:ordered_for_domain 在域名无观测时回退全局排序
        (可信)、有观测时按域名排(更可信)——排序可信度只由全局质量决定,域名
        维度不改变翻倍语义(域名无观测不是"排序不可信",恰是回退到全局排序)。
        """
        if not self.selector.has_quality():
            return min(self.max_retries, max(2, self.stagger_initial))
        return self.stagger_initial

    def _prep_http(self, proxies: List[str], host: str = "") -> tuple:
        """HTTP 竞速的启动参数:首批/补发按 stagger 配置取占位,返回 (initial_places, remaining)。

        供 _dispatch_single 拼接 _race_staggered 的调用。`initial_places` 是
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
            self.httpcache._http_cache_invalidate(domain)

        # 1) HTTP 响应缓存:GET 幂等响应直接命中,完全不经上游。
        cached_entry = self.httpcache._http_cache_get(method, url)
        if cached_entry:
            # 翻转:命中响应缓存,把入口记的 miss 撤回、改记 hit。
            self.http_cache_misses -= 1
            self.http_cache_hits += 1
            logger.debug("HTTP cache hit %s %s", method, url)
            await self._write_cached_response(writer, cached_entry['status_code'], cached_entry['reason_phrase'],
                                       cached_entry['headers'], cached_entry['content'])
            return

        # 1.2) 本地白名单强制直连:命中(local_direct_domains)的目标强制本机直连,
        #      不经任何远端代理——本机/内网服务不被全局 3s 转发超时掐断。失败回
        #      502 不绕远端(用户决策)。位置在缓存检查之后、在途聚合注册之前:
        #      (a) 白名单请求仍可命中直连写入的缓存; (b) 白名单请求不注册在途
        #      聚合,waiter 也不会落入 _dispatch_single 的远端竞速(跨路径坑)。
        if self._host_in_local_direct(domain):
            self.local_direct_hits += 1
            logger.debug("local-direct HTTP %s %s", method, url)
            await self._forward_local_direct_http(writer, method, url, hdrs, body, domain, client_ip)
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
        agg_key = self.httpcache._http_cache_key(method, url)
        agg_fut = None
        if method == 'GET':
            existing = self.httpcache._inflight_futures.get(agg_key)
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
                self.httpcache._inflight_futures[agg_key] = agg_fut
        try:
            await self._dispatch_single(writer, method, url, hdrs, body, domain,
                                        proto='http', client_ip=client_ip)
        finally:
            # 仅 GET 且本请求持有在途 Future 时 resolve(成功→结果,失败→None 让
            # waiter 自行竞速),并从在途表移除。若上方缓存/聚合命中则本请求不
            # 持有 Future,此为空操作。非 GET 永远不进聚合表,同样为空操作。
            if agg_fut is not None:
                self.httpcache._inflight_futures.pop(agg_key, None)
                if not agg_fut.done():
                    # 从响应缓存取刚写入的条目作为聚合结果(内容可能超
                    # STREAM_CACHE_LIMIT 未入缓存 → 无条目 → 回 None,waiter 自行竞速)。
                    entry = self.httpcache._http_cache_get(method, url)
                    if entry is not None:
                        agg_fut.set_result((entry['status_code'], entry['reason_phrase'],
                                            entry['headers'], entry['content']))
                    else:
                        agg_fut.set_result(None)

    def _observe_single_send(self, client_ip: str, domain: str, target: str, pid: str,
                             perf_t0: float) -> None:
        """慢单发采样:单发命中"发起到首字节"耗时超阈值即记一条带 IP 的日志。

        用 client_ip 归因慢/打不开的唯一锚点(成功请求路径不打 IP 日志)。仅对
        粘性/域名缓存命中的真实单发观测(竞速赢家不在此);失败已在调用方抛出让
        调用方回退,此处只观测成功的慢单发。默认阈值 0=关闭,零行为变化。
        """
        if self.single_send_slow_log_ms <= 0:
            return
        elapsed_ms = (time.perf_counter() - perf_t0) * 1000.0
        if elapsed_ms >= self.single_send_slow_log_ms:
            self.single_send_slow_logged += 1
            logger.info("slow single send client=%s domain=%s target=%s pid=%s ttfb=%.1fms "
                        "(threshold=%sms)", client_ip or "-", domain, target, pid,
                        elapsed_ms, self.single_send_slow_log_ms)

    def _observe_single_send_failure(self, client_ip: str, domain: str, target: str,
                                     pid: str, perf_t0: float, err: BaseException) -> None:
        """慢单发失败采样:单发失败(建连超时/握手失败等)耗时超阈值也记一条带 IP 日志。

        与 _observe_single_send 互补——成功路径观测不到建连失败型卡顿(失败抛出让
        调用方回退),而生产实测卡顿恰多为"某被钉代理 egress→源站建连/握手偶发超时
        (10s+)"。按同一 single_send_slow_log_ms 阈值观测,计入独立计数器
        single_send_fail_logged,opt.log 可区分"成功但慢"与"建连失败卡顿"两类。
        默认阈值 0=关闭,零行为变化。
        """
        if self.single_send_slow_log_ms <= 0:
            return
        elapsed_ms = (time.perf_counter() - perf_t0) * 1000.0
        if elapsed_ms >= self.single_send_slow_log_ms:
            self.single_send_fail_logged += 1
            err_name = type(err).__name__
            logger.info("slow single send FAILED client=%s domain=%s target=%s pid=%s "
                        "elapsed=%.1fms err=%s: %s (threshold=%sms)",
                        client_ip or "-", domain, target, pid, elapsed_ms, err_name,
                        str(err)[:120], self.single_send_slow_log_ms)

    async def _forward_single(self, writer, method: str, url: str, hdrs: dict, body, domain: str,
                             pid: str | None = None, instantiated=None, sticky: bool = False,
                             client_ip: str = ""):
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
                # 慢单发采样:测"发起到首字节"耗时。失败抛出让调用方回退(不观测)。
                _perf_t0 = time.perf_counter()
                _pid, method, url, resp, client = await self._try_http(
                    pid, self.proxy_store.proxy_url(pid), method, url, hdrs, body, domain)
                self._observe_single_send(client_ip, domain, url, pid, _perf_t0)
            except Exception as e:
                # 慢单发失败采样:建连/首字节失败超阈值也记带 IP 的日志(建连失败型
                # 卡顿是成功观测的盲区),再抛出让调用方回退到竞速。
                self._observe_single_send_failure(client_ip, domain, url, pid, _perf_t0, e)
                raise
        else:
            pid, resp = instantiated
        # 单发成功 → 记一次(单发失败回退竞速的不算);竞速赢家路径(instantiated)
        # 不计——竞速命中率只统计"未竞速即命中"的单发。
        if instantiated is None:
            if sticky:
                self.sticky.sticky_cache_hits += 1
            else:
                self.domain_cache_hits += 1
        try:
            buffered = await self._stream_upstream_response(writer, resp, method, url)
            if buffered is not None and resp.status_code in CACHEABLE_STATUS:
                self.httpcache._http_cache_set(method, url, resp.status_code, resp.reason_phrase,
                                                list(resp.headers.multi_items()), buffered)
            return resp.status_code
        finally:
            # 无论 _stream_upstream_response 是否抛 BaseException,都释放流式 resp 及其
            # 池化连接——否则异常路径会泄漏 httpx 连接池连接(每代理上限 200)。
            try:
                await resp.aclose()
            except Exception:
                pass

    async def _connect_single_send(self, *, pid: str, target: str, domain: str,
                                  client_ip: str = "") -> tuple:
        """CONNECT 单发隧道的一次尝试:建连 → 观测 → 收尾,失败抛出。

        折叠 _handle_connect 中 粘性单发 / 域名缓存单发 两处逐字重复内联体的
        公共部分。外包一层 try/except——慢单发失败采样(建连失败型卡顿归因)在
        helper 内部调用 _observe_single_send_failure 后再抛出,保证 _perf_t0
        覆盖整个单发尝试(与旧内联体同一语义)。

        除失败抛异常外,成功仅返回 (up_reader, up_writer, proxy_host, proxy_port,
        is_tunnel) 元组——**不在此处写 200/透传**:CONNECT 成功即长连接,若在
        helper 内 await 到 _relay_tunnel 返回才记账(TTL/命中/探路),长隧道下
        会迟迟不记;故成功账簿与 200-透传留在统一 _dispatch_single 决策链完成
        (relay 的 proxy_host/proxy_port 用返回值,不反查 proxy_store 双路径)。
        """
        proxy = None if pid == 'local' else self.proxy_store.get(pid)
        try:
            # 慢单发采样:测"发起到拿到 CONNECT 200"耗时(失败抛出让调用方驱逐/回退,不观测)。
            _perf_t0 = time.perf_counter()
            if proxy is None:
                _pid, up_reader, up_writer = await self._try_tunnel(pid, target, None, None, None)
            else:
                _pid, up_reader, up_writer = await self._try_tunnel(pid, target, proxy.host, proxy.port, proxy.auth)
                # CONNECT 目标半预连接(P2):单发命中说明该 target 高频,后台预热以下
                # 一条到上游代理的 TCP(不阻塞本请求)。预握手升级:把 proxy.auth 交给
                # pools,开启 prehandshake 时额外自建 TCP + CONNECT 预握手一条(库存进
                # established 池),否则只建 TCP 进 target 池(现状)。
                self._spawn_target_prewarm(proxy.host, proxy.port, target,
                                           proxy_auth=proxy.auth)
            self._observe_single_send(client_ip, target, target, pid, _perf_t0)
            # 请求簇预测预热:单发命中即 target 高频,记入客户端窗口(windows 关闭时
            # 学习全局共现图;开启新窗口时预测同簇 co-target 预建)。
            self.cluster.observe(client_ip, target, pid)
            return (up_reader, up_writer,
                    proxy.host if proxy else None,
                    proxy.port if proxy else None)
        except Exception as e:
            # 慢单发失败采样:建连/握手失败超阈值记带 IP 日志(建连失败型卡顿是成功观测
            # 盲区),再抛出让调用方驱逐回退竞速。
            self._observe_single_send_failure(client_ip, target, target, pid, _perf_t0, e)
            raise

    async def _forward_local_direct_http(self, writer, method: str, url: str, hdrs: dict, body,
                                         domain: str, client_ip: str = ""):
        """本地白名单目标强制本机直连(HTTP):内联 _try_http(relaxed=True)+流式转发+写缓存。

        由 _handle_http_request 在命中白名单时调用(拦截点在缓存检查之后、在途
        聚合注册之前)。直连成功流式转发并按 CACHEABLE_STATUS 写响应缓存(本地
        静态资源/RSC 可缓存);失败直接回 502 不绕远端(用户决策:白名单目标若
        绕远端又会被全局 3s 掐断)。不进入 sticky/域名缓存/竞速三层——白名单即
        显式授权直连,不走远端代理,也不计 domain_cache_hits/sticky 命中。
        """
        resp = None
        try:
            _perf_t0 = time.perf_counter()
            _pid, _m, url, resp, _c = await self._try_http(
                'local', None, method, url, hdrs, body, domain, relaxed=True)
            self._observe_single_send(client_ip, domain, url, 'local', _perf_t0)
            buffered = await self._stream_upstream_response(writer, resp, method, url)
            if buffered is not None and resp.status_code in CACHEABLE_STATUS:
                self.httpcache._http_cache_set(method, url, resp.status_code, resp.reason_phrase,
                                                list(resp.headers.multi_items()), buffered)
            return resp.status_code
        except Exception as e:
            # local 直连失败:不 record_failure(pid=='local' 既有约定,见 _try_http),
            # 直接 502 且不绕远端(用户决策)。
            self.local_direct_failures += 1
            logger.error("local-direct FAILED client=%s domain=%s target=%s err=%s",
                         client_ip or '-', domain, url, type(e).__name__)
            try:
                await self._write_cached_response(writer, 502, 'Bad Gateway',
                                                  {'Content-Type': 'text/plain'}, b'Bad Gateway')
            except Exception:
                pass
        finally:
            if resp is not None:
                try:
                    await resp.aclose()
                except Exception:
                    pass

    async def _dispatch_single(self, writer, method: str, url: str, hdrs: dict, body,
                               domain_key: str, *, proto: str = 'http', target: str = '',
                               client_reader=None, client_writer=None, client_ip: str = ""):
        """统一 HTTP/CONNECT 决策链 + 单发/竞速 + proto-specific 收尾。

        P3#8:把 HTTP 的旧决策链与 CONNECT 的 _handle_connect 决策部分合并为
        一份"选代理 → 执行 → 收尾"编排,消除两段几乎同构的复制。

        决策优先级(两者一致):会话粘性单发 → 域名缓存单发 → 竞速 → 兜底竞速 → 502。
        粘性/缓存命中失败逐级回退;竞速赢家回填域名缓存 meta(_record_win_meta 收进
        race 分支,统一点)+ 粘性表。

        proto 分派差异:
        - HTTP: 收尾 _forward_single(流式转发+写缓存);命中计数 sticky_cache_hits/
          domain_cache_hits 在 _forward_single 内自增。返回 status。
        - CONNECT: 收尾 _connect_established + _relay_tunnel(双向透传);慢单发观测
          在 _connect_single_send 内完成,_perf_t0 覆盖整个单发尝试。成功账簿
          (sticky_cache_hits/_bump_sticky/_spawn_sticky_probe/_record_sticky)与 200-
          透传在此完成。返回 None(隧道结束即返回)。
        """
        # 1) 会话粘性:同一客户端+domain 复用上次胜出的代理单发(滑动 TTL)。
        #    失败则驱逐该条目,回落到域名缓存/竞速。HTTP 单发 5xx 也驱逐(A2);
        #    CONNECT 隧道无状态码,跳过该专有分支。
        skip_domain_cache = False
        if self.sticky.stickiness_enabled:
            sticky_pid = self.sticky._get_sticky_proxy(client_ip, domain_key)
            if sticky_pid:
                try:
                    if proto == 'http':
                        status = await self._forward_single(
                            writer, method, url, hdrs, body, domain_key, sticky_pid, sticky=True,
                            client_ip=client_ip)
                        if status is not None and status >= 500:
                            self.sticky._evict_sticky(client_ip, domain_key)
                        else:
                            self.sticky._bump_sticky(client_ip, domain_key, sticky_pid)
                            # 杠杆A:粘性命中后台探路——竞争代理显著更快则驱逐(不阻塞单发)。
                            self._spawn_sticky_probe(client_ip, domain_key, sticky_pid)
                        return status
                    # CONNECT:成功账簿 + 200 + 透传。隧道为长连接,须在 _relay_tunnel
                    # 之前记账;helper 成功已观测、失败已观测并抛出。
                    up_reader, up_writer, ph, pp = await self._connect_single_send(
                        pid=sticky_pid, target=target, domain=domain_key, client_ip=client_ip)
                    logger.debug("proxy %s sticky hit CONNECT %s", sticky_pid, target)
                    self.sticky.sticky_cache_hits += 1
                    self.sticky._bump_sticky(client_ip, domain_key, sticky_pid)
                    # 杠杆A:粘性命中后台探路——竞争代理显著更快则驱逐(不阻塞单发)。
                    self._spawn_sticky_probe(client_ip, domain_key, sticky_pid)
                    await self._connect_established(client_writer, up_writer)
                    await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                             ph, pp, target)
                    return None
                except Exception:
                    # 慢单发失败采样已在 _forward_single(HTTP)/_connect_single_send
                    # (CONNECT)内部观测并抛出,此处只驱逐回退下一级。
                    logger.debug("sticky proxy %s failed for %s", sticky_pid, domain_key)
                    self.sticky._evict_sticky(client_ip, domain_key)
            elif self.sticky._sticky_recheck_due(client_ip, domain_key):
                # B2:探路重评估到期——驱逐并跳过域名缓存,直接竞速换新赢家。
                self.sticky._evict_sticky(client_ip, domain_key)
                skip_domain_cache = True

        # 2) 域名缓存:用上次胜出的代理单发,失败则回退到竞速。成功时也回填
        #    粘性表:粘性可能因上一轮 redispatch 被驱逐,而域名缓存仍有效。
        #    若不回填,该客户端+domain 会一直丢粘性直到域名缓存过期。
        if not skip_domain_cache:
            cached_pid = self._get_fresh_proxy(domain_key)
            if cached_pid:
                try:
                    if proto == 'http':
                        result = await self._forward_single(
                            writer, method, url, hdrs, body, domain_key, cached_pid,
                            client_ip=client_ip)
                        self.sticky._record_sticky(client_ip, domain_key, cached_pid)
                        return result
                    up_reader, up_writer, ph, pp = await self._connect_single_send(
                        pid=cached_pid, target=target, domain=domain_key, client_ip=client_ip)
                    logger.debug("proxy %s cache hit CONNECT %s", cached_pid, target)
                    self.sticky._record_sticky(client_ip, domain_key, cached_pid)
                    await self._connect_established(client_writer, up_writer)
                    await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                             ph, pp, target)
                    return None
                except Exception:
                    logger.debug("cached proxy %s failed for %s", cached_pid, domain_key)

        # 3) 竞速:首批并行 max_retries 个代理,全失败且还有剩余则对剩余再竞速。
        #    排序域名级(ordered_for_domain):该 domain 快代理进首批,而非全局
        #    EWMA 污染下被排到补发位置。策略路由(P1):按目标 domain 收窄候选集。
        #
        #    HTTP/CONNECT 竞速的差异集中在 4 点:占位形状(pid vs (pid,target))、
        #    cleanup(HTTP 流式 vs CONNECT 隧道归还)、错峰 kwargs(HTTP 带方法参数
        #    建 _try_http task;CONNECT 无需但 _race_staggered 默认 "" 亦可)、
        #    胜者收尾。统一后用 proto 分派选择,消除 duplicates。
        proxies = self.selector.ordered_for_domain(domain_key)
        if self._policies:
            proxies = self._policy_candidate_pids(domain_key, proxies)
        if not proxies and not self.enable_local_racing:
            await self._write_cached_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')
            return None

        # 计数:进入竞速(首批)。兜底批单独再 +1,故 invocations 可能 > 请求数。
        # CONNECT 不记 racing_invocations(与旧 _handle_connect 一致,经 /metrics
        # 仅统计 HTTP 竞速),HTTP 记。
        if proto == 'http':
            self.racing_invocations += 1

        if proto == 'http':
            if self.stagger_start:
                initial_places, remaining = self._prep_http(proxies, domain_key)
                winner = await self._race_staggered(
                    initial_places + remaining, cleanup=self._cleanup_http_result,
                    initial=len(initial_places), interval=self.stagger_interval,
                    method=method, url=url, headers=hdrs, body=body, domain=domain_key)
            else:
                places = self._build_racing_tasks_http(proxies, domain_key)
                tasks = {self._make_race_task(p, method, url, hdrs, body, domain_key)
                         for p in places}
                winner = await self._race(tasks, cleanup=self._cleanup_http_result)
                # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
                if not winner and len(proxies) > self.max_retries:
                    self.racing_invocations += 1
                    remaining = proxies[self.max_retries:]
                    places = self._build_racing_tasks_http(remaining)
                    tasks = {self._make_race_task(p, method, url, hdrs, body, domain_key)
                             for p in places}
                    winner = await self._race(tasks, cleanup=self._cleanup_http_result)
        else:  # CONNECT
            race_cleanup = functools.partial(self._cleanup_tunnel_result, target=target)
            if self.stagger_start:
                initial_places, remaining = self._prep_connect(proxies, target)
                winner = await self._race_staggered(
                    initial_places + remaining, cleanup=race_cleanup,
                    initial=len(initial_places), interval=self.stagger_interval)
            else:
                places = self._build_racing_tasks_connect(proxies, target)
                tasks = {self._make_race_task(p, '', '', None, None) for p in places}
                winner = await self._race(tasks, cleanup=race_cleanup)
                # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
                if not winner and len(proxies) > self.max_retries:
                    remaining = proxies[self.max_retries:]
                    places = self._build_racing_tasks_connect(remaining, target)
                    tasks = {self._make_race_task(p, '', '', None, None) for p in places}
                    winner = await self._race(tasks, cleanup=race_cleanup)

        if winner:
            if proto == 'http':
                pid, method, url, resp, client = winner
                logger.debug("proxy %s racing win %s %s", pid, method, url)
                # 仅赢家更新域名缓存 meta 与会话粘性表(败者只记了尝试统计,不会被覆写)。
                self._record_win_meta(domain_key, pid)
                self.sticky._record_sticky(client_ip, domain_key, pid)
                return await self._forward_single(
                    writer, method, url, hdrs, body, domain_key, instantiated=(pid, resp))
            pid, up_reader, up_writer = winner
            logger.debug("proxy %s racing CONNECT to %s for client %s",
                         pid, target,
                         client_writer.get_extra_info('peername') if client_writer else '')
            self._record_win_meta(domain_key, pid)
            self.sticky._record_sticky(client_ip, domain_key, pid)
            # CONNECT 目标半预连接(P2):竞速胜出说明该 target 高频且最优代理已
            # 确定,后台为 (proxy, target) 预热下一条到上游代理的 TCP(不阻塞本
            # 请求)。注意 pid 可能是 'local'(enable_local_racing 直连),此时无
            # 上游代理可预热,交由 _spawn_target_prewarm 内部跳过。
            win_proxy = None
            if pid != 'local':
                win_proxy = self.proxy_store.get(pid)
                if win_proxy is not None:
                    # 预握手升级:竞速胜出同单发分支,把 win_proxy.auth 交给 pools
                    # 预握手(竞速天然高频 target,库存直接进 established 池)。
                    self._spawn_target_prewarm(win_proxy.host, win_proxy.port, target,
                                               proxy_auth=win_proxy.auth)
            # 请求簇预测预热:竞速胜出同样记入客户端窗口(页面的每一跳都是一簇一员)。
            self.cluster.observe(client_ip, target, pid)
            await self._connect_established(client_writer, up_writer)
            await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                     win_proxy.host if win_proxy is not None else None,
                                     win_proxy.port if win_proxy is not None else None,
                                     target)
            return None

        # 4) 全失败:HTTP 写 502;CONNECT 写 502 并关闭客户端连接(与旧内联体一致)。
        #    CONNECT 仍记入客户端窗口(浏览器可能再次连接;pid=None 使该目标不进
        #    预测,但簇成员关系仍被学习——旧 _handle_connect 语义,HTTP 不记)。
        if proto == 'tunnel':
            self.cluster.observe(client_ip, target, None)
        logger.error("all proxies failed for %s request %s", proto, target or url)
        if proto == 'http':
            await self._write_cached_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')
        else:
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
        return None

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
        streamed = 0
        try:
            async for chunk in resp.aiter_raw():
                if buffering:
                    if len(buffered) + len(chunk) <= self.httpcache._stream_cache_limit:
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
                if use_chunked is False:
                    streamed += len(chunk)
            if use_chunked and not client_disconnected:
                try:
                    client_writer.write(b"0\r\n\r\n")
                    await client_writer.drain()
                except (BrokenPipeError, ConnectionError, OSError):
                    client_disconnected = True
        except Exception:
            client_disconnected = True
            # aiter_raw 抛异常(最常见:上游 Content-Length 未流式完就断开的截断响应,
            # httpcore 的 RemoteProtocolError)。客户端已拿到 200 头部,无法撤回,收到
            # 的是截断 body——交由下方统一长度校验暴露,失败观测由竞速 fail 回调记录。
        # 截断响应检测(#7):上游 Content-Length 声明 N 但实际流式字节 <N——循环正常
        # 走完或 aiter_raw 提前抛异常都会在这里比对。客户端已收到响应头,无法撤回,
        # 只能记 warn(URL + 承诺/实际长度)并关闭上游连接,暴露问题。客户端断开不
        # 在此列:断开后循环仍排空上游 body,streamed 会到 CL,不误报。
        if use_chunked is False and streamed != int(upstream_cl):
            logger.warning("truncated upstream response for %s: content-length=%s, streamed=%d",
                           url, upstream_cl, streamed)
            try:
                await resp.aclose()
            except Exception:
                pass
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
            # 统一归还判定:抽到 _maybe_return_established,与竞速败者清理共用。
            await self._maybe_return_established(up_writer, up_reader, proxy_host, proxy_port, target)

    async def _maybe_return_established(self, up_writer, up_reader,
                                        proxy_host, proxy_port, target) -> bool:
        """隧道/竞速败者结束时的统一"是否归还 _established_pool"判定。

        判定条件(抽取自 _relay_tunnel finally):conn_pool_enabled 且
        established_reuse 且经上游代理(proxy_host 非 None)且上游连接未关闭
        且无残留缓冲(_pipe 的 close_writer=False 透传路径和竞速败者都只读过
        CONNECT 响应头,缓冲天然干净)且预算/单键 cap 未超。归还成功返回 True;
        否则关闭连接并返回 False。_relay_tunnel 与 _cleanup_tunnel_result 共用,
        保证"正常隧道结束"与"竞速败者"同一套归还语义。
        """
        can_reuse = (self.pools.conn_pool_enabled and self.pools.conn_pool_established_reuse
                     and proxy_host is not None and target is not None
                     and not up_writer.is_closing())
        # 严格验证:上游残留缓冲 → 连接已脏,不归还(宁可不复用也不污染)。
        if can_reuse and up_reader._buffer and len(up_reader._buffer) > 0:
            logger.info("established pool NOT-RETURN %s via %s:%s (dirty buffer)",
                        target, proxy_host, proxy_port)
            can_reuse = False
        if can_reuse:
            key = f"{proxy_host}:{proxy_port}|{target}"
            # 预算/cap 检查:established 池计入全局 conn_pool_total(与另两池同口径),
            # 单键再受 _ESTABLISHED_KEY_CAP 上限。超限则关闭不复用——宁可这次不省
            # 建连也不让 fd 无界增长。三池快照统一用 pools._total_idle(见 #14)。
            if self.pools._total_idle() >= self.pools.conn_pool_total \
                    or len(self.pools._established_pool.get(key, [])) >= _ESTABLISHED_KEY_CAP:
                logger.info("established pool SKIP-RETURN %s via %s:%s (over budget/cap, returned=%d)",
                            target, proxy_host, proxy_port, self.pools.established_pool_returned)
                can_reuse = False
        if can_reuse:
            self._set_pool_keepalive(up_writer)
            up_writer._conn_pool_created = time.monotonic()
            # 打 established 标签:过期清理按此选独立 idle 超时(established 库存
            # 复访频率低,统一用通用池超时会在复访前被清——见 pools._pool_prune)。
            up_writer._established_pooled = True
            self.pools._established_pool.setdefault(key, []).append((up_reader, up_writer))
            self.pools.established_pool_returned += 1
            logger.info("established pool RETURN %s via %s:%s (returned=%d)",
                        target, proxy_host, proxy_port, self.pools.established_pool_returned)
        else:
            try:
                up_writer.close()
                await asyncio.wait_for(up_writer.wait_closed(), timeout=0.5)
            except Exception:
                pass
        return can_reuse

    async def _local_direct_connect(self, target: str, client_reader: asyncio.StreamReader,
                                    client_writer: asyncio.StreamWriter, client_ip: str = ""):
        """本地白名单目标强制本机直连(CONNECT):relaxed 直连 + 双向透传。

        由 _handle_connect 在命中白名单时调用。_try_tunnel('local', ..., relaxed=True)
        用 local_direct_timeout_sec(默认 10s)放宽建连/读响应,本机回环不被全局 3s
        掐断。透传复用 _relay_tunnel(proxy_host=None → 不归还 established 池不预热,
        正确)。失败回 502 不绕远端(用户决策:白名单目标若绕远端又会被全局 3s 掐)。
        """
        up_reader = up_writer = None
        try:
            _perf_t0 = time.perf_counter()
            _pid, up_reader, up_writer = await self._try_tunnel(
                'local', target, None, None, None, relaxed=True)
            self._observe_single_send(client_ip, self._try_tunnel_host(target), target, 'local', _perf_t0)
            await self._connect_established(client_writer, up_writer)
            await self._relay_tunnel(client_reader, up_writer, up_reader, client_writer,
                                     None, None, target)
        except Exception as e:
            self.local_direct_failures += 1
            logger.error("local-direct CONNECT FAILED client=%s target=%s err=%s",
                         client_ip or '-', target, type(e).__name__)
            try:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await client_writer.drain()
            except Exception:
                pass
            if up_writer is not None:
                try:
                    up_writer.close()
                    await asyncio.wait_for(up_writer.wait_closed(), timeout=0.5)
                except Exception:
                    pass

    async def _handle_connect(self, target: str, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter, client_ip: str = ""):
        """处理 CONNECT 请求:建立到 target 的隧道并双向透传数据。

        决策已统一进 _dispatch_single(proto='tunnel'):会话粘性命中 → 单发隧道;
        域名缓存命中 → 单发隧道;否则竞速(首批 max_retries,失败对剩余兜底)。
        胜出后回 200,用两个反向 _pipe 双向透传,任一方向结束即关闭。全失败
        回写 502 并关闭客户端连接。本函数只保留 CONNECT 专有的白名单强制直连
        拦截(命中 local_direct_domains 的目标直接 local 直连,不经任何远端代理,
        失败回 502 不绕远端——用户决策)。认证已在 handle_client 完成。
        """
        # 0) 本地白名单强制直连(CONNECT 无在途聚合,位置无跨路径约束)。
        host = self._try_tunnel_host(target)
        if self._host_in_local_direct(host):
            self.local_direct_hits += 1
            logger.debug("local-direct CONNECT %s", target)
            await self._local_direct_connect(target, client_reader, client_writer, client_ip)
            return
        await self._dispatch_single(
            None, '', '', None, None, target, proto='tunnel', target=target,
            client_reader=client_reader, client_writer=client_writer, client_ip=client_ip)

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
        # CONNECT 预热池(P1):refill_interval>0 时由 pools 启动后台补充循环。
        # (#14 拆 pools.py 后 refill 循环 task 归 ConnectionPools 自管,Router 不持 task)
        self.pools.start()
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
        # (#14 pools 自管 refill 循环 task,stop() 收敛在此)
        await self.pools.stop()
        # 请求簇预测预热:停用即清空瞬态窗口与共现图(簇是瞬态观察,不落盘)。
        if self.cluster.enabled:
            self.cluster.reset()
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


    # ── #14 白名单转发(pools/httpcache/sticky)──────────────
    # ConnectionPools(self.pools) / HttpCache(self.httpcache) / StickyCache(self.sticky)
    # 持有各自子系统的状态/计数器/配置/方法;Router 用 __getattr__/__setattr__ 把
    # 白名单成员转发到对应协作类,使热路径与测试的 self._conn_pool /
    # self._http_cache_set() / self._sticky_cache 等引用原样解析到协作对象,Router
    # 编排代码零改动。三张名单两两不相交;未在名单内的名字(如 prewarm/
    # _set_nodelay/handle_client 等)走正常属性查找。构造期协作类尚未创建时的
    # 转发名赋值走 __setattr__ 守卫(对应协作名不在 self.__dict__ → super().__setattr__)。

    _POOL_FORWARD = frozenset({'_conn_pool', '_target_pool', '_established_pool',
        'conn_pool_enabled', 'conn_pool_per_proxy', 'conn_pool_total', 'conn_pool_idle_timeout',
        'conn_pool_refill_interval', 'conn_pool_refill_target', 'conn_pool_connect_timeout',
        'conn_pool_target_prewarm', 'conn_pool_established_reuse', 'conn_pool_prehandshake',
        'conn_pool_refill_pause_minutes', 'conn_pool_refill_pause_activity_window',
        'conn_pool_refill_pause_min_requests', 'cluster_pool_idle_timeout',
        'prehandshake_enabled', 'established_pool_prehandshook', 'established_pool_prewarm_failed',
        'prehandshake_throttled_skips',
        'conn_pool_creates', 'conn_pool_hits', 'conn_pool_misses', 'conn_pool_expired',
        'target_pool_creates', 'target_pool_hits', 'target_pool_misses', 'target_pool_expired',
        'cluster_pool_creates', 'cluster_pool_hits', 'cluster_pool_expired',
        'cluster_pool_timing_miss', 'cluster_pool_bucket_miss', 'cluster_pool_consumed_expired',
        '_target_pool_cluster_ever',
        'target_prewarm_dispatched', 'target_prewarm_success', 'target_prewarm_failed',
        'established_pool_hits', 'established_pool_misses', 'established_pool_expired',
        'established_pool_returned', 'connect_new_conns',
        '_last_request_activity', '_activity_timestamps',
        '_record_request_activity', '_conn_pool_idle', '_conn_pool_refill',
        '_target_pool_refill', '_pool_prune', '_conn_pool_close_all', '_established_alive',
        '_conn_pool_peek', '_target_pool_peek', '_established_pool_peek'})

    _CACHE_FORWARD = frozenset({'_http_cache', '_http_cache_ttl', '_http_cache_max_entries',
        '_http_cache_max_bytes', '_stream_cache_limit', '_http_cache_bytes',
        'http_cache_evictions', '_http_cache_domain_index', '_inflight_futures',
        'enable_http_cache',
        '_http_cache_key', '_http_cache_get', '_http_cache_remove',
        '_http_cache_set', '_http_cache_invalidate'})

    _STICKY_FORWARD = frozenset({'stickiness_enabled', 'stickiness_ttl', 'stickiness_recheck_hits',
        'stickiness_max_entries', '_sticky_cache', 'sticky_cache_hits',
        'sticky_evictions', 'sticky_slow_probes',
        'stickiness_probe_interval_sec', 'stickiness_probe_fanout',
        'sticky_probe_due', '_sticky_probe_last',
        'sticky_probes_fired', 'sticky_probe_evictions',
        'get_sticky_cache', '_sticky_key', '_evict_sticky_key',
        '_get_sticky_proxy', '_sticky_recheck_due', '_sticky_degrade_due',
        '_record_sticky', '_bump_sticky', '_evict_sticky', '_evict_oldest_sticky',
        '_prune_sticky'})
        # 请求簇预测预热(#新增):ClusterGraph(self.cluster) 持有窗口/图/计数,
        # 白名单成员转发到协作对象,观察点与快照引用原样解析。
    _CLUSTER_FORWARD = frozenset({
        'cluster_windows_learned', 'cluster_predictions', 'cluster_prewarm_spawned',
        'cluster_bucket_spawns', 'cluster_fanout', 'cluster_proxy_fanout',
        'cluster_probe_decay_sec',
        '_active_windows', '_cooccur', '_last_predict',
        'observe', 'maybe_predict', 'prune', 'reset', 'graph_size', 'get_cluster_cache'})

    def __getattr__(self, name):
        # 仅在实例属性/类属性都未命中时被调用(正常查找失败);白名单成员转发到
        # 对应协作类(pools/httpcache/sticky/cluster)。
        if name in Router._POOL_FORWARD:
            return getattr(self.pools, name)
        if name in Router._CACHE_FORWARD:
            return getattr(self.httpcache, name)
        if name in Router._STICKY_FORWARD:
            return getattr(self.sticky, name)
        if name in Router._CLUSTER_FORWARD:
            return getattr(self.cluster, name)
        raise AttributeError(f"{type(self).__name__} has no attribute {name!r}")

    def __setattr__(self, name, value):
        # 构造期协作类(self.pools/self.httpcache/self.sticky/self.cluster)尚未存在时
        # 走正常赋值;建好后白名单成员 set 到对应协作类(如 sticky_cache_hits += 1
        # 读转发 get + set 转发到 sticky,不重绑 Router 上的名字)。
        if name in Router._POOL_FORWARD and 'pools' in self.__dict__:
            setattr(self.pools, name, value)
            return
        if name in Router._CACHE_FORWARD and 'httpcache' in self.__dict__:
            setattr(self.httpcache, name, value)
            return
        if name in Router._STICKY_FORWARD and 'sticky' in self.__dict__:
            setattr(self.sticky, name, value)
            return
        if name in Router._CLUSTER_FORWARD and 'cluster' in self.__dict__:
            setattr(self.cluster, name, value)
            return
        super().__setattr__(name, value)

