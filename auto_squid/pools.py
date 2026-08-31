"""CONNECT 上游 TCP 预热池子系统(从 Router 拆出,见 #14 pools.py)。

三个池统一成 `ConnectionPools` 类,共享单个全局 fd 预算(conn_pool_total):

- `_conn_pool`(第一阶段通用池):每上游代理维护少量空闲 TCP,CONNECT 请求到来
  时优先取已连接到代理的 socket 再发 CONNECT target,省掉"本机→上游代理"的
  建连 TTFB。
- `_target_pool`(第二阶段目标半预连接):命中域名缓存/粘性或竞速胜出的高频
  CONNECT target 在后台提前建立"到上游代理"的 TCP(不提前 CONNECT 到目标),
  按 (proxy, target) 键区分,下一次同 target 命中时直接复用,节省"取到通用池
  但 target 不同"时的 CONNECT 前建连。
- `_established_pool`(第三阶段已建握手隧道复用):隧道结束若连接干净(上游无
  残留缓冲)则归还而非关闭,下次同 (proxy, target) 直接复用已发 CONNECT 且
  收到 200 的连接、跳过握手。归还受全局预算 + 单键天花板(_ESTABLISHED_KEY_CAP)
  约束;复用前做廉价活性探测(_established_alive),死/脏连接回落到新建。

状态全部由事件循环单线程读写。prewarm 后台任务注册进 Router._running_tasks
(Router._spawn_target_prewarm 持有),本类只自管 refill 循环 task。
"""

import asyncio
import collections
import logging
import time
from typing import Optional, Tuple

logger = logging.getLogger(__name__)


def _discard_conn(writer: asyncio.StreamWriter):
    """fire-and-forget 关闭一条废弃连接(同步路径不能用 await 时的清理)。

    已握手池复用前发现脏缓冲 / 死连接 → 丢弃。close 放入后台 task 执行,不阻塞
    取用热路径;3.12 的 wait_closed() 严格等对端 FIN,用 0.5s 超时保护。

    #14 顺带修复:原模块方法以裸名 `_discard_conn(writer)` 被调用(router.py
    _established_pool_peek / _try_tunnel),Python 类内裸名不查类作用域 → 运行到
    即 NameError。抽成模块级函数,裸名调用直接命中本函数。
    """
    async def _close():
        try:
            writer.close()
            await asyncio.wait_for(writer.wait_closed(), timeout=0.5)
        except BaseException:
            pass
    try:
        asyncio.create_task(_close())
    except RuntimeError:
        # 事件循环未运行(如构造期)时直接 close,不等待。
        try:
            writer.close()
        except BaseException:
            pass


# 已握手隧道池单键上限:与 target_pool 的 cap=2 对齐,防同一 (proxy, target)
# 堆太多条全双工 socket(见 #15 拆池前的 #2 修复)。
_ESTABLISHED_KEY_CAP = 2
# 复用前活性探测超时(秒):read(1) 阻塞等这一时长的数据/EOF。
_ESTABLISHED_PROBE_TIMEOUT = 0.05


class ConnectionPools:
    """三个 CONNECT 预热池 + 空闲暂停/refill 的统一状态与操作。#14 pools.py。

    Router 持有本类实例 `self.pools`,并对池成员做白名单转发(__getattr__/
    __setattr__),使 Router 内已有的 `self._conn_pool` / `self.conn_pool_creates`
    等引用原样解析到本类。prewarm 后台任务(i-call Router._spawn_target_prewarm)
    的注册仍留在 Router._running_tasks,本类不自管。
    """

    def __init__(self, proxy_store, enabled, per_proxy, total, idle_timeout,
                 refill_interval, refill_target, connect_timeout, target_prewarm,
                 established_reuse, pause_minutes, pause_silence_sec,
                 pause_activity_window, pause_min_requests,
                 idle_timeout_cluster=600.0, idle_timeout_established=None):
        self.proxy_store = proxy_store
        self.conn_pool_enabled = bool(enabled)
        self.conn_pool_per_proxy = max(1, int(per_proxy))
        self.conn_pool_total = max(1, int(total))
        self.conn_pool_idle_timeout = max(1.0, float(idle_timeout))
        # cluster 预测预建连接的独立空闲超时(默认 600s):预测预建比被动预建早建
        # 得多,统一用 conn_pool_idle_timeout(生产 180s)常在真实 co-target 到达前
        # 被清 → timing_miss。预测连接打 _cluster_prewarmed 标签,_pool_prune 按
        # 连接级标签选超时;本值独立于被动预建的空闲超时。
        self.cluster_pool_idle_timeout = max(1.0, float(idle_timeout_cluster))
        # 已建握手隧道池(established_reuse)的独立空闲超时(默认 None=跟随
        # conn_pool_idle_timeout)。竞速败者/隧道结束归还的已握手连接,复访同一
        # (proxy,target) 的频率常低于通用池取用频率,统一用通用池超时会导致
        # 归还后 90% 在复访前被清(观测 returned=133/expired=120)。独立超时
        # 让已握手库存多活一阵等复访;连接打 _established_pooled 标签,_pool_prune
        # 按连接级标签选超时(cluster > established > conn)。
        self.established_pool_idle_timeout = max(
            1.0, float(idle_timeout_established)) if idle_timeout_established else None
        self.conn_pool_refill_interval = max(0.0, float(refill_interval))
        self.conn_pool_refill_target = max(0, min(self.conn_pool_per_proxy, int(refill_target)))
        self.conn_pool_connect_timeout = max(1.0, float(connect_timeout))
        # 空闲暂停时长(分钟)。0=不启用(默认):refill/目标预热不因空闲挂起。
        self.conn_pool_refill_pause_minutes = max(0.0, float(pause_minutes))
        # [已弃用] 旧版"间隔一刀切"活动判定窗口(秒),仅对旧配置兼容(换算新
        # 窗口的保守起点)。新逻辑见下 refill_pause_activity_window。
        self.conn_pool_refill_pause_silence_sec = max(0.0, float(pause_silence_sec))
        # 活动判定窗口(秒,窗口计数):窗口内请求数 ≥ min_requests 才算"密集活动"
        # 并刷新活动时间戳。真实流量是簇(一次页面加载数秒内对多个 hostname 并发
        # CONNECT,计数 5-30),后台心跳(GitHub Desktop 的 alive.github.com /
        # Windows 的 client.wns.windows.com 等,间隔 3-10 分钟)是孤例(窗口内计数
        # 1,极少 2)——据此区分,既不误伤真实孤立请求,又免疫心跳。
        # None=未显式配置:沿用旧 silence_sec 换算(取 ~1/4 当保守起点,旧"间隔
        # ≤窗口"的密集密度远低于簇,需收紧)。0=不启用窗口计数(任意请求都刷新)。
        if pause_activity_window is None:
            if self.conn_pool_refill_pause_silence_sec > 0:
                self.conn_pool_refill_pause_activity_window = max(
                    30.0, self.conn_pool_refill_pause_silence_sec / 4.0)
            else:
                self.conn_pool_refill_pause_activity_window = 0.0
        else:
            self.conn_pool_refill_pause_activity_window = max(0.0, float(pause_activity_window))
        # 窗口阈值:窗口内请求数 ≥ 此值才算"密集活动"。≤1 退化为任意请求都刷新。
        self.conn_pool_refill_pause_min_requests = max(0, int(pause_min_requests))
        # 最近一次"密集客户端请求"到达时间(monotonic 秒)。由 _record_request_
        # activity() 在每个有效请求(通过认证的 HTTP/CONNECT 首行)上按窗口计数
        # 判定后更新。初始为当前时刻:新路由器 60 分钟内不暂停(refill 照常),
        # 与旧语义一致;进程长时间运行后,后台心跳(间隔 3-10 分钟)构不成簇,
        # 不刷新活动时间戳,自然进入空闲暂停。
        self._last_request_activity = time.monotonic()
        # 窗口计数用的请求到达时间戳队列(monotonic 秒),滑动窗口内计数 ≥ 阈值
        # 判定活动。上限 1024 防高流量下无界增长(超出后每窗口重算自动恢复)。
        self._activity_timestamps = collections.deque(maxlen=1024)
        # 第二阶段(CONNECT 目标半预连接):需 conn_pool_enabled 且显式开启。
        self.conn_pool_target_prewarm = bool(target_prewarm)
        # 已建握手隧道复用:隧道结束时连接干净则归还而非关闭,同 (proxy,target)
        # 复用跳过 CONNECT 握手。需 conn_pool_enabled 且显式开启。
        self.conn_pool_established_reuse = bool(established_reuse)
        # {proxy_url: [StreamWriter,...]} —— 空闲预热连接。只由事件循环线程读写。
        self._conn_pool: dict = {}
        self.conn_pool_creates = 0      # 累计预热建连次数
        self.conn_pool_hits = 0         # 池中取用成功次数
        self.conn_pool_misses = 0       # 取池未中需新建的次数
        self.conn_pool_expired = 0      # 空闲超时关闭次数
        self._pool_task: Optional[asyncio.Task] = None
        # {proxy_host:port|target: [StreamWriter,...]} —— 按 CONNECT target 半预
        # 连接的"到上游代理"TCP(P2,见 _target_pool_*)。命中域名缓存/粘性的
        # target 后台预热,取用时优先于第一阶段通用池。共享 fd 预算/空闲超时。
        self._target_pool: dict = {}
        self.target_pool_creates = 0    # 累计 target 半预建连次数
        self.target_pool_hits = 0       # target 预连接取用成功次数
        self.target_pool_misses = 0     # 取 target 池未中需新建的次数
        self.target_pool_expired = 0    # target 预连接空闲超时关闭次数
        # cluster 专属归因(见 _target_pool_refill 的 source 参数):池里同一把
        # 键混装着"预测预建"(cluster 提前预建)与"被动预建"(域缓存/粘性/竞速
        # 胜出后补)。给连接打上来源标签,分别记创建/命中/过期,便于算 cluster
        # 专属命中率 = cluster_pool_hits / cluster_pool_creates,并把被动预建的
        # 贡献剥离出 target_pool_hits。
        self.cluster_pool_creates = 0   # 预测预建的 target 连接数(cluster 专属)
        self.cluster_pool_hits = 0      # 取用命中中被预测预建命中的次数
        self.cluster_pool_expired = 0   # 空闲超时关闭中被预测预建关闭的次数
        # C 探针(读写点:_target_pool_peek / _target_pool_refill / _pool_prune):
        # 区分 cluster 预建空转的病因——时序没赶上 vs 代理桶不匹配。
        self.cluster_pool_timing_miss = 0  # miss:该桶有 cluster 预建但此刻全灭(时序没赶上)
        self.cluster_pool_bucket_miss = 0  # miss:该桶从未有 cluster 预建(代理桶不匹配)
        self.cluster_pool_consumed_expired = 0  # 被消费后另行空转的预建被 prune 关闭(极少数)
        # 键级"该桶是否出现过 cluster 预建"的标记(诊断读,不进 snapshot)。
        self._target_pool_cluster_ever: dict = {}
        self.target_prewarm_dispatched = 0  # 后台预热协程发起次数
        self.target_prewarm_success = 0     # 预热建连成功次数
        self.target_prewarm_failed = 0      # 预热建连失败次数
        # 已建握手隧道池(P3):{ "proxy_host:port|target": [(reader, writer)] }。
        # 存"已发 CONNECT 且收到 200"的隧道连接,隧道结束若连接干净则归还,
        # 下次同 (proxy, target) 直接复用,跳过 CONNECT 握手。与 _target_pool
        # 区分:后者是"未发 CONNECT 的裸 TCP",取用后必须重新握手。
        self._established_pool: dict = {}
        self.established_pool_hits = 0      # 已握手连接取用成功次数
        self.established_pool_misses = 0    # 取已握手池未中需新建/新建握手的次数
        self.established_pool_expired = 0   # 已握手连接空闲超时关闭次数
        self.established_pool_returned = 0  # 隧道结束归还次数
        # 观测(P1 先观测后实现):CONNECT 到上游的新建 TCP 连接计数(不含预热池
        # 命中)。供压测算 HTTPS 建链成本、验证预热池收益。
        self.connect_new_conns = 0

    # ── 建连 / 收尾 / 取用热路径 ────────────────────────────────

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
        got = self._pool_peek(self._conn_pool, f"{proxy_host}:{proxy_port}")
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

        归因:连接的来源 tag(_cluster_prewarmed,见 _target_pool_refill)随
        writer 保存;命中时若取到的恰是 cluster 预测预建的连接,额外计
        cluster_pool_hits —— 该计数 / cluster_pool_creates 即 cluster 专属
        命中率(把被动预建的贡献从 target_pool_hits 里剥离)。

        诊断探针(C):区分 cluster 预建空转的两类病因——
        - 取用 miss 且该键曾有过 cluster 预建(_target_pool_cluster_ever)
          → 时序没赶上(预建存在但真实请求到达时已被取空/prune 清理);
        - miss 且键从未出现过 cluster 预建(标记缺失)→ "代理桶不匹配"(真实胜出代理
          的桶里根本没有 cluster 预建,预建躺在别的 (proxy|target) 桶里永不命中)。
        取用命中时,给被消费的 cluster 预建连接补 `_consumed_unexpired=True` 标记,
        _pool_prune 据此把"被真实消费后置空转"与"从未被消费直接空转"分开计数。
        """
        key = f"{proxy_host}:{proxy_port}|{target}"
        got = self._pool_peek(self._target_pool, key)
        if got is None:
            self.target_pool_misses += 1
            if self._target_pool_cluster_ever.get(key, 0) > 0:
                # 时序 miss:该桶出现过 cluster 预建但此刻为空(被取走/被 prune 清)。
                self.cluster_pool_timing_miss += 1
            else:
                # 桶不匹配:该桶从未有 cluster 预建(真实请求走的代理不是预测那个)。
                self.cluster_pool_bucket_miss += 1
            logger.info("target pool MISS %s via %s:%s (misses=%d)",
                        target, proxy_host, proxy_port, self.target_pool_misses)
        else:
            self.target_pool_hits += 1
            reader, writer = got
            if getattr(writer, '_cluster_prewarmed', False):
                self.cluster_pool_hits += 1
                writer._consumed_unexpired = True
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
        got = self._pool_peek(self._established_pool, f"{proxy_host}:{proxy_port}|{target}")
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

    async def _established_alive(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> bool:
        """复用已握手连接前的廉价活性探测(不超过 _ESTABLISHED_PROBE_TIMEOUT)。

        用 read(1):asyncio 的 read(0) 会在方法开头直接返回 b"" 不碰网络(故不可做
        探测);read(1) 才真正阻塞等待数据/EOF:
        - 超时(连接活且无数据)→ 可复用(探测被 wait_for 取消,未消费任何字节,无
          状态残留,`_waiter` 置空即可);
        - 返回 b""(对端已 FIN)→ 死,不可复用;
        - 抛出(对端已 RST,reader._exception 设置)→ 死,不可复用;
        - 返回非空 1 字节(上游残余/对端推送)→ 已脏,不可复用且该字节丢失可接受
          (连接整条丢弃,宁可不复用也不污染)。

        纯半开(对端静默、无 FIN 无数据)会让 read(1) 阻塞到超时 → 误判"活",由归还
        时设的 SO_KEEPALIVE 由 OS 兜底判死(KEEPIDLE 60s 后探测)。返回 False 时
        调用方负责丢弃连接并回落新建。
        """
        try:
            data = await asyncio.wait_for(reader.read(1), timeout=_ESTABLISHED_PROBE_TIMEOUT)
        except asyncio.TimeoutError:
            return True          # 活且无数据,探测未消费任何字节
        except asyncio.CancelledError:
            raise
        except Exception:
            return False         # read 抛异常 → 对端已关/RST
        # read(1) 返回了字节或 EOF
        return False

    # ── 空闲暂停 / refill 补充 ─────────────────────────────────

    def _record_request_activity(self):
        """刷新"密集活动"时间戳(refill 空闲感知的活动信号)。

        在每个通过认证的 HTTP/CONNECT 首行上调用(见 Router.handle_client,认证
        放行后)。**活动判定为"簇度计数"**:记录请求到达时间戳,若滑动窗口
        (conn_pool_refill_pause_activity_window,默认 120s)内请求数 ≥ 阈值
        (conn_pool_refill_pause_min_requests,默认 3),视为真实活动并刷新活动
        时间戳。真实流量是簇——一次页面加载数秒内对多个 hostname 并发 CONNECT
        (计数 5-30);后台心跳(如 alive.github.com 每 3-10 分钟一次)是孤例——
        窗口内计数 1,极少 2,不达标,不会刷新时间戳,因此无法阻止空闲暂停。
        若此前处于"空闲暂停"态且本次判定为活动,则立刻解除暂停并在 logger 留一条
        INFO,便于从日志确认恢复时刻。仅在有实际客户端请求的路径调用——探活/预热/
        本机自连都不算"请求",不应恢复预热。窗口计数不启用(窗口=0 或阈值≤1)时,
        任意请求都刷新(旧行为)。
        """
        now = time.monotonic()
        window = self.conn_pool_refill_pause_activity_window
        k = self.conn_pool_refill_pause_min_requests
        # 窗口计数不启用:任意请求都刷新(向后兼容旧行为)。
        if window <= 0 or k <= 1:
            was_idle = self._conn_pool_idle()
            self._last_request_activity = now
            if was_idle:
                logger.info("conn pool refill resumed by client request (was idle >= %.0fmin)",
                            self.conn_pool_refill_pause_minutes)
            return
        # 滑动窗口计数:本请求入队,弹出窗口外的旧时间戳。
        self._activity_timestamps.append(now)
        while self._activity_timestamps and \
                now - self._activity_timestamps[0] > window:
            self._activity_timestamps.popleft()
        # 窗口内请求数 ≥ 阈值才算"密集活动"(真实页面加载成簇);心跳孤例不达标。
        if len(self._activity_timestamps) < k:
            return
        was_idle = self._conn_pool_idle()
        self._last_request_activity = now
        if was_idle:
            logger.info("conn pool refill resumed by client request (was idle >= %.0fmin)",
                        self.conn_pool_refill_pause_minutes)

    def _conn_pool_idle(self) -> bool:
        """空闲暂停判定:距上次"密集活动"已超过 conn_pool_refill_pause_minutes。

        活动判定见 _record_request_activity——后台心跳(间隔 3-10 分钟,窗口内
        计数 1-2)构不成簇,不会刷新活动时间戳,因此即使深夜每分钟都有心跳,
        只要没有"密集活动"(窗口内 ≥ 阈值个真实请求成簇)持续到来,本判定在距
        最后密集活动超过阈值后返回 True,refill/目标预热挂起。**空闲暂停只挂起
        后台预建(refill/目标预热),不卡请求路径**:真实请求照常取池/新建/复用
        已握手连接(见 Router._try_tunnel / _established_pool_peek,均不检查本判定)。
        仅当预热池开启且配置了暂停时长才可能返回 True;0(默认关闭该特性)恒
        False——refill/目标预热行为与未加本特性时完全一致,不改变默认语义。
        """
        if not self.conn_pool_enabled or self.conn_pool_refill_pause_minutes <= 0:
            return False
        return (time.monotonic() - self._last_request_activity) >= self.conn_pool_refill_pause_minutes * 60.0

    def _total_idle(self) -> int:
        """三池空闲连接总数(共享全局 fd 预算 conn_pool_total 的当前占用)。

        #14(拆 pools.py)统一 #2 三处手写预算快照(conn_pool_refill /
        target_pool_refill / relay_tunnel 归还),消除对三池 dict 的三段重复求和。
        """
        return (sum(len(v) for v in self._conn_pool.values())
                + sum(len(v) for v in self._target_pool.values())
                + sum(len(v) for v in self._established_pool.values()))

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
        # 快照当前空闲总数,防止并发补充超过全局预算(三池共享 conn_pool_total)。
        total_idle = self._total_idle()
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

    async def _target_pool_refill(self, proxy_host: str, proxy_port: int, target: str,
                                  cap: int = 2, source: str = 'passive'):
        """为某 (proxy, target) 键补充半预连接到目标水位(一次调用补到 cap)。

        只建立"到上游代理"的 TCP(不提前 CONNECT 到目标),可安全复用于同
        target。全局受 conn_pool_total 钳制,单键受 cap 钳制(默认 2 条:取走
        1 条仍留 1 条备用,降低"取走即空→周期 miss";避免为单个 target 占用
        过多 fd)。建连失败静默(下次命中再试)。单事件循环线程调用,无需加锁。
        cap 从 1 提升到 2:生产实测 cap=1 时 target_pool_hits=1 / misses=71,
        单条预热被取走即空,下一条同 target 请求只能回退 miss。

        `source` 是归因标签:'cluster' = ClusterGraph 预测预建(提前为窗口内
        尚未到达的 co-target 建连);'passive' = 域缓存/粘性命中或竞速胜出后补。
        在唯一的建连处为 writer 打上来源 tag,供 _target_pool_peek 命中与
        _pool_prune 过期分别计 cluster 专属计数(见 cluster_pool_*)。连接在此池
        中没有第二个区分标识,故 tag 必须在建连处打。
        """
        if not self.conn_pool_enabled:
            return 0
        # 空闲暂停:无请求期间不发起目标预热(省建连;已有半连接照常可复用)。
        if self._conn_pool_idle():
            return 0
        key = f"{proxy_host}:{proxy_port}|{target}"
        # 全局 fd 预算:三池共享 conn_pool_total,超限则不补。
        total_idle = self._total_idle()
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
            if source == 'cluster':
                writer._cluster_prewarmed = True
                self.cluster_pool_creates += 1
                # 键级标记:该 (proxy|target) 桶出现过 cluster 预建。取用该键 miss
                # 时据此分诊"时序没赶上"(标记存在)vs"代理桶不匹配"(标记缺失)。
                self._target_pool_cluster_ever[key] = 1
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

    async def _target_pool_prewarm(self, proxy_host: str, proxy_port: int, target: str,
                                   cap: int = 2, source: str = 'passive'):
        """后台协程:为 (proxy, target) 键预热半连接,失败静默(被取消/超时/建连
        失败都只是少一条预热,不影响主请求)。由命中域名缓存/粘性或竞速胜出的
        CONNECT 触发,是"fire-and-forget",主请求不 await 本协程。cap=2 与
        _target_pool_refill 一致(见其 docstring)。task 的注册/排空由 Router 的
        _spawn_target_prewarm 负责(Router._running_tasks)。`source` 沿
        _target_pool_refill 传递(仅'cluster'打 cluster 专属归因标签)。
        """
        try:
            await self._target_pool_refill(proxy_host, proxy_port, target, cap=cap, source=source)
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.info("target prewarm FAILED %s via %s:%s", target, proxy_host, proxy_port)

    # ── 清理 / 生命周期 ─────────────────────────────────────────

    async def _pool_prune(self):
        """关闭空闲超时的预热连接(三池,每 refill 周期顺带清理,防 fd 泄漏)。

        用 writer._conn_pool_created 记录建连时间戳(各处建连统一打)。
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
                    # 连接级超时选择:cluster 预测预建 > established 已握手库存 >
                    # 通用池。各自有独立 idle 超时,避免归还没被复访就过早清掉。
                    timeout = self.conn_pool_idle_timeout
                    if getattr(writer, '_cluster_prewarmed', False):
                        timeout = self.cluster_pool_idle_timeout
                    elif getattr(writer, '_established_pooled', False) \
                            and self.established_pool_idle_timeout is not None:
                        timeout = self.established_pool_idle_timeout
                    if now - last > timeout:
                        stale.append(writer)
                        setattr(self, expired_counter, getattr(self, expired_counter) + 1)
                        if expired_counter == 'target_pool_expired' \
                                and getattr(writer, '_cluster_prewarmed', False):
                            self.cluster_pool_expired += 1
                            # 区分"被真实请求消费后再空转"(曾被打 _consumed_unexpired 标)
                            # 与"从未被消费直接空转"(case 1)。前者极罕见,后者是主存量。
                            if getattr(writer, '_consumed_unexpired', False):
                                self.cluster_pool_consumed_expired += 1
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
        """关闭全部预热连接(stop 时调用),含 target 半预连接池与已握手池。

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

    def start(self):
        """启动后台 refill 循环(与 Router.start 协作;refill_interval<=0 不启动)。

        幂等:已在运行或 refill_interval<=0 或池未启用时直接返回。
        """
        if not self.conn_pool_enabled or self.conn_pool_refill_interval <= 0:
            return
        if self._pool_task is None or self._pool_task.done():
            self._pool_task = asyncio.create_task(self._conn_pool_loop())

    async def stop(self):
        """停止 refill 循环并关闭全部预热连接(Router.stop 调用)。

        先取消并等待 refill 循环(它正在 _conn_pool_refill/_pool_prune,可能持有
        建连在途),再统一关闭三池。prewarm 后台 task 由 Router.stop 在
        _conn_pool_close_all 之后的 _running_tasks 排空阶段取消(保持原顺序)。
        """
        if self._pool_task and not self._pool_task.done():
            self._pool_task.cancel()
            try:
                await self._pool_task
            except (asyncio.CancelledError, Exception):
                pass
            self._pool_task = None
        await self._conn_pool_close_all()