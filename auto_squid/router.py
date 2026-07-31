"""代理路由核心:并行竞速转发 + 域名/响应缓存 + 客户端认证。

本模块实现一个正向代理:接受客户端的 HTTP 与 HTTPS(CONNECT)请求,经一组
上游代理并行竞速,取最先成功的响应回写客户端。核心机制:

- 并行竞速:同一请求同时发往多个上游,最先成功者获胜,其余取消并释放资源
  (_race / _try_http / _try_tunnel)。
- 域名缓存:某代理为某域名胜出后,在 cache_ttl 内复用该代理,避免每请求竞速
  (domain_meta 表 + _get_fresh_proxy)。
- HTTP 响应缓存:幂等 GET 的成功响应在内存缓存 60s(_http_cache_*),遵循
  Cache-Control。
- 客户端认证:可选 HTTP Basic,在 handle_client 分流前统一校验(auth.check_auth)。
- 优雅关闭:stop() 先停服务、再取消并等待在途连接,最后关 DB(_running_tasks)。

数据持久化用 SQLite(domain_stats / domain_meta),跨线程访问经 _db_lock 串行化。
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
from typing import Optional, List, Any
import httpx

from .proxy_store import ProxyStore
from .auth import check_auth

logger = logging.getLogger(__name__)

# 单个 HTTP 请求体的最大字节数。超过则返回 413，避免无 Content-Length 的
# 请求靠 read(-1) 读到 EOF 才返回（会破坏 keep-alive）及无界内存占用。
MAX_BODY = 10 * 1024 * 1024

# Hop-by-hop 请求头：只服务于"客户端→本代理"这一跳，绝不能转发给上游。
# 特别是 Proxy-Authorization——若把客户端访问本代理的凭据透传到上游，
# 上游 Squid 会用它校验缓存对象访问权限（ERR_CACHE_ACCESS_DENIED），
# 误返回 407 + Proxy-Authenticate，导致浏览器弹用户名密码框。
_HOP_BY_HOP_REQUEST_HEADERS = frozenset({
    'proxy-authorization', 'connection', 'proxy-connection', 'keep-alive',
    'te', 'trailer', 'transfer-encoding', 'upgrade',
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

    def __init__(self, proxy_store: ProxyStore, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3, db_path: str = "auto_squid.db", cache_ttl: int = 600, enable_local_racing: bool = False, auth_enabled: bool = False, auth_username: str = "", auth_password: str = ""):
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
        """
        self.proxy_store = proxy_store
        self.selector = ProxySelector(proxy_store)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.max_retries = max_retries
        self.enable_local_racing = enable_local_racing
        self.auth_enabled = auth_enabled
        self.auth_username = auth_username
        self.auth_password = auth_password
        self._server: Optional[asyncio.AbstractServer] = None
        # 跟踪所有正在处理的客户端连接 task，供 stop() 在关闭 DB 前取消并等待。
        self._running_tasks: set[asyncio.Task] = set()
        self.request_counts: dict[str, int] = {}
        self.attempted_counts: dict[str, int] = {}
        self.domain_stats: dict[str, dict[str, int]] = {}
        self.cache_ttl = cache_ttl
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        # 连接以 check_same_thread=False 跨线程共享，但 sqlite3 对同一连接
        # 的并发使用不是线程安全的；用一个锁串行化所有 DB 访问，避免
        # "database is locked" 与并发游标导致的 RecursiveCursor 错误。
        self._db_lock = threading.Lock()
        from datetime import datetime, timezone
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
        self._now_utc = lambda: datetime.now(timezone.utc).isoformat()
        self._http_cache: dict[str, dict] = {}
        self._http_cache_ttl = 60

    # ── DB helpers ──────────────────────────────────────────────

    def _save_domain_stats(self, domain: str, pid: str):
        """记录一次"代理 pid 为域名 domain 胜出"的事件,win 计数 +1。

        使用 UPSERT:存在则累加,不存在则插入。写操作全程持 _db_lock。
        """
        with self._db_lock:
            self._db.execute(
                "INSERT INTO domain_stats (domain, proxy_id, wins) VALUES (?, ?, 1) "
                "ON CONFLICT(domain, proxy_id) DO UPDATE SET wins = wins + 1",
                (domain, pid),
            )
            self._db.commit()

    def get_domain_stats_from_db(self) -> dict[str, dict[str, int]]:
        """读取全量域名胜出统计,组织为 {domain: {proxy_id: wins}}。"""
        with self._db_lock:
            rows = self._db.execute("SELECT domain, proxy_id, wins FROM domain_stats").fetchall()
        result: dict[str, dict[str, int]] = {}
        for domain, pid, wins in rows:
            result.setdefault(domain, {})[pid] = wins
        return result

    def _update_domain_meta(self, domain: str, pid: str):
        """更新某域名的"默认代理"为 pid,并刷新 updated_at 时间戳。

        域名缓存(_get_fresh_proxy)据此在 cache_ttl 内复用该代理。
        """
        with self._db_lock:
            self._db.execute(
                "INSERT INTO domain_meta (domain, default_proxy, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET default_proxy = excluded.default_proxy, updated_at = excluded.updated_at",
                (domain, pid, self._now_utc()),
            )
            self._db.commit()

    def get_domain_meta_from_db(self) -> dict[str, dict[str, str]]:
        """读取全量域名元数据 {domain: {default_proxy, updated_at}}。"""
        with self._db_lock:
            rows = self._db.execute("SELECT domain, default_proxy, updated_at FROM domain_meta").fetchall()
        return {domain: {"default_proxy": dp, "updated_at": ua} for domain, dp, ua in rows}

    def _get_fresh_proxy(self, domain: str) -> Optional[str]:
        """返回某域名在 cache_ttl 内的缓存代理 id;过期或无记录返回 None。

        用于域名缓存:命中则直接复用该代理,跳过竞速。
        """
        from datetime import datetime, timezone
        with self._db_lock:
            row = self._db.execute(
                "SELECT default_proxy, updated_at FROM domain_meta WHERE domain = ?",
                (domain,)
            ).fetchone()
        if not row:
            return None
        pid, updated_at_str = row
        try:
            dt = datetime.fromisoformat(updated_at_str)
            if (datetime.now(timezone.utc) - dt).total_seconds() < self.cache_ttl:
                return pid
        except Exception:
            pass
        return None

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
        """取 GET 的缓存响应;非 GET 或未命中或已过期返回 None。过期项顺便清除。"""
        if method != 'GET':
            return None
        key = self._http_cache_key(method, url)
        entry = self._http_cache.get(key)
        if not entry:
            return None
        if time.time() - entry['cached_at'] > self._http_cache_ttl:
            del self._http_cache[key]
            return None
        return entry

    def _http_cache_set(self, method: str, url: str, resp) -> None:
        """缓存一个 GET 成功响应(状态码、原因、头、body、时间戳)。

        仅缓存 2xx(调用方判断)。当上游未给 Content-Length 时,依据
        Cache-Control 的 no-cache/no-store/private 跳过缓存。
        """
        if method != 'GET':
            return
        cl = resp.headers.get('content-length')
        # 仅当上游未提供 Content-Length 时，才退而依据 Cache-Control 决定
        # 是否缓存。注意用 is None 而非 not cl：Content-Length: 0 时
        # cl == "0" 为真值字符串，不应进入此分支。
        if cl is None:
            cc = resp.headers.get('cache-control', '')
            if 'no-cache' in cc or 'no-store' in cc or 'private' in cc:
                return
        key = self._http_cache_key(method, url)
        self._http_cache[key] = {
            'status_code': resp.status_code,
            'reason_phrase': resp.reason_phrase,
            'headers': dict(resp.headers.items()),
            'content': resp.content,
            'cached_at': time.time(),
        }

    # ── 通用竞速 / pipe / 响应写入 ──────────────────────────────

    @staticmethod
    async def _race(tasks: set, cleanup=None) -> Optional[Any]:
        """取最先成功完成的 task 的结果；取消并清理其余 task。

        cleanup(result) 用于释放「已完成但未获胜」的 task 持有的资源
        （如 httpx client、上游连接）。被 cancel 的 task 由其自身的
        except BaseException 分支关闭资源，无需在这里调用 cleanup。
        """
        winner = None
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    winner = t.result()
                    break
                except Exception:
                    pass
            if winner:
                # 先取消尚未完成的 task，再统一等待；对已成功完成但未获胜的
                # task（不在 cancelled 状态）调用 cleanup 释放其资源。
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                for t in tasks:
                    if t.done() and not t.cancelled() and cleanup:
                        try:
                            await cleanup(t.result())
                        except Exception:
                            pass
                break
        return winner

    @staticmethod
    async def _cleanup_http_result(result):
        """关闭竞速中已完成但未获胜的 HTTP task 持有的 httpx client。"""
        if not result:
            return
        client = result[-1]
        try:
            await client.aclose()
        except (BrokenPipeError, ConnectionError, OSError):
            pass
        except Exception:
            pass

    @staticmethod
    async def _cleanup_tunnel_result(result):
        """关闭竞速中已完成但未获胜的 CONNECT task 持有的上游连接。"""
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
    async def _write_response(writer, status_code, reason_phrase, headers, body):
        """把一个 HTTP 响应写回客户端(用于代理自身生成的响应与转发的上游响应)。

        剔除 hop-by-hop 响应头(transfer-encoding/content-encoding/content-length 等),
        再按实际 body 长度重写 Content-Length——避免与上游头重复或与 chunked
        编码后的长度不一致。客户端已断开时静默忽略(BrokenPipe 等)。
        """
        # hop-by-hop 头由代理自身管理，不能透传；content-length 也剔除，
        # 因为我们按实际写入的 body 长度重新计算，避免与上游头重复或与
        # chunked 编码后的长度不一致。
        hop_by_hop = {'transfer-encoding', 'content-encoding', 'content-length',
                      'keep-alive', 'proxy-connection', 'te', 'trailer', 'upgrade'}
        try:
            writer.write(f"HTTP/1.1 {status_code} {reason_phrase}\r\n".encode('latin-1'))
            for k, v in headers.items():
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

    async def _try_http(self, pid: str, proxy_url: Optional[str], method: str, url: str, headers: dict, body: bytes, update_meta: bool = True):
        """经某上游代理尝试一次 HTTP 请求,作为竞速的一个候选。

        每次新建独立 httpx.AsyncClient(连接池隔离,便于胜出/落败后单独关闭)。
        成功则记录统计、更新域名元数据(除非 update_meta=False,如域名缓存命中
        不重复更新),返回 (pid, method, url, resp, client)——client 留给调用方关闭。
        失败(BaseException,含 CancelledError)自行关闭 client 并向上抛出,
        让 _race 的清理逻辑处理。
        """
        kw = {}
        if proxy_url:
            kw['proxy'] = proxy_url
        client = httpx.AsyncClient(**kw, limits=httpx.Limits(max_keepalive_connections=10, max_connections=50, keepalive_expiry=30))
        try:
            self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
            resp = await client.request(method, url, headers=headers, content=body, timeout=10)
            self.request_counts[pid] = self.request_counts.get(pid, 0) + 1
            domain = urllib.parse.urlparse(url).hostname or url
            per_domain = self.domain_stats.setdefault(domain, {})
            per_domain[pid] = per_domain.get(pid, 0) + 1
            self._save_domain_stats(domain, pid)
            if update_meta:
                self._update_domain_meta(domain, pid)
            return pid, method, url, resp, client
        except BaseException:
            try:
                await client.aclose()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            raise

    async def _try_tunnel(self, pid: str, target: str, proxy_host: Optional[str], proxy_port: Optional[int], proxy_auth: Optional[dict], update_meta: bool = True):
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
            per_domain = self.domain_stats.setdefault(target, {})
            per_domain[pid] = per_domain.get(pid, 0) + 1
            self._save_domain_stats(target, pid)
            if update_meta:
                self._update_domain_meta(target, pid)
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
        logger.info("client connected %s", peer)
        self._set_nodelay(writer)
        try:
            line = await reader.readline()
            if not line:
                return
            first = line.decode('latin-1').strip()
            headers = b''
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
                headers += h
            logger.debug("first line: %s", first)
            # 客户端认证：在 CONNECT/HTTP 分流前统一校验，未通过则返回 407，
            # 不进行任何上游连接/竞速/DB 写入。auth_enabled=False 时放行。
            if self.auth_enabled:
                client_hdrs = {}
                for h in headers.decode('latin-1').split('\r\n'):
                    if ':' in h:
                        k, v = h.split(':', 1)
                        client_hdrs[k.strip()] = v.strip()
                ok, reason = check_auth(client_hdrs, self.auth_enabled, self.auth_username, self.auth_password)
                if not ok:
                    logger.info("auth rejected for %s: %s", peer, reason)
                    await self._write_response(writer, 407, 'Proxy Authentication Required',
                                               {'Proxy-Authenticate': 'Basic realm="auto_squid"',
                                                'Content-Type': 'text/plain'},
                                               (reason or 'Authentication required').encode('latin-1'))
                    return
            if first.upper().startswith('CONNECT'):
                target = first.split(' ')[1]
                await self._handle_connect(target, reader, writer)
            else:
                body = b''
                cl = None
                for h in headers.decode('latin-1').split('\r\n'):
                    if h.lower().startswith('content-length:'):
                        cl = int(h.split(':', 1)[1].strip())
                        break
                if cl is not None and cl > 0:
                    if cl > MAX_BODY:
                        writer.write(b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 15\r\n\r\nPayload Too Large")
                        await writer.drain()
                        return
                    body = await reader.readexactly(cl)
                elif cl is None and first.upper().split(' ')[0] in ('POST', 'PUT', 'PATCH'):
                    # 无 Content-Length 头：分块读取至上限，避免 read(-1) 阻塞到
                    # 客户端关闭连接而破坏 HTTP keep-alive。注意 cl is None 与
                    # cl == 0 不同——后者表示头部存在但 body 为空，应直接用 b''。
                    body = b''
                    while len(body) < MAX_BODY:
                        chunk = await reader.read(MAX_BODY - len(body))
                        if not chunk:
                            break
                        body += chunk
                    if len(body) >= MAX_BODY:
                        writer.write(b"HTTP/1.1 413 Payload Too Large\r\nContent-Length: 15\r\n\r\nPayload Too Large")
                        await writer.drain()
                        return
                request_bytes = (first + '\r\n').encode('latin-1') + headers + b'\r\n' + body
                await self._handle_http_request(request_bytes, writer)
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

    async def _build_racing_tasks_http(self, proxies: List[str], method: str, url: str, headers: dict, body: bytes) -> set:
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

    async def _handle_http_request(self, request_bytes: bytes, writer: asyncio.StreamWriter):
        """处理一个完整 HTTP 请求(已含首行+头+body),按优先级回写响应。

        决策顺序(命中即返回):
        1. HTTP 响应缓存命中 → 直接回写缓存响应。
        2. 域名缓存命中 → 用该代理单发请求(不竞速);失败则继续。
        3. 竞速:首批 max_retries 个代理并行,全失败且有剩余则对剩余再竞速。
        4. 全失败 → 502。成功 2xx 顺带写入响应缓存。
        请求头转发前已剔除 hop-by-hop 头(见调用方)。
        """
        # 以字节边界解析请求头，避免把整个请求（含二进制 body）解码为
        # latin-1 再往返编码——那会破坏 0x100 以上的字节。头部用 latin-1
        # 解码是安全的（HTTP 头字段为 ASCII），body 则直接取原始字节。
        header_end = request_bytes.find(b'\r\n\r\n')
        head_part = request_bytes[:header_end if header_end != -1 else len(request_bytes)]
        body = request_bytes[header_end + 4:] if header_end != -1 else b''
        text = head_part.decode('latin-1', errors='ignore')
        first_line = text.split('\r\n')[0]
        parts = first_line.split(' ')
        if len(parts) < 3:
            try:
                writer.write(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 11\r\n\r\nBad Request")
                await writer.drain()
            except Exception:
                pass
            return
        method, url, _ = parts
        domain = urllib.parse.urlparse(url).hostname or url
        hdrs = {}
        lines = text.split('\r\n')
        i = 1
        while i < len(lines) and lines[i]:
            h = lines[i]
            if ':' in h:
                k, v = h.split(':', 1)
                hdrs[k.strip()] = v.strip()
            i += 1
        body = body or None
        # 剔除 hop-by-hop 请求头，避免把客户端访问本代理的凭据
        # （Proxy-Authorization）等透传给上游。
        hdrs = {k: v for k, v in hdrs.items() if k.lower() not in _HOP_BY_HOP_REQUEST_HEADERS}

        # 1) HTTP 响应缓存:GET 幂等响应直接命中,完全不经上游。
        cached_entry = self._http_cache_get(method, url)
        if cached_entry:
            logger.info("HTTP cache hit %s %s", method, url)
            await self._write_response(writer, cached_entry['status_code'], cached_entry['reason_phrase'],
                                       cached_entry['headers'], cached_entry['content'])
            return

        # 2) 域名缓存:用上次胜出的代理单发请求(update_meta=False 不重复更新),
        #    失败则回退到竞速。
        if domain:
            cached_pid = self._get_fresh_proxy(domain)
            if cached_pid:
                try:
                    proxy = self.proxy_store.get(cached_pid)
                    pid, method, url, resp, client = await self._try_http(
                        cached_pid, self._build_proxy_url(proxy), method, url, hdrs, body, update_meta=False)
                    logger.info("proxy %s cache hit %s %s", pid, method, url)
                    await self._write_response(writer, resp.status_code, resp.reason_phrase,
                                               dict(resp.headers.items()), resp.content)
                    try:
                        await client.aclose()
                    except (BrokenPipeError, ConnectionError, OSError):
                        pass
                    if resp.status_code >= 200 and resp.status_code < 300:
                        self._http_cache_set(method, url, resp)
                    return
                except Exception:
                    logger.debug("cached proxy %s failed for %s", cached_pid, domain)

        # 3) 竞速:首批并行 max_retries 个代理,全失败且还有剩余则对剩余再竞速。
        proxies = self.selector.ordered_proxies()
        if not proxies and not self.enable_local_racing:
            await self._write_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')
            return

        tasks = await self._build_racing_tasks_http(proxies, method, url, hdrs, body)
        winner_resp = await self._race(tasks, cleanup=self._cleanup_http_result)

        # 首批全失败且代理数超过 max_retries:对剩余代理再竞速兜底。
        if not winner_resp and len(proxies) > self.max_retries:
            remaining = proxies[self.max_retries:]
            tasks = await self._build_racing_tasks_http(remaining, method, url, hdrs, body)
            winner_resp = await self._race(tasks, cleanup=self._cleanup_http_result)

        if winner_resp:
            pid, method, url, resp, client = winner_resp
            logger.info("proxy %s racing win %s %s", pid, method, url)
            await self._write_response(writer, resp.status_code, resp.reason_phrase,
                                       dict(resp.headers.items()), resp.content)
            try:
                await client.aclose()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            if resp.status_code >= 200 and resp.status_code < 300:
                self._http_cache_set(method, url, resp)
            return

        logger.error("all proxies failed for HTTP request")
        await self._write_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')

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

    async def _handle_connect(self, target: str, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        """处理 CONNECT 请求:建立到 target 的隧道并双向透传数据。

        决策顺序与 HTTP 类似:域名缓存命中 → 单发隧道;否则竞速(首批
        max_retries,失败对剩余兜底)。胜出后回写 "200 Connection established",
        用两个反向 _pipe 做客户端↔上游的双向透传,任一方向结束即关闭。
        全失败回写 502。认证已在 handle_client 完成,此处不再校验。
        """
        # 1) 域名缓存命中:用上次胜出的代理单发隧道(update_meta=False),失败回退竞速。
        cached_pid = self._get_fresh_proxy(target)
        if cached_pid:
            try:
                proxy = self.proxy_store.get(cached_pid)
                pid, up_reader, up_writer = await self._try_tunnel(cached_pid, target, proxy.host, proxy.port, proxy.auth, update_meta=False)
                logger.info("proxy %s cache hit CONNECT %s", pid, target)
                self._set_nodelay(client_writer)
                self._set_nodelay(up_writer)
                client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await client_writer.drain()
                # 双向透传:客户端→上游 与 上游→客户端 同时搬运,任一结束即终止。
                await asyncio.gather(self._pipe(client_reader, up_writer), self._pipe(up_reader, client_writer))
                try:
                    up_writer.close()
                    await up_writer.wait_closed()
                except Exception:
                    pass
                return
            except Exception:
                logger.debug("cached proxy %s failed CONNECT %s", cached_pid, target)

        # 2) 竞速:首批并行 max_retries 个,全失败且还有剩余则对剩余再竞速。
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
            logger.info("proxy %s racing CONNECT to %s for client %s", pid, target, client_peer)
            self._set_nodelay(client_writer)
            self._set_nodelay(up_writer)
            client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await client_writer.drain()
            # 双向透传(同上)。
            await asyncio.gather(self._pipe(client_reader, up_writer), self._pipe(up_reader, client_writer))
            try:
                up_writer.close()
                await up_writer.wait_closed()
            except Exception:
                pass
            return

        # 3) 全失败:回写 502 并关闭客户端连接。
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
        """开始监听代理端口,接受客户端连接(非阻塞,返回后服务在后台运行)。"""
        self._server = await asyncio.start_server(self.handle_client, host=self.listen_host, port=self.listen_port)
        logger.info("Router listening on %s:%s", self.listen_host, self.listen_port)

    async def stop(self):
        """优雅关闭:停止接受新连接 → 取消并等待在途连接 → 关闭 DB。

        先关 _server(不再接受新连接),再取消所有正在处理的 handle_client task
        并等待它们退出,最后才 _db.close()——避免在 DB 关闭后仍有在途请求写库报错。
        """
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        # 停止接受新连接后，取消仍在处理的客户端连接 task 并等待它们退出，
        # 避免在 _db.close() 之后还有在途请求尝试写 DB 而报错。
        for t in list(self._running_tasks):
            t.cancel()
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)
            self._running_tasks.clear()
        self._db.close()
