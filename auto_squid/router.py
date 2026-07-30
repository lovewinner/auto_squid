import asyncio
import base64
import logging
import random
import socket
import sqlite3
import threading
import time
import urllib.parse
from typing import Optional, List
from datetime import datetime, timezone
import httpx

from .proxy_store import ProxyStore
from .policy_engine import PolicyEngine

logger = logging.getLogger(__name__)


class ProxySelector:
    def __init__(self, proxy_store: ProxyStore):
        self.proxy_store = proxy_store

    def ordered_proxies(self) -> List[str]:
        proxies = self.proxy_store.list()
        enabled = [p for p in proxies if p.enabled]
        random.shuffle(enabled)
        return [p.id for p in enabled]

    def best_proxy(self) -> Optional[str]:
        lst = self.ordered_proxies()
        return lst[0] if lst else None


class Router:
    def __init__(self, proxy_store: ProxyStore, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3, db_path: str = "auto_squid.db", cache_ttl: int = 600, enable_local_racing: bool = False):
        self.proxy_store = proxy_store
        self.selector = ProxySelector(proxy_store)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.max_retries = max_retries
        self.enable_local_racing = enable_local_racing
        self._server: Optional[asyncio.AbstractServer] = None
        self._running_tasks: set[asyncio.Task] = set()
        self.request_counts: dict[str, int] = {}
        self.attempted_counts: dict[str, int] = {}
        self.domain_stats: dict[str, dict[str, int]] = {}
        self.cache_ttl = cache_ttl
        self._db = sqlite3.connect(db_path, check_same_thread=False)
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
        self._db.execute("""
            CREATE TABLE IF NOT EXISTS policy_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_type TEXT NOT NULL,
                domain_pattern TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_proxy TEXT,
                tag_key TEXT,
                tag_value TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """)
        self._db.commit()
        self._now_utc = lambda: datetime.now(timezone.utc).isoformat()
        self._http_cache: dict[str, dict] = {}
        self._http_cache_ttl = 60
        self._db_lock = threading.Lock()
        self.policy_engine = PolicyEngine(self._db, proxy_store)

    # ── DB helpers ──────────────────────────────────────────────

    def _save_domain_stats(self, domain: str, pid: str):
        with self._db_lock:
            self._db.execute(
                "INSERT INTO domain_stats (domain, proxy_id, wins) VALUES (?, ?, 1) "
                "ON CONFLICT(domain, proxy_id) DO UPDATE SET wins = wins + 1",
                (domain, pid),
            )
            self._db.commit()

    def get_domain_stats_from_db(self) -> dict[str, dict[str, int]]:
        rows = self._db.execute("SELECT domain, proxy_id, wins FROM domain_stats").fetchall()
        result: dict[str, dict[str, int]] = {}
        for domain, pid, wins in rows:
            result.setdefault(domain, {})[pid] = wins
        return result

    def _update_domain_meta(self, domain: str, pid: str):
        with self._db_lock:
            self._db.execute(
                "INSERT INTO domain_meta (domain, default_proxy, updated_at) VALUES (?, ?, ?) "
                "ON CONFLICT(domain) DO UPDATE SET default_proxy = excluded.default_proxy, updated_at = excluded.updated_at",
                (domain, pid, self._now_utc()),
            )
            self._db.commit()

    def get_domain_meta_from_db(self) -> dict[str, dict[str, str]]:
        rows = self._db.execute("SELECT domain, default_proxy, updated_at FROM domain_meta").fetchall()
        return {domain: {"default_proxy": dp, "updated_at": ua} for domain, dp, ua in rows}

    def _get_fresh_proxy(self, domain: str) -> Optional[str]:
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
        sock = writer.get_extra_info('socket')
        if sock:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_QUICKACK, 1)
            except (OSError, AttributeError):
                pass

    # ── HTTP GET 缓存 ──────────────────────────────────────────

    def _http_cache_key(self, method: str, url: str) -> str:
        return f"{method}:{url}"

    def _http_cache_get(self, method: str, url: str) -> Optional[dict]:
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
        if method != 'GET':
            return
        cl = resp.headers.get('content-length')
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
    async def _race(tasks: set) -> Optional[any]:
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
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                break
        return winner

    @staticmethod
    async def _pipe(reader, writer):
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
        hop_by_hop = {'transfer-encoding', 'content-encoding', 'keep-alive',
                      'proxy-connection', 'te', 'trailer', 'upgrade'}
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
        if not proxy:
            return None
        if proxy.auth:
            user = urllib.parse.quote(proxy.auth['username'], safe='')
            pw = urllib.parse.quote(proxy.auth['password'], safe='')
            return f"http://{user}:{pw}@{proxy.host}:{proxy.port}"
        return f"http://{proxy.host}:{proxy.port}"

    async def _try_http(self, pid: str, proxy_url: Optional[str], method: str, url: str, headers: dict, body: bytes, update_meta: bool = True):
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
            raise RuntimeError(f'connect to {proxy_host or host}:{proxy_port or port} timed out: {e}')
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
                if cl and cl > 0:
                    body = await reader.readexactly(cl)
                elif not cl and first.upper().split(' ')[0] in ('POST', 'PUT', 'PATCH'):
                    MAX_BODY = 10 * 1024 * 1024
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
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            self._running_tasks.discard(task)

    # ── HTTP 请求处理 ──────────────────────────────────────────

    async def _build_racing_tasks_http(self, proxies: List[str], method: str, url: str, headers: dict, body: bytes) -> set:
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
        text = request_bytes.decode('latin-1', errors='ignore')
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
        body = '\r\n'.join(lines[i+1:]).encode('latin-1') if i+1 < len(lines) else None

        # 策略引擎：force 规则短路
        if self.policy_engine:
            forced_pid = self.policy_engine.resolve_force(domain)
            if forced_pid:
                proxy = self.proxy_store.get(forced_pid)
                if proxy:
                    try:
                        pid, method, url, resp, client = await self._try_http(
                            forced_pid, self._build_proxy_url(proxy), method, url, hdrs, body, update_meta=False)
                        logger.info("policy force %s for %s via %s", forced_pid, domain, forced_pid)
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
                        logger.debug("forced proxy %s failed for %s", forced_pid, domain)

        # HTTP GET 缓存
        cached_entry = self._http_cache_get(method, url)
        if cached_entry:
            logger.info("HTTP cache hit %s %s", method, url)
            await self._write_response(writer, cached_entry['status_code'], cached_entry['reason_phrase'],
                                       cached_entry['headers'], cached_entry['content'])
            return

        # 域名缓存
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

        # 竞速
        proxies = self.selector.ordered_proxies()
        # 策略引擎：deny + prefer 过滤/排序
        if self.policy_engine:
            if domain:
                proxies = self.policy_engine.evaluate_denies(domain, proxies)
                proxies = self.policy_engine.apply_prefers(domain, proxies)
        if not proxies and not self.enable_local_racing:
            await self._write_response(writer, 502, 'Bad Gateway', {'Content-Type': 'text/plain'}, b'Bad Gateway')
            return

        tasks = await self._build_racing_tasks_http(proxies, method, url, hdrs, body)
        winner_resp = await self._race(tasks)

        if not winner_resp and len(proxies) > self.max_retries:
            remaining = proxies[self.max_retries:]
            tasks = await self._build_racing_tasks_http(remaining, method, url, hdrs, body)
            winner_resp = await self._race(tasks)

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
        # 提取域名用于策略匹配
        domain = target.rsplit(':', 1)[0] if ':' in target else target

        # 策略引擎：force 规则短路
        if self.policy_engine:
            forced_pid = self.policy_engine.resolve_force(domain)
            if forced_pid:
                proxy = self.proxy_store.get(forced_pid)
                if proxy:
                    try:
                        pid, up_reader, up_writer = await self._try_tunnel(
                            forced_pid, target, proxy.host, proxy.port, proxy.auth, update_meta=False)
                        logger.info("policy force CONNECT %s via %s", target, pid)
                        self._set_nodelay(client_writer)
                        self._set_nodelay(up_writer)
                        client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                        await client_writer.drain()
                        await asyncio.gather(self._pipe(client_reader, up_writer), self._pipe(up_reader, client_writer))
                        try:
                            up_writer.close()
                            await up_writer.wait_closed()
                        except Exception:
                            pass
                        return
                    except Exception:
                        logger.debug("forced proxy %s failed CONNECT %s", forced_pid, target)

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
                await asyncio.gather(self._pipe(client_reader, up_writer), self._pipe(up_reader, client_writer))
                try:
                    up_writer.close()
                    await up_writer.wait_closed()
                except Exception:
                    pass
                return
            except Exception:
                logger.debug("cached proxy %s failed CONNECT %s", cached_pid, target)

        proxies = self.selector.ordered_proxies()
        # 策略引擎：deny + prefer 过滤/排序
        if self.policy_engine:
            proxies = self.policy_engine.evaluate_denies(domain, proxies)
            proxies = self.policy_engine.apply_prefers(domain, proxies)
        if not proxies and not self.enable_local_racing:
            try:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await client_writer.drain()
            except Exception:
                pass
            return

        tasks = await self._build_racing_tasks_connect(proxies, target)
        winner = await self._race(tasks)

        if not winner and len(proxies) > self.max_retries:
            remaining = proxies[self.max_retries:]
            tasks = await self._build_racing_tasks_connect(remaining, target)
            winner = await self._race(tasks)

        if winner:
            pid, up_reader, up_writer = winner
            client_peer = client_writer.get_extra_info('peername')
            logger.info("proxy %s racing CONNECT to %s for client %s", pid, target, client_peer)
            self._set_nodelay(client_writer)
            self._set_nodelay(up_writer)
            client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await client_writer.drain()
            await asyncio.gather(self._pipe(client_reader, up_writer), self._pipe(up_reader, client_writer))
            try:
                up_writer.close()
                await up_writer.wait_closed()
            except Exception:
                pass
            return

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
        self._server = await asyncio.start_server(self.handle_client, host=self.listen_host, port=self.listen_port)
        logger.info("Router listening on %s:%s", self.listen_host, self.listen_port)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for t in list(self._running_tasks):
            t.cancel()
        if self._running_tasks:
            await asyncio.gather(*self._running_tasks, return_exceptions=True)
            self._running_tasks.clear()
        self._db.close()
