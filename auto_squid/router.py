import asyncio
import base64
import logging
import random
import sqlite3
import urllib.parse
from typing import Optional, List
import httpx

from .proxy_store import ProxyStore
from .probe_engine import ProbeEngine

logger = logging.getLogger(__name__)


class ProxySelector:
    def __init__(self, probe_engine: ProbeEngine, proxy_store: ProxyStore):
        self.probe_engine = probe_engine
        self.proxy_store = proxy_store

    def ordered_proxies(self) -> List[str]:
        scores = self.probe_engine.get_scores()
        proxies = self.proxy_store.list()
        enabled = [p for p in proxies if p.enabled]
        scored = [(p.id, max(0.1, scores.get(p.id, 50.0))) for p in enabled]
        scored.sort(key=lambda t: -random.random() * t[1])
        return [pid for (pid, _) in scored]

    def best_proxy(self) -> Optional[str]:
        lst = self.ordered_proxies()
        return lst[0] if lst else None


class Router:
    """Router with retry/failover policy: try top N proxies on failure."""

    def __init__(self, proxy_store: ProxyStore, probe_engine: ProbeEngine, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3, db_path: str = "auto_squid.db"):
        self.proxy_store = proxy_store
        self.probe_engine = probe_engine
        self.selector = ProxySelector(probe_engine, proxy_store)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.max_retries = max_retries
        self._server: Optional[asyncio.AbstractServer] = None
        self.request_counts: dict[str, int] = {}
        self.attempted_counts: dict[str, int] = {}
        self.domain_stats: dict[str, dict[str, int]] = {}
        self._db = sqlite3.connect(db_path, check_same_thread=False)
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

    def _save_domain_stats(self, domain: str, pid: str):
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
        self._db.execute(
            "INSERT INTO domain_meta (domain, default_proxy, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(domain) DO UPDATE SET default_proxy = excluded.default_proxy, updated_at = excluded.updated_at",
            (domain, pid, self._now_utc()),
        )
        self._db.commit()

    def get_domain_meta_from_db(self) -> dict[str, dict[str, str]]:
        rows = self._db.execute("SELECT domain, default_proxy, updated_at FROM domain_meta").fetchall()
        return {domain: {"default_proxy": dp, "updated_at": ua} for domain, dp, ua in rows}

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        logger.info("client connected %s", peer)
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
                    body = await reader.read(-1)
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

    async def _try_proxy(self, pid: str, request_bytes: bytes):
        proxy = self.proxy_store.get(pid)
        if not proxy:
            raise ValueError(f"proxy {pid} not found")
        if proxy.auth:
            user = urllib.parse.quote(proxy.auth['username'], safe='')
            pw = urllib.parse.quote(proxy.auth['password'], safe='')
            proxy_url = f"http://{user}:{pw}@{proxy.host}:{proxy.port}"
        else:
            proxy_url = f"http://{proxy.host}:{proxy.port}"
        client = httpx.AsyncClient(proxy=proxy_url, limits=httpx.Limits(max_connections=1, max_keepalive_connections=0))
        try:
            self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
            text = request_bytes.decode('latin-1', errors='ignore')
            lines = text.split('\r\n')
            req_line = lines[0]
            parts = req_line.split(' ')
            if len(parts) < 3:
                raise ValueError('invalid request line')
            method, url, _ = parts
            hdrs = {}
            i = 1
            while i < len(lines) and lines[i]:
                h = lines[i]
                if ':' in h:
                    k, v = h.split(':', 1)
                    hdrs[k.strip()] = v.strip()
                i += 1
            body = '\r\n'.join(lines[i+1:]).encode('latin-1') if i+1 < len(lines) else None
            resp = await client.request(method, url, headers=hdrs, content=body, timeout=10)
            self.request_counts[pid] = self.request_counts.get(pid, 0) + 1
            domain = urllib.parse.urlparse(url).hostname or url
            per_domain = self.domain_stats.setdefault(domain, {})
            per_domain[pid] = per_domain.get(pid, 0) + 1
            self._save_domain_stats(domain, pid)
            self._update_domain_meta(domain, pid)
            return pid, method, url, resp, client
        except BaseException:
            try:
                await client.aclose()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            raise

    async def _handle_http_request(self, request_bytes: bytes, writer: asyncio.StreamWriter):
        proxies = self.selector.ordered_proxies()
        if not proxies:
            try:
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await writer.drain()
            except Exception:
                pass
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
            return

        tasks = set()
        for pid in proxies[:self.max_retries]:
            tasks.add(asyncio.create_task(self._try_proxy(pid, request_bytes)))

        winner_resp = None
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    pid, method, url, resp, client = t.result()
                    winner_resp = (pid, method, url, resp, client)
                    break
                except Exception:
                    pass
            if winner_resp:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                break

        if winner_resp:
            pid, method, url, resp, client = winner_resp
            logger.info("proxy %s racing win %s %s", pid, method, url)
            try:
                status_line = f"HTTP/1.1 {resp.status_code} {resp.reason_phrase}\r\n"
                writer.write(status_line.encode('latin-1'))
                hop_by_hop = {'transfer-encoding', 'content-encoding', 'keep-alive',
                              'proxy-connection', 'te', 'trailer', 'upgrade'}
                for k, v in resp.headers.items():
                    if k.lower() in hop_by_hop:
                        continue
                    writer.write(f"{k}: {v}\r\n".encode('latin-1'))
                writer.write(f"Content-Length: {len(resp.content)}\r\n".encode('latin-1'))
                writer.write(b"\r\n")
                writer.write(resp.content)
                await writer.drain()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            try:
                await client.aclose()
            except (BrokenPipeError, ConnectionError, OSError):
                pass
            return

        logger.error("all proxies failed for HTTP request")
        try:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await writer.drain()
        except Exception:
            pass
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass
    async def _try_connect(self, pid: str, target: str):
        proxy = self.proxy_store.get(pid)
        if not proxy:
            raise ValueError(f"proxy {pid} not found")
        up_reader, up_writer = await asyncio.open_connection(proxy.host, proxy.port)
        try:
            auth_hdr = ""
            if proxy.auth:
                raw = f"{proxy.auth['username']}:{proxy.auth['password']}"
                encoded = base64.b64encode(raw.encode()).decode()
                auth_hdr = f"Proxy-Authorization: Basic {encoded}\r\n"
            up_writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n{auth_hdr}\r\n".encode('latin-1'))
            await up_writer.drain()
            self.attempted_counts[pid] = self.attempted_counts.get(pid, 0) + 1
            status = await up_reader.readline()
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
            self._update_domain_meta(target, pid)
            return pid, up_reader, up_writer
        except Exception as e:
            try:
                up_writer.close()
                await up_writer.wait_closed()
            except Exception:
                pass
            logger.debug("_try_connect exception: %s", e)
            raise

    async def _handle_connect(self, target: str, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        proxies = self.selector.ordered_proxies()
        if not proxies:
            try:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await client_writer.drain()
            except Exception:
                pass
            return

        tasks = set()
        for pid in proxies[:self.max_retries]:
            tasks.add(asyncio.create_task(self._try_connect(pid, target)))

        winner = None
        while tasks:
            done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for t in done:
                try:
                    pid, up_reader, up_writer = t.result()
                    winner = (pid, up_reader, up_writer)
                    break
                except Exception:
                    pass
            if winner:
                for t in tasks:
                    t.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
                break

        if winner:
            pid, up_reader, up_writer = winner
            client_peer = client_writer.get_extra_info('peername')
            logger.info("proxy %s racing CONNECT to %s for client %s", pid, target, client_peer)
            client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await client_writer.drain()

            async def pipe(reader, writer):
                try:
                    while True:
                        data = await reader.read(4096)
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

            await asyncio.gather(pipe(client_reader, up_writer), pipe(up_reader, client_writer))
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
        self._db.close()
