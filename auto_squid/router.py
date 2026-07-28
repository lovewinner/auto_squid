import asyncio
import base64
import logging
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
        # filter out disabled
        enabled = [p for p in proxies if p.enabled]
        # attach score (default 50)
        scored = [(p.id, scores.get(p.id, 50.0)) for p in enabled]
        # sort desc
        scored.sort(key=lambda t: -t[1])
        return [pid for (pid, _) in scored]

    def best_proxy(self) -> Optional[str]:
        lst = self.ordered_proxies()
        return lst[0] if lst else None


class Router:
    """Router with retry/failover policy: try top N proxies on failure."""

    def __init__(self, proxy_store: ProxyStore, probe_engine: ProbeEngine, listen_host: str = "0.0.0.0", listen_port: int = 10808, max_retries: int = 3):
        self.proxy_store = proxy_store
        self.probe_engine = probe_engine
        self.selector = ProxySelector(probe_engine, proxy_store)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.max_retries = max_retries
        self._server: Optional[asyncio.AbstractServer] = None

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
                request_bytes = (first + '\r\n').encode('latin-1') + headers + body
                await self._handle_http_request(request_bytes, writer)
        except BaseException:
            logger.exception("error handling client")
        finally:
            try:
                writer.close()
            except BaseException:
                pass

    async def _handle_http_request(self, request_bytes: bytes, writer: asyncio.StreamWriter):
        proxies = self.selector.ordered_proxies()
        if not proxies:
            try:
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await writer.drain()
            except BaseException:
                pass
            try:
                writer.close()
            except BaseException:
                pass
            return

        last_exc = None
        for pid in proxies[:self.max_retries]:
            proxy = self.proxy_store.get(pid)
            if not proxy:
                continue
            if proxy.auth:
                proxy_url = f"http://{proxy.auth['username']}:{proxy.auth['password']}@{proxy.host}:{proxy.port}"
            else:
                proxy_url = f"http://{proxy.host}:{proxy.port}"
            async with httpx.AsyncClient(proxy=proxy_url) as client:
                try:
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
                    logger.info("proxy %s handling %s %s", pid, method, url)
                    status_line = f"HTTP/1.1 {resp.status_code} {resp.reason_phrase}\r\n"
                    writer.write(status_line.encode('latin-1'))
                    for k, v in resp.headers.items():
                        writer.write(f"{k}: {v}\r\n".encode('latin-1'))
                    writer.write(b"\r\n")
                    writer.write(resp.content)
                    await writer.drain()
                    return
                except BaseException as e:
                    logger.warning("proxy %s failed for HTTP request: %s", pid, e)
                    last_exc = e
                    continue
        logger.error("all proxies failed for HTTP request: %s", last_exc)
        try:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await writer.drain()
        except BaseException:
            pass
        try:
            writer.close()
        except BaseException:
            pass

    async def _handle_connect(self, target: str, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        proxies = self.selector.ordered_proxies()
        if not proxies:
            try:
                client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await client_writer.drain()
            except BaseException:
                pass
            try:
                client_writer.close()
            except BaseException:
                pass
            return

        last_exc = None
        for pid in proxies[:self.max_retries]:
            proxy = self.proxy_store.get(pid)
            if not proxy:
                continue
            upstream_reader = upstream_writer = None
            try:
                upstream_reader, upstream_writer = await asyncio.open_connection(proxy.host, proxy.port)
                auth_hdr = ""
                if proxy.auth:
                    raw = f"{proxy.auth['username']}:{proxy.auth['password']}"
                    encoded = base64.b64encode(raw.encode()).decode()
                    auth_hdr = f"Proxy-Authorization: Basic {encoded}\r\n"
                upstream_writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n{auth_hdr}\r\n".encode('latin-1'))
                await upstream_writer.drain()
                status = await upstream_reader.readline()
                if not status:
                    raise RuntimeError('no response from upstream')
                status_text = status.decode('latin-1')
                if '200' not in status_text:
                    while True:
                        h = await upstream_reader.readline()
                        if not h or h in (b"\r\n", b"\n"):
                            break
                    upstream_writer.close()
                    raise RuntimeError('upstream returned non-200 for CONNECT')
                # consume remaining CONNECT response headers and empty line
                while True:
                    h = await upstream_reader.readline()
                    if not h or h in (b"\r\n", b"\n"):
                        break
                client_peer = client_writer.get_extra_info('peername')
                logger.info("proxy %s handling CONNECT to %s for client %s", pid, target, client_peer)
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
                    except BaseException:
                        pass
                    try:
                        writer.close()
                    except BaseException:
                        pass

                await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))
                return
            except BaseException as e:
                logger.warning("proxy %s failed for CONNECT: %s", pid, e)
                last_exc = e
                if upstream_writer:
                    try:
                        upstream_writer.close()
                    except BaseException:
                        pass
                continue
        logger.error("all proxies failed for CONNECT: %s", last_exc)
        try:
            client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await client_writer.drain()
        except BaseException:
            pass
        try:
            client_writer.close()
        except BaseException:
            pass

    async def start(self):
        self._server = await asyncio.start_server(self.handle_client, host=self.listen_host, port=self.listen_port)
        logger.info("Router listening on %s:%s", self.listen_host, self.listen_port)

    async def stop(self):
        if self._server:
            self._server.close()
            await self._server.wait_closed()
