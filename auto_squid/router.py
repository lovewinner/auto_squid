import asyncio
import logging
from typing import Optional
import httpx

from .proxy_store import ProxyStore
from .probe_engine import ProbeEngine

logger = logging.getLogger(__name__)


class ProxySelector:
    def __init__(self, probe_engine: ProbeEngine, proxy_store: ProxyStore):
        self.probe_engine = probe_engine
        self.proxy_store = proxy_store

    def best_proxy(self) -> Optional[str]:
        # return proxy id with highest score; fall back to first enabled proxy
        scores = self.probe_engine.get_scores()
        proxies = self.proxy_store.list()
        if scores:
            best = max(scores.items(), key=lambda t: t[1])[0]
            return best
        for p in proxies:
            if p.enabled:
                return p.id
        return None


class Router:
    """Simple router that accepts client connections and forwards via chosen upstream proxy.

    - For HTTP requests (non-CONNECT): uses httpx AsyncClient with proxies set to upstream proxy.
    - For CONNECT: establishes CONNECT tunnel to upstream proxy and relays raw bytes.
    """

    def __init__(self, proxy_store: ProxyStore, probe_engine: ProbeEngine, listen_host: str = "0.0.0.0", listen_port: int = 10808):
        self.proxy_store = proxy_store
        self.probe_engine = probe_engine
        self.selector = ProxySelector(probe_engine, proxy_store)
        self.listen_host = listen_host
        self.listen_port = listen_port
        self._server: Optional[asyncio.AbstractServer] = None

    async def handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        peer = writer.get_extra_info('peername')
        logger.info("client connected %s", peer)
        try:
            # read first line to detect if CONNECT
            line = await reader.readline()
            if not line:
                writer.close()
                await writer.wait_closed()
                return
            first = line.decode('latin-1').strip()
            # read headers
            headers = b''
            while True:
                h = await reader.readline()
                if not h or h == b"\r\n" or h == b"\n":
                    break
                headers += h
            logger.debug("first line: %s", first)
            if first.upper().startswith('CONNECT'):
                # CONNECT host:port HTTP/1.1
                target = first.split(' ')[1]
                await self._handle_connect(target, reader, writer)
            else:
                # It's an HTTP request (e.g., GET http://example.com/ HTTP/1.1)
                # assemble request (first+headers+remaining body if any)
                # For simplicity, read any remaining body briefly (non-streaming)
                body = await reader.read(-1)
                request_bytes = (first + '\r\n').encode('latin-1') + headers + body
                await self._handle_http_request(request_bytes, writer)
        except Exception as e:
            logger.exception("error handling client: %s", e)
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def _handle_http_request(self, request_bytes: bytes, writer: asyncio.StreamWriter):
        # pick upstream proxy
        proxy_id = self.selector.best_proxy()
        if not proxy_id:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        proxy = self.proxy_store.get(proxy_id)
        if not proxy:
            writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            return
        proxy_url = f"http://{proxy.host}:{proxy.port}"
        # Use httpx to forward the request; we interpret request_bytes minimally
        async with httpx.AsyncClient(proxies={"http://": proxy_url, "https://": proxy_url}) as client:
            try:
                # httpx doesn't accept raw HTTP/1.1 bytes; reconstruct request minimally
                # parse the request line
                text = request_bytes.decode('latin-1', errors='ignore')
                lines = text.split('\r\n')
                req_line = lines[0]
                parts = req_line.split(' ')
                if len(parts) < 3:
                    raise ValueError('invalid request line')
                method, url, _ = parts
                # headers parse
                hdrs = {}
                i = 1
                while i < len(lines) and lines[i]:
                    h = lines[i]
                    if ':' in h:
                        k, v = h.split(':', 1)
                        hdrs[k.strip()] = v.strip()
                    i += 1
                # body after empty line
                body = '\r\n'.join(lines[i+1:]).encode('latin-1') if i+1 < len(lines) else None
                resp = await client.request(method, url, headers=hdrs, content=body, timeout=10)
                # write response back raw
                status_line = f"HTTP/1.1 {resp.status_code} {resp.reason_phrase}\r\n"
                writer.write(status_line.encode('latin-1'))
                for k, v in resp.headers.items():
                    writer.write(f"{k}: {v}\r\n".encode('latin-1'))
                writer.write(b"\r\n")
                writer.write(resp.content)
                await writer.drain()
            except Exception as e:
                logger.exception("forward error: %s", e)
                writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
                await writer.drain()
            finally:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception:
                    pass

    async def _handle_connect(self, target: str, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter):
        # choose upstream proxy
        proxy_id = self.selector.best_proxy()
        if not proxy_id:
            client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await client_writer.drain()
            client_writer.close()
            await client_writer.wait_closed()
            return
        proxy = self.proxy_store.get(proxy_id)
        if not proxy:
            client_writer.write(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 11\r\n\r\nBad Gateway")
            await client_writer.drain()
            client_writer.close()
            await client_writer.wait_closed()
            return
        # connect to upstream proxy
        try:
            upstream_reader, upstream_writer = await asyncio.open_connection(proxy.host, proxy.port)
            # send CONNECT command
            upstream_writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode('latin-1'))
            await upstream_writer.drain()
            # read status line and headers
            status = await upstream_reader.readline()
            if not status:
                raise RuntimeError('no response from upstream')
            status_text = status.decode('latin-1')
            if '200' not in status_text:
                # forward the status and close
                client_writer.write(status)
                # copy remaining header lines
                while True:
                    h = await upstream_reader.readline()
                    client_writer.write(h)
                    if not h or h in (b"\r\n", b"\n"):
                        break
                await client_writer.drain()
                client_writer.close()
                await client_writer.wait_closed()
                upstream_writer.close()
                await upstream_writer.wait_closed()
                return
            # send 200 to client
            client_writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await client_writer.drain()

            # tunnel data between client and upstream
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
                except Exception:
                    pass

            await asyncio.gather(pipe(client_reader, upstream_writer), pipe(upstream_reader, client_writer))
        except Exception as e:
            logger.exception("CONNECT forwarding failed: %s", e)
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
