import asyncio

import pytest

from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.config_schema import ProxyInfo


async def run_mock_proxy(host, port):
    async def handle(reader, writer):
        try:
            # read request line
            line = await reader.readline()
            if not line:
                writer.close()
                return
            first = line.decode('latin-1').strip()
            # read headers
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            if first.upper().startswith('CONNECT'):
                # respond OK for CONNECT
                writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await writer.drain()
                # For test simplicity, close
                await asyncio.sleep(0.1)
                writer.close()
                return
            else:
                # assume proxy-style GET: GET http://example.com/ HTTP/1.1
                body = b"proxied"
                writer.write(b"HTTP/1.1 200 OK\r\n")
                writer.write(f"Content-Length: {len(body)}\r\n".encode())
                writer.write(b"Content-Type: text/plain\r\n\r\n")
                writer.write(body)
                await writer.drain()
                writer.close()
        except Exception:
            try:
                writer.close()
            except Exception:
                pass

    server = await asyncio.start_server(handle, host=host, port=port)
    return server


@pytest.mark.asyncio
async def test_end_to_end_http_forwarding():
    host = '127.0.0.1'
    proxy_port = 31290
    router_port = 10808

    # start mock upstream proxy
    proxy_srv = await run_mock_proxy(host, proxy_port)

    # setup proxy store with mock proxy
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=host, port=proxy_port))

    # start router
    router = Router(proxy_store, listen_host=host, listen_port=router_port)
    await router.start()

    # connect as client to router and send a simple HTTP GET
    reader, writer = await asyncio.open_connection(host, router_port)
    req = 'GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n'
    writer.write(req.encode('latin-1'))
    await writer.drain()

    # read status line
    status = await reader.readline()
    assert b'200' in status
    # read headers
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
    # read body
    body = await reader.read()
    assert b'proxied' in body

    writer.close()
    await writer.wait_closed()

    await router.stop()
    proxy_srv.close()
    await proxy_srv.wait_closed()
