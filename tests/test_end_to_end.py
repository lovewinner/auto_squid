import asyncio
import base64
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.config_schema import ProxyInfo
from auto_squid.auth import check_auth
from auto_squid.api import app as api_app, mount

# ── test ports ───────────────────────────────────────────────────
PROXY_PORT = 31291
ROUTER_PORT = 10809
LOCAL_HTTP_PORT = 18081
HOST = '127.0.0.1'


# ── mock servers ──────────────────────────────────────────────────

async def run_mock_proxy(host, port, hit_counter=None):
    """HTTP/CONNECT mock proxy. For CONNECT, echoes data back."""
    async def handle(reader, writer):
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                return
            first = line.decode('latin-1').strip()
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            if first.upper().startswith('CONNECT'):
                writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await writer.drain()
                if hit_counter is not None:
                    hit_counter.append(1)
                try:
                    while True:
                        data = await asyncio.wait_for(reader.read(4096), timeout=5)
                        if not data:
                            break
                        writer.write(data)
                        await writer.drain()
                except (asyncio.TimeoutError, Exception):
                    pass
                writer.close()
            else:
                body = b"proxied"
                writer.write(b"HTTP/1.1 200 OK\r\n")
                writer.write(f"Content-Length: {len(body)}\r\n".encode())
                writer.write(b"Content-Type: text/plain\r\n\r\n")
                writer.write(body)
                await writer.drain()
                if hit_counter is not None:
                    hit_counter.append(1)
                writer.close()
        except Exception:
            try:
                writer.close()
            except Exception:
                pass
    server = await asyncio.start_server(handle, host=host, port=port)
    return server


async def run_local_http_server(host, port):
    """Simple HTTP server that returns 'local-response'."""
    async def handle(reader, writer):
        try:
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            body = b"local-response"
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


# ── helpers ───────────────────────────────────────────────────────

async def send_http_get(host, port, url=b"http://example.com/"):
    reader, writer = await asyncio.open_connection(host, port)
    req = b"GET " + url + b" HTTP/1.1\r\nHost: example.com\r\n\r\n"
    writer.write(req)
    await writer.drain()
    status = await reader.readline()
    assert b'200' in status, f"expected 200, got {status}"
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
    body = await reader.read()
    writer.close()
    await writer.wait_closed()
    return body


async def send_connect(host, port, target=b"example.com:443", payload=b"hello"):
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"CONNECT " + target + b" HTTP/1.1\r\nHost: " + target + b"\r\n\r\n")
    await writer.drain()
    status = await reader.readline()
    assert b'200' in status, f"expected 200, got {status}"
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
    writer.write(payload)
    await writer.drain()
    echo = await asyncio.wait_for(reader.read(len(payload)), timeout=5)
    writer.close()
    await writer.wait_closed()
    return echo


def _basic_auth_header(userpass: str | None) -> bytes:
    """Build a `Proxy-Authorization: Basic ...` header line (without trailing CRLF)."""
    if userpass is None:
        return b''
    token = base64.b64encode(userpass.encode()).decode()
    return f"Proxy-Authorization: Basic {token}\r\n".encode()


async def send_http_get_status(host, port, url=b"http://example.com/", userpass=None):
    """Like send_http_get but returns the raw status line and does not assert 200.
    Injects a Proxy-Authorization header when userpass is provided."""
    reader, writer = await asyncio.open_connection(host, port)
    auth = _basic_auth_header(userpass)
    req = b"GET " + url + b" HTTP/1.1\r\nHost: example.com\r\n" + auth + b"\r\n"
    writer.write(req)
    await writer.drain()
    status = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return status


async def send_connect_status(host, port, target=b"example.com:443", userpass=None):
    """Like send_connect but returns the raw status line and does not assert 200."""
    reader, writer = await asyncio.open_connection(host, port)
    auth = _basic_auth_header(userpass)
    writer.write(b"CONNECT " + target + b" HTTP/1.1\r\nHost: " + target + b"\r\n" + auth + b"\r\n")
    await writer.drain()
    status = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return status


# ── tests ─────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_end_to_end_http_forwarding():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        body = await send_http_get(HOST, ROUTER_PORT)
        assert b'proxied' in body
        assert len(hit) == 1
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_end_to_end_connect_forwarding():
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        echo = await send_connect(HOST, ROUTER_PORT, payload=b"conn-echo-test")
        assert echo == b"conn-echo-test"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_http_cache():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        body1 = await send_http_get(HOST, ROUTER_PORT, url=b"http://cachetest.example.com/")
        assert b'proxied' in body1
        proxy_hits_1 = len(hit)
        body2 = await send_http_get(HOST, ROUTER_PORT, url=b"http://cachetest.example.com/")
        assert b'proxied' in body2
        assert len(hit) == proxy_hits_1, "HTTP cache should have served the second request"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_domain_cache():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, cache_ttl=300, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        body1 = await send_http_get(HOST, ROUTER_PORT, url=b"http://domaincache.example.com/path1")
        assert b'proxied' in body1

        meta = router.get_domain_meta_from_db()
        assert 'domaincache.example.com' in meta
        cached_pid = meta['domaincache.example.com']['default_proxy']
        assert cached_pid == 'mock1'

        body2 = await send_http_get(HOST, ROUTER_PORT, url=b"http://domaincache.example.com/path2")
        assert b'proxied' in body2

        assert router.request_counts.get('mock1', 0) == 2
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_racing_http():
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    proxy_store = ProxyStore()
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, enable_local_racing=True, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        body = await send_http_get(HOST, ROUTER_PORT, url=f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode())
        assert b'local-response' in body
        assert router.request_counts.get('local', 0) > 0
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


# ── client auth tests ─────────────────────────────────────────────

def _authed_router(user='user', pw='pass'):
    """A Router with client auth enabled, backed by the mock proxy."""
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    return Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                  db_path=tempfile.mktemp(suffix='.db'),
                  auth_enabled=True, auth_username=user, auth_password=pw)


@pytest.mark.asyncio
async def test_auth_rejects_missing():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    router = _authed_router()
    await router.start()
    try:
        status = await send_http_get_status(HOST, ROUTER_PORT)
        assert b'407' in status, f"expected 407, got {status}"
        assert len(hit) == 0, "no upstream call should happen on auth failure"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_auth_accepts_valid():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    router = _authed_router()
    await router.start()
    try:
        status = await send_http_get_status(HOST, ROUTER_PORT, userpass='user:pass')
        assert b'200' in status, f"expected 200, got {status}"
        assert len(hit) == 1
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_auth_rejects_wrong():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    router = _authed_router()
    await router.start()
    try:
        status = await send_http_get_status(HOST, ROUTER_PORT, userpass='user:wrong')
        assert b'407' in status, f"expected 407, got {status}"
        assert len(hit) == 0
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_auth_connect():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    router = _authed_router()
    await router.start()
    try:
        # no creds → 407
        status = await send_connect_status(HOST, ROUTER_PORT)
        assert b'407' in status, f"expected 407, got {status}"
        assert len(hit) == 0
        # valid creds → 200 Connection established
        status = await send_connect_status(HOST, ROUTER_PORT, userpass='user:pass')
        assert b'200' in status, f"expected 200, got {status}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_auth_disabled_allows_all():
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        status = await send_http_get_status(HOST, ROUTER_PORT)  # no creds
        assert b'200' in status, f"expected 200 with auth disabled, got {status}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


async def run_header_echo_proxy(host, port):
    """HTTP mock proxy that echoes the request headers it received as the body,
    so a test can assert which headers were forwarded upstream."""
    async def handle(reader, writer):
        try:
            line = await reader.readline()
            echoed = line
            while True:
                h = await reader.readline()
                echoed += h
                if not h or h in (b"\r\n", b"\n"):
                    break
            # drain any body via Content-Length if present
            cl = 0
            for hl in echoed.split(b'\r\n'):
                if hl.lower().startswith(b'content-length:'):
                    cl = int(hl.split(b':', 1)[1].strip())
            if cl:
                await reader.readexactly(cl)
            body = echoed
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(f"Content-Length: {len(body)}\r\n".encode())
            writer.write(b"Content-Type: text/plain\r\nConnection: close\r\n\r\n")
            writer.write(body)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    server = await asyncio.start_server(handle, host=host, port=port)
    return server


@pytest.mark.asyncio
async def test_hop_by_hop_request_headers_not_forwarded():
    """Regression: the client's Proxy-Authorization (and other hop-by-hop
    request headers) must NOT be forwarded to the upstream proxy. Forwarding
    it caused upstream Squid to return 407 ERR_CACHE_ACCESS_DENIED."""
    proxy_srv = await run_header_echo_proxy(HOST, PROXY_PORT)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    auth_enabled=True, auth_username='asuser', auth_password='s3cretRRxc68a',
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        tok = base64.b64encode(b'asuser:s3cretRRxc68a').decode()
        req = (f"GET http://hop.test.example.com/ HTTP/1.1\r\n"
               f"Host: hop.test.example.com\r\n"
               f"Proxy-Authorization: Basic {tok}\r\n"
               f"Connection: keep-alive\r\n"
               f"X-Custom: keepme\r\n\r\n").encode()
        writer.write(req)
        await writer.drain()
        status = await reader.readline()
        assert b'200' in status, f"expected 200, got {status}"
        cl = 0
        while True:
            h = await reader.readline()
            if not h or h in (b"\r\n", b"\n"):
                break
            if h.lower().startswith(b'content-length:'):
                cl = int(h.split(b':', 1)[1].strip())
        echoed = await reader.readexactly(cl) if cl else b''
        writer.close()
        await writer.wait_closed()

        # the non-hop-by-hop custom header must survive
        assert b'x-custom: keepme' in echoed.lower(), f"custom header dropped: {echoed!r}"
        # hop-by-hop headers the client sent to OUR proxy must NOT reach the upstream
        assert b'proxy-authorization:' not in echoed.lower(), \
            f"Proxy-Authorization leaked to upstream: {echoed!r}"
        assert b'proxy-connection:' not in echoed.lower()
        # the request line + Host should still be present
        assert b'GET http://hop.test.example.com/' in echoed
        assert b'host: hop.test.example.com' in echoed.lower()
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


class TestCheckAuth:
    def test_disabled_allows(self):
        assert check_auth({}, False, 'u', 'p') == (True, None)

    def test_missing_header(self):
        assert check_auth({}, True, 'u', 'p')[0] is False

    def test_valid_credentials(self):
        token = base64.b64encode(b'u:p').decode()
        assert check_auth({'Proxy-Authorization': f'Basic {token}'}, True, 'u', 'p') == (True, None)

    def test_authorization_fallback(self):
        token = base64.b64encode(b'u:p').decode()
        assert check_auth({'Authorization': f'Basic {token}'}, True, 'u', 'p') == (True, None)

    def test_wrong_password(self):
        token = base64.b64encode(b'u:wrong').decode()
        assert check_auth({'Proxy-Authorization': f'Basic {token}'}, True, 'u', 'p')[0] is False

    def test_unsupported_scheme(self):
        assert check_auth({'Proxy-Authorization': 'Bearer xyz'}, True, 'u', 'p')[0] is False

    def test_malformed_header(self):
        assert check_auth({'Proxy-Authorization': 'Basic not-base64!!'}, True, 'u', 'p')[0] is False


async def run_echo_proxy(host, port):
    """HTTP mock proxy that echoes the request body back verbatim (binary-safe)."""
    async def handle(reader, writer):
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                await writer.wait_closed()
                return
            cl = 0
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
                if h.lower().startswith(b'content-length:'):
                    cl = int(h.split(b':', 1)[1].strip())
            body = await reader.readexactly(cl) if cl > 0 else b''
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(f"Content-Length: {len(body)}\r\n".encode())
            writer.write(b"Content-Type: application/octet-stream\r\nConnection: close\r\n\r\n")
            writer.write(body)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    server = await asyncio.start_server(handle, host=host, port=port)
    return server


async def send_http_post(host, port, url, body):
    reader, writer = await asyncio.open_connection(host, port)
    req = b"POST " + url + b" HTTP/1.1\r\nHost: example.com\r\n"
    req += b"Content-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body
    writer.write(req)
    await writer.drain()
    status = await reader.readline()
    assert b'200' in status, f"expected 200, got {status}"
    resp_cl = 0
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
        if h.lower().startswith(b'content-length:'):
            resp_cl = int(h.split(b':', 1)[1].strip())
    resp_body = await reader.readexactly(resp_cl) if resp_cl > 0 else b''
    writer.close()
    await writer.wait_closed()
    return resp_body


@pytest.mark.asyncio
async def test_binary_body_preserved():
    """Regression for #3: the request body must survive the router's header
    parsing byte-for-byte, including all 256 byte values and an embedded
    blank line (CRLFCRLF) which historically broke line-split body re-derivation."""
    echo_srv = await run_echo_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        for payload in (bytes(range(256)), b'part1\r\n\r\npart2', b'', b'\r\n', b'X' * 4096):
            resp = await send_http_post(HOST, ROUTER_PORT, b"http://bintest.example.com/", payload)
            assert resp == payload, f"body corrupted for payload {payload!r}: got {resp!r}"
    finally:
        await router.stop()
        echo_srv.close()
        await echo_srv.wait_closed()


# ── unit tests ────────────────────────────────────────────────────

class TestProxyStore:
    def test_add_get_list_remove(self):
        ps = ProxyStore()
        p = ProxyInfo(id='p1', host='1.2.3.4', port=8080)
        ps.add(p)
        assert ps.get('p1') is p
        assert len(ps.list()) == 1
        assert ps.remove('p1') is p
        assert ps.get('p1') is None
        assert len(ps.list()) == 0

    def test_remove_nonexistent(self):
        ps = ProxyStore()
        assert ps.remove('nonexistent') is None

    def test_save_and_load(self):
        with tempfile.NamedTemporaryFile(suffix='.yaml', mode='w', delete=False) as f:
            f.write('')
            tmppath = f.name
        try:
            ps = ProxyStore()
            ps.add(ProxyInfo(id='saver', host='5.6.7.8', port=3128))
            ps.save(tmppath)
            loaded = ProxyStore(tmppath)
            p = loaded.get('saver')
            assert p is not None
            assert p.host == '5.6.7.8'
            assert p.port == 3128
        finally:
            Path(tmppath).unlink(missing_ok=True)


class TestAPI:
    def test_health(self):
        client = TestClient(api_app)
        r = client.get("/health")
        assert r.status_code == 200
        assert r.json() == {"status": "ok"}

    def test_config_without_router(self):
        client = TestClient(api_app)
        r = client.get("/config")
        assert r.status_code == 200
        assert r.json() == {"enable_local_racing": False}

    def test_stats_with_mounted_router(self):
        proxy_store = ProxyStore()
        router = Router(proxy_store, db_path=tempfile.mktemp(suffix='.db'))
        mount(proxy_store, router)
        client = TestClient(api_app)
        r = client.get("/stats")
        assert r.status_code == 200
        data = r.json()
        assert "request_counts" in data
        assert "attempted_counts" in data
