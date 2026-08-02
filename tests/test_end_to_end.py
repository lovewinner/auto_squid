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


async def run_mock_proxy_tagged(host, port, tag, pre_header_delay=0.0):
    """HTTP mock proxy returning a distinctive body `tag` (so the caller can
    tell which upstream won the race). Optionally sleeps `pre_header_delay`
    before sending response headers — a "slow but still returns headers"
    upstream that should LOSE the race yet, before the cache-poisoning fix,
    could still overwrite the domain→proxy cache.
    """
    async def handle(reader, writer):
        try:
            line = await reader.readline()
            if not line:
                writer.close()
                return
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            if pre_header_delay:
                await asyncio.sleep(pre_header_delay)
            body = tag.encode('latin-1')
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
async def test_meta_cache_holds_winner_pid():
    """竞速败者不应污染域名缓存:只有确认赢家写 _meta_cache。

    断言不变式:在 _try_http / _try_tunnel 内部只调 _record_attempt(统计),
    不调 _record_win_meta(meta);_record_win_meta 只在 _handle_http_request /
    _handle_connect 判定赢家后调一次。用一个 spy 包住 _record_win_meta 记录
    调用栈来源,跑一轮真实竞速(fast 无延迟 / slow 有延迟),断言:
      1) 至少一次竞速触发 _record_win_meta(赢家写了一次);
      2) 竞速中 _try_http 的调用栈不出现(败者没写 meta)。
    这直接锁定"meta 只由赢家写"的契约,不依赖竞速时序的非确定性。
    """
    import traceback

    fast_port = 31391
    slow_port = 31392
    fast_srv = await run_mock_proxy_tagged(HOST, fast_port, 'FAST', pre_header_delay=0.0)
    slow_srv = await run_mock_proxy_tagged(HOST, slow_port, 'SLOW', pre_header_delay=0.05)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
    proxy_store.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    db_path=tempfile.mktemp(suffix='.db'))

    # spy:记录每次 _record_win_meta 的调用栈,看它是否从 _try_http 内部被调。
    meta_calls = []
    orig = router._record_win_meta

    def spy(domain, pid):
        stack = ''.join(traceback.format_stack())
        meta_calls.append((pid, stack))
        return orig(domain, pid)

    router._record_win_meta = spy
    await router.start()
    try:
        domain = 'racepoison.example.com'
        for i in range(5):
            url = f"http://{domain}/p{i}".encode()
            body = await send_http_get(HOST, ROUTER_PORT, url=url)
            assert b'FAST' in body, f"round {i}: winner body should be FAST, got {body!r}"
        # 赢家路径应至少写过一次 meta。
        assert meta_calls, "expected the confirmed winner to write meta at least once"
        assert all(pid == 'fast' for pid, _ in meta_calls), (
            f"meta written by non-winner pid: {meta_calls}")
        # 关键契约:没有任何 _record_win_meta 调用源自 _try_http 内部
        # (败者只应调 _record_attempt,不碰 meta)。
        for pid, stack in meta_calls:
            assert '_try_http' not in stack, (
                f"_record_win_meta called from within _try_http (loser would "
                f"poison cache); call stack:\n{stack}")
    finally:
        router._record_win_meta = orig
        await router.stop()
        fast_srv.close()
        await fast_srv.wait_closed()
        slow_srv.close()
        await slow_srv.wait_closed()


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


# ── streaming / pooling / DB-batching tests ───────────────────────


async def run_chunked_proxy(host, port):
    """HTTP mock proxy that returns a body WITHOUT Content-Length, using
    HTTP/1.1 chunked transfer encoding — exercises the streaming path's
    chunked framing (upstream_cl is None → use_chunked)."""
    async def handle(reader, writer):
        try:
            line = await reader.readline()
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            # A body larger than STREAM_CACHE_LIMIT so caching is also skipped.
            payload = b"A" * (2 * 1024 * 1024)
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(b"Content-Type: application/octet-stream\r\n")
            writer.write(b"Transfer-Encoding: chunked\r\n\r\n")
            chunk_size = 65536
            for i in range(0, len(payload), chunk_size):
                piece = payload[i:i + chunk_size]
                writer.write(f"{len(piece):X}\r\n".encode())
                writer.write(piece)
                writer.write(b"\r\n")
                await writer.drain()
            writer.write(b"0\r\n\r\n")
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
async def test_stream_chunked_upstream():
    """A chunked upstream response (no Content-Length) must be re-framed
    correctly to the client and delivered byte-for-byte."""
    proxy_srv = await run_chunked_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        req = b"GET http://chunk.example.com/ HTTP/1.1\r\nHost: chunk.example.com\r\n\r\n"
        writer.write(req)
        await writer.drain()
        status = await reader.readline()
        assert b'200' in status
        saw_chunked = False
        cl = None
        while True:
            h = await reader.readline()
            if not h or h in (b"\r\n", b"\n"):
                break
            if h.lower().startswith(b'transfer-encoding:'):
                saw_chunked = True
            if h.lower().startswith(b'content-length:'):
                cl = h.split(b':', 1)[1].strip()
        assert saw_chunked, "client must receive chunked framing"
        assert cl is None, "chunked response must not also carry content-length"
        # Dechunk the body and verify length.
        body = bytearray()
        while True:
            size_line = await reader.readline()
            size = int(size_line.strip(), 16)
            if size == 0:
                # trailing CRLF
                await reader.readline()
                break
            body.extend(await reader.readexactly(size))
            await reader.readexactly(2)  # CRLF
        writer.close()
        await writer.wait_closed()
        assert len(body) == 2 * 1024 * 1024
        assert body == b"A" * (2 * 1024 * 1024)
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_stream_large_response_with_content_length():
    """A large upstream response with Content-Length must stream through
    byte-for-byte without being fully buffered server-side (content-length
    path)."""
    payload = b"B" * (3 * 1024 * 1024)

    async def handle(reader, writer):
        try:
            await reader.readline()
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(f"Content-Length: {len(payload)}\r\n".encode())
            writer.write(b"Content-Type: application/octet-stream\r\nConnection: close\r\n\r\n")
            for i in range(0, len(payload), 65536):
                writer.write(payload[i:i + 65536])
                await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    srv = await asyncio.start_server(handle, host=HOST, port=PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        writer.write(b"GET http://large.example.com/ HTTP/1.1\r\nHost: large.example.com\r\n\r\n")
        await writer.drain()
        assert b'200' in (await reader.readline())
        cl = 0
        while True:
            h = await reader.readline()
            if not h or h in (b"\r\n", b"\n"):
                break
            if h.lower().startswith(b'content-length:'):
                cl = int(h.split(b':', 1)[1].strip())
        assert cl == len(payload)
        body = await reader.readexactly(cl)
        writer.close()
        await writer.wait_closed()
        assert body == payload
    finally:
        await router.stop()
        srv.close()
        await srv.wait_closed()


@pytest.mark.asyncio
async def test_upstream_client_pool_reused():
    """Two sequential HTTP requests through the same upstream must reuse the
    pooled httpx.AsyncClient (no new client created for the second request).
    Verified by tracking new-connection counts on the mock proxy."""
    conns = []

    async def handle(reader, writer):
        conns.append(1)
        try:
            await reader.readline()
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            body = b"ok"
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(f"Content-Length: {len(body)}\r\n".encode())
            writer.write(b"Connection: keep-alive\r\n\r\n")
            writer.write(body)
            await writer.drain()
            # keep the connection open briefly so keep-alive is honored
            await asyncio.sleep(0.2)
            writer.close()
            await writer.wait_closed()
        except Exception:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    srv = await asyncio.start_server(handle, host=HOST, port=PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        await send_http_get(HOST, ROUTER_PORT, url=b"http://pool.example.com/a")
        await send_http_get(HOST, ROUTER_PORT, url=b"http://pool.example.com/b")
        # Pooled client must exist and be the single shared instance.
        assert len(router._client_pool) == 1, f"expected 1 pooled client, got {router._client_pool}"
        client = next(iter(router._client_pool.values()))
        assert not client.is_closed
    finally:
        await router.stop()
        srv.close()
        await srv.wait_closed()


@pytest.mark.asyncio
async def test_try_http_send_failure_does_not_mask_error():
    """When client.send raises before a streaming resp is assigned, _try_http
    must re-raise the original error (not a swallowed UnboundLocalError from
    `await resp.aclose()`) and leave the pooled client intact for reuse."""
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))

    class _Boom(Exception):
        pass

    # Pre-seed the pool with a client whose .send always fails. This exercises
    # the `except BaseException` branch of _try_http with resp still unassigned.
    pool_key = f"http://{HOST}:{PROXY_PORT}"

    class _FailingClient:
        is_closed = False

        def build_request(self, *a, **k):
            return object()  # request object; never actually used

        async def send(self, *a, **k):
            raise _Boom("send-time failure before resp assigned")

        async def aclose(self):
            self.is_closed = True

    router._client_pool[pool_key] = _FailingClient()
    try:
        with pytest.raises(_Boom):
            await router._try_http(
                'mock1', pool_key, 'GET', 'http://boom.example.com/', {}, None)
        # The failing client must remain in the pool, unclosed, for reuse.
        assert pool_key in router._client_pool
        assert not router._client_pool[pool_key].is_closed
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_db_batching_durability_across_restart():
    """Stats accumulated in memory must persist after stop() (final flush)
    and be reloadable by a fresh Router on the same db file."""
    db = tempfile.mktemp(suffix='.db')
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    cache_ttl=300, db_path=db)
    await router.start()
    try:
        await send_http_get(HOST, ROUTER_PORT, url=b"http://persist.example.com/p1")
        await send_http_get(HOST, ROUTER_PORT, url=b"http://persist.example.com/p2")
        # In-memory stats reflect two wins for mock1 on this domain.
        stats = router.get_domain_stats_from_db()
        assert stats.get('persist.example.com', {}).get('mock1') == 2
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()
    # New Router on the same DB must reload the persisted stats.
    proxy_store2 = ProxyStore()
    proxy_store2.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router2 = Router(proxy_store2, listen_host=HOST, listen_port=ROUTER_PORT,
                     cache_ttl=300, db_path=db)
    try:
        stats2 = router2.get_domain_stats_from_db()
        assert stats2.get('persist.example.com', {}).get('mock1') == 2, \
            f"stats not persisted across restart: {stats2}"
        meta2 = router2.get_domain_meta_from_db()
        assert meta2.get('persist.example.com', {}).get('default_proxy') == 'mock1'
    finally:
        await router2.stop()


@pytest.mark.asyncio
async def test_db_batching_background_flush():
    """A forced background flush (via _flush_to_db) must write in-memory
    stats to SQLite without waiting for stop()."""
    import sqlite3
    db = tempfile.mktemp(suffix='.db')
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    cache_ttl=300, db_path=db)
    await router.start()
    try:
        await send_http_get(HOST, ROUTER_PORT, url=b"http://flush.example.com/x")
        # Before flush, the on-disk table is empty (stats only in memory).
        with sqlite3.connect(db) as conn:
            rows = conn.execute("SELECT wins FROM domain_stats WHERE domain='flush.example.com'").fetchall()
        assert rows == [], f"expected no on-disk stats before flush, got {rows}"
        # Force a flush and verify it landed on disk.
        router._flush_to_db()
        with sqlite3.connect(db) as conn:
            rows = conn.execute("SELECT wins FROM domain_stats WHERE domain='flush.example.com'").fetchall()
        assert rows == [(1,)], f"expected on-disk win=1 after flush, got {rows}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


# ── server-side counter tests ────────────────────────────────────

@pytest.mark.asyncio
async def test_http_cache_counters_hit_and_miss():
    """响应缓存命中 +1 http_cache_hits;未命中 +1 http_cache_misses。

    同一 GET URL 两次:首次 miss(并因 200 缓存),第二次 hit。断言计数器差值。
    """
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        before = router.snapshot_counters()
        await send_http_get(HOST, ROUTER_PORT, url=b"http://cnt.example.com/")
        await send_http_get(HOST, ROUTER_PORT, url=b"http://cnt.example.com/")
        after = router.snapshot_counters()
        # 第二次必命中响应缓存 → hits>=1;首次 miss → misses>=1。
        assert after["http_cache_hits"] - before["http_cache_hits"] >= 1, \
            f"http_cache_hits not incremented: {before} -> {after}"
        assert after["http_cache_misses"] - before["http_cache_misses"] >= 1, \
            f"http_cache_misses not incremented: {before} -> {after}"
        # 命中后上游只被打了 1 次(第二次走缓存)。
        assert len(hit) == 1, f"upstream hit twice (cache should serve 2nd): {hit}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_upstream_attempts_and_racing_counters():
    """冷域名(无域名缓存)触发竞速 → racing_invocations + upstream_attempts 增长。

    用一个全新域名首请求:必走竞速(域名缓存空),单代理扇出 1 次。断言
    racing_invocations 与 upstream_attempts 都 +1。
    """
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=1, enable_http_cache=False,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        before = router.snapshot_counters()
        await send_http_get(HOST, ROUTER_PORT, url=b"http://race-cnt.example.com/p0")
        after = router.snapshot_counters()
        assert after["racing_invocations"] - before["racing_invocations"] >= 1, \
            f"racing_invocations not incremented: {before} -> {after}"
        assert after["upstream_attempts"] - before["upstream_attempts"] >= 1, \
            f"upstream_attempts not incremented: {before} -> {after}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_domain_cache_hit_counter():
    """域名缓存命中(同域名第二次请求单发)→ domain_cache_hits +1。

    enable_http_cache=False 避免响应缓存抢先命中,确保走域名缓存单发路径。
    同域名两次请求:首次竞速建立域名缓存,第二次单发命中。
    """
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    cache_ttl=300, max_retries=1, enable_http_cache=False,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        before = router.snapshot_counters()
        # 同域名两次:首次竞速(写域名缓存),第二次域名缓存命中单发。
        await send_http_get(HOST, ROUTER_PORT, url=b"http://dom-cnt.example.com/a")
        await send_http_get(HOST, ROUTER_PORT, url=b"http://dom-cnt.example.com/b")
        after = router.snapshot_counters()
        assert after["domain_cache_hits"] - before["domain_cache_hits"] >= 1, \
            f"domain_cache_hits not incremented: {before} -> {after}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_http_cache_invalidated_by_write_method():
    """写方法(POST/PUT/DELETE/PATCH)必须失效该域名的 GET 响应缓存。

    覆盖两点:(a) 同 URL 失效——POST 后再 GET 同 URL 应回源;(b) 跨 URL 同域名
    失效——这是真实场景的核心:添加动作常打 POST /api/items,而刷新的列表页是
    GET /,两者 URL 不同。按 URL 精确失效会漏掉列表页,导致刷新仍返回旧内容;
    故失效必须按域名整域清空。
    """
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        list_url = b"http://write-invalidate.example.com/"          # GET 列表页
        api_url = b"http://write-invalidate.example.com/api/items"  # POST 添加

        # (a) 同 URL 失效:GET 填缓存 → POST 失效 → GET 应回源。
        await send_http_get(HOST, ROUTER_PORT, url=list_url)   # 上游 1(回源+缓存)
        await send_http_get(HOST, ROUTER_PORT, url=list_url)   # 命中缓存,上游仍 1
        assert len(hit) == 1, "second GET should have hit the response cache"

        await send_http_post(HOST, ROUTER_PORT, api_url, b'{"add":"item"}')  # 上游 2
        assert len(hit) == 2, "POST should have been forwarded upstream"
        assert router.snapshot_counters()["http_cache_entries"] == 0, \
            "POST must have invalidated the GET cache entry for its domain"

        await send_http_get(HOST, ROUTER_PORT, url=list_url)  # 缓存已失效,上游 3
        assert len(hit) == 3, \
            f"GET after POST should re-hit upstream, got hits={len(hit)} (stale cache served)"

        # (b) 跨 URL 同域名失效:用另一个列表页 /list 填缓存(URL 与 /api/items 不同),
        #     再 POST /api/items(又是一个不同 URL),应清掉整域缓存 → 再 GET /list 应回源。
        list_url_b = b"http://write-invalidate.example.com/list"  # 与 api_url 不同的 URL
        await send_http_get(HOST, ROUTER_PORT, url=list_url_b)   # 上游 4(回源+缓存)
        assert len(hit) == 4, f"expected 4 upstream hits, got {len(hit)}"
        await send_http_post(HOST, ROUTER_PORT, api_url, b'{"delete":5}')  # 上游 5,且应失效整域
        assert len(hit) == 5
        await send_http_get(HOST, ROUTER_PORT, url=list_url_b)   # 整域已失效,上游 6
        assert len(hit) == 6, \
            "GET list page after POST to a different URL must re-hit upstream " \
            f"(domain-wide invalidation), got hits={len(hit)}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_thundering_herd_avoided():
    """并发 GET 同一 URL 应去重聚合,上游只命中 1 次(Cache Stampede Protection)。

    无聚合时,N 个并发 GET 到同一未缓存 URL 会全部触发上游竞速(放大 N 倍)。
    引入 _inflight_futures 后,首个请求在途时其余请求 await 其 Future,复用结果,
    上游只被命中 1 次。断言:10 个并发 GET 全部成功返回相同 body,且上游仅命中 1。
    """
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        url = b"http://herd.example.com/api/items"

        async def one_get():
            return await send_http_get(HOST, ROUTER_PORT, url=url)

        # 10 个并发 GET 同一 URL。
        bodies = await asyncio.gather(*[one_get() for _ in range(10)])
        assert all(b == b"proxied" for b in bodies), "all responses should be identical"
        assert len(hit) == 1, \
            f"thundering herd: 10 concurrent GETs should hit upstream once, got {len(hit)}"
        # 聚合后无缓存命中(首个请求写缓存,后续 waitee 直接走 Future,不计 hit)。
        counters = router.snapshot_counters()
        assert counters["http_cache_entries"] == 1, \
            f"first GET should have cached the response, entries={counters['http_cache_entries']}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_coalescing_timeout_falls_back():
    """聚合等待超时(慢上游)时,waiter 应放弃聚合、自行竞速,不悬挂。

    用延迟 0.5s 的慢上游 mock:首个 GET 在途时,后续并发 GET await Future。
    若无超时保护,waiter 会无限等待首个请求(慢)完成 → 连接堆积(fd 暴涨)。
    加固后 waiter 在 _AGG_WAIT_TIMEOUT(0.2s)后放弃聚合、自行竞速。
    断言:
    1. 所有并发请求都在有限时间内完成(无悬挂)——整体 < 2s。
    2. 上游被命中 >1 次(首个请求 + 放弃聚合的 waiter 各自竞速)。

    注意用 2 并发而非更大:慢上游下 httpx 连接池对同一 client 的 HTTP/1.1
    连接不允许并发复用(首个请求占连接 0.5s 期间,后续请求复用会 ReadError),
    这是连接池的既有行为,与聚合超时无关,故用 2 并发避开。
    """
    proxy_srv = await run_mock_proxy_tagged(HOST, PROXY_PORT, tag='slow',
                                            pre_header_delay=0.5)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        url = b"http://slow-herd.example.com/api/items"

        async def one_get():
            return await send_http_get(HOST, ROUTER_PORT, url=url)

        import time
        t0 = time.monotonic()
        bodies = await asyncio.wait_for(asyncio.gather(*[one_get() for _ in range(2)]),
                                        timeout=3.0)
        elapsed = time.monotonic() - t0
        # 全部成功且 body 正确(慢 mock 返回 'slow')。
        assert all(b == b"slow" for b in bodies), f"all responses should be 'slow', got {bodies}"
        # 无悬挂:首个 0.5s,waiter 0.2s 超时后竞速又 0.5s,最坏 ~1.2s;远小于 3s。
        assert elapsed < 2.0, f"requests hung: took {elapsed:.2f}s"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()
