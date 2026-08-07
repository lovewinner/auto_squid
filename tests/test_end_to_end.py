import asyncio
import base64
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router, ProxySelector
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


async def run_mock_proxy_status(host, port, status=500, pre_header_delay=0.0):
    """HTTP mock proxy returning a fixed status with an empty body.

    Used to build an upstream that "works" (connects) but answers HTTP 5xx —
    the target of the A2 eviction-on-5xx semantics. `pre_header_delay` lets the
    caller make it lose races deterministically.
    """
    reason = {500: 'Internal Server Error', 502: 'Bad Gateway'}.get(status, 'Error')
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
            body = b""
            writer.write(f"HTTP/1.1 {status} {reason}\r\n".encode())
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

    def test_add_proxy_preserves_enabled_and_auth(self):
        """POST /proxies 的 enabled/auth 字段不得被丢弃(ProxyIn 与 ProxyInfo 对齐)。

        曾回归:ProxyIn 只含 id/name/host/port/protocol,经 ProxyInfo(**model_dump())
        添加时 enabled(禁用)与 auth(上游认证)被 pydantic 静默丢弃——无法通过
        管理 API 添加禁用/带认证的代理。本测试锁定:POST 后 store 里的 ProxyInfo
        保留这两个字段,GET 亦能回读。
        """
        proxy_store = ProxyStore()
        mount(proxy_store, None)
        client = TestClient(api_app)
        payload = {
            "id": "auth-proxy",
            "host": "203.0.113.5",
            "port": 6128,
            "protocol": "http",
            "enabled": False,
            "auth": {"username": "up", "password": "secret123"},
        }
        r = client.post("/proxies", json=payload)
        assert r.status_code == 200, r.text
        stored = proxy_store.get("auth-proxy")
        assert stored is not None
        assert stored.enabled is False, f"enabled should persist, got {stored.enabled}"
        assert stored.auth == {"username": "up", "password": "secret123"}, stored.auth
        # GET /proxies 应能回读完整字段。
        r2 = client.get("/proxies")
        assert r2.status_code == 200
        items = {p["id"]: p for p in r2.json()}
        assert items["auth-proxy"]["enabled"] is False
        assert items["auth-proxy"]["auth"] == {"username": "up", "password": "secret123"}
        assert items["auth-proxy"]["port"] == 6128


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


# ── session stickiness tests ─────────────────────────────────────

@pytest.mark.asyncio
async def test_stickiness_unit_hit_and_eviction():
    """粘性表基本语义:存取、TTL 过期驱逐、代理失效驱逐、_prune_sticky 清扫。

    单元级直测 _record_sticky / _get_sticky_proxy / _prune_sticky,不依赖
    网络时序。键为 '客户端IP|域名',纯内存,滑动 TTL。
    """
    ps = ProxyStore()
    ps.add(ProxyInfo(id='p1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    try:
        # 无记录 → 未命中。
        assert router._get_sticky_proxy('1.2.3.4', 'example.com') is None
        # 记录后可命中。
        router._record_sticky('1.2.3.4', 'example.com', 'p1')
        assert router._get_sticky_proxy('1.2.3.4', 'example.com') == 'p1'
        # 键含客户端与域名,不同客户端/域名不互相污染。
        assert router._get_sticky_proxy('1.2.3.5', 'example.com') is None
        assert router._get_sticky_proxy('1.2.3.4', 'other.com') is None
        assert '1.2.3.4|example.com' in router.get_sticky_cache()
        # TTL 过期 → 未命中并驱逐条目。
        router._sticky_cache['1.2.3.4|example.com']['updated_at'] = '2000-01-01T00:00:00+00:00'
        assert router._get_sticky_proxy('1.2.3.4', 'example.com') is None
        assert '1.2.3.4|example.com' not in router._sticky_cache
        # 指向不存在代理的条目 → 取用即失效并驱逐。
        router._record_sticky('1.2.3.4', 'example.com', 'gone')
        assert router._get_sticky_proxy('1.2.3.4', 'example.com') is None
        assert '1.2.3.4|example.com' not in router._sticky_cache
        # 记录到已删除代理的条目应被 _prune_sticky 清扫。
        router._record_sticky('9.9.9.9', 'example.com', 'gone')
        router._prune_sticky()
        assert '9.9.9.9|example.com' not in router._sticky_cache
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_stickiness_disabled_ignores_records():
    """stickiness_enabled=False 时:记录为空操作,查询恒 None(默认行为不变)。"""
    ps = ProxyStore()
    ps.add(ProxyInfo(id='p1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    stickiness_enabled=False, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    try:
        router._record_sticky('1.2.3.4', 'example.com', 'p1')
        assert router._sticky_cache == {}
        assert router._get_sticky_proxy('1.2.3.4', 'example.com') is None
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_session_stickiness_reuses_proxy():
    """同一客户端+域名:首次竞速后,后续请求应命中粘性代理单发(不竞速)。

    fast 无延迟、slow 延迟 0.05s:首次竞速 fast 必然胜出并写入粘性表
    (127.0.0.1|domain → fast)。第二次请求走粘性单发,不再触发竞速——
    slow 作为竞速候选不会被再次尝试(request_counts['slow'] 恒为 0)。
    """
    fast_port, slow_port = 31401, 31402
    fast_srv = await run_mock_proxy_tagged(HOST, fast_port, 'FAST', pre_header_delay=0.0)
    slow_srv = await run_mock_proxy_tagged(HOST, slow_port, 'SLOW', pre_header_delay=0.05)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
    ps.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        domain = 'stickiness.example.com'
        url = f"http://{domain}/p0".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body, f"first request should win via fast, got {body!r}"
        # 首次竞速 fast 胜出 → 已写粘性表。
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'fast'
        # 第二次请求:粘性命中 → 单发 fast,不竞速。
        body2 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body2
        counters = router.snapshot_counters()
        assert counters['sticky_cache_hits'] >= 1, "second request should be a sticky hit"
        # 粘性单发跳过竞速:slow 从未作为赢家被记录。
        assert router.request_counts.get('slow', 0) == 0, \
            f"sticky single-send should skip racing; request_counts={router.request_counts}"
    finally:
        await router.stop()
        fast_srv.close()
        await fast_srv.wait_closed()
        slow_srv.close()
        await slow_srv.wait_closed()


@pytest.mark.asyncio
async def test_stickiness_redispatch_on_proxy_failure():
    """粘性代理失败 → 驱逐该条目并回落竞速,竞速赢家回填粘性表(redispatch)。

    fast 无延迟、slow 延迟 0.05s:首次竞速 fast 胜出并粘住。停掉 fast 上游后,
    再次请求 → 粘性单发失败 → 驱逐 → 回落域名缓存(也指向 fast,失败) → 竞速
    → slow 胜出并回填粘性表;第三次请求直接粘到 slow。
    """
    fast_port, slow_port = 31411, 31412
    fast_srv = await run_mock_proxy_tagged(HOST, fast_port, 'FAST', pre_header_delay=0.0)
    slow_srv = await run_mock_proxy_tagged(HOST, slow_port, 'SLOW', pre_header_delay=0.05)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
    ps.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        domain = 'sticky-redispatch.example.com'
        url = f"http://{domain}/p0".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'fast'
        # 停掉 fast 上游 → 粘性单发失败 → redispatch 到 slow,并回填粘性表。
        fast_srv.close()
        await fast_srv.wait_closed()
        body2 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'SLOW' in body2, f"redispatch should land on slow, got {body2!r}"
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'slow', \
            "racing winner must repopulate the sticky table"
        # 第三次请求:已粘到 slow,直接单发不再竞速。
        body3 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'SLOW' in body3
    finally:
        await router.stop()
        fast_srv.close()
        await fast_srv.wait_closed()
        slow_srv.close()
        await slow_srv.wait_closed()


@pytest.mark.asyncio
async def test_stickiness_connect_reuses_proxy():
    """CONNECT 路径:同一客户端+target 复用粘性代理,sticky_cache_hits 累计。"""
    fast_port, slow_port = 31421, 31422
    fast_srv = await run_mock_proxy(HOST, fast_port, hit_counter=None)
    slow_srv = await run_mock_proxy(HOST, slow_port, hit_counter=None)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
    ps.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        target = b"sticky-conn.example.com:443"
        echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"a")
        assert echo == b"a"
        # 首次 CONNECT 竞速胜者(非确定性,fast/slow 均无延迟)写入粘性表。
        sticky_pid = router._get_sticky_proxy('127.0.0.1', 'sticky-conn.example.com:443')
        assert sticky_pid in ('fast', 'slow'), f"unexpected sticky pid {sticky_pid!r}"
        # 第二次 CONNECT:粘性命中(命中哪台取决于首次竞速胜者,这里直接看计数)。
        echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"b")
        assert echo2 == b"b"
        assert router.snapshot_counters()['sticky_cache_hits'] >= 1, \
            "second CONNECT should be a sticky hit"
        # 粘性表反映同一胜者(redispatch 未触发,两次 CONNECT 走同一代理)。
        assert router._get_sticky_proxy('127.0.0.1', 'sticky-conn.example.com:443') == sticky_pid
    finally:
        await router.stop()
        fast_srv.close()
        await fast_srv.wait_closed()
        slow_srv.close()
        await slow_srv.wait_closed()


@pytest.mark.asyncio
async def test_stickiness_repopulated_from_domain_cache():
    """粘性被驱逐后,若域名缓存仍有效,单发成功应回填粘性表。

    场景:首请求竞速 fast 胜出 → 粘性与域名缓存都指向 fast。手动清空粘性
    (模拟上轮 redispatch 驱逐),域名缓存仍指向 fast(有效)。再次请求应走
    域名缓存单发成功,并把粘性表回填为 fast——否则该客户端会一直丢粘性
    直到域名缓存过期(cache_ttl 默认 600s)。
    """
    fast_port, slow_port = 31431, 31432
    fast_srv = await run_mock_proxy_tagged(HOST, fast_port, 'FAST', pre_header_delay=0.0)
    slow_srv = await run_mock_proxy_tagged(HOST, slow_port, 'SLOW', pre_header_delay=0.05)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
    ps.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        domain = 'sticky-refill.example.com'
        url = f"http://{domain}/p0".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body
        # 竞速赢家同时回填粘性与域名缓存。
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'fast'
        assert router._get_fresh_proxy(domain) == 'fast'
        # 模拟粘性被驱逐(redispatch 场景),但域名缓存仍有效。
        router._evict_sticky('127.0.0.1', domain)
        assert router._get_sticky_proxy('127.0.0.1', domain) is None
        # 再次请求:域名缓存单发成功,应把粘性表回填为 fast。
        body2 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body2
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'fast', \
            "domain-cache single-send must repopulate the sticky table"
    finally:
        await router.stop()
        fast_srv.close()
        await fast_srv.wait_closed()
        slow_srv.close()
        await slow_srv.wait_closed()


@pytest.mark.asyncio
async def test_stickiness_local_racing_stays_sticky():
    """A1:本机竞速胜者('local')应能粘住,不被每次查询当失效代理驱逐。

    空 ProxyStore + enable_local_racing:竞速只有 local 候选。首次请求 local
    胜出并写入粘性表;第二次请求应粘性命中单发(直接连),'local' 条目不因
    proxy_store.get('local') 返回 None 而被驱逐。修复前每次查询即驱逐,
    sticky_cache_hits 恒为 0、每次请求都重新竞速。
    """
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    ps = ProxyStore()
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    enable_local_racing=True, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'local-response' in body
        # 首次竞速 local 胜出 → 粘性表写入 'local'(domain = urlparse 的 hostname)。
        assert router._get_sticky_proxy('127.0.0.1', HOST) == 'local'
        # 第二次请求:粘性命中 → 不再竞速,且 'local' 条目不因校验失败被驱逐。
        body2 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'local-response' in body2
        assert router._get_sticky_proxy('127.0.0.1', HOST) == 'local', \
            "local sticky entry must survive a sticky hit"
        counters = router.snapshot_counters()
        assert counters['sticky_cache_hits'] >= 1, "second request should be a sticky hit"
        assert counters['sticky_cache_size'] == 1
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


@pytest.mark.asyncio
async def test_stickiness_evicts_on_5xx():
    """A2:粘性代理返回 HTTP 5xx → 驱逐该条目(不回填),下一请求竞速换新。

    预置粘性指向返回 500 的坏代理:本请求把 500 原样转发给客户端(已流式发出
    不可重试),同时驱逐该条目;下一请求无粘性 → 竞速 ok 胜出并回填。断言
    sticky_evictions 计数、粘性表换新为 ok。
    """
    bad_port, ok_port = 31441, 31442
    bad_srv = await run_mock_proxy_status(HOST, bad_port, status=500, pre_header_delay=0.05)
    ok_srv = await run_mock_proxy_tagged(HOST, ok_port, 'OK', pre_header_delay=0.0)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='bad', host=HOST, port=bad_port))
    ps.add(ProxyInfo(id='ok', host=HOST, port=ok_port))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        domain = '5xx-sticky.example.com'
        url = f"http://{domain}/p0".encode()
        # 预置:该客户端+域名粘到返回 500 的坏代理。
        router._record_sticky('127.0.0.1', domain, 'bad')
        # 本请求:粘性单发拿到 500 → 原样回给客户端,同时驱逐粘性条目。
        st1 = await send_http_get_status(HOST, ROUTER_PORT, url=url)
        assert b'500' in st1, f"5xx from sticky proxy must pass through, got {st1}"
        assert router._get_sticky_proxy('127.0.0.1', domain) is None, \
            "5xx must evict the sticky entry"
        assert router.snapshot_counters()['sticky_evictions'] >= 1
        # 下一请求:无粘性 → 竞速 → ok(200)胜出并回填粘性表。
        st2 = await send_http_get_status(HOST, ROUTER_PORT, url=url)
        assert b'200' in st2, f"re-race should land on ok, got {st2}"
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'ok', \
            "race winner must repopulate the sticky table"
    finally:
        await router.stop()
        bad_srv.close()
        await bad_srv.wait_closed()
        ok_srv.close()
        await ok_srv.wait_closed()


@pytest.mark.asyncio
async def test_stickiness_capacity_limit_evicts_oldest():
    """B1:粘性表容量硬上限——超限写前先清过期,仍超则驱逐 updated_at 最旧。

    单元级直测 _record_sticky 的容量保护:stickiness_max_entries=3,连续写入
    4 个键,第 4 个应驱逐时间戳最旧的 'c1|d0',其余 3 个保留。
    """
    ps = ProxyStore()
    ps.add(ProxyInfo(id='p1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    stickiness_max_entries=3,
                    db_path=tempfile.mktemp(suffix='.db'))
    try:
        base = datetime.now(timezone.utc)
        router._record_sticky('c1', 'd0', 'p1')
        router._sticky_cache['c1|d0']['updated_at'] = (base - timedelta(seconds=3)).isoformat()
        router._record_sticky('c1', 'd1', 'p1')
        router._sticky_cache['c1|d1']['updated_at'] = (base - timedelta(seconds=2)).isoformat()
        router._record_sticky('c1', 'd2', 'p1')
        router._sticky_cache['c1|d2']['updated_at'] = (base - timedelta(seconds=1)).isoformat()
        assert len(router._sticky_cache) == 3
        # 第 4 个键:超容量 → 驱逐最旧的 'c1|d0',新键写入。
        router._record_sticky('c2', 'dX', 'p1')
        assert len(router._sticky_cache) == 3, "capacity must be capped at max_entries"
        assert 'c1|d0' not in router._sticky_cache, "oldest entry must be evicted"
        assert 'c2|dX' in router._sticky_cache
        assert router.snapshot_counters()['sticky_evictions'] >= 1
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_stickiness_recheck_reraces_after_hits():
    """B2:粘性命中 recheck_hits 次后触发探路重竞速,且跳过域名缓存直接竞速。

    recheck_hits=1:请求 1 竞速(fast 胜出,写粘性 hits=0);请求 2 粘性命中
    (_bump_sticky → hits=1);请求 3 达到阈值 → 驱逐并跳过仍有效的域名缓存,
    直接竞速换新。若未跳过域名缓存,请求 3 会走域名缓存单发(racing 不增);
    断言 racing_invocations >= 2 即证明请求 3 确实重新竞速了。
    """
    fast_port, slow_port = 31451, 31452
    fast_srv = await run_mock_proxy_tagged(HOST, fast_port, 'FAST', pre_header_delay=0.0)
    slow_srv = await run_mock_proxy_tagged(HOST, slow_port, 'SLOW', pre_header_delay=0.05)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
    ps.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=2, enable_http_cache=False,
                    stickiness_enabled=True, stickiness_ttl=1800,
                    stickiness_recheck_hits=1,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        domain = 'sticky-recheck.example.com'
        url = f"http://{domain}/p0".encode()
        body1 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body1
        assert router._get_sticky_proxy('127.0.0.1', domain) == 'fast'
        # 请求 2:粘性命中,累加 hits → 1。
        body2 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body2
        entry = router._sticky_cache[router._sticky_key('127.0.0.1', domain)]
        assert entry['hits'] == 1, f"sticky hit should accumulate hits, got {entry}"
        # 请求 3:hits(1) >= recheck_hits(1) → 驱逐并重新竞速(跳过域名缓存)。
        body3 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'FAST' in body3
        counters = router.snapshot_counters()
        assert counters['sticky_evictions'] >= 1, "recheck must evict the old entry"
        assert counters['racing_invocations'] >= 2, \
            "recheck must re-race instead of relying on the (still valid) domain cache"
        # 新赢家 hits 归零,开始下一轮计数。
        entry = router._sticky_cache[router._sticky_key('127.0.0.1', domain)]
        assert entry['hits'] == 0
    finally:
        await router.stop()
        fast_srv.close()
        await fast_srv.wait_closed()
        slow_srv.close()
        await slow_srv.wait_closed()


# ── multiple Set-Cookie preservation tests ────────────────────────

async def run_multi_setcookie_proxy(host, port):
    """HTTP mock proxy returning 200 with TWO distinct Set-Cookie headers.

    Mirrors Django/JumpServer setting several cookies in one response. httpx's
    Headers.items() merges same-name headers into a comma-joined single value,
    which browsers then parse as ONE cookie and drop the rest (e.g. the
    sessionid). The router must forward them as separate header lines.
    """
    async def handle(reader, writer):
        try:
            await reader.readline()
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            body = b"ok"
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(b"Set-Cookie: sid=abc123; Path=/\r\n")
            writer.write(b"Set-Cookie: csrf=xyz789; Path=/; HttpOnly\r\n")
            writer.write(f"Content-Length: {len(body)}\r\n".encode())
            writer.write(b"Content-Type: text/plain\r\n\r\n")
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


async def send_http_get_headers(host, port, url):
    """GET through the router, returning (status_line, [header_lines], body)."""
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"GET " + url + b" HTTP/1.1\r\nHost: example.com\r\n\r\n")
    await writer.drain()
    status = await reader.readline()
    headers = []
    cl = 0
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
        headers.append(h)
        if h.lower().startswith(b'content-length:'):
            cl = int(h.split(b':', 1)[1].strip())
    body = await reader.readexactly(cl) if cl > 0 else b''
    writer.close()
    await writer.wait_closed()
    return status, headers, body


def _setcookie_lines(headers):
    """Return the value of each Set-Cookie header line (case-insensitive)."""
    return [h.split(b':', 1)[1].strip().decode('latin-1')
            for h in headers if h.lower().startswith(b'set-cookie:')]


@pytest.mark.asyncio
async def test_multiple_setcookie_headers_preserved():
    """Regression: multiple Set-Cookie headers must reach the client as separate
    lines, not merged into one comma-joined header.

    httpx Headers.items() merges same-name headers (e.g. Django's multiple
    Set-Cookie) into a single value; browsers then parse only the first cookie
    and drop the rest — which broke the JumpServer login (sessionid cookie
    lost → '登录超时，请重新登录'). Both the live streaming path AND the HTTP
    response-cache path must preserve them separately.
    """
    proxy_srv = await run_multi_setcookie_proxy(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        url = b"http://setcookie.example.com/login"
        # 1) 流式转发路径(首次,未命中缓存)。
        status, headers, body = await send_http_get_headers(HOST, ROUTER_PORT, url)
        assert b'200' in status
        assert body == b"ok"
        cookies = _setcookie_lines(headers)
        assert len(cookies) == 2, f"expected 2 separate Set-Cookie lines, got {cookies}"
        assert cookies[0] == 'sid=abc123; Path=/'
        assert cookies[1] == 'csrf=xyz789; Path=/; HttpOnly'
        # 没有任何一行把两个 cookie 逗号拼接。
        for line in cookies:
            assert 'sid=abc123' not in line or 'csrf=xyz789' not in line, \
                f"Set-Cookie headers must not be merged: {cookies}"
        # 2) 缓存命中路径(第二次 GET 同 URL,200 + Content-Length 可缓存)。
        status2, headers2, body2 = await send_http_get_headers(HOST, ROUTER_PORT, url)
        assert b'200' in status2
        assert body2 == b"ok"
        cookies2 = _setcookie_lines(headers2)
        assert len(cookies2) == 2, \
            f"cached response must also keep separate Set-Cookie lines, got {cookies2}"
        assert cookies2[0] == 'sid=abc123; Path=/'
        assert cookies2[1] == 'csrf=xyz789; Path=/; HttpOnly'
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


class TestProxySelectorEWMA:
    """ProxySelector 的 EWMA 质量跟踪与竞速排序(纯内存,确定性)。"""

    def test_unknown_quality_ranked_last(self):
        """无观测的代理排在有观测的后面(未知质量放新手区)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        store.add(ProxyInfo(id='untested', host='h3', port=3128))
        sel = ProxySelector(store)
        sel.record_ttfb('fast', 0.05)
        sel.record_ttfb('slow', 0.90)
        lst = sel.ordered_proxies()
        # 有观测的两个一定排在没有观测的前面(排序键 0 < 1)。
        assert lst.index('fast') < lst.index('untested')
        assert lst.index('slow') < lst.index('untested')

    def test_ewma_formula(self):
        """EWMA = 0.7*old + 0.3*new;首次观测直接取当前值。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        sel = ProxySelector(store)
        sel.record_ttfb('p', 0.10)
        assert sel._quality['p']['ewma_ttfb'] == 0.10
        sel.record_ttfb('p', 0.50)
        # 0.7*0.10 + 0.3*0.50 = 0.07 + 0.15 = 0.22
        assert abs(sel._quality['p']['ewma_ttfb'] - 0.22) < 1e-9

    def test_fast_proxy_first_in_race_order(self):
        """快速代理在竞速顺序中靠前(决定谁先到/是否白占竞速槽)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host='h1', port=3128))
        store.add(ProxyInfo(id='fast', host='h2', port=3128))
        sel = ProxySelector(store)
        sel.record_ttfb('slow', 0.80)
        sel.record_ttfb('fast', 0.02)
        # fast 稳定优于 slow,应始终排在前面(多次抽签也不逆转)。
        for _ in range(50):
            lst = sel.ordered_proxies()
            assert lst[0] == 'fast'

    def test_disabled_proxy_excluded(self):
        """disabled 代理不参与排序。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='on', host='h1', port=3128))
        store.add(ProxyInfo(id='off', host='h2', port=3128, enabled=False))
        sel = ProxySelector(store)
        sel.record_ttfb('off', 0.001)
        lst = sel.ordered_proxies()
        assert lst == ['on']

    def test_reset_quality_clears_all(self):
        """reset_quality 清空全部观测(网络切换后重学,RFC 8305 §4)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        sel = ProxySelector(store)
        sel.record_ttfb('p', 0.10)
        assert sel._quality
        sel.reset_quality()
        assert sel._quality == {}


class TestStagger:
    """错峰启动(staggered start,RFC 8305 §5)行为。

    竞速首批只发最优 stagger_initial 个,间隔 interval 补发;首字节成功即取消其余。
    覆盖:冷启动加倍、5xx 不算胜出、配置钳制、错峰 vs 全发扇出对比。
    """

    @pytest.mark.asyncio
    async def test_race_staggered_lazy_launch_respects_initial(self):
        """首批只发 initial 个;赢家出现后未发候选不再创建(扇出下降)。"""
        ps = ProxyStore()
        ps.add(ProxyInfo(id='p1', host='127.0.0.1', port=31301))
        ps.add(ProxyInfo(id='p2', host='127.0.0.1', port=31302))
        ps.add(ProxyInfo(id='p3', host='127.0.0.1', port=31303))
        r = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                   max_retries=3, enable_http_cache=False, stagger_start=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        # 直接驱动:initial=1 + 一个立即成功的占位 → 只发 1 个,赢家即返回。
        launched = []
        async def fast(pid, proxy_url, method, url, headers, body):
            launched.append(pid)
            return pid, method, url, object(), object()
        r._make_race_task = lambda place, method, url, headers, body: \
            asyncio.create_task(fast(place, None, method, url, headers, body))
        win = await r._race_staggered(['p1', 'p2', 'p3'], initial=1, interval=0.01)
        assert win[0] == 'p1'
        # 首字节判胜 → 未发候选(p2/p3)不创建。
        assert launched == ['p1'], f"stagger should launch only initial, got {launched}"

    @pytest.mark.asyncio
    async def test_stagger_5xx_not_a_win(self):
        """HTTP 5xx 不算竞速胜出:即使 5xx 先应答,仍继续补发找到 200。"""
        bad_port, ok_port = 31311, 31312
        bad_srv = await run_mock_proxy_status(HOST, bad_port, status=500, pre_header_delay=0.0)
        ok_srv = await run_mock_proxy_tagged(HOST, ok_port, 'OK', pre_header_delay=0.0)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='bad', host=HOST, port=bad_port))
        ps.add(ProxyInfo(id='ok', host=HOST, port=ok_port))
        r = Router(ps, listen_host=HOST, listen_port=10819,
                   max_retries=2, enable_http_cache=False, stagger_start=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        await r.start()
        try:
            # 冷启动加倍:首批 2 个(no quality)同时发,两者都答 500/200;
            # 5xx 不算赢家,最终落在 200 的 ok。
            status = await send_http_get_status(HOST, 10819, url=b"http://stg5xx.example.com/p0")
            assert b'200' in status, f"expected 200, got {status!r}"
        finally:
            await r.stop()
            bad_srv.close()
            await bad_srv.wait_closed()
            ok_srv.close()
            await ok_srv.wait_closed()

    def test_stagger_interval_clamped(self):
        """interval 钳制到 RFC 8305 区间 [100ms, 2000ms];0/负值回默认 250ms。"""
        ps = ProxyStore()
        r = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                   max_retries=2, stagger_interval_ms=0)
        assert r.stagger_interval == 0.25
        r2 = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                    max_retries=2, stagger_interval_ms=5000)
        assert r2.stagger_interval == 2.0
        r3 = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                    max_retries=2, stagger_interval_ms=20)
        assert r3.stagger_interval == 0.1

    def test_stagger_initial_clamped_to_max_retries(self):
        """stagger_initial 钳制到 max_retries;冷启动(无 EWMA)时翻倍到 2。"""
        ps = ProxyStore()
        r = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                   max_retries=1, stagger_initial=5, stagger_start=True)
        assert r.stagger_initial == 1
        # 冷启动:无质量观测 → 首批翻倍(最多 max_retries)。
        assert r._stagger_initial() == 1  # min(max_retries=1, max(2,1))=1
        r2 = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                    max_retries=3, stagger_initial=1, stagger_start=True)
        assert r2._stagger_initial() == 2  # 冷启动翻倍到 2(<=3)
        # 学到质量后回落 stagger_initial=1。
        r2.selector.record_ttfb('x', 0.01)
        assert r2._stagger_initial() == 1

    @pytest.mark.asyncio
    async def test_stagger_reduces_fanout_on_learned_quality(self):
        """学得 EWMA 后错峰:快代理先发即胜,慢代理不参与(扇出下降)。"""
        fast_port, slow_port = 31321, 31322
        fast_srv = await run_mock_proxy_tagged(HOST, fast_port, 'FAST', pre_header_delay=0.0)
        slow_srv = await run_mock_proxy_tagged(HOST, slow_port, 'SLOW', pre_header_delay=0.3)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='fast', host=HOST, port=fast_port))
        ps.add(ProxyInfo(id='slow', host=HOST, port=slow_port))
        r = Router(ps, listen_host=HOST, listen_port=10819,
                   max_retries=2, enable_http_cache=False, stagger_start=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        # 预置质量:fast 远快于 slow → 排序 fast 在前。
        r.selector.record_ttfb('fast', 0.01)
        r.selector.record_ttfb('slow', 0.30)
        await r.start()
        try:
            url = b"http://stg-fanout.example.com/p0"
            # 首次竞速:首批只发 fast(有质量→stagger_initial=1),fast 立即胜,
            # slow 不参与 → upstream_attempts 只 +1(fast)。
            status = await send_http_get(HOST, 10819, url=url)
            assert b'FAST' in status
            c = r.snapshot_counters()
            assert c['upstream_attempts'] == 1, \
                f"stagger should not launch slow, upstream_attempts={c['upstream_attempts']}"
        finally:
            await r.stop()
            fast_srv.close()
            await fast_srv.wait_closed()
            slow_srv.close()
            await slow_srv.wait_closed()


class TestCircuitBreaker:
    """全局熔断器 + 指数退避探活 + slow-start。

    覆盖:连续失败达阈值熔断、退避期内不参与竞速、熔断代理不作域名缓存/粘性
    单发、成功归零计数、退避到期 slow-start 爬升、探活喂 EWMA/熔断、reset。
    """

    async def _circuit_router(self, **kw):
        ps = ProxyStore()
        ps.add(ProxyInfo(id='down', host=HOST, port=31990))   # 端口无人监听 → 连接失败
        ps.add(ProxyInfo(id='up', host=HOST, port=31991))
        r = Router(ps, listen_host=HOST, listen_port=10829,
                   max_retries=2, enable_http_cache=False,
                   probe_interval_sec=0.0,
                   circuit_threshold=3, circuit_max_backoff=10.0,
                   slow_start_window=60.0, slow_start_success=2,
                   db_path=tempfile.mktemp(suffix='.db'), **kw)
        return ps, r

    @pytest.mark.asyncio
    async def test_failure_threshold_trips_circuit(self):
        """连续失败达阈值 → 熔断;退避期内 ordered_proxies 剔除该代理。"""
        ps, r = await self._circuit_router()
        try:
            sel = r.selector
            # 连续失败 2 次(低于阈值 3):未熔断。
            sel.record_failure('down')
            sel.record_failure('down')
            assert sel.is_circuit_open('down') is False
            assert 'down' in sel.ordered_proxies()
            # 第 3 次失败:熔断开启。
            sel.record_failure('down')
            assert sel.is_circuit_open('down') is True
            assert sel.circuit_open_count == 1
            assert 'down' not in sel.ordered_proxies(), \
                "open circuit must be excluded from racing order"
            # 退避期内仍剔除。
            assert 'down' not in sel.ordered_proxies()
            state = sel.get_circuit_state()['down']
            assert state['open'] is True
            assert state['backoff'] > 0
        finally:
            await r.stop()

    @pytest.mark.asyncio
    async def test_success_clears_failure_count(self):
        """一次成功清零连续失败计数(健康后不会熔断)。"""
        _, r = await self._circuit_router()
        try:
            sel = r.selector
            sel.record_failure('down')
            sel.record_failure('down')
            sel.record_success('down')
            sel.record_failure('down')  # 第 3 次失败前已被成功清零 → 不熔断
            assert sel.is_circuit_open('down') is False
            assert sel.circuit_open_count == 0
        finally:
            await r.stop()

    @pytest.mark.asyncio
    async def test_backoff_expiry_triggers_slow_start(self):
        """退避到期 → 解熔断并置 slow-start(垫底),成功 N 次后恢复完整权重。"""
        ps = ProxyStore()
        ps.add(ProxyInfo(id='p1', host='h1', port=1))
        ps.add(ProxyInfo(id='p2', host='h2', port=2))
        ps.add(ProxyInfo(id='p3', host='h3', port=3))
        r = Router(ps, listen_host=HOST, listen_port=10829, max_retries=2,
                   probe_interval_sec=0.0, circuit_threshold=1,
                   circuit_max_backoff=100.0, slow_start_window=60.0,
                   slow_start_success=2, db_path=tempfile.mktemp(suffix='.db'))
        try:
            sel = r.selector
            # p1 熔断(阈值 1 → 一次失败即熔断),退避期 2s(circuit_max_backoff 未达)。
            sel.record_failure('p1')
            assert sel.is_circuit_open('p1') is True
            assert 'p1' not in sel.ordered_proxies()
            # 退避期未到,手工把 open_until 拨到过去 → 到期。
            sel._circuit['p1']['open_until'] = time.monotonic() - 0.1
            # 下次排序解熔断 → slow-start 垫底。
            sel.record_ttfb('p1', 0.01)
            sel.record_ttfb('p2', 0.02)
            sel.record_ttfb('p3', 0.03)
            lst = sel.ordered_proxies()
            assert 'p1' in lst, "expired circuit must be back in order"
            assert lst[-1] == 'p1', f"slow-start proxy should be last, got {lst}"
            state = sel.get_circuit_state()['p1']
            assert state['slow_start'] is True
            # 累计 2 次成功 → 恢复完整权重(不再垫底)。
            sel.record_success('p1')
            assert sel.get_circuit_state()['p1']['slow_start'] is True
            sel.record_success('p1')
            assert sel.get_circuit_state()['p1']['slow_start'] is False
            lst2 = sel.ordered_proxies()
            assert lst2[0] == 'p1', f"p1 should be first after slow-start completes, got {lst2}"
        finally:
            await r.stop()

    @pytest.mark.asyncio
    async def test_circuit_open_proxy_excluded_from_caches(self):
        """熔断代理不作域名缓存/粘性单发(退回竞速找健康代理)。

        预置:up mock 返回 'OK' 单代理;域名缓存与粘性表都指向一个已熔断的
        假代理 'down'(端口无人监听)。请求应因熔断跳过单发、直接竞速到 up。
        """
        up_srv = await run_mock_proxy_tagged(HOST, 31991, 'UP', pre_header_delay=0.0)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='down', host=HOST, port=31990))
        ps.add(ProxyInfo(id='up', host=HOST, port=31991))
        r = Router(ps, listen_host=HOST, listen_port=10829,
                   max_retries=2, enable_http_cache=False,
                   probe_interval_sec=0.0, circuit_threshold=1,
                   circuit_max_backoff=100.0,
                   cache_ttl=300,
                   db_path=tempfile.mktemp(suffix='.db'))
        await r.start()
        try:
            domain = 'circuit-cache.example.com'
            # 熔断 down。
            r.selector.record_failure('down')
            assert r.selector.is_circuit_open('down')
            # 域名缓存与粘性表指向 down(但 down 已熔断)。
            r._record_win_meta(domain, 'down')
            r._record_sticky('127.0.0.1', domain, 'down')
            # 请求:熔断代理被单发路径跳过 → 竞速 → up 胜出。
            body = await send_http_get(HOST, 10829, url=f"http://{domain}/p0".encode())
            assert b'UP' in body, f"should fall back to up, got {body!r}"
            assert r.attempted_counts.get('down', 0) == 0, \
                "circuit-open proxy must not be attempted"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_probe_records_success_and_failure(self):
        """后台探活:成功 → EWMA + probes_ok;失败 → 熔断计数(达阈值熔断)。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)  # CONNECT 200
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=31991))
        ps.add(ProxyInfo(id='down', host=HOST, port=31990))  # 无人监听 → 失败
        r = Router(ps, listen_host=HOST, listen_port=10829,
                   max_retries=2, enable_http_cache=False,
                   probe_interval_sec=0.0, probe_canary="1.1.1.1:443",
                   circuit_threshold=2, circuit_max_backoff=100.0,
                   db_path=tempfile.mktemp(suffix='.db'))
        try:
            # 探活成功:EWMA 写入 + 连续失败归零。
            await r._probe_proxy('up')
            assert r.probes_sent == 1 and r.probes_ok == 1
            assert 'up' in r.selector.get_quality(), "probe success must feed EWMA"
            # 探活失败:连续失败累计(达阈值 2 即熔断)。
            await r._probe_proxy('down')   # fail 1
            assert r.selector.is_circuit_open('down') is False
            await r._probe_proxy('down')   # fail 2 → open
            assert r.selector.is_circuit_open('down') is True
            assert r.selector.circuit_open_count == 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_real_request_failures_drive_circuit(self):
        """真实请求连续失败驱动熔断:坏代理被剔除,后续竞速不触发它。"""
        ok_srv = await run_mock_proxy_tagged(HOST, 31991, 'OK', pre_header_delay=0.0)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='down', host=HOST, port=31990))  # 无人监听
        ps.add(ProxyInfo(id='ok', host=HOST, port=31991))
        r = Router(ps, listen_host=HOST, listen_port=10829,
                   max_retries=2, enable_http_cache=False,
                   probe_interval_sec=0.0, circuit_threshold=2,
                   circuit_max_backoff=100.0, stagger_start=False,
                   db_path=tempfile.mktemp(suffix='.db'))
        await r.start()
        try:
            url = b"http://real-fail.example.com/p0"
            # 非错峰全发(禁用 stagger):每请求 down 与 ok 都参与竞速。
            # 单请求:down 连接失败(计数 1)+ ok 胜出。
            body = await send_http_get(HOST, 10829, url=url)
            assert b'OK' in body
            assert r.selector.is_circuit_open('down') is False, "1 fail < threshold 2"
            # 第二个冷域名请求:down 再失败(计数 2 → 熔断)。
            url2 = b"http://real-fail2.example.com/p0"
            body2 = await send_http_get(HOST, 10829, url=url2)
            assert b'OK' in body2
            assert r.selector.is_circuit_open('down') is True, "2 fails should trip circuit"
            # 熔断后:down 不再被尝试(attempted_counts 不再为 down 增加)。
            before = r.attempted_counts.get('down', 0)
            await send_http_get(HOST, 10829, url=b"http://real-fail3.example.com/p0")
            assert r.attempted_counts.get('down', 0) == before, \
                "open-circuit proxy must not be attempted"
        finally:
            await r.stop()
            ok_srv.close()
            await ok_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_reset_circuits(self):
        """reset 清空熔断状态(不动 EWMA),代理立即重新参与。"""
        _, r = await self._circuit_router()
        try:
            sel = r.selector
            sel.record_ttfb('up', 0.05)
            sel.record_failure('down')
            sel.record_failure('down')
            sel.record_failure('down')
            assert sel.is_circuit_open('down')
            assert sel.get_circuit_state()['down']['open'] is True
            sel.reset_circuits()
            assert sel.get_circuit_state() == {}
            assert sel.is_circuit_open('down') is False
            assert 'down' in sel.ordered_proxies()
            # EWMA 不受 reset_circuits 影响。
            assert sel.get_quality()['up']['ewma_ttfb'] == pytest.approx(0.05)
        finally:
            await r.stop()


class TestCircuitAPI:
    def test_circuit_endpoint_without_router(self):
        """/circuit 在未挂载 router 时返回空 dict(不抛 500)。"""
        mount(None, None)
        client = TestClient(api_app)
        r = client.get("/circuit")
        assert r.status_code == 200
        assert r.json() == {}


class TestInFlightSelection:
    """in-flight 计数 + 加权 least-request 选批(P2/Envoy LeastRequest)。

    竞速排序权重 = ewma × (1 + active)^lb_bias:快而空闲的代理靠前,背上在途
    积压的代理即使延迟历史最快也被压低排序——保护慢代理不被打爆。
    """

    def test_backlog_deprioritizes_fast_proxy(self):
        """fast 代理背上大量在途请求后,从首位被挤到 slow(轻载)之后。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        sel = ProxySelector(store, lb_bias=1.0)
        # fast 延迟历史远优于 slow:默认排序 fast 在前。
        sel.record_ttfb('fast', 0.02)
        sel.record_ttfb('slow', 0.05)
        assert sel.ordered_proxies()[0] == 'fast'
        # fast 背上 5 个在途请求(在途计数模拟并发打满)。
        for _ in range(5):
            sel._inflight_start('fast')
        # 权重:fast = 0.02 × (1+5)^1 = 0.12 > slow = 0.05 → slow 靠前。
        # 多次排序(含 shuffle)都不应逆转:fast 被挤出首位。
        for _ in range(50):
            assert sel.ordered_proxies()[0] == 'slow', \
                "backlogged fast proxy must not stay first"

    def test_finish_releases_backlog(self):
        """在途请求结束后排序恢复:积压释放后 fast 重新回到首位。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        sel = ProxySelector(store, lb_bias=1.0)
        sel.record_ttfb('fast', 0.02)
        sel.record_ttfb('slow', 0.05)
        for _ in range(5):
            sel._inflight_start('fast')
        for _ in range(5):
            sel._inflight_finish('fast')
        # 积压清零 → 退化为纯 EWMA 排序,fast 回到首位。
        assert sel.get_in_flight() == {}
        for _ in range(50):
            assert sel.ordered_proxies()[0] == 'fast'

    def test_lb_bias_zero_is_pure_ewma(self):
        """bias=0 时在途积压不影响排序(纯 EWMA 排序)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        sel = ProxySelector(store, lb_bias=0.0)
        sel.record_ttfb('fast', 0.02)
        sel.record_ttfb('slow', 0.05)
        for _ in range(50):
            sel._inflight_start('fast')
        for _ in range(50):
            assert sel.ordered_proxies()[0] == 'fast', \
                "bias=0 must ignore in-flight backlog"

    def test_unknown_quality_still_last_with_backlog(self):
        """未知质量代理即使无在途也排在末尾(新手区兜底仍在)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        store.add(ProxyInfo(id='untested', host='h3', port=3128))
        sel = ProxySelector(store, lb_bias=1.0)
        sel.record_ttfb('fast', 0.02)
        sel.record_ttfb('slow', 0.05)
        # fast 背上 5 个在途(slow 无在途):权重 fast = 0.02×6 = 0.12 > slow = 0.05
        # → slow 排 fast 前,但未知质量仍垫底。
        for _ in range(5):
            sel._inflight_start('fast')
        for _ in range(50):
            lst = sel.ordered_proxies()
            assert lst[-1] == 'untested'
            assert lst.index('slow') < lst.index('fast')

    def test_reset_quality_clears_in_flight(self):
        """reset_quality 一并清空在途计数(RFC 8305 §4:旧数据不跨网络沿用)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        sel = ProxySelector(store, lb_bias=1.0)
        sel._inflight_start('p')
        assert sel.get_in_flight() == {'p': 1}
        sel.reset_quality()
        assert sel.get_in_flight() == {}
        assert sel.max_in_flight == 1  # 高水位是历史峰值,不随 reset 清零

    async def test_real_http_attempt_updates_in_flight(self):
        """真实 HTTP 请求在途计数生命周期:发起 +1、结束(成功)归零。"""
        proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
        proxy_store = ProxyStore()
        proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
        router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=tempfile.mktemp(suffix='.db'))
        await router.start()
        try:
            await send_http_get(HOST, ROUTER_PORT,
                                url=b"http://inflight.example.com/one")
            # 请求完成后在途计数应归零(无泄漏)。
            assert router.selector.get_in_flight() == {}
            assert router.selector.max_in_flight >= 1
        finally:
            await router.stop()
            proxy_srv.close()
            await proxy_srv.wait_closed()
