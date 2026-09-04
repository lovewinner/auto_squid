import asyncio
import base64
import logging
import socket
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from auto_squid.proxy_store import ProxyStore
from auto_squid.router import (
    Router, ProxySelector, _hb,
    _MAX_REQUEST_HEADER_LINES, _MAX_REQUEST_HEADER_BYTES,
    _AGG_WAIT_TIMEOUT,
)
from auto_squid.cluster import ClusterGraph
from auto_squid.config_schema import (
    ProxyInfo, PolicyConfig, Config, RouterConfig, ConnPoolConfig, LoggingConfig,
)
from auto_squid.auth import check_auth
from auto_squid.api import app as api_app, mount

# ── test ports ───────────────────────────────────────────────────
PROXY_PORT = 31291
ROUTER_PORT = 10809
LOCAL_HTTP_PORT = 18081
LOCAL_TCP_ECHO_PORT = 18082
HOST = '127.0.0.1'


# ── mock servers ──────────────────────────────────────────────────

async def run_mock_proxy(host, port, hit_counter=None):
    """HTTP/CONNECT mock proxy. For CONNECT, echoes data back."""
    async def handle(reader, writer):
        try:
            # 3.12 的 Server.wait_closed() 等待活跃 handler 协程退出;预热连接
            # 建立后从不发数据,若 readline 无限等则 handler 永不退出,测试卡死。
            # 设 5s 超时:建立连接但长时间无请求 → 关闭,模拟真实上游 idle 超时。
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not line:
                writer.close()
                return
            first = line.decode('latin-1').strip()
            while True:
                h = await asyncio.wait_for(reader.readline(), timeout=5)
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


async def run_mock_proxy_delayed_connect(host, port, connect_delay=0.0):
    """CONNECT mock proxy with a deterministic per-connect response delay.

    竞速 CONNECT 的首赢胜出由"谁先回 200"决定,`connect_delay` 让该代理必输给
    无延迟的对手——用于测试里确定性控制"哪个代理胜出"(从而让直方图学到特定 pid)。
    返回的 server 挂 `._delay`(可变 float),调用方可随时翻转;每次 CONNECT 都读当前值。
    """
    async def handle(reader, writer):
        try:
            line = await asyncio.wait_for(reader.readline(), timeout=5)
            if not line:
                writer.close()
                return
            first = line.decode('latin-1').strip()
            while True:
                h = await asyncio.wait_for(reader.readline(), timeout=5)
                if not h or h in (b"\r\n", b"\n"):
                    break
            if first.upper().startswith('CONNECT'):
                delay = getattr(server, '_delay', 0.0)  # 每次读当前延迟,可随时翻转
                print(f"[mock CONNECT] port={writer.get_extra_info('sockname')[1]} delay={delay} target={first.split()[-1]}")
                if delay:
                    await asyncio.sleep(delay)
                writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
                await writer.drain()
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
                writer.close()
        except Exception:
            try:
                writer.close()
            except Exception:
                pass
    server = await asyncio.start_server(handle, host=host, port=port)
    server._delay = connect_delay  # 调用方可翻转:server._delay = 0.15
    return server


@pytest.mark.asyncio
async def test_end_to_end_http_forwarding_utf8_header():
    """A response header with non-ASCII UTF-8 bytes must be forwarded losslessly.

    Regression: httpx decodes such header bytes as utf-8 → str code points > 255,
    and the router's latin-1 re-encode used to raise UnicodeEncodeError, failing the
    whole request after the race was won. The utf-8 bytes must reach the client
    byte-identical.
    """
    hit = []
    proxy_srv = await run_mock_proxy_utf8_header(HOST, PROXY_PORT)
    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        writer.write(b"GET http://example.com/ HTTP/1.1\r\nHost: example.com\r\n\r\n")
        await writer.drain()
        status = await reader.readline()
        assert b'200' in status
        headers = bytearray()
        while True:
            h = await reader.readline()
            if not h or h in (b"\r\n", b"\n"):
                break
            headers += h
        body = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert b'proxied-utf8' in body
        # 中文头值 must round-trip byte-for-byte (utf-8 bytes, not mangled).
        # httpx/our forwarder lowercases header names (standard), so match name
        # case-insensitively but require the value bytes identical.
        raw_headers = bytes(headers).lower()
        assert b'x-upstream-info: ' + "中文头值".encode('utf-8') in raw_headers
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


def test_header_bytes_roundtrip():
    """_hb() must re-encode header strings losslessly for every httpx decode branch.

    httpx decodes upstream header bytes with an ascii → utf-8 → iso-8859-1 heuristic;
    the router re-encodes with _hb(), which must reproduce the original wire bytes:
    - pure ASCII → ascii branch
    - valid UTF-8 (code points > 255, e.g. a Chinese header value) → utf-8 branch
    - bytes not valid UTF-8 (each byte maps to a 0-255 code point) → latin-1/iso-8859-1 branch
    """
    cases = {
        b"text/plain": "ascii",
        "中文头值".encode("utf-8"): "utf-8",
        b"GB2312 GBK" + "数据".encode("utf-8"): "utf-8 mixed",
        bytes(range(128, 256)): "iso-8859-1 all high bytes",
        bytes([0xFF, 0xFE, 0x80]): "invalid utf-8 → latin-1",
    }
    for raw, label in cases.items():
        # mirror httpx's decode heuristic
        for enc in ("ascii", "utf-8", "iso-8859-1"):
            try:
                s = raw.decode(enc)
                break
            except UnicodeDecodeError:
                continue
        assert _hb(s) == raw, f"{label}: {_hb(s)!r} != {raw!r}"
    # str that cannot be latin-1 encoded must still work (utf-8 fallback)
    assert _hb("中文") == "中文".encode("utf-8")


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


async def run_mock_proxy_truncated(host, port, declared, sent):
    """HTTP mock proxy serving a truncated response: declares `declared` bytes of
    body but actually sends only `sent` (< declared), then closes — reproducing a
    dead/aborting upstream mid-body."""
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
            body = b"x" * sent
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write(f"Content-Length: {declared}\r\n".encode())
            writer.write(b"\r\n")
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


async def run_mock_proxy_utf8_header(host, port):
    """HTTP mock proxy returning a 200 whose headers contain a non-ASCII UTF-8 value.

    Regression for the latin-1 encode crash: httpx decodes valid-UTF-8 header bytes
    with the utf-8 codec (code points > 255), and the router previously re-encoded
    them with latin-1 → UnicodeEncodeError after the race was won, killing every
    request through such an upstream.
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
            body = b"proxied-utf8"
            # UTF-8 bytes (中文) on the wire — no Content-Length → router uses chunked.
            writer.write(b"HTTP/1.1 200 OK\r\n")
            writer.write("X-Upstream-Info: 中文头值".encode('utf-8') + b"\r\n")
            writer.write(b"Content-Type: text/plain; charset=utf-8\r\n\r\n")
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

    def test_proxy_authorization_case_insensitive(self):
        """审计 P2#4:HTTP 头大小写不敏感——小写/混合大小写键应同样识别。
        旧实现固定键名 get,发送 `proxy-authorization:` 会被误拒为 407。"""
        token = base64.b64encode(b'u:p').decode()
        assert check_auth({'proxy-authorization': f'Basic {token}'}, True, 'u', 'p') == (True, None)
        assert check_auth({'PrOxY-AuThOrIzAtIoN': f'Basic {token}'}, True, 'u', 'p') == (True, None)
        assert check_auth({'authorIZATION': f'Basic {token}'}, True, 'u', 'p') == (True, None)

    def test_proxy_authorization_takes_precedence(self):
        """两个头都出现时以 Proxy-Authorization 为准(标准代理凭据位)。"""
        good = base64.b64encode(b'u:p').decode()
        bad = base64.b64encode(b'u:wrong').decode()
        assert check_auth({'Authorization': f'Basic {bad}',
                           'proxy-authorization': f'Basic {good}'}, True, 'u', 'p') == (True, None)


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


class TestApiAuth:
    """管理 API 的 HTTP Basic 认证(api.auth,默认关闭)。

    mount() 不传 api_auth → 认证关闭(现有行为);传入启用状态 → 除 /health
    外全部端点需凭据,失败回 401 + WWW-Authenticate(浏览器据此弹出凭据框)。
    每个测试用 finally: mount(None, None) 复位模块级全局,防状态泄漏到其他用例。
    """

    USER, PASSWORD = "api_user", "api_pass"

    @staticmethod
    def _mount(enabled: bool = True):
        from auto_squid.config_schema import AuthConfig
        mount(None, None, api_auth=AuthConfig(enabled=enabled,
                                              username=TestApiAuth.USER,
                                              password=TestApiAuth.PASSWORD))

    @staticmethod
    def _auth(user: str = None, pw: str = None) -> dict:
        user = TestApiAuth.USER if user is None else user
        pw = TestApiAuth.PASSWORD if pw is None else pw
        import base64
        return {"Authorization": "Basic " + base64.b64encode(f"{user}:{pw}".encode()).decode()}

    def test_health_open_with_auth_on(self):
        """认证开启时 /health 仍无需凭据(健康检查/负载均衡探活)。"""
        self._mount()
        client = TestClient(api_app)
        try:
            assert client.get("/health").status_code == 200
        finally:
            mount(None, None)

    def test_requires_auth(self):
        """认证开启时无凭据 → 401 + WWW-Authenticate 头。"""
        self._mount()
        client = TestClient(api_app)
        try:
            r = client.get("/stats")
            assert r.status_code == 401
            assert "Basic realm" in r.headers.get("WWW-Authenticate", "")
        finally:
            mount(None, None)

    def test_accepts_valid_basic(self):
        """正确 Basic 凭据 → 200。"""
        self._mount()
        client = TestClient(api_app)
        try:
            assert client.get("/stats", headers=self._auth()).status_code == 200
        finally:
            mount(None, None)

    def test_rejects_wrong_creds(self):
        """错误凭据 / 非 Basic 方案 → 401。"""
        self._mount()
        client = TestClient(api_app)
        try:
            assert client.get("/stats", headers=self._auth("api_user", "wrong")).status_code == 401
            assert client.get("/stats", headers={"Authorization": "Bearer xyz"}).status_code == 401
        finally:
            mount(None, None)

    def test_all_routes_protected_except_health(self):
        """认证开启时全部数据端点需凭据,`/health` 与仪表盘页面本身开放。"""
        self._mount()
        client = TestClient(api_app)
        try:
            for path in ["/proxies", "/quality", "/policies", "/circuit", "/metrics",
                         "/server-stats", "/config", "/domains", "/domains/meta", "/stickiness"]:
                r = client.get(path)
                assert r.status_code == 401, f"{path} 应要求认证"
            assert client.get("/").status_code == 401  # 无凭据访问仪表盘页面
            assert client.get("/", headers=self._auth()).status_code == 200
        finally:
            mount(None, None)

    def test_mount_resets_auth_state(self):
        """mount(None, None) 清空认证状态 → API 恢复开放(防全局状态泄漏)。"""
        self._mount()
        client = TestClient(api_app)
        try:
            assert client.get("/stats").status_code == 401
        finally:
            mount(None, None)
        assert TestClient(api_app).get("/stats").status_code == 200


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
    """聚合等待是有界的:慢上游下 waiter 有界等待,不悬挂(有界保护仍在)。

    用延迟 0.5s 的慢上游 mock。_AGG_WAIT_TIMEOUT=3.0s(#8),首个请求 0.5s 完成
    时 waiter 已拿到聚合结果——并发 2 全成功、无悬挂(整体 ~首个请求的延迟,
    不是无限等)。远端线程若 5s 超时关闭 mock,waiter 也至多等 3s 便放弃。

    断言:全部并发请求有限时间内成功返回。
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

        url = b"http://slow-herd.example.com/api/items"

        async def one_get():
            return await send_http_get(HOST, ROUTER_PORT, url=url)

        bodies = await asyncio.wait_for(asyncio.gather(*[one_get() for _ in range(2)]),
                                        timeout=3.0)
        # 全部成功且 body 正确(慢 mock 返回 'slow')。
        assert all(b == b"slow" for b in bodies), f"all responses should be 'slow', got {bodies}"
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

    def test_legacy_metrics_restore_without_cum_fields_no_keyerror(self):
        """09-04 bug 回归:旧 DB 行(set_proxy_metrics 恢复)缺 cum_* 字段时,
        record_ttfb 不能抛 KeyError(热路径崩溃 → 全请求失败 → 熔断全开)。
        set_*_metrics 需惰性补全 _CUM_FIELDS 缺键。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        sel = ProxySelector(store)
        def _legacy_metric() -> dict:
            """构造一份独立的旧格式 metric dict(无 cum_* 字段)。"""
            return {
                "ttfb_samples": [0.1, 0.2],
                "ttlb_samples": [],
                "ttlb_ewma": None,
                "throughput_ewma": None,
                "success": 3,
                "total": 4,
                "errors": {"timeout": 1},
                "total_bytes": 100.0,
                "transfer_time": 0.5,
            }
        sel.set_proxy_metrics({"p": _legacy_metric()})
        sel.set_domain_metrics({"example.com": {"p": _legacy_metric()}})
        # 恢复后 record_ttfb 不再抛 KeyError;cum_* 已被补全。
        sel.record_ttfb('p', 0.05)                     # 全局 scope
        sel.record_ttfb('p', 0.06, 'example.com')      # 域名 scope
        m = sel._proxy_metrics['p']['metrics']
        # 双作用域双写(已修 domain=None 时的双重计数):0.05(无 domain)全局 +1;
        # 0.06(有 domain)域名桶 +1、全局桶 +1 → 全局共 2、域名共 1。
        # 修复前无 domain 的那次会被写两遍(全局 +2),导致全局共 3 —— 该旧期望
        # 编码的是 bug,已随修复更正。
        assert m['cum_success'] == 2
        assert m['cum_ttfb_n'] == 2
        dm = sel._domain_metrics['example.com']['p']['metrics']
        assert dm['cum_success'] == 1


class TestOrderedForDomain:
    """杠杆B:域名级竞速候选排序(ordered_for_domain)。

    竞速候选按域名级 EWMA 排序:该域名快代理进首批,而非全局 EWMA 污染下被排到
    补发位置。域名无观测时完全回退全局排序(与 ordered_proxies 等价)。
    """

    def _sel(self):
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        return ProxySelector(store)

    def test_domain_fast_first(self):
        """fast 全局慢但该域名快 → ordered_for_domain 首位是 fast,而
        ordered_proxies 首位仍是 slow(全局 EWMA 污染场景)。

        本测试验证的是「域名级 vs 全局」的 EWMA 排序机制,故显式关闭 Phase 2
        多目标 Cost 排序(其默认开,主延迟项用 P99 尾部,全局赢家会随尾部而非
        均值翻转)。Cost 排序本身由 tests/test_phase_metrics.py 的专属用例覆盖。
        """
        sel = self._sel()
        sel.cost_sort_enabled = False
        # slow 全局快、该域名慢;fast 全局慢、该域名快。
        sel.record_ttfb('slow', 0.02, 'other.com')
        sel.record_ttfb('slow', 0.30, 'example.com')
        sel.record_ttfb('fast', 0.10, 'example.com')
        sel.record_ttfb('fast', 0.20, 'other.com')
        # 全局:slow 靠前(0.02+0.30 EWMA 比 fast 的 0.10+0.20 小)。
        assert sel.ordered_proxies()[0] == 'slow'
        # 该域名:fast 靠前(0.10 vs slow 的 0.30)。
        for _ in range(50):
            assert sel.ordered_for_domain('example.com')[0] == 'fast'

    def test_unknown_domain_fallback_to_global(self):
        """域名无任何观测 → ordered_for_domain 与 ordered_proxies 等价。"""
        sel = self._sel()
        sel.record_ttfb('fast', 0.01, 'other.com')
        sel.record_ttfb('slow', 0.05, 'other.com')
        for _ in range(20):
            assert sel.ordered_for_domain('new.com') == sel.ordered_proxies()

    def test_domain_unknown_proxy_ranked_last(self):
        """域名有观测但某代理该域名无观测 → 该代理垫底(未知质量)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='new', host='h2', port=3128))
        sel = ProxySelector(store)
        sel.record_ttfb('fast', 0.01, 'example.com')
        sel.record_ttfb('new', 0.001, 'other.com')  # 全局极快但该域名无观测
        lst = sel.ordered_for_domain('example.com')
        assert lst[-1] == 'new'

    def test_stagger_initial_domain_agnostic(self):
        """冷启动判定保持全局:域名无观测时 ordered_for_domain 回退全局排序(可信),
        不翻倍;只有全局质量全空才翻倍。"""
        from auto_squid.router import Router
        ps = ProxyStore()
        ps.add(ProxyInfo(id='fast', host='h1', port=3128))
        ps.add(ProxyInfo(id='slow', host='h2', port=3128))
        r = Router(ps, listen_host='127.0.0.1', listen_port=10809,
                   db_path=tempfile.mktemp(suffix='.db'),
                   max_retries=3, stagger_initial=1, enable_http_cache=False)
        # 全局有观测 → 不翻倍(即使某域名无观测,排序回退全局仍可信)。
        r.selector.record_ttfb('fast', 0.01, 'example.com')
        assert r._stagger_initial() == 1
        # 全局无观测(冷启动)→ 翻倍。
        r2 = Router(ps, listen_host='127.0.0.1', listen_port=10809,
                    db_path=tempfile.mktemp(suffix='.db'),
                    max_retries=3, stagger_initial=1, enable_http_cache=False)
        assert r2._stagger_initial() == min(3, max(2, 1)) == 2


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
        r._make_race_task = lambda place, method, url, headers, body, domain=None: \
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
                   slow_start_success=2, db_path=tempfile.mktemp(suffix='.db'),
                   # 显式关闭 Phase 2 多目标 Cost 排序:本测试验证 slow-start 分层
                   # (垫底→恢复完整权重)机制,而 p1 早期失败拉低了成功率,在 Cost
                   # 排序下会正确地不优先——那会掩盖"是否恢复正常权重"的判据。
                   # Cost 排序由 tests/test_phase_metrics.py 专属用例覆盖。
                   cost_sort_enabled=False)
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
                   probe_interval_sec=0.0, probe_canary=f"{HOST}:31991",  # 本机可达
                   circuit_threshold=2, circuit_max_backoff=100.0,
                   db_path=tempfile.mktemp(suffix='.db'))
        try:
            # 探活成功:EWMA 写入 + 连续失败归零。
            await r._probe_proxy(ps.get('up'))
            assert r.probes_sent == 1 and r.probes_ok == 1
            assert 'up' in r.selector.get_quality(), "probe success must feed EWMA"
            # 探活失败:连续失败累计(达阈值 2 即熔断)。
            await r._probe_proxy(ps.get('down'))   # fail 1
            assert r.selector.is_circuit_open('down') is False
            await r._probe_proxy(ps.get('down'))   # fail 2 → open
            assert r.selector.is_circuit_open('down') is True
            assert r.selector.circuit_open_count == 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_probe_skips_when_canary_unreachable(self):
        """本机→canary 不可达时,探活整轮跳过(probes_skipped),不累计上游失败。

        这是修复"canary 选错导致健康代理被误熔断"的守卫:直连 canary 失败只
        说明本机路由/防火墙挡了 canary,不代表上游代理挂了,故不得 record_failure。
        """
        # 两个代理:up(可达 mock)+ down(无人监听)。若守卫失效,down 会累计失败。
        # canary 用 1.1.1.1:443(校网对 1.1.1.1 直连超时;若本机恰好可达则该
        # 测试改用 127.0.0.1:1 这类必达不通的目标,见下方对照)。为确定性,这里
        # 直接对可达 mock 作"本机不可达"模拟:把 canary 设成 127.0.0.1:1
        # (本机无人监听,直连必然失败),验证守卫把它当"环境不可达"处理。
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)  # CONNECT 200
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=31991))
        ps.add(ProxyInfo(id='down', host=HOST, port=31990))  # 无人监听 → 失败
        r = Router(ps, listen_host=HOST, listen_port=10829,
                   max_retries=2, enable_http_cache=False,
                   probe_interval_sec=0.0, probe_canary=f"127.0.0.1:1",
                   circuit_threshold=2, circuit_max_backoff=100.0,
                   db_path=tempfile.mktemp(suffix='.db'))
        try:
            # 单代理探活:本机直连 canary 失败 → 整轮跳过,不计入 sent/ok/失败。
            await r._probe_proxy(ps.get('up'))
            await r._probe_proxy(ps.get('down'))
            assert r.probes_sent == 0 and r.probes_ok == 0
            assert r.probes_skipped == 2
            # down 未累计失败(consec_fail 应仍为 0)→ 未触发熔断。
            st = r.selector.get_circuit_state().get('down')
            assert st is None or st.get('consec_fail') == 0, \
                "unreachable canary must not count as proxy failure"
            assert r.selector.circuit_open_count == 0
            # 对照:canary 可达时 down 照常累计失败并熔断。
            r.probe_canary = f"{HOST}:31991"  # 换成本机可达的 canary
            await r._probe_proxy(ps.get('down'))   # fail 1
            await r._probe_proxy(ps.get('down'))   # fail 2 → open
            assert r.selector.is_circuit_open('down') is True
        finally:
            await r.stop()
            up_srv.close()

    def test_canary_for_proxy_tag_matching(self):
        """多 canary:按代理 tags 命中第一条匹配的 canary;无匹配用兜底/全局。"""
        ps = ProxyStore()
        ps.add(ProxyInfo(id='cn', host='h', port=3128, tags={'region': 'cn'}))
        ps.add(ProxyInfo(id='hk', host='h', port=3128, tags={'region': 'hk'}))
        ps.add(ProxyInfo(id='plain', host='h', port=3128))
        r = Router(ps, listen_host='127.0.0.1', listen_port=10809,
                   db_path=tempfile.mktemp(suffix='.db'),
                   probe_canary="fallback:443",
                   probe_canaries=[
                       {"name": "cn", "target": "baidu.com:443", "tags": {"region": "cn"}},
                       {"name": "global", "target": "1.1.1.1:443"},
                   ])
        assert r._canary_for_proxy(ps.get('cn')) == "baidu.com:443"
        # hk 不匹配 cn canary → 命中无 tags 的兜底 canary。
        assert r._canary_for_proxy(ps.get('hk')) == "1.1.1.1:443"
        assert r._canary_for_proxy(ps.get('plain')) == "1.1.1.1:443"

    def test_canary_for_proxy_falls_back_to_global(self):
        """未配置多 canary 或全未命中 → 回退单 canary(probe_canary)。"""
        ps = ProxyStore()
        ps.add(ProxyInfo(id='p', host='h', port=3128, tags={'region': 'cn'}))
        r = Router(ps, listen_host='127.0.0.1', listen_port=10809,
                   db_path=tempfile.mktemp(suffix='.db'), probe_canary="1.1.1.1:443")
        assert r._canary_for_proxy(ps.get('p')) == "1.1.1.1:443"
        # 多 canary 全带 tags 且全不匹配 → 回退全局。
        r2 = Router(ps, listen_host='127.0.0.1', listen_port=10809,
                    db_path=tempfile.mktemp(suffix='.db'),
                    probe_canary="fallback:443",
                    probe_canaries=[{"name": "hk", "target": "hk.com:443",
                                     "tags": {"region": "hk"}}])
        assert r2._canary_for_proxy(ps.get('p')) == "fallback:443"

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

    def test_failure_penalty_depresses_ranking(self):
        """fail_penalty_weight 把近期连续失败折算成排序权重抬升,恒失败代理被挤出前排。

        关键语义:排序只看 EWMA(成功耗时),失败不降权——一个恒 0ms 失败代理靠老
        EWMA 赖在前排持续陪跑(2026-09 事故:247-246 单日 400+ 竞速失败)。连失后
        权重 = ewma × (1 + k*fail_penalty),不必等 3 次熔断就提前降温;连成功一次
        清零 consec_fail 后自动回归。
        """
        store = ProxyStore()
        store.add(ProxyInfo(id='dead', host='h1', port=3128))
        store.add(ProxyInfo(id='healthy', host='h2', port=3128))
        sel = ProxySelector(store, lb_bias=0.0, fail_penalty_weight=4.0)
        # 两者 EWMA 相同(dead 曾快,现在老 EWMA 还挂着)
        sel.record_ttfb('dead', 0.02)
        sel.record_ttfb('healthy', 0.02)
        assert sel._weighted_rank('dead') == sel._weighted_rank('healthy')  # 未失败时并列
        # dead 连失 2 次 → 权重被抬 x(1+2*4)=9,healthy 保持 → 排序后 healthy 在前
        sel.record_failure('dead')
        sel.record_failure('dead')
        for _ in range(30):
            lst = sel.ordered_proxies()
            assert lst[0] == 'healthy', f"连失代理应被挤出前排, got {lst}"
        assert sel._weighted_rank('dead') > sel._weighted_rank('healthy')
        # 恢复:dead 成功一次 → consec_fail 清零 → 权重回归,不再被惩罚
        sel.record_success('dead')
        assert sel._weighted_rank('dead') == sel._weighted_rank('healthy')
        # 域名维度同样受惩罚(domain_obs 是 per-pid dict,不是嵌套域名的 dict)
        sel2 = ProxySelector(store, lb_bias=0.0, fail_penalty_weight=4.0)
        sel2.record_ttfb('dead', 0.02, 'a.com')
        sel2.record_ttfb('healthy', 0.02, 'a.com')
        sel2.record_failure('dead')
        per = {'dead': {'ewma_ttfb': 0.02, 'obs': 1}, 'healthy': {'ewma_ttfb': 0.02, 'obs': 1}}
        assert sel2._domain_weighted_rank(per, 'dead') > sel2._domain_weighted_rank(per, 'healthy')

    def test_failure_penalty_zero_keeps_old_behavior(self):
        """fail_penalty_weight=0 时纯 EWMA 排序,失败不惩罚(旧行为)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='dead', host='h1', port=3128))
        store.add(ProxyInfo(id='healthy', host='h2', port=3128))
        sel = ProxySelector(store, lb_bias=0.0, fail_penalty_weight=0.0)
        sel.record_ttfb('dead', 0.02)
        sel.record_ttfb('healthy', 0.02)
        sel.record_failure('dead')
        sel.record_failure('dead')
        assert sel._weighted_rank('dead') == sel._weighted_rank('healthy')

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


class TestWinnerBackfillQualityGate:
    """方向 A:竞速赢家回填(域名缓存 _record_win_meta / 粘性表 _record_sticky)
    前的质量闸——赢家显著差于当前最优代理时,不回填,避免慢代理被钉住进入
    "钉住→降级→回填→再钉住"循环(生产:239-192 EWMA 275ms 反复触发)。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h1', port=3128))
        store.add(ProxyInfo(id='slow', host='h2', port=3128))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      stickiness_enabled=True, stickiness_ttl=1800,
                      single_send_degrade_ratio=2.0, single_send_degrade_slack_ms=10.0,
                      **kw)

    def test_worse_than_best_detects_slow_winner(self):
        """慢赢家 EWMA 显著差于最优代理 → _worse_than_best 返回 True。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.02)
        r.selector.record_ttfb('fast', 0.03)  # EWMA ≈ 0.023
        r.selector.record_ttfb('slow', 0.30)
        r.selector.record_ttfb('slow', 0.30)  # EWMA ≈ 0.30, > 2× best 且差 277ms
        assert r._worse_than_best('example.com', 'slow') is True
        assert r._worse_than_best('example.com', 'fast') is False

    def test_fast_winner_not_blocked(self):
        """最优代理自己是赢家 → 正常回填(不影响正常路径)。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.02)
        r.selector.record_ttfb('slow', 0.30)
        assert r._worse_than_best('example.com', 'fast') is False
        r._record_win_meta('example.com', 'fast')
        assert r._get_fresh_proxy('example.com') == 'fast'

    def test_slow_winner_not_pinned_to_domain_cache(self):
        """慢赢家不进域名缓存 meta(继续竞速,不钉住)。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.02)
        r.selector.record_ttfb('slow', 0.30)
        r._record_win_meta('example.com', 'slow')
        # 慢赢家被拦:域名缓存未钉住,继续走竞速。
        assert 'example.com' not in r._meta_cache
        assert r._get_fresh_proxy('example.com') is None

    def test_slow_winner_not_pinned_to_sticky(self):
        """慢赢家不进粘性表(同一域名回填两处一致)。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.02)
        r.selector.record_ttfb('slow', 0.30)
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        assert '1.2.3.4|example.com' not in r._sticky_cache
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') is None

    def test_single_obs_slow_winner_still_blocked(self):
        """单次观测的慢赢家也拦(竞速首胜常是 1 obs 新代理,不能放行慢者)。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.02)
        r.selector.record_ttfb('slow', 0.30)  # slow 仅 1 次观测,EWMA=0.30
        # 即使 obs=1,只要 EWMA 显著差于最优(0.30 > 0.02×2 且差 280ms)就拦。
        assert r._worse_than_best('example.com', 'slow') is True
        r._record_win_meta('example.com', 'slow')
        assert 'example.com' not in r._meta_cache

    def test_recover_fast_after_slow_heals(self):
        """慢代理恢复(EWMA 回到最优附近)→ 不再被质量闸拦截,可正常回填。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.02)
        r.selector.record_ttfb('fast', 0.02)  # fast 稳定观测,EWMA≈0.02
        r.selector.record_ttfb('slow', 0.30)
        # 慢时被拦。
        assert r._worse_than_best('example.com', 'slow') is True
        # 恢复:slow 多次观测到接近 fast 的延迟,EWMA 显著回落。
        # EWMA alpha=0.3,0.30 收敛到 ≤0.04(fast 0.02×2)需 0.7^n×0.30≤0.04 → n≥6。
        for _ in range(8):
            r.selector.record_ttfb('slow', 0.02)
        assert r._worse_than_best('example.com', 'slow') is False
        r._record_win_meta('example.com', 'slow')
        assert r._get_fresh_proxy('example.com') == 'slow'


class TestSingleSendDegrade:
    """质量感知的单发降级(Goal #6):域名缓存/粘性命中单发时,被钉住代理
    连续失败上升或 EWMA 恶化 → 主动降级回竞速。

    两条独立信号(见 Router._single_send_degraded):
      1) 连续失败 ≥ single_send_degrade_fail(熔断阈值的早告警);
      2) 当前 EWMA ≥ 钉住时基线 × single_send_degrade_ratio(且绝对差 > slack)。
    """

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      stickiness_enabled=True, stickiness_ttl=1800, **kw)

    def test_consec_fail_degrades_domain_and_sticky(self):
        """连续失败达阈值 → 域名缓存与粘性单发都降级回竞速(未熔断时)。"""
        r = self._router(single_send_degrade_fail=2)
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('example.com', 'p')
        r._record_sticky('1.2.3.4', 'example.com', 'p')
        # 连续失败 1 次:未达阈值 2,仍单发。
        r.selector.record_failure('p')
        assert r._get_fresh_proxy('example.com') == 'p'
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'p'
        # 连续失败 2 次:达阈值 → 均降级为 None(退回竞速)。
        r.selector.record_failure('p')
        assert r._get_fresh_proxy('example.com') is None
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') is None
        assert r.single_send_degrades >= 2

    def test_degraded_proxy_marked_for_repin(self):
        """降级后代理进入 _degraded_single_send;新赢家(_record_win_meta)清除。"""
        r = self._router(single_send_degrade_fail=2)
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('example.com', 'p')
        r._record_sticky('1.2.3.4', 'example.com', 'p')
        r.selector.record_failure('p')
        r.selector.record_failure('p')
        # 降级命中 → 加入失效集合,且 meta 中的基线 ref_ewma 已捕获。
        assert r._get_fresh_proxy('example.com') is None
        assert 'p' in r.get_degraded_single_send()
        # 竞速新赢家接管:该代理成功(清零连续失败)→ 清除失效标记,重新可单发,
        # 且 ref_ewma 刷新为当前 EWMA。
        r.selector.record_success('p')  # 竞速赢家会清零连续失败
        r.selector.record_ttfb('p', 0.02)
        r._record_win_meta('example.com', 'p')
        assert 'p' not in r.get_degraded_single_send()
        assert r._get_fresh_proxy('example.com') == 'p'

    def test_ewma_ratio_degrades_single_send(self):
        """EWMA 恶化 ratio 倍(绝对差超 slack)→ 单发降级回竞速。"""
        r = self._router(single_send_degrade_ratio=3.0, single_send_degrade_slack_ms=5.0)
        r.selector.record_ttfb('p', 0.01)  # obs=1,EWMA=0.01
        r._record_win_meta('example.com', 'p')
        r._record_sticky('1.2.3.4', 'example.com', 'p')
        assert r._meta_cache['example.com']['ref_ewma'] == 0.01
        # 恶化:连续观测 0.05 使 EWMA 上探到 ≥ 0.03。
        #   第1次 obs=2:EWMA=0.7*0.01+0.3*0.05=0.022(比值2.2<3,未达)
        #   第2次 obs=3:EWMA=0.7*0.022+0.3*0.05=0.0304(比值3.04≥3,
        #     绝对差0.0204s=20.4ms>5ms slack)→ 降级。
        r.selector.record_ttfb('p', 0.05)
        r.selector.record_ttfb('p', 0.05)
        assert r._get_fresh_proxy('example.com') is None
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') is None

    def test_ewma_ratio_slack_prevents_false_positive(self):
        """极低延迟下纯比值会误判:绝对差低于 slack 时不降级。"""
        r = self._router(single_send_degrade_ratio=3.0, single_send_degrade_slack_ms=10.0)
        r.selector.record_ttfb('p', 0.0002)  # 0.2ms
        r._record_win_meta('example.com', 'p')
        r.selector.record_ttfb('p', 0.0009)  # 0.9ms,比值 4.5 > 3 但绝对差 0.7ms < 10ms
        assert r._get_fresh_proxy('example.com') == 'p', \
            "absolute gap below slack must not trigger degrade"

    def test_ewma_ratio_requires_observations(self):
        """EWMA 降级只对有观测且 obs>=2 的代理生效:单观测 EWMA 不被视为恶化。"""
        r = self._router(single_send_degrade_ratio=3.0, single_send_degrade_slack_ms=1.0)
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('example.com', 'p')
        # 直接手工抬高质量表(模拟并发异常写入),未增加 obs → 不降级。
        r.selector._quality['p']['ewma_ttfb'] = 0.99
        assert r._get_fresh_proxy('example.com') == 'p'

    def test_ref_ewma_not_refreshed_by_sticky_bumps(self):
        """粘性命中(_bump_sticky)只滑动 TTL,不刷新基线——恶化判定始终相对钉住时刻。"""
        r = self._router(single_send_degrade_ratio=3.0, single_send_degrade_slack_ms=5.0)
        r.selector.record_ttfb('p', 0.01)
        r._record_sticky('1.2.3.4', 'example.com', 'p')
        assert r._sticky_cache['1.2.3.4|example.com']['ref_ewma'] == 0.01
        # 多次粘性命中(EWMA 已涨到 0.10),基线仍是钉住时的 0.01。
        for _ in range(8):
            r._bump_sticky('1.2.3.4', 'example.com', 'p')
        assert r._sticky_cache['1.2.3.4|example.com']['ref_ewma'] == 0.01
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'p'  # obs 未增,不降级

    def test_reset_quality_clears_degraded_markers(self):
        """reset_quality 清空全部质量与降级状态:降级集合与 ref_ewma 一并重置。"""
        r = self._router(single_send_degrade_fail=2)
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('example.com', 'p')
        r.selector.record_failure('p')
        r.selector.record_failure('p')
        assert r._get_fresh_proxy('example.com') is None
        assert 'p' in r.get_degraded_single_send()
        r.reset_proxy_quality()
        # reset 清空 _quality/熔断/降级集合 → 降级标记不再阻挡单发。
        assert r.get_degraded_single_send() == []
        # meta 仍指向 'p'(ref_ewma 基线保留但质量表已清,降级判定无据可依)。
        assert r._get_fresh_proxy('example.com') == 'p'


class TestDomainEWMA:
    """域名级 EWMA(selector._domain_quality):记录与读取语义。"""

    def _selector(self):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        store.add(ProxyInfo(id='q', host='h2', port=3128))
        return ProxySelector(store)

    def test_domain_formula_independent(self):
        """域名级 EWMA 独立于全局演进;两域名互不污染。"""
        s = self._selector()
        s.record_ttfb('p', 0.10, 'd1')      # obs=1,EWMA=0.10
        s.record_ttfb('p', 0.50, 'd1')      # obs=2:0.7*0.10+0.3*0.50=0.22
        assert s._domain_quality['d1']['p']['ewma_ttfb'] == pytest.approx(0.22)
        assert s._domain_quality['d1']['p']['obs'] == 2
        # 全局独立演进(0.10 → 0.22),与域名桶不同源。
        assert s._quality['p']['ewma_ttfb'] == pytest.approx(0.22)
        # 另一域名不污染 d1。
        s.record_ttfb('p', 0.99, 'd2')
        assert s._domain_quality['d1']['p']['ewma_ttfb'] == pytest.approx(0.22)
        assert s._domain_quality['d2']['p']['ewma_ttfb'] == pytest.approx(0.99)

    def test_no_domain_only_global(self):
        """record_ttfb 不带 domain(probe 语义)→ 不建域名桶。"""
        s = self._selector()
        s.record_ttfb('p', 0.10)
        assert s._domain_quality == {}
        assert s._quality['p']['ewma_ttfb'] == 0.10

    def test_reset_quality_clears_domain(self):
        """reset_quality 一并清空域名级质量表。"""
        s = self._selector()
        s.record_ttfb('p', 0.10, 'd1')
        s.reset_quality()
        assert s._domain_quality == {}
        assert s._quality == {}

    def test_best_domain_ewma_excludes_unusable(self):
        """域名 best 跳过熔断/禁用/已删除代理;exclude 生效。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host='h', port=1, enabled=True))
        store.add(ProxyInfo(id='open', host='h', port=2, enabled=True))
        store.add(ProxyInfo(id='disabled', host='h', port=3, enabled=False))
        store.add(ProxyInfo(id='gone', host='h', port=4, enabled=True))
        s = ProxySelector(store)
        s.record_ttfb('fast', 0.01, 'd')
        s.record_ttfb('open', 0.02, 'd')
        s.record_ttfb('disabled', 0.015, 'd')
        s.record_ttfb('gone', 0.02, 'd')
        store.remove('gone')            # 已删除(残留域名观测)
        s.record_failure('open'); s.record_failure('open'); s.record_failure('open')  # 熔断
        pid, ewma = s.best_domain_ewma('d')
        # 熔断/禁用/删除代理被跳过 → fast 是唯一可用观测者。
        assert pid == 'fast'
        assert ewma == pytest.approx(0.01)
        # exclude=fast 后无可用观测者 → (None, None)。
        assert s.best_domain_ewma('d', exclude='fast') == (None, None)
        # 空域名 → (None, None)。
        assert s.best_domain_ewma('nope') == (None, None)

    def test_prune_domain_quality_caps(self):
        """prune_domain_quality 超上限按 ts 淘汰最旧条目。"""
        s = self._selector()
        for i, d in enumerate(['d1', 'd2', 'd3']):
            s.record_ttfb('p', 0.01, d)
            s._domain_quality[d]['p']['ts'] = float(i)  # 覆写 ts 使顺序确定
        s.prune_domain_quality(max_entries=2)
        # 共 3 条超上限 2,淘汰 ts 最旧的 d1(ts=0.0),剩 d2(ts=1.0)/d3(ts=2.0)。
        assert set(s._domain_quality) == {'d2', 'd3'}
        assert 'd1' not in s._domain_quality


class TestDomainLevelDegrade:
    """域名级降级判定(核心价值 = 模拟生产 247-246 案例)。

    247-246 全局 EWMA 快(11.5ms,第 2 快)却因被其他域名拖累的全局 EWMA
    相对 ref_ewma 恶化被降级;但它对 github.com 实际很快。本类验证:该域名
    有自己的 EWMA 桶后,降级判定与方向 A 回填门都按域名级走,全局污染不再
    影响 github.com 决策。
    """

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='x', host='h', port=3128))
        store.add(ProxyInfo(id='y', host='h2', port=3128))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      stickiness_enabled=True, stickiness_ttl=1800,
                      single_send_degrade_ratio=2.0, single_send_degrade_slack_ms=5.0,
                      **kw)

    def test_domain_fast_not_degraded_despite_slow_global(self):
        """判别测试(旧逻辑必挂):全局 EWMA 被污染,域名级仍快 → 不降级。

        旧逻辑:ref=全局 0.01(钉住时全局未污染),随后全局被 other.com 桶推高
        → _single_send_degraded 全局 cur≈0.5 ≥ 0.01×2 且差>slack → 降级,断言
        失败。域名级:github.com 桶 cur 仍 0.01,obs=2,相对 ref 不恶化 → 放行。
        """
        r = self._router()
        # 代理 x 对 github.com 的域名级观测:obs=2,EWMA≈0.01。
        r.selector.record_ttfb('x', 0.01, 'github.com')
        r.selector.record_ttfb('x', 0.01, 'github.com')
        r._record_win_meta('github.com', 'x')
        # 污染全局 EWMA:other.com 桶(域名级)持续观测 0.50。
        for _ in range(5):
            r.selector.record_ttfb('x', 0.50, 'other.com')
        # github.com 的域名级判定不受全局污染影响。
        assert r._get_fresh_proxy('github.com') == 'x'
        assert 'x' not in r.get_degraded_single_send()
        r._record_sticky('1.2.3.4', 'github.com', 'x')
        assert r._get_sticky_proxy('1.2.3.4', 'github.com') == 'x'

    def test_domain_worse_than_best_blocks_winner(self):
        """同一代理在慢域名被拦、在快域名放行(方向 A 域名化)。"""
        r = self._router()
        # github.com 桶:x=0.01×2,y=0.02×2 → x 是该域名最优。
        r.selector.record_ttfb('x', 0.01, 'github.com')
        r.selector.record_ttfb('x', 0.01, 'github.com')
        r.selector.record_ttfb('y', 0.02, 'github.com')
        r.selector.record_ttfb('y', 0.02, 'github.com')
        # slow.example.com 桶:x=0.60×2,y=0.02×2 → x 在该域名显著慢。
        r.selector.record_ttfb('x', 0.60, 'slow.example.com')
        r.selector.record_ttfb('x', 0.60, 'slow.example.com')
        r.selector.record_ttfb('y', 0.02, 'slow.example.com')
        r.selector.record_ttfb('y', 0.02, 'slow.example.com')
        assert r._worse_than_best('github.com', 'x') is False
        assert r._worse_than_best('slow.example.com', 'x') is True
        # 慢域名赢家不进 meta(继续竞速)。
        r._record_win_meta('slow.example.com', 'x')
        assert 'slow.example.com' not in r._meta_cache
        # 快域名赢家正常钉住。
        r._record_win_meta('github.com', 'x')
        assert r._get_fresh_proxy('github.com') == 'x'

    def test_domain_only_observer_not_blocked(self):
        """该域名唯一有观测的是 pid 自己 → 不拦(无比较对象)。"""
        r = self._router()
        r.selector.record_ttfb('x', 0.60, 'github.com')
        r.selector.record_ttfb('x', 0.60, 'github.com')
        # 全局路径里 y 是最优(未观测 x 的域名时),但 x 有 github.com 域名观测
        # 且是唯一观测者 → 域名分支返回 False,不被全局最优误拦。
        r.selector.record_ttfb('y', 0.01)
        assert r._worse_than_best('github.com', 'x') is False
        r._record_win_meta('github.com', 'x')
        assert r._get_fresh_proxy('github.com') == 'x'

    def test_domain_ref_not_polluted_by_global(self):
        """ref_ewma 捕获域名级优先,不被其他域名桶污染。"""
        r = self._router()
        r.selector.record_ttfb('x', 0.01, 'github.com')
        r.selector.record_ttfb('x', 0.50, 'other.com')
        r._record_win_meta('github.com', 'x')
        # 域名桶 cur=0.01 有 obs=1 → ref 取域名级 0.01,而非全局≈0.24。
        assert r._meta_cache['github.com']['ref_ewma'] == pytest.approx(0.01)

    def test_http_connect_domain_split(self):
        """HTTP key('github.com')与 CONNECT key('github.com:443')独立桶。"""
        r = self._router()
        r.selector.record_ttfb('x', 0.01, 'github.com')
        r.selector.record_ttfb('x', 0.05, 'github.com:443')
        r._record_win_meta('github.com:443', 'x')
        # CONNECT 桶 ref=0.05,与其独立。
        assert r._meta_cache['github.com:443']['ref_ewma'] == pytest.approx(0.05)
        # HTTP 桶未被 CONNECT 桶污染。
        assert r.selector._domain_quality['github.com']['x']['ewma_ttfb'] == pytest.approx(0.01)


class TestStickySlowProbe:
    """方案C:粘性慢探路——sticky 代理显著差于域名最优可用代理时主动驱逐。

    与 Goal #6 的 _sticky_degrade_due(相对自身钉住基线)互补:后者只在代理
    "自己变差"或"失败"时触发,对"钉住时就慢→基线高→永远不超 ratio"的
    盲区无效。方案C 比较同域名下其他代理(由 best_domain_ewma 给出),复用
    single_send_degrade_ratio/slack_ms,无需新增配置。
    触发:sticky 代理域名级 EWMA > best_domain × ratio 且差值 > slack。
    非触发:best_domain 无观测(无更好选择)、该代理无域名观测(冷启动)、
    差距不够大、单发降级关闭。
    """

    def _router(self, proxies=('fast', 'slow'), **kw):
        store = ProxyStore()
        for i, pid in enumerate(proxies):
            store.add(ProxyInfo(id=pid, host=f'h{i}', port=3128))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      stickiness_enabled=True, stickiness_ttl=1800,
                      single_send_degrade_ratio=3.0,
                      single_send_degrade_slack_ms=10.0,
                      **kw)

    def test_slow_sticky_evicted_when_better_alt_exists(self):
        """慢探路核心场景:slow 钉住时 fast 无观测(方向A放行),fast 被观测到
        显著更快后 slow 被驱逐回竞速。"""
        r = self._router()
        r.selector.record_ttfb('slow', 0.50, 'example.com')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'slow'
        # fast 域名级 0.01:比值 50>3 且差 0.49s>10ms slack → 驱逐。
        r.selector.record_ttfb('fast', 0.01, 'example.com')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') is None
        assert r.sticky_slow_probes == 1

    def test_sticky_kept_when_close_to_best(self):
        """差距不够大(比值 1.5<3.0)→ 不驱逐,保持粘性单发。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.10, 'example.com')
        r.selector.record_ttfb('slow', 0.15, 'example.com')
        # 方向A放行:0.15 < 0.10*3=0.30,回填粘性表成功。
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'slow'
        assert r.sticky_slow_probes == 0

    def test_no_evict_when_only_one_proxy(self):
        """唯一代理:best_domain 无更好选择(exclude 后为空)→ 不驱逐。"""
        r = self._router(proxies=('single',))
        r.selector.record_ttfb('single', 0.50, 'example.com')
        r._record_sticky('1.2.3.4', 'example.com', 'single')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'single'
        assert r.sticky_slow_probes == 0

    def test_no_evict_when_no_domain_data(self):
        """该代理无域名级观测(冷启动钉住)→ 无比较数据不驱逐。"""
        r = self._router()
        r.selector.record_ttfb('fast', 0.01, 'example.com')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')  # slow 无观测,方向A放行
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'slow'
        assert r.sticky_slow_probes == 0

    def test_slow_probe_counter_reported_in_metrics(self):
        """驱逐计数进入 snapshot_counters() 供 /metrics 展示。"""
        r = self._router()
        r.selector.record_ttfb('slow', 0.50, 'example.com')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        r._record_sticky('5.6.7.8', 'example.com', 'slow')  # fast 未观测,方向A放行
        r.selector.record_ttfb('fast', 0.01, 'example.com')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') is None
        assert r._get_sticky_proxy('5.6.7.8', 'example.com') is None
        assert r.snapshot_counters()['sticky_slow_probes'] == 2


class TestPolicyRouting:
    """策略路由(P1):按目标域名(后缀/精确/正则)命中第一条策略,把候选代理集
    收窄到策略允许的 tags/ids 子集。作用于竞速候选、域名缓存、粘性取用三方
    (三者一致,防旧缓存绕过新策略)。"""

    @staticmethod
    def _proxy(pid, **kw):
        return ProxyInfo(id=pid, host='h', port=3128, **kw)

    def _router(self, policies=None, **kw):
        store = ProxyStore()
        store.add(self._proxy('cn-1', tags={'region': 'cn'}))
        store.add(self._proxy('hk-1', tags={'region': 'hk'}))
        store.add(self._proxy('plain'))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      policies=policies, **kw)

    def test_no_policies_candidates_unchanged(self):
        """无策略时:候选即全部 enabled 代理(等价旧行为)。"""
        r = self._router()
        assert sorted(r._policy_candidate_pids('any.example.com', ['cn-1', 'hk-1', 'plain'])) \
            == ['cn-1', 'hk-1', 'plain']
        assert r._policy_matches('any.example.com') is None
        assert r._policy_allows_sticky('any.example.com', 'cn-1')

    def test_suffix_policy_filters_candidates(self):
        """后缀命中 → 只保留允许子集;未命中的域名不过滤。"""
        pol = PolicyConfig(match={'domain_suffix': ['.cn', 'baidu.com']},
                           proxies={'tags': {'region': 'cn'}})
        r = self._router(policies=[pol])
        assert sorted(r._policy_candidate_pids('www.baidu.com', ['cn-1', 'hk-1', 'plain'])) == ['cn-1']
        assert sorted(r._policy_candidate_pids('a.cn', ['cn-1', 'hk-1', 'plain'])) == ['cn-1']
        # 未命中:全量保留。
        assert sorted(r._policy_candidate_pids('youtube.com', ['cn-1', 'hk-1', 'plain'])) \
            == ['cn-1', 'hk-1', 'plain']

    def test_ids_policy(self):
        """按 ids 收窄:直接列代理 id,与 tags 并集。"""
        pol = PolicyConfig(match={'domain_exact': ['api.example.com']},
                           proxies={'ids': ['cn-1', 'plain']})
        r = self._router(policies=[pol])
        assert sorted(r._policy_candidate_pids('api.example.com', ['cn-1', 'hk-1', 'plain'])) \
            == ['cn-1', 'plain']

    def test_regex_policy(self):
        """正则命中 → 收窄到允许子集。"""
        pol = PolicyConfig(match={'domain_regex': [r'.*\.api\.example\.com$']},
                           proxies={'tags': {'region': 'hk'}})
        r = self._router(policies=[pol])
        assert sorted(r._policy_candidate_pids('x.api.example.com', ['cn-1', 'hk-1', 'plain'])) == ['hk-1']
        assert r._policy_matches('y.api.example.com') is pol
        assert r._policy_matches('www.example.com') is None

    def test_connect_target_host_extraction(self):
        """CONNECT target 'host:port' 剥端口后匹配;IPv6 括号剥除。"""
        pol = PolicyConfig(match={'domain_suffix': ['example.com']},
                           proxies={'tags': {'region': 'cn'}})
        r = self._router(policies=[pol])
        assert r._normalize_host('www.example.com:443') == 'www.example.com'
        assert r._normalize_host('www.example.com.') == 'www.example.com'
        assert r._normalize_host('[2001:db8::1]:443') == '2001:db8::1'
        # target 带端口 → 命中。
        assert sorted(r._policy_candidate_pids('www.example.com:443', ['cn-1', 'hk-1', 'plain'])) == ['cn-1']

    def test_domain_cache_respects_policy(self):
        """策略命中但缓存代理不在子集 → 视为 miss(旧缓存不能绕过新策略)。"""
        pol = PolicyConfig(match={'domain_suffix': ['cn']},
                           proxies={'tags': {'region': 'cn'}})
        r = self._router(policies=[pol], stickiness_enabled=True)
        r.selector.record_ttfb('hk-1', 0.01)
        r._record_win_meta('a.cn', 'hk-1')  # 旧缓存钉在 hk-1
        assert r._get_fresh_proxy('a.cn') is None  # hk-1 不在 cn 子集 → miss
        # 符合策略的缓存代理正常命中。
        r.selector.record_ttfb('cn-1', 0.01)
        r._record_win_meta('b.cn', 'cn-1')
        assert r._get_fresh_proxy('b.cn') == 'cn-1'

    def test_sticky_respects_policy(self):
        """策略命中但粘性代理不在子集 → 视为 miss(旧粘性不能绕过新策略)。"""
        pol = PolicyConfig(match={'domain_suffix': ['cn']},
                           proxies={'tags': {'region': 'cn'}})
        r = self._router(policies=[pol], stickiness_enabled=True, stickiness_ttl=1800)
        r.selector.record_ttfb('hk-1', 0.01)
        r._record_sticky('1.2.3.4', 'a.cn', 'hk-1')
        assert r._get_sticky_proxy('1.2.3.4', 'a.cn') is None  # hk-1 不在子集 → miss

    def test_local_blocked_by_restrictive_policy(self):
        """策略限制 tags/ids 时本机直连(local)被排除;未限制策略放行。"""
        pol = PolicyConfig(match={'domain_suffix': ['cn']},
                           proxies={'tags': {'region': 'cn'}})
        r = self._router(policies=[pol], enable_local_racing=True)
        assert not r._policy_allows_sticky('a.cn', 'local')
        # 未限制策略(无 tags/ids)→ local 放行。
        pol2 = PolicyConfig(match={'domain_suffix': ['cn']}, proxies={})
        r2 = self._router(policies=[pol2], enable_local_racing=True)
        assert r2._policy_allows_sticky('a.cn', 'local')

    def test_policy_order_first_match_wins(self):
        """多策略按配置顺序,第一条命中即用(可配置优先级/覆盖)。"""
        p1 = PolicyConfig(match={'domain_suffix': ['example.com']},
                          proxies={'ids': ['cn-1']})
        p2 = PolicyConfig(match={'domain_suffix': ['sub.example.com']},
                          proxies={'ids': ['hk-1']})
        r = self._router(policies=[p1, p2])
        # 同时命中 p1(example.com 后缀)与 p2(sub 更具体),但顺序 p1 在前 → 取 p1。
        assert r._policy_matches('sub.example.com') is p1
        assert sorted(r._policy_candidate_pids('sub.example.com', ['cn-1', 'hk-1'])) == ['cn-1']

    def test_http_race_uses_policy_candidates(self):
        """端到端:命中策略的域名只在该子集内竞速(_policy_candidate_pids 收窄)。"""
        pol = PolicyConfig(match={'domain_suffix': ['cn']},
                           proxies={'tags': {'region': 'cn'}})
        r = self._router(policies=[pol])
        got = r._policy_candidate_pids('a.cn', ['cn-1', 'hk-1', 'plain'])
        assert got == ['cn-1']


class TestHttpCacheLRU:
    """HTTP 响应缓存 LRU + 容量上限(P2):_http_cache_set 写前按 max_entries /
    max_bytes 淘汰最久未访问条目,_get 命中刷新访问序,_bytes 计数与二级索引
    在所有删除路径(过期/LRU/按域名失效)保持一致。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    def test_lru_evicts_least_recently_used_by_entries(self):
        """max_entries 超限 → 淘汰 last_access 最旧的条目(即使刚写入)。"""
        r = self._router(http_cache_max_entries=3)
        r._http_cache_set('GET', 'http://a.example.com/', 200, 'OK', {}, b'aaa')
        r._http_cache_set('GET', 'http://b.example.com/', 200, 'OK', {}, b'bbb')
        r._http_cache_set('GET', 'http://c.example.com/', 200, 'OK', {}, b'ccc')
        assert len(r._http_cache) == 3
        assert r._http_cache_get('GET', 'http://a.example.com/') is not None
        # 第 4 条:淘汰最久未访问——b(未被访问)在 a 之前淘汰。
        r._http_cache_set('GET', 'http://d.example.com/', 200, 'OK', {}, b'ddd')
        assert r._http_cache_get('GET', 'http://a.example.com/') is not None
        assert r._http_cache_get('GET', 'http://b.example.com/') is None
        assert r._http_cache_get('GET', 'http://c.example.com/') is not None
        assert r._http_cache_get('GET', 'http://d.example.com/') is not None
        assert r.http_cache_evictions == 1
        assert len(r._http_cache) == 3

    def test_lru_evicts_by_bytes(self):
        """max_bytes 超限 → 按字节淘汰,直到回到上限以内。"""
        r = self._router(http_cache_max_bytes=50, http_cache_max_entries=1000)
        r._http_cache_set('GET', 'http://a.example.com/', 200, 'OK', {}, b'x' * 20)
        r._http_cache_set('GET', 'http://b.example.com/', 200, 'OK', {}, b'y' * 20)
        # 两条共 40 字节 ≤ 50:都保留。
        assert len(r._http_cache) == 2
        assert r._http_cache_bytes == 40
        r._http_cache_set('GET', 'http://c.example.com/', 200, 'OK', {}, b'z' * 20)
        # 三条共 60 > 50:淘汰最旧 a,留下 b/c。
        assert len(r._http_cache) == 2
        assert r._http_cache_get('GET', 'http://a.example.com/') is None
        assert r._http_cache_get('GET', 'http://b.example.com/') is not None
        assert r._http_cache_get('GET', 'http://c.example.com/') is not None
        assert r._http_cache_bytes == 40
        assert r.http_cache_evictions == 1

    def test_single_large_response_not_cached(self):
        """单一响应超过 max_bytes 一半 → 不缓存(防单条打满预算)。"""
        r = self._router(http_cache_max_bytes=100, http_cache_max_entries=1000)
        r._http_cache_set('GET', 'http://a.example.com/', 200, 'OK', {}, b'x' * 60)
        assert r._http_cache_get('GET', 'http://a.example.com/') is None
        assert r._http_cache_bytes == 0

    def test_update_existing_key_byte_accounting(self):
        """覆盖已有 key 时字节计数不重复(先归还旧字节再计入新字节)。"""
        r = self._router(http_cache_max_entries=1000, http_cache_max_bytes=100_000)
        r._http_cache_set('GET', 'http://a.example.com/', 200, 'OK', {}, b'aaa')
        assert r._http_cache_bytes == 3
        r._http_cache_set('GET', 'http://a.example.com/', 200, 'OK', {}, b'bbbbbb')
        assert r._http_cache_bytes == 6
        assert len(r._http_cache) == 1

    def test_invalidate_updates_bytes_and_index(self):
        """写方法按域名失效:字节计数与二级索引同步,不残留。"""
        r = self._router(http_cache_max_entries=1000, http_cache_max_bytes=100_000)
        r._http_cache_set('GET', 'http://a.example.com/x', 200, 'OK', {}, b'aaa')
        r._http_cache_set('GET', 'http://a.example.com/y', 200, 'OK', {}, b'bbb')
        r._http_cache_set('GET', 'http://b.example.com/', 200, 'OK', {}, b'ccc')
        assert r._http_cache_bytes == 9
        r._http_cache_invalidate('a.example.com')
        assert r._http_cache_get('GET', 'http://a.example.com/x') is None
        assert r._http_cache_get('GET', 'http://a.example.com/y') is None
        assert r._http_cache_get('GET', 'http://b.example.com/') is not None
        assert r._http_cache_bytes == 3
        assert 'a.example.com' not in r._http_cache_domain_index

    def test_get_refreshes_lru_order(self):
        """_get 命中刷新 last_access:访问过的条目不被误淘汰。"""
        r = self._router(http_cache_max_entries=3)
        for i in range(3):
            r._http_cache_set('GET', f'http://{i}.example.com/', 200, 'OK', {}, b'x')
        # 访问 0 与 1 → 2 成为最久未访问。
        r._http_cache_get('GET', 'http://0.example.com/')
        r._http_cache_get('GET', 'http://1.example.com/')
        r._http_cache_set('GET', 'http://3.example.com/', 200, 'OK', {}, b'x')
        assert r._http_cache_get('GET', 'http://2.example.com/') is None
        assert r._http_cache_get('GET', 'http://0.example.com/') is not None
        assert r._http_cache_get('GET', 'http://1.example.com/') is not None

    def test_snapshot_exposes_cache_metrics(self):
        """snapshot_counters 暴露 http_cache_bytes / http_cache_evictions。"""
        r = self._router(http_cache_max_entries=2)
        r._http_cache_set('GET', 'http://a.example.com/', 200, 'OK', {}, b'aaa')
        r._http_cache_set('GET', 'http://b.example.com/', 200, 'OK', {}, b'bbb')
        r._http_cache_set('GET', 'http://c.example.com/', 200, 'OK', {}, b'ccc')
        s = r.snapshot_counters()
        assert s['http_cache_bytes'] == 6  # 两条(淘汰后)
        assert s['http_cache_evictions'] == 1
        assert s['http_cache_entries'] == 2


class TestAdaptiveTTL:
    """自适应域名缓存 TTL(P2):稳定域名 TTL 上浮,抖动/恶化域名 TTL 回落。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        store.add(ProxyInfo(id='q', host='h', port=3129))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      adaptive_ttl=True, adaptive_ttl_min=60.0,
                      adaptive_ttl_max=1800.0, **kw)

    def test_stable_domain_ttl_grows(self):
        """同代理连续胜出 → TTL 上浮(1.5× 步进,封顶)。"""
        r = self._router()
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('d0.example.com', 'p')  # 首次钉住 → 基础 TTL(cache_ttl)
        base = r._domain_ttl_cache['d0.example.com']
        # 同域连续胜出 → TTL 逐步上浮。
        for _ in range(4):
            r._record_win_meta('d0.example.com', 'p')
        ttl = r._domain_ttl_cache['d0.example.com']
        assert ttl > base, f"stable domain TTL should grow, got {ttl} (base {base})"
        assert ttl <= r.adaptive_ttl_max
        assert r.domain_ttl_grows > 0
        assert r._get_fresh_proxy('d0.example.com') == 'p'

    def test_switch_resets_ttl_to_min(self):
        """赢家切换 → switch_count+1,TTL 回落下限。"""
        r = self._router()
        r.selector.record_ttfb('p', 0.01)
        r.selector.record_ttfb('q', 0.02)
        r._record_win_meta('d.example.com', 'p')
        r._record_win_meta('d.example.com', 'p')
        r._record_win_meta('d.example.com', 'p')  # TTL 已上浮
        base = r._domain_ttl_cache['d.example.com']
        assert base > r.adaptive_ttl_min
        r._record_win_meta('d.example.com', 'q')  # 切换
        assert r._domain_ttl_cache['d.example.com'] == r.adaptive_ttl_min
        assert r._domain_switch_count['d.example.com'] == 1
        assert r.domain_ttl_resets >= 1

    def test_degrade_resets_ttl(self):
        """单发降级(代理开始恶化)→ TTL 打回下限。"""
        r = self._router(single_send_degrade_fail=2)
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('d.example.com', 'p')
        r._record_win_meta('d.example.com', 'p')
        assert r._domain_ttl_cache['d.example.com'] > r.adaptive_ttl_min
        r.selector.record_failure('p')
        r.selector.record_failure('p')  # 达降级阈值
        assert r._get_fresh_proxy('d.example.com') is None
        assert r._domain_ttl_cache['d.example.com'] == r.adaptive_ttl_min

    def test_enriched_meta_includes_ttl_fields(self):
        """/domains/meta 展示 ttl/expires_at/switch_count(自适应开启时)。"""
        r = self._router()
        r.selector.record_ttfb('p', 0.01)
        r._record_win_meta('d.example.com', 'p')
        meta = r.get_domain_meta_enriched()
        assert 'ttl' in meta['d.example.com']
        assert 'expires_at' in meta['d.example.com']
        assert meta['d.example.com']['switch_count'] == 0
        # 关闭时与旧结构一致(无附加字段)。
        r2 = Router(ProxyStore(), listen_host='127.0.0.1', listen_port=10809,
                    db_path=tempfile.mktemp(suffix='.db'))
        r2.selector.record_ttfb('p', 0.01)
        r2._record_win_meta('d.example.com', 'p')
        m2 = r2.get_domain_meta_enriched()
        assert set(m2['d.example.com'].keys()) == {'default_proxy', 'updated_at', 'ref_ewma'}

    def test_slow_single_send_observation_logs_with_ip(self, caplog):
        """慢单发采样:首字节耗时超阈值 → 记带 client_ip 的日志并计数。"""
        import logging
        r = self._router(single_send_slow_log_ms=1500.0)
        with caplog.at_level(logging.INFO, logger='auto_squid.router'):
            # 用回溯 2s 的起始戳伪造一次慢观测,避免 sleep。
            r._observe_single_send('59.67.225.91', 'github.com', 'github.com', 'p',
                                   time.perf_counter() - 2.0)
        assert r.single_send_slow_logged == 1
        hit = [rec for rec in caplog.records
               if rec.getMessage().startswith("slow single send")]
        assert hit and "59.67.225.91" in hit[0].getMessage()

    def test_slow_single_send_below_threshold_and_disabled_noop(self, caplog):
        """低于阈值不记;阈值 0(默认关闭)不记,计数恒 0。"""
        import logging
        r = self._router(single_send_slow_log_ms=1500.0)
        with caplog.at_level(logging.INFO, logger='auto_squid.router'):
            # 实时戳(接近 0ms)→ 远低于阈值。
            r._observe_single_send('59.67.225.91', 'github.com', 'github.com', 'p',
                                   time.perf_counter())
        assert r.single_send_slow_logged == 0
        assert not any(rec.getMessage().startswith("slow single send")
                       for rec in caplog.records)
        # 默认关闭:即使传慢戳也不记。
        r0 = self._router()
        r0._observe_single_send('59.67.225.91', 'github.com', 'github.com', 'p',
                                time.perf_counter() - 5.0)
        assert r0.single_send_slow_logged == 0

    def test_slow_single_send_failure_observation_logs_with_ip(self, caplog):
        """慢单发失败采样:失败(建连超时)超阈值 → 记带 client_ip 的 FAILED 日志并计数。

        建连失败型卡顿(某代理 egress→源站建连 10s+)是成功路径观测的盲区,这里验证
        失败也按同一阈值捕获,计入独立的 single_send_fail_logged。
        """
        import logging
        r = self._router(single_send_slow_log_ms=1500.0)
        with caplog.at_level(logging.INFO, logger='auto_squid.router'):
            # 回溯 3s 伪造一次慢失败(超阈值);超时异常。
            r._observe_single_send_failure('59.67.225.91', 'github.com', 'github.com', 'p',
                                           time.perf_counter() - 3.0, TimeoutError("connect timed out"))
        assert r.single_send_fail_logged == 1
        assert r.single_send_slow_logged == 0  # 失败计数与成功慢分开
        hit = [rec for rec in caplog.records
               if rec.getMessage().startswith("slow single send FAILED")]
        assert hit and "59.67.225.91" in hit[0].getMessage()
        assert "TimeoutError" in hit[0].getMessage()

    def test_slow_single_send_failure_below_threshold_and_disabled_noop(self, caplog):
        """失败低于阈值不记;阈值 0(默认关闭)不记,计数恒 0。"""
        import logging
        r = self._router(single_send_slow_log_ms=1500.0)
        with caplog.at_level(logging.INFO, logger='auto_squid.router'):
            r._observe_single_send_failure('59.67.225.91', 'github.com', 'github.com', 'p',
                                           time.perf_counter(), TimeoutError("x"))
        assert r.single_send_fail_logged == 0
        assert not any(rec.getMessage().startswith("slow single send FAILED")
                       for rec in caplog.records)
        # 默认关闭:即使传慢失败戳也不记。
        r0 = self._router()
        r0._observe_single_send_failure('59.67.225.91', 'github.com', 'github.com', 'p',
                                        time.perf_counter() - 10.0, TimeoutError("x"))
        assert r0.single_send_fail_logged == 0


class TestSwitchDamping:
    """域名赢家切换阻尼(P3):新赢家不能因单次竞速抖动就替换稳定域名赢家。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        store.add(ProxyInfo(id='q', host='h', port=3129))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'),
                      switch_damping=True, switch_damping_min_wins=2, **kw)

    def test_requires_consecutive_wins_to_switch(self):
        """新赢家单次胜出不能替换旧赢家;连续胜出达阈值才替换。"""
        r = self._router()
        r.selector.record_ttfb('p', 0.01)
        r.selector.record_ttfb('q', 0.01)
        r._record_win_meta('d.example.com', 'p')  # 首次钉住 p
        # q 单次胜出 → 被阻尼挡下,仍保持 p。
        r._record_win_meta('d.example.com', 'q')
        assert r._meta_cache['d.example.com']['default_proxy'] == 'p'
        assert r.switch_damping_blocks == 1
        # q 连续第 2 次胜出 → 通过,替换为 q。
        r._record_win_meta('d.example.com', 'q')
        assert r._meta_cache['d.example.com']['default_proxy'] == 'q'

    def test_ewma_significant_advantage_switches_immediately(self):
        """新赢家 EWMA 显著优于旧赢家 → 跳过阻尼立即切换。"""
        r = self._router(switch_damping_ratio=0.8, switch_damping_abs_ms=0.0)
        r.selector.record_ttfb('p', 0.10)  # 旧赢家 EWMA 100ms
        r.selector.record_ttfb('q', 0.01)  # 新赢家 EWMA 10ms(快 90%)
        r._record_win_meta('d.example.com', 'p')
        r._record_win_meta('d.example.com', 'q')
        assert r._meta_cache['d.example.com']['default_proxy'] == 'q'
        assert r.switch_damping_fast_swaps == 1

    def test_circuit_open_old_winner_switches_immediately(self):
        """旧赢家熔断 → 跳过阻尼立即切换(对故障类不延迟换路)。"""
        r = self._router()
        r.selector.record_ttfb('p', 0.01)
        r.selector.record_ttfb('q', 0.01)
        r._record_win_meta('d.example.com', 'p')
        for _ in range(3):
            r.selector.record_failure('p')  # 熔断 p
        assert r.selector.is_circuit_open('p')
        r._record_win_meta('d.example.com', 'q')
        assert r._meta_cache['d.example.com']['default_proxy'] == 'q'
        assert r.switch_damping_blocks == 0

    def test_abs_ms_advantage_switches_immediately(self):
        """新赢家快 ≥ abs_ms 毫秒 → 立即切换。"""
        r = self._router(switch_damping_ratio=0.0, switch_damping_abs_ms=30.0)
        r.selector.record_ttfb('p', 0.050)  # 50ms
        r.selector.record_ttfb('q', 0.010)  # 10ms(快 40ms ≥ 30ms)
        r._record_win_meta('d.example.com', 'p')
        r._record_win_meta('d.example.com', 'q')
        assert r._meta_cache['d.example.com']['default_proxy'] == 'q'
        # 小优势(快 < 30ms)→ 不立即切换,走连续胜出。
        r2 = self._router(switch_damping_ratio=0.0, switch_damping_abs_ms=30.0)
        r2.selector.record_ttfb('p', 0.050)
        r2.selector.record_ttfb('q', 0.040)  # 快 10ms < 30ms
        r2._record_win_meta('d.example.com', 'p')
        r2._record_win_meta('d.example.com', 'q')
        assert r2._meta_cache['d.example.com']['default_proxy'] == 'p'

    def test_snapshot_exposes_damping_counters(self):
        """snapshot_counters 暴露 switch_damping 计数。"""
        r = self._router()
        r.selector.record_ttfb('p', 0.01)
        r.selector.record_ttfb('q', 0.01)
        r._record_win_meta('d.example.com', 'p')
        r._record_win_meta('d.example.com', 'q')
        s = r.snapshot_counters()
        assert s['switch_damping_enabled'] is True
        assert s['switch_damping_blocks'] == 1


class TestAdaptiveConcurrencyLimit:
    """自适应并发限制(P3):每代理并发上限成功加性增/失败乘性降,在途达上限
    的代理被过滤出竞速候选。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host='h', port=3128))
        store.add(ProxyInfo(id='fast', host='h', port=3129))
        kw.setdefault('concurrency_limit_enabled', True)
        kw.setdefault('concurrency_limit_initial', 4)
        kw.setdefault('concurrency_limit_min', 2)
        kw.setdefault('concurrency_limit_max', 32)
        kw.setdefault('concurrency_add_on_success', 2)
        kw.setdefault('concurrency_mult_on_failure', 0.5)
        kw.setdefault('concurrency_failure_window', 3)
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    def test_at_limit_filtered_from_candidates(self):
        """在途达上限 → 该代理从 ordered_proxies 过滤。"""
        r = self._router()
        sel = r.selector
        for _ in range(4):  # 填满 initial=4
            sel._inflight_start('slow')
        assert sel._at_concurrency_limit('slow') is True
        assert 'slow' not in sel.ordered_proxies()
        assert 'fast' in sel.ordered_proxies()

    def test_success_raises_limit_after_window(self):
        """成功 ≥ 窗口 → 加性提升上限。"""
        r = self._router()
        sel = r.selector
        assert sel._conc_state('slow')['limit'] == 4
        for _ in range(3):  # 窗口=3
            sel.record_ttfb('slow', 0.01)
        assert sel._conc_state('slow')['limit'] == 6  # 4 + add(2)

    def test_failure_lowers_limit(self):
        """失败 → 乘性降低上限(触底 min)。"""
        r = self._router()
        sel = r.selector
        sel.record_failure('slow')
        assert sel._conc_state('slow')['limit'] == 2  # 4 × 0.5
        # 触底:再失败不再降。
        sel.record_failure('slow')
        assert sel._conc_state('slow')['limit'] == 2

    def test_reset_quality_clears_limits(self):
        """reset_quality 清空并发限制状态(重新学)。"""
        r = self._router()
        sel = r.selector
        sel.record_failure('slow')
        assert sel._conc_state('slow')['limit'] == 2
        sel.reset_quality()
        assert sel._conc == {}

    def test_snapshot_exposes_limits(self):
        """snapshot_counters 暴露 proxy_concurrency_limits / enabled。"""
        r = self._router()
        s = r.snapshot_counters()
        assert s['concurrency_limit_enabled'] is True
        assert s['proxy_concurrency_limits'] == {}


class TestConnPool:
    """CONNECT 上游 TCP 预热池(P1):CONNECT 优先取池中已连接 socket,省建连;
    带 per-proxy/全局 fd 预算与空闲超时,防泄漏。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_per_proxy', 2)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动补)
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    @pytest.mark.asyncio
    async def test_refill_then_peek_reuses_connection(self):
        """预热补满后,peek 取到池中连接(hits),不再 miss。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        try:
            await r._conn_pool_refill()
            # refill_target 默认 2,per_proxy=2 → 预热 2 条。
            assert r.conn_pool_creates == 2
            key = f"{HOST}:31991"
            assert len(r._conn_pool.get(key, [])) == 2
            got = r._conn_pool_peek(HOST, 31991)
            assert got is not None
            assert r.conn_pool_hits == 1
            assert r.conn_pool_misses == 0
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_peek_empty_misses(self):
        """池空 → peek 返回 None 并计 misses。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        try:
            assert r._conn_pool_peek(HOST, 31991) is None
            assert r.conn_pool_misses == 1
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prune_closes_idle(self):
        """空闲超时 → prune 关闭并计 expired。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_idle_timeout=1.0)
        try:
            await r._conn_pool_refill()
            assert r.conn_pool_creates >= 1
            # 伪造创建时间在很久以前,触发空闲超时。
            for stack in r._conn_pool.values():
                for _, w in stack:
                    w._conn_pool_created = time.monotonic() - 100
            await r._pool_prune()
            assert r.conn_pool_expired >= 1
            assert sum(len(v) for v in r._conn_pool.values()) == 0
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_global_budget_respected(self):
        """refill 受全局 conn_pool_total 钳制。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='p1', host=HOST, port=31991))
        ps.add(ProxyInfo(id='p2', host=HOST, port=31992))
        up_srv2 = await run_mock_proxy(HOST, 31992, hit_counter=None)
        r = Router(ps, listen_host='127.0.0.1', listen_port=10809,
                   db_path=tempfile.mktemp(suffix='.db'),
                   conn_pool_enabled=True, conn_pool_per_proxy=5,
                   conn_pool_total=2, conn_pool_refill_target=5,
                   conn_pool_refill_interval=0.0)
        try:
            await r._conn_pool_refill()
            total = sum(len(v) for v in r._conn_pool.values())
            assert total <= 2, f"global budget exceeded: {total}"
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()
            up_srv2.close()
            await up_srv2.wait_closed()

    def test_snapshot_exposes_conn_pool(self):
        """snapshot_counters 暴露 conn_pool 计数。"""
        r = self._router()
        s = r.snapshot_counters()
        assert s['conn_pool_enabled'] is True
        assert s['conn_pool_hits'] == 0
        assert s['conn_pool_size'] == 0
        # 空闲暂停字段默认值(60 分钟,与 schema 一致)与当前空闲态暴露。
        # 新路由器从未"空闲过 60 分钟",故 idle_paused=False。
        assert s['conn_pool_refill_pause_minutes'] == 60.0
        assert s['conn_pool_idle_paused'] is False


class TestConnPoolIdlePause:
    """refill 空闲感知(conn_pool.refill_pause_minutes):连续 N 分钟无"密集活动"
    (窗口内 ≥ 阈值个客户端请求成簇)则挂起 refill/目标预热,避免深夜空闲期
    "建了又过期"的空转浪费;真实请求到来立即恢复。

    活动判定为"簇度计数"(refill_pause_activity_window / min_requests):真实流量
    是簇(一次页面加载数秒内多 hostname 并发 CONNECT),心跳是孤例(窗口内计数
    1-2)。据此区分——既不误伤真实孤立请求,又免疫后台心跳。窗口计数不启用
    (窗口=0 或阈值≤1)时任意请求都刷新(旧行为)。
    """

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_per_proxy', 2)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动补)
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    @pytest.mark.asyncio
    async def test_default_pause_off_keeps_old_behavior(self):
        """默认 refill_pause_minutes=0 → 不暂停,refill 照常建连(向后兼容)。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()  # 未传 → 默认 0
        try:
            await r._conn_pool_refill()
            assert r.conn_pool_creates == 2
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_idle_skips_refill(self):
        """超过 refill_pause_minutes 无活动 → refill 不再建新连。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        try:
            # 模拟已空闲 60+ 分钟(回拨活动时间戳),触发空闲暂停。
            r._last_request_activity = time.monotonic() - 3601
            assert r._conn_pool_idle() is True
            await r._conn_pool_refill()
            assert r.conn_pool_creates == 0
            assert sum(len(v) for v in r._conn_pool.values()) == 0
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_activity_resumes_refill(self):
        """密集活动到来(_record_request_activity,窗口内 ≥ 阈值成簇)立即解除暂停,
        refill 恢复。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        try:
            # 模拟已空闲 60+ 分钟(回拨活动时间戳)。
            r._last_request_activity = time.monotonic() - 3601
            assert r._conn_pool_idle() is True
            # 一簇密集请求:窗口内累计 ≥ K 个 → 刷新活动时间戳,解除暂停。
            for _ in range(3):
                r._record_request_activity()
            assert r._conn_pool_idle() is False
            await r._conn_pool_refill()
            assert r.conn_pool_creates == 2
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_idle_skips_target_prewarm(self):
        """空闲暂停同样挂起目标预热(第二阶段),不建半连接。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        try:
            r._last_request_activity = time.monotonic() - 3601
            await r._target_pool_refill(HOST, 31991, "www.baidu.com:443")
            assert r.target_pool_creates == 0
            # 密集活动(窗口内 ≥ K 成簇)恢复预热。
            for _ in range(3):
                r._record_request_activity()
            made = await r._target_pool_refill(HOST, 31991, "www.baidu.com:443")
            assert made == 2
            assert r.target_pool_creates == 2
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    def test_snapshot_exposes_pause(self):
        """snapshot_counters 暴露暂停配置/当前空闲态。"""
        r = self._router(conn_pool_refill_pause_minutes=60.0,
                         conn_pool_refill_pause_activity_window=120.0)
        r._last_request_activity = time.monotonic() - 3601
        s = r.snapshot_counters()
        assert s['conn_pool_refill_pause_minutes'] == 60.0
        assert s['conn_pool_refill_pause_activity_window'] == 120.0
        assert s['conn_pool_refill_pause_min_requests'] == 3
        assert s['conn_pool_idle_paused'] is True

    def test_heartbeat_does_not_resume_idle(self):
        """孤立请求(心跳,窗口内计数 1 < K)不刷新活动时间戳,无法解除空闲暂停。"""
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        r._last_request_activity = time.monotonic() - 3601
        assert r._conn_pool_idle() is True
        # 单个心跳:窗口内计数 1 < K(3),不刷新,时间戳保持陈旧。
        r._record_request_activity()
        assert (time.monotonic() - r._last_request_activity) >= 60 * 60
        assert r._conn_pool_idle() is True

    def test_heartbeat_never_prevents_idle(self):
        """周期心跳(如 alive.github.com 每 10 分钟)持续到来,空闲判定仍在 60 分钟
        后触发——心跳不会持续刷新活动时间戳,无法阻止空闲暂停。"""
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        r._last_request_activity = time.monotonic() - 3660
        assert r._conn_pool_idle() is True
        # 心跳轮番到达(模拟真实间隔:每次距上次 ≥ 窗口,前一条已滑出窗口,任何
        # 2 条都构不成"窗口内 ≥ K"的簇),时间戳保持陈旧,不进入空闲。
        for _ in range(6):
            r._record_request_activity()          # 第 1 条:计数 1 < K,不刷新
            r._last_request_activity = time.monotonic() - 7200  # 前 1 条已滑出窗口
            r._activity_timestamps.clear()        # 孤立心跳,清空窗口计数
        assert (time.monotonic() - r._last_request_activity) >= 60 * 60
        assert r._conn_pool_idle() is True

    def test_dense_requests_do_not_go_idle(self):
        """密集请求流(窗口内持续 ≥ K)刷新活动时间戳,60 分钟内不进入空闲。"""
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        for _ in range(10):
            r._record_request_activity()
        assert (time.monotonic() - r._last_request_activity) < 120.0
        assert r._conn_pool_idle() is False

    def test_min_requests_one_keeps_old_behavior(self):
        """阈值 ≤1:窗口计数不启用,任意请求(含心跳)都刷新(旧行为,向后兼容)。"""
        r = self._router(conn_pool_refill_pause_minutes=60.0,
                         conn_pool_refill_pause_min_requests=1)
        r._last_request_activity = time.monotonic() - 3601
        assert r._conn_pool_idle() is True
        # 孤立请求(单发)在 K=1 时仍刷新 → 解除暂停。
        r._record_request_activity()
        assert r._conn_pool_idle() is False

    def test_real_isolated_request_resumes(self):
        """修复目标:真实孤立请求(此前被 silence_sec 一刀切误伤)如今不误伤——
        窗口内 ≥ K 个请求(真实页面加载一簇)即使距上次活动很久也能解除暂停。"""
        r = self._router(conn_pool_refill_pause_minutes=60.0)
        r._last_request_activity = time.monotonic() - 7200  # 已空闲 2 小时
        assert r._conn_pool_idle() is True
        # 模拟一次真实页面加载:数秒内多个 hostname 的 CONNECT 成簇到达。
        for _ in range(3):
            r._record_request_activity()
        assert r._conn_pool_idle() is False

    @pytest.mark.asyncio
    async def test_idle_pause_does_not_block_requests(self):
        """方案3 安全属性回归:空闲暂停只挂起后台预建,不卡请求路径。

        空闲暂停期间真实 CONNECT 请求照常工作——取池/新建/复用已握手连接全不
        受暂停影响(_try_tunnel / _established_pool_peek / 归还均不检查空闲态)。
        """
        hit_counter = []
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=hit_counter)
        r = self._router(conn_pool_refill_pause_minutes=60.0,
                         conn_pool_established_reuse=True)
        await r.start()
        try:
            target = b"idle-request.example.com:443"
            # 置为空闲暂停态:距上次活动远超暂停阈值。
            r._last_request_activity = time.monotonic() - 7200
            assert r._conn_pool_idle() is True
            # 真实请求仍照常工作(单发即完成;established_reuse 的 peek 也不查空闲)。
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"x")
            assert echo == b"x"
            assert r.established_pool_misses == 1
            # 请求结束后连接照常归还已握手池(不受空闲暂停影响)。
            assert await self._wait_returned(r, target)
            assert r.established_pool_returned == 1
            # 第二条请求照常复用已握手连接。
            echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"y")
            assert echo2 == b"y"
            assert r.established_pool_hits == 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    async def _wait_returned(self, r, target, timeout=2.0):
        """轮询等待 (proxy,target) 的已握手连接归还池(归还走异步回调)。"""
        key = f"{HOST}:31991|{target.decode()}"
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if key in r._established_pool and len(r._established_pool[key]) > 0:
                return True
            await asyncio.sleep(0.02)
        return False


class TestTargetPrewarm:
    """CONNECT 目标半预连接(P2, P3-5 第二阶段):命中域名缓存/粘性的高频
    CONNECT target 后台提前建立"到上游代理"的 TCP(不提前 CONNECT 到目标),
    按 (proxy, target) 键区分,取用优先于第一阶段通用池。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_target_prewarm', True)
        kw.setdefault('conn_pool_per_proxy', 4)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动补)
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    @pytest.mark.asyncio
    async def test_prewarm_then_peek_reuses(self):
        """预热补满 cap=2 后,target_pool_peek 取到池中连接(不计 miss),且留 1 条备用。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        try:
            assert await r._target_pool_refill(HOST, 31991, 'example.com:443') == 2
            assert r.target_pool_creates == 2
            assert r.target_prewarm_success == 2
            key = f"{HOST}:31991|example.com:443"
            assert len(r._target_pool.get(key, [])) == 2
            got = r._target_pool_peek(HOST, 31991, 'example.com:443')
            assert got is not None
            assert r.target_pool_hits == 1
            assert r.target_pool_misses == 0
            # cap=2:取走 1 条后仍有 1 条备用,下一条同 target 请求可再次命中。
            got2 = r._target_pool_peek(HOST, 31991, 'example.com:443')
            assert got2 is not None
            assert r.target_pool_hits == 2
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_peek_empty_misses(self):
        """池空 → target_pool_peek 返回 None 并计 target_pool_misses。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        try:
            assert r._target_pool_peek(HOST, 31991, 'example.com:443') is None
            assert r.target_pool_misses == 1
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prewarm_skips_disabled_or_budget_full(self):
        """未启用或全局 fd 预算已满 → 不再预热。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_enabled=False)
        try:
            assert await r._target_pool_refill(HOST, 31991, 'example.com:443') == 0
            assert r.target_pool_creates == 0
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prune_closes_idle_target(self):
        """target 池空闲超时 → prune 关闭并计 target_pool_expired。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_idle_timeout=1.0)
        try:
            assert await r._target_pool_refill(HOST, 31991, 'example.com:443') == 2
            for stack in r._target_pool.values():
                for _, w in stack:
                    w._conn_pool_created = time.monotonic() - 100
            await r._pool_prune()
            assert r.target_pool_expired >= 1
            assert sum(len(v) for v in r._target_pool.values()) == 0
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_cluster_prewarm_outlives_passive_prune(self):
        """cluster 预建独立空闲超时:同键 cluster 预建比被动预建活得更久。

        _pool_prune 按连接级 _cluster_prewarmed 标签选超时——cluster 连接用
        cluster_pool_idle_timeout(默认 600s),被动/通用池连接仍用
        conn_pool_idle_timeout。同键混装(先 cluster 后 passive)验证两段超时并存。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_idle_timeout=1.0, cluster_pool_idle_timeout=1000.0)
        try:
            key = f"{HOST}:31991|split-timeout.example.com:443"
            # 先 cluster 补满 2 条(打 _cluster_prewarmed),再 passive 补到 cap=4。
            # 顺序不能反:refill 按 cap 是"目标水位"不是增量,若先 passive 建满 2 条,
            # 后续 cluster 会因 cap=2 不再补(0 条 cluster 连接)。
            assert await r._target_pool_refill(HOST, 31991, 'split-timeout.example.com:443',
                                               cap=2, source='cluster') == 2
            assert await r._target_pool_refill(HOST, 31991, 'split-timeout.example.com:443',
                                               cap=4, source='passive') == 2
            assert len(r._target_pool[key]) == 4
            # 全部伪造为 50s 前建连:超过被动超时(1s),未到 cluster 超时(1000s)。
            for stack in r._target_pool.values():
                for _, w in stack:
                    w._conn_pool_created = time.monotonic() - 50
            await r._pool_prune()
            assert r.target_pool_expired >= 2      # 被动 2 条被关
            assert r.cluster_pool_expired == 0     # cluster 2 条存活(独立超时生效)
            assert len(r._target_pool[key]) == 2
            assert all(getattr(w, '_cluster_prewarmed', False) for _, w in r._target_pool[key])
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_combined_budget_respected(self):
        """第一阶段与 target 池共享 conn_pool_total:target 池占满预算后,第一阶段
        refill 不再新增连接。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_total=1, conn_pool_refill_target=2)
        try:
            # target 池先占满唯一预算(1)。
            assert await r._target_pool_refill(HOST, 31991, 'example.com:443') == 1
            # 第一阶段 refill 应被预算钳制为 0。
            await r._conn_pool_refill()
            total = sum(len(v) for v in r._conn_pool.values()) \
                + sum(len(v) for v in r._target_pool.values())
            assert total == 1
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_established_outlives_passive_prune(self):
        """established 池独立空闲超时:已握手库存比通用池连接活得更久。

        _pool_prune 按连接级 _established_pooled 标签选超时——established 连接
        用 established_pool_idle_timeout(默认 None=跟随 conn_pool_idle_timeout,
        显式配置则独立),通用池仍用 conn_pool_idle_timeout。同一过期间隔下,
        established 标签连接存活、无标签连接被关。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        # 通用池 1s 过期;established 池独立 1000s(模拟选项1:库存多活等复访)。
        r = self._router(conn_pool_idle_timeout=1.0,
                         conn_pool_established_idle_timeout=1000.0)
        try:
            # 手工建两条 established 池连接(模拟竞速败者归还,打 _established_pooled)。
            for i in range(2):
                reader, writer = await asyncio.open_connection(HOST, 31991)
                writer._established_pooled = True
                writer._conn_pool_created = time.monotonic() - 50  # 超通用超时,未超 established
                key = f"{HOST}:31991|est-prune.example.com:443"
                r._established_pool.setdefault(key, []).append((reader, writer))
            # 手工建一条通用池连接(无 established 标签),同为 50s 前建连。
            reader, writer = await asyncio.open_connection(HOST, 31991)
            writer._conn_pool_created = time.monotonic() - 50
            r._conn_pool.setdefault(f"{HOST}:31991", []).append((reader, writer))
            await r._pool_prune()
            # established 2 条存活(独立超时 1000s 生效),通用池 1 条被关。
            assert len(r._established_pool[key]) == 2, \
                f"established should survive, keys={list(r._established_pool.keys())}"
            assert r.established_pool_expired == 0
            assert r.conn_pool_expired >= 1, "generic conn should be pruned"
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    def test_snapshot_exposes_target_pool(self):
        """snapshot_counters 暴露 target 半预连接池计数与开关。"""
        r = self._router()
        s = r.snapshot_counters()
        assert s['conn_pool_target_prewarm'] is True
        assert s['target_pool_hits'] == 0
        assert s['target_pool_size'] == 0
        assert s['target_prewarm_dispatched'] == 0

    @pytest.mark.asyncio
    async def test_spawn_dispatches_task(self):
        """_spawn_target_prewarm 发起后台预热并计入 dispatched(失败静默)。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        try:
            r._spawn_target_prewarm(HOST, 31991, 'example.com:443')
            # 给后台协程一个事件循环机会完成建连(cap=2 → 补 2 条)。
            for _ in range(100):
                if r.target_prewarm_dispatched >= 1 and sum(len(v) for v in r._target_pool.values()) >= 2:
                    break
                await asyncio.sleep(0.01)
            assert r.target_prewarm_dispatched == 1
            assert r.target_pool_creates == 2
            assert r.target_prewarm_success == 2
        finally:
            # 排空后台预热 task(stop 前先 gather),再关连接。
            for t in list(r._running_tasks):
                t.cancel()
            if r._running_tasks:
                await asyncio.gather(*r._running_tasks, return_exceptions=True)
            r._running_tasks.clear()
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_e2e_domain_cache_hit_reuses_prewarmed_conn(self):
        """端到端:第一次 CONNECT 竞速写域名缓存;第二次命中域名缓存触发预热
        (miss + 后台建连);第三次命中复用预热的到上游 TCP(target_pool_hits +1,
        target_pool_misses 不再增长)。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = b"e2e-preconn.example.com:443"
            # 1) 竞速 → 写域名缓存(无预热池可复用)。
            echo1 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo1 == b"one"
            assert r.target_pool_hits == 0
            # 2) 域名缓存命中 → 取池未中(miss),隧道建立后后台预热一条到上游 TCP。
            echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"two")
            assert echo2 == b"two"
            assert r.target_pool_misses >= 1
            # 等待后台预热协程完成建连。
            for _ in range(200):
                if sum(len(v) for v in r._target_pool.values()) >= 1:
                    break
                await asyncio.sleep(0.01)
            assert r.target_pool_creates >= 1
            # 3) 域名缓存命中 → 复用预热的到上游 TCP(target_pool_hits +1)。
            hits_before = r.target_pool_hits
            misses_before = r.target_pool_misses
            echo3 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"three")
            assert echo3 == b"three"
            assert r.target_pool_hits == hits_before + 1
            assert r.target_pool_misses == misses_before
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_spawn_skipped_when_not_enabled(self):
        """未开启 target_prewarm → 不发起后台预热,dispatched 保持 0。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_target_prewarm=False)
        try:
            r._spawn_target_prewarm(HOST, 31991, 'example.com:443')
            await asyncio.sleep(0.02)
            assert r.target_prewarm_dispatched == 0
            assert r.target_pool_creates == 0
        finally:
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prewarm_default_cap_refills_two(self):
        """默认预热 cap=2:_spawn_target_prewarm 后同一 (proxy, target) 键补到 2 条。

        生产实测 cap=1 时 target_pool_hits=1 / misses=71(取走即空→周期 miss);
        cap=2 让取走 1 条后仍留 1 条备用,显著降低 miss。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_total=8)
        try:
            r._spawn_target_prewarm(HOST, 31991, 'example.com:443')
            for _ in range(200):
                if sum(len(v) for v in r._target_pool.values()) >= 2:
                    break
                await asyncio.sleep(0.01)
            key = f"{HOST}:31991|example.com:443"
            assert len(r._target_pool.get(key, [])) == 2, \
                f"expected cap=2 prewarm, got {len(r._target_pool.get(key, []))}"
            assert r.target_prewarm_dispatched == 1
            assert r.target_pool_creates == 2
            assert r.target_prewarm_success == 2
        finally:
            for t in list(r._running_tasks):
                t.cancel()
            if r._running_tasks:
                await asyncio.gather(*r._running_tasks, return_exceptions=True)
            r._running_tasks.clear()
            await r._conn_pool_close_all()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_e2e_racing_winner_triggers_prewarm(self):
        """竞速胜出(非 local)的 CONNECT → 触发 (proxy, target) 预热。

        回归:_handle_connect 竞速赢家分支原本不预热,导致流量主体走竞速时
        target_prewarm 只服务极少数缓存命中请求(target_pool_hits=1 / misses=71)。
        需两个竞速候选代理;胜者须是真实上游(非 local)才预热。
        """
        up_srv1 = await run_mock_proxy(HOST, 31991, hit_counter=None)
        up_srv2 = await run_mock_proxy(HOST, 31992, hit_counter=None)
        store = ProxyStore()
        store.add(ProxyInfo(id='p1', host=HOST, port=31991))
        store.add(ProxyInfo(id='p2', host=HOST, port=31992))
        r = Router(store, listen_host='127.0.0.1', listen_port=10809,
                   db_path=tempfile.mktemp(suffix='.db'),
                   conn_pool_enabled=True, conn_pool_target_prewarm=True,
                   conn_pool_per_proxy=4, conn_pool_total=8,
                   conn_pool_refill_interval=0.0,  # 只取不补(测试手动补)
                   enable_local_racing=False,       # 排除 local 直连,确保胜者是上游
                   stickiness_enabled=False,
                   max_retries=2)
        await r.start()
        try:
            target = b"race-prewarm.example.com:443"
            # 首次竞速(域名缓存未建立)→ 胜出后应后台预热 (proxy, target)。
            echo1 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo1 == b"one"
            for _ in range(200):
                if sum(len(v) for v in r._target_pool.values()) >= 1:
                    break
                await asyncio.sleep(0.01)
            assert r.target_prewarm_dispatched >= 1, "racing winner should trigger prewarm"
            assert r.target_pool_creates >= 1, "racing winner should warm a (proxy,target) TCP"
            assert r.target_prewarm_success >= 1
            # 竞速路径的预热必须落到真实上游代理(键为 "host:port|target")。
            assert any(v for v in r._target_pool.values()), "prewarmed conn should exist for the racing winner proxy"
        finally:
            for t in list(r._running_tasks):
                t.cancel()
            if r._running_tasks:
                await asyncio.gather(*r._running_tasks, return_exceptions=True)
            r._running_tasks.clear()
            await r.stop()
            up_srv1.close()
            await up_srv1.wait_closed()
            up_srv2.close()
            await up_srv2.wait_closed()


class TestEstablishedTunnelReuse:
    """已建握手隧道复用(P3-6):CONNECT 隧道结束若连接干净则归还 _established_pool,
    下次同 (proxy, target) 复用,跳过 CONNECT 握手,省掉慢线路重建。严格验证:
    有残留缓冲则丢弃不复用,宁可不复用也不污染下一个客户端。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_established_reuse', True)
        kw.setdefault('conn_pool_target_prewarm', False)
        kw.setdefault('conn_pool_per_proxy', 4)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动)
        kw.setdefault('conn_pool_idle_timeout', 100.0)   # 长超时,防测试中途过期
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    async def _wait_returned(self, r, target, timeout=2.0):
        """轮询等待隧道归还(归还发生在 send_connect 返回后的异步 finally 里)。"""
        key = f"{HOST}:31991|{target.decode()}"
        for _ in range(int(timeout / 0.02)):
            if r._established_pool.get(key):
                return True
            await asyncio.sleep(0.02)
        return False

    @pytest.mark.asyncio
    async def test_tunnel_returned_to_pool(self):
        """隧道结束后连接归还 _established_pool(而非关闭),returned 计数 +1。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = b"reuse-return.example.com:443"
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo == b"one"
            # 客户端断开 → _relay_tunnel 异步归还已握手连接,轮询等待。
            assert await self._wait_returned(r, target), "连接应归还到已握手池"
            assert r.established_pool_returned == 1
            key = f"{HOST}:31991|reuse-return.example.com:443"
            assert len(r._established_pool[key]) == 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_reuse_skips_connect(self):
        """第二次同 target 命中已握手池,established_pool_hits +1,不再发 CONNECT。

        用 hit_counter 列表统计 mock 上游收到的 CONNECT 请求数:第二次应不增加。
        """
        hit_counter = []
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=hit_counter)
        r = self._router()
        await r.start()
        try:
            target = b"reuse-skip.example.com:443"
            # 第一次:建隧道 + CONNECT 计数 +1,结束后归还(异步,轮询等待)。
            echo1 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo1 == b"one"
            assert len(hit_counter) == 1, f"first CONNECT must hit mock, got {len(hit_counter)}"
            assert await self._wait_returned(r, target)
            # 第二次:命中已握手池,跳过 CONNECT,mock 收到 CONNECT 数不增。
            echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"two")
            assert echo2 == b"two"
            assert len(hit_counter) == 1, f"second CONNECT must be reused, got {len(hit_counter)} CONNECTs"
            # 第一次 peek miss(池空)→ misses=1;第二次命中 → hits=1,misses 不再增。
            assert r.established_pool_hits == 1
            assert r.established_pool_misses == 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_reuse_disabled_returns_none(self):
        """established_reuse=False → 不查已握手池,established_pool_misses 不增。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_established_reuse=False)
        await r.start()
        try:
            target = b"reuse-off.example.com:443"
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"x")
            assert echo == b"x"
            assert r.established_pool_misses == 0
            assert r.established_pool_returned == 0
            assert sum(len(v) for v in r._established_pool.values()) == 0
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_stop_closes_established_pool(self):
        """stop() 关闭全部已握手连接,池清空。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = b"reuse-stop.example.com:443"
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"x")
            assert echo == b"x"
            assert await self._wait_returned(r, target)
            assert sum(len(v) for v in r._established_pool.values()) == 1
        finally:
            await r.stop()
            # stop() 内部调 _conn_pool_close_all → 已握手池清空。
            assert sum(len(v) for v in r._established_pool.values()) == 0
            up_srv.close()
            await up_srv.wait_closed()

    def test_snapshot_exposes_established_pool(self):
        """snapshot_counters 暴露 established_pool 计数与开关。"""
        r = self._router()
        s = r.snapshot_counters()
        assert s['conn_pool_established_reuse'] is True
        assert s['established_pool_hits'] == 0
        assert s['established_pool_size'] == 0
        assert s['established_pool_returned'] == 0

    @pytest.mark.asyncio
    async def test_established_alive_probe_states(self):
        """复用前活性探测三态:活(read 超时)→True;EOF(FIN)→False;脏残留→False。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            # 活:对端空闲无数据 → read(1) 阻塞到超时 → 判活。
            alive_r, alive_w = await asyncio.open_connection(HOST, 31991)
            try:
                assert await r._established_alive(alive_r, alive_w) is True
            finally:
                alive_w.close()
            # EOF:对端已 FIN(数据读尽)→ read(1) 返回 b'' → 判死。
            eof_r, eof_w = await asyncio.open_connection(HOST, 31991)
            eof_r.feed_eof()
            try:
                assert await r._established_alive(eof_r, eof_w) is False
            finally:
                eof_w.close()
            # 脏残留:上游残余缓冲数据 → read(1) 返回该字节 → 判死。
            dirty_r, dirty_w = await asyncio.open_connection(HOST, 31991)
            dirty_r.feed_data(b"X")
            try:
                assert await r._established_alive(dirty_r, dirty_w) is False
            finally:
                dirty_w.close()
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_cap_limits_pool_per_key(self):
        """established 池单键上限=_ESTABLISHED_KEY_CAP(2):并发隧道归还超限被关闭。

        并发 4 条同 (proxy,target) 隧道全新建(mock 的 CONNECT 计数 4),结束后
        归还受 cap 限制:池最多驻 2 条,established_pool_returned 不超过 2。
        """
        hit_counter = []
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=hit_counter)
        r = self._router()
        await r.start()
        try:
            target = b"reuse-cap.example.com:443"

            async def one(_i):
                return await send_connect(HOST, ROUTER_PORT, target=target, payload=b"x")

            echoes = await asyncio.gather(*[one(i) for i in range(4)])
            assert echoes == [b"x"] * 4, f"all 4 tunnels must echo, got {echoes}"
            key = f"{HOST}:31991|{target.decode()}"
            # 等异步 finally 归还全部完成。
            for _ in range(int(2.0 / 0.02)):
                if r.established_pool_returned >= 2:
                    break
                await asyncio.sleep(0.02)
            # 池大小受 per-key cap 约束:最多 2,不会涨到 4。
            assert len(r._established_pool.get(key, [])) <= 2, \
                f"per-key cap breached: {len(r._established_pool.get(key, []))}"
            # hit_counter 应到 4:每次并发都是新建(池空启动,cap 限制下不无限复用)。
            assert len(hit_counter) == 4
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_budget_includes_established_pool(self):
        """established 池计入全局 conn_pool_total:预算耗尽时归还被 SKIP(close)。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        # conn_pool_total=1:先手动灌满 _conn_pool(占走全部预算),再归还 established
        # 连接应超预算被关闭,而不是新增到已握手池。
        r = self._router(conn_pool_total=1)
        await r.start()
        try:
            # 手动注入 1 条到"其它 target"的目标池 key,占走全部预算(total=1)。
            # 不能用 _conn_pool 的 proxy-key:当前 CONNECT 会 peek 通用池把它取走,
            # 归还时预算又归零。用不同 target 的目标池 key 既能占预算、又不会被取用。
            fake_r, fake_w = await asyncio.open_connection(HOST, 31991)
            fake_w._conn_pool_created = time.monotonic()
            r._target_pool[f"{HOST}:31991|other-target:443"] = [(fake_r, fake_w)]

            target = b"reuse-budget.example.com:443"
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"y")
            assert echo == b"y"
            # 归还应被预算检查拦下:established 池保持空。
            await asyncio.sleep(0.2)
            assert sum(len(v) for v in r._established_pool.values()) == 0, \
                "established pool must stay empty when global budget is exhausted"
            assert r.established_pool_returned == 0
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_pool_keepalive_set_on_return(self):
        """归还到 established 池的连接设了 SO_KEEPALIVE(OS 兜底判死半开)。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = b"reuse-keepalive.example.com:443"
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"k")
            assert echo == b"k"
            assert await self._wait_returned(r, target)
            key = f"{HOST}:31991|{target.decode()}"
            _reader, writer = r._established_pool[key][0]
            sock = writer.get_extra_info('socket')
            assert sock is not None
            if hasattr(socket, 'SO_KEEPALIVE'):
                ka = sock.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE)
                assert ka == 1, f"SO_KEEPALIVE should be 1, got {ka}"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_dead_probe_tunnel_falls_back_without_nameerror(self):
        """复用候选在活性探测时判死 → _discard_conn 丢弃并回落新建(回归 #14)。

        修复前 Router 的 `_discard_conn(up_writer)` 以裸名调用(类内裸名不查类作用域)
        → 命中的就是 NameError。本测试构造"通过 peek 干净检查(at_eof=False、无残留
        缓冲)但活性探测必死(reader 带 RST 异常)"的复用候选:探测抛异常 → 判死 →
        丢弃 → 回落新建 CONNECT,客户端仍拿到 echo,established_pool_expired 计数。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = b"reuse-dead.example.com:443"
            # 第一次:正常隧道 → 归还 established 池。
            echo1 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo1 == b"one"
            assert await self._wait_returned(r, target)
            key = f"{HOST}:31991|{target.decode()}"
            # 手动造一条"已 RST"的候选(模拟对端静默 RST 却又过了 peek 干净检查):
            # at_eof()=False、无残留缓冲 → _established_pool_peek 放行;read(1)
            # 抛出 -> _established_alive 判死。
            dead_r, dead_w = await asyncio.open_connection(HOST, 31991)
            dead_r.set_exception(ConnectionResetError("peer reset"))
            r._established_pool[key].append((dead_r, dead_w))
            # 第二次:peek 拿到死候选 → 探测判死 → _discard_conn → 回落 _conn_pool/新建。
            echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"two")
            assert echo2 == b"two", "死隧道探测后必须正常回落新建,且不再触发 NameError"
            assert r.established_pool_expired >= 1, "死连接应计入 expired(探测判死丢弃)"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()


class TestRaceLoserEstablishedReturn:
    """杠杆C:竞速败者(CONNECT 200 但输掉)隧道归还 _established_pool。

    竞速胜出后,已完成 CONNECT 200 但首字节更慢的败者隧道,过去被 _cleanup_tunnel_result
    直接关闭(浪费);现在经 partial 绑定 target 的清理路径归还已握手池,下次同
    (proxy,target) 请求复用跳过握手。established_reuse 关闭时败者仍关闭(回归保护)。
    """

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host=HOST, port=31991))
        store.add(ProxyInfo(id='slow', host=HOST, port=31992))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_established_reuse', True)
        kw.setdefault('conn_pool_target_prewarm', False)
        kw.setdefault('conn_pool_per_proxy', 4)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动)
        kw.setdefault('conn_pool_idle_timeout', 100.0)   # 长超时,防测试中途过期
        kw.setdefault('max_retries', 2)
        kw.setdefault('stickiness_enabled', False)
        kw.setdefault('enable_local_racing', False)       # 排除 local 直连,确保胜者是上游
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    @staticmethod
    def _keys(r):
        return {k for k, v in r._established_pool.items() if v}

    @pytest.mark.asyncio
    async def test_cleanup_returns_loser_to_pool(self):
        """_cleanup_tunnel_result 直接把"已 CONNECT 200 的败者"隧道归还池。

        单元级(无竞速时序):手动建一条已握手的隧道,调清理函数,断言落池且计数 +1。
        竞速时序里"败者已完成 200"是同一批次与赢家并列完成的情形,此处直接驱动。
        """
        up_srv = await run_mock_proxy_delayed_connect(HOST, 31992, connect_delay=0.0)
        r = self._router()
        await r.start()
        try:
            target = "loser-cleanup.example.com:443"
            # 手动模拟一个已 CONNECT 200 的败者隧道(经 slow 上游 31992 握手成功)。
            reader, writer = await asyncio.open_connection(HOST, 31992)
            writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode())
            await writer.drain()
            status = await reader.readline()
            assert b'200' in status, f"mock should return 200, got {status!r}"
            # 消费残余头部直到空行(与 _try_tunnel 的 200 读取一致),否则残留
            # \r\n 会让 _maybe_return_established 判脏不复用。
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            # 调用败者清理:应归还池而非关闭。
            await r._cleanup_tunnel_result(('slow', reader, writer), target=target)
            key = f"{HOST}:31992|{target}"
            assert r._established_pool.get(key), \
                f"loser tunnel should be returned, keys={self._keys(r)}"
            assert r.established_pool_returned == 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_e2e_race_pools_both_winner_and_loser(self):
        """E2E 竞速:两个无延迟代理同时完成 CONNECT 200,赢家经 relay 归还、
        败者经 cleanup 归还 → 两键都入池。"""
        up_srv_fast = await run_mock_proxy(HOST, 31991, hit_counter=None)
        up_srv_slow = await run_mock_proxy_delayed_connect(HOST, 31992, connect_delay=0.0)
        r = self._router(stagger_start=False)  # _race 全发,两候选同时完成 200
        await r.start()
        try:
            target = b"loser-e2e.example.com:443"
            echo = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo == b"one"
            # 两键最终都应入池:赢家(客户端断开后 relay 归还)+ 败者(cleanup 归还)。
            for _ in range(int(3.0 / 0.02)):
                if len(r._established_pool) >= 2:
                    break
                await asyncio.sleep(0.02)
            assert len(r._established_pool) == 2, \
                f"both winner+loser tunnels should be pooled, keys={self._keys(r)}"
            assert r.established_pool_returned >= 2
        finally:
            await r.stop()
            up_srv_fast.close()
            await up_srv_fast.wait_closed()
            up_srv_slow.close()
            await up_srv_slow.wait_closed()

    @pytest.mark.asyncio
    async def test_cleanup_closes_when_reuse_disabled(self):
        """established_reuse=False → 败者清理仍关闭连接,池保持空(回归保护)。"""
        up_srv = await run_mock_proxy_delayed_connect(HOST, 31992, connect_delay=0.0)
        r = self._router(conn_pool_established_reuse=False)
        await r.start()
        try:
            target = "loser-off.example.com:443"
            reader, writer = await asyncio.open_connection(HOST, 31992)
            writer.write(f"CONNECT {target} HTTP/1.1\r\nHost: {target}\r\n\r\n".encode())
            await writer.drain()
            status = await reader.readline()
            assert b'200' in status
            await r._cleanup_tunnel_result(('slow', reader, writer), target=target)
            assert sum(len(v) for v in r._established_pool.values()) == 0, \
                "reuse disabled: loser tunnels must be closed, not pooled"
            assert r.established_pool_returned == 0
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()


class TestPrehandshake:
    """预握手(被动预建升级):命中粘性/域缓存/竞速胜出的 CONNECT,在既有"只建 TCP"
    预建之外额外自建一条 TCP 并发 CONNECT 预握手,拿到 200 直接进 _established_pool
    ——提升库存产生率,等同 target 请求到来时复用跳过握手。

    触发路径:粘性命中 CONNECT 分支 _spawn_target_prewarm 带 proxy_auth;预握手
    库存打 _prehandshook 标签,复用经 _established_pool_peek 命中(prehandshook
    归因计数),prune 按 established 独立超时(而非通用池超时)。
    """

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991,
                            auth=dict(username="u", password="p")))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_target_prewarm', True)
        kw.setdefault('conn_pool_established_reuse', True)
        kw.setdefault('conn_pool_prehandshake', True)
        kw.setdefault('conn_pool_per_proxy', 4)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动)
        kw.setdefault('conn_pool_idle_timeout', 100.0)   # 长超时,防测试中途过期
        kw.setdefault('stickiness_enabled', True)
        kw.setdefault('stickiness_ttl', 1800.0)
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    async def _wait_prehandshake(self, r, target, timeout=2.0):
        """轮询等待预握手库存落池(预握手是 fire-and-forget 后台任务)。"""
        key = f"{HOST}:31991|{target.decode()}"
        for _ in range(int(timeout / 0.02)):
            if r._established_pool.get(key):
                return True
            await asyncio.sleep(0.02)
        return False

    @pytest.mark.asyncio
    async def test_prehandshake_stocks_established_pool(self):
        """粘性命中 CONNECT → 后台预握手一条进 established 池(_prehandshook 标签)。

        走 sticky 命中路径(sticky_pid 触发 _spawn_target_prewarm 带 proxy_auth)。
        第一次请求建立粘性,第二次粘性命中触发预握手;轮询等待库存落池。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = b"prehandshake.example.com:443"
            # 第一次:建隧道 + 粘性记录。
            echo1 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo1 == b"one"
            assert r.sticky_cache_hits == 0  # 第一次是 miss(建立粘性)
            # 第二次:粘性命中 → 触发后台预握手。
            echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"two")
            assert echo2 == b"two"
            assert r.sticky_cache_hits >= 1
            assert await self._wait_prehandshake(r, target), \
                "预握手库存应落 established 池"
            key = f"{HOST}:31991|{target.decode()}"
            # 预握手隧道与第二次请求自己的归还隧道(established_reuse)可能同键共存、
            # 落池时序不定——断言"池中存在带标签的预握手库存"而非 index 0。
            assert any(getattr(w, '_prehandshook', False)
                       for _, w in r._established_pool.get(key, [])), \
                "established 池中应存在预握手库存(_prehandshook 标签)"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prehandshake_reuse_skips_connect(self):
        """预握手库存被复用:established_pool_peek 命中预握手隧道,prehandshook 归因 +1。

        单元级(时序确定):先 _prehandshake_one 落一条预握手库存,再 _established_pool_peek
        取用——命中即计 established_pool_hits 与 established_pool_prehandshook 归因。
        (E2E 复用跳过 CONNECT 由既有 TestEstablishedTunnelReuse.test_reuse_skips_connect
        覆盖,此处只验 prehandshook 专属归因路径。)
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        try:
            target = "prehandshake-reuse.example.com:443"
            proxy = r.proxy_store.get('p')
            ok = await r.pools._prehandshake_one(proxy.host, proxy.port, target,
                                                 proxy.auth)
            assert ok, "预握手应成功落池"
            key = f"{HOST}:31991|{target}"
            assert r._established_pool.get(key), "预握手库存应在池中"
            writer = r._established_pool[key][0][1]
            assert getattr(writer, '_prehandshook', False), "应打 _prehandshook 标签"
            # 取用:命中预握手隧道,prehandshook 归因 +1。
            got = r._established_pool_peek(HOST, 31991, target)
            assert got is not None, "取用应命中预握手库存"
            assert r.established_pool_hits == 1
            assert r.established_pool_prehandshook == 1, \
                "预握手库存命中应计 prehandshook 归因"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prehandshake_off_falls_back_to_tcp(self):
        """prehandshake=False → 预建仍是裸 TCP 进 target 池,无 _prehandshook 库存。

        预握手关闭时,_target_pool_prewarm 只建"到上游"的裸 TCP 进 _target_pool;
        established 池里即使有连接(粘性请求自己的隧道经 relay 归还)也不带
        _prehandshook 标签。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_prehandshake=False)
        await r.start()
        try:
            target = b"prehandshake-off.example.com:443"
            echo1 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            assert echo1 == b"one"
            echo2 = await send_connect(HOST, ROUTER_PORT, target=target, payload=b"two")
            assert echo2 == b"two"
            # 给后台预建一点时间(预建仍是裸 TCP 进 target 池)。
            await asyncio.sleep(0.2)
            key = f"{HOST}:31991|{target.decode()}"
            # established 池里可能有一条粘性请求自己的隧道(relay 归还),但不带
            # _prehandshook 标签(预握手被关闭)。
            for reader, writer in r._established_pool.get(key, []):
                assert not getattr(writer, '_prehandshook', False), \
                    "prehandshake=False:established 池不应有预握手库存"
            assert r.established_pool_prehandshook == 0
            # 预建仍应进 target 池(既有行为)。
            assert r.target_pool_creates >= 1, "裸 TCP 预建应照常发生"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prehandshake_respects_budget(self):
        """预算/cap 检查:established 池满(预算或单键 cap)时预握手跳过。

        直接驱动 pools._prehandshake_one,验证超预算返回 False 且不落池。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_total=1)  # 预算极小:established 池接近满
        await r.start()
        try:
            target = "prehandshake-budget.example.com:443"
            # 手工占满 established 池预算:直接插入一条(算作已有库存)。
            r._established_pool[f"{HOST}:31991|{target}"] = [(None, None)]
            proxy = r.proxy_store.get('p')
            ok = await r.pools._prehandshake_one(proxy.host, proxy.port, target,
                                                 proxy.auth)
            assert not ok, "预算已满时预握手应跳过(不落池不计数)"
            assert r.established_pool_prewarm_failed == 0, \
                "预算跳过不算失败(失败只计建连/CONNECT 非 200)"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_prehandshake_throttle_limits_burst(self):
        """预握手节流:滑动窗口内最多 max_per_window 条,超限静默跳过。

        直接驱动 pools._prehandshake_one 连续调 6 次(不同 target 避开单键 cap),
        window=60s/max=3 → 只发射 3 条,其余 3 条被节流跳过(throttled_skips+3),
        跳过不算失败。窗口内计数对并发预握手任务共享(风暴削峰)。
        """
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(conn_pool_prehandshake_throttle_window_sec=60.0,
                         conn_pool_prehandshake_throttle_max_per_window=3)
        await r.start()
        try:
            proxy = r.proxy_store.get('p')
            results = []
            for i in range(6):
                target = f"ph-throttle-{i}.example.com:443"
                results.append(await r.pools._prehandshake_one(
                    proxy.host, proxy.port, target, proxy.auth))
            assert results.count(True) == 3, \
                f"节流下应只发射 3 条,实际 {results.count(True)}"
            assert r.pools.prehandshake_throttled_skips == 3, \
                f"应跳过 3 条,实际 {r.pools.prehandshake_throttled_skips}"
            assert r.established_pool_prewarm_failed == 0, \
                "节流跳过不算失败"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()


class TestStickyProbeEviction:
    """杠杆A:粘性命中后台探路——命中后 fire-and-forget 对竞争代理做 CONNECT-only
    探路,探路赢家显著快于粘性代理则驱逐粘性条目(慢窗口从 TTL 压到请求级)。

    设计要点(与方案C _sticky_slow_probe_due 互补但不同):方案C 在"取用"时
    被动比较域名 EWMA;杠杆A 在"命中"后主动探路,用探路赢家的 CONNECT 握手
    EWMA 作 best 基准。探路不写 _domain_quality(只握手不拉数据,单次握手延迟
    喂域名 EWMA 会混入噪声);建出的隧道经 C 的 _cleanup_tunnel_result 归还池。
    节流:probe_interval_sec 冷却,时间戳在 _sticky_probe_race 启动时刷新
    (前置门提前 return 的分支不消耗节流)。默认 interval=0.0 关闭。
    """

    @staticmethod
    def _proxy(pid, **kw):
        return ProxyInfo(id=pid, host='h', port=3128, **kw)

    def _router(self, proxies=('fast', 'slow'), **kw):
        store = ProxyStore()
        for i, pid in enumerate(proxies):
            store.add(self._proxy(pid))
        kw.setdefault('stickiness_enabled', True)
        kw.setdefault('stickiness_ttl', 1800)
        kw.setdefault('single_send_degrade_ratio', 3.0)
        kw.setdefault('single_send_degrade_slack_ms', 10.0)
        kw.setdefault('sticky_probe_interval_sec', 1.0)
        kw.setdefault('sticky_probe_fanout', 2)
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    def test_probe_due_respects_cooldown(self):
        """节流:interval<=0 关闭恒 False;冷却内 False;超冷却 True(只读不刷新)。"""
        r = self._router(sticky_probe_interval_sec=0.0)
        assert not r.sticky_probe_due('1.2.3.4', 'example.com')
        r = self._router(sticky_probe_interval_sec=60.0)
        # 从未探过:due。
        assert r.sticky_probe_due('1.2.3.4', 'example.com')
        # 模拟刚探过(手动塞时间戳):冷却内 → 不 due。
        r._sticky_probe_last[r._sticky_key('1.2.3.4', 'example.com')] = time.monotonic()
        assert not r.sticky_probe_due('1.2.3.4', 'example.com')
        # 冷却内调用不刷新时间戳(只读判定)。
        assert r._sticky_probe_last[r._sticky_key('1.2.3.4', 'example.com')] > 0
        # 时间戳推到冷却前 → due。
        r._sticky_probe_last[r._sticky_key('1.2.3.4', 'example.com')] = time.monotonic() - 61.0
        assert r.sticky_probe_due('1.2.3.4', 'example.com')
        # 从未探过(None sentinel):恒放行,不依赖单调钟与 interval 的绝对关系。
        # (fresh 容器单调钟从启动计,uptime<interval 时旧 0.0 默认会误判冷却内。)
        r._sticky_probe_last[r._sticky_key('1.2.3.4', 'example.com')] = None
        assert r.sticky_probe_due('1.2.3.4', 'example.com')

    def test_spawn_skips_when_interval_off(self):
        """interval<=0(默认关闭):_spawn_sticky_probe 直接 return,零任务、零计数。"""
        r = self._router(sticky_probe_interval_sec=0.0)
        r.selector.record_ttfb('slow', 0.5, 'example.com')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        r._spawn_sticky_probe('1.2.3.4', 'example.com', 'slow')
        assert r.sticky_probes_fired == 0
        # 无探路任务排队(probe_last 未写入,证明没进 race)。
        assert not r._sticky_probe_last

    def test_spawn_skips_when_no_domain_obs(self):
        """sticky 记账桶无域名观测:无从比较快慢,探路不发射。"""
        r = self._router()
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        r._spawn_sticky_probe('1.2.3.4', 'example.com', 'slow')
        assert r.sticky_probes_fired == 0
        assert not r._sticky_probe_last

    def test_spawn_skips_when_sticky_local(self):
        """sticky 是 local 直连:无上游可探,探路不发射。"""
        r = self._router()
        r.selector.record_ttfb('slow', 0.5, 'example.com')
        r._spawn_sticky_probe('1.2.3.4', 'example.com', 'local')
        assert r.sticky_probes_fired == 0

    @pytest.mark.asyncio
    async def test_probe_race_evicts_sticky(self, monkeypatch):
        """探路赢家显著快于粘性代理 → 驱逐粘性条目 + 计数。

        monkeypatch _race_staggered 直接返回 (fast, r, w) 作为探路赢家;sticky
        代理 slow 的 domain EWMA=0.50,fast 在 probe_target 桶 EWMA=0.01 →
        0.50 > 0.01*3.0 且差>10ms → 驱逐。探路计数 +1,驱逐计数 +1。
        """
        async def fake_race(places, cleanup=None, **kw):
            # 探路候选应剔除 sticky_pid/slow 与 local,只剩 fast。
            assert [p[0] for p in places] == ['fast'], f"places={places}"
            return ('fast', object(), object())

        r = self._router()
        r.selector.record_ttfb('slow', 0.50, 'example.com')
        # fast 在 CONNECT 桶("example.com:443")的 EWMA:探路基准(best)。
        r.selector.record_ttfb('fast', 0.01, 'example.com:443')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'slow'
        monkeypatch.setattr(r, '_race_staggered', fake_race)
        await r._sticky_probe_race('1.2.3.4', 'example.com', 'example.com:443', 'slow')
        # 驱逐后:下次取用 miss,回落竞速。
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') is None
        assert r.sticky_probes_fired == 1
        assert r.sticky_probe_evictions == 1

    @pytest.mark.asyncio
    async def test_probe_race_keeps_sticky_when_not_faster(self, monkeypatch):
        """探路赢家不比粘性代理显著快(慢于阈值)→ 不驱逐,保持粘性单发。"""
        async def fake_race(places, cleanup=None, **kw):
            return ('fast', object(), object())

        r = self._router()
        r.selector.record_ttfb('slow', 0.10, 'example.com')
        r.selector.record_ttfb('fast', 0.15, 'example.com:443')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'slow'
        monkeypatch.setattr(r, '_race_staggered', fake_race)
        await r._sticky_probe_race('1.2.3.4', 'example.com', 'example.com:443', 'slow')
        # 0.10 < 0.15*3=0.45 → 不驱逐,粘性保留;计数只加发射不加驱逐。
        assert r._get_sticky_proxy('1.2.3.4', 'example.com') == 'slow'
        assert r.sticky_probes_fired == 1
        assert r.sticky_probe_evictions == 0

    def test_counters_reported_in_metrics(self):
        """探路发射/驱逐计数进入 snapshot_counters() 供 /metrics 展示。"""
        r = self._router()
        r.selector.record_ttfb('slow', 0.50, 'example.com')
        r.selector.record_ttfb('fast', 0.01, 'example.com:443')
        r._record_sticky('1.2.3.4', 'example.com', 'slow')
        r.sticky.sticky_probes_fired = 5
        r.sticky.sticky_probe_evictions = 2
        c = r.snapshot_counters()
        assert c['sticky_probes_fired'] == 5
        assert c['sticky_probe_evictions'] == 2
        # 白名单:直接经 Router 访问也能解析(sticky 转发表)。
        assert r.sticky_probes_fired == 5
        assert r.sticky_probe_evictions == 2


class TestClusterPredictor:
    """请求簇预测预热(ClusterGraph):客户端窗口分组、全局共现图学习、窗口开口
    预测并提前预建同簇 co-target 的 TCP。单元级直接构造 ClusterGraph + 记录式
    spawn stub 驱动(注入合成时钟,零事件循环噪音);E2E 级用 send_connect 打真实
    CONNECT 到 mock 上游,验证预测预建最终落入 (proxy, co-target) 目标池。"""

    @staticmethod
    def _graph(**kw) -> ClusterGraph:
        """直接构造 ClusterGraph(默认 enabled + 一个可解析的代理 p)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        kw.setdefault('enabled', True)
        return ClusterGraph(store, **kw)

    @staticmethod
    def _stub_spawn():
        """记录式 spawn stub:(host, port, target) → 追加到 calls。"""
        class _Stub:
            def __init__(self):
                self.calls = []
            def __call__(self, host, port, target):
                self.calls.append((host, port, target))
        return _Stub()

    @staticmethod
    def _observe_window(g, ip, targets, pid='p', t0=10.0):
        """把 targets 作为一簇观察进同一窗口(全部落在窗口宽内),不关窗。"""
        for t in targets:
            g.observe(ip, t, pid, now=t0)
        return t0

    @classmethod
    def _learn(cls, g, ip, targets, pid='p', t0=10.0):
        """观察一簇目标后,用超过窗口宽的下一请求触发批量关闭学习(窗内共现入图)。

        各测试调用需递增 t0(步长 ≥ 12s > window_sec + close 偏移),避免后续观察
        误入上一关窗标记的窗口中。
        """
        cls._observe_window(g, ip, targets, pid, t0)
        g.observe(ip, 'close-marker.example:443', pid, now=t0 + 10.0)

    def test_window_groups_burst(self):
        """同一客户端 2s 内的并发 CONNECT 归同一窗口(簇),不提前关窗学习。"""
        g = self._graph(min_support=1)
        try:
            g.observe('9.9.9.9', 'a.com:443', 'p', now=10.0)
            g.observe('9.9.9.9', 'b.com:443', 'p', now=10.05)   # 距 a 0.05s < 2s → 同窗
            g.observe('9.9.9.9', 'c.com:443', 'p', now=10.1)
            assert g.cluster_windows_learned == 0               # 窗口未关即未学习
            g.observe('9.9.9.9', 'a.com:443', 'p', now=13.0)   # 距首请求 3s > 2s → 关窗学习
            assert g.cluster_windows_learned == 1
            assert g._active_windows['9.9.9.9'].observed == [('a.com:443', 'p')]
        finally:
            g.reset()

    def test_learn_cooccurrence_on_window_close(self):
        """窗口关闭学习共现:同窗 (a,b,c) → 双向边 hits=1(去重)、probe_pid 正确。"""
        g = self._graph()
        try:
            g.observe('9.9.9.9', 'a.com:443', 'p', now=10.0)
            g.observe('9.9.9.9', 'b.com:443', 'p', now=10.05)
            g.observe('9.9.9.9', 'c.com:443', 'p', now=10.1)
            g.observe('9.9.9.9', 'a.com:443', 'p', now=10.2)   # 同窗重复 → 去重,不双倍计数
            g.observe('9.9.9.9', 'x.example:443', 'p', now=20.0)  # 距首请求 > 窗口宽 → 关窗学习
            assert g.cluster_windows_learned == 1
            snap = g.get_cluster_cache()
            assert snap['a.com:443']['b.com:443'] == (1, 'p', {'p': 1.0})
            assert snap['b.com:443']['a.com:443'] == (1, 'p', {'p': 1.0})
            assert snap['b.com:443']['c.com:443'] == (1, 'p', {'p': 1.0})
            assert snap['a.com:443']['c.com:443'] == (1, 'p', {'p': 1.0})
            # 关窗触发器(x.example)不在被学窗口内 → 不产生任何边。
            assert 'a.com:443' not in snap.get('x.example:443', {})
        finally:
            g.reset()

    def test_min_support_gates_prediction(self):
        """min_support:单次共现(< 阈值)不预测;达到支持度后窗口开口即发射。"""
        # 情形 1:min_support=1,一次学习即可预测。
        spawn = self._stub_spawn()
        g = self._graph(min_support=1, prewarm_spawn=spawn)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=10.0)
            assert g.cluster_windows_learned == 1
            g.observe('7.7.7.7', 'a.com:443', 'p', now=30.0)   # 新窗开口带 pid → 预测 b
            assert g.cluster_predictions == 1
            assert g.cluster_prewarm_spawned == 1
            assert spawn.calls == [(HOST, 31991, 'b.com:443')]
        finally:
            g.reset()
        # 情形 2:min_support=2,首次共现不预测,第二次学习后才发射。
        spawn2 = self._stub_spawn()
        g = self._graph(min_support=2, prewarm_spawn=spawn2)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=10.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=30.0)   # 支持度 1 < 2 → 不发射
            assert g.cluster_predictions == 0
            assert g.cluster_prewarm_spawned == 0
            assert spawn2.calls == []
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=40.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=60.0)   # 支持度 2 → 发射
            assert g.cluster_predictions == 1
            assert g.cluster_prewarm_spawned == 1
            assert spawn2.calls == [(HOST, 31991, 'b.com:443')]
        finally:
            g.reset()

    def test_prediction_skips_co_in_window(self):
        """窗口开口预测跳过当前窗口已观察的目标;按 probe_pid 解析到 (host, port) 发射。"""
        spawn = self._stub_spawn()
        g = self._graph(min_support=1, prewarm_spawn=spawn)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443', 'c.com:443'], t0=10.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=30.0)   # 新窗开口:a 首带 pid → 预测 co
            assert spawn.calls == [(HOST, 31991, 'b.com:443'), (HOST, 31991, 'c.com:443')]
            g.observe('7.7.7.7', 'b.com:443', 'p', now=30.05)  # b 已真实到达,窗内已含 → 不重预测
            g.observe('7.7.7.7', 'c.com:443', 'p', now=30.1)
            assert g.cluster_predictions == 1                  # 整窗至多预测一次
        finally:
            g.reset()

    def test_throttle_suppresses_reload_repredict(self):
        """同 (src→co) 对在节流窗内不重复发射,防 reload 反复预建。"""
        spawn = self._stub_spawn()
        g = self._graph(min_support=1, prewarm_spawn=spawn, throttle_sec=30.0)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=10.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=30.0)   # 预测 b,节流开始(now=30)
            assert g.cluster_predictions == 1
            assert len(spawn.calls) == 1
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=40.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=55.0)   # 距上次 25s < 30s → 节流跳过
            assert g.cluster_predictions == 1
            assert len(spawn.calls) == 1
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=70.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=90.0)   # 距上次 60s > 30s → 恢复发射
            assert g.cluster_predictions == 2
            assert len(spawn.calls) == 2
        finally:
            g.reset()

    def test_probe_pids_learn_fanout(self):
        """方案 A:同一 co 在多个代理上胜出 → 直方图计数多个 pid;预测摊到计数最高的
        fanout 个桶各发射一条(单条 co 的 spawn 数 > 1,cluster_prewarm_spawned 增量为
        fanout 而非 1)。"""
        spawn = self._stub_spawn()
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        store.add(ProxyInfo(id='q', host=HOST, port=31992))
        g = ClusterGraph(store, enabled=True, min_support=1, proxy_fanout=2,
                         probe_decay_sec=1e6, prewarm_spawn=spawn)
        try:
            # 窗口1:同簇 (a,b),b 由 p 胜出。
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='p', t0=10.0)
            # 窗口2:同簇 (a,b),b 由 q 胜出 → 直方图 {p:1, q:1}。
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='q', t0=40.0)
            snap = g.get_cluster_cache()
            hist = snap['a.com:443']['b.com:443'][2]
            assert abs(hist['p'] - 1.0) < 1e-2 and abs(hist['q'] - 1.0) < 1e-2, \
                f"超大半衰期下直方图应≈{1.0, 1.0}(实测 {hist})"
            # 窗口3:开口先连 a → 预测 b 摊到 p、q 两个桶。预测计数用增量(学习窗的
            # 窗口开口首 pid 也会触发预测,直方图此时还没 q → 只发 1 桶,已计入基线)。
            sp0, bs0, pr0 = g.cluster_prewarm_spawned, g.cluster_bucket_spawns, g.cluster_predictions
            g.observe('7.7.7.7', 'a.com:443', 'p', now=70.0)
            assert g.cluster_predictions == pr0 + 1
            assert g.cluster_prewarm_spawned == sp0 + 2
            assert g.cluster_bucket_spawns == bs0 + 2
            assert sorted(spawn.calls[-2:]) == [(HOST, 31991, 'b.com:443'), (HOST, 31992, 'b.com:443')]
        finally:
            g.reset()

    def test_resolve_skips_circuit_open_proxy(self):
        """熔断感知:is_circuit_open(pid) 的代理被 _resolve_top 跳过,不摊到熔断桶。
        直方图 {p,q} 且 q 熔断 → 只发 p(不建白桶);桶数/发射条数相应减少。"""
        spawn = self._stub_spawn()
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        store.add(ProxyInfo(id='q', host=HOST, port=31992))
        open_pids = {'q'}
        g = ClusterGraph(store, enabled=True, min_support=1, proxy_fanout=2,
                         probe_decay_sec=1e6, prewarm_spawn=spawn,
                         is_circuit_open=lambda pid: pid in open_pids)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='p', t0=10.0)
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='q', t0=40.0)
            # 窗口3:直方图 {p:1,q:1},q 熔断 → 只解析出 p 一个桶。
            sp0, bs0 = g.cluster_prewarm_spawned, g.cluster_bucket_spawns
            g.observe('7.7.7.7', 'a.com:443', 'p', now=70.0)
            assert g.cluster_prewarm_spawned == sp0 + 1, "熔断桶不发射"
            assert g.cluster_bucket_spawns == bs0 + 1, "熔断桶不计数"
            assert spawn.calls[-1] == (HOST, 31991, 'b.com:443'), "只摊到非熔断桶 p"
        finally:
            g.reset()

    def test_resolve_without_circuit_callback_falls_back(self):
        """未注入 is_circuit_open(None)→ 等价全部代理可用,不跳过(向后兼容)。"""
        spawn = self._stub_spawn()
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        store.add(ProxyInfo(id='q', host=HOST, port=31992))
        g = ClusterGraph(store, enabled=True, min_support=1, proxy_fanout=2,
                         probe_decay_sec=1e6, prewarm_spawn=spawn)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='p', t0=10.0)
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='q', t0=40.0)
            g.observe('7.7.7.7', 'a.com:443', 'p', now=70.0)
            assert sorted(spawn.calls[-2:]) == [(HOST, 31991, 'b.com:443'), (HOST, 31992, 'b.com:443')]
        finally:
            g.reset()

    def test_bucket_spawns_equals_len_proxies(self):
        """cluster_bucket_spawns 语义:一次预测摊 N 个可解析代理桶 → 增量恰为 N,
        与实际发射条数(cluster_prewarm_spawned)一致但独立计数(不随 _fire 重复累加)。"""
        spawn = self._stub_spawn()
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        store.add(ProxyInfo(id='q', host=HOST, port=31992))
        g = ClusterGraph(store, enabled=True, min_support=1, proxy_fanout=2,
                         probe_decay_sec=1e6, prewarm_spawn=spawn)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='p', t0=10.0)
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='q', t0=40.0)
            sp0, bs0 = g.cluster_prewarm_spawned, g.cluster_bucket_spawns
            g.observe('7.7.7.7', 'a.com:443', 'p', now=70.0)
            assert g.cluster_prewarm_spawned - sp0 == 2
            assert g.cluster_bucket_spawns - bs0 == 2
            assert g.cluster_bucket_spawns == g.cluster_prewarm_spawned
        finally:
            g.reset()

    def test_probe_pids_decay_and_fanout_one(self):
        """方案 A 边界:probe_pids 直方图带 last_seen 衰减(旧 pid 计数折半后序仍在列);
        fanout=1 时退化为旧单桶行为(只发一条到最高计数桶)。"""
        # fanout=1:多 pid 直方图仍学习,但只摊 1 桶。
        spawn = self._stub_spawn()
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        store.add(ProxyInfo(id='q', host=HOST, port=31992))
        g = ClusterGraph(store, enabled=True, min_support=1, proxy_fanout=1,
                         probe_decay_sec=1e6, prewarm_spawn=spawn)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='p', t0=10.0)
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='q', t0=40.0)
            # 直方图 {p:1, q:1},fanout=1 → 只发 p 一个桶(增量:学习窗开口已发 1 条基线)。
            # 学习窗2 里 b 由 q 胜出(直方图 q 略高),故预测观察用一个由 p 胜出的目标作
            # src,且 window-open 时 b 的直方图 q 仍 > p —— 让 b 本轮由 p 胜出刷新 last_seen
            # 后再观察 a,使 p 成为最高桶。
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], pid='p', t0=70.0)
            sp0 = g.cluster_prewarm_spawned
            g.observe('7.7.7.7', 'a.com:443', 'p', now=100.0)
            assert g.cluster_prewarm_spawned == sp0 + 1
            assert spawn.calls[-1] == (HOST, 31991, 'b.com:443')
        finally:
            g.reset()
        # 衰减:距上次 bump 一个半衰期后,新胜出使旧 pid 计数折半;再一窗 p 复胜恢复。
        g = self._graph(min_support=1, probe_decay_sec=10.0)
        try:
            self._learn(g, '6.6.6.6', ['a.com:443', 'b.com:443'], t0=10.0)  # {p:1},last_seen=20
            self._learn(g, '6.6.6.6', ['a.com:443', 'b.com:443'], t0=40.0)  # 距上次 20s=2 半衰 → 旧 0.25 +1
            snap = g.get_cluster_cache()
            pv = snap['a.com:443']['b.com:443'][2]['p']
            assert pv < 1.5, f"旧计数应衰减而不是简单累加(实测 {pv})"
            assert pv > 1.0, f"衰减不应清空(0.25+1=1.25,实测 {pv})"
        finally:
            g.reset()

    def test_graph_cap_lru_evicts_oldest(self):
        """边数超 max_entries → 淘汰 last_seen 最旧的边(LRU 上限,仿 sticky)。"""
        g = self._graph(max_entries=3)
        try:
            self._learn(g, '6.6.6.6', ['a.com:443', 'b.com:443'], t0=10.0)  # a|b、b|a
            self._learn(g, '6.6.6.6', ['a.com:443', 'c.com:443'], t0=30.0)  # +a|c、c|a → 4 边 > 3
            assert g.graph_size() == 3                       # 超限即淘汰最旧(a→b,last_seen=10)
            snap = g.get_cluster_cache()
            assert 'b.com:443' in snap                        # b→a 仍在(direction 各自独立)
            assert 'b.com:443' not in snap['a.com:443']       # a→b 已被 LRU 淘汰
            assert snap['a.com:443'] == {'c.com:443': (1, 'p', {'p': 1.0})}
        finally:
            g.reset()

    def test_prune_ttl_and_stale_window_close(self):
        """prune:TTL 过期边清理;越过窗口宽未关的陈旧窗口兜底关窗学习。"""
        spawn = self._stub_spawn()
        g = self._graph(prewarm_spawn=spawn, ttl_sec=3600)
        try:
            self._learn(g, '7.7.7.7', ['a.com:443', 'b.com:443'], t0=10.0)  # 关窗时刻 20.0 → 边 last_seen=20
            assert g.graph_size() == 2
            g.prune(now=20.0 + 3600 + 1)                     # 距 last_seen 超过 ttl → 边清理
            assert g.graph_size() == 0
            # 陈旧窗口:观察后不关,prune 兜底关闭并学习(c,d 同窗共现)。
            g.observe('5.5.5.5', 'c.com:443', 'p', now=1000.0)
            g.observe('5.5.5.5', 'd.com:443', 'p', now=1001.0)
            assert g.cluster_windows_learned == 1            # 仅 a|b 一窗已学
            g.prune(now=1010.0)                              # 1010-1000=10s > 2s → 关窗
            assert g.cluster_windows_learned == 2
            assert g.graph_size() == 2                       # c|d、d|c 双向边
        finally:
            g.reset()

    @pytest.mark.asyncio
    async def test_e2e_learn_then_predict_prewarms(self):
        """端到端:一个窗口内两个 CONNECT('page-a','page-b')同簇入窗学习;再开新窗
        先连 page-a → 预测 page-b 并预建 (proxy, page-b) 的 TCP 进 _target_pool。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router()
        await r.start()
        key = f"{HOST}:31991|page-b.example.com:443"
        try:
            # 第一窗:page-a 与 page-b 同簇学习(同 client_ip,两个 CONNECT 间隔远小于
            # cluster_window_sec=2s → 同一窗口)。flush 周期 5s 太久,直接 sleep 越过
            # 窗口宽后手动 prune 关窗学习。
            await send_connect(HOST, ROUTER_PORT, target=b"page-a.example.com:443", payload=b"a")
            await send_connect(HOST, ROUTER_PORT, target=b"page-b.example.com:443", payload=b"b")
            await asyncio.sleep(2.2)          # > cluster_window_sec(2.0)
            r.cluster.prune()
            assert r.cluster_windows_learned == 1, "第一窗 (page-a,page-b) 应已学习"
            assert r.cluster_prewarm_spawned == 0
            # 第二窗:先连 page-a → 窗口开口预测 page-b 并预建(经 _spawn_target_prewarm)。
            await send_connect(HOST, ROUTER_PORT, target=b"page-a.example.com:443", payload=b"a2")
            for _ in range(200):
                if r.cluster_prewarm_spawned >= 1 and len(r._target_pool.get(key, [])) >= 1:
                    break
                await asyncio.sleep(0.01)
            assert r.cluster_predictions >= 1, "窗口开口应预测 page-a 的 co-target"
            assert r.cluster_prewarm_spawned >= 1, "预测应实际发射预建"
            assert len(r._target_pool.get(key, [])) >= 1, "预测预建应落入 (proxy, page-b) 目标池"
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_cluster_fanout_prewarms_multiple_proxies(self):
        """方案 A 端到端:co 在两个代理上各胜出过 → 预测把同一 co 预建到两个代理桶;
        cluster_bucket_spawns 增量随桶数累加,两桶都应有 cluster 预建连接落入。"""
        up1 = await run_mock_proxy_delayed_connect(HOST, 31991)          # p
        up2 = await run_mock_proxy_delayed_connect(HOST, 31992)          # q
        # 本测试必须自建 Router 并注册 p、q 两个代理(共享 _router 只有 p,直方图学不到 q)。
        # 预算调大:两桶各 cap=2 需 4 条,且测试期间有被动预建/竞速占 fd,默认 8 会被
        # 并发 refill 的陈旧快照饿到(先到的桶把预算占满,后到的桶跳过)。
        # stagger_start=False:错峰默认首批只发 1 个代理,慢的 p 仍无对手独赢;
        # 关掉错峰让 p、q 全发,`_delay` 才能逼目标代理赢(竞速判首回 200)。
        # min_support=2(默认):窗 1 只产生 hits=1 的边,窗 2 开口不预测——若窗 2 开口
        # 就预测,预建会先占住 p 桶,fa-b 的 _try_tunnel peek 复用 → 窗 2 学不到 q。
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        store.add(ProxyInfo(id='q', host=HOST, port=31992))
        r = Router(store, listen_host='127.0.0.1', listen_port=ROUTER_PORT,
                   db_path=tempfile.mktemp(suffix='.db'),
                   conn_pool_enabled=True, conn_pool_target_prewarm=True,
                   cluster_predict=True, cluster_min_support=2,
                   conn_pool_per_proxy=4, conn_pool_total=16,
                   conn_pool_refill_interval=0.0,  # 只取不补(测试手动触发)
                   stagger_start=False)
        await r.start()
        try:
            # 学习窗1:同簇 (fa-q, fa-b),fa-b 由代理 p(31991) 胜出(q 慢 0.15s → p 必赢)。
            # 直方图 {p:1};hits=1 < min_support=2,窗 2 开口不预测。
            up2._delay = 0.15
            await send_connect(HOST, ROUTER_PORT, target=b"fa-q.example.com:443", payload=b"q1")
            await send_connect(HOST, ROUTER_PORT, target=b"fa-b.example.com:443", payload=b"b1")
            await asyncio.sleep(2.2)
            r.cluster.prune()
            assert r.cluster_windows_learned == 1
            hist1 = r.cluster.get_cluster_cache()['fa-q.example.com:443']['fa-b.example.com:443'][2]
            assert hist1 == {'p': 1.0}, f"窗1直方图应只有 p(实测 {hist1})"
            await r._conn_pool_close_all()   # 去第一窗被动预建,清池
            # 学习窗2:p 慢 → fa-q、fa-b 都由 q(31992) 胜出 → 直方图 {p:1, q:1},hits=2。
            # 清空域缓存 + 粘性表让 fa-q/fa-b 重新竞速(p 慢 0.15s → q 必赢)。
            r._meta_cache.clear()
            r._sticky_cache.clear()
            up2._delay = 0.0
            up1._delay = 0.15
            await send_connect(HOST, ROUTER_PORT, target=b"fa-q.example.com:443", payload=b"q2")
            await send_connect(HOST, ROUTER_PORT, target=b"fa-b.example.com:443", payload=b"b2")
            await asyncio.sleep(2.2)
            r.cluster.prune()
            assert r.cluster_windows_learned == 2
            hist2 = r.cluster.get_cluster_cache()['fa-q.example.com:443']['fa-b.example.com:443'][2]
            assert 'p' in hist2 and 'q' in hist2, f"窗2直方图应含 p 与 q(实测 {hist2})"
            await r._conn_pool_close_all()
            # 再开新窗先连 fa-q → 预测 fa-b 应摊到 p、q 两桶(直方图含 p 与 q)。
            up1._delay = 0.0
            sp0 = r.cluster_prewarm_spawned
            bs0 = r.cluster_bucket_spawns
            await send_connect(HOST, ROUTER_PORT, target=b"fa-q.example.com:443", payload=b"q3")
            for _ in range(200):
                if (len(r._target_pool.get(f"{HOST}:31991|fa-b.example.com:443", [])) >= 1
                        and len(r._target_pool.get(f"{HOST}:31992|fa-b.example.com:443", [])) >= 1):
                    break
                await asyncio.sleep(0.01)
            assert r.cluster_prewarm_spawned == sp0 + 2, "直方图两代理 → 摊 2 桶"
            assert r.cluster_bucket_spawns == bs0 + 2
            assert len(r._target_pool.get(f"{HOST}:31991|fa-b.example.com:443", [])) >= 1
            assert len(r._target_pool.get(f"{HOST}:31992|fa-b.example.com:443", [])) >= 1
        finally:
            await r.stop()
            up1.close()
            await up1.wait_closed()
            up2.close()
            await up2.wait_closed()

    @pytest.mark.asyncio
    async def test_cluster_prewarm_conns_are_tagged(self):
        """归因:cluster 预测预建连接打 _cluster_prewarmed 标签并计 cluster_pool_creates;
        取用该连接计 cluster_pool_hits(同一命中同样计入 target_pool_hits)。学习窗内的
        两次 CONNECT 若各自触发被动预建会先占住键位,故两轮之间清空池子,确保第二轮
        只由预测预建填充。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(cluster_predict=True, cluster_min_support=1)
        await r.start()
        key_b = f"{HOST}:31991|tagged-b.example.com:443"
        try:
            # 第一窗:tagged-a 与 tagged-b 同簇学习(同 client_ip,间隔 < 窗口宽)。
            await send_connect(HOST, ROUTER_PORT, target=b"tagged-a.example.com:443", payload=b"a")
            await send_connect(HOST, ROUTER_PORT, target=b"tagged-b.example.com:443", payload=b"b")
            await asyncio.sleep(2.2)
            r.cluster.prune()
            assert r.cluster_windows_learned == 1
            # 清空池子:去掉第一轮固有被动预建(它们会先占住 tagged-b 键位)。
            await r._conn_pool_close_all()
            assert len(r._target_pool.get(key_b, [])) == 0
            # 第二窗:先连 tagged-a → 开口预测 tagged-b 并预建(仅 cluster 来源)。
            await send_connect(HOST, ROUTER_PORT, target=b"tagged-a.example.com:443", payload=b"a2")
            for _ in range(200):
                if len(r._target_pool.get(key_b, [])) >= 1:
                    break
                await asyncio.sleep(0.01)
            stack = r._target_pool[key_b]
            assert stack, "预测预建应落入 (proxy, tagged-b) 目标池"
            # 预测预建应已打上 cluster 标签并计入 cluster_pool_creates(被动归零)。
            assert all(getattr(w, '_cluster_prewarmed', False) for _, w in stack)
            tagged = r.cluster_pool_creates
            assert tagged >= 1
            assert r.cluster_pool_expired == 0
            # 取用该 cluster 预建连接:cluster_pool_hits +1(目标池命中同样计入)。
            got = r._target_pool_peek(HOST, 31991, 'tagged-b.example.com:443')
            assert got is not None
            assert r.cluster_pool_hits == 1
            assert r.target_pool_hits == 1
            assert r.cluster_pool_creates == tagged  # peek 不改变 creates
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_probe_distinguishes_timing_from_bucket_miss(self):
        """探针(C):区分 cluster 预建空转的病因——
        键曾在 cluster 预建但取用为空 → timing_miss(时序没赶上);
        键从未有 cluster 预建 → bucket_miss(代理桶不匹配)。
        计数是全局的(非按键),故断言用探测操作前后的增量,不用绝对 0/1。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        r = self._router(cluster_predict=True, cluster_min_support=1)
        await r.start()
        key_b = f"{HOST}:31991|pg-probe-b.example.com:443"
        try:
            # 第一窗学习 (pg-probe-a, pg-probe-b) 同簇。
            await send_connect(HOST, ROUTER_PORT, target=b"pg-probe-a.example.com:443", payload=b"a")
            await send_connect(HOST, ROUTER_PORT, target=b"pg-probe-b.example.com:443", payload=b"b")
            await asyncio.sleep(2.2)
            r.cluster.prune()
            await r._conn_pool_close_all()   # 去掉第一窗被动预建,清空池
            # 第二窗先连 pg-probe-a → 预测预建 pg-probe-b(cluster 来源,timing 标记出现)。
            await send_connect(HOST, ROUTER_PORT, target=b"pg-probe-a.example.com:443", payload=b"a2")
            for _ in range(200):
                if len(r._target_pool.get(key_b, [])) >= 1:
                    break
                await asyncio.sleep(0.01)
            assert r.cluster_pool_creates >= 1, "应出现 cluster 预建"
            # 池里有预建但连接是裸的(r.pools 侧)。标记随键映射由 pools 持有,经
            # _POOL_FORWARD 可读(诊断只读)。
            assert r._target_pool_cluster_ever.get(key_b) == 1, "桶应打上 cluster 预建标记"
            # 此刻忽略全局已累积的 miss(timing/bucket 都是全局计数),只测增量。
            tm0, bm0 = r.cluster_pool_timing_miss, r.cluster_pool_bucket_miss
            # 强制该 cluster 连接过期(不消费) → prune 关闭 → consumed_expired 不增。
            # 注意:cluster 连接用独立超时 cluster_pool_idle_timeout(默认 600s),
            # 伪造时间必须超过它才能被 prune 关掉(而非被动超时 180s)。
            for stack in r._target_pool.values():
                for _, w in stack:
                    w._conn_pool_created = time.monotonic() - 900
            await r._pool_prune()
            assert r.cluster_pool_expired >= 1
            assert r.cluster_pool_consumed_expired == 0, "未消费直接空转不应计入 consumed_expired"
            # 池空且该桶有 cluster 预建标记 → 此 miss 归 timing_miss。
            got = r._target_pool_peek(HOST, 31991, 'pg-probe-b.example.com:443')
            assert got is None
            assert r.cluster_pool_timing_miss == tm0 + 1
            assert r.cluster_pool_bucket_miss == bm0, "有 cluster 预建的桶不累计 bucket_miss"
            # 另一桶从未有 cluster 预建 → 此 miss 归 bucket_miss。
            got2 = r._target_pool_peek(HOST, 31991, 'pg-bucketmiss.example.com:443')
            assert got2 is None
            assert r.cluster_pool_bucket_miss == bm0 + 1
            assert r.cluster_pool_timing_miss == tm0 + 1
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_passive_prewarm_not_counted_as_cluster(self):
        """归因:被动预建(竞速胜出/域缓存触发,source 默认 'passive')不打 cluster 标签,
        命中复用也不计 cluster_pool_hits——cluster 计数只归预测预建。"""
        up_srv = await run_mock_proxy(HOST, 31991, hit_counter=None)
        # cluster_predict=False:只剩被动预建,是归因的反例(不得计入 cluster_pool_*)。
        r = self._router(cluster_predict=False, enable_local_racing=False)
        await r.start()
        try:
            target = b"passive-pre.example.com:443"
            # 首次竞速 → 胜者是真实上游(非 local)→ 后台被动预建到 (proxy, target)。
            await send_connect(HOST, ROUTER_PORT, target=target, payload=b"one")
            for _ in range(200):
                if len(r._target_pool.get(f"{HOST}:31991|passive-pre.example.com:443", [])) >= 1:
                    break
                await asyncio.sleep(0.01)
            assert r.target_pool_creates >= 1, "被动预建应建连"
            assert r.cluster_pool_creates == 0, "被动预建不得计入 cluster_pool_creates"
            # 复用该被动预建连接 → target_pool_hits +1,cluster_pool_hits 保持 0。
            hits_before = r.target_pool_hits
            await send_connect(HOST, ROUTER_PORT, target=target, payload=b"two")
            assert r.target_pool_hits == hits_before + 1
            assert r.cluster_pool_hits == 0
            assert r.cluster_pool_expired == 0
        finally:
            await r.stop()
            up_srv.close()
            await up_srv.wait_closed()

    def test_cluster_attribution_snapshot(self):
        """快照暴露 cluster 专属归因计数(观察点,供 /metrics 与 opt.log)。"""
        r = self._router(cluster_predict=True)
        s = r.snapshot_counters()
        assert s['cluster_pool_creates'] == 0
        assert s['cluster_pool_hits'] == 0
        assert s['cluster_pool_expired'] == 0
        assert s['cluster_pool_timing_miss'] == 0
        assert s['cluster_pool_bucket_miss'] == 0
        assert s['cluster_pool_consumed_expired'] == 0
        assert s['cluster_bucket_spawns'] == 0

    @staticmethod
    def _router(**kw) -> Router:
        """e2e 用 Router:enable 全链(cluster 依赖 conn_pool.enabled + target_prewarm)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        kw.setdefault('conn_pool_enabled', True)
        kw.setdefault('conn_pool_target_prewarm', True)
        kw.setdefault('cluster_predict', True)
        # e2e 只有一窗学习,min_support 默认 2 会门住预测 → 降到 1。
        kw.setdefault('cluster_min_support', 1)
        kw.setdefault('conn_pool_per_proxy', 4)
        kw.setdefault('conn_pool_total', 8)
        kw.setdefault('conn_pool_refill_interval', 0.0)  # 只取不补(测试手动触发)
        return Router(store, listen_host='127.0.0.1', listen_port=ROUTER_PORT,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)


class TestRouterConfigPassThrough:
    """回归 #15:Router(router_cfg=...) 与逐 kwarg 构造完全等价(配置单一真相源)。

    cli.py 删掉了 ~55 kwarg 手工映射,改为把 RouterConfig 整块传入。测试钉死:
    ① 两条构造路径的 snapshot_counters() 与关键配置属性一致;
    ② router_cfg 优先于同名 kwarg(不靠位置/默认值猜);
    ③ 池成员经 __getattr__/__setattr__ 白名单转发到 self.pools(同一对象)。
    """

    @staticmethod
    def _cfg_kwargs(cfg: RouterConfig) -> dict:
        """把 RouterConfig 还原成旧 Router(**kwargs) 的扁平 dict(镜像 #15 拆掉的映射)。"""
        c, cc, auth, stick = cfg, cfg.circuit, cfg.auth, cfg.stickiness
        hc, at, sd, cl, pc = (cfg.http_cache, cfg.adaptive_ttl, cfg.switch_damping,
                              cfg.concurrency_limit, cfg.conn_pool)
        return dict(
            max_retries=c.max_retries, cache_ttl=c.cache_ttl,
            enable_local_racing=c.enable_local_racing,
            stagger_start=c.stagger_start, stagger_initial=c.stagger_initial,
            stagger_interval_ms=c.stagger_interval_ms,
            probe_interval_sec=cc.probe_interval_sec, probe_canary=cc.probe_canary,
            probe_canaries=[p.model_dump() for p in cc.probe_canaries],
            circuit_threshold=cc.circuit_threshold, circuit_max_backoff=cc.circuit_max_backoff,
            slow_start_window=cc.slow_start_window, slow_start_success=cc.slow_start_success,
            lb_bias=cc.lb_bias,
            fail_penalty_weight=cc.fail_penalty_weight,
            single_send_degrade_fail=cc.single_send_degrade_fail,
            single_send_degrade_ratio=cc.single_send_degrade_ratio,
            single_send_degrade_slack_ms=cc.single_send_degrade_slack_ms,
            single_send_slow_log_ms=cc.single_send_slow_log_ms,
            connect_tunnel_timeout_sec=cc.connect_tunnel_timeout_sec,
            http_read_timeout_sec=cc.http_read_timeout_sec,
            auth_enabled=auth.enabled, auth_username=auth.username, auth_password=auth.password,
            enable_http_cache=hc.enabled, http_cache_ttl=hc.ttl,
            http_cache_max_entries=hc.max_entries, http_cache_max_bytes=hc.max_bytes,
            http_cache_stream_limit=hc.stream_cache_limit,
            stickiness_enabled=stick.enabled, stickiness_ttl=stick.ttl,
            stickiness_recheck_hits=stick.recheck_hits, stickiness_max_entries=stick.max_entries,
            adaptive_ttl=at.enabled, adaptive_ttl_min=at.min_sec, adaptive_ttl_max=at.max_sec,
            switch_damping=sd.enabled, switch_damping_min_wins=sd.min_wins,
            switch_damping_ratio=sd.ratio, switch_damping_abs_ms=sd.abs_ms,
            concurrency_limit_enabled=cl.enabled, concurrency_limit_initial=cl.initial,
            concurrency_limit_min=cl.min, concurrency_limit_max=cl.max,
            concurrency_add_on_success=cl.add_on_success, concurrency_mult_on_failure=cl.mult_on_failure,
            concurrency_failure_window=cl.failure_window,
            conn_pool_enabled=pc.enabled, conn_pool_per_proxy=pc.per_proxy,
            conn_pool_total=pc.total, conn_pool_idle_timeout=pc.idle_timeout,
            conn_pool_refill_interval=pc.refill_interval, conn_pool_refill_target=pc.refill_target,
            conn_pool_connect_timeout=pc.connect_timeout, conn_pool_target_prewarm=pc.target_prewarm,
            conn_pool_refill_pause_minutes=pc.refill_pause_minutes,
            conn_pool_refill_pause_silence_sec=pc.refill_pause_silence_sec,
            conn_pool_refill_pause_activity_window=pc.refill_pause_activity_window,
            conn_pool_refill_pause_min_requests=pc.refill_pause_min_requests,
            conn_pool_established_reuse=pc.established_reuse,
            cluster_predict=pc.cluster_predict, cluster_window_sec=pc.cluster_window_sec,
            cluster_predict_topk=pc.cluster_predict_topk, cluster_min_support=pc.cluster_min_support,
            cluster_graph_ttl_sec=pc.cluster_graph_ttl_sec,
            cluster_graph_max_entries=pc.cluster_graph_max_entries,
            cluster_predict_throttle_sec=pc.cluster_predict_throttle_sec,
            cluster_proxy_fanout=pc.cluster_proxy_fanout,
            cluster_probe_decay_sec=pc.cluster_probe_decay_sec,
            cluster_pool_idle_timeout=pc.cluster_pool_idle_timeout,
            policies=list(c.policies),
        )

    @staticmethod
    def _make_cfg() -> RouterConfig:
        """一份区分度足够高的 RouterConfig(每条路径都偏离默认值,防"都默认所以相等")。"""
        return RouterConfig(
            max_retries=5, cache_ttl=900, enable_local_racing=True,
            stagger_start=False, stagger_initial=2, stagger_interval_ms=300,
            circuit=dict(
                probe_interval_sec=0.0, probe_canary="canary.example:443",
                circuit_threshold=4, circuit_max_backoff=120.0,
                slow_start_window=30.0, slow_start_success=2, lb_bias=0.5,
                single_send_degrade_fail=2, single_send_degrade_ratio=2.5,
                single_send_degrade_slack_ms=20.0,
                single_send_slow_log_ms=1500.0,
                connect_tunnel_timeout_sec=4.0, http_read_timeout_sec=5.0),
            auth=dict(enabled=True, username="u", password="p"),
            http_cache=dict(enabled=False),
            stickiness=dict(enabled=True, ttl=900.0, recheck_hits=50, max_entries=5000),
            adaptive_ttl=dict(enabled=True, min_sec=30.0, max_sec=1200.0),
            switch_damping=dict(enabled=True, min_wins=3, ratio=0.7, abs_ms=15.0),
            concurrency_limit=dict(enabled=True, initial=8, min=1, max=32,
                                   add_on_success=2, mult_on_failure=0.6, failure_window=10),
            conn_pool=dict(enabled=True, per_proxy=3, total=20, idle_timeout=15.0,
                           refill_interval=2.0, refill_target=1, connect_timeout=5.0,
                           target_prewarm=True, established_reuse=True,
                           refill_pause_minutes=0.0,
                           cluster_predict=True, cluster_window_sec=3.5,
                           cluster_predict_topk=5, cluster_min_support=3,
                           cluster_graph_max_entries=5000, cluster_graph_ttl_sec=7200,
                           cluster_predict_throttle_sec=15.0,
                           cluster_proxy_fanout=3, cluster_probe_decay_sec=1234.0,
                           cluster_pool_idle_timeout=1234.0),
        )

    def _router(self, **kw) -> Router:
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        return Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    def test_router_cfg_equals_kwargs(self):
        """同 RouterConfig:router_cfg= 与全 kwarg 两条构造路径完全等价。"""
        cfg = self._make_cfg()
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host=HOST, port=31991))
        r_cfg = Router(store, listen_host='127.0.0.1', listen_port=10809,
                       db_path=tempfile.mktemp(suffix='.db'), router_cfg=cfg)
        r_kw = Router(store, listen_host='127.0.0.1', listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **self._cfg_kwargs(cfg))
        try:
            assert r_cfg.snapshot_counters() == r_kw.snapshot_counters(), \
                "router_cfg= 与逐 kwarg 构造的 snapshot_counters 必须一致"
            # 关键配置属性逐一比对(r_cfg vs r_kw;Router 自有属性直接比,selector 上的
            # 质控/熔断属性经 r.selector 比)。
            for attr in ('max_retries', 'cache_ttl', 'stagger_interval', 'probe_interval_sec',
                         'conn_pool_enabled', 'conn_pool_total', 'conn_pool_established_reuse',
                         'conn_pool_idle_timeout', 'cluster_proxy_fanout', 'cluster_probe_decay_sec',
                         'cluster_pool_idle_timeout', 'single_send_slow_log_ms',
                         'connect_tunnel_timeout_sec', 'http_read_timeout_sec'):
                assert getattr(r_cfg, attr) == getattr(r_kw, attr), f"{attr} 两条构造路径不一致"
            for attr in ('circuit_threshold', 'circuit_max_backoff', 'slow_start_window',
                         'slow_start_success', 'lb_bias', 'fail_penalty_weight'):
                assert getattr(r_cfg.selector, attr) == getattr(r_kw.selector, attr), \
                    f"selector.{attr} 两条构造路径不一致"
            assert r_cfg.conn_pool_total == 20 and r_cfg.max_retries == 5
            assert r_cfg.conn_pool_established_reuse is True
        finally:
            r_cfg._db.close()
            r_kw._db.close()

    def test_router_cfg_overrides_kwargs(self):
        """router_cfg 优先于同名 kwarg(配置单一真相源:显式的整块配置覆盖散参)。"""
        cfg = self._make_cfg()  # max_retries=5
        r = self._router(max_retries=3, router_cfg=cfg)
        try:
            assert r.max_retries == 5, "router_cfg.max_retries(=5) 应覆盖 kwarg max_retries(=3)"
            assert r.cache_ttl == cfg.cache_ttl
        finally:
            r._db.close()

    def test_pool_forward_identity(self):
        """池成员转发到 self.pools 的同一对象(白名单 __getattr__/__setattr__)。"""
        r = self._router(conn_pool_enabled=True, conn_pool_established_reuse=True)
        try:
            assert r._conn_pool is r.pools._conn_pool
            assert r._target_pool is r.pools._target_pool
            assert r._established_pool is r.pools._established_pool
            assert r.conn_pool_creates == r.pools.conn_pool_creates == 0
            # set 经 __setattr__ 转发到 pools。
            r.conn_pool_creates = 7
            assert r.pools.conn_pool_creates == 7 and r.conn_pool_creates == 7
        finally:
            r._db.close()


class TestRaceStaggeredCleanupOnAllFail:
    """回归:#3 _race_staggered 全失败时,completed 里失败的 5xx resp 候选必须被清理。"""

    @pytest.mark.asyncio
    async def test_all_fail_triggers_cleanup(self):
        """全部候选返回 5xx(无赢家)→ cleanup 下放处理 completed,而非泄漏 resp。

        monkeypatch _spawn_cleanup 捕获调用:全 5xx 走到底 return 时,completed
        里持有 5xx resp 的任务须触发清理(这正是泄漏源——resp 占 httpx 连接池)。
        """
        ps = ProxyStore()
        ps.add(ProxyInfo(id='p1', host='127.0.0.1', port=31341))
        ps.add(ProxyInfo(id='p2', host='127.0.0.1', port=31342))
        r = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                   max_retries=2, enable_http_cache=False, stagger_start=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        rented = []
        cleaned = []

        class FakeResp5xx:
            def __init__(self):
                self.status_code = 500
                self.aclosed = False

            async def aclose(self):
                self.aclosed = True

        async def fivexx(place, method, url, headers, body):
            # 成功返回 HTTP 结果元组 (pid, method, url, resp, client);resp 为 5xx,
            # 被 _is_acceptable_win 拒绝 → 留在 completed、占用连接(candidate 的形状)。
            return place, method, url, FakeResp5xx(), object()

        r._make_race_task = lambda place, method, url, headers, body, domain=None: \
            asyncio.create_task(fivexx(place, method, url, headers, body))

        async def cleanup_cb(result):
            cleaned.append(result)

        def spy_cleanup(losers, cleanup):
            rented.append(len(losers))
            # 放行真正的 _spawn_cleanup(保持与线上一致;闭包捕获原方法)。
            return Router._spawn_cleanup(r, losers, cleanup)

        r._spawn_cleanup = spy_cleanup
        win = await r._race_staggered(['p1', 'p2'], initial=1, interval=0.01,
                                      cleanup=cleanup_cb)
        assert win is None, "全部候选 5xx 应返回 None"
        # 全失败路径下,completed 里的 5xx 候选须触发 cleanup(否则 resp 泄漏)。
        # 正确形态:_spawn_cleanup 调 1 次,completed 一次性涵盖全部 2 个候选。
        assert rented, "all-fail path must spawn cleanup for completed 5xx candidates"
        assert rented[0] == 2, f"both p1,p2 returned 5xx → one cleanup covering 2, got {rented}"
        # 真正等清理 task 落地:cleanup 回调应对每个 5xx 候选执行(2 次)。
        for _ in range(int(0.5 / 0.02)):
            if len(cleaned) >= 2:
                break
            await asyncio.sleep(0.02)
        assert len(cleaned) >= 2, f"cleanup callback must run for both 5xx candidates, got {len(cleaned)}"

    @pytest.mark.asyncio
    async def test_winner_path_also_cleans_losers(self):
        """有赢家路径不受影响:败者照旧下放清理,赢家返回。"""
        ps = ProxyStore()
        ps.add(ProxyInfo(id='ok', host='127.0.0.1', port=31343))
        ps.add(ProxyInfo(id='dead', host='127.0.0.1', port=31344))
        r = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                   max_retries=2, enable_http_cache=False, stagger_start=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        rented = []

        async def ok_and_dead(place, method, url, headers, body):
            if place == 'ok':
                return 'ok', method, url, object(), object()
            raise RuntimeError("dead")

        r._make_race_task = lambda place, method, url, headers, body, domain=None: \
            asyncio.create_task(ok_and_dead(place, method, url, headers, body))

        def spy_cleanup(losers, cleanup):
            rented.append(len(losers))
            return Router._spawn_cleanup(r, losers, cleanup)

        r._spawn_cleanup = spy_cleanup
        win = await r._race_staggered(['ok', 'dead'], initial=2, interval=0.01)
        assert win is not None and win[0] == 'ok', "ok 应赢"
        # 赢家路径:败者(dead)进入 cleanup——这是既有行为,回归确认未破坏。
        assert any(n >= 1 for n in rented), f"winner path should clean dead loser, got {rented}"


class TestForwardSingleAcloseFinally:
    """回归:#4 _forward_single 在 _stream_upstream_response 抛异常时仍 aclose resp。"""

    @pytest.mark.asyncio
    async def test_stream_raise_still_aclose(self):
        """_stream_upstream_response 抛异常(BaseException 语义)→ resp.aclose() 仍被调。"""
        ps = ProxyStore()
        r = Router(ps, listen_host='127.0.0.1', listen_port=10819,
                   enable_http_cache=False, db_path=tempfile.mktemp(suffix='.db'))

        class FakeResp:
            """最小 fake:持有 status_code/headers,aclose 可观测。"""
            def __init__(self):
                self.status_code = 200
                self.reason_phrase = "OK"
                self.headers = type("H", (), {"multi_items": lambda self: [("x", "y")]})()
                self.aclosed = False

            async def aclose(self):
                self.aclosed = True

        resp = FakeResp()

        class FakeWriter:
            async def drain(self):
                pass

        calls = []

        async def boom(writer, resp_, method, url):
            calls.append("boom")
            raise asyncio.CancelledError  # BaseException,不被 except Exception 捕获

        orig_stream = r._stream_upstream_response
        r._stream_upstream_response = boom
        try:
            with pytest.raises(asyncio.CancelledError):
                await r._forward_single(None, "GET", "http://x.test/", {}, b"",
                                        "x.test", instantiated=("pid", resp))
        finally:
            r._stream_upstream_response = orig_stream
        assert calls == ["boom"], "stream 抛异常"
        assert resp.aclosed, "异常路径仍须 aclose(resp),防池化连接泄漏"


# ── #6/#7/#8 健壮性修复的回归测试 ──────────────────────────────

async def _raw_request_until_close(host, port, payload: bytes) -> bytes:
    """裸发请求字节,返回读到的全部响应字节(连接被对端关闭停止)。"""
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(payload)
    await writer.drain()
    out = await asyncio.wait_for(reader.read(), timeout=5.0)
    writer.close()
    await writer.wait_closed()
    return out


@pytest.mark.asyncio
async def test_request_header_line_count_limited():
    """#6: 请求头行数无上限 → 超过 _MAX_REQUEST_HEADER_LINES 即拒绝并关闭连接。

    回归:慢速 loris 式攻击发大量小 header 行曾让 headers bytearray 无界增长。
    现在超过 100 行(默认)直接拒连,客户端看到 EOF 而非挂住。
    """
    router = Router(ProxyStore(), listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        lines = b"".join(b"X-Trick: a\r\n" for _ in range(_MAX_REQUEST_HEADER_LINES + 5))
        req = b"GET http://loris.example.com/ HTTP/1.1\r\n" + lines + b"\r\n"
        out = await _raw_request_until_close(HOST, ROUTER_PORT, req)
        assert out == b"", f"header-overrun request must be closed, got {out!r}"
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_request_header_byte_limit():
    """#6: 请求头累计字节超 _MAX_REQUEST_HEADER_BYTES 也拒绝(行数未超限先触发字节限)。

    >64KB 的请求头被拒连。字节限在行数限之前触发(44 行 × 1.5KB ≈ 66KB)。
    """
    router = Router(ProxyStore(), listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        # 44 行 × ~1502B ≈ 66KB > 64KB,行数 44 < 100 → 字节限先触发。
        big_line = b"X-Big: " + b"a" * 1490 + b"\r\n"
        lines = big_line * 44
        req = b"GET http://loris2.example.com/ HTTP/1.1\r\n" + lines + b"\r\n"
        assert len(req) > _MAX_REQUEST_HEADER_BYTES
        out = await _raw_request_until_close(HOST, ROUTER_PORT, req)
        assert out == b"", f"over-byte-limit request must be closed, got {out!r}"
    finally:
        await router.stop()


@pytest.mark.asyncio
async def test_truncated_upstream_response_detected():
    """#7: 上游 Content-Length 声明 N 但实际只发 M(<N)字节时,记 warn 并关闭上游连接。

    客户端已收到 200 头,无法撤回;依赖 warn 日志暴露截断,否则客户端会挂到自己
    的超时。断言:body 是"少给"的截断内容(而非挂死),且出现 'truncated upstream
    response' 的 warning。
    """
    log = logging.getLogger('auto_squid.router')
    records = []

    class H(logging.Handler):
        def emit(self, record):
            records.append(record)

    handler = H()
    log.addHandler(handler)
    try:
        proxy_srv = await run_mock_proxy_truncated(HOST, PROXY_PORT, declared=1000, sent=5)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
        router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        enable_http_cache=False, db_path=tempfile.mktemp(suffix='.db'))
        await router.start()
        try:
            reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
            writer.write(b"GET http://trunc.example.com/ HTTP/1.1\r\nHost: trunc.example.com\r\n\r\n")
            await writer.drain()
            status = await reader.readline()
            assert b"200" in status, f"expected 200, got {status}"
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            body = await asyncio.wait_for(reader.read(), timeout=2.0)
            writer.close()
            await writer.wait_closed()
            # 截断:远少于承诺的 1000 字节。
            assert len(body) < 1000, f"truncated body should be < 1000, got {len(body)}"
        finally:
            await router.stop()
            proxy_srv.close()
            await proxy_srv.wait_closed()
    finally:
        log.removeHandler(handler)
    msgs = [r.getMessage() for r in records]
    assert any('truncated upstream response' in m for m in msgs), \
        f"must log truncated-response warning, got {msgs}"


@pytest.mark.asyncio
async def test_aggregation_wait_timeout_is_second_scale():
    """#8: 聚合等待超时必须提升到秒级(而非 100ms 形同虚设)。

    0.1s 比典型上游 TTFB 还短,waiter 几乎总是超时回退竞速,并发同 URL 时对上游
    形成 stampede。提升到秒级让聚合在真实 TTFB 窗口内真正生效,仍保留有界等待。
    常量校验回归:防止未来有人把它调回微秒级。
    """
    assert _AGG_WAIT_TIMEOUT >= 3.0, \
        f"_AGG_WAIT_TIMEOUT 应 ≥3s,当前 {_AGG_WAIT_TIMEOUT}s"


# ── #12 extra="forbid" + 跨字段校验 / #13 logging.level ────────────

class TestConfigSchemaStrict:
    """#12: 配置模型一律 extra="forbid",拼错键在启动即硬报错;跨字段一致性校验。"""

    def test_conn_pool_typo_rejected(self):
        with pytest.raises(Exception) as ei:
            ConnPoolConfig(enabled=True, refil_interval=1.0)  # refill 拼错
        assert "refil_interval" in str(ei.value)

    def test_unknown_key_in_example_config_still_loads(self):
        """脱敏样例配置经 extra="forbid" 后仍能原样加载(无过期残留键)。"""
        import yaml
        from pathlib import Path
        cfg = Config(**yaml.safe_load(Path("config_xxh_example.yaml").read_text()))
        assert cfg.router.conn_pool.enabled
        assert cfg.router.conn_pool.established_reuse
        assert cfg.router.stagger_initial == 2

    @pytest.mark.asyncio
    async def test_stagger_initial_over_max_retries_rejected(self):
        with pytest.raises(Exception) as ei:
            RouterConfig(stagger_initial=5, max_retries=3)
        assert "stagger_initial" in str(ei.value)

    @pytest.mark.asyncio
    async def test_stagger_initial_zero_rejected(self):
        with pytest.raises(Exception) as ei:
            RouterConfig(stagger_initial=0, max_retries=3)
        assert "stagger_initial" in str(ei.value)

    def test_adaptive_ttl_min_gt_max_rejected(self):
        with pytest.raises(Exception) as ei:
            RouterConfig(adaptive_ttl={"enabled": True, "min_sec": 900, "max_sec": 60})
        assert "min_sec" in str(ei.value)

    def test_concurrency_limit_out_of_order_rejected(self):
        with pytest.raises(Exception) as ei:
            RouterConfig(concurrency_limit={
                "enabled": True, "min": 4, "initial": 2, "max": 8})
        assert "min <= initial <= max" in str(ei.value)

    def test_conn_pool_stage_requires_enabled(self):
        # target_prewarm 依赖 enabled(第二/三阶段隐性依赖显式化)。
        with pytest.raises(Exception) as ei:
            ConnPoolConfig(enabled=False, target_prewarm=True)
        assert "conn_pool.enabled" in str(ei.value)
        with pytest.raises(Exception) as ei:
            ConnPoolConfig(enabled=False, established_reuse=True)
        assert "conn_pool.enabled" in str(ei.value)

    def test_conn_pool_cluster_requires_enabled(self):
        # cluster_predict 依赖 conn_pool.enabled(第三阶段隐性依赖显式化)。
        with pytest.raises(Exception) as ei:
            ConnPoolConfig(enabled=False, cluster_predict=True)
        assert "conn_pool.cluster_predict" in str(ei.value)

    def test_conn_pool_cluster_requires_target_prewarm(self):
        # cluster_predict 依赖 conn_pool.target_prewarm(预测预建经由目标池发射)。
        with pytest.raises(Exception) as ei:
            ConnPoolConfig(enabled=True, cluster_predict=True, target_prewarm=False)
        assert "conn_pool.cluster_predict" in str(ei.value)

    def test_conn_pool_cluster_requires_both_gates(self):
        """cluster_predict 在 enabled+target_prewarm 齐开时通过(全链依赖满足)。"""
        ConnPoolConfig(enabled=True, target_prewarm=True, cluster_predict=True)

    def test_conn_pool_enabled_allows_stages(self):
        ConnPoolConfig(enabled=True, target_prewarm=True, established_reuse=True)
        ConnPoolConfig(enabled=True, target_prewarm=True, cluster_predict=True)

    def test_logging_bad_level_rejected(self):
        with pytest.raises(Exception) as ei:
            LoggingConfig(level="WRAN")
        assert "logging.level" in str(ei.value)

    def test_proxy_info_stays_lenient(self):
        """数据模型保持宽松:proxies.yaml 的旧键/额外键不阻塞(与配置模型分开)。"""
        p = ProxyInfo(id="x", host="h", port=3128, future_key="whatever")
        assert p.id == "x"


class TestCliConfigErrorExit:
    """#13: 配置加载失败(坏 YAML / 未知键)→ 打印可读错误,不抛裸 traceback。"""

    def _run_load(self, tmp_path, content=None, flag_path=None):
        import subprocess, sys
        import auto_squid.cli  # noqa: F401  确保可导入(不 import 则 cli 未走)
        cfg_path = tmp_path / ("config.yaml" if content is not None else "missing.yaml")
        if content is not None:
            cfg_path.write_text(content)
        target = str(flag_path) if flag_path else str(cfg_path)
        import auto_squid.cli as cli
        return cli._load_config(target)

    def test_bad_yaml_exits_code_2(self, tmp_path):
        import subprocess, sys, textwrap
        p = tmp_path / "bad.yaml"
        p.write_text("router: [unclosed\n")
        r = subprocess.run(
            [sys.executable, "-c",
             "from auto_squid.cli import _load_config; _load_config('%(p)s')" % {"p": p}],
            capture_output=True, text=True)
        assert r.returncode == 2
        assert "config error" in r.stderr

    def test_unknown_key_exits_code_2(self, tmp_path):
        import subprocess, sys
        p = tmp_path / "cfg.yaml"
        p.write_text("router:\n  max_retries: 3\n  bogus_opt: 1\n")
        r = subprocess.run(
            [sys.executable, "-c",
             "from auto_squid.cli import _load_config; _load_config('%(p)s')" % {"p": p}],
            capture_output=True, text=True)
        assert r.returncode == 2
        assert "config error" in r.stderr
        assert "bogus_opt" in r.stderr

    def test_missing_file_exits_code_2(self, tmp_path):
        import subprocess, sys
        p = tmp_path / "nope.yaml"
        r = subprocess.run(
            [sys.executable, "-c",
             "from auto_squid.cli import _load_config; _load_config('%(p)s')" % {"p": p}],
            capture_output=True, text=True)
        assert r.returncode == 2
        assert "config error" in r.stderr

    def test_valid_config_returns_config(self, tmp_path):
        p = tmp_path / "good.yaml"
        p.write_text("logging:\n  level: INFO\n")
        cfg = self._run_load(tmp_path, content="logging:\n  level: INFO\n",
                             flag_path=p)
        assert cfg is not None
        assert cfg.logging.level == "INFO"


class TestSetupLoggingLevel:
    """#13: `logging.level` 配置控制文件 handler 与 auto_squid logger 级别。

    曾是硬编码 INFO:设 DEBUG 没反应。现在 logging.level: DEBUG 应让 per-request
    DEBUG 日志进文件;是 INFO 则 DEBUG 被短路。
    """

    @staticmethod
    def _reset_logging():
        import logging as _l
        root = _l.getLogger()
        for h in list(root.handlers):
            root.removeHandler(h)
        # 清除 cli.py 里显式钉死的子 logger 级别,避免跨测试串扰。
        for name in ("httpx", "httpcore", "uvicorn", "uvicorn.access",
                     "asyncio", "auto_squid"):
            _l.getLogger(name).setLevel(_l.NOTSET)

    def _setup(self, tmp_path, level):
        from auto_squid.cli import setup_logging
        cfg = Config(logging={"level": level, "file": str(tmp_path / "test.log")})
        self._reset_logging()
        setup_logging(cfg)
        return cfg

    def test_debug_level_writes_debug_to_file(self, tmp_path):
        cfg = self._setup(tmp_path, "DEBUG")
        log_path = tmp_path / "test.log"
        rlog = logging.getLogger("auto_squid.router")
        rlog.debug("per-request debug marker zzz")
        rlog.info("info marker")
        assert any("per-request debug marker zzz" in ln for ln in log_path.read_text().splitlines())
        self._reset_logging()

    def test_info_level_short_circuits_debug(self, tmp_path):
        cfg = self._setup(tmp_path, "INFO")
        log_path = tmp_path / "test.log"
        rlog = logging.getLogger("auto_squid.router")
        rlog.debug("debug marker that must NOT appear qqq")
        rlog.info("info marker should appear")
        body = log_path.read_text()
        assert "debug marker that must NOT appear qqq" not in body
        assert "info marker should appear" in body
        self._reset_logging()


# ── local_direct_domains 白名单强制直连 ──────────────────────────

def _local_direct_router(**kw):
    """Router with an empty proxy store + local_direct_domains=[HOST].

    Note: with a mock CONNECT proxy present at PROXY_PORT, a non-whitelisted
    target can race through it, so the non-whitelist HTTP test passes its own
    url with a non-whitelisted *host* while whitelisting HOST.
    """
    ps = ProxyStore()
    kw.setdefault('local_direct_domains', [HOST])
    kw.setdefault('db_path', tempfile.mktemp(suffix='.db'))
    kw.setdefault('enable_http_cache', False)
    return Router(ps, listen_host=HOST, listen_port=ROUTER_PORT, **kw)


@pytest.mark.asyncio
async def test_local_direct_http_hit_uses_local():
    """白名单目标 → 强制本机直连:body 来自本地服务,远端 mock 代理 0 命中。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=[])
    router = _local_direct_router()
    await router.start()
    try:
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'local-response' in body, f"expected local body, got {body!r}"
        assert router.request_counts.get('local', 0) > 0
        counters = router.snapshot_counters()
        assert counters['local_direct_hits'] >= 1
        assert counters['local_direct_failures'] == 0
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_http_non_whitelist_uses_upstream():
    """非白名单目标 → 不强制直连:请求走上游 mock 代理(需 store 有代理,否则 502)。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    local_direct_domains=[HOST], enable_http_cache=False,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        # 白名单是 HOST(127.0.0.1);目标用非白名单域名 example.com → 走上游。
        body = await send_http_get(HOST, ROUTER_PORT, url=b"http://example.com/x")
        assert b'proxied' in body, f"expected upstream body, got {body!r}"
        assert len(hit) >= 1, "upstream should have been hit"
        assert router.request_counts.get('local', 0) == 0
        counters = router.snapshot_counters()
        assert counters['local_direct_hits'] == 0
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_http_failure_502_no_upstream():
    """白名单目标直连失败 → 直接 502,不绕远端 mock 代理(用户决策)。"""
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    # LOCAL_HTTP_PORT 无服务 → 直连失败。
    router = _local_direct_router()
    await router.start()
    try:
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        status = await send_http_get_status(HOST, ROUTER_PORT, url=url)
        assert b'502' in status, f"expected 502, got {status}"
        assert len(hit) == 0, "white-listed failure must NOT fall back to upstream"
        counters = router.snapshot_counters()
        assert counters['local_direct_hits'] >= 1
        assert counters['local_direct_failures'] >= 1
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_http_circuit_short_no_local_call():
    """local 熔断短路:白名单目标在 local 熔断期直接 502,不打 local 服务端(节省
    10s 超时)。consec_fail 不被累加(否则永熔断),等熔断自然冷却后下次恢复。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    router = _local_direct_router()
    await router.start()
    try:
        # 手动让 local 进入熔断退避:直接写 circuit dict,控制 backoff 时长。
        import time as _t
        router.selector._circuit['local'] = {
            'consec_fail': 0,
            'open_until': _t.monotonic() + 10.0,  # 10s 退避
            'backoff': 10.0,
        }
        assert router.selector.is_circuit_open('local')
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        status = await send_http_get_status(HOST, ROUTER_PORT, url=url)
        assert b'502' in status, f"expected 502, got {status}"
        counters = router.snapshot_counters()
        assert counters['local_direct_circuit_short'] >= 1, \
            f"expected local_direct_circuit_short>=1, got {counters}"
        assert counters['local_direct_failures'] >= 1
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_connect_circuit_short_502():
    """CONNECT 白名单目标:local 熔断期直接 502,不打 local 端口。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    router = _local_direct_router()
    await router.start()
    try:
        import time as _t
        router.selector._circuit['local'] = {
            'consec_fail': 0,
            'open_until': _t.monotonic() + 10.0,
            'backoff': 10.0,
        }
        assert router.selector.is_circuit_open('local')
        # 手写 CONNECT 响应检查,不用 send_connect(它内部断言 200)。
        r, w = await asyncio.open_connection(HOST, ROUTER_PORT)
        w.write(b"CONNECT " + f"{HOST}:{LOCAL_HTTP_PORT}".encode() + b" HTTP/1.1\r\n\r\n")
        await w.drain()
        status = await r.readline()
        w.close()
        await w.wait_closed()
        assert b'502' in status, f"expected 502 CONNECT, got {status}"
        counters = router.snapshot_counters()
        assert counters['local_direct_circuit_short'] >= 1
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_http_circuit_recovers_after_backoff():
    """熔断退避到期后,白名单请求恢复直连:local_direct_circuit_short 不再涨。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    router = _local_direct_router()
    await router.start()
    try:
        import time as _t
        router.selector._circuit['local'] = {
            'consec_fail': 0,
            'open_until': _t.monotonic() + 0.05,  # 50ms 后到期
            'backoff': 0.05,
        }
        # 退避到期后再请求。
        _t.sleep(0.1)
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'local-response' in body, "backoff expired → 直连应恢复"
        counters = router.snapshot_counters()
        assert counters['local_direct_circuit_short'] == 0, \
            f"backoff 已冷却,不应短路,got {counters}"
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_http_cache_still_works():
    """白名单请求仍走 HTTP 响应缓存:第二次命中缓存,local 只打一次。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    router = _local_direct_router(enable_http_cache=True)
    await router.start()
    try:
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        body1 = await send_http_get(HOST, ROUTER_PORT, url=url)
        body2 = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'local-response' in body1 and b'local-response' in body2
        counters = router.snapshot_counters()
        assert counters['http_cache_hits'] >= 1, "second request should hit cache"
        assert router.request_counts.get('local', 0) == 1, "local origin hit once only"
        # 缓存命中分支在白名单拦截之前 return → 只算一次 local_direct_hits(直连那次)。
        assert counters['local_direct_hits'] == 1
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


async def send_connect_raw(host, port, target, payload=b"hello", expect=b"200"):
    """Send CONNECT, drain headers, write payload, read echo. Returns (status, echo)."""
    reader, writer = await asyncio.open_connection(host, port)
    writer.write(b"CONNECT " + target + b" HTTP/1.1\r\nHost: " + target + b"\r\n\r\n")
    await writer.drain()
    status = await reader.readline()
    if expect is not None and expect not in status:
        writer.close()
        await writer.wait_closed()
        return status, b""
    while True:
        h = await reader.readline()
        if not h or h in (b"\r\n", b"\n"):
            break
    writer.write(payload)
    await writer.drain()
    echo = await asyncio.wait_for(reader.read(len(payload)), timeout=5)
    writer.close()
    await writer.wait_closed()
    return status, echo


@pytest.mark.asyncio
async def test_local_direct_connect_echo():
    """白名单目标 CONNECT → 强制本机直连,payload 经 mock proxy 原样往返。

    _try_tunnel 直连分支会向目标发 CONNECT 请求并期待 200,因此本地目标用
    run_mock_proxy(它响应 CONNECT→200 后 echo 数据),而非裸 TCP echo。
    """
    echo_srv = await run_mock_proxy(HOST, LOCAL_TCP_ECHO_PORT)
    router = _local_direct_router()
    await router.start()
    try:
        target = f"{HOST}:{LOCAL_TCP_ECHO_PORT}".encode()
        status, echo = await send_connect_raw(HOST, ROUTER_PORT, target, payload=b"ping-123")
        assert b'200' in status, f"expected 200, got {status}"
        assert echo == b"ping-123", f"expected echo, got {echo!r}"
        counters = router.snapshot_counters()
        assert counters['local_direct_hits'] >= 1
        assert counters['local_direct_failures'] == 0
    finally:
        await router.stop()
        echo_srv.close()
        await echo_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_connect_failure_502():
    """白名单目标 CONNECT 直连失败 → 502,不绕远端 mock 代理。"""
    hit = []
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit)
    router = _local_direct_router()
    await router.start()
    try:
        # LOCAL_TCP_ECHO_PORT 无服务 → 直连失败。
        target = f"{HOST}:{LOCAL_TCP_ECHO_PORT}".encode()
        status, _ = await send_connect_raw(HOST, ROUTER_PORT, target, expect=None)
        assert b'502' in status, f"expected 502, got {status}"
        assert len(hit) == 0, "white-listed CONNECT failure must NOT fall back to upstream"
        counters = router.snapshot_counters()
        assert counters['local_direct_hits'] >= 1
        assert counters['local_direct_failures'] >= 1
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


def test_local_direct_norm_sensitivity():
    """host 归一:尾点/大小写/IPv6 括号不影响命中。"""
    r = _local_direct_router(local_direct_domains=['LOCAL.', '[::1]'])
    assert r._host_in_local_direct('local') is True
    assert r._host_in_local_direct('LOCAL') is True
    assert r._host_in_local_direct('local.') is True
    assert r._host_in_local_direct('::1') is True
    assert r._host_in_local_direct('[::1]') is True
    assert r._host_in_local_direct('other.example') is False
    assert r._host_in_local_direct('') is False
    assert r._host_in_local_direct(None) is False


@pytest.mark.asyncio
async def test_local_direct_independent_of_enable_local_racing():
    """白名单强制直连不依赖 enable_local_racing(关闭时仍命中)。"""
    local_srv = await run_local_http_server(HOST, LOCAL_HTTP_PORT)
    router = _local_direct_router(enable_local_racing=False)
    await router.start()
    try:
        url = f"http://{HOST}:{LOCAL_HTTP_PORT}/".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'local-response' in body, f"expected local body, got {body!r}"
        counters = router.snapshot_counters()
        assert counters['local_direct_hits'] >= 1
    finally:
        await router.stop()
        local_srv.close()
        await local_srv.wait_closed()


@pytest.mark.asyncio
async def test_local_direct_timeout_relaxed():
    """白名单直连用 local_direct_timeout_sec(10s)而非全局 3s:本地服务延迟
    4s 仍成功,证明不被全局 http_read_timeout_sec 掐断。"""
    delay = 4.0

    async def slow_handle(reader, writer):
        try:
            while True:
                h = await reader.readline()
                if not h or h in (b"\r\n", b"\n"):
                    break
            await asyncio.sleep(delay)
            body = b"slow-local-response"
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

    slow_srv = await asyncio.start_server(slow_handle, host=HOST, port=LOCAL_TCP_ECHO_PORT)
    router = _local_direct_router()  # local_direct_timeout_sec 默认 10s
    await router.start()
    try:
        url = f"http://{HOST}:{LOCAL_TCP_ECHO_PORT}/".encode()
        body = await send_http_get(HOST, ROUTER_PORT, url=url)
        assert b'slow-local-response' in body, f"expected slow local body, got {body!r}"
        assert router.request_counts.get('local', 0) > 0
    finally:
        await router.stop()
        slow_srv.close()
        await slow_srv.wait_closed()


class TestDispatchSingleUnified:
    """P3#8 统一 _dispatch_single:HTTP 与 CONNECT 竞速胜者都写域名缓存 meta 与会话粘性。

    锁定统一体 race winner 分支对两条 proto 都调 _record_win_meta + _record_sticky
    (败者只记尝试统计)。用 spy 记录调用,不依赖竞速时序确定性。
    """

    @pytest.mark.asyncio
    async def test_http_race_winner_writes_win_meta(self):
        """HTTP 竞速胜者:统一 _dispatch_single 的 race 分支写 win_meta + sticky。"""
        fast_srv = await run_mock_proxy_tagged(HOST, 31391, 'FAST')
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host=HOST, port=31391))
        r = Router(store, listen_host=HOST, listen_port=10809,
                   max_retries=2, enable_http_cache=False,
                   db_path=tempfile.mktemp(suffix='.db'))
        win_meta_calls, sticky_calls = [], []
        orig_meta, orig_sticky = r._record_win_meta, r.sticky._record_sticky
        r._record_win_meta = lambda d, p: (win_meta_calls.append((d, p)), orig_meta(d, p))[-1]
        r.sticky._record_sticky = lambda ip, d, p: (sticky_calls.append((d, p)), orig_sticky(ip, d, p))[-1]
        await r.start()
        try:
            body = await send_http_get(HOST, 10809, url=b"http://dispatch-unify.test/")
            assert body == b"FAST"
            assert win_meta_calls and win_meta_calls[-1][1] == 'fast'
            assert sticky_calls and sticky_calls[-1][1] == 'fast'
        finally:
            await r.stop()
            fast_srv.close()
            await fast_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_connect_race_winner_writes_win_meta(self):
        """CONNECT 竞速胜者:统一 _dispatch_single 的 race 分支写 win_meta + sticky。"""
        fast_srv = await run_mock_proxy(HOST, 31392)
        store = ProxyStore()
        store.add(ProxyInfo(id='fast', host=HOST, port=31392))
        r = Router(store, listen_host=HOST, listen_port=10810,
                   max_retries=2, enable_http_cache=False,
                   db_path=tempfile.mktemp(suffix='.db'))
        win_meta_calls, sticky_calls = [], []
        orig_meta, orig_sticky = r._record_win_meta, r.sticky._record_sticky
        r._record_win_meta = lambda d, p: (win_meta_calls.append((d, p)), orig_meta(d, p))[-1]
        r.sticky._record_sticky = lambda ip, d, p: (sticky_calls.append((d, p)), orig_sticky(ip, d, p))[-1]
        await r.start()
        try:
            echo = await send_connect(HOST, 10810, target=b"dispatch-unify.connect:443", payload=b"ping")
            assert echo == b"ping"
            assert win_meta_calls and win_meta_calls[-1][0] == "dispatch-unify.connect:443"
            assert sticky_calls and sticky_calls[-1][0] == "dispatch-unify.connect:443"
        finally:
            await r.stop()
            fast_srv.close()
            await fast_srv.wait_closed()

    @pytest.mark.asyncio
    async def test_connect_sticky_failure_marks_immediate_degrade(self):
        """CONNECT 粘性单发失败 → 即时降级门控(不再被域名缓存单发重复钉死)。

        锁定 _dispatch_single 粘性 catch 调 _degrade_send_proxy:被钉代理失败后立即可
        域名缓存再次单发(事故根因),而是被标记,下一请求回落到竞速。
        """
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host=HOST, port=31395))
        r = Router(store, listen_host=HOST, listen_port=10811,
                   max_retries=2, enable_http_cache=False, stickiness_enabled=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        target = "immediate-degrade.connect:443"
        # 预置粘性 + 域名缓存都钉 slow,使单发命中。
        r.sticky._sticky_cache["_test_ip|" + target] = {"proxy_id": 'slow', "updated_at": r._now_utc()}
        r._meta_cache[target] = {"default_proxy": 'slow', "updated_at": "2026-01-01T00:00:00+00:00", "_updated_mono": time.monotonic(), "ref_ewma": None}
        r.selector.record_failure('slow')  # consec_fail=1,不足以触发统计降级(阈值2)
        # mock 单发抛超时(RuntimeError,与 _try_tunnel 兔子转译一致)。
        r._connect_single_send = None  # type: ignore[method-assign]

        async def fail_send(*, pid, target, domain, client_ip=""):
            raise RuntimeError(f'connect to slow timed out: {pid}')

        r._connect_single_send = fail_send

        async def fake_race(*args, **kwargs):
            return None

        srv = await run_mock_proxy(HOST, 31395)
        r._race = fake_race
        await r.start()
        try:
            # 首次触发:粘性单发失败 → 驱逐 + 即时降级标记。
            try:
                await r._dispatch_single(None, '', '', None, None, target, proto='tunnel',
                                         target=target, client_reader=object(),
                                         client_writer=object(), client_ip="_test_ip")
            except Exception:
                pass
            assert 'slow' in r._immediate_degraded
            assert 'slow' in r._degraded_single_send
            assert r.single_send_degrades >= 1
            # 下一请求:域名缓存也应跳过 slow(门控),不再次单发。
            assert r._get_fresh_proxy(target) is None
            # 竞速赢家接管后清除标记(既有机制,纯即时集与展示集都清)。
            r._record_win_meta(target, 'slow')
            assert 'slow' not in r._immediate_degraded
            assert 'slow' not in r._degraded_single_send
        finally:
            await r.stop()
            srv.close()
            await srv.wait_closed()

    @pytest.mark.asyncio
    async def test_domain_cache_skips_immediately_degraded(self):
        """_get_fresh_proxy 对「即时降级」标记的代理直接 miss,回落到竞速。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host=HOST, port=31396))
        r = Router(store, listen_host=HOST, listen_port=10812,
                   max_retries=2, enable_http_cache=False,
                   db_path=tempfile.mktemp(suffix='.db'))
        r._immediate_degraded.add('slow')
        r._meta_cache["x.test"] = {"default_proxy": 'slow', "updated_at": "2026-01-01T00:00:00+00:00", "_updated_mono": time.monotonic(), "ref_ewma": None}
        assert r._get_fresh_proxy("x.test") is None

    @pytest.mark.asyncio
    async def test_http_sticky_failure_marks_immediate_degrade(self):
        """HTTP 粘性单发失败同样即时降级(与 CONNECT 对称,覆盖 _forward_single 异常)。"""
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host=HOST, port=31397))
        r = Router(store, listen_host=HOST, listen_port=10813,
                   max_retries=2, enable_http_cache=False, stickiness_enabled=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        domain = "x.test"
        r.sticky._sticky_cache["_test_ip|" + domain] = {"proxy_id": 'slow', "updated_at": r._now_utc()}
        r._meta_cache[domain] = {"default_proxy": 'slow', "updated_at": "2026-01-01T00:00:00+00:00", "_updated_mono": time.monotonic(), "ref_ewma": None}
        r.selector.record_failure('slow')

        async def fail_single(*args, **kwargs):
            raise RuntimeError('read timed out')

        async def fake_race(*args, **kwargs):
            return None

        r._forward_single = fail_single
        r._race = fake_race
        await r.start()
        try:
            try:
                await r._dispatch_single(None, 'GET', f'http://{domain}/', {}, None, domain,
                                         proto='http', client_ip="_test_ip")
            except Exception:
                pass
            assert 'slow' in r._immediate_degraded
            assert 'slow' in r._degraded_single_send
            assert r.single_send_degrades >= 1
            assert r._get_fresh_proxy(domain) is None
        finally:
            await r.stop()

    def test_attempt_failure_logs_on_single_send_timeout(self, caplog):
        """per-attempt 失败日志:CONNECT 单发超时 → 记 upstream attempt FAILED,带 pid/err 类型。"""
        import logging
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host=HOST, port=31398))
        r = Router(store, listen_host=HOST, listen_port=10814,
                   max_retries=2, enable_http_cache=False,
                   db_path=tempfile.mktemp(suffix='.db'))
        with caplog.at_level(logging.INFO, logger='auto_squid.router'):
            r._log_attempt_failure('59.67.225.91', 'github.com:443', 'github.com:443', 'slow',
                                   RuntimeError("connect to xxx timed out"), time.perf_counter() - 0.3)
        hit = [rec for rec in caplog.records
               if rec.getMessage().startswith("upstream attempt FAILED")]
        assert hit
        assert "github.com:443" in hit[0].getMessage()
        assert "slow" in hit[0].getMessage()
        assert "RuntimeError" in hit[0].getMessage()
        assert "59.67.225.91" in hit[0].getMessage()

    @pytest.mark.asyncio
    async def test_attempt_failure_logs_from_try_tunnel_except(self, caplog):
        """per-attempt 失败日志来自 _try_tunnel 的 except:真失败(如非法 target)记日志带类型。

        用"本机直连非法/invalid target"真实走 _try_tunnel 的 except——
        直接确认 except 内 _log_attempt_failure 被调用,而非 mock 掉 except。
        """
        import logging
        store = ProxyStore()
        store.add(ProxyInfo(id='slow', host=HOST, port=31399))
        r = Router(store, listen_host=HOST, listen_port=10815,
                   max_retries=2, enable_http_cache=False,
                   db_path=tempfile.mktemp(suffix='.db'))
        # 本机直连路径(proxy_host=None)对空 target 抛 ValueError,真实走 except。
        # 空 target → _try_tunnel_host 返回 '' → `if not host: raise ValueError`。
        with caplog.at_level(logging.INFO, logger='auto_squid.router'):
            try:
                await r._try_tunnel('local', '', None, None, None)
            except ValueError:
                pass
        hit = [rec for rec in caplog.records
               if rec.getMessage().startswith("upstream attempt FAILED")]
        assert hit
        assert "ValueError" in hit[-1].getMessage()
        assert "local" in hit[-1].getMessage()

    def test_local_real_failure_feeds_circuit(self):
        """竞速 local 失败喂熔断:真失败(非取消)累计 consec_fail,达阈值熔断 'local'。

        修复前 record_failure 对 pid=='local' 跳过,死本机端点(colo 防火墙挡住
        的 127.0.0.1)不锁熔断,每请求竞速白烧 3s。此刻真失败也累计,达阈值
        is_circuit_open('local') 为真。
        """
        store = ProxyStore()
        r = Router(store, listen_host=HOST, listen_port=10816,
                   max_retries=2, enable_http_cache=False,
                   enable_local_racing=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        try:
            assert r.enable_local_racing is True
            # 真失败累计(模拟建连被取消 -> 单发失败)
            for _ in range(r.selector.circuit_threshold):
                r.selector.record_failure('local')
            assert r.selector.is_circuit_open('local') is True
        finally:
            r.stop()

    def test_local_respected_when_circuit_open_in_race_builders(self):
        """熔断中的 local 不进入竞速候选(_prep_http/_build_racing_tasks_http)。

        local 仅在 enable_local_racing + 策略放行 + 未熔断时加入竞速;熔断后
        local 候选消失,不再每次白烧 3s。
        """
        store = ProxyStore()
        r = Router(store, listen_host=HOST, listen_port=10817,
                   max_retries=2, enable_http_cache=False,
                   enable_local_racing=True,
                   db_path=tempfile.mktemp(suffix='.db'))
        try:
            for _ in range(r.selector.circuit_threshold):
                r.selector.record_failure('local')
            assert r.selector.is_circuit_open('local') is True
            # HTTP 竞速构建:local 不应出现在任何占位
            hp_initial, hp_remaining = r._prep_http(['p1', 'p2'], 'example.com')
            assert 'local' not in hp_initial
            assert 'local' not in hp_remaining
            assert 'local' not in r._build_racing_tasks_http(['p1', 'p2'], 'example.com')
            # CONNECT 竞速构建
            cp_initial, cp_remaining = r._prep_connect(['p1', 'p2'], 'example.com:443')
            assert ('local', 'example.com:443') not in cp_initial
            assert ('local', 'example.com:443') not in cp_remaining
            assert ('local', 'example.com:443') not in r._build_racing_tasks_connect(['p1', 'p2'], 'example.com:443')
            # 熔断解除后 local 重新可参与
            r.selector.reset_circuits()
            assert r.selector.is_circuit_open('local') is False
            assert 'local' in r._build_racing_tasks_http(['p1', 'p2'], 'example.com')
        finally:
            r.stop()

    def test_local_real_failure_through_try_tunnel_except(self):
        """真失败经 _try_tunnel 的 except 喂熔断:record_failure('local') 被调用。

        空 target 抛 ValueError(真失败,非取消)——except 内应调用
        record_failure('local'),使 consec_fail 增长;连续 3 次后熔断。
        """
        store = ProxyStore()
        r = Router(store, listen_host=HOST, listen_port=10818,
                   max_retries=2, enable_http_cache=False,
                   enable_local_racing=True,
                   db_path=tempfile.mktemp(suffix='.db'))

        async def drive():
            for _ in range(3):
                try:
                    await r._try_tunnel('local', '', None, None, None)
                except ValueError:
                    pass

        try:
            asyncio.run(drive())
            assert r.selector.is_circuit_open('local') is True, \
                "3 次真失败应熔断 local"
        finally:
            r.stop()


class TestHttpCachePrivacy:
    """审计 P2#2 回归:共享缓存对"携带 Cookie/Authorization 的请求"在读、写
    两侧都按私密头集收敛,防止 A 客户端的个性化响应串给 B 客户端。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        return Router(store, listen_host=HOST, listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    def test_get_with_cookie_header_is_miss(self):
        """带 Cookie 的请求不命中共享缓存(键只有 method:url,命中即串读)。"""
        r = self._router()
        r._http_cache_set('GET', 'http://personal.example.com/', 200, 'OK', {}, b'for-bob')
        assert r._http_cache_get('GET', 'http://personal.example.com/', headers={'cookie': 'sid=bob'}) is None
        # 无凭据的请求仍可命中(共享缓存本义保留)。
        assert r._http_cache_get('GET', 'http://personal.example.com/') is not None

    def test_get_with_authorization_header_is_miss(self):
        r = self._router()
        r._http_cache_set('GET', 'http://personal.example.com/', 200, 'OK', {}, b'for-bob')
        assert r._http_cache_get('GET', 'http://personal.example.com/',
                                 headers={'Authorization': 'Bearer x', 'X-Other': '1'}) is None

    def test_get_empty_headers_still_hits(self):
        """headers={} 视为无私密头 → 正常命中(与缺省 None 等价)。"""
        r = self._router()
        r._http_cache_set('GET', 'http://personal.example.com/', 200, 'OK', {}, b'for-bob')
        assert r._http_cache_get('GET', 'http://personal.example.com/', headers={}) is not None

    def test_set_with_cookie_request_does_not_pollute(self):
        """带 Cookie 的请求拿回可缓存响应也不许写共享缓存,否则下次无凭据
        客户端会命中这条"个人化"响应。"""
        r = self._router()
        r._http_cache_set('GET', 'http://personal.example.com/', 200, 'OK', {}, b'for-bob',
                          request_headers={'cookie': 'sid=bob'})
        assert r._http_cache_get('GET', 'http://personal.example.com/') is None
        # 不带 request_headers 的写入不受影响。
        r._http_cache_set('GET', 'http://personal.example.com/', 200, 'OK', {}, b'public')
        assert r._http_cache_get('GET', 'http://personal.example.com/') is not None


class TestStickyProbePrune:
    """审计 P2#3 回归:_prune_sticky 顺带收紧 _sticky_probe_last 节流表,
    且探路表清扫不依赖粘性表非空(空表早退也会漏清这个独立表)。"""

    def _router(self, **kw):
        store = ProxyStore()
        store.add(ProxyInfo(id='p', host='h', port=3128))
        return Router(store, listen_host=HOST, listen_port=10809,
                      db_path=tempfile.mktemp(suffix='.db'), **kw)

    def test_probe_pruning_removes_stale_entries_with_empty_sticky_cache(self):
        r = self._router(sticky_probe_interval_sec=1.0, stickiness_ttl=10)
        assert not r._sticky_cache  # 粘性表为空:探路表仍应被清扫
        r._sticky_probe_last['1.2.3.4|a.com'] = time.monotonic() - 100.0
        r._sticky_probe_last['1.2.3.4|b.com'] = time.monotonic() - 50.0
        r._sticky_probe_last['5.6.7.8|c.com'] = time.monotonic()  # 刚探过,保留
        r.sticky._prune_sticky()
        assert '1.2.3.4|a.com' not in r._sticky_probe_last
        assert '1.2.3.4|b.com' not in r._sticky_probe_last
        assert '5.6.7.8|c.com' in r._sticky_probe_last

    def test_probe_pruning_keeps_recent_entries(self):
        r = self._router(sticky_probe_interval_sec=1.0, stickiness_ttl=10)
        r._sticky_probe_last['1.2.3.4|a.com'] = time.monotonic()
        r.sticky._prune_sticky()
        assert '1.2.3.4|a.com' in r._sticky_probe_last

    def test_probe_pruning_skipped_when_feature_off(self):
        r = self._router(sticky_probe_interval_sec=0.0, stickiness_ttl=10)
        r._sticky_probe_last['1.2.3.4|a.com'] = time.monotonic() - 100.0
        r.sticky._prune_sticky()
        # 特性关闭:表为空,不清也无需清——这里验证不因这路径抛错。
        assert '1.2.3.4|a.com' in r._sticky_probe_last


class TestDupHeaderParse:
    """审计 P2#5 回归:同名重复请求头在 handle_client 解析期合并(不覆盖),
    保证"首个常为关键的 Cookie"不再丢失。

    解析内联在 handle_client,不单独提炼;wire 级覆盖见
    test_duplicate_request_headers_forwarded_to_upstream。
    """


@pytest.mark.asyncio
async def test_duplicate_request_headers_forwarded_to_upstream():
    """E2E(审计 P2#5):客户端发两个同名 Cookie / X-Dup,上游必须同时收到两个
    值(旧实现 dict 后者覆盖前者,Cookie 首个值丢失)。"""
    proxy_srv = await run_header_echo_proxy(HOST, PROXY_PORT)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        req = (b"GET http://dup.test.example.com/ HTTP/1.1\r\n"
               b"Host: dup.test.example.com\r\n"
               b"Cookie: a=1\r\n"
               b"Cookie: b=2\r\n"
               b"X-Dup: one\r\n"
               b"X-Dup: two\r\n"
               b"\r\n")
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
        assert b'cookie: a=1; b=2' in echoed.lower(), f"Cookie 值丢失: {echoed!r}"
        assert b'x-dup: one, two' in echoed.lower(), f"重复头值丢失: {echoed!r}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_auth_accepts_lowercase_proxy_authorization():
    """E2E(审计 P2#4):发送小写 `proxy-authorization:` 也应通过认证(fail-open
    修复前会被误拒为 407)。"""
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
        req = (f"GET http://lower.test.example.com/ HTTP/1.1\r\n"
               f"Host: lower.test.example.com\r\n"
               f"proxy-authorization: Basic {tok}\r\n"
               f"\r\n").encode()
        writer.write(req)
        await writer.drain()
        status = await reader.readline()
        assert b'200' in status, f"expected 200, got {status} (lowercase header must be accepted)"
        writer.close()
        await writer.wait_closed()
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


async def run_mock_proxy_bogus_connect(host, port):
    """上游 mock:CONNECT 回 `HTTP/1.1 2000 <reason>`(状态码==2000,但子串
    含 "200"),HTTP 请求回 200 空 body。用于验证 CONNECT 状态码按"精确解析"
    而非子串匹配(审计 P3#7)。"""
    async def handle(reader, writer):
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                if line.upper().startswith(b'CONNECT '):
                    writer.write(b"HTTP/1.1 2000 WeirdStatus\r\n\r\n")
                    await writer.flush()
                    # CONNECT 建立后 echo 数据,让误判成功的路径有数据可测。
                    while True:
                        data = await reader.read(4096)
                        if not data:
                            break
                        writer.write(b'echo:' + data)
                        await writer.flush()
                    break
                # 普通 HTTP 请求:P3#7 只测 CONNECT,HTTP 直接回 200 空 body。
                while True:
                    h = await reader.readline()
                    if not h or h in (b"\r\n", b"\n"):
                        break
                writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                await writer.flush()
                # 单请求即断开,避免 keep-alive 复用干扰下一连接。
                break
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
    server = await asyncio.start_server(handle, host=host, port=port)
    return server


@pytest.mark.asyncio
async def test_connect_status_code_requires_exact_200():
    """E2E(审计 P3#7):上游 CONNECT 回 `HTTP/1.1 2000`,状态码不是 200,路由
    必须判失败(旧实现 `'200' in status_text` 会当成功并打通隧道)。"""
    up_srv = await run_mock_proxy_bogus_connect(HOST, PROXY_PORT)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    max_retries=1, db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        r, w = await asyncio.open_connection(HOST, ROUTER_PORT)
        w.write(f"CONNECT {HOST}:{PROXY_PORT} HTTP/1.1\r\n\r\n".encode())
        await w.drain()
        status = await r.readline()
        w.close()
        await w.wait_closed()
        assert b'502' in status, f"expected 502 for bogus 2000 CONNECT, got {status}"
    finally:
        await router.stop()
        up_srv.close()
        await up_srv.wait_closed()


@pytest.mark.asyncio
async def test_invalid_content_length_returns_400():
    """E2E(审计 P3#8):`Content-Length: abc`(非数值)应回明确 400,而不是
    int() 抛 ValueError 落到外层 except 静默断连。"""
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        writer.write(b"GET http://cl.test.example.com/ HTTP/1.1\r\n"
                     b"Host: cl.test.example.com\r\n"
                     b"Content-Length: abc\r\n"
                     b"\r\n")
        await writer.drain()
        status = await reader.readline()
        writer.close()
        await writer.wait_closed()
        assert b'400' in status, f"expected 400 for non-numeric content-length, got {status}"
    finally:
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()


@pytest.mark.asyncio
async def test_slow_client_header_timeout_closes_connection():
    """E2E(审计 P1#1):客户端发送半截请求头后停顿,超过 _CLIENT_HEADER_TIMEOUT
    即被断开——慢速客户端不能无限期挂住连接与 task(slow-loris)。"""
    import auto_squid.router as router_mod
    proxy_srv = await run_mock_proxy(HOST, PROXY_PORT)
    ps = ProxyStore()
    ps.add(ProxyInfo(id='mock1', host=HOST, port=PROXY_PORT))
    router = Router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                    db_path=tempfile.mktemp(suffix='.db'))
    orig = router_mod._CLIENT_HEADER_TIMEOUT
    router_mod._CLIENT_HEADER_TIMEOUT = 0.3  # 缩短等待窗口,测试可快速通过
    await router.start()
    try:
        reader, writer = await asyncio.open_connection(HOST, ROUTER_PORT)
        writer.write(b"GET http://slow.test.example.com/ HTTP/1.1\r\nHost: slow.test.example.com\r\nX-Half:")
        await writer.drain()
        # 只发半个头并停住:连接应在 ~0.3s 后被服务端关闭(read 返回 EOF)。
        data = await asyncio.wait_for(reader.read(1), timeout=5.0)
        assert data == b'', f"expected connection close on header timeout, got {data!r}"
        writer.close()
        await writer.wait_closed()
    finally:
        router_mod._CLIENT_HEADER_TIMEOUT = orig
        await router.stop()
        proxy_srv.close()
        await proxy_srv.wait_closed()
