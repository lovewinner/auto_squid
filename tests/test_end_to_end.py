import asyncio
import tempfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.config_schema import ProxyInfo, PolicyRule, RuleType, RuleTarget
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


# ── policy engine integration tests ────────────────────────────────

@pytest.mark.asyncio
async def test_policy_force_http():
    """FORCE rule should route the request through the forced proxy."""
    hit1, hit2 = [], []
    proxy_srv1 = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit1)
    PROXY_PORT2 = PROXY_PORT + 1
    proxy_srv2 = await run_mock_proxy(HOST, PROXY_PORT2, hit_counter=hit2)

    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='proxy1', host=HOST, port=PROXY_PORT))
    proxy_store.add(ProxyInfo(id='proxy2', host=HOST, port=PROXY_PORT2))

    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    # Add a FORCE rule that forces all *.forceme.com traffic to proxy2
    router.policy_engine.add_rule(PolicyRule(
        rule_type=RuleType.force,
        domain_pattern='*.forceme.com',
        target_type=RuleTarget.proxy_id,
        target_proxy='proxy2',
    ))
    await router.start()
    try:
        body = await send_http_get(HOST, ROUTER_PORT, url=b"http://test.forceme.com/")
        assert b'proxied' in body
        # proxy2 should have been hit (forced), proxy1 should NOT
        assert len(hit2) == 1, f"forced proxy2 should have been hit, got {hit2}"
        assert len(hit1) == 0, f"proxy1 should not have been hit, got {hit1}"
    finally:
        await router.stop()
        proxy_srv1.close()
        proxy_srv2.close()
        await asyncio.gather(proxy_srv1.wait_closed(), proxy_srv2.wait_closed())


@pytest.mark.asyncio
async def test_policy_deny_http():
    """DENY rule should prevent the denied proxy from receiving the request."""
    hit1, hit2 = [], []
    proxy_srv1 = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit1)
    PROXY_PORT2 = PROXY_PORT + 1
    proxy_srv2 = await run_mock_proxy(HOST, PROXY_PORT2, hit_counter=hit2)

    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='p1', host=HOST, port=PROXY_PORT))
    proxy_store.add(ProxyInfo(id='p2', host=HOST, port=PROXY_PORT2))

    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    # Deny p1 for *.secret.com
    router.policy_engine.add_rule(PolicyRule(
        rule_type=RuleType.deny,
        domain_pattern='*.secret.com',
        target_type=RuleTarget.proxy_id,
        target_proxy='p1',
    ))
    await router.start()
    try:
        body = await send_http_get(HOST, ROUTER_PORT, url=b"http://x.secret.com/")
        assert b'proxied' in body
        assert len(hit1) == 0, f"p1 should have been denied, got {hit1}"
        assert len(hit2) == 1, f"p2 should have been hit, got {hit2}"
    finally:
        await router.stop()
        proxy_srv1.close()
        proxy_srv2.close()
        await asyncio.gather(proxy_srv1.wait_closed(), proxy_srv2.wait_closed())


@pytest.mark.asyncio
async def test_policy_prefer_http():
    """PREFER rule should put the preferred proxy first so it wins the race."""
    hit1, hit2, hit3 = [], [], []
    proxy_srv1 = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit1)
    PROXY_PORT2 = PROXY_PORT + 1
    PROXY_PORT3 = PROXY_PORT + 2
    proxy_srv2 = await run_mock_proxy(HOST, PROXY_PORT2, hit_counter=hit2)
    proxy_srv3 = await run_mock_proxy(HOST, PROXY_PORT3, hit_counter=hit3)

    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='p1', host=HOST, port=PROXY_PORT))
    proxy_store.add(ProxyInfo(id='p2', host=HOST, port=PROXY_PORT2))
    proxy_store.add(ProxyInfo(id='p3', host=HOST, port=PROXY_PORT3))

    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, max_retries=1, db_path=tempfile.mktemp(suffix='.db'))
    router.policy_engine.add_rule(PolicyRule(
        rule_type=RuleType.prefer,
        domain_pattern='*.preferme.com',
        target_type=RuleTarget.proxy_id,
        target_proxy='p2',
    ))
    await router.start()
    try:
        body = await send_http_get(HOST, ROUTER_PORT, url=b"http://test.preferme.com/")
        assert b'proxied' in body
        assert len(hit2) == 1, f"preferred proxy2 should have won, got {hit2}"
        assert len(hit1) == 0, f"p1 should not have been hit, got {hit1}"
        assert len(hit3) == 0, f"p3 should not have been hit, got {hit3}"
    finally:
        await router.stop()
        proxy_srv1.close()
        proxy_srv2.close()
        proxy_srv3.close()
        await asyncio.gather(proxy_srv1.wait_closed(), proxy_srv2.wait_closed(), proxy_srv3.wait_closed())


@pytest.mark.asyncio
async def test_policy_force_connect():
    """FORCE rule should route CONNECT through the forced proxy."""
    hit1, hit2 = [], []
    proxy_srv1 = await run_mock_proxy(HOST, PROXY_PORT, hit_counter=hit1)
    PROXY_PORT2 = PROXY_PORT + 1
    proxy_srv2 = await run_mock_proxy(HOST, PROXY_PORT2, hit_counter=hit2)

    proxy_store = ProxyStore()
    proxy_store.add(ProxyInfo(id='p1', host=HOST, port=PROXY_PORT))
    proxy_store.add(ProxyInfo(id='p2', host=HOST, port=PROXY_PORT2))

    router = Router(proxy_store, listen_host=HOST, listen_port=ROUTER_PORT, db_path=tempfile.mktemp(suffix='.db'))
    router.policy_engine.add_rule(PolicyRule(
        rule_type=RuleType.force,
        domain_pattern='*.forceconn.com',
        target_type=RuleTarget.proxy_id,
        target_proxy='p2',
    ))
    await router.start()
    try:
        echo = await send_connect(HOST, ROUTER_PORT, target=b"test.forceconn.com:443", payload=b"force-conn-test")
        assert echo == b"force-conn-test"
        assert len(hit2) == 1, f"forced proxy2 should have been hit, got {hit2}"
        assert len(hit1) == 0, f"p1 should not have been hit, got {hit1}"
    finally:
        await router.stop()
        proxy_srv1.close()
        proxy_srv2.close()
        await asyncio.gather(proxy_srv1.wait_closed(), proxy_srv2.wait_closed())


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

    def test_policy_api_crud(self):
        proxy_store = ProxyStore()
        proxy_store.add(ProxyInfo(id='pa', host='1.2.3.4', port=8080))
        router = Router(proxy_store, db_path=tempfile.mktemp(suffix='.db'))
        mount(proxy_store, router)
        client = TestClient(api_app)

        r = client.get("/policy/rules")
        assert r.status_code == 200
        assert r.json() == []

        r = client.post("/policy/rules", json={
            "rule_type": "force",
            "domain_pattern": "*.example.com",
            "target_type": "proxy_id",
            "target_proxy": "pa",
            "priority": 10,
            "enabled": True,
        })
        assert r.status_code == 200
        data = r.json()
        assert data["rule_type"] == "force"
        assert data["domain_pattern"] == "*.example.com"
        rule_id = data["id"]

        r = client.get("/policy/rules")
        assert r.status_code == 200
        assert len(r.json()) == 1

        r = client.delete(f"/policy/rules/{rule_id}")
        assert r.status_code == 200

        r = client.get("/policy/rules")
        assert r.status_code == 200
        assert r.json() == []
