"""P1/P2 观测面:探活状态、最近失败、/metrics/summary、/system-stats、趋势桶。

这些是面板与运维依赖的"运行态"数据,口径错了比没有更糟:
- 探活状态必须区分 成功/失败/跳过,失败还要带阶段(connect vs handshake);
- 最近失败不能把竞速取消(CancelledError)算进去;
- 趋势桶必须是增量,否则曲线永远单调上升;
- /metrics/summary 不能顺带序列化域名统计(面板每 30s 拉一次)。
"""
import asyncio
import time

import pytest
from fastapi.testclient import TestClient

from auto_squid.api import app as api_app, mount
from auto_squid.config_schema import ProxyInfo
from auto_squid.proxy_store import ProxyStore
from auto_squid.router import make_router

HOST = '127.0.0.1'
MOCK_PORT = 10941
DEAD_PORT = 10942
ROUTER_PORT = 10943


async def _run_mock(port):
    """极简 mock:收到 CONNECT 回 200;探活预检(连上即关)直接结束。"""
    async def handle(reader, writer):
        try:
            line = await reader.readline()
            if line.upper().startswith(b'CONNECT'):
                while True:
                    h = await asyncio.wait_for(reader.readline(), timeout=1.0)
                    if not h or h in (b'\r\n', b'\n'):
                        break
                writer.write(b'HTTP/1.1 200 Connection established\r\n\r\n')
                await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass
    return await asyncio.start_server(handle, HOST, port)


class TestProbeAndFailureState:
    async def test_probe_success_and_connect_failure_are_recorded(self):
        srv = await _run_mock(MOCK_PORT)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=MOCK_PORT))
        ps.add(ProxyInfo(id='dead', host=HOST, port=DEAD_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0,
                        probe_canary=f'{HOST}:{MOCK_PORT}')
        try:
            await r._probe_proxy(ps.get('up'))
            ok = r.probe_state_snapshot()['up']
            assert ok['ok'] is True and ok['stage'] == 'ok'
            assert ok['latency_ms'] is not None and ok['err'] is None
            assert ok['at'] > 0

            await r._probe_proxy(ps.get('dead'))
            bad = r.probe_state_snapshot()['dead']
            assert bad['ok'] is False and bad['stage'] == 'connect'
            assert bad['err'] in ('ConnectionRefusedError', 'TimeoutError', 'OSError')
            # 失败仍要喂熔断(与真实请求同源),但不能污染成功路径的 EWMA 结论
            assert r.probes_failed == 1 and r.probes_ok == 1
        finally:
            await r.stop()
            srv.close()
            await srv.wait_closed()

    async def test_skip_marks_state_without_charging_failure(self):
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=DEAD_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0,
                        probe_canary=f'{HOST}:{DEAD_PORT}')  # 本机也不可达 → skip
        try:
            await r._probe_proxy(ps.get('up'))
            st = r.probe_state_snapshot()['up']
            assert st['ok'] is None and st['stage'] == 'skip'
            assert r.probes_skipped == 1
            assert r.probes_failed == 0 and r.probes_sent == 0
        finally:
            await r.stop()

    async def test_last_failure_ignores_racing_cancellation(self):
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=MOCK_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0)
        try:
            t0 = time.perf_counter()
            r._log_attempt_failure('', 'a.test:443', 'a.test:443', 'up',
                                   RuntimeError('boom'), t0)
            first = r.last_failure_snapshot('up')
            assert first['err'] == 'RuntimeError' and first['domain'] == 'a.test:443'

            # 竞速败者取消:不得覆盖"最近真实失败"
            r._log_attempt_failure('', 'b.test:443', 'b.test:443', 'up',
                                   asyncio.CancelledError(), t0)
            assert r.last_failure_snapshot('up') == first
        finally:
            await r.stop()


class TestOpsEndpoints:
    async def test_circuit_lists_all_enabled_proxies_with_detail(self):
        """健康代理也必须出现在 /circuit:面板按它渲染每行探活/在途/退避。"""
        srv = await _run_mock(MOCK_PORT)
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=MOCK_PORT))
        ps.add(ProxyInfo(id='dead', host=HOST, port=DEAD_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0,
                        probe_canary=f'{HOST}:{MOCK_PORT}')
        try:
            await r._probe_proxy(ps.get('up'))
            await r._probe_proxy(ps.get('dead'))
            mount(ps, r)
            try:
                p = TestClient(api_app).get('/circuit').json()['proxies']
                assert {'up', 'dead'} <= set(p), "健康代理被漏掉"
                assert p['up']['probe']['ok'] is True
                assert p['dead']['probe']['ok'] is False
                assert p['dead']['backoff_remaining_sec'] >= 0.0
                assert p['dead']['in_flight'] == 0
                assert 'last_failure' in p['up']
            finally:
                mount(None, None)
        finally:
            await r.stop()
            srv.close()
            await srv.wait_closed()

    async def test_metrics_summary_omits_domain_stats(self):
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=MOCK_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0)
        mount(ps, r)
        try:
            client = TestClient(api_app)
            s = client.get('/metrics/summary').json()
            assert set(s.keys()) == {'counters'}, "summary 不得携带域名统计/分位"
            assert 'probes_sent' in s['counters']
            full = client.get('/metrics').json()
            assert 'domain_stats' in full and 'proxy_metrics' in full
        finally:
            mount(None, None)
            await r.stop()

    def test_system_stats_reports_process_resources(self):
        mount(ProxyStore(), None)
        try:
            client = TestClient(api_app)
            st = client.get('/system-stats').json()
            assert st['rss_mb'] > 0
            assert st['fds'] > 0
            assert st['threads'] >= 1
            lag = st['event_loop_lag_ms']
            assert set(lag.keys()) == {'p50', 'p95', 'max', 'samples'}
            assert lag['samples'] >= 0
        finally:
            mount(None, None)

    async def test_trend_buckets_are_incremental(self):
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=MOCK_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0)
        try:
            r._record_trend_bucket()
            b1 = r.trend_snapshot()[-1]
            assert b1['probe_ok'] == 0 and b1['requests'] == 0

            r.probes_ok += 5
            r.probes_failed += 2
            r.probes_skipped += 1
            r.request_counts['up'] = 3
            r.selector.circuit_open_count += 2
            r._record_trend_bucket()
            b2 = r.trend_snapshot()[-1]
            assert (b2['probe_ok'], b2['probe_fail'], b2['probe_skip']) == (5, 2, 1)
            assert b2['requests'] == 3
            assert b2['circuit_opens'] == 2
            # 桶里绝不能是累计值:第二轮不能再看到 5/2/1 的累加后的总量
            assert b2['ts'] >= b1['ts']

            mount(ps, r)
            try:
                out = TestClient(api_app).get('/metrics/trend').json()
                assert out['interval_sec'] == r.trend_interval_sec
                assert len(out['buckets']) == 2
            finally:
                mount(None, None)
        finally:
            await r.stop()

    async def test_trend_loop_collects_buckets(self):
        ps = ProxyStore()
        ps.add(ProxyInfo(id='up', host=HOST, port=MOCK_PORT))
        r = make_router(ps, listen_host=HOST, listen_port=ROUTER_PORT,
                        db_path=f'/tmp/ops_metrics_{time.time_ns()}.db',
                        probe_interval_sec=0.0)
        r.trend_interval_sec = 0.05
        try:
            await r.start()
            await asyncio.sleep(0.22)
            assert len(r.trend_snapshot()) >= 2, "趋势循环未产生桶"
        finally:
            await r.stop()