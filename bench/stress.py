"""auto_squid 性能压测主驱动。

用法:
    python -m bench.stress                    # 默认:受控 mock 上游,staircase 模式
    python -m bench.stress --mode staircase   # 并发阶梯(测饱和点)
    python -m bench.stress --mode rate        # 恒定速率(测容量上限)
    python -m bench.stress --mode mixed       # 混合负载(冷热域名+大小响应+CONNECT)
    python -m bench.stress --mode soak --duration 120  # 长时稳定性
    python -m bench.stress --upstream real    # 用真实 proxies.yaml 上游(需可达)
    python -m bench.stress --quick            # 快速冒烟(小规模)
    python -m bench.stress --profile          # cProfile 覆盖(定位瓶颈)

指标(准确性设计):
- 吞吐(req/s)、TTFB 与 total 的 P50/P95/P99(客户端 raw socket 精确到状态行)
- 错误率与分类(连接错误 / 非 200 / 超时)
- **真实缓存命中率** = (客户端请求 - 上游命中) / 客户端请求
  (用 mock 上游的 hit 计数器;真实上游模式无法测,记为 N/A)
- **racing 放大率** = 上游命中 / 客户端请求 (竞速扇出开销)
- 资源采样:进程 RSS、文件描述符数、Router 连接池大小、HTTP 缓存条目数
- 报告:终端表格 + 结构化 JSON(带 git 版本,便于跨版本 diff)

可比性:同一 mock 配置 + 同一 Router 代码,多次跑结果可重复(延迟确定性高)。
"""

import argparse
import asyncio
import json
import os
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field, asdict
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bench.mock_upstream import UpstreamCluster, ResponseProfile
from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.config_schema import ProxyInfo


# ── git 版本(写入报告,便于跨版本 diff) ──────────────────────────

def git_version() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ).decode().strip()
    except Exception:
        return "unknown"


# ── 结果容器 ──────────────────────────────────────────────────

@dataclass
class RequestResult:
    ok: bool
    ttfb: float          # 首字节延迟(秒);失败为 0
    total: float         # 总耗时(秒)
    error: str = ""      # 失败原因分类(conn/timeout/echo-mismatch/http:<code>)
    status_code: str = ""  # 上游/Router 返回的 HTTP 状态码(如 "200"/"503");失败时仍可能记录


# ── 主机名空间:mock 伪域名 vs 真实可解析域名 ──────────────────

# mock 模式:这些主机名是 mock ResponseProfile 的匹配键(见 mock_upstream.py),
# 仅存在于本地 mock 世界,上游不会真去解析。
_MOCK_HOSTS = {
    "hot": "hot.example.com",
    "cold": "cold%d.example.com",   # 含 %d,调用方格式化
    "big": "big.example.com",
    "chunked": "chunked.example.com",
    "connect": "echo.example.com:443",
}

# real 模式:内置默认大站池,经真实上游代理可解析可达(www.baidu.com 已实测 200)。
# 热域名用前几个固定站点(命中域名/响应缓存);冷域名用全部轮换(制造缓存未命中)。
_DEFAULT_REAL_HOSTS = [
    "www.baidu.com", "www.qq.com", "www.163.com",
    "www.sina.com.cn", "www.taobao.com",
]


@dataclass
class HostSet:
    """按上游模式提供负载主机名。

    mock 模式返回伪域名(供 mock ResponseProfile 匹配);
    real 模式返回真实可解析域名(供真实上游代理转发)。
    hot(i):热域名(命中缓存);cold(i):冷域名(竞速,未命中);
    big(i)/chunked(i):mock 下测特定大小/编码,real 下退化为普通 GET(用真实域名);
    connect():CONNECT 隧道目标。
    """
    mode: str = "mock"
    real_hosts: list = field(default_factory=lambda: list(_DEFAULT_REAL_HOSTS))
    # 真实上游延迟远高于 mock,客户端超时上调,避免把"上游慢"误判为 timeout。
    # 在 __post_init__ 里按 mode 设定(字段默认值表达式无法引用其他字段)。
    timeout: float = 15.0
    # real 模式:真实站点对任意路径(/p0 等)常返回 3xx/4xx,但这不代表代理失败——
    # 代理已成功转发并回传源站响应,源站状态码与代理性能无关。故 real 模式把
    # "收到任何 HTTP 响应"都记为成功(仅 conn/timeout 算真失败)。mock 模式仍要求 200。
    success_any_status: bool = False

    def __post_init__(self):
        if self.mode == "real":
            self.timeout = 20.0
            self.success_any_status = True

    def hot(self, i: int) -> str:
        if self.mode == "mock":
            return _MOCK_HOSTS["hot"]
        # real:固定用前 2 个站点做热域名,最大化域名/响应缓存命中
        return self.real_hosts[i % min(2, len(self.real_hosts))]

    def cold(self, i: int) -> str:
        if self.mode == "mock":
            return _MOCK_HOSTS["cold"] % (i % 20)
        # real:轮换全部站点,加随机路径制造缓存未命中(走竞速)
        return self.real_hosts[i % len(self.real_hosts)]

    def big(self, i: int) -> str:
        # mock:特定大响应 host(命中 big profile);real:无大响应控制,退化为普通 GET
        if self.mode == "mock":
            return _MOCK_HOSTS["big"]
        return self.real_hosts[i % len(self.real_hosts)]

    def chunked(self, i: int) -> str:
        if self.mode == "mock":
            return _MOCK_HOSTS["chunked"]
        return self.real_hosts[(i + 1) % len(self.real_hosts)]

    def connect(self) -> str:
        if self.mode == "mock":
            return _MOCK_HOSTS["connect"]
        # real:用真实 443 站点建立隧道(不做 echo 校验,见 do_connect_request)
        return f"{self.real_hosts[0]}:443"



@dataclass
class ScenarioResult:
    name: str
    duration: float
    client_requests: int
    results: list = field(default_factory=list)
    upstream_hits: int = 0
    upstream_connects: int = 0
    # 资源采样(峰值/末值)
    rss_peak_mb: float = 0.0
    fd_peak: int = 0
    pool_size_end: int = 0
    http_cache_entries_end: int = 0

    def metrics(self) -> dict:
        ok = [r for r in self.results if r.ok]
        ttfs = [r.ttfb for r in ok]
        tots = [r.total for r in ok]
        errs = [r for r in self.results if not r.ok]
        # 错误细分:http:<code> 按状态码单独计数(如 {'503': 980, '502': 20}),
        # conn/timeout/echo-mismatch 维持原分类。这样 100% 失败时一眼看出是
        # "全部 503(DNS 失败)" 还是别的根因。
        err_kinds: dict[str, int] = {}
        for r in errs:
            key = r.error if r.error.startswith("http:") else r.error
            err_kinds[key] = err_kinds.get(key, 0) + 1
        # 状态码分布(含成功):real 模式 success_any_status 下,"成功"可能全是 3xx/4xx/5xx
        # (代理的盲区)。统计所有结果的状态码,一眼看出"100% 成功但全是 503"这类问题。
        status_dist: dict[str, int] = {}
        for r in self.results:
            if r.status_code:
                status_dist[r.status_code] = status_dist.get(r.status_code, 0) + 1
        def pct(vals, p):
            if not vals:
                return 0.0
            s = sorted(vals)
            k = max(0, min(len(s) - 1, int(len(s) * p)))
            return s[k]
        total_reqs = len(self.results)
        # 放大率:上游命中 / 客户端请求。>1 表示竞速扇出超过单发(冷请求竞速);
        # <1 表示缓存吸收了部分请求(域名缓存命中仍单发;HTTP 响应缓存命中则 0 命中)。
        amplification = self.upstream_hits / total_reqs if total_reqs else 0.0
        # 缓存命中率:被缓存吸收(未触达上游)的请求占比。竞速扇出使 hits 可能
        # 超过请求数,此时命中率为 0(没有请求被缓存吸收,反而被放大)。
        absorbed = max(0, total_reqs - self.upstream_hits)
        cache_hit_rate = absorbed / total_reqs if total_reqs else 0.0
        return {
            "name": self.name,
            "client_requests": total_reqs,
            "success": len(ok),
            "errors": len(errs),
            "error_breakdown": err_kinds,
            "status_distribution": status_dist,
            "error_rate": len(errs) / total_reqs if total_reqs else 0.0,
            "throughput_rps": len(ok) / self.duration if self.duration else 0.0,
            "ttfb_ms": {
                "p50": pct(ttfs, 0.50) * 1000,
                "p95": pct(ttfs, 0.95) * 1000,
                "p99": pct(ttfs, 0.99) * 1000,
                "mean": (statistics.mean(ttfs) * 1000) if ttfs else 0.0,
            },
            "total_ms": {
                "p50": pct(tots, 0.50) * 1000,
                "p95": pct(tots, 0.95) * 1000,
                "p99": pct(tots, 0.99) * 1000,
                "mean": (statistics.mean(tots) * 1000) if tots else 0.0,
            },
            "cache_hit_rate": cache_hit_rate,
            "racing_amplification": amplification,
            "upstream_hits": self.upstream_hits,
            "upstream_connects": self.upstream_connects,
            "rss_peak_mb": self.rss_peak_mb,
            "fd_peak": self.fd_peak,
            "pool_size_end": self.pool_size_end,
            "http_cache_entries_end": self.http_cache_entries_end,
        }


# ── 客户端:raw socket,精确测 TTFB(读到状态行) ────────────────

def _status_code(status_line: bytes) -> str:
    """从状态行提取 3 位状态码;解析失败返回 '000'。如 b'HTTP/1.1 503 ...' -> '503'。"""
    try:
        parts = status_line.split(b' ')
        code = parts[1].decode('latin-1').strip()
        return code if code.isdigit() else "000"
    except Exception:
        return "000"


def _host_of(url: bytes) -> bytes:
    """从绝对 URL(b'http://host/path')取 host,用于 Host 头(而非硬编码 example.com)。"""
    try:
        s = url.decode('latin-1')
        after = s.split('://', 1)[1] if '://' in s else s
        return after.split('/', 1)[0].encode('latin-1')
    except Exception:
        return b'example.com'


async def do_http_request(host: str, port: int, url: bytes, timeout: float = 15.0,
                          success_any_status: bool = False) -> RequestResult:
    """发一个 GET,返回 TTFB(读到状态行)与 total。失败分类:timeout/conn/http:<code>。

    非 200 时记录上游/Router 实际状态码(如 http:503),供 error_breakdown 细分,
    一眼看出是 DNS 失败(503)还是别的根因。Host 头取自 URL 主机名(非硬编码)。

    success_any_status=True(real 模式):收到任何 HTTP 响应即记成功——代理已成功
    转发,源站 3xx/4xx 与代理性能无关;仅 conn/timeout 算真失败。
    """
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout)
    except (asyncio.TimeoutError, OSError):
        return RequestResult(False, 0, time.monotonic() - t0, "conn")
    try:
        host_hdr = _host_of(url)
        writer.write(b"GET " + url + b" HTTP/1.1\r\nHost: " + host_hdr +
                     b"\r\nConnection: close\r\n\r\n")
        await writer.drain()
        try:
            status = await asyncio.wait_for(reader.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            return RequestResult(False, 0, time.monotonic() - t0, "timeout")
        ttfb = time.monotonic() - t0
        code = _status_code(status)
        if not success_any_status and b"200" not in status:
            return RequestResult(False, ttfb, time.monotonic() - t0, f"http:{code}", code)
        # 读头部到空行
        while True:
            h = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not h or h in (b"\r\n", b"\n"):
                break
        # 读 body 到 EOF(Connection: close)
        await asyncio.wait_for(reader.read(-1), timeout=timeout)
        total = time.monotonic() - t0
        # real 模式记成功,但仍把状态码记下供统计(非 200 记入 error_breakdown 的非 http: 前缀)。
        return RequestResult(True, ttfb, total, "", code)
    except asyncio.TimeoutError:
        return RequestResult(False, 0, time.monotonic() - t0, "timeout")
    except (OSError, asyncio.IncompleteReadError):
        return RequestResult(False, 0, time.monotonic() - t0, "conn")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


async def do_connect_request(host: str, port: int, target: bytes, payload: bytes = b"bench-echo",
                              timeout: float = 15.0, echo_check: bool = True) -> RequestResult:
    """发一个 CONNECT。TTFB = 读到 '200' 响应行的时间。

    echo_check=True(mock 模式):建隧道后发 payload,校验上游原样回显(隧道透明)。
    echo_check=False(real 模式):真实 TLS 隧道会加密 payload,无法原样回显,故
    **收到 200 Connection established 即记成功**,不再 echo 校验——只测隧道建立,
    不测隧道内容(真实 TLS 内容无法在代理层校验)。
    """
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port), timeout=timeout)
    except (asyncio.TimeoutError, OSError):
        return RequestResult(False, 0, time.monotonic() - t0, "conn")
    try:
        writer.write(b"CONNECT " + target + b" HTTP/1.1\r\nHost: " + target + b"\r\n\r\n")
        await writer.drain()
        try:
            status = await asyncio.wait_for(reader.readline(), timeout=timeout)
        except asyncio.TimeoutError:
            return RequestResult(False, 0, time.monotonic() - t0, "timeout")
        ttfb = time.monotonic() - t0
        code = _status_code(status)
        if b"200" not in status:
            return RequestResult(False, ttfb, time.monotonic() - t0, f"http:{code}", code)
        while True:
            h = await asyncio.wait_for(reader.readline(), timeout=timeout)
            if not h or h in (b"\r\n", b"\n"):
                break
        if not echo_check:
            # real 模式:隧道建立即成功,不校验回显。
            total = time.monotonic() - t0
            return RequestResult(True, ttfb, total, "", code)
        writer.write(payload)
        await writer.drain()
        echo = await asyncio.wait_for(reader.read(len(payload)), timeout=timeout)
        total = time.monotonic() - t0
        if echo != payload:
            return RequestResult(False, ttfb, total, "echo-mismatch")
        return RequestResult(True, ttfb, total, "", code)
    except asyncio.TimeoutError:
        return RequestResult(False, 0, time.monotonic() - t0, "timeout")
    except (OSError, asyncio.IncompleteReadError):
        return RequestResult(False, 0, time.monotonic() - t0, "conn")
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


# ── 资源采样 ──────────────────────────────────────────────────

def rss_mb() -> float:
    """当前进程 RSS(MB)。"""
    try:
        # ru_maxrss: Linux 上单位 KB
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:
        return 0.0


def fd_count() -> int:
    """当前进程打开的文件描述符数。"""
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return 0


async def sample_resources(router: Router, result: ScenarioResult, stop_evt: asyncio.Event,
                           interval: float = 1.0):
    """周期采样 RSS / fd / 连接池 / 缓存条目,记录峰值。"""
    while not stop_evt.is_set():
        try:
            result.rss_peak_mb = max(result.rss_peak_mb, rss_mb())
            result.fd_peak = max(result.fd_peak, fd_count())
        except Exception:
            pass
        try:
            await asyncio.wait_for(stop_evt.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass
    # 末值
    try:
        result.pool_size_end = len(router._client_pool)
        result.http_cache_entries_end = len(router._http_cache)
    except Exception:
        pass


# ── 负载模式 ──────────────────────────────────────────────────

async def run_concurrent(router_host: str, router_port: int, make_request, total: int,
                          concurrency: int) -> list:
    """以固定并发数跑 total 个请求(阶梯模式用)。

    make_request(i) -> RequestResult。用一个共享 semaphore 限制并发。
    """
    sem = asyncio.Semaphore(concurrency)
    results: list = []

    async def one(i):
        async with sem:
            return await make_request(i)

    tasks = [asyncio.create_task(one(i)) for i in range(total)]
    for t in asyncio.as_completed(tasks):
        results.append(await t)
    return results


async def run_rate(router_host: str, router_port: int, make_request, target_rps: float,
                   duration: float) -> list:
    """以固定速率 target_rps 持续 duration 秒发请求(恒定速率模式用)。

    用令牌桶控制注入节奏;并发自然产生(取决于响应延迟)。超量时适度丢弃以
    守住速率,避免无限堆积。
    """
    results: list = []
    interval = 1.0 / target_rps if target_rps > 0 else 0
    end = time.monotonic() + duration
    i = 0
    pending: set = set()
    loop = asyncio.get_event_loop()
    while time.monotonic() < end:
        next_send = loop.time() + interval
        t = asyncio.create_task(make_request(i))
        pending.add(t)
        t.add_done_callback(lambda x: (pending.discard(x), results.append(x.result())))
        i += 1
        sleep = next_send - loop.time()
        if sleep > 0:
            await asyncio.sleep(sleep)
    # 等待在途完成(带总超时,防卡死)
    if pending:
        try:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=30)
        except asyncio.TimeoutError:
            for t in pending:
                t.cancel()
    return results


# ── 场景定义 ──────────────────────────────────────────────────

def default_mock_cluster(quick: bool) -> UpstreamCluster:
    """默认 mock 集群:4 个上游,快/中/慢/不稳定,模拟真实异构代理。"""
    n_upstream = 2 if quick else 4
    # base_delay 递增模拟快→慢;最后一个带失败率模拟不稳定代理
    specs = []
    delays = [0.0, 0.05, 0.15, 0.3][:n_upstream]
    for i, d in enumerate(delays):
        profs = [ResponseProfile("hot", first_byte_delay=0.01, body_size=2048),
                 ResponseProfile("cold", first_byte_delay=0.02, body_size=1024),
                 ResponseProfile("big", first_byte_delay=0.02, body_size=512 * 1024, chunk_delay=0.005),
                 ResponseProfile("chunked", first_byte_delay=0.02, body_size=64 * 1024, chunked=True, chunk_delay=0.003)]
        fr = 0.3 if (i == n_upstream - 1 and not quick) else 0.0
        specs.append((d, [ResponseProfile(p.host_prefix, p.first_byte_delay, p.body_size,
                                          p.chunked, p.chunk_delay, fr) for p in profs]))
    return UpstreamCluster(specs, start_port=31200)


async def build_router(cluster: Optional[UpstreamCluster], real_proxies_path: str,
                       listen_port: int, max_retries: int, cache_ttl: int,
                       enable_http_cache: bool = True) -> Router:
    """构造并启动 Router。mock 模式用集群;real 模式从 proxies.yaml 加载。"""
    ps = ProxyStore()
    if cluster is not None:
        for i, u in enumerate(cluster.upstreams):
            ps.add(ProxyInfo(id=f"u{i}", host=u.host, port=u.port))
    else:
        ps = ProxyStore(real_proxies_path)
    router = Router(ps, listen_host="127.0.0.1", listen_port=listen_port,
                    max_retries=max_retries, cache_ttl=cache_ttl,
                    enable_http_cache=enable_http_cache,
                    db_path=tempfile.mktemp(suffix=".db"))
    await router.start()
    return router


async def scenario_staircase(router: Router, cluster: Optional[UpstreamCluster],
                             router_host: str, router_port: int, host_set: HostSet,
                             quick: bool) -> ScenarioResult:
    """并发阶梯:并发数 1→N,每级固定请求数,测吞吐与延迟随并发的变化,找饱和点。"""
    levels = [1, 10, 50, 100, 200] if not quick else [1, 10, 50]
    per_level = 200 if not quick else 50
    # 汇总成一个 scenario(分级别记录);此处合并为单一结果,各级在报告里细分
    result = ScenarioResult(name="staircase", duration=0.0, client_requests=0)
    # 用热域名集中竞争(测连接池/事件循环),混合少量冷域名
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(router, result, stop_evt))

    async def make(i):
        # 70% 热(命中域名/响应缓存),30% 冷
        if i % 10 < 7:
            url = f"http://{host_set.hot(i)}/p{i % 4}".encode()
        else:
            url = f"http://{host_set.cold(i)}/p{i % 3}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status)

    t_start = time.monotonic()
    for c in levels:
        if cluster:
            cluster.reset_counts()
        hits_before = cluster.total_hits() if cluster else -1
        res = await run_concurrent(router_host, router_port, make, per_level, c)
        result.results.extend(res)
        result.client_requests += len(res)
        dur = time.monotonic() - t_start
        ok = sum(1 for r in res if r.ok)
        hits = (cluster.total_hits() - hits_before) if cluster else -1
        amp = (hits / len(res)) if (cluster and len(res)) else 0.0
        chit = (max(0, len(res) - hits) / len(res) * 100) if (cluster and hits >= 0 and len(res)) else 0.0
        print(f"  concurrency={c:>4}  ok={ok}/{len(res)}  "
              f"rps={ok / (sum(r.total for r in res) or 1):.0f}(wall)  "
              f"upstream_hits={hits}  amp={amp:.2f}x  cache_hit={chit:.0f}%")
    result.duration = time.monotonic() - t_start
    stop_evt.set()
    await sampler
    if cluster:
        result.upstream_hits = cluster.total_hits()
    return result


async def scenario_rate(router: Router, cluster: Optional[UpstreamCluster],
                        router_host: str, router_port: int, host_set: HostSet,
                        quick: bool) -> ScenarioResult:
    """恒定速率:目标 RPS 阶梯上升,测延迟/错误率随负载的变化,找容量上限。"""
    rates = [100, 500, 1000, 2000] if not quick else [100, 500]
    per_rate_dur = 8.0 if not quick else 3.0
    result = ScenarioResult(name="rate", duration=0.0, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(router, result, stop_evt))

    async def make(i):
        if i % 10 < 7:
            url = f"http://{host_set.hot(i)}/p{i % 4}".encode()
        else:
            url = f"http://{host_set.cold(i)}/p{i % 3}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status)

    t_start = time.monotonic()
    for rps in rates:
        if cluster:
            cluster.reset_counts()
        hits_before = cluster.total_hits() if cluster else -1
        res = await run_rate(router_host, router_port, make, rps, per_rate_dur)
        result.results.extend(res)
        result.client_requests += len(res)
        ok = sum(1 for r in res if r.ok)
        hits = (cluster.total_hits() - hits_before) if cluster else -1
        amp = (hits / len(res)) if (cluster and len(res)) else 0.0
        chit = (max(0, len(res) - hits) / len(res) * 100) if (cluster and hits >= 0 and len(res)) else 0.0
        print(f"  target_rps={rps:>5}  ok={ok}  "
              f"actual_rps={ok/per_rate_dur:.0f}  err={len(res)-ok}  "
              f"upstream_hits={hits}  amp={amp:.2f}x  cache_hit={chit:.0f}%")
    result.duration = time.monotonic() - t_start
    stop_evt.set()
    await sampler
    if cluster:
        result.upstream_hits = cluster.total_hits()
    return result


async def scenario_mixed(router: Router, cluster: Optional[UpstreamCluster],
                         router_host: str, router_port: int, host_set: HostSet,
                         quick: bool) -> ScenarioResult:
    """混合负载:冷热域名 + 大小响应 + CONNECT 隧道,贴近真实流量结构。"""
    total = 2000 if not quick else 300
    concurrency = 100 if not quick else 30
    result = ScenarioResult(name="mixed", duration=0.0, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(router, result, stop_evt))
    # real 模式:真实 TLS 隧道无法原样回显 payload,故建隧道即成功(不 echo 校验);
    # mock 模式:隧道透明,做 echo 校验。
    echo_check = cluster is not None
    sas = host_set.success_any_status

    async def make(i):
        r = i % 10
        if r < 3:
            # 30% 热域名小响应(命中域名+响应缓存)
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.hot(i)}/p{i % 4}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas)
        elif r < 5:
            # 20% 大响应(content-length 流式;real 模式退化为普通 GET)
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.big(i)}/p{i % 2}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas)
        elif r < 7:
            # 20% chunked 响应(测流式 chunked 路径;real 模式退化为普通 GET)
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.chunked(i)}/p{i % 2}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas)
        elif r < 9:
            # 20% 冷域名(每次竞速,不命中缓存)
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.cold(i)}/p{i % 5}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas)
        else:
            # 10% CONNECT 隧道
            return await do_connect_request(router_host, router_port,
                                            host_set.connect().encode(),
                                            timeout=host_set.timeout, echo_check=echo_check)

    if cluster:
        cluster.reset_counts()
    t_start = time.monotonic()
    res = await run_concurrent(router_host, router_port, make, total, concurrency)
    result.results = res
    result.client_requests = len(res)
    result.duration = time.monotonic() - t_start
    stop_evt.set()
    await sampler
    if cluster:
        result.upstream_hits = cluster.total_hits()
        result.upstream_connects = cluster.total_connects()
    return result


async def scenario_soak(router: Router, cluster: Optional[UpstreamCluster],
                        router_host: str, router_port: int, host_set: HostSet,
                        duration: float, quick: bool) -> ScenarioResult:
    """长时稳定性:固定并发持续跑,周期打印资源趋势,抓内存/fd/缓存泄漏。"""
    duration = duration if not quick else min(duration, 20.0)
    concurrency = 100
    target_rps = 300
    result = ScenarioResult(name="soak", duration=duration, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(router, result, stop_evt, interval=2.0))

    async def make(i):
        if i % 10 < 7:
            url = f"http://{host_set.hot(i)}/p{i % 4}".encode()
        else:
            url = f"http://{host_set.cold(i)}/p{i % 5}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status)

    if cluster:
        cluster.reset_counts()
    # 恒定速率跑指定时长
    t_start = time.monotonic()
    last_print = t_start
    res_task = asyncio.create_task(run_rate(router_host, router_port, make, target_rps, duration))
    # 进度打印
    while time.monotonic() - t_start < duration:
        await asyncio.sleep(2.0)
        if time.monotonic() - last_print > 5.0:
            print(f"  t={time.monotonic()-t_start:.0f}s  "
                  f"rss={rss_mb():.0f}MB  fd={fd_count()}  "
                  f"pool={len(router._client_pool)}  http_cache={len(router._http_cache)}")
            last_print = time.monotonic()
    res = await res_task
    result.results = res
    result.client_requests = len(res)
    result.duration = time.monotonic() - t_start
    stop_evt.set()
    await sampler
    if cluster:
        result.upstream_hits = cluster.total_hits()
    return result


# ── 报告输出 ──────────────────────────────────────────────────

def print_report(metrics_list: list, git_ver: str, upstream_mode: str = "mock"):
    print("\n" + "=" * 78)
    print(f" auto_squid 压测报告  (git: {git_ver})  [上游: {upstream_mode}]")
    print("=" * 78)
    is_real = upstream_mode == "real"
    for m in metrics_list:
        print(f"\n■ 场景: {m['name']}")
        print(f"  请求数        : {m['client_requests']}  (成功 {m['success']}, 失败 {m['errors']})")
        if m['errors']:
            print(f"  错误分类      : {m['error_breakdown']}  (错误率 {m['error_rate']*100:.2f}%)")
        if m.get('status_distribution'):
            # real 模式下尤其关键:揭示"成功"背后的真实状态码分布(如全是 503)
            print(f"  状态码分布    : {m['status_distribution']}")
        print(f"  吞吐          : {m['throughput_rps']:.1f} req/s")
        print(f"  TTFB (ms)     : P50={m['ttfb_ms']['p50']:.1f}  P95={m['ttfb_ms']['p95']:.1f}  "
              f"P99={m['ttfb_ms']['p99']:.1f}  mean={m['ttfb_ms']['mean']:.1f}")
        print(f"  Total (ms)    : P50={m['total_ms']['p50']:.1f}  P95={m['total_ms']['p95']:.1f}  "
              f"P99={m['total_ms']['p99']:.1f}  mean={m['total_ms']['mean']:.1f}")
        if is_real:
            # 真实上游无命中计数器,缓存命中率/放大率无法测(并非 100% 命中)。
            print(f"  缓存命中率    : N/A (真实上游模式无法测)")
            print(f"  racing 放大率 : N/A (真实上游模式无法测)")
        else:
            print(f"  缓存命中率    : {m['cache_hit_rate']*100:.1f}%  (上游命中 {m['upstream_hits']})")
            print(f"  racing 放大率 : {m['racing_amplification']:.2f}x  "
                  f"(每客户端请求扇出到 {m['racing_amplification']:.2f} 个上游)")
        print(f"  资源峰值      : RSS={m['rss_peak_mb']:.0f}MB  fd={m['fd_peak']}  "
              f"连接池末值={m['pool_size_end']}  HTTP缓存条目末值={m['http_cache_entries_end']}")
    print("\n" + "=" * 78)


# ── 主入口 ────────────────────────────────────────────────────

async def amain(args):
    git_ver = git_version()
    print(f"auto_squid 压测  git={git_ver}  mode={args.mode}  upstream={args.upstream}  quick={args.quick}")
    cluster: Optional[UpstreamCluster] = None
    try:
        # 主机名空间:mock 用伪域名(供 mock ResponseProfile 匹配),real 用真实可解析域名。
        real_hosts = args.real_hosts.split(",") if args.real_hosts else list(_DEFAULT_REAL_HOSTS)
        host_set = HostSet(mode=args.upstream, real_hosts=[h.strip() for h in real_hosts if h.strip()])
        if args.upstream == "mock":
            cluster = default_mock_cluster(args.quick)
            await cluster.start()
            n = len(cluster.upstreams)
            print(f"mock 上游: {n} 个实例 (端口 31200..{31200+n-1})")
        else:
            print(f"真实上游: 从 {args.proxies} 加载  (压测主机名池: {len(host_set.real_hosts)} 个)")

        router = await build_router(cluster, args.proxies, args.router_port,
                                    max_retries=args.max_retries, cache_ttl=args.cache_ttl,
                                    enable_http_cache=not args.no_http_cache)
        print(f"Router 监听 127.0.0.1:{args.router_port}  max_retries={args.max_retries}  "
              f"cache_ttl={args.cache_ttl}  http_cache={'on' if not args.no_http_cache else 'off'}")

        results: list = []
        if args.profile:
            import cProfile, pstats, io
            pr = cProfile.Profile()
            pr.enable()

        if args.mode == "staircase":
            print("\n[staircase] 并发阶梯,测饱和点")
            results.append(await scenario_staircase(router, cluster, "127.0.0.1", args.router_port, host_set, args.quick))
        elif args.mode == "rate":
            print("\n[rate] 恒定速率阶梯,测容量上限")
            results.append(await scenario_rate(router, cluster, "127.0.0.1", args.router_port, host_set, args.quick))
        elif args.mode == "mixed":
            print("\n[mixed] 混合负载(冷热+大小+CONNECT)")
            results.append(await scenario_mixed(router, cluster, "127.0.0.1", args.router_port, host_set, args.quick))
        elif args.mode == "soak":
            print(f"\n[soak] 长时稳定性 {args.duration}s")
            results.append(await scenario_soak(router, cluster, "127.0.0.1", args.router_port, host_set,
                                                args.duration, args.quick))
        elif args.mode == "all":
            print("\n[staircase] 并发阶梯")
            results.append(await scenario_staircase(router, cluster, "127.0.0.1", args.router_port, host_set, args.quick))
            print("\n[rate] 恒定速率")
            results.append(await scenario_rate(router, cluster, "127.0.0.1", args.router_port, host_set, args.quick))
            print("\n[mixed] 混合负载")
            results.append(await scenario_mixed(router, cluster, "127.0.0.1", args.router_port, host_set, args.quick))
            print(f"\n[soak] 长时 {args.duration}s")
            results.append(await scenario_soak(router, cluster, "127.0.0.1", args.router_port, host_set,
                                                args.duration, args.quick))

        if args.profile:
            pr.disable()
            s = io.StringIO()
            pstats.Stats(pr, stream=s).sort_stats("cumulative").print_stats(25)
            print("\n[cProfile top 25 by cumulative]")
            print(s.getvalue())
            with open("bench_profile.txt", "w") as f:
                pstats.Stats(pr, stream=f).sort_stats("cumulative").print_stats(40)

        # 汇总指标 + 报告
        metrics_list = [r.metrics() for r in results]
        # 真实上游无命中计数器:把缓存命中率/放大率置 null,避免 JSON 误导读者。
        if args.upstream == "real":
            for m in metrics_list:
                m["cache_hit_rate"] = None
                m["racing_amplification"] = None
        print_report(metrics_list, git_ver, args.upstream)

        # 结构化 JSON
        report = {
            "git": git_ver,
            "mode": args.mode,
            "upstream": args.upstream,
            "quick": args.quick,
            "max_retries": args.max_retries,
            "cache_ttl": args.cache_ttl,
            "http_cache_enabled": not args.no_http_cache,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "scenarios": metrics_list,
        }
        out_path = args.output
        with open(out_path, "w") as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        print(f"\n结构化报告已写入: {out_path}")

        await router.stop()
    finally:
        if cluster:
            await cluster.stop()


def main():
    p = argparse.ArgumentParser(description="auto_squid 性能压测")
    p.add_argument("--mode", choices=["staircase", "rate", "mixed", "soak", "all"],
                   default="staircase", help="压测模式")
    p.add_argument("--upstream", choices=["mock", "real"], default="mock",
                   help="上游:mock(受控本地) 或 real(真实 proxies.yaml)")
    p.add_argument("--proxies", default="proxies.yaml", help="真实模式下的代理列表文件")
    p.add_argument("--real-hosts", default="",
                   help="真实模式下压测的主机名(逗号分隔);默认内置大站池"
                        "(www.baidu.com,www.qq.com,...)。主机名需可被上游代理解析")
    p.add_argument("--router-port", type=int, default=10820, help="被测 Router 监听端口")
    p.add_argument("--max-retries", type=int, default=3, help="竞速首批并行数")
    p.add_argument("--cache-ttl", type=int, default=300, help="域名缓存 TTL(秒)")
    p.add_argument("--no-http-cache", action="store_true",
                   help="禁用 HTTP 响应缓存(测纯路由性能,隔离缓存层)")
    p.add_argument("--duration", type=float, default=60.0, help="soak 模式时长(秒)")
    p.add_argument("--quick", action="store_true", help="快速冒烟(小规模,~10s)")
    p.add_argument("--profile", action="store_true", help="启用 cProfile 覆盖")
    p.add_argument("--output", default="bench_report.json", help="JSON 报告输出路径")
    args = p.parse_args()
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n中断")


if __name__ == "__main__":
    main()
