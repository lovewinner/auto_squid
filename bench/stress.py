"""auto_squid 性能压测主驱动(进程隔离版)。

用法:
    python -m bench.stress                    # 默认:受控 mock 上游,staircase 模式
    python -m bench.stress --mode staircase   # 并发阶梯(测饱和点)
    python -m bench.stress --mode rate        # 恒定速率(测容量上限)
    python -m bench.stress --mode mixed       # 混合负载(冷热域名+大小响应+CONNECT)
    python -m bench.stress --mode soak --duration 120  # 长时稳定性
    python -m bench.stress --mode conn-reuse  # 连接复用率(测 keepalive)
    python -m bench.stress --upstream real    # 用真实 proxies.yaml 上游(需可达)
    python -m bench.stress --quick            # 快速冒烟(小规模)
    python -m bench.stress --profile          # cProfile 覆盖(仅客户端进程)
    python -m bench.stress --mode soak --open-loop  # 开环 soak(不限速,测真实上限)
    python -m bench.stress --rounds 5         # 同一条件跑 5 轮,每轮全新子进程/缓存,取均值去噪声

进程隔离(本版核心改进):
- 被测 Router(+ mock 上游)跑在**独立子进程**(bench.server_proc),有自己的事件循环。
- 本进程只跑压测客户端,经 127.0.0.1 回环打过去——客户端开销不再污染被测方,
  消除旧版"客户端与服务端同循环争抢"导致的吞吐/延迟测量失真。
- 服务端计数器(缓存命中/竞速扇出)经子进程的 /metrics 跨进程拉取,
  在 mock 与 real 两种模式下**统一**计算缓存命中率/放大率(real 模式不再 N/A)。

指标(分组报告):
- requests: 客户端请求/注入/预热/成功/失败
- throughput: completed_rps(完成)、injected_rps(注入,soak 区分主动限速 vs 被动撑不住)
- latency: TTFB 与 total 的 P50/P95/P99(客户端 raw socket 精确到状态行)
- cache: http_hit_rate / domain_hit_rate(服务端计数器,两种模式通用)
- racing: amplification(上游扇出/客户端请求)、upstream_attempts、invocations
- resources: RSS/fd/连接池/缓存条目 + 服务端 CPU% 与事件循环延迟(子进程采样)
- correctness: mock 模式响应体校验(body 大小/内容,缓存命中字节一致);real 模式 N/A
- attribution: upstream_throttled(是否 429/503)、bottleneck(proxy/upstream)

可比性:同一 mock 配置 + 同一 Router 代码,多次跑结果可重复(延迟确定性高)。
报告 JSON 带 git 版本,便于跨版本 diff(注:本版结构已重新分组,旧报告不兼容)。
"""

import argparse
import asyncio
import cProfile
import io
import json
import os
import pstats
import resource
import socket
import statistics
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass, field
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx


# ── git 版本(写入报告,便于跨版本 diff) ──────────────────────────

def git_version() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        ).decode().strip()
    except Exception:
        return "unknown"


# ── 通用小工具 ─────────────────────────────────────────────────

def _percentile(vals: list[float], p: float) -> float:
    """计算有序百分位。空列表返回 0.0;索引越界钳制到两端。"""
    if not vals:
        return 0.0
    s = sorted(vals)
    k = max(0, min(len(s) - 1, int(len(s) * p)))
    return s[k]


def _free_port() -> int:
    """预选一个空闲本地端口(TCP)。bind 后立即关闭,由调用方尽快使用。"""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


# 服务端 loop-lag 未取到时的空默认值(与 ServerStatsSampler 的快照结构一致)。
_EMPTY_LOOP_LAG = {"p50": 0.0, "p95": 0.0, "max": 0.0}


# ── 结果容器 ──────────────────────────────────────────────────

@dataclass
class RequestResult:
    ok: bool
    ttfb: float          # 首字节延迟(秒);失败为 0
    total: float         # 总耗时(秒)
    error: str = ""      # 失败原因分类(conn/timeout/echo-mismatch/http:<code>)
    status_code: str = ""  # 上游/Router 返回的 HTTP 状态码;失败时仍可能记录
    body: bytes = b""    # 响应 body(capture_body=True 时填,供正确性校验)


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

# mock 模式下各 host 前缀对应的预期响应 body 大小(镜像 default_mock_cluster 的 profile)。
# 正确性校验据此断言 len(body)==size 且 body==b"x"*size(mock body 见 mock_upstream._handle)。
_MOCK_EXPECTED_BODY_BYTES = {
    "hot": 2048,
    "cold": 1024,
    "big": 512 * 1024,
    "chunked": 64 * 1024,
}

# real 模式:内置默认大站池,经真实上游代理可解析可达(www.baidu.com 已实测 200)。
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
    timeout: float = 15.0
    success_any_status: bool = False
    has_dead: bool = False

    def __post_init__(self):
        if self.mode == "real":
            self.timeout = 20.0
            self.success_any_status = True

    def hot(self, i: int) -> str:
        if self.mode == "mock":
            return _MOCK_HOSTS["hot"]
        return self.real_hosts[i % min(2, len(self.real_hosts))]

    def cold(self, i: int) -> str:
        if self.mode == "mock":
            return _MOCK_HOSTS["cold"] % (i % 20)
        return self.real_hosts[i % len(self.real_hosts)]

    def big(self, i: int) -> str:
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
        return f"{self.real_hosts[0]}:443"

    def cold_or_hot(self, i: int) -> str:
        """注入坏代理时用冷域名(强制竞速,让死代理被尝试);否则用热域名(缓存
        命中率高,省上游流量)。由 run_scenarios 按 dead_proxies 是否注入切换。"""
        return self.cold(i) if self.has_dead else self.hot(i)


# ── mock 集群规格序列化(供子进程重建 + 客户端预期 body) ─────────

def default_mock_specs(quick: bool) -> list:
    """生成 mock 集群规格(可 JSON 序列化),供子进程 _build_mock_cluster 重建。

    与旧 default_mock_cluster 同构:4 个上游(quick=2),快/中/慢/不稳定,
    最后一个带 0.3 失败率。规格为 [[base_delay, [profile_dict,...]], ...]。
    """
    n_upstream = 2 if quick else 4
    delays = [0.0, 0.05, 0.15, 0.3][:n_upstream]
    specs = []
    for i, d in enumerate(delays):
        # 最后一个上游(仅全量模式)带 0.3 失败率,模拟不稳定上游。
        fr = 0.3 if (i == n_upstream - 1 and not quick) else 0.0
        profs = [
            {"host_prefix": "hot", "first_byte_delay": 0.01, "body_size": 2048,
             "chunked": False, "chunk_delay": 0.0, "fail_rate": fr},
            {"host_prefix": "cold", "first_byte_delay": 0.02, "body_size": 1024,
             "chunked": False, "chunk_delay": 0.0, "fail_rate": fr},
            {"host_prefix": "big", "first_byte_delay": 0.02, "body_size": 512 * 1024,
             "chunked": False, "chunk_delay": 0.005, "fail_rate": fr},
            {"host_prefix": "chunked", "first_byte_delay": 0.02, "body_size": 64 * 1024,
             "chunked": True, "chunk_delay": 0.003, "fail_rate": fr},
        ]
        specs.append((d, profs))
    return specs


# ── 结果容器(场景级) ────────────────────────────────────────

@dataclass
class ScenarioResult:
    name: str
    duration: float
    client_requests: int
    results: list = field(default_factory=list)
    injected_requests: int = 0      # 实际创建的 task 数(run_rate 用,区分注入 vs 完成)
    warmup_requests: int = 0        # 预热请求数(不计入 results)
    # 服务端计数器快照:场景开始/结束各一次 /metrics 快照,差值算缓存/竞速。
    counters_before: dict = field(default_factory=dict)
    counters_after: dict = field(default_factory=dict)
    # mock 上游计数(交叉验证;real 模式无):连接复用场景用 new_conns。
    upstream_hits: int = 0
    upstream_new_conns: int = 0
    # 服务端资源(子进程经 /server-stats 拉取)
    server_stats: dict = field(default_factory=dict)
    # 客户端进程资源(峰值/末值)
    rss_peak_mb: float = 0.0
    fd_peak: int = 0
    # 正确性校验(mock 模式)
    correctness_checked: bool = False
    correctness_failures: list = field(default_factory=list)
    # 计数器拉取是否失败(失败则 cache/racing 组记 null)
    counter_fetch_failed: bool = False

    def metrics(self) -> dict:
        ok = [r for r in self.results if r.ok]
        ttfs = [r.ttfb for r in ok]
        tots = [r.total for r in ok]
        errs = [r for r in self.results if not r.ok]
        err_kinds: dict[str, int] = {}
        for r in errs:
            key = r.error if r.error.startswith("http:") else r.error
            err_kinds[key] = err_kinds.get(key, 0) + 1
        status_dist: dict[str, int] = {}
        for r in self.results:
            if r.status_code:
                status_dist[r.status_code] = status_dist.get(r.status_code, 0) + 1

        total_reqs = len(self.results)

        # 服务端计数器差值(两种模式统一)。拉取失败则全 null。
        if self.counter_fetch_failed or not self.counters_before or not self.counters_after:
            cache_group = {"http_hit_rate": None, "domain_hit_rate": None,
                           "http_hits": None, "http_misses": None,
                           "http_cache_entries_end": None}
            racing_group = {"amplification": None, "upstream_attempts": None,
                            "invocations": None}
            circuit_group = {"circuit_open_count": None, "probes_sent": None,
                             "probes_ok": None, "proxies_open_end": 0,
                             "single_send_degrades": None}
            inflight_group = {"max_in_flight": None, "in_flight_end": {}}
        else:
            cb, ca = self.counters_before, self.counters_after
            hits = ca.get("http_cache_hits", 0) - cb.get("http_cache_hits", 0)
            misses = ca.get("http_cache_misses", 0) - cb.get("http_cache_misses", 0)
            dom_hits = ca.get("domain_cache_hits", 0) - cb.get("domain_cache_hits", 0)
            attempts = ca.get("upstream_attempts", 0) - cb.get("upstream_attempts", 0)
            invocs = ca.get("racing_invocations", 0) - cb.get("racing_invocations", 0)
            denom = hits + misses
            cache_group = {
                "http_hit_rate": (hits / denom) if denom else 0.0,
                "domain_hit_rate": (dom_hits / total_reqs) if total_reqs else 0.0,
                "http_hits": hits, "http_misses": misses,
                "http_cache_entries_end": ca.get("http_cache_entries", 0),
            }
            racing_group = {
                "amplification": (attempts / total_reqs) if total_reqs else 0.0,
                "upstream_attempts": attempts, "invocations": invocs,
            }
            # 熔断/探活计数器差值(场景内熔断开合次数、探活次数) + 场景末仍处于
            # 熔断的代理数。R18 曾因未持久化导致无法确认熔断器是否开合过,此处补齐。
            circuit_group = {
                "circuit_open_count": ca.get("circuit_open_count", 0) - cb.get("circuit_open_count", 0),
                "probes_sent": ca.get("probes_sent", 0) - cb.get("probes_sent", 0),
                "probes_ok": ca.get("probes_ok", 0) - cb.get("probes_ok", 0),
                "proxies_open_end": sum(1 for v in (ca.get("circuit_state") or {}).values() if v.get("open")),
                "single_send_degrades": ca.get("single_send_degrades", 0) - cb.get("single_send_degrades", 0),
            }
            # 在途选批(in-flight)观测:场景内单代理在途数高水位(场景内峰值,而非
            # 累计,故取差值)与场景末各代理当前在途数(应回落为 0,证明计数无泄漏)。
            inflight_group = {
                "max_in_flight": ca.get("max_in_flight", 0) - cb.get("max_in_flight", 0),
                "in_flight_end": ca.get("proxy_in_flight", {}) or {},
            }

        # 瓶颈归因:状态分布含 429/503 → 上游触顶;否则 proxy。
        throttled = any(code in status_dist for code in ("429", "503"))
        bottleneck = "upstream" if (throttled and cache_group["http_hit_rate"] is not None
                                    and cache_group["http_hit_rate"] > 0.1) else "proxy"

        # 正确性(mock 模式):统计通过/失败。
        if self.correctness_checked:
            correctness = {
                "checked": True,
                "passed": total_reqs - len(self.correctness_failures),
                "failed": len(self.correctness_failures),
                "failures_sample": self.correctness_failures[:5],
            }
        else:
            correctness = {"checked": False, "passed": 0, "failed": 0, "failures_sample": []}

        # 服务端资源(子进程 CPU/loop-lag)。
        ss = self.server_stats or {}

        return {
            "name": self.name,
            "requests": {
                "client": total_reqs,
                "injected": self.injected_requests or total_reqs,
                "warmup": self.warmup_requests,
                "success": len(ok),
                "errors": len(errs),
            },
            "throughput": {
                "completed_rps": len(ok) / self.duration if self.duration else 0.0,
                # 注入速率:run_rate 记录了实际创建的 task 数;未记(阶梯/混合)则等于完成数。
                "injected_rps": ((self.injected_requests or total_reqs) / self.duration) if self.duration else 0.0,
            },
            "latency": {
                "ttfb_ms": {
                    "p50": _percentile(ttfs, 0.50) * 1000,
                    "p95": _percentile(ttfs, 0.95) * 1000,
                    "p99": _percentile(ttfs, 0.99) * 1000,
                    "mean": (statistics.mean(ttfs) * 1000) if ttfs else 0.0,
                },
                "total_ms": {
                    "p50": _percentile(tots, 0.50) * 1000,
                    "p95": _percentile(tots, 0.95) * 1000,
                    "p99": _percentile(tots, 0.99) * 1000,
                    "mean": (statistics.mean(tots) * 1000) if tots else 0.0,
                },
            },
            "errors": {"breakdown": err_kinds, "rate": len(errs) / total_reqs if total_reqs else 0.0},
            "status_distribution": status_dist,
            "cache": cache_group,
            "racing": racing_group,
            "circuit": circuit_group,
            "in_flight": inflight_group,
            "resources": {
                "rss_peak_mb": self.rss_peak_mb,
                "fd_peak": self.fd_peak,
                "pool_size_end": self.counters_after.get("client_pool_size", 0) if self.counters_after else 0,
                "server_cpu_pct": ss.get("cpu_pct", 0.0),
                "server_loop_lag_ms": ss.get("loop_lag_ms", _EMPTY_LOOP_LAG),
            },
            "correctness": correctness,
            "attribution": {"upstream_throttled": throttled, "bottleneck": bottleneck},
            "counter_fetch_failed": self.counter_fetch_failed,
            # mock 交叉验证字段(real 模式为 0/不适用)。
            "mock_upstream_hits": self.upstream_hits,
            "mock_upstream_new_conns": self.upstream_new_conns,
        }


# ── 多轮聚合:同一条件跑 N 轮,取 min/max/mean/stddev 去环境噪声 ──────

def _stats(vals: list) -> dict:
    """对一组跨轮标量求 {min, max, mean, stddev}。None 感知(cache/racing 组
    计数器拉取失败时为 None),空列表返回全 0。"""
    nums = [v for v in vals if v is not None and isinstance(v, (int, float))]
    n = len(nums)
    if n == 0:
        return {"min": 0.0, "max": 0.0, "mean": 0.0, "stddev": 0.0}
    mean = sum(nums) / n
    var = sum((x - mean) ** 2 for x in nums) / n if n > 1 else 0.0
    return {"min": min(nums), "max": max(nums), "mean": mean, "stddev": var ** 0.5}


_AGG_PASS_THROUGH = {"name", "breakdown", "failures_sample"}
_AGG_SCALAR_LEAF = (int, float)


def _agg_dict(dicts: list[dict]) -> dict:
    """把 N 轮的同一 dict(场景 metric)逐键聚合。

    标量叶 → _stats();ttfb_ms/total_ms 等数值子 dict → 递归聚合;
    bool → any()(如 counter_fetch_failed);name/breakdown/failures_sample →
    透传第 0 轮;缺失键(某轮 server-stats 未取到)透传第 0 轮。
    """
    out = {}
    for k in dicts[0]:
        if k in _AGG_PASS_THROUGH:
            out[k] = dicts[0][k]
            continue
        vals = [d.get(k) for d in dicts]
        if all(isinstance(v, _AGG_SCALAR_LEAF) and not isinstance(v, bool) for v in vals):
            out[k] = _stats(vals)
        elif all(isinstance(v, bool) for v in vals):
            out[k] = any(vals)
        elif all(isinstance(v, dict) for v in vals):
            out[k] = _agg_dict(vals)
        else:
            out[k] = dicts[0][k]  # 混合/缺失:透传第 0 轮
    return out


def aggregate_scenarios(round_metrics: list[list[dict]]) -> dict:
    """跨轮聚合场景指标。round_metrics: 外圈=轮次,内圈=场景(按名对齐)。

    返回按场景名分组的聚合树:{name: {min,max,mean,stddev} ...}。
    每轮场景列表须同名同序(由 run_scenarios 保证);防御性按 name 对齐。
    """
    agg: dict = {}
    if not round_metrics:
        return agg
    names = [m["name"] for m in round_metrics[0]]
    for idx, name in enumerate(names):
        per_round = [round_metrics[r][idx] for r in range(len(round_metrics))]
        agg[name] = _agg_dict(per_round)
    return agg


def squash_to_means(tree: dict) -> dict:
    """把 {min,max,mean,stddev} 叶子递归压成 mean,生成与旧版 schema 一致的均值视图。"""
    out = {}
    for k, v in tree.items():
        if isinstance(v, dict) and "mean" in v and set(v.keys()) == {"min", "max", "mean", "stddev"}:
            out[k] = v["mean"]
        elif isinstance(v, dict):
            out[k] = squash_to_means(v)
        else:
            out[k] = v
    return out


# ── 客户端:raw socket,精确测 TTFB(读到状态行) ────────────────

def _status_code(status_line: bytes) -> str:
    try:
        parts = status_line.split(b' ')
        code = parts[1].decode('latin-1').strip()
        return code if code.isdigit() else "000"
    except Exception:
        return "000"


def _host_of(url: bytes) -> bytes:
    try:
        s = url.decode('latin-1')
        after = s.split('://', 1)[1] if '://' in s else s
        return after.split('/', 1)[0].encode('latin-1')
    except Exception:
        return b'example.com'


async def _read_headers(reader, timeout: float):
    """跳过响应头,直到空行。返回后 reader 停留在 body 起始处。"""
    while True:
        h = await asyncio.wait_for(reader.readline(), timeout=timeout)
        if not h or h in (b"\r\n", b"\n"):
            return


async def do_http_request(host: str, port: int, url: bytes, timeout: float = 15.0,
                          success_any_status: bool = False,
                          capture_body: bool = False) -> RequestResult:
    """发一个 GET,返回 TTFB(读到状态行)与 total。失败时分类:timeout / conn / http:<..>。

    capture_body=True 时把响应 body 存进 RequestResult.body,供 mock 模式正确性校验
    (body 大小/内容,缓存命中字节一致)。real 模式 body 不可预测,不校验。
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
        await _read_headers(reader, timeout)
        body = await asyncio.wait_for(reader.read(-1), timeout=timeout)
        total = time.monotonic() - t0
        return RequestResult(True, ttfb, total, "", code,
                             body=body if capture_body else b"")
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

    echo_check=True(mock):建隧道后发 payload,校验上游原样回显。
    echo_check=False(real):收到 200 即成功,不校验回显(真实 TLS 加密无法校验)。
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
        await _read_headers(reader, timeout)
        if not echo_check:
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


# ── 资源采样(客户端进程) + 服务端计数器拉取 ────────────────────

def rss_mb() -> float:
    try:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except Exception:
        return 0.0


def fd_count() -> int:
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return 0


async def sample_resources(metrics_base: str, result: ScenarioResult, stop_evt: asyncio.Event,
                           interval: float = 1.0):
    """周期采样客户端 RSS/fd + 拉取子进程 /server-stats(服务端 CPU/loop-lag)。

    metrics_base: 子进程管理 API 的 base URL(如 http://127.0.0.1:port)。
    """
    async with httpx.AsyncClient(timeout=2.0) as client:
        while not stop_evt.is_set():
            try:
                result.rss_peak_mb = max(result.rss_peak_mb, rss_mb())
                result.fd_peak = max(result.fd_peak, fd_count())
            except Exception:
                pass
            try:
                r = await client.get(f"{metrics_base}/server-stats")
                if r.status_code == 200:
                    result.server_stats = r.json()
            except Exception:
                pass
            try:
                await asyncio.wait_for(stop_evt.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass


async def fetch_counters(metrics_base: str) -> Optional[dict]:
    """从子进程 /metrics 拉服务端计数器快照。失败返回 None。"""
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{metrics_base}/metrics")
            if r.status_code == 200:
                return r.json().get("counters")
    except Exception:
        pass
    return None


# ── 负载模式 ──────────────────────────────────────────────────

async def run_concurrent(make_request, total: int, concurrency: int,
                         progress_interval: float = 0.0) -> list:
    """以固定并发数跑 total 个请求(阶梯/混合/连接复用用)。返回结果列表。

    progress_interval > 0 时,按进度里程碑打印完成数(混合等长跑场景用,避免
    全程无输出)。打印按时间节流:同一秒内只打一次,避免快速跑刷屏。默认 0 关闭。
    """
    sem = asyncio.Semaphore(concurrency)
    results: list = []
    done = 0
    last_print = -1.0
    t0 = time.monotonic()
    milestone = max(1, total // 10)  # 每 10% 一档

    async def one(i):
        async with sem:
            return await make_request(i)

    tasks = [asyncio.create_task(one(i)) for i in range(total)]
    for t in asyncio.as_completed(tasks):
        results.append(await t)
        done += 1
        if progress_interval:
            now = time.monotonic() - t0
            if done % milestone == 0 and now - last_print >= 1.0:
                print(f"  t={now:.0f}s  done={done}/{total}  ({done*100//total}%)")
                last_print = now
    return results


async def run_rate(make_request, target_rps: float, duration: float) -> tuple[list, int]:
    """以固定速率 target_rps 持续 duration 秒发请求。返回 (结果列表, 注入数)。

    注入数 = 实际创建的 task 数;与完成数(=len(results))的差 = 被丢弃/超时。
    soak 据此区分"主动限速"与"被动撑不住"。
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
    if pending:
        try:
            await asyncio.wait_for(asyncio.gather(*pending, return_exceptions=True), timeout=30)
        except asyncio.TimeoutError:
            for t in pending:
                t.cancel()
    return results, i


async def run_open_loop(make_request, concurrency: int, stop_evt: asyncio.Event) -> list:
    """开环:固定并发 worker 持续拉请求,直到 stop_evt 置位。

    与 run_concurrent 不同,不预先创建 total 个 task(避免 10**9 这类总量爆炸),
    而是起 concurrency 个 worker 各自循环 await make_request(i),共享一个递增计数器。
    stop_evt 置位后 worker 退出,收集所有结果。用于开环 soak 与连接复用场景。
    """
    results: list = []
    counter = 0

    async def worker():
        nonlocal counter
        while not stop_evt.is_set():
            i = counter
            counter += 1
            try:
                r = await make_request(i)
                results.append(r)
            except Exception:
                pass

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    await stop_evt.wait()
    for w in workers:
        w.cancel()
    await asyncio.gather(*workers, return_exceptions=True)
    return results


# ── 正确性校验(mock 模式) ─────────────────────────────────────

def _check_correctness(result: ScenarioResult, host_kind: str, url: bytes):
    """对单个 mock 请求的 body 做正确性校验,失败记入 result.correctness_failures。

    host_kind: "hot"/"cold"/"big"/"chunked"(决定预期 body 大小)。
    mock body = b"x" * size(见 mock_upstream._handle),故校验大小与内容。
    big/chunked 的 body 可能超 STREAM_CACHE_LIMIT 不被代理缓冲,但仍应完整转发到客户端。
    """
    expected = _MOCK_EXPECTED_BODY_BYTES.get(host_kind)
    if expected is None:
        return
    # 找最近一条成功结果(调用方在 make_request 后立即校验,取 results 末尾)。
    if not result.results or not result.results[-1].ok:
        return
    body = result.results[-1].body
    if len(body) != expected:
        result.correctness_failures.append(
            f"{host_kind} {url.decode('latin-1','replace')}: size {len(body)} != {expected}")
        return
    if body != b"x" * expected:
        result.correctness_failures.append(
            f"{host_kind} {url.decode('latin-1','replace')}: content mismatch")


# ── 场景通用收尾与共享逻辑 ─────────────────────────────────────

def _check_body_sizes(result: ScenarioResult):
    """粗粒度正确性:所有成功响应的 body 大小必须匹配已知的 mock 预期大小。

    抓截断/错乱回归;不做逐字节比对(mock body = b"x"*size,大小已能区分)。
    """
    result.correctness_checked = True
    for r in result.results:
        if r.ok and r.body:
            if len(r.body) not in _MOCK_EXPECTED_BODY_BYTES.values():
                result.correctness_failures.append(f"unexpected body size {len(r.body)}")


async def _finish_scenario(result: ScenarioResult, metrics_base: str,
                           stop_evt: asyncio.Event, sampler: asyncio.Task):
    """场景末尾统一收尾:拉结束计数器 → 停采样器。当前场景耗时已写入 result.duration。"""
    result.counters_after = await fetch_counters(metrics_base) or {}
    if not result.counters_after:
        result.counter_fetch_failed = True
    stop_evt.set()
    await sampler


async def _run_open_loop(make_request, concurrency: int, duration: float) -> list:
    """固定并发 worker 持续跑满 duration 秒后停止,返回全部结果。"""
    loop_stop = asyncio.Event()

    async def _stopper():
        await asyncio.sleep(duration)
        loop_stop.set()
    asyncio.create_task(_stopper())
    return await run_open_loop(make_request, concurrency, loop_stop)


# ── 场景定义 ──────────────────────────────────────────────────

async def scenario_staircase(router_host: str, router_port: int, host_set: HostSet,
                             quick: bool, metrics_base: str) -> ScenarioResult:
    """并发阶梯:并发数 1→N,每级固定请求数,测吞吐与延迟随并发的变化,找饱和点。"""
    levels = [1, 10, 50, 100, 200, 400, 800] if not quick else [1, 10, 50, 100]
    per_level = 200 if not quick else 50
    result = ScenarioResult(name="staircase", duration=0.0, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(metrics_base, result, stop_evt))
    capture = (host_set.mode == "mock")  # mock 模式校验 body

    async def make(i):
        if i % 10 < 7:
            url = f"http://{host_set.cold_or_hot(i)}/p{i % 4}".encode()
        else:
            url = f"http://{host_set.cold(i)}/p{i % 3}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status,
                                     capture_body=capture)

    result.counters_before = await fetch_counters(metrics_base) or {}
    t_start = time.monotonic()
    for c in levels:
        # 预热:每级并发前发 per_level//5 个请求填缓存,不计入统计。
        warmup_n = max(1, per_level // 5)
        await run_concurrent(make, warmup_n, c)
        result.warmup_requests += warmup_n
        res = await run_concurrent(make, per_level, c)
        result.results.extend(res)
        result.client_requests += len(res)
        ok = sum(1 for r in res if r.ok)
        print(f"  concurrency={c:>4}  ok={ok}/{len(res)}  "
              f"rps={ok / (sum(r.total for r in res) or 1):.0f}(wall)")
    result.duration = time.monotonic() - t_start
    # mock 正确性:校验所有成功结果的 body 大小(粗粒度,抓截断/错乱回归)。
    if capture:
        _check_body_sizes(result)
    await _finish_scenario(result, metrics_base, stop_evt, sampler)
    return result


async def scenario_rate(router_host: str, router_port: int, host_set: HostSet,
                        quick: bool, metrics_base: str) -> ScenarioResult:
    """恒定速率:目标 RPS 阶梯上升,测延迟/错误率随负载的变化,找容量上限。"""
    rates = [100, 500, 1000, 2000] if not quick else [100, 500]
    per_rate_dur = 8.0 if not quick else 3.0
    result = ScenarioResult(name="rate", duration=0.0, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(metrics_base, result, stop_evt))

    async def make(i):
        if i % 10 < 7:
            url = f"http://{host_set.cold_or_hot(i)}/p{i % 4}".encode()
        else:
            url = f"http://{host_set.cold(i)}/p{i % 3}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status)

    result.counters_before = await fetch_counters(metrics_base) or {}
    t_start = time.monotonic()
    for rps in rates:
        res, injected = await run_rate(make, rps, per_rate_dur)
        result.results.extend(res)
        result.client_requests += len(res)
        result.injected_requests += injected
        ok = sum(1 for r in res if r.ok)
        print(f"  target_rps={rps:>5}  ok={ok}  actual_rps={ok/per_rate_dur:.0f}  "
              f"injected={injected}  err={len(res)-ok}")
    result.duration = time.monotonic() - t_start
    await _finish_scenario(result, metrics_base, stop_evt, sampler)
    return result


async def scenario_mixed(router_host: str, router_port: int, host_set: HostSet,
                         quick: bool, metrics_base: str) -> ScenarioResult:
    """混合负载:冷热域名 + 大小响应 + CONNECT 隧道,贴近真实流量结构。"""
    total = 2000 if not quick else 300
    concurrency = 100 if not quick else 30
    result = ScenarioResult(name="mixed", duration=0.0, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(metrics_base, result, stop_evt))
    echo_check = (host_set.mode == "mock")
    sas = host_set.success_any_status
    capture = (host_set.mode == "mock")

    async def make(i):
        r = i % 10
        if r < 3:
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.hot(i)}/p{i % 4}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas,
                                         capture_body=capture)
        elif r < 5:
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.big(i)}/p{i % 2}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas,
                                         capture_body=capture)
        elif r < 7:
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.chunked(i)}/p{i % 2}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas,
                                         capture_body=capture)
        elif r < 9:
            return await do_http_request(router_host, router_port,
                                         f"http://{host_set.cold(i)}/p{i % 5}".encode(),
                                         timeout=host_set.timeout, success_any_status=sas,
                                         capture_body=capture)
        else:
            return await do_connect_request(router_host, router_port,
                                            host_set.connect().encode(),
                                            timeout=host_set.timeout, echo_check=echo_check)

    # 预热:mock 50 个 / real 20 个(避免 hammer 真实源站)。
    warmup_n = 50 if host_set.mode == "mock" else 20
    await run_concurrent(make, warmup_n, min(concurrency, 20))
    result.warmup_requests += warmup_n

    result.counters_before = await fetch_counters(metrics_base) or {}
    t_start = time.monotonic()
    # 混合是长跑场景,周期打进度避免全程无输出。
    res = await run_concurrent(make, total, concurrency, progress_interval=2.0)
    result.results = res
    result.client_requests = len(res)
    result.injected_requests = len(res)
    result.duration = time.monotonic() - t_start
    # mock 正确性:校验所有成功 GET 的 body 大小。
    if capture:
        _check_body_sizes(result)
    await _finish_scenario(result, metrics_base, stop_evt, sampler)
    return result


async def scenario_soak(router_host: str, router_port: int, host_set: HostSet,
                        duration: float, quick: bool, metrics_base: str,
                        open_loop: bool = False) -> ScenarioResult:
    """长时稳定性:固定速率(或开环固定并发)持续跑,抓内存/fd/缓存泄漏与 CPU 趋势。

    open_loop=False(默认):run_rate(target_rps),限速;报告区分注入/完成速率。
    open_loop=True:run_concurrent(固定并发,不限速),让 Router 尽力跑,测真实上限。
    """
    duration = duration if not quick else min(duration, 20.0)
    concurrency = 100
    target_rps = 300
    result = ScenarioResult(name="soak", duration=duration, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(metrics_base, result, stop_evt, interval=2.0))

    async def make(i):
        if i % 10 < 7:
            url = f"http://{host_set.cold_or_hot(i)}/p{i % 4}".encode()
        else:
            url = f"http://{host_set.cold(i)}/p{i % 5}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status)

    result.counters_before = await fetch_counters(metrics_base) or {}
    t_start = time.monotonic()
    last_print = t_start
    if open_loop:
        # 开环:固定并发 worker 持续跑满 duration,测真实容量上限。
        res = await _run_open_loop(make, concurrency, duration)
        result.results = res
        result.client_requests = len(res)
        result.injected_requests = len(res)
    else:
        while time.monotonic() - t_start < duration:
            remain = duration - (time.monotonic() - t_start)
            chunk = min(remain, 2.0)
            res, injected = await run_rate(make, target_rps, chunk)
            result.results.extend(res)
            result.client_requests += len(res)
            result.injected_requests += injected
            if time.monotonic() - last_print > 5.0:
                print(f"  t={time.monotonic()-t_start:.0f}s  rss={rss_mb():.0f}MB  "
                      f"fd={fd_count()}  done={result.client_requests}")
                last_print = time.monotonic()
    result.duration = time.monotonic() - t_start
    await _finish_scenario(result, metrics_base, stop_evt, sampler)
    return result


async def scenario_conn_reuse(router_host: str, router_port: int, host_set: HostSet,
                              metrics_base: str) -> ScenarioResult:
    """连接复用:固定小并发、长时、同域名(命中域名缓存→单发上游→复用连接池)。

    报告 reuse_ratio = (请求 - 新建连接) / 请求(mock 上游 new_conn_count 计数)。
    >0 表示 keepalive 复用生效。仅 mock 模式有意义(real 无法读上游新连接数)。
    """
    duration = 20.0
    concurrency = 10
    result = ScenarioResult(name="conn-reuse", duration=duration, client_requests=0)
    stop_evt = asyncio.Event()
    sampler = asyncio.create_task(sample_resources(metrics_base, result, stop_evt, interval=2.0))

    async def make(i):
        # 同一热域名反复请求 → 域名缓存命中 → 单发同一上游 → 复用 keepalive 连接。
        # 注入坏代理时(has_dead)改用冷域名强制竞速,让死代理被尝试以喂熔断计数。
        url = f"http://{host_set.cold_or_hot(0)}/p{i % 4}".encode()
        return await do_http_request(router_host, router_port, url,
                                     timeout=host_set.timeout,
                                     success_any_status=host_set.success_any_status)

    result.counters_before = await fetch_counters(metrics_base) or {}
    t_start = time.monotonic()
    # 开环:固定并发 worker 跑满 duration,命中缓存 → 单发上游 → 复用连接池。
    res = await _run_open_loop(make, concurrency, duration)
    result.results = res
    result.client_requests = len(res)
    result.injected_requests = len(res)
    result.duration = time.monotonic() - t_start
    await _finish_scenario(result, metrics_base, stop_evt, sampler)
    # 从子进程 /server-stats 取 mock 上游新建连接数(连接复用率 = (请求-新连接)/请求)。
    mock = (result.server_stats or {}).get("mock") or {}
    result.upstream_new_conns = mock.get("new_conns", 0)
    return result


# ── 报告输出 ──────────────────────────────────────────────────

def _fmt_ms(v: float) -> str:
    return f"{v:.1f}"


def _fmt_rps(v: float) -> str:
    return f"{v:.0f}"


def _agg_val(stats_dict: dict, key: str):
    """从 {min,max,mean,stddev} 聚合叶取 mean(rounds==1 时为原始标量)。"""
    v = stats_dict.get(key)
    return v["mean"] if isinstance(v, dict) and "mean" in v else v


def print_report(metrics_list: list, git_ver: str, upstream_mode: str,
                 rounds: int = 1, round_results: list | None = None,
                 aggregates: dict | None = None):
    """输出压测报告。

    rounds==1:维持原有详细格式(零行为变化)。
    rounds>1:每个场景先打每轮紧凑表 + 均值±stddev 行,再打 round 0 的详细块。
    """
    print("\n" + "=" * 78)
    hdr = f" auto_squid 压测报告  (git: {git_ver})  [上游: {upstream_mode}]  [进程隔离]"
    if rounds > 1:
        hdr += f"  [rounds={rounds}]"
    print(hdr)
    print("=" * 78)
    for i, m in enumerate(metrics_list):
        print(f"\n■ 场景: {m['name']}")
        if rounds > 1:
            # 每轮紧凑表(数据来自各轮原始报告)。
            rows = []
            for r_i, rr in enumerate(round_results or []):
                mm = rr["scenarios"][i]
                rq = mm['requests']
                tp = mm['throughput']
                lat = mm['latency']
                cache = mm['cache']
                hit = (cache['http_hit_rate'] * 100) if cache['http_hit_rate'] is not None else float('nan')
                err = rq['errors'] / rq['client'] if rq['client'] else 0.0
                rows.append(
                    f"[{r_i+1}/{rounds}]  {_fmt_rps(tp['completed_rps']):>6}   {_fmt_rps(tp['injected_rps']):>6}   "
                    f"{_fmt_ms(lat['ttfb_ms']['p50']):>8}   {_fmt_ms(lat['ttfb_ms']['p95']):>8}   "
                    f"{_fmt_ms(lat['ttfb_ms']['p99']):>8}  {err*100:5.2f}  {hit:6.1f}")
            print("  " + "轮次   完成rps  注入rps  TTFB-p50  TTFB-p95  TTFB-p99  err%   缓存%")
            for row in rows:
                print("  " + row)
            # 均值±stddev 行(读 aggregates)。
            a = (aggregates or {}).get(m['name'], {})
            tp_a = a.get('throughput', {})
            lat_a = a.get('latency', {})
            err_a = a.get('errors', {})
            cache_a = a.get('cache', {})
            hrate = _agg_val(cache_a.get('http_hit_rate', {}), 'mean')
            hit_mean = (hrate * 100) if hrate is not None else float('nan')
            comp = _agg_val(tp_a.get('completed_rps', {}), 'mean')
            comp_sd = _agg_val(tp_a.get('completed_rps', {}), 'stddev')
            tfb = lat_a.get('ttfb_ms', {})
            p50m = _agg_val(tfb.get('p50', {}), 'mean'); p50s = _agg_val(tfb.get('p50', {}), 'stddev')
            p95m = _agg_val(tfb.get('p95', {}), 'mean'); p95s = _agg_val(tfb.get('p95', {}), 'stddev')
            p99m = _agg_val(tfb.get('p99', {}), 'mean'); p99s = _agg_val(tfb.get('p99', {}), 'stddev')
            errm = _agg_val(err_a.get('rate', {}), 'mean') * 100
            hsd = _agg_val(cache_a.get('http_hit_rate', {}), 'stddev') * 100
            print(f"  均值          {comp:.0f}±{comp_sd:.1f} req/s  "
                  f"TTFB p50 {p50m:.1f}±{p50s:.1f}  p95 {p95m:.1f}±{p95s:.1f}  "
                  f"p99 {p99m:.1f}±{p99s:.1f}  err {errm:.2f}%  缓存 {hit_mean:.1f}%±{hsd:.1f}")
            # 详细块(round 0 的丰富字段)。
            m = round_results[0]["scenarios"][i]
        rq = m['requests']
        print(f"  请求          : 客户端 {rq['client']} (成功 {rq['success']}, 失败 {rq['errors']}, "
              f"注入 {rq['injected']}, 预热 {rq['warmup']})")
        if m['errors']['breakdown']:
            print(f"  错误分类      : {m['errors']['breakdown']}  (错误率 {m['errors']['rate']*100:.2f}%)")
        if m.get('status_distribution'):
            print(f"  状态码分布    : {m['status_distribution']}")
        tp = m['throughput']
        print(f"  吞吐          : 完成 {tp['completed_rps']:.1f} req/s  注入 {tp['injected_rps']:.1f} req/s")
        lat = m['latency']
        print(f"  TTFB (ms)     : P50={lat['ttfb_ms']['p50']:.1f}  P95={lat['ttfb_ms']['p95']:.1f}  "
              f"P99={lat['ttfb_ms']['p99']:.1f}  mean={lat['ttfb_ms']['mean']:.1f}")
        print(f"  Total (ms)    : P50={lat['total_ms']['p50']:.1f}  P95={lat['total_ms']['p95']:.1f}  "
              f"P99={lat['total_ms']['p99']:.1f}  mean={lat['total_ms']['mean']:.1f}")
        c = m['cache']
        if c['http_hit_rate'] is not None:
            print(f"  缓存          : HTTP命中 {c['http_hit_rate']*100:.1f}%  域名命中 {c['domain_hit_rate']*100:.1f}%  "
                  f"(hits={c['http_hits']} misses={c['http_misses']} 条目末值={c['http_cache_entries_end']})")
        else:
            print(f"  缓存          : N/A (计数器拉取失败)")
        r = m['racing']
        if r['amplification'] is not None:
            print(f"  竞速          : 放大率 {r['amplification']:.2f}x  上游尝试 {r['upstream_attempts']}  竞速触发 {r['invocations']}")
        circ = m.get('circuit') or {}
        if circ.get('circuit_open_count') is not None:
            deg = circ.get('single_send_degrades')
            deg_txt = f"  单发降级 {deg} 次" if deg else ""
            print(f"  熔断          : 开合 {circ['circuit_open_count']} 次  探活 {circ['probes_sent']}/{circ['probes_ok']} 成功  "
                  f"末态熔断 {circ['proxies_open_end']} 个代理{deg_txt}")
        infl = m.get('in_flight') or {}
        if infl.get('max_in_flight') is not None:
            ends = infl.get('in_flight_end') or {}
            leaked = {k: v for k, v in ends.items() if v}
            print(f"  在途选批      : 单代理在途峰值 {infl['max_in_flight']}  "
                  f"末态在途 {ends} {'⚠️ 计数未归零!' if leaked else '(归零,无泄漏)'}")
        res = m['resources']
        lag = res['server_loop_lag_ms']
        print(f"  资源          : RSS={res['rss_peak_mb']:.0f}MB  fd={res['fd_peak']}  池末值={res['pool_size_end']}  "
              f"服务端CPU={res['server_cpu_pct']:.0f}%  loop-lag p95={lag['p95']:.2f}ms max={lag['max']:.2f}ms")
        # 连接复用场景:展示 reuse_ratio(mock 上游新建连接数)。
        if m['name'] == 'conn-reuse' and m.get('mock_upstream_new_conns') is not None:
            new_conns = m['mock_upstream_new_conns']
            total = m['requests']['client']
            reuse = (total - new_conns) / total if total else 0.0
            print(f"  连接复用      : 新建连接 {new_conns} / 请求 {total}  reuse_ratio={reuse:.2f} "
                  f"({'keepalive 生效' if reuse > 0 else '未复用'})")
        cor = m['correctness']
        if cor['checked']:
            tag = "✓" if cor['failed'] == 0 else "✗"
            print(f"  正确性        : {tag} 校验 {cor['passed']} 通过 / {cor['failed']} 失败")
        else:
            print(f"  正确性        : 未校验 (real 模式或该场景不捕获 body)")
        print(f"  归因          : 上游触顶={m['attribution']['upstream_throttled']}  瓶颈={m['attribution']['bottleneck']}")
    print("\n" + "=" * 78)


# ── 子进程管理 ────────────────────────────────────────────────

class ServerProcess:
    """管理被测方子进程:启动、读 READY 握手、终止。

    子进程(bench.server_proc)在独立事件循环跑 Router + mock + 管理 API。
    主进程经 metrics_base(http://127.0.0.1:<metrics_port>)拉 /metrics /server-stats。
    """

    def __init__(self, config: dict):
        self.config = config
        self.proc: Optional[subprocess.Popen] = None
        self.router_port: int = 0
        self.metrics_port: int = 0
        self.metrics_base: str = ""
        self._config_path: Optional[str] = None

    async def start(self, timeout: float = 15.0):
        """启动子进程并等待 READY 握手。超时或失败抛异常。"""
        # 配置写临时文件,子进程命令行 --config 读。
        fd, self._config_path = tempfile.mkstemp(suffix=".json")
        with os.fdopen(fd, "w") as f:
            json.dump(self.config, f)
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "bench.server_proc", "--config", self._config_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # 读 stdout 第一行拿 READY <router_port> <metrics_port>。
        loop = asyncio.get_event_loop()
        try:
            line = await asyncio.wait_for(
                loop.run_in_executor(None, self.proc.stdout.readline), timeout=timeout)
        except asyncio.TimeoutError:
            self._kill()
            raise RuntimeError("子进程 READY 握手超时")
        line = line.strip()
        if not line.startswith("READY"):
            stderr = ""
            try:
                stderr = self.proc.stderr.read() or ""
            except Exception:
                pass
            self._kill()
            raise RuntimeError(f"子进程未就绪: {line!r} stderr={stderr[:500]}")
        _, rp, mp = line.split()
        self.router_port = int(rp)
        self.metrics_port = int(mp)
        self.metrics_base = f"http://127.0.0.1:{self.metrics_port}"

    def _kill(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()

    def stop(self):
        """优雅终止:SIGTERM → 等 STOPPED/退出 → 超时 SIGKILL。清理临时配置。"""
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=5)
            except Exception:
                self.proc.kill()
        if self._config_path:
            try:
                os.unlink(self._config_path)
            except Exception:
                pass


# ── 主入口 ────────────────────────────────────────────────────

async def run_scenarios(args, host_set, router_host, router_port, metrics_base,
                        rnd: int = 1, total_rounds: int = 1) -> list:
    """在一轮的全新子进程上跑(args.mode 指定的)场景,返回 ScenarioResult 列表。

    rnd/total_rounds 仅用于进度标题,标识当前轮次。
    `all` 依次跑 staircase/rate/mixed/soak(不含 conn-reuse)。
    """
    tag = f"[r{rnd}/{total_rounds}] " if total_rounds > 1 else ""
    results: list = []

    # 需透传 quick/metrics_base 的常规场景:进度标题 + 场景函数。
    # soak 需透传 duration/open_loop、conn-reuse 不收 quick,均单独处理。
    _TITLES = {
        "staircase": ("[staircase] 并发阶梯,测饱和点", scenario_staircase),
        "rate": ("[rate] 恒定速率阶梯,测容量上限", scenario_rate),
        "mixed": ("[mixed] 混合负载(冷热+大小+CONNECT)", scenario_mixed),
    }

    async def _run_quick(title: str, fn):
        print(f"\n{tag}{title}")
        results.append(await fn(router_host, router_port, host_set, args.quick, metrics_base))

    if args.mode == "soak":
        print(f"\n{tag}[soak] 长时稳定性 {args.duration}s" + (" [开环]" if args.open_loop else ""))
        results.append(await scenario_soak(router_host, router_port, host_set, args.duration,
                                           args.quick, metrics_base, open_loop=args.open_loop))
    elif args.mode == "all":
        await _run_quick("[staircase] 并发阶梯", scenario_staircase)
        await _run_quick("[rate] 恒定速率", scenario_rate)
        await _run_quick("[mixed] 混合负载", scenario_mixed)
        print(f"\n{tag}[soak] 长时 {args.duration}s")
        results.append(await scenario_soak(router_host, router_port, host_set, args.duration,
                                           args.quick, metrics_base))
    elif args.mode == "conn-reuse":
        print(f"\n{tag}[conn-reuse] 连接复用率(测 keepalive)")
        results.append(await scenario_conn_reuse(router_host, router_port, host_set, metrics_base))
    else:
        title, fn = _TITLES[args.mode]
        await _run_quick(title, fn)
    return results


async def amain(args):
    git_ver = git_version()
    args.rounds = max(1, args.rounds)
    print(f"auto_squid 压测  git={git_ver}  mode={args.mode}  upstream={args.upstream}  "
          f"quick={args.quick}  open_loop={args.open_loop}  rounds={args.rounds}  [进程隔离]")
    real_hosts = args.real_hosts.split(",") if args.real_hosts else list(_DEFAULT_REAL_HOSTS)
    host_set = HostSet(mode=args.upstream, real_hosts=[h.strip() for h in real_hosts if h.strip()],
                       has_dead=bool(getattr(args, 'dead_proxies', []) or getattr(args, 'dead_proxy', [])))
    # mock 规格每轮一致 → 相同条件;提升到循环外只算一次。
    mock_specs = default_mock_specs(args.quick) if args.upstream == "mock" else []
    run_start = time.strftime("%Y-%m-%dT%H:%M:%S")

    round_reports: list = []
    for rnd in range(1, args.rounds + 1):
        if args.rounds > 1:
            print(f"\n{'='*70}\n[round {rnd}/{args.rounds}]  每轮全新子进程/SQLite/缓存\n{'='*70}")

        # 每轮全新子进程配置:新 db、新 metrics 端口、其余条件一致。
        db_path = tempfile.mktemp(suffix=".db")
        config = {
            "upstream": args.upstream,
            "router_port": args.router_port,
            "metrics_port": 0,  # 0 = uvicorn 随机选端口(实际由子进程绑定后回传 READY)
            "max_retries": args.max_retries,
            "cache_ttl": args.cache_ttl,
            "enable_http_cache": not args.no_http_cache,
            "stagger_start": not args.no_stagger,
            "stagger_initial": args.stagger_initial,
            "stagger_interval_ms": args.stagger_interval_ms,
            "probe_interval_sec": args.probe_interval_sec,
            "probe_canary": args.probe_canary,
            "circuit_threshold": args.circuit_threshold,
            "circuit_max_backoff": args.circuit_max_backoff,
            "slow_start_window": args.slow_start_window,
            "slow_start_success": args.slow_start_success,
            "lb_bias": args.lb_bias,
            "single_send_degrade_fail": getattr(args, 'single_send_degrade_fail', 0),
            "single_send_degrade_ratio": getattr(args, 'single_send_degrade_ratio', 0.0),
            "single_send_degrade_slack_ms": getattr(args, 'single_send_degrade_slack_ms', 10.0),
            "dead_proxies": getattr(args, 'dead_proxies', []) or getattr(args, 'dead_proxy', []),
            "proxies_path": args.proxies,
            "mock_specs": mock_specs,
            "db_path": db_path,
        }
        # metrics_port=0 时 uvicorn 绑定随机端口;但子进程需先知道端口才能 print READY。
        # 主进程预选一个空闲端口,避免子进程内探测端口的复杂度。
        config["metrics_port"] = _free_port()

        server = ServerProcess(config)
        try:
            await server.start()
            print(f"[round {rnd}] 子进程就绪: Router 127.0.0.1:{server.router_port}  "
                  f"管理API {server.metrics_base}  max_retries={args.max_retries}  "
                  f"cache_ttl={args.cache_ttl}  http_cache={'on' if not args.no_http_cache else 'off'}")

            rh, rp, mb = "127.0.0.1", server.router_port, server.metrics_base
            pr = None
            if args.profile and rnd == 1:
                # profile 只覆盖第 1 轮客户端进程(各轮客户端逻辑相同,避免噪声)。
                pr = cProfile.Profile()
                pr.enable()
                if args.rounds > 1:
                    print("  [profile] 仅覆盖第 1 轮客户端进程")
            results = await run_scenarios(args, host_set, rh, rp, mb, rnd, args.rounds)
            if pr is not None:
                pr.disable()
                s_io = io.StringIO()
                pstats.Stats(pr, stream=s_io).sort_stats("cumulative").print_stats(25)
                print("\n[cProfile top 25 by cumulative (客户端进程)]")
                print(s_io.getvalue())
                with open("bench_profile.txt", "w") as f:
                    pstats.Stats(pr, stream=f).sort_stats("cumulative").print_stats(40)

            metrics_list = [r.metrics() for r in results]
            round_reports.append({
                "git": git_ver, "mode": args.mode, "upstream": args.upstream,
                "quick": args.quick, "max_retries": args.max_retries, "cache_ttl": args.cache_ttl,
                "http_cache_enabled": not args.no_http_cache, "open_loop": args.open_loop,
                "isolated_process": True,
                "timestamp": run_start,
                "scenarios": metrics_list,
            })
        finally:
            server.stop()
        if rnd < args.rounds:
            await asyncio.sleep(0.5)  # 防端口 TIME_WAIT 冲突的便宜保险

    # 汇总:rounds==1 输出与旧版字节兼容;rounds>1 增补 round_results/aggregates。
    final = dict(round_reports[0])
    if args.rounds > 1:
        final["rounds"] = args.rounds
        round_metrics = [r["scenarios"] for r in round_reports]
        agg = aggregate_scenarios(round_metrics)
        final["scenarios"] = [squash_to_means(sc) for sc in agg.values()]
        final["round_results"] = round_reports
        final["aggregates"] = agg
        print_report(final["scenarios"], git_ver, args.upstream, rounds=args.rounds,
                     round_results=round_reports, aggregates=agg)
    else:
        print_report(final["scenarios"], git_ver, args.upstream)

    with open(args.output, "w") as f:
        json.dump(final, f, indent=2, ensure_ascii=False)
    print(f"\n结构化报告已写入: {args.output}")


def main():
    p = argparse.ArgumentParser(description="auto_squid 性能压测(进程隔离版)")
    p.add_argument("--mode", choices=["staircase", "rate", "mixed", "soak", "conn-reuse", "all"],
                   default="staircase", help="压测模式")
    p.add_argument("--upstream", choices=["mock", "real"], default="mock",
                   help="上游:mock(受控本地) 或 real(真实 proxies.yaml)")
    p.add_argument("--proxies", default="proxies.yaml", help="真实模式下的代理列表文件")
    p.add_argument("--real-hosts", default="",
                   help="真实模式下压测的主机名(逗号分隔);默认内置大站池")
    p.add_argument("--router-port", type=int, default=10820, help="被测 Router 监听端口")
    p.add_argument("--max-retries", type=int, default=3, help="竞速首批并行数")
    p.add_argument("--stagger-initial", type=int, default=1, help="错峰首批并发数(默认1,冷启动自动翻倍)")
    p.add_argument("--stagger-interval-ms", type=int, default=250, help="错峰启动间隔(毫秒,默认250)")
    p.add_argument("--cache-ttl", type=int, default=300, help="域名缓存 TTL(秒)")
    p.add_argument("--no-http-cache", action="store_true", help="禁用 HTTP 响应缓存")
    p.add_argument("--no-stagger", action="store_true",
                   help="禁用错峰启动(同时全发;默认启用错峰,RFC 8305 §5)")
    p.add_argument("--probe-interval-sec", type=float, default=0.0,
                   help="后台探活周期(秒);默认 0=关闭(压测隔离探活层)")
    p.add_argument("--probe-canary", default="1.1.1.1:443", help="探活目标 host:port")
    p.add_argument("--circuit-threshold", type=int, default=3, help="连续失败熔断阈值")
    p.add_argument("--circuit-max-backoff", type=float, default=300.0, help="熔断退避上限(秒)")
    p.add_argument("--slow-start-window", type=float, default=60.0, help="slow-start 爬升窗口(秒)")
    p.add_argument("--slow-start-success", type=int, default=3, help="slow-start 成功几次后恢复完整权重")
    p.add_argument("--lb-bias", type=float, default=1.0,
                   help="加权 least-request 在途惩罚指数(竞速排序权重 = ewma×(1+active)^bias;"
                        "0=纯 EWMA 排序,默认 1.0)")
    p.add_argument("--single-send-degrade-fail", type=int, default=0,
                   help="单发降级:连续失败阈值(默认 0=关闭;建议 circuit_threshold-1)")
    p.add_argument("--single-send-degrade-ratio", type=float, default=0.0,
                   help="单发降级:EWMA 恶化比值阈值(默认 0=关闭;如 3.0=延迟恶化3倍)")
    p.add_argument("--single-send-degrade-slack-ms", type=float, default=10.0,
                   help="EWMA 降级绝对下限毫秒(默认 10,防极低延迟误判)")
    p.add_argument("--dead-proxy", action="append", default=[],
                   metavar="ID:HOST:PORT",
                   help="注入指向死端口的代理(可重复),给熔断器制造连续失败负载;"
                        "如 --dead-proxy dead:127.0.0.1:31990")
    p.add_argument("--duration", type=float, default=60.0, help="soak 模式时长(秒)")
    p.add_argument("--open-loop", action="store_true", help="soak 开环模式(不限速,测真实上限)")
    p.add_argument("--quick", action="store_true", help="快速冒烟(小规模,~10s)")
    p.add_argument("--rounds", type=int, default=3,
                   help="同一条件跑 N 轮(每轮全新子进程/SQLite/缓存),取均值去环境噪声;默认 3")
    p.add_argument("--profile", action="store_true", help="启用 cProfile(仅客户端进程)")
    p.add_argument("--output", default="bench_report.json", help="JSON 报告输出路径")
    args = p.parse_args()
    try:
        asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n中断")


if __name__ == "__main__":
    main()
