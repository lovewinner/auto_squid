"""压测被测方子进程入口:独立事件循环内启动 Router(+ mock 上游)与管理 API。

本模块是进程隔离压测的服务端进程。主进程 `bench.stress` 用 subprocess 启动它,
二者**不共享事件循环**——主进程只跑压测客户端,本进程跑被测的 Router,从根上
消除"客户端与服务端同循环争抢"导致的测量污染。

生命周期与握手:
- 启动:读 JSON 配置 → 建 mock 集群(mock 模式)/ 加载 proxies.yaml(real)→
  建 Router → 起 uvicorn(复用 `auto_squid.api:app`,暴露 /metrics /server-stats)→
  往 stdout 打印 `READY <router_port> <metrics_port>` → 阻塞服务。
- 主进程读到 READY 行即开始发请求;计数器经 /metrics 跨进程拉取。
- 关闭:收到 SIGTERM → 停 uvicorn → router.stop() → cluster.stop() → 退出。
  5s grace 后主进程会 SIGKILL,故清理须在限内完成。

配置 JSON 字段(由主进程写入临时文件):
- upstream: "mock" | "real"
- router_port: int(被测代理监听端口)
- metrics_port: int(管理 API 监听端口,主进程据此拉 /metrics)
- max_retries / cache_ttl / enable_http_cache: Router 参数
- proxies_path: str(real 模式加载上游代理列表)
- mock_specs: list(mock 模式的集群规格,见 _build_mock_cluster)
- db_path: str(Router 的 SQLite 路径,主进程给 tempfile)
"""

import argparse
import asyncio
import json
import os
import resource
import signal
import sys
import tempfile
import time
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import uvicorn

from auto_squid.api import app as api_app, mount as mount_api
from auto_squid import api as api_mod
from auto_squid.config_schema import ProxyInfo
from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router

from bench.mock_upstream import UpstreamCluster, ResponseProfile


# ── mock 集群重建(规格来自主进程序列化的 default_mock_cluster) ──────

def _build_mock_cluster(mock_specs: list, start_port: int = 31200) -> UpstreamCluster:
    """从 JSON 规格重建 mock 集群(与 stress.default_mock_cluster 同构)。

    mock_specs: [[base_delay, [profile_dict, ...]], ...]
    profile_dict: {host_prefix, first_byte_delay, body_size, chunked, chunk_delay, fail_rate}
    """
    specs = []
    for base_delay, prof_dicts in mock_specs:
        profs = [ResponseProfile(
            p["host_prefix"], p["first_byte_delay"], p["body_size"],
            p["chunked"], p["chunk_delay"], p["fail_rate"]) for p in prof_dicts]
        specs.append((base_delay, profs))
    return UpstreamCluster(specs, start_port=start_port)


# ── 服务端资源采样:CPU + 事件循环延迟,写入 /server-stats ──────────

class ServerStatsSampler:
    """周期采样本进程(Router 所在进程)的 CPU 占用与事件循环延迟。

    CPU:用 getrusage 的 ru_utime+ru_stime 增量 / 墙钟增量,反映被测方算力饱和。
    loop-lag:测 asyncio.sleep(0.01) 的实际返回偏差——若 Router 被某同步操作卡住,
    事件循环调度会延迟,lag 上升。这是单线程异步系统瓶颈的最直接信号。
    采样结果写入 api_mod._server_stats,主进程经 /server-stats 拉取。
    """

    def __init__(self, interval: float = 1.0, probe_sleep: float = 0.01,
                 mock_counters_fn=None):
        self.interval = interval
        self.probe_sleep = probe_sleep
        # 可选:返回 mock 上游计数的回调(连接复用场景用),并入 /server-stats 快照。
        self._mock_counters_fn = mock_counters_fn
        self._last_cpu_time = self._cpu_time()
        self._last_wall = time.monotonic()
        self._lag_samples: list[float] = []
        self._cpu_pct_samples: list[float] = []

    @staticmethod
    def _cpu_time() -> float:
        ru = resource.getrusage(resource.RUSAGE_SELF)
        return ru.ru_utime + ru.ru_stime

    def snapshot(self) -> dict:
        """取并清空累计采样,返回当前快照(供 /server-stats 同步读)。"""
        lag = self._lag_samples
        cpu = self._cpu_pct_samples
        self._lag_samples = []
        self._cpu_pct_samples = []
        snap = {
            "cpu_pct": (sum(cpu) / len(cpu)) if cpu else 0.0,
            "loop_lag_ms": {
                "p50": _pct(lag, 0.50), "p95": _pct(lag, 0.95),
                "max": max(lag) if lag else 0.0,
                "samples": len(lag),
            },
        }
        if self._mock_counters_fn is not None:
            try:
                snap["mock"] = self._mock_counters_fn()
            except Exception:
                snap["mock"] = {}
        return snap

    async def run(self, stop_evt: asyncio.Event):
        """周期采样循环,被取消或 stop_evt 置位时退出。"""
        # 首个采样周期只校准基线(不记 CPU%):子进程刚启动时 CPU 增量/极小墙钟
        # 增量会算出 100% 假象。先 sleep 一个周期把基线推到稳态,再开始记。
        try:
            await asyncio.sleep(self.probe_sleep)
            self._last_cpu_time = self._cpu_time()
            self._last_wall = time.monotonic()
            while not stop_evt.is_set():
                # loop-lag 探针:sleep 的实际时长 - 预期 = 调度延迟(秒)。
                t0 = time.monotonic()
                await asyncio.sleep(self.probe_sleep)
                lag_ms = (time.monotonic() - t0 - self.probe_sleep) * 1000.0
                self._lag_samples.append(max(0.0, lag_ms))
                # CPU 占用:自上次采样至今的 CPU 增量 / 墙钟增量。
                now_cpu = self._cpu_time()
                now_wall = time.monotonic()
                dt_cpu = now_cpu - self._last_cpu_time
                dt_wall = now_wall - self._last_wall
                if dt_wall > 0:
                    self._cpu_pct_samples.append(dt_cpu / dt_wall * 100.0)
                self._last_cpu_time = now_cpu
                self._last_wall = now_wall
                # 把当前快照推到 /server-stats(主进程周期拉,这里持续更新最新值)。
                api_mod._server_stats = self.snapshot()
                try:
                    await asyncio.wait_for(stop_evt.wait(), timeout=self.interval)
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass


def _pct(vals: list[float], p: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    k = max(0, min(len(s) - 1, int(len(s) * p)))
    return s[k]


# ── 子进程主流程 ────────────────────────────────────────────────

async def _serve(config: dict):
    """启动 mock 集群 + Router + uvicorn,阻塞至收到停止信号。"""
    cluster: Optional[UpstreamCluster] = None
    server: Optional[uvicorn.Server] = None
    stop_evt = asyncio.Event()

    def _request_stop():
        stop_evt.set()

    # SIGTERM → 优雅停止(主进程 terminate() 触发)。
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            pass  # Windows 无 add_signal_handler,降级靠 KeyboardInterrupt

    try:
        # 1) 上游:mock 模式建集群;real 模式加载 proxies.yaml。
        ps = ProxyStore()
        if config["upstream"] == "mock":
            cluster = _build_mock_cluster(config["mock_specs"], start_port=31200)
            await cluster.start()
            for i, u in enumerate(cluster.upstreams):
                ps.add(ProxyInfo(id=f"u{i}", host=u.host, port=u.port))
        else:
            ps = ProxyStore(config["proxies_path"])

        # 2) Router(被测方)。
        router = Router(ps, listen_host="127.0.0.1", listen_port=config["router_port"],
                        max_retries=config["max_retries"], cache_ttl=config["cache_ttl"],
                        enable_http_cache=config["enable_http_cache"],
                        db_path=config["db_path"])
        await router.start()
        mount_api(ps, router)  # 注入 /metrics /server-stats 等端点

        # 3) 服务端资源采样器(写 api_mod._server_stats)。
        # mock 模式附带上游计数(hits/new_conns),供连接复用场景经 /server-stats 读。
        mock_fn = (lambda: {"hits": cluster.total_hits(),
                            "new_conns": cluster.total_new_conns()}
                   if cluster is not None else None)
        sampler = ServerStatsSampler(mock_counters_fn=mock_fn)
        sampler_task = asyncio.create_task(sampler.run(stop_evt))

        # 4) 管理 API(uvicorn),监听主进程指定的 metrics_port。
        uv_cfg = uvicorn.Config(api_app, host="127.0.0.1",
                                port=config["metrics_port"], log_level="warning")
        server = uvicorn.Server(uv_cfg)
        server_task = asyncio.create_task(server.serve())

        # 5) 就绪握手:打印 READY 行(主进程据此开始发请求)。
        print(f"READY {config['router_port']} {config['metrics_port']}", flush=True)

        # 阻塞至停止信号。
        await stop_evt.wait()

    finally:
        # 优雅关闭:停 uvicorn → 停采样 → router.stop → cluster.stop。
        # 启动中途失败时这些变量可能未绑定,全部防御性处理,避免掩盖真实错误。
        if 'server_task' in locals() and server_task is not None:
            server.should_exit = True
            try:
                await asyncio.wait_for(server_task, timeout=3.0)
            except (asyncio.TimeoutError, Exception):
                pass
        if 'sampler_task' in locals() and sampler_task is not None:
            sampler_task.cancel()
            try:
                await sampler_task
            except (asyncio.CancelledError, Exception):
                pass
        if 'router' in locals():
            try:
                await router.stop()
            except Exception as e:
                print(f"ERROR router stop: {e}", file=sys.stderr, flush=True)
        if cluster is not None:
            try:
                await cluster.stop()
            except Exception as e:
                print(f"ERROR cluster stop: {e}", file=sys.stderr, flush=True)
        print("STOPPED", flush=True)


def main():
    p = argparse.ArgumentParser(description="auto_squid 压测被测方子进程")
    p.add_argument("--config", required=True, help="JSON 配置文件路径(主进程写入)")
    args = p.parse_args()
    try:
        with open(args.config) as f:
            config = json.load(f)
    except Exception as e:
        print(f"ERROR config load: {e}", flush=True)
        sys.exit(1)
    try:
        asyncio.run(_serve(config))
    except Exception as e:
        print(f"ERROR serve: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
