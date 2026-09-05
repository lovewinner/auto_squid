"""对 github.com 经真实上游代理的压力测试(进程隔离,复用 bench.stress 客户端)。

与 `bench.stress --upstream real` 同构,但目标**锁定**在 github.com 家族:
- 默认目标 github.com / api.github.com / www.github.com,`--targets` 可覆盖;
- 负载以 CONNECT 隧道为主(真实访问 github 是 HTTPS,隧道成本就是本代理
  面向 github 的真实成本),`--mode http` / `mixed` 附加普通 HTTP absolute-form;
- real 语义:上游已成功转发即记成功(建隧道即成功 / 收到任何状态码即成功),与
  stress real 模式一致;状态码分布照常记录,用于区分"代理转发成功 vs 上游 4xx"。

前置条件:Router 只经 proxies.yaml 里的上游代理转发,无"无代理直连外网"的能力;
且编辑器所在网络可能到不了 github.com(墙/路由),**上游代理必须能访问 github**。
跑之前建议先 `python -m bench.github --probe` 探路一次(见下)。

用法:
    python -m bench.github                     # 默认:mixed,并发 64,时长 30s
    python -m bench.github --mode tunnel       # 只打 CONNECT github.com:443
    python -m bench.github --mode http         # 只打 http://github.com/ absolute-form
    python -m bench.github --conn-pool --conn-pool-target-prewarm   # 测预热池收益
    python -m bench.github --probe             # 只探路:上游可达 github 吗?
    python -m bench.github --rounds 3          # 每轮全新子进程,去环境噪声
"""

import argparse
import asyncio
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import httpx  # noqa: E402
from typing import Optional  # noqa: E402

from bench.stress import (  # noqa: E402
    ScenarioResult, ServerProcess, _percentile,
    do_connect_request, do_http_request, fetch_counters,
    git_version, run_concurrent, run_open_loop, sample_resources,
)

# 默认 github 目标家族。CONNECT 打 host:443;http 打 http://host/。
_DEFAULT_TARGETS = ["github.com", "api.github.com", "www.github.com"]

ROUTER_HOST = "127.0.0.1"  # 被测 Router 与压测客户端同机,经回环打过去

_REAL_TIMEOUT = 20.0  # 与 stress 的 HostSet.real 一致:真实站点建连/读取放宽


def default_targets() -> list:
    return list(_DEFAULT_TARGETS)


# ── 负载构造 ──────────────────────────────────────────────────

def _make_request(mode: str, targets: list, router_host: str, router_port: int,
                  timeout: float):
    """返回 make(i) 请求工厂:i 为全局递增序号,目标按 i 轮流。

    tunnel:CONNECT host:443,建隧道即成功(echo_check=False,真实 TLS 无法回显)。
    http:  absolute-form GET http://host/(成功判定=收到任何状态码,同 stress real)。
    mixed: 交替 CONNECT 与 GET,最贴近真实访问 github 的混合形态。
    """
    n = len(targets)

    async def make(i: int):
        host = targets[i % n]
        if mode == "tunnel":
            return await do_connect_request(router_host, router_port,
                                            f"{host}:443".encode(),
                                            timeout=timeout, echo_check=False)
        if mode == "http":
            return await do_http_request(router_host, router_port,
                                         f"http://{host}/".encode(),
                                         timeout=timeout,
                                         success_any_status=True)
        # mixed:偶数号走隧道,奇数号走 GET。
        if i % 2 == 0:
            return await do_connect_request(router_host, router_port,
                                            f"{host}:443".encode(),
                                            timeout=timeout, echo_check=False)
        return await do_http_request(router_host, router_port,
                                     f"http://{host}/".encode(),
                                     timeout=timeout, success_any_status=True)

    return make


async def _probe(args) -> None:
    """探路:启动一次被测 Router(real 上游),对每个目标发 CONNECT + GET 各一,
    报告可达性。只用于"上游能否到 github"的快速判定,不跑完整压测。"""
    print(f"probe: 上游={args.proxies} 目标={', '.join(args.targets)}")
    sp = ServerProcess(_server_config(args))
    await sp.start()
    ok, bad = 0, 0
    try:
        for host in args.targets:
            t = f"{host}:443"
            r = await do_connect_request(ROUTER_HOST, sp.router_port,
                                         t.encode(), timeout=args.timeout,
                                         echo_check=False)
            if r.ok:
                ok += 1
                print(f"  tunnel {t:<24} → OK   (ttfb={_percentile([r.ttfb], 0.5)*1000:.0f}ms)")
            else:
                bad += 1
                print(f"  tunnel {t:<24} → FAIL {r.error} ({r.status_code})")
            r = await do_http_request(ROUTER_HOST, sp.router_port,
                                      f"http://{host}/".encode(),
                                      timeout=args.timeout,
                                      success_any_status=True)
            if r.ok:
                ok += 1
                print(f"  http   http://{host}/    → OK {r.status_code}  (ttfb={_percentile([r.ttfb], 0.5)*1000:.0f}ms)")
            else:
                bad += 1
                print(f"  http   http://{host}/    → FAIL {r.error} ({r.status_code})")
    finally:
        sp.stop()
    print(f"probe done: ok={ok} bad={bad}"
          + ("  (上游到 github 不可达,压测无意义,先换可达的上游)"
             if bad and ok == 0 else ""))


async def fetch_proxy_snapshot(metrics_base: str) -> Optional[dict]:
    """拉 /metrics 的 per-pid request/attempt 计数快照,失败返回 None。

    Router 的 `request_counts`/`attempted_counts` 都是 {proxy_id: int}:
    - request_counts  : 该代理被选定(成功转发)的次数;
    - attempted_counts: 该代理被尝试的次数(竞速下 ≈ 选定次数 × 竞速放大)。
    场景首尾各拉一次,`proxy_deltas` 做差即"本次压测各上游代理的分布"。
    """
    try:
        async with httpx.AsyncClient(timeout=2.0) as client:
            r = await client.get(f"{metrics_base}/metrics")
            if r.status_code != 200:
                return None
            j = r.json()
        return {"request_counts": j.get("request_counts") or {},
                "attempted_counts": j.get("attempted_counts") or {}}
    except Exception:
        return None


def proxy_deltas(before: Optional[dict], after: Optional[dict]) -> Optional[dict]:
    """两个 per-proxy 快照做差,得到各上游代理的请求/尝试次数,按请求降序。"""
    if not before or not after:
        return None
    before_req, after_req = before.get("request_counts", {}), after.get("request_counts", {})
    before_att, after_att = before.get("attempted_counts", {}), after.get("attempted_counts", {})
    requests = {pid: after_req[pid] - before_req.get(pid, 0)
                for pid in after_req if after_req[pid] > before_req.get(pid, 0)}
    attempts = {pid: after_att[pid] - before_att.get(pid, 0)
                for pid in after_att if after_att[pid] > before_att.get(pid, 0)}
    return {"requests": dict(sorted(requests.items(), key=lambda kv: -kv[1])),
            "attempts": attempts}


def _server_config(args) -> dict:
    """组装传给 bench.server_proc 的 JSON 配置(real 模式,加载 proxies.yaml)。

    Router 参数只放本脚本要用的:缓存/竞速/连接池访问速度杠杆由 CLI 透传,
    其余用 server_proc 的默认值(与 stress 一一对应,避免重复维护全表)。
    """
    return {
        "upstream": "real",
        "router_port": args.router_port,
        "metrics_port": 0,  # 0 = uvicorn 随机选端口,子进程 READY 回传实际端口
        "max_retries": args.max_retries,
        "cache_ttl": args.cache_ttl,
        "enable_http_cache": not args.no_http_cache,
        "stagger_start": not args.no_stagger,
        "proxies_path": args.proxies,
        "mock_specs": [],
        "db_path": tempfile.mktemp(suffix=".db"),
        # 访问速度杠杆(默认关闭,传参即开启;github 场景最有价值的两个)。
        "conn_pool_enabled": args.conn_pool,
        "conn_pool_per_proxy": args.conn_pool_per_proxy,
        "conn_pool_target_prewarm": args.conn_pool_target_prewarm,
    }


async def _run_one_round(args, targets: list) -> tuple:
    """跑一轮:全新子进程 → warmup → 正式负载 → 拉计数,返回 (metrics, router, sp)。"""
    sp = ServerProcess(_server_config(args))
    await sp.start()
    try:
        result = ScenarioResult(name=f"github-{args.mode}", duration=0.0,
                                client_requests=0)
        stop_evt = asyncio.Event()
        sampler = asyncio.create_task(sample_resources(sp.metrics_base, result, stop_evt))
        make = _make_request(args.mode, targets, ROUTER_HOST, sp.router_port,
                             args.timeout)

        # 预热:发 concurrency 个请求填缓存/预热连接池,不计统计(真实站点不打太多)。
        await run_concurrent(make, args.concurrency, args.concurrency)
        result.warmup_requests += args.concurrency

        # 正式负载:counters_before 在预热后拉,差值只含正式段。
        result.counters_before = await fetch_counters(sp.metrics_base) or {}
        proxy_before = await fetch_proxy_snapshot(sp.metrics_base)
        t0 = time.monotonic()
        if args.requests and args.requests > 0:
            res = await run_concurrent(make, args.requests, args.concurrency,
                                       progress_interval=2.0)
        else:
            loop_stop = asyncio.Event()

            async def _stopper():
                await asyncio.sleep(args.duration)
                loop_stop.set()
            asyncio.create_task(_stopper())
            res = await run_open_loop(make, args.concurrency, loop_stop)
        result.results = res
        result.client_requests = len(res)
        result.injected_requests = len(res)
        result.duration = time.monotonic() - t0

        # 收尾:拉结束计数器 → 停采样器。失败置 counter_fetch_failed(与 stress
        # _finish_scenario 同语义;不依赖私有函数,保持独立脚本自足)。
        result.counters_after = await fetch_counters(sp.metrics_base) or {}
        if not result.counters_after:
            result.counter_fetch_failed = True
        m = result.metrics()
        m["proxies"] = proxy_deltas(proxy_before, await fetch_proxy_snapshot(sp.metrics_base))
        stop_evt.set()
        await sampler
        return m, sp
    finally:
        try:
            sp.stop()
        except Exception:
            pass


# ── 终端报告 ──────────────────────────────────────────────────

def _fmt_ms(v: float) -> str:
    return f"{v:.0f}"


def print_row(prefix: str, m: dict) -> None:
    req = m["requests"]
    lat = m["latency"]
    cache = m["cache"]
    racing = m["racing"]
    conn = m["conn_pool"]
    res = m["resources"]
    lag = res.get("server_loop_lag_ms") or {}
    print(f"\n■ {prefix}")
    print(f"  请求          : 客户端 {req['client']} (成功 {req['success']}, 失败 {req['errors']}, "
          f"注入 {req['injected']}, 预热 {req['warmup']})")
    print(f"  吞吐          : 完成 {m['throughput']['completed_rps']:.0f} req/s  "
          f"注入 {m['throughput']['injected_rps']:.0f} req/s")
    tt = lat["ttfb_ms"]
    to = lat["total_ms"]
    print(f"  TTFB (ms)     : P50={_fmt_ms(tt['p50'])}  P95={_fmt_ms(tt['p95'])}  "
          f"P99={_fmt_ms(tt['p99'])}  mean={_fmt_ms(tt['mean'])}")
    print(f"  total (ms)    : P50={_fmt_ms(to['p50'])}  P95={_fmt_ms(to['p95'])}  "
          f"P99={_fmt_ms(to['p99'])}  mean={_fmt_ms(to['mean'])}")
    print(f"  状态码分布    : {m['status_distribution']}")
    print(f"  错误          : {m['errors']['breakdown']}  (错误率 {m['errors']['rate']*100:.2f}%)")
    hr = cache["http_hit_rate"] if cache["http_hit_rate"] is not None else float("nan")
    dr = cache["domain_hit_rate"] if cache["domain_hit_rate"] is not None else float("nan")
    print(f"  缓存          : HTTP 命中 {hr*100:.0f}%  (hits={cache['http_hits']} misses={cache['http_misses']})  "
          f"域名命中 {dr*100:.0f}%")
    amp = racing["amplification"]
    amp_s = f"{amp:.2f}x" if amp is not None else "N/A"
    att_s = racing["upstream_attempts"] if racing["upstream_attempts"] is not None else "N/A"
    inv_s = racing["invocations"] if racing["invocations"] is not None else "N/A"
    print(f"  竞速          : 放大率 {amp_s}  (上游尝试 {att_s}, 触发 {inv_s})")
    proxies = m.get("proxies")
    if proxies:
        req = proxies["requests"] or {}
        att = proxies["attempts"] or {}
        parts = []
        for pid, n in req.items():
            a = att.get(pid)
            parts.append(f"{pid}: {n} 次" if a is None or a == n else f"{pid}: {n} 次 (尝试 {a})")
        print(f"  上游分布      : " + (" | ".join(parts) if parts else "(无增量)"))
    else:
        print(f"  上游分布      : N/A (per-proxy 计数拉取失败)")
    if conn.get("hits") is not None:
        print(f"  连接池        : hits {conn['hits']}  misses {conn['misses']}  新建 {conn['new_conns']}  "
              f"池末值 {conn['pool_size_end']}")
        if conn.get("target_creates") is not None:
            print(f"  target 预热   : hits {conn['target_hits']}  creates {conn['target_creates']}  "
                  f"池末值 {conn['target_pool_size_end']}  派发 {conn['target_prewarm_dispatched']}")
    print(f"  资源          : 客户端 RSS={res['rss_peak_mb']:.0f}MB  fd={res['fd_peak']}  "
          f"服务端 CPU={res['server_cpu_pct']:.0f}%  loop-lag p95={lag.get('p95', 0):.2f}ms max={lag.get('max', 0):.2f}ms")
    print(f"  归因          : 上游触顶={m['attribution']['upstream_throttled']}  "
          f"瓶颈={m['attribution']['bottleneck']}")


# ── 入口 ──────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="对 github.com 经真实上游代理的压力测试(进程隔离,复用 bench.stress 客户端)")
    p.add_argument("--proxies", default="proxies.yaml", help="真实上游代理列表文件(需可达 github)")
    p.add_argument("--targets", default="",
                   help="压测目标(逗号分隔,默认 github.com,api.github.com,www.github.com)")
    p.add_argument("--mode", choices=["tunnel", "http", "mixed"], default="mixed",
                   help="负载:tunnel=CONNECT :443(默认真实形态)/ http=absolute-form GET / mixed=交替")
    p.add_argument("--concurrency", type=int, default=64, help="并发连接数(默认 64)")
    p.add_argument("--duration", type=float, default=30.0, help="时长秒数(开环固定并发,默认 30)")
    p.add_argument("--requests", type=int, default=0,
                   help="固定请求总数(>0 时覆盖 --duration,闭环跑完即止)")
    p.add_argument("--timeout", type=float, default=_REAL_TIMEOUT, help="客户端单请求超时(默认 20s)")
    p.add_argument("--rounds", type=int, default=1, help="轮数;每轮全新子进程/SQLite/缓存,取均值去噪声")
    p.add_argument("--router-port", type=int, default=10821, help="被测 Router 监听端口")
    p.add_argument("--max-retries", type=int, default=3, help="竞速首批并行数")
    p.add_argument("--cache-ttl", type=int, default=300, help="域名缓存 TTL(秒)")
    p.add_argument("--no-http-cache", action="store_true", help="禁用 HTTP 响应缓存")
    p.add_argument("--no-stagger", action="store_true", help="禁用错峰启动")
    p.add_argument("--conn-pool", action="store_true", help="开启 CONNECT 上游 TCP 预热池")
    p.add_argument("--conn-pool-per-proxy", type=int, default=4, help="每代理预热连接数上限")
    p.add_argument("--conn-pool-target-prewarm", action="store_true",
                   help="CONNECT 目标半预连接(github 高频 host 预连到上游)")
    p.add_argument("--probe", action="store_true", help="只探路:确认上游能否访问 github,不跑完整压测")
    p.add_argument("--out", default="", help="JSON 报告输出路径(默认 github_report_<ts>.json)")
    return p


async def amain(args) -> int:
    git_ver = git_version()
    targets = [t.strip() for t in args.targets.split(",") if t.strip()] if args.targets \
        else default_targets()
    args.targets = targets

    if args.probe:
        await _probe(args)
        return 0

    if any(not p for p in [args.proxies]):
        print(f"[error] --proxies 为空或文件缺失({args.proxies!r});github 压测必须经上游代理。", file=sys.stderr)
        return 1

    print(f"github 压测 git={git_ver} mode={args.mode} targets={','.join(targets)}  "
          f"并发={args.concurrency} 时长={args.duration}s rounds={args.rounds}  [进程隔离, real 上游]")

    all_metrics = []
    for rnd in range(1, args.rounds + 1):
        if args.rounds > 1:
            print(f"\n{'='*70}\n[round {rnd}/{args.rounds}]  每轮全新子进程/SQLite/缓存\n{'='*70}")
        m, _sp = await _run_one_round(args, targets)
        all_metrics.append(m)
        print_row(f"github-{args.mode} [round {rnd}/{args.rounds}]", m)

    if args.rounds > 1:
        _print_round_summary(all_metrics, args)

    out = args.out or f"github_report_{time.strftime('%Y%m%d_%H%M%S')}.json"
    with open(out, "w") as f:
        json.dump({"git": git_ver, "mode": args.mode, "targets": targets,
                   "round_metrics": all_metrics}, f, indent=2)
    print(f"\n报告已写入 {out}")
    return 0


def _print_round_summary(metrics: list, args) -> None:
    """多轮紧凑汇总:每轮一行的关键指标表,便于横向看方差。"""
    print(f"\n{'='*70}\n多轮汇总 (rounds={args.rounds})\n{'='*70}")
    print(f"轮次  完成rps  TTFB p50   p95    p99    err%     HTTP缓存%")
    for i, m in enumerate(metrics, 1):
        tt = m["latency"]["ttfb_ms"]
        cache = m["cache"]
        hr = cache["http_hit_rate"] if cache["http_hit_rate"] is not None else float("nan")
        print(f"  {i}/{args.rounds}  {m['throughput']['completed_rps']:>6.0f}  "
              f"{_fmt_ms(tt['p50']):>5}   {_fmt_ms(tt['p95']):>6}   {_fmt_ms(tt['p99']):>6}   "
              f"{m['errors']['rate']*100:>5.2f}  {hr*100:>6.0f}")
    rps = [m["throughput"]["completed_rps"] for m in metrics]
    p50 = [m["latency"]["ttfb_ms"]["p50"] for m in metrics]
    if rps:
        print(f"  均值    {sum(rps)/len(rps):.1f}±{max(rps)-min(rps):.1f} req/s  "
              f"TTFB p50 {sum(p50)/len(p50):.0f}±{max(p50)-min(p50):.0f} ms")


def main():
    args = _build_parser().parse_args()
    try:
        rc = asyncio.run(amain(args))
    except KeyboardInterrupt:
        print("\n(keyboard interrupt)", file=sys.stderr)
        rc = 130
    sys.exit(rc)


if __name__ == "__main__":
    main()