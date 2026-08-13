#!/usr/bin/env python3
"""周期性采样 auto_squid 运行指标到 opt.log(生产观测用)。

用途
----
auto_squid 的 /metrics 只暴露"当前累计值"快照,没有历史曲线。本脚本以
固定间隔拉取 /metrics,把关键计数器逐行追加到 opt.log(纯文本,每采样点
一行,便于 grep/sort/awk 分析),为"预热池/熔断/竞速"等优化决策提供
跨时间的历史数据。

核心是"增量"思维:累计计数器(如 conn_pool_creates)单看绝对值无意义,
相邻两次采样之差才是该周期内的真实发生量。脚本在每行额外输出
`(d_*)` 增量字段(相对上一采样点),并统计该周期的目标池 HIT 数
(由 target_pool_hits 增量推导,预热的命中流量是优化评估的关键)。

日志文件约定
------------
- 文件:仓库根目录 opt.log(git 已忽略,*.log),也接受 --out 覆盖。
- 每行一条采样记录;行首有 `# 采样说明` 注释,便于人读。
- 格式(单行,无多行 JSON,避免 grep 跨行):
    [YYYY-MM-DD HH:MM:SS] cnt:<k>=<v>,<k>=<v>... d:<k>=<v>,<k>=<v>...
  其中 cnt 为累计值(绝对),d 为相对上一采样点的增量。
- 脚本启动时写一次"说明头"(sample_docs),并记录基线(第一采样点 d 全 0)。

用法
----
    # 后台启动,每 60 秒采样一次(默认 60s)
    python bench/sample_metrics.py &
    # 自定义间隔(秒)与输出文件
    python bench/sample_metrics.py --interval 120 --out /var/log/opt.log
    # 采样 N 次后退出(默认无限运行)
    python bench/sample_metrics.py --count 100
    # 立即采样一次(不进入循环),用于测试
    python bench/sample_metrics.py --once

说明头(sample_docs)含义
-----------------------
sample_docs 是给运维/后续分析者看的字段释义,解释每个采样字段代表什么、
优化决策关注哪些字段,避免日志时间久了忘了字段含义。
"""

import argparse
import json
import os
import sys
import time
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path

# 采样字段(全部取 countees 标量)。只挑优化分析相关的键,避免日志无限膨胀。
FIELD_ORDER = [
    # 请求量/路由形态
    "upstream_attempts",        # 上游尝试总次数(竞速 + 单发)
    "racing_invocations",       # 竞速发起次数(0 = 全走粘性/域名缓存单发)
    "domain_cache_hits",        # 域名缓存命中次数(单发复用)
    "sticky_cache_hits",        # 会话粘性命中次数(单发复用)
    "sticky_cache_size",        # 当前粘性表条目数
    "connect_new_conns",        # CONNECT 新建"本机→上游"连接数(池未中才建)
    # 第一阶段通用池
    "conn_pool_size",           # 当前通用池空闲连接数
    "conn_pool_hits",           # 通用池取用命中(省了建连)
    "conn_pool_misses",         # 通用池取用 miss(需新建)
    "conn_pool_creates",        # 通用池建连累计
    "conn_pool_expired",        # 通用池连接空闲超时被清
    # 第二阶段目标半预连接池
    "target_pool_size",         # 当前目标池空闲连接数(总是很小)
    "target_pool_hits",         # 目标池取用命中(省了建连,重点指标)
    "target_pool_misses",       # 目标池取用 miss(退回通用池/新建)
    "target_pool_creates",      # 目标池预建连累计(含 background prewarm)
    "target_pool_expired",      # 目标池连接空闲超时被清
    "target_prewarm_dispatched",# 预热协程发起次数
    "target_prewarm_success",   # 预热建连成功次数
    "target_prewarm_failed",    # 预热建连失败次数
    # 熔断/探活
    "circuit_open_count",       # 当前熔断的代理数
    "probes_sent",              # 后台探活已发
    "probes_ok",                # 探活成功
    "probes_failed",            # 探活失败
    "probes_skipped",           # 探活跳过(本机不可达 canary)
    "single_send_degrades",     # 单发降级次数(粘性/缓存命中被降级回竞速)
]

# 字段释义(写到日志文件头,供后续分析阅读)
FIELD_DOCS = {
    "upstream_attempts": "上游尝试总次数",
    "racing_invocations": "竞速发起次数(0=全走粘性/域名缓存单发)",
    "domain_cache_hits": "域名缓存命中(单发复用)",
    "sticky_cache_hits": "会话粘性命中(单发复用)",
    "sticky_cache_size": "粘性表当前条目数",
    "connect_new_conns": "CONNECT 新建到上游连接数(池未中才建)",
    "conn_pool_size": "通用池空闲连接数",
    "conn_pool_hits": "通用池取用命中",
    "conn_pool_misses": "通用池取用 miss",
    "conn_pool_creates": "通用池建连累计",
    "conn_pool_expired": "通用池连接空闲超时被清",
    "target_pool_size": "目标池空闲连接数(通常很小)",
    "target_pool_hits": "目标池取用命中(省建连,重点)",
    "target_pool_misses": "目标池取用 miss",
    "target_pool_creates": "目标池预建连累计",
    "target_pool_expired": "目标池连接空闲超时被清",
    "target_prewarm_dispatched": "预热协程发起次数",
    "target_prewarm_success": "预热建连成功次数",
    "target_prewarm_failed": "预热建连失败次数",
    "circuit_open_count": "熔断中的代理数",
    "probes_sent": "探活已发",
    "probes_ok": "探活成功",
    "probes_failed": "探活失败",
    "probes_skipped": "探活跳过",
    "single_send_degrades": "单发降级次数",
}

# 优化分析重点关注字段(供 grep 快速定位)
FOCUS = [
    "target_pool_hits",
    "target_pool_expired",
    "target_pool_creates",
    "conn_pool_hits",
    "conn_pool_expired",
    "racing_invocations",
]


def fetch_metrics(base_url: str, auth: str) -> dict:
    req = urllib.request.Request(base_url.rstrip("/") + "/metrics")
    if auth:
        req.add_header("Authorization", "Basic " + auth)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def snapshot_flat(counters: dict) -> dict:
    return {k: counters.get(k, 0) for k in FIELD_ORDER}


def fmt_pair(k: str, v: int) -> str:
    return f"{k}={v}"


def main() -> None:
    p = argparse.ArgumentParser(description="周期采样 auto_squid /metrics 到 opt.log")
    p.add_argument("--interval", type=float, default=60.0, help="采样间隔秒(默认 60)")
    p.add_argument("--out", default="opt.log", help="输出文件(默认 opt.log)")
    p.add_argument("--url", default="http://127.0.0.1:18080", help="管理 API 地址(默认 http://127.0.0.1:18080)")
    # 凭据从环境变量读取(不硬编码到脚本),缺省时走无认证路径。
    p.add_argument("--user", default=os.environ.get("AUTO_SQUID_API_USER", ""))
    p.add_argument("--pass", dest="password", default=os.environ.get("AUTO_SQUID_API_PASS", ""))
    p.add_argument("--count", type=int, default=0, help="采样次数(0=无限,默认)")
    p.add_argument("--once", action="store_true", help="只采样一次即退出")
    args = p.parse_args()

    auth = ""
    if args.user or args.password:
        import base64
        auth = base64.b64encode(f"{args.user}:{args.password}".encode()).decode()

    out_path = Path(args.out)
    out = open(out_path, "a", encoding="utf-8")

    def log(msg: str) -> None:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        out.write(f"[{ts}] {msg}\n")
        out.flush()

    # ---- 文件头注释:说明本日志用途 ----
    if out_path.stat().st_size == 0:
        header = [
            "# ==============================================================================",
            "# opt.log —— auto_squid 周期性运行指标采样",
            "# 用途:记录 /metrics 关键计数器的历史曲线,供优化决策分析。每行一条采样记录,",
            "#       cnt 为累计值, d 为相对上一采样点的增量。",
            "# 生成脚本:bench/sample_metrics.py(带 --interval/--count/--once 参数)",
            "# 关注字段:target_pool_hits/expired/creates、conn_pool_hits/expired、racing_invocations",
            "#           (目标池命中是第二阶段预热收益的判据:expired 长期追平 creates 说明预热浪费)",
            "# 字段释义:",
        ]
        for k in FIELD_ORDER:
            header.append(f"#   {k}: {FIELD_DOCS[k]}")
        header += [
            "# ==============================================================================",
        ]
        for line in header:
            out.write(line + "\n")
        out.flush()

    # ---- 首次采样:写基线(记录启动时刻,后续 d 基于它) ----
    try:
        d0 = fetch_metrics(args.url, auth)
    except Exception as e:
        print(f"无法连接 {args.url}: {e}", file=sys.stderr)
        sys.exit(1)
    counters0 = d0.get("counters", {})
    prev = snapshot_flat(counters0)

    def emit(now: dict, prev_snap: dict) -> dict:
        cnt = snapshot_flat(now.get("counters", {}))
        diff = {k: cnt[k] - prev_snap.get(k, 0) for k in FIELD_ORDER}
        cnt_s = ",".join(fmt_pair(k, cnt[k]) for k in FIELD_ORDER)
        diff_s = ",".join(fmt_pair(k, diff[k]) for k in FIELD_ORDER)
        # 增量字段里,若全 0 则省掉 d 段(信号只关注变化)
        log(f"cnt:{cnt_s} d:{diff_s}")
        return cnt

    log("# ---- 采样开始 ----")
    emit(d0, prev)  # 基线点,d 全 0
    if args.once:
        return

    # ---- 周期采样 ----
    i = 0
    while args.count == 0 or i < args.count:
        time.sleep(args.interval)
        try:
            d = fetch_metrics(args.url, auth)
        except Exception as e:
            log(f"# fetch error: {e}")
            continue
        prev = emit(d, prev)
        i += 1


if __name__ == "__main__":
    main()
