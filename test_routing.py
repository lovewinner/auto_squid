#!/usr/bin/env python3
"""Test script to analyze auto_squid routing details for a given URL.

This script inspects the routing decision chain without making actual requests
(unless --test-request is used). It shows:
- Local racing / local direct domain match
- Policy routing match and allowed proxy subset
- Domain cache (meta cache) status: winner, TTL, expiry
- Sticky cache status (if client IP provided)
- Proxy ordering for the domain with quality metrics
- Circuit breaker and concurrency status
"""

import argparse
import asyncio
import json
import sys
import urllib.parse
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from auto_squid.proxy_store import ProxyStore
from auto_squid.router import Router
from auto_squid.config_schema import Config, RouterConfig
from auto_squid.api import mount
import yaml
from pathlib import Path


def load_config(config_path: str = "") -> Config:
    """Load configuration from config.yaml or use defaults."""
    try:
        if config_path:
            return Config(**yaml.safe_load(Path(config_path).read_text()))
        default_yaml = Path("config.yaml")
        if default_yaml.exists():
            return Config(**yaml.safe_load(default_yaml.read_text()))
        return Config()
    except Exception as e:
        print(f"Config load error: {e}", file=sys.stderr)
        return Config()


def extract_domain(url: str) -> str:
    """Extract domain from URL (same logic as router)."""
    parsed = urllib.parse.urlparse(url)
    domain = parsed.hostname or url
    # Normalize: lowercase, strip trailing dot
    domain = domain.lower().rstrip('.')
    return domain


def domain_keys(url: str) -> List[str]:
    """Return the domain keys auto_squid uses for routing.

    - HTTP (non-https) requests keyed by bare hostname (host).
    - HTTPS/CONNECT requests keyed by "host:443".
    Returns both forms (deduplicated) so callers can look up either.
    """
    parsed = urllib.parse.urlparse(url)
    host = extract_domain(url)
    scheme = (parsed.scheme or "").lower()
    keys = [host]
    if scheme in ("https", "wss") or ":" in (parsed.netloc or ""):
        keys.append(f"{host}:443")
    return list(dict.fromkeys(keys))


def _lookup_domain(mapping: dict, keys: List[str]) -> dict:
    """Return the first matching entry from mapping for any of the given keys,
    attaching the matched key as `_key`. Returns {} if none match."""
    for k in keys:
        if k in mapping:
            entry = dict(mapping[k])
            entry["_key"] = k
            return entry
    return {}


def normalize_host(host: str) -> str:
    """Normalize hostname/IP for matching (same as router._norm_host)."""
    h = (host or "").strip()
    if h.startswith('[') and h.endswith(']'):
        h = h[1:-1]
    if h.endswith('.'):
        h = h[:-1]
    return h.lower()


class RoutingAnalyzer:
    """Analyzes routing decisions for a given domain."""

    def __init__(self, router: Router, domain: str, client_ip: Optional[str] = None,
                 routing_keys: Optional[List[str]] = None):
        self.router = router
        self.domain = domain
        self.routing_keys = routing_keys or [domain]
        self.client_ip = client_ip

    def _meta_entry(self):
        """Return meta cache entry for the routing key (host:443 preferred)."""
        for k in self.routing_keys:
            entry = self.router._meta_cache.get(k)
            if entry:
                return k, entry
        return None, None

    def analyze_local_racing(self) -> Dict[str, Any]:
        """Check local racing and local direct domains."""
        result = {
            "local_racing_enabled": self.router.enable_local_racing,
            "local_direct_match": False,
            "local_direct_domain": None,
        }
        if self.router._host_in_local_direct(self.domain):
            result["local_direct_match"] = True
            result["local_direct_domain"] = self.domain
        return result

    def analyze_policies(self) -> Dict[str, Any]:
        """Check which policy (if any) matches the domain."""
        if not self.router._policies:
            return {"matched": False, "policy_index": None, "allowed_proxies": []}

        for idx, pol in enumerate(self.router._policies):
            # Check domain_suffix
            for suffix in pol.match.domain_suffix:
                if self.domain.endswith(suffix.lower()):
                    return self._policy_result(idx, pol)
            # Check domain_exact
            if self.domain in [d.lower() for d in pol.match.domain_exact]:
                return self._policy_result(idx, pol)
            # Check domain_regex
            for pattern in pol.match.domain_regex:
                import re
                try:
                    if re.search(pattern, self.domain):
                        return self._policy_result(idx, pol)
                except re.error:
                    pass
        return {"matched": False, "policy_index": None, "allowed_proxies": []}

    def _policy_result(self, idx: int, pol) -> Dict[str, Any]:
        """Build policy match result with allowed proxies."""
        allowed = []
        for pid in self.router.selector.proxy_store.list():
            if self.router._policy_allows_proxy(pol, pid):
                allowed.append(pid.id)
        return {
            "matched": True,
            "policy_index": idx,
            "match_config": {
                "domain_suffix": pol.match.domain_suffix,
                "domain_exact": pol.match.domain_exact,
                "domain_regex": pol.match.domain_regex,
            },
            "allowed_proxies": allowed,
            "proxy_tags": pol.proxies.tags,
            "proxy_ids": pol.proxies.ids,
        }

    def analyze_domain_cache(self) -> Dict[str, Any]:
        """Analyze domain cache (meta cache) status."""
        _key, entry = self._meta_entry()
        if not entry:
            return {"hit": False, "reason": "no_entry"}

        pid = entry.get("default_proxy")
        updated_at = entry.get("updated_at")
        ref_ewma = entry.get("ref_ewma")

        # Check TTL
        ttl = self.router._domain_ttl(self.domain)
        is_valid = False
        mono = entry.get("_updated_mono")
        if mono is not None:
            import time
            is_valid = (time.monotonic() - mono) < ttl
        else:
            try:
                dt = datetime.fromisoformat(updated_at)
                age = (datetime.now(timezone.utc) - dt).total_seconds()
                is_valid = age < ttl
            except Exception:
                is_valid = False

        # Check circuit breaker
        circuit_open = False
        if pid:
            circuit_open = self.router.selector.is_circuit_open(pid)

        # Check immediate degraded
        immediate_degraded = pid in self.router._immediate_degraded if pid else False

        # Check quality degrade
        quality_degraded = False
        if pid and ref_ewma:
            quality_degraded = self.router._single_send_degraded(self.domain, pid, ref_ewma)

        result = {
            "hit": is_valid,
            "routing_key": _key,
            "proxy_id": pid,
            "updated_at": updated_at,
            "ttl_seconds": ttl,
            "ref_ewma": ref_ewma,
            "circuit_open": circuit_open,
            "immediate_degraded": immediate_degraded,
            "quality_degraded": quality_degraded,
        }

        if is_valid:
            result["expires_in_seconds"] = ttl - (age if 'age' in locals() else 0)

        return result

    def analyze_sticky_cache(self) -> Dict[str, Any]:
        """Analyze sticky cache status (requires client_ip)."""
        if not self.client_ip or not self.router.sticky.stickiness_enabled:
            return {"enabled": False, "reason": "sticky_disabled_or_no_client_ip"}

        sticky_key = self.router.sticky._sticky_key(self.client_ip, self.domain)
        entry = self.router.sticky._sticky_cache.get(sticky_key)
        if not entry:
            return {"hit": False, "reason": "no_entry"}

        pid = entry.get("proxy_id")
        updated_at = entry.get("updated_at")
        hits = entry.get("hits", 0)
        recheck_hits = entry.get("recheck_hits", 0)

        # Check TTL
        import time
        ttl = self.router.sticky.stickiness_ttl
        age = time.monotonic() - entry.get("updated_at_mono", 0)
        is_valid = age < ttl

        # Check circuit breaker
        circuit_open = False
        if pid:
            circuit_open = self.router.selector.is_circuit_open(pid)

        # Check policy allows
        policy_allows = True
        if pid and self.router._policies:
            policy_allows = self.router._policy_allows_sticky(self.domain, pid)

        # Check immediate degraded
        immediate_degraded = pid in self.router._immediate_degraded if pid else False

        return {
            "hit": is_valid and policy_allows and not circuit_open and not immediate_degraded,
            "proxy_id": pid,
            "updated_at": updated_at,
            "hits": hits,
            "recheck_hits": recheck_hits,
            "ttl_seconds": ttl,
            "age_seconds": age,
            "circuit_open": circuit_open,
            "policy_allows": policy_allows,
            "immediate_degraded": immediate_degraded,
        }

    def analyze_proxy_ordering(self) -> List[Dict[str, Any]]:
        """Get ordered proxy list for the domain with quality metrics."""
        ordered = self.router.selector.ordered_for_domain(self.domain)
        quality = self.router.selector.get_quality()
        domain_quality = self.router.selector.get_domain_quality()
        circuit_state = self.router.selector.get_circuit_state()
        in_flight = self.router.selector.get_in_flight()
        concurrency_limits = self.router.selector.get_concurrency_limits()
        # Phase 1 增强指标:每代理成功率/错误分类/握手+源站首字节/吞吐(见 /quality/meta)。
        pid_v2 = self.router.selector.get_pid_quality_v2()
        domain_v2 = self.router.selector.get_domain_metrics().get(self.domain, {})

        results = []
        for rank, pid in enumerate(ordered, 1):
            q = quality.get(pid, {})
            ewma = q.get("ewma_ttfb")
            obs = q.get("obs", 0)

            dq = domain_quality.get(self.domain, {}).get(pid, {})
            domain_ewma = dq.get("ewma_ttfb")
            domain_obs = dq.get("obs", 0)

            # Phase 1:该代理对该域名的实测指标(特定 URL 速度评估核心)。
            v2 = pid_v2.get(pid)
            dv2 = domain_v2.get(pid)   # 完整域名级条目:{metrics, percentiles}

            circuit = circuit_state.get(pid, {})
            circuit_open = circuit.get("open", False)
            consec_fail = circuit.get("consec_fail", 0)
            slow_start = circuit.get("slow_start", False)

            in_flight_count = in_flight.get(pid, 0)
            conc_limit = concurrency_limits.get(pid)

            # Check if proxy is in immediate degraded set
            immediate_degraded = pid in self.router._immediate_degraded

            # Check if proxy is in degraded single send (display set)
            degraded_display = pid in self.router._degraded_single_send

            # Check policy allows
            policy_allows = True
            if self.router._policies:
                proxy_info = self.router.proxy_store.get(pid)
                pol = self.router._policy_matches(self.domain)
                if pol:
                    policy_allows = self.router._policy_allows_proxy(pol, proxy_info)

            results.append({
                "rank": rank,
                "proxy_id": pid,
                "global_ewma_seconds": ewma,
                "global_obs": obs,
                "domain_ewma_seconds": domain_ewma,
                "domain_obs": domain_obs,
                # Phase 1 增强指标(见 /quality/meta 与 /domains/meta)
                "v2": v2,               # 全局(跨域名)每代理指标
                "v2_domain": dv2,       # 仅该域名的每代理指标(特定 URL 评估)
                "in_flight": in_flight_count,
                "concurrency_limit": conc_limit,
                "circuit_open": circuit_open,
                "consec_fail": consec_fail,
                "slow_start": slow_start,
                "immediate_degraded": immediate_degraded,
                "degraded_display": degraded_display,
                "policy_allows": policy_allows,
            })
        return results

    def get_stats_summary(self) -> Dict[str, Any]:
        """Get overall statistics summary."""
        counters = self.router.snapshot_counters()
        all_stats = self.router.get_domain_stats_from_db()
        all_meta = self.router.get_domain_meta_enriched()
        domain_stats, domain_meta = {}, {}
        for k in self.routing_keys:
            if k in all_stats:
                domain_stats = all_stats[k]
            if k in all_meta:
                domain_meta = all_meta[k]

        return {
            "domain_stats": domain_stats,
            "domain_meta": domain_meta,
            "counters": {
                "http_cache_hits": counters.get("http_cache_hits", 0),
                "http_cache_misses": counters.get("http_cache_misses", 0),
                "domain_cache_hits": counters.get("domain_cache_hits", 0),
                "racing_invocations": counters.get("racing_invocations", 0),
                "upstream_attempts": counters.get("upstream_attempts", 0),
                "sticky_cache_hits": counters.get("sticky_cache_hits", 0),
                "sticky_evictions": counters.get("sticky_evictions", 0),
            }
        }

    def full_analysis(self) -> Dict[str, Any]:
        """Run full routing analysis."""
        return {
            "domain": self.domain,
            "routing_keys": self.routing_keys,
            "client_ip": self.client_ip,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "local_racing": self.analyze_local_racing(),
            "policy": self.analyze_policies(),
            "domain_cache": self.analyze_domain_cache(),
            "sticky_cache": self.analyze_sticky_cache(),
            "proxy_ordering": self.analyze_proxy_ordering(),
            "stats": self.get_stats_summary(),
        }


async def make_test_request(router: Router, url: str, proxy_host: str = "127.0.0.1",
                            proxy_port: int = 10808,
                            proxy_user: str = "", proxy_pass: str = "") -> Dict[str, Any]:
    """Make an actual test request through the running auto_squid proxy.

    Sends Proxy-Authorization (if creds provided) so the request passes the
    router's client auth. Returns the response plus timing info.
    """
    import base64
    import httpx
    import time

    try:
        import urllib.parse
        # Embed credentials in the proxy URL so httpx sends them in the CONNECT
        # request (Proxy-Authorization header on the CONNECT line).
        proxy_url = f"http://{proxy_host}:{proxy_port}"
        if proxy_user:
            user = urllib.parse.quote(proxy_user, safe='')
            pw = urllib.parse.quote(proxy_pass, safe='')
            proxy_url = f"http://{user}:{pw}@{proxy_host}:{proxy_port}"

        def _err_msg(e: Exception) -> str:
            # httpx ReadTimeout / ConnectError's str() is empty; give a readable one
            if isinstance(e, httpx.ReadTimeout):
                return f"读取超时({timeout_seconds}秒)"
            if isinstance(e, httpx.ConnectError):
                msg = getattr(e, "__cause__", None) or e
                return f"连接错误: {msg}"
            return str(e) or e.__class__.__name__

        # httpx proxy support: set trust_env to avoid env http(s)_proxy interference
        # verify=False: skip TLS cert verification against the local proxy
        # (proxy may MITM with a self-signed/private CA). Diagnostic only.
        timeout_seconds = 60.0
        async with httpx.AsyncClient(proxy=proxy_url, timeout=timeout_seconds,
                                     trust_env=False, verify=False) as client:
            t0 = time.perf_counter()
            try:
                resp = await client.get(url)
                elapsed = time.perf_counter() - t0
            except Exception as e:
                return {
                    "success": False,
                    "error": _err_msg(e),
                    "elapsed_seconds": time.perf_counter() - t0,
                }
        return {
            "success": True,
            "status_code": resp.status_code,
            "elapsed_seconds": elapsed,
            "headers": {k: v for k, v in resp.headers.items()},
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e) or e.__class__.__name__,
        }


def _extract_proxy_auth(cfg: Config) -> tuple:
    """Return (username, password) for the client-facing proxy auth.

    Reads from config.yaml router.auth. Returns empty strings if disabled.
    """
    auth = cfg.router.auth
    if not auth.enabled:
        return "", ""
    return auth.username, auth.password


def _extract_api_auth(cfg: Config) -> tuple:
    """Return (username, password) for the management API auth."""
    auth = cfg.api.auth
    if not auth.enabled:
        return "", ""
    return auth.username, auth.password


def format_output(analysis: Dict[str, Any], json_output: bool = False) -> str:
    """格式化分析结果用于显示."""
    if json_output:
        return json.dumps(analysis, indent=2, ensure_ascii=False)

    lines = []
    lines.append(f"=== auto_squid 路由分析: {analysis['domain']} ===")
    lines.append(f"时间戳: {analysis['timestamp']}")
    if analysis['client_ip']:
        lines.append(f"客户端 IP: {analysis['client_ip']}")
    lines.append("")

    # 本地竞速
    lr = analysis['local_racing']
    lines.append("--- 本地竞速 / 直连 ---")
    lines.append(f"  本地竞速启用: {lr['local_racing_enabled']}")
    if lr['local_direct_match']:
        lines.append(f"  ⚡ 本地直连匹配: {lr['local_direct_domain']} (绕过所有代理)")
    else:
        lines.append("  无本地直连域名匹配")
    lines.append("")

    # 策略
    pol = analysis['policy']
    lines.append("--- 策略路由 ---")
    if pol['matched']:
        lines.append(f"  ✓ 策略 #{pol['policy_index']} 匹配")
        lines.append(f"    匹配配置: {json.dumps(pol['match_config'], ensure_ascii=False)}")
        lines.append(f"    允许的代理: {pol['allowed_proxies'] or '全部'}")
        if pol['proxy_tags']:
            lines.append(f"    代理标签过滤: {pol['proxy_tags']}")
        if pol['proxy_ids']:
            lines.append(f"    代理 ID 过滤: {pol['proxy_ids']}")
    else:
        lines.append("  无策略匹配 (所有启用代理均允许)")
    lines.append("")

    # 域名缓存
    dc = analysis['domain_cache']
    lines.append("--- 域名缓存 (Meta Cache) ---")
    if analysis.get('routing_keys'):
        lines.append(f"  路由键: {analysis['routing_keys']}")
    if dc['hit']:
        lines.append(f"  ✓ 缓存命中 (key={dc.get('routing_key', dc.get('_key','?'))}): {dc['proxy_id']}")
        if dc.get('updated_at'):
            lines.append(f"    更新时间: {dc['updated_at']}")
        if dc.get('ttl_seconds'):
            lines.append(f"    TTL: {dc['ttl_seconds']}秒")
        if 'expires_in_seconds' in dc:
            lines.append(f"    剩余有效时间: {dc['expires_in_seconds']:.1f}秒")
        if dc.get('ref_ewma') is not None:
            lines.append(f"    基准 EWMA: {dc['ref_ewma']*1000:.1f}ms")
        if dc.get('circuit_open'):
            lines.append(f"    ⚠ 熔断器打开 - 将回退到竞速")
        if dc.get('immediate_degraded'):
            lines.append(f"    ⚠ 立即降级 - 将回退到竞速")
        if dc.get('quality_degraded'):
            lines.append(f"    ⚠ 质量降级 - 将回退到竞速")
    else:
        lines.append(f"  ✗ 缓存未命中: {dc.get('reason', '未知')}")
        if dc.get('proxy_id'):
            lines.append(f"    过期条目: {dc['proxy_id']} (TTL 过期或降级)")
    lines.append("")

    # 粘性缓存
    sc = analysis['sticky_cache']
    lines.append("--- 粘性缓存 ---")
    if not sc.get('enabled', True):
        lines.append(f"  已禁用: {sc.get('reason', '未知')}")
    elif sc['hit']:
        lines.append(f"  ✓ 粘性命中: {sc['proxy_id']}")
        lines.append(f"    命中次数: {sc.get('hits',0)}, 复检命中: {sc.get('recheck_hits',0)}")
        if sc.get('updated_at'):
            lines.append(f"    更新时间: {sc['updated_at']}")
        lines.append(f"    年龄: {sc.get('age_seconds',0):.1f}秒 / TTL: {sc.get('ttl_seconds',0)}秒")
        if sc.get('circuit_open'):
            lines.append(f"    ⚠ 熔断器打开 - 将回退到竞速")
        if sc.get('policy_allows', True) is False:
            lines.append(f"    ⚠ 策略阻止 - 将回退到竞速")
        if sc.get('immediate_degraded'):
            lines.append(f"    ⚠ 立即降级 - 将回退到竞速")
    else:
        lines.append(f"  ✗ 粘性未命中: {sc.get('reason', '未知')}")
        if sc.get('proxy_id'):
            lines.append(f"    过期条目: {sc['proxy_id']}")
    lines.append("")

    # 代理排序
    lines.append("--- 域名代理排序 (竞争顺序) ---")
    for p in analysis['proxy_ordering']:
        status = []
        if p.get('circuit_open'):
            status.append("🔴 熔断打开")
        if p.get('slow_start'):
            status.append("🟡 慢启动")
        if p.get('immediate_degraded'):
            status.append("🟠 立即降级")
        if p.get('degraded_display'):
            status.append("🟠 降级显示")
        if not p.get('policy_allows', True):
            status.append("🚫 策略阻止")
        if p.get('in_flight', 0) > 0:
            status.append(f"⏳ 并发中:{p.get('in_flight')}")

        status_str = " ".join(status) if status else "✓ 健康"

        ewma_str = ""
        if p.get('domain_ewma_seconds') is not None:
            ewma_str = f" (域名 EWMA: {p['domain_ewma_seconds']*1000:.1f}ms, obs={p.get('domain_obs',0)})"
        elif p.get('global_ewma_seconds') is not None:
            ewma_str = f" (全局 EWMA: {p['global_ewma_seconds']*1000:.1f}ms, obs={p.get('global_obs',0)})"
        else:
            ewma_str = " (无 EWMA 数据)"

        conc_str = ""
        if p.get('concurrency_limit'):
            conc_str = f" 并发上限={p['concurrency_limit']}"

        # Phase 1:握手/源站首字节分位数 + 成功率 + 错误分类(优先仅该域名的,回退全局)。
        phase1 = ""
        dom = p.get('v2_domain')  # {metrics, percentiles} 或 None
        dom_metrics = (dom or {}).get('metrics') if dom else None
        base = dom_metrics or (p.get('v2') or {})
        ttfb_p = None
        ofb_p = None
        if dom:
            ttfb_p = dom.get('percentiles', {}).get('ttfb')
            ofb_p = dom.get('percentiles', {}).get('ofb')
        if not ttfb_p and p.get('v2'):
            ttfb_p = p['v2'].get('ttfb')
        if not ofb_p and p.get('v2'):
            ofb_p = p['v2'].get('ofb')
        if base:
            tot = base.get('total', 0)
            suc = base.get('success', 0)
            rate = (suc / tot) if tot else None
            errs = base.get('errors', {})
            err_str = ",".join(f"{k}:{v}" for k, v in errs.items() if v)
            bits = []
            if ttfb_p and ttfb_p.get('samples'):
                bits.append(f"握手 p50/p95/p99={ttfb_p.get('p50',0)*1000:.0f}/{ttfb_p.get('p95',0)*1000:.0f}/{ttfb_p.get('p99',0)*1000:.0f}ms(n={ttfb_p['samples']})")
            if ofb_p and ofb_p.get('samples'):
                bits.append(f"源站首字节 p50/p95/p99={ofb_p.get('p50',0)*1000:.0f}/{ofb_p.get('p95',0)*1000:.0f}/{ofb_p.get('p99',0)*1000:.0f}ms(n={ofb_p['samples']})")
            if rate is not None:
                bits.append(f"成功率={rate*100:.0f}%({suc}/{tot})")
            if err_str:
                bits.append(f"错误[{err_str}]")
            if base.get('throughput_ewma') is not None:
                bits.append(f"吞吐≈{base['throughput_ewma']:.1f}MB/s")
            if bits:
                phase1 = "  " + "  ".join(bits)

        lines.append(f"  #{p.get('rank','?')}: {p['proxy_id']}{ewma_str}{conc_str} 并发={p.get('in_flight',0)} 连续失败={p.get('consec_fail',0)} {status_str}{phase1}")
    lines.append("")

    # 统计
    stats = analysis['stats']
    lines.append("--- 统计信息 ---")
    lines.append(f"  域名胜出次数: {stats['domain_stats']}")
    if stats['domain_meta']:
        meta = stats['domain_meta']
        lines.append(f"  元数据: 默认代理={meta.get('default_proxy')}, 更新时间={meta.get('updated_at')}")
        if 'ttl' in meta:
            lines.append(f"    TTL: {meta['ttl']}秒, 过期时间: {meta.get('expires_at')}, 剩余TTL: {meta.get('ttl_remaining', 0):.1f}秒")
        if meta.get('switch_count') is not None:
            lines.append(f"    切换次数: {meta.get('switch_count', 0)}")
    c = stats['counters']
    lines.append(f"  缓存命中: 域名={c['domain_cache_hits']}, http={c['http_cache_hits']}")
    lines.append(f"  缓存未命中: http={c['http_cache_misses']}")
    lines.append(f"  竞速调用次数: {c['racing_invocations']}")
    lines.append(f"  上游尝试次数: {c['upstream_attempts']}")
    lines.append(f"  粘性命中: {c['sticky_cache_hits']}, 驱逐: {c['sticky_evictions']}")

    # 测试请求
    if 'test_request' in analysis:
        tr = analysis['test_request']
        lines.append("")
        lines.append("--- 实时测试请求结果 ---")
        if tr.get('success'):
            lines.append(f"  ✓ 请求成功: HTTP {tr['status_code']} 耗时 {tr['elapsed_seconds']:.2f}秒")
            winner = tr.get('winner_proxy')
            rkey = tr.get('routing_key')
            if winner:
                k = f" (key={rkey})" if rkey else ""
                lines.append(f"  ▶ 实际使用的胜出代理{k}: {winner}")
            else:
                lines.append("  胜出代理: (域名缓存仍为空 - 可能竞速尚未完成)")
        else:
            lines.append(f"  ✗ 请求失败: {tr.get('error')}")

    return "\n".join(lines)


# ── 增强指标视图(--metrics / --domains-list)渲染 ──────────────
# 复用 show_metrics.py 的表格渲染逻辑,纳入 test_routing.py;数据来自
# /quality/meta(每代理全局)与 /metrics/per-destination(域名×代理)。

_ERROR_LABELS = {
    "timeout": "超时", "connect": "连接", "http_5xx": "5xx", "tls": "TLS",
    "protocol": "协议", "cancelled": "取消", "other": "其他",
}


def _fmt_cumulative(cum, detail_only=False) -> str:
    """把累计视图格式化成一行可读字符串(终身累计/跨重启永久值)。

    cum 为 selector._cumulative_view 的产物,含 success/failure*/samples/
    success_rate/avg_ttfb_ms/avg_ofb_ms/throughput_mbps。无样本时给占位。
    detail_only=True 时不重复输出主表已展示的 成功率/吞吐,只给平均时延与失败细分。
    """
    if not isinstance(cum, dict) or not cum.get("samples"):
        return "  累计(永久值): — 暂无样本"
    at = cum.get("avg_ttfb_ms")
    at_s = f"{at:.0f}ms" if at is not None else "—"
    ob = cum.get("avg_ofb_ms")
    ob_s = f"{ob:.0f}ms" if ob is not None else "—"
    fp = (f"失败:{cum['failure']}(传输{cum['failure_transport']}"
          f"+5xx{cum['failure_5xx']})")
    if detail_only:
        # 主表已展示 成功率/吞吐,这里只给平均时延与失败细分。
        base = (f"  累计(永久值, n={cum['samples']}): 平均握手(代理)={at_s} "
                f"平均源站首字节={ob_s} | {fp}")
    else:
        sr = cum.get("success_rate")
        sr_s = f"{sr*100:.1f}%" if sr is not None else "—"
        tp = cum.get("throughput_mbps")
        tp_s = f"{tp:.2f}MB/s" if tp is not None else "—"
        base = (f"  累计(永久值, n={cum['samples']}): 成功率={sr_s} "
                f"平均握手(代理)={at_s} 平均源站首字节={ob_s} 吞吐={tp_s} | {fp}")
    # 终身分位数(t-digest rollup,有界内存):补"累计只有均值"的缺口,给全历史分位。
    lifeline = _fmt_lifetime_pct(cum)
    return base if not lifeline else base + "\n" + lifeline


def _fmt_lifetime_pct(cum) -> str:
    """终身分位数行(有界内存 t-digest rollup),无样本时返回空串。"""
    t = cum.get("ttfb_percentiles") or {}
    o = cum.get("ofb_percentiles") or {}
    if not t.get("samples"):
        return ""

    def _f(p):
        if not p or not p.get("samples"):
            return "—"
        return (f"{p.get('p50', 0)*1000:.0f}/{p.get('p95', 0)*1000:.0f}/"
                f"{p.get('p99', 0)*1000:.0f}ms")

    return (f"    终身分位(全历史 n={t['samples']}): 握手 P50/P95/P99={_f(t)} "
            f"源站首字节={_f(o)}")


def _fmt_pct(p) -> str:
    """分位数单元格: 'p50/p95/p99(n=样本数)'; 无样本给占位。

    入参是 selector._percentiles 的产物(ttfb / ofb 窗口)。
    """
    if not isinstance(p, dict) or not p.get("samples"):
        return "—"
    s = (f"{p.get('p50', 0)*1000:.0f}/{p.get('p95', 0)*1000:.0f}/"
         f"{p.get('p99', 0)*1000:.0f}(n={p['samples']})")
    if p.get("low_confidence"):
        s += " ⚠低样本"
    return s


def _fmt_cum_ewma(cum_val, ewma_val, fmt) -> str:
    """累计(永久值)为主, 窗口化 EWMA 为辅, 以 '累计/近期' 形式输出单格。"""
    c = fmt(cum_val) if cum_val is not None else None
    e = fmt(ewma_val) if ewma_val is not None else None
    if c is None and e is None:
        return "—"
    if c is not None and e is not None:
        return f"{c}/{e}"
    return c if c is not None else e


def _render_metrics_header(api_url: str) -> List[str]:
    return [f"=== auto_squid 采集指标 @ {api_url}  {datetime.now(timezone.utc).isoformat()} ==="]


def _render_per_destination_metrics(dom_metrics: Dict[str, Any], keys: List[str],
                                    quality: Dict[str, Any], qm: Dict[str, Any]) -> List[str]:
    """把 /metrics/per-destination 中匹配 keys 的 (域名,代理) 指标渲染成表格。

    keys: 目标域名键候选(如 ['github.com:443','github.com'])。qm 未用,保留签名
    以兼容全局概览扩展;排序用 quality 的 EWMA 反映竞速倾向。
    """
    lines = [f"目标实测指标: {keys[0] if keys else ''}", "-" * 40]
    matched = {k: per for k, per in dom_metrics.items() if k in set(keys)} if keys else dom_metrics
    if not matched:
        lines.append(f"  (该目标尚无域名级观测;候选键: {', '.join(keys)})")
        lines.append("")
        return lines
    for dkey, per in matched.items():
        lines.append(f"  ▶ 域名键: {dkey}")
        header = (f"      {'代理':<10}{'握手 p50/p95/p99':<26}{'源站首字节 p50/p95/p99':<26}"
                  f"{'成功率(累/近)':<14}{'吞吐(累/近)':<22}{'错误分类'}")
        lines.append(header)
        lines.append("      " + "-" * (len(header) - 6))
        order = sorted(per.items(), key=lambda kv: (quality.get(kv[0], {}).get("ewma_ttfb") or 1e9))
        for pid, m in order:
            per_t = m.get("percentiles") or {}
            tf = _fmt_pct(per_t.get("ttfb"))
            tl = _fmt_pct(per_t.get("ofb"))
            errs = {k: v for k, v in (m.get("errors") or {}).items() if v}
            err_str = ",".join(f"{_ERROR_LABELS.get(k, k)}:{v}" for k, v in errs.items()) or "—"
            total = m.get("total", 0)
            rate = (m.get("success", 0) / total) if total else None
            cum = m.get("cumulative") or {}
            rate_s = _fmt_cum_ewma(cum.get("success_rate"), rate,
                                   lambda p: f"{p*100:.1f}%")
            thr_s = _fmt_cum_ewma(cum.get("throughput_mbps"), m.get("throughput_ewma"),
                                  lambda v: f"{v:.2f}MB/s")
            lines.append(f"      {pid:<10}{tf:<26}{tl:<26}{rate_s:<14}{thr_s:<22}{err_str}"
                         f"  <<n={total}")
            # 累计(永久值)平均时延与失败细分, 成功率/吞吐已并入主列。
            lines.append(_fmt_cumulative(cum, detail_only=True))
        lines.append("")
    return lines


def _render_domain_list(dom_metrics: Dict[str, Any]) -> List[str]:
    """列出所有有观测的域名及每代理观测次数。"""
    lines = [f"所有有观测的域名(共 {len(dom_metrics)} 个)  [域名键 @ 代理: 观测次数]",
             "-" * 60]
    for dkey in sorted(dom_metrics.keys()):
        per = dom_metrics[dkey]
        parts = [f"{pid}:{m.get('total', 0)}"
                 for pid, m in sorted(per.items(), key=lambda kv: -kv[1].get("total", 0))]
        lines.append(f"  {dkey:<38} {'  '.join(parts)}")
    lines.append("")
    return lines


def _render_proxy_overview(qm: Dict[str, Any]) -> List[str]:
    """把 /quality/meta 的 {pid: metric} 渲染成每代理全局概览表。"""
    lines = ["全局每代理概览(跨域名聚合)", "-" * 40]
    if not qm:
        lines.append("  (尚无观测)")
        lines.append("")
        return lines
    header = (f"  {'代理':<10}{'握手 p50/p95/p99':<26}{'源站首字节 p50/p95/p99':<26}"
              f"{'成功率(累/近)':<14}{'吞吐(累/近)':<22}{'错误分类'}")
    lines.append(header)
    lines.append("  " + "-" * (len(header) - 2))
    for pid, m in sorted(qm.items(), key=lambda kv: (kv[1].get("success_rate") or 0)):
        tf = _fmt_pct(m.get("ttfb"))
        tl = _fmt_pct(m.get("ofb"))
        errs = {k: v for k, v in (m.get("errors") or {}).items() if v}
        err_str = ",".join(f"{_ERROR_LABELS.get(k, k)}:{v}" for k, v in errs.items()) or "—"
        cum = m.get("cumulative") or {}
        rate_s = _fmt_cum_ewma(cum.get("success_rate"), m.get("success_rate"),
                               lambda p: f"{p*100:.1f}%")
        thr_s = _fmt_cum_ewma(cum.get("throughput_mbps"), m.get("throughput_ewma_mbps"),
                              lambda v: f"{v:.2f}MB/s")
        lines.append(f"  {pid:<10}{tf:<26}{tl:<12}{rate_s:<14}{thr_s:<22}{err_str}")
        # 累计(永久值)平均时延与失败细分, 成功率/吞吐已并入主列。
        lines.append(_fmt_cumulative(cum, detail_only=True))
    lines.append("")
    return lines


async def main():
    parser = argparse.ArgumentParser(description="Analyze auto_squid routing for a URL")
    parser.add_argument("url", nargs="?", default="", help="URL to analyze (e.g., https://github.com/user/repo)")
    parser.add_argument("--domain", default="", help="Domain or host:port for the metrics view (overrides url)")
    parser.add_argument("--metrics", action="store_true",
                        help="Show collected Phase 1 metrics (handshake/OFB percentiles, success rate, errors, throughput)")
    parser.add_argument("--domains-list", action="store_true",
                        help="List all observed domains with per-proxy attempt counts")
    parser.add_argument("--client-ip", help="Client IP for sticky cache analysis")
    parser.add_argument("--config", default="", help="Path to config.yaml")
    parser.add_argument("--proxies", default="proxies.yaml", help="Path to proxies.yaml")
    parser.add_argument("--db", default="auto_squid.db", help="Path to SQLite database")
    parser.add_argument("--test-request", action="store_true", help="Make actual test request through proxy")
    parser.add_argument("--proxy-host", default="127.0.0.1", help="Proxy host for test request")
    parser.add_argument("--proxy-port", type=int, default=10808, help="Proxy port for test request")
    parser.add_argument("--proxy-user", default="", help="Proxy auth username (overrides config)")
    parser.add_argument("--proxy-pass", default="", help="Proxy auth password (overrides config)")
    parser.add_argument("--api-user", default="", help="Management API auth username (overrides config)")
    parser.add_argument("--api-pass", default="", help="Management API auth password (overrides config)")
    parser.add_argument("--json", action="store_true", help="Output as JSON")
    parser.add_argument("--api-url", default="",
                        help="Connect to running instance via API (e.g., http://localhost:18080). "
                             "Default http://127.0.0.1:18080 for --metrics/--domains-list.")

    args = parser.parse_args()

    # --metrics / --domains-list 只读指标视图:允许无 url。
    if not args.url and not args.metrics and not args.domains_list:
        parser.error("需要提供 URL 位置参数(或使用 --metrics / --domains-list)")

    # 指标视图默认连本机运行中的管理 API;原分析(无 --metrics)仍走本地模式。
    if (args.metrics or args.domains_list) and not args.api_url:
        args.api_url = "http://127.0.0.1:18080"

    # 指标视图的目标:优先 --domain,回退 url 推导;明文 https→host:443 等。
    metrics_target = args.domain or args.url
    domain = extract_domain(metrics_target) if metrics_target else ""

    if args.api_url:
        # Connect to running instance via API
        import httpx
        import base64

        cfg = load_config(args.config)
        api_user, api_pass = args.api_user, args.api_pass
        if not api_user:
            api_user, api_pass = _extract_api_auth(cfg)
        headers = {}
        if api_user:
            token = base64.b64encode(f"{api_user}:{api_pass}".encode()).decode()
            headers["Authorization"] = f"Basic {token}"

        async with httpx.AsyncClient(base_url=args.api_url, timeout=10.0) as client:
            # 增强指标视图(--metrics / --domains-list)只需质量明细端点。
            if args.metrics or args.domains_list:
                qm = (await client.get("/quality/meta", headers=headers)).json()
                pd = (await client.get("/metrics/per-destination", headers=headers)).json()
                qual = (await client.get("/quality", headers=headers)).json()
                if args.domains_list:
                    out = _render_domain_list(pd)
                    print("\n".join(out))
                    return
                if args.json:
                    print(json.dumps({"quality_meta": qm, "per_destination": pd},
                                     indent=2, ensure_ascii=False))
                    return
                out = _render_metrics_header(args.api_url)
                if metrics_target:
                    out += _render_per_destination_metrics(pd, domain_keys(metrics_target),
                                                           qual, qm)
                else:
                    out += _render_proxy_overview(qm)
                print("\n".join(out))
                return

            # Fetch domain meta, quality, circuit, etc.
            meta_resp = await client.get("/domains/meta", headers=headers)
            quality_resp = await client.get("/quality", headers=headers)
            circuit_resp = await client.get("/circuit", headers=headers)
            policies_resp = await client.get("/policies", headers=headers)
            config_resp = await client.get("/config", headers=headers)
            stats_resp = await client.get("/metrics", headers=headers)

            meta = meta_resp.json()
            quality = quality_resp.json()
            circuit = circuit_resp.json()
            policies = policies_resp.json()
            config = config_resp.json()
            stats = stats_resp.json()

            # Build proxy ordering from quality + circuit data
            proxy_ordering = []
            cir = circuit.get("proxies", {})
            for rank, pid in enumerate(sorted(quality.keys(), key=lambda p: quality[p].get("ewma_ttfb", 1e9)), 1):
                q = quality.get(pid, {})
                cs = cir.get(pid, {})
                proxy_ordering.append({
                    "rank": rank,
                    "proxy_id": pid,
                    "global_ewma_seconds": q.get("ewma_ttfb"),
                    "global_obs": q.get("obs", 0),
                    "circuit_open": cs.get("open", False),
                    "consec_fail": cs.get("consec_fail", 0),
                    "slow_start": cs.get("slow_start", False),
                })

            keys = domain_keys(args.url)
            meta_entry = _lookup_domain(meta, keys)
            analysis = {
                "domain": domain,
                "routing_keys": keys,
                "client_ip": args.client_ip,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "local_racing": {
                    "local_racing_enabled": config.get("enable_local_racing", False),
                    "local_direct_match": False,
                },
                "policy": {"matched": False},
                "domain_cache": {
                    "hit": bool(meta_entry),
                    "routing_key": meta_entry.get("_key"),
                    "proxy_id": meta_entry.get("default_proxy"),
                    "updated_at": meta_entry.get("updated_at"),
                    "ttl_seconds": meta_entry.get("ttl"),
                    "ref_ewma": meta_entry.get("ref_ewma"),
                },
                "sticky_cache": {"enabled": False},
                "proxy_ordering": proxy_ordering,
                "stats": {
                    "domain_stats": _lookup_domain(stats.get("domain_stats", {}), keys),
                    "domain_meta": meta_entry,
                    "counters": stats.get("counters", {}),
                },
            }

            # Test request if requested
            if args.test_request:
                print("正在发送测试请求...", file=sys.stderr)
                proxy_user, proxy_pass = args.proxy_user, args.proxy_pass
                if not proxy_user:
                    proxy_user, proxy_pass = _extract_proxy_auth(cfg)
                test_result = await make_test_request(
                    None, args.url, args.proxy_host, args.proxy_port,
                    proxy_user, proxy_pass)
                analysis["test_request"] = test_result
                # Re-fetch meta after the request to see the actual winner
                meta2 = (await client.get("/domains/meta", headers=headers)).json()
                meta_entry2 = _lookup_domain(meta2, keys)
                if meta_entry2:
                    analysis["test_request"]["winner_proxy"] = meta_entry2.get("default_proxy")
                    analysis["test_request"]["routing_key"] = meta_entry2.get("_key")
                else:
                    analysis["test_request"]["winner_proxy"] = None
    else:
        # Load local config and create router instance (read-only analysis)
        cfg = load_config(args.config)
        proxy_store = ProxyStore(args.proxies)

        # Create router with minimal config for analysis
        # We don't start the server, just use the router object for inspection
        router = Router(
            proxy_store,
            listen_host=cfg.listen.host,
            listen_port=cfg.listen.port,
            db_path=args.db,
            router_cfg=cfg.router,
        )
        # Load caches from DB
        router._load_caches_from_db()

        # 本地模式(--metrics / --domains-list)直接从运行实例的 selector 内存读。
        if args.metrics or args.domains_list:
            pd = router.selector.get_domain_metrics()
            if args.domains_list:
                print("\n".join(_render_domain_list(pd)))
                return
            if args.json:
                print(json.dumps({"quality_meta": router.selector.get_pid_quality_v2(),
                                  "per_destination": pd}, indent=2, ensure_ascii=False))
                return
            out = _render_metrics_header("(local)")
            if metrics_target:
                out += _render_per_destination_metrics(pd, domain_keys(metrics_target),
                                                       router.selector.get_quality(), {})
            else:
                out += _render_proxy_overview(router.selector.get_pid_quality_v2())
            print("\n".join(out))
            return

        # Determine proxy auth creds for test request
        proxy_user, proxy_pass = args.proxy_user, args.proxy_pass
        if not proxy_user:
            proxy_user, proxy_pass = _extract_proxy_auth(cfg)

        analyzer = RoutingAnalyzer(router, domain, args.client_ip, routing_keys=domain_keys(args.url))
        analysis = analyzer.full_analysis()

        # Test request if requested
        if args.test_request:
            print("正在发送测试请求...", file=sys.stderr)
            test_result = await make_test_request(router, args.url, args.proxy_host,
                                                  args.proxy_port, proxy_user, proxy_pass)
            analysis["test_request"] = test_result

            # Wait briefly for the router's background flush/probe to settle, then re-analyze
            await asyncio.sleep(0.5)
            if test_result.get("success"):
                cache = analyzer.analyze_domain_cache()
                analysis["test_request"]["winner_proxy"] = cache.get("proxy_id")
                analysis["test_request"]["routing_key"] = cache.get("routing_key")
            else:
                analysis["test_request"]["winner_proxy"] = None

    print(format_output(analysis, args.json))


if __name__ == "__main__":
    asyncio.run(main())