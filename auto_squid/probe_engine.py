import asyncio
import time
import logging
from typing import Dict, List, Tuple
import math

import httpx

from .proxy_store import ProxyStore
from .config_schema import ProbeConfig, ScoreConfig

logger = logging.getLogger(__name__)


class ProbeEngine:
    """Probe engine with concurrency controls, warming/cold-start state, and probe history.

    Samples: proxy_id -> list of (ts, latency_ms, throughput_kbps, success)
    States: proxy_id -> 'warming'|'normal'|'degraded'
    Supports per-domain concurrency limits when DomainIndex is provided.
    """

    def __init__(self, proxy_store: ProxyStore, probe_cfg: ProbeConfig | None = None, score_cfg: ScoreConfig | None = None, domain_index=None):
        self.proxy_store = proxy_store
        self.probe_cfg = probe_cfg or ProbeConfig()
        self.score_cfg = score_cfg or ScoreConfig()
        self.domain_index = domain_index
        self._samples: Dict[str, List[Tuple[float, float, float, bool]]] = {}
        self._states: Dict[str, str] = {}
        self._running = False
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        # concurrency controls
        self._global_sem = asyncio.Semaphore(self.probe_cfg.concurrency)
        self._proxy_sems: Dict[str, asyncio.Semaphore] = {}
        self._domain_sems: Dict[str, asyncio.Semaphore] = {}

    def _get_proxy_sem(self, proxy_id: str) -> asyncio.Semaphore:
        if proxy_id not in self._proxy_sems:
            self._proxy_sems[proxy_id] = asyncio.Semaphore(self.probe_cfg.per_proxy_concurrency)
        return self._proxy_sems[proxy_id]

    def _get_domain_sem(self, domain: str) -> asyncio.Semaphore:
        key = domain or '__global__'
        if key not in self._domain_sems:
            self._domain_sems[key] = asyncio.Semaphore(self.probe_cfg.per_domain_concurrency)
        return self._domain_sems[key]

    async def _probe_proxy(self, proxy_id: str, domain: str | None = None):
        """Probe a proxy. Domain is optional; included so per-domain semaphores can be used by caller."""
        proxy = self.proxy_store.get(proxy_id)
        if not proxy or not proxy.enabled:
            return
        host = proxy.host
        port = proxy.port
        proxy_url = f"http://{host}:{port}"
        ts = time.time()
        tcp_start = time.time()
        latency_ms = 0.0
        throughput_kbps = 0.0
        success = False
        try:
            reader, writer = await asyncio.open_connection(host, port)
            tcp_latency = (time.time() - tcp_start) * 1000.0
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            async with httpx.AsyncClient(proxies={"http://": proxy_url, "https://": proxy_url}, timeout=self.probe_cfg.timeout) as client:
                # set Host header to domain if provided to exercise domain-specific pathing
                headers = {"Host": domain} if domain else None
                start = time.time()
                r = await client.get(str(self.probe_cfg.url), headers=headers)
                r.raise_for_status()
                duration = time.time() - start
                http_latency = duration * 1000.0
                content_len = len(r.content or b"")
                if duration > 0 and content_len > 0:
                    throughput_kbps = (content_len * 8.0) / (duration * 1024.0)
                success = True
                latency_ms = http_latency
        except Exception as e:
            logger.debug("probe proxy %s failed: %s", proxy_id, e)
            success = False
            latency_ms = (time.time() - tcp_start) * 1000.0
            throughput_kbps = 0.0

        async with self._lock:
            lst = self._samples.setdefault(proxy_id, [])
            lst.append((ts, latency_ms, throughput_kbps, success))
            cutoff = time.time() - (self.probe_cfg.history_minutes * 60)
            self._samples[proxy_id] = [s for s in lst if s[0] >= cutoff]
            # update state
            if len(self._samples[proxy_id]) < self.probe_cfg.min_samples:
                self._states[proxy_id] = 'warming'
            else:
                failures = sum(1 for (_, _, _, ok) in self._samples[proxy_id] if not ok)
                if failures / max(1, len(self._samples[proxy_id])) > 0.5:
                    self._states[proxy_id] = 'degraded'
                else:
                    self._states[proxy_id] = 'normal'

    async def _probe_cycle(self):
        proxies = [p.id for p in self.proxy_store.list() if p.enabled]
        if not proxies:
            await asyncio.sleep(self.probe_cfg.interval)
            return

        # If domain_index provided, probe per (domain, proxy) with per-domain semaphores
        domains = []
        if self.domain_index:
            domains = self.domain_index.recent(limit=self.probe_cfg.batch_domains)
        # default to a single None domain to probe proxies generally
        if not domains:
            domains = [None]

        async def _p(pid, domain):
            async with self._global_sem:
                async with self._get_proxy_sem(pid):
                    async with self._get_domain_sem(domain or '__global__'):
                        await self._probe_proxy(pid, domain)

        tasks = []
        for domain in domains:
            for pid in proxies:
                tasks.append(_p(pid, domain))

        await asyncio.gather(*tasks)

    def _percentile(self, values: List[float], q: float) -> float:
        if not values:
            return 0.0
        s = sorted(values)
        idx = (len(s) - 1) * (q / 100.0)
        lo = math.floor(idx)
        hi = math.ceil(idx)
        if lo == hi:
            return s[int(idx)]
        frac = idx - lo
        return s[lo] * (1 - frac) + s[hi] * frac

    def _iqr_filter(self, values: List[float]) -> List[float]:
        if len(values) < 4:
            return values
        q1 = self._percentile(values, 25)
        q3 = self._percentile(values, 75)
        iqr = q3 - q1
        low = q1 - 1.5 * iqr
        high = q3 + 1.5 * iqr
        return [v for v in values if v >= low and v <= high]

    def _compute_score_for_proxy(self, proxy_id: str) -> float:
        samples = self._samples.get(proxy_id, [])
        if not samples:
            return 50.0
        now = time.time()
        half_life = max(1, self.score_cfg.half_life_minutes)
        timestamps = [ts for (ts, _, _, _) in samples]
        latencies = [lat for (_, lat, _, _) in samples]
        throughputs = [tp for (_, _, tp, _) in samples]
        successes = [1 if ok else 0 for (_, _, _, ok) in samples]

        latencies_f = self._iqr_filter(latencies)
        throughputs_f = self._iqr_filter(throughputs)
        if not latencies_f:
            latencies_f = latencies
        if not throughputs_f:
            throughputs_f = throughputs

        weights = [math.exp(-math.log(2) * ((now - ts) / 60.0) / half_life) for ts in timestamps]
        w_sum = sum(weights) if sum(weights) > 0 else 1.0

        avg_latency = sum(latencies_f) / len(latencies_f) if latencies_f else 2000.0
        avg_throughput = sum(throughputs_f) / len(throughputs_f) if throughputs_f else 0.0
        reliability = sum(w * s for w, s in zip(weights, successes)) / w_sum

        lat_score = max(0.0, 100.0 * (1.0 - min(avg_latency, 2000.0) / 2000.0))
        rel_score = reliability * 100.0
        tp_max = getattr(self.score_cfg, 'throughput_max', 1024.0)
        tp_score = max(0.0, 100.0 * (min(avg_throughput, tp_max) / tp_max))

        total = (self.score_cfg.latency_weight * lat_score +
                 self.score_cfg.throughput_weight * tp_score +
                 self.score_cfg.reliability_weight * rel_score)
        return total

    def get_scores(self) -> Dict[str, float]:
        proxies = [p.id for p in self.proxy_store.list()]
        return {pid: self._compute_score_for_proxy(pid) for pid in proxies}

    def get_states(self) -> Dict[str, str]:
        return dict(self._states)

    def get_history(self) -> Dict[str, List[Tuple[float, float, float, bool]]]:
        return dict(self._samples)

    async def run_loop(self):
        self._running = True
        logger.info("ProbeEngine starting loop interval=%s", self.probe_cfg.interval)
        try:
            while self._running:
                await self._probe_cycle()
                await asyncio.sleep(self.probe_cfg.interval)
        except asyncio.CancelledError:
            logger.info("ProbeEngine cancelled")
        finally:
            self._running = False

    def stop(self):
        self._running = False
