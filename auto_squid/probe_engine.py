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
    """Minimal probe engine that probes known proxies and keeps recent samples.

    Stores per-proxy samples: list of (ts, latency_ms, success_bool)
    """

    def __init__(self, proxy_store: ProxyStore, probe_cfg: ProbeConfig | None = None, score_cfg: ScoreConfig | None = None):
        self.proxy_store = proxy_store
        self.probe_cfg = probe_cfg or ProbeConfig()
        self.score_cfg = score_cfg or ScoreConfig()
        self._samples: Dict[str, List[Tuple[float, float, bool]]] = {}
        self._running = False
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def _probe_proxy(self, proxy_id: str):
        proxy = self.proxy_store.get(proxy_id)
        if not proxy or not proxy.enabled:
            return
        host = proxy.host
        port = proxy.port
        proxy_url = f"http://{host}:{port}"
        ts = time.time()
        # TCP connect time
        tcp_start = time.time()
        try:
            reader, writer = await asyncio.open_connection(host, port)
            tcp_latency = (time.time() - tcp_start) * 1000.0
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            # HTTP GET via proxy to probe URL to measure full round trip
            async with httpx.AsyncClient(proxies={"http://": proxy_url, "https://": proxy_url}, timeout=self.probe_cfg.timeout) as client:
                start = time.time()
                r = await client.get(str(self.probe_cfg.url))
                r.raise_for_status()
                http_latency = (time.time() - start) * 1000.0
                success = True
                latency_ms = http_latency
        except Exception as e:
            logger.debug("probe proxy %s failed: %s", proxy_id, e)
            success = False
            latency_ms = (time.time() - tcp_start) * 1000.0

        async with self._lock:
            self._samples.setdefault(proxy_id, []).append((ts, latency_ms, success))
            # trim history to configured history_minutes
            cutoff = time.time() - (self.probe_cfg.history_minutes * 60)
            self._samples[proxy_id] = [s for s in self._samples[proxy_id] if s[0] >= cutoff]

    async def _probe_cycle(self):
        proxies = [p.id for p in self.proxy_store.list() if p.enabled]
        if not proxies:
            await asyncio.sleep(self.probe_cfg.interval)
            return
        sem = asyncio.Semaphore(self.probe_cfg.concurrency)
        async def _p(pid):
            async with sem:
                await self._probe_proxy(pid)
        await asyncio.gather(*[_p(pid) for pid in proxies])

    def _compute_score_for_proxy(self, proxy_id: str) -> float:
        # Compute combined score 0-100 where higher is better
        samples = self._samples.get(proxy_id, [])
        if not samples:
            return 50.0  # neutral
        now = time.time()
        half_life = max(1, self.score_cfg.half_life_minutes)
        weights = [math.exp(-math.log(2) * ((now - ts) / 60.0) / half_life) for ts, _, _ in samples]
        latencies = [lat for (_, lat, ok) in samples]
        successes = [1 if ok else 0 for (_, _, ok) in samples]
        # simple weighted averages
        w_sum = sum(weights) if sum(weights) > 0 else 1.0
        avg_latency = sum(w * l for w, l in zip(weights, latencies)) / w_sum
        reliability = sum(w * s for w, s in zip(weights, successes)) / w_sum
        # map latency to score (assuming 2000ms -> 0, 0ms -> 100)
        lat_score = max(0.0, 100.0 * (1.0 - min(avg_latency, 2000.0) / 2000.0))
        rel_score = reliability * 100.0
        total = (self.score_cfg.latency_weight * lat_score + self.score_cfg.reliability_weight * rel_score)
        # normalize if throughput weight present (we don't measure throughput yet)
        # assume throughput neutral 50
        total = total + (self.score_cfg.throughput_weight * 50.0)
        return total

    def get_scores(self) -> Dict[str, float]:
        return {pid: self._compute_score_for_proxy(pid) for pid in self._samples.keys()}

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
