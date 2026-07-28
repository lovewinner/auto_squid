# Probing & Scoring — implementation details

This document refines the probe scheduling, outlier handling, and scoring computation.

1) Goals
- Produce a stable, responsive score per (domain, proxy) that reflects recent performance.
- Avoid being misled by transient spikes or insufficient data.

2) Key parameters (defaults)
- history_minutes = 10
- half_life_minutes = 5
- min_samples = 3
- latency_max = configurable (used to map latency to 0-100)
- throughput_max = configurable

3) Outlier filtering
- Remove obvious outliers using IQR or z-score. Example (IQR):
  - q1, q3 = percentile(samples, 25), percentile(samples, 75)
  - iqr = q3 - q1
  - keep samples within [q1 - 1.5*iqr, q3 + 1.5*iqr]

4) Minimum sample handling & cold start
- If samples < min_samples, compute a provisional score using TCP connect latency or global proxy stats and mark the domain as "warming". After the first full round of probes, elevate to normal scoring.

5) Time-weighted aggregation
- For each sample, compute time weight: w = exp(-ln(2) * age_minutes / half_life_minutes)
- Combine per-sample component scores (latency->L_score, throughput->T_score, reliability->R_score) using configured α/β/γ weights.

6) Reliability component R_score
- Compute as recent_success_count / total_attempts * 100 over sliding window (history_minutes). Successful = probe completed and returned expected status within timeout.

7) Rate-limiting & scheduling
- Global concurrency cap (e.g., 200) using asyncio.Semaphore
- Per-domain cap (e.g., 20) and per-proxy cap to protect backends
- Schedule priority: domains discovered from router logs (recent activity) > periodic scan

8) Pseudocode: probe loop

```
async def run_probe_cycle(domain_list):
    async with global_semaphore:
        for domain in chunk(domain_list, batch_domains):
            # per-domain concurrency
            async with per_domain_semaphore:
                await probe_all_enabled_proxies(domain)
                update_domain_scores(domain)
```

9) Suggested probes per pair
- TCP connect time (fast, low bandwidth)
- HTTP GET to probe_url (204 endpoints) to measure full RTT and response
- Throughput: optional small download (e.g., 8KB) and measure kbps

10) Timeouts
- Per probe HTTP timeout: config.timeout (default 10s)
- Per probe TCP connect timeout: 3s

