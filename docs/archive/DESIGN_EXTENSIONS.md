# Auto-Squid — Design Extensions

This file contains clarifications and concrete implementation guidance to supplement DESIGN.md. It collects the P0 and P1 items (HTTPS/transparent proxy details, security & credentials, API security, DB ops, probing & scoring refinements, router performance, and observability).

1) HTTPS / Transparent proxy (implementation constraints and recommendations)

Summary
- Prefer explicit proxy mode by default (user configures browser/system to use 127.0.0.1:10808). It is cross-platform and gives straightforward access to CONNECT host:port.
- Transparent mode (iptables REDIRECT or TPROXY) is supported on Linux but has constraints for HTTPS: to make routing decisions by domain you must obtain the domain name without decrypting TLS, typically by extracting SNI from the ClientHello or by preserving original destination via TPROXY/conntrack.

Details
- Explicit proxy mode (recommended initial target):
  - The client sends a CONNECT <host:port> line for TLS; the router can parse the CONNECT target and select a proxy. This requires no TLS parsing.
  - Works on Linux, macOS, Windows and is simple to implement.

- Transparent proxy mode (Linux):
  - If you use REDIRECT (iptables -t nat -A OUTPUT -p tcp --dport 443 -j REDIRECT --to-port 10808), the kernel rewrites the destination and the user-space listener sees a connection from client to local port, losing the original destination address; you must retrieve the original destination with getsockname/getsockopt(SO_ORIGINAL_DST) (netfilter) or use TPROXY so the kernel preserves original IP.
  - To resolve domain for HTTPS without decrypting TLS, extract SNI from the TLS ClientHello (inspecting the first bytes of the TLS handshake). This is best-effort: SNI may be absent (rare) or encrypted in future protocols (ECH/Encrypted Client Hello). ECH will make SNI unavailable; handle by fallback rules.

Linux example (TPROXY + nft/iptables hints)
- Basic REDIRECT (HTTP only):
  - sudo iptables -t nat -A OUTPUT -p tcp --dport 80 -j REDIRECT --to-ports 10808
- TPROXY requires more setup (capabilities, routing table). High-level references (do not copy blindly):
  - Use iptables mangle/TPROXY and set socket options.
  - Consider using nftables for newer systems.

SNI extraction (high-level pseudocode)
- Read the first bytes from client connection (non-blocking) up to TLS ClientHello length; parse TLS record header; locate ClientHello and SNI extension. Libraries exist (python-construct snippets, or use custom lightweight parser).

Python (very small sketch: not production-ready):

```
# pseudo: read initial bytes and parse TLS ClientHello to find SNI
# Use a library or carefully implement parsing; consider timeouts and partial reads
data = await reader.readexactly(5)  # TLS record header
# parse lengths, read more as needed, find extensions and SNI
```

Fallbacks
- If SNI not available or cannot be parsed, fall back to:
  1) Use the IPv4/IPv6 destination address (if available) and a DNS reverse mapping / recent DNS cache -> domain mapping
  2) Use global scoring (pick globally best proxy)
  3) Or, if allowed, perform quick TLS handshake probing via a trusted endpoint through candidate proxies (costly)

Platform differences
- Linux: full transparent mode possible (TPROXY, SO_ORIGINAL_DST). Documentation must instruct that admin privileges are needed.
- macOS/Windows: prefer explicit proxy mode; transparent mode may be possible with platform-specific kernel extensions (outside scope for v1).

2) Security & Credentials

Goals
- Avoid storing proxy credentials in plaintext files checked into disk or git.
- Default admin API binding should be localhost only.

Recommended options for storing credentials
- OS Keyring (recommended for desktop installs):
  - Use `keyring` (python) to store passwords: keyring.set_password("auto-squid", proxy_id, password)
  - On retrieval: keyring.get_password("auto-squid", proxy_id)
  - Pros: integrates with OS credential storage; avoids plaintext on disk.
  - Cons: automated headless servers may lack UI-based keyring; consider alternative.

- Encrypted config file + master password
  - Use libsodium or Fernet (cryptography) to encrypt credentials in the config file. On startup, user provides master password or the app reads it from an env var.
  - Provide a helper: `auto-squid encrypt-creds --in proxies.yaml --out proxies.enc` and `auto-squid decrypt-creds`.

- In-memory only (ephemeral)
  - CLI could accept credentials at runtime and never write to disk (good for ephemeral sessions but inconvenient for long-running services).

API access & tokens
- Default: management API binds to 127.0.0.1 and requires no auth for local use.
- If remote access is enabled, require at least API token in Authorization header (Bearer). Provide CLI helper to generate tokens and revoke them.
- For high-security deployments, recommend mutual TLS (mTLS) on management API.

Audit logging
- All mutating API calls should be logged with timestamp, client IP, actor (if authenticated), resource id, action, and result. Store minimal logs in route_log and a separate audit table if needed.

3) API Security (best practices)

- Default binding: 127.0.0.1:18080.
- If user config allows remote binding, require explicit `--bind-remote` flag and show a warning about exposing the API.
- Use an Authorization header: `Authorization: Bearer <token>`; server validates token against stored hashed tokens (use HMAC or bcrypt on tokens) and logs actor.
- Rate-limit admin endpoints to avoid brute-force attempts.

4) Database (SQLite) operations & maintenance

Startup pragmas (recommended)

```
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000;  -- milliseconds
PRAGMA cache_size = -2000;   -- approx 2 MB (or tune per host)
```

Schema versioning
- Add schema_version table to track applied migrations:

```
CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at REAL NOT NULL
);
```

Migration runner
- On startup, check current version and apply missing migrations sequentially from migrations/NNN_description.sql
- Provide a simple migration utility (python script) to run migrations with safety checks and backups.

Backup & recovery
- Backup using SQLite `.backup()` API or `VACUUM INTO 'backup.db'` for SQLite >= 3.27.
- Periodic backups recommended (configure retention and rotation). Example cron:
  - nightly: auto-squid backup --out /var/backups/auto-squid/backup-$(date +%F).db

Index & performance
- The provided idx_probes_domain_proxy_ts index is good for querying recent probes by domain+proxy.
- If you frequently query domain_scores by computed_at, consider an index on that column.

5) Probing & scoring algorithm refinements

Key parameters (defaults)
- probe.interval_seconds = 60
- probe.history_minutes = 10
- probe.half_life_minutes = 5
- probe.min_samples = 3
- probe.domain_batch_size = 10
- probe.per_domain_concurrency = 20
- probe.global_concurrency_cap = 200

Outlier handling
- Use IQR or z-score to detect and drop extreme probe latencies before computing component scores.
- Example: drop samples with z-score > 3 or outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR].

Weighted time decay
- For each probe sample compute weight = exp(-ln(2) * age_minutes / half_life_minutes).
- Apply weight when aggregating component scores.

Minimum samples & cold start
- Require at least min_samples successful probes to compute a stable score. If less than min_samples:
  - Use TCP connect latency as a temporary proxy for L_score.
  - Increase reliability weight until sufficient samples collected.
  - Mark domain as "preheated" after one full round of probes.

Pseudocode for computing score

```
def compute_score(probe_samples, weights):
    # probe_samples: list of {timestamp, latency_ms, throughput_kbps, success}
    # apply outlier removal
    samples = filter_outliers(probe_samples)
    if len(samples) < min_samples:
        return fallback_global_score_or_tcp()
    # compute component scores per sample (0-100)
    for s in samples:
        l_score = clamp(100 - (s.latency_ms / latency_max) * 100, 0, 100)
        t_score = clamp((s.throughput_kbps / throughput_max) * 100, 0, 100)
        r_score = compute_recent_success_rate(samples)
        s.weighted = weights.alpha * l_score + weights.beta * t_score + weights.gamma * r_score
        s.time_weight = exp(-ln(2) * age_minutes / half_life)
    score = sum(s.weighted * s.time_weight for s in samples) / sum(s.time_weight for s in samples)
    return score
```

Rate limiting & scheduling
- Maintain a global semaphore for probe concurrency (global_concurrency_cap) and per-domain semaphore (per_domain_concurrency).
- Prioritize domains by last_seen and probe_priority.

6) Router performance & connection handling

Key concerns
- Keep-alive and upstream (Squid) connection reuse improves latency and resource usage. Consider pooling connections to each Squid backend.
- Bound per-proxy concurrent connections to avoid overwhelming backends.
- Implement backpressure: when forwarding data from upstream to client, avoid buffering entire responses; stream and respect TCP flow control. Use asyncio streams or sockets with backpressure-aware loops.

Suggested parameters
- default upstream keepalive pool size: 10 per proxy (configurable)
- per-proxy max connections: 100 (configurable)
- client read/write timeouts: read_timeout=30s, write_timeout=60s

Connection reuse sketch
- Maintain an async connection pool keyed by proxy_id. When sending a new HTTP request, try to reuse an idle connection; otherwise open a new one.
- For CONNECT tunnels (HTTPS), the router usually must establish a TCP tunnel to the selected proxy and then relay raw TCP. Connection reuse is less useful for CONNECT because the client expects a direct tunnel, but you can still reuse connections for subsequent requests from same client if the client uses proxy keepalive.

7) Observability

Metrics (Prometheus names)
- auto_squid_probe_latency_ms_bucket{domain,proxy_id}
- auto_squid_probe_success_total{domain,proxy_id}
- auto_squid_probe_fail_total{domain,proxy_id}
- auto_squid_route_requests_total{domain,proxy_id}
- auto_squid_active_connections
- auto_squid_domain_queue_length
- auto_squid_db_write_errors_total

Logs
- Use structured JSON logs with fields:
  - timestamp, level, module, domain, proxy_id, client_ip, action, duration_ms, status, error

/metrics endpoint
- Expose a /metrics endpoint compatible with Prometheus text format. Use `prometheus_client` library or hand-rolled exposition.

Alerts (examples)
- Alert: probe_success_rate < 0.5 for 5 minutes for 10% of domains -> investigate network partition
- Alert: db_write_errors_total > 0 for 1 minute -> send pager

---

Appendix: next steps for implementation
- Add the above files to repository (SECURITY.md, DB.md, PROBING.md, OBSERVABILITY.md, config_schema.py) and a short DESIGN_EXTENSIONS.md referencing them.
- Implement a basic config validation using pydantic (config_schema.py) and add CLI to validate config.
- Implement a minimal probe runner and domain_index to validate the scoring design under load.
