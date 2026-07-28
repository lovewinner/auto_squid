# Database operations and maintenance (SQLite)

This document provides practical SQLite settings and migration/backup guidelines for Auto-Squid.

1) Startup PRAGMAs
- Recommended PRAGMAs to set on DB open (tune per environment):

```
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA busy_timeout = 5000; -- ms
PRAGMA foreign_keys = ON;
PRAGMA temp_store = MEMORY;
```

2) Schema versioning and migrations
- Create a schema_version table to track applied migrations.

```
CREATE TABLE IF NOT EXISTS schema_version (
  version INTEGER PRIMARY KEY,
  applied_at REAL NOT NULL
);
```

- Store migration files under `migrations/NNN_description.sql`. On startup, read current version and apply remaining migration files in order inside a transaction. Before applying migrations, run a backup.

3) Backup & restore
- Use `sqlite3` backup API or `VACUUM INTO 'backup.db'` (when available).
- Example backup script (python):

```python
import sqlite3
src = sqlite3.connect('perf.db')
dst = sqlite3.connect('backup.db')
with dst:
    src.backup(dst)
```

- Restore by stopping the service, replacing perf.db with backup.db, then starting the service and verifying integrity.

4) Indexes and query patterns
- Provided index: `idx_probes_domain_proxy_ts` focuses on recent probe queries.
- If you perform frequent domain_scores time-range queries, add indexes on computed_at.

5) Corruption handling
- In case of corruption, attempt `PRAGMA integrity_check;`. If corrupted, restore from latest backup and re-run any replayable operations (e.g., proxies.yaml is the source-of-truth for proxies).

6) Monitoring
- Monitor DB file size, probe insertion rate, and any busy-timeouts. Alert if `busy` errors or frequent `database is locked` errors occur.
