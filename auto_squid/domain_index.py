from typing import Dict, List


class DomainIndex:
    """Simple domain activity index (stub).

    Tracks domains of recent interest so probe_engine can prioritize them.
    """

    def __init__(self):
        self._domains: Dict[str, int] = {}

    def touch(self, domain: str):
        self._domains[domain] = self._domains.get(domain, 0) + 1

    def recent(self, limit: int = 100) -> List[str]:
        # return domains sorted by activity
        return sorted(self._domains.keys(), key=lambda d: -self._domains[d])[:limit]
