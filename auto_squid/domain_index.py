from typing import Dict, List


class DomainIndex:
    """Tracks domain activity counts to prioritize probing and caching."""

    def __init__(self):
        self._domains: Dict[str, int] = {}

    def touch(self, domain: str):
        self._domains[domain] = self._domains.get(domain, 0) + 1

    def recent(self, limit: int = 100) -> List[str]:
        return sorted(self._domains.keys(), key=lambda d: -self._domains[d])[:limit]

    def reset(self):
        self._domains.clear()
