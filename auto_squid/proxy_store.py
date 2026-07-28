from typing import Dict, List, Optional
from .config_schema import ProxyInfo


class ProxyStore:
    """In-memory proxy registry (stub)."""

    def __init__(self):
        self._proxies: Dict[str, ProxyInfo] = {}

    def add(self, proxy: ProxyInfo):
        self._proxies[proxy.id] = proxy

    def list(self) -> List[ProxyInfo]:
        return list(self._proxies.values())

    def get(self, proxy_id: str) -> Optional[ProxyInfo]:
        return self._proxies.get(proxy_id)
