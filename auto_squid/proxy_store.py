from typing import Dict, List, Optional
from .config_schema import ProxyInfo
import yaml
from pathlib import Path


class ProxyStore:
    """内存代理注册表，支持从 YAML 文件加载/保存"""

    def __init__(self, path: Optional[str] = None):
        self._proxies: Dict[str, ProxyInfo] = {}
        self.path = Path(path) if path else None
        if self.path and self.path.exists():
            self.load(self.path)

    def add(self, proxy: ProxyInfo):
        self._proxies[proxy.id] = proxy

    def remove(self, proxy_id: str):
        return self._proxies.pop(proxy_id, None)

    def list(self) -> List[ProxyInfo]:
        return list(self._proxies.values())

    def get(self, proxy_id: str) -> Optional[ProxyInfo]:
        return self._proxies.get(proxy_id)

    def save(self, path: Optional[str] = None):
        p = Path(path) if path else self.path
        if not p:
            raise RuntimeError("No path provided to save proxies")
        data = [proxy.model_dump() for proxy in self._proxies.values()]
        p.write_text(yaml.safe_dump(data, sort_keys=False))

    def load(self, path: str):
        p = Path(path)
        data = yaml.safe_load(p.read_text()) or []
        for entry in data:
            proxy = ProxyInfo(**entry)
            self.add(proxy)
