"""上游代理注册表(内存态,支持 YAML 加载/保存)。

`ProxyStore` 维护 `id -> ProxyInfo` 的映射,供 Router 在竞速时按 id 取
代理节点。可选地绑定一个 YAML 文件路径,构造时自动加载,`save()` 时写回。

注意:本注册表非线程安全,但 Router 仅在 asyncio 事件循环中使用它
(读写都在同一线程),因此无需加锁。SQLite 那边的并发是另一回事
(见 router._db_lock)。
"""

from pathlib import Path
from typing import Dict, List, Optional

import yaml

from .config_schema import ProxyInfo


class ProxyStore:
    """内存代理注册表，支持从 YAML 文件加载/保存"""

    def __init__(self, path: Optional[str] = None):
        """可选传入 YAML 路径;若文件存在则立即加载到内存。"""
        self._proxies: Dict[str, ProxyInfo] = {}
        self.path = Path(path) if path else None
        if self.path and self.path.exists():
            self.load(self.path)

    def add(self, proxy: ProxyInfo):
        """添加/覆盖一个代理节点(以 id 为键)。"""
        self._proxies[proxy.id] = proxy

    def remove(self, proxy_id: str) -> Optional[ProxyInfo]:
        """按 id 移除节点,返回被移除的 ProxyInfo(不存在则返回 None)。"""
        return self._proxies.pop(proxy_id, None)

    def list(self) -> List[ProxyInfo]:
        """返回所有代理节点的列表(快照副本)。"""
        return list(self._proxies.values())

    def get(self, proxy_id: str) -> Optional[ProxyInfo]:
        """按 id 查找单个代理节点,不存在返回 None。"""
        return self._proxies.get(proxy_id)

    def save(self, path: Optional[str] = None):
        """把当前所有代理节点写入 YAML 文件。

        优先用传入的 path,其次用构造时绑定的 self.path;两者都没有则报错。
        用 yaml.safe_dump 序列化(每个 ProxyInfo 经 model_dump 转为 dict)。
        """
        p = Path(path) if path else self.path
        if not p:
            raise RuntimeError("No path provided to save proxies")
        data = [proxy.model_dump() for proxy in self._proxies.values()]
        p.write_text(yaml.safe_dump(data, sort_keys=False))

    def load(self, path: str):
        """从 YAML 文件加载代理节点,逐条 add 进内存(覆盖同 id)。

        空文件视为 0 个代理(不会报错)。
        """
        p = Path(path)
        data = yaml.safe_load(p.read_text()) or []
        for entry in data:
            proxy = ProxyInfo(**entry)
            self.add(proxy)
