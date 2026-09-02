"""上游代理注册表(内存态,支持 YAML 加载/保存)。

`ProxyStore` 维护 `id -> ProxyInfo` 的映射,供 Router 在竞速时按 id 取
代理节点。可选地绑定一个 YAML 文件路径,构造时自动加载,`save()` 时写回。

注意:本注册表非线程安全,但 Router 仅在 asyncio 事件循环中使用它
(读写都在同一线程),因此无需加锁。SQLite 那边的并发是另一回事
(见 router._db_lock)。
"""

import urllib.parse
from pathlib import Path
from typing import Dict, List, Optional

import yaml

from .config_schema import ProxyInfo


class ProxyStore:
    """内存代理注册表，支持从 YAML 文件加载/保存"""

    def __init__(self, path: Optional[str] = None):
        """可选传入 YAML 路径;若文件存在则立即加载到内存。"""
        self._proxies: Dict[str, ProxyInfo] = {}
        # pid → 预计算代理 URL(http://[user:pw@]host:port)缓存:代理节点静态,
        # URL 构造含 urlquote(逐字节扫描),缓存消除每次单发/竞速的重复 quote。
        # 在 add/remove/load 处失效——这三个入口是仅有的变更点。
        self._url_cache: Dict[str, Optional[str]] = {}
        self.path = Path(path) if path else None
        if self.path and self.path.exists():
            self.load(self.path)

    def add(self, proxy: ProxyInfo):
        """添加/覆盖一个代理节点(以 id 为键)。"""
        self._proxies[proxy.id] = proxy
        self._url_cache.pop(proxy.id, None)

    def remove(self, proxy_id: str) -> Optional[ProxyInfo]:
        """按 id 移除节点,返回被移除的 ProxyInfo(不存在则返回 None)。"""
        self._url_cache.pop(proxy_id, None)
        return self._proxies.pop(proxy_id, None)

    def list(self) -> List[ProxyInfo]:
        """返回所有代理节点的列表(快照副本)。"""
        return list(self._proxies.values())

    def get(self, proxy_id: str) -> Optional[ProxyInfo]:
        """按 id 查找单个代理节点,不存在返回 None。"""
        return self._proxies.get(proxy_id)

    def proxy_url(self, proxy_id: str) -> Optional[str]:
        """取某代理的预计算 URL(http://[user:pw@]host:port),带缓存。

        首次计算后缓存于 _url_cache(add/remove/load 时失效),消除热路径
        每次单发/竞速重复的 urllib.parse.quote 与字符串拼接。返回 None
        表示代理不存在(与 get 语义一致)。本类非线程安全,但仅在 asyncio
        事件循环单线程中使用(见文件头注释),缓存读写无需加锁。
        """
        proxy = self._proxies.get(proxy_id)
        if proxy is None:
            return None
        cached = self._url_cache.get(proxy_id)
        if cached is not None:
            return cached
        # 构造逻辑与 Router._build_proxy_url 保持一致(见其注释)。
        url = f"http://{proxy.host}:{proxy.port}"
        if proxy.auth:
            user = urllib.parse.quote(proxy.auth.get('username', ''), safe='')
            pw = urllib.parse.quote(proxy.auth.get('password', ''), safe='')
            url = f"http://{user}:{pw}@{proxy.host}:{proxy.port}"
        self._url_cache[proxy_id] = url
        return url

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
        # load 可能整体替换节点集(如管理 API 重载),失效全部缓存。
        self._url_cache.clear()
