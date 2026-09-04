"""HTTP GET 响应缓存子系统(从 Router 拆出,见 #14 http_cache.py)。

`HttpCache` 持有 LRU + 容量上限的内存响应缓存:

- 幂等 GET 的成功响应(状态码 ∈ CACHEABLE_STATUS、未禁存)在内存缓存
  `_http_cache_ttl` 秒,命中滑动刷新 last_access(滑动 TTL + LRU 顺序);
- 写方法(POST/PUT/DELETE/PATCH, INVALIDATING_METHODS)在转发前经
  `_http_cache_invalidate` 按域名(O(K),二级索引)失效 GET 缓存,使变更后的
  GET 回源拿新内容;
- 在途 GET 去重聚合(Cache Stampede Protection)由 `_inflight_futures` 承担:
  同 URL 并发 GET 发现已有在途请求则 await 该 Future,不重复发上游。

`http_cache_hits`/`http_cache_misses` 计数器留在 Router(flow 直接自增,
snapshot_counters 直读)。流式响应转发的缓冲上限 `_stream_cache_limit` 也由
本类持有(Router._stream_upstream_response 经转发读取)。
"""

import asyncio
import logging
import time
import urllib.parse
from typing import Optional

logger = logging.getLogger(__name__)


# 可缓存的响应状态码。除 2xx 外,纳入幂等的 3xx 重定向(301/302/304)与
# 404/410——这些对象在真实流量中占比高且高度幂等:真实源站对随机路径常回
# 301/302/404,原"仅 2xx"策略使 HTTP 响应缓存对真实流量几乎完全失效
# (压测见 http_cache_entries_end 恒为 0)。5xx/4xx 中仅 404/410 安全可缓存
# (其余如 502 可能瞬态,缓存会放大故障)。
CACHEABLE_STATUS = frozenset({200, 203, 300, 301, 302, 304, 404, 410})

# 会改写资源的请求方法。命中即失效该 URL 的 GET 响应缓存,使随后的 GET
# 回源拿到变更后的内容,而不是 60s TTL 内返回变更前的旧响应体。缓存键为
# "GET:<url>",写方法不会被缓存(_http_cache_set 对非 GET 直接 return),
# 故失效只需按同一 URL 删 GET 条目;无需前缀扫描。
_INVALIDATING_METHODS = frozenset({'POST', 'PUT', 'DELETE', 'PATCH'})


class HttpCache:
    """HTTP GET 响应缓存(HttpCache 协作类,Router 持有 self.httpcache)。

    全部状态由事件循环单线程读写。Router 经类尾 `_CACHE_FORWARD` 白名单
    __getattr__/__setattr__ 转发本类的字段与方法,使热路径(_handle_http_request/
    _forward_single/_stream_upstream_response)与测试的 `self._http_cache_*`
    引用原样解析。
    """

    def __init__(self, enable_http_cache: bool = True, ttl: int = 60,
                 max_entries: int = 10_000, max_bytes: int = 256 * 1024 * 1024,
                 stream_limit: int = 1 * 1024 * 1024):
        # 缓存门开关:enable_http_cache=False 时 _http_cache_get 一律未命中(用于
        # 压测隔离缓存层,测纯路由性能)。
        self.enable_http_cache = enable_http_cache
        # P2:LRU + 容量上限。普通 dict 升级为 OrderedDict + 访问时间戳:
        #   - _http_cache_get 命中时刷新 last_access(滑动 TTL + LRU 顺序);
        #   - _http_cache_set 写入前检查 max_entries / max_bytes,超限淘汰
        #     last_access 最旧的条目(避免高基数 URL 下内存无界增长)。
        # 二级索引 _http_cache_domain_index 与淘汰同步维护,防漏删。
        self._http_cache: dict[str, dict] = {}
        self._http_cache_ttl = max(1, ttl)
        self._http_cache_max_entries = max(1, max_entries)
        self._http_cache_max_bytes = max(1, max_bytes)
        # 流式转发时响应 body 的缓冲上限(超过放弃缓存该响应)。原为 router 模块
        # 常量 STREAM_CACHE_LIMIT,现由配置 http_cache.stream_cache_limit 控制。
        self._stream_cache_limit = max(1024, stream_limit)
        self._http_cache_bytes = 0           # 当前缓存 body 总字节数
        self.http_cache_evictions = 0        # 累计淘汰次数(LRU 或 TTL)
        # 二级索引: domain → set[缓存键]。使 _http_cache_invalidate 从 O(N) 降为
        # O(K)(K=该域名条目数)。_http_cache_set 写入时同步更新,_http_cache_get
        # 过期清除时同步删除。索引与主 dict 无锁(均在同一个 asyncio 线程)。
        self._http_cache_domain_index: dict[str, set[str]] = {}
        # 在途 GET 去重聚合(Cache Stampede Protection): key = _http_cache_key('GET', url)
        # → 该 URL 首个转发上游的请求持有的 Future。同 URL 并发 GET 发现已有在途请求
        # 则 await 该 Future 拿首个请求的结果,不重复发上游。Future 结果为
        # (status_code, reason_phrase, headers, content) 或 None(上游失败,waiter 自行竞速)。
        self._inflight_futures: dict[str, asyncio.Future] = {}

    def _http_cache_key(self, method: str, url: str) -> str:
        """响应缓存键:"方法:URL"。仅 GET 缓存,故方法实际恒为 GET。"""
        return f"{method}:{url}"

    def _http_cache_get(self, method: str, url: str, headers=None) -> Optional[dict]:
        """取 GET 的缓存响应;非 GET 或未命中或已过期返回 None。过期项顺便清除。

        enable_http_cache=False 时一律未命中(用于压测隔离缓存层,测纯路由性能)。
        P2:命中刷新 last_access(滑动 TTL 兼作 LRU 顺序);过期项按 LRU 淘汰
        路径清除(同步维护 _http_cache_bytes 与二级索引)。

        headers(审计 P2#2):传入本次请求头时,若携带 Cookie / Authorization /
        Proxy-Authorization 等"随客户端变化"的私密头,则视为未命中——共享缓存
        键只含 method:url,若把某客户端的个性化响应直接给另一客户端会造成
        跨用户数据串读。缺省(None)保留旧行为(仅聚合/内务路径用)。
        """
        if not self.enable_http_cache or method != 'GET':
            return None
        if headers:
            # 大小写不敏感地检查私密头:携带即不命中共享缓存,回源取各自内容。
            for k in headers:
                lk = k.lower()
                if lk in ('cookie', 'authorization', 'proxy-authorization'):
                    return None
        key = self._http_cache_key(method, url)
        entry = self._http_cache.get(key)
        if not entry:
            return None
        now = time.time()
        if now - entry['cached_at'] > self._http_cache_ttl:
            self._http_cache_remove(key)
            return None
        entry['last_access'] = now  # 滑动 TTL + LRU 顺序
        return entry

    def _http_cache_remove(self, key: str) -> None:
        """从 _http_cache 删除 key,同步维护字节计数与 _http_cache_domain_index。

        所有删除路径统一走这里(过期清除、LRU 淘汰、写方法按域名失效),确保
        _http_cache_bytes 与二级索引不漏不重。返回是否删除了条目。
        """
        entry = self._http_cache.pop(key, None)
        if entry is None:
            return False
        self._http_cache_bytes -= len(entry.get('content', b'') or b'')
        cached_url = key[len('GET:'):] if key.startswith('GET:') else key
        cached_host = urllib.parse.urlparse(cached_url).hostname or cached_url
        idx = self._http_cache_domain_index.get(cached_host)
        if idx:
            idx.discard(key)
            if not idx:
                del self._http_cache_domain_index[cached_host]
        return True

    def _http_cache_set(self, method: str, url: str, status_code, reason_phrase, headers, content, request_headers=None) -> None:
        """缓存一个 GET 可缓存响应(状态码、原因、头、body、时间戳)。

        可缓存状态码由调用方按 CACHEABLE_STATUS 判断。无论上游是否给出
        Content-Length,都遵循 Cache-Control 的 no-store/no-cache/private:
        本代理是共享缓存(为多客户端服务),private 明确禁止共享缓存存储,
        no-store/no-cache 同理。原实现仅在缺 Content-Length 时查 Cache-Control,
        扩展到 3xx/404 后必须无条件查,否则会把源站标 private 的 302 也缓存。

        审计 P2#2:request_headers 传入本次请求头时,若请求携带 Cookie /
        Authorization / Proxy-Authorization 等私密头,则不写入共享缓存——该
        响应是"面向请求者"的个性化内容,存进 method:url 键会被无凭据的
        后续客户端命中串读。与 _http_cache_get 的对称检查配套:读和写两侧都
        按同一套私密头集合收发一致,避免"漏读"或"污染"两条泄漏路径。

        P2:写入前按 max_entries / max_bytes 做 LRU 淘汰(淘汰 last_access 最旧
        的条目),并维护 _http_cache_bytes。单一超大响应(>max_bytes 的一半)不缓存,
        避免单条即打满总预算。更新已有 key 时先归还旧字节再计入新字节。
        """
        if method != 'GET':
            return
        if request_headers:
            for k in request_headers:
                lk = k.lower()
                if lk in ('cookie', 'authorization', 'proxy-authorization'):
                    return
        # 共享缓存必须尊重源站的 Cache-Control 禁存指令(无论是否有
        # Content-Length)。no-cache 在此保守按"不存"处理:本代理不做再校验
        # (发条件请求),存了也只是徒增一次过期清除,不如直接不存。
        # headers 为 list[(name, value)](保留重复头)或 dict,两种都按 (k, v) 迭代。
        items = headers.items() if isinstance(headers, dict) else headers
        cc = next((v for k, v in items if k.lower() == 'cache-control'), '')
        if 'no-store' in cc or 'no-cache' in cc or 'private' in cc:
            return
        size = len(content or b'')
        if size > self._http_cache_max_bytes // 2:
            return  # 单一超大响应不缓存
        now = time.time()
        key = self._http_cache_key(method, url)
        # 更新已有 key:先归还旧字节,避免重复计数。
        old = self._http_cache.get(key)
        if old is not None:
            self._http_cache_bytes -= len(old.get('content', b'') or b'')
        self._http_cache[key] = {
            'status_code': status_code,
            'reason_phrase': reason_phrase,
            'headers': headers,
            'content': content,
            'cached_at': now,
            'last_access': now,
        }
        self._http_cache_bytes += size
        # 容量保护(LRU):条目数或字节数超限 → 淘汰 last_access 最旧的条目,
        # 直至回到上限以内。单次 O(N),写入远低于读取频率,可接受。
        while len(self._http_cache) > self._http_cache_max_entries \
                or self._http_cache_bytes > self._http_cache_max_bytes:
            oldest = min(self._http_cache, key=lambda k: self._http_cache[k].get('last_access', 0.0))
            self._http_cache_remove(oldest)
            self.http_cache_evictions += 1
        # 同步更新二级索引:缓存键 -> 域名,供 O(1) 域名级批量失效。
        cached_host = urllib.parse.urlparse(url).hostname or url
        self._http_cache_domain_index.setdefault(cached_host, set()).add(key)

    def _http_cache_invalidate(self, domain: str) -> None:
        """清空某域名下所有 GET 响应缓存条目(利用二级索引 O(K),K=该域名条目数)。

        写方法(POST/PUT/DELETE/PATCH)改写资源后调用。按域名而非按 URL 失效:
        添加动作常打 POST /api/items,而刷新的列表页是 GET /,两者 URL 不同,
        按 URL 精确失效会漏掉列表页缓存,导致刷新仍返回旧内容。整域清空可覆盖
        同站任意路径的 GET。利用 _http_cache_domain_index 直接取该域名下所有
        缓存键,避免 O(N) 遍历全量缓存与逐条重复 urlparse。
        提前(转发前)失效:即便写请求最终失败,后果也仅是下次 GET 多回源一次,
        不会返回错误内容。enable_http_cache=False 时缓存本就空,此处为空操作。
        """
        stale = self._http_cache_domain_index.pop(domain, set())
        for key in stale:
            self._http_cache_remove(key)