"""HTTP GET 响应缓存子系统(从 Router 拆出,见 #14 http_cache.py)。

`HttpCache` 持有 LRU + 容量上限的内存响应缓存:

- 幂等 GET 的成功响应(状态码 ∈ CACHEABLE_STATUS、未禁存)在内存缓存
  `_http_cache_ttl` 秒(TTL 按 cached_at 硬过期),命中刷新 last_access 并挪到
  OrderedDict 队尾(LRU 顺序),超容量时从队首 O(1) 淘汰;
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
import re
import time
import urllib.parse
from collections import OrderedDict
from typing import Optional

logger = logging.getLogger(__name__)


# 可缓存的响应状态码。除 2xx 外,纳入幂等的 3xx 重定向(301/302)与
# 404/410——这些对象在真实流量中占比高且高度幂等:真实源站对随机路径常回
# 301/302/404,原"仅 2xx"策略使 HTTP 响应缓存对真实流量几乎完全失效
# (压测见 http_cache_entries_end 恒为 0)。5xx/4xx 中仅 404/410 安全可缓存
# (其余如 502 可能瞬态,缓存会放大故障)。
#
# 304 **不可**缓存(RFC 9110 §15.4.5):它不是资源表示,而是"你本地那份仍有效"
# 的引用。缓存键只有 method:url,存下空体 304 后,后续**无条件** GET 命中就会
# 回一个 304 + 空 body —— 客户端被告知"用本地副本",而它手上什么都没有,
# 结果是空白页;而真正可用的 200 表示并不在这条缓存里,也无从再校验。
CACHEABLE_STATUS = frozenset({200, 203, 300, 301, 302, 404, 410})

# 按定义没有 body 的响应状态(RFC 9110 §6.4.1 把 204/304 与 1xx 明确排除)。
# 转发时既不能补 Content-Length 也不能加 Transfer-Encoding: chunked —— 给 304
# 套上 chunked 框架,客户端会把随后的字节当成本该为空的 body。
NO_BODY_STATUS = frozenset({204, 304})

# 会改写资源的请求方法。命中即失效该 URL 的 GET 响应缓存,使随后的 GET
# 回源拿到变更后的内容,而不是 60s TTL 内返回变更前的旧响应体。缓存键为
# "GET:<url>",写方法不会被缓存(_http_cache_set 对非 GET 直接 return),
# 故失效只需按同一 URL 删 GET 条目;无需前缀扫描。
_INVALIDATING_METHODS = frozenset({'POST', 'PUT', 'DELETE', 'PATCH'})

# 域名维度键的最大长度:DNS 主机名上限 253 字符,超出即视为伪造/垃圾输入。
DOMAIN_KEY_MAX_LEN = 253


def domain_key_from_url(url: str) -> str:
    """域名维度(缓存二级索引 / 统计 / 粘性 / meta)统一的键:取 URL 的 hostname。

    常见写法 `urlparse(url).hostname or url` 有两个问题:

    1. hostname 取不到时回退成**整段原文**。origin-form 请求
       (`GET /<svg onload=alert(1)> HTTP/1.1`,客户端直连本代理即可构造)会把
       请求行原文变成键:管理面板拼进 innerHTML 即是 XSS 源头,而且每个畸形
       路径都是一条永久条目(键空间无界)。
    2. 非法 IPv6 字面量(`http://[not::ipv6]/x`)让 urlparse **抛 ValueError**
       —— 而本函数被 _http_cache_remove/_set 这类"响应已写出之后"的内务路径调用,
       一次崩溃会把整条转发失败变成一个本该静默的记账错误。

    兜底不再回原文:净化到 URL 语法字符并截断,既保留"这是哪个请求"的可读线索,
    又不让 HTML 元字符或超长字符串进入键。
    """
    try:
        host = urllib.parse.urlparse(url).hostname
    except ValueError:
        host = None
    if host:
        return host
    cleaned = re.sub(r'[^A-Za-z0-9._~:/?#\[\]@!$()*+,;=%-]', '_',
                     url)[:DOMAIN_KEY_MAX_LEN]
    return cleaned or 'unknown'


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
        # P2:LRU + 容量上限。用 OrderedDict 把"最近使用顺序"直接编码进键顺序:
        #   - _http_cache_get 命中刷新 last_access 并 move_to_end(硬 TTL 仍按
        #     cached_at 过期,不随命中滑动);
        #   - _http_cache_set 写入(含覆盖已有 key)同样 move_to_end;
        #   - 超限淘汰 = 取队首 next(iter(...)),即 last_access 最旧的一条。
        # 淘汰必须是 O(1):默认 max_entries=10_000 在真实流量下很容易达到,而每次
        # 回源写入都要做容量检查;原实现用 min(全表, key=last_access) 扫描,缓存一满
        # 就退化成"每个缓存写入 O(N) 的事件循环停顿"(10k 条 ≈ 毫秒级,叠加高 QPS
        # 直接吃掉整个循环的预算,拖慢所有在途请求)。
        # 二级索引 _http_cache_domain_index 与淘汰同步维护,防漏删。
        self._http_cache: "OrderedDict[str, dict]" = OrderedDict()
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
        entry['last_access'] = now  # LRU 热度位点(TTL 仍按 cached_at 硬过期)
        # 命中即"最近使用":挪到队尾,使队首恒为 last_access 最旧的条目。move_to_end
        # 是 O(1),而省掉的正是写入侧的 O(N) 全表 min() 扫描(见 __init__ 注释)。
        self._http_cache.move_to_end(key)
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
        cached_host = domain_key_from_url(cached_url)
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
        items = list(headers.items() if isinstance(headers, dict) else headers)
        # 当前缓存键只含 method:url，无法安全表达响应按请求头变化的表示。
        # Set-Cookie 也绝不能进入共享缓存，否则会被后续客户端重放。
        if any(k.lower() == 'set-cookie' for k, _ in items):
            return
        if any(k.lower() == 'vary' and v.strip() for k, v in items):
            return
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
        # 新写入/覆盖都算"最近使用":OrderedDict 里对已存在的 key 赋值不会移动位置,
        # 必须显式挪到队尾,否则刚回源刷新的条目会滞留在队首被优先淘汰。
        self._http_cache.move_to_end(key)
        # 容量保护(LRU):条目数或字节数超限 → 从队首淘汰 last_access 最旧的条目,
        # 直至回到上限以内。队首即最久未访问(键顺序 = 访问顺序),取键 O(1);删除
        # 仍统一走 _http_cache_remove,保证字节数与二级索引不漏不重。
        while len(self._http_cache) > self._http_cache_max_entries \
                or self._http_cache_bytes > self._http_cache_max_bytes:
            oldest = next(iter(self._http_cache), None)
            if oldest is None:
                break
            self._http_cache_remove(oldest)
            self.http_cache_evictions += 1
        # 同步更新二级索引:缓存键 -> 域名,供 O(1) 域名级批量失效。
        cached_host = domain_key_from_url(url)
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
