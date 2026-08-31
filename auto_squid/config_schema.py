"""配置模型定义(基于 pydantic v2)。

所有模型均带默认值,因此缺失配置也能以合理默认行为启动。配置可通过
`config.yaml`(`--config` 传入)加载,结构为顶层各子配置块:

    listen:  代理监听地址/端口
    api:     管理 API 监听地址/端口
    router:  路由行为(竞速、缓存、客户端认证)
    logging: 日志级别/文件

`ProxyInfo` 是上游代理节点定义,由 `proxies.yaml` 加载(见 ProxyStore)。
"""

from typing import Dict, List, Optional
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ConfigBase(BaseModel):
    """配置模型基类:#12 把关。

    顶层/嵌套**配置**模型一律 `extra="forbid"`:拼错键(stagger_inital)或
    升级后改名(yaml 里残留旧键)在启动即硬报错,不再 pydantic 默认忽略未知键
    静默落默认。数据模型(ProxyInfo / ProbeCanaryConfig,由 proxies.yaml / 管理
    API 喂入)故意保持宽松,避免误伤运行时数据。
    """
    model_config = ConfigDict(extra="forbid")


class ListenConfig(ConfigBase):
    """代理端口的监听配置(面向客户端的 HTTP/CONNECT 代理端口)。"""
    host: str = Field("0.0.0.0")
    port: int = Field(10808)


class APIConfig(ConfigBase):
    """管理 API 的监听配置(独立于代理端口,默认 18080)。

    `auth` 复用 AuthConfig(默认关闭):开启后除 /health 外全部端点需 HTTP
    Basic 认证(经 api.py 的中间件统一校验)。注意 APIConfig 定义在 AuthConfig
    之后,故用字符串前向引用 + 模块末尾 model_rebuild() 解析。
    """
    host: str = Field("0.0.0.0")
    port: int = Field(18080)
    auth: "AuthConfig" = Field(default_factory=lambda: AuthConfig(),
                               description="管理 API 的 HTTP Basic 认证配置(默认关闭)")


class LoggingConfig(ConfigBase):
    """日志配置。`file` 为 None 时写默认文件 `auto_squid.log`。"""
    level: str = Field("INFO")
    file: Optional[str] = Field(None)

    # 合法级别集合(#13):把 level 拼写错误在加载期暴露,而不是静默落默认。
    _VALID_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}

    @model_validator(mode="after")
    def _check_level(self):
        if self.level.upper() not in self._VALID_LEVELS:
            raise ValueError(
                f"invalid logging.level {self.level!r} (expect one of "
                f"{sorted(self._VALID_LEVELS)})")
        return self


class ProxyInfo(BaseModel):
    """单个上游代理节点的配置。

    `auth` 是访问**该上游代理**所需的凭据(上游认证),会被拼进 httpx 的
    代理 URL 或 CONNECT 的 Proxy-Authorization;与客户端认证(auth.py)
    是两套独立机制。`tags` 预留给策略引擎使用(当前未启用)。
    """
    id: str
    name: Optional[str] = None
    host: str
    port: int = Field(3128)
    protocol: str = Field("http")
    auth: Optional[Dict[str, str]] = None
    enabled: bool = Field(True)
    tags: Optional[Dict[str, str]] = None


class AuthConfig(ConfigBase):
    """HTTP Basic 认证配置,同时用于:
    - 客户端访问**代理端口**的认证(开启后客户端每个请求都需带
      `Proxy-Authorization` 头,见 auth.check_auth);
    - 管理 API 的认证(api.auth,开启后除 /health 外全部端点需凭据)。

    默认 `enabled=False`(开放)。
    """
    enabled: bool = Field(False, description="要求 HTTP Basic 认证")
    username: str = Field("")
    password: str = Field("")


class PolicyProxiesConfig(ConfigBase):
    """策略命中后允许使用的代理集合:按 tags 或按 ids 收窄(两者都给则取并集)。

    tags 匹配 ProxyInfo.tags(如 region=cn);ids 直接列出代理 id。两者都缺省
    (空集合)时该策略只匹配不限制(防御:不阻断流量,等同全量候选)。
    """
    tags: Optional[Dict[str, str]] = Field(None, description="代理标签匹配,如 region=cn")
    ids: Optional[List[str]] = Field(None, description="允许的代理 id 列表")


class PolicyMatchConfig(ConfigBase):
    """策略匹配条件(目标域名维度)。任一子条件命中即匹配(OR)。

    host 提取规则:HTTP 用 URL hostname,CONNECT 用 target 去端口后的 host,
    均不含端口、小写化、去尾部点。
    """
    domain_suffix: List[str] = Field(default_factory=list, description="域名后缀,如 '.cn' / 'baidu.com'")
    domain_exact: List[str] = Field(default_factory=list, description="域名精确匹配")
    domain_regex: List[str] = Field(default_factory=list, description="域名正则(re.search)")


class PolicyConfig(ConfigBase):
    """一条策略路由:目标域名命中 match → 只在该子集代理中竞速/单发。

    作用于竞速候选收窄、域名缓存与粘性的取用校验(三者一致,防旧缓存绕过新
    策略)。match 命中按 policies 列表顺序取第一条。
    """
    match: PolicyMatchConfig = Field(default_factory=PolicyMatchConfig)
    proxies: PolicyProxiesConfig = Field(default_factory=PolicyProxiesConfig)


class StickinessConfig(ConfigBase):
    """会话粘性配置(per-client+domain 维度,内存-only)。

    enabled:    是否启用会话粘性。启用后同一客户端 IP 访问同一域名/目标时,
               复用该键上次胜出的代理单发(跳过竞速),保持 egress IP 稳定;
               粘性代理失败时驱逐并回落竞速,赢家回填粘性表(redispatch)。
    ttl:        粘性有效期(秒)。粘性命中成功会刷新 TTL(滑动),活跃会话不过期;
               到期后重新走竞速。
    recheck_hits: 粘性命中累计次数阈值。达到该次数后触发一次"探路重竞速",
               用新赢家替换可能已变慢的粘性代理(默认开启 100 次;0 表示关闭
               周期重评估)。驱逐后跳过域名缓存直接竞速,赢家 hits 归零。
    max_entries: 粘性表容量硬上限。写前先清过期条目,仍超限则驱逐 updated_at
               最旧的一条,防止客户端 IP 集合过大时内存无界增长(默认 100k)。
    probe_interval_sec: 杠杆A 粘性命中后台探路冷却(秒)。粘性单发命中后,
               fire-and-forget 对竞争代理做 CONNECT-only 探路,探路显著更快则
               驱逐粘性条目(慢窗口从 TTL 压到请求级)。冷却内不重复探。
               0=关闭(默认,生产灰度时再开)。
    probe_fanout: 探路并发竞争的代理数上限(取 ordered_for_domain 域名排序
               前 N 个,剔除粘性代理/直连)。默认 2。
    """
    enabled: bool = Field(False, description="启用会话粘性(同客户端+域名复用同一代理)")
    ttl: int = Field(1800, description="会话粘性有效期(秒)，滑动刷新")
    recheck_hits: int = Field(100, description="粘性命中 N 次后触发探路重竞速(0=关闭)")
    max_entries: int = Field(100_000, description="粘性表最大条目数,超出驱逐最旧(内存保护)")
    probe_interval_sec: float = Field(0.0, description="粘性命中后台探路冷却(秒),0=关闭")
    probe_fanout: int = Field(2, description="后台探路并发竞争的代理数上限")


class HttpCacheConfig(ConfigBase):
    """HTTP 响应缓存配置(P2:LRU + 容量上限)。

    enabled:            总开关。False 时 _http_cache_get 一律未命中(压测隔离
                       缓存层测纯路由性能)。默认 True。
    ttl:                条目有效期(秒),命中后滑动刷新。默认 60。
    max_entries:        条目数硬上限。写入前若超限,按 LRU 淘汰最久未访问条目。
                       默认 10000。
    max_bytes:          缓存总字节上限(body 内容之和)。超限按 LRU 淘汰直到
                       低于上限。默认 256 MiB。
    stream_cache_limit: 单条响应 body 缓冲上限(字节)。流式转发时超过该大小即
                       放弃缓存该响应(大文件不缓存)。默认 1 MiB。
    """
    enabled: bool = Field(True, description="HTTP 响应缓存总开关")
    ttl: int = Field(60, description="缓存条目有效期(秒),命中滑动刷新")
    max_entries: int = Field(10_000, description="缓存条目数硬上限,超限 LRU 淘汰")
    max_bytes: int = Field(256 * 1024 * 1024, description="缓存总字节上限,超限 LRU 淘汰")
    stream_cache_limit: int = Field(1 * 1024 * 1024, description="单条响应 body 缓冲上限(字节),超过放弃缓存")


class ProbeCanaryConfig(BaseModel):
    """一个探活 canary 目标(P2:多 canary/按标签探活)。

    name:    canary 名称(可观测/日志)。
    target:  "host:port"(或 "[ipv6]:port")。必须"本机直连可达 + 经目标标签
             代理也可达"。
    tags:    该 canary 适用哪些代理标签(如 region=cn)。代理 tags 全命中即选
             该 canary;未配置 tags 的 canary 作为兜底(默认第一条)。
    """
    name: str = Field("global", description="canary 名称")
    target: str = Field(..., description="探活目标 host:port")
    tags: Optional[Dict[str, str]] = Field(None, description="适用代理标签,全命中即选")


class ConnPoolConfig(ConfigBase):
    """CONNECT 上游 TCP 预热池(P1)。

    enabled:      总开关(默认关闭)。开启后为每个上游代理维护少量空闲 TCP
                 连接,CONNECT 请求到来时优先取已连接到代理的 socket 再发
                 CONNECT target,省掉"本机→上游代理"的建连 TTFB。
    per_proxy:    每个上游最多预热连接数(默认 4)。
    total:        全局预热连接数上限(fd 预算,默认 64)。
    idle_timeout: 空闲连接超时(秒,默认 30)。超时未取用则关闭,防泄漏。
    refill_interval: 后台补充预热连接的周期(秒,默认 5)。0=只取不补。
    refill_target: 每代理保持的空闲连接数目标(默认 2,受 per_proxy/total 钳制)。
    connect_timeout: 预热/取用建连超时(秒,默认 10)。
    target_prewarm: 第二阶段(默认关闭)。命中域名缓存/粘性的高频 CONNECT target
                在后台提前建立"到上游代理"的 TCP(不提前 CONNECT 到目标,避免打
                到源站),按 (proxy, target) 键区分,下一次同 target 命中时直接
                复用该 TCP 发 CONNECT,进一步压低 HTTPS 短连接 TTFB。与第一阶段
                共享 per-proxy 上限 + 全局 fd 预算 + 空闲超时;需 conn_pool.enabled
                为 True 才生效。
    refill_pause_minutes: 空闲暂停(分钟,默认 60)。连续 N 分钟无客户端请求时
                挂起后台 refill/目标预热,避免深夜空闲期"建了又过期"的空转浪费
                (生产实测:6 代理深夜 6h 白建 ~1400 条连接,100% 超时被清)。
                新请求到来立即恢复补充。0=不暂停(保持旧行为)。
    refill_pause_activity_window / refill_pause_min_requests: 活动判定(默认
                窗口 120s / 阈值 3)。生产实测发现后台心跳(GitHub Desktop 的
                alive.github.com / Windows 的 client.wns.windows.com / Edge 云消息,
                间隔 3-10 分钟)会把"距上次请求"持续拉近,使 refill_pause_minutes
                永不触发;但旧的"间隔一刀切"(refill_pause_silence_sec)又误伤真实
                孤立请求。为此活动判定改为"簇度计数":窗口内请求数 ≥ 阈值才算
                活动并刷新时间戳。真实流量是簇(一次页面加载数秒内对多个 hostname
                并发 CONNECT,计数 5-30),心跳是孤例(窗口内计数 1,极少 2)——
                据此区分,既不误伤真实请求,又免疫心跳。0=不启用窗口计数(任意
                请求都刷新,旧行为)。
    cluster_predict / cluster_window_sec / cluster_predict_topk / cluster_min_support /
    cluster_proxy_fanout /
                请求簇预测预热(默认关闭):按客户端窗口的 CONNECT 簇共现规律学习
                全局共现图,下次页面加载开口即预测同簇下一批 co-target 并提前预建
                到上游的裸 TCP(不 CONNECT 源站)。错预建 30s 空闲即被淘汰,预算共享
                conn_pool.total。cluster_predict 需 conn_pool.enabled +
                target_prewarm 同时开启。多桶并行预建 cluster_proxy_fanout 把同
                co-target 摊到胜出代理直方图 top-N 桶(默认 2),提升落中真实胜出桶
                概率,fd 预算 conn_pool.total 逐条兜底。
    """
    enabled: bool = Field(False, description="启用 CONNECT 上游 TCP 预热池")
    per_proxy: int = Field(4, description="每代理预热连接数上限")
    total: int = Field(64, description="全局预热连接数上限(fd 预算)")
    idle_timeout: float = Field(30.0, description="空闲连接超时(秒)")
    refill_interval: float = Field(5.0, description="后台补充预热连接的周期(秒),0=只取不补")
    refill_target: int = Field(2, description="每代理保持的空闲连接数目标")
    connect_timeout: float = Field(10.0, description="预热/取用建连超时(秒)")
    target_prewarm: bool = Field(False, description="CONNECT 目标半预连接(第二阶段):命中缓存/粘性的高频 target 提前预热到上游的 TCP")
    refill_pause_minutes: float = Field(60.0, description="空闲暂停(分钟):连续 N 分钟无客户端请求则挂起 refill/目标预热,新请求到来恢复;0=不暂停")
    refill_pause_silence_sec: float = Field(120.0, description="[已弃用,仅作兼容] 旧版活动判定:距上次请求超过该间隔的孤立请求不刷新活动时间戳。已由 refill_pause_activity_window/min_requests 窗口计数取代;本字段对旧配置静默兼容(值仅用于推算 K=1 的窗口,不参与新逻辑)。新配置请改设 refill_pause_activity_window")
    refill_pause_activity_window: Optional[float] = Field(None, description="活动判定窗口(秒)。窗口计数:窗口内出现 ≥ refill_pause_min_requests 个客户端请求才算『活动』并刷新活动时间戳。真实流量是簇(一次页面加载数秒内多 hostname 并发),窗口内计数高;后台心跳(如 alive.github.com / client.wns.windows.com,间隔 3-10 分钟)是孤例,计数低——据此区分,既不误伤真实孤立请求,又免疫心跳。默认 None=用旧 silence_sec(等价窗口 ≈ silence_sec/4,或 120s);0=不启用窗口计数(任意请求都刷新,旧行为)")
    refill_pause_min_requests: int = Field(3, description="活动判定窗口阈值(默认 3):窗口(见 refill_pause_activity_window)内请求数 ≥ 此值才刷新活动时间戳。真实页面加载一次 ≥3 个 hostname 的 CONNECT 簇即达标;心跳(孤例)不达标。阈值 ≤1 时退化为『任意请求都刷新』")
    established_reuse: bool = Field(False, description="已建握手隧道复用:隧道结束若连接干净则归还池,下次同 (proxy,target) 请求复用已 CONNECT 握手的连接,跳过握手,省掉重建。仅当 conn_pool.enabled 为 True 时生效")
    established_idle_timeout: Optional[float] = Field(None, description="已建握手隧道池的独立空闲超时(秒)。established 库存(竞速败者/隧道结束归还的已握手连接)复访同 (proxy,target) 的频率常低于通用池取用,统一用 idle_timeout 会导致归还后 90% 在复访前被清(生产观测 returned=133/expired=120,命中 7)。独立超时让库存多活一阵等复访;默认 None=跟随 idle_timeout(零行为变化)。")
    cluster_predict: bool = Field(False, description="请求簇预测预热:按客户端窗口的 CONNECT 簇共现规律,在窗口开口预测同簇下一批 co-target 并提前预建到上游代理的 TCP(不 CONNECT 源站)。仅当 conn_pool.enabled 且 target_prewarm 为 True 时生效")
    cluster_window_sec: float = Field(2.0, description="簇窗口宽(秒):同一客户端窗口内观察到的目标算一簇;下一请求距上一请求超过窗口宽则关闭并学习上一窗口")
    cluster_predict_topk: int = Field(3, description="窗口开口时预测的 co-target 数上限(按共现支持度取前 K,跳过当前窗口已观察的目标)")
    cluster_min_support: int = Field(2, description="co 关系需达到的最低共现窗口数才产生预测(免疫单次偶然共现)")
    cluster_graph_ttl_sec: int = Field(86400, description="共现图条目的 TTL(秒):超过未再共现的 (src→co) 边被周期清理")
    cluster_graph_max_entries: int = Field(100_000, description="共现图边数硬上限,超限驱逐 last_seen 最旧的边(防高基数 URL 内存无界,仿 sticky max_entries)")
    cluster_predict_throttle_sec: float = Field(30.0, description="同一 (src→co) 对的预测节流间隔(秒):节流内不重复发射,防 reload 反复预建")
    cluster_proxy_fanout: int = Field(2, description="多桶并行预建(方案 A):同 co-target 预测时并行预建的候选代理桶数上限(1=旧单桶行为)。每条共现边记胜出代理 id 直方图,预测摊到计数最高的前 N 个桶,显著提升落在真实胜出桶的概率(探针显示桶错配是主病因)。fd 预算 conn_pool.total 逐条兜底,超预算即静默少建")
    cluster_probe_decay_sec: float = Field(3600.0, description="胜出代理直方图计数的衰减半衰(秒):计数按指数遗忘窗衰减,保留近期谁常胜,防冷启动早期偶然胜出长期霸榜")
    cluster_pool_idle_timeout: float = Field(600.0, description="cluster 预测预建连接的空闲超时(秒):预测预建比被动预建早建得多,默认 600s(远超被动预建的空闲超时),让预测连接活到真实 co-target 到达;用户取向命中不省 fd,故不随 conn_pool.idle_timeout 走")

    @model_validator(mode="after")
    def _gate_on_enabled(self):
        # 第二/三阶段与集群预测依赖总开关:#12 把它们配成隐性依赖会给出费解的静默
        # 失效(开了 target_prewarm 却没开 enabled,功能不生效且无日志)。这里显式硬报错。
        if (self.target_prewarm or self.established_reuse) and not self.enabled:
            raise ValueError(
                "conn_pool.target_prewarm / established_reuse require conn_pool.enabled=True")
        if self.cluster_predict and not self.enabled:
            raise ValueError("conn_pool.cluster_predict requires conn_pool.enabled=True")
        if self.cluster_predict and not self.target_prewarm:
            raise ValueError("conn_pool.cluster_predict requires conn_pool.target_prewarm=True")
        return self


class ConcurrencyLimitConfig(ConfigBase):
    """自适应并发限制(P3):每代理并发上限随稳定性加减。

    enabled:  总开关(默认关闭)。
    initial:  每代理初始并发上限(默认 16)。
    min:      上限下限(默认 2)。
    max:      上限上限(默认 128)。
    add_on_success: 成功且稳定 → 加性增加上限(默认 +4,封顶 max)。
    mult_on_failure: 超时/5xx/EWMA 快速恶化 → 乘性降低上限(默认 0.5,触底 min)。
    failure_window:  用于 EWMA 恶化判定的成功观测窗口(默认 20 次观测内比较)。
    """
    enabled: bool = Field(False, description="启用自适应并发限制")
    initial: int = Field(16, description="每代理初始并发上限")
    min: int = Field(2, description="并发上限下限")
    max: int = Field(128, description="并发上限上限")
    add_on_success: int = Field(4, description="成功加性增加上限")
    mult_on_failure: float = Field(0.5, description="失败乘性降低上限")
    failure_window: int = Field(20, description="EWMA 恶化比较的观测窗口")


class SwitchDampingConfig(ConfigBase):
    """域名赢家切换阻尼(P3)。

    enabled:  总开关(默认关闭)。开启后新赢家不能因单次竞速抖动就替换稳定
              域名赢家,降低出口 IP 抖动(适合 egress 稳定要求高的部署)。
    min_wins: 新赢家需连续胜出多少次才替换旧赢家(默认 2)。
    ratio:    新赢家 EWMA ≤ 旧赢家 × ratio 即立即切换(如 0.8 = 快 20%)。
              0=关闭该维度。
    abs_ms:   新赢家 EWMA 比旧赢家快 ≥ abs_ms 毫秒即立即切换。0=关闭。
    """
    enabled: bool = Field(False, description="启用域名赢家切换阻尼")
    min_wins: int = Field(2, description="新赢家需连续胜出次数才替换")
    ratio: float = Field(0.8, description="新赢家 EWMA ≤ 旧×ratio 立即切换(0=关闭)")
    abs_ms: float = Field(30.0, description="新赢家快 ≥ 该毫秒立即切换(0=关闭)")


class AdaptiveTTLConfig(ConfigBase):
    """自适应域名缓存 TTL(P2)。

    enabled:  总开关(默认关闭,保持全局固定 cache_ttl 的旧行为)。
    min_sec:  每域名 TTL 下限(秒)。抖动域名/代理恶化 → TTL 回落到该值。
    max_sec:  每域名 TTL 上限(秒)。稳定域名连续同代理胜出 → TTL 上浮到该值封顶。
    """
    enabled: bool = Field(False, description="启用自适应域名缓存 TTL")
    min_sec: float = Field(60.0, description="每域名 TTL 下限(秒)")
    max_sec: float = Field(1800.0, description="每域名 TTL 上限(秒)")


class CircuitConfig(ConfigBase):
    """熔断器 + 后台探活 + slow-start + in-flight 选批配置(router.circuit)。

    probe_interval_sec: 后台探活周期(秒)。每周期对 enabled 代理做轻量 CONNECT
                       到 canary + 关闭,计延迟/成败 → 更新 EWMA 与熔断计数。
                       0=关闭主动探活(仅真实请求驱动熔断)。默认 30。
    probe_canary:       探活目标 "host:port"。轻量 CONNECT 只验证上游可达与
                       建连延迟,域名级最终仍由竞速决定(默认 1.1.1.1:443)。
    circuit_threshold:  连续失败多少次触发熔断(默认 3)。真实请求失败与探活
                       失败共享计数。
    circuit_max_backoff: 熔断退避上限(秒,默认 300)。退避指数增长:1s → 2s →
                       4s → ... 直到此上限。
    slow_start_window:  slow-start 爬升窗口(秒,默认 60)。熔断退避到期后该代理
                       在此窗口内低权重垫底。
    slow_start_success: slow-start 恢复期内累计成功多少次后恢复完整权重(默认 3)。
    lb_bias:            加权 least-request 的在途惩罚指数(默认 1.0)。竞速排序
                       权重 = ewma × (1 + active)^bias,在途积压多的代理被压低,
                       保护慢代理不被打爆(Envoy LeastRequest / Dubbo LeastActive)。
                       0 退化为纯 EWMA 排序。
    single_send_degrade_fail: 单发降级:连续失败阈值(默认 0=关闭)。域名缓存/粘性
                       命中的代理连续失败达该值,即使未到熔断阈值,单发路径也主动
                       降级回竞速(质量感知的确定性探路,Goal #6)。建议设为
                       circuit_threshold-1 作熔断早告警。
    single_send_degrade_ratio: 单发降级:EWMA 恶化阈值(默认 0=关闭)。被钉住代理的
                       当前 EWMA 相对钉住时基线的比值超过该值(如 3.0=延迟恶化
                       3 倍)即降级回竞速。0=只按连续失败降级。
    single_send_degrade_slack_ms: EWMA 降级的绝对下限(毫秒,默认 10)。基线与当前值
                       都极小时(如 0.2ms→0.9ms,比值 4.5 但绝对差距 <1ms)用纯比值
                       会误判剧烈恶化——绝对差值低于该 slack 时不降级。
    """
    probe_interval_sec: float = Field(30.0, description="后台探活周期(秒),0=关闭主动探活")
    probe_canary: str = Field("1.1.1.1:443", description="探活目标 host:port(单 canary;被 probe_canaries 覆盖)")
    probe_canaries: List[ProbeCanaryConfig] = Field(default_factory=list, description="多 canary(按代理标签选),配置后替代 probe_canary")
    circuit_threshold: int = Field(3, description="连续失败熔断阈值")
    circuit_max_backoff: float = Field(300.0, description="熔断退避上限(秒),指数增长到该值")
    slow_start_window: float = Field(60.0, description="slow-start 爬升窗口(秒)")
    slow_start_success: int = Field(3, description="slow-start 恢复期内成功多少次后恢复完整权重")
    lb_bias: float = Field(1.0, description="加权 least-request 在途惩罚指数,0=纯 EWMA 排序")
    single_send_degrade_fail: int = Field(0, description="单发降级:连续失败阈值,0=关闭")
    single_send_degrade_ratio: float = Field(0.0, description="单发降级:EWMA 恶化比值阈值,0=关闭(同时用于方案C:粘性慢探路)")
    single_send_degrade_slack_ms: float = Field(10.0, description="EWMA 降级绝对下限(毫秒)")


class RouterConfig(ConfigBase):
    """路由行为配置。

    cache_ttl:           域名缓存有效期(秒)。某代理为某域名竞速胜出后,
                         在该有效期内复用同一代理,避免每请求都竞速。
    enable_local_racing: 让网关主机自身作为代理节点直接参与竞速(不走上游)。
    max_retries:         竞速首批并行的代理数量;全失败后对剩余代理再竞速兜底。
    stagger_start:       是否启用错峰启动(RFC 8305 §5)。竞速首批不再同时全发:
                         先发最优 stagger_initial 个,间隔 stagger_interval_ms 补发
                         下一个,首个首字节成功即取消其余。显著减少 CONNECT 隧道
                         扇出与 HTTP 双写流量。默认 True。
    stagger_initial:     错峰首批并发数(>=1,经 max_retries 钳制)。冷启动(无任何
                         EWMA 历史)时自动翻倍到 2,避免随机首抽丢快代理。
    stagger_interval_ms: 相邻候选的启动间隔(毫秒),钳制到 [100, 2000](RFC 8305
                         §5 下限 100ms/绝对值 10ms、上限 2s)。默认 250。
    circuit:             熔断器 + 后台探活 + slow-start + in-flight 选批配置
                         (见 CircuitConfig)。
    auth:                客户端认证配置(AuthConfig)。
    stickiness:          会话粘性配置(见 StickinessConfig)。
    """
    cache_ttl: int = Field(600, description="域名缓存有效期(秒)，过期后重新竞速")
    enable_local_racing: bool = Field(False, description="将本机作为代理节点参与竞速")
    max_retries: int = Field(3, description="竞速首批并行的代理数量")
    stagger_start: bool = Field(True, description="启用错峰启动(RFC 8305 §5)")
    stagger_initial: int = Field(1, description="错峰首批并发数(冷启动自动翻倍到2)")
    stagger_interval_ms: int = Field(250, description="错峰启动间隔(毫秒),钳制到[100,2000]")
    circuit: CircuitConfig = Field(default_factory=CircuitConfig, description="熔断器/探活/slow-start 配置")
    auth: AuthConfig = Field(default_factory=AuthConfig, description="客户端认证配置")
    stickiness: StickinessConfig = Field(default_factory=StickinessConfig, description="会话粘性配置")
    policies: List[PolicyConfig] = Field(default_factory=list, description="策略路由:按目标域名收窄候选代理集(见 PolicyConfig)")
    http_cache: HttpCacheConfig = Field(default_factory=HttpCacheConfig, description="HTTP 响应缓存 LRU + 容量上限配置(见 HttpCacheConfig)")
    adaptive_ttl: AdaptiveTTLConfig = Field(default_factory=AdaptiveTTLConfig, description="自适应域名缓存 TTL(见 AdaptiveTTLConfig)")
    switch_damping: SwitchDampingConfig = Field(default_factory=SwitchDampingConfig, description="域名赢家切换阻尼(见 SwitchDampingConfig)")
    concurrency_limit: ConcurrencyLimitConfig = Field(default_factory=ConcurrencyLimitConfig, description="自适应并发限制(见 ConcurrencyLimitConfig)")
    conn_pool: ConnPoolConfig = Field(default_factory=ConnPoolConfig, description="CONNECT 上游 TCP 预热池(见 ConnPoolConfig)")

    @model_validator(mode="after")
    def _check_cross_field(self):
        # #12 跨字段一致性:无效/自我矛盾配置在启动即硬报错,而不是带病运行。
        if self.stagger_initial < 1:
            raise ValueError("router.stagger_initial must be >= 1")
        if self.stagger_initial > self.max_retries:
            raise ValueError(
                f"router.stagger_initial ({self.stagger_initial}) must be <= "
                f"max_retries ({self.max_retries})")
        if self.adaptive_ttl.enabled and self.adaptive_ttl.min_sec > self.adaptive_ttl.max_sec:
            raise ValueError(
                f"router.adaptive_ttl.min_sec ({self.adaptive_ttl.min_sec}) must be <= "
                f"max_sec ({self.adaptive_ttl.max_sec})")
        cl = self.concurrency_limit
        if cl.enabled and not (cl.min <= cl.initial <= cl.max):
            raise ValueError(
                f"concurrency_limit.min={cl.min}, initial={cl.initial}, max={cl.max} "
                f"must satisfy min <= initial <= max")
        return self


class Config(ConfigBase):
    """顶层配置,各字段均有默认值。"""
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


# APIConfig.auth 是字符串前向引用("AuthConfig"),在模块底部解析一次,
# 使 pydantic 能构建正确类型。AuthConfig 定义于本文件下方。
APIConfig.model_rebuild()
