"""配置模型定义(基于 pydantic v2)。

所有模型均带默认值,因此缺失配置也能以合理默认行为启动。配置可通过
`config.yaml`(`--config` 传入)加载,结构为顶层各子配置块:

    listen:  代理监听地址/端口
    api:     管理 API 监听地址/端口
    router:  路由行为(竞速、缓存、客户端认证)
    logging: 日志级别/文件

`ProxyInfo` 是上游代理节点定义,由 `proxies.yaml` 加载(见 ProxyStore)。
"""

from typing import Dict, Optional
from pydantic import BaseModel, Field


class ListenConfig(BaseModel):
    """代理端口的监听配置(面向客户端的 HTTP/CONNECT 代理端口)。"""
    host: str = Field("0.0.0.0")
    port: int = Field(10808)


class APIConfig(BaseModel):
    """管理 API 的监听配置(独立于代理端口,默认 18080)。

    注意:管理 API 不受客户端认证保护,生产环境需用防火墙限制访问。
    """
    host: str = Field("0.0.0.0")
    port: int = Field(18080)


class LoggingConfig(BaseModel):
    """日志配置。`file` 为 None 时写默认文件 `auto_squid.log`。"""
    level: str = Field("INFO")
    file: Optional[str] = Field(None)


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


class AuthConfig(BaseModel):
    """客户端访问代理端口所需的 HTTP Basic 认证配置。

    默认 `enabled=False`(开放代理),开启后客户端每个请求都需带
    `Proxy-Authorization` 头(见 auth.check_auth)。
    """
    enabled: bool = Field(False, description="要求客户端通过 HTTP Basic 认证")
    username: str = Field("")
    password: str = Field("")


class StickinessConfig(BaseModel):
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
    """
    enabled: bool = Field(False, description="启用会话粘性(同客户端+域名复用同一代理)")
    ttl: int = Field(1800, description="会话粘性有效期(秒)，滑动刷新")
    recheck_hits: int = Field(100, description="粘性命中 N 次后触发探路重竞速(0=关闭)")
    max_entries: int = Field(100_000, description="粘性表最大条目数,超出驱逐最旧(内存保护)")


class CircuitConfig(BaseModel):
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
    """
    probe_interval_sec: float = Field(30.0, description="后台探活周期(秒),0=关闭主动探活")
    probe_canary: str = Field("1.1.1.1:443", description="探活目标 host:port")
    circuit_threshold: int = Field(3, description="连续失败熔断阈值")
    circuit_max_backoff: float = Field(300.0, description="熔断退避上限(秒),指数增长到该值")
    slow_start_window: float = Field(60.0, description="slow-start 爬升窗口(秒)")
    slow_start_success: int = Field(3, description="slow-start 恢复期内成功多少次后恢复完整权重")
    lb_bias: float = Field(1.0, description="加权 least-request 在途惩罚指数,0=纯 EWMA 排序")


class RouterConfig(BaseModel):
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


class Config(BaseModel):
    """顶层配置,各字段均有默认值。"""
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
