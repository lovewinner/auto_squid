from enum import Enum
from typing import Dict, Optional, List
from pydantic import BaseModel, Field


class RuleType(str, Enum):
    force = "force"
    prefer = "prefer"
    deny = "deny"


class RuleTarget(str, Enum):
    proxy_id = "proxy_id"
    tag = "tag"


class ListenConfig(BaseModel):
    host: str = Field("0.0.0.0")
    port: int = Field(10808)


class APIConfig(BaseModel):
    host: str = Field("0.0.0.0")
    port: int = Field(18080)
    bind_remote: bool = Field(True)


class LoggingConfig(BaseModel):
    level: str = Field("INFO")
    file: Optional[str] = Field(None)


class ProxyInfo(BaseModel):
    """单个上游代理节点的配置"""
    id: str
    name: Optional[str] = None
    host: str
    port: int = Field(3128)
    protocol: str = Field("http")
    auth: Optional[Dict[str, str]] = None
    enabled: bool = Field(True)
    tags: Optional[Dict[str, str]] = None


class PolicyRule(BaseModel):
    """策略规则：按域名模式指定代理选择行为"""
    id: Optional[int] = None
    rule_type: RuleType
    domain_pattern: str = Field(..., description='glob 模式，如 "*.example.com"')
    target_type: RuleTarget
    target_proxy: Optional[str] = Field(None, description="target_type=proxy_id 时的代理 ID")
    tag_key: Optional[str] = Field(None, description="target_type=tag 时的标签键")
    tag_value: Optional[str] = Field(None, description="target_type=tag 时的标签值")
    priority: int = Field(0, description="优先级，数值越大越优先")
    enabled: bool = Field(True)


class PolicyRuleIn(BaseModel):
    """创建规则时的输入模型（不含 id）"""
    rule_type: RuleType
    domain_pattern: str
    target_type: RuleTarget
    target_proxy: Optional[str] = None
    tag_key: Optional[str] = None
    tag_value: Optional[str] = None
    priority: int = 0
    enabled: bool = True


class RouterConfig(BaseModel):
    cache_ttl: int = Field(600, description="域名缓存有效期(秒)，过期后重新竞速")
    enable_local_racing: bool = Field(False, description="将本机作为代理节点参与竞速")

class Config(BaseModel):
    """顶层配置，各字段均有默认值"""
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    proxies: Optional[List[ProxyInfo]] = None
