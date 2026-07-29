from typing import Dict, Optional, List
from pydantic import BaseModel, Field, AnyHttpUrl


class ListenConfig(BaseModel):
    host: str = Field("0.0.0.0")
    port: int = Field(10808)


class APIConfig(BaseModel):
    host: str = Field("0.0.0.0")
    port: int = Field(18080)
    bind_remote: bool = Field(True)


class ProbeConfig(BaseModel):
    url: AnyHttpUrl = Field("http://www.gstatic.com/generate_204")
    interval: int = Field(60)
    timeout: int = Field(10)
    concurrency: int = Field(20)
    history_minutes: int = Field(10)
    batch_domains: int = Field(10)
    half_life_minutes: int = Field(5)
    min_samples: int = Field(3)
    per_proxy_concurrency: int = Field(5)
    per_domain_concurrency: int = Field(20)


class ScoreConfig(BaseModel):
    latency_weight: float = Field(0.5)
    throughput_weight: float = Field(0.3)
    reliability_weight: float = Field(0.2)
    half_life_minutes: int = Field(5)
    # throughput_max in kilobits per second used to map throughput to 0-100
    throughput_max: float = Field(1024.0)


class LoggingConfig(BaseModel):
    level: str = Field("INFO")
    file: Optional[str] = Field(None)


class ProxyInfo(BaseModel):
    id: str
    name: Optional[str]
    host: str
    port: int = Field(3128)
    protocol: str = Field("http")
    auth: Optional[Dict[str, str]] = None
    enabled: bool = Field(True)
    tags: Optional[Dict[str, str]] = None


class RouterConfig(BaseModel):
    cache_ttl: int = Field(600, description="seconds before domain cache expires (default 10 min)")

class Config(BaseModel):
    listen: ListenConfig = Field(default_factory=ListenConfig)
    api: APIConfig = Field(default_factory=APIConfig)
    probe: ProbeConfig = Field(default_factory=ProbeConfig)
    score: ScoreConfig = Field(default_factory=ScoreConfig)
    router: RouterConfig = Field(default_factory=RouterConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    proxies: Optional[List[ProxyInfo]] = None
