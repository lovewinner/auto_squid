# syntax=docker/dockerfile:1

###############################################################################
# auto_squid 镜像 — 多阶段构建
#
#   阶段 1 (builder): 用 uv 解析依赖并构建虚拟环境(不含项目源码)。
#                     uv.lock 被仓库 gitignore,镜像内不预置锁文件,构建期
#                     联网解析一次并生成 lock(首次构建需网络;装好 BuildKit
#                     缓存后增量构建离线)。产物 .venv 拷贝进运行时。
#   阶段 2 (runtime): 精简 slim 运行时,以非 root 用户运行,自带自举
#                     proxies.yaml(占位上游,见 AUTO_SQUID_PROXY_IDS)。
#
# 构建参数:
#   AUTO_SQUID_PROXY_IDS  (默认 "squid-01")  生成 /app/proxies.yaml 的占位
#     上游节点,供自举验证。多上游用逗号分隔("squid-01,squid-02")。接入
#     真实代理时,优先挂载自己的 proxies.yaml(--proxies 指向挂载文件)。
###############################################################################

# ── 阶段 1:builder —──────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

# uv 需要 libgcc;slim 的 Python 自带部分,但显式安装保证完整。
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 \
    && rm -rf /var/lib/apt/lists/*

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

RUN pip install --no-cache-dir uv

WORKDIR /build

# 只拷贝清单文件;uv.lock 在镜像内现场生成(仓库 gitignore 了 uv.lock)。
COPY pyproject.toml ./
RUN uv lock

# 只装依赖(不装项目本体,运行时再 COPY 源码),--no-dev 不带 pytest 等。
# venv 落在 /build/.venv,运行时整段拷贝过去。
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-install-project

# ── 阶段 2:runtime —─────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

RUN apt-get update \
    && apt-get install -y --no-install-recommends libgcc-s1 wget \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# 非 root 运行(安全基线);/app 放代码,/data 放 SQLite/日志,均归该用户。
# /data 用 docker named volume 挂载时,首次创建会自动继承这里的属主(uid 10001),
# 无需手动 chown(绑定挂载则需在宿主机 chown,见 examples/docker/README.md)。
RUN useradd --create-home --uid 10001 auto_squid \
    && mkdir -p /app /data \
    && chown -R auto_squid:auto_squid /app /data

# 用 wget 做容器健康检查(探活管理 API)。镜像默认不启用,由 compose 覆盖。
HEALTHCHECK NONE

COPY --from=builder /build/.venv /app/.venv
COPY auto_squid/ /app/auto_squid/

# 自举 proxies.yaml:按 AUTO_SQUID_PROXY_IDS 生成占位上游。接入真实代理时
# 挂载自己的 proxies.yaml 覆盖(见 compose 注释与 examples/docker/README.md)。
ARG AUTO_SQUID_PROXY_IDS=squid-01
RUN { \
      echo "# bootstrap proxies.yaml — override by mounting your own"; \
      echo ""; \
      for p in $(echo "$AUTO_SQUID_PROXY_IDS" | tr ',' ' '); do \
        echo "- id: $p"; \
        echo "  name: \"$p\""; \
        echo "  host: 10.14.25.86"; \
        echo "  port: 3128"; \
        echo "  protocol: http"; \
        echo "  enabled: true"; \
        echo ""; \
      done; \
    } > /app/proxies.yaml \
    && chown auto_squid:auto_squid /app/proxies.yaml

USER auto_squid
WORKDIR /app

EXPOSE 10808 18080

# 代理端口(10808)与管理 API(18080)。默认配置从 CLI 兜底加载(config.yaml
# 不是必需);如需自定义,把 --config 指到挂载的配置(见 compose .env)。
ENTRYPOINT ["/app/.venv/bin/python", "-m", "auto_squid.cli"]
CMD ["--proxies", "/app/proxies.yaml", "--db", "/data/auto_squid.db"]
