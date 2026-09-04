"""有界内存的分位数摘要（t-digest 风格），用于终身累计分位数的长期 rollup。

背景（IMPROVEMENT_PLAN Phase 6.1 / 6.6）:
  窗口分位数只保留最近 `_OBS_WINDOW`(256) 个样本，反映近期表现（路由决策用）；
  但"该代理长期到底多快"无法从窗口回答——而逐样本留存的存储无上界，故需要把
  海量历史样本压缩成**内存有界**的 rollup。本模块即这个 rollup。

  原方案建议接入第三方 t-digest/HDRHistogram 库；这里自包含实现以避免动冻结的
  uv.lock（`uv sync --frozen`），并与 Dunning t-digest 同源：用 (均值, 权重) 质心
  近似分布，查询时按累积权重插值出分位数；压缩同样用 t-digest 的 **k1 尺度函数**
  聚类（见 _compress），质心数有硬上限 ⇒ 内存 O(1)。

  为什么做成 dict 子类：metric_dict 会被 `json.dumps` 原样落盘
  （router._flush_to_db → `json.dumps(m)`），非 JSON 原生类型会破坏持久化且
  旧 DB 行无法恢复。继承 dict 让它天然可序列化、deepcopy 安全，零改动接入。
  代价：`add()/quantile()` 原地改 self 的内部键（c/b/n/mn/mx），无属性式 API。
"""

import math
from typing import List, Optional

__all__ = ["TDigest"]


class TDigest(dict):
    """(均值, 权重) 质心摘要。键：

      c   : 质心列表 [[mean, weight], ...]，按 mean 升序
      b   : 未压缩的新样本缓冲 [[value, weight], ...]，满 BUFFER_LIMIT 才合并
      n   : 累计权重（样本总数）
      mn  : 历史最小值（分位数插值的左端点）
      mx  : 历史最大值（分位数插值的右端点）
    """

    MAX_CENTROIDS = 96      # 质心数上限 => 内存有界（每个质心 2 个数）
    BUFFER_LIMIT = 32       # 缓冲多少新样本后才做一次压缩合并

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.setdefault("c", [])
        self.setdefault("b", [])
        self.setdefault("n", 0.0)
        self.setdefault("mn", None)
        self.setdefault("mx", None)

    # ── 写入 ──────────────────────────────────────────────
    def add(self, value: float, weight: float = 1.0) -> None:
        """追加一个观测值（热路径：缓冲满才触发压缩，摊还 O(1)）。"""
        self["n"] += weight
        mn, mx = self["mn"], self["mx"]
        if mn is None or value < mn:
            self["mn"] = value
        if mx is None or value > mx:
            self["mx"] = value
        self["b"].append([value, weight])
        if len(self["b"]) >= self.BUFFER_LIMIT:
            self._compress()

    def _compress(self) -> None:
        """把缓冲并入质心，按 t-digest 的 **k1 尺度函数**聚类压缩。

        规则（Dunning t-digest clustering）：把累积权重映射到 k 空间
        K(q) = A·asin(2q−1)（A 使 K 的总跨度恰为 MAX_CENTROIDS），每个质心最多
        跨越 1 个 k 单位 ⇒ 质心总数 <= MAX_CENTROIDS（内存硬上界）。

        为什么不用"合并相邻最近"的贪心：那样会压缩**密集区**（长尾分布里正是中位数
        附近），实测对数正态 p50 误差 5.3%。k1 尺度反而在两端（q→0/1）要求质心极小，
        把分辨率让给尾部——代理延迟的长尾里 p99 是我们最关心的量（实测 0.7% 误差）。
        """
        buf = self["b"]
        if not buf:
            return
        points: List[list] = [[m, w] for m, w in self["c"]]
        points.extend(buf)
        del buf[:]
        points.sort(key=lambda c: c[0])
        total = self["n"]
        if total <= 0:
            self["c"] = []
            return

        limit = float(self.MAX_CENTROIDS)
        # K(q) = A·asin(2q−1)，A = limit/π ⇒ K(1) − K(0) = limit（跨度恰为质心上限）
        scale = limit / math.pi

        def k_of(weight_before: float) -> float:
            # q ∈ [0,1]，端点处 asin(±1) 有定义，故无需裁剪。
            q = weight_before / total
            if q <= 0.0:
                return -scale * (math.pi / 2.0)
            if q >= 1.0:
                return scale * (math.pi / 2.0)
            return scale * math.asin(2.0 * q - 1.0)

        out: List[list] = []
        cur_mean, cur_w = points[0][0], points[0][1]
        cum_before = 0.0           # 当前质心之前的累积权重
        k0 = k_of(0.0)
        for x, w in points[1:]:
            new_w = cur_w + w
            # 试探把 x 并入当前质心：k 空间跨度不超过 1 才允许
            if k_of(cum_before + new_w) - k0 <= 1.0:
                cur_mean = (cur_mean * cur_w + x * w) / new_w
                cur_w = new_w
            else:
                out.append([cur_mean, cur_w])
                cum_before += cur_w
                k0 = k_of(cum_before)
                cur_mean, cur_w = x, w
        out.append([cur_mean, cur_w])
        # k1 只保证"跨度 <= 1"，但入参里已有合并过的重质心（权重大、粒度粗），
        # 常因"下一个点塞不进当前质心"而提前收尾，实测质心数约为上限的 1.2 倍。
        # 这里再做一次兜底裁剪：超出上限就反复合并相邻最近的质心，把内存钉死在上限。
        # 只在越界时触发（裁掉的是最密的相邻对），对精度影响很小。
        while len(out) > self.MAX_CENTROIDS:
            best_i, best_gap = 0, float("inf")
            for i in range(len(out) - 1):
                gap = out[i + 1][0] - out[i][0]
                if gap < best_gap:
                    best_gap, best_i = gap, i
            m1, w1 = out[best_i]
            m2, w2 = out[best_i + 1]
            out[best_i] = [(m1 * w1 + m2 * w2) / (w1 + w2), w1 + w2]
            del out[best_i + 1]
        self["c"] = out

    # ── 查询 ──────────────────────────────────────────────
    def quantile(self, q: float) -> Optional[float]:
        """返回 q 分位数（q ∈ [0,1]）；无样本返回 None。"""
        self._compress()
        c = self["c"]
        if not c or self["n"] <= 0:
            return None
        if len(c) == 1:
            return c[0][0]
        target = q * self["n"]
        mn, mx = self["mn"], self["mx"]
        if target <= 0:
            return mn
        cum = 0.0
        n_c = len(c)
        for i, (m, w) in enumerate(c):
            prev = cum
            cum += w
            if target < cum:
                # 质心中心 m 对应 index = prev + w/2；向其左右邻质心线性插值。
                left = c[i - 1][0] if i > 0 else (mn if mn is not None else m)
                right = c[i + 1][0] if i + 1 < n_c else (mx if mx is not None else m)
                half = w / 2.0
                if half <= 0:
                    return m
                center = prev + half
                if target < center:
                    frac = (target - prev) / half
                    return left + (m - left) * frac
                frac = (target - center) / half
                return m + (right - m) * frac
        return mx

    def percentiles(self) -> dict:
        """返回 {p50, p95, p99, min, max, mean, samples}，与 ProxySelector._percentiles 同形。

        mean 是质心加权的近似均值（非精确算术平均），仅供量级参考；
        samples 为累计权重（真实样本总数，精确）。
        """
        self._compress()
        c = self["c"]
        n = self["n"]
        if not c or n <= 0:
            return {}
        total_w = sum(w for _, w in c)
        mean = (sum(m * w for m, w in c) / total_w) if total_w > 0 else None
        return {
            "p50": self.quantile(0.50),
            "p95": self.quantile(0.95),
            "p99": self.quantile(0.99),
            "min": self["mn"],
            "max": self["mx"],
            "mean": mean,
            "samples": int(n),
        }
