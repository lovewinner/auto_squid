import fnmatch
import logging
import sqlite3
from typing import Optional, List

from .proxy_store import ProxyStore
from .config_schema import PolicyRule, RuleType, RuleTarget

logger = logging.getLogger(__name__)


class PolicyEngine:
    """策略引擎：根据域名模式决定代理选择行为。

    三种规则类型：
    - force:  跳过竞速，强制使用指定代理（失败则回退到正常竞速）
    - prefer: 将匹配代理移到竞速列表前端
    - deny:   从竞速列表中移除匹配代理
    """

    def __init__(self, db: sqlite3.Connection, proxy_store: ProxyStore):
        self._db = db
        self.proxy_store = proxy_store
        self._cache: List[PolicyRule] = []
        self.load_rules()

    # ── DB helpers ──────────────────────────────────────────────

    def load_rules(self) -> List[PolicyRule]:
        """从 SQLite 加载所有启用的规则，按 priority 降序排列"""
        rows = self._db.execute(
            "SELECT id, rule_type, domain_pattern, target_type, "
            "target_proxy, tag_key, tag_value, priority, enabled "
            "FROM policy_rules WHERE enabled = 1 ORDER BY priority DESC"
        ).fetchall()
        self._cache = []
        for row in rows:
            self._cache.append(PolicyRule(
                id=row[0],
                rule_type=RuleType(row[1]),
                domain_pattern=row[2],
                target_type=RuleTarget(row[3]),
                target_proxy=row[4],
                tag_key=row[5],
                tag_value=row[6],
                priority=row[7],
                enabled=bool(row[8]),
            ))
        return self._cache

    def add_rule(self, rule: PolicyRule) -> PolicyRule:
        """添加规则到 SQLite 并刷新缓存"""
        cur = self._db.execute(
            "INSERT INTO policy_rules (rule_type, domain_pattern, target_type, "
            "target_proxy, tag_key, tag_value, priority, enabled) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (rule.rule_type.value, rule.domain_pattern,
             rule.target_type.value, rule.target_proxy,
             rule.tag_key, rule.tag_value, rule.priority,
             int(rule.enabled)),
        )
        self._db.commit()
        rule.id = cur.lastrowid
        self.load_rules()
        return rule

    def delete_rule(self, rule_id: int) -> bool:
        """删除一条规则"""
        cur = self._db.execute(
            "DELETE FROM policy_rules WHERE id = ?", (rule_id,)
        )
        self._db.commit()
        deleted = cur.rowcount > 0
        if deleted:
            self.load_rules()
        return deleted

    def get_rule(self, rule_id: int) -> Optional[PolicyRule]:
        """按 ID 读取单条规则"""
        for r in self._cache:
            if r.id == rule_id:
                return r
        return None

    # ── Domain matching ─────────────────────────────────────────

    @staticmethod
    def _match(domain: str, pattern: str) -> bool:
        """glob 匹配域名（不区分大小写）"""
        return fnmatch.fnmatch(domain.lower(), pattern.lower())

    # ── Tag helpers ─────────────────────────────────────────────

    def _proxies_with_tag(self, key: str, value: str) -> List[str]:
        """返回所有包含指定 tag 键值对的已启用代理 ID 列表"""
        result = []
        for p in self.proxy_store.list():
            if p.enabled and p.tags and p.tags.get(key) == value:
                result.append(p.id)
        return result

    def _proxy_has_tag(self, pid: str, key: str, value: str) -> bool:
        """检查指定代理是否有匹配的标签"""
        p = self.proxy_store.get(pid)
        if not p or not p.tags:
            return False
        return p.tags.get(key) == value

    # ── Core decision methods ───────────────────────────────────

    def resolve_force(self, domain: str) -> Optional[str]:
        """返回第一个匹配域名且 target 切实存在的 forced proxy ID，否则 None。

        失败时不返回 None，由调用方回退到正常的竞速流程。
        """
        for rule in self._cache:
            if rule.rule_type != RuleType.force:
                continue
            if not self._match(domain, rule.domain_pattern):
                continue
            if rule.target_type == RuleTarget.proxy_id:
                p = self.proxy_store.get(rule.target_proxy)
                if p and p.enabled:
                    return rule.target_proxy
            elif rule.target_type == RuleTarget.tag:
                proxies = self._proxies_with_tag(rule.tag_key, rule.tag_value)
                if proxies:
                    return proxies[0]
        return None

    def evaluate_denies(self, domain: str, proxy_ids: List[str]) -> List[str]:
        """从代理列表中移除被 DENY 规则命中的代理"""
        remove: set[str] = set()
        for rule in self._cache:
            if rule.rule_type != RuleType.deny:
                continue
            if not self._match(domain, rule.domain_pattern):
                continue
            if rule.target_type == RuleTarget.proxy_id:
                remove.add(rule.target_proxy)
            elif rule.target_type == RuleTarget.tag:
                for pid in proxy_ids:
                    if pid not in remove and self._proxy_has_tag(pid, rule.tag_key, rule.tag_value):
                        remove.add(pid)
        return [pid for pid in proxy_ids if pid not in remove]

    def apply_prefers(self, domain: str, proxy_ids: List[str]) -> List[str]:
        """将匹配 PREFER 规则的代理移到列表前端"""
        front: List[str] = []
        rest = list(proxy_ids)
        for rule in self._cache:
            if rule.rule_type != RuleType.prefer:
                continue
            if not self._match(domain, rule.domain_pattern):
                continue
            if rule.target_type == RuleTarget.proxy_id:
                if rule.target_proxy in rest:
                    rest.remove(rule.target_proxy)
                    front.append(rule.target_proxy)
            elif rule.target_type == RuleTarget.tag:
                preferred = [pid for pid in rest
                             if self._proxy_has_tag(pid, rule.tag_key, rule.tag_value)]
                for pid in preferred:
                    rest.remove(pid)
                    front.append(pid)
        return front + rest