import sqlite3
import tempfile
from pathlib import Path

import pytest

from auto_squid.proxy_store import ProxyStore
from auto_squid.policy_engine import PolicyEngine
from auto_squid.config_schema import ProxyInfo, PolicyRule, RuleType, RuleTarget


# ── fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def proxy_store():
    ps = ProxyStore()
    ps.add(ProxyInfo(id='p1', host='1.2.3.4', port=3128, tags={'region': 'beijing'}))
    ps.add(ProxyInfo(id='p2', host='5.6.7.8', port=3128, tags={'region': 'shanghai'}))
    ps.add(ProxyInfo(id='p3', host='9.10.11.12', port=3128, tags={'region': 'beijing', 'tier': 'premium'}))
    return ps


@pytest.fixture
def policy_engine(proxy_store):
    """Create a PolicyEngine backed by an in-memory SQLite database."""
    db = sqlite3.connect(tempfile.mktemp(suffix='.db'))
    db.execute("""
        CREATE TABLE IF NOT EXISTS policy_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_type TEXT NOT NULL,
            domain_pattern TEXT NOT NULL,
            target_type TEXT NOT NULL,
            target_proxy TEXT,
            tag_key TEXT,
            tag_value TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1
        )
    """)
    db.commit()
    return PolicyEngine(db, proxy_store)


# ── helpers ─────────────────────────────────────────────────────────

def add_rule(pe, rule_type, domain_pattern, target_type, target_proxy=None,
             tag_key=None, tag_value=None, priority=0):
    r = PolicyRule(
        rule_type=rule_type,
        domain_pattern=domain_pattern,
        target_type=target_type,
        target_proxy=target_proxy,
        tag_key=tag_key,
        tag_value=tag_value,
        priority=priority,
    )
    return pe.add_rule(r)


# ── domain pattern matching ─────────────────────────────────────────

class TestDomainMatching:
    def test_exact_match(self):
        assert PolicyEngine._match('example.com', 'example.com')
        assert PolicyEngine._match('Example.COM', 'example.com')
        assert not PolicyEngine._match('sub.example.com', 'example.com')

    def test_wildcard_prefix(self):
        assert PolicyEngine._match('foo.example.com', '*.example.com')
        assert PolicyEngine._match('bar.example.com', '*.example.com')
        assert not PolicyEngine._match('example.com', '*.example.com')

    def test_wildcard_suffix(self):
        assert PolicyEngine._match('images.google.cn', 'images.google.*')
        assert not PolicyEngine._match('maps.baidu.com', 'images.google.*')

    def test_wildcard_both_sides(self):
        assert PolicyEngine._match('news.google.com', '*.google.*')
        assert PolicyEngine._match('news.google.sg', '*.google.*')

    def test_catch_all(self):
        assert PolicyEngine._match('anything.example.com', '*')
        assert PolicyEngine._match('example.com', '*')

    def test_case_insensitive(self):
        assert PolicyEngine._match('FOO.Example.COM', '*.example.com')
        assert PolicyEngine._match('EXAMPLE.com', 'example.com')


# ── force rules ─────────────────────────────────────────────────────

class TestForceRules:
    def test_force_by_proxy_id(self, policy_engine):
        add_rule(policy_engine, RuleType.force, '*.force.com', RuleTarget.proxy_id, target_proxy='p2')
        assert policy_engine.resolve_force('test.force.com') == 'p2'

    def test_force_by_tag(self, policy_engine):
        add_rule(policy_engine, RuleType.force, '*.beijing-only.com', RuleTarget.tag, tag_key='region', tag_value='beijing')
        # p1 has region=beijing, comes first in list
        result = policy_engine.resolve_force('api.beijing-only.com')
        assert result in ['p1', 'p3']  # both p1 and p3 have region=beijing

    def test_force_no_match(self, policy_engine):
        add_rule(policy_engine, RuleType.force, '*.specific.com', RuleTarget.proxy_id, target_proxy='p1')
        assert policy_engine.resolve_force('other.com') is None

    def test_force_proxy_not_found(self, policy_engine):
        add_rule(policy_engine, RuleType.force, '*.test.com', RuleTarget.proxy_id, target_proxy='nonexistent')
        assert policy_engine.resolve_force('test.test.com') is None

    def test_force_tag_no_matching_proxy(self, policy_engine):
        add_rule(policy_engine, RuleType.force, '*.test.com', RuleTarget.tag, tag_key='region', tag_value='tokyo')
        assert policy_engine.resolve_force('test.test.com') is None


# ── deny rules ──────────────────────────────────────────────────────

class TestDenyRules:
    def test_deny_by_proxy_id(self, policy_engine):
        add_rule(policy_engine, RuleType.deny, '*.blocked.com', RuleTarget.proxy_id, target_proxy='p2')
        result = policy_engine.evaluate_denies('api.blocked.com', ['p1', 'p2', 'p3'])
        assert 'p2' not in result
        assert 'p1' in result
        assert 'p3' in result

    def test_deny_by_tag(self, policy_engine):
        add_rule(policy_engine, RuleType.deny, '*.overseas.com', RuleTarget.tag, tag_key='region', tag_value='beijing')
        result = policy_engine.evaluate_denies('api.overseas.com', ['p1', 'p2', 'p3'])
        assert 'p1' not in result  # p1 has region=beijing
        assert 'p3' not in result  # p3 has region=beijing
        assert 'p2' in result       # p2 has region=shanghai

    def test_deny_no_match_domain(self, policy_engine):
        add_rule(policy_engine, RuleType.deny, '*.blocked.com', RuleTarget.proxy_id, target_proxy='p1')
        result = policy_engine.evaluate_denies('other.com', ['p1', 'p2'])
        assert result == ['p1', 'p2']  # unchanged

    def test_deny_all_proxies(self, policy_engine):
        add_rule(policy_engine, RuleType.deny, '*', RuleTarget.proxy_id, target_proxy='p1')
        add_rule(policy_engine, RuleType.deny, '*', RuleTarget.proxy_id, target_proxy='p2')
        add_rule(policy_engine, RuleType.deny, '*', RuleTarget.proxy_id, target_proxy='p3')
        result = policy_engine.evaluate_denies('any.com', ['p1', 'p2', 'p3'])
        assert result == []


# ── prefer rules ────────────────────────────────────────────────────

class TestPreferRules:
    def test_prefer_by_proxy_id(self, policy_engine):
        add_rule(policy_engine, RuleType.prefer, '*.beijing.com', RuleTarget.proxy_id, target_proxy='p1')
        result = policy_engine.apply_prefers('app.beijing.com', ['p2', 'p3', 'p1'])
        assert result[0] == 'p1'

    def test_prefer_by_tag(self, policy_engine):
        add_rule(policy_engine, RuleType.prefer, '*.beijing.com', RuleTarget.tag, tag_key='region', tag_value='beijing')
        result = policy_engine.apply_prefers('app.beijing.com', ['p2', 'p3', 'p1'])
        # p1 and p3 both have region=beijing, should be at front
        assert result[0] in ('p1', 'p3')
        assert result[1] in ('p1', 'p3')
        assert result[0] != result[1]
        assert result[2] == 'p2'

    def test_prefer_no_match(self, policy_engine):
        add_rule(policy_engine, RuleType.prefer, '*.beijing.com', RuleTarget.proxy_id, target_proxy='p1')
        result = policy_engine.apply_prefers('other.com', ['p3', 'p2', 'p1'])
        assert result == ['p3', 'p2', 'p1']  # unchanged order

    def test_prefer_already_first(self, policy_engine):
        add_rule(policy_engine, RuleType.prefer, '*.test.com', RuleTarget.proxy_id, target_proxy='p1')
        result = policy_engine.apply_prefers('x.test.com', ['p1', 'p2', 'p3'])
        assert result[0] == 'p1'  # stays first

    def test_prefer_multiple_rules(self, policy_engine):
        add_rule(policy_engine, RuleType.prefer, '*.test.com', RuleTarget.proxy_id, target_proxy='p3', priority=0)
        add_rule(policy_engine, RuleType.prefer, '*.test.com', RuleTarget.proxy_id, target_proxy='p2', priority=10)
        result = policy_engine.apply_prefers('x.test.com', ['p1', 'p2', 'p3'])
        # higher priority (p2) first among preferred, then p3, then p1
        assert result[0] == 'p2'
        assert result[1] == 'p3'
        assert result[2] == 'p1'


# ── rule priority ───────────────────────────────────────────────────

class TestPriority:
    def test_force_higher_priority_wins(self, policy_engine):
        add_rule(policy_engine, RuleType.force, '*.test.com', RuleTarget.proxy_id, target_proxy='p1', priority=0)
        add_rule(policy_engine, RuleType.force, '*.test.com', RuleTarget.proxy_id, target_proxy='p2', priority=10)
        assert policy_engine.resolve_force('x.test.com') == 'p2'

    def test_deny_before_prefer(self, policy_engine):
        """Deny + prefer interaction: DENY rule should take effect even if PREFER exists."""
        add_rule(policy_engine, RuleType.deny, '*', RuleTarget.proxy_id, target_proxy='p1', priority=10)
        add_rule(policy_engine, RuleType.prefer, '*', RuleTarget.proxy_id, target_proxy='p1', priority=0)
        result = policy_engine.evaluate_denies('any.com', ['p1', 'p2'])
        assert 'p1' not in result  # deny has higher priority


# ── CRUD ────────────────────────────────────────────────────────────

class TestCrud:
    def test_add_and_load(self, policy_engine):
        r = add_rule(policy_engine, RuleType.force, '*.addtest.com', RuleTarget.proxy_id, target_proxy='p1')
        assert r.id is not None
        rules = policy_engine.load_rules()
        assert len(rules) == 1
        assert rules[0].domain_pattern == '*.addtest.com'

    def test_delete(self, policy_engine):
        r = add_rule(policy_engine, RuleType.deny, '*.del.com', RuleTarget.proxy_id, target_proxy='p2')
        assert policy_engine.delete_rule(r.id) is True
        assert policy_engine.delete_rule(999) is False
        assert len(policy_engine.load_rules()) == 0

    def test_disabled_rule_ignored(self, proxy_store):
        """Disabled rules should not affect policy decisions."""
        db = sqlite3.connect(tempfile.mktemp(suffix='.db'))
        db.execute("""
            CREATE TABLE IF NOT EXISTS policy_rules (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                rule_type TEXT NOT NULL,
                domain_pattern TEXT NOT NULL,
                target_type TEXT NOT NULL,
                target_proxy TEXT,
                tag_key TEXT,
                tag_value TEXT,
                priority INTEGER NOT NULL DEFAULT 0,
                enabled INTEGER NOT NULL DEFAULT 1
            )
        """)
        # Insert a disabled force rule directly
        db.execute(
            "INSERT INTO policy_rules (rule_type, domain_pattern, target_type, target_proxy, priority, enabled) "
            "VALUES ('force', '*.test.com', 'proxy_id', 'p1', 0, 0)"
        )
        db.commit()
        pe = PolicyEngine(db, proxy_store)
        # The disabled rule should not be loaded into the cache
        assert pe.resolve_force('www.test.com') is None