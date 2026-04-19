#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import logging
import time

from srdf.models import SQLObjectScopeRule


logger = logging.getLogger(__name__)

_RULES_CACHE = {
    "expires_at": 0.0,
    "rules": None,
}

RULES_CACHE_TTL_SECONDS = 10


def _normalize_text(value):
    return (value or "").strip()


def _normalize_engine(engine):
    engine = _normalize_text(engine).lower()
    if engine in ("mysql", "mariadb"):
        return "mysql_family"
    return engine


def _engine_matches(rule_engine, event_engine):
    rule_engine = _normalize_text(rule_engine).lower()
    event_engine = _normalize_text(event_engine).lower()

    if rule_engine == "all":
        return True

    if rule_engine == event_engine:
        return True

    rule_family = _normalize_engine(rule_engine)
    event_family = _normalize_engine(event_engine)

    if rule_family == "mysql_family" and event_family == "mysql_family":
        return True

    return False


def _match(value, rule_value, mode):
    rule_value = _normalize_text(rule_value)
    value = _normalize_text(value)

    if not rule_value:
        return True

    if mode == "exact":
        return value == rule_value

    if mode == "prefix":
        return value.startswith(rule_value)

    if mode == "contains":
        return rule_value in value

    return False


def _get_rules_cached(force_refresh=False):
    now = time.time()

    if (
        not force_refresh
        and _RULES_CACHE["rules"] is not None
        and now < _RULES_CACHE["expires_at"]
    ):
        return _RULES_CACHE["rules"]

    rules = list(
        SQLObjectScopeRule.objects
        .filter(is_enabled=True)
        .order_by("priority", "id")
    )

    _RULES_CACHE["rules"] = rules
    _RULES_CACHE["expires_at"] = now + RULES_CACHE_TTL_SECONDS
    return rules


def clear_rules_cache():
    _RULES_CACHE["rules"] = None
    _RULES_CACHE["expires_at"] = 0.0


def explain_rule_match(engine, database_name, table_name, force_refresh=False):
    engine = _normalize_text(engine).lower()
    database_name = _normalize_text(database_name)
    table_name = _normalize_text(table_name)

    rules = _get_rules_cached(force_refresh=force_refresh)

    matched_include = False
    has_include = False
    evaluation_trace = []

    for rule in rules:
        trace_item = {
            "rule_id": rule.id,
            "rule_name": rule.name,
            "policy_mode": rule.policy_mode,
            "object_type": rule.object_type,
            "match_mode": rule.match_mode,
            "rule_engine": rule.engine,
            "rule_database_name": rule.database_name,
            "rule_table_name": rule.table_name,
            "priority": rule.priority,
            "matched": False,
            "skip_reason": "",
        }

        if not _engine_matches(rule.engine, engine):
            trace_item["skip_reason"] = (
                f"engine mismatch: rule={rule.engine} event={engine}"
            )
            evaluation_trace.append(trace_item)
            continue

        if rule.object_type == "database":
            if not _match(database_name, rule.database_name, rule.match_mode):
                trace_item["skip_reason"] = (
                    f"database mismatch: rule={rule.database_name} event={database_name}"
                )
                evaluation_trace.append(trace_item)
                continue

        elif rule.object_type == "table":
            if not _match(database_name, rule.database_name, rule.match_mode):
                trace_item["skip_reason"] = (
                    f"database mismatch: rule={rule.database_name} event={database_name}"
                )
                evaluation_trace.append(trace_item)
                continue

            if not _match(table_name, rule.table_name, rule.match_mode):
                trace_item["skip_reason"] = (
                    f"table mismatch: rule={rule.table_name} event={table_name}"
                )
                evaluation_trace.append(trace_item)
                continue

        else:
            trace_item["skip_reason"] = f"unsupported object_type={rule.object_type}"
            evaluation_trace.append(trace_item)
            continue

        trace_item["matched"] = True
        evaluation_trace.append(trace_item)

        if rule.policy_mode == "exclude":
            return {
                "allowed": False,
                "decision": "denied_by_exclude_rule",
                "engine": engine,
                "database_name": database_name,
                "table_name": table_name,
                "matched_rule": {
                    "id": rule.id,
                    "name": rule.name,
                    "policy_mode": rule.policy_mode,
                    "object_type": rule.object_type,
                    "match_mode": rule.match_mode,
                    "engine": rule.engine,
                    "database_name": rule.database_name,
                    "table_name": rule.table_name,
                    "priority": rule.priority,
                },
                "has_include_rules": has_include,
                "evaluation_trace": evaluation_trace,
            }

        if rule.policy_mode == "include":
            has_include = True
            matched_include = True

    if has_include:
        if matched_include:
            return {
                "allowed": True,
                "decision": "allowed_by_include_rule",
                "engine": engine,
                "database_name": database_name,
                "table_name": table_name,
                "matched_rule": None,
                "has_include_rules": True,
                "evaluation_trace": evaluation_trace,
            }

        return {
            "allowed": False,
            "decision": "denied_because_include_rules_exist_but_no_match",
            "engine": engine,
            "database_name": database_name,
            "table_name": table_name,
            "matched_rule": None,
            "has_include_rules": True,
            "evaluation_trace": evaluation_trace,
        }

    return {
        "allowed": True,
        "decision": "allowed_by_default_no_include_rules",
        "engine": engine,
        "database_name": database_name,
        "table_name": table_name,
        "matched_rule": None,
        "has_include_rules": False,
        "evaluation_trace": evaluation_trace,
    }


def is_allowed(engine, database_name, table_name, debug=False, force_refresh=False):
    result = explain_rule_match(
        engine=engine,
        database_name=database_name,
        table_name=table_name,
        force_refresh=force_refresh,
    )

    if debug:
        logger.warning(
            (
                "[SRDF][RULES] allowed=%s decision=%s engine=%s "
                "database=%s table=%s matched_rule=%s"
            ),
            result["allowed"],
            result["decision"],
            result["engine"],
            result["database_name"],
            result["table_name"],
            result["matched_rule"]["name"] if result["matched_rule"] else "",
        )

    return bool(result["allowed"])