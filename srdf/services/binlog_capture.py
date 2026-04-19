#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import hashlib
import json
import logging
import os
import re
import subprocess
import time
import uuid
import pymysql

from django.db import transaction
from django.utils import timezone

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from srdf.services.sql_scope_rules import explain_rule_match, is_allowed
from srdf.services.settings_resolver import (
    get_srdf_setting_bool,
    get_srdf_setting_int,
)

from srdf.models import (
    AuditEvent,
    OutboundChangeEvent,
    ReplicationCheckpoint,
    ReplicationService,
    SRDFCaptureControlRun,
    SRDFCaptureControlItem,
    SRDFServiceExecutionLock,
)


logger = logging.getLogger(__name__)

UNKNOWN_COL_PATTERN = re.compile(r"^UNKNOWN_COL(\d+)$")
CONTROL_CONSOLE_PREVIEW_LIMIT = 50
CONTROL_PROGRESS_EVERY_ROWS = 250

MYSQLBINLOG_ASSIGNMENT_PATTERN = re.compile(
    r"^@(?P<index>\d+)\s*=\s*(?P<value>.*?)(?:\s*/\*.*)?$"
)

DDL_CREATE_DATABASE_PATTERN = re.compile(
    r"^\s*CREATE\s+DATABASE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([^`\s;]+)`?",
    re.IGNORECASE,
)

DDL_DROP_DATABASE_PATTERN = re.compile(
    r"^\s*DROP\s+DATABASE(?:\s+IF\s+EXISTS)?\s+`?([^`\s;]+)`?",
    re.IGNORECASE,
)

DDL_CREATE_TABLE_PATTERN = re.compile(
    r"^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([^`.\s;]+)`?\.`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_ALTER_TABLE_PATTERN = re.compile(
    r"^\s*ALTER\s+TABLE\s+`?([^`.\s;]+)`?\.`?([^`\s;(]+)`?\s+(.*)$",
    re.IGNORECASE,
)

DDL_DROP_TABLE_PATTERN = re.compile(
    r"^\s*DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+`?([^`.\s;]+)`?\.`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_CREATE_INDEX_PATTERN = re.compile(
    r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?([^`\s]+)`?\s+ON\s+`?([^`.\s;]+)`?\.`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_DROP_INDEX_PATTERN = re.compile(
    r"^\s*DROP\s+INDEX\s+`?([^`\s]+)`?\s+ON\s+`?([^`.\s;]+)`?\.`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_ADD_COLUMN_PATTERN = re.compile(
    r"^\s*ADD\s+(?:COLUMN\s+)?`?([^`\s,]+)`?",
    re.IGNORECASE,
)

DDL_DROP_COLUMN_PATTERN = re.compile(
    r"^\s*DROP\s+(?:COLUMN\s+)?`?([^`\s,]+)`?",
    re.IGNORECASE,
)

DDL_MODIFY_COLUMN_PATTERN = re.compile(
    r"^\s*MODIFY\s+(?:COLUMN\s+)?`?([^`\s,]+)`?",
    re.IGNORECASE,
)

DDL_CHANGE_COLUMN_PATTERN = re.compile(
    r"^\s*CHANGE\s+(?:COLUMN\s+)?`?([^`\s,]+)`?\s+`?([^`\s,]+)`?",
    re.IGNORECASE,
)

DDL_ADD_INDEX_PATTERN = re.compile(
    r"^\s*ADD\s+(?:UNIQUE\s+)?(?:INDEX|KEY)\s+`?([^`\s,(]+)`?",
    re.IGNORECASE,
)

DDL_DROP_PRIMARY_KEY_PATTERN = re.compile(
    r"^\s*DROP\s+PRIMARY\s+KEY\b",
    re.IGNORECASE,
)

DDL_ADD_PRIMARY_KEY_PATTERN = re.compile(
    r"^\s*ADD\s+PRIMARY\s+KEY\b",
    re.IGNORECASE,
)

DDL_USE_DATABASE_PATTERN = re.compile(
    r"^\s*use\s+`?([^`\s;]+)`?.*$",
    re.IGNORECASE,
)

DDL_CREATE_TABLE_NO_DB_PATTERN = re.compile(
    r"^\s*CREATE\s+TABLE(?:\s+IF\s+NOT\s+EXISTS)?\s+`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_ALTER_TABLE_NO_DB_PATTERN = re.compile(
    r"^\s*ALTER\s+TABLE\s+`?([^`\s;(]+)`?\s+(.*)$",
    re.IGNORECASE,
)

DDL_DROP_TABLE_NO_DB_PATTERN = re.compile(
    r"^\s*DROP\s+TABLE(?:\s+IF\s+EXISTS)?\s+`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_CREATE_INDEX_NO_DB_PATTERN = re.compile(
    r"^\s*CREATE\s+(?:UNIQUE\s+)?INDEX\s+`?([^`\s]+)`?\s+ON\s+`?([^`\s;(]+)`?",
    re.IGNORECASE,
)

DDL_DROP_INDEX_NO_DB_PATTERN = re.compile(
    r"^\s*DROP\s+INDEX\s+`?([^`\s]+)`?\s+ON\s+`?([^`\s;(]+)`?",
    re.IGNORECASE,
)


def _normalize_mysqlbinlog_scalar(raw_value):
    text = str(raw_value or "").strip()

    if text.upper() == "NULL":
        return None

    if text.startswith("'") and text.endswith("'") and len(text) >= 2:
        text = text[1:-1]
        text = text.replace("\\'", "'").replace("\\\\", "\\")
        return text

    if text in ("0", "1"):
        return int(text)

    try:
        if "." in text:
            return float(text)
        return int(text)
    except Exception:
        return text


def _parse_mysqlbinlog_assignment_line(line):
    line = str(line or "").strip()

    match = MYSQLBINLOG_ASSIGNMENT_PATTERN.match(line)
    if not match:
        return None

    col_index = int(match.group("index"))
    raw_value = match.group("value")
    return {
        "col_index": col_index,
        "column_name": f"UNKNOWN_COL{col_index - 1}",
        "value": _normalize_mysqlbinlog_scalar(raw_value),
        "raw_value": raw_value,
    }


def _parse_mysqlbinlog_section_lines(lines):
    result = {}
    for line in (lines or []):
        parsed = _parse_mysqlbinlog_assignment_line(line)
        if not parsed:
            continue
        result[parsed["column_name"]] = parsed["value"]
    return result


def _extract_pk_from_structured_rows(before_data, after_data):
    before_data = before_data or {}
    after_data = after_data or {}

    for source in (after_data, before_data):
        if "id" in source:
            return {"id": source.get("id")}

    merged_keys = sorted(set(list(before_data.keys()) + list(after_data.keys())))
    for key in merged_keys:
        if str(key).lower().endswith("_id"):
            if key in after_data:
                return {key: after_data.get(key)}
            if key in before_data:
                return {key: before_data.get(key)}

    for source in (after_data, before_data):
        for key, value in source.items():
            if value is None:
                continue
            if isinstance(value, (str, int, float, bool)):
                return {key: value}

    return {}


def _build_business_dedup_signature(item):
    payload = {
        "engine": item.get("engine") or "",
        "database_name": item.get("database_name") or "",
        "table_name": item.get("table_name") or "",
        "operation": item.get("operation") or "",
        "primary_key_data": item.get("primary_key_data") or {},
        "before_data": item.get("before_data") or {},
        "after_data": item.get("after_data") or {},
    }
    raw = _json_dumps_sorted(payload)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class BinlogCaptureError(Exception):
    pass


class ColumnResolutionError(Exception):
    pass


def _build_capture_result(
    ok,
    status,
    return_code,
    error_code="",
    error_message="",
    **extra,
    ):
    payload = {
        "ok": bool(ok),
        "status": str(status or ""),
        "return_code": str(return_code or ""),
        "error_code": str(error_code or ""),
        "error_message": str(error_message or ""),
    }
    payload.update(extra)
    return payload

def _parse_binlog_index(log_file):
    if not log_file or "." not in log_file:
        return -1
    try:
        return int(str(log_file).rsplit(".", 1)[1])
    except Exception:
        return -1




def _compare_binlog_position(file_a, pos_a, file_b, pos_b):
    idx_a = _parse_binlog_index(file_a)
    idx_b = _parse_binlog_index(file_b)

    if idx_a < idx_b:
        return -1
    if idx_a > idx_b:
        return 1

    pos_a = int(pos_a or 0)
    pos_b = int(pos_b or 0)

    if pos_a < pos_b:
        return -1
    if pos_a > pos_b:
        return 1
    return 0


def _is_position_at_or_before(file_a, pos_a, file_b, pos_b):
    return _compare_binlog_position(file_a, pos_a, file_b, pos_b) <= 0


def _is_position_after(file_a, pos_a, file_b, pos_b):
    return _compare_binlog_position(file_a, pos_a, file_b, pos_b) > 0

def _is_hard_excluded_table(table_name):
    table_name = (table_name or "").strip().lower()

    if not table_name:
        return False

    if table_name.startswith("srdf_"):
        return True

    if table_name in {
        "django_migrations",
        "django_content_type",
        "auth_permission",
        "django_admin_log",
    }:
        return True

    return False

def _estimate_backlog_bytes(
    binary_logs,
    binary_log_size_map,
    start_idx,
    end_idx,
    start_log_pos,
    current_master_before_pos,
    ):
    if start_idx < 0 or end_idx < 0 or end_idx < start_idx:
        return 0

    total_bytes = 0

    for idx in range(start_idx, end_idx + 1):
        log_name = binary_logs[idx]
        file_size = int(binary_log_size_map.get(log_name) or 0)

        if idx == start_idx and idx == end_idx:
            total_bytes += max(int(current_master_before_pos or 0) - int(start_log_pos or 0), 0)
        elif idx == start_idx:
            total_bytes += max(file_size - int(start_log_pos or 0), 0)
        elif idx == end_idx:
            total_bytes += max(int(current_master_before_pos or 0), 0)
        else:
            total_bytes += max(file_size, 0)

    return max(int(total_bytes or 0), 0)


def _resolve_adaptive_max_scanned_events(
    replication_service,
    checkpoint,
    resolved_engine,
    explicit_value,
    backlog_bytes,
    backlog_file_count,
    ):
    if explicit_value is not None:
        return max(int(explicit_value or 1), 1), False

    min_value = get_srdf_setting_int(
        key="capture.max_scanned_events_min",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=500,
        minimum=1,
    )

    hard_cap = get_srdf_setting_int(
        key="capture.max_scanned_events_hard_cap",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=50000,
        minimum=100,
    )

    bytes_per_event_hint = get_srdf_setting_int(
        key="capture.adaptive_bytes_per_event_hint",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=2048,
        minimum=128,
    )

    file_weight = get_srdf_setting_int(
        key="capture.adaptive_file_weight",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=1500,
        minimum=0,
    )

    previous_data = checkpoint.checkpoint_data or {}
    previous_scanned = int(previous_data.get("scanned_event_count") or 0)
    previous_skipped = int(previous_data.get("skipped_count") or 0)
    previous_captured = int(previous_data.get("captured_count") or 0)

    adaptive_value = int(min_value or 500)

    estimated_from_bytes = 0
    if int(bytes_per_event_hint or 0) > 0:
        estimated_from_bytes = int(float(backlog_bytes or 0) / float(bytes_per_event_hint))

    adaptive_value = max(adaptive_value, estimated_from_bytes)
    adaptive_value += max(int(backlog_file_count or 0) - 1, 0) * int(file_weight or 0)

    if previous_scanned > 0:
        skip_ratio = float(previous_skipped) / float(previous_scanned)
        if skip_ratio >= 0.90:
            adaptive_value = int(float(adaptive_value) * 2.0)
        elif skip_ratio >= 0.75:
            adaptive_value = int(float(adaptive_value) * 1.5)

    if previous_captured <= 0 and previous_skipped > 0:
        adaptive_value = int(float(adaptive_value) * 1.5)

    adaptive_value = max(adaptive_value, int(min_value or 1))
    adaptive_value = min(adaptive_value, int(hard_cap or adaptive_value))

    return max(int(adaptive_value or 1), 1), True


def _resolve_adaptive_scan_window_max_files(
    replication_service,
    resolved_engine,
    explicit_value,
    backlog_file_count,
):
    if explicit_value is not None:
        return max(int(explicit_value or 1), 1), False

    min_value = get_srdf_setting_int(
        key="capture.scan_window_max_files_min",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=2,
        minimum=1,
    )

    hard_cap = get_srdf_setting_int(
        key="capture.scan_window_max_files_hard_cap",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=12,
        minimum=1,
    )

    adaptive_value = max(int(min_value or 1), int(backlog_file_count or 0))
    adaptive_value = min(adaptive_value, int(hard_cap or adaptive_value))

    return max(int(adaptive_value or 1), 1), True


def _resolve_adaptive_scan_window_max_bytes(
    replication_service,
    resolved_engine,
    explicit_value,
    backlog_bytes,
    ):
    
    if explicit_value is not None:
        return max(int(explicit_value or 1), 1), False

    min_value = get_srdf_setting_int(
        key="capture.scan_window_max_bytes_min",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=64 * 1024 * 1024,
        minimum=1024,
    )

    hard_cap = get_srdf_setting_int(
        key="capture.scan_window_max_bytes_hard_cap",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=512 * 1024 * 1024,
        minimum=1024,
    )

    adaptive_value = max(int(min_value or 0), int(backlog_bytes or 0))
    adaptive_value = min(adaptive_value, int(hard_cap or adaptive_value))

    return max(int(adaptive_value or 1), 1), True

def _parse_row_header(line):
    match = re.match(
        r"^### (UPDATE|INSERT INTO|DELETE FROM) `([^`]+)`\.`([^`]+)`$",
        line.strip(),
    )
    if not match:
        return None

    raw_op = match.group(1)
    if raw_op == "UPDATE":
        op = "update"
    elif raw_op == "INSERT INTO":
        op = "insert"
    else:
        op = "delete"

    return {
        "operation": op,
        "database_name": match.group(2),
        "table_name": match.group(3),
    }


def _extract_mysqlbinlog_statement_sql(line):
    text = str(line or "").strip()

    if text.startswith("/*!"):
        return ""

    if text.startswith("use `"):
        return ""

    if text.startswith("SET TIMESTAMP="):
        return ""

    if text.startswith("DELIMITER "):
        return ""

    return text


def _extract_mysqlbinlog_use_database(line):
    text = str(line or "").strip()
    match = DDL_USE_DATABASE_PATTERN.match(text)
    if not match:
        return ""
    return str(match.group(1) or "").strip()


def _normalize_sql_text(sql_text):
    text = str(sql_text or "").strip()
    text = re.sub(r"\s+", " ", text)
    return text.strip().rstrip(";").strip()


def _split_alter_table_clauses(rest_sql):
    text = str(rest_sql or "").strip().rstrip(";").strip()
    if not text:
        return []

    parts = []
    current = []
    depth = 0

    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth = max(depth - 1, 0)

        if ch == "," and depth == 0:
            clause = "".join(current).strip()
            if clause:
                parts.append(clause)
            current = []
            continue

        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        parts.append(tail)

    return parts


def _build_ddl_item(
    engine,
    log_file,
    log_pos,
    database_name="",
    table_name="",
    object_type="",
    ddl_action="",
    object_name="",
    parent_object_name="",
    raw_sql="",
    operation="ddl",
):
    normalized_sql = _normalize_sql_text(raw_sql)

    return {
        "engine": engine or "",
        "event_family": "ddl",
        "database_name": database_name or "",
        "table_name": table_name or "",
        "operation": operation or "ddl",
        "object_type": object_type or "",
        "ddl_action": ddl_action or "",
        "object_name": object_name or "",
        "parent_object_name": parent_object_name or "",
        "raw_sql": normalized_sql,
        "log_file": log_file or "",
        "log_pos": int(log_pos or 0),
        "primary_key_data": {},
        "before_data": {},
        "after_data": {},
        "event_payload": {
            "event_type": "DDLStatement",
            "row_format": "mysqlbinlog_ddl",
            "raw_sql": normalized_sql,
        },
    }


def _parse_ddl_statement(sql_text, engine="", log_file="", log_pos=0, current_database=""):
    
    sql = _normalize_sql_text(sql_text)
    current_database = str(current_database or "").strip()
    if not sql:
        return []

    match = DDL_CREATE_DATABASE_PATTERN.match(sql)
    if match:
        db_name = match.group(1)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=db_name,
                object_type="database",
                ddl_action="create",
                object_name=db_name,
                raw_sql=sql,
            )
        ]

    match = DDL_DROP_DATABASE_PATTERN.match(sql)
    if match:
        db_name = match.group(1)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=db_name,
                object_type="database",
                ddl_action="drop",
                object_name=db_name,
                raw_sql=sql,
            )
        ]


    match = DDL_CREATE_TABLE_PATTERN.match(sql)
    if match:
        db_name = match.group(1)
        table_name = match.group(2)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=db_name,
                table_name=table_name,
                object_type="table",
                ddl_action="create",
                object_name=table_name,
                parent_object_name=db_name,
                raw_sql=sql,
            )
        ]

    match = DDL_CREATE_TABLE_NO_DB_PATTERN.match(sql)
    if match and current_database:
        table_name = match.group(1)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=current_database,
                table_name=table_name,
                object_type="table",
                ddl_action="create",
                object_name=table_name,
                parent_object_name=current_database,
                raw_sql=sql,
            )
        ]
    
    

    match = DDL_DROP_TABLE_PATTERN.match(sql)
    if match:
        db_name = match.group(1)
        table_name = match.group(2)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=db_name,
                table_name=table_name,
                object_type="table",
                ddl_action="drop",
                object_name=table_name,
                parent_object_name=db_name,
                raw_sql=sql,
            )
        ]
    

    match = DDL_DROP_TABLE_NO_DB_PATTERN.match(sql)
    if match and current_database:
        table_name = match.group(1)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=current_database,
                table_name=table_name,
                object_type="table",
                ddl_action="drop",
                object_name=table_name,
                parent_object_name=current_database,
                raw_sql=sql,
            )
        ]
    

    match = DDL_CREATE_INDEX_PATTERN.match(sql)
    if match:
        index_name = match.group(1)
        db_name = match.group(2)
        table_name = match.group(3)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=db_name,
                table_name=table_name,
                object_type="index",
                ddl_action="create",
                object_name=index_name,
                parent_object_name=table_name,
                raw_sql=sql,
            )
        ]
    

    match = DDL_CREATE_INDEX_NO_DB_PATTERN.match(sql)
    if match and current_database:
        index_name = match.group(1)
        table_name = match.group(2)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=current_database,
                table_name=table_name,
                object_type="index",
                ddl_action="create",
                object_name=index_name,
                parent_object_name=table_name,
                raw_sql=sql,
            )
        ]
    
    

    match = DDL_DROP_INDEX_PATTERN.match(sql)
    if match:
        index_name = match.group(1)
        db_name = match.group(2)
        table_name = match.group(3)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=db_name,
                table_name=table_name,
                object_type="index",
                ddl_action="drop",
                object_name=index_name,
                parent_object_name=table_name,
                raw_sql=sql,
            )
        ]


    match = DDL_DROP_INDEX_NO_DB_PATTERN.match(sql)
    if match and current_database:
        index_name = match.group(1)
        table_name = match.group(2)
        return [
            _build_ddl_item(
                engine=engine,
                log_file=log_file,
                log_pos=log_pos,
                database_name=current_database,
                table_name=table_name,
                object_type="index",
                ddl_action="drop",
                object_name=index_name,
                parent_object_name=table_name,
                raw_sql=sql,
            )
        ]
    
    


    match = DDL_ALTER_TABLE_PATTERN.match(sql)
    if match:
        db_name = match.group(1)
        table_name = match.group(2)
        rest_sql = match.group(3) or ""

        clauses = _split_alter_table_clauses(rest_sql)
        if not clauses:
            return [
                _build_ddl_item(
                    engine=engine,
                    log_file=log_file,
                    log_pos=log_pos,
                    database_name=db_name,
                    table_name=table_name,
                    object_type="table",
                    ddl_action="alter",
                    object_name=table_name,
                    parent_object_name=db_name,
                    raw_sql=sql,
                )
            ]

        items = []

        for clause in clauses:
            clause_sql = _normalize_sql_text(f"ALTER TABLE `{db_name}`.`{table_name}` {clause}")

            add_col = DDL_ADD_COLUMN_PATTERN.match(clause)
            if add_col:
                column_name = add_col.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="alter",
                        object_name=column_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            drop_col = DDL_DROP_COLUMN_PATTERN.match(clause)
            if drop_col:
                column_name = drop_col.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="drop",
                        object_name=column_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            modify_col = DDL_MODIFY_COLUMN_PATTERN.match(clause)
            if modify_col:
                column_name = modify_col.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="alter",
                        object_name=column_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            change_col = DDL_CHANGE_COLUMN_PATTERN.match(clause)
            if change_col:
                old_name = change_col.group(1)
                new_name = change_col.group(2)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="alter",
                        object_name=new_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                event_payload = items[-1].get("event_payload") or {}
                event_payload["old_object_name"] = old_name
                items[-1]["event_payload"] = event_payload
                continue

            add_index = DDL_ADD_INDEX_PATTERN.match(clause)
            if add_index:
                index_name = add_index.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="index",
                        ddl_action="create",
                        object_name=index_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            if DDL_ADD_PRIMARY_KEY_PATTERN.match(clause):
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="index",
                        ddl_action="create",
                        object_name="PRIMARY",
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            if DDL_DROP_PRIMARY_KEY_PATTERN.match(clause):
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="index",
                        ddl_action="drop",
                        object_name="PRIMARY",
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            items.append(
                _build_ddl_item(
                    engine=engine,
                    log_file=log_file,
                    log_pos=log_pos,
                    database_name=db_name,
                    table_name=table_name,
                    object_type="table",
                    ddl_action="alter",
                    object_name=table_name,
                    parent_object_name=table_name,
                    raw_sql=clause_sql,
                )
            )

        return items
    
    #====================================================
    #==== NEW ALTER TABLE                              ==
    #====================================================
    
    match = DDL_ALTER_TABLE_NO_DB_PATTERN.match(sql)
    if match and current_database:
        db_name = current_database
        table_name = match.group(1)
        rest_sql = match.group(2) or ""

        clauses = _split_alter_table_clauses(rest_sql)
        if not clauses:
            return [
                _build_ddl_item(
                    engine=engine,
                    log_file=log_file,
                    log_pos=log_pos,
                    database_name=db_name,
                    table_name=table_name,
                    object_type="table",
                    ddl_action="alter",
                    object_name=table_name,
                    parent_object_name=db_name,
                    raw_sql=sql,
                )
            ]

        items = []

        for clause in clauses:
            clause_sql = _normalize_sql_text(f"ALTER TABLE `{table_name}` {clause}")

            add_col = DDL_ADD_COLUMN_PATTERN.match(clause)
            if add_col:
                column_name = add_col.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="alter",
                        object_name=column_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            drop_col = DDL_DROP_COLUMN_PATTERN.match(clause)
            if drop_col:
                column_name = drop_col.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="drop",
                        object_name=column_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            modify_col = DDL_MODIFY_COLUMN_PATTERN.match(clause)
            if modify_col:
                column_name = modify_col.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="alter",
                        object_name=column_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            change_col = DDL_CHANGE_COLUMN_PATTERN.match(clause)
            if change_col:
                old_name = change_col.group(1)
                new_name = change_col.group(2)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="column",
                        ddl_action="alter",
                        object_name=new_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                event_payload = items[-1].get("event_payload") or {}
                event_payload["old_object_name"] = old_name
                items[-1]["event_payload"] = event_payload
                continue

            add_index = DDL_ADD_INDEX_PATTERN.match(clause)
            if add_index:
                index_name = add_index.group(1)
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="index",
                        ddl_action="create",
                        object_name=index_name,
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            if DDL_ADD_PRIMARY_KEY_PATTERN.match(clause):
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="index",
                        ddl_action="create",
                        object_name="PRIMARY",
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            if DDL_DROP_PRIMARY_KEY_PATTERN.match(clause):
                items.append(
                    _build_ddl_item(
                        engine=engine,
                        log_file=log_file,
                        log_pos=log_pos,
                        database_name=db_name,
                        table_name=table_name,
                        object_type="index",
                        ddl_action="drop",
                        object_name="PRIMARY",
                        parent_object_name=table_name,
                        raw_sql=clause_sql,
                    )
                )
                continue

            items.append(
                _build_ddl_item(
                    engine=engine,
                    log_file=log_file,
                    log_pos=log_pos,
                    database_name=db_name,
                    table_name=table_name,
                    object_type="table",
                    ddl_action="alter",
                    object_name=table_name,
                    parent_object_name=table_name,
                    raw_sql=clause_sql,
                )
            )

        return items    
    
    
    
    

    return []

def _json_dumps_sorted(data):
    return json.dumps(
        data or {},
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )


def _build_control_signature(item):
    payload = {
        "log_file": item.get("log_file") or "",
        "log_pos": int(item.get("log_pos") or 0),
        "engine": item.get("engine") or "",
        "database_name": item.get("database_name") or "",
        "table_name": item.get("table_name") or "",
        "operation": item.get("operation") or "",
        "primary_key_data": item.get("primary_key_data") or {},
        "before_data": item.get("before_data") or {},
        "after_data": item.get("after_data") or {},
    }
    raw = _json_dumps_sorted(payload)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _extract_control_row_count(item):
    payload = item.get("event_payload") or {}
    raw_row = payload.get("raw_row")
    if raw_row:
        return 1

    raw_lines = payload.get("raw_lines") or []
    if raw_lines:
        return 1

    return 0


def _format_console_table(headers, rows):
    widths = []
    for col_index, header in enumerate(headers):
        max_width = len(str(header))
        for row in rows:
            value = ""
            if col_index < len(row):
                value = row[col_index]
            value = str(value if value is not None else "")
            if len(value) > max_width:
                max_width = len(value)
        widths.append(max_width)

    def _fmt_row(values):
        parts = []
        for idx, value in enumerate(values):
            text = str(value if value is not None else "")
            parts.append(text.ljust(widths[idx]))
        return " | ".join(parts)

    separator = "-+-".join("-" * w for w in widths)

    lines = [
        _fmt_row(headers),
        separator,
    ]
    for row in rows:
        lines.append(_fmt_row(row))

    return "\n".join(lines)




def _print_console_table(headers, rows):
    return _format_console_table(headers, rows)




def _print_console_block(title, lines):
    safe_lines = [str(x) for x in (lines or [])]
    width = max([len(title)] + [len(x) for x in safe_lines] + [20])

    border = "=" * width
    return "\n".join([border, title, border] + safe_lines + [border])


def _build_control_console_header(control_mode, replication_service, collected):
    resolved = collected.get("resolved_settings") or {}


    lines = [
        f"mode                         : {control_mode}",
        f"service_id                   : {replication_service.id}",
        f"service_name                 : {replication_service.name}",
        f"engine                       : {replication_service.service_type}",
        f"checkpoint_start             : {collected.get('checkpoint_start_file', '')}:{int(collected.get('checkpoint_start_pos') or 0)}",
        f"current_master               : {collected.get('current_master_before_file', '')}:{int(collected.get('current_master_before_pos') or 0)}",
        f"window_end                   : {collected.get('window_end_file', '')}:{int(collected.get('window_end_pos') or 0)}",
        f"stop_reason                  : {collected.get('stop_reason', '')}",
        "",
        "resolved settings",
        f"  limit                      : {int(resolved.get('limit') or 0)}",
        f"  max_scanned_events         : {int(resolved.get('max_scanned_events') or 0)}",
        f"  adaptive_max_scanned_used  : {bool(resolved.get('adaptive_max_scanned_events_used'))}",
        f"  max_runtime_sec            : {int(resolved.get('max_runtime_seconds') or 0)}",
        f"  scan_window_files          : {int(resolved.get('scan_window_max_files') or 0)}",
        f"  adaptive_window_files_used : {bool(resolved.get('adaptive_scan_window_max_files_used'))}",
        f"  scan_window_bytes          : {int(resolved.get('scan_window_max_bytes') or 0)}",
        f"  adaptive_window_bytes_used : {bool(resolved.get('adaptive_scan_window_max_bytes_used'))}",
        f"  estimated_backlog_bytes    : {int(resolved.get('estimated_backlog_bytes') or 0)}",
        f"  estimated_backlog_file_cnt : {int(resolved.get('estimated_backlog_file_count') or 0)}",        f"  debug                      : {bool(resolved.get('debug'))}",
    ]    
    
    return lines



def _build_control_console_footer(
    total_binlog_updates=0,
    total_captured_matches=0,
    total_not_captured=0,
    total_duplicates=0,
    total_ambiguous=0,
    total_ignored=0,
    ):
    
    return [
        f"total_binlog_updates  : {int(total_binlog_updates or 0)}",
        f"captured_matches      : {int(total_captured_matches or 0)}",
        f"not_captured          : {int(total_not_captured or 0)}",
        f"duplicates            : {int(total_duplicates or 0)}",
        f"ambiguous             : {int(total_ambiguous or 0)}",
        f"ignored               : {int(total_ignored or 0)}",
    ]



def _emit_control_progress(
    control_mode,
    scanned_count,
    total_count,
    matched_count=0,
    ignored_count=0,
    ):
    total_count = int(total_count or 0)
    scanned_count = int(scanned_count or 0)
    matched_count = int(matched_count or 0)
    ignored_count = int(ignored_count or 0)

    percent = 0
    if total_count > 0:
        percent = int((float(scanned_count) / float(total_count)) * 100.0)
        if percent > 100:
            percent = 100

    line = (
        f"\r[SRDF CONTROL] mode={control_mode} "
        f"progress={percent:3d}% "
        f"scanned={scanned_count}/{total_count} "
        f"matched={matched_count} "
        f"ignored={ignored_count}"
    )
    print(line, end="", flush=True)
    
    
    
def _slice_console_preview_rows(rows, preview_limit=CONTROL_CONSOLE_PREVIEW_LIMIT):
    rows = rows or []
    preview_limit = max(int(preview_limit or 0), 0)

    if preview_limit <= 0:
        return [], 0

    if len(rows) <= preview_limit:
        return rows, 0

    return rows[:preview_limit], len(rows) - preview_limit


def _build_preview_footer(extra_hidden_count):
    extra_hidden_count = int(extra_hidden_count or 0)
    if extra_hidden_count <= 0:
        return []

    return [
        f"preview limited to first {CONTROL_CONSOLE_PREVIEW_LIMIT} rows",
        f"hidden rows            : {extra_hidden_count}",
        "full detail available in SQL tables:",
        "  - SRDFCaptureControlRun",
        "  - SRDFCaptureControlItem",
    ]

def _finish_control_progress():
    print("", flush=True)

def _build_control_lock_name(replication_service_id, control_mode):
    return f"srdf_capture_control:{control_mode}:service_{int(replication_service_id)}"


def _acquire_control_lock(replication_service, control_mode):
    lock_name = _build_control_lock_name(replication_service.id, control_mode)

    lock_obj, _ = SRDFServiceExecutionLock.objects.get_or_create(
        name=lock_name,
        replication_service=replication_service,
        defaults={
            "is_locked": False,
            "owner_token": "",
        },
    )

    if lock_obj.is_locked:
        raise BinlogCaptureError(
            f"Control lock already active for service #{replication_service.id} mode={control_mode}"
        )

    owner_token = str(uuid.uuid4())
    lock_obj.is_locked = True
    lock_obj.owner_token = owner_token
    lock_obj.locked_at = timezone.now()
    lock_obj.expires_at = None
    lock_obj.save(
        update_fields=[
            "is_locked",
            "owner_token",
            "locked_at",
            "expires_at",
            "updated_at",
        ]
    )
    return lock_obj, owner_token


def _release_control_lock(lock_obj, owner_token=""):
    if not lock_obj:
        return

    if owner_token and lock_obj.owner_token and lock_obj.owner_token != owner_token:
        return

    lock_obj.is_locked = False
    lock_obj.owner_token = ""
    lock_obj.locked_at = None
    lock_obj.expires_at = None
    lock_obj.save(
        update_fields=[
            "is_locked",
            "owner_token",
            "locked_at",
            "expires_at",
            "updated_at",
        ]
    )


def _create_control_run(
    replication_service,
    control_mode,
    initiated_by,
    command_line="",
    request_payload=None,
):
    return SRDFCaptureControlRun.objects.create(
        run_uid=str(uuid.uuid4()),
        control_mode=control_mode,
        status="running",
        replication_service=replication_service,
        node=replication_service.source_node,
        engine=replication_service.service_type or "",
        initiated_by=initiated_by or "",
        command_line=command_line or "",
        request_payload=request_payload or {},
    )


def _finalize_control_run(
    control_run,
    status,
    checkpoint_start_file="",
    checkpoint_start_pos=0,
    current_master_before_file="",
    current_master_before_pos=0,
    current_master_after_file="",
    current_master_after_pos=0,
    window_end_file="",
    window_end_pos=0,
    total_binlog_updates=0,
    total_captured_matches=0,
    total_not_captured=0,
    total_duplicates=0,
    total_ambiguous=0,
    total_ignored=0,
    last_error="",
    result_payload=None,
):
    finished_at = timezone.now()
    duration_seconds = 0.0
    if control_run.started_at:
        duration_seconds = round(
            (finished_at - control_run.started_at).total_seconds(),
            3,
        )

    control_run.status = status or "success"
    control_run.checkpoint_start_file = checkpoint_start_file or ""
    control_run.checkpoint_start_pos = int(checkpoint_start_pos or 0)
    control_run.current_master_before_file = current_master_before_file or ""
    control_run.current_master_before_pos = int(current_master_before_pos or 0)
    control_run.current_master_after_file = current_master_after_file or ""
    control_run.current_master_after_pos = int(current_master_after_pos or 0)
    control_run.window_end_file = window_end_file or ""
    control_run.window_end_pos = int(window_end_pos or 0)
    control_run.total_binlog_updates = int(total_binlog_updates or 0)
    control_run.total_captured_matches = int(total_captured_matches or 0)
    control_run.total_not_captured = int(total_not_captured or 0)
    control_run.total_duplicates = int(total_duplicates or 0)
    control_run.total_ambiguous = int(total_ambiguous or 0)
    control_run.total_ignored = int(total_ignored or 0)
    control_run.finished_at = finished_at
    control_run.duration_seconds = duration_seconds
    control_run.last_error = last_error or ""
    control_run.result_payload = result_payload or {}

    control_run.save(
        update_fields=[
            "status",
            "checkpoint_start_file",
            "checkpoint_start_pos",
            "current_master_before_file",
            "current_master_before_pos",
            "current_master_after_file",
            "current_master_after_pos",
            "window_end_file",
            "window_end_pos",
            "total_binlog_updates",
            "total_captured_matches",
            "total_not_captured",
            "total_duplicates",
            "total_ambiguous",
            "total_ignored",
            "finished_at",
            "duration_seconds",
            "last_error",
            "result_payload",
        ]
    )
    return control_run


def _save_control_items(control_run, replication_service, rows):
    instances = []

    for idx, row in enumerate(rows, start=1):
        item = row.get("item") or {}
        instances.append(
            SRDFCaptureControlItem(
                control_run=control_run,
                replication_service=replication_service,
                seq=idx,
                log_file=item.get("log_file") or "",
                log_pos=int(item.get("log_pos") or 0),
                engine=item.get("engine") or "",
                database_name=item.get("database_name") or "",
                table_name=item.get("table_name") or "",
                operation=item.get("operation") or "",
                row_count=int(row.get("row_count") or 0),
                event_signature=row.get("event_signature") or "",
                outbound_match_count=int(row.get("outbound_match_count") or 0),
                first_outbound_event_id=int(row.get("first_outbound_event_id") or 0),
                status=row.get("status") or "not_captured",
                notes=row.get("notes") or "",
                payload_preview=row.get("payload_preview") or {},
            )
        )

    if instances:
        SRDFCaptureControlItem.objects.bulk_create(
            instances,
            batch_size=250,
        )

    return len(instances)


def _load_outbound_signature_map(replication_service):
    qs = (
        OutboundChangeEvent.objects
        .filter(replication_service=replication_service)
        .only("id", "event_payload")
        .order_by("id")
    )

    signature_map = {}

    for obj in qs.iterator():
        payload = obj.event_payload or {}
        signature = (payload.get("control_signature") or "").strip()
        if not signature:
            continue
        signature_map.setdefault(signature, []).append(obj.id)

    return signature_map


def _build_control_match_row(item, signature_map, default_status="not_captured"):
    signature = _build_control_signature(item)
    outbound_ids = signature_map.get(signature, [])

    outbound_match_count = len(outbound_ids)
    first_outbound_event_id = outbound_ids[0] if outbound_ids else 0

    if default_status == "ignored":
        status = "ignored"
    elif outbound_match_count == 0:
        status = "not_captured"
    elif outbound_match_count == 1:
        status = "captured"
    else:
        status = "duplicate"

    return {
        "item": item,
        "row_count": _extract_control_row_count(item),
        "event_signature": signature,
        "outbound_match_count": outbound_match_count,
        "first_outbound_event_id": first_outbound_event_id,
        "status": status,
        "notes": "",
        "payload_preview": {
            "primary_key_data": item.get("primary_key_data") or {},
            "before_data": item.get("before_data") or {},
            "after_data": item.get("after_data") or {},
        },
    }


def _collect_binlog_control_rows(
    replication_service,
    initiated_by="srdf_control",
    limit=None,
    max_scanned_events=None,
    max_runtime_seconds=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
    debug=None,
    ):
    
    checkpoint = _get_or_create_capture_checkpoint(replication_service)
    resolved_engine = replication_service.service_type or ""
    
    metadata_conn = None
    column_cache = {}
    
    
    if limit is None:
        limit = get_srdf_setting_int(
            key="capture.limit",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=100,
            minimum=1,
        )
    else:
        limit = max(int(limit or 100), 1)

    if max_scanned_events is not None:
        max_scanned_events = max(int(max_scanned_events or 500), 1)
        
    if max_runtime_seconds is None:
        max_runtime_seconds = get_srdf_setting_int(
            key="capture.max_runtime_seconds",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=20,
            minimum=1,
        )
    else:
        max_runtime_seconds = max(int(max_runtime_seconds or 20), 1)



    if scan_window_max_files is not None:
        scan_window_max_files = max(int(scan_window_max_files or 1), 1)

    if scan_window_max_bytes is not None:
        scan_window_max_bytes = max(int(scan_window_max_bytes or (16 * 1024 * 1024)), 1)


    if debug is None:
        debug = get_srdf_setting_bool(
            key="capture.debug",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=False,
        )
    else:
        debug = bool(debug)

    start_log_file = checkpoint.log_file or ""
    start_log_pos = int(checkpoint.log_pos or 0)

    if not start_log_file:
        raise BinlogCaptureError("Missing checkpoint.log_file for control mode")

    current_master_before_file, current_master_before_pos = _fetch_current_master_status(replication_service)
    current_master_after_file = current_master_before_file
    current_master_after_pos = current_master_before_pos

    binary_log_rows = _fetch_binary_logs(replication_service)
    binary_logs = [row["log_name"] for row in binary_log_rows]
    binary_log_size_map = {
        row["log_name"]: int(row.get("file_size") or 0)
        for row in binary_log_rows
    }

    if start_log_file not in binary_logs:
        raise BinlogCaptureError(
            f"Checkpoint file not found in SHOW BINARY LOGS: {start_log_file}"
        )

    if current_master_before_file not in binary_logs:
        raise BinlogCaptureError(
            f"Current master file not found in SHOW BINARY LOGS: {current_master_before_file}"
        )

    start_idx = binary_logs.index(start_log_file)
    end_idx = binary_logs.index(current_master_before_file)
    
    
    
    backlog_file_count = max((end_idx - start_idx) + 1, 0)
    backlog_bytes = _estimate_backlog_bytes(
        binary_logs=binary_logs,
        binary_log_size_map=binary_log_size_map,
        start_idx=start_idx,
        end_idx=end_idx,
        start_log_pos=start_log_pos,
        current_master_before_pos=current_master_before_pos,
    )

    max_scanned_events, adaptive_max_scanned_events_used = _resolve_adaptive_max_scanned_events(
        replication_service=replication_service,
        checkpoint=checkpoint,
        resolved_engine=resolved_engine,
        explicit_value=max_scanned_events,
        backlog_bytes=backlog_bytes,
        backlog_file_count=backlog_file_count,
    )
    
    scan_window_max_files, adaptive_scan_window_max_files_used = _resolve_adaptive_scan_window_max_files(
        replication_service=replication_service,
        resolved_engine=resolved_engine,
        explicit_value=scan_window_max_files,
        backlog_file_count=backlog_file_count,
    )

    scan_window_max_bytes, adaptive_scan_window_max_bytes_used = _resolve_adaptive_scan_window_max_bytes(
        replication_service=replication_service,
        resolved_engine=resolved_engine,
        explicit_value=scan_window_max_bytes,
        backlog_bytes=backlog_bytes,
    )    



    if start_idx > end_idx:
        raise BinlogCaptureError(
            f"Checkpoint file is after current master file: checkpoint={start_log_file} master={current_master_before_file}"
        )

    max_files = max(int(scan_window_max_files or 1), 1)
    max_bytes = max(int(scan_window_max_bytes or 0), 0)

    window_end_idx = min(end_idx, start_idx + max_files - 1)
    window_end_file = binary_logs[window_end_idx]

    if start_idx == end_idx and window_end_file == current_master_before_file:
        if max_bytes > 0:
            window_end_pos = min(
                int(current_master_before_pos or 0),
                int(start_log_pos or 0) + max_bytes,
            )
        else:
            window_end_pos = int(current_master_before_pos or 0)
    elif window_end_idx < end_idx:
        window_end_pos = int(binary_log_size_map.get(window_end_file) or 0)
    else:
        window_end_pos = int(current_master_before_pos or 0)

    if _compare_binlog_position(start_log_file, start_log_pos, window_end_file, window_end_pos) >= 0:
        return {
            
            "rows": [],
            "resolved_settings": {
                "limit": int(limit or 0),
                "max_scanned_events": int(max_scanned_events or 0),
                "max_runtime_seconds": int(max_runtime_seconds or 0),
                "scan_window_max_files": int(scan_window_max_files or 0),
                "scan_window_max_bytes": int(scan_window_max_bytes or 0),
                "debug": bool(debug),
                "adaptive_max_scanned_events_used": bool(adaptive_max_scanned_events_used),
                "adaptive_scan_window_max_files_used": bool(adaptive_scan_window_max_files_used),
                "adaptive_scan_window_max_bytes_used": bool(adaptive_scan_window_max_bytes_used),
                "estimated_backlog_bytes": int(backlog_bytes or 0),
                "estimated_backlog_file_count": int(backlog_file_count or 0),
            },
            
            "checkpoint_start_file": start_log_file,
            "checkpoint_start_pos": start_log_pos,
            "current_master_before_file": current_master_before_file,
            "current_master_before_pos": int(current_master_before_pos or 0),
            "current_master_after_file": current_master_after_file,
            "current_master_after_pos": int(current_master_after_pos or 0),
            "window_end_file": window_end_file,
            "window_end_pos": int(window_end_pos or 0),
            "stop_reason": "no_updates_available",
        }

    binlog_files_to_read = binary_logs[start_idx:window_end_idx + 1]
    config = _get_capture_config(replication_service)
    mysqlbinlog_path = config.get("mysqlbinlog_path") or "mysqlbinlog"

    cmd = [
        mysqlbinlog_path,
        "--read-from-remote-server",
        "--verbose",
        "--base64-output=DECODE-ROWS",
        "-h",
        config["host"],
        "-P",
        str(int(config.get("port", 3306))),
        "-u",
        config["user"],
        f"--start-position={int(start_log_pos)}",
    ]

    if binlog_files_to_read:
        cmd.extend(binlog_files_to_read[:-1])
        cmd.append(f"--stop-position={int(window_end_pos)}")
        cmd.append(binlog_files_to_read[-1])



    env = os.environ.copy()
    if config.get("password"):
        env["MYSQL_PWD"] = str(config.get("password") or "")
    
    metadata_conn = _get_metadata_connection(config)
    
    timeout_seconds = int(float(max_runtime_seconds)) + 5 if max_runtime_seconds else None
    
    result = subprocess.run(
        cmd,
        capture_output=True,
        timeout=timeout_seconds,
        env=env,
    )

    stdout_text = (result.stdout or b"").decode("utf-8", errors="replace")
    stderr_text = (result.stderr or b"").decode("utf-8", errors="replace")

    if result.returncode != 0:
        raise BinlogCaptureError(
            "mysqlbinlog failed: "
            + (stderr_text.strip() or stdout_text.strip() or f"returncode={result.returncode}")
        )

    started_at_ts = time.monotonic()
    current_event_pos = start_log_pos
    current_event = None
    current_section = None
    current_binlog_file_index = 0
    
    current_event_file = binlog_files_to_read[0] if binlog_files_to_read else start_log_file
    last_seen_at_pos = int(start_log_pos or 0)
    current_statement_database = ""

    rows = []
    scanned_event_count = 0

    def finalize_current_event():
        nonlocal current_event
        nonlocal current_section
        nonlocal scanned_event_count

        if not current_event:
            return False

        event_log_pos = int(current_event.get("log_pos") or 0)
        event_log_file = current_event.get("log_file") or start_log_file

        scanned_event_count += 1

        if max_scanned_events and scanned_event_count > int(max_scanned_events):
            return True

        elapsed_seconds = time.monotonic() - started_at_ts
        if max_runtime_seconds and elapsed_seconds >= float(max_runtime_seconds):
            return True

        table_name = current_event.get("table_name") or ""
        database_name = current_event.get("database_name") or ""
        operation = current_event.get("operation") or ""

        if _is_hard_excluded_table(table_name):
            rows.append(
                {
                    
                    "item": {
                        "engine": replication_service.service_type,
                        "database_name": database_name,
                        "table_name": table_name,
                        "operation": operation,
                        "log_file": event_log_file,
                        "log_pos": event_log_pos,
                        "primary_key_data": {},
                        "before_data": {},
                        "after_data": {},
                        "event_payload": {
                            "event_type": current_event.get("event_type") or "",
                            "raw_lines": current_event.get("raw_lines") or [],
                        },
                    },
                    
                    "row_count": int(current_event.get("rows_count") or 0),
                    "event_signature": "",
                    "outbound_match_count": 0,
                    "first_outbound_event_id": 0,
                    "status": "ignored",
                    "notes": "Hard excluded table",
                    "payload_preview": {},
                }
            )
            current_event = None
            current_section = None
            return False
        


        before_data = _parse_mysqlbinlog_section_lines(current_event.get("before_lines") or [])
        after_data = _parse_mysqlbinlog_section_lines(current_event.get("after_lines") or [])
        primary_key_data = _extract_pk_from_structured_rows(before_data, after_data)

        item = {
            "engine": replication_service.service_type,
            "database_name": database_name,
            "table_name": table_name,
            "operation": operation,
            "log_file": event_log_file,
            "log_pos": event_log_pos,
            "primary_key_data": primary_key_data,
            "before_data": before_data,
            "after_data": after_data,
            "event_payload": {
                "event_type": current_event.get("event_type") or "",
                "raw_lines": current_event.get("raw_lines") or [],
                "before_lines": current_event.get("before_lines") or [],
                "after_lines": current_event.get("after_lines") or [],
                "row_format": "mysqlbinlog_text",
            },
        }

        item["event_payload"]["business_dedup_signature"] = _build_business_dedup_signature(item)
        
        item = _resolve_unknown_columns_for_item(
            replication_service=replication_service,
            item=item,
            metadata_conn=metadata_conn,
            column_cache=column_cache,
        )        

        rule_result = explain_rule_match(
            engine=item.get("engine"),
            database_name=item.get("database_name"),
            table_name=item.get("table_name"),
        )

        if not rule_result.get("allowed"):
            rows.append(
                {
                    "item": item,
                    "row_count": int(current_event.get("rows_count") or 0),
                    "event_signature": "",
                    "outbound_match_count": 0,
                    "first_outbound_event_id": 0,
                    "status": "ignored",
                    "notes": "Filtered by SQL scope rules",
                    "payload_preview": {
                        "decision": rule_result.get("decision") or "",
                        "matched_rule": rule_result.get("matched_rule") or {},
                    },
                }
            )
            current_event = None
            current_section = None
            return False

        rows.append(
            {
                "item": item,
                "row_count": int(current_event.get("rows_count") or 0),
                "event_signature": "",
                "outbound_match_count": 0,
                "first_outbound_event_id": 0,
                "status": "not_captured",
                "notes": "",
                "payload_preview": {
                    "primary_key_data": item.get("primary_key_data") or {},
                    "before_data": item.get("before_data") or {},
                    "after_data": item.get("after_data") or {},
                },
            }
        )

        current_event = None
        current_section = None

        if limit and len(rows) >= int(limit):
            return True

        return False

    for raw_line in stdout_text.splitlines():
        line = raw_line.rstrip("\n")
        stripped_line = line.strip()
        use_database = _extract_mysqlbinlog_use_database(stripped_line)
        if use_database:
            current_statement_database = use_database
            continue        
        

        if line.startswith("# at "):
            try:
                next_pos = int(line.split()[2])

                if (
                    next_pos < last_seen_at_pos
                    and current_binlog_file_index < len(binlog_files_to_read) - 1
                ):
                    current_binlog_file_index += 1
                    current_event_file = binlog_files_to_read[current_binlog_file_index]

                current_event_pos = next_pos
                last_seen_at_pos = next_pos
            except Exception:
                pass
            continue
        
        
        ddl_items = _parse_ddl_statement(
            sql_text=_extract_mysqlbinlog_statement_sql(stripped_line),
            engine=replication_service.service_type,
            log_file=current_event_file,
            log_pos=int(current_event_pos or 0),
            current_database=current_statement_database,
        )
        
        
        if ddl_items:
            should_stop = finalize_current_event()
            if should_stop:
                break
    
            for ddl_item in ddl_items:
                rows.append(
                        {
                            "item": ddl_item,
                            "row_count": 1,
                            "event_signature": "",
                            "outbound_match_count": 0,
                            "first_outbound_event_id": 0,
                            "status": "not_captured",
                            "notes": "",
                            "payload_preview": {
                                "raw_sql": ddl_item.get("raw_sql") or "",
                                "object_type": ddl_item.get("object_type") or "",
                                "ddl_action": ddl_item.get("ddl_action") or "",
                                "object_name": ddl_item.get("object_name") or "",
                                },
                        }
                    )
                scanned_event_count += 1
                if max_scanned_events and scanned_event_count > int(max_scanned_events):
                    break
    
            if max_scanned_events and scanned_event_count > int(max_scanned_events):
                break
    
            continue        


        if line.startswith("### "):
            header = _parse_row_header(line)
            if header:
                should_stop = finalize_current_event()
                if should_stop:
                    break

                current_event = {
                    "event_type": f"{header['operation'].title()}RowsEvent",
                    "operation": header["operation"],
                    "database_name": header["database_name"],
                    "table_name": header["table_name"],
                    "log_file": current_event_file,
                    "log_pos": int(current_event_pos or 0),
                    "before_lines": [],
                    "after_lines": [],
                    "raw_lines": [line],
                    "rows_count": 1,
                }
                current_section = None
                continue

            if current_event is not None:
                current_event["raw_lines"].append(line)

                stripped = line.strip()
                if stripped == "### WHERE":
                    current_section = "before"
                    continue
                if stripped == "### SET":
                    current_section = "after"
                    continue

                if line.startswith("###   "):
                    payload_line = line[6:].strip()
                    if current_section == "before":
                        current_event["before_lines"].append(payload_line)
                    elif current_section == "after":
                        current_event["after_lines"].append(payload_line)
                    else:
                        current_event["after_lines"].append(payload_line)
                continue

    try:
        
        if current_event is not None:
            finalize_current_event()
    
        return {
            "rows": rows,
            
            "resolved_settings": {
                "limit": int(limit or 0),
                "max_scanned_events": int(max_scanned_events or 0),
                "max_runtime_seconds": int(max_runtime_seconds or 0),
                "scan_window_max_files": int(scan_window_max_files or 0),
                "scan_window_max_bytes": int(scan_window_max_bytes or 0),
                "debug": bool(debug),
                "adaptive_max_scanned_events_used": bool(adaptive_max_scanned_events_used),
                "adaptive_scan_window_max_files_used": bool(adaptive_scan_window_max_files_used),
                "adaptive_scan_window_max_bytes_used": bool(adaptive_scan_window_max_bytes_used),
                "estimated_backlog_bytes": int(backlog_bytes or 0),
                "estimated_backlog_file_count": int(backlog_file_count or 0),
            },
            
            "checkpoint_start_file": start_log_file,
            "checkpoint_start_pos": int(start_log_pos or 0),
            "current_master_before_file": current_master_before_file,
            "current_master_before_pos": int(current_master_before_pos or 0),
            "current_master_after_file": current_master_after_file,
            "current_master_after_pos": int(current_master_after_pos or 0),
            "window_end_file": window_end_file,
            "window_end_pos": int(window_end_pos or 0),
            "stop_reason": "completed",
        }
    finally:
        if metadata_conn is not None:
            metadata_conn.close()    


def list_binlog_updates_since_checkpoint(
    replication_service_id,
    initiated_by="srdf_control",
    command_line="",
    limit=None,
    max_scanned_events=None,
    max_runtime_seconds=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
    debug=None,
    ):
    
    replication_service = _get_replication_service(replication_service_id)

    control_run = None
    lock_obj = None
    owner_token = ""

    try:
        lock_obj, owner_token = _acquire_control_lock(
            replication_service=replication_service,
            control_mode="list_updates",
        )

        request_payload = {
            "limit": limit,
            "max_scanned_events": max_scanned_events,
            "max_runtime_seconds": max_runtime_seconds,
            "scan_window_max_files": scan_window_max_files,
            "scan_window_max_bytes": scan_window_max_bytes,
            "debug": bool(debug),
        }

        control_run = _create_control_run(
            replication_service=replication_service,
            control_mode="list_updates",
            initiated_by=initiated_by,
            command_line=command_line,
            request_payload=request_payload,
        )

        collected = _collect_binlog_control_rows(
            replication_service=replication_service,
            initiated_by=initiated_by,
            limit=limit,
            max_scanned_events=max_scanned_events,
            max_runtime_seconds=max_runtime_seconds,
            scan_window_max_files=scan_window_max_files,
            scan_window_max_bytes=scan_window_max_bytes,
            debug=debug,
        )

        rows = collected.get("rows") or []
        signature_map = _load_outbound_signature_map(replication_service)

        matched_rows = []
        total_captured_matches = 0
        total_not_captured = 0
        total_duplicates = 0
        total_ignored = 0

        for row in rows:
            if row.get("status") == "ignored":
                matched_rows.append(row)
                total_ignored += 1
                continue

            matched = _build_control_match_row(
                item=row.get("item") or {},
                signature_map=signature_map,
            )
            matched_rows.append(matched)

            if matched["status"] == "captured":
                total_captured_matches += 1
            elif matched["status"] == "duplicate":
                total_duplicates += 1
            elif matched["status"] == "not_captured":
                total_not_captured += 1

        _save_control_items(
            control_run=control_run,
            replication_service=replication_service,
            rows=matched_rows,
        )
        
        

        console_rows = []
        total_rows = len(matched_rows)

        for idx, row in enumerate(matched_rows, start=1):
            item = row.get("item") or {}
            console_rows.append([
                idx,
                item.get("log_file") or "",
                int(item.get("log_pos") or 0),
                item.get("database_name") or "",
                item.get("table_name") or "",
                item.get("operation") or "",
                int(row.get("row_count") or 0),
                int(row.get("outbound_match_count") or 0),
                row.get("status") or "",
            ])

            if (
                idx == 1
                or idx == total_rows
                or (idx % CONTROL_PROGRESS_EVERY_ROWS) == 0
            ):
                _emit_control_progress(
                    control_mode="list_updates",
                    scanned_count=idx,
                    total_count=total_rows,
                    matched_count=(idx - total_ignored if idx >= total_ignored else 0),
                    ignored_count=total_ignored,
                )

        _finish_control_progress()

        preview_rows, hidden_row_count = _slice_console_preview_rows(
            console_rows,
            preview_limit=CONTROL_CONSOLE_PREVIEW_LIMIT,
        )

        header_text = _print_console_block(
            "SRDF CONTROL HEADER",
            _build_control_console_header(
                control_mode="list_updates",
                replication_service=replication_service,
                collected=collected,
            ),
        )

        headers = ["SEQ", "LOG FILE", "LOG POS", "DB", "TABLE", "OP", "ROWS", "MATCHES", "STATUS"]
        table_text = _print_console_table(headers, preview_rows)

        footer_lines = _build_control_console_footer(
            total_binlog_updates=total_rows,
            total_captured_matches=total_captured_matches,
            total_not_captured=total_not_captured,
            total_duplicates=total_duplicates,
            total_ambiguous=0,
            total_ignored=total_ignored,
        )
        footer_lines.extend(_build_preview_footer(hidden_row_count))

        footer_text = _print_console_block(
            "SRDF CONTROL SUMMARY",
            footer_lines,
        )        
        


        result_payload = {
            "resolved_settings": collected.get("resolved_settings") or {},
            "stop_reason": collected.get("stop_reason") or "",
            "header_text": header_text,
            "table_text": table_text,
            "footer_text": footer_text,
        }       

        _finalize_control_run(
            control_run=control_run,
            status="success" if total_not_captured == 0 else "warning",
            checkpoint_start_file=collected.get("checkpoint_start_file") or "",
            checkpoint_start_pos=collected.get("checkpoint_start_pos") or 0,
            current_master_before_file=collected.get("current_master_before_file") or "",
            current_master_before_pos=collected.get("current_master_before_pos") or 0,
            current_master_after_file=collected.get("current_master_after_file") or "",
            current_master_after_pos=collected.get("current_master_after_pos") or 0,
            window_end_file=collected.get("window_end_file") or "",
            window_end_pos=collected.get("window_end_pos") or 0,
            total_binlog_updates=len(matched_rows),
            total_captured_matches=total_captured_matches,
            total_not_captured=total_not_captured,
            total_duplicates=total_duplicates,
            total_ambiguous=0,
            total_ignored=total_ignored,
            result_payload=result_payload,
        )

        return {
            "ok": True,
            "control_mode": "list_updates",
            "control_run_id": control_run.id,
            "resolved_settings": collected.get("resolved_settings") or {},
            "stop_reason": collected.get("stop_reason") or "",
            "checkpoint_start_file": collected.get("checkpoint_start_file") or "",
            "checkpoint_start_pos": int(collected.get("checkpoint_start_pos") or 0),
            "current_master_before_file": collected.get("current_master_before_file") or "",
            "current_master_before_pos": int(collected.get("current_master_before_pos") or 0),
            "window_end_file": collected.get("window_end_file") or "",
            "window_end_pos": int(collected.get("window_end_pos") or 0),
            "total_binlog_updates": len(matched_rows),
            "total_captured_matches": total_captured_matches,
            "total_not_captured": total_not_captured,
            "total_duplicates": total_duplicates,
            "total_ambiguous": 0,
            "total_ignored": total_ignored,
            "header_text": header_text,
            "table_text": table_text,
            "footer_text": footer_text,            
        }

    except Exception as exc:
        if control_run:
            _finalize_control_run(
                control_run=control_run,
                status="error",
                last_error=str(exc),
                result_payload={"error": str(exc)},
            )
        raise
    finally:
        _release_control_lock(lock_obj, owner_token=owner_token)


def check_binlog_updates_vs_outbound(
    replication_service_id,
    initiated_by="srdf_control",
    command_line="",
    limit=None,
    max_scanned_events=None,
    max_runtime_seconds=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
    debug=None,
    ):
    
    replication_service = _get_replication_service(replication_service_id)

    control_run = None
    lock_obj = None
    owner_token = ""

    try:
        lock_obj, owner_token = _acquire_control_lock(
            replication_service=replication_service,
            control_mode="check_updates",
        )

        request_payload = {
            "limit": limit,
            "max_scanned_events": max_scanned_events,
            "max_runtime_seconds": max_runtime_seconds,
            "scan_window_max_files": scan_window_max_files,
            "scan_window_max_bytes": scan_window_max_bytes,
            "debug": bool(debug),
        }

        control_run = _create_control_run(
            replication_service=replication_service,
            control_mode="check_updates",
            initiated_by=initiated_by,
            command_line=command_line,
            request_payload=request_payload,
        )

        collected = _collect_binlog_control_rows(
            replication_service=replication_service,
            initiated_by=initiated_by,
            limit=limit,
            max_scanned_events=max_scanned_events,
            max_runtime_seconds=max_runtime_seconds,
            scan_window_max_files=scan_window_max_files,
            scan_window_max_bytes=scan_window_max_bytes,
            debug=debug,
        )

        rows = collected.get("rows") or []
        signature_map = _load_outbound_signature_map(replication_service)

        matched_rows = []
        total_captured_matches = 0
        total_not_captured = 0
        total_duplicates = 0
        total_ignored = 0

        for row in rows:
            if row.get("status") == "ignored":
                matched_rows.append(row)
                total_ignored += 1
                continue

            matched = _build_control_match_row(
                item=row.get("item") or {},
                signature_map=signature_map,
            )
            matched_rows.append(matched)

            if matched["status"] == "captured":
                total_captured_matches += 1
            elif matched["status"] == "duplicate":
                total_duplicates += 1
            elif matched["status"] == "not_captured":
                total_not_captured += 1

        _save_control_items(
            control_run=control_run,
            replication_service=replication_service,
            rows=matched_rows,
        )





        console_rows = []
        total_rows = len(matched_rows)

        for idx, row in enumerate(matched_rows, start=1):
            item = row.get("item") or {}
            console_rows.append([
                idx,
                item.get("log_file") or "",
                int(item.get("log_pos") or 0),
                item.get("database_name") or "",
                item.get("table_name") or "",
                item.get("operation") or "",
                int(row.get("row_count") or 0),
                int(row.get("outbound_match_count") or 0),
                row.get("status") or "",
            ])

            if (
                idx == 1
                or idx == total_rows
                or (idx % CONTROL_PROGRESS_EVERY_ROWS) == 0
            ):
                _emit_control_progress(
                    control_mode="check_updates",
                    scanned_count=idx,
                    total_count=total_rows,
                    matched_count=(idx - total_ignored if idx >= total_ignored else 0),
                    ignored_count=total_ignored,
                )

        _finish_control_progress()

        preview_rows, hidden_row_count = _slice_console_preview_rows(
            console_rows,
            preview_limit=CONTROL_CONSOLE_PREVIEW_LIMIT,
        )

        header_text = _print_console_block(
            "SRDF CONTROL HEADER",
            _build_control_console_header(
                control_mode="check_updates",
                replication_service=replication_service,
                collected=collected,
            ),
        )

        headers = ["SEQ", "LOG FILE", "LOG POS", "DB", "TABLE", "OP", "ROWS", "MATCHES", "STATUS"]
        table_text = _print_console_table(headers, preview_rows)

        footer_lines = _build_control_console_footer(
            total_binlog_updates=total_rows,
            total_captured_matches=total_captured_matches,
            total_not_captured=total_not_captured,
            total_duplicates=total_duplicates,
            total_ambiguous=0,
            total_ignored=total_ignored,
        )
        footer_lines.extend(_build_preview_footer(hidden_row_count))

        footer_text = _print_console_block(
            "SRDF CONTROL SUMMARY",
            footer_lines,
        )        
        
        
        

        result_payload = {
            "resolved_settings": collected.get("resolved_settings") or {},
            "stop_reason": collected.get("stop_reason") or "",
            "header_text": header_text,
            "table_text": table_text,
            "footer_text": footer_text,
        }       
        

        final_status = "success"
        if total_not_captured > 0 or total_duplicates > 0:
            final_status = "warning"

        _finalize_control_run(
            control_run=control_run,
            status=final_status,
            checkpoint_start_file=collected.get("checkpoint_start_file") or "",
            checkpoint_start_pos=collected.get("checkpoint_start_pos") or 0,
            current_master_before_file=collected.get("current_master_before_file") or "",
            current_master_before_pos=collected.get("current_master_before_pos") or 0,
            current_master_after_file=collected.get("current_master_after_file") or "",
            current_master_after_pos=collected.get("current_master_after_pos") or 0,
            window_end_file=collected.get("window_end_file") or "",
            window_end_pos=collected.get("window_end_pos") or 0,
            total_binlog_updates=len(matched_rows),
            total_captured_matches=total_captured_matches,
            total_not_captured=total_not_captured,
            total_duplicates=total_duplicates,
            total_ambiguous=0,
            total_ignored=total_ignored,
            result_payload=result_payload,
        )

        return {
            "ok": True,
            "control_mode": "check_updates",
            "control_run_id": control_run.id,
            "resolved_settings": collected.get("resolved_settings") or {},
            "stop_reason": collected.get("stop_reason") or "",
            "checkpoint_start_file": collected.get("checkpoint_start_file") or "",
            "checkpoint_start_pos": int(collected.get("checkpoint_start_pos") or 0),
            "current_master_before_file": collected.get("current_master_before_file") or "",
            "current_master_before_pos": int(collected.get("current_master_before_pos") or 0),
            "window_end_file": collected.get("window_end_file") or "",
            "window_end_pos": int(collected.get("window_end_pos") or 0),
            "total_binlog_updates": len(matched_rows),
            "total_captured_matches": total_captured_matches,
            "total_not_captured": total_not_captured,
            "total_duplicates": total_duplicates,
            "total_ambiguous": 0,
            "total_ignored": total_ignored,
            "header_text": header_text,
            "table_text": table_text,
            "footer_text": footer_text,
        }

    except Exception as exc:
        if control_run:
            _finalize_control_run(
                control_run=control_run,
                status="error",
                last_error=str(exc),
                result_payload={"error": str(exc)},
            )
        raise
    finally:
        _release_control_lock(lock_obj, owner_token=owner_token)
        
        

def capture_binlog_once(
    replication_service_id,
    limit=None,
    initiated_by="srdf_capture",
    initialize_only=False,
    force_current_pos=False,
    max_scanned_events=None,
    max_runtime_seconds=None,
    enable_filter_audit=None,
    disable_time_limit=None,
    debug=None,
    verify_with_mysqlbinlog=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
    bulk_insert_batch_size=None,
    wagon_max_events=None,
    progress_enabled=None,
    progress_every_events=None,
    progress_every_seconds=None,
    ):

    metadata_conn = None
    column_cache = {}

    replication_service = None
    config = None
    checkpoint = None

    captured_count = 0
    skipped_count = 0
    ignored_older_count = 0
    scanned_event_count = 0
    scanned_row_count = 0
    rotate_event_count = 0
    row_event_count = 0
    older_streak_count = 0
    stop_reason = "not_started"

    started_at_ts = time.monotonic()

    current_master_before_file = ""
    current_master_before_pos = 0
    current_master_after_file = ""
    current_master_after_pos = 0

    first_event_seen = None
    last_event_seen = None
    last_older_event_seen = None
    first_non_older_event_seen = None
    traced_events = []

    start_log_file = ""
    start_log_pos = 0
    last_consumed_log_file = ""
    last_consumed_log_pos = 0

    progress_message_count = 0
    progress_snapshots = []
    wagon_seq = 0

    resolved_settings_payload = {}

    try:
        replication_service = _get_replication_service(replication_service_id)
        config = _get_capture_config(replication_service)
        checkpoint = _get_or_create_capture_checkpoint(replication_service)
    except Exception as exc:
        return _build_capture_result(
            ok=False,
            status="error",
            return_code="CAPTURE_INIT_ERROR",
            error_code="CAPTURE_INIT_ERROR",
            error_message=str(exc),
            stop_reason="init_failed",
            replication_service_id=int(replication_service_id or 0),
            captured_count=0,
            skipped_count=0,
            ignored_older_count=0,
            scanned_event_count=0,
            scanned_row_count=0,
            rotate_event_count=0,
            row_event_count=0,
            older_streak_count=0,
            elapsed_seconds=round(time.monotonic() - started_at_ts, 3),
            log_file="",
            log_pos=0,
            start_log_file="",
            start_log_pos=0,
            current_master_before_file="",
            current_master_before_pos=0,
            current_master_after_file="",
            current_master_after_pos=0,
            first_event_seen={},
            last_event_seen={},
            last_older_event_seen={},
            first_non_older_event_seen={},
            traced_events=[],
            resolved_settings={},
        )


    requested_max_scanned_events = max_scanned_events
    requested_scan_window_max_files = scan_window_max_files
    requested_scan_window_max_bytes = scan_window_max_bytes
    resolved_engine = replication_service.service_type or ""   
    

    if limit is None:
        limit = get_srdf_setting_int(
            key="capture.limit",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=100,
            minimum=1,
        )
    else:
        limit = max(int(limit or 100), 1)

    if max_scanned_events is not None:
        max_scanned_events = max(int(max_scanned_events or 500), 1)
        
    if max_runtime_seconds is None:
        max_runtime_seconds = get_srdf_setting_int(
            key="capture.max_runtime_seconds",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=20,
            minimum=1,
        )
    else:
        max_runtime_seconds = max(int(max_runtime_seconds or 20), 1)



    if scan_window_max_files is not None:
        scan_window_max_files = max(int(scan_window_max_files or 1), 1)

    if scan_window_max_bytes is not None:
        scan_window_max_bytes = max(int(scan_window_max_bytes or (16 * 1024 * 1024)), 1)



    if bulk_insert_batch_size is None:
        bulk_insert_batch_size = get_srdf_setting_int(
            key="capture.bulk_insert_batch_size",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=250,
            minimum=1,
        )
    else:
        bulk_insert_batch_size = max(int(bulk_insert_batch_size or 250), 1)

    if wagon_max_events is None:
        wagon_max_events = get_srdf_setting_int(
            key="capture.wagon_max_events",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=200,
            minimum=1,
        )
    else:
        wagon_max_events = max(int(wagon_max_events or 200), 1)

    if progress_every_events is None:
        progress_every_events = get_srdf_setting_int(
            key="capture.progress_every_events",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=250,
            minimum=1,
        )
    else:
        progress_every_events = max(int(progress_every_events or 250), 1)

    if progress_every_seconds is None:
        progress_every_seconds = get_srdf_setting_int(
            key="capture.progress_every_seconds",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=5,
            minimum=1,
        )
    else:
        progress_every_seconds = max(int(progress_every_seconds or 5), 1)

    if enable_filter_audit is None:
        enable_filter_audit = get_srdf_setting_bool(
            key="capture.enable_filter_audit",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=False,
        )
    else:
        enable_filter_audit = bool(enable_filter_audit)

    if disable_time_limit is None:
        disable_time_limit = get_srdf_setting_bool(
            key="capture.disable_time_limit",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=False,
        )
    else:
        disable_time_limit = bool(disable_time_limit)

    if debug is None:
        debug = get_srdf_setting_bool(
            key="capture.debug",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=False,
        )
    else:
        debug = bool(debug)

    if verify_with_mysqlbinlog is None:
        verify_with_mysqlbinlog = get_srdf_setting_bool(
            key="capture.verify_with_mysqlbinlog",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=False,
        )
    else:
        verify_with_mysqlbinlog = bool(verify_with_mysqlbinlog)


    if progress_enabled is None:
        progress_enabled = get_srdf_setting_bool(
            key="capture.progress_enabled",
            replication_service=replication_service,
            engine=resolved_engine,
            fallback=False,
        )
    else:
        progress_enabled = bool(progress_enabled)
        
        
        
    resolved_settings_payload = {
        "limit": int(limit or 0),
        "max_scanned_events": int(max_scanned_events or 0),
        "max_runtime_seconds": int(max_runtime_seconds or 0),
        "enable_filter_audit": bool(enable_filter_audit),
        "disable_time_limit": bool(disable_time_limit),
        "debug": bool(debug),
        "verify_with_mysqlbinlog": bool(verify_with_mysqlbinlog),
        "scan_window_max_files": int(scan_window_max_files or 0),
        "scan_window_max_bytes": int(scan_window_max_bytes or 0),
        "bulk_insert_batch_size": int(bulk_insert_batch_size or 0),
        "wagon_max_events": int(wagon_max_events or 0),
        "progress_enabled": bool(progress_enabled),
        "progress_every_events": int(progress_every_events or 0),
        "progress_every_seconds": int(progress_every_seconds or 0),
        "requested_max_scanned_events": (
            int(requested_max_scanned_events)
            if requested_max_scanned_events is not None
            else None
        ),
        "requested_scan_window_max_files": (
            int(requested_scan_window_max_files)
            if requested_scan_window_max_files is not None
            else None
        ),
        "requested_scan_window_max_bytes": (
            int(requested_scan_window_max_bytes)
            if requested_scan_window_max_bytes is not None
            else None
        ),
        "adaptive_max_scanned_events_used": False,
        "adaptive_scan_window_max_files_used": False,
        "adaptive_scan_window_max_bytes_used": False,
        "estimated_backlog_bytes": 0,
        "estimated_backlog_file_count": 0,
    }    
    


    if force_current_pos or not checkpoint.log_file:
        current_log_file, current_log_pos = _fetch_current_master_status(replication_service)
        checkpoint.log_file = current_log_file
        checkpoint.log_pos = int(current_log_pos or 4)
        checkpoint.save(update_fields=["log_file", "log_pos", "updated_at"])

        if initialize_only:
            AuditEvent.objects.create(
                event_type="binlog_capture_initialized",
                level="warning",
                node=replication_service.source_node,
                message=f"Binlog checkpoint initialized for service '{replication_service.name}'",
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id,
                    "log_file": checkpoint.log_file or "",
                    "log_pos": checkpoint.log_pos or 0,
                    "initialize_only": True,
                    "force_current_pos": bool(force_current_pos),
                    "resolved_settings": resolved_settings_payload,
                },
            )
            
            return _build_capture_result(
                ok=True,
                status="success",
                return_code="CAPTURE_INITIALIZED_ONLY",
                error_code="",
                error_message="",
                captured_count=0,
                skipped_count=0,
                ignored_older_count=0,
                scanned_event_count=0,
                scanned_row_count=0,
                stop_reason="initialized_only",
                replication_service_id=replication_service.id,
                log_file=checkpoint.log_file or "",
                log_pos=checkpoint.log_pos or 0,
                initialized_only=True,
                resolved_settings=resolved_settings_payload,
            )
        

    started_at_ts = time.monotonic()

    captured_count      = 0
    skipped_count       = 0
    ignored_older_count = 0
    scanned_event_count = 0
    scanned_row_count   = 0
    rotate_event_count  = 0
    row_event_count     = 0
    older_streak_count  = 0
    stop_reason         = "completed"

    traced_events = []
    first_event_seen = None
    last_event_seen = None
    last_older_event_seen = None
    first_non_older_event_seen = None

    start_log_file = checkpoint.log_file or ""
    start_log_pos = int(checkpoint.log_pos or 0)

    current_master_before_file = ""
    current_master_before_pos = 0
    current_master_after_file = ""
    current_master_after_pos = 0

    last_consumed_log_file    = start_log_file
    last_consumed_log_pos     = start_log_pos
    pending_outbound_events   = []
    wagon_seq                 = 0
    current_wagon_id          = ""
    current_wagon_key         = ""
    current_wagon_event_count = 0
    progress_message_count    = 0
    last_progress_event_count = 0
    last_progress_ts          = started_at_ts
    progress_snapshots        = []    

    def _append_trace(seq, event_type, log_file, log_pos, schema, table, rows_count):
        nonlocal first_event_seen, last_event_seen

        payload = {
            "seq": seq,
            "event_type": event_type,
            "log_file": log_file or "",
            "log_pos": int(log_pos or 0),
            "schema": schema or "",
            "table": table or "",
            "rows_count": int(rows_count or 0),
        }

        if first_event_seen is None:
            first_event_seen = {
                "event_type": event_type,
                "log_file": log_file or "",
                "log_pos": int(log_pos or 0),
            }

        last_event_seen = {
            "event_type": event_type,
            "log_file": log_file or "",
            "log_pos": int(log_pos or 0),
        }

        if len(traced_events) < 50:
            traced_events.append(payload)


        _debug_log(
            debug,
            "warning",
            "[SRDF_CAPTURE][TRACE] seq=%s type=%s file=%s pos=%s schema=%s table=%s rows=%s",
            payload["seq"],
            payload["event_type"],
            payload["log_file"],
            payload["log_pos"],
            payload["schema"],
            payload["table"],
            payload["rows_count"],
        )
        


    
    
    def _flush_pending_outbound_events():
        nonlocal pending_outbound_events

        if not pending_outbound_events:
            return 0

        flushed_count = _bulk_save_outbound_events(pending_outbound_events)
        pending_outbound_events = []

        _emit_progress(force=False, reason="bulk_flush")

        return flushed_count
    
    
    def _build_wagon_key(item):
        return "|".join([
            str(item.get("engine") or ""),
            str(item.get("database_name") or ""),
            str(item.get("table_name") or ""),
            str(item.get("operation") or ""),
        ])

    def _attach_capture_wagon_metadata(item):
        nonlocal wagon_seq
        nonlocal current_wagon_id
        nonlocal current_wagon_key
        nonlocal current_wagon_event_count

        wagon_key = _build_wagon_key(item)
        max_events = max(int(wagon_max_events or 1), 1)

        if (
            not current_wagon_id
            or current_wagon_key != wagon_key
            or current_wagon_event_count >= max_events
        ):
            wagon_seq += 1
            current_wagon_id = str(uuid.uuid4())
            current_wagon_key = wagon_key
            current_wagon_event_count = 0

        current_wagon_event_count += 1

        event_payload = dict(item.get("event_payload") or {})
        event_payload["capture_wagon"] = {
            "wagon_id": current_wagon_id,
            "wagon_seq": wagon_seq,
            "wagon_key": current_wagon_key,
            "wagon_event_index": current_wagon_event_count,
            "wagon_max_events": max_events,
        }
        item["event_payload"] = event_payload

        return item    
    
    
    def _emit_progress(force=False, reason="progress"):
        nonlocal progress_message_count
        nonlocal last_progress_event_count
        nonlocal last_progress_ts

        if not progress_enabled and not force:
            return

        now_ts = time.monotonic()
        elapsed_seconds = now_ts - started_at_ts
        delta_events = scanned_event_count - last_progress_event_count
        delta_seconds = now_ts - last_progress_ts

        should_emit = force

        if not should_emit and progress_enabled:
            if progress_every_events and delta_events >= int(progress_every_events):
                should_emit = True
            elif progress_every_seconds and delta_seconds >= float(progress_every_seconds):
                should_emit = True

        if not should_emit:
            return

        rows_per_sec = 0.0
        if elapsed_seconds > 0:
            rows_per_sec = round(float(scanned_row_count) / float(elapsed_seconds), 2)

        progress_payload = {
            "reason": reason,
            "elapsed_seconds": round(elapsed_seconds, 3),
            "captured_count": int(captured_count or 0),
            "skipped_count": int(skipped_count or 0),
            "ignored_older_count": int(ignored_older_count or 0),
            "scanned_event_count": int(scanned_event_count or 0),
            "scanned_row_count": int(scanned_row_count or 0),
            "row_event_count": int(row_event_count or 0),
            "rotate_event_count": int(rotate_event_count or 0),
            "wagon_seq": int(wagon_seq or 0),
            "pending_buffer_size": len(pending_outbound_events),
            "current_log_file": last_consumed_log_file or start_log_file or "",
            "current_log_pos": int(last_consumed_log_pos or start_log_pos or 0),
            "window_end_file": window_end_file or "",
            "window_end_pos": int(window_end_pos or 0),
            "rows_per_sec": rows_per_sec,
        }

        progress_message_count += 1

        if len(progress_snapshots) < 50:
            progress_snapshots.append(progress_payload)



        logger.warning(
            "[SRDF_CAPTURE][PROGRESS] "
            "service_id=%s | reason=%s | elapsed=%ss | "
            "eligible_events=%s | raw_rows=%s | captured=%s | skipped=%s | "
            "wagons=%s | pending=%s | current=%s:%s | end=%s:%s | rps=%s",
            replication_service.id,
            progress_payload["reason"],
            progress_payload["elapsed_seconds"],
            progress_payload["scanned_event_count"],
            progress_payload["scanned_row_count"],
            progress_payload["captured_count"],
            progress_payload["skipped_count"],
            progress_payload["wagon_seq"],
            progress_payload["pending_buffer_size"],
            progress_payload["current_log_file"],
            progress_payload["current_log_pos"],
            progress_payload["window_end_file"],
            progress_payload["window_end_pos"],
            progress_payload["rows_per_sec"],
        )        
        

        last_progress_event_count = scanned_event_count
        last_progress_ts = now_ts    
    

    try:
        metadata_conn = _get_metadata_connection(config)
    
        current_master_before_file, current_master_before_pos = _fetch_current_master_status(replication_service)
    
        _debug_log(
            debug,
            "warning",
            "[SRDF_CAPTURE][START] service_id=%s start_file=%s start_pos=%s current_master_file=%s current_master_pos=%s",
            replication_service.id,
            start_log_file or "",
            start_log_pos or 0,
            current_master_before_file or "",
            current_master_before_pos or 0,
        )
        
        

        if not start_log_file:
            raise BinlogCaptureError("Missing checkpoint.log_file for capture")



        binary_log_rows = _fetch_binary_logs(replication_service)
        binary_logs = [row["log_name"] for row in binary_log_rows]
        binary_log_size_map = {
            row["log_name"]: int(row.get("file_size") or 0)
            for row in binary_log_rows
        }

        if start_log_file not in binary_logs:
            raise BinlogCaptureError(
                f"Checkpoint file not found in SHOW BINARY LOGS: {start_log_file}"
            )

        if current_master_before_file not in binary_logs:
            raise BinlogCaptureError(
                f"Current master file not found in SHOW BINARY LOGS: {current_master_before_file}"
            )



        start_idx = binary_logs.index(start_log_file)
        end_idx = binary_logs.index(current_master_before_file)
    
        backlog_file_count = max((end_idx - start_idx) + 1, 0)
        backlog_bytes = _estimate_backlog_bytes(
                binary_logs=binary_logs,
                binary_log_size_map=binary_log_size_map,
                start_idx=start_idx,
                end_idx=end_idx,
                start_log_pos=start_log_pos,
                current_master_before_pos=current_master_before_pos,
            )
    
        max_scanned_events, adaptive_max_scanned_events_used = _resolve_adaptive_max_scanned_events(
                replication_service=replication_service,
                checkpoint=checkpoint,
                resolved_engine=resolved_engine,
                explicit_value=requested_max_scanned_events,
                backlog_bytes=backlog_bytes,
                backlog_file_count=backlog_file_count,
            )
    
        scan_window_max_files, adaptive_scan_window_max_files_used = _resolve_adaptive_scan_window_max_files(
                replication_service=replication_service,
                resolved_engine=resolved_engine,
                explicit_value=requested_scan_window_max_files,
                backlog_file_count=backlog_file_count,
            )
    
        scan_window_max_bytes, adaptive_scan_window_max_bytes_used = _resolve_adaptive_scan_window_max_bytes(
                replication_service=replication_service,
                resolved_engine=resolved_engine,
                explicit_value=requested_scan_window_max_bytes,
                backlog_bytes=backlog_bytes,
            )
    
        resolved_settings_payload["max_scanned_events"] = int(max_scanned_events or 0)
        resolved_settings_payload["scan_window_max_files"] = int(scan_window_max_files or 0)
        resolved_settings_payload["scan_window_max_bytes"] = int(scan_window_max_bytes or 0)
        resolved_settings_payload["adaptive_max_scanned_events_used"] = bool(adaptive_max_scanned_events_used)
        resolved_settings_payload["adaptive_scan_window_max_files_used"] = bool(adaptive_scan_window_max_files_used)
        resolved_settings_payload["adaptive_scan_window_max_bytes_used"] = bool(adaptive_scan_window_max_bytes_used)
        resolved_settings_payload["estimated_backlog_bytes"] = int(backlog_bytes or 0)
        resolved_settings_payload["estimated_backlog_file_count"] = int(backlog_file_count or 0)        
        
        logger.warning("")
        logger.warning("======================================================================")
        logger.warning(" SRDF CAPTURE HEADER")
        logger.warning("======================================================================")
        logger.warning(" service_id                  : %s", replication_service.id)
        logger.warning(" service_name                : %s", replication_service.name)
        logger.warning(" engine                      : %s", resolved_engine)
        logger.warning(" initiated_by                : %s", initiated_by)
        logger.warning(" checkpoint_start            : %s:%s", start_log_file or "", start_log_pos or 0)
        logger.warning(" current_master_before       : %s:%s", current_master_before_file or "", current_master_before_pos or 0)
        logger.warning(" estimated_backlog_bytes     : %s", resolved_settings_payload["estimated_backlog_bytes"])
        logger.warning(" estimated_backlog_file_count: %s", resolved_settings_payload["estimated_backlog_file_count"])
        logger.warning(" limit                       : %s", resolved_settings_payload["limit"])
        logger.warning(" max_scanned_events          : %s", resolved_settings_payload["max_scanned_events"])
        logger.warning(" requested_max_scanned_events: %s", resolved_settings_payload["requested_max_scanned_events"])
        logger.warning(" adaptive_max_scanned_used   : %s", resolved_settings_payload["adaptive_max_scanned_events_used"])
        logger.warning(" scan_window_max_files       : %s", resolved_settings_payload["scan_window_max_files"])
        logger.warning(" requested_window_files      : %s", resolved_settings_payload["requested_scan_window_max_files"])
        logger.warning(" adaptive_window_files_used  : %s", resolved_settings_payload["adaptive_scan_window_max_files_used"])
        logger.warning(" scan_window_max_bytes       : %s", resolved_settings_payload["scan_window_max_bytes"])
        logger.warning(" requested_window_bytes      : %s", resolved_settings_payload["requested_scan_window_max_bytes"])
        logger.warning(" adaptive_window_bytes_used  : %s", resolved_settings_payload["adaptive_scan_window_max_bytes_used"])
        logger.warning(" max_runtime_seconds         : %s", resolved_settings_payload["max_runtime_seconds"])
        logger.warning(" bulk_insert_batch_size      : %s", resolved_settings_payload["bulk_insert_batch_size"])
        logger.warning(" wagon_max_events            : %s", resolved_settings_payload["wagon_max_events"])
        logger.warning(" progress_enabled            : %s", resolved_settings_payload["progress_enabled"])
        logger.warning(" progress_every_events       : %s", resolved_settings_payload["progress_every_events"])
        logger.warning(" progress_every_seconds      : %s", resolved_settings_payload["progress_every_seconds"])
        logger.warning(" enable_filter_audit         : %s", resolved_settings_payload["enable_filter_audit"])
        logger.warning(" disable_time_limit          : %s", resolved_settings_payload["disable_time_limit"])
        logger.warning(" debug                       : %s", resolved_settings_payload["debug"])
        logger.warning(" verify_with_mysqlbinlog     : %s", resolved_settings_payload["verify_with_mysqlbinlog"])
        logger.warning(" note                        : eligible_events exclude hard-filtered / rule-filtered rows")
        logger.warning("======================================================================")        

        if start_idx > end_idx:
            raise BinlogCaptureError(
                f"Checkpoint file is after current master file: checkpoint={start_log_file} master={current_master_before_file}"
            )

        max_files = max(int(scan_window_max_files or 1), 1)
        max_bytes = max(int(scan_window_max_bytes or 0), 0)

        window_end_idx = min(end_idx, start_idx + max_files - 1)
        window_end_file = binary_logs[window_end_idx]

        if start_idx == end_idx and window_end_file == current_master_before_file:
            if max_bytes > 0:
                window_end_pos = min(
                    int(current_master_before_pos or 0),
                    int(start_log_pos or 0) + max_bytes,
                )
            else:
                window_end_pos = int(current_master_before_pos or 0)
        elif window_end_idx < end_idx:
            window_end_pos = int(binary_log_size_map.get(window_end_file) or 0)
        else:
            window_end_pos = int(current_master_before_pos or 0)

        if window_end_idx == start_idx and window_end_pos <= int(start_log_pos or 0):
            stop_reason = "no_events_available"
            elapsed_seconds = time.monotonic() - started_at_ts

            AuditEvent.objects.create(
                event_type="binlog_capture_run",
                level="info",
                node=replication_service.source_node,
                message=f"Binlog capture completed for service '{replication_service.name}'",
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id,
                    "captured_count": 0,
                    "skipped_count": 0,
                    "ignored_older_count": 0,
                    "scanned_event_count": 0,
                    "scanned_row_count": 0,
                    "rotate_event_count": 0,
                    "row_event_count": 0,
                    "older_streak_count": 0,
                    "stop_reason": stop_reason,
                    "max_scanned_events": int(max_scanned_events or 0),
                    "max_runtime_seconds": float(max_runtime_seconds or 0),
                    "enable_filter_audit": bool(enable_filter_audit),
                    "debug": bool(debug),
                    "verify_with_mysqlbinlog": bool(verify_with_mysqlbinlog),
                    "progress_enabled": bool(progress_enabled),
                    "progress_every_events": int(progress_every_events or 0),
                    "progress_every_seconds": float(progress_every_seconds or 0),
                    "progress_message_count": int(progress_message_count or 0),
                    "progress_snapshots": progress_snapshots,
                    "scan_window_max_files": max_files,
                    "scan_window_max_bytes": max_bytes,
                    "elapsed_seconds": round(elapsed_seconds, 3),
                    "start_log_file": start_log_file or "",
                    "start_log_pos": start_log_pos or 0,
                    "window_end_file": window_end_file or "",
                    "window_end_pos": int(window_end_pos or 0),
                    "log_file": start_log_file or "",
                    "log_pos": start_log_pos or 0,
                    "current_master_before_file": current_master_before_file or "",
                    "current_master_before_pos": int(current_master_before_pos or 0),
                    "current_master_after_file": current_master_before_file or "",
                    "current_master_after_pos": int(current_master_before_pos or 0),
                    "first_event_seen": {},
                    "last_event_seen": {},
                    "last_older_event_seen": {},
                    "first_non_older_event_seen": {},
                    "traced_events": [],
                    "resolved_settings": resolved_settings_payload,
                },
            )

            return _build_capture_result(
                ok=True,
                status="success",
                return_code="CAPTURE_NO_EVENTS",
                error_code="",
                error_message="",
                captured_count=0,
                skipped_count=0,
                ignored_older_count=0,
                scanned_event_count=0,
                scanned_row_count=0,
                rotate_event_count=0,
                row_event_count=0,
                older_streak_count=0,
                stop_reason=stop_reason,
                max_scanned_events=int(max_scanned_events or 0),
                max_runtime_seconds=float(max_runtime_seconds or 0),
                enable_filter_audit=bool(enable_filter_audit),
                debug=bool(debug),
                verify_with_mysqlbinlog=bool(verify_with_mysqlbinlog),
                progress_enabled=bool(progress_enabled),
                progress_every_events=int(progress_every_events or 0),
                progress_every_seconds=float(progress_every_seconds or 0),
                progress_message_count=int(progress_message_count or 0),
                progress_snapshots=progress_snapshots,
                scan_window_max_files=max_files,
                scan_window_max_bytes=max_bytes,
                elapsed_seconds=round(elapsed_seconds, 3),
                replication_service_id=replication_service.id,
                start_log_file=start_log_file or "",
                start_log_pos=start_log_pos or 0,
                window_end_file=window_end_file or "",
                window_end_pos=int(window_end_pos or 0),
                log_file=start_log_file or "",
                log_pos=start_log_pos or 0,
                current_master_before_file=current_master_before_file or "",
                current_master_before_pos=int(current_master_before_pos or 0),
                current_master_after_file=current_master_before_file or "",
                current_master_after_pos=int(current_master_before_pos or 0),
                first_event_seen={},
                last_event_seen={},
                last_older_event_seen={},
                first_non_older_event_seen={},
                traced_events=[],
                resolved_settings=resolved_settings_payload,
            )
        
        binlog_files_to_read = binary_logs[start_idx:window_end_idx + 1]
        
        
        
        

        if _compare_binlog_position(
            start_log_file,
            start_log_pos,
            window_end_file,
            window_end_pos,
        ) >= 0:
            
            
            stop_reason = "no_events_available"
            elapsed_seconds = time.monotonic() - started_at_ts

            AuditEvent.objects.create(
                event_type="binlog_capture_run",
                level="info",
                node=replication_service.source_node,
                message=f"Binlog capture completed for service '{replication_service.name}'",
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id,
                    "captured_count": 0,
                    "skipped_count": 0,
                    "ignored_older_count": 0,
                    "scanned_event_count": 0,
                    "scanned_row_count": 0,
                    "rotate_event_count": 0,
                    "row_event_count": 0,
                    "older_streak_count": 0,
                    "stop_reason": stop_reason,
                    "max_scanned_events": int(max_scanned_events or 0),
                    "max_runtime_seconds": float(max_runtime_seconds or 0),
                    "enable_filter_audit": bool(enable_filter_audit),
                    "debug": bool(debug),
                    "verify_with_mysqlbinlog": bool(verify_with_mysqlbinlog),
                    "progress_enabled": bool(progress_enabled),
                    "progress_every_events": int(progress_every_events or 0),
                    "progress_every_seconds": float(progress_every_seconds or 0),
                    "progress_message_count": int(progress_message_count or 0),
                    "progress_snapshots": progress_snapshots,
                    "scan_window_max_files": int(scan_window_max_files or 0),
                    "scan_window_max_bytes": int(scan_window_max_bytes or 0),
                    "bulk_insert_batch_size": int(bulk_insert_batch_size or 0),
                    "wagon_max_events": int(wagon_max_events or 0),
                    "wagon_seq": int(wagon_seq or 0),
                    "window_end_file": window_end_file or "",
                    "window_end_pos": int(window_end_pos or 0),
                    "elapsed_seconds": round(elapsed_seconds, 3),
                    "start_log_file": start_log_file or "",
                    "start_log_pos": start_log_pos or 0,
                    "log_file": start_log_file or "",
                    "log_pos": start_log_pos or 0,
                    "current_master_before_file": current_master_before_file or "",
                    "current_master_before_pos": int(current_master_before_pos or 0),
                    "current_master_after_file": current_master_before_file or "",
                    "current_master_after_pos": int(current_master_before_pos or 0),
                    "first_event_seen": {},
                    "last_event_seen": {},
                    "last_older_event_seen": {},
                    "first_non_older_event_seen": {},
                    "traced_events": [],
                    "resolved_settings": resolved_settings_payload,
                },
            )

            return _build_capture_result(
                ok=True,
                status="success",
                return_code="CAPTURE_NO_EVENTS",
                error_code="",
                error_message="",
                captured_count=0,
                skipped_count=0,
                ignored_older_count=0,
                scanned_event_count=0,
                scanned_row_count=0,
                rotate_event_count=0,
                row_event_count=0,
                older_streak_count=0,
                stop_reason=stop_reason,
                max_scanned_events=int(max_scanned_events or 0),
                max_runtime_seconds=float(max_runtime_seconds or 0),
                enable_filter_audit=bool(enable_filter_audit),
                debug=bool(debug),
                verify_with_mysqlbinlog=bool(verify_with_mysqlbinlog),
                progress_enabled=bool(progress_enabled),
                progress_every_events=int(progress_every_events or 0),
                progress_every_seconds=float(progress_every_seconds or 0),
                progress_message_count=int(progress_message_count or 0),
                progress_snapshots=progress_snapshots,
                scan_window_max_files=int(scan_window_max_files or 0),
                scan_window_max_bytes=int(scan_window_max_bytes or 0),
                bulk_insert_batch_size=int(bulk_insert_batch_size or 0),
                wagon_max_events=int(wagon_max_events or 0),
                wagon_seq=int(wagon_seq or 0),
                window_end_file=window_end_file or "",
                window_end_pos=int(window_end_pos or 0),
                elapsed_seconds=round(elapsed_seconds, 3),
                replication_service_id=replication_service.id,
                start_log_file=start_log_file or "",
                start_log_pos=start_log_pos or 0,
                log_file=start_log_file or "",
                log_pos=start_log_pos or 0,
                current_master_before_file=current_master_before_file or "",
                current_master_before_pos=int(current_master_before_pos or 0),
                current_master_after_file=current_master_before_file or "",
                current_master_after_pos=int(current_master_before_pos or 0),
                first_event_seen={},
                last_event_seen={},
                last_older_event_seen={},
                first_non_older_event_seen={},
                traced_events=[],
                resolved_settings=resolved_settings_payload,
            )
        
        mysqlbinlog_path = config.get("mysqlbinlog_path") or "mysqlbinlog"


        cmd = [
            mysqlbinlog_path,
            "--read-from-remote-server",
            "--verbose",
            "--base64-output=DECODE-ROWS",
            "-h",
            config["host"],
            "-P",
            str(int(config.get("port", 3306))),
            "-u",
            config["user"],
            f"--start-position={int(start_log_pos)}",
        ]

        if binlog_files_to_read:
            cmd.extend(binlog_files_to_read[:-1])
            cmd.append(f"--stop-position={int(window_end_pos)}")
            cmd.append(binlog_files_to_read[-1]) 


        env = os.environ.copy()
        if config.get("password"):
            env["MYSQL_PWD"] = str(config.get("password") or "")

        timeout_seconds = None
        if not disable_time_limit and max_runtime_seconds:
            timeout_seconds = int(float(max_runtime_seconds)) + 5
            
        mysqlbinlog_verify = {
            "enabled": bool(verify_with_mysqlbinlog),
            "command": " ".join(cmd),
            "stdout_row_header_count": 0,
            "stdout_hash_line_count": 0,
            "srdf_traced_event_count": 0,
            "matches_traced_count": None,
        }

        result = subprocess.run(
            cmd,
            capture_output=True,
            timeout=timeout_seconds,
            env=env,
        )

        stdout_text = (result.stdout or b"").decode("utf-8", errors="replace")
        stderr_text = (result.stderr or b"").decode("utf-8", errors="replace")
        
        if verify_with_mysqlbinlog:
            mysqlbinlog_verify["stdout_row_header_count"] = sum(
                1
                for line in stdout_text.splitlines()
                if _parse_row_header(line) is not None
            )
            mysqlbinlog_verify["stdout_hash_line_count"] = sum(
                1
                for line in stdout_text.splitlines()
                if line.startswith("### ")
            )        

        if result.returncode != 0:
            raise BinlogCaptureError(
                "mysqlbinlog failed: "
                + (stderr_text.strip() or stdout_text.strip() or f"returncode={result.returncode}")
            )

        current_event_pos = start_log_pos
        current_event     = None
        current_section   = None
        current_binlog_file_index = 0
        
        current_event_file = binlog_files_to_read[0] if binlog_files_to_read else start_log_file
        last_seen_at_pos = int(start_log_pos or 0) 
        current_statement_database = ""

        def finalize_current_event():
            nonlocal current_event
            nonlocal current_section
            nonlocal captured_count
            nonlocal skipped_count
            nonlocal scanned_event_count
            nonlocal scanned_row_count
            nonlocal row_event_count
            nonlocal stop_reason
            nonlocal first_non_older_event_seen
            nonlocal last_consumed_log_file
            nonlocal last_consumed_log_pos

            if not current_event:
                return False

            event_log_pos = int(current_event.get("log_pos") or 0)
            
            event_log_file = current_event.get("log_file") or start_log_file            
            
            table_name = current_event.get("table_name") or ""
            database_name = current_event.get("database_name") or ""
            operation = current_event.get("operation") or ""



            row_event_count += 1
            scanned_row_count += int(current_event.get("rows_count") or 0)

            if _is_position_after(event_log_file, event_log_pos, last_consumed_log_file, last_consumed_log_pos):
                last_consumed_log_file = event_log_file
                last_consumed_log_pos = event_log_pos                
                
                
                

            if _is_hard_excluded_table(table_name):
                skipped_count += 1

                _debug_log(
                    debug,
                    "warning",
                    "[SRDF_CAPTURE][HARD_EXCLUDED] service_id=%s file=%s pos=%s table=%s",
                    replication_service.id,
                    event_log_file,
                    event_log_pos,
                    table_name,
                )

                current_event = None
                current_section = None
                return False            
            



            before_data = _parse_mysqlbinlog_section_lines(
                current_event.get("before_lines") or []
            )
            after_data = _parse_mysqlbinlog_section_lines(
                current_event.get("after_lines") or []
            )
            primary_key_data = _extract_pk_from_structured_rows(
                before_data,
                after_data,
            )

            item = {
                "engine": replication_service.service_type,
                "database_name": database_name,
                "table_name": table_name,
                "operation": operation,
                "log_file": event_log_file,
                "log_pos": event_log_pos,
                "primary_key_data": primary_key_data,
                "before_data": before_data,
                "after_data": after_data,
                "event_payload": {
                    "event_type": current_event.get("event_type") or "",
                    "raw_lines": current_event.get("raw_lines") or [],
                    "before_lines": current_event.get("before_lines") or [],
                    "after_lines": current_event.get("after_lines") or [],
                    "row_format": "mysqlbinlog_text",
                },
            }

            item["event_payload"]["business_dedup_signature"] = _build_business_dedup_signature(
                item
            )

            item = _resolve_unknown_columns_for_item(
                replication_service=replication_service,
                item=item,
                metadata_conn=metadata_conn,
                column_cache=column_cache,
            )

            rule_result = explain_rule_match(
                engine=item.get("engine"),
                database_name=item.get("database_name"),
                table_name=item.get("table_name"),
            )

            if not rule_result.get("allowed"):
                skipped_count += 1
                if enable_filter_audit:
                    AuditEvent.objects.create(                        
                        event_type="binlog_capture_event_filtered",
                        level="info",
                        node=replication_service.source_node,
                        message=(
                            f"Captured event filtered by SQL scope rules for "
                            f"{item.get('database_name')}.{item.get('table_name')}"
                        ),
                        created_by=initiated_by,
                        payload={
                            "replication_service_id": replication_service.id,
                            "engine": item.get("engine") or "",
                            "database_name": item.get("database_name") or "",
                            "table_name": item.get("table_name") or "",
                            "operation": item.get("operation") or "",
                            "decision": rule_result.get("decision") or "",
                            "matched_rule": rule_result.get("matched_rule") or {},
                            "evaluation_trace": rule_result.get("evaluation_trace") or [],
                        },
                    )
                current_event = None
                current_section = None
                return False            
            
            scanned_event_count += 1
            _emit_progress(force=False, reason="scan")

            if first_non_older_event_seen is None:
                first_non_older_event_seen = {
                    "event_type": current_event.get("event_type") or "",
                    "log_file": event_log_file,
                    "log_pos": event_log_pos,
                }

                _debug_log(
                    debug,
                    "warning",
                    "[SRDF_CAPTURE][FIRST_NON_OLDER] service_id=%s event_type=%s file=%s pos=%s checkpoint_file=%s checkpoint_pos=%s",
                    replication_service.id,
                    current_event.get("event_type") or "",
                    event_log_file,
                    event_log_pos,
                    start_log_file or "",
                    start_log_pos or 0,
                )

            _append_trace(
                scanned_event_count,
                current_event.get("event_type") or "",
                event_log_file,
                event_log_pos,
                database_name,
                table_name,
                current_event.get("rows_count") or 0,
            )            

            item = _attach_capture_wagon_metadata(item)
        
            pending_outbound_events.append(
                _build_outbound_event_instance(replication_service, item)
            )
            captured_count += 1
            
        
            if len(pending_outbound_events) >= int(bulk_insert_batch_size or 1):
                _flush_pending_outbound_events()
        
            current_event = None
            current_section = None
        
            if limit and captured_count >= int(limit):
                _flush_pending_outbound_events()
                stop_reason = "capture_limit_reached"
                return True
        
            if max_scanned_events and scanned_event_count >= int(max_scanned_events):
                _flush_pending_outbound_events()
                stop_reason = "max_scanned_events_reached"
                return True
        
            return False
        
        

        for raw_line in stdout_text.splitlines():
            line = raw_line.rstrip("\n")
            stripped_line = line.strip()
            use_database = _extract_mysqlbinlog_use_database(stripped_line)
            if use_database:
                current_statement_database = use_database
                continue            
            

            if line.startswith("# at "):
                try:
                    next_pos = int(line.split()[2])

                    if (
                        next_pos < last_seen_at_pos
                        and current_binlog_file_index < len(binlog_files_to_read) - 1
                    ):
                        current_binlog_file_index += 1
                        current_event_file = binlog_files_to_read[current_binlog_file_index]

                    current_event_pos = next_pos
                    last_seen_at_pos = next_pos
                except Exception:
                    pass
                continue
            
            
            ddl_items = _parse_ddl_statement(
                sql_text=_extract_mysqlbinlog_statement_sql(stripped_line),
                engine=replication_service.service_type,
                log_file=current_event_file,
                log_pos=int(current_event_pos or 0),
                current_database=current_statement_database,
            )
            
            
            if ddl_items:
                should_stop = finalize_current_event()
                if should_stop:
                    break
        
                for ddl_item in ddl_items:
                    scanned_event_count += 1
                    _emit_progress(force=False, reason="scan")
        
                    _append_trace(
                                scanned_event_count,
                                "DDLStatement",
                                current_event_file,
                                int(current_event_pos or 0),
                                ddl_item.get("database_name") or "",
                                ddl_item.get("table_name") or "",
                                1,
                            )
        
                    ddl_item = _attach_capture_wagon_metadata(ddl_item)
        
                    pending_outbound_events.append(
                                _build_outbound_event_instance(replication_service, ddl_item)
                            )
                    captured_count += 1
        
                    if len(pending_outbound_events) >= int(bulk_insert_batch_size or 1):
                        _flush_pending_outbound_events()
        
                    if limit and captured_count >= int(limit):
                        _flush_pending_outbound_events()
                        stop_reason = "capture_limit_reached"
                        should_stop = True
                        break
        
                    if max_scanned_events and scanned_event_count >= int(max_scanned_events):
                        _flush_pending_outbound_events()
                        stop_reason = "max_scanned_events_reached"
                        should_stop = True
                        break
        
                    if _is_position_after(
                                current_event_file,
                                int(current_event_pos or 0),
                                last_consumed_log_file,
                                last_consumed_log_pos,
                                ):
                        last_consumed_log_file = current_event_file
                        last_consumed_log_pos = int(current_event_pos or 0)
        
                if should_stop:
                    break
        
                continue   
            
            
            if line.startswith("### "):
                header = _parse_row_header(line)
                if header:
                    should_stop = finalize_current_event()
                    if should_stop:
                        break

                    current_event = {
                        "event_type": f"{header['operation'].title()}RowsEvent",
                        "operation": header["operation"],
                        "database_name": header["database_name"],
                        "table_name": header["table_name"],
                        "log_file": current_event_file,
                        "log_pos": int(current_event_pos or 0),
                        "before_lines": [],
                        "after_lines": [],
                        "raw_lines": [line],
                        "rows_count": 1,
                    }
                    
                    current_section = None
                    continue

                if current_event is not None:
                    current_event["raw_lines"].append(line)

                    stripped = line.strip()
                    if stripped == "### WHERE":
                        current_section = "before"
                        continue
                    if stripped == "### SET":
                        current_section = "after"
                        continue

                    if line.startswith("###   "):
                        payload_line = line[6:].strip()
                        if current_section == "before":
                            current_event["before_lines"].append(payload_line)
                        elif current_section == "after":
                            current_event["after_lines"].append(payload_line)
                        else:
                            current_event["after_lines"].append(payload_line)
                    continue


        if stop_reason == "completed":
            finalize_current_event()

        _flush_pending_outbound_events()

        current_master_after_file, current_master_after_pos = _fetch_current_master_status(replication_service)

        window_truncated = _compare_binlog_position(
                window_end_file,
                window_end_pos,
                current_master_before_file,
                current_master_before_pos,
                ) < 0
    
        if stop_reason == "completed" and window_truncated:
            stop_reason = "scan_window_limit_reached"        

        if _is_position_after(
            last_consumed_log_file,
            last_consumed_log_pos,
            checkpoint.log_file or "",
            int(checkpoint.log_pos or 0),
        ):
            _update_checkpoint(
                checkpoint=checkpoint,
                log_file=last_consumed_log_file,
                log_pos=last_consumed_log_pos,
                transaction_id="",
                checkpoint_data={
                    "last_event_type": last_event_seen.get("event_type") if last_event_seen else "",
                    "captured_count": captured_count,
                    "skipped_count": skipped_count,
                    "ignored_older_count": ignored_older_count,
                    "scanned_event_count": scanned_event_count,
                    "scanned_row_count": scanned_row_count,
                    "start_log_file": start_log_file or "",
                    "start_log_pos": start_log_pos or 0,
                },
            )

        elapsed_seconds = time.monotonic() - started_at_ts

        if stop_reason == "completed" and scanned_event_count == 0:
            stop_reason = "no_events_available"


        _debug_log(
            debug,
            "warning",
            "[SRDF_CAPTURE][END] service_id=%s stop_reason=%s captured=%s skipped=%s ignored_older=%s scanned_events=%s scanned_rows=%s first_event=%s last_event=%s first_non_older=%s current_master_before=%s:%s current_master_after=%s:%s",
            replication_service.id,
            stop_reason,
            captured_count,
            skipped_count,
            ignored_older_count,
            scanned_event_count,
            scanned_row_count,
            first_event_seen or {},
            last_event_seen or {},
            first_non_older_event_seen or {},
            current_master_before_file or "",
            current_master_before_pos or 0,
            current_master_after_file or "",
            current_master_after_pos or 0,
        )
        
        mysqlbinlog_verify["srdf_traced_event_count"] = len(traced_events)

        if verify_with_mysqlbinlog:
            mysqlbinlog_verify["matches_traced_count"] = (
                mysqlbinlog_verify["stdout_row_header_count"] == len(traced_events)
            )        
        
        _emit_progress(force=True, reason="final")

        AuditEvent.objects.create(
            event_type="binlog_capture_run",
            level="info" if skipped_count == 0 else "warning",
            node=replication_service.source_node,
            message=f"Binlog capture completed for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={
                "progress_enabled": bool(progress_enabled),
                "progress_every_events": int(progress_every_events or 0),
                "progress_every_seconds": float(progress_every_seconds or 0),
                "progress_message_count": int(progress_message_count or 0),
                "progress_snapshots": progress_snapshots,                
                "replication_service_id": replication_service.id,
                "captured_count": captured_count,
                "skipped_count": skipped_count,
                "ignored_older_count": ignored_older_count,
                "scanned_event_count": scanned_event_count,
                "scanned_row_count": scanned_row_count,
                "rotate_event_count": rotate_event_count,
                "row_event_count": row_event_count,
                "older_streak_count": older_streak_count,
                "stop_reason": stop_reason,
                "max_scanned_events": int(max_scanned_events or 0),
                "max_runtime_seconds": float(max_runtime_seconds or 0),
                "enable_filter_audit": bool(enable_filter_audit),
                "debug": bool(debug),
                "mysqlbinlog_verify": mysqlbinlog_verify,                
                "elapsed_seconds": round(elapsed_seconds, 3),
                "start_log_file": start_log_file or "",
                "start_log_pos": start_log_pos or 0,
                "log_file": last_consumed_log_file or "",
                "log_pos": last_consumed_log_pos or 0,
                "current_master_before_file": current_master_before_file or "",
                "current_master_before_pos": int(current_master_before_pos or 0),
                "current_master_after_file": current_master_after_file or "",
                "current_master_after_pos": int(current_master_after_pos or 0),
                "first_event_seen": first_event_seen or {},
                "last_event_seen": last_event_seen or {},
                "last_older_event_seen": last_older_event_seen or {},
                "first_non_older_event_seen": first_non_older_event_seen or {},
                "traced_events": traced_events,
                "scan_window_max_files": int(scan_window_max_files or 0),
                "bulk_insert_batch_size": int(bulk_insert_batch_size or 0),
                "wagon_max_events": int(wagon_max_events or 0),
                "wagon_seq": int(wagon_seq or 0),                
                "scan_window_max_bytes": int(scan_window_max_bytes or 0),
                "window_end_file": window_end_file or "",
                "window_end_pos": int(window_end_pos or 0),  
                "resolved_settings": resolved_settings_payload,
            },
        )
        
        
        logger.warning("")
        logger.warning("======================================================================")
        logger.warning(" SRDF CAPTURE SUMMARY")
        logger.warning("======================================================================")
        logger.warning(" service_id           : %s", replication_service.id)
        logger.warning(" stop_reason          : %s", stop_reason)
        logger.warning(" captured_count       : %s", captured_count)
        logger.warning(" skipped_count        : %s", skipped_count)
        logger.warning(" ignored_older_count  : %s", ignored_older_count)
        logger.warning(" eligible_events_seen : %s", scanned_event_count)
        logger.warning(" raw_rows_seen        : %s", scanned_row_count)
        logger.warning(" elapsed_seconds      : %s", round(elapsed_seconds, 3))
        logger.warning(" final_checkpoint     : %s:%s", last_consumed_log_file or "", last_consumed_log_pos or 0)
        logger.warning(" master_after         : %s:%s", current_master_after_file or "", current_master_after_pos or 0)
        logger.warning(" wagons               : %s", wagon_seq)
        logger.warning("======================================================================")
        
        
        
        capture_ok = False

        if captured_count > 0:
            capture_ok = True
        elif stop_reason in (
            "completed",
            "no_events_available",
            "capture_limit_reached",
            "max_scanned_events_reached",
            "scan_window_limit_reached",
            ):
            capture_ok = True

        final_status = "success" if capture_ok else "warning"
        final_return_code = "CAPTURE_COMPLETED"

        if stop_reason == "capture_limit_reached":
            final_return_code = "CAPTURE_LIMIT_REACHED"
        elif stop_reason == "max_scanned_events_reached":
            final_return_code = "CAPTURE_MAX_SCANNED_REACHED"
        elif stop_reason == "scan_window_limit_reached":
            final_return_code = "CAPTURE_SCAN_WINDOW_REACHED"
        elif stop_reason == "no_events_available":
            final_return_code = "CAPTURE_NO_EVENTS"

        return _build_capture_result(
            ok=capture_ok,
            status=final_status,
            return_code=final_return_code,
            error_code="",
            error_message="",
            captured_count=captured_count,
            skipped_count=skipped_count,
            ignored_older_count=ignored_older_count,
            scanned_event_count=scanned_event_count,
            scanned_row_count=scanned_row_count,
            rotate_event_count=rotate_event_count,
            row_event_count=row_event_count,
            older_streak_count=older_streak_count,
            stop_reason=stop_reason,
            max_scanned_events=int(max_scanned_events or 0),
            max_runtime_seconds=float(max_runtime_seconds or 0),
            enable_filter_audit=bool(enable_filter_audit),
            debug=bool(debug),
            mysqlbinlog_verify=mysqlbinlog_verify,
            elapsed_seconds=round(elapsed_seconds, 3),
            replication_service_id=replication_service.id,
            start_log_file=start_log_file or "",
            start_log_pos=start_log_pos or 0,
            log_file=last_consumed_log_file or "",
            log_pos=last_consumed_log_pos or 0,
            current_master_before_file=current_master_before_file or "",
            current_master_before_pos=int(current_master_before_pos or 0),
            current_master_after_file=current_master_after_file or "",
            current_master_after_pos=int(current_master_after_pos or 0),
            first_event_seen=first_event_seen or {},
            bulk_insert_batch_size=int(bulk_insert_batch_size or 0),
            wagon_max_events=int(wagon_max_events or 0),
            wagon_seq=int(wagon_seq or 0),
            last_event_seen=last_event_seen or {},
            last_older_event_seen=last_older_event_seen or {},
            first_non_older_event_seen=first_non_older_event_seen or {},
            traced_events=traced_events,
            scan_window_max_files=int(scan_window_max_files or 0),
            scan_window_max_bytes=int(scan_window_max_bytes or 0),
            window_end_file=window_end_file or "",
            window_end_pos=int(window_end_pos or 0),
            progress_enabled=bool(progress_enabled),
            progress_every_events=int(progress_every_events or 0),
            progress_every_seconds=float(progress_every_seconds or 0),
            progress_message_count=int(progress_message_count or 0),
            progress_snapshots=progress_snapshots,
            resolved_settings=resolved_settings_payload,
        )    
    
    

    except subprocess.TimeoutExpired as exc:
        elapsed_seconds = time.monotonic() - started_at_ts
        stop_reason = "timeout"

        try:
            current_master_after_file, current_master_after_pos = _fetch_current_master_status(replication_service)
        except Exception:
            current_master_after_file, current_master_after_pos = "", 0

        AuditEvent.objects.create(
            event_type="binlog_capture_error",
            level="error",
            node=replication_service.source_node,
            message=f"Binlog capture timed out for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={
                "replication_service_id": replication_service.id,
                "error": str(exc),
                "captured_count": captured_count,
                "skipped_count": skipped_count,
                "ignored_older_count": ignored_older_count,
                "scanned_event_count": scanned_event_count,
                "scanned_row_count": scanned_row_count,
                "stop_reason": stop_reason,
                "elapsed_seconds": round(elapsed_seconds, 3),
                "start_log_file": start_log_file or "",
                "start_log_pos": start_log_pos or 0,
                "current_master_before_file": current_master_before_file or "",
                "current_master_before_pos": int(current_master_before_pos or 0),
                "current_master_after_file": current_master_after_file or "",
                "current_master_after_pos": int(current_master_after_pos or 0),
                "traced_events": traced_events,
                "resolved_settings": resolved_settings_payload,
            },
        )

        return _build_capture_result(
            ok=False,
            status="timeout",
            return_code="CAPTURE_TIMEOUT",
            error_code="CAPTURE_TIMEOUT",
            error_message=str(exc),
            captured_count=captured_count,
            skipped_count=skipped_count,
            ignored_older_count=ignored_older_count,
            scanned_event_count=scanned_event_count,
            scanned_row_count=scanned_row_count,
            rotate_event_count=rotate_event_count,
            row_event_count=row_event_count,
            older_streak_count=older_streak_count,
            stop_reason=stop_reason,
            elapsed_seconds=round(elapsed_seconds, 3),
            replication_service_id=replication_service.id,
            start_log_file=start_log_file or "",
            start_log_pos=start_log_pos or 0,
            log_file=last_consumed_log_file or "",
            log_pos=last_consumed_log_pos or 0,
            current_master_before_file=current_master_before_file or "",
            current_master_before_pos=int(current_master_before_pos or 0),
            current_master_after_file=current_master_after_file or "",
            current_master_after_pos=int(current_master_after_pos or 0),
            first_event_seen=first_event_seen or {},
            last_event_seen=last_event_seen or {},
            last_older_event_seen=last_older_event_seen or {},
            first_non_older_event_seen=first_non_older_event_seen or {},
            traced_events=traced_events,
            progress_enabled=bool(progress_enabled),
            progress_every_events=int(progress_every_events or 0),
            progress_every_seconds=float(progress_every_seconds or 0),
            progress_message_count=int(progress_message_count or 0),
            progress_snapshots=progress_snapshots,
            resolved_settings=resolved_settings_payload,
        )    
    
    



    except Exception as exc:
        elapsed_seconds = time.monotonic() - started_at_ts
        stop_reason = "exception"

        try:
            current_master_after_file, current_master_after_pos = _fetch_current_master_status(replication_service)
        except Exception:
            current_master_after_file, current_master_after_pos = "", 0

        AuditEvent.objects.create(
            event_type="binlog_capture_error",
            level="error",
            node=replication_service.source_node,
            message=f"Binlog capture failed for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={
                "replication_service_id": replication_service.id,
                "error": str(exc),
                "captured_count": captured_count,
                "skipped_count": skipped_count,
                "ignored_older_count": ignored_older_count,
                "scanned_event_count": scanned_event_count,
                "scanned_row_count": scanned_row_count,
                "rotate_event_count": rotate_event_count,
                "row_event_count": row_event_count,
                "older_streak_count": older_streak_count,
                "stop_reason": stop_reason,
                "elapsed_seconds": round(elapsed_seconds, 3),
                "start_log_file": start_log_file or "",
                "start_log_pos": start_log_pos or 0,
                "log_file": last_consumed_log_file or "",
                "log_pos": last_consumed_log_pos or 0,
                "current_master_before_file": current_master_before_file or "",
                "current_master_before_pos": int(current_master_before_pos or 0),
                "current_master_after_file": current_master_after_file or "",
                "current_master_after_pos": int(current_master_after_pos or 0),
                "first_event_seen": first_event_seen or {},
                "last_event_seen": last_event_seen or {},
                "last_older_event_seen": last_older_event_seen or {},
                "first_non_older_event_seen": first_non_older_event_seen or {},
                "traced_events": traced_events,
                "resolved_settings": resolved_settings_payload,
            },
        )

        return _build_capture_result(
            ok=False,
            status="error",
            return_code="CAPTURE_EXCEPTION",
            error_code="CAPTURE_EXCEPTION",
            error_message=str(exc),
            captured_count=captured_count,
            skipped_count=skipped_count,
            ignored_older_count=ignored_older_count,
            scanned_event_count=scanned_event_count,
            scanned_row_count=scanned_row_count,
            rotate_event_count=rotate_event_count,
            row_event_count=row_event_count,
            older_streak_count=older_streak_count,
            stop_reason=stop_reason,
            elapsed_seconds=round(elapsed_seconds, 3),
            replication_service_id=replication_service.id,
            start_log_file=start_log_file or "",
            start_log_pos=start_log_pos or 0,
            log_file=last_consumed_log_file or "",
            log_pos=last_consumed_log_pos or 0,
            current_master_before_file=current_master_before_file or "",
            current_master_before_pos=int(current_master_before_pos or 0),
            current_master_after_file=current_master_after_file or "",
            current_master_after_pos=int(current_master_after_pos or 0),
            first_event_seen=first_event_seen or {},
            last_event_seen=last_event_seen or {},
            last_older_event_seen=last_older_event_seen or {},
            first_non_older_event_seen=first_non_older_event_seen or {},
            traced_events=traced_events,
            progress_enabled=bool(progress_enabled),
            progress_every_events=int(progress_every_events or 0),
            progress_every_seconds=float(progress_every_seconds or 0),
            progress_message_count=int(progress_message_count or 0),
            progress_snapshots=progress_snapshots,
            resolved_settings=resolved_settings_payload,
        )

def capture_binlog_once_new(
    replication_service_id,
    limit=100,
    initiated_by="srdf_capture",
    initialize_only=False,
    force_current_pos=False,
    max_scanned_events=500,
    max_runtime_seconds=20,
    enable_filter_audit=False,
    disable_time_limit=False,
    ):
    
    replication_service = _get_replication_service(replication_service_id)
    config = _get_capture_config(replication_service)
    checkpoint = _get_or_create_capture_checkpoint(replication_service)

    if force_current_pos or not checkpoint.log_file:
        current_log_file, current_log_pos = _fetch_current_master_status(replication_service)
        checkpoint.log_file = current_log_file
        checkpoint.log_pos = int(current_log_pos or 4)
        checkpoint.save(update_fields=["log_file", "log_pos", "updated_at"])

        if initialize_only:
            AuditEvent.objects.create(
                event_type="binlog_capture_initialized",
                level="warning",
                node=replication_service.source_node,
                message=f"Binlog checkpoint initialized for service '{replication_service.name}'",
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id,
                    "log_file": checkpoint.log_file or "",
                    "log_pos": checkpoint.log_pos or 0,
                    "initialize_only": True,
                    "force_current_pos": bool(force_current_pos),
                },
            )
            return {
                "ok": True,
                "captured_count": 0,
                "skipped_count": 0,
                "replication_service_id": replication_service.id,
                "log_file": checkpoint.log_file or "",
                "log_pos": checkpoint.log_pos or 0,
                "initialized_only": True,
            }

    stream = None
    metadata_conn = None

    captured_count = 0
    skipped_count = 0
    scanned_event_count = 0
    scanned_row_count = 0
    rotate_event_count = 0
    row_event_count = 0
    stop_reason = "completed"

    started_at_ts = time.monotonic()

    current_master_before_file = ""
    current_master_before_pos = 0
    current_master_after_file = ""
    current_master_after_pos = 0

    first_event_seen = None
    last_event_seen = None
    traced_events = []

    start_log_file = checkpoint.log_file or None
    start_log_pos = int(checkpoint.log_pos or 4)

    last_log_file = start_log_file
    last_log_pos = start_log_pos

    column_cache = {}

    try:
        metadata_conn = _get_metadata_connection(config)
        current_master_before_file, current_master_before_pos = _fetch_current_master_status(replication_service)


        _debug_log(
            debug,
            "warning",
            "[SRDF_CAPTURE][START] service_id=%s start_file=%s start_pos=%s current_master_file=%s current_master_pos=%s",
            replication_service.id,
            start_log_file or "",
            start_log_pos or 0,
            current_master_before_file or "",
            current_master_before_pos or 0,
        )
        

        stream = BinLogStreamReader(
            connection_settings={
                "host": config["host"],
                "port": int(config.get("port", 3306)),
                "user": config["user"],
                "passwd": config.get("password", ""),
            },
            server_id=int(config.get("binlog_server_id", 900001)),
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, RotateEvent],
            blocking=False,
            resume_stream=True,
            only_schemas=config.get("only_schemas") or None,
            only_tables=config.get("only_tables") or None,
            log_file=start_log_file,
            log_pos=start_log_pos,
            freeze_schema=True,
            slave_heartbeat=None,
            is_mariadb=(replication_service.service_type == "mariadb"),
        )

        for event in stream:
            elapsed_seconds = time.monotonic() - started_at_ts

            if max_runtime_seconds and elapsed_seconds >= float(max_runtime_seconds) and not disable_time_limit:
                stop_reason = "max_runtime_seconds_reached"
                break

            if isinstance(event, RotateEvent):
                rotate_event_count += 1

                next_binlog = getattr(event, "next_binlog", "") or last_log_file
                rotate_pos = int(getattr(event, "position", last_log_pos) or last_log_pos)

                if next_binlog:
                    last_log_file = next_binlog
                    last_log_pos = rotate_pos

                    _update_checkpoint(
                        checkpoint=checkpoint,
                        log_file=last_log_file,
                        log_pos=last_log_pos,
                        transaction_id="",
                        checkpoint_data={
                            "last_event_type": event.__class__.__name__,
                            "captured_count": captured_count,
                            "skipped_count": skipped_count,
                            "scanned_event_count": scanned_event_count,
                            "scanned_row_count": scanned_row_count,
                            "start_log_file": start_log_file or "",
                            "start_log_pos": start_log_pos or 0,
                        },
                    )
                continue

            event_log_file = getattr(stream, "log_file", None) or last_log_file or start_log_file or ""
            event_log_pos = int(getattr(stream, "log_pos", 0) or last_log_pos or start_log_pos or 4)

            event_seen_payload = {
                "event_type": event.__class__.__name__,
                "log_file": event_log_file,
                "log_pos": event_log_pos,
            }

            if first_event_seen is None:
                first_event_seen = dict(event_seen_payload)

            last_event_seen = dict(event_seen_payload)

            scanned_event_count += 1

            if len(traced_events) < 50:
                traced_events.append({
                    "seq": scanned_event_count,
                    "event_type": event.__class__.__name__,
                    "log_file": event_log_file,
                    "log_pos": event_log_pos,
                    "schema": getattr(event, "schema", "") or "",
                    "table": getattr(event, "table", "") or "",
                    "rows_count": len(getattr(event, "rows", []) or []),
                })

            if max_scanned_events and scanned_event_count > int(max_scanned_events):
                stop_reason = "max_scanned_events_reached"
                break

            raw_table_name = getattr(event, "table", "") or ""

            if _is_hard_excluded_table(raw_table_name):
                skipped_count += 1

                last_log_file = event_log_file
                last_log_pos = event_log_pos

                _update_checkpoint(
                    checkpoint=checkpoint,
                    log_file=last_log_file,
                    log_pos=last_log_pos,
                    transaction_id="",
                    checkpoint_data={
                        "last_event_type": event.__class__.__name__,
                        "captured_count": captured_count,
                        "skipped_count": skipped_count,
                        "scanned_event_count": scanned_event_count,
                        "scanned_row_count": scanned_row_count,
                        "start_log_file": start_log_file or "",
                        "start_log_pos": start_log_pos or 0,
                    },
                )
                continue

            normalized_events = _normalize_binlog_event(
                replication_service=replication_service,
                event=event,
                current_log_file=event_log_file,
                current_log_pos=event_log_pos,
            )

            row_event_count += 1
            scanned_row_count += len(normalized_events)

            for item in normalized_events:
                rule_result = explain_rule_match(
                    engine=item.get("engine"),
                    database_name=item.get("database_name"),
                    table_name=item.get("table_name"),
                )

                if not rule_result.get("allowed"):
                    skipped_count += 1

                    if enable_filter_audit:
                        AuditEvent.objects.create(
                            event_type="binlog_capture_event_filtered",
                            level="info",
                            node=replication_service.source_node,
                            message=(
                                f"Captured event filtered by SQL scope rules for "
                                f"{item.get('database_name')}.{item.get('table_name')}"
                            ),
                            created_by=initiated_by,
                            payload={
                                "replication_service_id": replication_service.id,
                                "engine": item.get("engine") or "",
                                "database_name": item.get("database_name") or "",
                                "table_name": item.get("table_name") or "",
                                "operation": item.get("operation") or "",
                                "decision": rule_result.get("decision") or "",
                                "matched_rule": rule_result.get("matched_rule") or {},
                                "evaluation_trace": rule_result.get("evaluation_trace") or [],
                            },
                        )
                    continue

                try:
                    resolved_item = _resolve_item_column_names(
                        item=item,
                        metadata_conn=metadata_conn,
                        column_cache=column_cache,
                    )
                    _save_outbound_event(replication_service, resolved_item)
                    captured_count += 1
                    
                except ColumnResolutionError as exc:
                    skipped_count += 1
                    AuditEvent.objects.create(
                        event_type="binlog_capture_column_resolution_error",
                        level="error",
                        node=replication_service.source_node,
                        message=f"Column resolution failed for service '{replication_service.name}'",
                        created_by=initiated_by,
                        payload={
                            "replication_service_id": replication_service.id,
                            "database_name": item.get("database_name") or "",
                            "table_name": item.get("table_name") or "",
                            "operation": item.get("operation") or "",
                            "error": str(exc),
                            "item_preview": {
                                "primary_key_data": item.get("primary_key_data") or {},
                                "before_data": item.get("before_data") or {},
                                "after_data": item.get("after_data") or {},
                            },
                        },
                    )

            last_log_file = event_log_file
            last_log_pos = event_log_pos

            _update_checkpoint(
                checkpoint=checkpoint,
                log_file=last_log_file,
                log_pos=last_log_pos,
                transaction_id="",
                checkpoint_data={
                    "last_event_type": event.__class__.__name__,
                    "captured_count": captured_count,
                    "skipped_count": skipped_count,
                    "scanned_event_count": scanned_event_count,
                    "scanned_row_count": scanned_row_count,
                    "start_log_file": start_log_file or "",
                    "start_log_pos": start_log_pos or 0,
                },
            )

            if limit and captured_count >= limit:
                stop_reason = "capture_limit_reached"
                break

        elapsed_seconds = time.monotonic() - started_at_ts
        current_master_after_file, current_master_after_pos = _fetch_current_master_status(replication_service)

        if (
            stop_reason == "completed"
            and captured_count == 0
            and skipped_count == 0
            and scanned_event_count == 0
        ):
            stop_reason = "no_events_available"

        AuditEvent.objects.create(
            event_type="binlog_capture_run",
            level="info" if skipped_count == 0 else "warning",
            node=replication_service.source_node,
            message=f"Binlog capture completed for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={
                "replication_service_id": replication_service.id,
                "captured_count": captured_count,
                "skipped_count": skipped_count,
                "ignored_older_count": 0,
                "scanned_event_count": scanned_event_count,
                "scanned_row_count": scanned_row_count,
                "rotate_event_count": rotate_event_count,
                "row_event_count": row_event_count,
                "older_streak_count": 0,
                "stop_reason": stop_reason,
                "max_scanned_events": int(max_scanned_events or 0),
                "max_runtime_seconds": float(max_runtime_seconds or 0),
                "enable_filter_audit": bool(enable_filter_audit),
                "elapsed_seconds": round(elapsed_seconds, 3),
                "start_log_file": start_log_file or "",
                "start_log_pos": start_log_pos or 0,
                "log_file": last_log_file or "",
                "log_pos": last_log_pos or 0,
                "current_master_before_file": current_master_before_file or "",
                "current_master_before_pos": int(current_master_before_pos or 0),
                "current_master_after_file": current_master_after_file or "",
                "current_master_after_pos": int(current_master_after_pos or 0),
                "first_event_seen": first_event_seen or {},
                "last_event_seen": last_event_seen or {},
                "last_older_event_seen": {},
                "first_non_older_event_seen": first_event_seen or {},
                "traced_events": traced_events,
            },
        )

        return {
            "ok": skipped_count == 0,
            "captured_count": captured_count,
            "skipped_count": skipped_count,
            "ignored_older_count": 0,
            "scanned_event_count": scanned_event_count,
            "scanned_row_count": scanned_row_count,
            "rotate_event_count": rotate_event_count,
            "row_event_count": row_event_count,
            "older_streak_count": 0,
            "stop_reason": stop_reason,
            "max_scanned_events": int(max_scanned_events or 0),
            "max_runtime_seconds": float(max_runtime_seconds or 0),
            "enable_filter_audit": bool(enable_filter_audit),
            "elapsed_seconds": round(elapsed_seconds, 3),
            "replication_service_id": replication_service.id,
            "start_log_file": start_log_file or "",
            "start_log_pos": start_log_pos or 0,
            "log_file": last_log_file or "",
            "log_pos": last_log_pos or 0,
            "current_master_before_file": current_master_before_file or "",
            "current_master_before_pos": int(current_master_before_pos or 0),
            "current_master_after_file": current_master_after_file or "",
            "current_master_after_pos": int(current_master_after_pos or 0),
            "first_event_seen": first_event_seen or {},
            "last_event_seen": last_event_seen or {},
            "last_older_event_seen": {},
            "first_non_older_event_seen": first_event_seen or {},
            "traced_events": traced_events,
        }

    except Exception as exc:
        elapsed_seconds = time.monotonic() - started_at_ts

        try:
            current_master_after_file, current_master_after_pos = _fetch_current_master_status(replication_service)
        except Exception:
            current_master_after_file, current_master_after_pos = "", 0

        AuditEvent.objects.create(
            event_type="binlog_capture_error",
            level="error",
            node=replication_service.source_node,
            message=f"Binlog capture failed for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={
                "replication_service_id": replication_service.id,
                "error": str(exc),
                "captured_count": captured_count,
                "skipped_count": skipped_count,
                "scanned_event_count": scanned_event_count,
                "scanned_row_count": scanned_row_count,
                "rotate_event_count": rotate_event_count,
                "row_event_count": row_event_count,
                "stop_reason": "exception",
                "elapsed_seconds": round(elapsed_seconds, 3),
                "start_log_file": start_log_file or "",
                "start_log_pos": start_log_pos or 0,
                "log_file": last_log_file or "",
                "log_pos": last_log_pos or 0,
                "current_master_before_file": current_master_before_file or "",
                "current_master_before_pos": int(current_master_before_pos or 0),
                "current_master_after_file": current_master_after_file or "",
                "current_master_after_pos": int(current_master_after_pos or 0),
                "first_event_seen": first_event_seen or {},
                "last_event_seen": last_event_seen or {},
            },
        )
        raise BinlogCaptureError(str(exc))

    finally:
        if stream is not None:
            stream.close()
        if metadata_conn is not None:
            metadata_conn.close()





def _get_replication_service(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .filter(service_type__in=["mariadb", "mysql"])
        .first()
    )
    if not replication_service:
        raise BinlogCaptureError(
            f"Enabled ReplicationService #{replication_service_id} (mysql/mariadb) not found"
        )
    return replication_service


def _get_capture_config(replication_service):
    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise BinlogCaptureError(
            f"ReplicationService '{replication_service.name}' has invalid config"
        )

    required = ["host", "port", "user"]
    missing = [key for key in required if not config.get(key) and config.get(key) != 0]
    if missing:
        raise BinlogCaptureError(
            f"ReplicationService '{replication_service.name}' missing config keys: {', '.join(missing)}"
        )

    if "password" not in config:
        config["password"] = ""

    return config


def _get_metadata_connection(config):
    return pymysql.connect(
        host=config["host"],
        port=int(config.get("port", 3306)),
        user=config["user"],
        password=config.get("password", ""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _fetch_current_master_status(replication_service):
    config = _get_capture_config(replication_service)

    conn = pymysql.connect(
        host=config["host"],
        port=int(config.get("port", 3306)),
        user=config["user"],
        password=config.get("password", ""),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW MASTER STATUS")
            row = cursor.fetchone()
            if not row:
                raise BinlogCaptureError("SHOW MASTER STATUS returned no row")
            return row.get("File") or "", int(row.get("Position") or 4)
    finally:
        conn.close()


def _get_or_create_capture_checkpoint(replication_service):
    engine = replication_service.service_type
    checkpoint, _ = ReplicationCheckpoint.objects.get_or_create(
        replication_service=replication_service,
        node=replication_service.source_node,
        direction="capture",
        defaults={
            "engine": engine,
            "log_file": "",
            "log_pos": 4,
            "transaction_id": "",
            "checkpoint_data": {},
        },
    )
    return checkpoint


def _normalize_binlog_event(
    replication_service,
    event,
    current_log_file="",
    current_log_pos=0,
    ):
    
    schema_name = getattr(event, "schema", "") or ""
    table_name = getattr(event, "table", "") or ""

    normalized = []
    engine = replication_service.service_type

    if isinstance(event, WriteRowsEvent):
        for row in event.rows:
            values = row.get("values") or {}
            normalized.append(
                {
                    "engine": engine,
                    "database_name": schema_name,
                    "table_name": table_name,
                    "operation": "insert",
                    "log_file": current_log_file or "",
                    "log_pos": int(current_log_pos or 0),
                    "primary_key_data": _extract_primary_key(values),
                    "before_data": {},
                    "after_data": _sanitize_dict(values),
                    "event_payload": {
                        "event_type": "WriteRowsEvent",
                        "raw_row": _sanitize_dict(row),
                    },
                }
            )

    elif isinstance(event, UpdateRowsEvent):
        for row in event.rows:
            before_values = row.get("before_values") or {}
            after_values = row.get("after_values") or {}
            normalized.append(
                {
                    "engine": engine,
                    "database_name": schema_name,
                    "table_name": table_name,
                    "operation": "update",
                    "log_file": current_log_file or "",
                    "log_pos": int(current_log_pos or 0),
                    "primary_key_data": _extract_primary_key(after_values or before_values),
                    "before_data": _sanitize_dict(before_values),
                    "after_data": _sanitize_dict(after_values),
                    "event_payload": {
                        "event_type": "UpdateRowsEvent",
                        "raw_row": _sanitize_dict(row),
                    },
                }
            )

    elif isinstance(event, DeleteRowsEvent):
        for row in event.rows:
            values = row.get("values") or {}
            normalized.append(
                {
                    "engine": engine,
                    "database_name": schema_name,
                    "table_name": table_name,
                    "operation": "delete",
                    "log_file": current_log_file or "",
                    "log_pos": int(current_log_pos or 0),
                    "primary_key_data": _extract_primary_key(values),
                    "before_data": _sanitize_dict(values),
                    "after_data": {},
                    "event_payload": {
                        "event_type": "DeleteRowsEvent",
                        "raw_row": _sanitize_dict(row),
                    },
                }
            )

    return normalized



def _resolve_item_column_names(item, metadata_conn, column_cache):
    database_name = (item.get("database_name") or "").strip()
    table_name = (item.get("table_name") or "").strip()

    if not database_name:
        raise ColumnResolutionError("Missing database_name on captured event")

    if not table_name:
        raise ColumnResolutionError("Missing table_name on captured event")

    columns = _get_table_columns(metadata_conn, column_cache, database_name, table_name)

    resolved = {
        "engine": item.get("engine") or "",
        "database_name": database_name,
        "table_name": table_name,
        "operation": item.get("operation") or "",
        "log_file": item.get("log_file") or "",
        "log_pos": int(item.get("log_pos") or 0),
        "primary_key_data": _remap_unknown_structure(item.get("primary_key_data") or {}, columns),
        "before_data": _remap_unknown_structure(item.get("before_data") or {}, columns),
        "after_data": _remap_unknown_structure(item.get("after_data") or {}, columns),
        "event_payload": _remap_unknown_structure(item.get("event_payload") or {}, columns),
    }

    _assert_no_unknown_columns(resolved["primary_key_data"])
    _assert_no_unknown_columns(resolved["before_data"])
    _assert_no_unknown_columns(resolved["after_data"])
    _assert_no_unknown_columns(resolved["event_payload"])

    return resolved


def _get_table_columns(metadata_conn, column_cache, database_name, table_name):
    cache_key = (database_name, table_name)
    if cache_key in column_cache:
        return column_cache[cache_key]

    with metadata_conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            [database_name, table_name],
        )
        rows = cursor.fetchall()

    columns = [row.get("column_name") for row in rows if row.get("column_name")]
    if not columns:
        raise ColumnResolutionError(
            f"No column metadata found for '{database_name}.{table_name}'"
        )

    column_cache[cache_key] = columns
    return columns


def _remap_unknown_structure(data, columns):
    if isinstance(data, dict):
        remapped = {}
        for key, value in data.items():
            resolved_key = key
            if isinstance(key, bytes):
                resolved_key = key.decode("utf-8", errors="replace")
            else:
                resolved_key = str(key)

            match = UNKNOWN_COL_PATTERN.match(resolved_key)
            if match:
                col_index = int(match.group(1))
                if col_index >= len(columns):
                    raise ColumnResolutionError(
                        f"Unknown column index {col_index} exceeds metadata column count {len(columns)}"
                    )
                resolved_key = columns[col_index]

            remapped[resolved_key] = _remap_unknown_structure(value, columns)
        return remapped

    if isinstance(data, list):
        return [_remap_unknown_structure(value, columns) for value in data]

    return data


def _debug_log(enabled, level, message, *args):
    if not enabled:
        return

    log_fn = getattr(logger, level, logger.warning)
    log_fn(message, *args)


def _fetch_binary_logs(replication_service):
    config = _get_capture_config(replication_service)

    conn = pymysql.connect(
        host=config["host"],
        port=int(config.get("port", 3306)),
        user=config["user"],
        password=config.get("password", ""),
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW BINARY LOGS")
            rows = cursor.fetchall() or []

            normalized = []
            for row in rows:
                log_name = row.get("Log_name")
                file_size = int(row.get("File_size") or 0)
                if log_name:
                    normalized.append({
                        "log_name": log_name,
                        "file_size": file_size,
                    })
            return normalized
    finally:
        conn.close()
        
def _assert_no_unknown_columns(data):
    if isinstance(data, dict):
        for key, value in data.items():
            key_text = str(key)
            if UNKNOWN_COL_PATTERN.match(key_text):
                raise ColumnResolutionError(f"Unresolved column name '{key_text}'")
            _assert_no_unknown_columns(value)
        return

    if isinstance(data, list):
        for value in data:
            _assert_no_unknown_columns(value)


def _extract_primary_key(values):
    if not isinstance(values, dict):
        return {}

    if "id" in values:
        return {"id": values.get("id")}

    for key, value in values.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            return {key: value}

    return {}


def _sanitize_dict(data):
    if isinstance(data, dict):
        clean = {}
        for key, value in data.items():
            if isinstance(key, bytes):
                key = key.decode("utf-8", errors="replace")
            else:
                key = str(key)
            clean[key] = _sanitize_dict(value)
        return clean

    if isinstance(data, list):
        return [_sanitize_dict(value) for value in data]

    if isinstance(data, bytes):
        return data.decode("utf-8", errors="replace")

    if isinstance(data, (str, int, float, bool)) or data is None:
        return data

    return str(data)


def _resolve_unknown_columns_for_item(replication_service, item, metadata_conn, column_cache):
    database_name = item.get("database_name") or ""
    table_name = item.get("table_name") or ""

    if not database_name or not table_name:
        return item

    columns = _get_table_columns(
        metadata_conn=metadata_conn,
        column_cache=column_cache,
        database_name=database_name,
        table_name=table_name,
    )

    resolved = {
        "engine": item.get("engine") or "",
        "event_family": item.get("event_family") or "dml",
        "database_name": database_name,
        "table_name": table_name,
        "operation": item.get("operation") or "",
        "object_type": item.get("object_type") or "",
        "ddl_action": item.get("ddl_action") or "",
        "object_name": item.get("object_name") or "",
        "parent_object_name": item.get("parent_object_name") or "",
        "raw_sql": item.get("raw_sql") or "",
        "log_file": item.get("log_file") or "",
        "log_pos": int(item.get("log_pos") or 0),
        "primary_key_data": _remap_unknown_structure(item.get("primary_key_data") or {}, columns),
        "before_data": _remap_unknown_structure(item.get("before_data") or {}, columns),
        "after_data": _remap_unknown_structure(item.get("after_data") or {}, columns),
        "event_payload": _remap_unknown_structure(item.get("event_payload") or {}, columns),
    }

    _assert_no_unknown_columns(resolved["primary_key_data"])
    _assert_no_unknown_columns(resolved["before_data"])
    _assert_no_unknown_columns(resolved["after_data"])

    return resolved



def _build_outbound_event_instance(replication_service, item):
    event_uid = str(uuid.uuid4())

    event_payload = dict(item.get("event_payload") or {})
    event_payload["control_signature"] = _build_control_signature(item)
    event_payload["business_dedup_signature"] = _build_business_dedup_signature(item)

    return OutboundChangeEvent(
        replication_service=replication_service,
        source_node=replication_service.source_node,
        engine=item["engine"],
        event_family=item.get("event_family") or "dml",
        database_name=item.get("database_name") or "",
        table_name=item.get("table_name") or "",
        operation=item.get("operation") or "",
        object_type=item.get("object_type") or "",
        ddl_action=item.get("ddl_action") or "",
        object_name=item.get("object_name") or "",
        parent_object_name=item.get("parent_object_name") or "",
        raw_sql=item.get("raw_sql") or "",
        event_uid=event_uid,
        transaction_id="",
        log_file=item.get("log_file", ""),
        log_pos=item.get("log_pos", 0),
        primary_key_data=item.get("primary_key_data") or {},
        before_data=item.get("before_data") or {},
        after_data=item.get("after_data") or {},
        event_payload=event_payload,
        status="pending",
    )



@transaction.atomic
def _bulk_save_outbound_events(instances):
    if not instances:
        return 0

    OutboundChangeEvent.objects.bulk_create(
        instances,
        batch_size=len(instances),
    )
    return len(instances)


@transaction.atomic
def _save_outbound_event(replication_service, item):
    event_uid = str(uuid.uuid4())

    event_payload = dict(item.get("event_payload") or {})
    event_payload["control_signature"] = _build_control_signature(item)
    event_payload["business_dedup_signature"] = _build_business_dedup_signature(item)

    OutboundChangeEvent.objects.create(
        replication_service=replication_service,
        source_node=replication_service.source_node,
        engine=item["engine"],
        event_family=item.get("event_family") or "dml",
        database_name=item.get("database_name") or "",
        table_name=item.get("table_name") or "",
        operation=item.get("operation") or "",
        object_type=item.get("object_type") or "",
        ddl_action=item.get("ddl_action") or "",
        object_name=item.get("object_name") or "",
        parent_object_name=item.get("parent_object_name") or "",
        raw_sql=item.get("raw_sql") or "",
        event_uid=event_uid,
        transaction_id="",
        log_file=item.get("log_file", ""),
        log_pos=item.get("log_pos", 0),
        primary_key_data=item.get("primary_key_data") or {},
        before_data=item.get("before_data") or {},
        after_data=item.get("after_data") or {},
        event_payload=event_payload,
        status="pending",
    )


def _update_checkpoint(checkpoint, log_file, log_pos, transaction_id="", checkpoint_data=None):
    checkpoint.log_file = log_file or ""
    checkpoint.log_pos = int(log_pos or 0)
    checkpoint.transaction_id = transaction_id or ""
    checkpoint.checkpoint_data = checkpoint_data or {}
    checkpoint.save(
        update_fields=[
            "log_file",
            "log_pos",
            "transaction_id",
            "checkpoint_data",
            "updated_at",
        ]
    )