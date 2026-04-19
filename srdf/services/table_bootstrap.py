#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import hashlib
import json

import pymysql

from srdf.models import AuditEvent, ReplicationService


class TableBootstrapError(Exception):
    pass


def bootstrap_table_once(
    replication_service_id,
    database_name,
    table_name,
    recreate_table=False,
    truncate_target=True,
    initiated_by="srdf_bootstrap",
    dry_run=False,
    validate_snapshot=False,
    ):
    
    replication_service = _get_replication_service(replication_service_id)

    database_name = (database_name or "").strip()
    table_name = (table_name or "").strip()

    if not database_name:
        raise TableBootstrapError("database_name is required")

    if not table_name:
        raise TableBootstrapError("table_name is required")

    source_db_config = _get_source_db_config(replication_service, database_name)
    target_db_config = _get_target_db_config(replication_service)

    source_conn = _get_source_connection(source_db_config)
    target_server_conn = _get_target_server_connection(target_db_config)

    copied_rows = 0
    source_row_count = 0
    target_row_count = 0
    source_checksum = ""
    target_checksum = ""
    table_created = False
    table_truncated = False

    try:
        source_row_count = _count_rows(source_conn, table_name)
        if validate_snapshot:
            source_checksum = _compute_table_checksum(source_conn, table_name)

        if dry_run:
            AuditEvent.objects.create(
                event_type="transport_table_bootstrap_dry_run",
                level="warning",
                node=replication_service.target_node,
                message=(
                    f"Dry-run bootstrap for table '{database_name}.{table_name}' "
                    f"on service '{replication_service.name}'"
                ),
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id,
                    "database_name": database_name,
                    "table_name": table_name,
                    "recreate_table": bool(recreate_table),
                    "truncate_target": bool(truncate_target),
                    "source_row_count": source_row_count,
                    "source_checksum": source_checksum,
                    "dry_run": True,
                },
            )

            return {
                "ok": True,
                "message": f"Dry-run bootstrap completed for '{database_name}.{table_name}'",
                "replication_service_id": replication_service.id,
                "database_name": database_name,
                "table_name": table_name,
                "recreate_table": bool(recreate_table),
                "truncate_target": bool(truncate_target),
                "copied_rows": 0,
                "source_row_count": source_row_count,
                "target_row_count": 0,
                "source_checksum": source_checksum,
                "target_checksum": "",
                "table_created": False,
                "table_truncated": False,
                "dry_run": True,
                "validate_snapshot": bool(validate_snapshot),
            }

        _create_target_database_if_missing(target_server_conn, target_db_config["database"])

        target_conn = _get_target_db_connection(target_db_config)
        try:
            create_sql = _fetch_source_show_create_table(source_conn, table_name)

            if recreate_table:
                _drop_target_table_if_exists(target_conn, table_name)
                _create_target_table_from_show_create(target_conn, create_sql)
                table_created = True
            else:
                if not _target_table_exists(target_conn, table_name):
                    _create_target_table_from_show_create(target_conn, create_sql)
                    table_created = True
                elif truncate_target:
                    _truncate_target_table(target_conn, table_name)
                    table_truncated = True

            copied_rows = _copy_full_table_data(
                source_conn=source_conn,
                target_conn=target_conn,
                table_name=table_name,
            )

            target_row_count = _count_rows(target_conn, table_name)
            if validate_snapshot:
                target_checksum = _compute_table_checksum(target_conn, table_name)

        finally:
            target_conn.close()

    finally:
        source_conn.close()
        target_server_conn.close()

    snapshot_ok = True
    if validate_snapshot:
        snapshot_ok = (
            int(source_row_count or 0) == int(target_row_count or 0)
            and (source_checksum or "") == (target_checksum or "")
        )

    AuditEvent.objects.create(
        event_type="transport_table_bootstrap_completed",
        level="info" if snapshot_ok else "warning",
        node=replication_service.target_node,
        message=(
            f"Bootstrap completed for table '{database_name}.{table_name}' "
            f"on service '{replication_service.name}'"
        ),
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "database_name": database_name,
            "table_name": table_name,
            "recreate_table": bool(recreate_table),
            "truncate_target": bool(truncate_target),
            "copied_rows": copied_rows,
            "source_row_count": source_row_count,
            "target_row_count": target_row_count,
            "source_checksum": source_checksum,
            "target_checksum": target_checksum,
            "snapshot_ok": snapshot_ok,
            "table_created": table_created,
            "table_truncated": table_truncated,
            "dry_run": False,
            "validate_snapshot": bool(validate_snapshot),
        },
    )

    return {
        "ok": snapshot_ok,
        "message": f"Bootstrap completed for '{database_name}.{table_name}'",
        "replication_service_id": replication_service.id,
        "database_name": database_name,
        "table_name": table_name,
        "recreate_table": bool(recreate_table),
        "truncate_target": bool(truncate_target),
        "copied_rows": copied_rows,
        "source_row_count": source_row_count,
        "target_row_count": target_row_count,
        "source_checksum": source_checksum,
        "target_checksum": target_checksum,
        "snapshot_ok": snapshot_ok,
        "table_created": table_created,
        "table_truncated": table_truncated,
        "dry_run": False,
        "validate_snapshot": bool(validate_snapshot),
    }



def _get_replication_service(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    if not replication_service:
        raise TableBootstrapError(
            f"Enabled ReplicationService #{replication_service_id} not found"
        )
    return replication_service


def _get_source_db_config(replication_service, database_name):
    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise TableBootstrapError(
            f"Invalid config for ReplicationService '{replication_service.name}'"
        )

    required = ["host", "port", "user"]
    missing = [key for key in required if not config.get(key) and config.get(key) != 0]
    if missing:
        raise TableBootstrapError(
            f"ReplicationService '{replication_service.name}' missing source DB config keys: {', '.join(missing)}"
        )

    return {
        "host": config["host"],
        "port": int(config.get("port", 3306)),
        "user": config["user"],
        "password": config.get("password", ""),
        "database": database_name,
    }


def _get_target_db_config(replication_service):
    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise TableBootstrapError(
            f"Invalid config for ReplicationService '{replication_service.name}'"
        )

    required = ["target_host", "target_port", "target_user", "target_database"]
    missing = [key for key in required if not config.get(key) and config.get(key) != 0]
    if missing:
        raise TableBootstrapError(
            f"ReplicationService '{replication_service.name}' missing target DB config keys: {', '.join(missing)}"
        )

    return {
        "host": config["target_host"],
        "port": int(config.get("target_port", 3306)),
        "user": config["target_user"],
        "password": config.get("target_password", ""),
        "database": config["target_database"],
    }


def _get_source_connection(source_db_config):
    return pymysql.connect(
        host=source_db_config["host"],
        port=source_db_config["port"],
        user=source_db_config["user"],
        password=source_db_config["password"],
        database=source_db_config["database"],
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _get_target_server_connection(target_db_config):
    return pymysql.connect(
        host=target_db_config["host"],
        port=target_db_config["port"],
        user=target_db_config["user"],
        password=target_db_config["password"],
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _get_target_db_connection(target_db_config):
    return pymysql.connect(
        host=target_db_config["host"],
        port=target_db_config["port"],
        user=target_db_config["user"],
        password=target_db_config["password"],
        database=target_db_config["database"],
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _create_target_database_if_missing(target_server_conn, database_name):
    with target_server_conn.cursor() as cursor:
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")


def _fetch_source_show_create_table(source_conn, table_name):
    with source_conn.cursor() as cursor:
        cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
        row = cursor.fetchone()

    if not row:
        raise TableBootstrapError(f"SHOW CREATE TABLE returned no row for '{table_name}'")

    for key, value in row.items():
        if str(key).lower().startswith("create table"):
            return value

    raise TableBootstrapError(f"SHOW CREATE TABLE payload invalid for '{table_name}'")


def _target_table_exists(target_conn, table_name):
    with target_conn.cursor() as cursor:
        cursor.execute("SHOW TABLES LIKE %s", [table_name])
        row = cursor.fetchone()
        return bool(row)


def _drop_target_table_if_exists(target_conn, table_name):
    with target_conn.cursor() as cursor:
        cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")


def _create_target_table_from_show_create(target_conn, create_sql):
    with target_conn.cursor() as cursor:
        cursor.execute(create_sql)


def _truncate_target_table(target_conn, table_name):
    with target_conn.cursor() as cursor:
        cursor.execute(f"TRUNCATE TABLE `{table_name}`")


def _copy_full_table_data(source_conn, target_conn, table_name, batch_size=500):
    copied_rows = 0

    with source_conn.cursor() as source_cursor:
        source_cursor.execute(f"SELECT * FROM `{table_name}`")

        columns = [desc[0] for desc in source_cursor.description]
        if not columns:
            return 0

        column_sql = ", ".join([f"`{col}`" for col in columns])
        placeholder_sql = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO `{table_name}` ({column_sql}) VALUES ({placeholder_sql})"

        while True:
            rows = source_cursor.fetchmany(batch_size)
            if not rows:
                break

            payload = []
            for row in rows:
                payload.append([row.get(col) for col in columns])

            with target_conn.cursor() as target_cursor:
                target_cursor.executemany(insert_sql, payload)

            copied_rows += len(payload)

    return copied_rows


def discover_source_tables(
    replication_service_id,
    database_name,
    tables_csv="",
    like_filter="",
    exclude_csv="",
    auto_exclude_srdf=True,
):
    replication_service = _get_replication_service(replication_service_id)
    source_db_config = _get_source_db_config(replication_service, database_name)

    conn = _get_source_connection(source_db_config)
    try:
        tables = []

        if tables_csv:
            tables = [t.strip() for t in tables_csv.split(",") if t.strip()]
        else:
            with conn.cursor() as cursor:
                if like_filter:
                    cursor.execute("SHOW TABLES LIKE %s", [like_filter])
                else:
                    cursor.execute("SHOW TABLES")

                rows = cursor.fetchall()
                for row in rows:
                    tables.append(list(row.values())[0])

        excluded = set()
        if exclude_csv:
            excluded.update([t.strip() for t in exclude_csv.split(",") if t.strip()])

        if auto_exclude_srdf:
            for table_name in tables:
                if table_name.startswith("srdf_"):
                    excluded.add(table_name)

            excluded.update(
                {
                    "django_migrations",
                    "django_admin_log",
                }
            )

        tables = [t for t in tables if t not in excluded]
        return tables

    finally:
        conn.close()


def _count_rows(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) AS cnt FROM `{table_name}`")
        row = cursor.fetchone() or {}
        return int(row.get("cnt") or 0)


def _compute_table_checksum(conn, table_name, sample_limit=5000):
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT * FROM `{table_name}` LIMIT %s", [sample_limit])
        rows = cursor.fetchall() or []

    raw = json.dumps(rows, sort_keys=True, ensure_ascii=False, default=str, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()