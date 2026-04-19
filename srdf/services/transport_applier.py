#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import pymysql

from django.db import transaction
from django.utils import timezone

from srdf.models import AuditEvent, InboundChangeEvent, ReplicationCheckpoint, ReplicationService

from srdf.services.table_bootstrap import bootstrap_table_once
from srdf.services.locks import acquire_lock, release_lock, LockError

MAX_RETRY = 5
RETRYABLE_ERRORS = (
    pymysql.err.OperationalError,
    pymysql.err.InternalError,
)
SCHEMA_MISSING_COLUMN_ERROR = "SCHEMA_MISSING_COLUMN"

class TransportApplierError(Exception):
    pass

def _is_unknown_column_error(exc):
    error_code = exc.args[0] if getattr(exc, "args", None) else None
    message = str(exc or "").lower()

    if error_code == 1054:
        return True

    if "unknown column" in message:
        return True

    return False


def _find_pending_table_ddls(replication_service, inbound):
    if not replication_service or not inbound:
        return []

    database_name = str(getattr(inbound, "database_name", "") or "").strip()
    table_name = str(getattr(inbound, "table_name", "") or "").strip()

    if not table_name:
        return []

    return list(
        InboundChangeEvent.objects
        .filter(
            replication_service=replication_service,
            event_family="ddl",
            database_name=database_name,
            table_name=table_name,
            status__in=["received", "failed"],
        )
        .order_by("id")
    )


def _apply_pending_table_ddls_before_replay(
    conn,
    inbound,
    replication_service,
    target_db_config,
    auto_create_database=False,
    initiated_by="srdf_applier",
    ):
    
    ddl_events = _find_pending_table_ddls(replication_service, inbound)
    if not ddl_events:
        return {
            "applied_count": 0,
            "ddl_event_ids": [],
        }

    applied_ids = []

    for ddl_inbound in ddl_events:
        if int(ddl_inbound.id or 0) == int(inbound.id or 0):
            continue

        _apply_ddl(
            conn=conn,
            inbound=ddl_inbound,
            replication_service=replication_service,
            target_db_config=target_db_config,
            auto_create_database=auto_create_database,
            initiated_by=initiated_by,
        )

        ddl_inbound.status = "applied"
        ddl_inbound.applied_at = timezone.now()
        ddl_inbound.last_error = ""
        ddl_inbound.save(update_fields=["status", "applied_at", "last_error"])

        _update_applier_checkpoint(replication_service, ddl_inbound)
        applied_ids.append(int(ddl_inbound.id))

    if applied_ids:
        AuditEvent.objects.create(
            event_type="transport_apply_preceding_ddl_replay",
            level="warning",
            node=replication_service.target_node if replication_service else None,
            message=(
                f"Applied pending DDL before replay for inbound event #{inbound.id}"
            ),
            created_by=initiated_by,
            payload={
                "replication_service_id": replication_service.id if replication_service else 0,
                "inbound_event_id": inbound.id,
                "database_name": inbound.database_name or "",
                "table_name": inbound.table_name or "",
                "applied_ddl_event_ids": applied_ids,
            },
        )

    return {
        "applied_count": len(applied_ids),
        "ddl_event_ids": applied_ids,
    }


def apply_inbound_events_once(replication_service_id, limit=100, initiated_by="srdf_applier", retry_failed=False):
    
    lock_name = f"srdf_apply_{replication_service_id}"

    try:
        acquire_lock(lock_name)
    except LockError as exc:
        raise TransportApplierError(str(exc))

    replication_service = _get_replication_service(replication_service_id)
    target_db_config = _get_target_db_config(replication_service)
    auto_create_database = _is_auto_create_database_enabled(replication_service)
    auto_create_table = _is_auto_create_table_enabled(replication_service)  
    
    statuses = ["received"]
    if retry_failed:
        statuses.append("failed")

    inbound_events = list(
        InboundChangeEvent.objects
        .filter(
            replication_service=replication_service,
            status__in=statuses,
        )
        .order_by("id")[:limit]
    ) 
    
    

    if not inbound_events:
        return {
            "ok": True,
            "message": f"No inbound events to apply for service '{replication_service.name}'",
            "replication_service_id": replication_service.id,
            "processed_count": 0,
            "applied_count": 0,
            "failed_count": 0,
        }

    applied_count = 0
    failed_count = 0
    processed_ids = []
    
    

    conn = _get_connection_with_optional_database_create(
        replication_service=replication_service,
        target_db_config=target_db_config,
        auto_create_database=auto_create_database,
        initiated_by=initiated_by,
    )    
    
    

    try:
        for inbound in inbound_events:
            if inbound.status == "applied":
                continue            
            processed_ids.append(inbound.id)


            try:
                conn.begin()


                _apply_single_event(
                    conn,
                    inbound,
                    replication_service=replication_service,
                    target_db_config=target_db_config,
                    auto_create_database=auto_create_database,
                    auto_create_table=auto_create_table,
                    initiated_by=initiated_by,
                )
                

                conn.commit()
                
                
                with transaction.atomic():
                    inbound.status = "applied"
                    inbound.applied_at = timezone.now()
                    inbound.last_error = ""
                    inbound.save(update_fields=["status", "applied_at", "last_error"])

                _update_applier_checkpoint(replication_service, inbound)

                applied_count += 1                
                

            except Exception as exc:
                try:
                    conn.rollback()
                except Exception:
                    pass

                is_retryable = isinstance(exc, RETRYABLE_ERRORS)

                with transaction.atomic():
                    inbound.retry_count += 1
                    inbound.last_error = str(exc)

                    if inbound.retry_count >= MAX_RETRY:
                        inbound.status = "dead"
                    else:
                        inbound.status = "failed"

                    inbound.save(update_fields=["status", "retry_count", "last_error"])

                failed_count += 1

                AuditEvent.objects.create(
                    event_type="transport_apply_error",
                    level="error",
                    node=replication_service.target_node,
                    message=f"Apply failed for inbound event #{inbound.id}",
                    created_by=initiated_by,
                    payload={
                        "replication_service_id": replication_service.id,
                        "inbound_event_id": inbound.id,
                        "table_name": inbound.table_name,
                        "operation": inbound.operation,
                        "retry_count": inbound.retry_count,
                        "status": inbound.status,
                        "retryable": is_retryable,
                        "error": str(exc),
                    },
                )                
                
              
              
                
    finally:
        try:
            conn.close()
        finally:
            release_lock(lock_name)

    AuditEvent.objects.create(
        event_type="transport_apply_run",
        level="info" if failed_count == 0 else "warning",
        node=replication_service.target_node,
        message=f"Inbound apply completed for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "processed_count": len(inbound_events),
            "applied_count": applied_count,
            "failed_count": failed_count,
            "processed_ids": processed_ids,
            },
    )

    return {
        "ok": failed_count == 0,
        "message": f"Inbound apply completed for service '{replication_service.name}'",
        "replication_service_id": replication_service.id,
        "processed_count": len(inbound_events),
        "applied_count": applied_count,
        "failed_count": failed_count,
        "processed_ids": processed_ids,
    }



def _get_replication_service(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    if not replication_service:
        raise TransportApplierError(
            f"Enabled ReplicationService #{replication_service_id} not found"
        )
    return replication_service


def _get_target_db_config(replication_service):
    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise TransportApplierError(
            f"Invalid config for ReplicationService '{replication_service.name}'"
        )

    required = [
        "target_host",
        "target_port",
        "target_user",
        "target_database",
    ]
    missing = [key for key in required if not config.get(key) and config.get(key) != 0]
    if missing:
        raise TransportApplierError(
            f"ReplicationService '{replication_service.name}' missing target DB config keys: {', '.join(missing)}"
        )

    if "target_password" not in config:
        config["target_password"] = ""

    return {
        "host": config["target_host"],
        "port": int(config.get("target_port", 3306)),
        "user": config["target_user"],
        "password": config.get("target_password", ""),
        "database": config["target_database"],
    }


def _is_auto_create_database_enabled(replication_service):
    config = replication_service.config or {}
    if not isinstance(config, dict):
        return False
    return bool(config.get("auto_create_database", False))


def _is_auto_create_table_enabled(replication_service):
    config = replication_service.config or {}
    if not isinstance(config, dict):
        return False
    return bool(config.get("auto_create_table", False))

def _get_connection(target_db_config):
    return pymysql.connect(
        host=target_db_config["host"],
        port=int(target_db_config.get("port", 3306)),
        user=target_db_config["user"],
        password=target_db_config.get("password", ""),
        database=target_db_config["database"],
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )

def _get_connection_with_optional_database_create(
    replication_service,
    target_db_config,
    auto_create_database=False,
    initiated_by="srdf_applier",
    ):
    
    try:
        return _get_connection(target_db_config)
    except pymysql.err.OperationalError as exc:
        error_code = exc.args[0] if exc.args else None
        database_name = target_db_config.get("database") or ""

        # MySQL/MariaDB unknown database
        if error_code == 1049 and auto_create_database and database_name:
            _create_database_if_missing(
                replication_service=replication_service,
                target_db_config=target_db_config,
                database_name=database_name,
                initiated_by=initiated_by,
            )
            return _get_connection(target_db_config)

        raise
    
    
def _create_database_if_missing(replication_service, target_db_config, database_name, initiated_by):
    conn = pymysql.connect(
        host=target_db_config["host"],
        port=int(target_db_config.get("port", 3306)),
        user=target_db_config["user"],
        password=target_db_config.get("password", ""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{database_name}`")
    finally:
        conn.close()

    AuditEvent.objects.create(
        event_type="transport_target_database_created",
        level="warning",
        node=replication_service.target_node,
        message=f"Target database '{database_name}' created for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "database_name": database_name,
            "target_host": target_db_config["host"],
            "target_port": target_db_config.get("port", 3306),
        },
    )


def _apply_single_event(
    conn,
    inbound,
    replication_service=None,
    target_db_config=None,
    auto_create_database=False,
    auto_create_table=False,
    initiated_by="srdf_applier",
    ):
    
    event_family = str(getattr(inbound, "event_family", "") or "dml").strip().lower()
    table_name = inbound.table_name or ""
    operation = str(inbound.operation or "").strip().lower()

    if event_family == "ddl":
        _apply_ddl(
            conn=conn,
            inbound=inbound,
            replication_service=replication_service,
            target_db_config=target_db_config,
            auto_create_database=auto_create_database,
            initiated_by=initiated_by,
        )
        return

    if not table_name:
        raise TransportApplierError(f"Inbound event #{inbound.id} has no table_name")

    if operation == "insert":
        
        _try_apply_with_auto_create(
            conn,
            inbound,
            lambda: _apply_insert(conn, inbound),
            replication_service,
            target_db_config,
            auto_create_database,
            auto_create_table,
            initiated_by,
        )
        
        return

    if operation == "update":
        
        _try_apply_with_auto_create(
            conn,
            inbound,
            lambda: _apply_update(conn, inbound),
            replication_service,
            target_db_config,
            auto_create_database,
            auto_create_table,
            initiated_by,
        )
        
        return

    if operation == "delete":
        _try_apply_with_auto_create(
            conn,
            inbound,
            lambda: _apply_delete(conn, inbound),
            replication_service,
            target_db_config,
            auto_create_database,
            auto_create_table,
            initiated_by,
        )       
        
        return

    raise TransportApplierError(
        f"Inbound event #{inbound.id} has unsupported operation '{operation}'"
    )


def _get_admin_connection(target_db_config):
    return pymysql.connect(
        host=target_db_config["host"],
        port=int(target_db_config.get("port", 3306)),
        user=target_db_config["user"],
        password=target_db_config.get("password", ""),
        autocommit=False,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _quote_identifier(name):
    value = str(name or "").strip()
    if not value:
        return ""
    return "`" + value.replace("`", "``") + "`"


def _apply_ddl(
    conn,
    inbound,
    replication_service=None,
    target_db_config=None,
    auto_create_database=False,
    initiated_by="srdf_applier",
    ):
    
    raw_sql = str(getattr(inbound, "raw_sql", "") or "").strip()
    database_name = str(getattr(inbound, "database_name", "") or "").strip()
    object_type = str(getattr(inbound, "object_type", "") or "").strip().lower()
    ddl_action = str(getattr(inbound, "ddl_action", "") or "").strip().lower()

    if not raw_sql:
        raise TransportApplierError(f"Inbound event #{inbound.id} has empty raw_sql for DDL")

    # Case 1: database-level DDL must run without binding to a single selected DB
    if object_type == "database":
        if ddl_action == "create" and auto_create_database and database_name and replication_service and target_db_config:
            # keep existing audit path consistent
            _create_database_if_missing(
                replication_service=replication_service,
                target_db_config=target_db_config,
                database_name=database_name,
                initiated_by=initiated_by,
            )
            return

        admin_conn = _get_admin_connection(target_db_config)
        try:
            with admin_conn.cursor() as cursor:
                cursor.execute(raw_sql)
            admin_conn.commit()
        except Exception:
            try:
                admin_conn.rollback()
            except Exception:
                pass
            raise
        finally:
            admin_conn.close()
        return

    # Case 2: table/column/index DDL
    if database_name:
        if auto_create_database and replication_service and target_db_config:
            _create_database_if_missing(
                replication_service=replication_service,
                target_db_config=target_db_config,
                database_name=database_name,
                initiated_by=initiated_by,
            )

        with conn.cursor() as cursor:
            cursor.execute(f"USE {_quote_identifier(database_name)}")
            cursor.execute(raw_sql)
        return

    with conn.cursor() as cursor:
        cursor.execute(raw_sql)



def _apply_insert(conn, inbound):
    after_data = inbound.after_data or {}
    if not after_data:
        raise TransportApplierError(f"Inbound event #{inbound.id} has empty after_data for insert")

    columns = list(after_data.keys())
    values = [after_data[col] for col in columns]

    column_sql = ", ".join([f"`{col}`" for col in columns])
    placeholder_sql = ", ".join(["%s"] * len(columns))

    sql = f"INSERT INTO `{inbound.table_name}` ({column_sql}) VALUES ({placeholder_sql})"

    with conn.cursor() as cursor:
        cursor.execute(sql, values)


def _apply_update(conn, inbound):
    after_data = inbound.after_data or {}
    primary_key_data = inbound.primary_key_data or {}

    if not after_data:
        raise TransportApplierError(f"Inbound event #{inbound.id} has empty after_data for update")

    if not primary_key_data:
        raise TransportApplierError(f"Inbound event #{inbound.id} has empty primary_key_data for update")

    set_columns = [col for col in after_data.keys()]
    set_sql = ", ".join([f"`{col}` = %s" for col in set_columns])
    set_values = [after_data[col] for col in set_columns]

    where_columns = [col for col in primary_key_data.keys()]
    where_sql = " AND ".join([f"`{col}` = %s" for col in where_columns])
    where_values = [primary_key_data[col] for col in where_columns]

    sql = f"UPDATE `{inbound.table_name}` SET {set_sql} WHERE {where_sql}"

    with conn.cursor() as cursor:
        cursor.execute(sql, set_values + where_values)


def _apply_delete(conn, inbound):
    primary_key_data = inbound.primary_key_data or {}
    if not primary_key_data:
        raise TransportApplierError(f"Inbound event #{inbound.id} has empty primary_key_data for delete")

    where_columns = [col for col in primary_key_data.keys()]
    where_sql = " AND ".join([f"`{col}` = %s" for col in where_columns])
    where_values = [primary_key_data[col] for col in where_columns]

    sql = f"DELETE FROM `{inbound.table_name}` WHERE {where_sql}"

    with conn.cursor() as cursor:
        cursor.execute(sql, where_values)
      
      
        
def _try_apply_with_auto_create(
    conn,
    inbound,
    apply_callable,
    replication_service,
    target_db_config,
    auto_create_database,
    auto_create_table,
    initiated_by,
    ):

    try:
        apply_callable()
        return
    except pymysql.err.OperationalError as exc:
        error_code = exc.args[0] if exc.args else None

        # Table doesn't exist
        if error_code == 1146 and auto_create_table:
            bootstrap_result = bootstrap_table_once(
                replication_service_id=replication_service.id,
                database_name=inbound.database_name or "",
                table_name=inbound.table_name or "",
                recreate_table=False,
                truncate_target=True,
                initiated_by=initiated_by,
            )

            AuditEvent.objects.create(
                event_type="transport_apply_auto_bootstrap_replay",
                level="warning",
                node=replication_service.target_node if replication_service else None,
                message=(
                    f"Auto-bootstrap executed before replay for inbound event #{inbound.id}"
                    ),
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id if replication_service else 0,
                    "inbound_event_id": inbound.id,
                    "database_name": inbound.database_name or "",
                    "table_name": inbound.table_name or "",
                    "operation": inbound.operation or "",
                    "bootstrap_result": bootstrap_result or {},
                    },
            )

            apply_callable()
            return

        if _is_unknown_column_error(exc):
            ddl_replay = _apply_pending_table_ddls_before_replay(
                conn=conn,
                inbound=inbound,
                replication_service=replication_service,
                target_db_config=target_db_config,
                auto_create_database=auto_create_database,
                initiated_by=initiated_by,
            )

            if int(ddl_replay.get("applied_count") or 0) > 0:
                apply_callable()
                return

            detailed_message = (
                f"{SCHEMA_MISSING_COLUMN_ERROR}: inbound event #{inbound.id} "
                f"cannot be applied because target schema is missing one or more columns. "
                f"No pending inbound DDL was found for table '{inbound.table_name or ''}'. "
                f"Original error: {str(exc)}"
            )

            AuditEvent.objects.create(
                event_type="transport_apply_schema_missing_column",
                level="error",
                node=replication_service.target_node if replication_service else None,
                message=(
                    f"Schema missing column before apply for inbound event #{inbound.id}"
                    ),
                created_by=initiated_by,
                payload={
                    "replication_service_id": replication_service.id if replication_service else 0,
                    "inbound_event_id": inbound.id,
                    "database_name": inbound.database_name or "",
                    "table_name": inbound.table_name or "",
                    "operation": inbound.operation or "",
                    "error_code": error_code,
                    "error": str(exc),
                    "schema_error_code": SCHEMA_MISSING_COLUMN_ERROR,
                    },
            )

            raise TransportApplierError(detailed_message)

        raise    
    
def _update_applier_checkpoint(replication_service, inbound):
    ReplicationCheckpoint.objects.update_or_create(
        replication_service=replication_service,
        node=replication_service.target_node,
        direction="applier",
        defaults={
            "engine": replication_service.service_type,
            "log_file": inbound.source_log_file or "",
            "log_pos": int(inbound.source_log_pos or 0),
            "transaction_id": inbound.transaction_id or "",
            "checkpoint_data": {
                "inbound_event_id": inbound.id,
                "table_name": inbound.table_name or "",
                "operation": inbound.operation or "",
                "status": inbound.status,
            },
        },
    )
    
