#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json
import time
import uuid

from dataclasses import dataclass

from django.apps import apps
from django.conf import settings
from django.db import connections, transaction
from django.utils import timezone

from srdf.models import (
    AuditEvent,
    ReplicationService,
    SRDFArchiveExecutionLock,
    SRDFArchiveRun,
    SRDFArchiveRunItem,
    SRDFCronExecution,
    SRDFSettingDefinition,
    SRDFSettingValue,
)


ARCHIVE_DB_ALIAS = "srdf_archive"


class SRDFArchiveError(Exception):
    pass




@dataclass
class ArchiveTablePlan:
    model_label: str
    table_name: str
    date_field: str = "created_at"
    service_field: str = ""
    enabled: bool = True
    archive_mode: str = "simple"

ARCHIVE_TABLE_PLANS = [
    ArchiveTablePlan("srdf.AuditEvent", "srdf_auditevent", "created_at", "", True, "simple"),
    ArchiveTablePlan("srdf.SRDFCronExecution", "srdf_srdfcronexecution", "created_at", "replication_service", True, "simple"),
    ArchiveTablePlan("srdf.SRDFOrchestratorRun", "srdf_srdforchestratorrun", "created_at", "", True, "simple"),
    ArchiveTablePlan("srdf.SRDFServicePhaseExecution", "srdf_srdfservicephaseexecution", "created_at", "replication_service", True, "simple"),
    ArchiveTablePlan("srdf.SRDFCaptureControlRun", "srdf_srdfcapturecontrolrun", "created_at", "replication_service", True, "simple"),
    ArchiveTablePlan("srdf.SRDFCaptureControlItem", "srdf_srdfcapturecontrolitem", "created_at", "replication_service", True, "simple"),
    ArchiveTablePlan("srdf.SRDFDedupRun", "srdf_srdfdeduprun", "created_at", "replication_service", True, "simple"),
    ArchiveTablePlan("srdf.SRDFDedupDecision", "srdf_srdfdedupdecision", "created_at", "replication_service", True, "simple"),

    ArchiveTablePlan("srdf.OutboundChangeEvent", "srdf_outboundchangeevent", "created_at", "replication_service", True, "outbound_finalized"),
    ArchiveTablePlan("srdf.InboundChangeEvent", "srdf_inboundchangeevent", "received_at", "replication_service", True, "inbound_finalized"),
    ArchiveTablePlan("srdf.TransportBatch", "srdf_transportbatch", "created_at", "replication_service", True, "transport_batch_finalized"),
    ArchiveTablePlan("srdf.ReplicationCheckpoint", "srdf_replicationcheckpoint", "updated_at", "replication_service", True, "checkpoint_history"),
]


def run_archive_once(
    replication_service_id=None,
    initiated_by="srdf_archive",
    trigger_mode="manual",
    dry_run=False,
    progress_callback=None,
    ):
    
    started_perf = time.perf_counter()
    owner_token = uuid.uuid4().hex
    archive_lock = _acquire_archive_lock(owner_token=owner_token)

    if not archive_lock:
        raise SRDFArchiveError("Archive execution already locked")

    archive_run = None

    try:
        service = None

        if replication_service_id:
            service = (
                ReplicationService.objects
                .filter(id=replication_service_id, is_enabled=True)
                .first()
            )
            if not service:
                raise SRDFArchiveError(
                    f"Enabled ReplicationService #{replication_service_id} not found"
                )

        settings_payload = _load_archive_settings(service=service)
        archive_enabled = bool(settings_payload.get("archive_enabled", False))
        archive_database_name = str(settings_payload.get("archive_database_name") or "").strip()
        retention_days = int(settings_payload.get("archive_retention_days") or 30)
        batch_size = int(settings_payload.get("archive_batch_size") or 1000)
        max_tables_per_run = int(settings_payload.get("archive_max_tables_per_run") or 100)
        max_rows_per_table_per_run = int(settings_payload.get("archive_max_rows_per_table_per_run") or 5000)
        delete_after_copy = bool(settings_payload.get("archive_delete_after_copy", True))
        archive_auto_create_tables = bool(settings_payload.get("archive_auto_create_tables", False))
        archive_fail_if_table_missing = bool(settings_payload.get("archive_fail_if_table_missing", True))

        if not archive_enabled:
            raise SRDFArchiveError("Archive is disabled by settings")

        if not archive_database_name:
            raise SRDFArchiveError("Archive database name is empty in settings")

        _validate_archive_database_alias(archive_database_name)

        ensure_result = _ensure_archive_tables_ready(
            archive_db_alias=archive_database_name,
            auto_create=archive_auto_create_tables,
            fail_if_missing=archive_fail_if_table_missing,
            progress_callback=progress_callback,
        )

        archive_before_datetime = timezone.now() - timezone.timedelta(days=retention_days)

        archive_run = SRDFArchiveRun.objects.create(
            run_uid=f"archive-{timezone.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}",
            status="running",
            trigger_mode=trigger_mode,
            initiated_by=initiated_by,
            archive_database_name=archive_database_name,
            retention_days=retention_days,
            batch_size=batch_size,
            archive_before_datetime=archive_before_datetime,
            request_payload={
                "replication_service_id": service.id if service else None,
                "replication_service_name": service.name if service else "",
                "dry_run": bool(dry_run),
                "settings": settings_payload,
                "ensure_archive_tables_result": ensure_result,
            },
        )

        _emit(
            progress_callback,
            (
                f"Archive run started "
                f"service_id={service.id if service else 'ALL'} "
                f"retention_days={retention_days} "
                f"batch_size={batch_size} "
                f"archive_before={archive_before_datetime.isoformat()}"
            ),
            "info",
        )

        success_tables = 0
        failed_tables = 0
        total_rows_selected = 0
        total_rows_inserted = 0
        total_rows_deleted = 0
        item_results = []
        archived_rows_by_table = {}
        failed_tables_detail = []

        plans = [plan for plan in ARCHIVE_TABLE_PLANS if plan.enabled][:max_tables_per_run]

        for plan in plans:
            item_result = _archive_single_table(
                archive_run=archive_run,
                service=service,
                plan=plan,
                archive_before_datetime=archive_before_datetime,
                batch_size=batch_size,
                max_rows=max_rows_per_table_per_run,
                delete_after_copy=delete_after_copy,
                dry_run=dry_run,
                progress_callback=progress_callback,
            )
            item_results.append(item_result)
            

            selected_count = int(item_result.get("selected_count") or 0)
            inserted_count = int(item_result.get("inserted_count") or 0)
            deleted_count = int(item_result.get("deleted_count") or 0)

            total_rows_selected += selected_count
            total_rows_inserted += inserted_count
            total_rows_deleted += deleted_count

            archived_rows_by_table[plan.table_name] = {
                "status": item_result.get("status"),
                "selected_count": selected_count,
                "inserted_count": inserted_count,
                "deleted_count": deleted_count,
            }

            if item_result.get("status") in ("success", "skipped"):
                success_tables += 1
            else:
                failed_tables += 1
                failed_tables_detail.append({
                    "table_name": plan.table_name,
                    "error": item_result.get("error") or "",
                })
                

        duration_seconds = round(time.perf_counter() - started_perf, 3)
        archive_run.status = "success" if failed_tables == 0 else "partial"
        archive_run.total_tables = len(plans)
        archive_run.success_tables = success_tables
        archive_run.failed_tables = failed_tables
        archive_run.total_rows_selected = total_rows_selected
        archive_run.total_rows_inserted = total_rows_inserted
        archive_run.total_rows_deleted = total_rows_deleted
        archive_run.finished_at = timezone.now()
        archive_run.duration_seconds = duration_seconds
        archive_run.last_error = "" if failed_tables == 0 else f"{failed_tables} archive item(s) failed"
        
        archive_run.result_payload = {
            "replication_service_id": service.id if service else None,
            "replication_service_name": service.name if service else "",
            "dry_run": bool(dry_run),
            "settings": settings_payload,
            "ensure_archive_tables_result": ensure_result,
            "item_results": item_results,
            "archived_rows_by_table": archived_rows_by_table,
            "failed_tables_detail": failed_tables_detail,
        }
        
        archive_run.save(
            update_fields=[
                "status",
                "total_tables",
                "success_tables",
                "failed_tables",
                "total_rows_selected",
                "total_rows_inserted",
                "total_rows_deleted",
                "finished_at",
                "duration_seconds",
                "last_error",
                "result_payload",
            ]
        )

        AuditEvent.objects.create(
            event_type="srdf_archive_completed",
            level="info" if failed_tables == 0 else "warning",
            node=service.source_node if service else None,
            message="SRDF archive run completed",
            created_by=initiated_by,
            payload={
                "archive_run_id": archive_run.id,
                "replication_service_id": service.id if service else None,
                "status": archive_run.status,
                "total_tables": archive_run.total_tables,
                "success_tables": archive_run.success_tables,
                "failed_tables": archive_run.failed_tables,
                "total_rows_selected": archive_run.total_rows_selected,
                "total_rows_inserted": archive_run.total_rows_inserted,
                "total_rows_deleted": archive_run.total_rows_deleted,
                "dry_run": bool(dry_run),
            },
        )

        return {
            "ok": failed_tables == 0,
            "message": "SRDF archive completed" if failed_tables == 0 else "SRDF archive completed with partial failures",
            "archive_run_id": archive_run.id,
            "archive_run_uid": archive_run.run_uid,
            "status": archive_run.status,
            "replication_service_id": service.id if service else None,
            "replication_service_name": service.name if service else "",
            "total_tables": archive_run.total_tables,
            "success_tables": archive_run.success_tables,
            "failed_tables": archive_run.failed_tables,
            "total_rows_selected": archive_run.total_rows_selected,
            "total_rows_inserted": archive_run.total_rows_inserted,
            "total_rows_deleted": archive_run.total_rows_deleted,
            "dry_run": bool(dry_run),
            "ensure_archive_tables_result": ensure_result,
        }

    except Exception as exc:
        if archive_run:
            duration_seconds = round(time.perf_counter() - started_perf, 3)
            archive_run.status = "failed"
            archive_run.finished_at = timezone.now()
            archive_run.duration_seconds = duration_seconds
            archive_run.last_error = str(exc)
            archive_run.result_payload = {
                "error": str(exc),
            }
            archive_run.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "duration_seconds",
                    "last_error",
                    "result_payload",
                ]
            )

            AuditEvent.objects.create(
                event_type="srdf_archive_failed",
                level="error",
                node=service.source_node if service else None,
                message="SRDF archive run failed",
                created_by=initiated_by,
                payload={
                    "archive_run_id": archive_run.id,
                    "replication_service_id": service.id if service else None,
                    "error": str(exc),
                    "dry_run": bool(dry_run),
                },
            )

        raise

    finally:
        _release_archive_lock(owner_token=owner_token)        
        
        
        
        
        


def _archive_single_table(
    archive_run,
    service,
    plan,
    archive_before_datetime,
    batch_size,
    max_rows,
    delete_after_copy,
    dry_run,
    progress_callback=None,
    ):
    
    model = apps.get_model(plan.model_label)
    if model is None:
        raise SRDFArchiveError(f"Model '{plan.model_label}' not found")

    item = SRDFArchiveRunItem.objects.create(
        archive_run=archive_run,
        replication_service=service,
        table_name=plan.table_name,
        archive_model_label=plan.model_label,
        archive_before_datetime=archive_before_datetime,
        status="running",
        
        request_payload={
            "date_field": plan.date_field,
            "service_field": plan.service_field,
            "archive_mode": plan.archive_mode,
            "batch_size": int(batch_size or 0),
            "max_rows": int(max_rows or 0),
            "delete_after_copy": bool(delete_after_copy),
            "dry_run": bool(dry_run),
        },
        
    )

    started_perf = time.perf_counter()

    try:
        
        
        qs = _build_archive_queryset(
                model=model,
                plan=plan,
                service=service,
                archive_before_datetime=archive_before_datetime,
            )
    
        qs = qs.order_by("id")
        selected_ids = list(qs.values_list("id", flat=True)[:max_rows])
        
        

        selected_count = len(selected_ids)
        if selected_count == 0:
            item.status = "skipped"
            item.selected_count = 0
            item.inserted_count = 0
            item.deleted_count = 0
            item.finished_at = timezone.now()
            item.duration_seconds = round(time.perf_counter() - started_perf, 3)
            item.result_payload = {
                "message": "No eligible rows for archive",
            }
            item.save(
                update_fields=[
                    "status",
                    "selected_count",
                    "inserted_count",
                    "deleted_count",
                    "finished_at",
                    "duration_seconds",
                    "result_payload",
                ]
            )
            return {
                "table_name": plan.table_name,
                "status": "skipped",
                "selected_count": 0,
                "inserted_count": 0,
                "deleted_count": 0,
            }

        records = list(model.objects.filter(id__in=selected_ids).order_by("id").values())
        inserted_count = 0
        deleted_count = 0

        _emit(
            progress_callback,
            f"Archive table={plan.table_name} selected_count={selected_count}",
            "info",
        )

        if not dry_run:
            
            inserted_count = _insert_rows_into_archive_table(
                archive_db_alias=archive_run.archive_database_name or ARCHIVE_DB_ALIAS,
                table_name=plan.table_name,
                records=records,
                batch_size=batch_size,
            )
            

            if inserted_count != selected_count:
                raise SRDFArchiveError(
                    f"Archive insert count mismatch for table '{plan.table_name}' "
                    f"(selected={selected_count}, inserted={inserted_count})"
                )

            if delete_after_copy:
                deleted_count, _deleted_detail = model.objects.filter(id__in=selected_ids).delete()
            else:
                deleted_count = 0

        item.status = "success"
        item.selected_count = selected_count
        item.inserted_count = inserted_count if not dry_run else selected_count
        item.deleted_count = deleted_count if not dry_run else 0
        item.finished_at = timezone.now()
        item.duration_seconds = round(time.perf_counter() - started_perf, 3)
        item.result_payload = {
            "selected_ids_preview": selected_ids[:20],
            "dry_run": bool(dry_run),
        }
        item.save(
            update_fields=[
                "status",
                "selected_count",
                "inserted_count",
                "deleted_count",
                "finished_at",
                "duration_seconds",
                "result_payload",
            ]
        )

        return {
            "table_name": plan.table_name,
            "status": "success",
            "selected_count": selected_count,
            "inserted_count": inserted_count if not dry_run else selected_count,
            "deleted_count": deleted_count if not dry_run else 0,
        }

    except Exception as exc:
        item.status = "failed"
        item.finished_at = timezone.now()
        item.duration_seconds = round(time.perf_counter() - started_perf, 3)
        item.last_error = str(exc)
        item.result_payload = {
            "error": str(exc),
        }
        item.save(
            update_fields=[
                "status",
                "finished_at",
                "duration_seconds",
                "last_error",
                "result_payload",
            ]
        )

        _emit(
            progress_callback,
            f"Archive table failed table={plan.table_name} error={exc}",
            "error",
        )

        return {
            "table_name": plan.table_name,
            "status": "failed",
            "selected_count": 0,
            "inserted_count": 0,
            "deleted_count": 0,
            "error": str(exc),
        }
    
    
def _build_archive_queryset(model, plan, service, archive_before_datetime):
    qs = model.objects.all()

    if plan.date_field:
        qs = qs.filter(**{f"{plan.date_field}__lt": archive_before_datetime})

    if service and plan.service_field:
        qs = qs.filter(**{plan.service_field: service})

    archive_mode = str(plan.archive_mode or "simple").strip().lower()

    if archive_mode == "simple":
        return qs

    if archive_mode == "outbound_finalized":
        return qs.filter(status__in=["applied", "dead"])

    if archive_mode == "inbound_finalized":
        return qs.filter(status__in=["applied", "dead"])

    if archive_mode == "transport_batch_finalized":
        qs = qs.filter(status__in=["acked", "failed"])
        return qs.exclude(
            inbound_events__status__in=["received", "queued", "applying"]
        ).distinct()

    if archive_mode == "checkpoint_history":
        return _filter_checkpoint_history_queryset(
            qs=qs,
            archive_before_datetime=archive_before_datetime,
            service=service,
        )

    return qs


def _filter_checkpoint_history_queryset(qs, archive_before_datetime, service=None):
    base_qs = qs.filter(updated_at__lt=archive_before_datetime)

    if service:
        base_qs = base_qs.filter(replication_service=service)

    selected_ids = []
    grouped_rows = (
        base_qs.values(
            "replication_service_id",
            "node_id",
            "direction",
        )
        .distinct()
    )

    for row in grouped_rows:
        group_qs = base_qs.filter(
            replication_service_id=row["replication_service_id"],
            node_id=row["node_id"],
            direction=row["direction"],
            ).order_by("-updated_at", "-id")

        group_ids = list(group_qs.values_list("id", flat=True))
        if len(group_ids) <= 1:
            continue

        selected_ids.extend(group_ids[1:])

    if not selected_ids:
        return qs.none()

    return qs.filter(id__in=selected_ids)


def _insert_rows_into_archive_table(archive_db_alias, table_name, records, batch_size=1000):
    
    if not records:
        return 0

    connection = connections[archive_db_alias]
    
    inserted_count = 0

    with connection.cursor() as cursor:
        for i in range(0, len(records), max(int(batch_size or 1000), 1)):
            chunk = records[i:i + max(int(batch_size or 1000), 1)]
            columns = list(chunk[0].keys())
            placeholders = ", ".join(["%s"] * len(columns))
            quoted_columns = ", ".join([f"`{col}`" for col in columns])

            sql = (
                f"INSERT INTO `{table_name}` ({quoted_columns}) "
                f"VALUES ({placeholders})"
            )

            params = []
            for row in chunk:
                row_values = []
                for col in columns:
                    value = row.get(col)
                    if isinstance(value, (dict, list)):
                        value = json.dumps(value, ensure_ascii=False)
                    row_values.append(value)
                params.append(tuple(row_values))

            cursor.executemany(sql, params)
            inserted_count += len(chunk)

    return inserted_count


def _load_archive_settings(service=None):
    
    values = {
        "archive_enabled": False,
        "archive_interval_hours": 24,
        "archive_retention_days": 30,
        "archive_database_name": "",
        "archive_batch_size": 1000,
        "archive_pause_services_during_run": True,
        "archive_delete_after_copy": True,
        "archive_only_finalized_records": True,
        "archive_max_tables_per_run": 100,
        "archive_max_rows_per_table_per_run": 5000,
        "archive_auto_create_tables": False,
        "archive_fail_if_table_missing": True,
    }
    

    definitions = SRDFSettingDefinition.objects.filter(
        key__in=list(values.keys()),
        is_enabled=True,
    )

    for definition in definitions:
        raw_value = _resolve_setting_value(definition.key, service=service)
        if raw_value is None:
            raw_value = definition.default_value_json or definition.default_value

        if definition.value_type == "boolean":
            values[definition.key] = _to_bool(raw_value, values[definition.key])
        elif definition.value_type == "integer":
            values[definition.key] = _to_int(raw_value, values[definition.key])
        else:
            values[definition.key] = raw_value

    if not values.get("archive_database_name"):
        values["archive_database_name"] = ARCHIVE_DB_ALIAS

    return values


def _resolve_setting_value(key, service=None):
    qs = (
        SRDFSettingValue.objects
        .select_related("setting_definition")
        .filter(
            setting_definition__key=key,
            is_enabled=True,
        )
        .order_by("priority", "id")
    )

    if service:
        service_value = qs.filter(scope_type="service", replication_service=service).first()
        if service_value:
            return _extract_setting_value(service_value)

    global_value = qs.filter(scope_type="global").first()
    if global_value:
        return _extract_setting_value(global_value)

    return None


def _extract_setting_value(setting_value):
    if setting_value.value_json not in ({}, None):
        return setting_value.value_json
    return setting_value.value_text


def _to_bool(value, default_value=False):
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value or "").strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off", ""):
        return False
    return bool(default_value)


def _to_int(value, default_value=0):
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default_value)


def _acquire_archive_lock(owner_token):
    with transaction.atomic():
        lock, _created = SRDFArchiveExecutionLock.objects.select_for_update().get_or_create(
            name="srdf_archive_execution_lock",
            defaults={
                "is_locked": False,
            },
        )

        now = timezone.now()
    
        if lock.is_locked:
            if lock.expires_at and lock.expires_at > now:
                return None
    
            lock.is_locked = False
            lock.owner_token = ""
            lock.locked_at = None
            lock.expires_at = None
            lock.save(
                    update_fields=[
                        "is_locked",
                        "owner_token",
                        "locked_at",
                        "expires_at",
                        "updated_at",
                    ]
                )
        lock.is_locked = True
        lock.owner_token = owner_token
        lock.locked_at = now
        lock.expires_at = now + timezone.timedelta(minutes=30)
        lock.save(update_fields=["is_locked", "owner_token", "locked_at", "expires_at", "updated_at"])
        return lock


def _release_archive_lock(owner_token):
    lock = SRDFArchiveExecutionLock.objects.filter(owner_token=owner_token).first()
    if not lock:
        return

    lock.is_locked = False
    lock.owner_token = ""
    lock.expires_at = None
    lock.save(update_fields=["is_locked", "owner_token", "expires_at", "updated_at"])

def _validate_archive_database_alias(archive_db_alias):
    databases = getattr(settings, "DATABASES", {})
    if archive_db_alias not in databases:
        raise SRDFArchiveError(
            f"Archive database alias '{archive_db_alias}' is not configured in settings.DATABASES"
        )


def _ensure_archive_tables_ready(
    archive_db_alias,
    auto_create=False,
    fail_if_missing=True,
    progress_callback=None,
):
    missing_tables = _get_missing_archive_tables(archive_db_alias)

    if not missing_tables:
        return {
            "ok": True,
            "archive_db_alias": archive_db_alias,
            "auto_create": bool(auto_create),
            "created_tables": [],
            "missing_tables": [],
            "message": "All archive tables already exist",
        }

    if auto_create:
        created_tables = _create_missing_archive_tables(
            archive_db_alias=archive_db_alias,
            table_names=missing_tables,
            progress_callback=progress_callback,
        )

        remaining_missing = _get_missing_archive_tables(archive_db_alias)
        if remaining_missing:
            raise SRDFArchiveError(
                f"Archive tables still missing after auto-create on alias '{archive_db_alias}': "
                f"{', '.join(remaining_missing)}"
            )

        return {
            "ok": True,
            "archive_db_alias": archive_db_alias,
            "auto_create": True,
            "created_tables": created_tables,
            "missing_tables": [],
            "message": "Missing archive tables created automatically",
        }

    if fail_if_missing:
        raise SRDFArchiveError(
            f"Missing archive tables on alias '{archive_db_alias}': {', '.join(missing_tables)}"
        )

    return {
        "ok": False,
        "archive_db_alias": archive_db_alias,
        "auto_create": False,
        "created_tables": [],
        "missing_tables": missing_tables,
        "message": "Some archive tables are missing but execution is allowed to continue",
    }


def _get_missing_archive_tables(archive_db_alias):
    existing_tables = set(_list_tables_on_connection(archive_db_alias))
    expected_tables = [plan.table_name for plan in ARCHIVE_TABLE_PLANS if plan.enabled]
    return [table_name for table_name in expected_tables if table_name not in existing_tables]


def _list_tables_on_connection(db_alias):
    connection = connections[db_alias]
    vendor = connection.vendor

    with connection.cursor() as cursor:
        if vendor in ("mysql",):
            cursor.execute("SHOW TABLES")
            rows = cursor.fetchall()
            return [list(row)[0] for row in rows]

        introspection = connection.introspection
        table_info = introspection.get_table_list(cursor)
        return [item.name for item in table_info]


def _create_missing_archive_tables(archive_db_alias, table_names, progress_callback=None):
    source_connection = connections["default"]
    archive_connection = connections[archive_db_alias]
    created_tables = []

    source_vendor = source_connection.vendor
    if source_vendor != archive_connection.vendor:
        raise SRDFArchiveError(
            f"Archive DB vendor mismatch: default={source_vendor} archive={archive_connection.vendor}"
        )

    with source_connection.cursor() as source_cursor, archive_connection.cursor() as archive_cursor:
        for table_name in table_names:
            ddl_sql = _build_create_table_sql_from_source(
                source_cursor=source_cursor,
                source_vendor=source_vendor,
                table_name=table_name,
            )
            if not ddl_sql:
                raise SRDFArchiveError(
                    f"Could not extract CREATE TABLE statement for '{table_name}' from source database"
                )

            _emit(
                progress_callback,
                f"Creating missing archive table '{table_name}' on alias '{archive_db_alias}'",
                "warning",
            )

            archive_cursor.execute(ddl_sql)
            created_tables.append(table_name)

    return created_tables


def _build_create_table_sql_from_source(source_cursor, source_vendor, table_name):
    if source_vendor == "mysql":
        source_cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
        row = source_cursor.fetchone()
        if not row:
            return ""

        if isinstance(row, (list, tuple)) and len(row) >= 2:
            return row[1]

        if isinstance(row, dict):
            for key, value in row.items():
                if str(key).lower().startswith("create table"):
                    return value

    return ""


def _emit(progress_callback, message, level="info"):
    if progress_callback:
        progress_callback(message, level)