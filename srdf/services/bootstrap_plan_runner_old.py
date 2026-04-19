#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import time

from django.utils import timezone

from srdf.models import AuditEvent, BootstrapPlan
from srdf.services.table_bootstrap import bootstrap_table_once, discover_source_tables


class BootstrapPlanRunnerError(Exception):
    pass


def run_bootstrap_plan_once(
    bootstrap_plan_id,
    initiated_by="srdf_bootstrap_plan",
    dry_run_override=None,
):
    plan = (
        BootstrapPlan.objects
        .select_related("replication_service", "replication_service__target_node")
        .filter(id=bootstrap_plan_id, is_enabled=True)
        .first()
    )
    if not plan:
        raise BootstrapPlanRunnerError(f"Enabled BootstrapPlan #{bootstrap_plan_id} not found")

    started_at = timezone.now()
    started_perf = time.perf_counter()

    plan.status = "running"
    plan.last_error = ""
    plan.save(update_fields=["status", "last_error", "updated_at"])

    try:
        tables_csv = ""
        if plan.scope_mode == "tables":
            tables_csv = plan.include_tables_csv or ""

        tables = discover_source_tables(
            replication_service_id=plan.replication_service_id,
            database_name=plan.source_database_name,
            tables_csv=tables_csv,
            like_filter=plan.like_filter or "",
            exclude_csv=plan.exclude_tables_csv or "",
            auto_exclude_srdf=bool(plan.auto_exclude_srdf),
        )

        if plan.table_limit and int(plan.table_limit) > 0:
            tables = tables[:int(plan.table_limit)]

        total_tables = len(tables)
        success_count = 0
        failed_count = 0
        results = []

        for table_name in tables:
            try:
                result = bootstrap_table_once(
                    replication_service_id=plan.replication_service_id,
                    database_name=plan.source_database_name,
                    table_name=table_name,
                    recreate_table=bool(plan.recreate_table),
                    truncate_target=bool(plan.truncate_target),
                    initiated_by=initiated_by,
                    dry_run=bool(dry_run_override) if dry_run_override is not None else False,
                    validate_snapshot=bool(plan.validate_snapshot),
                )
                results.append(result)
                success_count += 1
            except Exception as exc:
                results.append(
                    {
                        "ok": False,
                        "table_name": table_name,
                        "error": str(exc),
                    }
                )
                failed_count += 1

        duration_seconds = round(time.perf_counter() - started_perf, 3)

        plan.status = "success" if failed_count == 0 else "failed"
        plan.last_run_at = timezone.now()
        plan.last_duration_seconds = duration_seconds
        plan.last_total_tables = total_tables
        plan.last_success_count = success_count
        plan.last_failed_count = failed_count
        plan.last_error = "" if failed_count == 0 else f"{failed_count} table(s) failed"
        plan.last_result_payload = {
            "bootstrap_plan_id": plan.id,
            "replication_service_id": plan.replication_service_id,
            "database_name": plan.source_database_name,
            "total_tables": total_tables,
            "success_count": success_count,
            "failed_count": failed_count,
            "results": results,
        }
        plan.save(
            update_fields=[
                "status",
                "last_run_at",
                "last_duration_seconds",
                "last_total_tables",
                "last_success_count",
                "last_failed_count",
                "last_error",
                "last_result_payload",
                "updated_at",
            ]
        )

        AuditEvent.objects.create(
            event_type="bootstrap_plan_completed",
            level="info" if failed_count == 0 else "warning",
            node=plan.replication_service.target_node,
            message=f"BootstrapPlan executed: '{plan.name}'",
            created_by=initiated_by,
            payload=plan.last_result_payload,
        )

        return plan.last_result_payload

    except Exception as exc:
        duration_seconds = round(time.perf_counter() - started_perf, 3)

        plan.status = "failed"
        plan.last_run_at = timezone.now()
        plan.last_duration_seconds = duration_seconds
        plan.last_error = str(exc)
        plan.save(
            update_fields=[
                "status",
                "last_run_at",
                "last_duration_seconds",
                "last_error",
                "updated_at",
            ]
        )

        AuditEvent.objects.create(
            event_type="bootstrap_plan_error",
            level="error",
            node=plan.replication_service.target_node,
            message=f"BootstrapPlan failed: '{plan.name}'",
            created_by=initiated_by,
            payload={
                "bootstrap_plan_id": plan.id,
                "error": str(exc),
            },
        )
        raise