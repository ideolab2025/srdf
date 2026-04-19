#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import os
import shlex
import subprocess
import time

import pymysql

from concurrent.futures import ThreadPoolExecutor, as_completed

from django.utils import timezone

from srdf.models import (
    AuditEvent,
    BootstrapPlan,
    BootstrapPlanExecution,
    BootstrapPlanExecutionItem,
    BootstrapPlanTableSelection,
)

from srdf.services.table_bootstrap import bootstrap_table_once, discover_source_tables


class BootstrapPlanRunnerError(Exception):
    pass

def _resolve_last_bootstrap_execution_for_plan(plan, failed_only=False):
    if not plan:
        return None

    qs = (
        BootstrapPlanExecution.objects
        .select_related("bootstrap_plan", "replication_service")
        .filter(bootstrap_plan=plan)
        .order_by("-id")
    )

    if failed_only:
        qs = qs.filter(status="failed")

    return qs.first()


def _resolve_replay_source_execution(
    bootstrap_plan_id=None,
    bootstrap_plan_name=None,
    replay_last=False,
    replay_last_failed=False,
    ):
    plan = _resolve_bootstrap_plan(
        bootstrap_plan_id=bootstrap_plan_id,
        bootstrap_plan_name=bootstrap_plan_name,
    )
    if not plan:
        lookup = (
            f"id={bootstrap_plan_id}"
            if bootstrap_plan_id
            else f"name='{bootstrap_plan_name}'"
        )
        raise BootstrapPlanRunnerError(f"Enabled BootstrapPlan not found ({lookup})")

    if replay_last_failed:
        source_execution = _resolve_last_bootstrap_execution_for_plan(
            plan=plan,
            failed_only=True,
        )
        if not source_execution:
            raise BootstrapPlanRunnerError(
                f"No failed BootstrapPlanExecution found for plan '{plan.name}'"
            )
        return plan, source_execution

    if replay_last:
        source_execution = _resolve_last_bootstrap_execution_for_plan(
            plan=plan,
            failed_only=False,
        )
        if not source_execution:
            raise BootstrapPlanRunnerError(
                f"No BootstrapPlanExecution found for plan '{plan.name}'"
            )
        return plan, source_execution

    return plan, None


def _resolve_bootstrap_execution(execution_id):
    if not execution_id:
        return None

    return (
        BootstrapPlanExecution.objects
        .select_related("bootstrap_plan", "replication_service")
        .filter(id=execution_id)
        .first()
    )



def _resolve_bootstrap_plan(bootstrap_plan_id=None, bootstrap_plan_name=None):
    qs = (
        BootstrapPlan.objects
        .select_related("replication_service", "replication_service__target_node")
        .filter(is_enabled=True)
    )

    if bootstrap_plan_id:
        return qs.filter(id=bootstrap_plan_id).first()

    if bootstrap_plan_name:
        return qs.filter(name=bootstrap_plan_name).first()

    return None


def run_bootstrap_plan_once(
    bootstrap_plan_id=None,
    bootstrap_plan_name=None,
    bootstrap_execution_id=None,
    replay_last=False,
    replay_last_failed=False,
    retry_failed_only=False,
    initiated_by="srdf_bootstrap_plan",
    dry_run_override=None,
    progress_callback=None,
    ):
    
    
    source_execution = None

    if bootstrap_execution_id:
        source_execution = _resolve_bootstrap_execution(bootstrap_execution_id)
        if not source_execution:
            raise BootstrapPlanRunnerError(
                f"BootstrapPlanExecution #{bootstrap_execution_id} not found"
            )
        plan = source_execution.bootstrap_plan
        if not plan or not plan.is_enabled:
            raise BootstrapPlanRunnerError(
                f"BootstrapPlan linked to execution #{bootstrap_execution_id} is missing or disabled"
            )
    else:
        plan, source_execution = _resolve_replay_source_execution(
            bootstrap_plan_id=bootstrap_plan_id,
            bootstrap_plan_name=bootstrap_plan_name,
            replay_last=bool(replay_last),
            replay_last_failed=bool(replay_last_failed),
        )



    started_perf = time.perf_counter()

    plan.status = "running"
    plan.last_error = ""
    plan.save(update_fields=["status", "last_error", "updated_at"])
    
    
    execution = BootstrapPlanExecution.objects.create(
        bootstrap_plan=plan,
        parent_execution=source_execution,
        replication_service=plan.replication_service,
        initiated_by=initiated_by,
        trigger_mode="cli" if str(initiated_by).startswith("cli_") else "manual",
        replay_mode="execution_replay" if source_execution else "",
        retry_failed_only=bool(retry_failed_only),
        status="running",
        dry_run=bool(dry_run_override) if dry_run_override is not None else False,
    )  

    try:
        
        if source_execution:
            work_items = _build_replay_work_items(
                source_execution=source_execution,
                retry_failed_only=bool(retry_failed_only),
            )
        else:
            work_items = _build_work_items(plan)
            
            
            
        total_databases = len(sorted(set([x["database_name"] for x in work_items])))
        total_tables = len(work_items)

        if total_tables == 0:
            if source_execution:
                raise BootstrapPlanRunnerError(
                    f"No work item found for replay on plan '{plan.name}'"
                )
            raise BootstrapPlanRunnerError(
                f"No work item found for plan '{plan.name}'"
            )

        success_count = 0       
        
        
        failed_count = 0
        results = []
        

        if progress_callback:
            if source_execution:
                progress_callback(
                    f"[PLAN REPLAY START] plan='{plan.name}' "
                    f"mode={'last_failed' if replay_last_failed else 'last'} "
                    f"retry_failed_only={'yes' if bool(retry_failed_only) else 'no'} "
                    f"method={plan.bootstrap_method} "
                    f"databases={total_databases} tables={total_tables} "
                    f"dry_run={'yes' if bool(dry_run_override) else 'no'}"
                )
            else:
                progress_callback(
                    f"[PLAN START] plan='{plan.name}' "
                    f"method={plan.bootstrap_method} "
                    f"databases={total_databases} tables={total_tables} "
                    f"dry_run={'yes' if bool(dry_run_override) else 'no'}"
                )            

        db_groups = {}
        for item in work_items:
            db_groups.setdefault(item["database_name"], []).append(item)

        database_parallel_workers = max(1, int(plan.database_parallel_workers or 1))

        def _run_database_group(database_name, items):
            if progress_callback:
                progress_callback(
                    f"[DATABASE START] db={database_name} tables={len(items)}"
                )
                

            group_results = _run_database_items(
                plan=plan,
                execution=execution,
                database_name=database_name,
                items=items,
                initiated_by=initiated_by,
                dry_run_override=dry_run_override,
                progress_callback=progress_callback,
                total_tables=total_tables,
            )
            

            if progress_callback:
                ok_count = len([x for x in group_results if x.get("ok")])
                ko_count = len(group_results) - ok_count
                level = "success" if ko_count == 0 else "warning"
                progress_callback(
                    f"[DATABASE END] db={database_name} tables={len(items)} "
                    f"success={ok_count} failed={ko_count}",
                    level=level,
                )

            return group_results

        if database_parallel_workers == 1:
            grouped_results = []
            for database_name, items in db_groups.items():
                grouped_results.append(_run_database_group(database_name, items))
        else:
            grouped_results = []
            with ThreadPoolExecutor(max_workers=database_parallel_workers) as executor:
                future_map = {}
                for database_name, items in db_groups.items():
                    future = executor.submit(_run_database_group, database_name, items)
                    future_map[future] = database_name

                for future in as_completed(future_map):
                    grouped_results.append(future.result())

        for group_result in grouped_results:
            for item_result in group_result:
                results.append(item_result)
                if item_result.get("ok"):
                    success_count += 1
                else:
                    failed_count += 1

        duration_seconds = round(time.perf_counter() - started_perf, 3)

        plan.status = "success" if failed_count == 0 else "failed"
        plan.last_run_at = timezone.now()
        plan.last_duration_seconds = duration_seconds
        plan.last_total_databases = total_databases
        plan.last_total_tables = total_tables
        plan.last_success_count = success_count
        plan.last_failed_count = failed_count
        plan.last_error = "" if failed_count == 0 else f"{failed_count} item(s) failed"
        
        
        plan.last_result_payload = {
            "bootstrap_plan_id": plan.id,
            "bootstrap_plan_name": plan.name,
            "replication_service_id": plan.replication_service_id,
            "bootstrap_method": plan.bootstrap_method,
            "total_databases": total_databases,
            "total_tables": total_tables,
            "success_count": success_count,
            "failed_count": failed_count,
            "duration_seconds": duration_seconds,
            "results": results,
            "bootstrap_execution_id": execution.id,
            "parent_execution_id": source_execution.id if source_execution else None,
            "replay_mode": "execution_replay" if source_execution else "",
            "retry_failed_only": bool(retry_failed_only),
        }
        
        
        plan.save(
            update_fields=[
                "status",
                "last_run_at",
                "last_duration_seconds",
                "last_total_databases",
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


        if progress_callback:
            progress_callback(
                f"[PLAN END] plan='{plan.name}' "
                f"databases={total_databases} tables={total_tables} "
                f"success={success_count} failed={failed_count} "
                f"duration={duration_seconds}s",
                level="success" if failed_count == 0 else "warning",
            )      
            
        execution.status = "success" if failed_count == 0 else "failed"
        execution.total_databases = total_databases
        execution.total_tables = total_tables
        execution.success_count = success_count
        execution.failed_count = failed_count
        execution.finished_at = timezone.now()
        execution.duration_seconds = duration_seconds
        execution.last_error = "" if failed_count == 0 else f"{failed_count} item(s) failed"
        execution.result_payload = plan.last_result_payload
        execution.save(
            update_fields=[
                "status",
                "total_databases",
                "total_tables",
                "success_count",
                "failed_count",
                "finished_at",
                "duration_seconds",
                "last_error",
                "result_payload",
            ]
        )    

        return plan.last_result_payload

    except Exception as exc:
        duration_seconds = round(time.perf_counter() - started_perf, 3)


        plan.status = "failed"
        plan.last_run_at = timezone.now()
        plan.last_duration_seconds = duration_seconds
        plan.last_error = str(exc)
        
        
        plan.last_result_payload = {
            "bootstrap_plan_id": plan.id,
            "bootstrap_plan_name": plan.name,
            "bootstrap_execution_id": execution.id,
            "parent_execution_id": source_execution.id if source_execution else None,
            "replication_service_id": plan.replication_service_id,
            "replay_mode": "execution_replay" if source_execution else "",
            "retry_failed_only": bool(retry_failed_only),
            "duration_seconds": duration_seconds,
            "error": str(exc),
        }    
        
        plan.save(
            update_fields=[
                "status",
                "last_run_at",
                "last_duration_seconds",
                "last_error",
                "last_result_payload",
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
                "bootstrap_plan_name": plan.name,
                "error": str(exc),
            },
        )


        if progress_callback:
            progress_callback(
                f"[PLAN ERROR] plan='{plan.name}' error={exc}",
                level="error",
            )          
            
            
        execution.status = "failed"
        execution.finished_at = timezone.now()
        execution.duration_seconds = duration_seconds
        execution.last_error = str(exc)
        
        execution.result_payload = {
            "bootstrap_plan_id": plan.id,
            "bootstrap_plan_name": plan.name,
            "bootstrap_execution_id": execution.id,
            "replication_service_id": plan.replication_service_id,
            "error": str(exc),
            
            "parent_execution_id": source_execution.id if source_execution else None,
            "replay_mode": "execution_replay" if source_execution else "",
            "retry_failed_only": bool(retry_failed_only),
            "duration_seconds": duration_seconds,
        }
        
        execution.save(
            update_fields=[
                "status",
                "finished_at",
                "duration_seconds",
                "last_error",
                "result_payload",
            ]
        )
            
        raise

def _build_work_items(plan):
    
    
    explicit_items = list(
        plan.table_selections
        .select_related("source_table", "source_table__source_database")
        .filter(is_enabled=True)
        .order_by("execution_order", "id")
    )
    
    if explicit_items:
        work_items = []
        for item in explicit_items:
            work_items.append(
                {
                    "source": "plan_selection",
                    "item_id": item.id,
                    "database_name": item.source_table.source_database.database_name,
                    "table_name": item.source_table.table_name,
                    "recreate_table": item.recreate_table,
                    "truncate_target": item.truncate_target,
                    "validate_snapshot": item.validate_snapshot,
                }
            )
            
        return work_items

    database_names = _discover_plan_databases(plan)
    work_items = []

    for database_name in database_names:
        tables = discover_source_tables(
            replication_service_id=plan.replication_service_id,
            database_name=database_name,
            tables_csv=plan.include_tables_csv if plan.scope_mode == "tables" else "",
            like_filter=plan.like_filter or "",
            exclude_csv=plan.exclude_tables_csv or "",
            auto_exclude_srdf=bool(plan.auto_exclude_srdf),
        )

        if plan.table_limit and int(plan.table_limit) > 0:
            tables = tables[:int(plan.table_limit)]

        for table_name in tables:
            work_items.append(
                {
                    "source": "plan_auto",
                    "item_id": None,
                    "database_name": database_name,
                    "table_name": table_name,
                    "recreate_table": bool(plan.recreate_table),
                    "truncate_target": bool(plan.truncate_target),
                    "validate_snapshot": bool(plan.validate_snapshot),
                }
            )

    return work_items


def _build_replay_work_items(source_execution, retry_failed_only=False):
    items_qs = (
        source_execution.items
        .select_related("plan_table_selection")
        .order_by("id")
    )

    if retry_failed_only:
        items_qs = items_qs.filter(status="failed")

    work_items = []
    for item in items_qs:
        work_items.append(
            {
                "source": "execution_replay",
                "execution_item_id": item.id,
                "item_id": item.plan_table_selection_id,
                "database_name": item.database_name,
                "table_name": item.table_name,
                "recreate_table": bool(item.recreate_table),
                "truncate_target": bool(item.truncate_target),
                "validate_snapshot": bool(item.validate_snapshot),
            }
        )

    return work_items


def _discover_plan_databases(plan):
    if plan.source_database_name:
        return [plan.source_database_name]

    config = plan.replication_service.config or {}
    host = config.get("host")
    port = int(config.get("port", 3306))
    user = config.get("user")
    password = config.get("password", "")

    if not host or not user:
        raise BootstrapPlanRunnerError("Missing source DB connection config")

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW DATABASES")
            rows = cursor.fetchall() or []
            databases = [list(row.values())[0] for row in rows]

        include_set = _csv_to_set(plan.database_include_csv)
        exclude_set = _csv_to_set(plan.database_exclude_csv)
        like_pattern = (plan.database_like_pattern or "").strip()

        filtered = []
        for database_name in databases:
            if include_set and database_name not in include_set:
                continue
            if exclude_set and database_name in exclude_set:
                continue
            if like_pattern and not _sql_like_match(database_name, like_pattern):
                continue
            filtered.append(database_name)

        return filtered

    finally:
        conn.close()


def _run_database_items(
    plan,
    execution,
    database_name,
    items,
    initiated_by,
    dry_run_override=None,
    progress_callback=None,
    total_tables=0,
    ):
    
    parallel_workers = max(1, int(plan.parallel_workers or 1))
    results = []

    def _worker(item):
        
        return _run_single_work_item(
            plan=plan,
            execution=execution,
            item=item,
            initiated_by=initiated_by,
            dry_run_override=dry_run_override,
            progress_callback=progress_callback,
            total_tables=total_tables,
        )
    

    if parallel_workers == 1:
        for item in items:
            results.append(_worker(item))
        return results

    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        future_map = {}
        for item in items:
            future = executor.submit(_worker, item)
            future_map[future] = item

        for future in as_completed(future_map):
            results.append(future.result())

    return results


def _run_single_work_item(
    plan,
    execution,
    item,
    initiated_by,
    dry_run_override=None,
    progress_callback=None,
    total_tables=0,
    ):
    
    started_perf = time.perf_counter()

    plan_item = None
    if item.get("item_id"):
        plan_item = BootstrapPlanTableSelection.objects.filter(id=item["item_id"]).first()

    execution_item = BootstrapPlanExecutionItem.objects.create(
        bootstrap_execution=execution,
        bootstrap_plan=plan,
        plan_table_selection=plan_item,
        database_name=item["database_name"],
        table_name=item["table_name"],
        bootstrap_method=plan.bootstrap_method,
        status="running",
        recreate_table=bool(item["recreate_table"]),
        truncate_target=bool(item["truncate_target"]),
        validate_snapshot=bool(item["validate_snapshot"]),
        dry_run=bool(dry_run_override) if dry_run_override is not None else False,
    )

    if plan_item:
        plan_item.status = "running"
        plan_item.last_error = ""
        plan_item.save(update_fields=["status", "last_error", "updated_at"])

    try:
        if plan.bootstrap_method == "sql_copy":
            result = bootstrap_table_once(
                replication_service_id=plan.replication_service_id,
                database_name=item["database_name"],
                table_name=item["table_name"],
                recreate_table=bool(item["recreate_table"]),
                truncate_target=bool(item["truncate_target"]),
                initiated_by=initiated_by,
                dry_run=bool(dry_run_override) if dry_run_override is not None else False,
                validate_snapshot=bool(item["validate_snapshot"]),
            )
        elif plan.bootstrap_method == "mysqldump":
            result = _bootstrap_table_with_mysqldump(
                replication_service=plan.replication_service,
                database_name=item["database_name"],
                table_name=item["table_name"],
                truncate_target=bool(item["truncate_target"]),
                initiated_by=initiated_by,
                dry_run=bool(dry_run_override) if dry_run_override is not None else False,
            )
        else:
            raise BootstrapPlanRunnerError(f"Unsupported bootstrap_method '{plan.bootstrap_method}'")

        duration_seconds = round(time.perf_counter() - started_perf, 3)

        result["ok"] = bool(result.get("ok", True))
        result["database_name"] = item["database_name"]
        result["table_name"] = item["table_name"]
        result["duration_seconds"] = duration_seconds


        if plan_item:
            plan_item.status = "success" if result["ok"] else "failed"
            plan_item.last_run_at = timezone.now()
            plan_item.last_duration_seconds = duration_seconds
            
            plan_item.last_error = "" if result["ok"] else (
                result.get("error") or result.get("message") or "Unknown error"
            )
            
            plan_item.last_result_payload = result
            plan_item.save(
                update_fields=[
                    "status",
                    "last_run_at",
                    "last_duration_seconds",
                    "last_error",
                    "last_result_payload",
                    "updated_at",
                ]
            )
            
        execution_item.status = "success" if result["ok"] else "failed"
        execution_item.copied_rows = int(result.get("copied_rows") or 0)
        execution_item.source_row_count = int(result.get("source_row_count") or 0)
        execution_item.target_row_count = int(result.get("target_row_count") or 0)
        execution_item.source_checksum = result.get("source_checksum") or ""
        execution_item.target_checksum = result.get("target_checksum") or ""
        execution_item.snapshot_ok = bool(result.get("snapshot_ok", True))
        execution_item.table_created = bool(result.get("table_created", False))
        execution_item.table_truncated = bool(result.get("table_truncated", False))
        execution_item.finished_at = timezone.now()
        execution_item.duration_seconds = duration_seconds
        
        execution_item.error_message = "" if result["ok"] else (
            result.get("error") or result.get("message") or "Unknown error"
        )
        
        execution_item.result_payload = result
        execution_item.save(
            update_fields=[
                "status",
                "copied_rows",
                "source_row_count",
                "target_row_count",
                "source_checksum",
                "target_checksum",
                "snapshot_ok",
                "table_created",
                "table_truncated",
                "finished_at",
                "duration_seconds",
                "error_message",
                "result_payload",
            ]
        )
            

        copied_rows = result.get("copied_rows", 0)
        source_row_count = result.get("source_row_count", 0)
        target_row_count = result.get("target_row_count", 0)

        if progress_callback:
            progress_callback(
                f"[TABLE OK] db={item['database_name']} table={item['table_name']} "
                f"rows={copied_rows} source_rows={source_row_count} "
                f"target_rows={target_row_count} duration={duration_seconds}s",
                level="success" if result["ok"] else "warning",
            )

        return result

    except Exception as exc:
        duration_seconds = round(time.perf_counter() - started_perf, 3)

        if plan_item:
            plan_item.status = "failed"
            plan_item.last_run_at = timezone.now()
            plan_item.last_duration_seconds = duration_seconds
            plan_item.last_error = str(exc)
            plan_item.last_result_payload = {
                "ok": False,
                "database_name": item["database_name"],
                "table_name": item["table_name"],
                "duration_seconds": duration_seconds,
                "error": str(exc),
            }
            plan_item.save(
                update_fields=[
                    "status",
                    "last_run_at",
                    "last_duration_seconds",
                    "last_error",
                    "last_result_payload",
                    "updated_at",
                ]
            )
            
        execution_item.status = "failed"
        execution_item.finished_at = timezone.now()
        execution_item.duration_seconds = duration_seconds
        execution_item.error_message = str(exc)
        
        execution_item.result_payload = {
            "ok": False,
            "database_name": item["database_name"],
            "table_name": item["table_name"],
            "bootstrap_method": plan.bootstrap_method,
            "recreate_table": bool(item["recreate_table"]),
            "truncate_target": bool(item["truncate_target"]),
            "validate_snapshot": bool(item["validate_snapshot"]),
            "dry_run": bool(dry_run_override) if dry_run_override is not None else False,
            "duration_seconds": duration_seconds,
            "error": str(exc),
        }
        
        execution_item.save(
            update_fields=[
                "status",
                "finished_at",
                "duration_seconds",
                "error_message",
                "result_payload",
            ]
        )

        if progress_callback:
            progress_callback(
                f"[TABLE ERROR] db={item['database_name']} table={item['table_name']} "
                f"duration={duration_seconds}s error={exc}",
                level="error",
            )

        return {
            "ok": False,
            "database_name": item["database_name"],
            "table_name": item["table_name"],
            "bootstrap_method": plan.bootstrap_method,
            "recreate_table": bool(item["recreate_table"]),
            "truncate_target": bool(item["truncate_target"]),
            "validate_snapshot": bool(item["validate_snapshot"]),
            "dry_run": bool(dry_run_override) if dry_run_override is not None else False,
            "duration_seconds": duration_seconds,
            "error": str(exc),
        }

def _bootstrap_table_with_mysqldump(
    replication_service,
    database_name,
    table_name,
    truncate_target=True,
    initiated_by="srdf_bootstrap_plan",
    dry_run=False,
):
    config = replication_service.config or {}

    source_host = config.get("host")
    source_port = int(config.get("port", 3306))
    source_user = config.get("user")
    source_password = config.get("password", "")

    target_host = config.get("target_host")
    target_port = int(config.get("target_port", 3306))
    target_user = config.get("target_user")
    target_password = config.get("target_password", "")
    target_database = config.get("target_database")

    if not source_host or not source_user:
        raise BootstrapPlanRunnerError("Missing source config for mysqldump")
    if not target_host or not target_user or not target_database:
        raise BootstrapPlanRunnerError("Missing target config for mysqldump")

    dump_cmd = [
        "mysqldump",
        f"--host={source_host}",
        f"--port={source_port}",
        f"--user={source_user}",
        f"--password={source_password}",
        "--single-transaction",
        "--skip-lock-tables",
        database_name,
        table_name,
    ]

    import_cmd = [
        "mysql",
        f"--host={target_host}",
        f"--port={target_port}",
        f"--user={target_user}",
        f"--password={target_password}",
        target_database,
    ]

    if dry_run:
        return {
            "ok": True,
            "message": f"Dry-run mysqldump for '{database_name}.{table_name}'",
            "database_name": database_name,
            "table_name": table_name,
            "dump_cmd": " ".join([shlex.quote(x) for x in dump_cmd[:-1]]) + f" {table_name}",
            "import_cmd": " ".join([shlex.quote(x) for x in import_cmd[:-1]]) + f" {target_database}",
            "dry_run": True,
        }

    if truncate_target:
        _truncate_target_table_mysql_cli(
            host=target_host,
            port=target_port,
            user=target_user,
            password=target_password,
            database_name=target_database,
            table_name=table_name,
        )

    dump_proc = subprocess.Popen(
        dump_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
    )
    import_proc = subprocess.Popen(
        import_cmd,
        stdin=dump_proc.stdout,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy(),
    )

    dump_proc.stdout.close()

    import_stdout, import_stderr = import_proc.communicate()
    dump_stderr = dump_proc.stderr.read().decode("utf-8", errors="replace")
    dump_return = dump_proc.wait()

    if dump_return != 0:
        raise BootstrapPlanRunnerError(f"mysqldump failed: {dump_stderr}")

    if import_proc.returncode != 0:
        raise BootstrapPlanRunnerError(
            f"mysql import failed: {import_stderr.decode('utf-8', errors='replace')}"
        )

    AuditEvent.objects.create(
        event_type="bootstrap_table_mysqldump_completed",
        level="info",
        node=replication_service.target_node,
        message=f"mysqldump bootstrap completed for '{database_name}.{table_name}'",
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "database_name": database_name,
            "table_name": table_name,
            "target_database": target_database,
            "method": "mysqldump",
        },
    )

    return {
        "ok": True,
        "message": f"mysqldump bootstrap completed for '{database_name}.{table_name}'",
        "database_name": database_name,
        "table_name": table_name,
        "target_database": target_database,
        "method": "mysqldump",
        "dry_run": False,
    }


def _truncate_target_table_mysql_cli(host, port, user, password, database_name, table_name):
    conn = pymysql.connect(
        host=host,
        port=int(port),
        user=user,
        password=password,
        database=database_name,
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"TRUNCATE TABLE `{table_name}`")
    finally:
        conn.close()


def _csv_to_set(value):
    return {x.strip() for x in (value or "").split(",") if x.strip()}


def _sql_like_match(value, pattern):
    if not pattern:
        return True

    value = value or ""
    regex = "^" + (
        pattern
        .replace("\\", "\\\\")
        .replace(".", "\\.")
        .replace("%", ".*")
        .replace("_", ".")
    ) + "$"

    import re
    return bool(re.match(regex, value))