#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import os
import shlex
import subprocess
import time

import pymysql

from concurrent.futures import ThreadPoolExecutor, as_completed

from django.utils import timezone

from srdf.models import AuditEvent, BootstrapPlan, BootstrapPlanItem
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

    started_perf = time.perf_counter()

    plan.status = "running"
    plan.last_error = ""
    plan.save(update_fields=["status", "last_error", "updated_at"])

    try:
        work_items = _build_work_items(plan)

        total_databases = len(sorted(set([x["database_name"] for x in work_items])))
        total_tables = len(work_items)
        success_count = 0
        failed_count = 0
        results = []

        db_groups = {}
        for item in work_items:
            db_groups.setdefault(item["database_name"], []).append(item)

        database_parallel_workers = max(1, int(plan.database_parallel_workers or 1))

        def _run_database_group(database_name, items):
            return _run_database_items(
                plan=plan,
                database_name=database_name,
                items=items,
                initiated_by=initiated_by,
                dry_run_override=dry_run_override,
            )

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
            "replication_service_id": plan.replication_service_id,
            "bootstrap_method": plan.bootstrap_method,
            "total_databases": total_databases,
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


def _build_work_items(plan):
    explicit_items = list(
        plan.items.filter(is_enabled=True).order_by("execution_order", "id")
    )

    if explicit_items:
        work_items = []
        for item in explicit_items:
            work_items.append(
                {
                    "source": "plan_item",
                    "item_id": item.id,
                    "database_name": item.source_database_name,
                    "table_name": item.table_name,
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


def _run_database_items(plan, database_name, items, initiated_by, dry_run_override=None):
    parallel_workers = max(1, int(plan.parallel_workers or 1))
    results = []

    def _worker(item):
        return _run_single_work_item(
            plan=plan,
            item=item,
            initiated_by=initiated_by,
            dry_run_override=dry_run_override,
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


def _run_single_work_item(plan, item, initiated_by, dry_run_override=None):
    started_perf = time.perf_counter()
    plan_item = None

    if item.get("item_id"):
        plan_item = BootstrapPlanItem.objects.filter(id=item["item_id"]).first()
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

        if plan_item:
            plan_item.status = "success"
            plan_item.last_run_at = timezone.now()
            plan_item.last_duration_seconds = duration_seconds
            plan_item.last_error = ""
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

        result["ok"] = bool(result.get("ok", True))
        result["database_name"] = item["database_name"]
        result["table_name"] = item["table_name"]
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
                "error": str(exc),
                "database_name": item["database_name"],
                "table_name": item["table_name"],
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

        return {
            "ok": False,
            "database_name": item["database_name"],
            "table_name": item["table_name"],
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