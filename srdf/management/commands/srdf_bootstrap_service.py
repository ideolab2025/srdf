#! /usr/bin/python3.1
# -*- coding: utf-8 -*-



import pymysql

from concurrent.futures import ThreadPoolExecutor, as_completed

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.models import AuditEvent, ReplicationService
from srdf.services.table_bootstrap import bootstrap_table_once, discover_source_tables


class Command(BaseCommand):
    help = "Bootstrap multiple tables for a ReplicationService"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, required=True)

        parser.add_argument(
            "--database-name",
            type=str,
            required=True,
            help="Source database name",
        )

        parser.add_argument(
            "--tables",
            type=str,
            default="",
            help="Comma separated list of tables (optional)",
        )

        parser.add_argument(
            "--like",
            type=str,
            default="",
            help="SQL LIKE filter (example: user_%%)",
        )

        parser.add_argument(
            "--exclude",
            type=str,
            default="",
            help="Comma separated list of tables to exclude",
        )

        parser.add_argument(
            "--recreate-table",
            action="store_true",
        )

        parser.add_argument(
            "--no-truncate-target",
            action="store_true",
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=0,
            help="Limit number of tables",
        )
        
        parser.add_argument(
            "--parallel",
            type=int,
            default=1,
            help="Number of worker threads for bootstrap",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Discover and simulate bootstrap without writing target DB",
        )
        parser.add_argument(
            "--progress",
            action="store_true",
            help="Display progress percentage",
        )
        parser.add_argument(
            "--no-auto-exclude-srdf",
            action="store_true",
            help="Do not auto-exclude SRDF and Django internal tables",
        )
        parser.add_argument(
            "--validate-snapshot",
            action="store_true",
            help="Validate source/target row count and sample checksum",
        )        


    def handle(self, *args, **options):

        service_id = options["service_id"]
        database_name = options["database_name"]
        tables_csv = options["tables"]
        like_filter = options["like"]
        exclude_csv = options["exclude"]
        recreate_table = bool(options["recreate_table"])
        truncate_target = not bool(options["no_truncate_target"])
        limit = int(options["limit"] or 0)
        parallel = max(1, int(options["parallel"] or 1))
        dry_run = bool(options["dry_run"])
        show_progress = bool(options["progress"])
        auto_exclude_srdf = not bool(options["no_auto_exclude_srdf"])
        validate_snapshot = bool(options["validate_snapshot"])

        started_at = timezone.now()

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF BOOTSTRAP SERVICE")
        self.stdout.write("============================================================")
        self.stdout.write(f"service_id         : {service_id}")
        self.stdout.write(f"database_name      : {database_name}")
        self.stdout.write(f"recreate_table     : {recreate_table}")
        self.stdout.write(f"truncate_target    : {truncate_target}")
        self.stdout.write(f"parallel           : {parallel}")
        self.stdout.write(f"dry_run            : {dry_run}")
        self.stdout.write(f"progress           : {show_progress}")
        self.stdout.write(f"auto_exclude_srdf  : {auto_exclude_srdf}")
        self.stdout.write(f"validate_snapshot  : {validate_snapshot}")
        self.stdout.write("")

        replication_service = (
            ReplicationService.objects
            .filter(id=service_id, is_enabled=True)
            .first()
        )

        if not replication_service:
            raise CommandError(f"ReplicationService #{service_id} not found")

        tables = discover_source_tables(
            replication_service_id=service_id,
            database_name=database_name,
            tables_csv=tables_csv,
            like_filter=like_filter,
            exclude_csv=exclude_csv,
            auto_exclude_srdf=auto_exclude_srdf,
        )

        if not tables:
            raise CommandError("No tables found to bootstrap")

        if limit > 0:
            tables = tables[:limit]

        total_tables = len(tables)
        success_count = 0
        failed_count = 0
        results = []

        self.stdout.write(f"tables_to_process  : {total_tables}")
        self.stdout.write("")

        def _worker(table_name):
            return bootstrap_table_once(
                replication_service_id=service_id,
                database_name=database_name,
                table_name=table_name,
                recreate_table=recreate_table,
                truncate_target=truncate_target,
                initiated_by="cli_bootstrap_service",
                dry_run=dry_run,
                validate_snapshot=validate_snapshot,
            )

        if parallel == 1:
            for idx, table_name in enumerate(tables, start=1):
                if show_progress:
                    pct = round((idx / total_tables) * 100, 1)
                    self.stdout.write(f"[{idx}/{total_tables}] {table_name} progress={pct}%")
                else:
                    self.stdout.write(f"[{idx}/{total_tables}] {table_name}")

                try:
                    result = _worker(table_name)
                    results.append(result)
                    self.stdout.write(
                        f"   OK rows={result.get('copied_rows', 0)} "
                        f"snapshot_ok={result.get('snapshot_ok', True)}"
                    )
                    success_count += 1
                except Exception as exc:
                    results.append(
                        {
                            "ok": False,
                            "table_name": table_name,
                            "error": str(exc),
                        }
                    )
                    self.stdout.write(f"   FAIL {str(exc)}")
                    failed_count += 1
        else:
            future_map = {}
            with ThreadPoolExecutor(max_workers=parallel) as executor:
                for table_name in tables:
                    future = executor.submit(_worker, table_name)
                    future_map[future] = table_name

                done_count = 0
                for future in as_completed(future_map):
                    table_name = future_map[future]
                    done_count += 1

                    if show_progress:
                        pct = round((done_count / total_tables) * 100, 1)
                        self.stdout.write(f"[{done_count}/{total_tables}] {table_name} progress={pct}%")
                    else:
                        self.stdout.write(f"[{done_count}/{total_tables}] {table_name}")

                    try:
                        result = future.result()
                        results.append(result)
                        self.stdout.write(
                            f"   OK rows={result.get('copied_rows', 0)} "
                            f"snapshot_ok={result.get('snapshot_ok', True)}"
                        )
                        success_count += 1
                    except Exception as exc:
                        results.append(
                            {
                                "ok": False,
                                "table_name": table_name,
                                "error": str(exc),
                            }
                        )
                        self.stdout.write(f"   FAIL {str(exc)}")
                        failed_count += 1

        duration = round((timezone.now() - started_at).total_seconds(), 3)

        AuditEvent.objects.create(
            event_type="transport_service_bootstrap_completed",
            level="info" if failed_count == 0 else "warning",
            node=replication_service.target_node,
            message=f"Service bootstrap completed for service '{replication_service.name}'",
            created_by="cli_bootstrap_service",
            payload={
                "replication_service_id": replication_service.id,
                "database_name": database_name,
                "total_tables": total_tables,
                "success_count": success_count,
                "failed_count": failed_count,
                "parallel": parallel,
                "dry_run": dry_run,
                "validate_snapshot": validate_snapshot,
                "auto_exclude_srdf": auto_exclude_srdf,
                "duration_seconds": duration,
                "results": results,
            },
        )

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SUMMARY")
        self.stdout.write("============================================================")
        self.stdout.write(f"total_tables : {total_tables}")
        self.stdout.write(f"success      : {success_count}")
        self.stdout.write(f"failed       : {failed_count}")
        self.stdout.write(f"duration     : {duration}s")
        self.stdout.write("============================================================")
        self.stdout.write("")        
        

