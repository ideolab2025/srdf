#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#------------------------------------------------------
#--- migrate_safe         v 1.0                     ---
#--- MARCH 2026                                     ---
#--- ORIGINAL AUTHOR  LOCKHEED LLC   IDEO-LAB       ---
#--- GUILLAUME CLAUDE ONEILL                        ---
#--- https://www.ideo-lab.com                       ---
#------------------------------------------------------


import time
from importlib   import import_module
from pathlib     import Path
from contextlib  import contextmanager
from dataclasses import dataclass, field
from typing      import List, Optional, Tuple

import json
from typing      import Any, Dict

from django.apps                 import apps
from django.core.management.base import BaseCommand, CommandError, no_translations
from django.core.management.sql  import emit_post_migrate_signal, emit_pre_migrate_signal
from django.db                   import DEFAULT_DB_ALIAS, connections, router
from django.db.migrations.autodetector import MigrationAutodetector
from django.db.migrations.executor import MigrationExecutor
from django.db.migrations.loader import AmbiguityError
from django.db.migrations.state  import ModelState, ProjectState
from django.utils.module_loading import module_has_submodule
from django.utils.text           import Truncator
from django.utils                import timezone

from migrate2026.models import SafeMigrationRun, SafeMigrationDDL

@dataclass
class DDLEntry:
    sql: str
    params_repr: str = ""
    status: str = "pending"   # pending / ok / failed
    error: str = ""
    sql_kind: str = ""

@dataclass
class MigrateSafeReport:
    started_at: float = 0.0
    finished_at: float = 0.0
    applied_before: set = field(default_factory=set)
    applied_after_success: set = field(default_factory=set)
    applied_after_failure: set = field(default_factory=set)
    rollback_after: set = field(default_factory=set)
    applied_migrations: List[str] = field(default_factory=list)
    rollbacked_migrations: List[str] = field(default_factory=list)
    ddl_entries: List[DDLEntry] = field(default_factory=list)
    failed_error: str = ""
    rollback_error: str = ""

    @property
    def duration_seconds(self) -> float:
        if not self.started_at or not self.finished_at:
            return 0.0
        return self.finished_at - self.started_at

    @property
    def ddl_executed_count(self) -> int:
        return sum(1 for x in self.ddl_entries if x.status == "ok")

    @property
    def ddl_failed_count(self) -> int:
        return sum(1 for x in self.ddl_entries if x.status == "failed")

    @property
    def failed_ddl_entries(self) -> List[DDLEntry]:
        return [x for x in self.ddl_entries if x.status == "failed"]


class DDLRecorder:
    def __init__(self, stdout=None, enabled=False):
        self.stdout = stdout
        self.enabled = enabled
        self.entries: List[DDLEntry] = []        
        
    def _detect_sql_kind(self, sql_text: str) -> str:
        value = (sql_text or "").strip().upper()
        if not value:
            return ""
    
        if value.startswith("CREATE TABLE"):
            return "CREATE_TABLE"
        if value.startswith("ALTER TABLE"):
            return "ALTER_TABLE"
        if value.startswith("DROP TABLE"):
            return "DROP_TABLE"
        if value.startswith("CREATE UNIQUE INDEX"):
            return "CREATE_UNIQUE_INDEX"
        if value.startswith("CREATE INDEX"):
            return "CREATE_INDEX"
        if value.startswith("DROP INDEX"):
            return "DROP_INDEX"
        if value.startswith("INSERT"):
            return "INSERT"
        if value.startswith("UPDATE"):
            return "UPDATE"
        if value.startswith("DELETE"):
            return "DELETE"
    
        return value.split()[0]    

    def before_execute(self, sql, params=None) -> DDLEntry:
        sql_text = (str(sql).strip() if sql else "").strip()
        params_repr = repr(params) if params else ""

        entry = DDLEntry(
            sql=sql_text,
            params_repr=params_repr,
            status="pending",
            error="",
            sql_kind=self._detect_sql_kind(sql_text),
        )
        
        self.entries.append(entry)

        if self.enabled and self.stdout and sql_text:
            line = f"    SQL> {sql_text}"
            if params_repr:
                line += f" -- params={params_repr}"
            self.stdout.write(line)

        return entry

    def mark_success(self, entry: DDLEntry) -> None:
        entry.status = "ok"

    def mark_failure(self, entry: DDLEntry, exc: Exception) -> None:
        entry.status = "failed"
        entry.error = str(exc)

        if self.enabled and self.stdout:
            self.stdout.write(self.stdout.style_func("ERROR")(f"    SQL FAILED> {entry.error}"))


@contextmanager
def patch_schema_editor(connection, recorder: DDLRecorder):
    original_schema_editor = connection.schema_editor

    def wrapped_schema_editor(*args, **kwargs):
        schema_editor = original_schema_editor(*args, **kwargs)
        original_execute = schema_editor.execute

        def execute(sql, params=()):
            entry = recorder.before_execute(sql, params)
            try:
                result = original_execute(sql, params)
                recorder.mark_success(entry)
                return result
            except Exception as exc:
                recorder.mark_failure(entry, exc)
                raise

        schema_editor.execute = execute
        return schema_editor

    connection.schema_editor = wrapped_schema_editor
    try:
        yield
    finally:
        connection.schema_editor = original_schema_editor


class Command(BaseCommand):
    help = "Safe migrate with DDL tracing, best-effort rollback, and structured final report."

    requires_system_checks = []

    def add_arguments(self, parser):
        parser.add_argument("--skip-checks", action="store_true", help="Skip system checks.")
        parser.add_argument("app_label", nargs="?", help="App label.")
        parser.add_argument("migration_name", nargs="?", help='Target migration name, or "zero".')
        parser.add_argument(
            "--noinput", "--no-input",
            action="store_false",
            dest="interactive",
            help="Do not prompt for input.",
        )
        parser.add_argument(
            "--database",
            default=DEFAULT_DB_ALIAS,
            help='Database alias. Defaults to "default".',
        )
        parser.add_argument("--fake", action="store_true", help="Mark migrations as run without running them.")
        parser.add_argument(
            "--fake-initial",
            action="store_true",
            help="Detect existing tables and fake-apply initial migrations if possible.",
        )
        parser.add_argument("--plan", action="store_true", help="Show migration plan.")
        parser.add_argument("--run-syncdb", action="store_true", help="Create tables for apps without migrations.")
        parser.add_argument(
            "--check",
            action="store_true",
            dest="check_unapplied",
            help="Exit non-zero if unapplied migrations exist.",
        )
        parser.add_argument("--prune", action="store_true", dest="prune", help="Not implemented in V2.")
        parser.add_argument(
            "--rollback-on-error",
            action="store_true",
            help="Attempt best-effort rollback if migrate fails.",
        )
        parser.add_argument(
            "--ddl-verbose",
            action="store_true",
            help="Print each DDL/SQL emitted by schema_editor.",
        )
        
        parser.add_argument(
            "--report-json",
            default="",
            help="Optional JSON file path for the final structured report.",
        )
        parser.add_argument(
            "--report-txt",
            default="",
            help="Optional text file path for the final structured report.",
        )        

    @no_translations
    def handle(self, *args, **options):
        database = options["database"]
        if not options["skip_checks"]:
            self.check(databases=[database])

        self.verbosity = options["verbosity"]
        self.interactive = options["interactive"]

        for app_config in apps.get_app_configs():
            if module_has_submodule(app_config.module, "management"):
                import_module(".management", app_config.name)

        connection = connections[database]
        connection.prepare_database()


        recorder = DDLRecorder(
            stdout=self._stdout_proxy(),
            enabled=bool(options["ddl_verbose"]),
        )        
        
        report = MigrateSafeReport(started_at=time.monotonic())
        
        run_row = SafeMigrationRun.objects.create(
            app_label=options.get("app_label") or "",
            migration_target=options.get("migration_name") or "",
            database_alias=database,
            status=SafeMigrationRun.STATUS_RUNNING,
            rollback_on_error=bool(options["rollback_on_error"]),
            ddl_verbose=bool(options["ddl_verbose"]),
        )
        
        if self.verbosity >= 1:
            self.stdout.write(
                self.style.SUCCESS(f"SafeMigrationRun created: id={run_row.id}")
            )        
        
        executor = MigrationExecutor(connection, self.migration_progress_callback)        
        executor.loader.check_consistent_history(connection)



        conflicts = executor.loader.detect_conflicts()
        if conflicts:
            name_str = "; ".join(
                "%s in %s" % (", ".join(names), app)
                for app, names in conflicts.items()
            )
            msg = "Conflicting migrations detected; multiple leaf nodes in migration graph: (%s)." % name_str
        
            self._finalize_run(
                run_row=run_row,
                report=report,
                status=SafeMigrationRun.STATUS_FAILED,
                failed_error=msg,
            )
            raise CommandError(msg)        
        

        run_syncdb = options["run_syncdb"]
        target_app_labels_only = True

        if options["app_label"]:
            app_label = options["app_label"]
            try:
                apps.get_app_config(app_label)
            except LookupError as err:
                msg = str(err)
                self._finalize_run(
                    run_row=run_row,
                    report=report,
                    status=SafeMigrationRun.STATUS_FAILED,
                    failed_error=msg,
                )
                raise CommandError(msg)            

            if run_syncdb:
                if app_label in executor.loader.migrated_apps:
                    msg = "Can't use run_syncdb with app '%s' as it has migrations." % app_label
                    self._finalize_run(
                        run_row=run_row,
                        report=report,
                        status=SafeMigrationRun.STATUS_FAILED,
                        failed_error=msg,
                    )
                    raise CommandError(msg)
            elif app_label not in executor.loader.migrated_apps:
                msg = "App '%s' does not have migrations." % app_label
                self._finalize_run(
                    run_row=run_row,
                    report=report,
                    status=SafeMigrationRun.STATUS_FAILED,
                    failed_error=msg,
                )
                raise CommandError(msg)

        if options["app_label"] and options["migration_name"]:
            migration_name = options["migration_name"]
            if migration_name == "zero":
                targets = [(app_label, None)]
            else:
                
                
                try:
                    migration = executor.loader.get_migration_by_prefix(app_label, migration_name)
                except AmbiguityError:
                    msg = (
                        "More than one migration matches '%s' in app '%s'. Please be more specific."
                        % (migration_name, app_label)
                    )
                    self._finalize_run(
                        run_row=run_row,
                        report=report,
                        status=SafeMigrationRun.STATUS_FAILED,
                        failed_error=msg,
                    )
                    raise CommandError(msg)
                except KeyError:
                    msg = (
                        "Cannot find a migration matching '%s' from app '%s'."
                        % (migration_name, app_label)
                    )
                    self._finalize_run(
                        run_row=run_row,
                        report=report,
                        status=SafeMigrationRun.STATUS_FAILED,
                        failed_error=msg,
                    )
                    raise CommandError(msg)                
                
                target = (app_label, migration.name)
                if target not in executor.loader.graph.nodes and target in executor.loader.replacements:
                    incomplete_migration = executor.loader.replacements[target]
                    target = incomplete_migration.replaces[-1]
                targets = [target]
            target_app_labels_only = False
        elif options["app_label"]:
            targets = [key for key in executor.loader.graph.leaf_nodes() if key[0] == app_label]
        else:
            targets = executor.loader.graph.leaf_nodes()



        if options["plan"]:
            plan = executor.migration_plan(targets)
            self.stdout.write("Planned operations:", self.style.MIGRATE_LABEL)
            if not plan:
                self.stdout.write("  No planned migration operations.")
            for migration, backwards in plan:
                self.stdout.write(str(migration), self.style.MIGRATE_HEADING)
                for operation in migration.operations:
                    message, is_error = self.describe_operation(operation, backwards)
                    style = self.style.WARNING if is_error else None
                    self.stdout.write("    " + message, style)
        
            self._finalize_run(
                run_row=run_row,
                report=report,
                status=SafeMigrationRun.STATUS_SUCCESS,
                recorder=recorder,
            )
            return        

        if options["prune"]:
            msg = "V4 migrate_safe does not implement --prune yet."
            self._finalize_run(
                run_row=run_row,
                report=report,
                status=SafeMigrationRun.STATUS_FAILED,
                failed_error=msg,
            )
            raise CommandError(msg)


        plan = executor.migration_plan(targets)
        run_syncdb = options["run_syncdb"] and executor.loader.unmigrated_apps

        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Operations to perform:"))
            if run_syncdb:
                if options["app_label"]:
                    self.stdout.write(self.style.MIGRATE_LABEL("  Synchronize unmigrated app: %s" % app_label))
                else:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL("  Synchronize unmigrated apps: ")
                        + ", ".join(sorted(executor.loader.unmigrated_apps))
                    )

            if target_app_labels_only:
                self.stdout.write(
                    self.style.MIGRATE_LABEL("  Apply all migrations: ")
                    + (", ".join(sorted({a for a, _ in targets})) or "(none)")
                )
            else:
                if targets[0][1] is None:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL("  Unapply all migrations: ") + str(targets[0][0])
                    )
                else:
                    self.stdout.write(
                        self.style.MIGRATE_LABEL("  Target specific migration: ")
                        + "%s, from %s" % (targets[0][1], targets[0][0])
                    )

        pre_migrate_state = executor._create_project_state(with_applied_migrations=True)
        pre_migrate_apps = pre_migrate_state.apps

        emit_pre_migrate_signal(
            self.verbosity,
            self.interactive,
            connection.alias,
            stdout=self.stdout,
            apps=pre_migrate_apps,
            plan=plan,
        )

        if run_syncdb:
            if self.verbosity >= 1:
                self.stdout.write(self.style.MIGRATE_HEADING("Synchronizing apps without migrations:"))
            if options["app_label"]:
                self.sync_apps(connection, [app_label])
            else:
                self.sync_apps(connection, executor.loader.unmigrated_apps)

        if self.verbosity >= 1:
            self.stdout.write(self.style.MIGRATE_HEADING("Running migrations:"))

        if not plan:
            if self.verbosity >= 1:
                self.stdout.write("  No migrations to apply.")
                autodetector = MigrationAutodetector(
                    executor.loader.project_state(),
                    ProjectState.from_apps(apps),
                )
                changes = autodetector.changes(graph=executor.loader.graph)
                if changes:
                    self.stdout.write(
                        self.style.NOTICE(
                            "  Your models in app(s): %s have changes not yet reflected in a migration."
                            % ", ".join(repr(app) for app in sorted(changes))
                        )
                    )
                    
            self._finalize_run(
                run_row=run_row,
                report=report,
                status=SafeMigrationRun.STATUS_SUCCESS,
                recorder=recorder,
            )
            return            

        fake = options["fake"]
        fake_initial = options["fake_initial"]
        report.applied_before = set(executor.loader.applied_migrations)

        try:
            with patch_schema_editor(connection, recorder):
                post_migrate_state = executor.migrate(
                    targets,
                    plan=plan,
                    state=pre_migrate_state.clone(),
                    fake=fake,
                    fake_initial=fake_initial,
                )

            refreshed_executor = MigrationExecutor(connection, self.migration_progress_callback)
            report.applied_after_success = set(refreshed_executor.loader.applied_migrations)
            report.applied_migrations = self._format_migration_names(
                report.applied_after_success - report.applied_before
            )

            post_migrate_state.clear_delayed_apps_cache()
            post_migrate_apps = post_migrate_state.apps

            with post_migrate_apps.bulk_update():
                model_keys = []
                for model_state in post_migrate_apps.real_models:
                    model_key = model_state.app_label, model_state.name_lower
                    model_keys.append(model_key)
                    post_migrate_apps.unregister_model(*model_key)
            post_migrate_apps.render_multiple(
                [ModelState.from_model(apps.get_model(*model)) for model in model_keys]
            )

            emit_post_migrate_signal(
                self.verbosity,
                self.interactive,
                connection.alias,
                stdout=self.stdout,
                apps=post_migrate_apps,
                plan=plan,
            )

        except Exception as exc:
            report.failed_error = str(exc)

            failed_executor = MigrationExecutor(connection, self.migration_progress_callback)
            report.applied_after_failure = set(failed_executor.loader.applied_migrations)
            report.applied_migrations = self._format_migration_names(
                report.applied_after_failure - report.applied_before
            )

            self.stdout.write("")
            self.stdout.write(self.style.ERROR(f"Migration failed: {exc}"))

            if options["rollback_on_error"]:
                rollbacked, rollback_error = self._attempt_best_effort_rollback(
                    connection=connection,
                    applied_before=report.applied_before,
                )
                report.rollbacked_migrations = rollbacked
                report.rollback_error = rollback_error or ""


            final_status = SafeMigrationRun.STATUS_FAILED
                
            if options["rollback_on_error"]:
                final_status = (
                    SafeMigrationRun.STATUS_ROLLBACK_FAILED
                    if report.rollback_error
                    else SafeMigrationRun.STATUS_FAILED_THEN_ROLLBACKED
                )
            
            self._finalize_run(
                run_row=run_row,
                report=report,
                status=final_status,
                recorder=recorder,
            )
            raise


        self._finalize_run(
            run_row=run_row,
            report=report,
            status=SafeMigrationRun.STATUS_SUCCESS,
            recorder=recorder,
        )        
        

    def _attempt_best_effort_rollback(self, connection, applied_before) -> Tuple[List[str], Optional[str]]:
        self.stdout.write(self.style.WARNING("Attempting best-effort rollback..."))

        rollback_executor = MigrationExecutor(connection, self.migration_progress_callback)
        applied_after = set(rollback_executor.loader.applied_migrations)
        newly_applied = applied_after - applied_before

        if not newly_applied:
            self.stdout.write(self.style.WARNING("No newly applied migrations detected. Nothing to rollback."))
            return [], None

        app_targets = {}
        for app_label, migration_name in newly_applied:
            app_targets.setdefault(app_label, []).append(migration_name)

        rollback_targets = []
        for app_label in sorted(app_targets.keys()):
            previous = self._find_previous_applied_migration_name(
                rollback_executor,
                app_label,
                applied_before,
            )
            rollback_targets.append((app_label, previous))

        self.stdout.write("Rollback targets:")
        for app_label, migration_name in rollback_targets:
            self.stdout.write(f"  - {app_label}: {migration_name or 'zero'}")

        rollbacked_names = self._format_migration_names(newly_applied)

        try:
            rollback_executor.migrate(
                rollback_targets,
                plan=rollback_executor.migration_plan(rollback_targets),
                state=rollback_executor._create_project_state(with_applied_migrations=True).clone(),
                fake=False,
                fake_initial=False,
            )
            self.stdout.write(self.style.SUCCESS("Best-effort rollback completed."))
            return rollbacked_names, None
        except Exception as rollback_exc:
            self.stdout.write(self.style.ERROR(f"Rollback failed: {rollback_exc}"))
            self.stdout.write(
                self.style.WARNING(
                    "Database may now be partially migrated. Manual inspection is required."
                )
            )
            return rollbacked_names, str(rollback_exc)
        
        
    def _finalize_run(
        self,
        run_row: SafeMigrationRun,
        report: MigrateSafeReport,
        status: str,
        recorder: Optional[DDLRecorder] = None,
        failed_error: str = "",
        ) -> None:
        
        if failed_error and not report.failed_error:
            report.failed_error = failed_error
    
        if recorder is not None:
            report.ddl_entries = recorder.entries
    
        report.finished_at = time.monotonic()
    
        run_row.status = status
        run_row.finished_at = timezone.now()
        run_row.duration_ms = int(report.duration_seconds * 1000)
        run_row.migrations_applied_count = len(report.applied_migrations)
        run_row.migrations_rollbacked_count = len(report.rollbacked_migrations)
        run_row.ddl_executed_count = report.ddl_executed_count
        run_row.ddl_failed_count = report.ddl_failed_count
        run_row.failed_error = report.failed_error or ""
        run_row.rollback_error = report.rollback_error or ""
        run_row.applied_migrations_text = "\n".join(report.applied_migrations)
        run_row.rollbacked_migrations_text = "\n".join(report.rollbacked_migrations)
        run_row.save()
    
        run_row.ddl_entries.all().delete()
    
        ddl_rows = []
        for idx, entry in enumerate(report.ddl_entries, start=1):
            ddl_rows.append(
                SafeMigrationDDL(
                    run=run_row,
                    seq_no=idx,
                    status=entry.status,
                    sql_kind=getattr(entry, "sql_kind", "") or "",
                    sql_text=entry.sql,
                    params_text=entry.params_repr,
                    error_text=entry.error,
                )
            )
    
        if ddl_rows:
            SafeMigrationDDL.objects.bulk_create(ddl_rows) 
        self._print_final_report(report, status, run_id=run_row.id)

    def _find_previous_applied_migration_name(self, executor, app_label, applied_before):
        app_applied = sorted(name for a, name in applied_before if a == app_label)
        if not app_applied:
            return None
        return app_applied[-1]

    def _format_migration_names(self, migrations_set) -> List[str]:
        return [f"{app}.{name}" for app, name in sorted(migrations_set)]


    def _print_final_report(
        self,
        report: MigrateSafeReport,
        status: str,
        run_id: Optional[int] = None,
        ) -> None:
        
        self.stdout.write("")
        self.stdout.write(self.style.MIGRATE_HEADING("Final migrate_safe report"))
        if run_id is not None:
            self.stdout.write(f"SafeMigrationRun id     : {run_id}")        
        self.stdout.write(f"Status                 : {status}")
        self.stdout.write(f"Duration total         : {report.duration_seconds:.3f}s")
        
        self.stdout.write(f"Migrations applied     : {len(report.applied_migrations)}")
        self.stdout.write(f"Migrations rollbacked  : {len(report.rollbacked_migrations)}")
        self.stdout.write(f"DDL executed           : {report.ddl_executed_count}")
        self.stdout.write(f"DDL failed             : {report.ddl_failed_count}")

        self.stdout.write("")
        self.stdout.write("Applied migrations:")
        if report.applied_migrations:
            for item in report.applied_migrations:
                self.stdout.write(f"- {item}")
        else:
            self.stdout.write("- none")

        self.stdout.write("")
        self.stdout.write("Rollbacked migrations:")
        if report.rollbacked_migrations:
            for item in report.rollbacked_migrations:
                self.stdout.write(f"- {item}")
        else:
            self.stdout.write("- none")

        self.stdout.write("")
        self.stdout.write("Failed DDL:")
        failed_ddl = report.failed_ddl_entries
        if failed_ddl:
            for idx, entry in enumerate(failed_ddl, start=1):
                self.stdout.write(f"- [{idx}] {entry.sql}")
                if entry.params_repr:
                    self.stdout.write(f"  params: {entry.params_repr}")
                if entry.error:
                    self.stdout.write(f"  error : {entry.error}")
        else:
            self.stdout.write("- none")

        if report.failed_error:
            self.stdout.write("")
            self.stdout.write(self.style.ERROR(f"Migration error: {report.failed_error}"))

        if report.rollback_error:
            self.stdout.write(self.style.ERROR(f"Rollback error : {report.rollback_error}"))


    def _report_to_dict(self, report: MigrateSafeReport) -> Dict[str, Any]:
        return {
            "status": self._compute_final_status(report),
            "duration_seconds": round(report.duration_seconds, 6),
            "migrations_applied_count": len(report.applied_migrations),
            "migrations_rollbacked_count": len(report.rollbacked_migrations),
            "ddl_executed_count": report.ddl_executed_count,
            "ddl_failed_count": report.ddl_failed_count,
            "migrations_applied": list(report.applied_migrations),
            "migrations_rollbacked": list(report.rollbacked_migrations),
            "failed_error": report.failed_error,
            "rollback_error": report.rollback_error,
            "failed_ddl": [
                {
                    "sql": entry.sql,
                    "params": entry.params_repr,
                    "error": entry.error,
                }
                for entry in report.failed_ddl_entries
            ],
            "ddl_entries": [
                {
                    "sql": entry.sql,
                    "params": entry.params_repr,
                    "status": entry.status,
                    "error": entry.error,
                }
                for entry in report.ddl_entries
            ],
        }
    
    
    def _compute_final_status(self, report: MigrateSafeReport) -> str:
        if report.rollback_error:
            return "rollback_failed"
        if report.failed_error and report.rollbacked_migrations:
            return "failed_then_rollbacked"
        if report.failed_error:
            return "failed"
        return "success"
    
    
    def _write_json_report(self, report: MigrateSafeReport, output_path: str) -> None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = self._report_to_dict(report)
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    
    
    def _write_text_report(self, report: MigrateSafeReport, output_path: str) -> None:
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)
    
        status = self._compute_final_status(report)
    
        lines: List[str] = []
        lines.append("migrate_safe report")
        lines.append("===================")
        lines.append(f"Status                : {status}")
        lines.append(f"Duration total        : {report.duration_seconds:.3f}s")
        lines.append(f"Migrations applied    : {len(report.applied_migrations)}")
        lines.append(f"Migrations rollbacked : {len(report.rollbacked_migrations)}")
        lines.append(f"DDL executed          : {report.ddl_executed_count}")
        lines.append(f"DDL failed            : {report.ddl_failed_count}")
        lines.append("")
    
        lines.append("Applied migrations:")
        if report.applied_migrations:
            for item in report.applied_migrations:
                lines.append(f"- {item}")
        else:
            lines.append("- none")
        lines.append("")
    
        lines.append("Rollbacked migrations:")
        if report.rollbacked_migrations:
            for item in report.rollbacked_migrations:
                lines.append(f"- {item}")
        else:
            lines.append("- none")
        lines.append("")
    
        lines.append("Failed DDL:")
        failed_ddl = report.failed_ddl_entries
        if failed_ddl:
            for idx, entry in enumerate(failed_ddl, start=1):
                lines.append(f"- [{idx}] {entry.sql}")
                if entry.params_repr:
                    lines.append(f"  params: {entry.params_repr}")
                if entry.error:
                    lines.append(f"  error : {entry.error}")
        else:
            lines.append("- none")
        lines.append("")
    
        if report.failed_error:
            lines.append(f"Migration error: {report.failed_error}")
        if report.rollback_error:
            lines.append(f"Rollback error : {report.rollback_error}")
    
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    
    
    
    def _export_final_reports(self, report: MigrateSafeReport, options) -> None:
        report_json = options.get("report_json") or ""
        report_txt = options.get("report_txt") or ""
    
        if report_json:
            self._write_json_report(report, report_json)
            self.stdout.write(self.style.SUCCESS(f"JSON report written: {report_json}"))
    
        if report_txt:
            self._write_text_report(report, report_txt)
            self.stdout.write(self.style.SUCCESS(f"Text report written: {report_txt}"))    


    def _stdout_proxy(self):
        class _Proxy:
            def __init__(self, stdout, style):
                self.stdout = stdout
                self.style = style

            def write(self, msg):
                self.stdout.write(msg)

            def style_func(self, kind):
                if kind == "ERROR":
                    return self.style.ERROR
                return lambda x: x

        return _Proxy(self.stdout, self.style)

    def migration_progress_callback(self, action, migration=None, fake=False):
        if self.verbosity >= 1:
            compute_time = self.verbosity > 1
            if action == "apply_start":
                if compute_time:
                    self.start = time.monotonic()
                self.stdout.write("  Applying %s..." % migration, ending="")
                self.stdout.flush()
            elif action == "apply_success":
                elapsed = (" (%.3fs)" % (time.monotonic() - self.start) if compute_time else "")
                self.stdout.write(self.style.SUCCESS((" FAKED" if fake else " OK") + elapsed))
            elif action == "unapply_start":
                if compute_time:
                    self.start = time.monotonic()
                self.stdout.write("  Unapplying %s..." % migration, ending="")
                self.stdout.flush()
            elif action == "unapply_success":
                elapsed = (" (%.3fs)" % (time.monotonic() - self.start) if compute_time else "")
                self.stdout.write(self.style.SUCCESS((" FAKED" if fake else " OK") + elapsed))
            elif action == "render_start":
                if compute_time:
                    self.start = time.monotonic()
                self.stdout.write("  Rendering model states...", ending="")
                self.stdout.flush()
            elif action == "render_success":
                elapsed = (" (%.3fs)" % (time.monotonic() - self.start) if compute_time else "")
                self.stdout.write(self.style.SUCCESS(" DONE" + elapsed))

    def describe_operation(self, operation, backwards):
        is_error = False
        if hasattr(operation, "code"):
            action = "IRREVERSIBLE" if backwards else "RUN PYTHON"
            is_error = backwards and not operation.reversible
        elif hasattr(operation, "sql"):
            action = "RUN SQL"
        else:
            action = operation.__class__.__name__.upper()

        if backwards:
            action = "UNDO " + action

        if hasattr(operation, "name"):
            description = operation.name
        elif hasattr(operation, "model_name"):
            description = operation.model_name
        else:
            description = str(operation)

        return f"{action}: {Truncator(description).chars(40)}", is_error

    def sync_apps(self, connection, app_labels):
        with connection.cursor() as cursor:
            tables = connection.introspection.table_names(cursor)

        all_models = [
            (
                app_config.label,
                router.get_migratable_models(
                    app_config,
                    connection.alias,
                    include_auto_created=False,
                ),
            )
            for app_config in apps.get_app_configs()
            if app_config.models_module is not None and app_config.label in app_labels
        ]

        manifest = {
            model._meta.db_table: model
            for app_name, model_list in all_models
            for model in model_list
            if model._meta.can_migrate(connection)
        }

        if self.verbosity >= 1:
            self.stdout.write("  Creating tables...")
        with connection.schema_editor() as editor:
            for model in manifest.values():
                if model._meta.db_table not in tables:
                    if self.verbosity >= 1:
                        self.stdout.write("    Creating table %s" % model._meta.db_table)
                    editor.create_model(model)

        if self.verbosity >= 1:
            self.stdout.write("    Running deferred SQL...")