#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import uuid

from django.core.management.base import BaseCommand
from django.utils import timezone

from srdf.models import (
    ReplicationService,
    SRDFCronExecution,
    SRDFServiceExecutionLock,
)

from srdf.services.binlog_capture import (
    capture_binlog_once,
    capture_binlog_once_new,
    list_binlog_updates_since_checkpoint,
    check_binlog_updates_vs_outbound,
)


CRON_NAME = "srdf_capture_mariadb_binlog"
CRON_TYPE = "capture"


def _build_capture_cron_lock_name(service_id):
    return f"srdf_capture_cron:service_{int(service_id)}"


def _build_command_line(
    service_id,
    limit=None,
    initialize_only=False,
    force_current_pos=False,
    max_scanned_events=None,
    max_runtime_seconds=None,
    enable_filter_audit=False,
    disable_time_limit=False,
    debug=False,
    verify_with_mysqlbinlog=False,
    progress_enabled=False,
    progress_every_events=None,
    progress_every_seconds=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
    list_updates=False,
    check_updates=False,
    ):
    
    parts = [
        "python",
        "manage.py",
        CRON_NAME,
        f"--service-id={service_id}",
    ]

    if limit is not None:
        parts.append(f"--limit={limit}")
    if initialize_only:
        parts.append("--initialize-only")
    if force_current_pos:
        parts.append("--force-current-pos")
    if max_scanned_events is not None:
        parts.append(f"--max-scanned-events={max_scanned_events}")
    if max_runtime_seconds is not None:
        parts.append(f"--max-runtime-seconds={max_runtime_seconds}")
    if enable_filter_audit:
        parts.append("--enable-filter-audit")
    if disable_time_limit:
        parts.append("--disable-time-limit")
    if debug:
        parts.append("--debug")
    if verify_with_mysqlbinlog:
        parts.append("--verify")
    if progress_enabled:
        parts.append("--progress")
    if progress_every_events is not None:
        parts.append(f"--progress-every-events={progress_every_events}")
    if progress_every_seconds is not None:
        parts.append(f"--progress-every-seconds={progress_every_seconds}")
    if scan_window_max_files is not None:
        parts.append(f"--scan-window-max-files={scan_window_max_files}")
    if scan_window_max_bytes is not None:
        parts.append(f"--scan-window-max-bytes={scan_window_max_bytes}")
    if list_updates:
        parts.append("--list-updates")
    if check_updates:
        parts.append("--check-updates")

    return " ".join(parts)


def _acquire_capture_cron_lock(replication_service):
    lock_name = _build_capture_cron_lock_name(replication_service.id)

    lock_obj, _ = SRDFServiceExecutionLock.objects.get_or_create(
        name=lock_name,
        replication_service=replication_service,
        defaults={
            "is_locked": False,
            "owner_token": "",
        },
    )

    if lock_obj.is_locked:
        return None, ""

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


def _release_capture_cron_lock(lock_obj, owner_token=""):
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


def _create_cron_execution(replication_service, command_line, input_params, initiated_by):
    return SRDFCronExecution.objects.create(
        execution_uid=str(uuid.uuid4()),
        cron_name=CRON_NAME,
        cron_type=CRON_TYPE,
        replication_service=replication_service,
        node=replication_service.source_node if replication_service else None,
        engine=replication_service.service_type if replication_service else "",
        command_line=command_line or "",
        initiated_by=initiated_by or "",
        status="running",
        input_params=input_params or {},
    )


def _finalize_cron_execution(
    cron_exec,
    status,
    return_code,
    error_code="",
    error_message="",
    result_payload=None,
    progress_payload=None,
    ):
    
    finished_at = timezone.now()
    duration_seconds = 0.0
    if cron_exec.started_at:
        duration_seconds = round(
            (finished_at - cron_exec.started_at).total_seconds(),
            3,
        )

    cron_exec.status = status or "success"
    cron_exec.return_code = return_code or ""
    cron_exec.error_code = error_code or ""
    cron_exec.error_message = error_message or ""
    cron_exec.finished_at = finished_at
    cron_exec.duration_seconds = duration_seconds
    cron_exec.result_payload = result_payload or {}
    cron_exec.progress_payload = progress_payload or {}

    cron_exec.save(
        update_fields=[
            "status",
            "return_code",
            "error_code",
            "error_message",
            "finished_at",
            "duration_seconds",
            "result_payload",
            "progress_payload",
            "updated_at",
        ]
    )
    return cron_exec


def _map_capture_result_to_cron_outcome(result):
    result = result or {}

    if result.get("control_mode") == "list_updates":
        total_not_captured = int(result.get("total_not_captured") or 0)
        if total_not_captured > 0:
            return "warning", "CAPTURE_CONTROL_LIST_WARNING", "", ""
        return "success", "CAPTURE_CONTROL_LIST_OK", "", ""

    if result.get("control_mode") == "check_updates":
        total_not_captured = int(result.get("total_not_captured") or 0)
        total_duplicates = int(result.get("total_duplicates") or 0)
        if total_not_captured > 0 or total_duplicates > 0:
            return "warning", "CAPTURE_CONTROL_CHECK_WARNING", "", ""
        return "success", "CAPTURE_CONTROL_CHECK_OK", "", ""

    if result.get("initialized_only"):
        return "success", "CAPTURE_INIT_ONLY", "", ""

    stop_reason = (result.get("stop_reason") or "").strip().lower()
    captured_count = int(result.get("captured_count") or 0)
    skipped_count = int(result.get("skipped_count") or 0)

    if stop_reason == "no_events_available":
        return "success", "CAPTURE_NO_EVENTS", "", ""

    if stop_reason in ("capture_limit_reached", "max_scanned_events_reached", "scan_window_limit_reached"):
        if captured_count > 0:
            return "success", "CAPTURE_OK_PARTIAL", "", ""
        if skipped_count > 0:
            return "warning", "CAPTURE_WARNING_SKIPPED_ONLY", "", ""
        return "success", "CAPTURE_OK_PARTIAL", "", ""

    if result.get("ok"):
        if skipped_count > 0:
            return "warning", "CAPTURE_WARNING", "", ""
        return "success", "CAPTURE_OK", "", ""

    return "error", "CAPTURE_ERROR", "CAPTURE_ERROR", "Capture returned a non-success result"


def _map_return_code_to_exit_code(return_code):
    code = str(return_code or "").strip().upper()

    if code in {
        "CAPTURE_OK",
        "CAPTURE_OK_PARTIAL",
        "CAPTURE_NO_EVENTS",
        "CAPTURE_INIT_ONLY",
        "CAPTURE_CONTROL_LIST_OK",
        "CAPTURE_CONTROL_CHECK_OK",
    }:
        return 0

    if code in {
        "CAPTURE_WARNING",
        "CAPTURE_WARNING_SKIPPED_ONLY",
        "CAPTURE_CONTROL_LIST_WARNING",
        "CAPTURE_CONTROL_CHECK_WARNING",
    }:
        return 10

    if code in {
        "CAPTURE_BLOCKED_ALREADY_RUNNING",
    }:
        return 20

    if code in {
        "CAPTURE_INVALID_ARGS",
    }:
        return 30

    if code in {
        "CAPTURE_TIMEOUT",
    }:
        return 50

    if code in {
        "CAPTURE_EXCEPTION",
        "CAPTURE_ERROR",
    }:
        return 40

    return 40


def _exit_with_code(exit_code):
    raise SystemExit(int(exit_code or 0))



class Command(BaseCommand):
    help = "Capture MySQL/MariaDB binlog events into OutboundChangeEvent"

    def add_arguments(self, parser):
        parser.add_argument(
            "--service-id",
            type=int,
            required=True,
            help="ReplicationService ID (mysql or mariadb)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=None,
            help="Maximum number of outbound events to capture in one run (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--initialize-only",
            action="store_true",
            help="Initialize checkpoint at current master position and stop without capturing",
        )
        parser.add_argument(
            "--force-current-pos",
            action="store_true",
            help="Force checkpoint to current master position before capture",
        )
        parser.add_argument(
            "--max-scanned-events",
            type=int,
            default=None,
            help="Maximum number of binlog events scanned during one run (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--max-runtime-seconds",
            type=int,
            default=None,
            help="Maximum runtime in seconds for one capture run (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--enable-filter-audit",
            action="store_true",
            help="Create one AuditEvent for each filtered event",
        )
        parser.add_argument(
            "--disable-time-limit",
            action="store_true",
            help="Disable runtime timeout for this execution only",
        )
        parser.add_argument(
            "--debug",
            action="store_true",
            help="Debug and verbose mode",
        )
        parser.add_argument(
            "--verify",
            action="store_true",
            help="Verify captured stream against mysqlbinlog output",
        )
        parser.add_argument(
            "--progress",
            action="store_true",
            help="Enable regular progress messages during capture",
        )
        parser.add_argument(
            "--progress-every-events",
            type=int,
            default=None,
            help="Emit progress every N scanned events (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--progress-every-seconds",
            type=int,
            default=None,
            help="Emit progress at least every N seconds (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--scan-window-max-files",
            type=int,
            default=None,
            help="Maximum number of binlog files scanned in one run (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--scan-window-max-bytes",
            type=int,
            default=None,
            help="Maximum number of bytes scanned in one run (None = resolve from SRDF settings)",
        )
        parser.add_argument(
            "--list-updates",
            action="store_true",
            help="Read-only mode: list SQL updates found in binlog since the last SRDF capture checkpoint",
        )
        parser.add_argument(
            "--check-updates",
            action="store_true",
            help="Read-only mode: compare SQL updates found in binlog with existing OutboundChangeEvent rows",
        )        
        
    def handle(self, *args, **options):
        service_id = options["service_id"]

        limit = options.get("limit")
        initialize_only = bool(options["initialize_only"])
        force_current_pos = bool(options["force_current_pos"])

        max_scanned_events = options.get("max_scanned_events")
        max_runtime_seconds = options.get("max_runtime_seconds")

        enable_filter_audit = True if options.get("enable_filter_audit") else None
        disable_time_limit = True if options.get("disable_time_limit") else None

        progress_enabled = True if options.get("progress") else None
        progress_every_events = options.get("progress_every_events")
        progress_every_seconds = options.get("progress_every_seconds")

        scan_window_max_files = options.get("scan_window_max_files")
        scan_window_max_bytes = options.get("scan_window_max_bytes")

        debug = True if options.get("debug") else None
        verify_with_mysqlbinlog = True if options.get("verify") else None



        list_updates = bool(options.get("list_updates"))
        check_updates = bool(options.get("check_updates"))

        replication_service = ReplicationService.objects.get(pk=service_id)

        if list_updates and check_updates:
            cron_exec = _create_cron_execution(
                replication_service=replication_service,
                command_line="invalid_args",
                input_params={"list_updates": True, "check_updates": True},
                initiated_by="srdf_capture_mariadb_binlog",
            )

            _finalize_cron_execution(
                cron_exec=cron_exec,
                status="error",
                return_code="CAPTURE_INVALID_ARGS",
                error_code="INVALID_ARGUMENTS",
                error_message="list_updates and check_updates cannot be used together",
                result_payload={},
                progress_payload={},
            )

            self.stdout.write(
                self.style.ERROR(
                    f"Invalid arguments: list_updates AND check_updates (service_id={service_id})"
                )
            )
            _exit_with_code(30)


        command_line = _build_command_line(
            service_id=service_id,
            limit=limit,
            initialize_only=initialize_only,
            force_current_pos=force_current_pos,
            max_scanned_events=max_scanned_events,
            max_runtime_seconds=max_runtime_seconds,
            enable_filter_audit=bool(enable_filter_audit),
            disable_time_limit=bool(disable_time_limit),
            debug=bool(debug),
            verify_with_mysqlbinlog=bool(verify_with_mysqlbinlog),
            progress_enabled=bool(progress_enabled),
            progress_every_events=progress_every_events,
            progress_every_seconds=progress_every_seconds,
            scan_window_max_files=scan_window_max_files,
            scan_window_max_bytes=scan_window_max_bytes,
            list_updates=list_updates,
            check_updates=check_updates,
        )

        input_params = {
            "service_id": service_id,
            "limit": limit,
            "initialize_only": bool(initialize_only),
            "force_current_pos": bool(force_current_pos),
            "max_scanned_events": max_scanned_events,
            "max_runtime_seconds": max_runtime_seconds,
            "enable_filter_audit": bool(enable_filter_audit),
            "disable_time_limit": bool(disable_time_limit),
            "debug": bool(debug),
            "verify_with_mysqlbinlog": bool(verify_with_mysqlbinlog),
            "progress_enabled": bool(progress_enabled),
            "progress_every_events": progress_every_events,
            "progress_every_seconds": progress_every_seconds,
            "scan_window_max_files": scan_window_max_files,
            "scan_window_max_bytes": scan_window_max_bytes,
            "list_updates": bool(list_updates),
            "check_updates": bool(check_updates),
        }

        initiated_by = "srdf_capture_mariadb_binlog"
        cron_exec = None
        lock_obj = None
        owner_token = ""

        try:
            cron_exec = _create_cron_execution(
                replication_service=replication_service,
                command_line=command_line,
                input_params=input_params,
                initiated_by=initiated_by,
            )

            lock_obj, owner_token = _acquire_capture_cron_lock(replication_service)
            
            
            if not lock_obj:
                blocked_payload = {
                    "message": "Capture cron already running for this replication service",
                    "service_id": service_id,
                    "lock_name": _build_capture_cron_lock_name(service_id),
                }

                _finalize_cron_execution(
                    cron_exec=cron_exec,
                    status="blocked",
                    return_code="CAPTURE_BLOCKED_ALREADY_RUNNING",
                    error_code="CAPTURE_ALREADY_RUNNING",
                    error_message="Capture cron already running",
                    result_payload=blocked_payload,
                    progress_payload={},
                )


                self.stdout.write(
                    self.style.WARNING(
                        f"Capture blocked. service_id={service_id} return_code=CAPTURE_BLOCKED_ALREADY_RUNNING"
                    )
                )
                _exit_with_code(20)
            
            

            if list_updates:
                result = list_binlog_updates_since_checkpoint(
                    replication_service_id=service_id,
                    initiated_by="srdf_capture_mariadb_binlog:list_updates",
                    command_line=command_line,
                    limit=limit,
                    max_scanned_events=max_scanned_events,
                    max_runtime_seconds=max_runtime_seconds,
                    scan_window_max_files=scan_window_max_files,
                    scan_window_max_bytes=scan_window_max_bytes,
                    debug=debug,
                )
            elif check_updates:
                result = check_binlog_updates_vs_outbound(
                    replication_service_id=service_id,
                    initiated_by="srdf_capture_mariadb_binlog:check_updates",
                    command_line=command_line,
                    limit=limit,
                    max_scanned_events=max_scanned_events,
                    max_runtime_seconds=max_runtime_seconds,
                    scan_window_max_files=scan_window_max_files,
                    scan_window_max_bytes=scan_window_max_bytes,
                    debug=debug,
                )
            else:
                result = capture_binlog_once(
                    replication_service_id=service_id,
                    limit=limit,
                    initiated_by="srdf_capture_mariadb_binlog",
                    initialize_only=initialize_only,
                    force_current_pos=force_current_pos,
                    max_scanned_events=max_scanned_events,
                    max_runtime_seconds=max_runtime_seconds,
                    enable_filter_audit=enable_filter_audit,
                    disable_time_limit=disable_time_limit,
                    debug=debug,
                    verify_with_mysqlbinlog=verify_with_mysqlbinlog,
                    progress_enabled=progress_enabled,
                    progress_every_events=progress_every_events,
                    progress_every_seconds=progress_every_seconds,
                    scan_window_max_files=scan_window_max_files,
                    scan_window_max_bytes=scan_window_max_bytes,
                )

            status, return_code, error_code, error_message = _map_capture_result_to_cron_outcome(result)

            progress_payload = {}
            if isinstance(result, dict):
                progress_payload = {
                    "progress_enabled": result.get("progress_enabled"),
                    "progress_every_events": result.get("progress_every_events"),
                    "progress_every_seconds": result.get("progress_every_seconds"),
                    "progress_message_count": result.get("progress_message_count"),
                    "progress_snapshots": result.get("progress_snapshots") or [],
                }

            _finalize_cron_execution(
                cron_exec=cron_exec,
                status=status,
                return_code=return_code,
                error_code=error_code,
                error_message=error_message,
                result_payload=result if isinstance(result, dict) else {"raw_result": str(result)},
                progress_payload=progress_payload,
            )


            if result.get("initialized_only"):
                self.stdout.write(
                    self.style.WARNING(
                        f"Capture initialized only. service_id={result['replication_service_id']} "
                        f"log_file={result['log_file']} "
                        f"log_pos={result['log_pos']} "
                        f"return_code={return_code}"
                    )
                )
                _exit_with_code(_map_return_code_to_exit_code(return_code))
            
            

            if result.get("control_mode") == "list_updates":
                header_text = result.get("header_text") or ""
                table_text = result.get("table_text") or ""
                footer_text = result.get("footer_text") or ""

                if header_text:
                    self.stdout.write("")
                    self.stdout.write(header_text)
                    self.stdout.write("")

                if table_text:
                    self.stdout.write(table_text)
                    self.stdout.write("")

                if footer_text:
                    self.stdout.write(footer_text)
                    self.stdout.write("")

                style_fn = self.style.SUCCESS
                if int(result.get("total_not_captured") or 0) > 0:
                    style_fn = self.style.WARNING


                self.stdout.write(
                    style_fn(
                        f"List-updates finished. "
                        f"service_id={service_id} "
                        f"control_run_id={result.get('control_run_id')} "
                        f"return_code={return_code} "
                        f"total_binlog_updates={result.get('total_binlog_updates', 0)} "
                        f"captured_matches={result.get('total_captured_matches', 0)} "
                        f"not_captured={result.get('total_not_captured', 0)} "
                        f"duplicates={result.get('total_duplicates', 0)} "
                        f"ignored={result.get('total_ignored', 0)} "
                        f"stop_reason={result.get('stop_reason', '')} "
                        f"checkpoint_start={result.get('checkpoint_start_file', '')}:{result.get('checkpoint_start_pos', 0)} "
                        f"window_end={result.get('window_end_file', '')}:{result.get('window_end_pos', 0)}"
                    )
                )
                _exit_with_code(_map_return_code_to_exit_code(return_code))
            
            

            if result.get("control_mode") == "check_updates":
                header_text = result.get("header_text") or ""
                table_text = result.get("table_text") or ""
                footer_text = result.get("footer_text") or ""

                if header_text:
                    self.stdout.write("")
                    self.stdout.write(header_text)
                    self.stdout.write("")

                if table_text:
                    self.stdout.write(table_text)
                    self.stdout.write("")

                if footer_text:
                    self.stdout.write(footer_text)
                    self.stdout.write("")

                style_fn = self.style.SUCCESS
                if (
                    int(result.get("total_not_captured") or 0) > 0
                    or int(result.get("total_duplicates") or 0) > 0
                ):
                    style_fn = self.style.WARNING


                self.stdout.write(
                    style_fn(
                        f"Check-updates finished. "
                        f"service_id={service_id} "
                        f"control_run_id={result.get('control_run_id')} "
                        f"return_code={return_code} "
                        f"total_binlog_updates={result.get('total_binlog_updates', 0)} "
                        f"captured_matches={result.get('total_captured_matches', 0)} "
                        f"not_captured={result.get('total_not_captured', 0)} "
                        f"duplicates={result.get('total_duplicates', 0)} "
                        f"ignored={result.get('total_ignored', 0)} "
                        f"stop_reason={result.get('stop_reason', '')} "
                        f"checkpoint_start={result.get('checkpoint_start_file', '')}:{result.get('checkpoint_start_pos', 0)} "
                        f"window_end={result.get('window_end_file', '')}:{result.get('window_end_pos', 0)}"
                    )
                )
                _exit_with_code(_map_return_code_to_exit_code(return_code))
            
            

            style_fn = self.style.SUCCESS
            if status == "warning":
                style_fn = self.style.WARNING


            self.stdout.write(
                style_fn(
                    f"Capture finished. "
                    f"service_id={result['replication_service_id']} "
                    f"return_code={return_code} "
                    f"captured={result['captured_count']} "
                    f"skipped={result['skipped_count']} "
                    f"ignored_older={result['ignored_older_count']} "
                    f"scanned_events={result['scanned_event_count']} "
                    f"scanned_rows={result['scanned_row_count']} "
                    f"stop_reason={result['stop_reason']} "
                    f"elapsed={result['elapsed_seconds']}s "
                    f"log_file={result['log_file']} "
                    f"log_pos={result['log_pos']}"
                )
            )
            _exit_with_code(_map_return_code_to_exit_code(return_code))


        except Exception as exc:
            if cron_exec:
                _finalize_cron_execution(
                    cron_exec=cron_exec,
                    status="error",
                    return_code="CAPTURE_EXCEPTION",
                    error_code="CAPTURE_EXCEPTION",
                    error_message=str(exc),
                    result_payload={"error": str(exc)},
                    progress_payload={},
                )

            self.stdout.write(
                self.style.ERROR(
                    f"Capture failed. service_id={service_id} "
                    f"return_code=CAPTURE_EXCEPTION "
                    f"error={str(exc)}"
                )
            )
            _exit_with_code(40)
        

        finally:
            _release_capture_cron_lock(lock_obj, owner_token=owner_token)