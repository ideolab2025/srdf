#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import uuid

from django.utils import timezone

from srdf.models import (
    SRDFCronExecution,
    SRDFServiceExecutionLock,
)

from srdf.services.binlog_capture import capture_binlog_once


CRON_NAME = "srdf_capture_mariadb_binlog"
CRON_TYPE = "capture"


def _build_capture_cron_lock_name(service_id):
    return f"srdf_capture_cron:service_{int(service_id)}"


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

    if result.get("initialized_only"):
        return "success", "CAPTURE_INIT_ONLY", "", ""

    stop_reason = (result.get("stop_reason") or "").strip().lower()
    captured_count = int(result.get("captured_count") or 0)
    skipped_count = int(result.get("skipped_count") or 0)

    if stop_reason == "no_events_available":
        return "success", "CAPTURE_NO_EVENTS", "", ""

    if stop_reason in (
        "capture_limit_reached",
        "max_scanned_events_reached",
        "scan_window_limit_reached",
    ):
        if captured_count > 0 and skipped_count > 0:
            return "warning", "CAPTURE_WARNING", "", ""
        if captured_count > 0:
            return "success", "CAPTURE_OK_PARTIAL", "", ""
        if skipped_count > 0:
            return "warning", "CAPTURE_WARNING_SKIPPED_ONLY", "", ""
        return "success", "CAPTURE_OK_PARTIAL", "", ""

    if captured_count > 0 and skipped_count > 0:
        return "warning", "CAPTURE_WARNING", "", ""

    if captured_count > 0:
        return "success", "CAPTURE_OK", "", ""

    if skipped_count > 0:
        return "warning", "CAPTURE_WARNING_SKIPPED_ONLY", "", ""

    if result.get("ok"):
        return "success", "CAPTURE_OK", "", ""

    return "error", "CAPTURE_ERROR", "CAPTURE_ERROR", "Capture returned a non-success result"



def _build_orchestrator_capture_command_line(
    service_id,
    limit=None,
    max_scanned_events=None,
    max_runtime_seconds=None,
    enable_filter_audit=None,
    disable_time_limit=None,
    debug=None,
    verify_with_mysqlbinlog=None,
    progress_enabled=None,
    progress_every_events=None,
    progress_every_seconds=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
):
    parts = [
        "python",
        "manage.py",
        CRON_NAME,
        f"--service-id={service_id}",
    ]

    if limit is not None:
        parts.append(f"--limit={limit}")
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

    return " ".join(parts)


def capture_changes(
    service,
    limit=None,
    max_scanned_events=None,
    max_runtime_seconds=None,
    enable_filter_audit=None,
    disable_time_limit=None,
    debug=None,
    verify_with_mysqlbinlog=None,
    progress_enabled=None,
    progress_every_events=None,
    progress_every_seconds=None,
    scan_window_max_files=None,
    scan_window_max_bytes=None,
    initiated_by="orchestrator:capture",
    ):
    """
    Couche de raccord utilisée par l'orchestrator.
    Exécute la capture réelle, crée SRDFCronExecution, gère le lock et
    retourne un dict normalisé pour SRDFOrchestrator._execute_phase().
    """
    command_line = _build_orchestrator_capture_command_line(
        service_id=service.id,
        limit=limit,
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

    input_params = {
        "service_id": service.id,
        "limit": limit,
        "max_scanned_events": max_scanned_events,
        "max_runtime_seconds": max_runtime_seconds,
        "enable_filter_audit": bool(enable_filter_audit) if enable_filter_audit is not None else None,
        "disable_time_limit": bool(disable_time_limit) if disable_time_limit is not None else None,
        "debug": bool(debug) if debug is not None else None,
        "verify_with_mysqlbinlog": bool(verify_with_mysqlbinlog) if verify_with_mysqlbinlog is not None else None,
        "progress_enabled": bool(progress_enabled) if progress_enabled is not None else None,
        "progress_every_events": progress_every_events,
        "progress_every_seconds": progress_every_seconds,
        "scan_window_max_files": scan_window_max_files,
        "scan_window_max_bytes": scan_window_max_bytes,
    }

    cron_exec = None
    lock_obj = None
    owner_token = ""

    try:
        cron_exec = _create_cron_execution(
            replication_service=service,
            command_line=command_line,
            input_params=input_params,
            initiated_by=initiated_by,
        )

        lock_obj, owner_token = _acquire_capture_cron_lock(service)
        if not lock_obj:
            blocked_payload = {
                "message": "Capture cron already running for this replication service",
                "service_id": service.id,
                "lock_name": _build_capture_cron_lock_name(service.id),
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

            return {
                "ok": True,
                "status": "retry",
                "return_code": "CAPTURE_BLOCKED_ALREADY_RUNNING",
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": 1,
                "retry_after_seconds": 15,
                "result_payload": blocked_payload,
            }

        result = capture_binlog_once(
            replication_service_id=service.id,
            limit=limit,
            initiated_by=initiated_by,
            initialize_only=False,
            force_current_pos=False,
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

        captured_count = int(result.get("captured_count") or 0)
        skipped_count = int(result.get("skipped_count") or 0)



        normalized_status = "success"
        retry_after_seconds = 0

        if return_code == "CAPTURE_BLOCKED_ALREADY_RUNNING":
            normalized_status = "retry"
            retry_after_seconds = 15

        elif return_code == "CAPTURE_WARNING_SKIPPED_ONLY":
            normalized_status = "retry"
            retry_after_seconds = 15

        elif return_code == "CAPTURE_WARNING":
            if captured_count > 0:
                normalized_status = "success"
                retry_after_seconds = 0
            else:
                normalized_status = "retry"
                retry_after_seconds = 15

        elif return_code in (
            "CAPTURE_EXCEPTION",
            "CAPTURE_ERROR",
        ):
            normalized_status = "failed"
            
            

        return {
            "ok": normalized_status != "failed",
            "status": normalized_status,
            "return_code": return_code,
            "processed": captured_count + skipped_count,
            "success": captured_count,
            "failed": 0 if normalized_status != "failed" else 1,
            "skipped": skipped_count,
            "retry_after_seconds": retry_after_seconds,
            "result_payload": {
                "cron_execution_id": cron_exec.id,
                "cron_status": status,
                "capture_result": result,
            },
        }

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

        return {
            "ok": False,
            "status": "failed",
            "return_code": "CAPTURE_EXCEPTION",
            "processed": 0,
            "success": 0,
            "failed": 1,
            "skipped": 0,
            "retry_after_seconds": 0,
            "result_payload": {
                "error": str(exc),
            },
        }

    finally:
        _release_capture_cron_lock(lock_obj, owner_token=owner_token)