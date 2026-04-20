#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json
import uuid

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.models import (
    AuditEvent,
    OutboundChangeEvent,
    ReplicationService,
    SRDFCronExecution,
    SRDFServiceExecutionLock,
    TransportBatch,
)


from srdf.services.dedup_engine import run_deduplicate_once
from srdf.services.transport_batch_builder import build_transport_batch_once

from srdf.services.transport_sender import (
    send_transport_batch_once,
    resume_failed_batch_once,
    hard_reset_transport_state_once,
)


CRON_NAME = "srdf_transport"
CRON_TYPE = "shipper"


def _build_transport_command_line(
    action,
    service_id=None,
    batch_id=None,
    limit=None,
    strict_send=False,
    reset_source_binlog=False,
    confirm_reset_source_binlog=False,
):
    parts = [
        "python",
        "manage.py",
        CRON_NAME,
        f"--action={action}",
    ]

    if service_id is not None:
        parts.append(f"--service-id={service_id}")

    if batch_id is not None:
        parts.append(f"--batch-id={batch_id}")

    if limit is not None:
        parts.append(f"--limit={limit}")

    if strict_send:
        parts.append("--strict-send")

    if reset_source_binlog:
        parts.append("--reset-source-binlog")

    if confirm_reset_source_binlog:
        parts.append("--confirm-reset-source-binlog")

    return " ".join(parts)


def _resolve_replication_service_for_transport(action, service_id=None, batch_id=None):
    if service_id:
        return (
            ReplicationService.objects
            .select_related("source_node", "target_node")
            .filter(id=service_id)
            .first()
        )

    if batch_id:
        batch = (
            TransportBatch.objects
            .select_related("replication_service__source_node", "replication_service__target_node")
            .filter(id=batch_id)
            .first()
        )
        if batch:
            return batch.replication_service

    return None


def _create_transport_cron_execution(replication_service, command_line, input_params, initiated_by):
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


def _finalize_transport_cron_execution(
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


def _map_transport_result_to_cron_outcome(action, result):
    result = result or {}

    if result.get("ok"):
        message = str(result.get("message") or "").lower()

        if result.get("skipped"):
            return "warning", f"TRANSPORT_{action.upper()}_SKIPPED", "", ""

        if action == "send_batch":
            batch_status = str((result.get("transport_batch") or {}).get("status") or "").lower()
            if batch_status == "failed":
                return "warning", "TRANSPORT_SEND_BATCH_FAILED_ACCEPTED", "", ""

        if action == "cycle" and not result.get("ok"):
            return "error", "TRANSPORT_CYCLE_ERROR", "TRANSPORT_CYCLE_ERROR", result.get("message") or ""

        return "success", f"TRANSPORT_{action.upper()}_OK", "", ""

    return (
        "error",
        f"TRANSPORT_{action.upper()}_ERROR",
        f"TRANSPORT_{action.upper()}_ERROR",
        result.get("message") or "transport action failed",
    )



def _build_transport_lock_name(action, service_id):
    action = str(action or "").strip().lower()
    return f"srdf_transport:{action}:service_{int(service_id)}"


def _acquire_transport_lock(replication_service, action):
    lock_name = _build_transport_lock_name(action, replication_service.id)

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


def _release_transport_lock(lock_obj, owner_token=""):
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



class Command(BaseCommand):
    help = "SRDF transport pilot command: build_batch, send_batch, cycle"

    def add_arguments(self, parser):
        parser.add_argument(
            "--action",
            type=str,
            required=True,
            choices=["deduplicate", "build_batch", "send_batch", "resume_batch", "hard_reset", "cycle"],
            help="Transport action to execute",
        )
        parser.add_argument(
            "--service-id",
            type=int,
            default=None,
            help="ReplicationService ID for build_batch or cycle",
        )
        parser.add_argument(
            "--batch-id",
            type=int,
            default=None,
            help="TransportBatch ID for send_batch",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Maximum number of outbound events for build_batch",
        )
        parser.add_argument(
            "--strict-send",
            action="store_true",
            help="In cycle mode, fail if send does not end with sent status",
        )
        parser.add_argument(
            "--reset-source-binlog",
            action="store_true",
            help="With hard_reset only: also execute RESET MASTER on source DB",
        )
        parser.add_argument(
            "--confirm-reset-source-binlog",
            action="store_true",
            help="Required together with --reset-source-binlog",
        )        

    def handle(self, *args, **options):
        action = (options.get("action") or "").strip()
        service_id = options.get("service_id")
        batch_id = options.get("batch_id")
        limit = int(options.get("limit") or 100)
        strict_send = bool(options.get("strict_send"))
        reset_source_binlog = bool(options.get("reset_source_binlog"))
        confirm_reset_source_binlog = bool(options.get("confirm_reset_source_binlog"))

        started_at = timezone.now()
        initiated_by = "srdf_transport"

        replication_service = _resolve_replication_service_for_transport(
            action=action,
            service_id=service_id,
            batch_id=batch_id,
        )

        command_line = _build_transport_command_line(
            action=action,
            service_id=service_id,
            batch_id=batch_id,
            limit=limit,
            strict_send=strict_send,
            reset_source_binlog=reset_source_binlog,
            confirm_reset_source_binlog=confirm_reset_source_binlog,
        )

        input_params = {
            "action": action,
            "service_id": service_id,
            "batch_id": batch_id,
            "limit": limit,
            "strict_send": strict_send,
            "reset_source_binlog": reset_source_binlog,
            "confirm_reset_source_binlog": confirm_reset_source_binlog,
        }

        cron_exec = _create_transport_cron_execution(
            replication_service=replication_service,
            command_line=command_line,
            input_params=input_params,
            initiated_by=initiated_by,
        )

        lock_obj = None
        owner_token = ""

        result = {}
        return_code = ""
        error_code = ""
        error_message = ""
        final_status = "success"

        try:
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write(self.style.WARNING("SRDF TRANSPORT COMMAND - START"))
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write(f"action={action}")
            self.stdout.write(f"service_id={service_id}")
            self.stdout.write(f"batch_id={batch_id}")
            self.stdout.write(f"limit={limit}")
            self.stdout.write(f"strict_send={strict_send}")
            self.stdout.write("")

            if replication_service:
                lock_obj, owner_token = _acquire_transport_lock(
                    replication_service=replication_service,
                    action=action,
                )

                if not lock_obj:
                    result = {
                        "ok": False,
                        "message": f"Transport action already running for service '{replication_service.name}'",
                        "action": action,
                        "service_id": replication_service.id,
                        "lock_name": _build_transport_lock_name(action, replication_service.id),
                        "blocked": True,
                    }
                    final_status = "blocked"
                    return_code = f"TRANSPORT_{action.upper()}_BLOCKED_ALREADY_RUNNING"
                    error_code = "TRANSPORT_ALREADY_RUNNING"
                    error_message = result["message"]

                    self.stdout.write(
                        self.style.WARNING(
                            f"[LOCK][BLOCKED] name={result['lock_name']}"
                        )
                    )

            if final_status != "blocked":
                if action == "deduplicate":
                    result = self._handle_deduplicate(service_id=service_id, limit=limit)

                elif action == "build_batch":
                    result = self._handle_build_batch(service_id=service_id, limit=limit)

                elif action == "send_batch":
                    result = self._handle_send_batch(batch_id=batch_id)

                elif action == "resume_batch":
                    result = self._handle_resume_batch(batch_id=batch_id)

                elif action == "hard_reset":
                    result = self._handle_hard_reset(
                        service_id=service_id,
                        reset_source_binlog=reset_source_binlog,
                        confirm_reset_source_binlog=confirm_reset_source_binlog,
                        preserve_cron_execution_id=cron_exec.id,
                    )

                elif action == "cycle":
                    result = self._handle_cycle(
                        service_id=service_id,
                        limit=limit,
                        strict_send=strict_send,
                    )

                else:
                    result = {
                        "ok": False,
                        "message": f"Unsupported action '{action}'",
                        "action": action,
                    }

                final_status, return_code, error_code, error_message = _map_transport_result_to_cron_outcome(
                    action=action,
                    result=result,
                )

        except Exception as exc:
            result = {
                "ok": False,
                "message": str(exc),
                "action": action,
                "service_id": service_id,
                "batch_id": batch_id,
            }
            final_status = "error"
            return_code = f"TRANSPORT_{action.upper()}_EXCEPTION" if action else "TRANSPORT_EXCEPTION"
            error_code = return_code
            error_message = str(exc)

        finally:
            _release_transport_lock(lock_obj, owner_token=owner_token)

        finished_at = timezone.now()
        duration_seconds = round((finished_at - started_at).total_seconds(), 3)

        _finalize_transport_cron_execution(
            cron_exec=cron_exec,
            status=final_status,
            return_code=return_code,
            error_code=error_code,
            error_message=error_message,
            result_payload=result if isinstance(result, dict) else {"raw_result": str(result)},
            progress_payload={
                "action": action,
                "service_id": service_id,
                "batch_id": batch_id,
                "duration_seconds": duration_seconds,
            },
        )

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF TRANSPORT COMMAND - FINAL SUMMARY"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"ok={result.get('ok')}")
        self.stdout.write(f"message={result.get('message')}")
        self.stdout.write(f"return_code={return_code}")
        self.stdout.write(f"duration_seconds={duration_seconds}")
        self.stdout.write("")
        self.stdout.write("JSON_RESULT_BEGIN")
        self.stdout.write(json.dumps(result, indent=2, ensure_ascii=False))
        self.stdout.write("JSON_RESULT_END")
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF TRANSPORT COMMAND - END"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write("")

        if not result.get("ok"):
            raise CommandError(result.get("message") or "SRDF transport command failed")




    # -----------------------------------------------------------------
    # ACTIONS
    # -----------------------------------------------------------------


    def _handle_deduplicate(self, service_id, limit):
        if not service_id:
            raise CommandError("--service-id is required for --action deduplicate")

        service = self._get_service(service_id)

        pending_before = self._count_pending_events(service)
        dead_before = self._count_dead_events(service)

        self.stdout.write(self.style.WARNING("[DEDUP] Starting deduplicate"))
        self.stdout.write(f"[DEDUP] service={service.name} id={service.id}")
        self.stdout.write(f"[DEDUP] pending_before={pending_before}")
        self.stdout.write(f"[DEDUP] dead_before={dead_before}")

        result = run_deduplicate_once(
            replication_service_id=service.id,
            limit=max(int(limit or 100), 1),
            initiated_by="srdf_transport:deduplicate",
        )

        pending_after = self._count_pending_events(service)
        dead_after = self._count_dead_events(service)

        processed_count = int(result.get("processed_count") or 0)
        survivor_count = int(result.get("survivor_count") or 0)
        duplicate_count = int(result.get("duplicate_count") or 0)
        exact_duplicate_count = int(result.get("exact_duplicate_count") or 0)
        neutralized_count = int(result.get("neutralized_count") or 0)
        collapsed_count = int(result.get("collapsed_count") or 0)
        skipped = bool(result.get("skipped"))

        self.stdout.write(f"[DEDUP] pending_after={pending_after}")
        self.stdout.write(f"[DEDUP] dead_after={dead_after}")
        self.stdout.write(f"[DEDUP] processed_count={processed_count}")
        self.stdout.write(f"[DEDUP] survivor_count={survivor_count}")
        self.stdout.write(f"[DEDUP] duplicate_count={duplicate_count}")
        self.stdout.write(f"[DEDUP] exact_duplicate_count={exact_duplicate_count}")
        self.stdout.write(f"[DEDUP] neutralized_count={neutralized_count}")
        self.stdout.write(f"[DEDUP] collapsed_count={collapsed_count}")

        if skipped:
            self.stdout.write(self.style.WARNING("[DEDUP] no pending DML events to deduplicate"))
        else:
            self.stdout.write(
                self.style.SUCCESS(
                    f"[DEDUP][OK] dedup_run_id={result.get('dedup_run_id')} "
                    f"survivors={survivor_count} duplicates={duplicate_count}"
                )
            )

        return {
            "ok": True,
            "message": result.get("message") or "deduplicate completed",
            "action": "deduplicate",
            "service_id": service.id,
            "service_name": service.name,
            "pending_before": pending_before,
            "pending_after": pending_after,
            "dead_before": dead_before,
            "dead_after": dead_after,
            "processed_count": processed_count,
            "survivor_count": survivor_count,
            "duplicate_count": duplicate_count,
            "exact_duplicate_count": exact_duplicate_count,
            "neutralized_count": neutralized_count,
            "collapsed_count": collapsed_count,
            "dedup_run_id": result.get("dedup_run_id"),
            "duplicate_event_ids_preview": result.get("duplicate_event_ids_preview") or [],
            "survivor_event_ids_preview": result.get("survivor_event_ids_preview") or [],
            "neutralized_event_ids_preview": result.get("neutralized_event_ids_preview") or [],
            "collapsed_event_ids_preview": result.get("collapsed_event_ids_preview") or [],
            "dedup_result": result,
        }
    
    





    def _handle_build_batch(self, service_id, limit):
        if not service_id:
            raise CommandError("--service-id is required for --action build_batch")

        service = self._get_service(service_id)
        pending_before = self._count_pending_events(service)
        batch_count_before = self._count_batches(service)

        self.stdout.write(self.style.WARNING("[BUILD] Starting build_batch"))
        self.stdout.write(f"[BUILD] service={service.name} id={service.id}")
        self.stdout.write(f"[BUILD] pending_before={pending_before}")
        self.stdout.write(f"[BUILD] batch_count_before={batch_count_before}")

        result = build_transport_batch_once(
            replication_service_id=service.id,
            limit=limit,
            initiated_by="srdf_transport:build_batch",
        )

        batch = None
        batch_id = result.get("transport_batch_id")
        if batch_id:
            batch = TransportBatch.objects.filter(id=batch_id).first()

        pending_after = self._count_pending_events(service)
        batched_after = self._count_batched_events(service)
        batch_count_after = self._count_batches(service)

        self.stdout.write(f"[BUILD] pending_after={pending_after}")
        self.stdout.write(f"[BUILD] batched_after={batched_after}")
        self.stdout.write(f"[BUILD] batch_count_after={batch_count_after}")

        if batch:
            self.stdout.write(
                self.style.SUCCESS(
                    f"[BUILD][OK] batch_id={batch.id} batch_uid={batch.batch_uid} "
                    f"status={batch.status} event_count={batch.event_count} checksum={batch.checksum}"
                )
            )
        else:
            self.stdout.write(self.style.WARNING("[BUILD] no batch created"))

        return {
            "ok": True,
            "message": result.get("message") or "build_batch completed",
            "action": "build_batch",
            "service_id": service.id,
            "service_name": service.name,
            "pending_before": pending_before,
            "pending_after": pending_after,
            "batched_after": batched_after,
            "batch_count_before": batch_count_before,
            "batch_count_after": batch_count_after,
            "build_result": result,
            "transport_batch": {
                "id": batch.id if batch else None,
                "batch_uid": batch.batch_uid if batch else "",
                "status": batch.status if batch else None,
                "event_count": batch.event_count if batch else 0,
                "first_event_id": batch.first_event_id if batch else None,
                "last_event_id": batch.last_event_id if batch else None,
                "checksum": batch.checksum if batch else "",
            },
        }

    def _handle_send_batch(self, batch_id):
        if not batch_id:
            raise CommandError("--batch-id is required for --action send_batch")

        batch = self._get_batch(batch_id)

        self.stdout.write(self.style.WARNING("[SEND] Starting send_batch"))
        self.stdout.write(f"[SEND] batch_id={batch.id}")
        self.stdout.write(f"[SEND] batch_uid={batch.batch_uid}")
        self.stdout.write(f"[SEND] current_status={batch.status}")
        self.stdout.write(f"[SEND] target_node={batch.replication_service.target_node.name}")

        send_exception = None
        send_result = None


        try:
            send_result = send_transport_batch_once(
                batch_id=batch.id,
                initiated_by="srdf_transport:send_batch",
            )
        except Exception as exc:
            send_exception = str(exc)

        batch.refresh_from_db()

        if isinstance(send_result, dict) and not send_result.get("ok"):
            send_exception = send_result.get("message") or send_exception
        
        

        if isinstance(send_result, dict) and not send_result.get("ok"):
            send_exception = send_result.get("message") or send_exception        
        

        self.stdout.write(f"[SEND] status_after={batch.status}")
        self.stdout.write(f"[SEND] retry_count={batch.retry_count}")
        self.stdout.write(f"[SEND] last_error={batch.last_error or '-'}")

        ok = batch.status in ("sent", "acked", "failed")



        if batch.status == "sent":
            self.stdout.write(
                self.style.SUCCESS(
                    f"[SEND][OK] batch_id={batch.id} sent_at={batch.sent_at}"
                )
            )
        elif batch.status == "acked":
            self.stdout.write(
                self.style.SUCCESS(
                    f"[SEND][OK] batch_id={batch.id} acked_at={batch.acked_at}"
                )
            )
        elif batch.status == "failed":
            self.stdout.write(
                self.style.WARNING(
                    f"[SEND][FAILED-ACCEPTED] batch_id={batch.id} error={batch.last_error}"
                )
            )
        else:
            self.stdout.write(
                self.style.ERROR(
                    f"[SEND][FAIL] batch_id={batch.id} unexpected_status={batch.status}"
                )
            )            
            

        return {
            "ok": ok,
            "message": "send_batch completed" if ok else "send_batch failed",
            "action": "send_batch",
            "batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "send_result": send_result or {},
            "send_exception": send_exception,
            "transport_batch": {
                "id": batch.id,
                "status": batch.status,
                "retry_count": batch.retry_count,
                "last_error": batch.last_error,
                "sent_at": batch.sent_at.isoformat() if batch.sent_at else None,
                "acked_at": batch.acked_at.isoformat() if batch.acked_at else None,
            },
            "return_hint": "warning" if batch.status == "failed" else "success",
        }    
    
    def _handle_resume_batch(self, batch_id):
        if not batch_id:
            raise CommandError("--batch-id is required for --action resume_batch")

        batch = self._get_batch(batch_id)

        self.stdout.write(self.style.WARNING("[RESUME] Starting resume_batch"))
        self.stdout.write(f"[RESUME] batch_id={batch.id}")
        self.stdout.write(f"[RESUME] batch_uid={batch.batch_uid}")
        self.stdout.write(f"[RESUME] current_status={batch.status}")

        result = resume_failed_batch_once(
            batch_id=batch.id,
            initiated_by="srdf_transport:resume_batch",
        )

        pending_after = self._count_pending_events(batch.replication_service)

        self.stdout.write(f"[RESUME] pending_after={pending_after}")
        self.stdout.write(
            self.style.SUCCESS(
                f"[RESUME][OK] batch_id={batch.id} requeued_events={result.get('requeued_events', 0)}"
            )
        )

        return {
            "ok": True,
            "message": result.get("message") or "resume_batch completed",
            "action": "resume_batch",
            "batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "pending_after": pending_after,
            "resume_result": result,
        }
    

    def _handle_hard_reset(self, 
                           service_id, 
                           reset_source_binlog=False, 
                           confirm_reset_source_binlog=False,
                           preserve_cron_execution_id=0,
                           ):
        if not service_id:
            raise CommandError("--service-id is required for --action hard_reset")

        service = self._get_service(service_id)

        self.stdout.write(self.style.WARNING("[HARD_RESET] Starting hard_reset"))
        self.stdout.write(f"[HARD_RESET] service={service.name} id={service.id}")
        self.stdout.write(f"[HARD_RESET] reset_source_binlog={reset_source_binlog}")
        self.stdout.write(f"[HARD_RESET] confirm_reset_source_binlog={confirm_reset_source_binlog}")

        result = hard_reset_transport_state_once(
            replication_service_id=service.id,
            reset_source_binlog=reset_source_binlog,
            confirm_reset_source_binlog=confirm_reset_source_binlog,
            initiated_by="srdf_transport:hard_reset",
            preserve_cron_execution_id=preserve_cron_execution_id,
        )

        self.stdout.write(
            self.style.SUCCESS(
                f"[HARD_RESET][OK] "
                f"outbound_deleted={result.get('outbound_deleted', 0)} "
                f"inbound_deleted={result.get('inbound_deleted', 0)} "
                f"batches_deleted={result.get('batches_deleted', 0)} "
                f"checkpoints_deleted={result.get('checkpoints_deleted', 0)} "
                f"capture_control_runs_deleted={result.get('capture_control_runs_deleted', 0)} "
                f"capture_control_items_deleted={result.get('capture_control_items_deleted', 0)} "
                f"phase_executions_deleted={result.get('phase_executions_deleted', 0)} "
                f"cron_executions_deleted={result.get('cron_executions_deleted', 0)} "
                f"dedup_runs_deleted={result.get('dedup_runs_deleted', 0)} "
                f"dedup_decisions_deleted={result.get('dedup_decisions_deleted', 0)} "
                f"audit_events_deleted={result.get('audit_events_deleted', 0)} "
                f"binlog_reset_done={result.get('binlog_reset_done', False)}"
            )
        )

        return {
            "ok": True,
            "message": result.get("message") or "hard_reset completed",
            "action": "hard_reset",
            "service_id": service.id,
            "service_name": service.name,
            "hard_reset_result": result,
        }    

    def _handle_cycle(self, service_id, limit, strict_send=False):
        if not service_id:
            raise CommandError("--service-id is required for --action cycle")

        service = self._get_service(service_id)

        self.stdout.write(self.style.WARNING("[CYCLE] Starting cycle"))
        self.stdout.write(f"[CYCLE] service={service.name} id={service.id}")

        pending_before = self._count_pending_events(service)
        self.stdout.write(f"[CYCLE] pending_before={pending_before}")

        build_result = build_transport_batch_once(
            replication_service_id=service.id,
            limit=limit,
            initiated_by="srdf_transport:cycle:build_batch",
        )

        batch_id = build_result.get("transport_batch_id")
        batch = TransportBatch.objects.filter(id=batch_id).first() if batch_id else None

        if batch:
            self.stdout.write(
                self.style.SUCCESS(
                    f"[CYCLE][BUILD] batch_id={batch.id} status={batch.status} event_count={batch.event_count}"
                )
            )
        else:
            self.stdout.write(self.style.WARNING("[CYCLE][BUILD] no batch created"))

        send_result = None
        send_exception = None

        if batch:
            try:
                send_result = send_transport_batch_once(
                    batch_id=batch.id,
                    initiated_by="srdf_transport:cycle:send_batch",
                )
            except Exception as exc:
                send_exception = str(exc)
                batch.refresh_from_db()

            batch.refresh_from_db()
            self.stdout.write(
                f"[CYCLE][SEND] batch_id={batch.id} status_after_send={batch.status} "
                f"retry_count={batch.retry_count} last_error={batch.last_error or '-'}"
            )

        pending_after = self._count_pending_events(service)
        batched_after = self._count_batched_events(service)
        sent_after = self._count_batches_by_status(service, "sent")
        failed_after = self._count_batches_by_status(service, "failed")

        ok = True
        if batch:
            ok = batch.status in ("sent", "failed")
            if strict_send:
                ok = batch.status == "sent"

        if batch is None and build_result.get("batch_created") is False:
            ok = True

        message = "cycle completed"
        if strict_send and batch and batch.status != "sent":
            message = "cycle completed but strict_send failed"

        self.stdout.write(f"[CYCLE] pending_after={pending_after}")
        self.stdout.write(f"[CYCLE] batched_after={batched_after}")
        self.stdout.write(f"[CYCLE] sent_after={sent_after}")
        self.stdout.write(f"[CYCLE] failed_after={failed_after}")

        if ok:
            self.stdout.write(self.style.SUCCESS(f"[CYCLE][OK] {message}"))
        else:
            self.stdout.write(self.style.ERROR(f"[CYCLE][FAIL] {message}"))

        return {
            "ok": ok,
            "message": message,
            "action": "cycle",
            "service_id": service.id,
            "service_name": service.name,
            "pending_before": pending_before,
            "pending_after": pending_after,
            "batched_after": batched_after,
            "sent_after": sent_after,
            "failed_after": failed_after,
            "build_result": build_result,
            "send_result": send_result or {},
            "send_exception": send_exception,
            "transport_batch": {
                "id": batch.id if batch else None,
                "batch_uid": batch.batch_uid if batch else "",
                "status": batch.status if batch else None,
                "retry_count": batch.retry_count if batch else 0,
                "last_error": batch.last_error if batch else "",
                "sent_at": batch.sent_at.isoformat() if batch and batch.sent_at else None,
            },
        }

    # -----------------------------------------------------------------
    # HELPERS
    # -----------------------------------------------------------------

    def _get_service(self, service_id):
        service = (
            ReplicationService.objects
            .select_related("source_node", "target_node")
            .filter(id=service_id, is_enabled=True)
            .first()
        )
        if not service:
            raise CommandError(f"Enabled ReplicationService #{service_id} not found")
        return service

    def _get_batch(self, batch_id):
        batch = (
            TransportBatch.objects
            .select_related("replication_service__target_node", "replication_service__source_node")
            .filter(id=batch_id)
            .first()
        )
        if not batch:
            raise CommandError(f"TransportBatch #{batch_id} not found")
        return batch

    def _count_pending_events(self, service):
        return (
            OutboundChangeEvent.objects
            .filter(replication_service=service, status="pending")
            .count()
        )
    
    def _count_dead_events(self, service):
        return (
            OutboundChangeEvent.objects
            .filter(replication_service=service, status="dead")
            .count()
        )    

    def _count_batched_events(self, service):
        return (
            OutboundChangeEvent.objects
            .filter(replication_service=service, status="batched")
            .count()
        )

    def _count_batches(self, service):
        return TransportBatch.objects.filter(replication_service=service).count()

    def _count_batches_by_status(self, service, status):
        return (
            TransportBatch.objects
            .filter(replication_service=service, status=status)
            .count()
        )