#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import time
import traceback
import uuid

from dataclasses import dataclass

from django.db import transaction
from django.utils import timezone

from srdf.models import (
    AuditEvent,
    BootstrapPlan,
    ExecutionLock,
    ReplicationService,
    SRDFArchiveRun,
    SRDFOrchestratorRun,
    SRDFServiceExecutionLock,
    SRDFServicePhaseExecution,
    SRDFServiceRuntimeState,
)
from srdf.models import InboundChangeEvent, TransportBatch
from srdf.models import TransportBatch, OutboundChangeEvent

from srdf.services.archive_engine          import run_archive_once
from srdf.services.bootstrap_plan_runner   import run_bootstrap_plan_once
from srdf.services.capture_engine          import capture_changes
from srdf.services.dedup_engine            import run_deduplicate_once
from srdf.services.transport_applier       import apply_inbound_events_once
from srdf.services.node_api_client         import run_remote_action
from srdf.services.transport_batch_builder import build_transport_batch_once
from srdf.services.transport_sender        import send_transport_batch_once, resume_failed_batch_once
from srdf.services.transport_payload_codec import (
    build_logical_payload,
    encode_transport_payload,
)

RETURN_OK      = "ok"
RETURN_EMPTY   = "empty"
RETURN_RETRY   = "retry"
RETURN_FAILED  = "failed"
RETURN_STOPPED = "stopped"
RETURN_SKIPPED = "skipped"


@dataclass
class PhaseResult:
    status: str
    return_code: str
    next_phase: str = ""
    processed_count: int = 0
    success_count: int = 0
    failed_count: int = 0
    skipped_count: int = 0
    retry_after_seconds: int = 0
    error_message: str = ""
    payload: dict = None

    def to_dict(self):
        return {
            "status": self.status,
            "return_code": self.return_code,
            "next_phase": self.next_phase,
            "processed_count": int(self.processed_count or 0),
            "success_count": int(self.success_count or 0),
            "failed_count": int(self.failed_count or 0),
            "skipped_count": int(self.skipped_count or 0),
            "retry_after_seconds": int(self.retry_after_seconds or 0),
            "error_message": self.error_message or "",
            "payload": self.payload or {},
        }


class SRDFOrchestratorError(Exception):
    pass


class SRDFOrchestrator:
    PHASE_CAPTURE = "capture"
    PHASE_DEDUPLICATE = "deduplicate"
    PHASE_COMPRESS = "compress"
    PHASE_ENCRYPT = "encrypt"
    PHASE_BATCH_BUILD = "batch_build"
    PHASE_SHIP = "ship"
    PHASE_WAIT_ACK = "wait_ack"
    PHASE_RECEIVE = "receive"
    PHASE_APPLY = "apply"
    PHASE_CHECKPOINT = "checkpoint"
    PHASE_BOOTSTRAP = "bootstrap"
    PHASE_IDLE = "idle"
    PHASE_ERROR = "error"
    PHASE_PAUSED = "paused"

    DEFAULT_PHASE_ORDER = [
        PHASE_CAPTURE,
        PHASE_DEDUPLICATE,
        PHASE_COMPRESS,
        PHASE_ENCRYPT,
        PHASE_BATCH_BUILD,
        PHASE_SHIP,
        PHASE_WAIT_ACK,
        PHASE_RECEIVE,
        PHASE_APPLY,
        PHASE_CHECKPOINT,
        PHASE_BOOTSTRAP,
    ]

    def __init__(
        self,
        sleep_seconds=10,
        service_names=None,
        initiated_by="srdf_daemon",
        stop_on_service_error=False,
        progress_callback=None,
        ):
        self.sleep_seconds = int(sleep_seconds or 10)
        self.service_names = service_names or []
        self.initiated_by = initiated_by or "srdf_daemon"
        self.stop_on_service_error = bool(stop_on_service_error)
        self.progress_callback = progress_callback
        
        
    def _build_pending_transport_payload_events(self, service, limit=100):
        limit = max(int(limit or 100), 1)

        pending_events = list(
            OutboundChangeEvent.objects
            .filter(
                replication_service=service,
                status="pending",
            )
            .order_by("id")[:limit]
        )

        payload_events = []
        for event in pending_events:
            payload_events.append({
                "id": event.id,
                "event_uid": event.event_uid or "",
                "transaction_id": event.transaction_id or "",
                "engine": event.engine or "",
                "event_family": event.event_family or "dml",
                "database_name": event.database_name or "",
                "table_name": event.table_name or "",
                "operation": event.operation or "",
                "object_type": event.object_type or "",
                "ddl_action": event.ddl_action or "",
                "object_name": event.object_name or "",
                "parent_object_name": event.parent_object_name or "",
                "raw_sql": event.raw_sql or "",
                "log_file": event.log_file or "",
                "log_pos": int(event.log_pos or 0),
                "primary_key_data": event.primary_key_data or {},
                "before_data": event.before_data or {},
                "after_data": event.after_data or {},
                "event_payload": event.event_payload or {},
                "created_at": event.created_at.isoformat() if event.created_at else None,
            })

        return pending_events, payload_events


    def _get_transport_codec_settings(self, service):
        config = service.config or {}
        if not isinstance(config, dict):
            config = {}

        compression = str(config.get("transport_compression") or "zlib").strip().lower()
        encryption = str(config.get("transport_encryption") or "xor").strip().lower()
        encryption_key = str(config.get("transport_encryption_key") or "").strip()

        return {
            "compression": compression or "zlib",
            "encryption": encryption or "xor",
            "encryption_key": encryption_key,
        }    

    def run_once(self):
        run_uid = f"srdf-run-{timezone.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        orchestrator_run = SRDFOrchestratorRun.objects.create(
            run_uid=run_uid,
            trigger_mode="manual" if not str(self.initiated_by).startswith("daemon") else "daemon",
            status="running",
            sleep_seconds=self.sleep_seconds,
            service_filter_csv=",".join(self.service_names),
        )

        started_perf = time.perf_counter()
        success_services = 0
        failed_services = 0
        service_results = []

        try:
            
            services = list(self._load_services())
            orchestrator_run.total_services = len(services)
            orchestrator_run.save(update_fields=["total_services"])

            archive_supervision_result = self._maybe_run_archive_before_services(
                orchestrator_run=orchestrator_run,
                services=services,
            )

            for service in services:
                service_result = self._run_service_cycle(
                    orchestrator_run=orchestrator_run,
                    service=service,
                )
                service_results.append(service_result)

                if service_result.get("ok"):
                    success_services += 1
                else:
                    failed_services += 1
                    if self.stop_on_service_error:
                        break

            duration_seconds = round(time.perf_counter() - started_perf, 3)

            orchestrator_run.status = "success" if failed_services == 0 else "partial"
            orchestrator_run.success_services = success_services
            orchestrator_run.failed_services = failed_services
            orchestrator_run.finished_at = timezone.now()
            orchestrator_run.duration_seconds = duration_seconds
            orchestrator_run.last_error = "" if failed_services == 0 else f"{failed_services} service(s) failed"
            
            
            orchestrator_run.result_payload = {
                "run_uid": run_uid,
                "total_services": len(services),
                "success_services": success_services,
                "failed_services": failed_services,
                "archive_supervision_result": archive_supervision_result,
                "service_results": service_results,
            }
            
            
            orchestrator_run.save(
                update_fields=[
                    "status",
                    "success_services",
                    "failed_services",
                    "finished_at",
                    "duration_seconds",
                    "last_error",
                    "result_payload",
                ]
            )
            return orchestrator_run.result_payload

        except Exception as exc:
            duration_seconds = round(time.perf_counter() - started_perf, 3)
            orchestrator_run.status = "failed"
            orchestrator_run.finished_at = timezone.now()
            orchestrator_run.duration_seconds = duration_seconds
            orchestrator_run.last_error = str(exc)
            orchestrator_run.result_payload = {
                "run_uid": run_uid,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
            orchestrator_run.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "duration_seconds",
                    "last_error",
                    "result_payload",
                ]
            )
            raise
        
        
    def run_one_cycle(self):
        """Exécute un cycle complet et enregistre tout"""
        run = self._create_orchestrator_run()

        try:
            services = self._get_services_to_process()
            run.total_services = len(services)
            run.save()

            success_count = 0
            failed_count = 0

            for service in services:
                service_success = self._process_single_service(service, run)
                if service_success:
                    success_count += 1
                else:
                    failed_count += 1
                    if self.stop_on_service_error:
                        self.progress_callback(f"Stopping cycle due to error on {service.name}", "warning")
                        break

            run.success_services = success_count
            run.failed_services = failed_count
            run.status = "partial" if failed_count > 0 else "success"

        except Exception as e:
            run.status = "failed"
            run.last_error = str(e)
            self.progress_callback(f"Cycle failed: {e}", "error")
        finally:
            run.finished_at = timezone.now()
            run.duration_seconds = (run.finished_at - run.started_at).total_seconds()
            run.save()

            self.progress_callback(
                f"Cycle finished - Services: {run.success_services} success / {run.failed_services} failed",
                "info"
            )    
    

    def run_forever(self):
        while True:
            self.run_once()
            time.sleep(self.sleep_seconds)

    def _load_services(self):
        qs = ReplicationService.objects.filter(is_enabled=True).select_related("source_node", "target_node")

        if self.service_names:
            qs = qs.filter(name__in=self.service_names)

        return qs.order_by("id")
    
    
    def _maybe_run_archive_before_services(self, orchestrator_run, services):
        if not services:
            return {
                "archive_due": False,
                "archive_executed": False,
                "reason": "no_services",
            }

        reference_service = services[0]

        archive_enabled = self._get_config_bool(
            reference_service,
            "archive_enabled",
            False,
        )
        if not archive_enabled:
            return {
                "archive_due": False,
                "archive_executed": False,
                "reason": "archive_disabled",
            }

        archive_pause_services = self._get_config_bool(
            reference_service,
            "archive_pause_services_during_run",
            True,
        )
        if not archive_pause_services:
            return {
                "archive_due": False,
                "archive_executed": False,
                "reason": "archive_pause_disabled",
            }

        archive_due_result = self._is_archive_due(reference_service)
        if not archive_due_result.get("archive_due"):
            return archive_due_result

        self._emit(
            "Archive window reached - preparing global pause",
            level="warning",
        )

        pause_result = self._pause_services_for_archive(services)

        try:
            settle_result = self._wait_until_services_quiet_for_archive(services)
            if not settle_result.get("ok"):
                raise SRDFOrchestratorError(
                    settle_result.get("message") or "Services did not settle before archive"
                )

            archive_result = run_archive_once(
                replication_service_id=None,
                initiated_by=self.initiated_by,
                trigger_mode="orchestrator",
                dry_run=False,
                progress_callback=self.progress_callback,
            )

            AuditEvent.objects.create(
                event_type="srdf_archive_triggered_by_orchestrator",
                level="info" if archive_result.get("ok") else "warning",
                node=None,
                message="Archive executed by orchestrator",
                created_by=self.initiated_by,
                payload={
                    "orchestrator_run_id": orchestrator_run.id,
                    "archive_result": archive_result,
                },
            )

            return {
                "archive_due": True,
                "archive_executed": True,
                "pause_result": pause_result,
                "settle_result": settle_result,
                "archive_result": archive_result,
            }

        finally:
            resume_result = self._resume_services_after_archive(services)
            orchestrator_run.result_payload = {
                **(orchestrator_run.result_payload or {}),
                "archive_resume_result": resume_result,
            }
            orchestrator_run.save(update_fields=["result_payload"])


    def _is_archive_due(self, service):
        interval_hours = self._get_config_int(
            service,
            "archive_interval_hours",
            24,
        )

        last_archive_run = (
            SRDFArchiveRun.objects
            .filter(status__in=["success", "partial"])
            .order_by("-id")
            .first()
        )

        if not last_archive_run:
            return {
                "archive_due": True,
                "archive_executed": False,
                "reason": "no_previous_archive_run",
                "interval_hours": interval_hours,
            }

        reference_dt = last_archive_run.finished_at or last_archive_run.started_at or last_archive_run.created_at
        if not reference_dt:
            return {
                "archive_due": True,
                "archive_executed": False,
                "reason": "previous_archive_run_without_datetime",
                "interval_hours": interval_hours,
            }

        elapsed_seconds = int((timezone.now() - reference_dt).total_seconds())
        required_seconds = int(interval_hours * 3600)

        if elapsed_seconds < required_seconds:
            return {
                "archive_due": False,
                "archive_executed": False,
                "reason": "archive_interval_not_reached",
                "interval_hours": interval_hours,
                "elapsed_seconds": elapsed_seconds,
                "remaining_seconds": max(required_seconds - elapsed_seconds, 0),
                "last_archive_run_id": last_archive_run.id,
            }

        return {
            "archive_due": True,
            "archive_executed": False,
            "reason": "archive_interval_reached",
            "interval_hours": interval_hours,
            "elapsed_seconds": elapsed_seconds,
            "last_archive_run_id": last_archive_run.id,
        }


    def _pause_services_for_archive(self, services):
        paused_services = []

        for service in services:
            state = self._get_or_create_runtime_state(service)
            if not state.is_paused:
                state.is_paused = True
                state.next_phase = self.PHASE_IDLE
                state.save(update_fields=["is_paused", "next_phase", "updated_at"])
            paused_services.append(service.name)

        self._emit(
            f"Global archive pause requested for {len(paused_services)} service(s)",
            level="warning",
        )

        return {
            "ok": True,
            "paused_services": paused_services,
        }


    def _resume_services_after_archive(self, services):
        resumed_services = []

        for service in services:
            state = self._get_or_create_runtime_state(service)
            if state.is_paused:
                state.is_paused = False
                state.current_phase = self.PHASE_IDLE
                state.next_phase = self.PHASE_CAPTURE
                state.save(
                    update_fields=[
                        "is_paused",
                        "current_phase",
                        "next_phase",
                        "updated_at",
                    ]
                )
            resumed_services.append(service.name)

        self._emit(
            f"Global archive resume completed for {len(resumed_services)} service(s)",
            level="info",
        )

        return {
            "ok": True,
            "resumed_services": resumed_services,
        }


    def _wait_until_services_quiet_for_archive(self, services):
        active_phases = {
            self.PHASE_CAPTURE,
            self.PHASE_DEDUPLICATE,
            self.PHASE_COMPRESS,
            self.PHASE_ENCRYPT,
            self.PHASE_BATCH_BUILD,
            self.PHASE_SHIP,
            self.PHASE_WAIT_ACK,
            self.PHASE_RECEIVE,
            self.PHASE_APPLY,
            self.PHASE_CHECKPOINT,
            self.PHASE_BOOTSTRAP,
        }

        max_wait_seconds = max(self.sleep_seconds * 3, 15)
        started_at = timezone.now()

        while True:
            blocking_services = []

            for service in services:
                state = self._get_or_create_runtime_state(service)
                current_phase = str(state.current_phase or "").strip().lower()

                lock_exists = SRDFServiceExecutionLock.objects.filter(
                    replication_service=service,
                    is_locked=True,
                    expires_at__gt=timezone.now(),
                ).exists()

                if current_phase in active_phases or lock_exists:
                    blocking_services.append({
                        "service_name": service.name,
                        "current_phase": current_phase,
                        "locked": lock_exists,
                    })

            if not blocking_services:
                return {
                    "ok": True,
                    "message": "All services settled before archive",
                    "waited_seconds": int((timezone.now() - started_at).total_seconds()),
                }

            waited_seconds = int((timezone.now() - started_at).total_seconds())
            if waited_seconds >= max_wait_seconds:
                return {
                    "ok": False,
                    "message": "Timeout while waiting services to settle before archive",
                    "waited_seconds": waited_seconds,
                    "blocking_services": blocking_services,
                }

            self._emit(
                f"Waiting services to settle before archive - blocking={len(blocking_services)}",
                level="warning",
            )
            time.sleep(1)    
    

    def _run_service_cycle(self, orchestrator_run, service):
        lock_token = uuid.uuid4().hex
        if not self._acquire_service_lock(service=service, owner_token=lock_token):
            return {
                "ok": False,
                "service": service.name,
                "return_code": "service_locked",
                "error": "Service already locked",
            }

        state = self._get_or_create_runtime_state(service)
        started_perf = time.perf_counter()

        try:
            if state.is_paused:
                self._emit(service.name, "paused", "warning")
                return {
                    "ok": True,
                    "service": service.name,
                    "return_code": "paused",
                    "skipped": True,
                }


            state.last_run_started_at = timezone.now()
            state.last_error = ""
            state.save(update_fields=["last_run_started_at", "last_error", "updated_at"])

            self.progress_callback(
                (
                    f"[SERVICE START] "
                    f"service={service.name} "
                    f"cycle={int(state.cycle_number or 0) + 1} "
                    f"current_phase={state.current_phase or '-'} "
                    f"next_phase={state.next_phase or self.PHASE_CAPTURE}"
                ),
                "info",
            )            

            phase_results = []
            next_phase = state.next_phase or self.PHASE_CAPTURE
            state.cycle_number = int(state.cycle_number or 0) + 1
            state.save(update_fields=["cycle_number", "updated_at"])

            while next_phase:
                if state.stop_requested:
                    result = PhaseResult(
                        status="stopped",
                        return_code=RETURN_STOPPED,
                        next_phase="",
                        error_message="Stop requested",
                    )
                    phase_results.append(self._persist_phase_execution(
                        orchestrator_run=orchestrator_run,
                        service=service,
                        state=state,
                        phase_name=next_phase,
                        request_payload={"reason": "stop_requested"},
                        result=result,
                    ))
                    state.current_phase = self.PHASE_PAUSED
                    state.next_phase = ""
                    state.last_return_code = RETURN_STOPPED
                    state.last_error = "Stop requested"
                    state.last_run_finished_at = timezone.now()
                    state.save(
                        update_fields=[
                            "current_phase",
                            "next_phase",
                            "last_return_code",
                            "last_error",
                            "last_run_finished_at",
                            "updated_at",
                        ]
                    )
                    return {
                        "ok": False,
                        "service": service.name,
                        "return_code": RETURN_STOPPED,
                        "phase_results": phase_results,
                    }


                state.current_phase = next_phase
                state.save(update_fields=["current_phase", "updated_at"])

                handler = self._get_phase_handler(next_phase)
                request_payload = self._build_phase_request(
                    service=service,
                    state=state,
                    phase_name=next_phase,
                )

                self.progress_callback(
                    (
                        f"[PHASE START] "
                        f"service={service.name} "
                        f"phase={next_phase} "
                        f"cycle={int(state.cycle_number or 0)} "
                        f"request_keys={','.join(sorted(request_payload.keys())) if request_payload else '-'}"
                    ),
                    "info",
                )

                started_phase_perf = time.perf_counter()

                result = handler(service=service, state=state, request_payload=request_payload)

                self._emit_phase_summary(
                    service=service,
                    phase_name=next_phase,
                    result=result,
                    when="END",
                )                
                
                if not isinstance(result, PhaseResult):
                    raise SRDFOrchestratorError(f"Phase '{next_phase}' returned invalid result object")


                persisted = self._persist_phase_execution(
                    orchestrator_run=orchestrator_run,
                    service=service,
                    state=state,
                    phase_name=next_phase,
                    request_payload=request_payload,
                    result=result,
                    started_perf=started_phase_perf,
                )
                phase_results.append(persisted)

                self._apply_phase_result_to_state(
                    state=state,
                    phase_name=next_phase,
                    result=result,
                )
                

                if result.status == "failed":
                    
                    self.progress_callback(
                        (
                            f"[SERVICE END] "
                            f"service={service.name} "
                            f"status=failed "
                            f"phase={next_phase} "
                            f"return_code={result.return_code} "
                            f"error={result.error_message or '-'}"
                        ),
                        "error",
                    )                    
                    
                    return {
                        "ok": False,
                        "service": service.name,
                        "return_code": result.return_code,
                        "phase_results": phase_results,
                        "error": result.error_message,
                    }

                if result.status == "retry":
                    
                    self.progress_callback(
                        (
                            f"[SERVICE END] "
                            f"service={service.name} "
                            f"status=retry "
                            f"phase={next_phase} "
                            f"return_code={result.return_code} "
                            f"retry_after={int(result.retry_after_seconds or 0)}"
                        ),
                        "warning",
                    )
                    
                    return {
                        "ok": True,
                        "service": service.name,
                        "return_code": result.return_code,
                        "phase_results": phase_results,
                        "retry_after_seconds": int(result.retry_after_seconds or 0),
                    }

                if result.status in ("stopped", "skipped") and not result.next_phase:
                    break

                next_phase = result.next_phase or ""

            duration_seconds = round(time.perf_counter() - started_perf, 3)
            state.current_phase = self.PHASE_IDLE
            state.next_phase = self.PHASE_CAPTURE
            state.last_return_code = RETURN_OK
            state.last_run_finished_at = timezone.now()
            state.runtime_payload = {
                "last_cycle_duration_seconds": duration_seconds,
                "last_cycle_phase_count": len(phase_results),
            }
            state.save(
                update_fields=[
                    "current_phase",
                    "next_phase",
                    "last_return_code",
                    "last_run_finished_at",
                    "runtime_payload",
                    "updated_at",
                ]
            )
            
            self.progress_callback(
                (
                    f"[SERVICE END] "
                    f"service={service.name} "
                    f"status=success "
                    f"duration_seconds={duration_seconds} "
                    f"phase_count={len(phase_results)} "
                    f"last_return_code={state.last_return_code or '-'} "
                    f"next_phase_reset={self.PHASE_CAPTURE}"
                ),
                "success",
            )            

            return {
                "ok": True,
                "service": service.name,
                "return_code": RETURN_OK,
                "duration_seconds": duration_seconds,
                "phase_results": phase_results,
                "summary": {
                    "service_name": service.name,
                    "phase_count": len(phase_results),
                    "last_success_phase": state.last_success_phase or "",
                    "last_return_code": state.last_return_code or "",
                    "next_phase_after_cycle": state.next_phase or "",
                },
            }
        
        except Exception as exc:
            duration_seconds = round(time.perf_counter() - started_perf, 3)
            state.current_phase = self.PHASE_ERROR
            state.next_phase = ""
            state.consecutive_error_count = int(state.consecutive_error_count or 0) + 1
            state.last_return_code = RETURN_FAILED
            state.last_error = str(exc)
            state.last_run_finished_at = timezone.now()
            state.runtime_payload = {
                "error": str(exc),
                "traceback": traceback.format_exc(),
                "duration_seconds": duration_seconds,
            }
            state.save(
                update_fields=[
                    "current_phase",
                    "next_phase",
                    "consecutive_error_count",
                    "last_return_code",
                    "last_error",
                    "last_run_finished_at",
                    "runtime_payload",
                    "updated_at",
                ]
            )
            
            self.progress_callback(
                (
                    f"[SERVICE EXCEPTION] "
                    f"service={service.name} "
                    f"current_phase={state.current_phase or '-'} "
                    f"next_phase={state.next_phase or '-'} "
                    f"error={str(exc)}"
                ),
                "error",
            )            
            
            return {
                "ok": False,
                "service": service.name,
                "return_code": RETURN_FAILED,
                "error": str(exc),
                "summary": {
                    "service_name": service.name,
                    "current_phase": state.current_phase or "",
                    "next_phase": state.next_phase or "",
                    "last_return_code": state.last_return_code or "",
                    "last_error": state.last_error or "",
                },
            }
        
        finally:
            self._release_service_lock(service=service, owner_token=lock_token)

    def _get_or_create_runtime_state(self, service):
        state, _created = SRDFServiceRuntimeState.objects.get_or_create(
            replication_service=service,
            defaults={
                "is_enabled": True,
                "current_phase": self.PHASE_IDLE,
                "next_phase": self.PHASE_CAPTURE,
            },
        )
        return state

    def _acquire_service_lock(self, service, owner_token):
        with transaction.atomic():
            lock, _created = SRDFServiceExecutionLock.objects.select_for_update().get_or_create(
                name=f"srdf_service_lock_{service.id}",
                replication_service=service,
                defaults={
                    "is_locked": False,
                },
            )

            now = timezone.now()
            if lock.is_locked and lock.expires_at and lock.expires_at > now:
                return False

            lock.is_locked = True
            lock.owner_token = owner_token
            lock.locked_at = now
            lock.expires_at = now + timezone.timedelta(seconds=max(30, self.sleep_seconds * 4))
            lock.save(update_fields=["is_locked", "owner_token", "locked_at", "expires_at", "updated_at"])
            return True

    def _release_service_lock(self, service, owner_token):
        lock = SRDFServiceExecutionLock.objects.filter(
            replication_service=service,
            owner_token=owner_token,
        ).first()
        if not lock:
            return
        lock.is_locked = False
        lock.owner_token = ""
        lock.expires_at = None
        lock.save(update_fields=["is_locked", "owner_token", "expires_at", "updated_at"])

    def _persist_phase_execution(
        self,
        orchestrator_run,
        service,
        state,
        phase_name,
        request_payload,
        result,
        started_perf=None,
    ):
        duration_seconds = round(time.perf_counter() - started_perf, 3) if started_perf else 0
        phase_execution = SRDFServicePhaseExecution.objects.create(
            orchestrator_run=orchestrator_run,
            replication_service=service,
            runtime_state=state,
            cycle_number=int(state.cycle_number or 0),
            phase_name=phase_name,
            status=result.status,
            return_code=result.return_code,
            finished_at=timezone.now(),
            duration_seconds=duration_seconds,
            processed_count=int(result.processed_count or 0),
            success_count=int(result.success_count or 0),
            failed_count=int(result.failed_count or 0),
            skipped_count=int(result.skipped_count or 0),
            retry_after_seconds=int(result.retry_after_seconds or 0),
            last_error=result.error_message or "",
            request_payload=request_payload or {},
            result_payload=result.to_dict(),
        )
        return {
            "phase_name": phase_name,
            "status": result.status,
            "return_code": result.return_code,
            "processed_count": int(result.processed_count or 0),
            "failed_count": int(result.failed_count or 0),
            "duration_seconds": duration_seconds,
            "phase_execution_id": phase_execution.id,
        }

    def _apply_phase_result_to_state(self, state, phase_name, result):
        state.last_return_code = result.return_code
        state.last_error = result.error_message or ""

        if result.status == "success":
            state.last_success_phase = phase_name
            state.consecutive_error_count = 0
        elif result.status == "failed":
            state.consecutive_error_count = int(state.consecutive_error_count or 0) + 1

        state.next_phase = result.next_phase or ""
        now = timezone.now()

        if phase_name == self.PHASE_CAPTURE:
            state.last_capture_at = now
        elif phase_name == self.PHASE_DEDUPLICATE:
            state.last_deduplicate_at = now
        elif phase_name == self.PHASE_COMPRESS:
            state.last_compress_at = now
        elif phase_name == self.PHASE_ENCRYPT:
            state.last_encrypt_at = now
        elif phase_name == self.PHASE_BATCH_BUILD:
            state.last_batch_build_at = now
        elif phase_name == self.PHASE_SHIP:
            state.last_ship_at = now
        elif phase_name == self.PHASE_WAIT_ACK:
            state.last_wait_ack_at = now
        elif phase_name == self.PHASE_RECEIVE:
            state.last_receive_at = now
        elif phase_name == self.PHASE_APPLY:
            state.last_apply_at = now
        elif phase_name == self.PHASE_CHECKPOINT:
            state.last_checkpoint_at = now
        elif phase_name == self.PHASE_BOOTSTRAP:
            state.last_bootstrap_at = now

        state.save(
            update_fields=[
                "last_return_code",
                "last_error",
                "last_success_phase",
                "consecutive_error_count",
                "next_phase",
                "last_capture_at",
                "last_deduplicate_at",
                "last_compress_at",
                "last_encrypt_at",
                "last_batch_build_at",
                "last_ship_at",
                "last_wait_ack_at",
                "last_receive_at",
                "last_apply_at",
                "last_checkpoint_at",
                "last_bootstrap_at",
                "updated_at",
            ]
        )
        
        
    def _get_oldest_open_transport_batch(self, service):
        return (
            TransportBatch.objects
            .filter(
                replication_service=service,
                status__in=["ready", "sent", "acked", "failed"],
            )
            .order_by("id")
            .first()
        )  
    
    
    def _get_oldest_failed_transport_batch(self, service):
        return (
            TransportBatch.objects
            .filter(
                replication_service=service,
                status="failed",
            )
            .order_by("id")
            .first()
        )    
    
    def _can_auto_resume_failed_batch(self, service, batch):
        auto_recovery_enabled = self._get_config_bool(
            service,
            "source_auto_resume_failed_batch",
            True,
        )
        max_auto_resumes = self._get_config_int(
            service,
            "source_max_auto_resume_failed_batch",
            3,
        )
        cooldown_seconds = self._get_config_int(
            service,
            "source_failed_batch_resume_cooldown_seconds",
            max(self.sleep_seconds * 3, 30),
        )

        if not auto_recovery_enabled:
            return {
                "allowed": False,
                "reason_code": "SOURCE_RETRY_DISABLED",
                "message": "Automatic failed batch recovery is disabled",
                "cooldown_seconds": cooldown_seconds,
                "max_auto_resumes": max_auto_resumes,
            }

        retry_count = int(batch.retry_count or 0)
        if retry_count >= max_auto_resumes:
            return {
                "allowed": False,
                "reason_code": "SOURCE_RETRY_MAX_ATTEMPTS_REACHED",
                "message": f"Automatic failed batch recovery limit reached ({retry_count}/{max_auto_resumes})",
                "cooldown_seconds": cooldown_seconds,
                "max_auto_resumes": max_auto_resumes,
            }

        reference_dt = batch.sent_at or batch.acked_at or batch.built_at or batch.created_at
        if reference_dt:
            elapsed = (timezone.now() - reference_dt).total_seconds()
            if elapsed < cooldown_seconds:
                return {
                    "allowed": False,
                    "reason_code": "SOURCE_RETRY_COOLDOWN_ACTIVE",
                    "message": (
                        f"Automatic failed batch recovery cooldown active "
                        f"({int(elapsed)}s elapsed / {int(cooldown_seconds)}s required)"
                    ),
                    "cooldown_seconds": cooldown_seconds,
                    "elapsed_seconds": int(elapsed),
                    "remaining_seconds": max(int(cooldown_seconds - elapsed), 0),
                    "max_auto_resumes": max_auto_resumes,
                }

        return {
            "allowed": True,
            "reason_code": "SOURCE_RETRY_ALLOWED",
            "message": "Failed batch can be resumed automatically",
            "cooldown_seconds": cooldown_seconds,
            "max_auto_resumes": max_auto_resumes,
        }    
    
        
    def _get_services_to_process(self):
        qs = (
            ReplicationService.objects
            .filter(is_enabled=True)
            .select_related("runtime_state")
        )
    
        if self.service_names:
            qs = qs.filter(name__in=self.service_names)
    
        services = []
        for service in qs.order_by("id"):
            runtime_state = getattr(service, "runtime_state", None)
            if runtime_state and runtime_state.is_enabled is False:
                continue
            services.append(service)
    
        return services    
        
        
    def _create_orchestrator_run(self):
        return SRDFOrchestratorRun.objects.create(
            run_uid=f"run_{timezone.now().strftime('%Y%m%d_%H%M%S')}",
            trigger_mode="daemon" if not self.service_names else "manual",
            sleep_seconds=self.sleep_seconds,
            service_filter_csv=",".join(self.service_names),
            initiated_by=self.initiated_by,  # tu peux ajouter ce champ si besoin
        )    

    def _build_phase_request(self, service, state, phase_name):
        config = service.config or {}
        return {
            "service_id": service.id,
            "service_name": service.name,
            "service_type": service.service_type,
            "phase_name": phase_name,
            "cycle_number": int(state.cycle_number or 0),
            "runtime_state_id": state.id,
            "config": config,
            "capture_checkpoint": state.capture_checkpoint or {},
            "transport_checkpoint": state.transport_checkpoint or {},
            "apply_checkpoint": state.apply_checkpoint or {},
        }
    
    def _get_config_int(self, service, key, default_value):
        config = service.config or {}
        raw_value = config.get(key, default_value)
        try:
            return int(raw_value)
        except (TypeError, ValueError):
            return int(default_value)
        
        
    def _get_config_bool(self, service, key, default_value=False):
        config = service.config or {}
        raw_value = config.get(key, default_value)

        if isinstance(raw_value, bool):
            return raw_value

        if isinstance(raw_value, (int, float)):
            return bool(raw_value)

        text = str(raw_value or "").strip().lower()
        if text in ("1", "true", "yes", "y", "on"):
            return True
        if text in ("0", "false", "no", "n", "off", ""):
            return False

        return bool(default_value)    
        
        
    def _count_inbound_by_status(self, service, statuses):
        return int(
            InboundChangeEvent.objects
            .filter(
                replication_service=service,
                status__in=list(statuses),
            )
            .count()
        )   
    
    def _process_remote_batch_callbacks(self, service):
        candidate_batches = list(
            TransportBatch.objects
            .filter(replication_service=service)
            .order_by("id")
        )

        callback_sent_count = 0
        callback_skipped_count = 0
        callback_failed_count = 0
        callback_results = []

        for batch in candidate_batches:
            result = self._process_single_remote_batch_callback(service, batch)
            callback_results.append(result)

            if result.get("status") == "sent":
                callback_sent_count += 1
            elif result.get("status") == "failed":
                callback_failed_count += 1
            else:
                callback_skipped_count += 1

        return {
            "callback_sent_count": callback_sent_count,
            "callback_skipped_count": callback_skipped_count,
            "callback_failed_count": callback_failed_count,
            "callback_results": callback_results,
        }

    def _process_single_remote_batch_callback(self, service, batch):
        inbound_qs = InboundChangeEvent.objects.filter(
            replication_service=service,
            transport_batch=batch,
        )

        total_count = int(inbound_qs.count())
        if total_count <= 0:
            return {
                "batch_id": batch.id,
                "batch_uid": batch.batch_uid,
                "status": "skipped",
                "reason": "no_inbound_for_batch",
            }

        received_count = int(inbound_qs.filter(status="received").count())
        queued_count = int(inbound_qs.filter(status="queued").count())
        applying_count = int(inbound_qs.filter(status="applying").count())
        applied_count = int(inbound_qs.filter(status="applied").count())
        failed_count = int(inbound_qs.filter(status="failed").count())
        dead_count = int(inbound_qs.filter(status="dead").count())

        final_status = ""
        if applied_count == total_count and total_count > 0:
            final_status = "applied"
        elif (failed_count + dead_count) > 0 and applied_count > 0:
            final_status = "partial"
        elif (failed_count + dead_count) > 0 and (received_count + queued_count + applying_count) == 0:
            final_status = "failed"

        if not final_status:
            return {
                "batch_id": batch.id,
                "batch_uid": batch.batch_uid,
                "status": "skipped",
                "reason": "batch_not_finalized_yet",
                "counts": {
                    "total": total_count,
                    "received": received_count,
                    "queued": queued_count,
                    "applying": applying_count,
                    "applied": applied_count,
                    "failed": failed_count,
                    "dead": dead_count,
                },
            }

        counts = {
            "total": total_count,
            "received": received_count,
            "queued": queued_count,
            "applying": applying_count,
            "applied": applied_count,
            "failed": failed_count,
            "dead": dead_count,
        }

        payload_meta = batch.payload or {}
        callback_meta = payload_meta.get("__remote_callback") or {}
        previous_status = str(callback_meta.get("final_status") or "").strip().lower()
        previous_counts = callback_meta.get("counts") or {}

        if previous_status == final_status and previous_counts == counts:
            return {
                "batch_id": batch.id,
                "batch_uid": batch.batch_uid,
                "status": "skipped",
                "reason": "callback_already_sent_for_same_state",
                "final_status": final_status,
                "counts": counts,
            }

        callback_payload = {
            "batch_uid": batch.batch_uid,
            "replication_service_id": service.id,
            "replication_service_name": service.name,
            "target_node_id": service.target_node_id,
            "received_count": total_count,
            "processed_count": applied_count + failed_count + dead_count,
            "applied_count": applied_count,
            "failed_count": failed_count + dead_count,
            "final_status": final_status,
            "applied_inbound_ids": list(
                inbound_qs.filter(status="applied").order_by("id").values_list("id", flat=True)[:500]
            ),
            "failed_inbound_ids": list(
                inbound_qs.filter(status="failed").order_by("id").values_list("id", flat=True)[:500]
            ),
            "dead_inbound_ids": list(
                inbound_qs.filter(status="dead").order_by("id").values_list("id", flat=True)[:500]
            ),
            "counts": counts,
            "callback_origin": "srdf_remote_daemon",
            "applied_at": timezone.now().isoformat(),
        }

        try:
            remote_result = run_remote_action(
                service.source_node,
                action_name="transport.batch_status_callback",
                params={
                    "batch_uid": batch.batch_uid,
                    "final_status": final_status,
                    "callback_payload": callback_payload,
                    "initiated_by": self.initiated_by,
                },
            )
        except Exception as exc:
            AuditEvent.objects.create(
                event_type="transport_remote_batch_callback_failed",
                level="error",
                node=service.target_node,
                message=f"Remote daemon callback failed for batch '{batch.batch_uid}'",
                created_by=self.initiated_by,
                payload={
                    "replication_service_id": service.id,
                    "batch_id": batch.id,
                    "batch_uid": batch.batch_uid,
                    "final_status": final_status,
                    "counts": counts,
                    "error": str(exc),
                },
            )
            return {
                "batch_id": batch.id,
                "batch_uid": batch.batch_uid,
                "status": "failed",
                "final_status": final_status,
                "counts": counts,
                "error": str(exc),
            }

        payload_meta["__remote_callback"] = {
            "final_status": final_status,
            "counts": counts,
            "sent_at": timezone.now().isoformat(),
            "callback_origin": "srdf_remote_daemon",
        }
        batch.payload = payload_meta
        batch.save(update_fields=["payload"])

        AuditEvent.objects.create(
            event_type="transport_remote_batch_callback_sent",
            level="info" if final_status == "applied" else "warning",
            node=service.target_node,
            message=f"Remote daemon callback sent for batch '{batch.batch_uid}'",
            created_by=self.initiated_by,
            payload={
                "replication_service_id": service.id,
                "batch_id": batch.id,
                "batch_uid": batch.batch_uid,
                "final_status": final_status,
                "counts": counts,
                "remote_result": remote_result,
            },
        )

        return {
            "batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "status": "sent",
            "final_status": final_status,
            "counts": counts,
            "remote_result": remote_result,
        }    
    
    

    def _get_next_ready_batch(self, service):
        return (
            TransportBatch.objects
            .filter(
                replication_service=service,
                status="ready",
            )
            .order_by("id")
            .first()
        )
    
    

    def _get_oldest_waiting_transport_batch(self, service):
        return (
            TransportBatch.objects
            .filter(
                replication_service=service,
                status__in=["sent", "acked", "failed"],
            )
            .order_by("id")
            .first()
        )  
    
    def _has_transport_batch_in_flight(self, service):
        return TransportBatch.objects.filter(
            replication_service=service,
            status__in=["sent", "acked"],
        ).exists()    
    
    
    def _build_source_batch_transport_snapshot(self, service, batch):
        outbound_qs = OutboundChangeEvent.objects.filter(
            replication_service=service,
            id__gte=batch.first_event_id,
            id__lte=batch.last_event_id,
        )

        total_count = int(outbound_qs.count())
        pending_count = int(outbound_qs.filter(status="pending").count())
        batched_count = int(outbound_qs.filter(status="batched").count())
        sent_count = int(outbound_qs.filter(status="sent").count())
        acked_count = int(outbound_qs.filter(status="acked").count())
        applied_count = int(outbound_qs.filter(status="applied").count())
        failed_count = int(outbound_qs.filter(status="failed").count())
        dead_count = int(outbound_qs.filter(status="dead").count())

        return {
            "batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "batch_status": batch.status,
            "batch_last_error": batch.last_error or "",
            "total_count": total_count,
            "pending_count": pending_count,
            "batched_count": batched_count,
            "sent_count": sent_count,
            "acked_count": acked_count,
            "applied_count": applied_count,
            "failed_count": failed_count,
            "dead_count": dead_count,
        }    
    

    def _get_phase_handler(self, phase_name):
        mapping = {
            self.PHASE_CAPTURE: self._phase_capture,
            self.PHASE_DEDUPLICATE: self._phase_deduplicate,
            self.PHASE_COMPRESS: self._phase_compress,
            self.PHASE_ENCRYPT: self._phase_encrypt,
            self.PHASE_BATCH_BUILD: self._phase_batch_build,
            self.PHASE_SHIP: self._phase_ship,
            self.PHASE_WAIT_ACK: self._phase_wait_ack,
            self.PHASE_RECEIVE: self._phase_receive,
            self.PHASE_APPLY: self._phase_apply,
            self.PHASE_CHECKPOINT: self._phase_checkpoint,
            self.PHASE_BOOTSTRAP: self._phase_bootstrap,
        }
        handler = mapping.get(phase_name)
        if not handler:
            raise SRDFOrchestratorError(f"Unsupported phase '{phase_name}'")
        return handler
    
    
    
    def _process_single_service(self, service, orchestrator_run):
        """Traite un service complet avec verrou + traçabilité"""
        
        
        lock_token = uuid.uuid4().hex
        lock = self._acquire_service_lock(service=service, owner_token=lock_token)
        if not lock:
            self.progress_callback(f"Service {service.name} skipped (already locked)", "warning")
            return True      
                

        runtime_state = self._get_or_create_runtime_state(service)

        try:
            if runtime_state.is_paused:
                runtime_state.current_phase = "paused"
                runtime_state.save(update_fields=["current_phase", "updated_at"])
                self.progress_callback(f"Service {service.name} paused", "warning")
                return True
        
            if runtime_state.stop_requested:
                runtime_state.current_phase = "idle"
                runtime_state.last_return_code = "SERVICE_STOP_REQUESTED"
                runtime_state.last_run_finished_at = timezone.now()
                runtime_state.save(
                    update_fields=[
                        "current_phase",
                        "last_return_code",
                        "last_run_finished_at",
                        "updated_at",
                    ]
                )
                self.progress_callback(f"Service {service.name} stop requested", "warning")
                return True
        
            runtime_state.cycle_number = int(runtime_state.cycle_number or 0) + 1           

            
            #========================================
            #==== SUITE DU PROCESS DE RUNTIME      ==
            #========================================
            runtime_state.last_run_started_at = timezone.now()
            runtime_state.save(update_fields=["cycle_number", "last_run_started_at", "updated_at"])

            self.progress_callback(f"Processing service: {service.name}", "info")
            # Exécution des phases dans l'ordre logique
            
            phases = [
                ("capture", self._phase_capture),
                ("deduplicate", self._phase_deduplicate),
                ("batch_build", self._phase_batch_build),
                ("ship", self._phase_ship),
                ("wait_ack", self._phase_wait_ack),
                ("retry", self._phase_retry),
            ]
            
            

            for phase_name, phase_func in phases:
                success = self._execute_phase(phase_name, phase_func, service, runtime_state, orchestrator_run)
                if not success and self.stop_on_service_error:
                    return False



            runtime_state.current_phase = "idle"
            runtime_state.last_success_phase = "full_cycle"
            runtime_state.last_run_finished_at = timezone.now()
            runtime_state.consecutive_error_count = 0
            runtime_state.save(
                update_fields=[
                    "current_phase",
                    "last_success_phase",
                    "last_run_finished_at",
                    "consecutive_error_count",
                    "updated_at",
                ]
            )

            return True      
        
        except Exception as e:
            runtime_state.consecutive_error_count += 1
            runtime_state.last_error = str(e)
            runtime_state.last_return_code = "SERVICE_EXCEPTION"
            runtime_state.current_phase = "error"
            runtime_state.last_run_finished_at = timezone.now()
            runtime_state.save(
                update_fields=[
                    "consecutive_error_count",
                    "last_error",
                    "last_return_code",
                    "current_phase",
                    "last_run_finished_at",
                    "updated_at",
                ]
            )

            self.progress_callback(f"Service {service.name} failed: {e}", "error")
            return False        
        

        finally:
            self._release_service_lock(service=service, owner_token=lock_token)
            
            
    def _normalize_phase_result(self, phase_name, raw_result):
        """
        Normalise le résultat d'une phase pour l'orchestrator.
        Retourne toujours un dict standardisé.
        """
        if raw_result is None:
            return {
                "ok": True,
                "status": "success",
                "return_code": f"{phase_name.upper()}_OK",
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "retry_after_seconds": 0,
                "result_payload": {},
            }
        

        if isinstance(raw_result, PhaseResult):
            payload = raw_result.to_dict()
            return {
                "ok": raw_result.status not in ("failed", "stopped"),
                "status": str(raw_result.status or "").strip().lower() or "success",
                "return_code": str(raw_result.return_code or "").strip() or f"{phase_name.upper()}_OK",
                "processed": int(raw_result.processed_count or 0),
                "success": int(raw_result.success_count or 0),
                "failed": int(raw_result.failed_count or 0),
                "skipped": int(raw_result.skipped_count or 0),
                "retry_after_seconds": int(raw_result.retry_after_seconds or 0),
                "result_payload": payload,
            }

        if not isinstance(raw_result, dict):
            return {
                "ok": True,
                "status": "success",
                "return_code": f"{phase_name.upper()}_OK",
                "processed": 0,
                "success": 0,
                "failed": 0,
                "skipped": 0,
                "retry_after_seconds": 0,
                "result_payload": {"raw_result": str(raw_result)},
            }    

        normalized = {
            "ok": bool(raw_result.get("ok", True)),
            "status": str(raw_result.get("status") or "").strip().lower(),
            "return_code": str(raw_result.get("return_code") or "").strip(),
            "processed": int(raw_result.get("processed", 0) or 0),
            "success": int(raw_result.get("success", 0) or 0),
            "failed": int(raw_result.get("failed", 0) or 0),
            "skipped": int(raw_result.get("skipped", 0) or 0),
            "retry_after_seconds": int(raw_result.get("retry_after_seconds", 0) or 0),
            "result_payload": raw_result,
        }

        if not normalized["status"]:
            if normalized["ok"]:
                normalized["status"] = "success"
            else:
                normalized["status"] = "failed"

        if not normalized["return_code"]:
            normalized["return_code"] = f"{phase_name.upper()}_OK" if normalized["ok"] else f"{phase_name.upper()}_ERROR"

        return normalized    
    
    
    def _classify_phase_outcome(self, phase_name, normalized_result):
        """
        Transforme un résultat normalisé en statut orchestrator :
        success / retry / failed / skipped
        """
        return_code = (normalized_result.get("return_code") or "").strip().upper()
        status = (normalized_result.get("status") or "").strip().lower()
        ok = bool(normalized_result.get("ok"))

        if status in ("skipped",):
            return "skipped"



        if status in ("retry",):
            return "retry"


        if phase_name == "capture":
            if return_code in (
                "CAPTURE_OK",
                "CAPTURE_OK_PARTIAL",
                "CAPTURE_NO_EVENTS",
                "CAPTURE_INIT_ONLY",
                "CAPTURE_CONTROL_LIST_OK",
                "CAPTURE_CONTROL_CHECK_OK",
            ):
                return "success"

            if return_code in (
                "CAPTURE_WARNING",
                "CAPTURE_WARNING_SKIPPED_ONLY",
                "CAPTURE_CONTROL_LIST_WARNING",
                "CAPTURE_CONTROL_CHECK_WARNING",
                "CAPTURE_BLOCKED_ALREADY_RUNNING",
            ):
                return "retry"

            if return_code in (
                "CAPTURE_EXCEPTION",
                "CAPTURE_ERROR",
            ):
                return "failed"
            
            
            
        if phase_name == "deduplicate":
            if return_code in (
                "DEDUP_OK",
                "DEDUP_OK_WITH_DUPLICATES",
                "DEDUP_OK_WITH_NEUTRALIZED",
            ):
                return "success"
            if return_code in ("DEDUP_RETRY",):
                return "retry"
            if return_code in ("DEDUP_ERROR", "DEDUP_EXCEPTION"):
                return "failed"
            
            
            
        if phase_name == "batch_build":
            if return_code in (
                "BATCH_BUILD_OK",
                "BATCH_BUILD_EMPTY",
            ):
                return "success"

            if return_code in (
                "BATCH_BUILD_BLOCKED_OPEN_BATCH_EXISTS",
            ):
                return "retry"




        if phase_name == "ship":
            if return_code in (
                "SHIP_OK",
                "SHIP_SKIPPED",
                "SHIP_NO_READY_BATCH",
            ):
                return "success"

            if return_code in (
                "SHIP_BLOCKED_IN_FLIGHT_BATCH",
            ):
                return "retry"


        
        
        
        if phase_name == "wait_ack":
            if return_code in (
                "WAIT_ACK_EMPTY",
                "WAIT_ACK_REMOTE_APPLIED",
                "WAIT_ACK_UNEXPECTED_STATE",
            ):
                return "success"

            if return_code in (
                "WAIT_ACK_PENDING_REMOTE_CALLBACK",
                "WAIT_ACK_ACKED_PENDING_APPLY",
                "WAIT_ACK_REMOTE_PARTIAL",
            ):
                return "retry"

            if return_code in (
                "WAIT_ACK_REMOTE_FAILED",
                "WAIT_ACK_LOCAL_BATCH_FAILED",
            ):
                return "failed"
        
        
        if phase_name == "receive":
            if return_code in (
                "REMOTE_RECEIVE_PENDING",
                "REMOTE_RECEIVE_FAILED_PENDING_RETRY",
                "REMOTE_RECEIVE_EMPTY",
            ):
                return "success"


        if phase_name == "apply":
            if return_code in (
                "REMOTE_APPLY_OK",
                "REMOTE_APPLY_EMPTY",
            ):
                return "success"
            if return_code in (
                "REMOTE_APPLY_PARTIAL",
            ):
                return "retry"



        if phase_name == "retry":
            if return_code in (
                "SOURCE_RETRY_EMPTY",
                "SOURCE_RETRY_RESUMED_BATCH",
                "REMOTE_RETRY_OK",
                "REMOTE_RETRY_EMPTY",
                "REMOTE_RETRY_NO_WORK",
            ):
                return "success"

            if return_code in (
                "SOURCE_RETRY_DISABLED",
                "SOURCE_RETRY_COOLDOWN_ACTIVE",
                "SOURCE_RETRY_MAX_ATTEMPTS_REACHED",
                "REMOTE_RETRY_PARTIAL",
            ):
                return "retry"

        

        if ok:
            return "success"

        return "failed"        


    def _execute_phase(self, phase_name, phase_func, service, runtime_state, orchestrator_run):
        """Exécute une phase et enregistre tout dans SRDFServicePhaseExecution"""
        phase_exec = SRDFServicePhaseExecution.objects.create(
            orchestrator_run=orchestrator_run,
            replication_service=service,
            runtime_state=runtime_state,
            phase_name=phase_name,
            cycle_number=runtime_state.cycle_number,
            status="running",
        )

        runtime_state.current_phase = phase_name
        runtime_state.last_run_started_at = timezone.now()
        runtime_state.save(update_fields=["current_phase", "last_run_started_at", "updated_at"])

        start = timezone.now()
        normalized_result = None

        try:
            request_payload = {
                "service_id": service.id,
                "service_name": service.name,
                "phase_name": phase_name,
                "cycle_number": int(runtime_state.cycle_number or 0),
            }
            raw_result = phase_func(service, runtime_state, request_payload)            
            
            normalized_result = self._normalize_phase_result(phase_name, raw_result)
            orchestrator_status = self._classify_phase_outcome(phase_name, normalized_result)

            phase_exec.status = orchestrator_status
            phase_exec.return_code = normalized_result.get("return_code") or ""
            phase_exec.processed_count = int(normalized_result.get("processed") or 0)
            phase_exec.success_count = int(normalized_result.get("success") or 0)
            phase_exec.failed_count = int(normalized_result.get("failed") or 0)
            phase_exec.skipped_count = int(normalized_result.get("skipped") or 0)
            phase_exec.retry_after_seconds = int(normalized_result.get("retry_after_seconds") or 0)
            phase_exec.result_payload = normalized_result.get("result_payload") or {}

            runtime_state.last_return_code = phase_exec.return_code
            runtime_state.runtime_payload = normalized_result.get("result_payload") or {}

            if orchestrator_status == "success":
                runtime_state.last_success_phase = phase_name
                runtime_state.last_error = ""
                runtime_state.consecutive_error_count = 0

                if phase_name == "capture":
                    runtime_state.last_capture_at = timezone.now()
                elif phase_name == "deduplicate":
                    runtime_state.last_deduplicate_at = timezone.now()
                elif phase_name == "batch_build":
                    runtime_state.last_batch_build_at = timezone.now()
                elif phase_name == "ship":
                    runtime_state.last_ship_at = timezone.now()
                elif phase_name == "receive":
                    runtime_state.last_receive_at = timezone.now()
                elif phase_name == "apply":
                    runtime_state.last_apply_at = timezone.now()
                elif phase_name == "retry":
                    runtime_state.last_checkpoint_at = timezone.now()

                self.progress_callback(
                    f"Phase {phase_name} OK for {service.name} (return_code={phase_exec.return_code})",
                    "info",
                )

            elif orchestrator_status == "retry":
                runtime_state.last_error = ""
                self.progress_callback(
                    f"Phase {phase_name} RETRY for {service.name} (return_code={phase_exec.return_code})",
                    "warning",
                )

            elif orchestrator_status == "skipped":
                self.progress_callback(
                    f"Phase {phase_name} SKIPPED for {service.name} (return_code={phase_exec.return_code})",
                    "warning",
                )

            else:
                runtime_state.consecutive_error_count += 1
                runtime_state.last_error = (
                    normalized_result.get("result_payload", {}).get("error")
                    or f"Phase {phase_name} failed"
                )
                self.progress_callback(
                    f"Phase {phase_name} FAILED for {service.name} (return_code={phase_exec.return_code})",
                    "error",
                )

        except Exception as e:
            phase_exec.status = "failed"
            phase_exec.return_code = f"{phase_name.upper()}_EXCEPTION"
            phase_exec.last_error = str(e)
            phase_exec.result_payload = {"error": str(e)}

            runtime_state.last_return_code = phase_exec.return_code
            runtime_state.last_error = str(e)
            runtime_state.consecutive_error_count += 1

            self.progress_callback(
                f"Phase {phase_name} FAILED for {service.name}: {e}",
                "error",
            )

        finally:
            phase_exec.finished_at = timezone.now()
            phase_exec.duration_seconds = (phase_exec.finished_at - start).total_seconds()
            phase_exec.save()

            runtime_state.last_run_finished_at = timezone.now()
            runtime_state.save()

        return phase_exec.status in ("success", "retry", "skipped")    
    



    def _phase_deduplicate(self, service, state, request_payload):
        result = run_deduplicate_once(
            replication_service_id=service.id,
            limit=5000,
            initiated_by=self.initiated_by,
        )

        processed_count = int(result.get("processed_count") or 0)
        duplicate_count = int(result.get("duplicate_count") or 0)
        survivor_count = int(result.get("survivor_count") or 0)
        neutralized_count = int(result.get("neutralized_count") or 0)
        collapsed_count = int(result.get("collapsed_count") or 0)

        return_code = "DEDUP_OK"
        if duplicate_count > 0:
            return_code = "DEDUP_OK_WITH_DUPLICATES"
        if neutralized_count > 0:
            return_code = "DEDUP_OK_WITH_NEUTRALIZED"
            
            
        self.progress_callback(
            (
                f"Deduplicate analyzed service={service.name} "
                f"processed={processed_count} "
                f"survivors={survivor_count} "
                f"duplicates={duplicate_count} "
                f"neutralized={neutralized_count} "
                f"collapsed={collapsed_count}"
            ),
            "warning" if duplicate_count > 0 else "info",
        )

        return PhaseResult(
            status="success",
            return_code=return_code,
            next_phase=self.PHASE_COMPRESS,
            processed_count=processed_count,
            success_count=survivor_count,
            failed_count=0,
            skipped_count=duplicate_count,
            payload={
                "replication_service_id": service.id,
                "processed_count": processed_count,
                "survivor_count": survivor_count,
                "duplicate_count": duplicate_count,
                "neutralized_count": neutralized_count,
                "collapsed_count": collapsed_count,
                "duplicate_event_ids_preview": result.get("duplicate_event_ids_preview") or [],
                "survivor_event_ids_preview": result.get("survivor_event_ids_preview") or [],
                "neutralized_event_ids_preview": result.get("neutralized_event_ids_preview") or [],
                "collapsed_event_ids_preview": result.get("collapsed_event_ids_preview") or [],
                "implemented": True,
            },
        ) 
    
    
    
    def _phase_capture(self, service, state, request_payload):
        result = capture_changes(
            service=service,
            initiated_by=self.initiated_by,
        )

        processed_count = int(result.get("processed") or 0)
        success_count = int(result.get("success") or 0)
        failed_count = int(result.get("failed") or 0)
        skipped_count = int(result.get("skipped") or 0)
        retry_after_seconds = int(result.get("retry_after_seconds") or 0)
        return_code = str(result.get("return_code") or "CAPTURE_ERROR")

        phase_status = "success"
        if str(result.get("status") or "").strip().lower() == "retry":
            phase_status = "retry"
        elif str(result.get("status") or "").strip().lower() == "failed":
            phase_status = "failed"

        return PhaseResult(
            status=phase_status,
            return_code=return_code,
            next_phase=self.PHASE_DEDUPLICATE if phase_status == "success" else "",
            processed_count=processed_count,
            success_count=success_count,
            failed_count=failed_count,
            skipped_count=skipped_count,
            retry_after_seconds=retry_after_seconds,
            error_message=str((result.get("result_payload") or {}).get("error") or ""),
            payload=result.get("result_payload") or {},
        )    
    

    def _phase_compress(self, service, state, request_payload):
        pending_events, payload_events = self._build_pending_transport_payload_events(
            service=service,
            limit=100,
        )
    
        logical_payload = build_logical_payload(
            replication_service=service,
            payload_events=payload_events,
            extra_payload={
                "created_at": timezone.now().isoformat(),
                "phase": "compress",
                "cycle_number": int(state.cycle_number or 0),
            },
        )
    
        codec_settings = self._get_transport_codec_settings(service)
        compression = codec_settings.get("compression") or "zlib"
    
        encoded = encode_transport_payload(
            logical_payload=logical_payload,
            compression=compression,
            encryption="none",
            encryption_key="",
        )
    
        AuditEvent.objects.create(
            event_type="srdf_phase_compress_completed",
            level="info",
            node=service.source_node,
            message=f"Compress phase completed for service '{service.name}'",
            created_by=self.initiated_by,
            payload={
                "replication_service_id": service.id,
                "cycle_number": int(state.cycle_number or 0),
                "pending_event_count": len(pending_events),
                "compression": compression,
                "logical_checksum": encoded.get("logical_checksum") or "",
                "logical_size_bytes": int(encoded.get("logical_size_bytes") or 0),
                "compressed_size_bytes": int(encoded.get("compressed_size_bytes") or 0),
            },
        )
        
        self.progress_callback(
            (
                f"Compress completed service={service.name} "
                f"pending_event_count={len(pending_events)} "
                f"compression={compression} "
                f"logical_size_bytes={int(encoded.get('logical_size_bytes') or 0)} "
                f"compressed_size_bytes={int(encoded.get('compressed_size_bytes') or 0)}"
            ),
            "info",
        )        
    
        return PhaseResult(
            status="success",
            return_code="PHASE_COMPRESS_OK",
            next_phase=self.PHASE_ENCRYPT,
            processed_count=len(pending_events),
            success_count=len(pending_events),
            failed_count=0,
            skipped_count=0,
            payload={
                "transport_payload_stage": "compressed",
                "pending_event_count": len(pending_events),
                "compression": compression,
                "encryption": "none",
                "encoded_payload": encoded.get("encoded_payload") or "",
                "logical_checksum": encoded.get("logical_checksum") or "",
                "logical_size_bytes": int(encoded.get("logical_size_bytes") or 0),
                "compressed_size_bytes": int(encoded.get("compressed_size_bytes") or 0),
                "encoded_size_bytes": int(encoded.get("encoded_size_bytes") or 0),
                "transport_meta": encoded.get("transport_meta") or {},
            },
        )
    
    
    def _phase_encrypt(self, service, state, request_payload):
        pending_events, payload_events = self._build_pending_transport_payload_events(
            service=service,
            limit=100,
        )
    
        logical_payload = build_logical_payload(
            replication_service=service,
            payload_events=payload_events,
            extra_payload={
                "created_at": timezone.now().isoformat(),
                "phase": "encrypt",
                "cycle_number": int(state.cycle_number or 0),
            },
        )
    
        codec_settings = self._get_transport_codec_settings(service)
        compression = codec_settings.get("compression") or "zlib"
        encryption = codec_settings.get("encryption") or "xor"
        encryption_key = codec_settings.get("encryption_key") or ""
    
        encoded = encode_transport_payload(
            logical_payload=logical_payload,
            compression=compression,
            encryption=encryption,
            encryption_key=encryption_key,
        )
    
        AuditEvent.objects.create(
            event_type="srdf_phase_encrypt_completed",
            level="info",
            node=service.source_node,
            message=f"Encrypt phase completed for service '{service.name}'",
            created_by=self.initiated_by,
            payload={
                "replication_service_id": service.id,
                "cycle_number": int(state.cycle_number or 0),
                "pending_event_count": len(pending_events),
                "compression": compression,
                "encryption": encryption,
                "logical_checksum": encoded.get("logical_checksum") or "",
                "logical_size_bytes": int(encoded.get("logical_size_bytes") or 0),
                "compressed_size_bytes": int(encoded.get("compressed_size_bytes") or 0),
                "encoded_size_bytes": int(encoded.get("encoded_size_bytes") or 0),
            },
        )
        
        self.progress_callback(
            (
                f"Encrypt completed service={service.name} "
                f"pending_event_count={len(pending_events)} "
                f"compression={compression} "
                f"encryption={encryption} "
                f"logical_size_bytes={int(encoded.get('logical_size_bytes') or 0)} "
                f"compressed_size_bytes={int(encoded.get('compressed_size_bytes') or 0)} "
                f"encoded_size_bytes={int(encoded.get('encoded_size_bytes') or 0)}"
            ),
            "info",
        )        
    
        return PhaseResult(
            status="success",
            return_code="PHASE_ENCRYPT_OK",
            next_phase=self.PHASE_BATCH_BUILD,
            processed_count=len(pending_events),
            success_count=len(pending_events),
            failed_count=0,
            skipped_count=0,
            payload={
                "transport_payload_stage": "encrypted",
                "pending_event_count": len(pending_events),
                "compression": compression,
                "encryption": encryption,
                "encoded_payload": encoded.get("encoded_payload") or "",
                "logical_checksum": encoded.get("logical_checksum") or "",
                "logical_size_bytes": int(encoded.get("logical_size_bytes") or 0),
                "compressed_size_bytes": int(encoded.get("compressed_size_bytes") or 0),
                "encoded_size_bytes": int(encoded.get("encoded_size_bytes") or 0),
                "transport_meta": encoded.get("transport_meta") or {},
            },
        )    
    
    

    def _phase_batch_build(self, service, state, request_payload):
        batch_limit = self._get_config_int(service, "transport_batch_limit", 100)

        open_batch = self._get_oldest_open_transport_batch(service)
        if open_batch:
            payload = {
                "replication_service_id": service.id,
                "batch_limit": batch_limit,
                "open_batch_id": open_batch.id,
                "open_batch_uid": open_batch.batch_uid,
                "open_batch_status": open_batch.status,
                "open_batch_event_count": int(open_batch.event_count or 0),
                "open_batch_last_error": open_batch.last_error or "",
            }

            self.progress_callback(
                (
                    f"Batch build blocked service={service.name} "
                    f"open_batch_id={open_batch.id} "
                    f"open_batch_status={open_batch.status} "
                    f"event_count={int(open_batch.event_count or 0)}"
                ),
                "warning",
            )

            next_phase = ""
            if open_batch.status in ("ready",):
                next_phase = self.PHASE_SHIP
            elif open_batch.status in ("sent", "acked", "failed"):
                next_phase = self.PHASE_WAIT_ACK

            return PhaseResult(
                status="retry",
                return_code="BATCH_BUILD_BLOCKED_OPEN_BATCH_EXISTS",
                next_phase=next_phase,
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                retry_after_seconds=self.sleep_seconds,
                payload=payload,
            )

        result = build_transport_batch_once(
            replication_service_id=service.id,
            limit=batch_limit,
            initiated_by=self.initiated_by,
        )

        batch_created = bool(result.get("batch_created"))
        event_count = int(result.get("event_count") or 0)
        batch_id = int(result.get("batch_id") or 0) if result.get("batch_id") else 0
        batch_uid = str(result.get("batch_uid") or "")

        if batch_created and batch_id > 0:
            self.progress_callback(
                f"Batch built service={service.name} batch_id={batch_id} event_count={event_count}",
                "info",
            )

            return PhaseResult(
                status="success",
                return_code="BATCH_BUILD_OK",
                next_phase=self.PHASE_SHIP,
                processed_count=event_count,
                success_count=1,
                failed_count=0,
                skipped_count=0,
                payload={
                    "replication_service_id": service.id,
                    "batch_limit": batch_limit,
                    "batch_id": batch_id,
                    "batch_uid": batch_uid,
                    "event_count": event_count,
                    "builder_result": result,
                },
            )

        self.progress_callback(
            f"Batch build found no eligible events for service={service.name}",
            "warning",
        )

        return PhaseResult(
            status="success",
            return_code="BATCH_BUILD_EMPTY",
            next_phase="",
            processed_count=0,
            success_count=0,
            failed_count=0,
            skipped_count=1,
            payload={
                "replication_service_id": service.id,
                "batch_limit": batch_limit,
                "builder_result": result,
            },
        )    
    
    

    def _phase_ship(self, service, state, request_payload):
        in_flight_batch = self._get_oldest_waiting_transport_batch(service)
        if in_flight_batch and in_flight_batch.status in ("sent", "acked"):
            snapshot = self._build_source_batch_transport_snapshot(service, in_flight_batch)

            self.progress_callback(
                (
                    f"Ship blocked service={service.name} "
                    f"batch_id={snapshot['batch_id']} "
                    f"batch_status={snapshot['batch_status']} "
                    f"sent={snapshot['sent_count']} "
                    f"acked={snapshot['acked_count']} "
                    f"applied={snapshot['applied_count']} "
                    f"failed={snapshot['failed_count']}"
                ),
                "warning",
            )

            return PhaseResult(
                status="retry",
                return_code="SHIP_BLOCKED_IN_FLIGHT_BATCH",
                next_phase=self.PHASE_WAIT_ACK,
                processed_count=int(snapshot["total_count"] or 0),
                success_count=0,
                failed_count=int(snapshot["failed_count"] or 0),
                skipped_count=1,
                retry_after_seconds=self.sleep_seconds,
                payload={
                    "replication_service_id": service.id,
                    "blocked_batch_snapshot": snapshot,
                },
            )

        ready_batch = self._get_next_ready_batch(service)
        if not ready_batch:
            return PhaseResult(
                status="skipped",
                return_code="SHIP_NO_READY_BATCH",
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "message": "No ready batch to send",
                },
            )

        result = send_transport_batch_once(
            batch_id=ready_batch.id,
            initiated_by=self.initiated_by,
        )

        skipped = bool(result.get("skipped"))
        if skipped:
            return PhaseResult(
                status="skipped",
                return_code="SHIP_SKIPPED",
                next_phase=self.PHASE_WAIT_ACK,
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "batch_id": ready_batch.id,
                    "batch_uid": ready_batch.batch_uid,
                    "sender_result": result,
                },
            )

        self.progress_callback(
            f"Batch sent service={service.name} batch_id={ready_batch.id} batch_uid={ready_batch.batch_uid}",
            "info",
        )

        return PhaseResult(
            status="success",
            return_code="SHIP_OK",
            next_phase=self.PHASE_WAIT_ACK,
            processed_count=int(ready_batch.event_count or 0),
            success_count=1,
            failed_count=0,
            skipped_count=0,
            payload={
                "replication_service_id": service.id,
                "batch_id": ready_batch.id,
                "batch_uid": ready_batch.batch_uid,
                "event_count": int(ready_batch.event_count or 0),
                "sender_result": result,
            },
        )    
    
    
    def _phase_wait_ack(self, service, state, request_payload):
        waiting_batch = self._get_oldest_waiting_transport_batch(service)
        if not waiting_batch:
            return PhaseResult(
                status="skipped",
                return_code="WAIT_ACK_EMPTY",
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "message": "No sent/acked/failed batch currently waiting",
                },
            )

        waiting_batch.refresh_from_db()
        snapshot = self._build_source_batch_transport_snapshot(service, waiting_batch)

        self.progress_callback(
            (
                f"Wait ack service={service.name} "
                f"batch_id={snapshot['batch_id']} "
                f"batch_status={snapshot['batch_status']} "
                f"sent={snapshot['sent_count']} "
                f"acked={snapshot['acked_count']} "
                f"applied={snapshot['applied_count']} "
                f"failed={snapshot['failed_count']} "
                f"dead={snapshot['dead_count']}"
            ),
            "info" if snapshot["failed_count"] == 0 else "warning",
        )        

        total_count = int(snapshot["total_count"] or 0)
        sent_count = int(snapshot["sent_count"] or 0)
        acked_count = int(snapshot["acked_count"] or 0)
        applied_count = int(snapshot["applied_count"] or 0)
        failed_count = int(snapshot["failed_count"] or 0)
        dead_count = int(snapshot["dead_count"] or 0)

        # 1) Remote apply fully confirmed
        if total_count > 0 and applied_count == total_count:
            return PhaseResult(
                status="success",
                return_code="WAIT_ACK_REMOTE_APPLIED",
                next_phase="",
                processed_count=total_count,
                success_count=applied_count,
                failed_count=0,
                skipped_count=0,
                payload={
                    "replication_service_id": service.id,
                    "transport_snapshot": snapshot,
                },
            )

        # 2) Remote failure fully confirmed
        if total_count > 0 and (failed_count + dead_count) == total_count:
            return PhaseResult(
                status="failed",
                return_code="WAIT_ACK_REMOTE_FAILED",
                next_phase="",
                processed_count=total_count,
                success_count=0,
                failed_count=failed_count + dead_count,
                skipped_count=0,
                error_message=waiting_batch.last_error or "Remote batch failed",
                payload={
                    "replication_service_id": service.id,
                    "transport_snapshot": snapshot,
                },
            )

        # 3) Mixed final state (partial remote apply)
        if total_count > 0 and applied_count > 0 and (failed_count + dead_count) > 0:
            return PhaseResult(
                status="retry",
                return_code="WAIT_ACK_REMOTE_PARTIAL",
                next_phase="",
                processed_count=total_count,
                success_count=applied_count,
                failed_count=failed_count + dead_count,
                skipped_count=0,
                retry_after_seconds=self.sleep_seconds,
                error_message="Remote batch partial apply",
                payload={
                    "replication_service_id": service.id,
                    "transport_snapshot": snapshot,
                },
            )

        # 4) Remote acknowledged receipt, apply not finalized yet
        if waiting_batch.status == "acked" or acked_count > 0:
            return PhaseResult(
                status="retry",
                return_code="WAIT_ACK_ACKED_PENDING_APPLY",
                next_phase="",
                processed_count=total_count,
                success_count=acked_count,
                failed_count=failed_count,
                skipped_count=0,
                retry_after_seconds=self.sleep_seconds,
                payload={
                    "replication_service_id": service.id,
                    "transport_snapshot": snapshot,
                },
            )

        # 5) Packet sent, callback still pending
        if waiting_batch.status == "sent" or sent_count > 0:
            return PhaseResult(
                status="retry",
                return_code="WAIT_ACK_PENDING_REMOTE_CALLBACK",
                next_phase="",
                processed_count=total_count,
                success_count=sent_count,
                failed_count=failed_count,
                skipped_count=0,
                retry_after_seconds=self.sleep_seconds,
                payload={
                    "replication_service_id": service.id,
                    "transport_snapshot": snapshot,
                },
            )

        # 6) Batch marked failed locally before remote callback stabilization
        if waiting_batch.status == "failed":
            return PhaseResult(
                status="failed",
                return_code="WAIT_ACK_LOCAL_BATCH_FAILED",
                next_phase="",
                processed_count=total_count,
                success_count=0,
                failed_count=max(failed_count, 1),
                skipped_count=0,
                error_message=waiting_batch.last_error or "Local sender marked batch as failed",
                payload={
                    "replication_service_id": service.id,
                    "transport_snapshot": snapshot,
                },
            )

        return PhaseResult(
            status="skipped",
            return_code="WAIT_ACK_UNEXPECTED_STATE",
            next_phase="",
            processed_count=total_count,
            success_count=0,
            failed_count=failed_count,
            skipped_count=1,
            payload={
                "replication_service_id": service.id,
                "transport_snapshot": snapshot,
            },
        )    

    def _phase_receive(self, service, state, request_payload):
        received_count = self._count_inbound_by_status(service, ["received"])
        failed_count = self._count_inbound_by_status(service, ["failed"])
        applied_count = self._count_inbound_by_status(service, ["applied"])
        dead_count = self._count_inbound_by_status(service, ["dead"])

        if received_count > 0:
            self.progress_callback(
                f"Remote receive queue service={service.name} received={received_count}",
                "info",
            )
            return PhaseResult(
                status="success",
                return_code="REMOTE_RECEIVE_PENDING",
                next_phase=self.PHASE_APPLY,
                processed_count=received_count,
                success_count=received_count,
                failed_count=0,
                skipped_count=0,
                payload={
                    "replication_service_id": service.id,
                    "received_count": received_count,
                    "failed_count": failed_count,
                    "applied_count": applied_count,
                    "dead_count": dead_count,
                },
            )

        if failed_count > 0:
            self.progress_callback(
                f"Remote receive queue service={service.name} no new received events, failed={failed_count} waiting for retry",
                "warning",
            )
            return PhaseResult(
                status="success",
                return_code="REMOTE_RECEIVE_FAILED_PENDING_RETRY",
                next_phase="retry",
                processed_count=failed_count,
                success_count=0,
                failed_count=failed_count,
                skipped_count=0,
                payload={
                    "replication_service_id": service.id,
                    "received_count": received_count,
                    "failed_count": failed_count,
                    "applied_count": applied_count,
                    "dead_count": dead_count,
                },
            )

        return PhaseResult(
            status="skipped",
            return_code="REMOTE_RECEIVE_EMPTY",
            next_phase="",
            processed_count=0,
            success_count=0,
            failed_count=0,
            skipped_count=1,
            payload={
                "replication_service_id": service.id,
                "received_count": received_count,
                "failed_count": failed_count,
                "applied_count": applied_count,
                "dead_count": dead_count,
            },
        )
    
    

    def _phase_apply(self, service, state, request_payload):
        apply_limit = self._get_config_int(service, "remote_apply_limit", 100)

        result = apply_inbound_events_once(
            replication_service_id=service.id,
            limit=apply_limit,
            initiated_by=self.initiated_by,
            retry_failed=False,
        )

        callback_result = self._process_remote_batch_callbacks(service)

        processed_count = int(result.get("processed_count") or 0)
        applied_count = int(result.get("applied_count") or 0)
        failed_count = int(result.get("failed_count") or 0)

        if processed_count == 0:
            return PhaseResult(
                status="skipped",
                return_code="REMOTE_APPLY_EMPTY",
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "apply_limit": apply_limit,
                    "apply_result": result,
                    "callback_result": callback_result,
                },
            )

        if failed_count > 0:
            self.progress_callback(
                f"Remote apply partial service={service.name} processed={processed_count} applied={applied_count} failed={failed_count}",
                "warning",
            )
            return PhaseResult(
                status="retry",
                return_code="REMOTE_APPLY_PARTIAL",
                next_phase="retry",
                processed_count=processed_count,
                success_count=applied_count,
                failed_count=failed_count,
                skipped_count=0,
                retry_after_seconds=self.sleep_seconds,
                payload={
                    "replication_service_id": service.id,
                    "apply_limit": apply_limit,
                    "apply_result": result,
                    "callback_result": callback_result,
                },
            )

        self.progress_callback(
            f"Remote apply OK service={service.name} processed={processed_count} applied={applied_count}",
            "info",
        )

        return PhaseResult(
            status="success",
            return_code="REMOTE_APPLY_OK",
            next_phase="",
            processed_count=processed_count,
            success_count=applied_count,
            failed_count=0,
            skipped_count=0,
            payload={
                "replication_service_id": service.id,
                "apply_limit": apply_limit,
                "apply_result": result,
                "callback_result": callback_result,
            },
        )
    
    
    def _phase_retry_source_batch_failed(self, service, state, request_payload):
        failed_batch = self._get_oldest_failed_transport_batch(service)
        if not failed_batch:
            return PhaseResult(
                status="skipped",
                return_code="SOURCE_RETRY_EMPTY",
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "message": "No failed transport batch to resume",
                },
            )

        snapshot = {
            "batch_id": failed_batch.id,
            "batch_uid": failed_batch.batch_uid,
            "batch_status": failed_batch.status,
            "batch_event_count": int(failed_batch.event_count or 0),
            "batch_last_error": failed_batch.last_error or "",
            "batch_retry_count": int(failed_batch.retry_count or 0),
            "sent_at": failed_batch.sent_at.isoformat() if failed_batch.sent_at else "",
            "acked_at": failed_batch.acked_at.isoformat() if failed_batch.acked_at else "",
            "built_at": failed_batch.built_at.isoformat() if failed_batch.built_at else "",
            "created_at": failed_batch.created_at.isoformat() if failed_batch.created_at else "",
        }

        recovery_guard = self._can_auto_resume_failed_batch(service, failed_batch)
        if not recovery_guard.get("allowed"):
            self.progress_callback(
                (
                    f"Source retry blocked service={service.name} "
                    f"batch_id={failed_batch.id} "
                    f"reason={recovery_guard.get('reason_code')}"
                ),
                "warning",
            )

            return PhaseResult(
                status="retry",
                return_code=str(recovery_guard.get("reason_code") or "SOURCE_RETRY_BLOCKED"),
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=1,
                skipped_count=1,
                retry_after_seconds=int(recovery_guard.get("remaining_seconds") or self.sleep_seconds),
                error_message=str(recovery_guard.get("message") or ""),
                payload={
                    "replication_service_id": service.id,
                    "failed_batch_snapshot": snapshot,
                    "recovery_guard": recovery_guard,
                },
            )

        self.progress_callback(
            (
                f"Source retry resume failed batch service={service.name} "
                f"batch_id={failed_batch.id} "
                f"batch_uid={failed_batch.batch_uid}"
            ),
            "warning",
        )

        result = resume_failed_batch_once(
            batch_id=failed_batch.id,
            initiated_by=self.initiated_by,
        )

        requeued_events = int(result.get("requeued_events") or 0)

        return PhaseResult(
            status="success",
            return_code="SOURCE_RETRY_RESUMED_BATCH",
            next_phase=self.PHASE_BATCH_BUILD,
            processed_count=requeued_events,
            success_count=1,
            failed_count=0,
            skipped_count=0,
            payload={
                "replication_service_id": service.id,
                "failed_batch_snapshot": snapshot,
                "recovery_guard": recovery_guard,
                "resume_result": result,
            },
        )   
    
    
    
    def _phase_retry(self, service, state, request_payload):
        return self._phase_retry_source_batch_failed(service, state, request_payload)
    
    
    
    
    
    def _phase_checkpoint(self, service, state, request_payload):
        return self._phase_not_implemented(
            phase_name=self.PHASE_CHECKPOINT,
            next_phase=self.PHASE_BOOTSTRAP,
        )

    def _phase_bootstrap(self, service, state, request_payload):
        plans = list(
            BootstrapPlan.objects
            .filter(replication_service=service, is_enabled=True)
            .order_by("execution_order", "id")
        )

        if not plans:
            return PhaseResult(
                status="skipped",
                return_code=RETURN_SKIPPED,
                next_phase="",
                skipped_count=1,
                payload={"reason": "no_bootstrap_plan"},
            )

        success_count = 0
        failed_count = 0
        results = []

        for plan in plans:
            try:
                result = run_bootstrap_plan_once(
                    bootstrap_plan_id=plan.id,
                    bootstrap_plan_name=plan.name,
                    initiated_by=self.initiated_by,
                    dry_run_override=False,
                    progress_callback=None,
                )
                success_count += 1
                results.append(
                    {
                        "plan_name": plan.name,
                        "ok": True,
                        "result": result,
                    }
                )
            except Exception as exc:
                failed_count += 1
                results.append(
                    {
                        "plan_name": plan.name,
                        "ok": False,
                        "error": str(exc),
                    }
                )

        if failed_count > 0:
            return PhaseResult(
                status="failed",
                return_code=RETURN_FAILED,
                next_phase="",
                processed_count=len(plans),
                success_count=success_count,
                failed_count=failed_count,
                error_message=f"{failed_count} bootstrap plan(s) failed",
                payload={"plans": results},
            )

        return PhaseResult(
            status="success",
            return_code=RETURN_OK,
            next_phase="",
            processed_count=len(plans),
            success_count=success_count,
            failed_count=0,
            payload={"plans": results},
        )

    def _phase_not_implemented(self, phase_name, next_phase):
        return PhaseResult(
            status="success",
            return_code=RETURN_OK,
            next_phase=next_phase,
            processed_count=0,
            payload={
                "phase_name": phase_name,
                "implemented": False,
            },
        )
    
    
    def _emit_phase_summary(self, service, phase_name, result, when="END"):
        if not self.progress_callback:
            return

        if not isinstance(result, PhaseResult):
            self.progress_callback(
                f"[PHASE {when}] service={service.name} phase={phase_name} invalid_result_object",
                "error",
            )
            return

        payload = result.payload or {}

        self.progress_callback(
            (
                f"[PHASE {when}] "
                f"service={service.name} "
                f"phase={phase_name} "
                f"status={result.status} "
                f"return_code={result.return_code} "
                f"next_phase={result.next_phase or '-'} "
                f"processed={int(result.processed_count or 0)} "
                f"success={int(result.success_count or 0)} "
                f"failed={int(result.failed_count or 0)} "
                f"skipped={int(result.skipped_count or 0)} "
                f"retry_after={int(result.retry_after_seconds or 0)} "
                f"error={result.error_message or '-'} "
                f"payload_keys={','.join(sorted(payload.keys())) if isinstance(payload, dict) and payload else '-'}"
            ),
            "error" if result.status == "failed" else "warning" if result.status == "retry" else "info",
        )    


    def _emit(self, message, detail="", level="info"):
        if self.progress_callback:
            self.progress_callback(f"{message} {detail}".strip(), level=level)

    def request_stop(self, service_name):
        service = ReplicationService.objects.filter(name=service_name).first()
        if not service:
            raise SRDFOrchestratorError(f"ReplicationService '{service_name}' not found")

        state = self._get_or_create_runtime_state(service)
        state.stop_requested = True
        state.save(update_fields=["stop_requested", "updated_at"])
        
        
        return {
            "ok": True,
            "service_name": service.name,
            "action": "stop_requested",
        }


    def request_resume(self, service_name, from_phase=""):
        service = ReplicationService.objects.filter(name=service_name).first()
        if not service:
            raise SRDFOrchestratorError(f"ReplicationService '{service_name}' not found")

        state = self._get_or_create_runtime_state(service)
        state.is_paused = False
        state.stop_requested = False
        state.current_phase = self.PHASE_IDLE
        state.next_phase = from_phase or self.PHASE_CAPTURE
        state.last_error = ""
        state.save(
            update_fields=[
                "is_paused",
                "stop_requested",
                "current_phase",
                "next_phase",
                "last_error",
                "updated_at",
            ]
        )
        
        return {
            "ok": True,
            "service_name": service.name,
            "action": "resume_requested",
            "next_phase": state.next_phase,
        }
    
    
    def request_pause(self, service_name):
        service = ReplicationService.objects.filter(name=service_name).first()
        if not service:
            raise SRDFOrchestratorError(f"ReplicationService '{service_name}' not found")

        state = self._get_or_create_runtime_state(service)
        state.is_paused = True
        state.save(update_fields=["is_paused", "updated_at"])
        
        return {
            "ok": True,
            "service_name": service.name,
            "action": "pause_requested",
            "current_phase": state.current_phase or self.PHASE_PAUSED,
        }    
    
    def get_service_status(self, service_name=None):
        qs = (
            SRDFServiceRuntimeState.objects
            .select_related("replication_service")
            .order_by("replication_service__name")
        )
    
        if service_name:
            qs = qs.filter(replication_service__name=service_name)
    
        rows = []
        for state in qs:
            rows.append({
                "service_name": state.replication_service.name,
                "is_enabled": bool(state.is_enabled),
                "is_paused": bool(state.is_paused),
                "stop_requested": bool(state.stop_requested),
                "current_phase": state.current_phase,
                "next_phase": state.next_phase,
                "last_success_phase": state.last_success_phase,
                "cycle_number": int(state.cycle_number or 0),
                "last_return_code": state.last_return_code or "",
                "last_error": state.last_error or "",
                "last_run_started_at": state.last_run_started_at,
                "last_run_finished_at": state.last_run_finished_at,
            })
        return rows    