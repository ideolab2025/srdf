#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import time
import traceback

from django.core.management.base import BaseCommand
from django.utils import timezone

from srdf.services.srdf_orchestrator import SRDFOrchestrator, PhaseResult
from srdf.services.transport_applier import apply_inbound_events_once

DEFAULT_SLEEP = 10


class Command(BaseCommand):
    help = "SRDF remote daemon - target-side runtime for receive/apply cycles"

    def add_arguments(self, parser):
        parser.add_argument("--sleep", type=int, default=DEFAULT_SLEEP)
        parser.add_argument("--once", action="store_true")
        parser.add_argument("--service", action="append", dest="services", default=[])
        parser.add_argument("--stop-on-service-error", action="store_true")

        parser.add_argument("--pause-service", type=str, default="")
        parser.add_argument("--resume-service", type=str, default="")
        parser.add_argument("--resume-from-phase", type=str, default="")
        parser.add_argument("--stop-service", type=str, default="")
        parser.add_argument("--status", action="store_true")

    def handle(self, *args, **options):
        sleep_seconds = int(options.get("sleep") or DEFAULT_SLEEP)
        run_once = bool(options.get("once"))
        service_names = options.get("services") or []
        stop_on_service_error = bool(options.get("stop_on_service_error"))

        pause_service = (options.get("pause_service") or "").strip()
        resume_service = (options.get("resume_service") or "").strip()
        resume_from_phase = (options.get("resume_from_phase") or "").strip()
        stop_service = (options.get("stop_service") or "").strip()
        status_mode = bool(options.get("status"))

        orchestrator = SRDFRemoteOrchestrator(
            sleep_seconds=sleep_seconds,
            service_names=service_names,
            initiated_by="srdf_remote_daemon",
            stop_on_service_error=stop_on_service_error,
            progress_callback=self._progress,
        )

        if pause_service:
            result = orchestrator.request_pause(pause_service)
            self.stdout.write(
                self.style.WARNING(
                    f"Paused remote service={result['service_name']} current_phase={result['current_phase']}"
                )
            )
            return

        if resume_service:
            result = orchestrator.request_resume(
                resume_service,
                from_phase=(resume_from_phase or None),
            )
            self.stdout.write(
                self.style.SUCCESS(
                    f"Resumed remote service={result['service_name']} next_phase={result['next_phase']}"
                )
            )
            return

        if stop_service:
            result = orchestrator.request_stop(stop_service)
            self.stdout.write(
                self.style.WARNING(
                    f"Stop requested for remote service={result['service_name']}"
                )
            )
            return

        if status_mode:
            self._print_block(
                "SRDF REMOTE DAEMON STATUS",
                [
                    f"service_filter        : {service_names[0] if len(service_names) == 1 else 'ALL'}",
                    "explanation           : status is built from SRDFServiceRuntimeState rows on target side.",
                ],
            )
            rows = orchestrator.get_service_status(
                service_name=service_names[0] if len(service_names) == 1 else None
            )
            self._print_status(rows)
            return

        if run_once:
            self._print_block(
                "SRDF REMOTE DAEMON - ONE SHOT MODE",
                [
                    f"sleep_seconds         : {sleep_seconds}",
                    f"service_filter        : {', '.join(service_names) if service_names else 'ALL'}",
                    "explanation           : one remote orchestrator cycle will run now, without infinite loop.",
                ],
            )
            result = orchestrator.run_once()
            self._print_block(
                "SRDF REMOTE DAEMON - ONE SHOT COMPLETED",
                [
                    f"total_services        : {result.get('total_services', 0)}",
                    f"success_services      : {result.get('success_services', 0)}",
                    f"failed_services       : {result.get('failed_services', 0)}",
                    "explanation           : receive/apply runtime states and phase executions should now exist in SQL.",
                ],
            )
            return

        self._print_block(
            "SRDF REMOTE DAEMON STARTED",
            [
                f"sleep_seconds         : {sleep_seconds}",
                f"run_mode              : daemon",
                f"stop_on_service_error : {stop_on_service_error}",
                f"service_filter        : {', '.join(service_names) if service_names else 'ALL'}",
                "explanation           : the remote daemon will execute one orchestrator cycle, then sleep, then restart.",
            ],
        )

        while True:
            cycle_started_at = timezone.now()
            try:
                orchestrator.run_one_cycle()
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"FATAL REMOTE DAEMON ERROR: {exc}"))
                traceback.print_exc()

            duration = (timezone.now() - cycle_started_at).total_seconds()

            self._print_block(
                "SRDF REMOTE DAEMON CYCLE COMPLETED",
                [
                    f"duration_seconds      : {round(duration, 3)}",
                    f"sleep_seconds         : {sleep_seconds}",
                    "explanation           : one full remote orchestrator cycle has completed; daemon is going to sleep.",
                ],
            )

            time.sleep(sleep_seconds)

    def _progress(self, message, level="info"):
        level_text = str(level or "info").upper()
        ts = timezone.now().strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] [{level_text}] {message}"

        if level == "error":
            self.stderr.write(self.style.ERROR(line))
        elif level == "warning":
            self.stdout.write(self.style.WARNING(line))
        elif level == "success":
            self.stdout.write(self.style.SUCCESS(line))
        else:
            self.stdout.write(line)

    def _print_block(self, title, lines):
        safe_lines = [str(x) for x in (lines or [])]
        width = max([len(title)] + [len(x) for x in safe_lines] + [24])

        border = "=" * width
        self.stdout.write("")
        self.stdout.write(border)
        self.stdout.write(title)
        self.stdout.write(border)
        for line in safe_lines:
            self.stdout.write(line)
        self.stdout.write(border)

    def _print_status(self, rows):
        if not rows:
            self._print_block(
                "SRDF REMOTE STATUS - NO RUNTIME STATE",
                [
                    "message               : no runtime state found on target side.",
                    "explanation           : the remote daemon has probably never executed a cycle yet.",
                    "next_step             : run 'python manage.py srdf_remote_daemon --once'",
                ],
            )
            return

        headers = [
            "SERVICE",
            "ENABLED",
            "PAUSED",
            "STOP_REQ",
            "PHASE",
            "NEXT",
            "LAST_OK",
            "CYCLE",
            "RETURN_CODE",
        ]

        matrix = []
        for row in rows:
            matrix.append([
                row.get("service_name") or "",
                "Y" if row.get("is_enabled") else "N",
                "Y" if row.get("is_paused") else "N",
                "Y" if row.get("stop_requested") else "N",
                row.get("current_phase") or "",
                row.get("next_phase") or "",
                row.get("last_success_phase") or "",
                str(int(row.get("cycle_number") or 0)),
                row.get("last_return_code") or "",
            ])

        widths = []
        for idx, header in enumerate(headers):
            width = len(header)
            for row in matrix:
                width = max(width, len(str(row[idx])))
            widths.append(width)

        def fmt(values):
            return " | ".join(str(values[i]).ljust(widths[i]) for i in range(len(values)))

        sep = "-+-".join("-" * w for w in widths)

        self.stdout.write(fmt(headers))
        self.stdout.write(sep)
        for row in matrix:
            self.stdout.write(fmt(row))


class SRDFRemoteOrchestrator(SRDFOrchestrator):
    """
    Target-side orchestrator:
    - no capture
    - no deduplicate
    - no batch_build
    - no ship
    - focus on receive/apply
    """

    def _process_single_service(self, service, orchestrator_run):
        lock_token = __import__("uuid").uuid4().hex
        lock = self._acquire_service_lock(service=service, owner_token=lock_token)
        if not lock:
            self.progress_callback(f"Remote service {service.name} skipped (already locked)", "warning")
            return True

        runtime_state = self._get_or_create_runtime_state(service)

        try:
            if runtime_state.is_paused:
                runtime_state.current_phase = "paused"
                runtime_state.save(update_fields=["current_phase", "updated_at"])
                self.progress_callback(f"Remote service {service.name} paused", "warning")
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
                self.progress_callback(f"Remote service {service.name} stop requested", "warning")
                return True

            runtime_state.cycle_number = int(runtime_state.cycle_number or 0) + 1
            runtime_state.last_run_started_at = timezone.now()
            runtime_state.save(update_fields=["cycle_number", "last_run_started_at", "updated_at"])

            self.progress_callback(f"Processing remote service: {service.name}", "info")

            phases = [
                ("receive", self._phase_receive),
                ("apply", self._phase_apply),
                ("retry", self._phase_retry),
            ]

            for phase_name, phase_func in phases:
                success = self._execute_phase(phase_name, phase_func, service, runtime_state, orchestrator_run)
                if not success and self.stop_on_service_error:
                    return False

            runtime_state.current_phase = "idle"
            runtime_state.last_success_phase = "remote_full_cycle"
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
            runtime_state.last_return_code = "REMOTE_SERVICE_EXCEPTION"
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

            self.progress_callback(f"Remote service {service.name} failed: {e}", "error")
            return False

        finally:
            self._release_service_lock(service=service, owner_token=lock_token)
            
            
    def _phase_retry(self, service, state, request_payload):
        retry_limit = self._get_config_int(service, "remote_retry_limit", 100)

        failed_pending = self._count_inbound_by_status(service, ["failed"])
        if failed_pending <= 0:
            return PhaseResult(
                status="skipped",
                return_code="REMOTE_RETRY_EMPTY",
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "retry_limit": retry_limit,
                    "failed_pending": 0,
                },
            )

        result = apply_inbound_events_once(
            replication_service_id=service.id,
            limit=retry_limit,
            initiated_by=self.initiated_by,
            retry_failed=True,
        )

        callback_result = self._process_remote_batch_callbacks(service)

        processed_count = int(result.get("processed_count") or 0)
        applied_count = int(result.get("applied_count") or 0)
        failed_count = int(result.get("failed_count") or 0)

        if processed_count == 0:
            return PhaseResult(
                status="skipped",
                return_code="REMOTE_RETRY_NO_WORK",
                next_phase="",
                processed_count=0,
                success_count=0,
                failed_count=0,
                skipped_count=1,
                payload={
                    "replication_service_id": service.id,
                    "retry_limit": retry_limit,
                    "retry_result": result,
                    "callback_result": callback_result,
                },
            )

        if failed_count > 0:
            self.progress_callback(
                f"Remote retry partial service={service.name} processed={processed_count} applied={applied_count} failed={failed_count}",
                "warning",
            )
            return PhaseResult(
                status="retry",
                return_code="REMOTE_RETRY_PARTIAL",
                next_phase="",
                processed_count=processed_count,
                success_count=applied_count,
                failed_count=failed_count,
                skipped_count=0,
                retry_after_seconds=self.sleep_seconds,
                payload={
                    "replication_service_id": service.id,
                    "retry_limit": retry_limit,
                    "retry_result": result,
                    "callback_result": callback_result,
                },
            )

        self.progress_callback(
            f"Remote retry OK service={service.name} processed={processed_count} applied={applied_count}",
            "info",
        )

        return PhaseResult(
            status="success",
            return_code="REMOTE_RETRY_OK",
            next_phase="",
            processed_count=processed_count,
            success_count=applied_count,
            failed_count=0,
            skipped_count=0,
            payload={
                "replication_service_id": service.id,
                "retry_limit": retry_limit,
                "retry_result": result,
                "callback_result": callback_result,
            },
        )    