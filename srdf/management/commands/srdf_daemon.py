#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import time
import traceback

from django.core.management.base import BaseCommand
from django.utils import timezone

from srdf.services.srdf_orchestrator import SRDFOrchestrator


DEFAULT_SLEEP = 10


class Command(BaseCommand):
    help = "Final SRDF daemon - central runtime for all SRDF cron phases"

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

        orchestrator = SRDFOrchestrator(
            sleep_seconds=sleep_seconds,
            service_names=service_names,
            initiated_by="srdf_daemon",
            stop_on_service_error=stop_on_service_error,
            progress_callback=self._progress,
        )

        if pause_service:
            result = orchestrator.request_pause(pause_service)
            self.stdout.write(
                self.style.WARNING(
                    f"Paused service={result['service_name']} current_phase={result['current_phase']}"
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
                    f"Resumed service={result['service_name']} next_phase={result['next_phase']}"
                )
            )
            return

        if stop_service:
            result = orchestrator.request_stop(stop_service)
            self.stdout.write(
                self.style.WARNING(
                    f"Stop requested for service={result['service_name']}"
                )
            )
            return



        if status_mode:
            self._print_block(
                "SRDF DAEMON STATUS",
                [
                    f"service_filter        : {service_names[0] if len(service_names) == 1 else 'ALL'}",
                    "explanation           : status is built from SRDFServiceRuntimeState rows.",
                ],
            )
            rows = orchestrator.get_service_status(
                service_name=service_names[0] if len(service_names) == 1 else None
            )
            self._print_status(rows)
            return        
        

        if run_once:
            self._print_block(
                "SRDF DAEMON - ONE SHOT MODE",
                [
                    f"sleep_seconds         : {sleep_seconds}",
                    f"service_filter        : {', '.join(service_names) if service_names else 'ALL'}",
                    "explanation           : one orchestrator cycle will run now, without infinite loop.",
                ],
            )
            result = orchestrator.run_once()
            self._print_block(
                "SRDF DAEMON - ONE SHOT COMPLETED",
                [
                    f"total_services        : {result.get('total_services', 0)}",
                    f"success_services      : {result.get('success_services', 0)}",
                    f"failed_services       : {result.get('failed_services', 0)}",
                    "explanation           : runtime states and phase executions should now exist in SQL.",
                ],
            )
            return

        self._print_block(
            "SRDF FINAL DAEMON STARTED",
            [
                f"sleep_seconds         : {sleep_seconds}",
                f"run_mode              : daemon",
                f"stop_on_service_error : {stop_on_service_error}",
                f"service_filter        : {', '.join(service_names) if service_names else 'ALL'}",
                "explanation           : the daemon will execute one orchestrator cycle, then sleep, then restart.",
            ],
        )


        while True:
            cycle_started_at = timezone.now()
            try:
                orchestrator.run_one_cycle()
            except Exception as exc:
                self.stderr.write(self.style.ERROR(f"FATAL DAEMON ERROR: {exc}"))
                traceback.print_exc()

            duration = (timezone.now() - cycle_started_at).total_seconds()
            
            
            self._print_block(
                "SRDF DAEMON CYCLE COMPLETED",
                [
                    f"duration_seconds      : {round(duration, 3)}",
                    f"sleep_seconds         : {sleep_seconds}",
                    "explanation           : one full orchestrator cycle has completed; daemon is going to sleep.",
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
                "SRDF STATUS - NO RUNTIME STATE",
                [
                    "message               : no runtime state found.",
                    "explanation           : the daemon has probably never executed a cycle yet.",
                    "next_step             : run 'python manage.py srdf_daemon --once'",
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