#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import time

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.models import AuditEvent, ReplicationService
from srdf.services.binlog_capture import capture_binlog_once
from srdf.services.locks import acquire_lock, release_lock, LockError


class Command(BaseCommand):
    help = "SRDF capture agent - service-ready loop for continuous binlog capture"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, required=True, help="ReplicationService ID")
        parser.add_argument("--interval-seconds", type=int, default=5, help="Sleep interval between iterations")
        parser.add_argument("--limit", type=int, default=500, help="Capture limit per iteration")
        parser.add_argument(
            "--initialize-on-start",
            action="store_true",
            help="Initialize checkpoint on agent startup before entering loop",
        )
        parser.add_argument(
            "--force-current-pos-on-start",
            action="store_true",
            help="Force checkpoint to current master status on startup",
        )
        parser.add_argument(
            "--max-iterations",
            type=int,
            default=0,
            help="Optional max iterations for controlled runs (0 = infinite)",
        )
        parser.add_argument(
            "--idle-break-seconds",
            type=int,
            default=0,
            help="Optional stop after N idle seconds (0 = disabled)",
        )

    def handle(self, *args, **options):
        service_id = int(options["service_id"])
        interval_seconds = max(1, int(options["interval_seconds"] or 5))
        limit = max(1, int(options["limit"] or 500))
        initialize_on_start = bool(options["initialize_on_start"])
        force_current_pos_on_start = bool(options["force_current_pos_on_start"])
        max_iterations = max(0, int(options["max_iterations"] or 0))
        idle_break_seconds = max(0, int(options["idle_break_seconds"] or 0))

        service = (
            ReplicationService.objects
            .select_related("source_node", "target_node")
            .filter(id=service_id, is_enabled=True)
            .first()
        )
        if not service:
            raise CommandError(f"ReplicationService #{service_id} not found")

        lock_name = f"srdf_capture_agent_service_{service.id}"

        try:
            acquire_lock(lock_name)
        except LockError as exc:
            raise CommandError(str(exc))

        started_at = timezone.now()
        iteration = 0
        total_captured = 0
        total_skipped = 0
        total_ignored_older = 0
        last_activity_at = timezone.now()

        AuditEvent.objects.create(
            event_type="srdf_capture_agent_started",
            level="warning",
            node=service.source_node,
            message=f"SRDF capture agent started for service '{service.name}'",
            created_by="srdf_capture_agent",
            payload={
                "replication_service_id": service.id,
                "interval_seconds": interval_seconds,
                "limit": limit,
                "initialize_on_start": initialize_on_start,
                "force_current_pos_on_start": force_current_pos_on_start,
                "max_iterations": max_iterations,
                "idle_break_seconds": idle_break_seconds,
            },
        )

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF CAPTURE AGENT - START")
        self.stdout.write("============================================================")
        self.stdout.write(f"service_id={service.id}")
        self.stdout.write(f"service_name={service.name}")
        self.stdout.write(f"interval_seconds={interval_seconds}")
        self.stdout.write(f"limit={limit}")
        self.stdout.write(f"max_iterations={max_iterations}")
        self.stdout.write(f"idle_break_seconds={idle_break_seconds}")
        self.stdout.write("")

        try:
            if initialize_on_start:
                init_result = capture_binlog_once(
                    replication_service_id=service.id,
                    limit=0,
                    initiated_by="srdf_capture_agent:init",
                    initialize_only=True,
                    force_current_pos=force_current_pos_on_start,
                )
                self.stdout.write(
                    self.style.WARNING(
                        f"[INIT] log_file={init_result.get('log_file')} log_pos={init_result.get('log_pos')}"
                    )
                )

            while True:
                iteration += 1
                loop_started = timezone.now()

                try:
                    result = capture_binlog_once(
                        replication_service_id=service.id,
                        limit=limit,
                        initiated_by="srdf_capture_agent",
                    )

                    captured = int(result.get("captured_count") or 0)
                    skipped = int(result.get("skipped_count") or 0)
                    ignored_older = int(result.get("ignored_older_count") or 0)

                    total_captured += captured
                    total_skipped += skipped
                    total_ignored_older += ignored_older

                    if captured > 0:
                        last_activity_at = timezone.now()

                    self.stdout.write(
                        f"[LOOP #{iteration}] captured={captured} skipped={skipped} "
                        f"ignored_older={ignored_older} log_file={result.get('log_file')} "
                        f"log_pos={result.get('log_pos')}"
                    )

                except Exception as exc:
                    AuditEvent.objects.create(
                        event_type="srdf_capture_agent_iteration_error",
                        level="error",
                        node=service.source_node,
                        message=f"SRDF capture agent iteration failed for service '{service.name}'",
                        created_by="srdf_capture_agent",
                        payload={
                            "replication_service_id": service.id,
                            "iteration": iteration,
                            "error": str(exc),
                        },
                    )
                    self.stdout.write(self.style.ERROR(f"[LOOP #{iteration}] ERROR {str(exc)}"))

                if max_iterations and iteration >= max_iterations:
                    self.stdout.write(self.style.WARNING("[AGENT] max_iterations reached"))
                    break

                if idle_break_seconds > 0:
                    idle_seconds = int((timezone.now() - last_activity_at).total_seconds())
                    if idle_seconds >= idle_break_seconds:
                        self.stdout.write(
                            self.style.WARNING(
                                f"[AGENT] idle_break reached after {idle_seconds}s"
                            )
                        )
                        break

                elapsed_loop = (timezone.now() - loop_started).total_seconds()
                sleep_seconds = max(0, interval_seconds - elapsed_loop)
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)

        except KeyboardInterrupt:
            self.stdout.write(self.style.WARNING("[AGENT] interrupted by user"))

        finally:
            release_lock(lock_name)

            duration = round((timezone.now() - started_at).total_seconds(), 3)

            AuditEvent.objects.create(
                event_type="srdf_capture_agent_stopped",
                level="warning",
                node=service.source_node,
                message=f"SRDF capture agent stopped for service '{service.name}'",
                created_by="srdf_capture_agent",
                payload={
                    "replication_service_id": service.id,
                    "iterations": iteration,
                    "total_captured": total_captured,
                    "total_skipped": total_skipped,
                    "total_ignored_older": total_ignored_older,
                    "duration_seconds": duration,
                },
            )

            self.stdout.write("")
            self.stdout.write("============================================================")
            self.stdout.write("SRDF CAPTURE AGENT - STOP")
            self.stdout.write("============================================================")
            self.stdout.write(f"iterations={iteration}")
            self.stdout.write(f"total_captured={total_captured}")
            self.stdout.write(f"total_skipped={total_skipped}")
            self.stdout.write(f"total_ignored_older={total_ignored_older}")
            self.stdout.write(f"duration_seconds={duration}")
            self.stdout.write("")