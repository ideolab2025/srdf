#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json
import time
import uuid

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.models import ReplicationService, SRDFCronExecution
from srdf.services.archive_engine import run_archive_once


class Command(BaseCommand):
    help = "SRDF archive command"

    def add_arguments(self, parser):
        parser.add_argument(
            "--service-id",
            type=int,
            default=None,
            help="Optional ReplicationService ID",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Analyze archive eligibility without inserting/deleting rows",
        )
        parser.add_argument(
            "--trigger-mode",
            type=str,
            default="manual",
            choices=["manual", "scheduled", "orchestrator", "recovery"],
            help="Archive trigger mode",
        )

    def handle(self, *args, **options):
        service_id = options.get("service_id")
        dry_run = bool(options.get("dry_run"))
        trigger_mode = str(options.get("trigger_mode") or "manual").strip()

        execution_uid = f"srdf-archive-{timezone.now().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"
        command_line = self._build_command_line(service_id=service_id, dry_run=dry_run, trigger_mode=trigger_mode)

        service = None
        if service_id:
            service = ReplicationService.objects.filter(id=service_id).first()
            if not service:
                raise CommandError(f"ReplicationService #{service_id} not found")

        cron_execution = SRDFCronExecution.objects.create(
            execution_uid=execution_uid,
            cron_name="srdf_archive",
            cron_type="other",
            replication_service=service,
            node=service.source_node if service else None,
            engine=service.service_type if service else "",
            command_line=command_line,
            initiated_by="srdf_archive",
            status="running",
            input_params={
                "service_id": service_id,
                "dry_run": dry_run,
                "trigger_mode": trigger_mode,
            },
        )

        started_at = timezone.now()
        started_perf = time.perf_counter()

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE COMMAND - START"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"service_id={service_id}")
        self.stdout.write(f"dry_run={dry_run}")
        self.stdout.write(f"trigger_mode={trigger_mode}")
        self.stdout.write("")

        progress_messages = []

        def progress_callback(message, level="info"):
            progress_messages.append({
                "level": level,
                "message": message,
                "at": timezone.now().isoformat(),
            })

            if level == "error":
                self.stdout.write(self.style.ERROR(message))
            elif level == "warning":
                self.stdout.write(self.style.WARNING(message))
            else:
                self.stdout.write(message)

        try:
            result = run_archive_once(
                replication_service_id=service_id,
                initiated_by="srdf_archive",
                trigger_mode=trigger_mode,
                dry_run=dry_run,
                progress_callback=progress_callback,
            )

            finished_at = timezone.now()
            duration_seconds = round(time.perf_counter() - started_perf, 3)

            cron_execution.status = "success" if result.get("ok") else "warning"
            cron_execution.return_code = "ARCHIVE_OK" if result.get("ok") else "ARCHIVE_PARTIAL"
            cron_execution.finished_at = finished_at
            cron_execution.duration_seconds = duration_seconds
            cron_execution.result_payload = result
            cron_execution.progress_payload = {
                "messages": progress_messages,
            }
            cron_execution.save(
                update_fields=[
                    "status",
                    "return_code",
                    "finished_at",
                    "duration_seconds",
                    "result_payload",
                    "progress_payload",
                    "updated_at",
                ]
            )

            self.stdout.write("")
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write(self.style.WARNING("SRDF ARCHIVE COMMAND - FINAL SUMMARY"))
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write(f"ok={result.get('ok')}")
            self.stdout.write(f"message={result.get('message')}")
            self.stdout.write(f"duration_seconds={duration_seconds}")
            self.stdout.write("JSON_RESULT_BEGIN")
            self.stdout.write(json.dumps(result, indent=2, ensure_ascii=False, default=str))
            self.stdout.write("JSON_RESULT_END")
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write(self.style.WARNING("SRDF ARCHIVE COMMAND - END"))
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write("")

            if not result.get("ok"):
                raise CommandError(result.get("message") or "SRDF archive completed with warnings")

        except Exception as exc:
            finished_at = timezone.now()
            duration_seconds = round(time.perf_counter() - started_perf, 3)

            cron_execution.status = "error"
            cron_execution.return_code = "ARCHIVE_ERROR"
            cron_execution.error_code = "ARCHIVE_EXCEPTION"
            cron_execution.error_message = str(exc)
            cron_execution.finished_at = finished_at
            cron_execution.duration_seconds = duration_seconds
            cron_execution.result_payload = {
                "error": str(exc),
            }
            cron_execution.progress_payload = {
                "messages": progress_messages,
            }
            cron_execution.save(
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

            raise

    def _build_command_line(self, service_id=None, dry_run=False, trigger_mode="manual"):
        parts = [
            "python manage.py srdf_archive",
        ]
        if service_id:
            parts.append(f"--service-id={service_id}")
        if dry_run:
            parts.append("--dry-run")
        if trigger_mode:
            parts.append(f"--trigger-mode={trigger_mode}")
        return " ".join(parts)