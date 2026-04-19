#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import time
import traceback

from django.core.management.base import BaseCommand
from django.utils import timezone

from srdf.models import ReplicationService, AuditEvent
from srdf.services.locks import acquire_lock, release_lock, LockError

from srdf.services.binlog_capture import capture_binlog_once
from srdf.services.transport_batch_builder import build_transport_batch_once
from srdf.services.transport_sender import send_transport_batch_once
from srdf.services.transport_receiver import receive_transport_batch_once
from srdf.services.transport_applier import apply_inbound_events_once

class Command(BaseCommand):
    help = "SRDF continuous pipeline agent (capture → build → send → apply)"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, required=True)
        parser.add_argument("--interval", type=int, default=2)
        parser.add_argument("--limit", type=int, default=100)
        parser.add_argument("--loop", action="store_true")
        parser.add_argument("--once", action="store_true")

    def handle(self, *args, **options):
        service_id = options["service_id"]
        interval = int(options.get("interval") or 2)
        limit = int(options.get("limit") or 100)
        loop = bool(options.get("loop"))
        once = bool(options.get("once"))

        service = ReplicationService.objects.filter(id=service_id, is_enabled=True).first()
        if not service:
            self.stderr.write(f"Service #{service_id} not found or disabled")
            return

        lock_name = f"srdf_pipeline_{service_id}"

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("================================================="))
        self.stdout.write(self.style.WARNING("SRDF PIPELINE AGENT START"))
        self.stdout.write(self.style.WARNING("================================================="))
        self.stdout.write(f"service={service.name} id={service.id}")
        self.stdout.write(f"interval={interval}s limit={limit}")
        self.stdout.write("")

        while True:
            cycle_started = timezone.now()

            try:
                acquire_lock(lock_name)

                self.stdout.write(self.style.WARNING("[PIPELINE] cycle start"))

                # -------------------------------------------------
                # 1. CAPTURE
                # -------------------------------------------------
                capture_result = capture_binlog_once(
                    replication_service_id=service.id,
                    limit=limit,
                    initiated_by="pipeline_agent:capture",
                )

                self.stdout.write(
                    f"[CAPTURE] captured={capture_result.get('captured_count')} "
                    f"skipped={capture_result.get('skipped_count')} "
                    f"ignored_older={capture_result.get('ignored_older_count')}"
                )
                
                # -------------------------------------------------
                # 2. BUILD
                # -------------------------------------------------
                build_result = build_transport_batch_once(
                    replication_service_id=service.id,
                    limit=limit,
                    initiated_by="pipeline_agent:build",
                )

                batch_id = build_result.get("transport_batch_id")


                if batch_id:
                    self.stdout.write(
                        self.style.SUCCESS(
                            f"[BUILD] batch_id={batch_id} batch_uid={build_result.get('batch_uid')}"
                        )
                    )                    
                else:
                    self.stdout.write("[BUILD] no batch")

                # -------------------------------------------------
                # 3. SEND
                # -------------------------------------------------
                if batch_id:
                    try:
                        send_result = send_transport_batch_once(
                            batch_id=batch_id,
                            initiated_by="pipeline_agent:send",
                        )

                        self.stdout.write(
                            self.style.SUCCESS(
                                f"[SEND] batch_id={batch_id} sent status={send_result.get('status')}"
                            )
                        )
                        
                    except Exception as exc:
                        self.stdout.write(
                            self.style.ERROR(f"[SEND][ERROR] {str(exc)}")
                        )

                # -------------------------------------------------
                # 4. APPLY
                # -------------------------------------------------
                apply_result = apply_inbound_events_once(
                    replication_service_id=service.id,
                    limit=limit,
                    initiated_by="pipeline_agent:apply",
                    retry_failed=True,
                )

                self.stdout.write(
                    self.style.SUCCESS(
                        f"[APPLY] processed={apply_result.get('processed_count')} "
                        f"applied={apply_result.get('applied_count')} "
                        f"failed={apply_result.get('failed_count')}"
                    )
                )
                
                duration = (timezone.now() - cycle_started).total_seconds()

                self.stdout.write(
                    self.style.SUCCESS(
                        f"[PIPELINE][OK] cycle_duration={round(duration, 3)}s"
                    )
                )

            except LockError:
                self.stdout.write("[PIPELINE] already running, skipping cycle")

            except Exception as exc:
                self.stdout.write(self.style.ERROR("[PIPELINE][FATAL]"))
                self.stdout.write(str(exc))
                self.stdout.write(traceback.format_exc())

                AuditEvent.objects.create(
                    event_type="pipeline_agent_error",
                    level="error",
                    message="Pipeline agent failure",
                    payload={
                        "service_id": service.id,
                        "error": str(exc),
                    },
                )

            finally:
                try:
                    release_lock(lock_name)
                except Exception:
                    pass

            if once:
                break

            if not loop:
                break

            time.sleep(interval)