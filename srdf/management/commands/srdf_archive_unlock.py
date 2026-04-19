#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand
from django.utils import timezone

from srdf.models import SRDFArchiveExecutionLock, AuditEvent


class Command(BaseCommand):
    help = "Release SRDF archive execution lock"

    def add_arguments(self, parser):
        parser.add_argument(
            "--lock-name",
            type=str,
            default="srdf_archive_execution_lock",
            help="Archive lock name",
        )
        parser.add_argument(
            "--delete",
            action="store_true",
            help="Delete the lock row instead of resetting it",
        )

    def handle(self, *args, **options):
        lock_name = str(options.get("lock_name") or "srdf_archive_execution_lock").strip()
        delete_mode = bool(options.get("delete"))

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE UNLOCK COMMAND - START"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"lock_name={lock_name}")
        self.stdout.write(f"delete_mode={delete_mode}")
        self.stdout.write("")

        lock = SRDFArchiveExecutionLock.objects.filter(name=lock_name).first()

        if not lock:
            self.stdout.write(self.style.WARNING("[UNLOCK] No archive lock found"))
            self.stdout.write("")
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write(self.style.WARNING("SRDF ARCHIVE UNLOCK COMMAND - END"))
            self.stdout.write(self.style.WARNING("============================================================"))
            self.stdout.write("")
            return

        if delete_mode:
            lock_id = lock.id
            lock.delete()

            AuditEvent.objects.create(
                event_type="srdf_archive_lock_deleted",
                level="warning",
                node=None,
                message=f"SRDF archive lock '{lock_name}' deleted",
                created_by="srdf_archive_unlock",
                payload={
                    "lock_id": lock_id,
                    "lock_name": lock_name,
                    "mode": "delete",
                    "executed_at": timezone.now().isoformat(),
                },
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f"[UNLOCK][OK] archive lock deleted lock_id={lock_id} lock_name={lock_name}"
                )
            )
        else:
            lock.is_locked = False
            lock.owner_token = ""
            lock.locked_at = None
            lock.expires_at = None
            lock.save(
                update_fields=[
                    "is_locked",
                    "owner_token",
                    "locked_at",
                    "expires_at",
                    "updated_at",
                ]
            )

            AuditEvent.objects.create(
                event_type="srdf_archive_lock_released",
                level="warning",
                node=None,
                message=f"SRDF archive lock '{lock_name}' released",
                created_by="srdf_archive_unlock",
                payload={
                    "lock_id": lock.id,
                    "lock_name": lock.name,
                    "mode": "reset",
                    "executed_at": timezone.now().isoformat(),
                },
            )

            self.stdout.write(
                self.style.SUCCESS(
                    f"[UNLOCK][OK] archive lock released lock_id={lock.id} lock_name={lock.name}"
                )
            )

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE UNLOCK COMMAND - END"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write("")