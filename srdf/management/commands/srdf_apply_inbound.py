#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.models import InboundChangeEvent, ReplicationService
from srdf.services.transport_applier import apply_inbound_events_once


class Command(BaseCommand):
    help = "Apply inbound replication events on target database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--service-id",
            type=int,
            required=True,
            help="ReplicationService ID",
        )

        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Max number of inbound events to process",
        )

        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Do not apply SQL, only count events",
        )
        
        parser.add_argument(
            "--retry-failed",
            action="store_true",
            help="Also retry inbound events currently in failed status",
        )        

    def handle(self, *args, **options):
        service_id = options["service_id"]
        limit = options["limit"]
        dry_run = options["dry_run"]
        retry_failed = options["retry_failed"]

        started_at = timezone.now()

        self.stdout.write("")
        self.stdout.write("========================================")
        self.stdout.write(" SRDF APPLY INBOUND")
        self.stdout.write("========================================")

        # --- validation service ---
        try:
            service = ReplicationService.objects.get(id=service_id, is_enabled=True)
        except ReplicationService.DoesNotExist:
            raise CommandError(f"ReplicationService #{service_id} not found or disabled")

        self.stdout.write(f"Service        : {service.name}")
        self.stdout.write(f"Source node    : {service.source_node.name}")
        self.stdout.write(f"Target node    : {service.target_node.name}")
        self.stdout.write(f"Limit          : {limit}")
        self.stdout.write(f"Dry run        : {dry_run}")
        self.stdout.write(f"Retry failed   : {retry_failed}")
        self.stdout.write("")

        # --- stats BEFORE ---
        qs = InboundChangeEvent.objects.filter(replication_service=service)

        total_received = qs.filter(status="received").count()
        total_failed = qs.filter(status="failed").count()
        total_applied = qs.filter(status="applied").count()

        self.stdout.write("---- BEFORE ----")
        self.stdout.write(f"received : {total_received}")
        self.stdout.write(f"applied  : {total_applied}")
        self.stdout.write(f"failed   : {total_failed}")
        self.stdout.write("")

        if dry_run:
            to_process = qs.filter(status="received")[:limit].count()
            self.stdout.write("---- DRY RUN ----")
            self.stdout.write(f"would process : {to_process}")
            self.stdout.write("No SQL executed")
            return

        # --- EXECUTION ---
        result = apply_inbound_events_once(
            replication_service_id=service_id,
            limit=limit,
            initiated_by="cli_apply_inbound",
            retry_failed=retry_failed,
        )

        # --- stats AFTER ---
        total_received_after = qs.filter(status="received").count()
        total_failed_after = qs.filter(status="failed").count()
        total_applied_after = qs.filter(status="applied").count()

        duration = (timezone.now() - started_at).total_seconds()

        self.stdout.write("")
        self.stdout.write("---- RESULT ----")
        self.stdout.write(f"processed : {result.get('processed_count')}")
        self.stdout.write(f"applied   : {result.get('applied_count')}")
        self.stdout.write(f"failed    : {result.get('failed_count')}")
        self.stdout.write("")

        self.stdout.write("---- AFTER ----")
        self.stdout.write(f"received : {total_received_after}")
        self.stdout.write(f"applied  : {total_applied_after}")
        self.stdout.write(f"failed   : {total_failed_after}")
        self.stdout.write("")

        self.stdout.write(f"Duration : {duration:.2f}s")

        self.stdout.write("")
        self.stdout.write("========================================")
        self.stdout.write(" DONE")
        self.stdout.write("========================================")
        self.stdout.write("")