#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import (
    InboundChangeEvent,
    OutboundChangeEvent,
    ReplicationCheckpoint,
    ReplicationService,
    TransportBatch,
)


class Command(BaseCommand):
    help = "Display SRDF monitoring status for one ReplicationService"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, required=True)

    def handle(self, *args, **options):
        service_id = int(options["service_id"])

        service = (
            ReplicationService.objects
            .select_related("source_node", "target_node")
            .filter(id=service_id, is_enabled=True)
            .first()
        )
        if not service:
            raise CommandError(f"ReplicationService #{service_id} not found")

        outbound_pending = OutboundChangeEvent.objects.filter(replication_service=service, status="pending").count()
        outbound_batched = OutboundChangeEvent.objects.filter(replication_service=service, status="batched").count()
        outbound_sent = OutboundChangeEvent.objects.filter(replication_service=service, status="sent").count()
        outbound_acked = OutboundChangeEvent.objects.filter(replication_service=service, status="acked").count()
        outbound_failed = OutboundChangeEvent.objects.filter(replication_service=service, status="failed").count()
        outbound_dead = OutboundChangeEvent.objects.filter(replication_service=service, status="dead").count()

        inbound_received = InboundChangeEvent.objects.filter(replication_service=service, status="received").count()
        inbound_applied = InboundChangeEvent.objects.filter(replication_service=service, status="applied").count()
        inbound_failed = InboundChangeEvent.objects.filter(replication_service=service, status="failed").count()
        inbound_dead = InboundChangeEvent.objects.filter(replication_service=service, status="dead").count()

        batch_ready = TransportBatch.objects.filter(replication_service=service, status="ready").count()
        batch_sent = TransportBatch.objects.filter(replication_service=service, status="sent").count()
        batch_acked = TransportBatch.objects.filter(replication_service=service, status="acked").count()
        batch_failed = TransportBatch.objects.filter(replication_service=service, status="failed").count()

        checkpoints = ReplicationCheckpoint.objects.filter(replication_service=service).order_by("direction")

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF MONITOR STATUS")
        self.stdout.write("============================================================")
        self.stdout.write(f"service       : {service.name} (id={service.id})")
        self.stdout.write(f"source_node   : {service.source_node.name}")
        self.stdout.write(f"target_node   : {service.target_node.name}")
        self.stdout.write("")

        self.stdout.write("---- OUTBOUND ----")
        self.stdout.write(f"pending : {outbound_pending}")
        self.stdout.write(f"batched : {outbound_batched}")
        self.stdout.write(f"sent    : {outbound_sent}")
        self.stdout.write(f"acked   : {outbound_acked}")
        self.stdout.write(f"failed  : {outbound_failed}")
        self.stdout.write(f"dead    : {outbound_dead}")
        self.stdout.write("")

        self.stdout.write("---- INBOUND ----")
        self.stdout.write(f"received : {inbound_received}")
        self.stdout.write(f"applied  : {inbound_applied}")
        self.stdout.write(f"failed   : {inbound_failed}")
        self.stdout.write(f"dead     : {inbound_dead}")
        self.stdout.write("")

        self.stdout.write("---- BATCHES ----")
        self.stdout.write(f"ready  : {batch_ready}")
        self.stdout.write(f"sent   : {batch_sent}")
        self.stdout.write(f"acked  : {batch_acked}")
        self.stdout.write(f"failed : {batch_failed}")
        self.stdout.write("")

        self.stdout.write("---- CHECKPOINTS ----")
        for cp in checkpoints:
            self.stdout.write(
                f"{cp.direction:<8} log_file={cp.log_file} log_pos={cp.log_pos} updated_at={cp.updated_at}"
            )

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("")