#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand
from django.utils import timezone

from srdf.models import AuditEvent, Node, ReplicationService
from srdf.services.node_api_client import get_status, ping_node
from srdf.services.replication_status_sync import sync_replication_statuses_for_node
from srdf.services.mariadb_replication import get_replication_status

class Command(BaseCommand):
    help = "Poll enabled SRDF nodes and refresh their status"

    def add_arguments(self, parser):
        parser.add_argument(
            "--node-id",
            type=int,
            default=None,
            help="Poll only one node by ID",
        )
        
    def _get_mariadb_config_for_node(self, node):
        replication_service = (
            ReplicationService.objects.filter(
                service_type="mariadb",
                is_enabled=True,
            )
            .filter(source_node=node)
            .order_by("id")
            .first()
        )

        if not replication_service:
            replication_service = (
                ReplicationService.objects.filter(
                    service_type="mariadb",
                    is_enabled=True,
                )
                .filter(target_node=node)
                .order_by("id")
                .first()
            )

        if not replication_service:
            return None

        config = replication_service.config or {}
        if not isinstance(config, dict):
            return None

        return config    

    def handle(self, *args, **options):
        qs = Node.objects.filter(is_enabled=True).order_by("id")
        node_id = options.get("node_id")
        if node_id:
            qs = qs.filter(id=node_id)

        total = qs.count()
        ok_count = 0
        error_count = 0
        degraded_count = 0
        replication_updates_total = 0

        self.stdout.write(f"SRDF polling started. nodes={total}")

        for node in qs:
            try:
                ping_payload = ping_node(node)
                status_payload = get_status(node)
                
                try:
                    config = self._get_mariadb_config_for_node(node)
                    if config:
                        mariadb_status = get_replication_status(config)
                        status_payload.setdefault("services", {})["mariadb"] = mariadb_status
                except Exception as exc:
                    status_payload.setdefault("services", {})["mariadb"] = {
                        "error": str(exc),
                        "running": False,
                    }                    
                    

                node.status = self._compute_node_status(status_payload)
                node.last_seen_at = timezone.now()
                node.last_status_payload = {
                    "ping": ping_payload,
                    "status": status_payload,
                }
                node.save(
                    update_fields=[
                        "status",
                        "last_seen_at",
                        "last_status_payload",
                        "updated_at",
                    ]
                )

                replication_updates = sync_replication_statuses_for_node(node, status_payload)
                replication_updates_total += replication_updates

                if node.status == "degraded":
                    degraded_count += 1

                AuditEvent.objects.create(
                    event_type="node_poll_success",
                    level="info" if node.status == "online" else "warning",
                    node=node,
                    message=f"Node poll success: {node.name} -> {node.status}",
                    created_by="srdf_poll_nodes",
                    payload={
                        "ping": ping_payload,
                        "status_summary": self._extract_status_summary(status_payload),
                        "replication_updates": replication_updates,
                    },
                )

                ok_count += 1
                self.stdout.write(self.style.SUCCESS(f"[OK] {node.name} -> {node.status}"))

            except Exception as exc:
                node.status = "offline"
                node.save(update_fields=["status", "updated_at"])

                AuditEvent.objects.create(
                    event_type="node_poll_error",
                    level="error",
                    node=node,
                    message=f"Node poll failed: {node.name}",
                    created_by="srdf_poll_nodes",
                    payload={
                        "error": str(exc),
                    },
                )

                error_count += 1
                self.stdout.write(self.style.ERROR(f"[ERROR] {node.name} -> {exc}"))

        AuditEvent.objects.create(
            event_type="node_poll_run_summary",
            level="info" if error_count == 0 else "warning",
            message=(
                f"SRDF polling finished. "
                f"ok={ok_count} error={error_count} degraded={degraded_count} "
                f"replication_updates={replication_updates_total} total={total}"
            ),
            created_by="srdf_poll_nodes",
            payload={
                "ok_count": ok_count,
                "error_count": error_count,
                "degraded_count": degraded_count,
                "replication_updates_total": replication_updates_total,
                "total": total,
            },
        )

        self.stdout.write(
            self.style.WARNING(
                f"SRDF polling finished. ok={ok_count} error={error_count} degraded={degraded_count} "
                f"replication_updates={replication_updates_total} total={total}"
            )
        )

    def _compute_node_status(self, status_payload):
        if not isinstance(status_payload, dict):
            return "degraded"

        services = status_payload.get("services") or {}
        if not isinstance(services, dict):
            return "online"

        critical_errors = 0

        for _, service_data in services.items():
            if not isinstance(service_data, dict):
                continue

            installed = service_data.get("installed", True)
            running = service_data.get("running", True)

            if installed and running is False:
                critical_errors += 1

        if critical_errors > 0:
            return "degraded"

        return "online"

    def _extract_status_summary(self, status_payload):
        if not isinstance(status_payload, dict):
            return {}

        services = status_payload.get("services") or {}
        summary = {}

        for service_name, service_data in services.items():
            if isinstance(service_data, dict):
                summary[service_name] = {
                    "running": service_data.get("running"),
                    "installed": service_data.get("installed"),
                    "is_primary": service_data.get("is_primary"),
                    "replication_lag_seconds": service_data.get("replication_lag_seconds"),
                }

        return summary