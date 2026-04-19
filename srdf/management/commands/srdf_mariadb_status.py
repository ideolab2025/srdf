#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand
from django.db.models import Q

from srdf.models import AuditEvent, Node, ReplicationService
from srdf.services.mariadb_replication import get_replication_status


class Command(BaseCommand):
    help = "Check MariaDB replication status for SRDF nodes"

    def add_arguments(self, parser):
        parser.add_argument(
            "--node-id",
            type=int,
            default=None,
            help="Check only one node by ID",
        )

    def handle(self, *args, **options):
        qs = Node.objects.filter(is_enabled=True).order_by("id")

        node_id = options.get("node_id")
        if node_id:
            qs = qs.filter(id=node_id)

        total = qs.count()
        ok_count = 0
        error_count = 0

        self.stdout.write(f"SRDF MariaDB status check started. nodes={total}")

        for node in qs:
            try:
                config, replication_service = self._get_mariadb_config_for_node(node)
                if not config:
                    self.stdout.write(
                        self.style.WARNING(
                            f"[SKIP] {node.name} -> no enabled MariaDB ReplicationService"
                        )
                    )
                    continue

                status = get_replication_status(config)

                AuditEvent.objects.create(
                    event_type="mariadb_status_check",
                    level="info",
                    node=node,
                    message=f"MariaDB status collected for node '{node.name}'",
                    created_by="srdf_mariadb_status",
                    payload={
                        "replication_service_id": replication_service.id,
                        "replication_service_name": replication_service.name,
                        "mariadb_status": status,
                    },
                )

                ok_count += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"[OK] {node.name} -> role={status.get('role')} "
                        f"running={status.get('running')} "
                        f"lag={status.get('lag_seconds')} "
                        f"error={status.get('last_error') or '-'}"
                    )
                )

            except Exception as exc:
                AuditEvent.objects.create(
                    event_type="mariadb_status_check_error",
                    level="error",
                    node=node,
                    message=f"MariaDB status check failed for node '{node.name}'",
                    created_by="srdf_mariadb_status",
                    payload={
                        "error": str(exc),
                    },
                )

                error_count += 1
                self.stdout.write(
                    self.style.ERROR(f"[ERROR] {node.name} -> {exc}")
                )

        self.stdout.write(
            self.style.WARNING(
                f"SRDF MariaDB status check finished. ok={ok_count} error={error_count} total={total}"
            )
        )

    def _get_mariadb_config_for_node(self, node):
        replication_service = (
            ReplicationService.objects.filter(
                service_type="mariadb",
                is_enabled=True,
            )
            .filter(Q(source_node=node) | Q(target_node=node))
            .order_by("id")
            .first()
        )

        if not replication_service:
            return None, None

        config = replication_service.config or {}
        if not isinstance(config, dict):
            raise ValueError(
                f"Invalid config for ReplicationService '{replication_service.name}'"
            )

        return config, replication_service