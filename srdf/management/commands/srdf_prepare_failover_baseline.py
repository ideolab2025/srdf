#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import (
    BootstrapPlan,
    FailoverPlan,
    ReplicationService,
)


class Command(BaseCommand):
    help = "Create or update a baseline SRDF FailoverPlan from ReplicationService"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, default=0)
        parser.add_argument("--service-name", type=str, default="")
        parser.add_argument("--plan-name", type=str, default="")
        parser.add_argument(
            "--status",
            type=str,
            default="draft",
            choices=["draft", "ready", "running", "success", "failed", "cancelled"],
        )
        parser.add_argument(
            "--include-bootstrap",
            action="store_true",
            help="Attach bootstrap metadata to failover baseline",
        )

    def handle(self, *args, **options):
        service = self._resolve_service(
            service_id=int(options.get("service_id") or 0),
            service_name=(options.get("service_name") or "").strip(),
        )
        if not service:
            raise CommandError("ReplicationService not found")

        plan_name = (options.get("plan_name") or "").strip()
        if not plan_name:
            plan_name = f"Failover {service.name}"

        source_node = service.source_node
        target_node = service.target_node
        include_bootstrap = bool(options.get("include_bootstrap"))

        bootstrap_plan = (
            BootstrapPlan.objects
            .filter(replication_service=service)
            .order_by("execution_order", "id")
            .first()
        )

        plan_data = {
            "replication_service": {
                "id": service.id,
                "name": service.name,
                "service_type": service.service_type,
                "mode": service.mode,
                "is_enabled": service.is_enabled,
            },
            "source_node": {
                "id": source_node.id if source_node else 0,
                "name": source_node.name if source_node else "",
                "hostname": source_node.hostname if source_node else "",
                "site": source_node.site if source_node else "",
                "role": source_node.role if source_node else "",
                "api_base_url": source_node.api_base_url if source_node else "",
            },
            "target_node": {
                "id": target_node.id if target_node else 0,
                "name": target_node.name if target_node else "",
                "hostname": target_node.hostname if target_node else "",
                "site": target_node.site if target_node else "",
                "role": target_node.role if target_node else "",
                "api_base_url": target_node.api_base_url if target_node else "",
            },
            "steps": [
                {
                    "order": 10,
                    "code": "freeze_source_writes",
                    "label": "Freeze source writes",
                    "enabled": True,
                    "status": "pending",
                },
                {
                    "order": 20,
                    "code": "validate_replication_backlog",
                    "label": "Validate replication backlog",
                    "enabled": True,
                    "status": "pending",
                },
                {
                    "order": 30,
                    "code": "stop_source_services",
                    "label": "Stop source SRDF services",
                    "enabled": True,
                    "status": "pending",
                },
                {
                    "order": 40,
                    "code": "promote_target",
                    "label": "Promote target as new primary",
                    "enabled": True,
                    "status": "pending",
                },
                {
                    "order": 50,
                    "code": "switch_api_clients",
                    "label": "Switch clients and API endpoints",
                    "enabled": True,
                    "status": "pending",
                },
                {
                    "order": 60,
                    "code": "resume_services_on_target",
                    "label": "Resume SRDF services on target",
                    "enabled": True,
                    "status": "pending",
                },
            ],
            "validation": {
                "source_node_online_required": True,
                "target_node_online_required": True,
                "replication_service_enabled_required": True,
            },
        }

        if include_bootstrap and bootstrap_plan:
            plan_data["bootstrap_plan"] = {
                "id": bootstrap_plan.id,
                "name": bootstrap_plan.name,
                "source_database_name": bootstrap_plan.source_database_name,
                "scope_mode": bootstrap_plan.scope_mode,
                "bootstrap_method": bootstrap_plan.bootstrap_method,
            }

        obj, created = FailoverPlan.objects.update_or_create(
            name=plan_name,
            defaults={
                "source_site": source_node.site if source_node else "",
                "target_site": target_node.site if target_node else "",
                "status": options["status"],
                "plan_data": plan_data,
                "last_error": "",
            },
        )

        if created:
            self.stdout.write(self.style.SUCCESS(f"[CREATED] FailoverPlan id={obj.id} name='{obj.name}'"))
        else:
            self.stdout.write(self.style.WARNING(f"[UPDATED] FailoverPlan id={obj.id} name='{obj.name}'"))

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF PREPARE FAILOVER BASELINE - DONE")
        self.stdout.write("============================================================")
        self.stdout.write(f"failover_plan_id   : {obj.id}")
        self.stdout.write(f"source_site        : {obj.source_site}")
        self.stdout.write(f"target_site        : {obj.target_site}")
        self.stdout.write(f"status             : {obj.status}")
        self.stdout.write("")

    def _resolve_service(self, service_id=0, service_name=""):
        if service_id:
            return (
                ReplicationService.objects
                .select_related("source_node", "target_node")
                .filter(id=service_id)
                .first()
            )

        if service_name:
            return (
                ReplicationService.objects
                .select_related("source_node", "target_node")
                .filter(name=service_name)
                .order_by("id")
                .first()
            )

        return None