#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import (
    Node,
    SRDFInstallProfile,
    SRDFSettingDefinition,
    SRDFSettingValue,
)


class Command(BaseCommand):
    help = "Build SRDFSettingValue entries with scope=node from Node and SRDFInstallProfile"

    def add_arguments(self, parser):
        parser.add_argument("--profile-id", type=int, default=0)
        parser.add_argument("--profile-name", type=str, default="")
        parser.add_argument("--node-id", type=int, default=0)
        parser.add_argument("--node-name", type=str, default="")
        parser.add_argument(
            "--preset",
            type=str,
            default="baseline",
            choices=["baseline", "laptop", "production"],
        )
        parser.add_argument(
            "--priority",
            type=int,
            default=100,
        )
        parser.add_argument(
            "--disabled",
            action="store_true",
            help="Create values in disabled state",
        )

    def handle(self, *args, **options):
        profile = self._resolve_profile(
            profile_id=int(options.get("profile_id") or 0),
            profile_name=(options.get("profile_name") or "").strip(),
        )
        if not profile:
            raise CommandError("SRDFInstallProfile not found")

        node = self._resolve_node(
            node_id=int(options.get("node_id") or 0),
            node_name=(options.get("node_name") or "").strip(),
        )

        nodes = []
        if node:
            nodes = [node]
        else:
            source_node = Node.objects.filter(name=profile.source_node_name).first()
            target_node = Node.objects.filter(name=profile.target_node_name).first()

            if not source_node and not target_node:
                raise CommandError("No matching source/target nodes found from SRDFInstallProfile")

            if source_node:
                nodes.append(source_node)
            if target_node and (not source_node or target_node.id != source_node.id):
                nodes.append(target_node)

        preset = (options.get("preset") or "baseline").strip()
        priority = int(options.get("priority") or 100)
        is_enabled = not bool(options.get("disabled"))

        created_count = 0
        updated_count = 0

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF BUILD NODE SETTING VALUES")
        self.stdout.write("============================================================")
        self.stdout.write(f"profile            : {profile.name}")
        self.stdout.write(f"preset             : {preset}")
        self.stdout.write(f"nodes_count         : {len(nodes)}")
        self.stdout.write("")

        for current_node in nodes:
            value_map = self._build_node_value_map(
                node=current_node,
                profile=profile,
                preset=preset,
            )

            self.stdout.write(f"[NODE] {current_node.name} ({current_node.role})")

            for key, value_text in value_map.items():
                definition = (
                    SRDFSettingDefinition.objects
                    .filter(key=key, is_enabled=True)
                    .order_by("id")
                    .first()
                )
                if not definition:
                    self.stdout.write(self.style.WARNING(f"  [SKIPPED] Missing definition for {key}"))
                    continue

                obj, created = SRDFSettingValue.objects.update_or_create(
                    setting_definition=definition,
                    scope_type="node",
                    engine="all",
                    company_name="",
                    client_name="",
                    replication_service=None,
                    node=current_node,
                    priority=priority,
                    defaults={
                        "value_text": str(value_text),
                        "value_json": {},
                        "notes": f"Auto-built from profile={profile.name} preset={preset}",
                        "is_enabled": is_enabled,
                    },
                )

                if created:
                    created_count += 1
                    self.stdout.write(self.style.SUCCESS(f"  [CREATED] {key}"))
                else:
                    updated_count += 1
                    self.stdout.write(self.style.WARNING(f"  [UPDATED] {key}"))

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS("SRDF BUILD NODE SETTING VALUES - DONE"))
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS(f"created = {created_count}"))
        self.stdout.write(self.style.SUCCESS(f"updated = {updated_count}"))
        self.stdout.write("")

    def _resolve_profile(self, profile_id=0, profile_name=""):
        qs = SRDFInstallProfile.objects.filter(is_enabled=True).order_by("id")

        if profile_id:
            return qs.filter(id=profile_id).first()

        if profile_name:
            return qs.filter(name=profile_name).first()

        return qs.first()

    def _resolve_node(self, node_id=0, node_name=""):
        if node_id:
            return Node.objects.filter(id=node_id).first()

        if node_name:
            return Node.objects.filter(name=node_name).order_by("id").first()

        return None

    def _build_node_value_map(self, node, profile, preset="baseline"):
        api_base_url = (node.api_base_url or "").strip().rstrip("/")
        api_token = (node.api_token or profile.api_token or "").strip()

        if not api_base_url:
            if node.role == "primary":
                api_base_url = (profile.source_api_base_url or "").strip().rstrip("/")
            elif node.role == "secondary":
                api_base_url = (profile.target_api_base_url or "").strip().rstrip("/")

        if preset == "laptop" and not api_base_url:
            api_base_url = "http://127.0.0.1"

        if preset == "production" and not api_base_url:
            api_base_url = "https://srdf.example.com"

        return {
            "api.public_base_url": api_base_url,
            "api.healthcheck_path": "/srdf/api/ping/",
            "api.status_path": "/srdf/api/status/",
            "security.api_token": api_token,
        }