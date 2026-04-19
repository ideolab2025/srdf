#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from srdf.models import (
    BootstrapPlan,
    Node,
    ReplicationCheckpoint,
    ReplicationService,
    SRDFInstallProfile,
    SRDFServiceRuntimeState,
)


class Command(BaseCommand):
    help = "Initialize SRDF environment from SRDFInstallProfile"

    def add_arguments(self, parser):
        parser.add_argument("--profile-id", type=int, default=0)
        parser.add_argument("--profile-name", type=str, default="")
        parser.add_argument(
            "--settings-preset",
            type=str,
            default="baseline",
            choices=["baseline", "laptop", "production", "large_binlog", "debug_capture"],
        )
        parser.add_argument(
            "--skip-settings",
            action="store_true",
            help="Do not seed SRDF settings/values",
        )
        parser.add_argument(
            "--skip-exclusions",
            action="store_true",
            help="Do not seed SRDF internal table exclusions",
        )
        parser.add_argument(
            "--skip-bootstrap-plan",
            action="store_true",
            help="Do not create/update default bootstrap plan",
        )

    def handle(self, *args, **options):
        profile = self._resolve_profile(
            profile_id=int(options.get("profile_id") or 0),
            profile_name=(options.get("profile_name") or "").strip(),
        )
        if not profile:
            raise CommandError("SRDFInstallProfile not found")

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF INIT ENVIRONMENT")
        self.stdout.write("============================================================")
        self.stdout.write(f"profile            : {profile.name}")
        self.stdout.write(f"install_scope      : {profile.install_scope}")
        self.stdout.write(f"service_name       : {profile.service_name}")
        self.stdout.write(f"service_type       : {profile.service_type}")
        self.stdout.write(f"mode               : {profile.mode}")
        self.stdout.write(f"source_api_base    : {profile.source_api_base_url}")
        self.stdout.write(f"target_api_base    : {profile.target_api_base_url}")
        self.stdout.write("")

        source_node = self._upsert_source_node(profile)
        target_node = self._upsert_target_node(profile)
        service = self._upsert_replication_service(profile, source_node, target_node)
        runtime_state = self._upsert_runtime_state(service)
        checkpoint_count = self._upsert_checkpoints(service, source_node, target_node)

        bootstrap_plan = None
        if not bool(options.get("skip_bootstrap_plan")):
            bootstrap_plan = self._upsert_bootstrap_plan(profile, service)

        if not bool(options.get("skip_settings")):
            self._seed_settings(profile, service, options["settings_preset"])

        if not bool(options.get("skip_exclusions")):
            call_command("seed_srdf_exclusions", verbosity=1)

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS("SRDF INIT ENVIRONMENT - DONE"))
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS(f"source_node_id         = {source_node.id}"))
        self.stdout.write(self.style.SUCCESS(f"target_node_id         = {target_node.id}"))
        self.stdout.write(self.style.SUCCESS(f"replication_service_id = {service.id}"))
        self.stdout.write(self.style.SUCCESS(f"runtime_state_id       = {runtime_state.id}"))
        self.stdout.write(self.style.SUCCESS(f"checkpoint_count       = {checkpoint_count}"))
        if bootstrap_plan:
            self.stdout.write(self.style.SUCCESS(f"bootstrap_plan_id      = {bootstrap_plan.id}"))
        self.stdout.write("")

    def _resolve_profile(self, profile_id=0, profile_name=""):
        qs = SRDFInstallProfile.objects.filter(is_enabled=True).order_by("id")

        if profile_id:
            return qs.filter(id=profile_id).first()

        if profile_name:
            return qs.filter(name=profile_name).first()

        return qs.first()

    def _upsert_source_node(self, profile):
        obj, _ = Node.objects.update_or_create(
            name=profile.source_node_name,
            defaults={
                "hostname": profile.source_hostname,
                "db_port": int(profile.source_db_port or 3306),
                "site": profile.source_site or "",
                "role": "primary",
                "api_base_url": (profile.source_api_base_url or "").strip().rstrip("/"),
                "api_token": profile.api_token or "",
                "is_enabled": True,
                "status": "unknown",
            },
        )
        return obj

    def _upsert_target_node(self, profile):
        obj, _ = Node.objects.update_or_create(
            name=profile.target_node_name,
            defaults={
                "hostname": profile.target_hostname,
                "db_port": int(profile.target_db_port or 3307),
                "site": profile.target_site or "",
                "role": "secondary",
                "api_base_url": (profile.target_api_base_url or "").strip().rstrip("/"),
                "api_token": profile.api_token or "",
                "is_enabled": True,
                "status": "unknown",
            },
        )
        return obj

    def _upsert_replication_service(self, profile, source_node, target_node):
        config = {
            # --- source connection: legacy/main keys expected by admin_db_catalog
            "host": profile.source_hostname,
            "port": int(profile.source_db_port or 3306),
            "user": profile.source_db_user,
            "password": profile.source_db_password,
            "database": profile.source_db_name,
        
            # --- explicit source-prefixed aliases
            "source_db_host": profile.source_hostname,
            "source_db_port": int(profile.source_db_port or 3306),
            "source_db_name": profile.source_db_name,
            "source_db_user": profile.source_db_user,
            "source_db_password": profile.source_db_password,
        
            # --- target connection
            "target_host": profile.target_hostname,
            "target_port": int(profile.target_db_port or 3307),
            "target_database": profile.target_db_name,
            "target_user": profile.db_user,
            "target_password": profile.db_password,
        
            # --- explicit target-prefixed aliases
            "target_db_host": profile.target_hostname,
            "target_db_port": int(profile.target_db_port or 3307),
            "target_db_name": profile.target_db_name,
            "target_db_user": profile.db_user,
            "target_db_password": profile.db_password,
        
            # --- control / archive / runtime metadata
            "binlog_server_id": int(profile.binlog_server_id or 0),
            "control_db_host": profile.db_host,
            "control_db_port": int(profile.db_port or 3307),
            "control_db_name": profile.control_db_name,
            "archive_db_name": profile.archive_db_name,
        }

        obj, _ = ReplicationService.objects.update_or_create(
            name=profile.service_name,
            defaults={
                "service_type": profile.service_type,
                "mode": profile.mode,
                "source_node": source_node,
                "target_node": target_node,
                "is_enabled": True,
                "config": config,
            },
        )
        return obj

    def _upsert_runtime_state(self, service):
        obj, _ = SRDFServiceRuntimeState.objects.update_or_create(
            replication_service=service,
            defaults={
                "is_enabled": True,
                "is_paused": False,
                "stop_requested": False,
                "current_phase": "idle",
                "last_success_phase": "",
                "next_phase": "",
                "cycle_number": 0,
                "consecutive_error_count": 0,
                "last_return_code": "",
                "last_error": "",
                "capture_checkpoint": {},
                "transport_checkpoint": {},
                "apply_checkpoint": {},
                "runtime_payload": {},
            },
        )
        return obj

    def _upsert_checkpoints(self, service, source_node, target_node):
        rows = [
            ("capture", source_node),
            ("shipper", source_node),
            ("receiver", target_node),
            ("applier", target_node),
        ]

        count = 0
        for direction, node in rows:
            ReplicationCheckpoint.objects.update_or_create(
                replication_service=service,
                node=node,
                direction=direction,
                defaults={
                    "engine": service.service_type,
                    "log_file": "",
                    "log_pos": 0,
                    "transaction_id": "",
                    "checkpoint_data": {},
                },
            )
            count += 1
        return count

    def _upsert_bootstrap_plan(self, profile, service):
        obj, _ = BootstrapPlan.objects.update_or_create(
            name=f"Bootstrap {profile.service_name}",
            replication_service=service,
            defaults={
                "is_enabled": True,
                "bootstrap_method": "sql_copy",
                "scope_mode": "database",
                "source_database_name": profile.source_db_name,
                "database_include_csv": profile.source_db_name,
                "database_exclude_csv": "",
                "database_like_pattern": "",
                "include_tables_csv": "",
                "exclude_tables_csv": "",
                "like_filter": "",
                "recreate_table": False,
                "truncate_target": True,
                "auto_exclude_srdf": True,
                "validate_snapshot": False,
                "parallel_workers": 1,
                "database_parallel_workers": 1,
                "table_limit": 0,
                "execution_order": 100,
                "status": "draft",
                "notes": f"Auto-created from install profile '{profile.name}'",
            },
        )
        return obj

    def _seed_settings(self, profile, service, settings_preset):
        call_command("srdf_seed_settings", verbosity=1)
        call_command("srdf_seed_archive_settings", verbosity=1)

        call_command(
            "srdf_seed_setting_values",
            scope="global",
            preset=settings_preset,
            verbosity=1,
        )

        call_command(
            "srdf_seed_archive_settings_values",
            verbosity=1,
        )

        call_command(
            "srdf_seed_setting_values",
            scope="engine",
            engine=profile.service_type,
            preset="baseline",
            verbosity=1,
        )

        call_command(
            "srdf_seed_setting_values",
            scope="service",
            service_id=service.id,
            engine=profile.service_type,
            preset=settings_preset,
            verbosity=1,
        )