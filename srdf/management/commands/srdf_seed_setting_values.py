#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import ReplicationService, SRDFSettingDefinition, SRDFSettingValue


GLOBAL_CAPTURE_VALUES = {
    "capture.limit": "100",
    "capture.max_scanned_events": "500",
    "capture.max_runtime_seconds": "20",
    "capture.scan_window_max_files": "2",
    "capture.scan_window_max_bytes": str(64 * 1024 * 1024),
    "capture.bulk_insert_batch_size": "250",
    "capture.wagon_max_events": "200",
    "capture.progress_enabled": "false",
    "capture.progress_every_events": "250",
    "capture.progress_every_seconds": "5",
    "capture.enable_filter_audit": "false",
    "capture.disable_time_limit": "false",
    "capture.debug": "false",
    "capture.verify_with_mysqlbinlog": "false",
}

GLOBAL_TRANSPORT_VALUES = {
    "transport.batch_limit": "100",
    "transport.compression": "gzip",
    "transport.auto_resume_failed_batch": "true",
    "transport.max_auto_resume_attempts": "3",
    "transport.failed_batch_resume_cooldown_seconds": "30",
}

GLOBAL_API_VALUES = {
    "api.healthcheck_path": "/srdf/api/ping/",
    "api.status_path": "/srdf/api/status/",
    "api.public_base_url": "",
}

GLOBAL_BOOTSTRAP_VALUES = {
    "bootstrap.parallel_workers": "1",
    "bootstrap.database_parallel_workers": "1",
    "bootstrap.auto_exclude_srdf": "true",
}

GLOBAL_ORCHESTRATOR_VALUES = {
    "orchestrator.sleep_seconds": "10",
    "orchestrator.enable_archive_phase": "false",
}

GLOBAL_SECURITY_VALUES = {
    "security.api_token": "",
}


MARIADB_ENGINE_VALUES = {
    "capture.scan_window_max_files": "2",
    "capture.scan_window_max_bytes": str(64 * 1024 * 1024),
    "capture.bulk_insert_batch_size": "250",
    "capture.wagon_max_events": "200",
}


MYSQL_ENGINE_VALUES = {
    "capture.scan_window_max_files": "2",
    "capture.scan_window_max_bytes": str(64 * 1024 * 1024),
    "capture.bulk_insert_batch_size": "250",
    "capture.wagon_max_events": "200",
}


PRESET_LARGE_BINLOG = {
    "capture.limit": "1000",
    "capture.max_scanned_events": "5000",
    "capture.max_runtime_seconds": "60",
    "capture.scan_window_max_files": "4",
    "capture.scan_window_max_bytes": str(256 * 1024 * 1024),
    "capture.bulk_insert_batch_size": "500",
    "capture.wagon_max_events": "500",
    "capture.progress_enabled": "true",
    "capture.progress_every_events": "500",
    "capture.progress_every_seconds": "5",
}


PRESET_DEBUG_CAPTURE = {
    "capture.progress_enabled": "true",
    "capture.progress_every_events": "100",
    "capture.progress_every_seconds": "2",
    "capture.debug": "true",
    "capture.verify_with_mysqlbinlog": "true",
}

PRESET_LAPTOP = {
    "api.public_base_url": "http://127.0.0.1",
}

PRESET_PRODUCTION = {
    "api.public_base_url": "https://srdf.example.com",
}


class Command(BaseCommand):
    help = "Seed SRDF setting values for global, engine, company, client, or service scope"

    def add_arguments(self, parser):
        parser.add_argument(
            "--scope",
            type=str,
            default="global",
            choices=["global", "engine", "company", "client", "service"],
            help="Target scope for seeded values",
        )
        parser.add_argument(
            "--engine",
            type=str,
            default="all",
            choices=["all", "mysql", "mariadb", "postgresql", "oracle", "filesync", "nginx"],
            help="Target engine",
        )
        parser.add_argument(
            "--company-name",
            type=str,
            default="",
            help="Company name for company scope",
        )
        parser.add_argument(
            "--client-name",
            type=str,
            default="",
            help="Client name for client scope",
        )
        parser.add_argument(
            "--service-id",
            type=int,
            default=0,
            help="ReplicationService ID for service scope",
        )
        parser.add_argument(
            "--service-name",
            type=str,
            default="",
            help="ReplicationService name for service scope",
        )
        parser.add_argument(
            "--preset",
            type=str,
            default="baseline",
            choices=["baseline", "large_binlog", "debug_capture", "laptop", "production"],
            help="Preset values to seed",
        )
        parser.add_argument(
            "--priority",
            type=int,
            default=100,
            help="Priority for seeded setting values",
        )
        parser.add_argument(
            "--disabled",
            action="store_true",
            help="Create values in disabled state",
        )

    def handle(self, *args, **options):
        scope = (options["scope"] or "global").strip()
        engine = (options["engine"] or "all").strip()
        company_name = (options["company_name"] or "").strip()
        client_name = (options["client_name"] or "").strip()
        service_id = int(options["service_id"] or 0)
        service_name = (options["service_name"] or "").strip()
        preset = (options["preset"] or "baseline").strip()
        priority = int(options["priority"] or 100)
        is_enabled = not bool(options["disabled"])

        replication_service = None

        if scope == "company" and not company_name:
            raise CommandError("--company-name is required when --scope=company")

        if scope == "client" and not client_name:
            raise CommandError("--client-name is required when --scope=client")

        if scope == "service":
            replication_service = self._resolve_replication_service(
                service_id=service_id,
                service_name=service_name,
            )
            
            if not replication_service:
                raise CommandError(
                    "Service scope requires --service-id or --service-name matching an existing ReplicationService"
                )
            
            if engine == "all":
                engine = replication_service.service_type or "all"

        value_map = self._build_value_map(scope=scope, engine=engine, preset=preset)

        created_count = 0
        updated_count = 0

        for key, value_text in value_map.items():
            definition = (
                SRDFSettingDefinition.objects
                .filter(key=key, is_enabled=True)
                .order_by("id")
                .first()
            )
            if not definition:
                self.stdout.write(self.style.WARNING(f"[SKIPPED] Missing definition for {key}"))
                continue

            obj, created = self._upsert_setting_value(
                definition=definition,
                scope=scope,
                engine=engine,
                company_name=company_name,
                client_name=client_name,
                replication_service=replication_service,
                value_text=value_text,
                priority=priority,
                is_enabled=is_enabled,
                notes=f"Seed preset={preset}",
            )

            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"[CREATED] {definition.key} [{scope}]"))
            else:
                updated_count += 1
                self.stdout.write(self.style.WARNING(f"[UPDATED] {definition.key} [{scope}]"))

        self.stdout.write(
            self.style.SUCCESS(
                f"SRDF setting values seed completed. "
                f"scope={scope} preset={preset} created={created_count} updated={updated_count}"
            )
        )

    def _build_value_map(self, scope, engine, preset):
        value_map = {}
    
        # Base globale commune
        value_map.update(GLOBAL_CAPTURE_VALUES)
        value_map.update(GLOBAL_TRANSPORT_VALUES)
        value_map.update(GLOBAL_API_VALUES)
        value_map.update(GLOBAL_BOOTSTRAP_VALUES)
        value_map.update(GLOBAL_ORCHESTRATOR_VALUES)
        value_map.update(GLOBAL_SECURITY_VALUES)
    
        # Presets spécialisés
        if preset == "large_binlog":
            value_map.update(PRESET_LARGE_BINLOG)
    
        elif preset == "debug_capture":
            value_map.update(PRESET_DEBUG_CAPTURE)
    
        elif preset == "laptop":
            value_map.update(PRESET_LAPTOP)
    
        elif preset == "production":
            value_map.update(PRESET_PRODUCTION)
    
        # Surcharge engine
        if scope == "engine" and engine == "mariadb":
            value_map.update(MARIADB_ENGINE_VALUES)
    
        if scope == "engine" and engine == "mysql":
            value_map.update(MYSQL_ENGINE_VALUES)
    
        return value_map
    
    
    

    def _resolve_replication_service(self, service_id=0, service_name=""):
        if service_id:
            return (
                ReplicationService.objects
                .filter(id=service_id)
                .first()
            )

        if service_name:
            return (
                ReplicationService.objects
                .filter(name=service_name)
                .order_by("id")
                .first()
            )

        return None

    def _upsert_setting_value(
        self,
        definition,
        scope,
        engine,
        company_name,
        client_name,
        replication_service,
        value_text,
        priority,
        is_enabled,
        notes="",
    ):
        qs = SRDFSettingValue.objects.filter(
            setting_definition=definition,
            scope_type=scope,
            engine=engine,
            company_name=company_name,
            client_name=client_name,
            replication_service=replication_service,
            node__isnull=True,
        ).order_by("id")

        obj = qs.first()

        if obj:
            obj.value_text = str(value_text)
            obj.priority = int(priority)
            obj.is_enabled = bool(is_enabled)
            obj.notes = notes
            obj.save(
                update_fields=[
                    "value_text",
                    "priority",
                    "is_enabled",
                    "notes",
                    "updated_at",
                ]
            )
            return obj, False

        obj = SRDFSettingValue.objects.create(
            setting_definition=definition,
            scope_type=scope,
            engine=engine,
            company_name=company_name,
            client_name=client_name,
            replication_service=replication_service,
            node=None,
            value_text=str(value_text),
            value_json={},
            notes=notes,
            priority=int(priority),
            is_enabled=bool(is_enabled),
        )
        return obj, True