#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand

from srdf.models import SRDFSettingDefinition


CAPTURE_DEFINITIONS = [
    {
        "key": "capture.limit",
        "label": "Capture limit",
        "description": "Maximum number of outbound events captured in one run.",
        "category": "capture",
        "phase": "capture",
        "engine": "all",
        "value_type": "integer",
        "default_value": "100",
        "default_value_json": {},
        "is_required": False,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 10,
    },
    {
        "key": "capture.max_scanned_events",
        "label": "Capture max scanned events",
        "description": "Maximum number of scanned binlog row events during one run.",
        "category": "capture",
        "phase": "capture",
        "engine": "all",
        "value_type": "integer",
        "default_value": "500",
        "default_value_json": {},
        "is_required": False,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 20,
    },
    {
        "key": "capture.max_runtime_seconds",
        "label": "Capture max runtime seconds",
        "description": "Maximum runtime allowed for one capture run.",
        "category": "capture",
        "phase": "capture",
        "engine": "all",
        "value_type": "integer",
        "default_value": "20",
        "default_value_json": {},
        "is_required": False,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 30,
    },
]

TRANSPORT_DEFINITIONS = [
    {
        "key": "transport.batch_limit",
        "label": "Transport batch limit",
        "description": "Maximum outbound events packed into one transport batch.",
        "category": "transport",
        "phase": "batch_build",
        "engine": "all",
        "value_type": "integer",
        "default_value": "100",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 200,
    },
    {
        "key": "transport.compression",
        "label": "Transport compression",
        "description": "Compression mode used for batches.",
        "category": "transport",
        "phase": "compress",
        "engine": "all",
        "value_type": "string",
        "default_value": "gzip",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 210,
    },
    {
        "key": "transport.auto_resume_failed_batch",
        "label": "Auto resume failed batch",
        "description": "Allow automatic retry of failed transport batches.",
        "category": "transport",
        "phase": "ship",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "true",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 220,
    },
    {
        "key": "transport.max_auto_resume_attempts",
        "label": "Transport max auto resume attempts",
        "description": "Maximum automatic retry count for failed transport batches.",
        "category": "transport",
        "phase": "ship",
        "engine": "all",
        "value_type": "integer",
        "default_value": "3",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 230,
    },
    {
        "key": "transport.failed_batch_resume_cooldown_seconds",
        "label": "Failed batch resume cooldown seconds",
        "description": "Cooldown between automatic retries of failed transport batches.",
        "category": "transport",
        "phase": "ship",
        "engine": "all",
        "value_type": "integer",
        "default_value": "30",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 240,
    },
]

API_DEFINITIONS = [
    {
        "key": "api.public_base_url",
        "label": "Public API base URL",
        "description": "Public internet-visible base URL for this control plane.",
        "category": "other",
        "phase": "orchestrator",
        "engine": "all",
        "value_type": "string",
        "default_value": "",
        "default_value_json": {},
        "is_required": False,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 300,
    },
    {
        "key": "api.healthcheck_path",
        "label": "API healthcheck path",
        "description": "Relative path used for SRDF health checks.",
        "category": "other",
        "phase": "orchestrator",
        "engine": "all",
        "value_type": "string",
        "default_value": "/srdf/api/ping/",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 310,
    },
    {
        "key": "api.status_path",
        "label": "API status path",
        "description": "Relative path used for SRDF status calls.",
        "category": "other",
        "phase": "orchestrator",
        "engine": "all",
        "value_type": "string",
        "default_value": "/srdf/api/status/",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 320,
    },
]

BOOTSTRAP_DEFINITIONS = [
    {
        "key": "bootstrap.parallel_workers",
        "label": "Bootstrap parallel workers",
        "description": "Parallel workers used for bootstrap table copy.",
        "category": "bootstrap",
        "phase": "bootstrap",
        "engine": "all",
        "value_type": "integer",
        "default_value": "1",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 400,
    },
    {
        "key": "bootstrap.database_parallel_workers",
        "label": "Bootstrap database parallel workers",
        "description": "Parallel workers at database level.",
        "category": "bootstrap",
        "phase": "bootstrap",
        "engine": "all",
        "value_type": "integer",
        "default_value": "1",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 410,
    },
    {
        "key": "bootstrap.auto_exclude_srdf",
        "label": "Bootstrap auto exclude SRDF",
        "description": "Exclude SRDF internal tables during bootstrap.",
        "category": "bootstrap",
        "phase": "bootstrap",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "true",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 420,
    },
]

ORCHESTRATOR_DEFINITIONS = [
    {
        "key": "orchestrator.sleep_seconds",
        "label": "Orchestrator sleep seconds",
        "description": "Sleep interval between orchestrator cycles.",
        "category": "orchestrator",
        "phase": "orchestrator",
        "engine": "all",
        "value_type": "integer",
        "default_value": "10",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": True,
        "sort_order": 500,
    },
    {
        "key": "orchestrator.enable_archive_phase",
        "label": "Enable archive phase",
        "description": "Enable archive phase in orchestrator cycle.",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "false",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 510,
    },
]

SECURITY_DEFINITIONS = [
    {
        "key": "security.api_token",
        "label": "Security API token",
        "description": "SRDF API token used for node-to-node calls.",
        "category": "security",
        "phase": "orchestrator",
        "engine": "all",
        "value_type": "string",
        "default_value": "",
        "default_value_json": {},
        "is_required": False,
        "is_enabled": True,
        "is_secret": True,
        "is_system": False,
        "sort_order": 600,
    },
]

ALL_DEFINITIONS = (
    CAPTURE_DEFINITIONS
    + TRANSPORT_DEFINITIONS
    + API_DEFINITIONS
    + BOOTSTRAP_DEFINITIONS
    + ORCHESTRATOR_DEFINITIONS
    + SECURITY_DEFINITIONS
)

ALL_SETTING_DEFINITIONS = (
    CAPTURE_DEFINITIONS
    + TRANSPORT_DEFINITIONS
    + API_DEFINITIONS
    + BOOTSTRAP_DEFINITIONS
    + ORCHESTRATOR_DEFINITIONS
    + SECURITY_DEFINITIONS
)


class Command(BaseCommand):
    help = "Seed SRDF setting definitions"

    def handle(self, *args, **options):
        created_count = 0
        updated_count = 0

        for payload in ALL_SETTING_DEFINITIONS:
            obj, created = SRDFSettingDefinition.objects.update_or_create(
                key=payload["key"],
                defaults=payload,
            )

            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"[CREATED] {obj.key}"))
            else:
                updated_count += 1
                self.stdout.write(self.style.WARNING(f"[UPDATED] {obj.key}"))

        self.stdout.write(
            self.style.SUCCESS(
                f"SRDF settings seed completed. created={created_count} updated={updated_count}"
            )
        )