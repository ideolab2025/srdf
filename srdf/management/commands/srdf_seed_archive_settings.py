#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand

from srdf.models import SRDFSettingDefinition


ARCHIVE_SETTING_DEFINITIONS = [
    {
        "key": "archive_enabled",
        "label": "Archive enabled",
        "description": "Enable SRDF archive engine",
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
        "sort_order": 100,
    },
    {
        "key": "archive_interval_hours",
        "label": "Archive interval hours",
        "description": "Minimum interval between archive runs",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "integer",
        "default_value": "24",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 110,
    },
    {
        "key": "archive_retention_days",
        "label": "Archive retention days",
        "description": "Archive rows older than this number of days",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "integer",
        "default_value": "30",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 120,
    },
    {
        "key": "archive_database_name",
        "label": "Archive database alias",
        "description": "Django database alias used for archive writes",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "string",
        "default_value": "srdf_archive",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 130,
    },
    {
        "key": "archive_batch_size",
        "label": "Archive batch size",
        "description": "Insert batch size for archive writes",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "integer",
        "default_value": "1000",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 140,
    },
    {
        "key": "archive_pause_services_during_run",
        "label": "Pause services during archive",
        "description": "Pause orchestrator managed services during archive execution",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "true",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 150,
    },
    {
        "key": "archive_delete_after_copy",
        "label": "Delete after copy",
        "description": "Delete source rows after successful archive copy",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "true",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 160,
    },
    {
        "key": "archive_only_finalized_records",
        "label": "Archive only finalized records",
        "description": "Archive only finalized technical records when applicable",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "true",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 170,
    },
    {
        "key": "archive_max_tables_per_run",
        "label": "Archive max tables per run",
        "description": "Maximum number of tables handled during one archive run",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "integer",
        "default_value": "100",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 180,
    },
    {
        "key": "archive_max_rows_per_table_per_run",
        "label": "Archive max rows per table per run",
        "description": "Maximum rows archived per table during one run",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "integer",
        "default_value": "5000",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 190,
    },
    {
        "key": "archive_auto_create_tables",
        "label": "Archive auto create tables",
        "description": "Automatically create missing archive tables before archive execution",
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
        "sort_order": 200,
    },
    {
        "key": "archive_fail_if_table_missing",
        "label": "Archive fail if table missing",
        "description": "Fail archive execution when archive table is missing and auto create is disabled",
        "category": "orchestrator",
        "phase": "archive",
        "engine": "all",
        "value_type": "boolean",
        "default_value": "true",
        "default_value_json": {},
        "is_required": True,
        "is_enabled": True,
        "is_secret": False,
        "is_system": False,
        "sort_order": 210,
    },    
]


class Command(BaseCommand):
    help = "Create or update SRDF archive setting definitions"

    def add_arguments(self, parser):
        parser.add_argument(
            "--disable-missing",
            action="store_true",
            help="Disable existing archive setting definitions not present in this command",
        )

    def handle(self, *args, **options):
        disable_missing = bool(options.get("disable_missing"))

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE SETTINGS SEED - START"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"disable_missing={disable_missing}")
        self.stdout.write("")

        created_count = 0
        updated_count = 0
        processed_keys = []

        for data in ARCHIVE_SETTING_DEFINITIONS:
            key = data["key"]
            processed_keys.append(key)

            obj, created = SRDFSettingDefinition.objects.update_or_create(
                key=key,
                defaults=data,
            )

            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"[CREATED] {obj.key}"))
            else:
                updated_count += 1
                self.stdout.write(self.style.WARNING(f"[UPDATED] {obj.key}"))

        disabled_count = 0

        if disable_missing:
            archive_qs = SRDFSettingDefinition.objects.filter(
                phase="archive"
            ).exclude(key__in=processed_keys)

            for obj in archive_qs:
                if obj.is_enabled:
                    obj.is_enabled = False
                    obj.save(update_fields=["is_enabled", "updated_at"])
                    disabled_count += 1
                    self.stdout.write(self.style.WARNING(f"[DISABLED] {obj.key}"))

        total_count = len(ARCHIVE_SETTING_DEFINITIONS)

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE SETTINGS SEED - FINAL SUMMARY"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"total_definitions={total_count}")
        self.stdout.write(f"created={created_count}")
        self.stdout.write(f"updated={updated_count}")
        self.stdout.write(f"disabled={disabled_count}")
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE SETTINGS SEED - END"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write("")