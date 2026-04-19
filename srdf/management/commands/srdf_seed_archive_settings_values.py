#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand

from srdf.models import SRDFSettingDefinition, SRDFSettingValue


ARCHIVE_GLOBAL_SETTING_VALUES = [
    {"key": "archive_enabled", "value_text": "true"},
    {"key": "archive_interval_hours", "value_text": "24"},
    {"key": "archive_retention_days", "value_text": "30"},
    {"key": "archive_database_name", "value_text": "srdf_archive"},
    {"key": "archive_batch_size", "value_text": "1000"},
    {"key": "archive_pause_services_during_run", "value_text": "true"},
    {"key": "archive_delete_after_copy", "value_text": "true"},
    {"key": "archive_only_finalized_records", "value_text": "true"},
    {"key": "archive_max_tables_per_run", "value_text": "100"},
    {"key": "archive_max_rows_per_table_per_run", "value_text": "5000"},
    {"key": "archive_auto_create_tables", "value_text": "true"},
    {"key": "archive_fail_if_table_missing", "value_text": "true"},
]


class Command(BaseCommand):
    help = "Create or update SRDF archive global setting values"

    def handle(self, *args, **options):
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE SETTING VALUES SEED - START"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write("")

        created_count = 0
        updated_count = 0

        for row in ARCHIVE_GLOBAL_SETTING_VALUES:
            key = row["key"]
            definition = SRDFSettingDefinition.objects.filter(key=key).first()

            if not definition:
                self.stdout.write(self.style.ERROR(f"[MISSING DEFINITION] {key}"))
                continue

            obj, created = SRDFSettingValue.objects.update_or_create(
                setting_definition=definition,
                scope_type="global",
                replication_service=None,
                node=None,
                company_name="",
                client_name="",
                priority=100,
                defaults={
                    "engine": "all",
                    "value_text": row["value_text"],
                    "value_json": {},
                    "notes": "Seeded by srdf_seed_archive_setting_values",
                    "is_enabled": True,
                },
            )

            if created:
                created_count += 1
                self.stdout.write(self.style.SUCCESS(f"[CREATED] {key}={obj.value_text}"))
            else:
                updated_count += 1
                self.stdout.write(self.style.WARNING(f"[UPDATED] {key}={obj.value_text}"))

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE SETTING VALUES SEED - FINAL SUMMARY"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"created={created_count}")
        self.stdout.write(f"updated={updated_count}")
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF ARCHIVE SETTING VALUES SEED - END"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write("")