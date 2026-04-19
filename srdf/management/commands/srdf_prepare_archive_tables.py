#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json

from django.core.management.base import BaseCommand, CommandError

from srdf.services.archive_engine import _ensure_archive_tables_ready


class Command(BaseCommand):
    help = "Prepare SRDF archive tables on archive database"

    def add_arguments(self, parser):
        parser.add_argument(
            "--archive-db-alias",
            type=str,
            default="srdf_archive",
            help="Django database alias for archive database",
        )
        parser.add_argument(
            "--auto-create",
            action="store_true",
            help="Automatically create missing archive tables",
        )
        parser.add_argument(
            "--allow-missing",
            action="store_true",
            help="Do not fail if some archive tables are missing",
        )

    def handle(self, *args, **options):
        archive_db_alias = str(options.get("archive_db_alias") or "srdf_archive").strip()
        auto_create = bool(options.get("auto_create"))
        allow_missing = bool(options.get("allow_missing"))

        result = _ensure_archive_tables_ready(
            archive_db_alias=archive_db_alias,
            auto_create=auto_create,
            fail_if_missing=not allow_missing,
            progress_callback=None,
        )

        self.stdout.write("JSON_RESULT_BEGIN")
        self.stdout.write(json.dumps(result, indent=2, ensure_ascii=False, default=str))
        self.stdout.write("JSON_RESULT_END")

        if not result.get("ok") and not allow_missing:
            raise CommandError(result.get("message") or "Archive table preparation failed")