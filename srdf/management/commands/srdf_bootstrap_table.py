#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.services.table_bootstrap import bootstrap_table_once


class Command(BaseCommand):
    help = "Bootstrap one source table to target using SHOW CREATE TABLE + full data copy"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, required=True, help="ReplicationService ID")
        parser.add_argument("--database-name", type=str, required=True, help="Source database name")
        parser.add_argument("--table-name", type=str, required=True, help="Source table name")
        parser.add_argument(
            "--recreate-table",
            action="store_true",
            help="Drop and recreate target table before copying data",
        )
        parser.add_argument(
            "--no-truncate-target",
            action="store_true",
            help="Do not truncate target table when it already exists",
        )

    def handle(self, *args, **options):
        service_id = options["service_id"]
        database_name = options["database_name"]
        table_name = options["table_name"]
        recreate_table = bool(options["recreate_table"])
        truncate_target = not bool(options["no_truncate_target"])

        started_at = timezone.now()

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF BOOTSTRAP TABLE")
        self.stdout.write("============================================================")
        self.stdout.write(f"service_id      : {service_id}")
        self.stdout.write(f"database_name   : {database_name}")
        self.stdout.write(f"table_name      : {table_name}")
        self.stdout.write(f"recreate_table  : {recreate_table}")
        self.stdout.write(f"truncate_target : {truncate_target}")
        self.stdout.write("")

        try:
            result = bootstrap_table_once(
                replication_service_id=service_id,
                database_name=database_name,
                table_name=table_name,
                recreate_table=recreate_table,
                truncate_target=truncate_target,
                initiated_by="cli_bootstrap_table",
            )
        except Exception as exc:
            raise CommandError(str(exc))

        duration = round((timezone.now() - started_at).total_seconds(), 3)

        self.stdout.write("---- RESULT ----")
        self.stdout.write(f"message     : {result.get('message')}")
        self.stdout.write(f"copied_rows : {result.get('copied_rows', 0)}")
        self.stdout.write(f"duration    : {duration}s")
        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("DONE")
        self.stdout.write("============================================================")
        self.stdout.write("")