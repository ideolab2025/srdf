#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from srdf.models import ReplicationService


class Command(BaseCommand):
    help = "Initialize SRDF setting definitions and baseline default values"

    def add_arguments(self, parser):
        parser.add_argument(
            "--service-id",
            type=int,
            default=0,
            help="Optional ReplicationService ID for service-scope baseline values",
        )
        parser.add_argument(
            "--service-name",
            type=str,
            default="",
            help="Optional ReplicationService name for service-scope baseline values",
        )
        parser.add_argument(
            "--engine",
            type=str,
            default="all",
            choices=["all", "mysql", "mariadb", "postgresql", "oracle", "filesync", "nginx"],
            help="Engine used for optional engine-scope seed",
        )
        parser.add_argument(
            "--with-engine-scope",
            action="store_true",
            help="Also seed engine-scope baseline values",
        )
        parser.add_argument(
            "--preset",
            type=str,
            default="baseline",
            choices=["baseline", "large_binlog", "debug_capture"],
            help="Preset used for default values",
        )

    def handle(self, *args, **options):
        service_id = int(options.get("service_id") or 0)
        service_name = (options.get("service_name") or "").strip()
        engine = (options.get("engine") or "all").strip()
        with_engine_scope = bool(options.get("with_engine_scope"))
        preset = (options.get("preset") or "baseline").strip()

        if service_id and service_name:
            raise CommandError("Use either --service-id or --service-name, not both")

        replication_service = None
        if service_id:
            replication_service = (
                ReplicationService.objects
                .filter(id=service_id)
                .first()
            )
        elif service_name:
            replication_service = (
                ReplicationService.objects
                .filter(name=service_name)
                .order_by("id")
                .first()
            )

        if (service_id or service_name) and not replication_service:
            raise CommandError("ReplicationService not found")

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF INIT SETTINGS")
        self.stdout.write("============================================================")
        self.stdout.write(f"preset             : {preset}")
        self.stdout.write(f"engine             : {engine}")
        self.stdout.write(f"with_engine_scope  : {with_engine_scope}")
        self.stdout.write(
            f"service            : {replication_service.name if replication_service else '-'}"
        )
        self.stdout.write("")

        self.stdout.write("[STEP] Seed setting definitions")
        call_command("srdf_seed_settings", verbosity=1)
        self.stdout.write(self.style.SUCCESS("[OK] setting definitions seeded"))

        self.stdout.write("[STEP] Seed global baseline values")
        call_command(
            "srdf_seed_setting_values",
            scope="global",
            preset=preset,
            verbosity=1,
        )
        self.stdout.write(self.style.SUCCESS("[OK] global values seeded"))

        if with_engine_scope and engine != "all":
            self.stdout.write(f"[STEP] Seed engine baseline values for engine={engine}")
            call_command(
                "srdf_seed_setting_values",
                scope="engine",
                engine=engine,
                preset=preset,
                verbosity=1,
            )
            self.stdout.write(self.style.SUCCESS("[OK] engine values seeded"))

        if replication_service:
            self.stdout.write(
                f"[STEP] Seed service baseline values for service_id={replication_service.id}"
            )
            call_command(
                "srdf_seed_setting_values",
                scope="service",
                service_id=replication_service.id,
                preset=preset,
                verbosity=1,
            )
            self.stdout.write(self.style.SUCCESS("[OK] service values seeded"))

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("DONE")
        self.stdout.write("============================================================")
        self.stdout.write("")