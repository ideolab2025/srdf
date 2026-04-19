#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import (
    ReplicationService,
    SourceDatabaseCatalog,
    SourceTableCatalog,
)
from srdf.services.admin_db_catalog import (
    AdminDBCatalogError,
    list_source_databases,
    list_source_tables,
)


class Command(BaseCommand):
    help = "Refresh source database/table catalog for one ReplicationService"

    def add_arguments(self, parser):
        parser.add_argument("--service-id", type=int, default=0)
        parser.add_argument("--service-name", type=str, default="")
        parser.add_argument(
            "--deactivate-missing",
            action="store_true",
            help="Mark missing databases/tables as inactive",
        )
        parser.add_argument(
            "--database-name",
            type=str,
            default="",
            help="Refresh only one source database",
        )

    def handle(self, *args, **options):
        service = self._resolve_service(
            service_id=int(options.get("service_id") or 0),
            service_name=(options.get("service_name") or "").strip(),
        )
        if not service:
            raise CommandError("ReplicationService not found")

        database_name_filter = (options.get("database_name") or "").strip()
        deactivate_missing = bool(options.get("deactivate_missing"))

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF REFRESH SOURCE CATALOG")
        self.stdout.write("============================================================")
        self.stdout.write(f"service_id         : {service.id}")
        self.stdout.write(f"service_name       : {service.name}")
        self.stdout.write(f"service_type       : {service.service_type}")
        self.stdout.write(f"database_filter    : {database_name_filter or '-'}")
        self.stdout.write(f"deactivate_missing : {deactivate_missing}")
        self.stdout.write("")

        try:
            discovered_databases = list_source_databases(service)
        except AdminDBCatalogError as exc:
            raise CommandError(str(exc))
        except Exception as exc:
            raise CommandError(f"Unable to list source databases: {exc}")

        if database_name_filter:
            discovered_databases = [
                x for x in discovered_databases
                if (x or "").strip() == database_name_filter
            ]

        discovered_databases = sorted({(x or "").strip() for x in discovered_databases if (x or "").strip()})

        created_db_count = 0
        updated_db_count = 0
        created_table_count = 0
        updated_table_count = 0

        active_db_names = set()
        active_table_pairs = set()

        for db_name in discovered_databases:
            db_obj, created = SourceDatabaseCatalog.objects.update_or_create(
                replication_service=service,
                database_name=db_name,
                defaults={
                    "engine": service.service_type or "",
                    "is_active": True,
                },
            )

            active_db_names.add(db_name)

            if created:
                created_db_count += 1
                self.stdout.write(self.style.SUCCESS(f"[DB CREATED] {db_name}"))
            else:
                updated_db_count += 1
                self.stdout.write(self.style.WARNING(f"[DB UPDATED] {db_name}"))

            try:
                tables = list_source_tables(service, database_name=db_name)
            except AdminDBCatalogError as exc:
                self.stdout.write(self.style.ERROR(f"[DB ERROR] {db_name} -> {exc}"))
                continue
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"[DB ERROR] {db_name} -> {exc}"))
                continue

            table_names = sorted({(x or "").strip() for x in tables if (x or "").strip()})

            for table_name in table_names:
                _, created_table = SourceTableCatalog.objects.update_or_create(
                    replication_service=service,
                    source_database=db_obj,
                    table_name=table_name,
                    defaults={
                        "is_active": True,
                    },
                )

                active_table_pairs.add((db_name, table_name))

                if created_table:
                    created_table_count += 1
                    self.stdout.write(self.style.SUCCESS(f"    [TABLE CREATED] {db_name}.{table_name}"))
                else:
                    updated_table_count += 1

        if deactivate_missing:
            db_qs = SourceDatabaseCatalog.objects.filter(replication_service=service)
            if database_name_filter:
                db_qs = db_qs.filter(database_name=database_name_filter)

            for db_obj in db_qs.exclude(database_name__in=list(active_db_names)):
                if db_obj.is_active:
                    db_obj.is_active = False
                    db_obj.save(update_fields=["is_active", "discovered_at"])
                    self.stdout.write(self.style.WARNING(f"[DB DISABLED] {db_obj.database_name}"))

            table_qs = SourceTableCatalog.objects.filter(replication_service=service)
            if database_name_filter:
                table_qs = table_qs.filter(source_database__database_name=database_name_filter)

            for table_obj in table_qs.select_related("source_database"):
                key = (table_obj.source_database.database_name, table_obj.table_name)
                if key not in active_table_pairs and table_obj.is_active:
                    table_obj.is_active = False
                    table_obj.save(update_fields=["is_active", "discovered_at"])
                    self.stdout.write(
                        self.style.WARNING(
                            f"[TABLE DISABLED] {table_obj.source_database.database_name}.{table_obj.table_name}"
                        )
                    )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS("SRDF REFRESH SOURCE CATALOG - DONE"))
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS(f"databases_created = {created_db_count}"))
        self.stdout.write(self.style.SUCCESS(f"databases_updated = {updated_db_count}"))
        self.stdout.write(self.style.SUCCESS(f"tables_created    = {created_table_count}"))
        self.stdout.write(self.style.SUCCESS(f"tables_updated    = {updated_table_count}"))
        self.stdout.write("")

    def _resolve_service(self, service_id=0, service_name=""):
        if service_id:
            return ReplicationService.objects.filter(id=service_id).first()

        if service_name:
            return (
                ReplicationService.objects
                .filter(name=service_name)
                .order_by("id")
                .first()
            )

        return None