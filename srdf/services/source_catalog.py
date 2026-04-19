#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from srdf.models import (
    ReplicationService,
    SourceDatabaseCatalog,
    SourceTableCatalog,
)
from srdf.services.admin_db_catalog import list_source_databases, list_source_tables


class SourceCatalogError(Exception):
    pass


def refresh_source_catalog(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    if not replication_service:
        raise SourceCatalogError(f"ReplicationService #{replication_service_id} not found")

    engine = replication_service.service_type
    database_names = list_source_databases(replication_service)

    SourceDatabaseCatalog.objects.filter(replication_service=replication_service).update(is_active=False)
    SourceTableCatalog.objects.filter(replication_service=replication_service).update(is_active=False)

    database_count = 0
    table_count = 0

    for database_name in database_names:
        db_obj, _ = SourceDatabaseCatalog.objects.update_or_create(
            replication_service=replication_service,
            database_name=database_name,
            defaults={
                "engine": engine,
                "is_active": True,
            },
        )
        database_count += 1

        table_names = list_source_tables(replication_service, database_name)
        for table_name in table_names:
            SourceTableCatalog.objects.update_or_create(
                replication_service=replication_service,
                source_database=db_obj,
                table_name=table_name,
                defaults={
                    "is_active": True,
                },
            )
            table_count += 1

    return {
        "ok": True,
        "replication_service_id": replication_service.id,
        "database_count": database_count,
        "table_count": table_count,
    }