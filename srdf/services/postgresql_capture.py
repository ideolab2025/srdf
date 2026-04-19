#!/usr/bin/python3.1
# -*- coding: utf-8 -*-

import json
import uuid
import psycopg2
from psycopg2.extras import LogicalReplicationConnection

from django.db import transaction
from django.utils import timezone

from srdf.models import (
    AuditEvent,
    OutboundChangeEvent,
    ReplicationCheckpoint,
    ReplicationService,
)


class PostgresCaptureError(Exception):
    pass


def capture_postgres_logical_once(replication_service_id, limit=100, initiated_by="srdf_capture_postgres"):
    replication_service = _get_replication_service(replication_service_id)
    config = _get_capture_config(replication_service)

    checkpoint = _get_or_create_capture_checkpoint(replication_service)

    conn = None
    cursor = None
    captured_count = 0

    try:
        # Connexion en mode replication
        conn = psycopg2.connect(
            host=config["host"],
            port=int(config.get("port", 5432)),
            user=config["user"],
            password=config.get("password", ""),
            dbname=config["database"],
            connection_factory=LogicalReplicationConnection,
            application_name="srdf_capture",
        )
        cursor = conn.cursor()

        slot_name = config.get("replication_slot", f"srdf_{replication_service.id}")
        publication_name = config.get("publication", f"srdf_pub_{replication_service.id}")

        # Création du slot si nécessaire
        _ensure_replication_slot(cursor, slot_name, publication_name)

        # Position de départ
        if not checkpoint.log_pos:
            cursor.execute("SELECT pg_current_wal_lsn()::text")
            start_lsn = cursor.fetchone()[0]
            checkpoint.log_pos = start_lsn
            checkpoint.save(update_fields=["log_pos", "updated_at"])

        # Lecture des changements
        cursor.start_replication(
            slot_name=slot_name,
            decode=True,
            start_lsn=checkpoint.log_pos,
            status_interval=5,
        )

        while captured_count < limit:
            msg = cursor.read_message()
            if msg is None:
                break

            change = msg.payload   # déjà décodé grâce à decode=True + pgoutput

            for event in _normalize_pg_change(replication_service, change):
                _save_outbound_event(replication_service, event)
                captured_count += 1

            # Mise à jour du checkpoint
            checkpoint.log_pos = msg.data_start
            checkpoint.checkpoint_data = {"last_lsn": str(msg.data_start)}
            checkpoint.save(update_fields=["log_pos", "checkpoint_data", "updated_at"])

            cursor.send_feedback(write_lsn=msg.data_start)

        AuditEvent.objects.create(
            event_type="postgres_capture_run",
            level="info",
            node=replication_service.source_node,
            message=f"PostgreSQL logical capture completed for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={
                "replication_service_id": replication_service.id,
                "captured_count": captured_count,
                "last_lsn": str(checkpoint.log_pos),
            },
        )

        return {
            "ok": True,
            "captured_count": captured_count,
            "replication_service_id": replication_service.id,
            "last_lsn": str(checkpoint.log_pos),
        }

    except Exception as exc:
        AuditEvent.objects.create(
            event_type="postgres_capture_error",
            level="error",
            node=replication_service.source_node,
            message=f"PostgreSQL capture failed for service '{replication_service.name}'",
            created_by=initiated_by,
            payload={"error": str(exc)},
        )
        raise PostgresCaptureError(str(exc))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# ====================== HELPERS ======================

def _get_replication_service(replication_service_id):
    service = ReplicationService.objects.filter(
        id=replication_service_id, is_enabled=True, service_type="postgresql"
    ).select_related("source_node").first()
    if not service:
        raise PostgresCaptureError(f"Enabled PostgreSQL ReplicationService #{replication_service_id} not found")
    return service


def _get_capture_config(replication_service):
    config = replication_service.config or {}
    required = ["host", "database", "user"]
    missing = [k for k in required if not config.get(k)]
    if missing:
        raise PostgresCaptureError(f"Missing config keys: {missing}")
    return config


def _ensure_replication_slot(cursor, slot_name, publication_name):
    cursor.execute("SELECT 1 FROM pg_replication_slots WHERE slot_name = %s", (slot_name,))
    if not cursor.fetchone():
        cursor.execute(
            "SELECT pg_create_logical_replication_slot(%s, 'pgoutput')",
            (slot_name,)
        )
    # Publication (créée une seule fois)
    cursor.execute(f"CREATE PUBLICATION {publication_name} FOR ALL TABLES;")


def _normalize_pg_change(replication_service, change):
    # change est déjà un dict grâce à decode=True + pgoutput
    normalized = []
    for row in change.get("change", []):
        normalized.append({
            "engine": "postgresql",
            "database_name": replication_service.config.get("database", ""),
            "table_name": row.get("table", ""),
            "operation": row.get("kind", "").lower(),   # insert / update / delete
            "log_pos": str(change.get("lsn", "")),
            "primary_key_data": row.get("oldkeys", {}) or row.get("keys", {}),
            "before_data": row.get("oldkeys", {}) or {},
            "after_data": row.get("newtuple", {}) or {},
            "event_payload": row,
        })
    return normalized


@transaction.atomic
def _save_outbound_event(replication_service, item):
    OutboundChangeEvent.objects.create(
        replication_service=replication_service,
        source_node=replication_service.source_node,
        engine=item["engine"],
        database_name=item["database_name"],
        table_name=item["table_name"],
        operation=item["operation"],
        event_uid=str(uuid.uuid4()),
        log_pos=item["log_pos"],
        primary_key_data=item["primary_key_data"],
        before_data=item["before_data"],
        after_data=item["after_data"],
        event_payload=item["event_payload"],
        status="pending",
    )


def _get_or_create_capture_checkpoint(replication_service):
    checkpoint, _ = ReplicationCheckpoint.objects.get_or_create(
        replication_service=replication_service,
        node=replication_service.source_node,
        direction="capture",
        defaults={"engine": "postgresql", "log_pos": "", "checkpoint_data": {}},
    )
    return checkpoint