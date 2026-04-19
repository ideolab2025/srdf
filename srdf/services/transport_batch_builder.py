#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import hashlib
import json
import uuid

from django.db import transaction
from django.utils import timezone

from srdf.models import AuditEvent, OutboundChangeEvent, ReplicationService, TransportBatch


class TransportBatchBuilderError(Exception):
    pass


def build_transport_batch_once(replication_service_id, limit=100, initiated_by="srdf_batch_builder"):
    replication_service = _get_replication_service(replication_service_id)
    pending_events = _get_pending_events(replication_service, limit=limit)

    if not pending_events:
        return {
            "ok": True,
            "message": f"No pending outbound events for service '{replication_service.name}'",
            "replication_service_id": replication_service.id,
            "batch_created": False,
            "event_count": 0,
        }

    payload_events = [_serialize_event(event) for event in pending_events]
    payload = {
        "version": 1,
        "engine": replication_service.service_type,
        "replication_service_id": replication_service.id,
        "replication_service_name": replication_service.name,
        "source_node_id": replication_service.source_node_id,
        "target_node_id": replication_service.target_node_id,
        "created_at": timezone.now().isoformat(),
        "event_count": len(payload_events),
        "events": payload_events,
    }

    checksum = _compute_checksum(payload)
    batch_uid = str(uuid.uuid4())
    now = timezone.now()

    with transaction.atomic():
        batch = TransportBatch.objects.create(
            replication_service=replication_service,
            batch_uid=batch_uid,
            event_count=len(pending_events),
            first_event_id=pending_events[0].id,
            last_event_id=pending_events[-1].id,
            payload=payload,
            compression="none",
            checksum=checksum,
            status="ready",
            built_at=now,
        )

        OutboundChangeEvent.objects.filter(
            id__in=[event.id for event in pending_events]
        ).update(
            status="batched",
            batched_at=now,
        )

    AuditEvent.objects.create(
        event_type="transport_batch_built",
        level="info",
        node=replication_service.source_node,
        message=f"TransportBatch built for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "transport_batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "event_count": batch.event_count,
            "first_event_id": batch.first_event_id,
            "last_event_id": batch.last_event_id,
            "checksum": batch.checksum,
        },
    )

    return {
        "ok": True,
        "message": f"TransportBatch built for service '{replication_service.name}'",
        "replication_service_id": replication_service.id,
        "transport_batch_id": batch.id,
        "batch_uid": batch.batch_uid,
        "batch_created": True,
        "event_count": batch.event_count,
        "first_event_id": batch.first_event_id,
        "last_event_id": batch.last_event_id,
        "checksum": batch.checksum,
        "status": batch.status,
    }


def _get_replication_service(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    if not replication_service:
        raise TransportBatchBuilderError(
            f"Enabled ReplicationService #{replication_service_id} not found"
        )
    return replication_service


def _get_pending_events(replication_service, limit=100):
    limit = max(int(limit or 100), 1)

    pending_events = list(
        OutboundChangeEvent.objects
        .filter(
            replication_service=replication_service,
            status="pending",
        )
        .order_by("id")[: max(limit * 10, limit)]
    )

    if not pending_events:
        return []

    ordered_events = _reorder_events_for_transport(pending_events)
    return ordered_events[:limit]



def _reorder_events_for_transport(events):
    decorated = []

    for event in events:
        decorated.append(
            (
                _compute_transport_priority(event),
                int(event.id or 0),
                event,
            )
        )

    decorated.sort(key=lambda row: (row[0], row[1]))
    return [row[2] for row in decorated]


def _compute_transport_priority(event):
    event_family = str(getattr(event, "event_family", "") or "dml").strip().lower()
    object_type = str(getattr(event, "object_type", "") or "").strip().lower()

    if event_family == "ddl":
        if object_type == "database":
            return 10
        if object_type == "table":
            return 20
        if object_type in ("column", "index"):
            return 30
        return 40

    return 100



def _serialize_event(event):
    return {
        "id": event.id,
        "event_uid": event.event_uid or "",
        "transaction_id": event.transaction_id or "",
        "engine": event.engine or "",
        "event_family": event.event_family or "dml",
        "database_name": event.database_name or "",
        "table_name": event.table_name or "",
        "operation": event.operation or "",
        "object_type": event.object_type or "",
        "ddl_action": event.ddl_action or "",
        "object_name": event.object_name or "",
        "parent_object_name": event.parent_object_name or "",
        "raw_sql": event.raw_sql or "",
        "log_file": event.log_file or "",
        "log_pos": int(event.log_pos or 0),
        "primary_key_data": event.primary_key_data or {},
        "before_data": event.before_data or {},
        "after_data": event.after_data or {},
        "event_payload": event.event_payload or {},
        "created_at": event.created_at.isoformat() if event.created_at else None,
    }


def _compute_checksum(payload):
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()