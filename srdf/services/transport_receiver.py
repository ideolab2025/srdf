#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import hashlib
import json

from django.db import transaction

from srdf.models import AuditEvent, InboundChangeEvent, ReplicationService, TransportBatch
from srdf.services.node_api_client import run_remote_action

class TransportReceiverError(Exception):
    pass


def receive_transport_batch_once(batch_uid, payload, checksum, initiated_by="srdf_receiver"):
    if not batch_uid:
        raise TransportReceiverError("batch_uid is required")

    if not isinstance(payload, dict):
        raise TransportReceiverError("payload must be a dict")

    if not checksum:
        raise TransportReceiverError("checksum is required")

    expected_checksum = _compute_checksum(payload)
    if checksum != expected_checksum:
        AuditEvent.objects.create(
            event_type="transport_batch_receive_error",
            level="error",
            message=f"Checksum mismatch for incoming batch '{batch_uid}'",
            created_by=initiated_by,
            payload={
                "batch_uid": batch_uid,
                "checksum_received": checksum,
                "checksum_expected": expected_checksum,
            },
        )
        raise TransportReceiverError("checksum mismatch")

    replication_service = _get_replication_service_from_payload(payload)
    source_batch = _get_source_batch(batch_uid)

    events = payload.get("events") or []
    if not isinstance(events, list):
        raise TransportReceiverError("payload.events must be a list")

    created_count = 0
    duplicate_count = 0
    inbound_ids = []

    with transaction.atomic():
        for item in events:
            event_uid = (item.get("event_uid") or "").strip()

            if event_uid:
                exists = InboundChangeEvent.objects.filter(
                    replication_service=replication_service,
                    event_uid=event_uid,
                ).exists()
                if exists:
                    duplicate_count += 1
                    continue


            inbound = InboundChangeEvent.objects.create(
                replication_service=replication_service,
                target_node=replication_service.target_node,
                transport_batch=source_batch,
                engine=item.get("engine") or replication_service.service_type or "mariadb",
                event_family=item.get("event_family") or "dml",
                database_name=item.get("database_name") or "",
                table_name=item.get("table_name") or "",
                operation=item.get("operation") or "",
                object_type=item.get("object_type") or "",
                ddl_action=item.get("ddl_action") or "",
                object_name=item.get("object_name") or "",
                parent_object_name=item.get("parent_object_name") or "",
                raw_sql=item.get("raw_sql") or "",
                event_uid=event_uid,
                transaction_id=item.get("transaction_id") or "",
                source_log_file=item.get("log_file") or "",
                source_log_pos=int(item.get("log_pos") or 0),
                primary_key_data=item.get("primary_key_data") or {},
                before_data=item.get("before_data") or {},
                after_data=item.get("after_data") or {},
                event_payload=item.get("event_payload") or {},
                status="received",
            )
            
            
            inbound_ids.append(inbound.id)
            created_count += 1



    ack_remote = None
    try:
        ack_remote = run_remote_action(
            replication_service.source_node,
            action_name="transport.ack_batch",
            params={
                "batch_uid": batch_uid,
            },
        )
    except Exception as exc:
        AuditEvent.objects.create(
            event_type="transport_batch_ack_error",
            level="error",
            node=replication_service.target_node,
            message=f"ACK failed for incoming batch '{batch_uid}'",
            created_by=initiated_by,
            payload={
                "batch_uid": batch_uid,
                "replication_service_id": replication_service.id,
                "error": str(exc),
            },
        )

    AuditEvent.objects.create(
        event_type="transport_batch_received",
        level="info",
        node=replication_service.target_node,
        message=f"Incoming TransportBatch received for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "batch_uid": batch_uid,
            "replication_service_id": replication_service.id,
            "created_count": created_count,
            "duplicate_count": duplicate_count,
            "inbound_ids": inbound_ids,
            "ack_remote": ack_remote or {},
        },
    )

    return {
        "ok": True,
        "message": f"Incoming batch processed for service '{replication_service.name}'",
        "batch_uid": batch_uid,
        "replication_service_id": replication_service.id,
        "created_count": created_count,
        "duplicate_count": duplicate_count,
        "status": "received",
        "ack_remote": ack_remote or {},
    }



def _get_replication_service_from_payload(payload):
    replication_service_id = payload.get("replication_service_id")
    if not replication_service_id:
        raise TransportReceiverError("payload.replication_service_id is required")

    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    if not replication_service:
        raise TransportReceiverError(
            f"Enabled ReplicationService #{replication_service_id} not found"
        )

    return replication_service


def _get_source_batch(batch_uid):
    return (
        TransportBatch.objects
        .filter(batch_uid=batch_uid)
        .order_by("-id")
        .first()
    )


def _compute_checksum(payload):
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()