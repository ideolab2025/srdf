#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.db import transaction
from django.utils import timezone

from srdf.models import AuditEvent, OutboundChangeEvent, TransportBatch


class TransportCallbackHandlerError(Exception):
    pass


def handle_remote_batch_status_callback(
    batch_uid,
    final_status,
    callback_payload=None,
    initiated_by="srdf_local_callback",
):
    batch_uid = str(batch_uid or "").strip()
    final_status = str(final_status or "").strip().lower()
    callback_payload = callback_payload or {}

    if not batch_uid:
        raise TransportCallbackHandlerError("batch_uid is required")

    if final_status not in ("received", "applied", "partial", "failed"):
        raise TransportCallbackHandlerError(
            f"Unsupported final_status '{final_status}'"
        )

    batch = (
        TransportBatch.objects
        .select_related("replication_service__source_node")
        .filter(batch_uid=batch_uid)
        .order_by("-id")
        .first()
    )
    if not batch:
        raise TransportCallbackHandlerError(
            f"TransportBatch with uid '{batch_uid}' not found on source side"
        )

    now = timezone.now()

    applied_inbound_ids = callback_payload.get("applied_inbound_ids") or []
    failed_inbound_ids = callback_payload.get("failed_inbound_ids") or []
    dead_inbound_ids = callback_payload.get("dead_inbound_ids") or []

    with transaction.atomic():
        if final_status == "received":
            batch.status = "acked"
            if not batch.sent_at:
                batch.sent_at = now
            batch.acked_at = now
            batch.last_error = ""
            batch.save(update_fields=["status", "sent_at", "acked_at", "last_error"])

            OutboundChangeEvent.objects.filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
            ).exclude(status__in=["dead"]).update(
                status="acked",
                sent_at=now,
                acked_at=now,
                last_error="",
            )

        elif final_status == "applied":
            batch.status = "acked"
            if not batch.sent_at:
                batch.sent_at = now
            batch.acked_at = now
            batch.last_error = ""
            batch.save(update_fields=["status", "sent_at", "acked_at", "last_error"])

            OutboundChangeEvent.objects.filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
            ).exclude(status__in=["dead"]).update(
                status="applied",
                sent_at=now,
                acked_at=now,
                applied_at=now,
                last_error="",
            )

        elif final_status == "partial":
            batch.status = "acked"
            if not batch.sent_at:
                batch.sent_at = now
            batch.acked_at = now
            batch.last_error = "Remote apply partial"
            batch.save(update_fields=["status", "sent_at", "acked_at", "last_error"])

            OutboundChangeEvent.objects.filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
                status__in=["batched", "sent", "acked"],
            ).exclude(status__in=["dead"]).update(
                status="acked",
                sent_at=now,
                acked_at=now,
            )

        elif final_status == "failed":
            batch.status = "failed"
            if not batch.sent_at:
                batch.sent_at = now
            batch.last_error = str(callback_payload.get("error") or "Remote apply failed")
            batch.retry_count = int(batch.retry_count or 0) + 1
            batch.save(update_fields=["status", "sent_at", "last_error", "retry_count"])

            OutboundChangeEvent.objects.filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
            ).exclude(status__in=["dead", "applied"]).update(
                status="failed",
                sent_at=now,
                last_error=batch.last_error,
            )

    AuditEvent.objects.create(
        event_type="transport_remote_batch_status_callback",
        level="info" if final_status in ("received", "applied") else "warning",
        node=batch.replication_service.source_node if batch.replication_service else None,
        message=f"Remote callback processed for batch '{batch_uid}'",
        created_by=initiated_by,
        payload={
            "batch_uid": batch_uid,
            "batch_id": batch.id,
            "replication_service_id": batch.replication_service.id if batch.replication_service else 0,
            "final_status": final_status,
            "applied_inbound_ids": applied_inbound_ids[:200],
            "failed_inbound_ids": failed_inbound_ids[:200],
            "dead_inbound_ids": dead_inbound_ids[:200],
            "callback_payload": callback_payload,
        },
    )

    return {
        "ok": True,
        "message": f"Remote callback processed for batch '{batch_uid}'",
        "batch_uid": batch_uid,
        "batch_id": batch.id,
        "final_status": final_status,
        "transport_batch_status": batch.status,
    }