#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.db import transaction
from django.utils import timezone

from srdf.models import AuditEvent, InboundChangeEvent, ReplicationService, TransportBatch
from srdf.services.transport_receiver import receive_transport_batch_once
from srdf.services.transport_applier import apply_inbound_events_once
from srdf.services.node_api_client import run_remote_action


class TransportRemoteOrchestratorError(Exception):
    pass


def _send_callback_to_source(replication_service, callback_payload, initiated_by):
    if not replication_service:
        raise TransportRemoteOrchestratorError("replication_service is required for callback")

    source_node = replication_service.source_node
    final_status = str(callback_payload.get("final_status") or "").strip().lower()
    if not final_status:
        raise TransportRemoteOrchestratorError("callback_payload.final_status is required")

    return run_remote_action(
        source_node,
        action_name="transport.batch_status_callback",
        params={
            "batch_uid": callback_payload.get("batch_uid") or "",
            "final_status": final_status,
            "callback_payload": callback_payload,
            "initiated_by": initiated_by,
        },
    )


def receive_and_apply_remote_batch_once(
    batch_uid,
    payload,
    checksum,
    initiated_by="srdf_remote_orchestrator",
    apply_limit=None,
    ):
    
    if not batch_uid:
        raise TransportRemoteOrchestratorError("batch_uid is required")

    if not isinstance(payload, dict):
        raise TransportRemoteOrchestratorError("payload must be a dict")

    replication_service = _get_replication_service_from_payload(payload)

    receive_result = receive_transport_batch_once(
        batch_uid=batch_uid,
        payload=payload,
        checksum=checksum,
        initiated_by=initiated_by,
    )

    batch = _get_transport_batch_by_uid(batch_uid)
    inbound_qs = InboundChangeEvent.objects.filter(
        replication_service=replication_service,
        transport_batch=batch,
    ).order_by("id")

    received_count = inbound_qs.count()

    if apply_limit is None:
        apply_limit = max(int(received_count or 0), 1)

    apply_result = apply_inbound_events_once(
        replication_service_id=replication_service.id,
        limit=int(apply_limit),
        initiated_by=initiated_by,
        retry_failed=False,
    )

    applied_count = int(apply_result.get("applied_count") or 0)
    failed_count = int(apply_result.get("failed_count") or 0)
    processed_count = int(apply_result.get("processed_count") or 0)

    final_status = "applied"
    if failed_count > 0 and applied_count > 0:
        final_status = "partial"
    elif failed_count > 0:
        final_status = "failed"

    callback_payload = {
        "batch_uid": batch_uid,
        "replication_service_id": replication_service.id,
        "replication_service_name": replication_service.name,
        "target_node_id": replication_service.target_node_id,
        "received_count": int(received_count),
        "processed_count": processed_count,
        "applied_count": applied_count,
        "failed_count": failed_count,
        "final_status": final_status,
        "applied_inbound_ids": _get_inbound_ids_for_status(
            replication_service=replication_service,
            transport_batch=batch,
            status="applied",
            limit=500,
        ),
        "failed_inbound_ids": _get_inbound_ids_for_status(
            replication_service=replication_service,
            transport_batch=batch,
            status="failed",
            limit=500,
        ),
        "dead_inbound_ids": _get_inbound_ids_for_status(
            replication_service=replication_service,
            transport_batch=batch,
            status="dead",
            limit=500,
        ),
        "applied_at": timezone.now().isoformat(),
    }
    
    

    callback_result = None
    callback_error = ""

    try:
        callback_result = _send_callback_to_source(
            replication_service=replication_service,
            callback_payload=callback_payload,
            initiated_by=initiated_by,
        )
    except Exception as exc:
        callback_error = str(exc)

    AuditEvent.objects.create(
        event_type="transport_remote_orchestrator_completed",
        level="info" if failed_count == 0 and not callback_error else "warning",
        node=replication_service.target_node,
        message=f"Remote transport orchestrator completed for batch '{batch_uid}'",
        created_by=initiated_by,
        payload={
            "batch_uid": batch_uid,
            "replication_service_id": replication_service.id,
            "receive_result": receive_result,
            "apply_result": apply_result,
            "callback_payload": callback_payload,
            "callback_result": callback_result or {},
            "callback_error": callback_error,
        },
    )

    return {
        "ok": failed_count == 0 and not bool(callback_error),
        "message": f"Remote transport orchestrator completed for batch '{batch_uid}'",
        "batch_uid": batch_uid,
        "replication_service_id": replication_service.id,
        "replication_service_name": replication_service.name,
        "receive_result": receive_result,
        "apply_result": apply_result,
        "callback_payload": callback_payload,
        "callback_result": callback_result or {},
        "callback_error": callback_error,
        "final_status": final_status,
    }



def _get_replication_service_from_payload(payload):
    replication_service_id = payload.get("replication_service_id")
    replication_service_name = str(payload.get("replication_service_name") or "").strip()

    qs = ReplicationService.objects.select_related("source_node", "target_node").filter(is_enabled=True)

    replication_service = None

    if replication_service_name:
        replication_service = qs.filter(name=replication_service_name).first()

    if not replication_service and replication_service_id:
        replication_service = qs.filter(id=replication_service_id).first()

    if not replication_service:
        raise TransportRemoteOrchestratorError(
            "Unable to resolve target-local ReplicationService from payload"
        )

    return replication_service


def _get_transport_batch_by_uid(batch_uid):
    batch = (
        TransportBatch.objects
        .filter(batch_uid=batch_uid)
        .order_by("-id")
        .first()
    )
    if not batch:
        raise TransportRemoteOrchestratorError(
            f"TransportBatch with uid '{batch_uid}' not found on remote side"
        )
    return batch


def _get_inbound_ids_for_status(replication_service, transport_batch, status, limit=500):
    return list(
        InboundChangeEvent.objects
        .filter(
            replication_service=replication_service,
            transport_batch=transport_batch,
            status=status,
        )
        .order_by("id")
        .values_list("id", flat=True)[: max(int(limit or 1), 1)]
    )