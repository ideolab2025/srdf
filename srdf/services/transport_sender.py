#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import pymysql

from django.utils import timezone
from django.db import transaction

from srdf.models import (
    AuditEvent,
    InboundChangeEvent,
    OutboundChangeEvent,
    ReplicationCheckpoint,
    ReplicationService,
    TransportBatch,
    SRDFCaptureControlItem,
    SRDFCaptureControlRun,
    SRDFCronExecution,
    SRDFDedupDecision,
    SRDFDedupRun,
    SRDFOrchestratorRun,
    SRDFServicePhaseExecution,
)


from srdf.services.node_api_client import run_remote_action

class TransportSenderError(Exception):
    pass


def send_transport_batch_once(batch_id, initiated_by="srdf_sender"):
    batch, err = _get_batch(batch_id)
    if err:
        return err
    
    if batch.status != "ready":
        return {
            "ok": True,
            "message": f"Batch #{batch.id} not in ready state (current={batch.status})",
            "batch_id": batch.id,
            "skipped": True,
        }

    target_node = batch.replication_service.target_node

    payload = {
        "batch_uid": batch.batch_uid,
        "payload": batch.payload,
        "checksum": batch.checksum,
        "initiated_by": initiated_by,
    }

    try:
        remote = run_remote_action(
            target_node,
            action_name="transport.receive_and_apply_batch",
            params=payload,
        )

        now = timezone.now()

        with transaction.atomic():
            batch.refresh_from_db()

            if batch.status != "acked":
                batch.status = "sent"
                batch.sent_at = now
                batch.last_error = ""
                batch.save(update_fields=["status", "sent_at", "last_error"])
            else:
                if not batch.sent_at:
                    batch.sent_at = now
                    batch.save(update_fields=["sent_at"])

            OutboundChangeEvent.objects.filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
                status="batched",
            ).update(
                status="sent",
                sent_at=now,
                last_error="",
            )
        _update_shipper_checkpoint(batch.replication_service, batch)            
            

        AuditEvent.objects.create(
            event_type="transport_batch_sent",
            level="info",
            node=batch.replication_service.source_node,
            message=f"Batch #{batch.id} sent to node '{target_node.name}'",
            created_by=initiated_by,
            payload={
                "batch_id": batch.id,
                "target_node_id": target_node.id,
                "remote": remote,
            },
        )

        return {
            "ok": True,
            "message": f"Batch sent to '{target_node.name}'",
            "batch_id": batch.id,
            "status": "sent",
        }


    except Exception as exc:
        error_msg = str(exc)
    
        _mark_batch_failed(batch, error_msg, initiated_by)
    
        return {
            "ok": False,
            "error_code": "SEND_FAILED",
            "message": error_msg,
            "batch_id": batch.id,
        }    


def _get_batch(batch_id):
    batch = TransportBatch.objects.select_related(
        "replication_service__target_node",
        "replication_service__source_node",
    ).filter(id=batch_id).first()


    if not batch:
        return None, {
            "ok": False,
            "error_code": "BATCH_NOT_FOUND",
            "message": f"TransportBatch #{batch_id} not found",
            "batch_id": batch_id,
        }
    
    return batch, None



def _mark_batch_failed(batch, error, initiated_by):
    with transaction.atomic():
        batch.status = "failed"
        batch.retry_count += 1
        batch.last_error = error
        batch.save(update_fields=["status", "retry_count", "last_error"])

        requeued_events = (
            OutboundChangeEvent.objects
            .filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
                status="batched",
            )
            .update(
                status="pending",
                batched_at=None,
                last_error=error,
            )
        )

    AuditEvent.objects.create(
        event_type="transport_batch_failed_requeued",
        level="error",
        node=batch.replication_service.source_node,
        message=f"Batch #{batch.id} failed and events requeued",
        created_by=initiated_by,
        payload={
            "batch_id": batch.id,
            "error": error,
            "requeued_events": requeued_events,
            "first_event_id": batch.first_event_id,
            "last_event_id": batch.last_event_id,
        },
    )
    
def resume_failed_batch_once(batch_id, initiated_by="srdf_sender"):
    batch = _get_batch(batch_id)

    if batch.status != "failed":
        return {
            "ok": True,
            "message": f"Batch #{batch.id} is not failed (current={batch.status})",
            "batch_id": batch.id,
            "skipped": True,
        }

    with transaction.atomic():
        requeued_events = (
            OutboundChangeEvent.objects
            .filter(
                replication_service=batch.replication_service,
                id__gte=batch.first_event_id,
                id__lte=batch.last_event_id,
            )
            .exclude(status__in=["acked", "applied", "dead"])
            .update(
                status="pending",
                batched_at=None,
                sent_at=None,
                acked_at=None,
                applied_at=None,
            )
        )

        batch.last_error = f"[RESUMED] {batch.last_error or ''}".strip()
        batch.save(update_fields=["last_error"])

    AuditEvent.objects.create(
        event_type="transport_batch_resumed",
        level="warning",
        node=batch.replication_service.source_node,
        message=f"Batch #{batch.id} resumed and events requeued",
        created_by=initiated_by,
        payload={
            "batch_id": batch.id,
            "requeued_events": requeued_events,
            "first_event_id": batch.first_event_id,
            "last_event_id": batch.last_event_id,
        },
    )

    return {
        "ok": True,
        "message": f"Batch #{batch.id} resumed",
        "batch_id": batch.id,
        "requeued_events": requeued_events,
        "status": batch.status,
    }


def hard_reset_transport_state_once(
    replication_service_id,
    reset_source_binlog=False,
    confirm_reset_source_binlog=False,
    initiated_by="srdf_sender",
    ):
    
    replication_service, err = _get_replication_service(replication_service_id)
    if err:
        return err    

    if reset_source_binlog and not confirm_reset_source_binlog:
        return {
            "ok": False,
            "error_code": "CONFIRMATION_REQUIRED",
            "message": "Hard reset with source binlog reset requires explicit confirmation",
        }


    outbound_count = OutboundChangeEvent.objects.filter(
        replication_service=replication_service
    ).count()

    inbound_count = InboundChangeEvent.objects.filter(
        replication_service=replication_service
    ).count()

    batch_count = TransportBatch.objects.filter(
        replication_service=replication_service
    ).count()

    checkpoint_count = ReplicationCheckpoint.objects.filter(
        replication_service=replication_service
    ).count()

    capture_control_run_count = SRDFCaptureControlRun.objects.filter(
        replication_service=replication_service
    ).count()

    capture_control_item_count = SRDFCaptureControlItem.objects.filter(
        replication_service=replication_service
    ).count()

    phase_execution_count = SRDFServicePhaseExecution.objects.filter(
        replication_service=replication_service
    ).count()

    cron_execution_count = SRDFCronExecution.objects.filter(
        replication_service=replication_service
    ).count()

    dedup_run_count = SRDFDedupRun.objects.filter(
        replication_service=replication_service
    ).count()

    dedup_decision_count = SRDFDedupDecision.objects.filter(
        replication_service=replication_service
    ).count()

    audit_event_count = AuditEvent.objects.filter(
        node_id__in=[
            replication_service.source_node_id,
            replication_service.target_node_id,
        ]
    ).count()
    
    

    with transaction.atomic():
        SRDFDedupDecision.objects.filter(
            replication_service=replication_service
        ).delete()

        SRDFDedupRun.objects.filter(
            replication_service=replication_service
        ).delete()

        SRDFCaptureControlItem.objects.filter(
            replication_service=replication_service
        ).delete()

        SRDFCaptureControlRun.objects.filter(
            replication_service=replication_service
        ).delete()

        SRDFServicePhaseExecution.objects.filter(
            replication_service=replication_service
        ).delete()

        SRDFCronExecution.objects.filter(
            replication_service=replication_service
        ).delete()

        InboundChangeEvent.objects.filter(
            replication_service=replication_service
        ).delete()

        TransportBatch.objects.filter(
            replication_service=replication_service
        ).delete()

        OutboundChangeEvent.objects.filter(
            replication_service=replication_service
        ).delete()

        ReplicationCheckpoint.objects.filter(
            replication_service=replication_service
        ).delete()

        AuditEvent.objects.filter(
            node_id__in=[
                replication_service.source_node_id,
                replication_service.target_node_id,
            ]
        ).delete()

        SRDFOrchestratorRun.objects.filter(
            phase_executions__isnull=True,
            dedup_runs__isnull=True,
        ).delete()
        
        

    binlog_reset_done = False
    if reset_source_binlog:
        _reset_source_binlog(replication_service)
        binlog_reset_done = True


    AuditEvent.objects.create(
        event_type="transport_hard_reset",
        level="warning",
        node=replication_service.source_node,
        message=f"Hard reset executed for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "outbound_deleted": outbound_count,
            "inbound_deleted": inbound_count,
            "batches_deleted": batch_count,
            "checkpoints_deleted": checkpoint_count,
            "capture_control_runs_deleted": capture_control_run_count,
            "capture_control_items_deleted": capture_control_item_count,
            "phase_executions_deleted": phase_execution_count,
            "cron_executions_deleted": cron_execution_count,
            "dedup_runs_deleted": dedup_run_count,
            "dedup_decisions_deleted": dedup_decision_count,
            "audit_events_deleted": audit_event_count,
            "binlog_reset_done": binlog_reset_done,
        },
    )
    
    

    return {
        "ok": True,
        "message": f"Hard reset completed for service '{replication_service.name}'",
        "replication_service_id": replication_service.id,
        "outbound_deleted": outbound_count,
        "inbound_deleted": inbound_count,
        "batches_deleted": batch_count,
        "checkpoints_deleted": checkpoint_count,
        "capture_control_runs_deleted": capture_control_run_count,
        "capture_control_items_deleted": capture_control_item_count,
        "phase_executions_deleted": phase_execution_count,
        "cron_executions_deleted": cron_execution_count,
        "dedup_runs_deleted": dedup_run_count,
        "dedup_decisions_deleted": dedup_decision_count,
        "audit_events_deleted": audit_event_count,
        "binlog_reset_done": binlog_reset_done,
    }


def _get_replication_service(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    
    
    if not replication_service:
        return None, {
            "ok": False,
            "error_code": "SERVICE_NOT_FOUND",
            "message": f"Enabled ReplicationService #{replication_service_id} not found",
            "replication_service_id": replication_service_id,
        }
    
    return replication_service, None



def _get_source_db_config(replication_service):
    config = replication_service.config or {}
    
    if not isinstance(config, dict):
        return None, {
            "ok": False,
            "error_code": "INVALID_CONFIG",
            "message": f"Invalid config for ReplicationService '{replication_service.name}'",
        }        

    required = ["host", "port", "user"]
    missing = [key for key in required if not config.get(key) and config.get(key) != 0]
    
    if missing:
        return None, {
            "ok": False,
            "error_code": "INVALID_CONFIG",
            "message": f"ReplicationService '{replication_service.name}' missing source DB config keys: {', '.join(missing)}"
        }        
        

    if "password" not in config:
        config["password"] = ""

    return config


def _reset_source_binlog(replication_service):
    config = _get_source_db_config(replication_service)

    conn = pymysql.connect(
        host=config["host"],
        port=int(config.get("port", 3306)),
        user=config["user"],
        password=config.get("password", ""),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )
    try:
        with conn.cursor() as cursor:
            cursor.execute("RESET MASTER")
    finally:
        conn.close()
        
        
def _update_shipper_checkpoint(replication_service, batch):
    ReplicationCheckpoint.objects.update_or_create(
        replication_service=replication_service,
        node=replication_service.source_node,
        direction="shipper",
        defaults={
            "engine": replication_service.service_type,
            "log_file": batch.payload.get("events", [{}])[-1].get("log_file", "") if batch.payload.get("events") else "",
            "log_pos": int(batch.payload.get("events", [{}])[-1].get("log_pos", 0)) if batch.payload.get("events") else 0,
            "transaction_id": "",
            "checkpoint_data": {
                "batch_id": batch.id,
                "batch_uid": batch.batch_uid,
                "status": batch.status,
                "event_count": batch.event_count,
            },
        },
    )
    
        


def ack_transport_batch_once(batch_uid, initiated_by="srdf_sender"):
    
    if not batch_uid:
        return {
            "ok": False,
            "error_code": "MISSING_BATCH_UID",
            "message": "batch_uid is required",
        }    

    batch = (
        TransportBatch.objects
        .select_related("replication_service__source_node")
        .filter(batch_uid=batch_uid)
        .order_by("-id")
        .first()
    )

    if not batch:
        return {
            "ok": False,
            "error_code": "BATCH_NOT_FOUND",
            "message": f"TransportBatch with uid '{batch_uid}' not found",
        }
    
    now = timezone.now()
    

    with transaction.atomic():
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

    AuditEvent.objects.create(
        event_type="transport_batch_acked",
        level="info",
        node=batch.replication_service.source_node,
        message=f"Batch '{batch.batch_uid}' acknowledged",
        created_by=initiated_by,
        payload={
            "batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "replication_service_id": batch.replication_service.id,
            "first_event_id": batch.first_event_id,
            "last_event_id": batch.last_event_id,
        },
    )

    return {
        "ok": True,
        "message": f"Batch '{batch.batch_uid}' acknowledged",
        "batch_id": batch.id,
        "batch_uid": batch.batch_uid,
        "status": batch.status,
    }