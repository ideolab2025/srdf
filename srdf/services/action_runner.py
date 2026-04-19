#! /usr/bin/python3.1
# -*- coding: utf-8 -*-




import time

from django.utils import timezone

from srdf.models import ActionExecution, AuditEvent, Node, ReplicationService
from srdf.services.failover_runner import execute_failover, prepare_failover
from srdf.services.node_api_client import get_status, ping_node, run_remote_action
from srdf.services.mariadb_replication import (
    get_replication_status,
    stop_replica,
    start_replica,
    promote,
    set_read_only,
)
from srdf.services.transport_batch_builder import build_transport_batch_once


from srdf.services.transport_sender import (
    send_transport_batch_once,
    ack_transport_batch_once,
    resume_failed_batch_once,
    hard_reset_transport_state_once,
)


from srdf.services.transport_receiver import receive_transport_batch_once
from srdf.services.transport_applier import apply_inbound_events_once
from srdf.services.transport_remote_orchestrator import receive_and_apply_remote_batch_once

from srdf.services.transport_callback_handler import handle_remote_batch_status_callback

ALLOWED_ACTIONS = {
    "node.ping",
    "node.refresh_status",
    "failover.prepare",
    "failover.execute",
    "failover.execute_dry_run",
    "replication.check",
    "transport.build_batch",
    "transport.send_batch",
    "transport.resume_batch",
    "transport.hard_reset",
    "transport.receive_batch",
    "transport.receive_and_apply_batch",
    "transport.ack_batch",
    "transport.apply_inbound",
    "transport.batch_status_callback",
    "mariadb.status",
    "mariadb.stop_replica",
    "mariadb.start_replica",
    "mariadb.promote",
    "mariadb.set_read_only",
}




def run_srdf_action(action_name, node_id=None, params=None, initiated_by="api"):
    params = params or {}

    if action_name not in ALLOWED_ACTIONS:
        raise ValueError(f"Action '{action_name}' is not allowed")


    node = None
    if node_id:
        node = Node.objects.get(pk=node_id)
    else:
        node = _resolve_node_for_action(action_name=action_name, params=params)

    started = timezone.now()
    started_perf = time.perf_counter()

    execution = ActionExecution.objects.create(
        node=node,
        action_name=action_name,
        params=params,
        status="running",
        started_at=started,
        initiated_by=initiated_by,
    )

    try:
        result_payload = _execute_action(
            action_name=action_name,
            node=node,
            params=params,
            initiated_by=initiated_by,
        )

        finished = timezone.now()
        duration_ms = int((time.perf_counter() - started_perf) * 1000)

        execution.status = "success"
        execution.result_payload = result_payload
        execution.finished_at = finished
        execution.duration_ms = duration_ms
        execution.stdout = _safe_pretty_text(result_payload)
        execution.save(
            update_fields=[
                "status",
                "result_payload",
                "finished_at",
                "duration_ms",
                "stdout",
            ]
        )

        AuditEvent.objects.create(
            event_type="api_action",
            level="info",
            node=node,
            message=f"SRDF action executed: {action_name}",
            created_by=initiated_by,
            payload={
                "execution_id": execution.id,
                "action_name": action_name,
                "params": params,
                "result_payload": result_payload,
            },
        )

        return execution

    except Exception as exc:
        finished = timezone.now()
        duration_ms = int((time.perf_counter() - started_perf) * 1000)

        execution.status = "failed"
        execution.stderr = str(exc)
        execution.finished_at = finished
        execution.duration_ms = duration_ms
        execution.save(
            update_fields=["status", "stderr", "finished_at", "duration_ms"]
        )

        AuditEvent.objects.create(
            event_type="api_action_error",
            level="error",
            node=node,
            message=f"SRDF action failed: {action_name}",
            created_by=initiated_by,
            payload={
                "execution_id": execution.id,
                "action_name": action_name,
                "params": params,
                "error": str(exc),
            },
        )
        raise
    
    
def _resolve_node_for_action(action_name, params):
    params = params or {}


    if action_name in ("transport.receive_batch", "transport.receive_and_apply_batch"):
        payload = params.get("payload") or {}
        replication_service_id = payload.get("replication_service_id")
        if replication_service_id:
            replication_service = (
                ReplicationService.objects
                .select_related("target_node")
                .filter(id=replication_service_id, is_enabled=True)
                .first()
            )
            if replication_service and replication_service.target_node_id:
                return replication_service.target_node
            
            

    if action_name == "transport.apply_inbound":
        replication_service_id = params.get("replication_service_id")
        if replication_service_id:
            replication_service = (
                ReplicationService.objects
                .select_related("target_node")
                .filter(id=replication_service_id, is_enabled=True)
                .first()
            )
            if replication_service and replication_service.target_node_id:
                return replication_service.target_node

    return None


def _get_mariadb_config_for_node(node):
    if not node:
        raise ValueError("node is required")

    replication_service = (
        ReplicationService.objects.filter(
            service_type="mariadb",
            is_enabled=True,
        )
        .filter(source_node=node)
        .order_by("id")
        .first()
    )

    if not replication_service:
        replication_service = (
            ReplicationService.objects.filter(
                service_type="mariadb",
                is_enabled=True,
            )
            .filter(target_node=node)
            .order_by("id")
            .first()
        )

    if not replication_service:
        raise ValueError(f"No enabled MariaDB ReplicationService found for node '{node.name}'")

    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise ValueError(f"Invalid MariaDB config for ReplicationService '{replication_service.name}'")

    return config


def _execute_action(action_name, node, params, initiated_by="api"):
    if action_name == "node.ping":
        if not node:
            raise ValueError("node_id is required for node.ping")
        payload = ping_node(node)
        return {
            "message": f"Ping OK for node '{node.name}'",
            "node_id": node.id,
            "remote_payload": payload,
        }

    if action_name == "node.refresh_status":
        if not node:
            raise ValueError("node_id is required for node.refresh_status")
        payload = get_status(node)
        return {
            "message": f"Status refreshed for node '{node.name}'",
            "node_id": node.id,
            "remote_payload": payload,
        }

    if action_name.startswith("mariadb."):
        if not node:
            raise ValueError("node_id required for mariadb actions")

        config = _get_mariadb_config_for_node(node)

        if action_name == "mariadb.status":
            status = get_replication_status(config)
            return {
                "message": f"MariaDB status collected for node '{node.name}'",
                "node_id": node.id,
                "mariadb_status": status,
            }

        if action_name == "mariadb.stop_replica":
            stop_replica(config)
            return {
                "message": f"MariaDB replica stopped for node '{node.name}'",
                "node_id": node.id,
            }

        if action_name == "mariadb.start_replica":
            start_replica(config)
            return {
                "message": f"MariaDB replica started for node '{node.name}'",
                "node_id": node.id,
            }

        if action_name == "mariadb.promote":
            promote(config)
            return {
                "message": f"MariaDB node promoted: '{node.name}'",
                "node_id": node.id,
            }

        if action_name == "mariadb.set_read_only":
            value = params.get("value", True)
            set_read_only(config, value)
            return {
                "message": f"MariaDB read_only updated for node '{node.name}'",
                "node_id": node.id,
                "value": bool(value),
            }
        
        
    if action_name == "transport.build_batch":
        replication_service_id = params.get("replication_service_id")
        limit = params.get("limit", 100)

        if not replication_service_id:
            raise ValueError("replication_service_id is required for transport.build_batch")

        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise ValueError("limit must be an integer")

        result = build_transport_batch_once(
            replication_service_id=replication_service_id,
            limit=limit,
            initiated_by=initiated_by,
        )

        return result    
    
    
    if action_name == "transport.send_batch":
        batch_id = params.get("batch_id")

        if not batch_id:
            raise ValueError("batch_id is required for transport.send_batch")

        try:
            batch_id = int(batch_id)
        except (TypeError, ValueError):
            raise ValueError("batch_id must be an integer")

        result = send_transport_batch_once(
            batch_id=batch_id,
            initiated_by=initiated_by,
        )

        return result   
    
    
    if action_name == "transport.resume_batch":
        batch_id = params.get("batch_id")

        if not batch_id:
            raise ValueError("batch_id is required for transport.resume_batch")

        try:
            batch_id = int(batch_id)
        except (TypeError, ValueError):
            raise ValueError("batch_id must be an integer")

        result = resume_failed_batch_once(
            batch_id=batch_id,
            initiated_by=initiated_by,
        )

        return result   
    
    
    if action_name == "transport.hard_reset":
        replication_service_id = params.get("replication_service_id")
        reset_source_binlog = bool(params.get("reset_source_binlog", False))
        confirm_reset_source_binlog = bool(params.get("confirm_reset_source_binlog", False))

        if not replication_service_id:
            raise ValueError("replication_service_id is required for transport.hard_reset")

        try:
            replication_service_id = int(replication_service_id)
        except (TypeError, ValueError):
            raise ValueError("replication_service_id must be an integer")

        result = hard_reset_transport_state_once(
            replication_service_id=replication_service_id,
            reset_source_binlog=reset_source_binlog,
            confirm_reset_source_binlog=confirm_reset_source_binlog,
            initiated_by=initiated_by,
        )

        return result    
    
    if action_name == "transport.receive_batch":
        batch_uid = params.get("batch_uid")
        payload = params.get("payload")
        checksum = params.get("checksum")

        result = receive_transport_batch_once(
            batch_uid=batch_uid,
            payload=payload,
            checksum=checksum,
            initiated_by=initiated_by,
        )

        return result  
    
    
    if action_name == "transport.receive_and_apply_batch":
        batch_uid = params.get("batch_uid")
        payload = params.get("payload")
        checksum = params.get("checksum")
        apply_limit = params.get("apply_limit")

        if apply_limit is not None:
            try:
                apply_limit = int(apply_limit)
            except (TypeError, ValueError):
                raise ValueError("apply_limit must be an integer")

        result = receive_and_apply_remote_batch_once(
            batch_uid=batch_uid,
            payload=payload,
            checksum=checksum,
            initiated_by=initiated_by,
            apply_limit=apply_limit,
        )

        return result    
    
    
    if action_name == "transport.ack_batch":
        batch_uid = (params.get("batch_uid") or "").strip()
        if not batch_uid:
            raise ValueError("batch_uid is required for transport.ack_batch")

        result = ack_transport_batch_once(
            batch_uid=batch_uid,
            initiated_by=initiated_by,
        )
        return result
    
    
    if action_name == "transport.batch_status_callback":
        batch_uid = (params.get("batch_uid") or "").strip()
        final_status = (params.get("final_status") or "").strip().lower()
        callback_payload = params.get("callback_payload") or {}

        if not batch_uid:
            raise ValueError("batch_uid is required for transport.batch_status_callback")

        if not final_status:
            raise ValueError("final_status is required for transport.batch_status_callback")

        result = handle_remote_batch_status_callback(
            batch_uid=batch_uid,
            final_status=final_status,
            callback_payload=callback_payload,
            initiated_by=initiated_by,
        )
        return result    
    
    

    if action_name == "transport.apply_inbound":
        replication_service_id = params.get("replication_service_id")
        limit = params.get("limit", 100)

        if not replication_service_id:
            raise ValueError("replication_service_id is required for transport.apply_inbound")

        try:
            limit = int(limit)
        except (TypeError, ValueError):
            raise ValueError("limit must be an integer")

        result = apply_inbound_events_once(
            replication_service_id=replication_service_id,
            limit=limit,
            initiated_by=initiated_by,
        )

        return result    
    
    

    if action_name == "replication.check":
        if node:
            remote = run_remote_action(node, action_name, params=params)
            return {
                "message": f"Replication check executed for node '{node.name}'",
                "node_id": node.id,
                "remote_payload": remote,
            }
        return {
            "message": "Replication check requested",
            "node_id": None,
            "params": params,
        }

    if action_name == "failover.prepare":
        return prepare_failover(
            plan_id=params.get("plan_id"),
            initiated_by=initiated_by,
        )

    if action_name == "failover.execute":
        return execute_failover(
            plan_id=params.get("plan_id"),
            initiated_by=initiated_by,
            dry_run=False,
        )

    if action_name == "failover.execute_dry_run":
        return execute_failover(
            plan_id=params.get("plan_id"),
            initiated_by=initiated_by,
            dry_run=True,
        )

    raise ValueError(f"Unsupported action '{action_name}'")


def _safe_pretty_text(payload):
    try:
        import json
        return json.dumps(payload, indent=2, ensure_ascii=False)
    except Exception:
        return str(payload)