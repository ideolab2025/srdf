#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import uuid

from django.utils import timezone

from srdf.models import AuditEvent, FailoverPlan, Node, ReplicationService
from srdf.services.failover_validator import validate_failover_plan
from srdf.services.locks import LockError, acquire_lock, release_lock
from srdf.services.node_api_client import run_remote_action
from srdf.services.mariadb_replication import (
    get_replication_status,
    stop_replica,
    start_replica,
    promote,
    set_read_only,
)


FAILOVER_LOCK_NAME = "global_failover_lock"


class FailoverRunnerError(Exception):
    pass


def prepare_failover(plan_id=None, initiated_by="system"):
    plan = _resolve_plan(plan_id)
    validation = validate_failover_plan(plan)
    if not validation["ok"]:
        raise FailoverRunnerError(
            "Invalid failover plan: " + " | ".join(validation["errors"])
        )

    lock_acquired = False

    try:
        acquire_lock(FAILOVER_LOCK_NAME)
        lock_acquired = True

        plan.status = "ready"
        plan.last_error = ""
        plan.save(update_fields=["status", "last_error", "updated_at"])

        steps = _get_steps(plan)
        payload = {
            "message": f"Failover plan '{plan.name}' prepared",
            "plan_id": plan.id,
            "steps_count": len(steps),
            "dry_run_supported": True,
            "validation": validation,
        }

        AuditEvent.objects.create(
            event_type="failover_prepare",
            level="info",
            message=f"Failover prepared: {plan.name}",
            created_by=initiated_by,
            payload=payload,
        )
        return payload

    except LockError as exc:
        raise FailoverRunnerError(str(exc))
    finally:
        if lock_acquired:
            release_lock(FAILOVER_LOCK_NAME)


def execute_failover(plan_id=None, initiated_by="system", dry_run=False):
    plan = _resolve_plan(plan_id)

    validation = validate_failover_plan(plan)
    if not validation["ok"]:
        raise FailoverRunnerError(
            "Invalid failover plan: " + " | ".join(validation["errors"])
        )

    steps = _get_steps(plan)
    if not steps:
        raise FailoverRunnerError(f"Failover plan '{plan.name}' has no steps")

    lock_acquired = False
    execution_results = []

    try:
        acquire_lock(FAILOVER_LOCK_NAME)
        lock_acquired = True

        plan.status = "running"
        plan.started_at = timezone.now()
        plan.last_error = ""
        plan.save(update_fields=["status", "started_at", "last_error", "updated_at"])

        for index, step in enumerate(steps, start=1):
            step_result = _execute_step(
                plan=plan,
                step=step,
                step_index=index,
                initiated_by=initiated_by,
                dry_run=dry_run,
            )
            execution_results.append(step_result)

            AuditEvent.objects.create(
                event_type="failover_step_success",
                level="info",
                message=f"Failover step success: {plan.name} step #{index}",
                created_by=initiated_by,
                payload={
                    "plan_id": plan.id,
                    "step_index": index,
                    "step_result": step_result,
                    "dry_run": dry_run,
                },
            )

        plan.status = "success"
        plan.finished_at = timezone.now()
        plan.save(update_fields=["status", "finished_at", "updated_at"])

        payload = {
            "message": f"Failover plan '{plan.name}' executed successfully",
            "plan_id": plan.id,
            "dry_run": dry_run,
            "steps_count": len(steps),
            "validation": validation,
            "results": execution_results,
        }

        AuditEvent.objects.create(
            event_type="failover_execute",
            level="info",
            message=f"Failover executed: {plan.name}",
            created_by=initiated_by,
            payload=payload,
        )

        return payload

    except Exception as exc:
        plan.status = "failed"
        plan.last_error = str(exc)
        plan.finished_at = timezone.now()
        plan.save(update_fields=["status", "last_error", "finished_at", "updated_at"])

        AuditEvent.objects.create(
            event_type="failover_execute_error",
            level="error",
            message=f"Failover execution failed: {plan.name}",
            created_by=initiated_by,
            payload={
                "plan_id": plan.id,
                "error": str(exc),
                "dry_run": dry_run,
                "results": execution_results,
            },
        )
        raise FailoverRunnerError(str(exc))
    finally:
        if lock_acquired:
            release_lock(FAILOVER_LOCK_NAME)


def _resolve_plan(plan_id=None):
    if plan_id:
        plan = FailoverPlan.objects.filter(pk=plan_id).first()
        if not plan:
            raise FailoverRunnerError(f"FailoverPlan #{plan_id} not found")
        return plan

    plan = FailoverPlan.objects.filter(status__in=["draft", "ready"]).order_by("-updated_at", "-id").first()
    if not plan:
        raise FailoverRunnerError("No available failover plan found")

    return plan


def _get_steps(plan):
    plan_data = plan.plan_data or {}
    steps = plan_data.get("steps") or []
    return [step for step in steps if isinstance(step, dict)]


def _get_mariadb_config_for_node(node):
    if not node:
        raise FailoverRunnerError("node is required")

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
        raise FailoverRunnerError(
            f"No enabled MariaDB ReplicationService found for node '{node.name}'"
        )

    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise FailoverRunnerError(
            f"Invalid MariaDB config for ReplicationService '{replication_service.name}'"
        )

    return config

def _execute_step(plan, step, step_index, initiated_by, dry_run=False):
    action_name = (step.get("action") or "").strip()
    node_name = (step.get("node") or "").strip()
    params = step.get("params") or {}

    if not action_name:
        raise FailoverRunnerError(f"Plan '{plan.name}' step #{step_index} has no action")

    if dry_run:
        return {
            "step_index": step_index,
            "node": node_name,
            "action": action_name,
            "params": params,
            "dry_run": True,
            "status": "success",
            "message": "Dry run step validated",
        }



    if not node_name:
        raise FailoverRunnerError(f"Plan '{plan.name}' step #{step_index} has no node")

    node = Node.objects.filter(name=node_name).first()
    if not node:
        raise FailoverRunnerError(
            f"Plan '{plan.name}' step #{step_index}: node '{node_name}' not found"
        )

    if action_name.startswith("mariadb."):
        config = _get_mariadb_config_for_node(node)

        if action_name == "mariadb.status":
            mariadb_status = get_replication_status(config)
            return {
                "step_index": step_index,
                "node": node.name,
                "action": action_name,
                "params": params,
                "status": "success",
                "mariadb_status": mariadb_status,
            }

        if action_name == "mariadb.stop_replica":
            stop_replica(config)
            return {
                "step_index": step_index,
                "node": node.name,
                "action": action_name,
                "params": params,
                "status": "success",
                "message": f"MariaDB replica stopped for node '{node.name}'",
            }

        if action_name == "mariadb.start_replica":
            start_replica(config)
            return {
                "step_index": step_index,
                "node": node.name,
                "action": action_name,
                "params": params,
                "status": "success",
                "message": f"MariaDB replica started for node '{node.name}'",
            }

        if action_name == "mariadb.promote":
            promote(config)
            return {
                "step_index": step_index,
                "node": node.name,
                "action": action_name,
                "params": params,
                "status": "success",
                "message": f"MariaDB node promoted: '{node.name}'",
            }

        if action_name == "mariadb.set_read_only":
            value = params.get("value", True)
            set_read_only(config, value)
            return {
                "step_index": step_index,
                "node": node.name,
                "action": action_name,
                "params": params,
                "status": "success",
                "message": f"MariaDB read_only updated for node '{node.name}'",
                "value": bool(value),
            }

        raise FailoverRunnerError(
            f"Plan '{plan.name}' step #{step_index}: unsupported MariaDB action '{action_name}'"
        )

    remote_params = dict(params)
    remote_params["request_id"] = f"fo-{plan.id}-{step_index}-{uuid.uuid4().hex[:8]}"

    remote_payload = run_remote_action(node, action_name, params=remote_params)

    return {
        "step_index": step_index,
        "node": node.name,
        "action": action_name,
        "params": remote_params,
        "status": "success",
        "remote_payload": remote_payload,
    }