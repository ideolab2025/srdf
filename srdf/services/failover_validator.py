#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from srdf.models import FailoverPlan, Node


def validate_failover_plan(plan):
    errors = []
    warnings = []

    if not isinstance(plan, FailoverPlan):
        return {
            "ok": False,
            "errors": ["Invalid plan object"],
            "warnings": [],
            "steps_count": 0,
        }

    plan_data = plan.plan_data or {}
    steps = plan_data.get("steps") or []

    if not isinstance(steps, list) or not steps:
        errors.append("plan_data.steps is missing or empty")
        return {
            "ok": False,
            "errors": errors,
            "warnings": warnings,
            "steps_count": 0,
        }

    for idx, step in enumerate(steps, start=1):
        if not isinstance(step, dict):
            errors.append(f"Step #{idx}: invalid step format")
            continue

        action_name = (step.get("action") or "").strip()
        node_name = (step.get("node") or "").strip()
        params = step.get("params") or {}

        if not action_name:
            errors.append(f"Step #{idx}: missing action")

        if not node_name:
            errors.append(f"Step #{idx}: missing node")
        else:
            node = Node.objects.filter(name=node_name).first()
            if not node:
                errors.append(f"Step #{idx}: node '{node_name}' not found")
            elif not node.is_enabled:
                warnings.append(f"Step #{idx}: node '{node_name}' is disabled")

        if not isinstance(params, dict):
            errors.append(f"Step #{idx}: params must be a dict")
            
            
        if action_name.startswith("mariadb."):
            if action_name not in [
                "mariadb.stop_replica",
                "mariadb.start_replica",
                "mariadb.promote",
                "mariadb.set_read_only",
            ]:
                errors.append(f"Step #{idx}: unknown mariadb action {action_name}")        
    

    return {
        "ok": len(errors) == 0,
        "errors": errors,
        "warnings": warnings,
        "steps_count": len(steps),
    }