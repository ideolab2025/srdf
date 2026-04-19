#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.db.models import Max
from django.utils import timezone

from srdf.models import (
    ActionExecution,
    AuditEvent,
    ExecutionLock,
    FailoverPlan,
    Node,
    ReplicationService,
    ReplicationStatus,
)


def build_global_status():
    now = timezone.now()

    nodes_total = Node.objects.count()
    nodes_enabled = Node.objects.filter(is_enabled=True).count()
    nodes_online = Node.objects.filter(status="online").count()
    nodes_offline = Node.objects.filter(status="offline").count()
    nodes_degraded = Node.objects.filter(status="degraded").count()

    replication_total = ReplicationService.objects.count()
    replication_enabled = ReplicationService.objects.filter(is_enabled=True).count()

    latest_replication = (
        ReplicationStatus.objects.select_related("service")
        .order_by("-collected_at")[:20]
    )

    latest_actions = (
        ActionExecution.objects.select_related("node")
        .order_by("-created_at")[:20]
    )

    latest_audits = (
        AuditEvent.objects.select_related("node")
        .order_by("-created_at")[:20]
    )

    active_failovers = FailoverPlan.objects.filter(status__in=["ready", "running"]).count()
    locked_objects = ExecutionLock.objects.filter(is_locked=True).count()

    return {
        "ok": True,
        "timestamp": now.isoformat(),
        "summary": {
            "nodes_total": nodes_total,
            "nodes_enabled": nodes_enabled,
            "nodes_online": nodes_online,
            "nodes_offline": nodes_offline,
            "nodes_degraded": nodes_degraded,
            "replication_total": replication_total,
            "replication_enabled": replication_enabled,
            "active_failovers": active_failovers,
            "locked_objects": locked_objects,
        },
        "latest_replication_statuses": [
            {
                "service_id": row.service_id,
                "service_name": row.service.name,
                "status": row.status,
                "lag_seconds": row.lag_seconds,
                "collected_at": row.collected_at.isoformat() if row.collected_at else None,
            }
            for row in latest_replication
        ],
        "latest_actions": [
            {
                "id": row.id,
                "node_id": row.node_id,
                "node_name": row.node.name if row.node_id else "",
                "action_name": row.action_name,
                "status": row.status,
                "duration_ms": row.duration_ms,
                "created_at": row.created_at.isoformat() if row.created_at else None,
            }
            for row in latest_actions
        ],
        "latest_audits": [
            {
                "id": row.id,
                "event_type": row.event_type,
                "level": row.level,
                "node_name": row.node.name if row.node_id else "",
                "created_at": row.created_at.isoformat() if row.created_at else None,
                "message": row.message,
            }
            for row in latest_audits
        ],
    }


def build_dashboard_context():
    summary = build_global_status()["summary"]

    nodes = list(
        Node.objects.order_by("site", "name").values(
            "id",
            "name",
            "hostname",
            "site",
            "role",
            "status",
            "is_enabled",
            "last_seen_at",
        )
    )

    latest_status_rows = (
        ReplicationStatus.objects.values("service_id")
        .annotate(last_collected_at=Max("collected_at"))
        .order_by()
    )

    latest_status_lookup = {
        row["service_id"]: row["last_collected_at"]
        for row in latest_status_rows
    }

    replication_rows = []
    services = ReplicationService.objects.select_related("source_node", "target_node").order_by("name")

    for service in services:
        latest_collected_at = latest_status_lookup.get(service.id)
        latest_status = None

        if latest_collected_at:
            latest_status = (
                ReplicationStatus.objects.filter(
                    service=service,
                    collected_at=latest_collected_at,
                )
                .order_by("-id")
                .first()
            )

        replication_rows.append(
            {
                "id": service.id,
                "name": service.name,
                "service_type": service.service_type,
                "mode": service.mode,
                "source_node": service.source_node.name if service.source_node_id else "",
                "target_node": service.target_node.name if service.target_node_id else "",
                "is_enabled": service.is_enabled,
                "status": latest_status.status if latest_status else "unknown",
                "lag_seconds": latest_status.lag_seconds if latest_status else 0,
                "last_error": latest_status.last_error if latest_status else "",
                "collected_at": latest_status.collected_at if latest_status else None,
            }
        )

    latest_actions = (
        ActionExecution.objects.select_related("node")
        .order_by("-created_at")[:15]
    )

    latest_audits = (
        AuditEvent.objects.select_related("node")
        .order_by("-created_at")[:15]
    )

    failover_plans = FailoverPlan.objects.order_by("-updated_at", "-id")[:12]    
    
    locks = ExecutionLock.objects.order_by("name")

    recent_journal = build_recent_journal(latest_audits, latest_actions)

    return {
        "summary": summary,
        "nodes": nodes,
        "replication_rows": replication_rows,
        "latest_actions": latest_actions,
        "latest_audits": latest_audits,
        "failover_plans": failover_plans,
        "locks": locks,
        "recent_journal": recent_journal,
        "generated_at": timezone.now(),
    }


def build_recent_journal(latest_audits, latest_actions):
    journal = []

    for row in latest_audits[:10]:
        created = row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else "-"
        node_name = row.node.name if row.node_id else "-"
        journal.append({
            "kind": "audit",
            "created_at": row.created_at,
            "line": f"[{created}] AUDIT {row.level.upper()} | {row.event_type} | node={node_name} | {row.message}",
        })

    for row in latest_actions[:10]:
        created = row.created_at.strftime("%Y-%m-%d %H:%M:%S") if row.created_at else "-"
        node_name = row.node.name if row.node_id else "-"
        journal.append({
            "kind": "action",
            "created_at": row.created_at,
            "line": f"[{created}] ACTION {row.status.upper()} | {row.action_name} | node={node_name} | duration={row.duration_ms}ms",
        })

    journal.sort(key=lambda x: x["created_at"] or timezone.now(), reverse=True)
    return journal[:20]