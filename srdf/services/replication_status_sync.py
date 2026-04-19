#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.db.models import Q
from django.utils import timezone

from srdf.models import ReplicationService, ReplicationStatus


SERVICE_KEY_MAP = {
    "postgresql": "postgresql",
    "mariadb": "mariadb",
    "filesync": "filesync",
    "nginx": "nginx",
}


def sync_replication_statuses_for_node(node, status_payload):
    if not isinstance(status_payload, dict):
        return 0

    services_payload = status_payload.get("services") or {}
    if not isinstance(services_payload, dict):
        return 0

    updated_count = 0

    replication_services = (
        ReplicationService.objects.filter(is_enabled=True)
        .filter(Q(source_node=node) | Q(target_node=node))
        .select_related("source_node", "target_node")
    )

    now = timezone.now()

    for replication_service in replication_services:
        payload_key = SERVICE_KEY_MAP.get(replication_service.service_type)
        if not payload_key:
            continue

        service_data = services_payload.get(payload_key)
        if not isinstance(service_data, dict):
            continue

        status_value = compute_replication_status(replication_service.service_type, service_data)
        lag_seconds = extract_lag_seconds(service_data)
        last_error = extract_last_error(service_data)

        ReplicationStatus.objects.create(
            service=replication_service,
            status=status_value,
            lag_seconds=lag_seconds,
            last_error=last_error,
            payload=service_data,
            collected_at=now,
        )
        updated_count += 1

    return updated_count


def compute_replication_status(service_type, service_data):
    installed = service_data.get("installed", True)
    running = service_data.get("running", True)
    error = (service_data.get("error") or "").strip()

    if not installed:
        return "unknown"

    if error:
        return "error"

    if running is False:
        return "error"

    lag_seconds = extract_lag_seconds(service_data)

    if service_type in ("postgresql", "mariadb", "filesync"):
        if lag_seconds > 30:
            return "lagging"

    return "ok"


def extract_lag_seconds(service_data):
    for key in (
        "replication_lag_seconds",
        "lag_seconds",
        "delay_seconds",
    ):
        value = service_data.get(key)
        if value is None:
            continue
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    return 0


def extract_last_error(service_data):
    for key in ("error", "last_error", "message"):
        value = service_data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""