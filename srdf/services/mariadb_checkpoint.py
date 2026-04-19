#! /usr/bin/python3.1
# -*- coding: utf-8 -*-


from django.utils import timezone
from srdf.models import AuditEvent


def save_checkpoint(service, payload):
    AuditEvent.objects.create(
        event_type="mariadb_checkpoint",
        level="info",
        message=f"Checkpoint saved for service {service.name}",
        payload={
            "service_id": service.id,
            "checkpoint": payload,
            "timestamp": timezone.now().isoformat(),
        },
    )