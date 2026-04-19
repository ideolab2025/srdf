#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json

from django.utils import timezone
from django.db.models import Q

from srdf.models import ReplicationService, SRDFSettingDefinition, SRDFSettingValue


class SRDFSettingsError(Exception):
    pass


def get_srdf_setting_value(
    key,
    replication_service=None,
    replication_service_id=None,
    engine="",
    company_name="",
    client_name="",
    node=None,
    node_id=None,
    fallback=None,
    ):
    """
    Resolution order:
    1) service
    2) node
    3) client
    4) company
    5) engine
    6) global
    7) definition default
    8) fallback
    """

    key = (key or "").strip()
    if not key:
        raise SRDFSettingsError("key is required")

    if replication_service is None and replication_service_id:
        replication_service = (
            ReplicationService.objects
            .select_related("source_node", "target_node")
            .filter(id=replication_service_id)
            .first()
        )

    if replication_service and not engine:
        engine = replication_service.service_type or ""

    definition = (
        SRDFSettingDefinition.objects
        .filter(
            key=key,
            is_enabled=True,
        )
        .order_by("id")
        .first()
    )

    if not definition:
        return fallback

    now = timezone.now()

    base_qs = (
        SRDFSettingValue.objects
        .select_related("setting_definition", "replication_service", "node")
        .filter(
            setting_definition=definition,
            is_enabled=True,
        )
        .order_by("priority", "id")
    )

    base_qs = _filter_by_validity(base_qs, now=now)
    base_qs = _filter_by_engine(base_qs, engine=engine)

    # 1) service
    if replication_service:
        hit = (
            base_qs
            .filter(
                scope_type="service",
                replication_service=replication_service,
            )
            .first()
        )
        if hit:
            return _decode_setting_value(definition, hit)

    # 2) node
    resolved_node = node
    if resolved_node is None and node_id:
        from srdf.models import Node
        resolved_node = Node.objects.filter(id=node_id).first()

    if resolved_node:
        hit = (
            base_qs
            .filter(
                scope_type="node",
                node=resolved_node,
            )
            .first()
        )
        if hit:
            return _decode_setting_value(definition, hit)

    # 3) client
    if client_name:
        hit = (
            base_qs
            .filter(
                scope_type="client",
                client_name=client_name,
            )
            .first()
        )
        if hit:
            return _decode_setting_value(definition, hit)

    # 4) company
    if company_name:
        hit = (
            base_qs
            .filter(
                scope_type="company",
                company_name=company_name,
            )
            .first()
        )
        if hit:
            return _decode_setting_value(definition, hit)

    # 5) engine
    if engine:
        hit = (
            base_qs
            .filter(
                scope_type="engine",
                engine=engine,
            )
            .first()
        )
        if hit:
            return _decode_setting_value(definition, hit)

    # 6) global
    hit = (
        base_qs
        .filter(scope_type="global")
        .first()
    )
    if hit:
        return _decode_setting_value(definition, hit)

    # 7) definition default
    default_value = _decode_definition_default(definition)
    if default_value is not None:
        return default_value

    # 8) fallback
    return fallback


def get_srdf_setting_int(
    key,
    replication_service=None,
    replication_service_id=None,
    engine="",
    company_name="",
    client_name="",
    node=None,
    node_id=None,
    fallback=0,
    minimum=None,
    maximum=None,
    ):
    value = get_srdf_setting_value(
        key=key,
        replication_service=replication_service,
        replication_service_id=replication_service_id,
        engine=engine,
        company_name=company_name,
        client_name=client_name,
        node=node,
        node_id=node_id,
        fallback=fallback,
    )

    try:
        value = int(value)
    except (TypeError, ValueError):
        value = int(fallback or 0)

    if minimum is not None:
        value = max(value, int(minimum))
    if maximum is not None:
        value = min(value, int(maximum))

    return value


def get_srdf_setting_float(
    key,
    replication_service=None,
    replication_service_id=None,
    engine="",
    company_name="",
    client_name="",
    node=None,
    node_id=None,
    fallback=0.0,
    minimum=None,
    maximum=None,
    ):
    value = get_srdf_setting_value(
        key=key,
        replication_service=replication_service,
        replication_service_id=replication_service_id,
        engine=engine,
        company_name=company_name,
        client_name=client_name,
        node=node,
        node_id=node_id,
        fallback=fallback,
    )

    try:
        value = float(value)
    except (TypeError, ValueError):
        value = float(fallback or 0.0)

    if minimum is not None:
        value = max(value, float(minimum))
    if maximum is not None:
        value = min(value, float(maximum))

    return value


def get_srdf_setting_bool(
    key,
    replication_service=None,
    replication_service_id=None,
    engine="",
    company_name="",
    client_name="",
    node=None,
    node_id=None,
    fallback=False,
    ):
    value = get_srdf_setting_value(
        key=key,
        replication_service=replication_service,
        replication_service_id=replication_service_id,
        engine=engine,
        company_name=company_name,
        client_name=client_name,
        node=node,
        node_id=node_id,
        fallback=fallback,
    )

    return _to_bool(value, fallback=fallback)


def _filter_by_validity(qs, now):
    return qs.filter(
        Q(valid_from__isnull=True, valid_until__isnull=True)
        | Q(valid_from__lte=now, valid_until__isnull=True)
        | Q(valid_from__isnull=True, valid_until__gte=now)
        | Q(valid_from__lte=now, valid_until__gte=now)
    )

def _filter_by_engine(qs, engine=""):
    engine = (engine or "").strip()
    if not engine:
        return qs.filter(engine="all")

    return qs.filter(engine__in=["all", engine])


def _decode_setting_value(definition, setting_value):
    value_type = (definition.value_type or "string").strip()

    if value_type == "json":
        return setting_value.value_json or {}

    raw = setting_value.value_text

    if value_type == "integer":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    if value_type == "float":
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    if value_type == "boolean":
        return _to_bool(raw, fallback=False)

    return raw or ""


def _decode_definition_default(definition):
    value_type = (definition.value_type or "string").strip()

    if value_type == "json":
        return definition.default_value_json or {}

    raw = definition.default_value

    if raw in (None, ""):
        return None

    if value_type == "integer":
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    if value_type == "float":
        try:
            return float(raw)
        except (TypeError, ValueError):
            return 0.0

    if value_type == "boolean":
        return _to_bool(raw, fallback=False)

    return raw


def _to_bool(value, fallback=False):
    if isinstance(value, bool):
        return value

    if value is None:
        return bool(fallback)

    if isinstance(value, (int, float)):
        return bool(value)

    text = str(value).strip().lower()

    if text in ("1", "true", "yes", "y", "on"):
        return True

    if text in ("0", "false", "no", "n", "off", ""):
        return False

    return bool(fallback)