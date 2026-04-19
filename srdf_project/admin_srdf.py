#!/usr/bin/env python3
# -*- coding: utf-8 -*-


from django.contrib import admin
import json
from django.utils.html import format_html
from django.utils import timezone

from srdf.models import *



from django import forms


from django.http import JsonResponse, HttpResponseRedirect
from django.template.response import TemplateResponse


from django.urls import path, reverse
from django.utils.html import format_html
from django.contrib import messages


from srdf.services.admin_db_catalog import (
    AdminDBCatalogError,
    list_source_databases,
    list_source_tables,
    resolve_replication_service_from_plan_id,
    search_values,
)

from srdf.services.archive_engine import run_archive_once, _ensure_archive_tables_ready
from srdf.services.bootstrap_plan_runner import run_bootstrap_plan_once
from srdf.services.transport_batch_builder import build_transport_batch_once
from srdf.services.transport_sender import resume_failed_batch_once, send_transport_batch_once

# =========================
# UTILS
# =========================

def pretty_json(value):
    try:
        return format_html(
            "<pre style='white-space:pre-wrap;max-width:1200px;'>{}</pre>",
            json.dumps(value or {}, indent=2, ensure_ascii=False, default=str),
        )
    except Exception:
        return format_html("<pre>{}</pre>", str(value))
    
    
def _config_bool(config, key, default_value=False):
    raw_value = (config or {}).get(key, default_value)

    if isinstance(raw_value, bool):
        return raw_value

    if isinstance(raw_value, (int, float)):
        return bool(raw_value)

    text = str(raw_value or "").strip().lower()
    if text in ("1", "true", "yes", "y", "on"):
        return True
    if text in ("0", "false", "no", "n", "off", ""):
        return False

    return bool(default_value)


def _config_int(config, key, default_value):
    raw_value = (config or {}).get(key, default_value)
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return int(default_value)


def _compute_failed_batch_recovery_guard(batch):
    service = getattr(batch, "replication_service", None)
    config = (service.config or {}) if service else {}

    auto_recovery_enabled = _config_bool(
        config,
        "source_auto_resume_failed_batch",
        True,
    )
    max_auto_resumes = _config_int(
        config,
        "source_max_auto_resume_failed_batch",
        3,
    )
    cooldown_seconds = _config_int(
        config,
        "source_failed_batch_resume_cooldown_seconds",
        30,
    )

    retry_count = int(batch.retry_count or 0)
    reference_dt = batch.sent_at or batch.acked_at or batch.built_at or batch.created_at

    if batch.status != "failed":
        return {
            "reason_code": "BATCH_NOT_FAILED",
            "allowed": False,
            "message": "Batch is not in failed state",
            "remaining_seconds": 0,
            "retry_count": retry_count,
            "max_auto_resumes": max_auto_resumes,
            "cooldown_seconds": cooldown_seconds,
        }

    if not auto_recovery_enabled:
        return {
            "reason_code": "SOURCE_RETRY_DISABLED",
            "allowed": False,
            "message": "Automatic failed batch recovery is disabled",
            "remaining_seconds": 0,
            "retry_count": retry_count,
            "max_auto_resumes": max_auto_resumes,
            "cooldown_seconds": cooldown_seconds,
        }

    if retry_count >= max_auto_resumes:
        return {
            "reason_code": "SOURCE_RETRY_MAX_ATTEMPTS_REACHED",
            "allowed": False,
            "message": f"Automatic failed batch recovery limit reached ({retry_count}/{max_auto_resumes})",
            "remaining_seconds": 0,
            "retry_count": retry_count,
            "max_auto_resumes": max_auto_resumes,
            "cooldown_seconds": cooldown_seconds,
        }

    if reference_dt:
        elapsed = int((timezone.now() - reference_dt).total_seconds())
        if elapsed < cooldown_seconds:
            remaining = max(int(cooldown_seconds - elapsed), 0)
            return {
                "reason_code": "SOURCE_RETRY_COOLDOWN_ACTIVE",
                "allowed": False,
                "message": (
                    f"Cooldown active ({elapsed}s elapsed / {cooldown_seconds}s required)"
                    ),
                "remaining_seconds": remaining,
                "elapsed_seconds": elapsed,
                "retry_count": retry_count,
                "max_auto_resumes": max_auto_resumes,
                "cooldown_seconds": cooldown_seconds,
            }

    return {
        "reason_code": "SOURCE_RETRY_ALLOWED",
        "allowed": True,
        "message": "Failed batch can be resumed automatically",
        "remaining_seconds": 0,
        "retry_count": retry_count,
        "max_auto_resumes": max_auto_resumes,
        "cooldown_seconds": cooldown_seconds,
    }


def _extract_retry_guard_from_phase_execution(obj):
    result_payload = obj.result_payload or {}
    payload = result_payload.get("payload") or {}
    recovery_guard = payload.get("recovery_guard") or {}

    return {
        "reason_code": recovery_guard.get("reason_code") or "",
        "message": recovery_guard.get("message") or "",
        "remaining_seconds": int(recovery_guard.get("remaining_seconds") or 0),
    }


def _extract_retry_guard_from_runtime_state(obj):
    runtime_payload = obj.runtime_payload or {}
    payload = runtime_payload.get("payload") or {}
    recovery_guard = payload.get("recovery_guard") or {}

    return {
        "reason_code": recovery_guard.get("reason_code") or "",
        "message": recovery_guard.get("message") or "",
        "remaining_seconds": int(recovery_guard.get("remaining_seconds") or 0),
    }
    
    
class BootstrapPlanAdminForm(forms.ModelForm):
    class Meta:
        model = BootstrapPlan
        fields = "__all__"
        widgets = {
            "source_database_name": forms.TextInput(
                attrs={
                    "autocomplete": "off",
                    "placeholder": "Start typing a database name...",
                }
            ),
            "include_tables_csv": forms.Textarea(
                attrs={
                    "rows": 3,
                    "placeholder": "Optional manual CSV list",
                }
            ),
            "exclude_tables_csv": forms.Textarea(
                attrs={
                    "rows": 3,
                    "placeholder": "Optional exclusion CSV list",
                }
            ),
        }


# =========================
# NODE
# =========================

class BootstrapPlanTableSelectionAdminForm(forms.ModelForm):
    class Meta:
        model = BootstrapPlanTableSelection
        fields = "__all__"


class SourceDatabaseCatalogAdminForm(forms.ModelForm):
    class Meta:
        model = SourceDatabaseCatalog
        fields = "__all__"


class SourceTableCatalogAdminForm(forms.ModelForm):
    class Meta:
        model = SourceTableCatalog
        fields = "__all__"

class NodeAdmin(admin.ModelAdmin):
    list_display = ("name", "hostname", "db_port", "site", "role", "status", "is_enabled", "last_seen_at")

    list_filter = ("role", "status", "site", "is_enabled")
    search_fields = ("name", "hostname", "site")

    readonly_fields = ("last_seen_at", "created_at", "updated_at", "pretty_payload")

    def pretty_payload(self, obj):
        return pretty_json(obj.last_status_payload)

    pretty_payload.short_description = "Last Payload"


# =========================
# REPLICATION SERVICE
# =========================


class ReplicationServiceAdmin(admin.ModelAdmin):
    list_display = ("name", "service_type", "mode", "source_node", "target_node", "is_enabled")
    list_filter = ("service_type", "mode", "is_enabled")
    search_fields = ("name",)

    readonly_fields = ("created_at", "updated_at", "pretty_config")
    

    def pretty_config(self, obj):
        return pretty_json(obj.config)

    pretty_config.short_description = "Config"


# =========================
# REPLICATION STATUS
# =========================


class ReplicationStatusAdmin(admin.ModelAdmin):
    list_display = ("service", "status", "lag_seconds", "collected_at")
    list_filter = ("status",)
    search_fields = ("service__name",)

    readonly_fields = ("pretty_payload",)

    def pretty_payload(self, obj):
        return pretty_json(obj.payload)

    pretty_payload.short_description = "Payload"


# =========================
# ACTION EXECUTION
# =========================


class ActionExecutionAdmin(admin.ModelAdmin):
    list_display = ("id", "node", "action_name", "status", "duration_ms", "started_at", "finished_at")
    list_filter = ("status", "action_name")
    search_fields = ("action_name", "node__name")

    readonly_fields = ("pretty_params", "pretty_result", "stdout", "stderr")

    def pretty_params(self, obj):
        return pretty_json(obj.params)

    def pretty_result(self, obj):
        return pretty_json(obj.result_payload)

    pretty_params.short_description = "Params"
    pretty_result.short_description = "Result"


# =========================
# FAILOVER PLAN
# =========================


class FailoverPlanAdmin(admin.ModelAdmin):
    list_display = ("name", "source_site", "target_site", "status", "started_at", "finished_at")
    list_filter = ("status",)
    search_fields = ("name",)

    readonly_fields = ("pretty_plan",)

    def pretty_plan(self, obj):
        return pretty_json(obj.plan_data)

    pretty_plan.short_description = "Plan Data"


# =========================
# AUDIT
# =========================

class AuditEventAdmin(admin.ModelAdmin):
    list_display = ("event_type", "level", "node", "created_at")
    list_filter = ("level", "event_type")
    search_fields = ("message", "node__name")

    readonly_fields = ("pretty_payload",)

    def pretty_payload(self, obj):
        return pretty_json(obj.payload)

    pretty_payload.short_description = "Payload"


# =========================
# LOCK
# =========================


class ExecutionLockAdmin(admin.ModelAdmin):
    """
    Administration améliorée pour les verrous d'exécution (ExecutionLock)
    """
    list_display = (
        "name",
        "is_locked_badge",
        "locked_at",
        "locked_duration",
    )

    list_filter = ("is_locked",)
    search_fields = ("name",)

    readonly_fields = (
        "name",
        "is_locked",
        "locked_at",
        "pretty_locked_duration",
    )

    ordering = ("name",)

    # Actions personnalisées
    actions = ["action_release_locks"]

    def is_locked_badge(self, obj):
        """Affiche un badge coloré selon l'état du verrou"""
        if obj.is_locked:
            return format_html(
                '<span style="background-color:#d32f2f;color:white;padding:4px 8px;border-radius:4px;font-weight:bold;">'
                'LOCKED'
                '</span>'
            )
        else:
            return format_html(
                '<span style="background-color:#388e3c;color:white;padding:4px 8px;border-radius:4px;font-weight:bold;">'
                'FREE'
                '</span>'
            )

    is_locked_badge.short_description = "Status"
    is_locked_badge.admin_order_field = "is_locked"

    def locked_duration(self, obj):
        """Affiche depuis combien de temps le verrou est actif"""
        if not obj.is_locked or not obj.locked_at:
            return "-"
        
        duration = timezone.now() - obj.locked_at
        minutes = int(duration.total_seconds() // 60)
        
        if minutes < 60:
            return f"{minutes} min"
        elif minutes < 1440:  # moins d'un jour
            hours = minutes // 60
            return f"{hours}h {minutes % 60}min"
        else:
            days = minutes // 1440
            return f"{days}j"

    locked_duration.short_description = "Durée du verrou"

    def pretty_locked_duration(self, obj):
        """Version détaillée pour l'écran de détail"""
        if not obj.is_locked or not obj.locked_at:
            return "Le verrou n'est pas actif."
        
        duration = timezone.now() - obj.locked_at
        return f"Verrou actif depuis {duration} (depuis {obj.locked_at})"

    pretty_locked_duration.short_description = "Durée détaillée du verrou"

    # ======================
    # Actions personnalisées
    # ======================

    def action_release_locks(self, request, queryset):
        """Action permettant de libérer plusieurs verrous en une fois"""
        updated = 0
        for lock in queryset.filter(is_locked=True):
            lock.is_locked = False
            lock.locked_at = None
            lock.save()
            updated += 1

        if updated == 1:
            self.message_user(request, "1 verrou a été libéré avec succès.")
        else:
            self.message_user(request, f"{updated} verrous ont été libérés avec succès.")

    action_release_locks.short_description = "🔓 Libérer les verrous sélectionnés"
    action_release_locks.allowed_permissions = ['change']

    # ======================
    # Personnalisation de l'affichage détail
    # ======================

    fieldsets = (
        ("Verrou", {
            "fields": ("name", "is_locked", "locked_at"),
        }),
        ("Informations système", {
            "fields": ("created_at", "pretty_locked_duration"),
            "classes": ("collapse",),
        }),
    )

    def has_add_permission(self, request):
        """On ne permet pas de créer manuellement des verrous via l'admin"""
        return False

    def has_delete_permission(self, request, obj=None):
        """On autorise la suppression uniquement si le verrou n'est pas actif"""
        if obj and obj.is_locked:
            return False
        return True    
    
    
class OutboundChangeEventAdmin(admin.ModelAdmin):
    
    list_display = (
        "id",
        "event_family",
        "object_type",
        "ddl_action",
        "database_name",
        "table_name",
        "object_name",
        "operation",
        "status",
        "short_pk",
        "log_file",
        "log_pos",
        "created_at",
    )
    
    list_filter = (
        "engine",
        "event_family",
        "object_type",
        "ddl_action",
        "operation",
        "status",
        "database_name",
        "table_name",
    )
    
    
    search_fields = (
        "event_uid",
        "transaction_id",
        "database_name",
        "table_name",
        "object_name",
        "parent_object_name",
        "raw_sql",
        "log_file",
    )

    readonly_fields = (
        "replication_service",
        "source_node",
        "engine",
        "event_family",
        "object_type",
        "ddl_action",
        "database_name",
        "table_name",
        "object_name",
        "parent_object_name",
        "operation",
        "raw_sql",
        "event_uid",
        "transaction_id",
        "log_file",
        "log_pos",
        "status",
        "retry_count",
        "last_error",
        "batched_at",
        "sent_at",
        "acked_at",
        "applied_at",
        "created_at",
        "pretty_primary_key_data",
        "pretty_before_data",
        "pretty_after_data",
        "pretty_event_payload",
    )

    def short_pk(self, obj):
        try:
            if obj.primary_key_data:
                return json.dumps(obj.primary_key_data, ensure_ascii=False)[:80]
        except Exception:
            pass
        return "-"
    
    def short_raw_sql(self, obj):
        value = (obj.raw_sql or "").strip()
        if not value:
            return "-"
        if len(value) > 120:
            return value[:120] + "..."
        return value

    short_raw_sql.short_description = "Raw SQL"    

    short_pk.short_description = "Primary key"

    def pretty_primary_key_data(self, obj):
        return pretty_json(obj.primary_key_data)

    def pretty_before_data(self, obj):
        return pretty_json(obj.before_data)

    def pretty_after_data(self, obj):
        return pretty_json(obj.after_data)

    def pretty_event_payload(self, obj):
        return pretty_json(obj.event_payload)
    
    list_editable = ['status', ]
    
    fieldsets = (
        (
            "Main",
            {
                "fields": (
                    "replication_service",
                    "source_node",
                    "engine",
                    "event_family",
                    "status",
                )
            },
        ),
        (
            "DML / DDL identity",
            {
                "fields": (
                    "operation",
                    "object_type",
                    "ddl_action",
                    "database_name",
                    "table_name",
                    "object_name",
                    "parent_object_name",
                )
            },
        ),
        (
            "Binlog",
            {
                "fields": (
                    "event_uid",
                    "transaction_id",
                    "log_file",
                    "log_pos",
                )
            },
        ),
        (
            "Payload",
            {
                "fields": (
                    "pretty_primary_key_data",
                    "pretty_before_data",
                    "pretty_after_data",
                    "raw_sql",
                    "pretty_event_payload",
                )
            },
        ),
        (
            "Lifecycle",
            {
                "fields": (
                    "retry_count",
                    "last_error",
                    "batched_at",
                    "sent_at",
                    "acked_at",
                    "applied_at",
                    "created_at",
                )
            },
        ),
    )    

    pretty_primary_key_data.short_description = "Primary key data"
    pretty_before_data.short_description = "Before data"
    pretty_after_data.short_description = "After data"
    pretty_event_payload.short_description = "Event payload"

class TransportBatchAdmin(admin.ModelAdmin):
    actions = [
        "action_resume_failed_batch_now",
        "action_rebuild_and_resend_now",
        "action_resume_rebuild_resend_dry_run",
    ]

    list_display = (
        "id",
        "replication_service",
        "batch_uid",
        "event_count",
        "real_outbound_count",
        "status_badge",
        "retry_count",
        "auto_resume_badge",
        "auto_resume_reason",
        "admin_open_outbound_events_action",
        "admin_resume_action",
        "admin_rebuild_resend_action",
        "admin_dry_run_action",
        "admin_open_console_action",
        "short_last_error",
        "built_at",
        "sent_at",
        "acked_at",
    )

    list_filter = (
        "status",
        "replication_service",
    )

    search_fields = (
        "batch_uid",
        "last_error",
        "replication_service__name",
    )

    readonly_fields = (
        "replication_service",
        "batch_uid",
        "event_count",
        "first_event_id",
        "last_event_id",
        "compression",
        "checksum",
        "status",
        "retry_count",
        "last_error",
        "built_at",
        "sent_at",
        "acked_at",
        "pretty_payload",
        "pretty_recovery_monitor",
        "admin_resume_action_detail",
        "admin_rebuild_resend_action_detail",
        "admin_dry_run_action_detail",
    )

    fieldsets = (
        (
            "Main",
            {
                "fields": (
                    "replication_service",
                    "batch_uid",
                    "event_count",
                    "first_event_id",
                    "last_event_id",
                    "status",
                    "retry_count",
                    "last_error",
                )
            },
        ),
        (
            "Transport lifecycle",
            {
                "fields": (
                    "built_at",
                    "sent_at",
                    "acked_at",
                )
            },
        ),
        
        (
            "Recovery monitoring",
            {
                "fields": (
                    "pretty_recovery_monitor",
                    "admin_resume_action_detail",
                    "admin_rebuild_resend_action_detail",
                    "admin_dry_run_action_detail",
                )
            },
        ),
        
        (
            "Payload",
            {
                "fields": (
                    "compression",
                    "checksum",
                    "pretty_payload",
                )
            },
        ),
    )
    
    
    def changelist_view(self, request, extra_context=None):
        extra_context = extra_context or {}
        extra_context["transport_recovery_console_url"] = reverse(
            f"{self.admin_site.name}:srdf_transport_recovery_console"
        )
        return super().changelist_view(request, extra_context=extra_context)    
    
    
    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "transport-recovery-console/",
                self.admin_site.admin_view(self.transport_recovery_console_view),
                name="srdf_transport_recovery_console",
            ),
            path(
                "<int:batch_id>/resume-now/",
                self.admin_site.admin_view(self.resume_now_view),
                name="srdf_transportbatch_resume_now",
            ),
            path(
                "<int:batch_id>/rebuild-resend-now/",
                self.admin_site.admin_view(self.rebuild_resend_now_view),
                name="srdf_transportbatch_rebuild_resend_now",
            ),
            path(
                "<int:batch_id>/dry-run-resume-rebuild-resend/",
                self.admin_site.admin_view(self.dry_run_resume_rebuild_resend_view),
                name="srdf_transportbatch_dry_run_resume_rebuild_resend",
            ),
        ]
        return custom_urls + urls
    

    def status_badge(self, obj):
        value = (obj.status or "").strip().lower()
        color = "#546e7a"
        label = value.upper() or "-"

        if value == "ready":
            color = "#1976d2"
        elif value == "sent":
            color = "#6a1b9a"
        elif value == "acked":
            color = "#2e7d32"
        elif value == "failed":
            color = "#d32f2f"

        return format_html(
            '<span style="background-color:{};color:white;padding:4px 8px;border-radius:4px;font-weight:bold;">{}</span>',
            color,
            label,
        )
    
    
    def real_outbound_count(self, obj):
        if not obj.replication_service_id:
            return 0

        first_event_id = int(obj.first_event_id or 0)
        last_event_id = int(obj.last_event_id or 0)

        if first_event_id <= 0 or last_event_id <= 0:
            return 0

        return (
            OutboundChangeEvent.objects
            .filter(
                replication_service_id=obj.replication_service_id,
                id__gte=first_event_id,
                id__lte=last_event_id,
            )
            .count()
        )

    real_outbound_count.short_description = "Outbound events"


    def admin_open_outbound_events_action(self, obj):
        if not obj.replication_service_id:
            return "-"

        first_event_id = int(obj.first_event_id or 0)
        last_event_id = int(obj.last_event_id or 0)

        if first_event_id <= 0 or last_event_id <= 0:
            return "-"

        url = reverse(f"{self.admin_site.name}:srdf_outboundchangeevent_changelist")
        url = (
            f"{url}"
            f"?replication_service__id__exact={obj.replication_service_id}"
            f"&id__gte={first_event_id}"
            f"&id__lte={last_event_id}"
        )

        return format_html(
            '<a class="button" href="{}" style="background:#00897b;color:white;padding:4px 8px;border-radius:4px;">Open events</a>',
            url,
        )

    admin_open_outbound_events_action.short_description = "Outbound events"    
    
    def admin_resume_action(self, obj):
        if (obj.status or "").strip().lower() != "failed":
            return "-"

        url = reverse(f"{self.admin_site.name}:srdf_transportbatch_resume_now", args=[obj.id])
        return format_html(
            '<a class="button" href="{}" style="background:#d32f2f;color:white;padding:4px 8px;border-radius:4px;">Resume now</a>',
            url,
        )

    admin_resume_action.short_description = "Manual recovery"

    def admin_resume_action_detail(self, obj):
        return self.admin_resume_action(obj)

    admin_resume_action_detail.short_description = "Manual recovery"    

    status_badge.short_description = "Status"
    status_badge.admin_order_field = "status"
    
    
    def admin_rebuild_resend_action(self, obj):
        if (obj.status or "").strip().lower() != "failed":
            return "-"

        url = reverse(
            f"{self.admin_site.name}:srdf_transportbatch_rebuild_resend_now",
            args=[obj.id],
        )
        return format_html(
            '<a class="button" href="{}" style="background:#1565c0;color:white;padding:4px 8px;border-radius:4px;">Rebuild &amp; resend</a>',
            url,
        )

    admin_rebuild_resend_action.short_description = "Manual rebuild/send"

    def admin_rebuild_resend_action_detail(self, obj):
        return self.admin_rebuild_resend_action(obj)
    

    admin_rebuild_resend_action_detail.short_description = "Manual rebuild/send"    

    def auto_resume_badge(self, obj):
        guard = _compute_failed_batch_recovery_guard(obj)
        reason_code = guard.get("reason_code") or ""

        color = "#9e9e9e"
        label = "N/A"

        if reason_code == "SOURCE_RETRY_ALLOWED":
            color = "#2e7d32"
            label = "AUTO-RESUME OK"
        elif reason_code == "SOURCE_RETRY_DISABLED":
            color = "#616161"
            label = "DISABLED"
        elif reason_code == "SOURCE_RETRY_COOLDOWN_ACTIVE":
            color = "#ef6c00"
            label = "COOLDOWN"
        elif reason_code == "SOURCE_RETRY_MAX_ATTEMPTS_REACHED":
            color = "#d32f2f"
            label = "MAX ATTEMPTS"
        elif reason_code == "BATCH_NOT_FAILED":
            color = "#546e7a"
            label = "NOT FAILED"

        return format_html(
            '<span style="background-color:{};color:white;padding:4px 8px;border-radius:4px;font-weight:bold;">{}</span>',
            color,
            label,
        )

    auto_resume_badge.short_description = "Auto recovery"
    
    
    
    def admin_dry_run_action(self, obj):
        if (obj.status or "").strip().lower() != "failed":
            return "-"

        url = reverse(
            f"{self.admin_site.name}:srdf_transportbatch_dry_run_resume_rebuild_resend",
            args=[obj.id],
        )
        return format_html(
            '<a class="button" href="{}" style="background:#455a64;color:white;padding:4px 8px;border-radius:4px;">Dry-run diagnostic</a>',
            url,
        )

    admin_dry_run_action.short_description = "Dry-run"

    def admin_dry_run_action_detail(self, obj):
        return self.admin_dry_run_action(obj)

    admin_dry_run_action_detail.short_description = "Dry-run"   
    
    
    
    def admin_open_console_action(self, obj):
        url = reverse(f"{self.admin_site.name}:srdf_transport_recovery_console")
        if obj.replication_service_id:
            url = f"{url}?replication_service_id={obj.replication_service_id}"
        return format_html(
            '<a class="button" href="{}" style="background:#37474f;color:white;padding:4px 8px;border-radius:4px;">Open console</a>',
            url,
        )

    admin_open_console_action.short_description = "Console"    
    

    def auto_resume_reason(self, obj):
        guard = _compute_failed_batch_recovery_guard(obj)
        reason_code = guard.get("reason_code") or ""
        remaining = int(guard.get("remaining_seconds") or 0)

        if reason_code == "SOURCE_RETRY_COOLDOWN_ACTIVE":
            return f"{reason_code} ({remaining}s left)"
        return reason_code or "-"

    auto_resume_reason.short_description = "Recovery reason"

    def short_last_error(self, obj):
        value = (obj.last_error or "").strip()
        if not value:
            return "-"
        if len(value) > 100:
            return value[:100] + "..."
        return value

    short_last_error.short_description = "Last error"

    def pretty_payload(self, obj):
        return pretty_json(obj.payload)

    pretty_payload.short_description = "Payload"

    def pretty_recovery_monitor(self, obj):
        guard = _compute_failed_batch_recovery_guard(obj)
        return pretty_json(guard)

    pretty_recovery_monitor.short_description = "Recovery monitor"
    
    
    def _resume_failed_batch_admin(self, request, batch):
        if not batch:
            messages.error(request, "TransportBatch not found.")
            return False

        if (batch.status or "").strip().lower() != "failed":
            messages.warning(
                request,
                f"Batch '{batch.batch_uid or batch.id}' is not in failed state.",
            )
            return False

        try:
            result = resume_failed_batch_once(
                batch_id=batch.id,
                initiated_by=(
                    f"admin:{request.user.username}"
                    if request.user and request.user.is_authenticated
                    else "admin:anonymous"
                ),
            )

            messages.success(
                request,
                (
                    f"Manual resume OK for batch '{batch.batch_uid or batch.id}' - "
                    f"requeued_events={int(result.get('requeued_events') or 0)}"
                ),
            )
            return True

        except Exception as exc:
            messages.error(
                request,
                f"Manual resume failed for batch '{batch.batch_uid or batch.id}': {exc}",
            )
            return False 
        
        
    def _rebuild_and_resend_batch_admin(self, request, batch):
        if not batch:
            messages.error(request, "TransportBatch not found.")
            return False

        if (batch.status or "").strip().lower() != "failed":
            messages.warning(
                request,
                f"Batch '{batch.batch_uid or batch.id}' is not in failed state.",
            )
            return False

        service = batch.replication_service
        if not service:
            messages.error(
                request,
                f"Batch '{batch.batch_uid or batch.id}' has no replication service.",
            )
            return False

        initiated_by = (
            f"admin:{request.user.username}"
            if request.user and request.user.is_authenticated
            else "admin:anonymous"
        )

        try:
            resume_result = resume_failed_batch_once(
                batch_id=batch.id,
                initiated_by=initiated_by,
            )

            batch_limit = int(service.config.get("transport_batch_limit", 100) or 100)

            build_result = build_transport_batch_once(
                replication_service_id=service.id,
                limit=batch_limit,
                initiated_by=initiated_by,
            )

            batch_created = bool(build_result.get("batch_created"))
            
            new_batch_id = (
                int(build_result.get("transport_batch_id") or 0)
                if build_result.get("transport_batch_id")
                else 0
            )            
            new_batch_uid = str(build_result.get("batch_uid") or "")

            if not batch_created or new_batch_id <= 0:
                messages.warning(
                    request,
                    (
                        f"Manual resume OK for batch '{batch.batch_uid or batch.id}', "
                        f"but no new batch was built."
                    ),
                )
                return True

            send_result = send_transport_batch_once(
                batch_id=new_batch_id,
                initiated_by=initiated_by,
            )

            messages.success(
                request,
                (
                    f"Manual rebuild/resend OK - "
                    f"old_batch='{batch.batch_uid or batch.id}' "
                    f"requeued_events={int(resume_result.get('requeued_events') or 0)} "
                    f"new_batch_id={new_batch_id} "
                    f"new_batch_uid='{new_batch_uid}'"
                ),
            )

            if send_result.get("status"):
                messages.info(
                    request,
                    f"Send result status={send_result.get('status')}"
                )

            return True

        except Exception as exc:
            messages.error(
                request,
                f"Manual rebuild/resend failed for batch '{batch.batch_uid or batch.id}': {exc}",
            )
            return False    
        
        
    def _dry_run_resume_rebuild_resend_batch_admin(self, request, batch):
        if not batch:
            messages.error(request, "TransportBatch not found.")
            return False

        service = batch.replication_service
        if not service:
            messages.error(
                request,
                f"Batch '{batch.batch_uid or batch.id}' has no replication service.",
            )
            return False

        if (batch.status or "").strip().lower() != "failed":
            messages.warning(
                request,
                f"Dry-run skipped: batch '{batch.batch_uid or batch.id}' is not in failed state.",
            )
            return False

        recovery_guard = _compute_failed_batch_recovery_guard(batch)
        batch_limit = _config_int(service.config or {}, "transport_batch_limit", 100)

        outbound_qs = OutboundChangeEvent.objects.filter(
            replication_service=service,
            id__gte=int(batch.first_event_id or 0),
            id__lte=int(batch.last_event_id or 0),
        )

        total_events = int(outbound_qs.count())
        pending_events = int(outbound_qs.filter(status="pending").count())
        batched_events = int(outbound_qs.filter(status="batched").count())
        sent_events = int(outbound_qs.filter(status="sent").count())
        acked_events = int(outbound_qs.filter(status="acked").count())
        applied_events = int(outbound_qs.filter(status="applied").count())
        failed_events = int(outbound_qs.filter(status="failed").count())
        dead_events = int(outbound_qs.filter(status="dead").count())

        open_batches_qs = TransportBatch.objects.filter(
            replication_service=service,
            status__in=["ready", "sent", "acked", "failed"],
        ).exclude(id=batch.id)

        open_batches = list(
            open_batches_qs.values("id", "batch_uid", "status", "event_count")[:10]
        )

        would_resume = True
        would_build = len(open_batches) == 0
        would_send = False

        if would_build:
            projected_requeue = failed_events if failed_events > 0 else total_events
            would_send = projected_requeue > 0

        diagnostic = {
            "mode": "dry_run_resume_rebuild_resend",
            "batch_id": batch.id,
            "batch_uid": batch.batch_uid,
            "batch_status": batch.status,
            "replication_service_id": service.id,
            "replication_service_name": service.name,
            "transport_batch_limit": batch_limit,
            "recovery_guard": recovery_guard,
            "event_window": {
                "first_event_id": int(batch.first_event_id or 0),
                "last_event_id": int(batch.last_event_id or 0),
                "event_count_on_batch": int(batch.event_count or 0),
            },
            "outbound_status_counts": {
                "total": total_events,
                "pending": pending_events,
                "batched": batched_events,
                "sent": sent_events,
                "acked": acked_events,
                "applied": applied_events,
                "failed": failed_events,
                "dead": dead_events,
            },
            "open_batches_on_service": open_batches,
            "would_resume": would_resume,
            "would_build": would_build,
            "would_send": would_send,
            "notes": [
                "No database mutation was performed.",
                "No batch was rebuilt.",
                "No send was executed.",
                "This is an admin dry-run diagnostic only.",
            ],
        }

        if open_batches:
            messages.warning(
                request,
                (
                    f"Dry-run diagnostic for batch '{batch.batch_uid or batch.id}': "
                    f"resume would be possible, but rebuild would likely be blocked by "
                    f"{len(open_batches)} open batch(es) on service '{service.name}'."
                ),
            )
        else:
            messages.info(
                request,
                (
                    f"Dry-run diagnostic for batch '{batch.batch_uid or batch.id}': "
                    f"resume/build/send path looks structurally possible on service '{service.name}'."
                ),
            )

        messages.info(
            request,
            json.dumps(diagnostic, indent=2, default=str),
        )        
        
        return True  
    
    
    
    def _build_console_row_data(self, batch):
        guard = _compute_failed_batch_recovery_guard(batch)
        service = batch.replication_service

        outbound_qs = OutboundChangeEvent.objects.filter(
            replication_service=service,
            id__gte=int(batch.first_event_id or 0),
            id__lte=int(batch.last_event_id or 0),
        )

        total_events = int(outbound_qs.count())
        failed_events = int(outbound_qs.filter(status="failed").count())
        applied_events = int(outbound_qs.filter(status="applied").count())
        sent_events = int(outbound_qs.filter(status="sent").count())
        acked_events = int(outbound_qs.filter(status="acked").count())
        dead_events = int(outbound_qs.filter(status="dead").count())

        open_batches = list(
            TransportBatch.objects.filter(
                replication_service=service,
                status__in=["ready", "sent", "acked", "failed"],
            )
            .exclude(id=batch.id)
            .values("id", "batch_uid", "status", "event_count")[:5]
        )

        return {
            "batch": batch,
            "guard": guard,
            "counts": {
                "total": total_events,
                "failed": failed_events,
                "applied": applied_events,
                "sent": sent_events,
                "acked": acked_events,
                "dead": dead_events,
            },
            "open_batches": open_batches,
            "resume_url": reverse(
                f"{self.admin_site.name}:srdf_transportbatch_resume_now",
                args=[batch.id],
            ),
            "rebuild_resend_url": reverse(
                f"{self.admin_site.name}:srdf_transportbatch_rebuild_resend_now",
                args=[batch.id],
            ),
            "dry_run_url": reverse(
                f"{self.admin_site.name}:srdf_transportbatch_dry_run_resume_rebuild_resend",
                args=[batch.id],
            ),
            "change_url": reverse(
                f"{self.admin_site.name}:srdf_transportbatch_change",
                args=[batch.id],
            ),
        }  
    
    
    def _build_console_summary(self, replication_service_id="", status_filter="failed"):
        summary = {
            "total_batches": 0,
            "failed_batches": 0,
            "ready_batches": 0,
            "sent_batches": 0,
            "acked_batches": 0,
            "services": [],
        }

        base_qs = (
            TransportBatch.objects
            .select_related("replication_service")
            .all()
        )

        if replication_service_id:
            base_qs = base_qs.filter(replication_service_id=replication_service_id)

        service_ids = list(
            base_qs.values_list("replication_service_id", flat=True).distinct()
        )

        services = (
            ReplicationService.objects
            .filter(id__in=service_ids)
            .order_by("name")
        )

        for service in services:
            qs = base_qs.filter(replication_service=service)

            failed_count = int(qs.filter(status="failed").count())
            ready_count = int(qs.filter(status="ready").count())
            sent_count = int(qs.filter(status="sent").count())
            acked_count = int(qs.filter(status="acked").count())
            total_count = failed_count + ready_count + sent_count + acked_count

            summary["services"].append({
                "service_id": service.id,
                "service_name": service.name,
                "failed_batches": failed_count,
                "ready_batches": ready_count,
                "sent_batches": sent_count,
                "acked_batches": acked_count,
                "total_batches": total_count,
                "console_url": (
                    reverse(f"{self.admin_site.name}:srdf_transport_recovery_console")
                    + f"?replication_service_id={service.id}&status={status_filter or 'failed'}"
                ),
            })

            summary["failed_batches"] += failed_count
            summary["ready_batches"] += ready_count
            summary["sent_batches"] += sent_count
            summary["acked_batches"] += acked_count
            summary["total_batches"] += total_count

        return summary    
    
    
    
    def transport_recovery_console_view(self, request):
        replication_service_id = request.GET.get("replication_service_id", "").strip()
        status_filter = request.GET.get("status", "failed").strip()

        services_qs = ReplicationService.objects.filter(is_enabled=True).order_by("name")

        batches_qs = (
            TransportBatch.objects
            .select_related("replication_service")
            .all()
            .order_by("-id")
        )

        if replication_service_id:
            batches_qs = batches_qs.filter(replication_service_id=replication_service_id)

        if status_filter:
            batches_qs = batches_qs.filter(status=status_filter)

        batches_qs = batches_qs[:100]



        rows = [self._build_console_row_data(batch) for batch in batches_qs]
        summary = self._build_console_summary(
                replication_service_id=replication_service_id,
                status_filter=status_filter,
            )
    
        context = dict(
                self.admin_site.each_context(request),
                title="Transport Recovery Console",
                opts=self.model._meta,
                services=list(services_qs),
                selected_replication_service_id=replication_service_id,
                selected_status=status_filter,
                rows=rows,
                total_rows=len(rows),
                summary=summary,
                changelist_url=reverse(f"{self.admin_site.name}:srdf_transportbatch_changelist"),
            )
        
        

        return TemplateResponse(
            request,
            "admin/srdf/transport_recovery_console.html",
            context,
        )    
    
        
    def resume_now_view(self, request, batch_id):
        batch = TransportBatch.objects.filter(id=batch_id).select_related("replication_service").first()
        self._resume_failed_batch_admin(request, batch)

        changelist_url = reverse(f"{self.admin_site.name}:srdf_transportbatch_changelist")
        return HttpResponseRedirect(changelist_url)  
    
    
    def rebuild_resend_now_view(self, request, batch_id):
        batch = (
            TransportBatch.objects
            .filter(id=batch_id)
            .select_related("replication_service")
            .first()
        )
        self._rebuild_and_resend_batch_admin(request, batch)

        changelist_url = reverse(f"{self.admin_site.name}:srdf_transportbatch_changelist")
        return HttpResponseRedirect(changelist_url)    
    
    
    def dry_run_resume_rebuild_resend_view(self, request, batch_id):
        batch = (
            TransportBatch.objects
            .filter(id=batch_id)
            .select_related("replication_service")
            .first()
        )
        self._dry_run_resume_rebuild_resend_batch_admin(request, batch)

        changelist_url = reverse(f"{self.admin_site.name}:srdf_transportbatch_changelist")
        return HttpResponseRedirect(changelist_url)    
    
    
    def action_resume_failed_batch_now(self, request, queryset):
        resumed_count = 0
        skipped_count = 0

        for batch in queryset:
            if (batch.status or "").strip().lower() != "failed":
                skipped_count += 1
                continue

            ok = self._resume_failed_batch_admin(request, batch)
            if ok:
                resumed_count += 1
            else:
                skipped_count += 1

        if resumed_count > 0:
            self.message_user(
                request,
                f"{resumed_count} failed batch(es) resumed manually.",
            )

        if skipped_count > 0:
            self.message_user(
                request,
                f"{skipped_count} batch(es) skipped or failed during manual resume.",
                level=messages.WARNING,
            )

    action_resume_failed_batch_now.short_description = "Resume failed batch now (manual bypass)"    
    
    def action_rebuild_and_resend_now(self, request, queryset):
        success_count = 0
        skipped_count = 0

        for batch in queryset:
            if (batch.status or "").strip().lower() != "failed":
                skipped_count += 1
                continue

            ok = self._rebuild_and_resend_batch_admin(request, batch)
            if ok:
                success_count += 1
            else:
                skipped_count += 1

        if success_count > 0:
            self.message_user(
                request,
                f"{success_count} failed batch(es) rebuilt and resent manually.",
            )

        if skipped_count > 0:
            self.message_user(
                request,
                f"{skipped_count} batch(es) skipped or failed during manual rebuild/resend.",
                level=messages.WARNING,
            )

    action_rebuild_and_resend_now.short_description = "Rebuild and resend now (manual bypass)"    
    
    
    def action_resume_rebuild_resend_dry_run(self, request, queryset):
        success_count = 0
        skipped_count = 0

        for batch in queryset:
            ok = self._dry_run_resume_rebuild_resend_batch_admin(request, batch)
            if ok:
                success_count += 1
            else:
                skipped_count += 1

        if success_count > 0:
            self.message_user(
                request,
                f"{success_count} batch(es) analyzed in dry-run mode.",
            )

        if skipped_count > 0:
            self.message_user(
                request,
                f"{skipped_count} batch(es) skipped during dry-run diagnostic.",
                level=messages.WARNING,
            )

    action_resume_rebuild_resend_dry_run.short_description = "Dry-run diagnostic: resume / rebuild / resend"    

class InboundChangeEventAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "replication_service",
        "target_node",
        "engine",
        "database_name",
        "table_name",
        "operation",
        "status",
        "source_log_file",
        "source_log_pos",
        "received_at",
    )
    list_filter = ("engine", "operation", "status")
    search_fields = ("event_uid", "transaction_id", "database_name", "table_name")
    readonly_fields = ("primary_key_data", "before_data", "after_data", "event_payload")


class ReplicationCheckpointAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "replication_service",
        "node",
        "direction",
        "engine",
        "log_file",
        "log_pos",
        "transaction_id",
        "updated_at",
    )
    list_filter = ("direction", "engine")
    search_fields = ("transaction_id", "log_file")
    readonly_fields = ("checkpoint_data",)


class SQLObjectScopeRuleAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "is_enabled",
        "is_system_rule",
        "engine",
        "policy_mode",
        "object_type",
        "match_mode",
        "database_name",
        "table_name",
        "priority",
    )

    list_filter = (
        "is_enabled",
        "is_system_rule",
        "engine",
        "policy_mode",
        "object_type",
        "match_mode",
    )

    search_fields = (
        "name",
        "database_name",
        "table_name",
    )

    ordering = ("priority", "id")
    
    
class BootstrapPlanTableSelectionInline(admin.TabularInline):
    model = BootstrapPlanTableSelection
    form = BootstrapPlanTableSelectionAdminForm
    extra = 0
    autocomplete_fields = ("source_table",)
    fields = (
        "is_enabled",
        "execution_order",
        "source_table",
        "recreate_table",
        "truncate_target",
        "validate_snapshot",
    )  
    
    
class SourceDatabaseCatalogAdmin(admin.ModelAdmin):
    form = SourceDatabaseCatalogAdminForm

    list_display = (
        "id",
        "replication_service",
        "engine",
        "database_name",
        "is_active",
        "discovered_at",
    )
    list_filter = ("replication_service", "engine", "is_active")
    search_fields = ("database_name",)

    readonly_fields = (
        "replication_service",
        "engine",
        "database_name",
        "is_active",
        "discovered_at",
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class SourceTableCatalogAdmin(admin.ModelAdmin):
    form = SourceTableCatalogAdminForm

    list_display = (
        "id",
        "replication_service",
        "source_database",
        "table_name",
        "is_active",
        "discovered_at",
    )
    list_filter = ("replication_service", "source_database", "is_active")
    search_fields = ("table_name", "source_database__database_name")
    autocomplete_fields = ("source_database",)

    readonly_fields = (
        "replication_service",
        "source_database",
        "table_name",
        "is_active",
        "discovered_at",
    )

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False


class BootstrapPlanAdmin(admin.ModelAdmin):
    form = BootstrapPlanAdminForm

    actions = (
        "action_run_now",
        "action_replay_last",
        "action_replay_last_failed",
        "action_replay_last_failed_only",
    )
    
    
    list_display = (
        "id",
        "name",
        "replication_service",
        "is_enabled",
        "bootstrap_method",
        "scope_mode",
        "source_database_name",
        "parallel_workers",
        "database_parallel_workers",
        "execution_order",
        "status",
        "last_run_at",
        "last_total_databases",
        "last_total_tables",
        "last_success_count",
        "last_failed_count",
        "admin_replay_actions",
    )
    

    list_filter = (
        "is_enabled",
        "bootstrap_method",
        "scope_mode",
        "status",
        "recreate_table",
        "truncate_target",
        "auto_exclude_srdf",
        "validate_snapshot",
    )

    search_fields = (
        "name",
        "source_database_name",
        "database_include_csv",
        "database_exclude_csv",
        "include_tables_csv",
        "exclude_tables_csv",
        "notes",
    )

    readonly_fields = (
        "last_run_at",
        "last_duration_seconds",
        "last_total_databases",
        "last_total_tables",
        "last_success_count",
        "last_failed_count",
        "last_error",
        "last_result_payload",
        "created_at",
        "updated_at",
    )

    inlines = [BootstrapPlanTableSelectionInline]
    ordering = ("execution_order", "id")
    filter_horizontal =['selected_tables',]
    
    
    def get_urls(self):
        urls = super().get_urls()
        
        custom_urls = [
            path(
                "db-autocomplete/",
                self.admin_site.admin_view(self.db_autocomplete_view),
                name="srdf_bootstrapplan_db_autocomplete",
            ),
            path(
                "table-autocomplete/",
                self.admin_site.admin_view(self.table_autocomplete_view),
                name="srdf_bootstrapplan_table_autocomplete",
            ),
            path(
                "plan-service/",
                self.admin_site.admin_view(self.plan_service_view),
                name="srdf_bootstrapplan_plan_service",
            ),
            path(
                "<int:plan_id>/run-now/",
                self.admin_site.admin_view(self.run_now_view),
                name="srdf_bootstrapplan_run_now",
            ),
            path(
                "<int:plan_id>/replay-last/",
                self.admin_site.admin_view(self.replay_last_view),
                name="srdf_bootstrapplan_replay_last",
            ),
            path(
                "<int:plan_id>/replay-last-failed/",
                self.admin_site.admin_view(self.replay_last_failed_view),
                name="srdf_bootstrapplan_replay_last_failed",
            ),
            path(
                "<int:plan_id>/replay-last-failed-only/",
                self.admin_site.admin_view(self.replay_last_failed_only_view),
                name="srdf_bootstrapplan_replay_last_failed_only",
            ),
        ]
        
        return custom_urls + urls

    def db_autocomplete_view(self, request):
        replication_service_id = request.GET.get("replication_service_id")
        q = request.GET.get("q", "")

        try:
            replication_service = ReplicationService.objects.filter(id=replication_service_id).first()
            if not replication_service:
                return JsonResponse({"ok": False, "results": [], "error": "ReplicationService not found"}, status=404)

            values = list_source_databases(replication_service)
            values = search_values(values, q=q, limit=100)

            return JsonResponse(
                {
                    "ok": True,
                    "results": [{"value": x, "label": x} for x in values],
                }
            )
        except AdminDBCatalogError as exc:
            return JsonResponse({"ok": False, "results": [], "error": str(exc)}, status=400)
        except Exception as exc:
            return JsonResponse({"ok": False, "results": [], "error": str(exc)}, status=500)

    def table_autocomplete_view(self, request):
        replication_service_id = request.GET.get("replication_service_id")
        database_name = request.GET.get("database_name", "")
        q = request.GET.get("q", "")

        try:
            replication_service = ReplicationService.objects.filter(id=replication_service_id).first()
            if not replication_service:
                return JsonResponse({"ok": False, "results": [], "error": "ReplicationService not found"}, status=404)

            values = list_source_tables(replication_service, database_name=database_name)
            values = search_values(values, q=q, limit=100)

            return JsonResponse(
                {
                    "ok": True,
                    "results": [{"value": x, "label": x} for x in values],
                }
            )
        except AdminDBCatalogError as exc:
            return JsonResponse({"ok": False, "results": [], "error": str(exc)}, status=400)
        except Exception as exc:
            return JsonResponse({"ok": False, "results": [], "error": str(exc)}, status=500)



    def admin_replay_actions(self, obj):
        
        run_url = reverse(f"{self.admin_site.name}:srdf_bootstrapplan_run_now", args=[obj.id])
        replay_last_url = reverse(f"{self.admin_site.name}:srdf_bootstrapplan_replay_last", args=[obj.id])
        replay_last_failed_url = reverse(f"{self.admin_site.name}:srdf_bootstrapplan_replay_last_failed", args=[obj.id])
        replay_last_failed_only_url = reverse(f"{self.admin_site.name}:srdf_bootstrapplan_replay_last_failed_only", args=[obj.id])

        return format_html(
            '{} &nbsp; {} &nbsp; {} &nbsp; {}',
            format_html('<a class="button" href="{}">Run</a>', run_url),
            format_html('<a class="button" href="{}">Replay last</a>', replay_last_url),
            format_html('<a class="button" href="{}">Replay last failed</a>', replay_last_failed_url),
            format_html('<a class="button" href="{}">Replay failed only</a>', replay_last_failed_only_url),
        )

    admin_replay_actions.short_description = "Actions"



    def _redirect_plan_changelist(self):
        return HttpResponseRedirect(
            reverse(f"{self.admin_site.name}:srdf_bootstrapplan_changelist")
        )


    def _run_plan_admin(self, request, plan, replay_last=False, replay_last_failed=False, retry_failed_only=False):
        try:
            result = run_bootstrap_plan_once(
                bootstrap_plan_id=plan.id,
                bootstrap_plan_name=plan.name,
                replay_last=bool(replay_last),
                replay_last_failed=bool(replay_last_failed),
                retry_failed_only=bool(retry_failed_only),
                initiated_by=(
                    f"admin:{request.user.username}"
                    if request.user and request.user.is_authenticated
                    else "admin:anonymous"
                ),
                dry_run_override=False,
                progress_callback=None,
            )

            label = "Replay" if (replay_last or replay_last_failed) else "Run"
            messages.success(
                request,
                (
                    f"{label} OK for plan '{plan.name}' - "
                    f"databases={result.get('total_databases', 0)} "
                    f"tables={result.get('total_tables', 0)} "
                    f"success={result.get('success_count', 0)} "
                    f"failed={result.get('failed_count', 0)} "
                    f"duration={result.get('duration_seconds', 0)}s"
                ),
            )
        except Exception as exc:
            messages.error(request, f"Plan '{plan.name}' failed: {exc}")

        return self._redirect_plan_changelist()

    def run_now_view(self, request, plan_id):
        plan = BootstrapPlan.objects.filter(id=plan_id).first()
        if not plan:
            messages.error(request, f"BootstrapPlan #{plan_id} not found")
            return self._redirect_plan_changelist()
        return self._run_plan_admin(request, plan)

    def replay_last_view(self, request, plan_id):
        plan = BootstrapPlan.objects.filter(id=plan_id).first()
        if not plan:
            messages.error(request, f"BootstrapPlan #{plan_id} not found")
            return self._redirect_plan_changelist()
        return self._run_plan_admin(request, plan, replay_last=True)

    def replay_last_failed_view(self, request, plan_id):
        plan = BootstrapPlan.objects.filter(id=plan_id).first()
        if not plan:
            messages.error(request, f"BootstrapPlan #{plan_id} not found")
            return self._redirect_plan_changelist()
        return self._run_plan_admin(request, plan, replay_last_failed=True)

    def replay_last_failed_only_view(self, request, plan_id):
        plan = BootstrapPlan.objects.filter(id=plan_id).first()
        if not plan:
            messages.error(request, f"BootstrapPlan #{plan_id} not found")
            return self._redirect_plan_changelist()
        return self._run_plan_admin(
            request,
            plan,
            replay_last_failed=True,
            retry_failed_only=True,
        )

    def action_run_now(self, request, queryset):
        count = 0
        for plan in queryset:
            self._run_plan_admin(request, plan)
            count += 1
        self.message_user(request, f"{count} plan(s) launched.")

    action_run_now.short_description = "Run selected plans now"

    def action_replay_last(self, request, queryset):
        count = 0
        for plan in queryset:
            self._run_plan_admin(request, plan, replay_last=True)
            count += 1
        self.message_user(request, f"{count} plan(s) replayed from last execution.")

    action_replay_last.short_description = "Replay last execution for selected plans"

    def action_replay_last_failed(self, request, queryset):
        count = 0
        for plan in queryset:
            self._run_plan_admin(request, plan, replay_last_failed=True)
            count += 1
        self.message_user(request, f"{count} plan(s) replayed from last failed execution.")

    action_replay_last_failed.short_description = "Replay last failed execution for selected plans"

    def action_replay_last_failed_only(self, request, queryset):
        count = 0
        for plan in queryset:
            self._run_plan_admin(
                request,
                plan,
                replay_last_failed=True,
                retry_failed_only=True,
            )
            count += 1
        self.message_user(request, f"{count} plan(s) replayed with failed tables only.")

    action_replay_last_failed_only.short_description = "Replay only failed tables from last failed execution"



    def plan_service_view(self, request):
        bootstrap_plan_id = request.GET.get("bootstrap_plan_id")

        try:
            replication_service = resolve_replication_service_from_plan_id(bootstrap_plan_id)
            return JsonResponse(
                {
                    "ok": True,
                    "replication_service_id": replication_service.id,
                }
            )
        except AdminDBCatalogError as exc:
            return JsonResponse({"ok": False, "error": str(exc)}, status=400)
        except Exception as exc:
            return JsonResponse({"ok": False, "error": str(exc)}, status=500)

    class Media:
        js = ("srdf/js/bootstrap_admin_autocomplete.js",)    
    
    
    
class BootstrapPlanTableSelectionAdmin(admin.ModelAdmin):
    form = BootstrapPlanTableSelectionAdminForm

    list_display = (
        "id",
        "bootstrap_plan",
        "is_enabled",
        "status",
        "execution_order",
        "source_table",
        "recreate_table",
        "truncate_target",
        "validate_snapshot",
        "last_run_at",
        "last_duration_seconds",
        "short_last_error",
        "created_at",
    )
    list_filter = (
        "is_enabled",
        "status",
        "recreate_table",
        "truncate_target",
        "validate_snapshot",
        "bootstrap_plan",
    )
    search_fields = (
        "bootstrap_plan__name",
        "source_table__table_name",
        "source_table__source_database__database_name",
        "last_error",
    )
    autocomplete_fields = ("bootstrap_plan", "source_table")

    readonly_fields = (
        "status",
        "last_run_at",
        "last_duration_seconds",
        "last_error",
        "pretty_last_result_payload",
        "created_at",
        "updated_at",
    )
    
    def pretty_last_result_payload(self, obj):
            return pretty_json(obj.last_result_payload)
    
    pretty_last_result_payload.short_description = "Last result payload"    

    def short_last_error(self, obj):
        value = (obj.last_error or "").strip()
        if not value:
            return "-"
        if len(value) > 120:
            return value[:120] + "..."
        return value

    short_last_error.short_description = "Last error"
    
    
class BootstrapPlanExecutionItemInline(admin.TabularInline):
    model = BootstrapPlanExecutionItem
    extra = 0
    fields = (
        "status",
        "database_name",
        "table_name",
        "bootstrap_method",
        "copied_rows",
        "source_row_count",
        "target_row_count",
        "duration_seconds",
        "error_message",
    )
    readonly_fields = fields
    can_delete = False
    show_change_link = True


class BootstrapPlanExecutionAdmin(admin.ModelAdmin):
    
    
    list_display = (
        "id",
        "bootstrap_plan",
        "parent_execution",
        "replication_service",
        "status",
        "dry_run",
        "initiated_by",
        "trigger_mode",
        "replay_mode",
        "retry_failed_only",
        "total_databases",
        "total_tables",
        "success_count",
        "failed_count",
        "started_at",
        "finished_at",
        "duration_seconds",
    ) 
    
    
    list_filter = (
        "status",
        "dry_run",
        "trigger_mode",
        "replay_mode",
        "retry_failed_only",
        "replication_service",
        "bootstrap_plan",
    )
    
    
    search_fields = (
        "bootstrap_plan__name",
        "replication_service__name",
        "initiated_by",
        "last_error",
    )
    
    
    readonly_fields = (
        "bootstrap_plan",
        "parent_execution",
        "replication_service",
        "initiated_by",
        "trigger_mode",
        "replay_mode",
        "retry_failed_only",
        "status",
        "dry_run",
        "total_databases",
        "total_tables",
        "success_count",
        "failed_count",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
        "pretty_result_payload",
        "created_at",
    )
    
    
    inlines = [BootstrapPlanExecutionItemInline]

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"


class BootstrapPlanExecutionItemAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "bootstrap_execution",
        "status",
        "database_name",
        "table_name",
        "bootstrap_method",
        "copied_rows",
        "source_row_count",
        "target_row_count",
        "duration_seconds",
        "short_error_message",
        "finished_at",
    )
    list_filter = (
        "status",
        "bootstrap_method",
        "dry_run",
        "snapshot_ok",
        "table_created",
        "table_truncated",
        "bootstrap_plan",
    )
    search_fields = (
        "database_name",
        "table_name",
        "bootstrap_plan__name",
        "error_message",
    )
    readonly_fields = (
        "bootstrap_execution",
        "bootstrap_plan",
        "plan_table_selection",
        "database_name",
        "table_name",
        "bootstrap_method",
        "status",
        "recreate_table",
        "truncate_target",
        "validate_snapshot",
        "dry_run",
        "copied_rows",
        "source_row_count",
        "target_row_count",
        "source_checksum",
        "target_checksum",
        "snapshot_ok",
        "table_created",
        "table_truncated",
        "started_at",
        "finished_at",
        "duration_seconds",
        "error_message",
        "pretty_result_payload",
        "created_at",
    )
    autocomplete_fields = (
        "bootstrap_execution",
        "bootstrap_plan",
        "plan_table_selection",
    )

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"

    def short_error_message(self, obj):
        value = (obj.error_message or "").strip()
        if not value:
            return "-"
        if len(value) > 120:
            return value[:120] + "..."
        return value

    short_error_message.short_description = "Error"
    
    
    
class SRDFServicePhaseExecutionInline(admin.TabularInline):
    model = SRDFServicePhaseExecution
    extra = 0
    fields = (
        "phase_name",
        "status",
        "return_code",
        "processed_count",
        "success_count",
        "failed_count",
        "retry_after_seconds",
        "duration_seconds",
        "last_error",
        "finished_at",
    )
    readonly_fields = fields
    can_delete = False
    show_change_link = True


class SRDFOrchestratorRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "run_uid",
        "status",
        "trigger_mode",
        "initiated_by",
        "sleep_seconds",
        "total_services",
        "success_services",
        "failed_services",
        "started_at",
        "finished_at",
        "duration_seconds",
    )
    
    
    list_filter = (
        "status",
        "trigger_mode",
    )
    search_fields = (
        "run_uid",
        "last_error",
        "service_filter_csv",
    )
    
    
    readonly_fields = (
        "run_uid",
        "status",
        "trigger_mode",
        "initiated_by",
        "sleep_seconds",
        "service_filter_csv",
        "started_at",
        "finished_at",
        "duration_seconds",
        "total_services",
        "success_services",
        "failed_services",
        "last_error",
        "pretty_result_payload",
        "created_at",
    )
    
    
    inlines = [SRDFServicePhaseExecutionInline]

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"


class SRDFServiceRuntimeStateAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "replication_service",
        "is_enabled",
        "is_paused",
        "stop_requested",
        "current_phase",
        "next_phase",
        "last_success_phase",
        "cycle_number",
        "consecutive_error_count",
        "last_return_code",
        "retry_monitor_reason",
        "last_run_started_at",
        "last_run_finished_at",
        "updated_at",
    )
    
    list_filter = (
        "is_enabled",
        "is_paused",
        "stop_requested",
        "current_phase",
        "next_phase",
        "last_success_phase",
        "replication_service",
    )
    search_fields = (
        "replication_service__name",
        "last_return_code",
        "last_error",
    )
    
    
    readonly_fields = (
        "replication_service",
        "current_phase",
        "last_success_phase",
        "next_phase",
        "cycle_number",
        "consecutive_error_count",
        "last_return_code",
        "last_error",
        "retry_monitor_pretty",
        "pretty_capture_checkpoint",
        "pretty_transport_checkpoint",
        "pretty_apply_checkpoint",
        "last_capture_at",
        "last_deduplicate_at",
        "last_compress_at",
        "last_encrypt_at",
        "last_batch_build_at",
        "last_ship_at",
        "last_wait_ack_at",
        "last_receive_at",
        "last_apply_at",
        "last_checkpoint_at",
        "last_bootstrap_at",
        "last_run_started_at",
        "last_run_finished_at",
        "pretty_runtime_payload",
        "created_at",
        "updated_at",
    )
    
    def retry_monitor_pretty(self, obj):
        return pretty_json(_extract_retry_guard_from_runtime_state(obj))

    retry_monitor_pretty.short_description = "Retry monitor"    
    
    def retry_monitor_reason(self, obj):
        data = _extract_retry_guard_from_runtime_state(obj)
        reason_code = data.get("reason_code") or ""
        remaining = int(data.get("remaining_seconds") or 0)

        if reason_code == "SOURCE_RETRY_COOLDOWN_ACTIVE":
            return f"{reason_code} ({remaining}s left)"
        return reason_code or "-"

    retry_monitor_reason.short_description = "Retry monitor"    

    def pretty_capture_checkpoint(self, obj):
        return pretty_json(obj.capture_checkpoint)

    pretty_capture_checkpoint.short_description = "Capture checkpoint"

    def pretty_transport_checkpoint(self, obj):
        return pretty_json(obj.transport_checkpoint)

    pretty_transport_checkpoint.short_description = "Transport checkpoint"

    def pretty_apply_checkpoint(self, obj):
        return pretty_json(obj.apply_checkpoint)

    pretty_apply_checkpoint.short_description = "Apply checkpoint"

    def pretty_runtime_payload(self, obj):
        return pretty_json(obj.runtime_payload)

    pretty_runtime_payload.short_description = "Runtime payload"


class SRDFServicePhaseExecutionAdmin(admin.ModelAdmin):
    
    
    list_display = (
        "id",
        "orchestrator_run",
        "replication_service",
        "phase_name",
        "status",
        "return_code",
        "retry_reason_short",
        "cycle_number",
        "processed_count",
        "success_count",
        "failed_count",
        "retry_after_seconds",
        "duration_seconds",
        "finished_at",
    )
    
    
    list_filter = (
        "status",
        "phase_name",
        "return_code",
        "replication_service",
        "orchestrator_run",
    )
    
    
    search_fields = (
        "replication_service__name",
        "phase_name",
        "return_code",
        "last_error",
    )
    
    
    readonly_fields = (
        "orchestrator_run",
        "replication_service",
        "runtime_state",
        "cycle_number",
        "phase_name",
        "status",
        "return_code",
        "retry_reason_pretty",
        "started_at",
        "finished_at",
        "duration_seconds",
        "processed_count",
        "success_count",
        "failed_count",
        "skipped_count",
        "retry_after_seconds",
        "last_error",
        "pretty_request_payload",
        "pretty_result_payload",
        "created_at",
    )
    
    
    autocomplete_fields = (
        "orchestrator_run",
        "replication_service",
        "runtime_state",
    )
    
    def retry_reason_pretty(self, obj):
        return pretty_json(_extract_retry_guard_from_phase_execution(obj))

    retry_reason_pretty.short_description = "Retry monitor"    
    
    def retry_reason_short(self, obj):
        if (obj.phase_name or "").strip().lower() != "retry":
            return "-"

        data = _extract_retry_guard_from_phase_execution(obj)
        reason_code = data.get("reason_code") or ""
        remaining = int(data.get("remaining_seconds") or 0)

        if reason_code == "SOURCE_RETRY_COOLDOWN_ACTIVE":
            return f"{reason_code} ({remaining}s left)"
        return reason_code or "-"

    retry_reason_short.short_description = "Retry reason"    

    def pretty_request_payload(self, obj):
        return pretty_json(obj.request_payload)

    pretty_request_payload.short_description = "Request payload"

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"

class SRDFServiceExecutionLockAdmin(admin.ModelAdmin):
    """
    Administration améliorée pour les verrous d'exécution par service (SRDFServiceExecutionLock)
    """
    list_display = (
        "name",
        "replication_service",
        "is_locked_badge",
        "owner_token_short",
        "locked_at",
        "expires_at",
        "remaining_time",
        "updated_at",
    )

    list_filter = (
        "is_locked",
        "replication_service",
    )

    search_fields = (
        "name",
        "replication_service__name",
        "owner_token",
    )

    ordering = ("replication_service__name", "name")

    readonly_fields = (
        "name",
        "replication_service",
        "is_locked",
        "owner_token",
        "locked_at",
        "expires_at",
        "created_at",
        "updated_at",
        "pretty_remaining_time",
    )

    autocomplete_fields = ("replication_service",)

    # Actions personnalisées
    actions = ["action_release_locks"]

    # ======================
    # Colonnes personnalisées
    # ======================

    def is_locked_badge(self, obj):
        """Badge coloré selon l'état du verrou"""
        if obj.is_locked:
            return format_html(
                '<span style="background-color:#d32f2f;color:white;padding:4px 8px;border-radius:4px;font-weight:bold;">'
                '🔒 LOCKED'
                '</span>'
            )
        else:
            return format_html(
                '<span style="background-color:#388e3c;color:white;padding:4px 8px;border-radius:4px;font-weight:bold;">'
                '✅ FREE'
                '</span>'
            )

    is_locked_badge.short_description = "Statut"
    is_locked_badge.admin_order_field = "is_locked"

    def owner_token_short(self, obj):
        """Affiche une version raccourcie du token"""
        if not obj.owner_token:
            return "-"
        return obj.owner_token[:16] + "..." if len(obj.owner_token) > 16 else obj.owner_token

    owner_token_short.short_description = "Owner Token"

    def remaining_time(self, obj):
        """Temps restant avant expiration du verrou"""
        if not obj.is_locked or not obj.expires_at:
            return "-"
        
        now = timezone.now()
        if obj.expires_at <= now:
            return format_html(
                '<span style="color:#d32f2f;">Expiré</span>'
            )
        
        remaining = obj.expires_at - now
        minutes = int(remaining.total_seconds() // 60)
        
        if minutes < 60:
            return f"{minutes} min"
        elif minutes < 1440:
            hours = minutes // 60
            return f"{hours}h {minutes % 60}min"
        else:
            days = minutes // 1440
            return f"{days}j"

    remaining_time.short_description = "Temps restant"

    def pretty_remaining_time(self, obj):
        """Version détaillée pour l'écran de détail"""
        if not obj.is_locked or not obj.expires_at:
            return "Le verrou n'est pas actif ou n'a pas de date d'expiration."
        
        now = timezone.now()
        if obj.expires_at <= now:
            return "Le verrou a expiré."
        
        remaining = obj.expires_at - now
        return f"Temps restant : {remaining} (expire le {obj.expires_at})"

    pretty_remaining_time.short_description = "Temps restant détaillé"

    # ======================
    # Actions
    # ======================

    def action_release_locks(self, request, queryset):
        """Libère les verrous sélectionnés"""
        updated = 0
        for lock in queryset.filter(is_locked=True):
            lock.is_locked = False
            lock.locked_at = None
            lock.expires_at = None
            lock.owner_token = ""
            lock.save()
            updated += 1

        if updated == 1:
            self.message_user(request, "1 verrou a été libéré avec succès.")
        else:
            self.message_user(request, f"{updated} verrous ont été libérés avec succès.")

    action_release_locks.short_description = "🔓 Libérer les verrous sélectionnés"
    action_release_locks.allowed_permissions = ['change']

    # ======================
    # Organisation des champs
    # ======================

    fieldsets = (
        ("Verrou", {
            "fields": (
                "name",
                "replication_service",
                "is_locked",
                "owner_token",
            ),
        }),
        ("Dates", {
            "fields": (
                "locked_at",
                "expires_at",
                "pretty_remaining_time",
            ),
        }),
        ("Informations système", {
            "fields": ("created_at", "updated_at"),
            "classes": ("collapse",),
        }),
    )

    def has_add_permission(self, request):
        """Interdit la création manuelle de verrous via l'admin"""
        return False

    def has_delete_permission(self, request, obj=None):
        """Autorise la suppression uniquement si le verrou n'est pas actif"""
        if obj and obj.is_locked:
            return False
        return True
    
    
class SRDFSettingDefinitionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "key",
        "label",
        "category",
        "phase",
        "engine",
        "value_type",
        "is_enabled",
        "is_required",
        "is_secret",
        "is_system",
        "sort_order",
        "updated_at",
    )

    list_filter = (
        "category",
        "phase",
        "engine",
        "value_type",
        "is_enabled",
        "is_required",
        "is_secret",
        "is_system",
    )

    search_fields = (
        "key",
        "label",
        "description",
    )

    ordering = ("category", "phase", "engine", "sort_order", "id")

    readonly_fields = (
        "created_at",
        "updated_at",
        "pretty_default_value_json",
    )

    fieldsets = (
        (
            "Identity",
            {
                "fields": (
                    "key",
                    "label",
                    "description",
                )
            },
        ),
        (
            "Classification",
            {
                "fields": (
                    "category",
                    "phase",
                    "engine",
                    "value_type",
                    "sort_order",
                )
            },
        ),
        (
            "Default value",
            {
                "fields": (
                    "default_value",
                    "default_value_json",
                    "pretty_default_value_json",
                )
            },
        ),
        (
            "Flags",
            {
                "fields": (
                    "is_required",
                    "is_enabled",
                    "is_secret",
                    "is_system",
                )
            },
        ),
        (
            "Dates",
            {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    def pretty_default_value_json(self, obj):
        return pretty_json(obj.default_value_json)

    pretty_default_value_json.short_description = "Default JSON preview"


class SRDFSettingValueAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "setting_definition",
        "scope_type",
        "engine",
        "company_name",
        "client_name",
        "replication_service",
        "node",
        "priority",
        "is_enabled",
        "valid_from",
        "valid_until",
        "updated_at",
    )

    list_filter = (
        "scope_type",
        "engine",
        "is_enabled",
        "replication_service",
        "node",
    )

    search_fields = (
        "setting_definition__key",
        "setting_definition__label",
        "company_name",
        "client_name",
        "value_text",
        "notes",
    )

    ordering = ("setting_definition__key", "priority", "id")

    autocomplete_fields = (
        "setting_definition",
        "replication_service",
        "node",
    )

    readonly_fields = (
        "created_at",
        "updated_at",
        "pretty_value_json",
    )

    fieldsets = (
        (
            "Setting",
            {
                "fields": (
                    "setting_definition",
                    "scope_type",
                    "engine",
                    "priority",
                    "is_enabled",
                )
            },
        ),
        (
            "Scope targeting",
            {
                "fields": (
                    "company_name",
                    "client_name",
                    "replication_service",
                    "node",
                )
            },
        ),
        (
            "Value",
            {
                "fields": (
                    "value_text",
                    "value_json",
                    "pretty_value_json",
                    "notes",
                )
            },
        ),
        (
            "Validity",
            {
                "fields": (
                    "valid_from",
                    "valid_until",
                )
            },
        ),
        (
            "Dates",
            {
                "fields": (
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    def pretty_value_json(self, obj):
        return pretty_json(obj.value_json)

    pretty_value_json.short_description = "Value JSON preview"
    
    
class SRDFCaptureControlItemInline(admin.TabularInline):
    model = SRDFCaptureControlItem
    extra = 0
    fields = (
        "seq",
        "log_file",
        "log_pos",
        "database_name",
        "table_name",
        "operation",
        "row_count",
        "outbound_match_count",
        "status",
        "notes",
    )
    readonly_fields = fields
    can_delete = False
    show_change_link = True


class SRDFCaptureControlRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "run_uid",
        "control_mode",
        "status",
        "replication_service",
        "engine",
        "total_binlog_updates",
        "total_captured_matches",
        "total_not_captured",
        "total_duplicates",
        "total_ambiguous",
        "total_ignored",
        "started_at",
        "finished_at",
        "duration_seconds",
    )

    list_filter = (
        "control_mode",
        "status",
        "engine",
        "replication_service",
    )

    search_fields = (
        "run_uid",
        "replication_service__name",
        "checkpoint_start_file",
        "current_master_before_file",
        "window_end_file",
        "last_error",
    )

    readonly_fields = (
        "run_uid",
        "control_mode",
        "status",
        "replication_service",
        "node",
        "engine",
        "initiated_by",
        "command_line",
        "checkpoint_start_file",
        "checkpoint_start_pos",
        "current_master_before_file",
        "current_master_before_pos",
        "current_master_after_file",
        "current_master_after_pos",
        "window_end_file",
        "window_end_pos",
        "total_binlog_updates",
        "total_captured_matches",
        "total_not_captured",
        "total_duplicates",
        "total_ambiguous",
        "total_ignored",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
        "pretty_request_payload",
        "pretty_result_payload",
        "created_at",
    )

    inlines = [SRDFCaptureControlItemInline]

    def pretty_request_payload(self, obj):
        return pretty_json(obj.request_payload)

    pretty_request_payload.short_description = "Request payload"

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"


class SRDFCaptureControlItemAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "control_run",
        "replication_service",
        "seq",
        "log_file",
        "log_pos",
        "database_name",
        "table_name",
        "operation",
        "row_count",
        "outbound_match_count",
        "status",
        "short_signature",
        "created_at",
    )

    list_filter = (
        "status",
        "operation",
        "engine",
        "replication_service",
        "database_name",
        "table_name",
    )

    search_fields = (
        "event_signature",
        "database_name",
        "table_name",
        "log_file",
        "notes",
    )

    readonly_fields = (
        "control_run",
        "replication_service",
        "seq",
        "log_file",
        "log_pos",
        "engine",
        "database_name",
        "table_name",
        "operation",
        "row_count",
        "event_signature",
        "outbound_match_count",
        "first_outbound_event_id",
        "status",
        "notes",
        "pretty_payload_preview",
        "created_at",
    )

    def short_signature(self, obj):
        value = (obj.event_signature or "").strip()
        if not value:
            return "-"
        if len(value) > 24:
            return value[:24] + "..."
        return value

    short_signature.short_description = "Signature"

    def pretty_payload_preview(self, obj):
        return pretty_json(obj.payload_preview)

    pretty_payload_preview.short_description = "Payload preview"
    
    
class SRDFCronExecutionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "execution_uid",
        "cron_name",
        "cron_type",
        "replication_service",
        "node",
        "status",
        "return_code",
        "duration_seconds",
        "started_at",
        "finished_at",
    )
    list_filter = (
        "status",
        "cron_type",
        "replication_service",
        "node",
        "engine",
    )
    search_fields = (
        "execution_uid",
        "cron_name",
        "replication_service__name",
        "error_message",
        "command_line",
    )

    readonly_fields = (
        "execution_uid",
        "cron_name",
        "cron_type",
        "replication_service",
        "node",
        "engine",
        "command_line",
        "initiated_by",
        "status",
        "return_code",
        "error_code",
        "error_message",
        "started_at",
        "finished_at",
        "duration_seconds",
        "pretty_input_params",
        "pretty_result_payload",
        "pretty_progress_payload",
        "created_at",
        "updated_at",
    )

    def pretty_input_params(self, obj):
        return pretty_json(obj.input_params)

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    def pretty_progress_payload(self, obj):
        return pretty_json(obj.progress_payload)

    pretty_input_params.short_description = "Input params"
    pretty_result_payload.short_description = "Result payload"
    pretty_progress_payload.short_description = "Progress payload"
    
    
    
class SRDFDedupDecisionInline(admin.TabularInline):
    model = SRDFDedupDecision
    extra = 0
    fields = (
        "decision_type",
        "reason_code",
        "database_name",
        "table_name",
        "operation",
        "event_id",
        "survivor_event_id",
        "log_file",
        "log_pos",
    )
    readonly_fields = fields
    can_delete = False
    show_change_link = True


class SRDFDedupRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "replication_service",
        "status",
        "processed_count",
        "survivor_count",
        "duplicate_count",
        "exact_duplicate_count",
        "neutralized_count",
        "collapsed_count",
        "started_at",
        "finished_at",
        "duration_seconds",
    )
    list_filter = (
        "status",
        "replication_service",
    )
    search_fields = (
        "run_uid",
        "initiated_by",
        "last_error",
    )
    readonly_fields = (
        "replication_service",
        "orchestrator_run",
        "phase_execution",
        "runtime_state",
        "run_uid",
        "initiated_by",
        "status",
        "processed_count",
        "survivor_count",
        "duplicate_count",
        "exact_duplicate_count",
        "neutralized_count",
        "collapsed_count",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
        "pretty_result_payload",
        "created_at",
    )
    inlines = [SRDFDedupDecisionInline]

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"


class SRDFDedupDecisionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "dedup_run",
        "decision_type",
        "reason_code",
        "database_name",
        "table_name",
        "operation",
        "event_id",
        "survivor_event_id",
        "short_pk",
        "log_file",
        "log_pos",
        "created_at",
    )
    list_filter = (
        "decision_type",
        "reason_code",
        "database_name",
        "table_name",
        "replication_service",
    )
    search_fields = (
        "database_name",
        "table_name",
        "reason_code",
        "log_file",
        "group_key",
        "business_signature",
    )
    readonly_fields = (
        "dedup_run",
        "replication_service",
        "database_name",
        "table_name",
        "operation",
        "decision_type",
        "reason_code",
        "event_id",
        "survivor_event_id",
        "log_file",
        "log_pos",
        "group_key",
        "business_signature",
        "pretty_primary_key_data",
        "pretty_before_data",
        "pretty_after_data",
        "pretty_payload_preview",
        "created_at",
    )

    def short_pk(self, obj):
        try:
            if obj.primary_key_data:
                return json.dumps(obj.primary_key_data, ensure_ascii=False)[:80]
        except Exception:
            pass
        return "-"

    short_pk.short_description = "Primary key"

    def pretty_primary_key_data(self, obj):
        return pretty_json(obj.primary_key_data)

    def pretty_before_data(self, obj):
        return pretty_json(obj.before_data)

    def pretty_after_data(self, obj):
        return pretty_json(obj.after_data)

    def pretty_payload_preview(self, obj):
        return pretty_json(obj.payload_preview)

    pretty_primary_key_data.short_description = "Primary key data"
    pretty_before_data.short_description = "Before data"
    pretty_after_data.short_description = "After data"
    pretty_payload_preview.short_description = "Payload preview"
    
    
    
class SRDFArchiveRunItemInline(admin.TabularInline):
    model = SRDFArchiveRunItem
    extra = 0
    can_delete = False
    fields = (
        "table_name",
        "archive_model_label",
        "status",
        "selected_count",
        "inserted_count",
        "deleted_count",
        "duration_seconds",
        "last_error",
    )
    readonly_fields = fields
    show_change_link = True


class SRDFArchiveExecutionLockAdmin(admin.ModelAdmin):
    list_display = (
        "name",
        "is_locked",
        "owner_token",
        "locked_at",
        "expires_at",
        "updated_at",
    )
    list_filter = ("is_locked",)
    search_fields = ("name", "owner_token")
    readonly_fields = (
        "name",
        "is_locked",
        "owner_token",
        "locked_at",
        "expires_at",
        "created_at",
        "updated_at",
    )


class SRDFArchiveRunItemAdmin(admin.ModelAdmin):
    
    list_display = (
        "id",
        "archive_run",
        "replication_service",
        "table_name",
        "status",
        "selected_count",
        "inserted_count",
        "deleted_count",
        "skipped_count",
        "duration_seconds",
        "finished_at",
    )
    
    list_filter = (
        "status",
        "table_name",
        "replication_service",
        "archive_run",
    )
    
    search_fields = (
        "table_name",
        "archive_model_label",
        "last_error",
    )
    readonly_fields = (
        "archive_run",
        "replication_service",
        "table_name",
        "archive_model_label",
        "archive_before_datetime",
        "status",
        "selected_count",
        "inserted_count",
        "deleted_count",
        "skipped_count",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
        "request_payload",
        "result_payload",
        "created_at",
    )


class SRDFArchiveRunAdmin(admin.ModelAdmin):
    inlines = [SRDFArchiveRunItemInline]

    list_display = (
        "id",
        "run_uid",
        "status",
        "trigger_mode",
        "archive_database_name",
        "retention_days",
        "total_tables",
        "success_tables",
        "failed_tables",
        "failed_items_count",
        "total_rows_selected",
        "total_rows_inserted",
        "total_rows_deleted",
        "archived_tables_summary",
        "admin_open_failed_items_action",
        "admin_run_archive_now_action",
        "admin_prepare_archive_tables_action",
        "started_at",
        "finished_at",
    )

    list_filter = (
        "status",
        "trigger_mode",
        "archive_database_name",
    )

    search_fields = (
        "run_uid",
        "initiated_by",
        "last_error",
    )

    readonly_fields = (
        "run_uid",
        "status",
        "trigger_mode",
        "initiated_by",
        "archive_database_name",
        "retention_days",
        "batch_size",
        "archive_before_datetime",
        "total_tables",
        "success_tables",
        "failed_tables",
        "total_rows_selected",
        "total_rows_inserted",
        "total_rows_deleted",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
        "pretty_request_payload",
        "pretty_result_payload",
        "admin_run_archive_now_action_detail",
        "admin_prepare_archive_tables_action_detail",
        "created_at",
    )

    actions = [
        "action_run_archive_now",
        "action_prepare_archive_tables_now",
    ]

    fieldsets = (
        (
            "Main",
            {
                "fields": (
                    "run_uid",
                    "status",
                    "trigger_mode",
                    "initiated_by",
                    "archive_database_name",
                    "retention_days",
                    "batch_size",
                    "archive_before_datetime",
                )
            },
        ),
        (
            "Results",
            {
                "fields": (
                    "total_tables",
                    "success_tables",
                    "failed_tables",
                    "total_rows_selected",
                    "total_rows_inserted",
                    "total_rows_deleted",
                    "last_error",
                )
            },
        ),
        (
            "Actions",
            {
                "fields": (
                    "admin_run_archive_now_action_detail",
                    "admin_prepare_archive_tables_action_detail",
                )
            },
        ),
        (
            "Payload",
            {
                "fields": (
                    "pretty_request_payload",
                    "pretty_result_payload",
                )
            },
        ),
        (
            "Timing",
            {
                "fields": (
                    "started_at",
                    "finished_at",
                    "duration_seconds",
                    "created_at",
                )
            },
        ),
    )
    
    def failed_items_count(self, obj):
        return obj.items.filter(status="failed").count()

    failed_items_count.short_description = "Failed items"


    def archived_tables_summary(self, obj):
        payload = obj.result_payload or {}
        rows = payload.get("archived_rows_by_table") or {}

        if not rows:
            return "-"

        parts = []
        for table_name, stats in rows.items():
            inserted_count = int((stats or {}).get("inserted_count") or 0)
            deleted_count = int((stats or {}).get("deleted_count") or 0)
            status = str((stats or {}).get("status") or "").strip().lower()

            badge = "OK"
            if status == "failed":
                badge = "ERR"
            elif status == "skipped":
                badge = "SKIP"

            parts.append(f"{table_name}: {badge} i={inserted_count} d={deleted_count}")

        return " | ".join(parts[:6]) + (" ..." if len(parts) > 6 else "")

    archived_tables_summary.short_description = "Rows by table"


    def admin_open_failed_items_action(self, obj):
        failed_count = obj.items.filter(status="failed").count()
        if failed_count <= 0:
            return "-"

        url = reverse(f"{self.admin_site.name}:srdf_archiverunitem_changelist")
        url = f"{url}?archive_run__id__exact={obj.id}&status__exact=failed"

        return format_html(
            '<a class="button" href="{}" style="background:#c62828;color:white;padding:4px 8px;border-radius:4px;">Failed items ({})</a>',
            url,
            failed_count,
        )

    admin_open_failed_items_action.short_description = "Failed items"    

    def get_urls(self):
        urls = super().get_urls()
        custom_urls = [
            path(
                "run-now/",
                self.admin_site.admin_view(self.run_archive_now_view),
                name="srdf_archiverun_run_now",
            ),
            path(
                "prepare-tables/",
                self.admin_site.admin_view(self.prepare_archive_tables_now_view),
                name="srdf_archiverun_prepare_tables",
            ),
        ]
        return custom_urls + urls

    def pretty_request_payload(self, obj):
        return pretty_json(obj.request_payload)

    pretty_request_payload.short_description = "Request payload"

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"

    def admin_run_archive_now_action(self, obj):
        url = reverse(f"{self.admin_site.name}:srdf_archiverun_run_now")
        return format_html(
            '<a class="button" href="{}" style="background:#2e7d32;color:white;padding:4px 8px;border-radius:4px;">Run archive now</a>',
            url,
        )

    admin_run_archive_now_action.short_description = "Run archive"

    def admin_run_archive_now_action_detail(self, obj):
        return self.admin_run_archive_now_action(obj)

    admin_run_archive_now_action_detail.short_description = "Run archive"

    def admin_prepare_archive_tables_action(self, obj):
        url = reverse(f"{self.admin_site.name}:srdf_archiverun_prepare_tables")
        return format_html(
            '<a class="button" href="{}" style="background:#1565c0;color:white;padding:4px 8px;border-radius:4px;">Prepare archive tables</a>',
            url,
        )

    admin_prepare_archive_tables_action.short_description = "Prepare tables"

    def admin_prepare_archive_tables_action_detail(self, obj):
        return self.admin_prepare_archive_tables_action(obj)

    admin_prepare_archive_tables_action_detail.short_description = "Prepare tables"

    def run_archive_now_view(self, request):
        try:
            result = run_archive_once(
                replication_service_id=None,
                initiated_by=(
                    f"admin:{request.user.username}"
                    if request.user and request.user.is_authenticated
                    else "admin:anonymous"
                ),
                trigger_mode="manual",
                dry_run=False,
                progress_callback=None,
            )
            messages.success(
                request,
                f"Archive run completed: status={result.get('status')} archive_run_id={result.get('archive_run_id')}",
            )
        except Exception as exc:
            messages.error(request, f"Archive run failed: {exc}")

        return HttpResponseRedirect(
            reverse(f"{self.admin_site.name}:srdf_archiverun_changelist")
        )

    def prepare_archive_tables_now_view(self, request):
        try:
            result = _ensure_archive_tables_ready(
                archive_db_alias="srdf_archive",
                auto_create=True,
                fail_if_missing=True,
                progress_callback=None,
            )
            messages.success(
                request,
                (
                    f"Archive tables prepared successfully - "
                    f"created={len(result.get('created_tables') or [])}"
                ),
            )
        except Exception as exc:
            messages.error(request, f"Prepare archive tables failed: {exc}")

        return HttpResponseRedirect(
            reverse(f"{self.admin_site.name}:srdf_archiverun_changelist")
        )

    def action_run_archive_now(self, request, queryset):
        try:
            result = run_archive_once(
                replication_service_id=None,
                initiated_by=(
                    f"admin:{request.user.username}"
                    if request.user and request.user.is_authenticated
                    else "admin:anonymous"
                ),
                trigger_mode="manual",
                dry_run=False,
                progress_callback=None,
            )
            self.message_user(
                request,
                f"Archive run completed: status={result.get('status')} archive_run_id={result.get('archive_run_id')}",
            )
        except Exception as exc:
            self.message_user(
                request,
                f"Archive run failed: {exc}",
                level=messages.ERROR,
            )

    action_run_archive_now.short_description = "Run archive now"

    def action_prepare_archive_tables_now(self, request, queryset):
        try:
            result = _ensure_archive_tables_ready(
                archive_db_alias="srdf_archive",
                auto_create=True,
                fail_if_missing=True,
                progress_callback=None,
            )
            self.message_user(
                request,
                f"Archive tables prepared successfully - created={len(result.get('created_tables') or [])}",
            )
        except Exception as exc:
            self.message_user(
                request,
                f"Prepare archive tables failed: {exc}",
                level=messages.ERROR,
            )

    action_prepare_archive_tables_now.short_description = "Prepare archive tables now"
    
    
class SRDFInstallRunInline(admin.TabularInline):
    model = SRDFInstallRun
    extra = 0
    can_delete = False
    fields = (
        "status",
        "initiated_by",
        "control_db_name",
        "target_db_name",
        "replication_service_id_value",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
    )
    readonly_fields = fields
    show_change_link = True


class SRDFInstallProfileAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "name",
        "is_enabled",
        "install_scope",
        "db_host",
        "db_port",
        "control_db_name",
        "target_db_name",
        "service_name",
        "service_type",
        "mode",
        "status",
        "last_run_at",
    )

    list_filter = (
        "is_enabled",
        "install_scope",
        "service_type",
        "mode",
        "run_migrate",
        "seed_settings",
        "create_archive_db",
        "auto_create_target_db",
        "status",
    )

    search_fields = (
        "name",
        "control_db_name",
        "target_db_name",
        "service_name",
        "source_node_name",
        "target_node_name",
        "source_api_base_url",
        "target_api_base_url",
    )

    readonly_fields = (
        "status",
        "last_run_at",
        "last_error",
        "pretty_last_result_payload",
        "created_at",
        "updated_at",
    )

    fieldsets = (
        (
            "Main",
            {
                "fields": (
                    "name",
                    "is_enabled",
                    "install_scope",
                    "status",
                    "notes",
                )
            },
        ),
        (
            "Remote DB control plane",
            {
                "fields": (
                    "db_host",
                    "db_port",
                    "db_user",
                    "db_password",
                    "control_db_name",
                    "archive_db_name",
                    "target_db_name",
                )
            },
        ),
        (
            "Source node",
            {
                "fields": (
                    "source_node_name",
                    "source_hostname",
                    "source_db_port",
                    "source_site",
                    "source_api_base_url",
                )
            },
        ),
        (
            "Target node",
            {
                "fields": (
                    "target_node_name",
                    "target_hostname",
                    "target_db_port",
                    "target_site",
                    "target_api_base_url",
                )
            },
        ),
        (
            "Service",
            {
                "fields": (
                    "api_token",
                    "service_name",
                    "service_type",
                    "mode",
                    "source_db_name",
                    "source_db_user",
                    "source_db_password",
                    "binlog_server_id",
                )
            },
        ),
        (
            "Install options",
            {
                "fields": (
                    "run_migrate",
                    "seed_settings",
                    "create_archive_db",
                    "auto_create_target_db",
                )
            },
        ),
        (
            "Last execution",
            {
                "fields": (
                    "last_run_at",
                    "last_error",
                    "pretty_last_result_payload",
                    "created_at",
                    "updated_at",
                )
            },
        ),
    )

    inlines = [SRDFInstallRunInline]

    def pretty_last_result_payload(self, obj):
        return pretty_json(obj.last_result_payload)

    pretty_last_result_payload.short_description = "Last result payload"


class SRDFInstallRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "install_profile",
        "status",
        "initiated_by",
        "control_db_name",
        "target_db_name",
        "replication_service_id_value",
        "started_at",
        "finished_at",
        "duration_seconds",
    )

    list_filter = (
        "status",
        "install_profile",
    )

    search_fields = (
        "install_profile__name",
        "initiated_by",
        "control_db_name",
        "target_db_name",
        "last_error",
    )

    readonly_fields = (
        "install_profile",
        "run_uid",
        "status",
        "initiated_by",
        "control_db_name",
        "archive_db_name",
        "target_db_name",
        "source_node_id_value",
        "target_node_id_value",
        "replication_service_id_value",
        "started_at",
        "finished_at",
        "duration_seconds",
        "last_error",
        "pretty_result_payload",
        "created_at",
    )

    def pretty_result_payload(self, obj):
        return pretty_json(obj.result_payload)

    pretty_result_payload.short_description = "Result payload"