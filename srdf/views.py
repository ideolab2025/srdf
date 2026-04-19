#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json

from django.contrib import messages
from django.http import JsonResponse
from django.shortcuts import redirect, render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST

from django.apps import apps
from django.utils.html import format_html, format_html_join

from srdf.services.model_migration_doctor import ModelMigrationDoctorService

from srdf.models import ActionExecution, FailoverPlan, Node, BootstrapPlan

from srdf.services.action_runner import run_srdf_action
from srdf.services.api_auth import check_api_token
from srdf.services.status_builder import build_dashboard_context, build_global_status
from srdf.services.failover_validator import validate_failover_plan
from srdf.services.bootstrap_plan_runner import run_bootstrap_plan_once


@require_GET
def doctor_modal_view(request):
    app_label = str(request.GET.get("app_label", "")).strip()
    model_name = str(request.GET.get("model_name", "")).strip()

    if not app_label or not model_name:
        return JsonResponse({
            "success": False,
            "error": "Missing app_label or model_name",
        }, status=400)

    try:
        model = apps.get_model(app_label, model_name)
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": "Model resolution failed: {}".format(exc),
        }, status=404)

    try:
        service = ModelMigrationDoctorService(model=model, using="default")
        doctor = service.analyze()
    except Exception as exc:
        return JsonResponse({
            "success": False,
            "error": "Doctor analyze failed: {}".format(exc),
        }, status=500)
    
    
    modal_key = "{}_{}".format(app_label, model_name)
    severity = doctor["severity"]
    if severity == "CRITICAL":
        status_color = "#ff8a80"
        status_bg = "#5a1f1f"
    elif severity == "WARNING":
        status_color = "#ffca28"
        status_bg = "#5c4712"
    else:
        status_color = "#66bb6a"
        status_bg = "#173a52"

        structure_html = format_html_join(
            "",
            '<tr style="background:#101733;">'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);color:#f3f7ff;font-weight:bold;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);color:#d6e2ff;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#c7d6f8;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#c7d6f8;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#c7d6f8;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#7fd4ff;font-weight:bold;">{}</td>'
            '</tr>',
            (
                (
                    row["name"],
                    row["type"],
                    "Y" if row["null"] else "-",
                    "Y" if row["primary_key"] else "-",
                    "Y" if row["unique"] else "-",
                    row["relation_type"],
                )
                for row in doctor["django_fields"]
            ),
        )
        
    db_objects_html = format_html(
        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:18px;">'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;'
            'background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Table status</div>'
                '<div style="margin-bottom:8px;color:#e6efff;">Exists: <strong>{}</strong></div>'
                '<div style="margin-bottom:8px;color:#e6efff;">DB columns: <strong>{}</strong></div>'
                '<div style="margin-bottom:8px;color:#e6efff;">PK columns: <strong>{}</strong></div>'
                '<div style="margin-bottom:8px;color:#e6efff;">Unique columns: <strong>{}</strong></div>'
                '<div style="margin-bottom:8px;color:#e6efff;">FK columns: <strong>{}</strong></div>'
            '</div>'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;'
            'background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Schema diff</div>'
                '<div style="margin-bottom:8px;color:#e6efff;">Missing in DB: <strong>{}</strong></div>'
                '<div style="margin-bottom:8px;color:#e6efff;">Extra in DB: <strong>{}</strong></div>'
            '</div>'
        '</div>',
        "YES" if doctor["db_snapshot"]["table_exists"] else "NO",
        doctor["schema_diff"]["db_column_count"],
        ", ".join(doctor["db_snapshot"]["primary_key_columns"]) or "-",
        ", ".join(doctor["db_snapshot"]["unique_columns"]) or "-",
        ", ".join(doctor["db_snapshot"]["foreign_key_columns"]) or "-",
        ", ".join(doctor["schema_diff"]["missing_in_db"]) or "-",
        ", ".join(doctor["schema_diff"]["extra_in_db"]) or "-",
    )    

    relation_html = format_html_join(
        "",
        '<div style="margin-bottom:10px;padding:10px 12px;background:linear-gradient(180deg,#121a3c 0%,#101733 100%);'
        'border:1px solid rgba(143,176,209,0.16);border-radius:8px;">'
            '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
                '<span style="display:inline-block;padding:2px 7px;border-radius:999px;background:#1a2552;color:#e6efff;font-size:10px;font-weight:bold;">{}</span>'
                '<span style="font-size:11px;color:#91a3d8;font-weight:bold;">Relation</span>'
            '</div>'
            '<div style="font-size:12px;color:#f2f6ff;font-weight:bold;">{}</div>'
            '<div style="margin-top:4px;font-size:11px;color:#9fb1e5;">&#8594; {}</div>'
        '</div>',
        (
            (
                row["relation_type"],
                row["left_side"],
                row["right_side"],
            )
            for row in doctor["relation_entries"]
        ),
    ) if doctor["relation_entries"] else format_html(
        '<div style="padding:12px;color:#91a3d8;">No relation detected.</div>'
    )

    risk_items = []
    for item in doctor["index_risks"]["risky_indexed_fields"]:
        risk_items.append(("Indexed field risk", item))
    for item in doctor["index_risks"]["risky_unique_fields"]:
        risk_items.append(("Unique field risk", item))
    for item in doctor["index_risks"]["missing_fk_indexes"]:
        risk_items.append(("FK index check", item))
    for item in doctor["index_risks"]["risky_model_indexes"]:
        risk_items.append(("Composite index risk", "{} ({})".format(item["name"], ", ".join(item["fields"]))))

    risks_html = format_html_join(
        "",
        '<div style="margin-bottom:10px;padding:10px 12px;background:linear-gradient(180deg,#2b1f10 0%,#20170d 100%);'
        'border:1px solid rgba(255,202,40,0.20);border-radius:8px;">'
            '<div style="font-size:11px;color:#ffe28a;font-weight:bold;">{}</div>'
            '<div style="margin-top:5px;font-size:12px;color:#fff4cc;">{}</div>'
        '</div>',
        risk_items,
    ) if risk_items else format_html(
        '<div style="padding:12px;color:#8fe3ff;">No obvious index risk detected.</div>'
    )

    impact_notes_html = format_html_join(
        "",
        '<div style="margin-bottom:8px;padding:10px 12px;background:#101f16;border:1px solid rgba(102,187,106,0.18);border-radius:8px;color:#b8f5be;font-size:12px;">{}</div>',
        ((item,) for item in doctor["migration_impact"]["notes"]),
    ) if doctor["migration_impact"]["notes"] else format_html(
        '<div style="padding:12px;color:#91a3d8;">No migration impact note.</div>'
    )

    repair_plan_html = format_html_join(
        "",
        '<div style="margin-bottom:10px;padding:10px 12px;background:linear-gradient(180deg,#101f16 0%,#0c1711 100%);'
        'border:1px solid rgba(102,187,106,0.20);border-radius:8px;">'
            '<div style="font-size:12px;color:#b8f5be;font-weight:bold;">{}</div>'
        '</div>',
        ((item,) for item in doctor["recommended_actions"]),
    )

    overview_panel_html = format_html(
        '<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;">'
            '<div style="padding:12px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#101733 0%,#0d1430 100%);"><div style="font-size:11px;color:#91a3d8;font-weight:bold;">Rows</div><div style="margin-top:6px;font-size:20px;color:#f2f6ff;font-weight:bold;">{}</div></div>'
            '<div style="padding:12px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#101733 0%,#0d1430 100%);"><div style="font-size:11px;color:#91a3d8;font-weight:bold;">Django fields</div><div style="margin-top:6px;font-size:20px;color:#f2f6ff;font-weight:bold;">{}</div></div>'
            '<div style="padding:12px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#101733 0%,#0d1430 100%);"><div style="font-size:11px;color:#91a3d8;font-weight:bold;">Relations</div><div style="margin-top:6px;font-size:20px;color:#f2f6ff;font-weight:bold;">{}</div></div>'
            '<div style="padding:12px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#101733 0%,#0d1430 100%);"><div style="font-size:11px;color:#91a3d8;font-weight:bold;">DB columns</div><div style="margin-top:6px;font-size:20px;color:#f2f6ff;font-weight:bold;">{}</div></div>'
        '</div>'
        '<div style="margin-top:16px;display:grid;grid-template-columns:1fr 1fr;gap:18px;">'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Missing in DB</div>'
                '<div style="color:#f2f6ff;">{}</div>'
            '</div>'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Extra in DB</div>'
                '<div style="color:#f2f6ff;">{}</div>'
            '</div>'
        '</div>',
        doctor["row_count"] if doctor["row_count"] >= 0 else "?",
        len(doctor["django_fields"]),
        len(doctor["relation_entries"]),
        doctor["schema_diff"]["db_column_count"],
        ", ".join(doctor["schema_diff"]["missing_in_db"]) or "-",
        ", ".join(doctor["schema_diff"]["extra_in_db"]) or "-",
    )

    html = format_html(
        '<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px;">'
            '<span style="display:inline-block;padding:5px 12px;border-radius:999px;background:{status_bg};color:{status_color};font-size:12px;font-weight:bold;">{severity}</span>'
            '<span style="font-size:12px;color:#9fb1e5;">table: {db_table} | db alias: {db_alias}</span>'
        '</div>'

        '<div class="srdf-doctor-tabs" style="display:flex;gap:8px;padding:0 0 16px 0;">'
            '<button type="button" class="srdf-doctor-tab-button is-active" data-tab-target="{modal_id}_overview" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#1a2552;color:#e6efff;font-size:12px;font-weight:bold;">Overview</button>'
            '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}_structure" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Structure</button>'
            '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}_db" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">DB Objects</button>'
            '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}_relations" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Relations</button>'
            '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}_risks" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Index Risks</button>'
            '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}_impact" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Migration Impact</button>'
            '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}_repair" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Repair Plan</button>'
        '</div>'

        '<div id="{modal_id}_overview" class="srdf-doctor-tab-panel" style="display:block;">{overview_panel_html}</div>'

        '<div id="{modal_id}_structure" class="srdf-doctor-tab-panel" style="display:none;">'
        '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
        '<table style="width:100%;border-collapse:collapse;font-size:12px;background:#101733;">'                    '<thead><tr style="background:#1b2755;">'
        '<th style="text-align:left;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#f3f7ff;">Field</th>'
        '<th style="text-align:left;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#f3f7ff;">Type</th>'
        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#f3f7ff;">Null</th>'
        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#f3f7ff;">PK</th>'
        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#f3f7ff;">Unique</th>'
        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#f3f7ff;">Relation</th>'                    '</tr></thead>'
                    '<tbody>{structure_html}</tbody>'
                '</table>'
            '</div>'
        '</div>'

        '<div id="{modal_id}_db" class="srdf-doctor-tab-panel" style="display:none;">'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{db_objects_html}</div>'
        '</div>'

        '<div id="{modal_id}_relations" class="srdf-doctor-tab-panel" style="display:none;">'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{relation_html}</div>'
        '</div>'

        '<div id="{modal_id}_risks" class="srdf-doctor-tab-panel" style="display:none;">'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{risks_html}</div>'
        '</div>'

        '<div id="{modal_id}_impact" class="srdf-doctor-tab-panel" style="display:none;">'
            '{impact_html}'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{impact_notes_html}</div>'
        '</div>'

        '<div id="{modal_id}_repair" class="srdf-doctor-tab-panel" style="display:none;">'
            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{repair_plan_html}</div>'
        '</div>',
        modal_id=modal_key,
        status_bg=status_bg,
        status_color=status_color,
        severity=severity,
        db_table=doctor["db_table"],
        db_alias=doctor["database_alias"],
        overview_panel_html=overview_panel_html,
        structure_html=structure_html,
        db_objects_html=db_objects_html,
        relation_html=relation_html,
        risks_html=risks_html,
        impact_html=format_html(
            '<div style="margin-bottom:14px;padding:12px 14px;background:linear-gradient(180deg,#121a3c 0%,#101733 100%);'
            'border:1px solid rgba(143,176,209,0.16);border-radius:10px;">'
                '<div style="font-size:12px;color:#91a3d8;font-weight:bold;">Migration impact</div>'
                '<div style="margin-top:8px;color:#f2f6ff;">Impact level: <strong>{}</strong></div>'
                '<div style="margin-top:4px;color:#f2f6ff;">Impact score: <strong>{}</strong></div>'
            '</div>',
            doctor["migration_impact"]["impact_level"],
            doctor["migration_impact"]["impact_score"],
        ),
        impact_notes_html=impact_notes_html,
        repair_plan_html=repair_plan_html,
    )
    
    
    
    

    return JsonResponse({
        "success": True,
        "html": str(html),
    })

def dashboard(request):
    context = build_dashboard_context()
    context["active_tab"] = request.GET.get("tab", "tab-overview")
    context["base_menu"] = "y"
    context["srdf_home_url"] = "/srdf/"
    context["srdf_admin_base_url"] = "/admin-api/"
    return render(request, "srdf/dashboard.html", context)


@require_POST
def dashboard_run_action(request):
    action_name = (request.POST.get("action_name") or "").strip()
    next_tab = (request.POST.get("next_tab") or "tab-nodes").strip()
    node_id = request.POST.get("node_id")

    if not action_name:
        messages.error(request, "Missing action_name.")
        return redirect(f"/srdf/?tab={next_tab}")

    node = None
    if node_id:
        try:
            node = Node.objects.get(pk=node_id)
        except Node.DoesNotExist:
            messages.error(request, f"Node #{node_id} not found.")
            return redirect(f"/srdf/?tab={next_tab}")

    params = {}

    if action_name in ("failover.prepare", "failover.execute", "failover.execute_dry_run"):
        plan_id = request.POST.get("plan_id")
        if plan_id:
            params["plan_id"] = plan_id

    if action_name == "transport.build_batch":
        replication_service_id = request.POST.get("replication_service_id")
        limit = request.POST.get("limit")

        if replication_service_id:
            params["replication_service_id"] = replication_service_id

        if limit:
            params["limit"] = limit
            
            
    if action_name == "transport.send_batch":
        batch_id = request.POST.get("batch_id")

        if batch_id:
            params["batch_id"] = batch_id    

    try:
        execution = run_srdf_action(
            action_name=action_name,
            node_id=node.id if node else None,
            params=params,
            initiated_by=f"dashboard:{request.user.username if request.user.is_authenticated else 'anonymous'}",
        )
        
        msg = execution.result_payload.get("message") or f"Action '{action_name}' executed."
        messages.success(request, msg)
    except Exception as exc:
        messages.error(request, f"Action failed: {exc}")

    return redirect(f"/srdf/?tab={next_tab}")




@require_GET
def action_detail(request, execution_id):
    try:
        execution = (
            ActionExecution.objects
            .select_related("node")
            .get(pk=execution_id)
        )
    except ActionExecution.DoesNotExist:
        return JsonResponse(
            {"ok": False, "error": f"Execution #{execution_id} not found"},
            status=404,
        )

    return JsonResponse(
        {
            "ok": True,
            "execution": {
                "id": execution.id,
                "node_name": execution.node.name if execution.node_id else "",
                "action_name": execution.action_name,
                "status": execution.status,
                "request_id": execution.request_id,
                "initiated_by": execution.initiated_by,
                "duration_ms": execution.duration_ms,
                "started_at": execution.started_at.isoformat() if execution.started_at else None,
                "finished_at": execution.finished_at.isoformat() if execution.finished_at else None,
                "params": execution.params or {},
                "result_payload": execution.result_payload or {},
                "stdout": execution.stdout or "",
                "stderr": execution.stderr or "",
            },
        },
        status=200,
    )


@require_GET
def node_detail(request, node_id):
    try:
        node = Node.objects.get(pk=node_id)
    except Node.DoesNotExist:
        return JsonResponse(
            {"ok": False, "error": f"Node #{node_id} not found"},
            status=404,
        )

    payload = node.last_status_payload or {}
    ping_payload = payload.get("ping") if isinstance(payload, dict) else {}
    status_payload = payload.get("status") if isinstance(payload, dict) else {}
    services_payload = status_payload.get("services") if isinstance(status_payload, dict) else {}

    return JsonResponse(
        {
            "ok": True,
            "node": {
                "id": node.id,
                "name": node.name,
                "hostname": node.hostname,
                "site": node.site,
                "role": node.role,
                "status": node.status,
                "is_enabled": node.is_enabled,
                "api_base_url": node.api_base_url,
                "last_seen_at": node.last_seen_at.isoformat() if node.last_seen_at else None,
                "ping_payload": ping_payload or {},
                "status_payload": status_payload or {},
                "services_payload": services_payload or {},
            },
        },
        status=200,
    )


@require_GET
def failover_plan_detail(request, plan_id):
    try:
        plan = FailoverPlan.objects.get(pk=plan_id)
    except FailoverPlan.DoesNotExist:
        return JsonResponse(
            {"ok": False, "error": f"FailoverPlan #{plan_id} not found"},
            status=404,
        )


    validation = validate_failover_plan(plan)

    return JsonResponse(
        {
            "ok": True,
            "plan": {
                "id": plan.id,
                "name": plan.name,
                "source_site": plan.source_site,
                "target_site": plan.target_site,
                "status": plan.status,
                "last_error": plan.last_error or "",
                "started_at": plan.started_at.isoformat() if plan.started_at else None,
                "finished_at": plan.finished_at.isoformat() if plan.finished_at else None,
                "created_at": plan.created_at.isoformat() if plan.created_at else None,
                "updated_at": plan.updated_at.isoformat() if plan.updated_at else None,
                "plan_data": plan.plan_data or {},
                "validation": validation,
            },
        },
        status=200,
    )


@require_POST
def bootstrap_plan_run(request, plan_id):
    mode = (request.POST.get("mode") or "run").strip()
    dry_run = str(request.POST.get("dry_run") or "").strip().lower() in ("1", "true", "yes", "y")

    plan = BootstrapPlan.objects.filter(id=plan_id, is_enabled=True).first()
    if not plan:
        return JsonResponse(
            {"ok": False, "error": f"BootstrapPlan #{plan_id} not found"},
            status=404,
        )

    replay_last = False
    replay_last_failed = False
    retry_failed_only = False

    if mode == "run":
        pass
    elif mode == "replay_last":
        replay_last = True
    elif mode == "replay_last_failed":
        replay_last_failed = True
    elif mode == "replay_last_failed_only":
        replay_last_failed = True
        retry_failed_only = True
    else:
        return JsonResponse(
            {"ok": False, "error": f"Unsupported mode '{mode}'"},
            status=400,
        )

    try:
        result = run_bootstrap_plan_once(
            bootstrap_plan_id=plan.id,
            bootstrap_plan_name=plan.name,
            replay_last=replay_last,
            replay_last_failed=replay_last_failed,
            retry_failed_only=retry_failed_only,
            initiated_by=(
                f"dashboard:{request.user.username}"
                if request.user.is_authenticated
                else "dashboard:anonymous"
            ),
            dry_run_override=dry_run,
            progress_callback=None,
        )
    except Exception as exc:
        return JsonResponse(
            {"ok": False, "error": str(exc)},
            status=400,
        )

    return JsonResponse(
        {
            "ok": True,
            "plan_name": plan.name,
            "mode": mode,
            "result": result,
        },
        status=200,
    )

@require_GET
def api_ping(request):
    auth_error = check_api_token(request)
    if auth_error:
        return auth_error

    return JsonResponse(
        {
            "ok": True,
            "service": "srdf",
        },
        status=200,
    )


@require_GET
def api_status(request):
    auth_error = check_api_token(request)
    if auth_error:
        return auth_error

    data = build_global_status()
    return JsonResponse(data, status=200)


@csrf_exempt
@require_POST
def api_action(request):
    auth_error = check_api_token(request)
    if auth_error:
        return auth_error

    try:
        payload = json.loads(request.body.decode("utf-8") or "{}")
    except json.JSONDecodeError:
        return JsonResponse(
            {"ok": False, "error": "Invalid JSON body"},
            status=400,
        )

    action_name = (payload.get("action_name") or "").strip()
    node_id = payload.get("node_id")
    params = payload.get("params") or {}

    if not action_name:
        return JsonResponse(
            {"ok": False, "error": "Missing action_name"},
            status=400,
        )

    try:
        execution = run_srdf_action(
            action_name=action_name,
            node_id=node_id,
            params=params,
            initiated_by="srdf_api",
        )
    except Exception as exc:
        return JsonResponse(
            {"ok": False, "error": str(exc)},
            status=400,
        )

    return JsonResponse(
        {
            "ok": True,
            "execution_id": execution.id,
            "status": execution.status,
            "result_payload": execution.result_payload,
        },
        status=200,
    )