#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This file was generated with the customdashboard management command, it
contains the two classes for the main dashboard and app index dashboard.
You can customize these classes as you want.

To activate your index dashboard add the following to your settings.py::
    ADMIN_TOOLS_INDEX_DASHBOARD = 'medstat.dashboard.CustomIndexDashboard'

And to activate the app index dashboard::
    ADMIN_TOOLS_APP_INDEX_DASHBOARD = 'medstat.dashboard.CustomAppIndexDashboard'
"""

from django.apps import apps
from django.urls import reverse
from django.utils.html import format_html
from django.utils.html import format_html_join
from admin_tools.dashboard import modules
from django.http import JsonResponse


from srdf.services.model_migration_doctor import ModelMigrationDoctorService

from django.utils.translation import gettext_lazy as _
try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse

from admin_tools.dashboard import modules, Dashboard, AppIndexDashboard
from admin_tools.utils import get_admin_site_name


class VisibilityToggleModule(modules.DashboardModule):
    template = "admin/srdf/dashboard_visibility_toggle.html"

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("title", "")
        kwargs.setdefault("draggable", False)
        kwargs.setdefault("deletable", False)
        kwargs.setdefault("collapsible", False)
        super().__init__(*args, **kwargs)

    def init_with_context(self, context):
        request = context["request"]
        get_data = request.GET

        toggle_submitted = str(get_data.get("toggle_submitted", "")).strip()
        if toggle_submitted:
            self.show_non_empty_only = str(get_data.get("show_non_empty_only", "")).strip().lower() in (
                "1", "true", "yes", "on"
            )
        else:
            self.show_non_empty_only = True

class CountedAppList(modules.AppList):
    """
    AppList with SQL row count displayed before each model row.
    Rebuilds model rows from the real Django model classes.
    """
    
    
    def _build_admin_changelist_url(self, related_model, context):
        site_name = get_admin_site_name(context)
        return reverse(
            "%s:%s_%s_changelist" % (
                site_name,
                related_model._meta.app_label,
                related_model._meta.model_name,
            )
        )
    
    

    def _collect_related_model_entries(self, model, context, show_non_empty_only=False):
        related_entries = []
        seen = set()

        for field in model._meta.get_fields():
            if not getattr(field, "is_relation", False):
                continue

            related_model = getattr(field, "related_model", None)
            if related_model is None:
                continue

            if related_model == model:
                continue

            relation_label = ""
            if getattr(field, "many_to_many", False):
                relation_label = "M2M"
            elif getattr(field, "one_to_one", False):
                relation_label = "O2O"
            elif getattr(field, "many_to_one", False):
                relation_label = "FK"
            else:
                relation_label = "REL"

            if show_non_empty_only and relation_label != "FK":
                continue

            key = (
                related_model._meta.app_label,
                related_model._meta.model_name,
                relation_label,
            )
            if key in seen:
                continue

            try:
                related_count = int(related_model.objects.count())
            except Exception:
                related_count = -1

            if show_non_empty_only and related_count <= 0:
                continue

            seen.add(key)

            try:
                changelist_url = self._build_admin_changelist_url(related_model, context)
            except Exception:
                continue

            related_entries.append({
                "label": str(related_model._meta.verbose_name_plural or related_model._meta.model_name),
                "url": changelist_url,
                "relation_type": relation_label,
                "row_count": related_count,
            })

        related_entries.sort(
            key=lambda x: (
                0 if x["relation_type"] == "FK" else 1,
                x["label"].lower(),
            )
        )
        return related_entries
    
    
    def _collect_structure_entries(self, model):
        rows = []

        for field in model._meta.get_fields():
            if getattr(field, "auto_created", False) and not getattr(field, "concrete", False):
                continue

            field_name = str(getattr(field, "name", "") or "")
            internal_type = ""
            try:
                internal_type = str(field.get_internal_type() or "")
            except Exception:
                internal_type = field.__class__.__name__

            relation_type = ""
            if getattr(field, "many_to_many", False):
                relation_type = "M2M"
            elif getattr(field, "one_to_one", False):
                relation_type = "O2O"
            elif getattr(field, "many_to_one", False):
                relation_type = "FK"
            else:
                relation_type = "-"

            rows.append({
                "name": field_name,
                "type": internal_type,
                "null": bool(getattr(field, "null", False)),
                "blank": bool(getattr(field, "blank", False)),
                "pk": bool(getattr(field, "primary_key", False)),
                "unique": bool(getattr(field, "unique", False)),
                "relation": relation_type,
            })

        return rows   
    
    
    def _build_structure_modal_html(self, model):
        structure_rows = self._collect_structure_entries(model)
        if not structure_rows:
            return ""

        modal_id = "srdf-struct-{}-{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )

        rows_html = format_html_join(
            "",
            (
                '<tr>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;font-weight:bold;color:#234;">{}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;color:#355;">{}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;text-align:center;">{}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;text-align:center;">{}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;text-align:center;">{}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;text-align:center;">{}</td>'
                '<td style="padding:6px 8px;border-bottom:1px solid #edf2f7;text-align:center;color:#607d8b;font-weight:bold;">{}</td>'
                '</tr>'
            ),
            (
                (
                    row["name"],
                    row["type"],
                    "Y" if row["null"] else "-",
                    "Y" if row["blank"] else "-",
                    "Y" if row["pk"] else "-",
                    "Y" if row["unique"] else "-",
                    row["relation"],
                )
                for row in structure_rows
            ),
        )

        return format_html(
            '<span class="srdf-structure-wrap" style="display:inline-block;margin-right:6px;">'
            '<button type="button" '
            ' title="Structure" '
            'class="srdf-structure-button" '
            'data-target-id="{}" '
            'style="cursor:pointer;display:inline-block;'
            'background:#f5efe0;border:1px solid #d6c28d;border-radius:5px;'
            'padding:2px 8px;font-size:11px;font-weight:bold;color:#6b5b22;vertical-align:middle;">'
            '▦'
            '</button>'
            '</span>'

            '<div id="{}" class="srdf-structure-modal" '
            'style="display:none;position:fixed;left:0;top:0;right:0;bottom:0;z-index:100001;'
            'background:rgba(20,30,40,0.18);">'
                '<div class="srdf-structure-modal-box" '
                'style="position:absolute;left:50%;top:70px;transform:translateX(-50%);'
                'width:900px;max-width:96vw;max-height:82vh;overflow:auto;'
                'background:#fff;border:1px solid #b8c7d3;border-radius:8px;'
                'box-shadow:0 10px 28px rgba(0,0,0,0.22);">'
                    '<div style="padding:10px 14px;border-bottom:1px solid #e6edf2;'
                    'background:#f8fafc;display:flex;justify-content:space-between;align-items:center;">'
                        '<div>'
                            '<div style="font-size:12px;color:#607d8b;font-weight:bold;">Data Model Structure</div>'
                            '<div style="font-size:18px;color:#234;font-weight:bold;">{}.{}</div>'
                        '</div>'
                        '<button type="button" '
                        'class="srdf-structure-close" '
                        'data-target-id="{}" '
                        'style="cursor:pointer;background:#fff;border:1px solid #ccd7e2;'
                        'border-radius:5px;padding:4px 10px;font-weight:bold;color:#456;">X</button>'
                    '</div>'

                    '<div style="padding:12px 14px;">'
                        '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
                            '<thead>'
                                '<tr style="background:#f3f7fb;">'
                                    '<th style="text-align:left;padding:7px 8px;border-bottom:1px solid #dfe7ef;">Field</th>'
                                    '<th style="text-align:left;padding:7px 8px;border-bottom:1px solid #dfe7ef;">Type</th>'
                                    '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid #dfe7ef;">Null</th>'
                                    '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid #dfe7ef;">Blank</th>'
                                    '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid #dfe7ef;">PK</th>'
                                    '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid #dfe7ef;">Unique</th>'
                                    '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid #dfe7ef;">Relation</th>'
                                '</tr>'
                            '</thead>'
                            '<tbody>{}</tbody>'
                        '</table>'
                    '</div>'
                '</div>'
            '</div>'

            '<script>'
            '(function(){{'
            'if (window.__srdfStructureModalInitDone) return;'
            'window.__srdfStructureModalInitDone = true;'

            'function closeAllStructureModals(exceptId){{'
            '  var modals = document.querySelectorAll(".srdf-structure-modal");'
            '  for (var i = 0; i < modals.length; i++){{'
            '    if (!exceptId || modals[i].id !== exceptId){{'
            '      modals[i].style.display = "none";'
            '    }}'
            '  }}'
            '}}'

            'document.addEventListener("click", function(ev){{'
            '  var openButton = ev.target.closest(".srdf-structure-button");'
            '  if (openButton){{'
            '    ev.preventDefault();'
            '    ev.stopPropagation();'
            '    var targetId = openButton.getAttribute("data-target-id");'
            '    var modal = document.getElementById(targetId);'
            '    if (!modal) return;'
            '    var isOpen = modal.style.display === "block";'
            '    closeAllStructureModals();'
            '    modal.style.display = isOpen ? "none" : "block";'
            '    return;'
            '  }}'

            '  var closeButton = ev.target.closest(".srdf-structure-close");'
            '  if (closeButton){{'
            '    ev.preventDefault();'
            '    var closeId = closeButton.getAttribute("data-target-id");'
            '    var closeModal = document.getElementById(closeId);'
            '    if (closeModal) closeModal.style.display = "none";'
            '    return;'
            '  }}'

            '  var modalBg = ev.target.closest(".srdf-structure-modal");'
            '  if (modalBg && ev.target === modalBg){{'
            '    modalBg.style.display = "none";'
            '    return;'
            '  }}'
            '}});'

            'document.addEventListener("keydown", function(ev){{'
            '  if (ev.key === "Escape"){{'
            '    closeAllStructureModals();'
            '  }}'
            '}});'
            '}})();'
            '</script>',
            modal_id,
            modal_id,
            model._meta.app_label,
            model._meta.model_name,
            modal_id,
            rows_html,
        )  
    
    
    def _collect_relation_diagram_entries(self, model):
        entries = []
        seen = set()

        current_model_label = "{}.{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )

        for field in model._meta.get_fields():
            if not getattr(field, "is_relation", False):
                continue

            related_model = getattr(field, "related_model", None)
            if related_model is None:
                continue

            if related_model == model:
                continue

            relation_type = "REL"
            if getattr(field, "many_to_many", False):
                relation_type = "M2M"
            elif getattr(field, "one_to_one", False):
                relation_type = "O2O"
            elif getattr(field, "many_to_one", False):
                relation_type = "FK"

            related_model_label = "{}.{}".format(
                related_model._meta.app_label,
                related_model._meta.model_name,
            )

            field_name = str(getattr(field, "name", "") or "")
            auto_created = bool(getattr(field, "auto_created", False))
            concrete = bool(getattr(field, "concrete", False))

            if concrete:
                left_side = "{}.{}".format(current_model_label, field_name)
                right_side = "{}.id".format(related_model_label)
            else:
                left_side = "{}.{}".format(related_model_label, field_name)
                right_side = "{}.id".format(current_model_label)

            key = (relation_type, left_side, right_side)
            if key in seen:
                continue
            seen.add(key)

            entries.append({
                "relation_type": relation_type,
                "left_side": left_side,
                "right_side": right_side,
                "related_model_label": related_model_label,
                "field_name": field_name,
                "auto_created": auto_created,
            })

        entries.sort(
            key=lambda x: (
                0 if x["relation_type"] == "FK" else 1,
                x["related_model_label"].lower(),
                x["field_name"].lower(),
            )
        )
        return entries 
    
    
    def _build_relation_diagram_modal_html(self, model):
        entries = self._collect_relation_diagram_entries(model)
        if not entries:
            return ""
    
        modal_id = "srdf-rel-graph-{}-{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )
    
        current_model_label = "{}.{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )
    
        canvas_width = 920
        box_w = 240
        box_h = 68
        center_x = 340
        center_y = 34
        left_x = 36
        right_x = 644
        start_y = 150
        step_y = 86
    
        positioned_entries = []
        for idx, row in enumerate(entries):
            side = "left" if idx % 2 == 0 else "right"
            x = left_x if side == "left" else right_x
            y = start_y + (idx // 2) * step_y if len(entries) > 1 else start_y
    
            relation_type = row["relation_type"]
            if relation_type == "FK":
                stroke = "#29b6f6"
                glow = "rgba(41,182,246,0.45)"
                badge_bg = "#173a52"
                badge_fg = "#8fe3ff"
            elif relation_type == "O2O":
                stroke = "#ab47bc"
                glow = "rgba(171,71,188,0.45)"
                badge_bg = "#442050"
                badge_fg = "#f0b6ff"
            elif relation_type == "M2M":
                stroke = "#ffca28"
                glow = "rgba(255,202,40,0.45)"
                badge_bg = "#5c4712"
                badge_fg = "#ffe28a"
            else:
                stroke = "#66bb6a"
                glow = "rgba(102,187,106,0.45)"
                badge_bg = "#1f4a2a"
                badge_fg = "#b8f5be"
    
            if side == "left":
                x1 = x + box_w
                y1 = y + (box_h // 2)
                x2 = center_x
                y2 = center_y + (box_h // 2)
                mid_x = x1 + 30
                path_d = "M {x1} {y1} L {mx} {y1} L {mx} {y2} L {x2} {y2}".format(
                    x1=x1,
                    y1=y1,
                    mx=mid_x,
                    y2=y2,
                    x2=x2,
                )
                arrow_x1 = center_x - 14
                arrow_y1 = y2 - 8
                arrow_x2 = center_x
                arrow_y2 = y2
                arrow_x3 = center_x - 14
                arrow_y3 = y2 + 8
            else:
                x1 = x
                y1 = y + (box_h // 2)
                x2 = center_x + box_w
                y2 = center_y + (box_h // 2)
                mid_x = x1 - 30
                path_d = "M {x1} {y1} L {mx} {y1} L {mx} {y2} L {x2} {y2}".format(
                    x1=x1,
                    y1=y1,
                    mx=mid_x,
                    y2=y2,
                    x2=x2,
                )
                arrow_x1 = center_x + box_w + 14
                arrow_y1 = y2 - 8
                arrow_x2 = center_x + box_w
                arrow_y2 = y2
                arrow_x3 = center_x + box_w + 14
                arrow_y3 = y2 + 8
    
            positioned_entries.append({
                "side": side,
                "x": x,
                "y": y,
                "stroke": stroke,
                "glow": glow,
                "badge_bg": badge_bg,
                "badge_fg": badge_fg,
                "path_d": path_d,
                "arrow_points": "{},{}, {},{}, {},{}".format(
                    arrow_x1, arrow_y1,
                    arrow_x2, arrow_y2,
                    arrow_x3, arrow_y3,
                ),
                "relation_type": relation_type,
                "related_model_label": row["related_model_label"],
                "field_name": row["field_name"],
                "left_side": row["left_side"],
                "right_side": row["right_side"],
            })
    
        canvas_height = max(360, start_y + ((max(1, (len(entries) + 1) // 2) - 1) * step_y) + 120)
    
        svg_paths_html = format_html_join(
            "",
            (
                '<g>'
                '<path d="{}" fill="none" stroke="{}" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round" />'
                '<path d="{}" fill="none" stroke="{}" stroke-width="7" stroke-linecap="round" stroke-linejoin="round" opacity="0.14" />'
                '<circle cx="{}" cy="{}" r="3.5" fill="{}" />'
                '<polygon points="{}" fill="{}" />'
                '</g>'
            ),
            (
                (
                    row["path_d"],
                    row["stroke"],
                    row["path_d"],
                    row["glow"],
                    center_x if row["side"] == "left" else center_x + box_w,
                    center_y + (box_h // 2),
                    row["stroke"],
                    row["arrow_points"],
                    row["stroke"],
                )
                for row in positioned_entries
            ),
        )
    
        relation_boxes_html = format_html_join(
            "",
            (
                '<div style="position:absolute;left:{}px;top:{}px;width:{}px;height:{}px;'
                'background:linear-gradient(180deg,#212a57 0%,#1c2548 100%);'
                'border:1px solid rgba(156,181,255,0.22);border-radius:10px;'
                'box-shadow:0 8px 24px rgba(0,0,0,0.22), inset 0 0 0 1px rgba(255,255,255,0.02);'
                'padding:10px 12px;box-sizing:border-box;color:#dfe8ff;">'
                    '<div style="display:flex;align-items:center;justify-content:space-between;gap:8px;">'
                        '<span style="display:inline-block;padding:2px 7px;border-radius:999px;'
                        'background:{};color:{};font-size:10px;font-weight:bold;">{}</span>'
                        '<span style="font-size:10px;color:#8ea0d6;">FK field</span>'
                    '</div>'
                    '<div style="margin-top:8px;font-size:13px;font-weight:bold;color:#f2f6ff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{}</div>'
                    '<div style="margin-top:6px;font-size:11px;color:#9fb1e5;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{}</div>'
                '</div>'
            ),
            (
                (
                    row["x"],
                    row["y"],
                    box_w,
                    box_h,
                    row["badge_bg"],
                    row["badge_fg"],
                    row["relation_type"],
                    row["related_model_label"],
                    row["field_name"],
                )
                for row in positioned_entries
            ),
        )
    
        connected_tables_html = format_html_join(
            "",
            (
                '<div style="display:flex;align-items:center;justify-content:space-between;'
                'gap:12px;margin:0 0 10px 0;padding:10px 12px;'
                'background:linear-gradient(180deg,#121a3c 0%,#101733 100%);'
                'border:1px solid rgba(156,181,255,0.16);border-radius:8px;'
                'box-shadow:0 2px 6px rgba(0,0,0,0.12);">'
                    '<div style="display:flex;align-items:center;gap:10px;">'
                        '<span style="display:inline-block;min-width:38px;text-align:center;'
                        'background:{};color:{};border-radius:5px;'
                        'padding:3px 6px;font-size:11px;font-weight:bold;">{}</span>'
                        '<div style="font-size:13px;color:#e8efff;font-weight:bold;">{}</div>'
                    '</div>'
                    '<div style="font-size:11px;color:#8ea0d6;font-weight:bold;">{}</div>'
                '</div>'
            ),
            (
                (
                    row["badge_bg"],
                    row["badge_fg"],
                    row["relation_type"],
                    row["related_model_label"],
                    row["field_name"],
                )
                for row in positioned_entries
            ),
        )
    
        mapping_cards_html = format_html_join(
            "",
            (
                '<div style="margin-bottom:10px;padding:10px 12px;'
                'background:linear-gradient(180deg,#131b3f 0%,#0f1533 100%);'
                'border:1px solid rgba(143,176,209,0.20);border-radius:9px;">'
                    '<div style="display:flex;align-items:center;gap:8px;margin-bottom:7px;">'
                        '<span style="display:inline-block;padding:2px 7px;border-radius:999px;'
                        'background:{};color:{};font-size:10px;font-weight:bold;">{}</span>'
                        '<span style="font-size:11px;color:#95a8df;font-weight:bold;">PK &lt;-&gt; FK</span>'
                    '</div>'
                    '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
                        '<div style="padding:8px 10px;background:#172247;border:1px solid rgba(88,138,255,0.25);'
                        'border-radius:7px;color:#e3ecff;font-size:12px;font-weight:bold;">{}</div>'
                        '<div style="font-size:18px;color:{};font-weight:bold;">&#8594;</div>'
                        '<div style="padding:8px 10px;background:#231a12;border:1px solid rgba(255,202,40,0.22);'
                        'border-radius:7px;color:#ffe7a4;font-size:12px;font-weight:bold;">{}</div>'
                    '</div>'
                '</div>'
            ),
            (
                (
                    row["badge_bg"],
                    row["badge_fg"],
                    row["relation_type"],
                    row["left_side"],
                    row["stroke"],
                    row["right_side"],
                )
                for row in positioned_entries
            ),
        )
    
        use_tabbed_layout = len(positioned_entries) >= 8
    
        tab_menu_html = ""
        tab_body_html = ""
    
        if use_tabbed_layout:
            tab_menu_html = format_html(
                '<div class="srdf-rel-tabs" style="display:flex;gap:8px;padding:0 18px 14px 18px;">'
                    '<button type="button" class="srdf-rel-tab-button is-active" data-tab-target="{}-diagram" '
                    'style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);'
                    'background:#1a2552;color:#e6efff;font-size:12px;font-weight:bold;">Diagram</button>'
                    '<button type="button" class="srdf-rel-tab-button" data-tab-target="{}-tables" '
                    'style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);'
                    'background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Connected tables</button>'
                    '<button type="button" class="srdf-rel-tab-button" data-tab-target="{}-mapping" '
                    'style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);'
                    'background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">PK &lt;-&gt; FK mapping</button>'
                '</div>',
                modal_id,
                modal_id,
                modal_id,
            )
    
            tab_body_html = format_html(
                '<div style="padding:0 18px 18px 18px;">'
    
                    '<div id="{}-diagram" class="srdf-rel-tab-panel" style="display:block;">'
                        '<div style="position:relative;width:{}px;height:{}px;max-width:100%;'
                        'margin:0 auto;border-radius:14px;overflow:auto;'
                        'background:radial-gradient(circle at top center, rgba(63,81,181,0.18) 0%, rgba(10,16,40,0.00) 38%),'
                        'linear-gradient(180deg,#0c1330 0%,#0a1028 100%);'
                        'border:1px solid rgba(115,138,220,0.12);">'
                            '<svg width="{}" height="{}" viewBox="0 0 {} {}" style="position:absolute;left:0;top:0;">'
                                '<defs>'
                                    '<pattern id="srdf-grid-{}" width="28" height="28" patternUnits="userSpaceOnUse">'
                                        '<path d="M 28 0 L 0 0 0 28" fill="none" stroke="rgba(120,140,210,0.07)" stroke-width="1" />'
                                    '</pattern>'
                                '</defs>'
                                '<rect x="0" y="0" width="{}" height="{}" fill="url(#srdf-grid-{})" />'
                                '{}'
                            '</svg>'
    
                            '<div style="position:absolute;left:{}px;top:{}px;width:{}px;height:{}px;'
                            'background:linear-gradient(180deg,#24346f 0%,#1b2755 100%);'
                            'border:1px solid rgba(120,170,255,0.26);border-radius:12px;'
                            'box-shadow:0 10px 26px rgba(0,0,0,0.22), 0 0 24px rgba(41,182,246,0.08);'
                            'padding:12px 14px;box-sizing:border-box;color:#eaf2ff;text-align:center;">'
                                '<div style="font-size:11px;color:#a8bbef;font-weight:bold;">CURRENT DATA MODEL</div>'
                                '<div style="margin-top:7px;font-size:18px;font-weight:bold;color:#ffffff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{}</div>'
                                '<div style="margin-top:8px;font-size:11px;color:#9ec7ff;">Primary key anchor</div>'
                            '</div>'
    
                            '{}'
                        '</div>'
                    '</div>'
    
                    '<div id="{}-tables" class="srdf-rel-tab-panel" style="display:none;">'
                        '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);'
                        'border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Connected tables</div>'
                            '{}'
                        '</div>'
                    '</div>'
    
                    '<div id="{}-mapping" class="srdf-rel-tab-panel" style="display:none;">'
                        '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);'
                        'border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">PK &lt;-&gt; FK mapping</div>'
                            '{}'
                        '</div>'
                    '</div>'
    
                '</div>',
                modal_id,
                canvas_width,
                canvas_height,
                canvas_width,
                canvas_height,
                canvas_width,
                canvas_height,
                modal_id,
                canvas_width,
                canvas_height,
                modal_id,
                svg_paths_html,
                center_x,
                center_y,
                box_w,
                box_h,
                current_model_label,
                relation_boxes_html,
                modal_id,
                connected_tables_html,
                modal_id,
                mapping_cards_html,
            )
        else:
            tab_body_html = format_html(
                '<div style="padding:18px;">'
    
                    '<div style="position:relative;width:{}px;height:{}px;max-width:100%;'
                    'margin:0 auto 18px auto;border-radius:14px;overflow:hidden;'
                    'background:radial-gradient(circle at top center, rgba(63,81,181,0.18) 0%, rgba(10,16,40,0.00) 38%),'
                    'linear-gradient(180deg,#0c1330 0%,#0a1028 100%);'
                    'border:1px solid rgba(115,138,220,0.12);">'
    
                        '<svg width="{}" height="{}" viewBox="0 0 {} {}" style="position:absolute;left:0;top:0;">'
                            '<defs>'
                                '<pattern id="srdf-grid-{}" width="28" height="28" patternUnits="userSpaceOnUse">'
                                    '<path d="M 28 0 L 0 0 0 28" fill="none" stroke="rgba(120,140,210,0.07)" stroke-width="1" />'
                                '</pattern>'
                            '</defs>'
                            '<rect x="0" y="0" width="{}" height="{}" fill="url(#srdf-grid-{})" />'
                            '{}'
                        '</svg>'
    
                        '<div style="position:absolute;left:{}px;top:{}px;width:{}px;height:{}px;'
                        'background:linear-gradient(180deg,#24346f 0%,#1b2755 100%);'
                        'border:1px solid rgba(120,170,255,0.26);border-radius:12px;'
                        'box-shadow:0 10px 26px rgba(0,0,0,0.22), 0 0 24px rgba(41,182,246,0.08);'
                        'padding:12px 14px;box-sizing:border-box;color:#eaf2ff;text-align:center;">'
                            '<div style="font-size:11px;color:#a8bbef;font-weight:bold;">CURRENT DATA MODEL</div>'
                            '<div style="margin-top:7px;font-size:18px;font-weight:bold;color:#ffffff;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">{}</div>'
                            '<div style="margin-top:8px;font-size:11px;color:#9ec7ff;">Primary key anchor</div>'
                        '</div>'
    
                        '{}'
                    '</div>'
    
                    '<div style="display:grid;grid-template-columns:1fr 1fr;gap:18px;">'
                        '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);'
                        'border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Connected tables</div>'
                            '{}'
                        '</div>'
    
                        '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);'
                        'border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">PK &lt;-&gt; FK mapping</div>'
                            '{}'
                        '</div>'
                    '</div>'
    
                '</div>',
                canvas_width,
                canvas_height,
                canvas_width,
                canvas_height,
                canvas_width,
                canvas_height,
                modal_id,
                canvas_width,
                canvas_height,
                modal_id,
                svg_paths_html,
                center_x,
                center_y,
                box_w,
                box_h,
                current_model_label,
                relation_boxes_html,
                connected_tables_html,
                mapping_cards_html,
            )
    
        return format_html(
            '<span class="srdf-relgraph-wrap" style="display:inline-block;margin-right:6px;">'
            '<button type="button" '
            'class="srdf-relgraph-button" '
            'data-target-id="{}" '
            'style="cursor:pointer;display:inline-block;'
            'background:#efe8fa;border:1px solid #c5b1e5;border-radius:5px;'
            'padding:2px 8px;font-size:11px;font-weight:bold;color:#5b3d8a;vertical-align:middle;">'
            'REL'
            '</button>'
            '</span>'
    
            '<div id="{}" class="srdf-relgraph-modal" '
            'style="display:none;position:fixed;left:0;top:0;right:0;bottom:0;z-index:100002;'
            'background:rgba(6,10,26,0.58);backdrop-filter:blur(2px);">'
                '<div style="position:absolute;left:50%;top:38px;transform:translateX(-50%);'
                'width:1120px;max-width:97vw;max-height:90vh;overflow:auto;'
                'background:linear-gradient(180deg,#0d1433 0%,#0a1028 100%);'
                'border:1px solid rgba(120,146,255,0.16);border-radius:12px;'
                'box-shadow:0 18px 44px rgba(0,0,0,0.36);">'
    
                    '<div style="padding:14px 18px;border-bottom:1px solid rgba(150,170,255,0.10);'
                    'background:linear-gradient(180deg,#11193f 0%,#0d1433 100%);'
                    'display:flex;justify-content:space-between;align-items:center;">'
                        '<div>'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;letter-spacing:0.3px;">Neural Relation Diagram</div>'
                            '<div style="font-size:20px;color:#f3f7ff;font-weight:bold;">{}</div>'
                        '</div>'
                        '<button type="button" '
                        'class="srdf-relgraph-close" '
                        'data-target-id="{}" '
                        'style="cursor:pointer;background:#121a3c;border:1px solid rgba(182,197,255,0.20);'
                        'border-radius:6px;padding:5px 11px;font-weight:bold;color:#d8e3ff;">X</button>'
                    '</div>'
    
                    '{}'
                    '{}'
    
                '</div>'
            '</div>'
    
            '<script>'
            '(function(){{'
            'if (window.__srdfRelGraphInitDone) return;'
            'window.__srdfRelGraphInitDone = true;'
    
            'function closeAllRelGraphModals(exceptId){{'
            '  var modals = document.querySelectorAll(".srdf-relgraph-modal");'
            '  for (var i = 0; i < modals.length; i++){{'
            '    if (!exceptId || modals[i].id !== exceptId){{'
            '      modals[i].style.display = "none";'
            '    }}'
            '  }}'
            '}}'
    
            'function activateRelTab(button){{'
            '  var modal = button.closest(".srdf-relgraph-modal");'
            '  if (!modal) return;'
            '  var targetId = button.getAttribute("data-tab-target");'
            '  var buttons = modal.querySelectorAll(".srdf-rel-tab-button");'
            '  var panels = modal.querySelectorAll(".srdf-rel-tab-panel");'
    
            '  for (var i = 0; i < buttons.length; i++){{'
            '    buttons[i].classList.remove("is-active");'
            '    buttons[i].style.background = "#0f1738";'
            '    buttons[i].style.color = "#91a3d8";'
            '  }}'
    
            '  for (var j = 0; j < panels.length; j++){{'
            '    panels[j].style.display = "none";'
            '  }}'
    
            '  button.classList.add("is-active");'
            '  button.style.background = "#1a2552";'
            '  button.style.color = "#e6efff";'
    
            '  var panel = modal.querySelector("#" + targetId);'
            '  if (panel){{'
            '    panel.style.display = "block";'
            '  }}'
            '}}'
    
            'document.addEventListener("click", function(ev){{'
            '  var openButton = ev.target.closest(".srdf-relgraph-button");'
            '  if (openButton){{'
            '    ev.preventDefault();'
            '    ev.stopPropagation();'
            '    var targetId = openButton.getAttribute("data-target-id");'
            '    var modal = document.getElementById(targetId);'
            '    if (!modal) return;'
            '    var isOpen = modal.style.display === "block";'
            '    closeAllRelGraphModals();'
            '    modal.style.display = isOpen ? "none" : "block";'
            '    return;'
            '  }}'
    
            '  var tabButton = ev.target.closest(".srdf-rel-tab-button");'
            '  if (tabButton){{'
            '    ev.preventDefault();'
            '    activateRelTab(tabButton);'
            '    return;'
            '  }}'
    
            '  var closeButton = ev.target.closest(".srdf-relgraph-close");'
            '  if (closeButton){{'
            '    ev.preventDefault();'
            '    var closeId = closeButton.getAttribute("data-target-id");'
            '    var closeModal = document.getElementById(closeId);'
            '    if (closeModal) closeModal.style.display = "none";'
            '    return;'
            '  }}'
    
            '  var modalBg = ev.target.closest(".srdf-relgraph-modal");'
            '  if (modalBg && ev.target === modalBg){{'
            '    modalBg.style.display = "none";'
            '    return;'
            '  }}'
            '}});'
    
            'document.addEventListener("keydown", function(ev){{'
            '  if (ev.key === "Escape"){{'
            '    closeAllRelGraphModals();'
            '  }}'
            '}});'
            '}})();'
            '</script>',
            modal_id,
            modal_id,
            current_model_label,
            modal_id,
            tab_menu_html,
            tab_body_html,
        )    
    
    
    def _collect_doctor_summary(self, model):
        service = ModelMigrationDoctorService(model=model, using="default")
        return service.analyze()    
    
    
    def _build_doctor_modal_html(self, model):
        doctor = self._collect_doctor_summary(model)
        modal_id = "srdf-doctor-{}-{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )
    
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
            '<tr>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);color:#f2f6ff;font-weight:bold;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);color:#b9c8ef;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#b9c8ef;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#b9c8ef;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#b9c8ef;">{}</td>'
            '<td style="padding:6px 8px;border-bottom:1px solid rgba(143,176,209,0.12);text-align:center;color:#8fe3ff;font-weight:bold;">{}</td>'
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
                '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                    '<div style="font-size:12px;color:#91a3d8;font-weight:bold;margin-bottom:10px;">Table status</div>'
                    '<div style="margin-bottom:8px;color:#e6efff;">Exists: <strong>{}</strong></div>'
                    '<div style="margin-bottom:8px;color:#e6efff;">DB columns: <strong>{}</strong></div>'
                    '<div style="margin-bottom:8px;color:#e6efff;">PK columns: <strong>{}</strong></div>'
                    '<div style="margin-bottom:8px;color:#e6efff;">Unique columns: <strong>{}</strong></div>'
                    '<div style="margin-bottom:8px;color:#e6efff;">FK columns: <strong>{}</strong></div>'
                '</div>'
                '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
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
    
        impact_html = format_html(
            '<div style="margin-bottom:14px;padding:12px 14px;background:linear-gradient(180deg,#121a3c 0%,#101733 100%);'
            'border:1px solid rgba(143,176,209,0.16);border-radius:10px;">'
                '<div style="font-size:12px;color:#91a3d8;font-weight:bold;">Migration impact</div>'
                '<div style="margin-top:8px;color:#f2f6ff;">Impact level: <strong>{}</strong></div>'
                '<div style="margin-top:4px;color:#f2f6ff;">Impact score: <strong>{}</strong></div>'
            '</div>',
            doctor["migration_impact"]["impact_level"],
            doctor["migration_impact"]["impact_score"],
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
    
        return format_html(
            '<span class="srdf-doctor-wrap" style="display:inline-block;margin-right:4px;">'
                '<button type="button" class="srdf-doctor-button" data-target-id="{modal_id}" title="Migration Doctor" '
                'style="cursor:pointer;display:inline-block;background:#fbe9d7;border:1px solid #dcb98f;'
                'border-radius:5px;padding:2px 7px;font-size:11px;font-weight:bold;color:#7a4b1f;vertical-align:middle;">'
                'DOC'
                '</button>'
            '</span>'
    
            '<div id="{modal_id}" class="srdf-doctor-modal" '
            'style="display:none;position:fixed;left:0;top:0;right:0;bottom:0;z-index:100003;'
            'background:rgba(6,10,26,0.62);backdrop-filter:blur(2px);">'
                '<div style="position:absolute;left:50%;top:34px;transform:translateX(-50%);'
                'width:1180px;max-width:97vw;max-height:91vh;overflow:auto;'
                'background:linear-gradient(180deg,#0d1433 0%,#0a1028 100%);'
                'border:1px solid rgba(120,146,255,0.16);border-radius:12px;'
                'box-shadow:0 18px 44px rgba(0,0,0,0.36);">'
    
                    '<div style="padding:14px 18px;border-bottom:1px solid rgba(150,170,255,0.10);'
                    'background:linear-gradient(180deg,#11193f 0%,#0d1433 100%);'
                    'display:flex;justify-content:space-between;align-items:center;">'
                        '<div>'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;letter-spacing:0.3px;">Migration Doctor – Technical Model Audit</div>'
                            '<div style="font-size:20px;color:#f3f7ff;font-weight:bold;">{app_label}.{model_name}</div>'
                            '<div style="margin-top:4px;font-size:11px;color:#9fb1e5;">table: {db_table} | db alias: {db_alias}</div>'
                        '</div>'
                        '<div style="display:flex;align-items:center;gap:12px;">'
                            '<span style="display:inline-block;padding:5px 12px;border-radius:999px;background:{status_bg};color:{status_color};font-size:12px;font-weight:bold;">{severity}</span>'
                            '<button type="button" class="srdf-doctor-close" data-target-id="{modal_id}" '
                            'style="cursor:pointer;background:#121a3c;border:1px solid rgba(182,197,255,0.20);'
                            'border-radius:6px;padding:5px 11px;font-weight:bold;color:#d8e3ff;">X</button>'
                        '</div>'
                    '</div>'
    
                    '<div class="srdf-doctor-tabs" style="display:flex;gap:8px;padding:16px 18px 0 18px;">'
                        '<button type="button" class="srdf-doctor-tab-button is-active" data-tab-target="{modal_id}-overview" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#1a2552;color:#e6efff;font-size:12px;font-weight:bold;">Overview</button>'
                        '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}-structure" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Structure</button>'
                        '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}-db" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">DB Objects</button>'
                        '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}-relations" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Relations</button>'
                        '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}-risks" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Index Risks</button>'
                        '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}-impact" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Migration Impact</button>'
                        '<button type="button" class="srdf-doctor-tab-button" data-tab-target="{modal_id}-repair" style="cursor:pointer;padding:8px 14px;border-radius:8px;border:1px solid rgba(143,176,209,0.18);background:#0f1738;color:#91a3d8;font-size:12px;font-weight:bold;">Repair Plan</button>'
                    '</div>'
    
                    '<div style="padding:18px;">'
                        '<div id="{modal_id}-overview" class="srdf-doctor-tab-panel" style="display:block;">{overview_panel_html}</div>'
    
                        '<div id="{modal_id}-structure" class="srdf-doctor-tab-panel" style="display:none;">'
                            '<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">'
                                '<table style="width:100%;border-collapse:collapse;font-size:12px;">'
                                    '<thead><tr style="background:#15204a;">'
                                        '<th style="text-align:left;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#e6efff;">Field</th>'
                                        '<th style="text-align:left;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#e6efff;">Type</th>'
                                        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#e6efff;">Null</th>'
                                        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#e6efff;">PK</th>'
                                        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#e6efff;">Unique</th>'
                                        '<th style="text-align:center;padding:7px 8px;border-bottom:1px solid rgba(143,176,209,0.16);color:#e6efff;">Relation</th>'
                                    '</tr></thead>'
                                    '<tbody>{structure_html}</tbody>'
                                '</table>'
                            '</div>'
                        '</div>'
    
                        '<div id="{modal_id}-db" class="srdf-doctor-tab-panel" style="display:none;">{db_objects_html}</div>'
                        '<div id="{modal_id}-relations" class="srdf-doctor-tab-panel" style="display:none;"><div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{relation_html}</div></div>'
                        '<div id="{modal_id}-risks" class="srdf-doctor-tab-panel" style="display:none;"><div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{risks_html}</div></div>'
                        '<div id="{modal_id}-impact" class="srdf-doctor-tab-panel" style="display:none;">{impact_html}<div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{impact_notes_html}</div></div>'
                        '<div id="{modal_id}-repair" class="srdf-doctor-tab-panel" style="display:none;"><div style="padding:14px;border:1px solid rgba(143,176,209,0.16);border-radius:10px;background:linear-gradient(180deg,#0f1738 0%,#0c122d 100%);">{repair_plan_html}</div></div>'
                    '</div>'
                '</div>'
            '</div>'
    
            '<script>'
            '(function(){{'
            'if (window.__srdfDoctorInitDone) return;'
            'window.__srdfDoctorInitDone = true;'
    
            'function closeAllDoctorModals(exceptId){{'
            '  var modals = document.querySelectorAll(".srdf-doctor-modal");'
            '  for (var i = 0; i < modals.length; i++){{'
            '    if (!exceptId || modals[i].id !== exceptId){{'
            '      modals[i].style.display = "none";'
            '    }}'
            '  }}'
            '}}'
    
            'function activateDoctorTab(button){{'
            '  var modal = button.closest(".srdf-doctor-modal");'
            '  if (!modal) return;'
            '  var targetId = button.getAttribute("data-tab-target");'
            '  var buttons = modal.querySelectorAll(".srdf-doctor-tab-button");'
            '  var panels = modal.querySelectorAll(".srdf-doctor-tab-panel");'
    
            '  for (var i = 0; i < buttons.length; i++){{'
            '    buttons[i].classList.remove("is-active");'
            '    buttons[i].style.background = "#0f1738";'
            '    buttons[i].style.color = "#91a3d8";'
            '  }}'
    
            '  for (var j = 0; j < panels.length; j++){{'
            '    panels[j].style.display = "none";'
            '  }}'
    
            '  button.classList.add("is-active");'
            '  button.style.background = "#1a2552";'
            '  button.style.color = "#e6efff";'
    
            '  var panel = modal.querySelector("#" + targetId);'
            '  if (panel){{'
            '    panel.style.display = "block";'
            '  }}'
            '}}'
    
            'document.addEventListener("click", function(ev){{'
            '  var openButton = ev.target.closest(".srdf-doctor-button");'
            '  if (openButton){{'
            '    ev.preventDefault();'
            '    ev.stopPropagation();'
            '    var targetId = openButton.getAttribute("data-target-id");'
            '    var modal = document.getElementById(targetId);'
            '    if (!modal) return;'
            '    var isOpen = modal.style.display === "block";'
            '    closeAllDoctorModals();'
            '    modal.style.display = isOpen ? "none" : "block";'
            '    return;'
            '  }}'
    
            '  var tabButton = ev.target.closest(".srdf-doctor-tab-button");'
            '  if (tabButton){{'
            '    ev.preventDefault();'
            '    activateDoctorTab(tabButton);'
            '    return;'
            '  }}'
    
            '  var closeButton = ev.target.closest(".srdf-doctor-close");'
            '  if (closeButton){{'
            '    ev.preventDefault();'
            '    var closeId = closeButton.getAttribute("data-target-id");'
            '    var closeModal = document.getElementById(closeId);'
            '    if (closeModal) closeModal.style.display = "none";'
            '    return;'
            '  }}'
    
            '  var modalBg = ev.target.closest(".srdf-doctor-modal");'
            '  if (modalBg && ev.target === modalBg){{'
            '    modalBg.style.display = "none";'
            '    return;'
            '  }}'
            '}});'
    
            'document.addEventListener("keydown", function(ev){{'
            '  if (ev.key === "Escape"){{'
            '    closeAllDoctorModals();'
            '  }}'
            '}});'
            '}})();'
            '</script>',
            modal_id=modal_id,
            app_label=doctor["app_label"],
            model_name=doctor["model_name"],
            db_table=doctor["db_table"],
            db_alias=doctor["database_alias"],
            status_bg=status_bg,
            status_color=status_color,
            severity=severity,
            overview_panel_html=overview_panel_html,
            structure_html=structure_html,
            db_objects_html=db_objects_html,
            relation_html=relation_html,
            risks_html=risks_html,
            impact_html=impact_html,
            impact_notes_html=impact_notes_html,
            repair_plan_html=repair_plan_html,
        )    






    def _build_relation_diagram_modal_html_old(self, model):
        entries = self._collect_relation_diagram_entries(model)
        if not entries:
            return ""

        modal_id = "srdf-rel-graph-{}-{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )

        current_model_label = "{}.{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )

        related_boxes_html = format_html_join(
            "",
            (
                '<div style="display:flex;align-items:center;justify-content:space-between;'
                'gap:12px;margin:0 0 10px 0;padding:10px 12px;'
                'background:#ffffff;border:1px solid #dce7f0;border-radius:8px;'
                'box-shadow:0 2px 6px rgba(0,0,0,0.05);">'
                    '<div style="display:flex;align-items:center;gap:10px;">'
                        '<span style="display:inline-block;min-width:38px;text-align:center;'
                        'background:#efe8fa;border:1px solid #c5b1e5;border-radius:5px;'
                        'padding:3px 6px;font-size:11px;font-weight:bold;color:#5b3d8a;">{}</span>'
                        '<div style="font-size:13px;color:#244766;font-weight:bold;">{}</div>'
                    '</div>'
                    '<div style="font-size:11px;color:#0b63ce;font-weight:bold;">{}</div>'
                '</div>'
            ),
            (
                (
                    row["relation_type"],
                    row["related_model_label"],
                    row["field_name"],
                )
                for row in entries
            ),
        )

        mapping_cards_html = format_html_join(
            "",
            (
                '<div style="margin-bottom:10px;padding:10px 12px;'
                'border:1px solid #e3ebf3;border-radius:8px;background:#fbfdff;">'
                    '<div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">'
                        '<span style="display:inline-block;min-width:38px;text-align:center;'
                        'background:#eef4fb;border:1px solid #c7d7e6;border-radius:5px;'
                        'padding:3px 6px;font-size:11px;font-weight:bold;color:#355;">{}</span>'
                        '<span style="font-size:12px;color:#607d8b;font-weight:bold;">PK &lt;-&gt; FK mapping</span>'
                    '</div>'
                    '<div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">'
                        '<div style="padding:8px 10px;background:#eef6ff;border:1px solid #cddff1;'
                        'border-radius:7px;color:#144a75;font-size:12px;font-weight:bold;">{}</div>'
                        '<div style="font-size:20px;color:#8aa4bf;font-weight:bold;">&#8594;</div>'
                        '<div style="padding:8px 10px;background:#fff8eb;border:1px solid #ead9ac;'
                        'border-radius:7px;color:#6b5b22;font-size:12px;font-weight:bold;">{}</div>'
                    '</div>'
                '</div>'
            ),
            (
                (
                    row["relation_type"],
                    row["left_side"],
                    row["right_side"],
                )
                for row in entries
            ),
        )

        arrow_lines_html = format_html_join(
            "",
            (
                '<div style="display:flex;align-items:center;justify-content:center;margin:8px 0;">'
                    '<div style="width:2px;height:18px;background:#c8d6e5;"></div>'
                '</div>'
            ),
            tuple(() for _ in entries[:1]),
        )

        return format_html(
            '<span class="srdf-relgraph-wrap" style="display:inline-block;margin-right:6px;">'
            '<button type="button" '
            'class="srdf-relgraph-button" '
            'data-target-id="{}" '
            'style="cursor:pointer;display:inline-block;'
            'background:#efe8fa;border:1px solid #c5b1e5;border-radius:5px;'
            'padding:2px 8px;font-size:11px;font-weight:bold;color:#5b3d8a;vertical-align:middle;">'
            'REL'
            '</button>'
            '</span>'

            '<div id="{}" class="srdf-relgraph-modal" '
            'style="display:none;position:fixed;left:0;top:0;right:0;bottom:0;z-index:100002;'
            'background:rgba(20,30,40,0.22);">'
                '<div style="position:absolute;left:50%;top:60px;transform:translateX(-50%);'
                'width:980px;max-width:96vw;max-height:86vh;overflow:auto;'
                'background:#fff;border:1px solid #b8c7d3;border-radius:10px;'
                'box-shadow:0 14px 36px rgba(0,0,0,0.24);">'

                    '<div style="padding:12px 16px;border-bottom:1px solid #e6edf2;'
                    'background:linear-gradient(180deg,#faf7fe 0%,#f5f8fc 100%);'
                    'display:flex;justify-content:space-between;align-items:center;">'
                        '<div>'
                            '<div style="font-size:12px;color:#7b6a99;font-weight:bold;">Relation Diagram</div>'
                            '<div style="font-size:19px;color:#234;font-weight:bold;">{}</div>'
                        '</div>'
                        '<button type="button" '
                        'class="srdf-relgraph-close" '
                        'data-target-id="{}" '
                        'style="cursor:pointer;background:#fff;border:1px solid #d6c9ea;'
                        'border-radius:5px;padding:4px 10px;font-weight:bold;color:#5d4a7d;">X</button>'
                    '</div>'

                    '<div style="padding:16px;">'

                        '<div style="display:flex;flex-direction:column;align-items:center;margin-bottom:18px;">'
                            '<div style="padding:12px 18px;min-width:320px;text-align:center;'
                            'background:#eef6ff;border:1px solid #cddff1;border-radius:10px;'
                            'box-shadow:0 3px 8px rgba(0,0,0,0.05);">'
                                '<div style="font-size:11px;color:#607d8b;font-weight:bold;">CURRENT DATA MODEL</div>'
                                '<div style="font-size:19px;color:#144a75;font-weight:bold;margin-top:4px;">{}</div>'
                            '</div>'
                            '{}'
                        '</div>'

                        '<div style="display:grid;grid-template-columns:1fr 1fr;gap:18px;">'

                            '<div style="padding:12px;border:1px solid #e3ebf3;border-radius:10px;background:#f9fbfd;">'
                                '<div style="font-size:12px;color:#607d8b;font-weight:bold;margin-bottom:10px;">RELATED TABLES</div>'
                                '{}'
                            '</div>'

                            '<div style="padding:12px;border:1px solid #e3ebf3;border-radius:10px;background:#fcfcff;">'
                                '<div style="font-size:12px;color:#607d8b;font-weight:bold;margin-bottom:10px;">RELATION MAP</div>'
                                '{}'
                            '</div>'

                        '</div>'
                    '</div>'
                '</div>'
            '</div>'

            '<script>'
            '(function(){{'
            'if (window.__srdfRelGraphInitDone) return;'
            'window.__srdfRelGraphInitDone = true;'

            'function closeAllRelGraphModals(exceptId){{'
            '  var modals = document.querySelectorAll(".srdf-relgraph-modal");'
            '  for (var i = 0; i < modals.length; i++){{'
            '    if (!exceptId || modals[i].id !== exceptId){{'
            '      modals[i].style.display = "none";'
            '    }}'
            '  }}'
            '}}'

            'document.addEventListener("click", function(ev){{'
            '  var openButton = ev.target.closest(".srdf-relgraph-button");'
            '  if (openButton){{'
            '    ev.preventDefault();'
            '    ev.stopPropagation();'
            '    var targetId = openButton.getAttribute("data-target-id");'
            '    var modal = document.getElementById(targetId);'
            '    if (!modal) return;'
            '    var isOpen = modal.style.display === "block";'
            '    closeAllRelGraphModals();'
            '    modal.style.display = isOpen ? "none" : "block";'
            '    return;'
            '  }}'

            '  var closeButton = ev.target.closest(".srdf-relgraph-close");'
            '  if (closeButton){{'
            '    ev.preventDefault();'
            '    var closeId = closeButton.getAttribute("data-target-id");'
            '    var closeModal = document.getElementById(closeId);'
            '    if (closeModal) closeModal.style.display = "none";'
            '    return;'
            '  }}'

            '  var modalBg = ev.target.closest(".srdf-relgraph-modal");'
            '  if (modalBg && ev.target === modalBg){{'
            '    modalBg.style.display = "none";'
            '    return;'
            '  }}'
            '}});'

            'document.addEventListener("keydown", function(ev){{'
            '  if (ev.key === "Escape"){{'
            '    closeAllRelGraphModals();'
            '  }}'
            '}});'
            '}})();'
            '</script>',
            modal_id,
            modal_id,
            current_model_label,
            modal_id,
            current_model_label,
            arrow_lines_html,
            related_boxes_html,
            mapping_cards_html,
        ) 
    
    
    def _build_related_models_menu_html(self, model, context, show_non_empty_only=False):
        related_entries = self._collect_related_model_entries(
            model,
            context,
            show_non_empty_only=show_non_empty_only,
        )
        if not related_entries:
            return ""

        menu_id = "srdf-relmenu-{}-{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )

        items_html = format_html_join(
            "",
            (
                '<li style="display:block;width:100%;margin:0;padding:0;border-bottom:1px solid #eef2f5;">'
                '<a href="{}" '
                'style="display:block;width:100%;padding:8px 12px;box-sizing:border-box;'
                'color:#234;text-decoration:none;white-space:nowrap;'
                'transition:background-color 0.12s ease,color 0.12s ease;" '
                'onmouseover="this.style.backgroundColor=\'#e8f2ff\';this.style.color=\'#0b3d91\';" '
                'onmouseout="this.style.backgroundColor=\'transparent\';this.style.color=\'#234\';">'
                '<span style="display:inline-block;min-width:34px;color:#666;font-size:11px;font-weight:bold;">{}</span>'
                '<span style="display:inline-block;min-width:170px;vertical-align:middle;">{}</span>'
                '<span style="float:right;color:#0b63ce;font-size:11px;font-weight:bold;padding-left:12px;">{}</span>'
                '</a>'
                '</li>'
            ),
            (
                (
                    item["url"],
                    item["relation_type"],
                    item["label"],
                    item["row_count"] if item["row_count"] >= 0 else "?",
                )
                for item in related_entries
            ),
        )

        return format_html(
            '<span class="srdf-related-menu-wrap" style="display:inline-block;margin-right:8px;">'
            '<button type="button" '
            'class="srdf-related-menu-button" '
            'data-target-id="{}" '
            'style="cursor:pointer;display:inline-block;'
            'background:#dfeaf7;border:1px solid #8fb0d1;border-radius:5px;'
            'padding:2px 9px;font-size:11px;font-weight:bold;color:#244766;vertical-align:middle;">'
            'FK'
            '</button>'
            '<div id="{}" '
            'class="srdf-related-menu-panel" '
            'style="display:none;position:fixed;z-index:99999;min-width:280px;max-width:380px;'
            'background:#fff;border:1px solid #b8c7d3;border-radius:6px;'
            'box-shadow:0 6px 18px rgba(0,0,0,0.18);padding:0;overflow:hidden;">'
            '<div style="padding:8px 12px;font-size:11px;font-weight:bold;color:#607d8b;'
            'background:#f8fafc;border-bottom:1px solid #e6edf2;">Related tables</div>'
            '<ul style="display:block;list-style:none;margin:0;padding:0;">{}</ul>'
            '</div>'
            '</span>'
            '<script>'
            '(function(){{'
            'if (window.__srdfRelatedMenuInitDone) return;'
            'window.__srdfRelatedMenuInitDone = true;'

            'function closeAllMenus(exceptId){{'
            '  var panels = document.querySelectorAll(".srdf-related-menu-panel");'
            '  for (var i = 0; i < panels.length; i++){{'
            '    if (!exceptId || panels[i].id !== exceptId){{'
            '      panels[i].style.display = "none";'
            '    }}'
            '  }}'
            '}}'

            'function openMenuNearButton(button, panel){{'
            '  var rect = button.getBoundingClientRect();'
            '  panel.style.display = "block";'
            '  panel.style.left = Math.round(rect.left) + "px";'
            '  panel.style.top = Math.round(rect.bottom + 6) + "px";'

            '  var panelRect = panel.getBoundingClientRect();'
            '  var viewportWidth = window.innerWidth || document.documentElement.clientWidth;'
            '  var viewportHeight = window.innerHeight || document.documentElement.clientHeight;'

            '  if (panelRect.right > viewportWidth - 12){{'
            '    var newLeft = Math.max(12, viewportWidth - panelRect.width - 12);'
            '    panel.style.left = Math.round(newLeft) + "px";'
            '  }}'

            '  if (panelRect.bottom > viewportHeight - 12){{'
            '    var aboveTop = rect.top - panelRect.height - 6;'
            '    if (aboveTop >= 12){{'
            '      panel.style.top = Math.round(aboveTop) + "px";'
            '    }}'
            '  }}'
            '}}'

            'document.addEventListener("click", function(ev){{'
            '  var button = ev.target.closest(".srdf-related-menu-button");'
            '  if (button){{'
            '    ev.preventDefault();'
            '    ev.stopPropagation();'
            '    var targetId = button.getAttribute("data-target-id");'
            '    var panel = document.getElementById(targetId);'
            '    if (!panel) return;'
            '    var isOpen = panel.style.display === "block";'
            '    closeAllMenus();'
            '    if (!isOpen){{'
            '      openMenuNearButton(button, panel);'
            '    }}'
            '    return;'
            '  }}'

            '  if (!ev.target.closest(".srdf-related-menu-panel")){{'
            '    closeAllMenus();'
            '  }}'
            '}});'

            'window.addEventListener("resize", function(){{'
            '  closeAllMenus();'
            '}});'

            'window.addEventListener("scroll", function(){{'
            '  closeAllMenus();'
            '}}, true);'

            'document.addEventListener("keydown", function(ev){{'
            '  if (ev.key === "Escape"){{'
            '    closeAllMenus();'
            '  }}'
            '}});'
            '}})();'
            '</script>',
            menu_id,
            menu_id,
            items_html,
        )
    
    def _show_non_empty_only_enabled(self, request):
        get_data = request.GET
        toggle_submitted = str(get_data.get("toggle_submitted", "")).strip()

        if toggle_submitted:
            return str(get_data.get("show_non_empty_only", "")).strip().lower() in (
                "1", "true", "yes", "on"
            )

        return True  
    
    
    def _build_doctor_lazy_button_html(self, model):
        modal_id = "srdf-doctor-{}-{}".format(
            model._meta.app_label,
            model._meta.model_name,
        )

        endpoint = reverse("srdf_doctor_modal")

        return format_html(
            '<span class="srdf-doctor-wrap" style="display:inline-block;margin-right:4px;">'
                '<button type="button" '
                'class="srdf-doctor-lazy-button" '
                'data-target-id="{}" '
                'data-endpoint="{}" '
                'data-app-label="{}" '
                'data-model-name="{}" '
                'title="Migration Doctor" '
                'style="cursor:pointer;display:inline-block;background:#fbe9d7;border:1px solid #dcb98f;'
                'border-radius:5px;padding:2px 7px;font-size:11px;font-weight:bold;color:#7a4b1f;vertical-align:middle;">'
                'DOC'
                '</button>'
            '</span>'

            '<div id="{}" class="srdf-doctor-lazy-modal" '
            'style="display:none;position:fixed;left:0;top:0;right:0;bottom:0;z-index:100003;'
            'background:rgba(6,10,26,0.62);backdrop-filter:blur(2px);">'
                '<div style="position:absolute;left:50%;top:34px;transform:translateX(-50%);'
                'width:1180px;max-width:97vw;max-height:91vh;overflow:auto;'
                'background:linear-gradient(180deg,#0d1433 0%,#0a1028 100%);'
                'border:1px solid rgba(120,146,255,0.16);border-radius:12px;'
                'box-shadow:0 18px 44px rgba(0,0,0,0.36);">'
                    '<div style="padding:14px 18px;border-bottom:1px solid rgba(150,170,255,0.10);'
                    'background:linear-gradient(180deg,#11193f 0%,#0d1433 100%);'
                    'display:flex;justify-content:space-between;align-items:center;">'
                        '<div>'
                            '<div style="font-size:12px;color:#91a3d8;font-weight:bold;letter-spacing:0.3px;">Migration Doctor – Lazy Loading</div>'
                            '<div style="font-size:20px;color:#f3f7ff;font-weight:bold;">{}.{}</div>'
                        '</div>'
                        '<button type="button" class="srdf-doctor-lazy-close" data-target-id="{}" '
                        'style="cursor:pointer;background:#121a3c;border:1px solid rgba(182,197,255,0.20);'
                        'border-radius:6px;padding:5px 11px;font-weight:bold;color:#d8e3ff;">X</button>'
                    '</div>'

                    '<div class="srdf-doctor-lazy-body" style="padding:18px;color:#d8e3ff;">'
                        '<div style="padding:18px;border:1px dashed rgba(143,176,209,0.28);border-radius:10px;'
                        'background:rgba(255,255,255,0.02);">'
                            'Doctor modal not loaded yet.'
                        '</div>'
                    '</div>'
                '</div>'
            '</div>'

            '<script>'
            '(function(){{'
            'if (window.__srdfDoctorLazyInitDone) return;'
            'window.__srdfDoctorLazyInitDone = true;'

            'function closeAllDoctorLazyModals(exceptId){{'
            '  var modals = document.querySelectorAll(".srdf-doctor-lazy-modal");'
            '  for (var i = 0; i < modals.length; i++){{'
            '    if (!exceptId || modals[i].id !== exceptId){{'
            '      modals[i].style.display = "none";'
            '    }}'
            '  }}'
            '}}'

            'function bindDoctorModalTabs(modal){{'
            '  var buttons = modal.querySelectorAll(".srdf-doctor-tab-button");'
            '  for (var i = 0; i < buttons.length; i++){{'
            '    buttons[i].addEventListener("click", function(ev){{'
            '      ev.preventDefault();'
            '      var currentButton = this;'
            '      var targetId = currentButton.getAttribute("data-tab-target");'
            '      var localButtons = modal.querySelectorAll(".srdf-doctor-tab-button");'
            '      var panels = modal.querySelectorAll(".srdf-doctor-tab-panel");'

            '      for (var j = 0; j < localButtons.length; j++){{'
            '        localButtons[j].classList.remove("is-active");'
            '        localButtons[j].style.background = "#0f1738";'
            '        localButtons[j].style.color = "#91a3d8";'
            '      }}'

            '      for (var k = 0; k < panels.length; k++){{'
            '        panels[k].style.display = "none";'
            '      }}'

            '      currentButton.classList.add("is-active");'
            '      currentButton.style.background = "#1a2552";'
            '      currentButton.style.color = "#e6efff";'

            '      var panel = modal.querySelector("#" + targetId);'
            '      if (panel){{'
            '        panel.style.display = "block";'
            '      }}'
            '    }});'
            '  }}'
            '}}'

            'document.addEventListener("click", function(ev){{'
            '  var openButton = ev.target.closest(".srdf-doctor-lazy-button");'
            '  if (openButton){{'
            '    ev.preventDefault();'
            '    ev.stopPropagation();'

            '    var modalId = openButton.getAttribute("data-target-id");'
            '    var endpoint = openButton.getAttribute("data-endpoint");'
            '    var appLabel = openButton.getAttribute("data-app-label");'
            '    var modelName = openButton.getAttribute("data-model-name");'
            '    var modal = document.getElementById(modalId);'
            '    if (!modal) return;'

            '    var isOpen = modal.style.display === "block";'
            '    closeAllDoctorLazyModals();'
            '    if (isOpen){{'
            '      modal.style.display = "none";'
            '      return;'
            '    }}'

            '    modal.style.display = "block";'

            '    if (modal.getAttribute("data-loaded") === "1"){{'
            '      return;'
            '    }}'

            '    var body = modal.querySelector(".srdf-doctor-lazy-body");'
            '    if (body){{'
            '      body.innerHTML = \'<div style="padding:18px;border:1px dashed rgba(143,176,209,0.28);border-radius:10px;background:rgba(255,255,255,0.02);">Loading doctor analysis...</div>\';'
            '    }}'

            '    var url = endpoint + "?app_label=" + encodeURIComponent(appLabel) + "&model_name=" + encodeURIComponent(modelName);'

            '    fetch(url, {{headers: {{"X-Requested-With": "XMLHttpRequest"}}}})'
            '      .then(function(response){{ return response.json(); }})'
            '      .then(function(payload){{'
            '        if (!payload.success){{'
            '          throw new Error(payload.error || "Unknown doctor error");'
            '        }}'
            '        if (body){{'
            '          body.innerHTML = payload.html;'
            '        }}'
            '        modal.setAttribute("data-loaded", "1");'
            '        bindDoctorModalTabs(modal);'
            '      }})'
            '      .catch(function(error){{'
            '        if (body){{'
            '          body.innerHTML = \'<div style="padding:18px;border:1px solid rgba(255,138,128,0.25);border-radius:10px;background:rgba(90,31,31,0.35);color:#ffb4ae;">Doctor loading failed: \' + String(error) + \'</div>\';'
            '        }}'
            '      }});'

            '    return;'
            '  }}'

            '  var closeButton = ev.target.closest(".srdf-doctor-lazy-close");'
            '  if (closeButton){{'
            '    ev.preventDefault();'
            '    var closeId = closeButton.getAttribute("data-target-id");'
            '    var closeModal = document.getElementById(closeId);'
            '    if (closeModal) closeModal.style.display = "none";'
            '    return;'
            '  }}'

            '  var modalBg = ev.target.closest(".srdf-doctor-lazy-modal");'
            '  if (modalBg && ev.target === modalBg){{'
            '    modalBg.style.display = "none";'
            '    return;'
            '  }}'
            '}});'

            'document.addEventListener("keydown", function(ev){{'
            '  if (ev.key === "Escape"){{'
            '    closeAllDoctorLazyModals();'
            '  }}'
            '}});'
            '}})();'
            '</script>',
            modal_id,
            endpoint,
            model._meta.app_label,
            model._meta.model_name,
            modal_id,
            model._meta.app_label,
            model._meta.model_name,
            modal_id,
        )    

    def init_with_context(self, context):
        if self._initialized:
            return

        request = context["request"]
        show_non_empty_only = self._show_non_empty_only_enabled(request)

        items = self._visible_models(request)
        apps_dict = {}

        for model, perms in items:
            app_label = model._meta.app_label

            if app_label not in apps_dict:
                apps_dict[app_label] = {
                    "title": apps.get_app_config(app_label).verbose_name,
                    "url": self._get_admin_app_list_url(model, context),
                    "models": [],
                }

            try:
                row_count = int(model.objects.count())
            except Exception:
                row_count = -1

            if show_non_empty_only and row_count <= 0:
                continue



            model_label = str(model._meta.verbose_name_plural or "")


            doctor_modal_html = self._build_doctor_lazy_button_html(model)
            structure_modal_html = self._build_structure_modal_html(model)
            relation_diagram_html = self._build_relation_diagram_modal_html(model)
        
            related_menu_html = self._build_related_models_menu_html(
                        model,
                        context,
                        show_non_empty_only=show_non_empty_only,
                    )
            
            
            
            if row_count > 0:
                model_title = format_html(
                    '<span style="display:inline-block;min-width:58px;color:#0b63ce;font-weight:bold;">[{}]</span> {}{}{}{}'
                    '<span style="color:#1b5e20;font-weight:bold;">{}</span>',
                    row_count,
                    doctor_modal_html,
                    structure_modal_html,
                    relation_diagram_html,
                    related_menu_html,
                    model_label,
                )
            else:
                model_title = format_html(
                    '<span style="display:inline-block;min-width:58px;"></span> {}{}{}{}'
                    '<span style="color:#8a8a8a;">{}</span>',
                    doctor_modal_html,
                    structure_modal_html,
                    relation_diagram_html,
                    related_menu_html,
                    model_label,
                )    
                
                
                

            model_dict = {
                "title": model_title,
            }

            if perms.get("change") or perms.get("view", False):
                model_dict["change_url"] = self._get_admin_change_url(model, context)

            if perms.get("add"):
                model_dict["add_url"] = self._get_admin_add_url(model, context)

            apps_dict[app_label]["models"].append(model_dict)

        self.children = []
        for app_label in sorted(apps_dict.keys()):
            if not apps_dict[app_label]["models"]:
                continue

            apps_dict[app_label]["models"].sort(key=lambda x: str(x["title"]))
            self.children.append(apps_dict[app_label])

        self._initialized = True
        
        
class CustomIndexDashboard(Dashboard):
    """
    Custom index dashboard for SRDF.
    """
    columns =  2
    
    def init_with_context(self, context):
        site_name = get_admin_site_name(context)
        # append a link list module for "quick links"
        self.children.append(modules.LinkList(
            _('Quick links'),
            layout='inline',
            draggable=False,
            deletable=False,
            collapsible=False,
            children=[
                [_('Return to site'), '/'],
                [_('Change password'),
                 reverse('%s:password_change' % site_name)],
                [_('Log out'), reverse('%s:logout' % site_name)],
            ]
        ))

        # append an app list module for "Applications"
        self.children.append(modules.AppList(
            _('Applications'),
            exclude=('django.contrib.*',),
        ))

        # append an app list module for "Administration"
        self.children.append(modules.AppList(
            _('Administration'),
            models=('django.contrib.*',),
        ))

        # append a recent actions module
        self.children.append(modules.RecentActions(_('Recent Actions'), 5))

        # append a feed module
        self.children.append(modules.Feed(
            _('Latest Django News'),
            feed_url='http://www.djangoproject.com/rss/weblog/',
            limit=5
        ))

        # append another link list module for "support".
        self.children.append(modules.LinkList(
            _('Support'),
            children=[
                {
                    'title': _('Django documentation'),
                    'url': 'http://docs.djangoproject.com/',
                    'external': True,
                },
                {
                    'title': _('Django "django-users" mailing list'),
                    'url': 'http://groups.google.com/group/django-users',
                    'external': True,
                },
                {
                    'title': _('Django irc channel'),
                    'url': 'irc://irc.freenode.net/django',
                    'external': True,
                },
            ]
        ))
        
        # Copy following code into your custom dashboard
        # append following code after recent actions module or
        # a link list module for "quick links"
        graph_list = get_active_graph()
        for i in graph_list:
            kwargs = {}
            kwargs['graph_key'] = i.graph_key
            kwargs['require_chart_jscss'] = False
        
            if context['request'].POST.get('select_box_' + i.graph_key):
                kwargs['select_box_' + i.graph_key] = context['request'].POST['select_box_' + i.graph_key]
        
            self.children.append(DashboardCharts(**kwargs))        
        


class CustomAppIndexDashboard(AppIndexDashboard):
    """
    Custom app index dashboard for SRDF.
    """

    # we disable title because its redundant with the model list module
    title = ''

    def __init__(self, *args, **kwargs):
        AppIndexDashboard.__init__(self, *args, **kwargs)

        # append a model list module and a recent actions module
        self.children += [
            modules.ModelList(self.app_title, self.models),
            modules.RecentActions(
                _('Recent Actions'),
                include_list=self.get_app_content_types(),
                limit=5
            )
        ]

    def init_with_context(self, context):
        """
        Use this method if you need to access the request context.
        """
        return super(CustomAppIndexDashboard, self).init_with_context(context)
