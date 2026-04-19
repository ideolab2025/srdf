#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.urls import path
from srdf.views import doctor_modal_view

from . import views

app_name = "srdf"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("dashboard/action/", views.dashboard_run_action, name="dashboard_run_action"),
    path("dashboard/bootstrap-plan/<int:plan_id>/run/", views.bootstrap_plan_run, name="bootstrap_plan_run"),
    path("action/<int:execution_id>/", views.action_detail, name="action_detail"),
    path("node/<int:node_id>/", views.node_detail, name="node_detail"),
    path("failover-plan/<int:plan_id>/", views.failover_plan_detail, name="failover_plan_detail"),
    path("api/ping/", views.api_ping, name="api_ping"),
    path("api/status/", views.api_status, name="api_status"),
    path("api/action/", views.api_action, name="api_action"),
    
    #=========================================
    #==== MIGRATION DOCTOR                 ===
    path("doctor/modal/", doctor_modal_view, name="srdf_doctor_modal"),
]