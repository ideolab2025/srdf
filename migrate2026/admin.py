#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from django.contrib import admin
from .models import SafeMigrationDDL, SafeMigrationRun


class SafeMigrationDDLInline(admin.TabularInline):
    model = SafeMigrationDDL
    extra = 0
    fields = (
        "seq_no",
        "status",
        "sql_kind",
        "sql_text",
        "params_text",
        "error_text",
        "created_at",
    )
    readonly_fields = fields
    ordering = ("seq_no",)
    show_change_link = False
    can_delete = False


@admin.register(SafeMigrationRun)
class SafeMigrationRunAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "created_at",
        "app_label",
        "migration_target",
        "database_alias",
        "status",
        "duration_ms",
        "migrations_applied_count",
        "migrations_rollbacked_count",
        "ddl_executed_count",
        "ddl_failed_count",
    )
    list_filter = (
        "status",
        "app_label",
        "database_alias",
        "rollback_on_error",
        "ddl_verbose",
        "created_at",
    )
    search_fields = (
        "app_label",
        "migration_target",
        "failed_error",
        "rollback_error",
        "applied_migrations_text",
        "rollbacked_migrations_text",
    )
    readonly_fields = (
        "created_at",
        "finished_at",
        "app_label",
        "migration_target",
        "database_alias",
        "status",
        "rollback_on_error",
        "ddl_verbose",
        "duration_ms",
        "migrations_applied_count",
        "migrations_rollbacked_count",
        "ddl_executed_count",
        "ddl_failed_count",
        "failed_error",
        "rollback_error",
        "applied_migrations_text",
        "rollbacked_migrations_text",
    )
    inlines = [SafeMigrationDDLInline]
    ordering = ("-created_at",)


@admin.register(SafeMigrationDDL)
class SafeMigrationDDLAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "run",
        "seq_no",
        "status",
        "sql_kind",
        "created_at",
    )
    list_filter = (
        "status",
        "sql_kind",
        "created_at",
    )
    search_fields = (
        "sql_text",
        "params_text",
        "error_text",
    )
    readonly_fields = (
        "run",
        "seq_no",
        "status",
        "sql_kind",
        "sql_text",
        "params_text",
        "error_text",
        "created_at",
    )
    ordering = ("-id",)