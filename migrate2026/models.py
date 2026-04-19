#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from django.db import models


class SafeMigrationRun(models.Model):
    STATUS_RUNNING = "running"
    STATUS_SUCCESS = "success"
    STATUS_FAILED = "failed"
    STATUS_FAILED_THEN_ROLLBACKED = "failed_then_rollbacked"
    STATUS_ROLLBACK_FAILED = "rollback_failed"

    STATUS_CHOICES = [
        (STATUS_RUNNING, "Running"),
        (STATUS_SUCCESS, "Success"),
        (STATUS_FAILED, "Failed"),
        (STATUS_FAILED_THEN_ROLLBACKED, "Failed then rollbacked"),
        (STATUS_ROLLBACK_FAILED, "Rollback failed"),
    ]

    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    app_label = models.CharField(max_length=120, blank=True, default="", db_index=True)
    migration_target = models.CharField(max_length=180, blank=True, default="")
    database_alias = models.CharField(max_length=60, blank=True, default="default", db_index=True)

    status = models.CharField(
        max_length=32,
        choices=STATUS_CHOICES,
        blank=True,
        default=STATUS_RUNNING,
        db_index=True,
    )

    rollback_on_error = models.BooleanField(default=False)
    ddl_verbose = models.BooleanField(default=False)

    duration_ms = models.PositiveIntegerField(default=0)

    migrations_applied_count = models.PositiveIntegerField(default=0)
    migrations_rollbacked_count = models.PositiveIntegerField(default=0)
    ddl_executed_count = models.PositiveIntegerField(default=0)
    ddl_failed_count = models.PositiveIntegerField(default=0)

    failed_error = models.TextField(blank=True, default="")
    rollback_error = models.TextField(blank=True, default="")

    applied_migrations_text = models.TextField(blank=True, default="")
    rollbacked_migrations_text = models.TextField(blank=True, default="")

    class Meta:
        ordering = ["-created_at"]

    def __str__(self) -> str:
        label = self.app_label or "all_apps"
        return f"{label} | {self.status} | {self.created_at:%Y-%m-%d %H:%M:%S}"


class SafeMigrationDDL(models.Model):
    STATUS_PENDING = "pending"
    STATUS_OK = "ok"
    STATUS_FAILED = "failed"

    STATUS_CHOICES = [
        (STATUS_PENDING, "Pending"),
        (STATUS_OK, "OK"),
        (STATUS_FAILED, "Failed"),
    ]

    run = models.ForeignKey(
        SafeMigrationRun,
        on_delete=models.CASCADE,
        related_name="ddl_entries",
    )

    seq_no = models.PositiveIntegerField(default=0, db_index=True)
    status = models.CharField(
        max_length=16,
        choices=STATUS_CHOICES,
        blank=True,
        default=STATUS_PENDING,
        db_index=True,
    )
    sql_kind = models.CharField(max_length=40, blank=True, default="", db_index=True)

    sql_text = models.TextField(blank=True, default="")
    params_text = models.TextField(blank=True, default="")
    error_text = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["seq_no"]

    def __str__(self) -> str:
        return f"DDL #{self.seq_no} [{self.status}]"