#! /usr/bin/python3.1
# -*- coding: utf-8 -*-



from django.db import models
from django.utils import timezone


class Node(models.Model):
    ROLE_CHOICES = [
        ("primary", "Primary"),
        ("secondary", "Secondary"),
        ("witness", "Witness"),
        ("control", "Control Plane"),
    ]

    STATUS_CHOICES = [
        ("online", "Online"),
        ("offline", "Offline"),
        ("degraded", "Degraded"),
        ("unknown", "Unknown"),
    ]

    name = models.CharField(max_length=120)
    hostname = models.CharField(max_length=255, blank=True, default="")
    db_port = models.IntegerField(default=3306)
    site = models.CharField(max_length=120, blank=True, default="")
    role = models.CharField(max_length=32, choices=ROLE_CHOICES, default="secondary")
    
    api_base_url = models.CharField(max_length=255, blank=True, default="")
    api_token = models.CharField(max_length=255, blank=True, default="")

    is_enabled = models.BooleanField(default=True)
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="unknown")

    last_seen_at = models.DateTimeField(null=True, blank=True)
    last_status_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    
    def __str__(self):
        return f"{self.name} ({self.hostname}:{self.db_port})"

    class Meta:
        ordering = ["name"]
        
        
class ReplicationService(models.Model):
    SERVICE_TYPE_CHOICES = [
        ("postgresql", "PostgreSQL"),
        ("mysql", "MySQL"),
        ("mariadb", "MariaDB"),
        ("filesync", "File Sync"),
        ("nginx", "Nginx/Proxy"),
    ]

    MODE_CHOICES = [
        ("sync", "Synchronous"),
        ("async", "Asynchronous"),
        ("manual", "Manual"),
    ]

    name = models.CharField(max_length=120)
    service_type = models.CharField(max_length=32, choices=SERVICE_TYPE_CHOICES)
    mode = models.CharField(max_length=16, choices=MODE_CHOICES, default="async")

    source_node = models.ForeignKey("srdf.Node", on_delete=models.CASCADE, related_name="source_services")
    target_node = models.ForeignKey("srdf.Node", on_delete=models.CASCADE, related_name="target_services")

    is_enabled = models.BooleanField(default=True)
    config = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"{self.name} ({self.service_type})"
    
    
    
class ReplicationStatus(models.Model):
    service = models.ForeignKey('srdf.ReplicationService', on_delete=models.CASCADE, related_name="statuses")
    status = models.CharField(max_length=32, default="unknown")
    lag_seconds = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, default="")
    payload = models.JSONField(default=dict, blank=True)
    collected_at = models.DateTimeField()
    
    
class ActionExecution(models.Model):
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("timeout", "Timeout"),
        ("cancelled", "Cancelled"),
    ]

    node = models.ForeignKey("srdf.Node", on_delete=models.CASCADE, related_name="executions")
    action_name = models.CharField(max_length=120)
    request_id = models.CharField(max_length=120, blank=True, default="")
    params = models.JSONField(default=dict, blank=True)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="pending")
    stdout = models.TextField(blank=True, default="")
    stderr = models.TextField(blank=True, default="")
    result_payload = models.JSONField(default=dict, blank=True)

    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_ms = models.IntegerField(default=0)

    initiated_by = models.CharField(max_length=120, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    
    
class FailoverPlan(models.Model):
    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("ready", "Ready"),
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("cancelled", "Cancelled"),
    ]

    name = models.CharField(max_length=120)
    source_site = models.CharField(max_length=120, blank=True, default="")
    target_site = models.CharField(max_length=120, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="draft")

    plan_data = models.JSONField(default=dict, blank=True)
    last_error = models.TextField(blank=True, default="")
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    
class AuditEvent(models.Model):
    LEVEL_CHOICES = [
        ("info", "Info"),
        ("warning", "Warning"),
        ("error", "Error"),
        ("critical", "Critical"),
    ]

    event_type = models.CharField(max_length=120)
    level = models.CharField(max_length=32, choices=LEVEL_CHOICES, default="info")
    message = models.TextField()
    node = models.ForeignKey("srdf.Node", null=True, blank=True, on_delete=models.SET_NULL)
    payload = models.JSONField(default=dict, blank=True)
    created_by = models.CharField(max_length=120, blank=True, default="")
    created_at = models.DateTimeField(auto_now_add=True)
    
    
class ExecutionLock(models.Model):
    name = models.CharField(max_length=120, unique=True)

    is_locked = models.BooleanField(default=False)
    locked_at = models.DateTimeField(null=True, blank=True)

    def __str__(self):
        return f"{self.name} - locked={self.is_locked}"
    
    
class OutboundChangeEvent(models.Model):
    
    STATUS_CHOICES = [
        ("pending", "Pending"),
        ("batched", "Batched"),
        ("sent", "Sent"),
        ("acked", "Acknowledged"),
        ("applied", "Applied"),
        ("failed", "Failed"),
        ("dead", "Dead Letter"),
    ]    
    
    EVENT_FAMILY_CHOICES = [
        ("dml", "DML"),
        ("ddl", "DDL"),
    ]

    OBJECT_TYPE_CHOICES = [
        ("", "Unknown"),
        ("database", "Database"),
        ("table", "Table"),
        ("column", "Column"),
        ("index", "Index"),
    ]

    DDL_ACTION_CHOICES = [
        ("", "Unknown"),
        ("create", "Create"),
        ("alter", "Alter"),
        ("drop", "Drop"),
    ]

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="outbound_events",
    )

    source_node = models.ForeignKey(
        "srdf.Node",
        on_delete=models.CASCADE,
        related_name="outbound_events",
    )

    engine = models.CharField(max_length=32, default="mariadb")

    event_family = models.CharField(
        max_length=16,
        choices=EVENT_FAMILY_CHOICES,
        default="dml",
    )

    database_name = models.CharField(max_length=120, blank=True, default="")
    table_name = models.CharField(max_length=120, blank=True, default="")
    operation = models.CharField(max_length=32, blank=True, default="")

    object_type = models.CharField(
        max_length=32,
        choices=OBJECT_TYPE_CHOICES,
        blank=True,
        default="",
    )

    ddl_action = models.CharField(
        max_length=32,
        choices=DDL_ACTION_CHOICES,
        blank=True,
        default="",
    )

    object_name = models.CharField(max_length=255, blank=True, default="")
    parent_object_name = models.CharField(max_length=255, blank=True, default="")
    raw_sql = models.TextField(blank=True, default="")


    event_uid = models.CharField(max_length=120, blank=True, default="")
    transaction_id = models.CharField(max_length=120, blank=True, default="")
    log_file = models.CharField(max_length=255, blank=True, default="")
    log_pos = models.BigIntegerField(default=0)

    primary_key_data = models.JSONField(default=dict, blank=True)
    before_data = models.JSONField(default=dict, blank=True)
    after_data = models.JSONField(default=dict, blank=True)
    event_payload = models.JSONField(default=dict, blank=True)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="pending")
    retry_count = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    batched_at = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)
    acked_at = models.DateTimeField(null=True, blank=True)
    applied_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["id"]


class TransportBatch(models.Model):
    STATUS_CHOICES = [
        ("building", "Building"),
        ("ready", "Ready"),
        ("sent", "Sent"),
        ("acked", "Acknowledged"),
        ("failed", "Failed"),
    ]

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="transport_batches",
    )

    batch_uid = models.CharField(max_length=120, blank=True, default="")
    event_count = models.IntegerField(default=0)

    first_event_id = models.BigIntegerField(default=0)
    last_event_id = models.BigIntegerField(default=0)

    payload = models.JSONField(default=dict, blank=True)
    compression = models.CharField(max_length=32, blank=True, default="")
    checksum = models.CharField(max_length=255, blank=True, default="")

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="building")
    retry_count = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    built_at = models.DateTimeField(null=True, blank=True)
    sent_at = models.DateTimeField(null=True, blank=True)
    acked_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["-id"]


class InboundChangeEvent(models.Model):
    
    
    STATUS_CHOICES = [
        ("received", "Received"),
        ("queued", "Queued"),
        ("applying", "Applying"),
        ("applied", "Applied"),
        ("failed", "Failed"),
        ("dead", "Dead Letter"),
    ]
    
    EVENT_FAMILY_CHOICES = [
        ("dml", "DML"),
        ("ddl", "DDL"),
    ]

    OBJECT_TYPE_CHOICES = [
        ("", "Unknown"),
        ("database", "Database"),
        ("table", "Table"),
        ("column", "Column"),
        ("index", "Index"),
    ]

    DDL_ACTION_CHOICES = [
        ("", "Unknown"),
        ("create", "Create"),
        ("alter", "Alter"),
        ("drop", "Drop"),
    ]    

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="inbound_events",
    )

    target_node = models.ForeignKey(
        "srdf.Node",
        on_delete=models.CASCADE,
        related_name="inbound_events",
    )

    transport_batch = models.ForeignKey(
        "srdf.TransportBatch",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="inbound_events",
    )

    engine = models.CharField(max_length=32, default="mariadb")

    event_family = models.CharField(
        max_length=16,
        choices=EVENT_FAMILY_CHOICES,
        default="dml",
    )

    database_name = models.CharField(max_length=120, blank=True, default="")
    table_name = models.CharField(max_length=120, blank=True, default="")
    operation = models.CharField(max_length=32, blank=True, default="")

    object_type = models.CharField(
        max_length=32,
        choices=OBJECT_TYPE_CHOICES,
        blank=True,
        default="",
    )

    ddl_action = models.CharField(
        max_length=32,
        choices=DDL_ACTION_CHOICES,
        blank=True,
        default="",
    )

    object_name = models.CharField(max_length=255, blank=True, default="")
    parent_object_name = models.CharField(max_length=255, blank=True, default="")
    raw_sql = models.TextField(blank=True, default="")
    event_uid = models.CharField(max_length=120, blank=True, default="")
    transaction_id = models.CharField(max_length=120, blank=True, default="")
    source_log_file = models.CharField(max_length=255, blank=True, default="")
    source_log_pos = models.BigIntegerField(default=0)

    primary_key_data = models.JSONField(default=dict, blank=True)
    before_data = models.JSONField(default=dict, blank=True)
    after_data = models.JSONField(default=dict, blank=True)
    event_payload = models.JSONField(default=dict, blank=True)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="received")
    retry_count = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, default="")

    received_at = models.DateTimeField(auto_now_add=True)
    applied_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ["id"]


class ReplicationCheckpoint(models.Model):
    DIRECTION_CHOICES = [
        ("capture", "Capture"),
        ("shipper", "Shipper"),
        ("receiver", "Receiver"),
        ("applier", "Applier"),
    ]

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="checkpoints",
    )

    node = models.ForeignKey(
        "srdf.Node",
        on_delete=models.CASCADE,
        related_name="checkpoints",
    )

    direction = models.CharField(max_length=32, choices=DIRECTION_CHOICES)

    engine = models.CharField(max_length=32, default="mariadb")
    log_file = models.CharField(max_length=255, blank=True, default="")
    log_pos = models.BigIntegerField(default=0)
    transaction_id = models.CharField(max_length=120, blank=True, default="")

    checkpoint_data = models.JSONField(default=dict, blank=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["replication_service_id", "node_id", "direction"]
        
        
class SQLObjectScopeRule(models.Model):
    ENGINE_CHOICES = [
        ("all", "All"),
        ("mariadb", "MariaDB"),
        ("mysql", "MySQL"),
        ("postgresql", "PostgreSQL"),
    ]

    POLICY_CHOICES = [
        ("include", "Include"),
        ("exclude", "Exclude"),
    ]

    OBJECT_TYPE_CHOICES = [
        ("database", "Database"),
        ("table", "Table"),
    ]

    MATCH_MODE_CHOICES = [
        ("exact", "Exact"),
        ("prefix", "Prefix"),
        ("contains", "Contains"),
    ]

    name = models.CharField(max_length=150)
    is_enabled = models.BooleanField(default=True)
    is_system_rule = models.BooleanField(default=False)

    engine = models.CharField(max_length=32, choices=ENGINE_CHOICES, default="all")
    policy_mode = models.CharField(max_length=16, choices=POLICY_CHOICES, default="exclude")
    object_type = models.CharField(max_length=32, choices=OBJECT_TYPE_CHOICES, default="table")
    match_mode = models.CharField(max_length=16, choices=MATCH_MODE_CHOICES, default="exact")

    database_name = models.CharField(max_length=120, blank=True, default="")
    table_name = models.CharField(max_length=120, blank=True, default="")

    priority = models.IntegerField(default=100)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.name} [{self.policy_mode}]"

    class Meta:
        ordering = ["priority", "id"]
        
class BootstrapPlan(models.Model):
    SCOPE_CHOICES = [
        ("database", "Whole database"),
        ("tables", "Selected tables"),
        ("pattern", "Pattern based"),
    ]

    METHOD_CHOICES = [
        ("sql_copy", "SQL copy"),
        ("mysqldump", "mysqldump"),
    ]

    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("ready", "Ready"),
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
    ]

    name = models.CharField(max_length=150)
    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="bootstrap_plans",
    )

    is_enabled = models.BooleanField(default=True)
    bootstrap_method = models.CharField(max_length=32, choices=METHOD_CHOICES, default="sql_copy")
    scope_mode = models.CharField(max_length=32, choices=SCOPE_CHOICES, default="database")

    source_database_name = models.CharField(max_length=120, blank=True, default="")
    database_include_csv = models.TextField(blank=True, default="")
    database_exclude_csv = models.TextField(blank=True, default="")
    database_like_pattern = models.CharField(max_length=120, blank=True, default="")

    include_tables_csv = models.TextField(blank=True, default="")
    exclude_tables_csv = models.TextField(blank=True, default="")
    like_filter = models.CharField(max_length=120, blank=True, default="")

    recreate_table = models.BooleanField(default=False)
    truncate_target = models.BooleanField(default=True)
    auto_exclude_srdf = models.BooleanField(default=True)
    validate_snapshot = models.BooleanField(default=False)

    parallel_workers = models.IntegerField(default=1)
    database_parallel_workers = models.IntegerField(default=1)
    table_limit = models.IntegerField(default=0)
    execution_order = models.IntegerField(default=100)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="draft")
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_duration_seconds = models.FloatField(default=0)
    last_total_databases = models.IntegerField(default=0)
    last_total_tables = models.IntegerField(default=0)
    last_success_count = models.IntegerField(default=0)
    last_failed_count = models.IntegerField(default=0)
    last_error = models.TextField(blank=True, default="")
    last_result_payload = models.JSONField(default=dict, blank=True)

    notes = models.TextField(blank=True, default="")

    selected_tables = models.ManyToManyField(
        "srdf.SourceTableCatalog",
        through="srdf.BootstrapPlanTableSelection",
        related_name="bootstrap_plans",
        blank=True,
    )

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name}"

    class Meta:
        ordering = ["execution_order", "id"]
        
        

class SourceDatabaseCatalog(models.Model):
    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="source_database_catalogs",
    )
    engine = models.CharField(max_length=32, blank=True, default="")
    database_name = models.CharField(max_length=120)
    is_active = models.BooleanField(default=True)
    discovered_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.database_name}"

    class Meta:
        ordering = ["database_name"]


class SourceTableCatalog(models.Model):
    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="source_table_catalogs",
    )
    source_database = models.ForeignKey(
        "srdf.SourceDatabaseCatalog",
        on_delete=models.CASCADE,
        related_name="table_catalogs",
    )
    table_name = models.CharField(max_length=120)
    is_active = models.BooleanField(default=True)
    discovered_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.source_database.database_name}.{self.table_name}"

    class Meta:
        ordering = ["source_database__database_name", "table_name"]


class BootstrapPlanTableSelection(models.Model):
    STATUS_CHOICES = [
        ("draft", "Draft"),
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
    ]

    bootstrap_plan = models.ForeignKey(
        "srdf.BootstrapPlan",
        on_delete=models.CASCADE,
        related_name="table_selections",
    )
    source_table = models.ForeignKey(
        "srdf.SourceTableCatalog",
        on_delete=models.CASCADE,
        related_name="bootstrap_plan_selections",
    )

    is_enabled = models.BooleanField(default=True)
    execution_order = models.IntegerField(default=100)

    recreate_table = models.BooleanField(default=False)
    truncate_target = models.BooleanField(default=True)
    validate_snapshot = models.BooleanField(default=False)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="draft")
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_duration_seconds = models.FloatField(default=0)
    last_error = models.TextField(blank=True, default="")
    last_result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.bootstrap_plan.name} -> {self.source_table}"

    class Meta:
        ordering = ["execution_order", "id"]
        
        
class BootstrapPlanExecution(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("cancelled", "Cancelled"),
    ]

    bootstrap_plan = models.ForeignKey(
        "srdf.BootstrapPlan",
        on_delete=models.CASCADE,
        related_name="executions",
    )

    parent_execution = models.ForeignKey(
        "srdf.BootstrapPlanExecution",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="replays",
    )

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="bootstrap_plan_executions",
    )


    initiated_by = models.CharField(max_length=120, blank=True, default="")
    trigger_mode = models.CharField(max_length=32, blank=True, default="manual")
    replay_mode = models.CharField(max_length=32, blank=True, default="")
    retry_failed_only = models.BooleanField(default=False)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")

    dry_run = models.BooleanField(default=False)

    total_databases = models.IntegerField(default=0)
    total_tables = models.IntegerField(default=0)
    success_count = models.IntegerField(default=0)
    failed_count = models.IntegerField(default=0)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    last_error = models.TextField(blank=True, default="")
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"BootstrapExecution #{self.id} - {self.bootstrap_plan.name}"

    class Meta:
        ordering = ["-id"]


class BootstrapPlanExecutionItem(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
    ]

    bootstrap_execution = models.ForeignKey(
        "srdf.BootstrapPlanExecution",
        on_delete=models.CASCADE,
        related_name="items",
    )

    bootstrap_plan = models.ForeignKey(
        "srdf.BootstrapPlan",
        on_delete=models.CASCADE,
        related_name="execution_items",
    )

    plan_table_selection = models.ForeignKey(
        "srdf.BootstrapPlanTableSelection",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="execution_items",
    )

    database_name = models.CharField(max_length=120, blank=True, default="")
    table_name = models.CharField(max_length=120, blank=True, default="")

    bootstrap_method = models.CharField(max_length=32, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")

    recreate_table = models.BooleanField(default=False)
    truncate_target = models.BooleanField(default=False)
    validate_snapshot = models.BooleanField(default=False)
    dry_run = models.BooleanField(default=False)

    copied_rows = models.BigIntegerField(default=0)
    source_row_count = models.BigIntegerField(default=0)
    target_row_count = models.BigIntegerField(default=0)

    source_checksum = models.CharField(max_length=255, blank=True, default="")
    target_checksum = models.CharField(max_length=255, blank=True, default="")
    snapshot_ok = models.BooleanField(default=True)

    table_created = models.BooleanField(default=False)
    table_truncated = models.BooleanField(default=False)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    error_message = models.TextField(blank=True, default="")
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return (
            f"ExecutionItem #{self.id} - "
            f"{self.database_name}.{self.table_name}"
        )

    class Meta:
        ordering = ["id"]
        
        
        
class SRDFOrchestratorRun(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("partial", "Partial"),
        ("stopped", "Stopped"),
    ]

    TRIGGER_CHOICES = [
        ("daemon", "Daemon"),
        ("manual", "Manual"),
        ("resume", "Resume"),
        ("recovery", "Recovery"),
    ]

    run_uid = models.CharField(max_length=120, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")
    trigger_mode = models.CharField(max_length=32, choices=TRIGGER_CHOICES, default="daemon")
    initiated_by = models.CharField(max_length=120, blank=True, default="")
    
    sleep_seconds = models.IntegerField(default=10)
    service_filter_csv = models.TextField(blank=True, default="")

    started_at = models.DateTimeField(auto_now_add=True)
    
    
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    total_services = models.IntegerField(default=0)
    success_services = models.IntegerField(default=0)
    failed_services = models.IntegerField(default=0)

    last_error = models.TextField(blank=True, default="")
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"SRDFRun {self.run_uid or self.id}"

    class Meta:
        ordering = ["-id"]


class SRDFServiceRuntimeState(models.Model):
    
    PHASE_CHOICES = [
        ("idle", "Idle"),
        ("capture", "Capture"),
        ("deduplicate", "Deduplicate"),
        ("compress", "Compress"),
        ("encrypt", "Encrypt"),
        ("batch_build", "Batch Build"),
        ("ship", "Ship"),
        ("wait_ack", "Wait Ack"),
        ("receive", "Receive"),
        ("apply", "Apply"),
        ("checkpoint", "Checkpoint"),
        ("bootstrap", "Bootstrap"),
        ("archive", "Archive"),
        ("paused", "Paused"),
        ("error", "Error"),
    ]
    

    replication_service = models.OneToOneField(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="runtime_state",
    )

    is_enabled = models.BooleanField(default=True)
    is_paused = models.BooleanField(default=False)
    stop_requested = models.BooleanField(default=False)

    current_phase = models.CharField(max_length=32, choices=PHASE_CHOICES, default="idle")
    last_success_phase = models.CharField(max_length=32, blank=True, default="")
    next_phase = models.CharField(max_length=32, blank=True, default="")

    cycle_number = models.BigIntegerField(default=0)
    consecutive_error_count = models.IntegerField(default=0)
    last_return_code = models.CharField(max_length=64, blank=True, default="")
    last_error = models.TextField(blank=True, default="")

    capture_checkpoint = models.JSONField(default=dict, blank=True)
    transport_checkpoint = models.JSONField(default=dict, blank=True)
    apply_checkpoint = models.JSONField(default=dict, blank=True)

    last_capture_at = models.DateTimeField(null=True, blank=True)
    last_deduplicate_at = models.DateTimeField(null=True, blank=True)
    last_compress_at = models.DateTimeField(null=True, blank=True)
    last_encrypt_at = models.DateTimeField(null=True, blank=True)
    last_batch_build_at = models.DateTimeField(null=True, blank=True)
    last_ship_at = models.DateTimeField(null=True, blank=True)
    last_wait_ack_at = models.DateTimeField(null=True, blank=True)
    last_receive_at = models.DateTimeField(null=True, blank=True)
    last_apply_at = models.DateTimeField(null=True, blank=True)
    last_checkpoint_at = models.DateTimeField(null=True, blank=True)
    last_bootstrap_at = models.DateTimeField(null=True, blank=True)

    last_run_started_at = models.DateTimeField(null=True, blank=True)
    last_run_finished_at = models.DateTimeField(null=True, blank=True)

    runtime_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"Runtime {self.replication_service.name}"

    class Meta:
        ordering = ["replication_service_id"]


class SRDFServicePhaseExecution(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("retry", "Retry"),
        ("failed", "Failed"),
        ("skipped", "Skipped"),
        ("stopped", "Stopped"),
    ]

    orchestrator_run = models.ForeignKey(
        "srdf.SRDFOrchestratorRun",
        on_delete=models.CASCADE,
        related_name="phase_executions",
    )

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="phase_executions",
    )

    runtime_state = models.ForeignKey(
        "srdf.SRDFServiceRuntimeState",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="phase_executions",
    )

    cycle_number = models.BigIntegerField(default=0)
    phase_name = models.CharField(max_length=32, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")
    return_code = models.CharField(max_length=64, blank=True, default="")

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    processed_count = models.BigIntegerField(default=0)
    success_count = models.BigIntegerField(default=0)
    failed_count = models.BigIntegerField(default=0)
    skipped_count = models.BigIntegerField(default=0)
    retry_after_seconds = models.IntegerField(default=0)

    last_error = models.TextField(blank=True, default="")
    request_payload = models.JSONField(default=dict, blank=True)
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.replication_service.name} - {self.phase_name} - {self.status}"

    class Meta:
        ordering = ["id"]


class SRDFServiceExecutionLock(models.Model):
    name = models.CharField(max_length=180, unique=True)
    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="service_locks",
    )

    is_locked = models.BooleanField(default=False)
    owner_token = models.CharField(max_length=120, blank=True, default="")
    locked_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} locked={self.is_locked}"

    class Meta:
        ordering = ["replication_service_id", "name"]
        
        
class SRDFSettingDefinition(models.Model):
    
    ENGINE_CHOICES = [
        ("all", "All"),
        ("mysql", "MySQL"),
        ("mariadb", "MariaDB"),
        ("postgresql", "PostgreSQL"),
        ("oracle", "Oracle"),
        ("filesync", "File Sync"),
        ("nginx", "Nginx/Proxy"),
    ]

    PHASE_CHOICES = [
        ("all", "All"),
        ("capture", "Capture"),
        ("deduplicate", "Deduplicate"),
        ("compress", "Compress"),
        ("encrypt", "Encrypt"),
        ("batch_build", "Batch Build"),
        ("ship", "Ship"),
        ("wait_ack", "Wait ACK"),
        ("receive", "Receive"),
        ("apply", "Apply"),
        ("checkpoint", "Checkpoint"),
        ("bootstrap", "Bootstrap"),
        ("archive", "Archive"),
        ("orchestrator", "Orchestrator"),
    ]

    VALUE_TYPE_CHOICES = [
        ("string", "String"),
        ("integer", "Integer"),
        ("float", "Float"),
        ("boolean", "Boolean"),
        ("json", "JSON"),
    ]

    CATEGORY_CHOICES = [
        ("capture", "Capture"),
        ("transport", "Transport"),
        ("apply", "Apply"),
        ("bootstrap", "Bootstrap"),
        ("orchestrator", "Orchestrator"),
        ("security", "Security"),
        ("performance", "Performance"),
        ("other", "Other"),
    ]

    key = models.CharField(max_length=150)
    label = models.CharField(max_length=200, blank=True, default="")
    description = models.TextField(blank=True, default="")

    category = models.CharField(max_length=32, choices=CATEGORY_CHOICES, default="other")
    phase = models.CharField(max_length=32, choices=PHASE_CHOICES, default="all")
    engine = models.CharField(max_length=32, choices=ENGINE_CHOICES, default="all")
    value_type = models.CharField(max_length=32, choices=VALUE_TYPE_CHOICES, default="string")

    default_value = models.TextField(blank=True, default="")
    default_value_json = models.JSONField(default=dict, blank=True)

    is_required = models.BooleanField(default=False)
    is_enabled = models.BooleanField(default=True)
    is_secret = models.BooleanField(default=False)
    is_system = models.BooleanField(default=False)

    sort_order = models.IntegerField(default=100)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.key}"

    class Meta:
        ordering = ["category", "phase", "engine", "sort_order", "id"]


class SRDFSettingValue(models.Model):
    SCOPE_TYPE_CHOICES = [
        ("global", "Global"),
        ("engine", "Engine"),
        ("company", "Company"),
        ("client", "Client"),
        ("service", "Replication Service"),
        ("node", "Node"),
    ]

    ENGINE_CHOICES = [
        ("all", "All"),
        ("mysql", "MySQL"),
        ("mariadb", "MariaDB"),
        ("postgresql", "PostgreSQL"),
        ("oracle", "Oracle"),
        ("filesync", "File Sync"),
        ("nginx", "Nginx/Proxy"),
    ]

    setting_definition = models.ForeignKey(
        "srdf.SRDFSettingDefinition",
        on_delete=models.CASCADE,
        related_name="values",
    )

    scope_type = models.CharField(max_length=32, choices=SCOPE_TYPE_CHOICES, default="global")
    engine = models.CharField(max_length=32, choices=ENGINE_CHOICES, default="all")

    company_name = models.CharField(max_length=150, blank=True, default="")
    client_name = models.CharField(max_length=150, blank=True, default="")

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="setting_values",
    )

    node = models.ForeignKey(
        "srdf.Node",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="setting_values",
    )

    value_text = models.TextField(blank=True, default="")
    value_json = models.JSONField(default=dict, blank=True)

    notes = models.TextField(blank=True, default="")

    priority = models.IntegerField(default=100)
    is_enabled = models.BooleanField(default=True)

    valid_from = models.DateTimeField(null=True, blank=True)
    valid_until = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return (
            f"{self.setting_definition.key} "
            f"[{self.scope_type}]"
        )

    class Meta:
        ordering = ["setting_definition__key", "priority", "id"]
        
class SRDFCronExecution(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("warning", "Warning"),
        ("error", "Error"),
        ("blocked", "Blocked"),
        ("timeout", "Timeout"),
        ("cancelled", "Cancelled"),
    ]

    CRON_TYPE_CHOICES = [
        ("capture", "Capture"),
        ("shipper", "Shipper"),
        ("receiver", "Receiver"),
        ("applier", "Applier"),
        ("bootstrap", "Bootstrap"),
        ("orchestrator", "Orchestrator"),
        ("other", "Other"),
    ]

    execution_uid = models.CharField(max_length=120, blank=True, default="")
    cron_name = models.CharField(max_length=150, blank=True, default="")
    cron_type = models.CharField(max_length=32, choices=CRON_TYPE_CHOICES, default="other")

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="cron_executions",
    )

    node = models.ForeignKey(
        "srdf.Node",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="cron_executions",
    )

    engine = models.CharField(max_length=32, blank=True, default="")
    command_line = models.TextField(blank=True, default="")
    initiated_by = models.CharField(max_length=120, blank=True, default="")

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")
    return_code = models.CharField(max_length=64, blank=True, default="")
    error_code = models.CharField(max_length=64, blank=True, default="")
    error_message = models.TextField(blank=True, default="")

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    input_params = models.JSONField(default=dict, blank=True)
    result_payload = models.JSONField(default=dict, blank=True)
    progress_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.cron_name or self.cron_type} [{self.status}]"

    class Meta:
        ordering = ["-id"]
        
        
        
class SRDFArchiveExecutionLock(models.Model):
    name = models.CharField(max_length=180, unique=True)

    is_locked = models.BooleanField(default=False)
    owner_token = models.CharField(max_length=120, blank=True, default="")
    locked_at = models.DateTimeField(null=True, blank=True)
    expires_at = models.DateTimeField(null=True, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.name} locked={self.is_locked}"

    class Meta:
        ordering = ["name"]


class SRDFArchiveRun(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("partial", "Partial"),
        ("failed", "Failed"),
        ("cancelled", "Cancelled"),
    ]

    TRIGGER_CHOICES = [
        ("manual", "Manual"),
        ("scheduled", "Scheduled"),
        ("orchestrator", "Orchestrator"),
        ("recovery", "Recovery"),
    ]

    run_uid = models.CharField(max_length=120, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")
    trigger_mode = models.CharField(max_length=32, choices=TRIGGER_CHOICES, default="scheduled")

    initiated_by = models.CharField(max_length=120, blank=True, default="")
    archive_database_name = models.CharField(max_length=150, blank=True, default="")
    retention_days = models.IntegerField(default=30)
    batch_size = models.IntegerField(default=1000)

    archive_before_datetime = models.DateTimeField(null=True, blank=True)

    total_tables = models.IntegerField(default=0)
    success_tables = models.IntegerField(default=0)
    failed_tables = models.IntegerField(default=0)

    total_rows_selected = models.BigIntegerField(default=0)
    total_rows_inserted = models.BigIntegerField(default=0)
    total_rows_deleted = models.BigIntegerField(default=0)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    last_error = models.TextField(blank=True, default="")
    request_payload = models.JSONField(default=dict, blank=True)
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"ArchiveRun {self.run_uid or self.id}"

    class Meta:
        ordering = ["-id"]


class SRDFArchiveRunItem(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("partial", "Partial"),
        ("failed", "Failed"),
        ("skipped", "Skipped"),
    ]

    archive_run = models.ForeignKey(
        "srdf.SRDFArchiveRun",
        on_delete=models.CASCADE,
        related_name="items",
    )

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="archive_items",
    )

    table_name = models.CharField(max_length=150)
    archive_model_label = models.CharField(max_length=150, blank=True, default="")
    archive_before_datetime = models.DateTimeField(null=True, blank=True)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")

    selected_count = models.BigIntegerField(default=0)
    inserted_count = models.BigIntegerField(default=0)
    deleted_count = models.BigIntegerField(default=0)
    skipped_count = models.BigIntegerField(default=0)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    last_error = models.TextField(blank=True, default="")
    request_payload = models.JSONField(default=dict, blank=True)
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.table_name} [{self.status}]"

    class Meta:
        ordering = ["archive_run_id", "id"]
        
        
class SRDFCaptureControlRun(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("warning", "Warning"),
        ("error", "Error"),
        ("blocked", "Blocked"),
        ("cancelled", "Cancelled"),
    ]

    MODE_CHOICES = [
        ("list_updates", "List updates"),
        ("check_updates", "Check updates"),
    ]

    run_uid = models.CharField(max_length=120, blank=True, default="")
    control_mode = models.CharField(max_length=32, choices=MODE_CHOICES, default="list_updates")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="capture_control_runs",
    )

    node = models.ForeignKey(
        "srdf.Node",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="capture_control_runs",
    )

    engine = models.CharField(max_length=32, blank=True, default="")
    initiated_by = models.CharField(max_length=120, blank=True, default="")
    command_line = models.TextField(blank=True, default="")

    checkpoint_start_file = models.CharField(max_length=255, blank=True, default="")
    checkpoint_start_pos = models.BigIntegerField(default=0)

    current_master_before_file = models.CharField(max_length=255, blank=True, default="")
    current_master_before_pos = models.BigIntegerField(default=0)

    current_master_after_file = models.CharField(max_length=255, blank=True, default="")
    current_master_after_pos = models.BigIntegerField(default=0)

    window_end_file = models.CharField(max_length=255, blank=True, default="")
    window_end_pos = models.BigIntegerField(default=0)

    total_binlog_updates = models.BigIntegerField(default=0)
    total_captured_matches = models.BigIntegerField(default=0)
    total_not_captured = models.BigIntegerField(default=0)
    total_duplicates = models.BigIntegerField(default=0)
    total_ambiguous = models.BigIntegerField(default=0)
    total_ignored = models.BigIntegerField(default=0)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    last_error = models.TextField(blank=True, default="")
    request_payload = models.JSONField(default=dict, blank=True)
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.control_mode} #{self.id} - {self.replication_service.name}"

    class Meta:
        ordering = ["-id"]


class SRDFCaptureControlItem(models.Model):
    STATUS_CHOICES = [
        ("not_captured", "Not captured"),
        ("captured", "Captured"),
        ("duplicate", "Duplicate"),
        ("ambiguous", "Ambiguous"),
        ("ignored", "Ignored"),
        ("error", "Error"),
    ]

    control_run = models.ForeignKey(
        "srdf.SRDFCaptureControlRun",
        on_delete=models.CASCADE,
        related_name="items",
    )

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="capture_control_items",
    )

    seq = models.BigIntegerField(default=0)

    log_file = models.CharField(max_length=255, blank=True, default="")
    log_pos = models.BigIntegerField(default=0)

    engine = models.CharField(max_length=32, blank=True, default="")
    database_name = models.CharField(max_length=120, blank=True, default="")
    table_name = models.CharField(max_length=120, blank=True, default="")
    operation = models.CharField(max_length=32, blank=True, default="")

    row_count = models.IntegerField(default=0)

    event_signature = models.CharField(max_length=255, blank=True, default="")
    outbound_match_count = models.IntegerField(default=0)
    first_outbound_event_id = models.BigIntegerField(default=0)

    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="not_captured")
    notes = models.TextField(blank=True, default="")
    payload_preview = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return (
            f"{self.control_run_id} #{self.seq} "
            f"{self.database_name}.{self.table_name} {self.operation}"
        )

    class Meta:
        ordering = ["control_run_id", "seq"]
        
        
        
class SRDFDedupRun(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("warning", "Warning"),
        ("failed", "Failed"),
    ]

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="dedup_runs",
    )

    orchestrator_run = models.ForeignKey(
        "srdf.SRDFOrchestratorRun",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="dedup_runs",
    )

    phase_execution = models.ForeignKey(
        "srdf.SRDFServicePhaseExecution",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="dedup_runs",
    )

    runtime_state = models.ForeignKey(
        "srdf.SRDFServiceRuntimeState",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="dedup_runs",
    )

    run_uid = models.CharField(max_length=120, blank=True, default="")
    initiated_by = models.CharField(max_length=120, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")

    processed_count = models.IntegerField(default=0)
    survivor_count = models.IntegerField(default=0)
    duplicate_count = models.IntegerField(default=0)
    exact_duplicate_count = models.IntegerField(default=0)
    neutralized_count = models.IntegerField(default=0)
    collapsed_count = models.IntegerField(default=0)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    last_error = models.TextField(blank=True, default="")
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"DedupRun #{self.id} - {self.replication_service.name}"

    class Meta:
        ordering = ["-id"]


class SRDFDedupDecision(models.Model):
    DECISION_CHOICES = [
        ("survivor", "Survivor"),
        ("exact_duplicate", "Exact duplicate"),
        ("collapsed", "Collapsed"),
        ("neutralized", "Neutralized"),
        ("passthrough", "Passthrough"),
    ]

    dedup_run = models.ForeignKey(
        "srdf.SRDFDedupRun",
        on_delete=models.CASCADE,
        related_name="decisions",
    )

    replication_service = models.ForeignKey(
        "srdf.ReplicationService",
        on_delete=models.CASCADE,
        related_name="dedup_decisions",
    )

    database_name = models.CharField(max_length=120, blank=True, default="")
    table_name = models.CharField(max_length=120, blank=True, default="")
    operation = models.CharField(max_length=32, blank=True, default="")

    decision_type = models.CharField(max_length=32, choices=DECISION_CHOICES, default="passthrough")
    reason_code = models.CharField(max_length=64, blank=True, default="")

    event_id = models.BigIntegerField(default=0)
    survivor_event_id = models.BigIntegerField(default=0)

    log_file = models.CharField(max_length=255, blank=True, default="")
    log_pos = models.BigIntegerField(default=0)

    group_key = models.TextField(blank=True, default="")
    business_signature = models.CharField(max_length=255, blank=True, default="")

    primary_key_data = models.JSONField(default=dict, blank=True)
    before_data = models.JSONField(default=dict, blank=True)
    after_data = models.JSONField(default=dict, blank=True)
    payload_preview = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"DedupDecision #{self.id} - {self.decision_type}"

    class Meta:
        ordering = ["id"]
        
        
class SRDFInstallProfile(models.Model):
    INSTALL_SCOPE_CHOICES = [
        ("remote_control", "Remote control plane only"),
        ("full_remote", "Full remote install"),
    ]

    SERVICE_TYPE_CHOICES = [
        ("postgresql", "PostgreSQL"),
        ("mysql", "MySQL"),
        ("mariadb", "MariaDB"),
        ("filesync", "File Sync"),
        ("nginx", "Nginx/Proxy"),
    ]

    MODE_CHOICES = [
        ("sync", "Synchronous"),
        ("async", "Asynchronous"),
        ("manual", "Manual"),
    ]

    name = models.CharField(max_length=150)
    is_enabled = models.BooleanField(default=True)

    install_scope = models.CharField(
        max_length=32,
        choices=INSTALL_SCOPE_CHOICES,
        default="remote_control",
    )

    db_host = models.CharField(max_length=255, default="127.0.0.1")
    db_port = models.IntegerField(default=3307)
    db_user = models.CharField(max_length=120, default="root")
    db_password = models.CharField(max_length=255, blank=True, default="")

    control_db_name = models.CharField(max_length=150, default="srdf_remote_control")
    archive_db_name = models.CharField(max_length=150, default="srdf_remote_archive")
    target_db_name = models.CharField(max_length=150, default="target_db")

    source_node_name = models.CharField(max_length=120, default="mysql-source")
    source_hostname = models.CharField(max_length=255, default="localhost")
    source_db_port = models.IntegerField(default=3306)
    source_site = models.CharField(max_length=120, blank=True, default="site-a")
    source_api_base_url = models.CharField(max_length=255, blank=True, default="")

    target_node_name = models.CharField(max_length=120, default="mariadb-target")
    target_hostname = models.CharField(max_length=255, default="localhost")
    target_db_port = models.IntegerField(default=3307)
    target_site = models.CharField(max_length=120, blank=True, default="site-b")
    target_api_base_url = models.CharField(max_length=255, blank=True, default="")

    api_token = models.CharField(max_length=255, blank=True, default="")

    service_name = models.CharField(max_length=150, default="Mariadb-replica")
    service_type = models.CharField(
        max_length=32,
        choices=SERVICE_TYPE_CHOICES,
        default="mariadb",
    )
    mode = models.CharField(
        max_length=16,
        choices=MODE_CHOICES,
        default="async",
    )

    source_db_name = models.CharField(max_length=150, default="oxygen_db6")
    source_db_user = models.CharField(max_length=120, default="root")
    source_db_password = models.CharField(max_length=255, blank=True, default="")
    binlog_server_id = models.IntegerField(default=900001)

    run_migrate = models.BooleanField(default=True)
    seed_settings = models.BooleanField(default=True)
    create_archive_db = models.BooleanField(default=True)
    auto_create_target_db = models.BooleanField(default=True)

    status = models.CharField(max_length=32, default="draft")
    last_run_at = models.DateTimeField(null=True, blank=True)
    last_error = models.TextField(blank=True, default="")
    last_result_payload = models.JSONField(default=dict, blank=True)

    notes = models.TextField(blank=True, default="")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return self.name

    class Meta:
        ordering = ["name", "id"]


class SRDFInstallRun(models.Model):
    STATUS_CHOICES = [
        ("running", "Running"),
        ("success", "Success"),
        ("failed", "Failed"),
        ("cancelled", "Cancelled"),
    ]

    install_profile = models.ForeignKey(
        "srdf.SRDFInstallProfile",
        on_delete=models.CASCADE,
        related_name="install_runs",
    )

    run_uid = models.CharField(max_length=120, blank=True, default="")
    status = models.CharField(max_length=32, choices=STATUS_CHOICES, default="running")
    initiated_by = models.CharField(max_length=120, blank=True, default="")

    control_db_name = models.CharField(max_length=150, blank=True, default="")
    archive_db_name = models.CharField(max_length=150, blank=True, default="")
    target_db_name = models.CharField(max_length=150, blank=True, default="")

    source_node_id_value = models.BigIntegerField(default=0)
    target_node_id_value = models.BigIntegerField(default=0)
    replication_service_id_value = models.BigIntegerField(default=0)

    started_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    duration_seconds = models.FloatField(default=0)

    last_error = models.TextField(blank=True, default="")
    result_payload = models.JSONField(default=dict, blank=True)

    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"InstallRun #{self.id} - {self.install_profile.name}"

    class Meta:
        ordering = ["-id"]