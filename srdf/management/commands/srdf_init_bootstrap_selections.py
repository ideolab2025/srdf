#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import (
    BootstrapPlan,
    BootstrapPlanTableSelection,
    SQLObjectScopeRule,
    SourceTableCatalog,
)


class Command(BaseCommand):
    help = "Initialize BootstrapPlanTableSelection from SourceTableCatalog"

    def add_arguments(self, parser):
        parser.add_argument("--plan-id", type=int, default=0)
        parser.add_argument("--plan-name", type=str, default="")
        parser.add_argument(
            "--disable-missing",
            action="store_true",
            help="Disable existing selections not present anymore in source catalog scope",
        )
        parser.add_argument(
            "--reset-orders",
            action="store_true",
            help="Recompute execution_order from scratch",
        )

    def handle(self, *args, **options):
        plan = self._resolve_plan(
            plan_id=int(options.get("plan_id") or 0),
            plan_name=(options.get("plan_name") or "").strip(),
        )
        if not plan:
            raise CommandError("BootstrapPlan not found")

        disable_missing = bool(options.get("disable_missing"))
        reset_orders = bool(options.get("reset_orders"))

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write("SRDF INIT BOOTSTRAP SELECTIONS")
        self.stdout.write("============================================================")
        self.stdout.write(f"plan_id          : {plan.id}")
        self.stdout.write(f"plan_name        : {plan.name}")
        self.stdout.write(f"scope_mode       : {plan.scope_mode}")
        self.stdout.write(f"source_database  : {plan.source_database_name or '-'}")
        self.stdout.write(f"disable_missing  : {disable_missing}")
        self.stdout.write(f"reset_orders     : {reset_orders}")
        self.stdout.write("")

        source_tables = self._load_candidate_tables(plan)
        included_tables = []
        skipped_count = 0

        for source_table in source_tables:
            if self._is_excluded_by_scope_rules(plan, source_table):
                skipped_count += 1
                continue

            included_tables.append(source_table)

        created_count = 0
        updated_count = 0
        active_ids = []
        execution_order = 100

        for source_table in included_tables:
            obj, created = BootstrapPlanTableSelection.objects.update_or_create(
                bootstrap_plan=plan,
                source_table=source_table,
                defaults={
                    "is_enabled": True,
                    "execution_order": execution_order if reset_orders else self._resolve_execution_order(plan, source_table, execution_order),
                    "recreate_table": plan.recreate_table,
                    "truncate_target": plan.truncate_target,
                    "validate_snapshot": plan.validate_snapshot,
                },
            )

            active_ids.append(obj.id)

            if created:
                created_count += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"[CREATED] {source_table.source_database.database_name}.{source_table.table_name}"
                    )
                )
            else:
                updated_count += 1
                self.stdout.write(
                    self.style.WARNING(
                        f"[UPDATED] {source_table.source_database.database_name}.{source_table.table_name}"
                    )
                )

            execution_order += 10

        disabled_count = 0
        if disable_missing:
            qs = BootstrapPlanTableSelection.objects.filter(bootstrap_plan=plan).exclude(id__in=active_ids)
            for obj in qs:
                if obj.is_enabled:
                    obj.is_enabled = False
                    obj.save(update_fields=["is_enabled", "updated_at"])
                    disabled_count += 1
                    self.stdout.write(
                        self.style.WARNING(
                            f"[DISABLED] {obj.source_table.source_database.database_name}.{obj.source_table.table_name}"
                        )
                    )

        self.stdout.write("")
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS("SRDF INIT BOOTSTRAP SELECTIONS - DONE"))
        self.stdout.write(self.style.SUCCESS("============================================================"))
        self.stdout.write(self.style.SUCCESS(f"source_tables_scanned = {len(source_tables)}"))
        self.stdout.write(self.style.SUCCESS(f"included_tables       = {len(included_tables)}"))
        self.stdout.write(self.style.SUCCESS(f"skipped_tables        = {skipped_count}"))
        self.stdout.write(self.style.SUCCESS(f"created               = {created_count}"))
        self.stdout.write(self.style.SUCCESS(f"updated               = {updated_count}"))
        self.stdout.write(self.style.SUCCESS(f"disabled              = {disabled_count}"))
        self.stdout.write("")

    def _resolve_plan(self, plan_id=0, plan_name=""):
        qs = BootstrapPlan.objects.select_related("replication_service").order_by("id")

        if plan_id:
            return qs.filter(id=plan_id).first()

        if plan_name:
            return qs.filter(name=plan_name).first()

        return None

    def _load_candidate_tables(self, plan):
        qs = (
            SourceTableCatalog.objects
            .select_related("source_database", "replication_service")
            .filter(
                replication_service=plan.replication_service,
                is_active=True,
                source_database__is_active=True,
            )
            .order_by("source_database__database_name", "table_name")
        )

        if plan.source_database_name:
            qs = qs.filter(source_database__database_name=plan.source_database_name)

        if plan.scope_mode == "tables":
            include_csv = self._csv_to_list(plan.include_tables_csv)
            if include_csv:
                qs = qs.filter(table_name__in=include_csv)

        if plan.scope_mode == "pattern":
            if plan.like_filter:
                needle = plan.like_filter.strip()
                if needle:
                    qs = qs.filter(table_name__icontains=needle.replace("%", ""))

        exclude_csv = self._csv_to_list(plan.exclude_tables_csv)
        if exclude_csv:
            qs = qs.exclude(table_name__in=exclude_csv)

        return list(qs)

    def _is_excluded_by_scope_rules(self, plan, source_table):
        rules = (
            SQLObjectScopeRule.objects
            .filter(is_enabled=True)
            .order_by("priority", "id")
        )

        db_name = source_table.source_database.database_name
        table_name = source_table.table_name
        engine = plan.replication_service.service_type or "all"

        for rule in rules:
            if rule.engine not in ("all", engine):
                continue

            if rule.object_type == "database":
                if self._match_value(rule.match_mode, db_name, rule.database_name):
                    return rule.policy_mode == "exclude"

            elif rule.object_type == "table":
                database_ok = True
                if rule.database_name:
                    database_ok = self._match_value(rule.match_mode, db_name, rule.database_name)

                table_ok = self._match_value(rule.match_mode, table_name, rule.table_name)

                if database_ok and table_ok:
                    return rule.policy_mode == "exclude"

        return False

    def _match_value(self, match_mode, current_value, rule_value):
        current_value = (current_value or "").strip()
        rule_value = (rule_value or "").strip()

        if not rule_value:
            return False

        if match_mode == "exact":
            return current_value == rule_value

        if match_mode == "prefix":
            return current_value.startswith(rule_value)

        if match_mode == "contains":
            return rule_value in current_value

        return False

    def _csv_to_list(self, raw_value):
        if not raw_value:
            return []
        return [x.strip() for x in str(raw_value).split(",") if x.strip()]

    def _resolve_execution_order(self, plan, source_table, default_value):
        existing = (
            BootstrapPlanTableSelection.objects
            .filter(bootstrap_plan=plan, source_table=source_table)
            .order_by("id")
            .first()
        )
        if existing and existing.execution_order:
            return existing.execution_order
        return default_value