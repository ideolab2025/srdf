#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from django.apps import apps
from django.db import connections


class ModelMigrationDoctorService:
    """
    Technical migration audit for a single Django model.

    Scope:
    - Django model structure
    - Real DB table / columns / constraints / indexes
    - Risk detection for migrations
    - Suggested actions
    """

    LONG_INDEX_THRESHOLD = 191

    def __init__(self, model, using="default"):
        self.model = model
        self.using = using
        self.connection = connections[using]
        self.meta = model._meta

    def analyze(self):
        django_fields = self._collect_django_fields()
        relation_entries = self._collect_relation_entries()
        db_snapshot = self._collect_db_snapshot()

        schema_diff = self._build_schema_diff(
            django_fields=django_fields,
            db_snapshot=db_snapshot,
        )

        index_risks = self._build_index_risks(
            django_fields=django_fields,
            db_snapshot=db_snapshot,
        )

        relation_risks = self._build_relation_risks(
            relation_entries=relation_entries,
        )

        migration_impact = self._build_migration_impact(
            relation_entries=relation_entries,
            index_risks=index_risks,
            schema_diff=schema_diff,
        )

        recommended_actions = self._build_recommended_actions(
            schema_diff=schema_diff,
            index_risks=index_risks,
            relation_risks=relation_risks,
            migration_impact=migration_impact,
        )

        severity = self._compute_severity(
            schema_diff=schema_diff,
            index_risks=index_risks,
            relation_risks=relation_risks,
            migration_impact=migration_impact,
        )

        return {
            "app_label": self.meta.app_label,
            "model_name": self.meta.model_name,
            "object_name": self.meta.object_name,
            "db_table": self.meta.db_table,
            "database_alias": self.using,
            "row_count": self._safe_row_count(),
            "django_fields": django_fields,
            "relation_entries": relation_entries,
            "db_snapshot": db_snapshot,
            "schema_diff": schema_diff,
            "index_risks": index_risks,
            "relation_risks": relation_risks,
            "migration_impact": migration_impact,
            "recommended_actions": recommended_actions,
            "severity": severity,
        }

    def _safe_row_count(self):
        try:
            return int(self.model.objects.using(self.using).count())
        except Exception:
            return -1

    def _collect_django_fields(self):
        rows = []

        for field in self.meta.get_fields():
            if getattr(field, "auto_created", False) and not getattr(field, "concrete", False):
                continue

            try:
                internal_type = field.get_internal_type()
            except Exception:
                internal_type = field.__class__.__name__

            relation_type = "-"
            if getattr(field, "many_to_many", False):
                relation_type = "M2M"
            elif getattr(field, "one_to_one", False):
                relation_type = "O2O"
            elif getattr(field, "many_to_one", False):
                relation_type = "FK"
            elif getattr(field, "one_to_many", False):
                relation_type = "REL"

            rows.append({
                "name": getattr(field, "name", ""),
                "column": getattr(field, "column", None),
                "type": internal_type,
                "null": bool(getattr(field, "null", False)),
                "blank": bool(getattr(field, "blank", False)),
                "primary_key": bool(getattr(field, "primary_key", False)),
                "unique": bool(getattr(field, "unique", False)),
                "db_index": bool(getattr(field, "db_index", False)),
                "max_length": getattr(field, "max_length", None),
                "relation_type": relation_type,
                "concrete": bool(getattr(field, "concrete", False)),
            })

        return rows

    def _collect_relation_entries(self):
        rows = []
        seen = set()

        current_model_label = f"{self.meta.app_label}.{self.meta.model_name}"

        for field in self.meta.get_fields():
            if not getattr(field, "is_relation", False):
                continue

            related_model = getattr(field, "related_model", None)
            if related_model is None or related_model == self.model:
                continue

            relation_type = "REL"
            if getattr(field, "many_to_many", False):
                relation_type = "M2M"
            elif getattr(field, "one_to_one", False):
                relation_type = "O2O"
            elif getattr(field, "many_to_one", False):
                relation_type = "FK"

            related_model_label = f"{related_model._meta.app_label}.{related_model._meta.model_name}"
            field_name = str(getattr(field, "name", "") or "")
            concrete = bool(getattr(field, "concrete", False))

            if concrete:
                left_side = f"{current_model_label}.{field_name}"
                right_side = f"{related_model_label}.id"
            else:
                left_side = f"{related_model_label}.{field_name}"
                right_side = f"{current_model_label}.id"

            key = (relation_type, left_side, right_side)
            if key in seen:
                continue
            seen.add(key)

            rows.append({
                "relation_type": relation_type,
                "field_name": field_name,
                "related_model_label": related_model_label,
                "left_side": left_side,
                "right_side": right_side,
                "is_concrete": concrete,
            })

        rows.sort(key=lambda x: (x["relation_type"], x["related_model_label"], x["field_name"]))
        return rows

    def _collect_db_snapshot(self):
        result = {
            "table_exists": False,
            "columns": [],
            "column_names": [],
            "constraints": {},
            "indexes": [],
            "primary_key_columns": [],
            "unique_columns": [],
            "foreign_key_columns": [],
            "errors": [],
        }

        table_name = self.meta.db_table

        try:
            existing_tables = self.connection.introspection.table_names(self.connection.cursor())
        except Exception as exc:
            result["errors"].append(f"table_names introspection failed: {exc}")
            return result

        if table_name not in existing_tables:
            return result

        result["table_exists"] = True

        try:
            with self.connection.cursor() as cursor:
                description = self.connection.introspection.get_table_description(cursor, table_name)
                constraints = self.connection.introspection.get_constraints(cursor, table_name)
        except Exception as exc:
            result["errors"].append(f"table introspection failed: {exc}")
            return result

        for col in description:
            row = {
                "name": getattr(col, "name", None),
                "null_ok": bool(getattr(col, "null_ok", False)),
                "internal_size": getattr(col, "internal_size", None),
                "precision": getattr(col, "precision", None),
                "scale": getattr(col, "scale", None),
                "default": getattr(col, "default", None),
            }
            result["columns"].append(row)
            if row["name"]:
                result["column_names"].append(row["name"])

        result["constraints"] = constraints

        indexes = []
        pk_cols = []
        unique_cols = []
        fk_cols = []

        for constraint_name, payload in constraints.items():
            columns = list(payload.get("columns") or [])
            if payload.get("index"):
                indexes.append({
                    "name": constraint_name,
                    "columns": columns,
                    "unique": bool(payload.get("unique", False)),
                    "primary_key": bool(payload.get("primary_key", False)),
                    "foreign_key": payload.get("foreign_key"),
                })

            if payload.get("primary_key"):
                pk_cols.extend(columns)

            if payload.get("unique"):
                unique_cols.extend(columns)

            if payload.get("foreign_key"):
                fk_cols.extend(columns)

        result["indexes"] = indexes
        result["primary_key_columns"] = sorted(set(pk_cols))
        result["unique_columns"] = sorted(set(unique_cols))
        result["foreign_key_columns"] = sorted(set(fk_cols))

        return result

    def _build_schema_diff(self, django_fields, db_snapshot):
        django_columns = []
        for row in django_fields:
            if row["concrete"] and row["column"]:
                django_columns.append(row["column"])

        django_column_set = set(django_columns)
        db_column_set = set(db_snapshot["column_names"])

        missing_in_db = sorted(django_column_set - db_column_set)
        extra_in_db = sorted(db_column_set - django_column_set)

        missing_table = not db_snapshot["table_exists"]

        return {
            "missing_table": missing_table,
            "django_column_count": len(django_column_set),
            "db_column_count": len(db_column_set),
            "missing_in_db": missing_in_db,
            "extra_in_db": extra_in_db,
        }

    def _build_index_risks(self, django_fields, db_snapshot):
        risky_indexed_fields = []
        risky_unique_fields = []
        risky_model_indexes = []
        missing_fk_indexes = []

        for row in django_fields:
            field_type = row["type"]
            max_length = row["max_length"]

            if row["db_index"] and field_type in {"CharField", "SlugField", "URLField"}:
                if max_length and int(max_length) > self.LONG_INDEX_THRESHOLD:
                    risky_indexed_fields.append(
                        f'{row["name"]} ({field_type}({max_length}) db_index=True)'
                    )

            if row["unique"] and field_type in {"CharField", "SlugField", "URLField"}:
                if max_length and int(max_length) > self.LONG_INDEX_THRESHOLD:
                    risky_unique_fields.append(
                        f'{row["name"]} ({field_type}({max_length}) unique=True)'
                    )

            if row["db_index"] and field_type == "TextField":
                risky_indexed_fields.append(
                    f'{row["name"]} (TextField db_index=True)'
                )

            if row["relation_type"] == "FK" and row["column"]:
                if row["column"] not in set(db_snapshot["foreign_key_columns"]) and row["column"] not in self._all_indexed_columns(db_snapshot):
                    missing_fk_indexes.append(
                        f'{row["name"]} -> column {row["column"]}'
                    )

        for index in getattr(self.meta, "indexes", []) or []:
            dangerous_fields = []

            for field_name in list(getattr(index, "fields", []) or []):
                clean_name = field_name.lstrip("-")
                try:
                    django_field = self.meta.get_field(clean_name)
                except Exception:
                    continue

                field_type = django_field.__class__.__name__
                max_length = getattr(django_field, "max_length", None)

                if field_type == "TextField":
                    dangerous_fields.append(clean_name)
                elif field_type in {"CharField", "SlugField", "URLField"}:
                    if max_length and int(max_length) > self.LONG_INDEX_THRESHOLD:
                        dangerous_fields.append(clean_name)

            if dangerous_fields:
                risky_model_indexes.append({
                    "name": getattr(index, "name", "<unnamed_index>"),
                    "fields": dangerous_fields,
                })

        return {
            "risky_indexed_fields": risky_indexed_fields,
            "risky_unique_fields": risky_unique_fields,
            "risky_model_indexes": risky_model_indexes,
            "missing_fk_indexes": missing_fk_indexes,
        }

    def _all_indexed_columns(self, db_snapshot):
        names = set()
        for item in db_snapshot["indexes"]:
            for col in item["columns"]:
                names.add(col)
        return names

    def _build_relation_risks(self, relation_entries):
        inbound_rel_count = 0
        outbound_fk_count = 0
        m2m_count = 0

        for row in relation_entries:
            if row["relation_type"] == "FK" and row["left_side"].startswith(f"{self.meta.app_label}.{self.meta.model_name}."):
                outbound_fk_count += 1
            elif row["relation_type"] == "M2M":
                m2m_count += 1
            else:
                inbound_rel_count += 1

        warnings = []
        if outbound_fk_count >= 4:
            warnings.append("High outbound FK density")
        if inbound_rel_count >= 6:
            warnings.append("High inbound dependency density")
        if len(relation_entries) >= 10:
            warnings.append("Model is a dependency hub")

        return {
            "outbound_fk_count": outbound_fk_count,
            "inbound_rel_count": inbound_rel_count,
            "m2m_count": m2m_count,
            "warnings": warnings,
        }

    def _build_migration_impact(self, relation_entries, index_risks, schema_diff):
        score = 0
        notes = []

        if schema_diff["missing_table"]:
            score += 5
            notes.append("Table does not exist in database")

        if schema_diff["missing_in_db"]:
            score += 4
            notes.append("Some Django columns are missing in database")

        if index_risks["risky_indexed_fields"]:
            score += 2
            notes.append("Risky indexed fields detected")

        if index_risks["risky_unique_fields"]:
            score += 2
            notes.append("Risky unique fields detected")

        if index_risks["risky_model_indexes"]:
            score += 3
            notes.append("Dangerous composite indexes detected")

        if len(relation_entries) >= 8:
            score += 2
            notes.append("High relation density increases migration fragility")

        level = "LOW"
        if score >= 8:
            level = "HIGH"
        elif score >= 4:
            level = "MEDIUM"

        return {
            "impact_score": score,
            "impact_level": level,
            "notes": notes,
        }

    def _build_recommended_actions(self, schema_diff, index_risks, relation_risks, migration_impact):
        actions = []

        if schema_diff["missing_table"]:
            actions.append("Verify whether the target table should already exist before running migrate")
        if schema_diff["missing_in_db"]:
            actions.append("Compare the real DB schema with the last applied migration before any new migrate")
        if index_risks["risky_indexed_fields"]:
            actions.append("Remove db_index=True from long text-like fields and replace with a short technical hash if needed")
        if index_risks["risky_unique_fields"]:
            actions.append("Review unique=True on long fields and consider a short technical key")
        if index_risks["risky_model_indexes"]:
            actions.append("Split CreateModel and AddIndex into separate migrations")
        if index_risks["missing_fk_indexes"]:
            actions.append("Verify foreign-key supporting indexes in the real database")
        if relation_risks["warnings"]:
            actions.append("Review FK ordering and migration dependency chain before applying follow-up migrations")
        if migration_impact["impact_level"] == "HIGH":
            actions.append("Generate and review SQL plan before migrate, and prepare backup first")

        if not actions:
            actions.append("No critical migration action suggested for this model at this stage")

        return actions

    def _compute_severity(self, schema_diff, index_risks, relation_risks, migration_impact):
        if schema_diff["missing_table"] or schema_diff["missing_in_db"]:
            return "CRITICAL"

        if (
            index_risks["risky_indexed_fields"]
            or index_risks["risky_unique_fields"]
            or index_risks["risky_model_indexes"]
            or relation_risks["warnings"]
            or migration_impact["impact_level"] == "HIGH"
        ):
            return "WARNING"

        return "OK"