#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json
import re

import pymysql

from django.core.management.base import BaseCommand, CommandError
from django.db.models import Max
from django.utils import timezone

from srdf.models import (
    AuditEvent,
    OutboundChangeEvent,
    ReplicationCheckpoint,
    ReplicationService,
    TransportBatch,
)
from srdf.services.binlog_capture import capture_binlog_once
from srdf.services.transport_batch_builder import build_transport_batch_once
from srdf.services.transport_sender import send_transport_batch_once


class Command(BaseCommand):
    help = "Official SRDF validation test runner for source update -> capture -> build batch -> send batch"

    def add_arguments(self, parser):
        parser.add_argument(
            "--service-id",
            type=int,
            required=True,
            help="ReplicationService ID (mariadb)",
        )
        parser.add_argument(
            "--table",
            type=str,
            required=True,
            help="Source MariaDB table name used for the validation test",
        )
        parser.add_argument(
            "--pk-field",
            type=str,
            default="id",
            help="Primary key column name",
        )
        parser.add_argument(
            "--pk-value",
            required=True,
            help="Primary key value of the row to update",
        )
        parser.add_argument(
            "--update-column",
            type=str,
            required=True,
            help="Column to update during the validation test",
        )
        parser.add_argument(
            "--update-value",
            type=str,
            default="",
            help="Optional fixed value. If omitted, an automatic SRDF_TEST_* value is generated",
        )
        parser.add_argument(
            "--capture-limit",
            type=int,
            default=100,
            help="Maximum number of events to capture in the capture phase",
        )
        parser.add_argument(
            "--build-limit",
            type=int,
            default=100,
            help="Maximum number of outbound events to include in the batch build phase",
        )
        parser.add_argument(
            "--skip-send",
            action="store_true",
            help="Skip phase 4 (send batch)",
        )
        parser.add_argument(
            "--strict-send",
            action="store_true",
            help="Fail the test if the batch is not actually sent",
        )

    def handle(self, *args, **options):
        service = self._get_service(options["service_id"])
        table_name = self._validate_identifier(options["table"], "table")
        pk_field = self._validate_identifier(options["pk_field"], "pk-field")
        update_column = self._validate_identifier(options["update_column"], "update-column")
        pk_value = options["pk_value"]
        capture_limit = int(options["capture_limit"])
        build_limit = int(options["build_limit"])
        skip_send = bool(options["skip_send"])
        strict_send = bool(options["strict_send"])

        update_value = options["update_value"].strip()
        if not update_value:
            update_value = self._generate_update_value()

        source_db_config = self._get_source_db_config(service)

        started_at = timezone.now()

        initial_snapshot = self._collect_initial_snapshot(service)

        report = {
            "ok": True,
            "started_at": started_at.isoformat(),
            "service_id": service.id,
            "service_name": service.name,
            "source_node": service.source_node.name if service.source_node_id else "",
            "target_node": service.target_node.name if service.target_node_id else "",
            "table": table_name,
            "pk_field": pk_field,
            "pk_value": str(pk_value),
            "update_column": update_column,
            "update_value": update_value,
            "phases": {},
        }

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF TEST VALIDATION - START"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"service_id={service.id} service_name={service.name}")
        self.stdout.write(f"source_node={service.source_node.name if service.source_node_id else '-'}")
        self.stdout.write(f"target_node={service.target_node.name if service.target_node_id else '-'}")
        self.stdout.write(f"table={table_name} pk={pk_field}={pk_value} update_column={update_column}")
        self.stdout.write("")

        try:
            # ---------------------------------------------------------
            # PHASE 0 - PREPARATION
            # ---------------------------------------------------------
            phase_0 = self._run_phase_0_preparation(
                service=service,
                source_db_config=source_db_config,
                table_name=table_name,
                pk_field=pk_field,
                pk_value=pk_value,
                initial_snapshot=initial_snapshot,
            )
            report["phases"]["phase_0_preparation"] = phase_0
            self._print_phase_result("PHASE 0 - PREPARATION", phase_0)

            if not phase_0["ok"]:
                raise CommandError("Phase 0 failed")

            # ---------------------------------------------------------
            # PHASE 1 - SOURCE UPDATE
            # ---------------------------------------------------------
            phase_1 = self._run_phase_1_source_update(
                source_db_config=source_db_config,
                table_name=table_name,
                pk_field=pk_field,
                pk_value=pk_value,
                update_column=update_column,
                update_value=update_value,
            )
            report["phases"]["phase_1_source_update"] = phase_1
            self._print_phase_result("PHASE 1 - SOURCE UPDATE", phase_1)

            if not phase_1["ok"]:
                raise CommandError("Phase 1 failed")

            # ---------------------------------------------------------
            # PHASE 2 - CAPTURE
            # ---------------------------------------------------------
            phase_2 = self._run_phase_2_capture(
                service=service,
                initial_snapshot=initial_snapshot,
                table_name=table_name,
                pk_value=pk_value,
                capture_limit=capture_limit,
            )
            report["phases"]["phase_2_capture"] = phase_2
            self._print_phase_result("PHASE 2 - CAPTURE", phase_2)

            if not phase_2["ok"]:
                raise CommandError("Phase 2 failed")

            # ---------------------------------------------------------
            # PHASE 3 - BUILD BATCH
            # ---------------------------------------------------------
            phase_3 = self._run_phase_3_build_batch(
                service=service,
                initial_snapshot=initial_snapshot,
                outbound_event_id=phase_2.get("matched_outbound_event_id"),
                build_limit=build_limit,
            )
            report["phases"]["phase_3_build_batch"] = phase_3
            self._print_phase_result("PHASE 3 - BUILD BATCH", phase_3)

            if not phase_3["ok"]:
                raise CommandError("Phase 3 failed")

            # ---------------------------------------------------------
            # PHASE 4 - SEND BATCH
            # ---------------------------------------------------------
            if skip_send:
                phase_4 = {
                    "ok": True,
                    "skipped": True,
                    "message": "Phase 4 skipped by --skip-send",
                }
            else:
                phase_4 = self._run_phase_4_send_batch(
                    batch_id=phase_3.get("transport_batch_id"),
                    strict_send=strict_send,
                )

            report["phases"]["phase_4_send_batch"] = phase_4
            self._print_phase_result("PHASE 4 - SEND BATCH", phase_4)

            if not phase_4["ok"]:
                report["ok"] = False

        except Exception as exc:
            report["ok"] = False
            report["fatal_error"] = str(exc)
            self.stdout.write("")
            self.stdout.write(self.style.ERROR(f"FATAL ERROR: {exc}"))

        finished_at = timezone.now()
        report["finished_at"] = finished_at.isoformat()
        report["duration_seconds"] = round((finished_at - started_at).total_seconds(), 3)

        summary = self._build_summary(report)
        report["summary"] = summary

        self._create_final_audit_event(service, report)

        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF TEST VALIDATION - FINAL SUMMARY"))
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(f"global_ok={report['ok']}")
        self.stdout.write(f"duration_seconds={report['duration_seconds']}")
        self.stdout.write(f"phases_ok={summary['phases_ok']}")
        self.stdout.write(f"phases_failed={summary['phases_failed']}")
        self.stdout.write(f"outbound_new_count={summary['outbound_new_count']}")
        self.stdout.write(f"transport_batch_new_count={summary['transport_batch_new_count']}")
        self.stdout.write(f"audit_new_count={summary['audit_new_count']}")
        self.stdout.write(f"checkpoint_advanced={summary['checkpoint_advanced']}")
        self.stdout.write("")
        self.stdout.write("JSON_REPORT_BEGIN")
        self.stdout.write(json.dumps(report, indent=2, ensure_ascii=False))
        self.stdout.write("JSON_REPORT_END")
        self.stdout.write("")
        self.stdout.write(self.style.WARNING("============================================================"))
        self.stdout.write(self.style.WARNING("SRDF TEST VALIDATION - END"))
        self.stdout.write(self.style.WARNING("============================================================"))

        if not report["ok"]:
            raise CommandError("SRDF test validation failed")

    # -----------------------------------------------------------------
    # PHASES
    # -----------------------------------------------------------------

    def _run_phase_0_preparation(self, service, source_db_config, table_name, pk_field, pk_value, initial_snapshot):
        row_exists = self._source_row_exists(
            source_db_config=source_db_config,
            table_name=table_name,
            pk_field=pk_field,
            pk_value=pk_value,
        )

        capture_checkpoint = initial_snapshot.get("capture_checkpoint") or {}

        details = {
            "service_id": service.id,
            "service_name": service.name,
            "row_exists": row_exists,
            "initial_outbound_max_id": initial_snapshot["outbound_max_id"],
            "initial_batch_max_id": initial_snapshot["batch_max_id"],
            "initial_audit_max_id": initial_snapshot["audit_max_id"],
            "initial_capture_checkpoint": capture_checkpoint,
        }

        return {
            "ok": bool(row_exists),
            "message": "Preparation completed" if row_exists else "Source test row not found",
            "details": details,
        }

    def _run_phase_1_source_update(self, source_db_config, table_name, pk_field, pk_value, update_column, update_value):
        before_value = self._fetch_source_value(
            source_db_config=source_db_config,
            table_name=table_name,
            pk_field=pk_field,
            pk_value=pk_value,
            column_name=update_column,
        )

        affected_rows = self._execute_source_update(
            source_db_config=source_db_config,
            table_name=table_name,
            pk_field=pk_field,
            pk_value=pk_value,
            update_column=update_column,
            update_value=update_value,
        )

        after_value = self._fetch_source_value(
            source_db_config=source_db_config,
            table_name=table_name,
            pk_field=pk_field,
            pk_value=pk_value,
            column_name=update_column,
        )

        ok = affected_rows >= 1 and str(after_value) == str(update_value)

        return {
            "ok": ok,
            "message": "Source update executed" if ok else "Source update failed",
            "details": {
                "before_value": self._to_text(before_value),
                "after_value": self._to_text(after_value),
                "expected_value": self._to_text(update_value),
                "affected_rows": affected_rows,
            },
        }

    def _run_phase_2_capture(self, service, initial_snapshot, table_name, pk_value, capture_limit):
        result = capture_binlog_once(
            replication_service_id=service.id,
            limit=capture_limit,
            initiated_by="srdf_test_validation",
        )

        initial_outbound_max_id = initial_snapshot["outbound_max_id"]

        new_outbound_qs = (
            OutboundChangeEvent.objects
            .filter(replication_service=service, id__gt=initial_outbound_max_id)
            .order_by("id")
        )

        new_outbound_count = new_outbound_qs.count()
        matched_event = self._find_matching_outbound_event(
            events=list(new_outbound_qs),
            table_name=table_name,
            pk_value=pk_value,
        )

        checkpoint_after = self._get_capture_checkpoint_snapshot(service)
        checkpoint_before = initial_snapshot.get("capture_checkpoint") or {}
        checkpoint_advanced = self._checkpoint_has_advanced(checkpoint_before, checkpoint_after)

        ok = bool(matched_event) and matched_event.status == "pending"

        return {
            "ok": ok,
            "message": "Capture phase completed" if ok else "Capture phase failed",
            "captured_count": result.get("captured_count", 0),
            "matched_outbound_event_id": matched_event.id if matched_event else None,
            "details": {
                "capture_result": result,
                "new_outbound_count": new_outbound_count,
                "matched_event_status": matched_event.status if matched_event else None,
                "matched_event_operation": matched_event.operation if matched_event else None,
                "matched_event_table": matched_event.table_name if matched_event else None,
                "matched_event_primary_key_data": matched_event.primary_key_data if matched_event else {},
                "matched_event_after_data": matched_event.after_data if matched_event else {},
                "capture_checkpoint_before": checkpoint_before,
                "capture_checkpoint_after": checkpoint_after,
                "checkpoint_advanced": checkpoint_advanced,
            },
        }

    def _run_phase_3_build_batch(self, service, initial_snapshot, outbound_event_id, build_limit):
        result = build_transport_batch_once(
            replication_service_id=service.id,
            limit=build_limit,
            initiated_by="srdf_test_validation",
        )

        batch_id = result.get("transport_batch_id")
        batch = TransportBatch.objects.filter(id=batch_id).first() if batch_id else None

        outbound_event = None
        if outbound_event_id:
            outbound_event = OutboundChangeEvent.objects.filter(id=outbound_event_id).first()

        ok = bool(batch) and batch.status == "ready"

        if outbound_event_id:
            ok = ok and bool(outbound_event) and outbound_event.status == "batched"

        return {
            "ok": ok,
            "message": "Build batch phase completed" if ok else "Build batch phase failed",
            "transport_batch_id": batch.id if batch else None,
            "details": {
                "build_result": result,
                "transport_batch_status": batch.status if batch else None,
                "transport_batch_event_count": batch.event_count if batch else 0,
                "transport_batch_first_event_id": batch.first_event_id if batch else None,
                "transport_batch_last_event_id": batch.last_event_id if batch else None,
                "transport_batch_checksum": batch.checksum if batch else "",
                "outbound_event_status_after_build": outbound_event.status if outbound_event else None,
            },
        }

    def _run_phase_4_send_batch(self, batch_id, strict_send=False):
        batch = TransportBatch.objects.filter(id=batch_id).first()
        if not batch:
            return {
                "ok": False,
                "message": f"TransportBatch #{batch_id} not found before send",
                "details": {},
            }

        send_exception = None
        send_result = None

        try:
            send_result = send_transport_batch_once(
                batch_id=batch.id,
                initiated_by="srdf_test_validation",
            )
        except Exception as exc:
            send_exception = str(exc)

        batch.refresh_from_db()

        acceptable_status = batch.status in ("sent", "failed")
        ok = acceptable_status

        if strict_send:
            ok = batch.status == "sent"

        return {
            "ok": ok,
            "message": "Send batch phase completed" if ok else "Send batch phase failed",
            "details": {
                "send_result": send_result or {},
                "send_exception": send_exception,
                "transport_batch_status_after_send": batch.status,
                "transport_batch_retry_count": batch.retry_count,
                "transport_batch_last_error": batch.last_error,
                "transport_batch_sent_at": batch.sent_at.isoformat() if batch.sent_at else None,
            },
        }

    # -----------------------------------------------------------------
    # HELPERS
    # -----------------------------------------------------------------

    def _get_service(self, service_id):
        service = (
            ReplicationService.objects
            .select_related("source_node", "target_node")
            .filter(id=service_id, is_enabled=True, service_type="mariadb")
            .first()
        )
        if not service:
            raise CommandError(f"Enabled mariadb ReplicationService #{service_id} not found")
        return service

    def _validate_identifier(self, value, label):
        if not value or not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", value):
            raise CommandError(f"Invalid {label}: '{value}'")
        return value

    def _generate_update_value(self):
        return f"SRDF_TEST_{timezone.now().strftime('%Y%m%d_%H%M%S_%f')}"

    def _get_source_db_config(self, service):
        config = service.config or {}
        if not isinstance(config, dict):
            raise CommandError(f"Invalid config for ReplicationService '{service.name}'")

        required = ["host", "port", "user", "database"]
        missing = [key for key in required if not config.get(key) and config.get(key) != 0]
        if missing:
            raise CommandError(
                f"ReplicationService '{service.name}' missing source DB config keys: {', '.join(missing)}"
            )

        if "password" not in config:
            config["password"] = ""

        return config

    def _get_connection(self, source_db_config):
        return pymysql.connect(
            host=source_db_config["host"],
            port=int(source_db_config.get("port", 3306)),
            user=source_db_config["user"],
            password=source_db_config.get("password", ""),
            database=source_db_config["database"],
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )

    def _source_row_exists(self, source_db_config, table_name, pk_field, pk_value):
        conn = self._get_connection(source_db_config)
        try:
            sql = f"SELECT 1 AS x FROM `{table_name}` WHERE `{pk_field}` = %s LIMIT 1"
            with conn.cursor() as cursor:
                cursor.execute(sql, [pk_value])
                row = cursor.fetchone()
                return bool(row)
        finally:
            conn.close()

    def _fetch_source_value(self, source_db_config, table_name, pk_field, pk_value, column_name):
        conn = self._get_connection(source_db_config)
        try:
            sql = f"SELECT `{column_name}` AS value FROM `{table_name}` WHERE `{pk_field}` = %s LIMIT 1"
            with conn.cursor() as cursor:
                cursor.execute(sql, [pk_value])
                row = cursor.fetchone()
                if not row:
                    return None
                return row.get("value")
        finally:
            conn.close()

    def _execute_source_update(self, source_db_config, table_name, pk_field, pk_value, update_column, update_value):
        conn = self._get_connection(source_db_config)
        try:
            sql = f"UPDATE `{table_name}` SET `{update_column}` = %s WHERE `{pk_field}` = %s"
            with conn.cursor() as cursor:
                cursor.execute(sql, [update_value, pk_value])
                return int(cursor.rowcount or 0)
        finally:
            conn.close()

    def _collect_initial_snapshot(self, service):
        outbound_max_id = (
            OutboundChangeEvent.objects
            .filter(replication_service=service)
            .aggregate(value=Max("id"))
            .get("value") or 0
        )
        batch_max_id = (
            TransportBatch.objects
            .filter(replication_service=service)
            .aggregate(value=Max("id"))
            .get("value") or 0
        )
        audit_max_id = (
            AuditEvent.objects
            .aggregate(value=Max("id"))
            .get("value") or 0
        )

        return {
            "outbound_max_id": outbound_max_id,
            "batch_max_id": batch_max_id,
            "audit_max_id": audit_max_id,
            "capture_checkpoint": self._get_capture_checkpoint_snapshot(service),
        }

    def _get_capture_checkpoint_snapshot(self, service):
        checkpoint = (
            ReplicationCheckpoint.objects
            .filter(replication_service=service, direction="capture")
            .order_by("-id")
            .first()
        )

        if not checkpoint:
            return {}

        return {
            "id": checkpoint.id,
            "log_file": checkpoint.log_file or "",
            "log_pos": int(checkpoint.log_pos or 0),
            "transaction_id": checkpoint.transaction_id or "",
            "updated_at": checkpoint.updated_at.isoformat() if checkpoint.updated_at else None,
        }

    def _checkpoint_has_advanced(self, before_data, after_data):
        before_file = (before_data or {}).get("log_file") or ""
        before_pos = int((before_data or {}).get("log_pos") or 0)
        after_file = (after_data or {}).get("log_file") or ""
        after_pos = int((after_data or {}).get("log_pos") or 0)

        if after_file and before_file and after_file != before_file:
            return True

        return after_pos > before_pos

    def _find_matching_outbound_event(self, events, table_name, pk_value):
        pk_value_text = str(pk_value)

        for event in events:
            if event.table_name != table_name:
                continue

            if event.operation != "update":
                continue

            primary_key_data = event.primary_key_data or {}
            if pk_value_text in json.dumps(primary_key_data, ensure_ascii=False, sort_keys=True):
                return event

        return None

    def _print_phase_result(self, label, phase_result):
        ok = bool(phase_result.get("ok"))
        message = phase_result.get("message", "")
        details = phase_result.get("details", {})

        status_label = "OK" if ok else "FAIL"
        color = self.style.SUCCESS if ok else self.style.ERROR

        self.stdout.write(color(f"{label}: {status_label}"))
        if message:
            self.stdout.write(f"  message={message}")

        if phase_result.get("skipped"):
            self.stdout.write("  skipped=True")

        if details:
            for key, value in details.items():
                self.stdout.write(f"  {key}={self._to_text(value)}")

        self.stdout.write("")

    def _build_summary(self, report):
        phase_results = report.get("phases", {})

        phases_ok = 0
        phases_failed = 0

        for _, phase_data in phase_results.items():
            if phase_data.get("ok"):
                phases_ok += 1
            else:
                phases_failed += 1

        service_id = report["service_id"]

        outbound_new_count = self._count_new_outbound_events(
            service_id=service_id,
            min_id=(report.get("phases", {})
                   .get("phase_0_preparation", {})
                   .get("details", {})
                   .get("initial_outbound_max_id", 0))
        )

        batch_new_count = self._count_new_batches(
            service_id=service_id,
            min_id=(report.get("phases", {})
                   .get("phase_0_preparation", {})
                   .get("details", {})
                   .get("initial_batch_max_id", 0))
        )

        audit_new_count = self._count_new_audits(
            min_id=(report.get("phases", {})
                   .get("phase_0_preparation", {})
                   .get("details", {})
                   .get("initial_audit_max_id", 0))
        )

        checkpoint_advanced = bool(
            report.get("phases", {})
            .get("phase_2_capture", {})
            .get("details", {})
            .get("checkpoint_advanced")
        )

        return {
            "phases_ok": phases_ok,
            "phases_failed": phases_failed,
            "outbound_new_count": outbound_new_count,
            "transport_batch_new_count": batch_new_count,
            "audit_new_count": audit_new_count,
            "checkpoint_advanced": checkpoint_advanced,
        }

    def _count_new_outbound_events(self, service_id, min_id):
        return (
            OutboundChangeEvent.objects
            .filter(replication_service_id=service_id, id__gt=min_id)
            .count()
        )

    def _count_new_batches(self, service_id, min_id):
        return (
            TransportBatch.objects
            .filter(replication_service_id=service_id, id__gt=min_id)
            .count()
        )

    def _count_new_audits(self, min_id):
        return AuditEvent.objects.filter(id__gt=min_id).count()

    def _create_final_audit_event(self, service, report):
        AuditEvent.objects.create(
            event_type="srdf_test_validation_run",
            level="info" if report.get("ok") else "warning",
            node=service.source_node,
            message=f"SRDF test validation completed for service '{service.name}'",
            created_by="srdf_test_validation",
            payload=report,
        )

    def _to_text(self, value):
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False, sort_keys=True)
        return str(value)