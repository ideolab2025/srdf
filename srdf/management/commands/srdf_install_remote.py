#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json
import os
import subprocess
import sys
import time
import uuid

import pymysql

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from srdf.models import (
    Node,
    ReplicationService,
    SRDFInstallProfile,
    SRDFInstallRun,
    SRDFServiceRuntimeState,
)


class Command(BaseCommand):
    help = "Install and initialize SRDF remote control plane from SQL install profile"

    def add_arguments(self, parser):
        parser.add_argument("--profile-id", type=int, default=0, help="SRDFInstallProfile ID")
        parser.add_argument("--profile-name", type=str, default="", help="SRDFInstallProfile name")
        parser.add_argument(
            "--remote-settings",
            type=str,
            default="srdf_project.settings_target",
            help="Django settings module used for remote control plane commands",
        )
        parser.add_argument(
            "--remote-manage-py",
            type=str,
            default="manage.py",
            help="Path to manage.py used to execute remote control plane commands",
        )


    def handle(self, *args, **options):
        self.remote_settings = (options.get("remote_settings") or "").strip()
        self.remote_manage_py = os.path.abspath(options.get("remote_manage_py") or "manage.py")        
        profile_id = int(options["profile_id"] or 0)
        profile_name = (options["profile_name"] or "").strip()

        if not profile_id and not profile_name:
            raise CommandError("Use --profile-id or --profile-name")

        if profile_id and profile_name:
            raise CommandError("Use either --profile-id or --profile-name, not both")

        profile = self._resolve_profile(profile_id=profile_id, profile_name=profile_name)
        if not profile:
            raise CommandError("SRDFInstallProfile not found or disabled")

        started_perf = time.perf_counter()
        run = SRDFInstallRun.objects.create(
            install_profile=profile,
            run_uid=str(uuid.uuid4()),
            status="running",
            initiated_by="cli:srdf_install_remote",
            control_db_name=profile.control_db_name,
            archive_db_name=profile.archive_db_name,
            target_db_name=profile.target_db_name,
        )

        profile.status = "running"
        profile.last_error = ""
        profile.save(update_fields=["status", "last_error", "updated_at"])

        try:
            self.stdout.write("")
            self.stdout.write("============================================================")
            self.stdout.write("SRDF INSTALL REMOTE")
            self.stdout.write("============================================================")
            self.stdout.write(f"profile_id          : {profile.id}")
            self.stdout.write(f"profile_name        : {profile.name}")
            self.stdout.write(f"db_host             : {profile.db_host}")
            self.stdout.write(f"db_port             : {profile.db_port}")
            self.stdout.write(f"control_db_name     : {profile.control_db_name}")
            self.stdout.write(f"archive_db_name     : {profile.archive_db_name}")
            self.stdout.write(f"target_db_name      : {profile.target_db_name}")
            self.stdout.write("")

            self._create_database_if_missing(
                host=profile.db_host,
                port=profile.db_port,
                user=profile.db_user,
                password=profile.db_password,
                database_name=profile.control_db_name,
            )

            if profile.create_archive_db:
                self._create_database_if_missing(
                    host=profile.db_host,
                    port=profile.db_port,
                    user=profile.db_user,
                    password=profile.db_password,
                    database_name=profile.archive_db_name,
                )

            if profile.auto_create_target_db:
                self._create_database_if_missing(
                    host=profile.db_host,
                    port=profile.db_port,
                    user=profile.db_user,
                    password=profile.db_password,
                    database_name=profile.target_db_name,
                )

            self.stdout.write(self.style.SUCCESS("[OK] remote databases checked/created"))


            if profile.run_migrate:
                self.stdout.write("[STEP] Applying migrations on remote control DB...")
                migrate_result = self._run_remote_management_command(
                    command_name="migrate",
                    extra_args=["--noinput"],
                )
                self.stdout.write(migrate_result["stdout"])
                if migrate_result["stderr"]:
                    self.stdout.write(self.style.WARNING(migrate_result["stderr"]))
                self.stdout.write(self.style.SUCCESS("[OK] remote migrate completed"))
            else:
                self.stdout.write(self.style.WARNING("[SKIPPED] migrate"))
                
                

            self.stdout.write("[STEP] Initializing remote SRDF core objects...")
            remote_init_result = self._run_remote_management_command(
                command_name="srdf_install_remote_finalize",
                extra_args=[
                    f"--source-node-name={profile.source_node_name}",
                    f"--source-hostname={profile.source_hostname}",
                    f"--source-db-port={profile.source_db_port}",
                    f"--source-site={profile.source_site}",
                    f"--source-api-base-url={profile.source_api_base_url}",
                    f"--target-node-name={profile.target_node_name}",
                    f"--target-hostname={profile.target_hostname}",
                    f"--target-db-port={profile.target_db_port}",
                    f"--target-site={profile.target_site}",
                    f"--target-api-base-url={profile.target_api_base_url}",
                    f"--api-token={profile.api_token}",
                    f"--service-name={profile.service_name}",
                    f"--service-type={profile.service_type}",
                    f"--mode={profile.mode}",
                    f"--source-db-name={profile.source_db_name}",
                    f"--source-db-user={profile.source_db_user}",
                    f"--source-db-password={profile.source_db_password}",
                    f"--target-db-name={profile.target_db_name}",
                    f"--target-db-user={profile.db_user}",
                    f"--target-db-password={profile.db_password}",
                    f"--binlog-server-id={profile.binlog_server_id}",
                ],
            )
            self.stdout.write(remote_init_result["stdout"])
            if remote_init_result["stderr"]:
                self.stdout.write(self.style.WARNING(remote_init_result["stderr"]))
            
            remote_payload = remote_init_result.get("json_result") or {}
            source_node_id = int(remote_payload.get("source_node_id") or 0)
            target_node_id = int(remote_payload.get("target_node_id") or 0)
            replication_service_id = int(remote_payload.get("replication_service_id") or 0)
            runtime_state_id = int(remote_payload.get("runtime_state_id") or 0)
            
            if source_node_id <= 0 or target_node_id <= 0 or replication_service_id <= 0:
                raise CommandError("Remote finalize did not return valid source/target/service IDs")            
        
        


            if profile.seed_settings:
                self.stdout.write("[STEP] Seeding SRDF setting definitions on remote control DB...")
                seed_def_result = self._run_remote_management_command(
                    command_name="srdf_seed_settings",
                    extra_args=[],
                )
                self.stdout.write(seed_def_result["stdout"])
                if seed_def_result["stderr"]:
                    self.stdout.write(self.style.WARNING(seed_def_result["stderr"]))
                self.stdout.write(self.style.SUCCESS("[OK] remote srdf_seed_settings completed"))
            
                self.stdout.write("[STEP] Seeding SRDF global setting values on remote control DB...")
                seed_global_result = self._run_remote_management_command(
                    command_name="srdf_seed_setting_values",
                    extra_args=["--scope=global", "--preset=baseline"],
                )
                self.stdout.write(seed_global_result["stdout"])
                if seed_global_result["stderr"]:
                    self.stdout.write(self.style.WARNING(seed_global_result["stderr"]))
                self.stdout.write(self.style.SUCCESS("[OK] remote global setting values seeded"))
            else:
                self.stdout.write(self.style.WARNING("[SKIPPED] SRDF settings seed"))


            duration_seconds = round(time.perf_counter() - started_perf, 3)

            result_payload = {
                "ok": True,
                "install_profile_id": profile.id,
                "install_profile_name": profile.name,
                "control_db_name": profile.control_db_name,
                "archive_db_name": profile.archive_db_name,
                "target_db_name": profile.target_db_name,
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "replication_service_id": replication_service_id,
                "runtime_state_id": runtime_state_id,
                "duration_seconds": duration_seconds,
            }

            run.status = "success"
            run.finished_at = timezone.now()
            run.duration_seconds = duration_seconds
            
            run.source_node_id_value = source_node_id
            run.target_node_id_value = target_node_id
            run.replication_service_id_value = replication_service_id
            
            run.result_payload = result_payload
            run.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "duration_seconds",
                    "source_node_id_value",
                    "target_node_id_value",
                    "replication_service_id_value",
                    "result_payload",
                ]
            )

            profile.status = "success"
            profile.last_run_at = timezone.now()
            profile.last_error = ""
            profile.last_result_payload = result_payload
            profile.save(
                update_fields=[
                    "status",
                    "last_run_at",
                    "last_error",
                    "last_result_payload",
                    "updated_at",
                ]
            )

            self.stdout.write("")
            self.stdout.write("============================================================")
            self.stdout.write("INSTALL SUMMARY")
            self.stdout.write("============================================================")
            self.stdout.write(json.dumps(result_payload, indent=2, ensure_ascii=False))
            self.stdout.write("============================================================")
            self.stdout.write("")
            return

        except Exception as exc:
            duration_seconds = round(time.perf_counter() - started_perf, 3)

            run.status = "failed"
            run.finished_at = timezone.now()
            run.duration_seconds = duration_seconds
            run.last_error = str(exc)
            run.result_payload = {
                "ok": False,
                "install_profile_id": profile.id,
                "install_profile_name": profile.name,
                "error": str(exc),
                "duration_seconds": duration_seconds,
            }
            run.save(
                update_fields=[
                    "status",
                    "finished_at",
                    "duration_seconds",
                    "last_error",
                    "result_payload",
                ]
            )

            profile.status = "failed"
            profile.last_run_at = timezone.now()
            profile.last_error = str(exc)
            profile.last_result_payload = run.result_payload
            profile.save(
                update_fields=[
                    "status",
                    "last_run_at",
                    "last_error",
                    "last_result_payload",
                    "updated_at",
                ]
            )

            raise CommandError(str(exc))

    def _resolve_profile(self, profile_id=0, profile_name=""):
        qs = SRDFInstallProfile.objects.filter(is_enabled=True).order_by("id")

        if profile_id:
            return qs.filter(id=profile_id).first()

        if profile_name:
            return qs.filter(name=profile_name).first()

        return None
    
    
    
    def _extract_json_result(self, stdout_text):
        start_marker = "JSON_RESULT_BEGIN"
        end_marker = "JSON_RESULT_END"
    
        if start_marker not in stdout_text or end_marker not in stdout_text:
            return {}
    
        try:
            raw = stdout_text.split(start_marker, 1)[1].split(end_marker, 1)[0].strip()
            if not raw:
                return {}
            return json.loads(raw)
        except Exception:
            return {}    
    
    
    def _run_remote_management_command(self, command_name, extra_args=None):
        extra_args = extra_args or []
    
        manage_py = self.remote_manage_py
        if not os.path.exists(manage_py):
            raise CommandError(f"manage.py not found: {manage_py}")
    
        if not self.remote_settings:
            raise CommandError("Missing remote settings module")
    
        command = [
            sys.executable,
            manage_py,
            command_name,
            f"--settings={self.remote_settings}",
        ] + list(extra_args)
    
        proc = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
    
        stdout_text = proc.stdout or ""
        stderr_text = proc.stderr or ""
    
        if proc.returncode != 0:
            raise CommandError(
                f"Remote command '{command_name}' failed "
                f"(rc={proc.returncode})\nSTDOUT:\n{stdout_text}\nSTDERR:\n{stderr_text}"
            )
    
        json_result = self._extract_json_result(stdout_text)
    
        return {
            "ok": True,
            "command_name": command_name,
            "stdout": stdout_text,
            "stderr": stderr_text,
            "json_result": json_result,
        }
    
    
    def _create_database_if_missing(self, host, port, user, password, database_name):
        conn = pymysql.connect(
            host=host,
            port=int(port),
            user=user,
            password=password,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"CREATE DATABASE IF NOT EXISTS `{database_name}` "
                    f"CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"
                )
        finally:
            conn.close()