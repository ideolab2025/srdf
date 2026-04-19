#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import json

from django.core.management.base import BaseCommand

from srdf.models import Node, ReplicationService, SRDFServiceRuntimeState


class Command(BaseCommand):
    help = "Finalize remote SRDF install on remote control DB"

    def add_arguments(self, parser):
        parser.add_argument("--source-node-name", type=str, required=True)
        parser.add_argument("--source-hostname", type=str, required=True)
        parser.add_argument("--source-db-port", type=int, required=True)
        parser.add_argument("--source-site", type=str, default="site-a")
        parser.add_argument("--source-api-base-url", type=str, required=True)

        parser.add_argument("--target-node-name", type=str, required=True)
        parser.add_argument("--target-hostname", type=str, required=True)
        parser.add_argument("--target-db-port", type=int, required=True)
        parser.add_argument("--target-site", type=str, default="site-b")
        parser.add_argument("--target-api-base-url", type=str, required=True)

        parser.add_argument("--api-token", type=str, required=True)

        parser.add_argument("--service-name", type=str, required=True)
        parser.add_argument(
            "--service-type",
            type=str,
            required=True,
            choices=["postgresql", "mysql", "mariadb", "filesync", "nginx"],
        )
        parser.add_argument(
            "--mode",
            type=str,
            required=True,
            choices=["sync", "async", "manual"],
        )

        parser.add_argument("--source-db-name", type=str, required=True)
        parser.add_argument("--source-db-user", type=str, required=True)
        parser.add_argument("--source-db-password", type=str, default="")

        parser.add_argument("--target-db-name", type=str, required=True)
        parser.add_argument("--target-db-user", type=str, required=True)
        parser.add_argument("--target-db-password", type=str, default="")

        parser.add_argument("--binlog-server-id", type=int, required=True)

    def handle(self, *args, **options):
        source_node, _ = Node.objects.update_or_create(
            name=options["source_node_name"],
            defaults={
                "hostname": options["source_hostname"],
                "db_port": int(options["source_db_port"]),
                "site": options["source_site"],
                "role": "primary",
                "api_base_url": (options["source_api_base_url"] or "").strip().rstrip("/"),
                "api_token": options["api_token"],
                "is_enabled": True,
                "status": "unknown",
            },
        )

        target_node, _ = Node.objects.update_or_create(
            name=options["target_node_name"],
            defaults={
                "hostname": options["target_hostname"],
                "db_port": int(options["target_db_port"]),
                "site": options["target_site"],
                "role": "secondary",
                "api_base_url": (options["target_api_base_url"] or "").strip().rstrip("/"),
                "api_token": options["api_token"],
                "is_enabled": True,
                "status": "unknown",
            },
        )

        service_config = {
            "host": options["source_hostname"],
            "port": int(options["source_db_port"]),
            "user": options["source_db_user"],
            "password": options["source_db_password"],
            "database": options["source_db_name"],
            "is_mariadb": options["service_type"] == "mariadb",
            "target_host": options["target_hostname"],
            "target_port": int(options["target_db_port"]),
            "target_user": options["target_db_user"],
            "target_password": options["target_db_password"],
            "target_database": options["target_db_name"],
            "binlog_server_id": int(options["binlog_server_id"]),
            "auto_create_table": True,
            "auto_create_database": True,
        }

        service, _ = ReplicationService.objects.update_or_create(
            name=options["service_name"],
            defaults={
                "service_type": options["service_type"],
                "mode": options["mode"],
                "source_node": source_node,
                "target_node": target_node,
                "is_enabled": True,
                "config": service_config,
            },
        )

        runtime_state, _ = SRDFServiceRuntimeState.objects.get_or_create(
            replication_service=service,
            defaults={
                "is_enabled": True,
                "is_paused": False,
                "stop_requested": False,
                "current_phase": "idle",
                "last_success_phase": "",
                "next_phase": "",
            },
        )

        payload = {
            "ok": True,
            "source_node_id": source_node.id,
            "target_node_id": target_node.id,
            "replication_service_id": service.id,
            "runtime_state_id": runtime_state.id,
        }

        self.stdout.write("JSON_RESULT_BEGIN")
        self.stdout.write(json.dumps(payload, indent=2, ensure_ascii=False))
        self.stdout.write("JSON_RESULT_END")