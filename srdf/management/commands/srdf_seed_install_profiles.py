#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand

from srdf.models import SRDFInstallProfile


DEFAULT_PROFILES = [
    {
        "name": "Laptop Source-Target Local",
        "is_enabled": True,
        "install_scope": "remote_control",
        "db_host": "127.0.0.1",
        "db_port": 3307,
        "db_user": "root",
        "db_password": "",
        "control_db_name": "srdf_target_control",
        "archive_db_name": "srdf_remote_archive",
        "target_db_name": "target_db",
        "source_node_name": "mysql-source",
        "source_hostname": "127.0.0.1",
        "source_db_port": 3306,
        "source_site": "site-a",
        "source_api_base_url": "http://127.0.0.1:8010",
        "target_node_name": "mariadb-target",
        "target_hostname": "127.0.0.1",
        "target_db_port": 3307,
        "target_site": "site-b",
        "target_api_base_url": "http://127.0.0.1:8011",
        "api_token": "change-me-srdf-token",
        "service_name": "Mariadb-replica",
        "service_type": "mariadb",
        "mode": "async",
        "source_db_name": "oxygen_db6",
        "source_db_user": "root",
        "source_db_password": "",
        "binlog_server_id": 900001,
        "run_migrate": True,
        "seed_settings": True,
        "create_archive_db": True,
        "auto_create_target_db": True,
        "status": "draft",
        "notes": "Local laptop standalone profile",
    },
    {
        "name": "Production Template",
        "is_enabled": True,
        "install_scope": "remote_control",
        "db_host": "db-target.example.com",
        "db_port": 3306,
        "db_user": "srdf_admin",
        "db_password": "CHANGE_ME",
        "control_db_name": "srdf_target_control",
        "archive_db_name": "srdf_archive",
        "target_db_name": "app_target_db",
        "source_node_name": "source-prod",
        "source_hostname": "db-source.example.com",
        "source_db_port": 3306,
        "source_site": "dc-a",
        "source_api_base_url": "https://srdf-source.example.com",
        "target_node_name": "target-prod",
        "target_hostname": "db-target.example.com",
        "target_db_port": 3306,
        "target_site": "dc-b",
        "target_api_base_url": "https://srdf-target.example.com",
        "api_token": "CHANGE_ME",
        "service_name": "Prod-replica",
        "service_type": "mariadb",
        "mode": "async",
        "source_db_name": "app_source_db",
        "source_db_user": "srdf_reader",
        "source_db_password": "CHANGE_ME",
        "binlog_server_id": 910001,
        "run_migrate": True,
        "seed_settings": True,
        "create_archive_db": True,
        "auto_create_target_db": False,
        "status": "draft",
        "notes": "Production template profile with public domains",
    },
]

class Command(BaseCommand):
    help = "Seed default SRDF remote install profiles"

    def handle(self, *args, **options):
        for payload in DEFAULT_PROFILES:
            obj, created = SRDFInstallProfile.objects.update_or_create(
                name=payload["name"],
                defaults=payload,
            )
    
            if created:
                self.stdout.write(self.style.SUCCESS(f"[CREATED] install profile '{obj.name}' id={obj.id}"))
            else:
                self.stdout.write(self.style.WARNING(f"[UPDATED] install profile '{obj.name}' id={obj.id}"))