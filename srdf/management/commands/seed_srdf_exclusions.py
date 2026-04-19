#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand
from srdf.models import SQLObjectScopeRule


class Command(BaseCommand):
    help = "Seed SRDF internal tables exclusions (capture filter)"

    SRDF_TABLES = [
        "srdf_node",
        "srdf_replicationservice",
        "srdf_replicationstatus",
        "srdf_actionexecution",
        "srdf_failoverplan",
        "srdf_auditevent",
        "srdf_executionlock",
        "srdf_outboundchangeevent",
        "srdf_transportbatch",
        "srdf_inboundchangeevent",
        "srdf_replicationcheckpoint",
    ]

    def handle(self, *args, **options):
        created = 0
        existing = 0

        self.stdout.write("=== SRDF EXCLUSION SEED START ===")

        for table_name in self.SRDF_TABLES:
            obj, is_created = SQLObjectScopeRule.objects.get_or_create(
                name=f"EXCLUDE {table_name}",
                defaults={
                    "is_system_rule": True,
                    "engine": "all",
                    "policy_mode": "exclude",
                    "object_type": "table",
                    "match_mode": "exact",
                    "table_name": table_name,
                    "priority": 10,
                },
            )

            if is_created:
                created += 1
                self.stdout.write(f"[CREATED] {table_name}")
            else:
                existing += 1
                self.stdout.write(f"[EXISTS ] {table_name}")

        self.stdout.write("=== DONE ===")
        self.stdout.write(f"Created : {created}")
        self.stdout.write(f"Existing: {existing}")