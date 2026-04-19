#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand
from srdf.models import Node
from srdf.services.mariadb_replication import get_replication_status


class Command(BaseCommand):
    help = "Test MariaDB replication"

    def handle(self, *args, **options):
        for node in Node.objects.filter(is_enabled=True):
            config = node.extra_config.get("mariadb") if node.extra_config else None
            if not config:
                continue

            status = get_replication_status(config)
            self.stdout.write(f"{node.name}: {status}")