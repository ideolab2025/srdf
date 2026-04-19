#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand

from srdf.models import FailoverPlan
from srdf.services.failover_validator import validate_failover_plan


class Command(BaseCommand):
    help = "Validate SRDF failover plans"

    def handle(self, *args, **options):
        plans = FailoverPlan.objects.order_by("id")
        total = plans.count()
        ok_count = 0
        error_count = 0

        self.stdout.write(f"SRDF failover validation started. plans={total}")

        for plan in plans:
            result = validate_failover_plan(plan)
            if result["ok"]:
                ok_count += 1
                self.stdout.write(
                    self.style.SUCCESS(
                        f"[OK] plan#{plan.id} {plan.name} steps={result['steps_count']}"
                    )
                )
            else:
                error_count += 1
                self.stdout.write(
                    self.style.ERROR(
                        f"[ERROR] plan#{plan.id} {plan.name} -> {' | '.join(result['errors'])}"
                    )
                )

        self.stdout.write(
            self.style.WARNING(
                f"SRDF failover validation finished. ok={ok_count} error={error_count} total={total}"
            )
        )