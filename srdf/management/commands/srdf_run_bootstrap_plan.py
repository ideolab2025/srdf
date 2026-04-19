#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.models import BootstrapPlan
from srdf.services.bootstrap_plan_runner import run_bootstrap_plan_once


class Command(BaseCommand):
    help = "Run one BootstrapPlan or all enabled BootstrapPlans"

    def add_arguments(self, parser):
        
        parser.add_argument("--plan-id", type=int, default=0, help="BootstrapPlan ID")
        parser.add_argument("--plan-name", type=str, default="", help="BootstrapPlan name")
        parser.add_argument("--replay-last", action="store_true", help="Replay the last execution of the plan")
        parser.add_argument("--replay-last-failed", action="store_true", help="Replay the last failed execution of the plan")
        parser.add_argument("--retry-failed-only", action="store_true", help="Replay only failed execution items")
        parser.add_argument("--all-enabled", action="store_true", help="Run all enabled plans")
        parser.add_argument("--dry-run", action="store_true", help="Run in dry-run mode")
        parser.add_argument(
            "--verbose-progress",
            action="store_true",
            help="Show progress messages for each database/table",
        )   

    def handle(self, *args, **options):
        
        plan_id = int(options["plan_id"] or 0)
        plan_name = (options.get("plan_name") or "").strip()
        replay_last = bool(options["replay_last"])
        replay_last_failed = bool(options["replay_last_failed"])
        retry_failed_only = bool(options["retry_failed_only"])
        all_enabled = bool(options["all_enabled"])
        dry_run = bool(options["dry_run"])
        verbose_progress = bool(options["verbose_progress"])


        if not plan_id and not plan_name and not all_enabled:
            raise CommandError("Use --plan-id, --plan-name or --all-enabled")

        if plan_id and plan_name:
            raise CommandError("Use either --plan-id or --plan-name, not both")

        if (replay_last or replay_last_failed or retry_failed_only) and not plan_name:
            raise CommandError("Replay options require --plan-name")

        if replay_last and replay_last_failed:
            raise CommandError("Use either --replay-last or --replay-last-failed, not both")

        if (replay_last or replay_last_failed) and all_enabled:
            raise CommandError("Replay options cannot be used with --all-enabled")

        if retry_failed_only and not (replay_last or replay_last_failed):
            raise CommandError("--retry-failed-only requires --replay-last or --replay-last-failed")


        def cli_progress(message, level="info"):
            if level == "error":
                self.stdout.write(self.style.ERROR(message))
            elif level == "warning":
                self.stdout.write(self.style.WARNING(message))
            elif level == "success":
                self.stdout.write(self.style.SUCCESS(message))
            else:
                self.stdout.write(message)


        

        if plan_id or plan_name:
            result = run_bootstrap_plan_once(
                bootstrap_plan_id=plan_id or None,
                bootstrap_plan_name=plan_name or None,
                replay_last=replay_last,
                replay_last_failed=replay_last_failed,
                retry_failed_only=retry_failed_only,
                initiated_by="cli_bootstrap_plan",
                dry_run_override=dry_run,
                progress_callback=cli_progress if verbose_progress else None,
            )

            resolved_plan = result.get("bootstrap_plan_name") or plan_name or f"#{plan_id}"

            label = "replay" if (replay_last or replay_last_failed) else "done"
            self.stdout.write(self.style.SUCCESS(
                f"BootstrapPlan {resolved_plan} {label}. "
                f"databases={result.get('total_databases', 0)} "
                f"tables={result.get('total_tables', 0)} "
                f"success={result.get('success_count', 0)} "
                f"failed={result.get('failed_count', 0)} "
                f"duration={result.get('duration_seconds', 0)}s"
            ))
            return  
        

        plans = BootstrapPlan.objects.filter(is_enabled=True).order_by("execution_order", "id")
        if not plans.exists():
            raise CommandError("No enabled BootstrapPlan found")

        for plan in plans:
            self.stdout.write(f"[PLAN] #{plan.id} {plan.name}")
            result = run_bootstrap_plan_once(
                bootstrap_plan_id=plan.id,
                bootstrap_plan_name=None,
                initiated_by="cli_bootstrap_plan",
                dry_run_override=dry_run,
                progress_callback=cli_progress if verbose_progress else None,
            )
            self.stdout.write(self.style.SUCCESS(
                f"   databases={result.get('total_databases', 0)} "
                f"tables={result.get('total_tables', 0)} "
                f"success={result.get('success_count', 0)} "
                f"failed={result.get('failed_count', 0)} "
                f"duration={result.get('duration_seconds', 0)}s"
            ))