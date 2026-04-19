#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.core.management.base import BaseCommand, CommandError

from srdf.services.dedup_engine import undo_deduplicate_run


class Command(BaseCommand):
    help = "Undo one SRDF deduplicate run and restore dead outbound events to pending"

    def add_arguments(self, parser):
        parser.add_argument(
            "--dedup-run-id",
            type=int,
            required=True,
            help="SRDFDedupRun ID to undo",
        )
        parser.add_argument(
            "--initiated-by",
            type=str,
            default="management_command:srdf_dedup_undo",
            help="Audit initiator label",
        )

    def handle(self, *args, **options):
        dedup_run_id = int(options["dedup_run_id"])
        initiated_by = str(options["initiated_by"] or "management_command:srdf_dedup_undo").strip()

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write(" SRDF DEDUP UNDO")
        self.stdout.write("============================================================")
        self.stdout.write(f" dedup_run_id : {dedup_run_id}")
        self.stdout.write(f" initiated_by : {initiated_by}")
        self.stdout.write("============================================================")

        try:
            result = undo_deduplicate_run(
                dedup_run_id=dedup_run_id,
                initiated_by=initiated_by,
            )
        except Exception as exc:
            raise CommandError(str(exc))

        restored_count = int(result.get("restored_count") or 0)
        deleted_decision_count = int(result.get("deleted_decision_count") or 0)
        skipped = bool(result.get("skipped"))

        self.stdout.write("")
        self.stdout.write("============================================================")
        self.stdout.write(" SRDF DEDUP UNDO SUMMARY")
        self.stdout.write("============================================================")
        self.stdout.write(f" ok                     : {bool(result.get('ok'))}")
        self.stdout.write(f" skipped                : {skipped}")
        self.stdout.write(f" dedup_run_id           : {int(result.get('dedup_run_id') or 0)}")
        self.stdout.write(f" replication_service_id : {int(result.get('replication_service_id') or 0)}")
        self.stdout.write(f" restored_count         : {restored_count}")
        self.stdout.write(f" deleted_decision_count : {deleted_decision_count}")
        self.stdout.write(f" message                : {result.get('message') or ''}")
        self.stdout.write("============================================================")
        self.stdout.write("")

        return result.get("message") or "SRDF dedup undo completed"