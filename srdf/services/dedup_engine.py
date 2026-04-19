#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import hashlib
import json

from django.db import transaction
from django.utils import timezone

from srdf.models import (
    AuditEvent,
    OutboundChangeEvent,
    ReplicationService,
    SRDFDedupRun,
    SRDFDedupDecision,
)

class DedupEngineError(Exception):
    pass


DEDUP_ERROR_EXACT = "DEDUP_EXACT_DUPLICATE"
DEDUP_ERROR_COLLAPSED = "DEDUP_COLLAPSED_BY_PK_SEQUENCE"
DEDUP_ERROR_NEUTRALIZED = "DEDUP_NEUTRALIZED_SEQUENCE"


def run_deduplicate_once(
    replication_service_id,
    limit=5000,
    initiated_by="srdf_deduplicate",
    ):
    
    replication_service = _get_replication_service(replication_service_id)
    
    started_at = timezone.now()
    
    dedup_run = SRDFDedupRun.objects.create(
        replication_service=replication_service,
        run_uid=str(timezone.now().strftime("dedup_%Y%m%d_%H%M%S")),
        initiated_by=initiated_by,
        status="running",
    )    



    pending_events = list(
        OutboundChangeEvent.objects
        .filter(
            replication_service=replication_service,
            status="pending",
            event_family="dml",
        )
        .order_by("id")[:max(int(limit or 5000), 1)]
    )   
    

    if not pending_events:
        return {
            "ok": True,
            "message": f"No pending outbound events to deduplicate for service '{replication_service.name}'",
            "replication_service_id": replication_service.id,
            "processed_count": 0,
            "survivor_count": 0,
            "duplicate_count": 0,
            "neutralized_count": 0,
            "collapsed_count": 0,
            "duplicate_event_ids": [],
            "neutralized_event_ids": [],
            "collapsed_event_ids": [],
            "skipped": True,
        }

    exact_survivor_ids = set()
    exact_duplicate_ids = set()

    exact_groups = {}
    for event in pending_events:
        signature = _resolve_exact_signature(event)
        exact_groups.setdefault(signature, []).append(event)
        
        



    exact_survivors = []
    stage1_events = []

    for _signature, events in exact_groups.items():
        events = sorted(events, key=lambda obj: obj.id)
        survivor = events[0]
        exact_survivors.append(survivor)
        exact_survivor_ids.add(survivor.id)
        stage1_events.append(survivor)

        if len(events) > 1:
            for obj in events[1:]:
                exact_duplicate_ids.add(obj.id)

    sequence_duplicate_ids = set()
    neutralized_ids = set()
    collapsed_ids = set()

    pk_groups = {}
    passthrough_events = []

    for event in sorted(stage1_events, key=lambda obj: obj.id):
        pk_key = _build_pk_group_key(event)
        if pk_key:
            pk_groups.setdefault(pk_key, []).append(event)
        else:
            passthrough_events.append(event)

    final_survivor_ids = set(obj.id for obj in passthrough_events)

    for _pk_key, events in pk_groups.items():
        resolution = _resolve_pk_sequence(events)

        survivor_id = resolution.get("survivor_id")
        final_survivor_ids.update(resolution.get("survivor_ids") or [])

        for dead_id in resolution.get("dead_ids") or []:
            sequence_duplicate_ids.add(dead_id)

        for dead_id in resolution.get("neutralized_ids") or []:
            neutralized_ids.add(dead_id)

        for dead_id in resolution.get("collapsed_ids") or []:
            collapsed_ids.add(dead_id)

        if survivor_id:
            final_survivor_ids.add(survivor_id)

    all_dead_ids = set()
    all_dead_ids.update(exact_duplicate_ids)
    all_dead_ids.update(sequence_duplicate_ids)
    all_dead_ids.update(neutralized_ids)
    all_dead_ids.update(collapsed_ids)

    all_dead_ids = sorted(all_dead_ids)
    exact_duplicate_ids = sorted(exact_duplicate_ids)
    sequence_duplicate_ids = sorted(sequence_duplicate_ids)
    neutralized_ids = sorted(neutralized_ids)
    collapsed_ids = sorted(collapsed_ids)
    final_survivor_ids = sorted(final_survivor_ids)
    
    
    decision_rows = []
    
    for event in pending_events:
        decision_type = "passthrough"
        reason_code = ""
    
        if event.id in exact_duplicate_ids:
            decision_type = "exact_duplicate"
            reason_code = DEDUP_ERROR_EXACT
        elif event.id in neutralized_ids:
            decision_type = "neutralized"
            reason_code = DEDUP_ERROR_NEUTRALIZED
        elif event.id in collapsed_ids:
            decision_type = "collapsed"
            reason_code = DEDUP_ERROR_COLLAPSED
        elif event.id in final_survivor_ids:
            decision_type = "survivor"
            reason_code = "DEDUP_SURVIVOR"
    
        survivor_event_id = 0
        pk_group_key = _build_pk_group_key(event)
        same_group_events = []
        if pk_group_key:
            same_group_events = pk_groups.get(pk_group_key) or []
            resolution = _resolve_pk_sequence(same_group_events) if same_group_events else {}
            survivor_event_id = int(resolution.get("survivor_id") or 0)
    
        decision_rows.append(
            SRDFDedupDecision(
                dedup_run=dedup_run,
                replication_service=replication_service,
                database_name=event.database_name or "",
                table_name=event.table_name or "",
                operation=event.operation or "",
                decision_type=decision_type,
                reason_code=reason_code,
                event_id=event.id,
                survivor_event_id=survivor_event_id,
                log_file=event.log_file or "",
                log_pos=int(event.log_pos or 0),
                group_key=pk_group_key or "",
                business_signature=str((event.event_payload or {}).get("business_dedup_signature") or ""),
                primary_key_data=event.primary_key_data or {},
                before_data=event.before_data or {},
                after_data=event.after_data or {},
                payload_preview={
                    "event_payload": event.event_payload or {},
                },
            )
        )
    
    if decision_rows:
        SRDFDedupDecision.objects.bulk_create(decision_rows, batch_size=500)        

    
    
    

    now = timezone.now()

    with transaction.atomic():
        if exact_duplicate_ids:
            OutboundChangeEvent.objects.filter(
                id__in=exact_duplicate_ids,
                replication_service=replication_service,
                status="pending",
            ).update(
                status="dead",
                last_error=DEDUP_ERROR_EXACT,
            )

        if neutralized_ids:
            OutboundChangeEvent.objects.filter(
                id__in=neutralized_ids,
                replication_service=replication_service,
                status="pending",
            ).update(
                status="dead",
                last_error=DEDUP_ERROR_NEUTRALIZED,
            )

        collapse_only_ids = [obj_id for obj_id in collapsed_ids if obj_id not in neutralized_ids]
        if collapse_only_ids:
            OutboundChangeEvent.objects.filter(
                id__in=collapse_only_ids,
                replication_service=replication_service,
                status="pending",
            ).update(
                status="dead",
                last_error=DEDUP_ERROR_COLLAPSED,
            )

    duplicate_count = len(all_dead_ids)
    survivor_count = len(final_survivor_ids)
    neutralized_count = len(neutralized_ids)
    collapsed_count = len(collapsed_ids)

    AuditEvent.objects.create(
        event_type="srdf_deduplicate_run",
        level="info" if duplicate_count == 0 else "warning",
        node=replication_service.source_node,
        message=f"Deduplicate completed for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "replication_service_id": replication_service.id,
            "processed_count": len(pending_events),
            "survivor_count": survivor_count,
            "duplicate_count": duplicate_count,
            "exact_duplicate_count": len(exact_duplicate_ids),
            "neutralized_count": neutralized_count,
            "collapsed_count": collapsed_count,
            "duplicate_event_ids_preview": all_dead_ids[:100],
            "survivor_event_ids_preview": final_survivor_ids[:100],
            "executed_at": now.isoformat(),
        },
    )
    
    finished_at = timezone.now()
    dedup_run.status = "warning" if duplicate_count > 0 else "success"
    dedup_run.processed_count = len(pending_events)
    dedup_run.survivor_count = survivor_count
    dedup_run.duplicate_count = duplicate_count
    dedup_run.exact_duplicate_count = len(exact_duplicate_ids)
    dedup_run.neutralized_count = neutralized_count
    dedup_run.collapsed_count = collapsed_count
    dedup_run.finished_at = finished_at
    dedup_run.duration_seconds = round((finished_at - started_at).total_seconds(), 3)
    dedup_run.result_payload = {
        "duplicate_event_ids_preview": all_dead_ids[:100],
        "survivor_event_ids_preview": final_survivor_ids[:100],
    }
    dedup_run.save(
        update_fields=[
            "status",
            "processed_count",
            "survivor_count",
            "duplicate_count",
            "exact_duplicate_count",
            "neutralized_count",
            "collapsed_count",
            "finished_at",
            "duration_seconds",
            "result_payload",
        ]
    )    
    

    return {
        "ok": True,
        "message": f"Deduplicate completed for service '{replication_service.name}'",
        "replication_service_id": replication_service.id,
        "processed_count": len(pending_events),
        "survivor_count": survivor_count,
        "duplicate_count"              : duplicate_count,
        "exact_duplicate_count"        : len(exact_duplicate_ids),
        "neutralized_count"            : neutralized_count,
        "collapsed_count"              : collapsed_count,
        "duplicate_event_ids"          : all_dead_ids,
        "duplicate_event_ids_preview"  : all_dead_ids[:100],
        "survivor_event_ids_preview"   : final_survivor_ids[:100],
        "neutralized_event_ids_preview": neutralized_ids[:100],
        "collapsed_event_ids_preview"  : collapsed_ids[:100],
        "skipped"      : False,
        "dedup_run_id" : dedup_run.id,
    }


def undo_deduplicate_run(
    dedup_run_id,
    initiated_by="srdf_deduplicate_undo",
    ):
    
    dedup_run = (
        SRDFDedupRun.objects
        .select_related("replication_service")
        .filter(id=dedup_run_id)
        .first()
    )
    if not dedup_run:
        raise DedupEngineError(f"SRDFDedupRun #{dedup_run_id} not found")

    replication_service = dedup_run.replication_service

    decision_rows = list(
        SRDFDedupDecision.objects
        .filter(dedup_run=dedup_run)
        .order_by("id")
    )

    if not decision_rows:
        return {
            "ok": True,
            "message": f"No SRDFDedupDecision rows found for dedup run #{dedup_run.id}",
            "dedup_run_id": dedup_run.id,
            "replication_service_id": replication_service.id if replication_service else 0,
            "restored_event_ids": [],
            "restored_count": 0,
            "deleted_decision_count": 0,
            "skipped": True,
        }

    event_ids_to_restore = sorted({
        int(row.event_id or 0)
        for row in decision_rows
        if row.decision_type in ("exact_duplicate", "neutralized", "collapsed")
        and int(row.event_id or 0) > 0
    })

    restored_count = 0

    with transaction.atomic():
        if event_ids_to_restore:
            restored_count = OutboundChangeEvent.objects.filter(
                id__in=event_ids_to_restore,
                replication_service=replication_service,
                status="dead",
            ).update(
                status="pending",
                last_error="",
            )

        deleted_decision_count, _ = SRDFDedupDecision.objects.filter(
            dedup_run=dedup_run
        ).delete()

        dedup_run.status = "undone"
        result_payload = dict(dedup_run.result_payload or {})
        result_payload["undo"] = {
            "undone_at": timezone.now().isoformat(),
            "undone_by": initiated_by,
            "restored_event_ids_preview": event_ids_to_restore[:100],
            "restored_count": int(restored_count or 0),
            "deleted_decision_count": int(deleted_decision_count or 0),
        }
        dedup_run.result_payload = result_payload
        dedup_run.save(update_fields=["status", "result_payload"])

    AuditEvent.objects.create(
        event_type="srdf_deduplicate_undo",
        level="warning",
        node=replication_service.source_node if replication_service else None,
        message=f"Deduplicate undo completed for service '{replication_service.name}'",
        created_by=initiated_by,
        payload={
            "dedup_run_id": dedup_run.id,
            "replication_service_id": replication_service.id if replication_service else 0,
            "restored_event_ids_preview": event_ids_to_restore[:100],
            "restored_count": int(restored_count or 0),
            "deleted_decision_count": int(deleted_decision_count or 0),
        },
    )

    return {
        "ok": True,
        "message": f"Deduplicate undo completed for service '{replication_service.name}'",
        "dedup_run_id": dedup_run.id,
        "replication_service_id": replication_service.id if replication_service else 0,
        "restored_event_ids": event_ids_to_restore,
        "restored_event_ids_preview": event_ids_to_restore[:100],
        "restored_count": int(restored_count or 0),
        "deleted_decision_count": int(deleted_decision_count or 0),
        "skipped": False,
    }


def _get_replication_service(replication_service_id):
    replication_service = (
        ReplicationService.objects
        .select_related("source_node", "target_node")
        .filter(id=replication_service_id, is_enabled=True)
        .first()
    )
    if not replication_service:
        raise DedupEngineError(
            f"Enabled ReplicationService #{replication_service_id} not found"
        )
    return replication_service


def _resolve_exact_signature(event):
    payload = event.event_payload or {}

    business_signature = str(payload.get("business_dedup_signature") or "").strip()
    if business_signature:
        return business_signature

    control_signature = str(payload.get("control_signature") or "").strip()
    if control_signature:
        return control_signature
    

    fallback_payload = {
        "engine": event.engine or "",
        "database_name": event.database_name or "",
        "table_name": event.table_name or "",
        "operation": event.operation or "",
        "primary_key_data": event.primary_key_data or {},
        "before_data": event.before_data or {},
        "after_data": event.after_data or {},
    }
    raw = json.dumps(
        fallback_payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()




def _build_pk_group_key(event):
    pk_data = event.primary_key_data or {}

    if not pk_data:
        after_data = event.after_data or {}
        before_data = event.before_data or {}

        if "id" in after_data:
            pk_data = {"id": after_data.get("id")}
        elif "id" in before_data:
            pk_data = {"id": before_data.get("id")}
        else:
            merged_keys = sorted(set(list(before_data.keys()) + list(after_data.keys())))
            for key in merged_keys:
                if str(key).lower().endswith("_id"):
                    if key in after_data:
                        pk_data = {key: after_data.get(key)}
                        break
                    if key in before_data:
                        pk_data = {key: before_data.get(key)}
                        break

    if not pk_data:
        return ""

    payload = {
        "engine": event.engine or "",
        "database_name": event.database_name or "",
        "table_name": event.table_name or "",
        "primary_key_data": pk_data,
    }

    return json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    )
    
    



def _resolve_pk_sequence(events):
    events = sorted(events, key=lambda obj: (obj.id, obj.log_pos))
    if len(events) == 1:
        return {
            "survivor_id": events[0].id,
            "survivor_ids": [events[0].id],
            "dead_ids": [],
            "neutralized_ids": [],
            "collapsed_ids": [],
        }

    ops = [str(obj.operation or "").strip().lower() for obj in events]
    first_op = ops[0]
    last_op = ops[-1]

    #-------------------------------------------------------------------
    # Case A: insert + delete => neutralized sequence, keep nothing
    #-------------------------------------------------------------------
    if first_op == "insert" and last_op == "delete":
        return {
            "survivor_id": 0,
            "survivor_ids": [],
            "dead_ids": [obj.id for obj in events],
            "neutralized_ids": [obj.id for obj in events],
            "collapsed_ids": [],
        }


    #__________________________________________________________________________________________________
    # Case B: insert + updates => keep insert + last update, collapse intermediate updates
    #--------------------------------------------------------------------------------------------------
    if first_op == "insert" and all(op in ("insert", "update") for op in ops):
        if len(events) == 1:
            return {
                "survivor_id": events[0].id,
                "survivor_ids": [events[0].id],
                "dead_ids": [],
                "neutralized_ids": [],
                "collapsed_ids": [],
            }
    
        last_update = None
        for obj in reversed(events):
            if str(obj.operation or "").strip().lower() == "update":
                last_update = obj
                break
    
        if last_update is None:
            return {
                "survivor_id": events[0].id,
                "survivor_ids": [events[0].id],
                "dead_ids": [obj.id for obj in events[1:]],
                "neutralized_ids": [],
                "collapsed_ids": [obj.id for obj in events[1:]],
            }
    
        survivor_ids = [events[0].id, last_update.id]
        dead_ids = [
            obj.id
            for obj in events
            if obj.id not in survivor_ids
        ]
    
        return {
            "survivor_id": last_update.id,
            "survivor_ids": survivor_ids,
            "dead_ids": dead_ids,
            "neutralized_ids": [],
            "collapsed_ids": dead_ids,
        } 
    
    #-----------------------------------------------------------------------
    # Case C: updates only => keep last update, collapse previous ones
    #------------------------------------------------------------------------
    if all(op == "update" for op in ops):
        survivor = events[-1]
        return {
            "survivor_id": survivor.id,
            "survivor_ids": [survivor.id],
            "dead_ids": [obj.id for obj in events[:-1]],
            "neutralized_ids": [],
            "collapsed_ids": [obj.id for obj in events[:-1]],
        }


    #--------------------------------------------------------------------------
    # Case D: update(s) + delete => keep delete, collapse previous updates
    #-------------------------------------------------------------------------
    if last_op == "delete" and all(op in ("update", "delete") for op in ops):
        survivor = events[-1]
        return {
            "survivor_id": survivor.id,
            "survivor_ids": [survivor.id],
            "dead_ids": [obj.id for obj in events[:-1]],
            "neutralized_ids": [],
            "collapsed_ids": [obj.id for obj in events[:-1]],
        }


    #---------------------------------------------------------------------
    # Case E: delete only or other mixed sequence => keep last event
    #---------------------------------------------------------------------
    survivor = events[-1]
    return {
        "survivor_id": survivor.id,
        "survivor_ids": [survivor.id],
        "dead_ids": [obj.id for obj in events[:-1]],
        "neutralized_ids": [],
        "collapsed_ids": [obj.id for obj in events[:-1]],
    }