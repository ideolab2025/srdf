# -*- coding: utf-8 -*-

from srdf.services.settings_resolver import (
    get_srdf_setting_int,
)


def estimate_backlog_bytes(
    binary_logs,
    binary_log_size_map,
    start_idx,
    end_idx,
    start_log_pos,
    current_master_before_pos,
):
    if start_idx < 0 or end_idx < 0 or end_idx < start_idx:
        return 0

    total_bytes = 0

    for idx in range(start_idx, end_idx + 1):
        log_name = binary_logs[idx]
        file_size = int(binary_log_size_map.get(log_name) or 0)

        if idx == start_idx and idx == end_idx:
            total_bytes += max(int(current_master_before_pos or 0) - int(start_log_pos or 0), 0)
        elif idx == start_idx:
            total_bytes += max(file_size - int(start_log_pos or 0), 0)
        elif idx == end_idx:
            total_bytes += max(int(current_master_before_pos or 0), 0)
        else:
            total_bytes += max(file_size, 0)

    return max(int(total_bytes or 0), 0)


def resolve_adaptive_max_scanned_events(
    replication_service,
    checkpoint,
    resolved_engine,
    explicit_value,
    backlog_bytes,
    backlog_file_count,
):
    if explicit_value is not None:
        return max(int(explicit_value or 1), 1), False

    min_value = get_srdf_setting_int(
        key="capture.max_scanned_events_min",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=500,
        minimum=1,
    )

    hard_cap = get_srdf_setting_int(
        key="capture.max_scanned_events_hard_cap",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=50000,
        minimum=100,
    )

    bytes_per_event_hint = get_srdf_setting_int(
        key="capture.adaptive_bytes_per_event_hint",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=2048,
        minimum=128,
    )

    file_weight = get_srdf_setting_int(
        key="capture.adaptive_file_weight",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=1500,
        minimum=0,
    )

    previous_data = checkpoint.checkpoint_data or {}
    previous_scanned = int(previous_data.get("scanned_event_count") or 0)
    previous_skipped = int(previous_data.get("skipped_count") or 0)
    previous_captured = int(previous_data.get("captured_count") or 0)

    adaptive_value = int(min_value or 500)

    estimated_from_bytes = 0
    if int(bytes_per_event_hint or 0) > 0:
        estimated_from_bytes = int(float(backlog_bytes or 0) / float(bytes_per_event_hint))

    adaptive_value = max(adaptive_value, estimated_from_bytes)
    adaptive_value += max(int(backlog_file_count or 0) - 1, 0) * int(file_weight or 0)

    if previous_scanned > 0:
        skip_ratio = float(previous_skipped) / float(previous_scanned)
        if skip_ratio >= 0.90:
            adaptive_value = int(float(adaptive_value) * 2.0)
        elif skip_ratio >= 0.75:
            adaptive_value = int(float(adaptive_value) * 1.5)

    if previous_captured <= 0 and previous_skipped > 0:
        adaptive_value = int(float(adaptive_value) * 1.5)

    adaptive_value = max(adaptive_value, int(min_value or 1))
    adaptive_value = min(adaptive_value, int(hard_cap or adaptive_value))

    return max(int(adaptive_value or 1), 1), True


def resolve_adaptive_scan_window_max_files(
    replication_service,
    resolved_engine,
    explicit_value,
    backlog_file_count,
):
    if explicit_value is not None:
        return max(int(explicit_value or 1), 1), False

    min_value = get_srdf_setting_int(
        key="capture.scan_window_max_files_min",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=2,
        minimum=1,
    )

    hard_cap = get_srdf_setting_int(
        key="capture.scan_window_max_files_hard_cap",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=12,
        minimum=1,
    )

    adaptive_value = max(int(min_value or 1), int(backlog_file_count or 0))
    adaptive_value = min(adaptive_value, int(hard_cap or adaptive_value))

    return max(int(adaptive_value or 1), 1), True


def resolve_adaptive_scan_window_max_bytes(
    replication_service,
    resolved_engine,
    explicit_value,
    backlog_bytes,
):
    if explicit_value is not None:
        return max(int(explicit_value or 1), 1), False

    min_value = get_srdf_setting_int(
        key="capture.scan_window_max_bytes_min",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=64 * 1024 * 1024,
        minimum=1024,
    )

    hard_cap = get_srdf_setting_int(
        key="capture.scan_window_max_bytes_hard_cap",
        replication_service=replication_service,
        engine=resolved_engine,
        fallback=512 * 1024 * 1024,
        minimum=1024,
    )

    adaptive_value = max(int(min_value or 0), int(backlog_bytes or 0))
    adaptive_value = min(adaptive_value, int(hard_cap or adaptive_value))

    return max(int(adaptive_value or 1), 1), True


def build_capture_window_plan(
    replication_service,
    checkpoint,
    resolved_engine,
    binary_logs,
    binary_log_size_map,
    start_log_file,
    start_log_pos,
    current_master_before_file,
    current_master_before_pos,
    requested_max_scanned_events=None,
    requested_scan_window_max_files=None,
    requested_scan_window_max_bytes=None,
):
    if not start_log_file:
        raise ValueError("Missing start_log_file")

    if start_log_file not in binary_logs:
        raise ValueError(f"Checkpoint file not found in SHOW BINARY LOGS: {start_log_file}")

    if current_master_before_file not in binary_logs:
        raise ValueError(
            f"Current master file not found in SHOW BINARY LOGS: {current_master_before_file}"
        )

    start_idx = binary_logs.index(start_log_file)
    end_idx = binary_logs.index(current_master_before_file)

    if start_idx > end_idx:
        raise ValueError(
            f"Checkpoint file is after current master file: checkpoint={start_log_file} master={current_master_before_file}"
        )

    backlog_file_count = max((end_idx - start_idx) + 1, 0)
    backlog_bytes = estimate_backlog_bytes(
        binary_logs=binary_logs,
        binary_log_size_map=binary_log_size_map,
        start_idx=start_idx,
        end_idx=end_idx,
        start_log_pos=start_log_pos,
        current_master_before_pos=current_master_before_pos,
    )

    max_scanned_events, adaptive_max_scanned_events_used = resolve_adaptive_max_scanned_events(
        replication_service=replication_service,
        checkpoint=checkpoint,
        resolved_engine=resolved_engine,
        explicit_value=requested_max_scanned_events,
        backlog_bytes=backlog_bytes,
        backlog_file_count=backlog_file_count,
    )

    scan_window_max_files, adaptive_scan_window_max_files_used = resolve_adaptive_scan_window_max_files(
        replication_service=replication_service,
        resolved_engine=resolved_engine,
        explicit_value=requested_scan_window_max_files,
        backlog_file_count=backlog_file_count,
    )

    scan_window_max_bytes, adaptive_scan_window_max_bytes_used = resolve_adaptive_scan_window_max_bytes(
        replication_service=replication_service,
        resolved_engine=resolved_engine,
        explicit_value=requested_scan_window_max_bytes,
        backlog_bytes=backlog_bytes,
    )

    max_files = max(int(scan_window_max_files or 1), 1)
    max_bytes = max(int(scan_window_max_bytes or 0), 0)

    window_end_idx = min(end_idx, start_idx + max_files - 1)
    window_end_file = binary_logs[window_end_idx]

    if start_idx == end_idx and window_end_file == current_master_before_file:
        if max_bytes > 0:
            window_end_pos = min(
                int(current_master_before_pos or 0),
                int(start_log_pos or 0) + max_bytes,
            )
        else:
            window_end_pos = int(current_master_before_pos or 0)
    elif window_end_idx < end_idx:
        window_end_pos = int(binary_log_size_map.get(window_end_file) or 0)
    else:
        window_end_pos = int(current_master_before_pos or 0)

    is_empty_window = (
        window_end_idx == start_idx and window_end_pos <= int(start_log_pos or 0)
    )

    compare_ready = (
        str(start_log_file or "").strip(),
        int(start_log_pos or 0),
        str(window_end_file or "").strip(),
        int(window_end_pos or 0),
    )

    return {
        "start_idx": int(start_idx),
        "end_idx": int(end_idx),
        "backlog_file_count": int(backlog_file_count or 0),
        "backlog_bytes": int(backlog_bytes or 0),
        "max_scanned_events": int(max_scanned_events or 0),
        "adaptive_max_scanned_events_used": bool(adaptive_max_scanned_events_used),
        "scan_window_max_files": int(scan_window_max_files or 0),
        "adaptive_scan_window_max_files_used": bool(adaptive_scan_window_max_files_used),
        "scan_window_max_bytes": int(scan_window_max_bytes or 0),
        "adaptive_scan_window_max_bytes_used": bool(adaptive_scan_window_max_bytes_used),
        "window_end_idx": int(window_end_idx),
        "window_end_file": window_end_file or "",
        "window_end_pos": int(window_end_pos or 0),
        "binlog_files_to_read": list(binary_logs[start_idx:window_end_idx + 1]),
        "is_empty_window": bool(is_empty_window),
        "compare_ready": {
            "start_log_file": compare_ready[0],
            "start_log_pos": compare_ready[1],
            "window_end_file": compare_ready[2],
            "window_end_pos": compare_ready[3],
            },
    }


def is_empty_window_plan(plan):
    plan = plan or {}
    return bool(plan.get("is_empty_window"))