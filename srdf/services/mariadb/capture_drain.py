#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

def _parse_pos(value, default=0):
    try:
        return int(value or default)
    except Exception:
        return int(default)


def normalize_retry_limit(value, fallback=2, minimum=0, maximum=5):
    try:
        parsed = int(value if value is not None else fallback)
    except Exception:
        parsed = int(fallback)

    if parsed < int(minimum):
        parsed = int(minimum)
    if parsed > int(maximum):
        parsed = int(maximum)

    return parsed


def should_retry_after_internal_noise(
    result,
    attempt=0,
    retry_limit=2,
):
    result = result or {}

    stop_reason = str(result.get("stop_reason") or "").strip()
    if stop_reason != "internal_noise_only":
        return False

    captured_count = _parse_pos(result.get("captured_count"), 0)
    if captured_count > 0:
        return False

    attempt = _parse_pos(attempt, 0)
    retry_limit = normalize_retry_limit(retry_limit, fallback=2)

    if attempt >= retry_limit:
        return False

    read_log_file = str(result.get("read_log_file") or "").strip()
    business_log_file = str(result.get("business_log_file") or "").strip()
    read_log_pos = _parse_pos(result.get("read_log_pos"), 0)
    business_log_pos = _parse_pos(result.get("business_log_pos"), 0)

    current_master_after_file = str(result.get("current_master_after_file") or "").strip()
    current_master_after_pos = _parse_pos(result.get("current_master_after_pos"), 0)

    if not read_log_file:
        return False

    # read cursor must have progressed beyond business checkpoint
    if read_log_file == business_log_file and read_log_pos <= business_log_pos:
        return False

    # if we are already at/after current master, no reason to retry
    if read_log_file == current_master_after_file and read_log_pos >= current_master_after_pos:
        return False

    return True