#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.db import transaction
from django.utils import timezone

from srdf.models import ExecutionLock


class LockError(Exception):
    pass


def acquire_lock(lock_name):
    with transaction.atomic():
        lock, _ = ExecutionLock.objects.select_for_update().get_or_create(
            name=lock_name,
            defaults={
                "is_locked": False,
                "locked_at": None,
            },
        )

        if lock.is_locked:
            raise LockError(f"Lock '{lock_name}' is already active")

        lock.is_locked = True
        lock.locked_at = timezone.now()
        lock.save(update_fields=["is_locked", "locked_at"])
        return lock


def release_lock(lock_name):
    with transaction.atomic():
        lock, _ = ExecutionLock.objects.select_for_update().get_or_create(
            name=lock_name,
            defaults={
                "is_locked": False,
                "locked_at": None,
            },
        )
        lock.is_locked = False
        lock.locked_at = None
        lock.save(update_fields=["is_locked", "locked_at"])
        return lock


def is_lock_active(lock_name):
    lock = ExecutionLock.objects.filter(name=lock_name).first()
    if not lock:
        return False
    return bool(lock.is_locked)