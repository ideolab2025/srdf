#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from oxygen_site.settings import *

# ============================================================
# SRDF REMOTE TEST SETTINGS
# Dedicated Django control plane for remote/target simulation
# ============================================================

SITE_NAME = "127.0.0.1:8001"
DOMAIN = "127.0.0.1:8001"
MAIN_DOMAIN = "127.0.0.1:8001"
HOST_API = "127.0.0.1:8001"

# ------------------------------------------------------------
# Remote SRDF control DB on MariaDB 3307
# ------------------------------------------------------------
SRDF_REMOTE_DB_NAME = "srdf_remote_control"
SRDF_REMOTE_TARGET_DB_NAME = "target_db"

DATABASES["default"] = {
    "ENGINE": "django.db.backends.mysql",
    "NAME": SRDF_REMOTE_DB_NAME,
    "USER": "root",
    "PASSWORD": "",
    "HOST": "127.0.0.1",
    "PORT": "3307",
    "CONN_MAX_AGE": 300,
    "OPTIONS": {
        "charset": "utf8mb4",
        "use_unicode": True,
        "init_command": (
            "SET foreign_key_checks = 0; "
            "SET sql_mode='STRICT_TRANS_TABLES'; "
            "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci' "
        ),
    },
}

# Optional archive DB on remote engine too
DATABASES["srdf_archive"] = {
    "ENGINE": "django.db.backends.mysql",
    "NAME": "srdf_remote_archive",
    "USER": "root",
    "PASSWORD": "",
    "HOST": "127.0.0.1",
    "PORT": "3307",
    "CONN_MAX_AGE": 300,
    "OPTIONS": {
        "charset": "utf8mb4",
        "use_unicode": True,
        "init_command": (
            "SET foreign_key_checks = 0; "
            "SET sql_mode='STRICT_TRANS_TABLES'; "
            "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci' "
        ),
    },
}

# ------------------------------------------------------------
# SRDF API token for local laptop simulation
# ------------------------------------------------------------
SRDF_API_TOKEN = "change-me-srdf-token"

# ------------------------------------------------------------
# Make it explicit in console logs
# ------------------------------------------------------------
print("============================================================")
print("SRDF REMOTE SETTINGS ACTIVE")
print(f"default DB  : {DATABASES['default']['NAME']} @ {DATABASES['default']['HOST']}:{DATABASES['default']['PORT']}")
print(f"target DB   : {SRDF_REMOTE_TARGET_DB_NAME}")
print(f"SITE_NAME   : {SITE_NAME}")
print("============================================================")