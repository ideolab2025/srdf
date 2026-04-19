from .settings_base import *


ROOT_URLCONF = "srdf_project.urls_target"

SRDF_NODE_ROLE = "target"

DATABASES["default"]["NAME"] = os.environ.get(
    "SRDF_TARGET_CONTROL_DB_NAME",
    "srdf_target_control",
)

DATABASES["default"]["PORT"] = os.environ.get(
    "SRDF_TARGET_CONTROL_DB_PORT",
    "3307",
)

DATABASES["srdf_archive"] = {
    "ENGINE": "django.db.backends.mysql",
    "NAME": os.environ.get("SRDF_ARCHIVE_DB_NAME", "srdf_remote_archive"),
    "USER": os.environ.get("SRDF_ARCHIVE_DB_USER", "root"),
    "PASSWORD": os.environ.get("SRDF_ARCHIVE_DB_PASSWORD", ""),
    "HOST": os.environ.get("SRDF_ARCHIVE_DB_HOST", "127.0.0.1"),
    "PORT": os.environ.get("SRDF_ARCHIVE_DB_PORT", "3307"),
    "CONN_MAX_AGE": 300,
    "OPTIONS": {
        "charset": "utf8mb4",
        "use_unicode": True,
        "init_command": (
            "SET foreign_key_checks = 0; "
            "SET sql_mode='STRICT_TRANS_TABLES'; "
            "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
        ),
    },
}

SRDF_REMOTE_TARGET_DB_NAME = os.environ.get(
    "SRDF_REMOTE_TARGET_DB_NAME",
    "target_db",
)