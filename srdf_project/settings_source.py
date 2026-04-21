from .settings_base import *

ADMIN_EXTENDED = True
ROOT_URLCONF = "srdf_project.urls_source"

SRDF_NODE_ROLE = "source"

DATABASES["default"]["NAME"] = os.environ.get(
    "SRDF_SOURCE_CONTROL_DB_NAME",
    "srdf_source_control",
)

DATABASES["default"]["PORT"] = os.environ.get(
    "SRDF_SOURCE_CONTROL_DB_PORT",
    "3306",
)

DATABASES["srdf_archive"] = {
    "ENGINE"   : "django.db.backends.mysql",
    "NAME"     : "SRDF_archive",
    "USER"     : os.environ.get("SRDF_CONTROL_DB_USER", "root"),
    "PASSWORD" : os.environ.get("SRDF_CONTROL_DB_PASSWORD", ""),
    "HOST"     : os.environ.get("SRDF_CONTROL_DB_HOST", "127.0.0.1"),
    "PORT"     : os.environ.get("SRDF_CONTROL_DB_PORT", "3306"),
    "OPTIONS": {
        "charset"      : "utf8mb4",
        "use_unicode"  : True,
        "init_command" : "SET foreign_key_checks = 0; SET sql_mode='STRICT_TRANS_TABLES'; SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci' ",
    },
}