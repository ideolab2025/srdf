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