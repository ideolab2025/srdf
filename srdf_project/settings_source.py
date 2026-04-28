from .settings_base import *


#==================================
#==== LICENSE CENTER            ===
#==================================
IDEOLAB_LICENSE_API_URL = "http://127.0.0.1:8000"
IDEOLAB_LICENSE_KEY = "TEST-KEY"

IDEOLAB_PRODUCT_CODE = "ideolab_admin_tools"
IDEOLAB_PRODUCT_VERSION = "1.0.0"

IDEOLAB_LICENSE_TIMEOUT = 5
IDEOLAB_LICENSE_CACHE_PATH = BASE_DIR / "ideolab_license_cache.json"
IDEOLAB_LICENSE_DOMAIN = "localhost"
IDEOLAB_LICENSE_HOSTNAME = "devbox"

#===== END LICENSE KEY               ===
#=======================================

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