from pathlib import Path
import os
import sys
import mimetypes


# ---------------------------------------------
# ---------------------------------------------
# -- mime type hack for css and Js Libraries
# ---------------------------------------------
# ---------------------------------------------
mimetypes.init()
mimetypes.types_map['.css']    = 'text/css'
mimetypes.types_map['.less']   = 'text/css'
mimetypes.types_map['.js']     = 'text/javascript'
mimetypes.add_type("image/svg+xml", ".svg", True)
mimetypes.add_type("application/octet-stream", ".html", True)

ADMIN_EXTENDED = True
ENABLE_DJANGO_NEWS = False
BASE_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = BASE_DIR.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


SECRET_KEY = os.environ.get("SRDF_SECRET_KEY", "srdf-dev-secret-key")
DEBUG = os.environ.get("SRDF_DEBUG", "1") == "1"

ALLOWED_HOSTS = [
    host.strip()
    for host in os.environ.get(
        "SRDF_ALLOWED_HOSTS",
        "127.0.0.1,localhost,testserver"
    ).split(",")
    if host.strip()
]

INSTALLED_APPS = [
    #==================================
    #==== ADMIN DEFINITIONS         ===
    'admin_tools'                        ,
    'admin_tools.theming'                ,
    'admin_tools.menu'                   ,
    'admin_tools.dashboard'              ,
    "ideolab_admin_tools.apps.IdeolabAdminToolsConfig",
    
    "django.contrib.admin"               ,
    "django.contrib.auth"                ,
    "django.contrib.contenttypes"        ,
    "django.contrib.sessions"            ,
    "django.contrib.messages"            ,
    "django.contrib.staticfiles"         ,
    "django.contrib.sites"               ,
    'django.contrib.flatpages'           ,
    'django.contrib.humanize'            ,
    'django.contrib.sitemaps'            ,
    
    "oauth2_provider"                    ,
    'compressor'                         ,
    "srdf"                               ,
    'migrate2026'                        ,
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

ROOT_URLCONF = "srdf_project.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [REPO_ROOT / "templates",],
        "APP_DIRS": False,
        "OPTIONS": {
            'debug' : True,
            'loaders': [
                'admin_tools.template_loaders.Loader'                , 
                ('django.template.loaders.cached.Loader'             , [
                    'django.template.loaders.filesystem.Loader'      ,
                    'django.template.loaders.app_directories.Loader' ,
                ]),
            ],		            
            
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
                'django.template.context_processors.media'    ,
                "django.template.context_processors.static",
            ],
        },
    },
]

WSGI_APPLICATION = "srdf_project.wsgi.application"
ASGI_APPLICATION = "srdf_project.asgi.application"

DATABASES = {
    "default": {
        "ENGINE"    : "django.db.backends.mysql",
        "NAME"      : os.environ.get("SRDF_CONTROL_DB_NAME", "srdf_control"),
        "USER"      : os.environ.get("SRDF_CONTROL_DB_USER", "root"),
        "PASSWORD"  : os.environ.get("SRDF_CONTROL_DB_PASSWORD", ""),
        "HOST"      : os.environ.get("SRDF_CONTROL_DB_HOST", "127.0.0.1"),
        "PORT"      : os.environ.get("SRDF_CONTROL_DB_PORT", "3306"),
        "CONN_MAX_AGE": 300,
        "OPTIONS": {
            "charset"     : "utf8mb4",
            "use_unicode" : True,
            "init_command": (
                "SET foreign_key_checks = 0; "
                "SET sql_mode='STRICT_TRANS_TABLES'; "
                "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_unicode_ci'"
            ),
        },
    }
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

AUTH_USER_MODEL = "auth.User"

PASSWORD_HASHERS = [
    "django.contrib.auth.hashers.MD5PasswordHasher",
]

SRDF_API_TOKEN = os.environ.get("SRDF_API_TOKEN", "change-me-srdf-token")

LOGIN_URL = "/admin/login/"
ADMIN_URL = "admin-api"

SESSION_COOKIE_NAME = "srdf_source_sessionid"
CSRF_COOKIE_NAME = "srdf_source_csrftoken"
SESSION_COOKIE_PATH = "/"
CSRF_COOKIE_PATH = "/"
SESSION_COOKIE_DOMAIN = None
CSRF_COOKIE_DOMAIN = None


# -----------------------------------------------
# -----------------------------------------------
# ----  LOCAL   SETTINGS ON EACH SERVER
# -----------------------------------------------
# -----------------------------------------------
try:
    from srdf_project.local_settings import *
except ImportError as e:
    pass  



# --------------------------------
# ----  ADMIN EXTENDED         ---
# --------------------------------
if ADMIN_EXTENDED == True:
    LOGIN_URL='/%s/login/'  % (ADMIN_URL)
    
    
#  -----------------------------------------------------
#  -----------------------------------------------------
#  ---  A D M I N       S E T T I N G S              ---
#  -----------------------------------------------------
#  -----------------------------------------------------
ADMIN_URL                          =  'admin-api'       #  --- official django admin url               ---
ADMIN_EXTENDED                     =  True              # ----  Extended Admin Tools                   ---
ADMIN_DESIGN                       =  False
ADMIN_PLUS                         =  False             # ----  Enable Django Admin Plus               ---
ADMIN_LOGIN_FLAG                   = 'admin_login'      # --- detect if admin login mode                            ---
ADMIN_LOGIN_CALLER                 = 'admin_caller'     # --- detect original User who did a Fake admin login       --- 
ADMIN_LOGIN_URL_RETURN             = 'admin_return'     # --- detect original Url from where User who did a Fake admin login  --- 

ADMIN_TOOLS_INDEX_DASHBOARD        =   {
    'django.contrib.admin.site'                                : 'srdf_project.dashboard_parameters.CustomIndexDashboard',               #  --- Koclicko appli --
    'srdf_project.admin_parameters.parameters_admin_site'      : 'srdf_project.dashboard_parameters.CustomIndexDashboard',    #  --- Koclicko appli        --
}

ADMIN_TOOLS_APP_INDEX_DASHBOARD    =   {
    'django.contrib.admin.site'                                : 'srdf_project.dashboard_parameters.CustomAppIndexDashboard',             #  --- Koclicko appli --
    'srdf_project.admin_parameters.parameters_admin_site'      : 'srdf_project.dashboard_parameters.CustomAppIndexDashboard',  #  --- Koclicko appli        --
}

ADMIN_TOOLS_MENU                   =  {
    'srdf_project.admin_parameters.parameters_admin_site'      : 'srdf_project.menu_parameters.CustomMenu',
}
ADMIN_TOOLS_THEMING_CSS            =    'css/pages/admin.min.css'


