import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "srdf_project.settings_source")

application = get_asgi_application()