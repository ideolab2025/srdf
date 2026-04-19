import os

from django.core.wsgi import get_wsgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "srdf_project.settings_source")

application = get_wsgi_application()