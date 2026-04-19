from django.contrib import admin
from django.http import HttpResponse
from django.shortcuts import redirect
from django.urls import include, path
from srdf_project.admin_parameters import  parameters_admin_site
from django.conf import settings
from django.urls                  import  re_path as url


if settings.ADMIN_EXTENDED:
    adminUrl  = 'admin-system'
else:
    adminUrl  = 'admin'

adminUrlParameters         =  settings.ADMIN_URL

def root_redirect(request):
    return redirect("/srdf/")


def health(request):
    return HttpResponse("srdf_project:ok")


urlpatterns = [
    path("", root_redirect, name="root_redirect"),
    path("health/", health, name="health"),
    path("srdf/", include("srdf.urls")),
]


if settings.ADMIN_EXTENDED:
    urlpatterns += [
        #  --------------------------------------
        #  ---   admin Url                    ---
        #  --------------------------------------
        url(r'admin_tools/', include('admin_tools.urls')),
        url(r'^%s/' % adminUrl           , admin.site.urls),
        url(r'^%s/' % adminUrlParameters , parameters_admin_site.urls),
    ]
else:
    urlpatterns += [
        path('admin/' , admin.site.urls),
        ]