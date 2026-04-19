#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

from django.conf import settings
from django.http import JsonResponse


def get_expected_token():
    return getattr(settings, "SRDF_API_TOKEN", "").strip()


def check_api_token(request):
    expected = get_expected_token()
    if not expected:
        return JsonResponse(
            {"ok": False, "error": "SRDF_API_TOKEN is not configured"},
            status=500,
        )

    auth = request.headers.get("Authorization", "").strip()
    if not auth.startswith("Bearer "):
        return JsonResponse(
            {"ok": False, "error": "Missing or invalid Authorization header"},
            status=401,
        )

    token = auth[len("Bearer "):].strip()
    if token != expected:
        return JsonResponse(
            {"ok": False, "error": "Invalid token"},
            status=403,
        )

    return None