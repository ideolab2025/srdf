#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import requests


DEFAULT_TIMEOUT = 10


class NodeApiError(Exception):
    pass


def _build_headers(node):
    token = (node.api_token or "").strip()
    headers = {
        "Accept": "application/json",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return headers


def _build_url(node, path):
    base = (node.api_base_url or "").strip().rstrip("/")
    if not base:
        raise NodeApiError(f"Node '{node.name}' has no api_base_url configured")
    return f"{base}/{path.lstrip('/')}"


def ping_node(node, timeout=DEFAULT_TIMEOUT):
    url = _build_url(node, "/ping/")
    try:
        response = requests.get(url, headers=_build_headers(node), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise NodeApiError(f"Ping failed for node '{node.name}': {exc}") from exc


def get_status(node, timeout=DEFAULT_TIMEOUT):
    url = _build_url(node, "/status/")
    try:
        response = requests.get(url, headers=_build_headers(node), timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as exc:
        raise NodeApiError(f"Status failed for node '{node.name}': {exc}") from exc


def run_remote_action(node, action_name, params=None, timeout=DEFAULT_TIMEOUT):
    params = params or {}
    url = _build_url(node, "/action/")
    payload = {
        "action_name": action_name,
        "node_id": node.id,
        "params": params,
    }
    try:
        response = requests.post(
            url,
            headers=_build_headers(node),
            json=payload,
            timeout=timeout,
        )

        if response.status_code >= 400:
            response_text = ""
            try:
                response_text = response.text
            except Exception:
                response_text = "<no response body>"

            raise NodeApiError(
                f"Action '{action_name}' failed for node '{node.name}': "
                f"HTTP {response.status_code} response={response_text}"
            )

        data = response.json()
        return {
            "ok": True,
            "status_code": response.status_code,
            "data": data,
        }
    except requests.RequestException as exc:
        raise NodeApiError(
            f"Action '{action_name}' failed for node '{node.name}': {exc}"
        ) from exc