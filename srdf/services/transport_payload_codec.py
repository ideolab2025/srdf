#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import base64
import hashlib
import json
import zlib


class TransportPayloadCodecError(Exception):
    pass


DEFAULT_COMPRESSION = "zlib"
DEFAULT_ENCRYPTION = "xor"


def build_logical_payload(
    replication_service,
    payload_events,
    extra_payload=None,
):
    payload = {
        "version": 1,
        "engine": replication_service.service_type,
        "replication_service_id": replication_service.id,
        "replication_service_name": replication_service.name,
        "source_node_id": replication_service.source_node_id,
        "target_node_id": replication_service.target_node_id,
        "event_count": len(payload_events or []),
        "events": payload_events or [],
    }

    if extra_payload and isinstance(extra_payload, dict):
        payload.update(extra_payload)

    return payload


def encode_transport_payload(
    logical_payload,
    compression=DEFAULT_COMPRESSION,
    encryption=DEFAULT_ENCRYPTION,
    encryption_key="",
):
    if not isinstance(logical_payload, dict):
        raise TransportPayloadCodecError("logical_payload must be a dict")

    raw_json_bytes = json.dumps(
        logical_payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    logical_checksum = hashlib.sha256(raw_json_bytes).hexdigest()

    compressed_bytes = _compress_bytes(
        raw_json_bytes,
        compression=compression,
    )

    encrypted_bytes = _encrypt_bytes(
        compressed_bytes,
        encryption=encryption,
        encryption_key=encryption_key,
    )

    encoded_payload = base64.b64encode(encrypted_bytes).decode("ascii")

    return {
        "encoded_payload": encoded_payload,
        "logical_checksum": logical_checksum,
        "logical_size_bytes": len(raw_json_bytes),
        "compressed_size_bytes": len(compressed_bytes),
        "encoded_size_bytes": len(encoded_payload.encode("utf-8")),
        "compression": compression,
        "encryption": encryption,
        "transport_meta": {
            "version": 1,
            "content_type": "application/json",
            "compression": compression,
            "encryption": encryption,
            "logical_checksum": logical_checksum,
            "logical_size_bytes": len(raw_json_bytes),
            "compressed_size_bytes": len(compressed_bytes),
            "encoded_size_bytes": len(encoded_payload.encode("utf-8")),
        },
    }


def decode_transport_payload(
    encoded_payload,
    compression=DEFAULT_COMPRESSION,
    encryption=DEFAULT_ENCRYPTION,
    encryption_key="",
    expected_logical_checksum="",
):
    if not encoded_payload:
        raise TransportPayloadCodecError("encoded_payload is required")

    try:
        encrypted_bytes = base64.b64decode(encoded_payload.encode("ascii"))
    except Exception as exc:
        raise TransportPayloadCodecError(f"Invalid base64 encoded payload: {exc}")

    compressed_bytes = _decrypt_bytes(
        encrypted_bytes,
        encryption=encryption,
        encryption_key=encryption_key,
    )

    raw_json_bytes = _decompress_bytes(
        compressed_bytes,
        compression=compression,
    )

    logical_checksum = hashlib.sha256(raw_json_bytes).hexdigest()

    if expected_logical_checksum and logical_checksum != str(expected_logical_checksum):
        raise TransportPayloadCodecError(
            "Logical payload checksum mismatch after decode"
        )

    try:
        logical_payload = json.loads(raw_json_bytes.decode("utf-8"))
    except Exception as exc:
        raise TransportPayloadCodecError(f"Decoded payload is not valid JSON: {exc}")

    return {
        "logical_payload": logical_payload,
        "logical_checksum": logical_checksum,
        "logical_size_bytes": len(raw_json_bytes),
    }


def _compress_bytes(raw_bytes, compression=DEFAULT_COMPRESSION):
    compression = str(compression or "").strip().lower()

    if compression in ("", "none"):
        return raw_bytes

    if compression == "zlib":
        return zlib.compress(raw_bytes, level=6)

    raise TransportPayloadCodecError(
        f"Unsupported compression '{compression}'"
    )


def _decompress_bytes(raw_bytes, compression=DEFAULT_COMPRESSION):
    compression = str(compression or "").strip().lower()

    if compression in ("", "none"):
        return raw_bytes

    if compression == "zlib":
        try:
            return zlib.decompress(raw_bytes)
        except Exception as exc:
            raise TransportPayloadCodecError(f"zlib decompress failed: {exc}")

    raise TransportPayloadCodecError(
        f"Unsupported compression '{compression}'"
    )


def _encrypt_bytes(raw_bytes, encryption=DEFAULT_ENCRYPTION, encryption_key=""):
    encryption = str(encryption or "").strip().lower()

    if encryption in ("", "none"):
        return raw_bytes

    if encryption == "xor":
        return _xor_bytes(raw_bytes, encryption_key=encryption_key)

    raise TransportPayloadCodecError(
        f"Unsupported encryption '{encryption}'"
    )


def _decrypt_bytes(raw_bytes, encryption=DEFAULT_ENCRYPTION, encryption_key=""):
    encryption = str(encryption or "").strip().lower()

    if encryption in ("", "none"):
        return raw_bytes

    if encryption == "xor":
        return _xor_bytes(raw_bytes, encryption_key=encryption_key)

    raise TransportPayloadCodecError(
        f"Unsupported encryption '{encryption}'"
    )


def _xor_bytes(raw_bytes, encryption_key=""):
    key_text = str(encryption_key or "").strip()
    if not key_text:
        raise TransportPayloadCodecError(
            "XOR encryption requires a non-empty encryption_key"
        )

    key_bytes = key_text.encode("utf-8")
    key_len = len(key_bytes)

    result = bytearray()
    for idx, value in enumerate(raw_bytes):
        result.append(value ^ key_bytes[idx % key_len])

    return bytes(result)