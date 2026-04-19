#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import pymysql


class MariaDBError(Exception):
    pass


def _connect(config):
    return pymysql.connect(
        host=config.get("host"),
        port=config.get("port", 3306),
        user=config.get("user"),
        password=config.get("password"),
        database=config.get("database"),
        autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
    )


def _execute_first_success(cursor, statements):
    last_error = None

    for statement in statements:
        try:
            cursor.execute(statement)
            return statement
        except Exception as exc:
            last_error = exc

    if last_error:
        raise MariaDBError(str(last_error))

    raise MariaDBError("No SQL statement provided")


def _fetch_replication_row(cursor):
    last_error = None

    for statement in ("SHOW REPLICA STATUS", "SHOW SLAVE STATUS"):
        try:
            cursor.execute(statement)
            row = cursor.fetchone()
            return row or None
        except Exception as exc:
            last_error = exc

    if last_error:
        raise MariaDBError(f"Unable to read replication status: {last_error}")

    return None


def _get_global_variable(cursor, variable_name):
    candidates = [
        f"SHOW GLOBAL VARIABLES LIKE '{variable_name}'",
        f"SHOW VARIABLES LIKE '{variable_name}'",
    ]

    for statement in candidates:
        try:
            cursor.execute(statement)
            row = cursor.fetchone()
            if not row:
                continue

            value = row.get("Value")
            if value is None:
                for _, item in row.items():
                    if item != variable_name:
                        value = item
                        break
            return value
        except Exception:
            continue

    return None


def _bool_from_db_value(value):
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    s = str(value).strip().lower()
    return s in ("1", "on", "yes", "true")


def get_replication_status(config):
    conn = _connect(config)
    try:
        with conn.cursor() as cursor:
            replication_row = _fetch_replication_row(cursor)
            read_only_value = _get_global_variable(cursor, "read_only")
            read_only = _bool_from_db_value(read_only_value)

            if not replication_row:
                return {
                    "role": "primary",
                    "running": True,
                    "lag_seconds": 0,
                    "master_host": "",
                    "io_running": None,
                    "sql_running": None,
                    "read_only": read_only,
                    "last_error": "",
                }

            io_running = replication_row.get("Slave_IO_Running")
            sql_running = replication_row.get("Slave_SQL_Running")

            return {
                "role": "replica",
                "running": io_running == "Yes" and sql_running == "Yes",
                "lag_seconds": replication_row.get("Seconds_Behind_Master") or 0,
                "master_host": replication_row.get("Master_Host") or "",
                "io_running": io_running,
                "sql_running": sql_running,
                "read_only": read_only,
                "last_error": replication_row.get("Last_Error") or "",
            }
    finally:
        conn.close()


def stop_replica(config):
    conn = _connect(config)
    try:
        with conn.cursor() as cursor:
            _execute_first_success(cursor, ["STOP REPLICA", "STOP SLAVE"])
    finally:
        conn.close()


def start_replica(config):
    conn = _connect(config)
    try:
        with conn.cursor() as cursor:
            _execute_first_success(cursor, ["START REPLICA", "START SLAVE"])
    finally:
        conn.close()


def set_read_only(config, value=True):
    conn = _connect(config)
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"SET GLOBAL read_only = {1 if value else 0}")
    finally:
        conn.close()


def promote(config):
    conn = _connect(config)
    try:
        with conn.cursor() as cursor:
            _execute_first_success(cursor, ["STOP REPLICA", "STOP SLAVE"])
            _execute_first_success(cursor, ["RESET REPLICA ALL", "RESET SLAVE ALL"])
            cursor.execute("SET GLOBAL read_only = 0")
    finally:
        conn.close()