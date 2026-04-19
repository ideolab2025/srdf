#! /usr/bin/python3.1
# -*- coding: utf-8 -*-

import pymysql

from srdf.models import BootstrapPlan, ReplicationService


class AdminDBCatalogError(Exception):
    pass


def get_replication_service_source_info(replication_service):
    if not replication_service:
        raise AdminDBCatalogError("ReplicationService is required")

    config = replication_service.config or {}
    if not isinstance(config, dict):
        raise AdminDBCatalogError("Invalid ReplicationService config")

    service_type = (replication_service.service_type or "").strip().lower()

    if service_type in ("mysql", "mariadb"):
        host = config.get("host")
        port = int(config.get("port", 3306))
        user = config.get("user")
        password = config.get("password", "")

        if not host or not user:
            raise AdminDBCatalogError("Missing source DB config: host/user")

        return {
            "engine": service_type,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
        }

    if service_type == "postgresql":
        host = config.get("host")
        port = int(config.get("port", 5432))
        user = config.get("user")
        password = config.get("password", "")
        database = config.get("database") or "postgres"

        if not host or not user:
            raise AdminDBCatalogError("Missing source DB config: host/user")

        return {
            "engine": service_type,
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
        }

    raise AdminDBCatalogError(f"Unsupported service_type '{service_type}'")


def resolve_replication_service_from_plan_id(bootstrap_plan_id):
    plan = (
        BootstrapPlan.objects
        .select_related("replication_service")
        .filter(id=bootstrap_plan_id)
        .first()
    )
    if not plan or not plan.replication_service_id:
        raise AdminDBCatalogError(f"BootstrapPlan #{bootstrap_plan_id} not found")
    return plan.replication_service


def list_source_databases(replication_service):
    source = get_replication_service_source_info(replication_service)
    engine = source["engine"]

    if engine in ("mysql", "mariadb"):
        conn = pymysql.connect(
            host=source["host"],
            port=source["port"],
            user=source["user"],
            password=source["password"],
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW DATABASES")
                rows = cursor.fetchall() or []
                values = [list(row.values())[0] for row in rows]
            return sorted(values)
        finally:
            conn.close()

    if engine == "postgresql":
        try:
            import psycopg2
        except ImportError:
            raise AdminDBCatalogError("psycopg2 is required for PostgreSQL admin autocomplete")

        conn = psycopg2.connect(
            host=source["host"],
            port=source["port"],
            user=source["user"],
            password=source["password"],
            dbname=source["database"],
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT datname
                    FROM pg_database
                    WHERE datistemplate = false
                    ORDER BY datname
                    """
                )
                rows = cursor.fetchall() or []
                return [row[0] for row in rows]
        finally:
            conn.close()

    raise AdminDBCatalogError(f"Unsupported engine '{engine}'")


def list_source_tables(replication_service, database_name):
    database_name = (database_name or "").strip()
    if not database_name:
        return []

    source = get_replication_service_source_info(replication_service)
    engine = source["engine"]

    if engine in ("mysql", "mariadb"):
        conn = pymysql.connect(
            host=source["host"],
            port=source["port"],
            user=source["user"],
            password=source["password"],
            database=database_name,
            autocommit=True,
            cursorclass=pymysql.cursors.DictCursor,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute("SHOW TABLES")
                rows = cursor.fetchall() or []
                values = [list(row.values())[0] for row in rows]
            return sorted(values)
        finally:
            conn.close()

    if engine == "postgresql":
        try:
            import psycopg2
        except ImportError:
            raise AdminDBCatalogError("psycopg2 is required for PostgreSQL admin autocomplete")

        conn = psycopg2.connect(
            host=source["host"],
            port=source["port"],
            user=source["user"],
            password=source["password"],
            dbname=database_name,
        )
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT tablename
                    FROM pg_catalog.pg_tables
                    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
                    ORDER BY tablename
                    """
                )
                rows = cursor.fetchall() or []
                return [row[0] for row in rows]
        finally:
            conn.close()

    raise AdminDBCatalogError(f"Unsupported engine '{engine}'")


def search_values(values, q="", limit=100):
    q = (q or "").strip().lower()
    values = values or []

    if q:
        values = [v for v in values if q in str(v).lower()]

    return values[:limit]