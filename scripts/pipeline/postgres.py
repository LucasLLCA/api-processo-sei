"""PostgreSQL connection helpers.

Wraps psycopg2.connect with settings-driven credentials so no script has to
hardcode hosts, ports, or passwords.
"""

from __future__ import annotations

from typing import Any

import psycopg2

from .config import Settings


def make_pg_conn(settings: Settings, *, autocommit: bool = False) -> Any:
    """Open a new PostgreSQL connection using the pipeline settings."""
    settings.require_postgres()
    conn = psycopg2.connect(
        host=settings.pg_host,
        port=settings.pg_port,
        user=settings.pg_user,
        password=settings.pg_password,
        database=settings.pg_db,
    )
    conn.autocommit = autocommit
    return conn
