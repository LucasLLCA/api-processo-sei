from __future__ import annotations

import pytest

from pipeline.config import ConfigError, Settings


def test_overlay_ignores_none_values() -> None:
    s = Settings(neo4j_uri="bolt://a", neo4j_password="pw")
    s2 = s.overlay(neo4j_uri=None, neo4j_user="other")
    assert s2.neo4j_uri == "bolt://a"
    assert s2.neo4j_user == "other"
    assert s2.neo4j_password == "pw"


def test_overlay_puts_unknown_keys_in_extra() -> None:
    s = Settings().overlay(custom_flag="yes")
    assert s.extra.get("custom_flag") == "yes"


def test_require_neo4j_without_password_raises() -> None:
    s = Settings()
    with pytest.raises(ConfigError, match="NEO4J_PASSWORD"):
        s.require_neo4j()


def test_require_neo4j_with_password_passes() -> None:
    Settings(neo4j_password="pw").require_neo4j()


def test_require_postgres_missing_fields_raises() -> None:
    s = Settings(pg_host="h", pg_user="u", pg_password="p")  # missing pg_db
    with pytest.raises(ConfigError, match="PG_DB"):
        s.require_postgres()


def test_require_postgres_all_fields_passes() -> None:
    Settings(
        pg_host="h",
        pg_db="d",
        pg_user="u",
        pg_password="p",
    ).require_postgres()


def test_require_fernet_missing_raises() -> None:
    with pytest.raises(ConfigError, match="FERNET_KEY"):
        Settings().require_fernet()
