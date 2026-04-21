"""Centralized configuration for pipeline scripts.

Resolution precedence (highest wins):
    CLI flag  >  environment variable  >  .env file  >  dataclass default

Secrets have NO defaults — missing required secrets raise ``ConfigError`` at
startup with a clear message naming the env var that needs to be set.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field, fields
from pathlib import Path
from typing import Any

try:
    from dotenv import load_dotenv as _load_dotenv
except ImportError:  # python-dotenv is optional
    def _load_dotenv(*_args: Any, **_kwargs: Any) -> bool:
        return False


class ConfigError(RuntimeError):
    """Raised when a required setting is missing."""


_LOADED_DOTENV = False


def _ensure_dotenv_loaded() -> None:
    global _LOADED_DOTENV
    if _LOADED_DOTENV:
        return
    _LOADED_DOTENV = True
    for candidate in (Path.cwd() / ".env", Path(__file__).resolve().parents[2] / ".env"):
        if candidate.is_file():
            _load_dotenv(candidate, override=False)
            return


def _env(name: str, default: Any = None, *, cast: type | None = None) -> Any:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    if cast is int:
        return int(raw)
    if cast is float:
        return float(raw)
    if cast is bool:
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return raw


@dataclass
class Settings:
    # --- Neo4j ------------------------------------------------------------
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str | None = None  # SECRET — no default
    neo4j_database: str | None = None

    # --- PostgreSQL -------------------------------------------------------
    pg_host: str | None = None
    pg_port: int = 5432
    pg_db: str | None = None
    pg_user: str | None = None
    pg_password: str | None = None  # SECRET — no default

    # --- Crypto -----------------------------------------------------------
    fernet_key: str | None = None  # SECRET — required only by scripts that decrypt credentials

    # --- Pipeline runtime -------------------------------------------------
    batch_size: int = 1000
    workers: int = 8
    emit_json_dir: Path | None = None
    read_json_dir: Path | None = None
    log_level: str = "INFO"

    # Bag of unknown CLI args so scripts can extend without subclassing.
    extra: dict[str, Any] = field(default_factory=dict)

    # ---------------------------------------------------------------------

    @classmethod
    def from_env(cls) -> Settings:
        _ensure_dotenv_loaded()
        return cls(
            neo4j_uri=_env("NEO4J_URI", cls.neo4j_uri),
            neo4j_user=_env("NEO4J_USER", cls.neo4j_user),
            neo4j_password=_env("NEO4J_PASSWORD"),
            neo4j_database=_env("NEO4J_DATABASE"),
            pg_host=_env("PG_HOST") or _env("POSTGRES_HOST"),
            pg_port=_env("PG_PORT", cls.pg_port, cast=int) or _env("POSTGRES_PORT", cls.pg_port, cast=int),
            pg_db=_env("PG_DATABASE") or _env("POSTGRES_DB"),
            pg_user=_env("PG_USER") or _env("POSTGRES_USER"),
            pg_password=_env("PG_PASSWORD") or _env("POSTGRES_PASSWORD"),
            fernet_key=_env("FERNET_KEY"),
            batch_size=_env("PIPELINE_BATCH_SIZE", cls.batch_size, cast=int),
            workers=_env("PIPELINE_WORKERS", cls.workers, cast=int),
            log_level=_env("LOG_LEVEL", cls.log_level),
        )

    def overlay(self, **overrides: Any) -> Settings:
        """Return a copy with non-None overrides applied."""
        data = {f.name: getattr(self, f.name) for f in fields(self)}
        for key, value in overrides.items():
            if value is None:
                continue
            if key not in data:
                data["extra"] = {**data.get("extra", {}), key: value}
                continue
            data[key] = value
        return Settings(**data)

    # ---------------------------------------------------------------------
    # Validation helpers — called by each script for the fields it actually
    # needs, so unrelated scripts don't have to supply every secret.
    # ---------------------------------------------------------------------

    def require_neo4j(self) -> None:
        if not self.neo4j_password:
            raise ConfigError(
                "NEO4J_PASSWORD is not set. Provide it via env var, .env, "
                "or --neo4j-password."
            )

    def require_postgres(self) -> None:
        missing = [
            name for name in ("pg_host", "pg_db", "pg_user", "pg_password")
            if not getattr(self, name)
        ]
        if missing:
            raise ConfigError(
                "Missing PostgreSQL settings: "
                + ", ".join(m.upper() for m in missing)
                + ". Provide them via PG_* env vars or CLI flags."
            )

    def require_fernet(self) -> None:
        if not self.fernet_key:
            raise ConfigError("FERNET_KEY is not set — required to decrypt stored credentials.")
