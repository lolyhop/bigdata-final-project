from __future__ import annotations

import re
from pathlib import Path
from typing import Final

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
_ENV_FILE: Final[Path] = _REPO_ROOT / ".env"

_IDENTIFIER_RE: Final[re.Pattern[str]] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class Settings(BaseSettings):
    """Strongly typed configuration for dataset paths, table name, and PostgreSQL."""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        extra="ignore",
    )

    dataset_input_path: Path
    dataset_filtered_path: Path
    table_name: str
    pg_host: str = Field(default="localhost", validation_alias="PGHOST")
    pg_port: int = Field(default=5432, validation_alias="PGPORT")
    pg_user: str = Field(validation_alias="PGUSER")
    pg_password: str = Field(validation_alias="PGPASSWORD")
    pg_database: str = Field(validation_alias="PGDATABASE")

    @field_validator("table_name", mode="before")
    @classmethod
    def _normalize_table_name(cls, value: object) -> str:
        """Strip ``table_name`` and reject empty values."""
        if not isinstance(value, str):
            msg = "TABLE_NAME must be a string."
            raise TypeError(msg)
        stripped = value.strip()
        if not stripped:
            msg = "TABLE_NAME must be set to a non-empty value."
            raise ValueError(msg)
        return stripped

    @field_validator("table_name")
    @classmethod
    def _validate_table_name(cls, value: str) -> str:
        """Ensure ``TABLE_NAME`` is a safe SQL identifier fragment."""
        if not _IDENTIFIER_RE.match(value):
            msg = f"TABLE_NAME must match [a-zA-Z_][a-zA-Z0-9_]*, got {value!r}."
            raise ValueError(msg)
        return value

    @field_validator("pg_user", "pg_database", mode="before")
    @classmethod
    def _require_non_empty_str(cls, value: object) -> str:
        """Reject blank ``PGUSER`` / ``PGDATABASE`` after strip."""
        if not isinstance(value, str):
            msg = "Value must be a string."
            raise TypeError(msg)
        stripped = value.strip()
        if not stripped:
            msg = "Value must be set to a non-empty string."
            raise ValueError(msg)
        return stripped


settings: Settings = Settings()
