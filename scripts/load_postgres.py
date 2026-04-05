"""Create the loans table and bulk-load a preprocessed CSV with asyncpg.

Public API:
    main: CLI entrypoint (async pipeline via ``asyncio.run``). Configuration from ``settings``.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import Final

import asyncpg

from loan_schema import LOAN_CSV_COLUMNS, LOAN_TABLE_DDL_TEMPLATE
from settings import settings

LOGGER = logging.getLogger(__name__)

COPY_COLUMNS: Final[list[str]] = list(LOAN_CSV_COLUMNS)


def _connect_kwargs() -> dict[str, str | int]:
    """Build keyword arguments for ``asyncpg.connect`` from ``settings``.

    Returns:
        Keyword arguments for ``asyncpg.connect`` (host, port, user, password, database).
    """
    return {
        "host": settings.pg_host,
        "port": settings.pg_port,
        "user": settings.pg_user,
        "password": settings.pg_password,
        "database": settings.pg_database,
    }


async def _load_csv(conn: asyncpg.Connection, table_name: str, csv_path: Path) -> None:
    """Run ``COPY``-style ingest using asyncpg.

    Args:
        conn: Open asyncpg connection.
        table_name: Destination table name.
        csv_path: UTF-8 CSV with header matching ``COPY_COLUMNS``.

    Raises:
        OSError: If the file cannot be read.
        asyncpg.PostgresError: On server-side errors.
    """
    with csv_path.open("rb") as handle:
        await conn.copy_to_table(
            table_name,
            source=handle,
            format="csv",
            header=True,
            null="",
            columns=COPY_COLUMNS,
        )


async def run_load() -> None:
    """Connect, recreate the table, load data, and analyze using ``settings``.

    Raises:
        FileNotFoundError: If the CSV path is not a file.
        OSError: If the CSV cannot be read.
        asyncpg.PostgresError: On database errors.
    """
    csv_path = settings.dataset_filtered_path
    table_name = settings.table_name
    connect_kwargs = _connect_kwargs()
    if not csv_path.is_file():
        msg = f"Input CSV not found: {csv_path}"
        raise FileNotFoundError(msg)

    ddl = LOAN_TABLE_DDL_TEMPLATE.format(table_name=table_name).strip()

    conn = await asyncpg.connect(**connect_kwargs)
    try:
        await conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        await conn.execute(ddl)
        LOGGER.info("Loading %s into %s ...", csv_path, table_name)
        await _load_csv(conn, table_name, csv_path)
        await conn.execute(f"ANALYZE {table_name}")
        LOGGER.info("ANALYZE %s complete.", table_name)
    finally:
        await conn.close()


def main() -> int:
    """CLI entrypoint; uses ``settings`` (repo-root ``.env`` and environment).

    Returns:
        Process exit code (0 on success).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    try:
        asyncio.run(run_load())
    except FileNotFoundError as exc:
        LOGGER.error("%s", exc)
        return 1
    except (OSError, asyncpg.PostgresError) as exc:
        LOGGER.error("Database or I/O error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
