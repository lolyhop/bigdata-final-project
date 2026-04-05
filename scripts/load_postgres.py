import asyncio
import logging
import sys
from pathlib import Path
from typing import Any, Coroutine, Dict, List, Union

import asyncpg
from typing_extensions import Final

from loan_schema import LOAN_CSV_COLUMNS, LOAN_TABLE_DDL_TEMPLATE
import settings

LOGGER = logging.getLogger(__name__)

COPY_COLUMNS: Final[List[str]] = list(LOAN_CSV_COLUMNS)


def _connect_kwargs() -> Dict[str, Union[str, int]]:
    """Build keyword arguments for ``asyncpg.connect`` from module ``settings``.

    Returns:
        Keyword arguments for ``asyncpg.connect`` (host, port, user, password, database).
    """
    return {
        "host": settings.PGHOST,
        "port": settings.PGPORT,
        "user": settings.PGUSER,
        "password": settings.PGPASSWORD,
        "database": settings.PGDATABASE,
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
    """Connect, recreate the table, load data, and analyze using module ``settings``.

    Raises:
        FileNotFoundError: If the CSV path is not a file.
        OSError: If the CSV cannot be read.
        asyncpg.PostgresError: On database errors.
    """
    csv_path = settings.DATASET_FILTERED_PATH
    table_name = settings.TABLE_NAME
    connect_kwargs = _connect_kwargs()
    if not csv_path.is_file():
        msg = "Input CSV not found: {}".format(csv_path)
        raise FileNotFoundError(msg)

    ddl = LOAN_TABLE_DDL_TEMPLATE.format(table_name=table_name).strip()

    conn = await asyncpg.connect(**connect_kwargs)
    try:
        await conn.execute("DROP TABLE IF EXISTS {}".format(table_name))
        await conn.execute(ddl)
        LOGGER.info("Loading %s into %s ...", csv_path, table_name)
        await _load_csv(conn, table_name, csv_path)
        await conn.execute("ANALYZE {}".format(table_name))
        LOGGER.info("ANALYZE %s complete.", table_name)
    finally:
        await conn.close()


def _run_async(coro: Coroutine[Any, Any, None]) -> None:
    """Run ``coro`` to completion (Python 3.6-compatible; no ``asyncio.run``).

    Args:
        coro: Coroutine returned from an async function (e.g. ``run_load()``).
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(coro)
    finally:
        loop.close()


def main() -> int:
    """CLI entrypoint; uses module ``settings`` (``.env`` + environment).

    Returns:
        Process exit code (0 on success).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    try:
        _run_async(run_load())
    except FileNotFoundError as exc:
        LOGGER.error("%s", exc)
        return 1
    except (OSError, asyncpg.PostgresError) as exc:
        LOGGER.error("Database or I/O error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
