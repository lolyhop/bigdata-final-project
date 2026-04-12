import logging
import sys
from pathlib import Path
from typing import Any, List

from pyhive import hive
from pyhive.exc import (
    DatabaseError,
    OperationalError,
    ProgrammingError,
)

import settings

_REPO_ROOT: Path = Path(__file__).resolve().parents[1]
_SQL_DIR: Path = _REPO_ROOT / "sql"

LOGGER = logging.getLogger(__name__)


def _read_hql(filename: str, **kwargs: str) -> str:
    """Read a HiveQL file from the sql/ directory and apply placeholder substitution.

    Args:
        filename: File name relative to the sql/ directory (e.g. ``'db.hql'``).
        **kwargs: Values forwarded to ``str.format`` on the file content.

    Returns:
        Formatted HiveQL string ready for execution.

    Raises:
        FileNotFoundError: If the HQL file does not exist.
    """
    path = _SQL_DIR / filename
    content = path.read_text(encoding="utf-8")
    return content.format(**kwargs) if kwargs else content


def _open_connection(*, database: str) -> Any:
    """Open a PyHive connection using module ``settings``.

    Args:
        database: Hive database name for the session.

    Returns:
        Open ``hive.Connection``.

    Raises:
        OSError: If the network connection fails.
    """
    auth = settings.HIVE_AUTH
    connect_kwargs = {
        "host": settings.HIVE_HOST,
        "port": settings.HIVE_PORT,
        "database": database,
        "username": settings.HIVE_USER,
        "auth": auth,
    }
    # PyHive: password is allowed only for LDAP or CUSTOM (see Connection.__init__).
    if auth is not None and str(auth).strip().upper() in ("LDAP", "CUSTOM"):
        connect_kwargs["password"] = settings.HIVE_PASSWORD
    return hive.connect(**connect_kwargs)


def _execute_statements(conn: Any, statements: List[str]) -> None:
    """Run DDL statements sequentially on one connection.

    Args:
        conn: Open Hive connection.
        statements: Non-empty SQL strings.

    Raises:
        OperationalError: On HiveServer2 execution errors.
        ProgrammingError: On SQL errors.
        DatabaseError: On other DB-API errors.
    """
    cursor = conn.cursor()
    try:
        for sql in statements:
            stripped = sql.strip()
            if not stripped:
                continue
            LOGGER.info("Executing: %s", stripped.split("\n", 1)[0][:120])
            cursor.execute(stripped)
    finally:
        cursor.close()


def run_hive_ddl() -> None:
    """Register Sqoop Parquet via a staging external table, then materialize the final table.

    Expects ``HDFS_DATA_DIR`` to already contain Parquet from Sqoop (run stage 1 first).
    Drops any existing final (managed) and staging tables, creates staging, runs CTAS into
    ``TABLE_NAME``, then drops staging so only the curated table remains.

    Raises:
        OperationalError: If HiveServer2 rejects a statement.
        ProgrammingError: If a statement is invalid.
        DatabaseError: On other Hive errors.
        OSError: On connection failures.
    """
    hdfs_data_dir = settings.HDFS_DATA_DIR
    hive_db = settings.HIVE_DATABASE
    final_table = settings.TABLE_NAME
    staging_table_1 = f"{settings.TABLE_NAME}_staging_1"
    staging_table_2 = f"{settings.TABLE_NAME}_staging_2"

    hive_warehouse_dir = settings.HIVE_WAREHOUSE_DIR

    LOGGER.info(
        "Hive DDL: database=%s final_table=%s staging_table_1=%s staging_table_2=%s sqoop_location=%s warehouse=%s",
        hive_db,
        final_table,
        staging_table_1,
        staging_table_2,
        hdfs_data_dir,
        hive_warehouse_dir,
    )

    conn_default = _open_connection(database="default")
    try:
        _execute_statements(
            conn_default,
            ["CREATE DATABASE IF NOT EXISTS {hive_db}".format(hive_db=hive_db)],
        )
    finally:
        conn_default.close()

    conn_db = _open_connection(database=hive_db)
    try:
        import_hql = _read_hql(
            "import_data.hql",
            staging_table_1=staging_table_1,
            staging_table_2=staging_table_2,
            final_table=final_table,
            hdfs_data_dir=hdfs_data_dir,
            hive_warehouse_dir=hive_warehouse_dir,
        )
        import_statements = [s.strip() for s in import_hql.split(";") if s.strip()]
        _execute_statements(conn_db, import_statements)
    finally:
        conn_db.close()

    LOGGER.info(
        "Curated table %s ready; staging tables dropped (Sqoop files remain under %s).",
        final_table,
        hdfs_data_dir,
    )


def main() -> int:
    """CLI entrypoint; configuration comes from ``settings``.

    Returns:
        Process exit code (0 on success).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    try:
        run_hive_ddl()
    except settings.SettingsError as exc:
        LOGGER.error("%s", exc)
        return 1
    except (OperationalError, ProgrammingError, DatabaseError) as exc:
        LOGGER.error("Hive error: %s", exc)
        return 1
    except OSError as exc:
        LOGGER.error("Connection error: %s", exc)
        return 1
    LOGGER.info("Hive curated Parquet table registered.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
