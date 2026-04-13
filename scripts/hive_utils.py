import csv
import logging
from pathlib import Path
from typing import Any

from pyhive import hive

import settings

_REPO_ROOT: Path = Path(__file__).resolve().parent.parent
_SQL_DIR: Path = _REPO_ROOT / "sql"

LOGGER = logging.getLogger(__name__)


def read_hql(rel_path: str, **kwargs: str) -> str:
    """Read a HiveQL file from the sql/ directory and apply placeholder substitution.

    Args:
        rel_path: File path relative to sql/ (e.g., ``'import_data.hql'`` or
            ``'eda/q1.hql'``).
        **kwargs: Values forwarded to ``str.format`` on the file content.

    Returns:
        Formatted HiveQL string ready for execution.

    Raises:
        FileNotFoundError: If the HQL file does not exist.
    """
    path = _SQL_DIR / rel_path
    content = path.read_text(encoding="utf-8")
    return content.format(**kwargs) if kwargs else content


def open_connection(*, database: str) -> Any:
    """Open a PyHive connection using module ``settings``.

    Args:
        database: Hive database name for the session.

    Returns:
        Open ``hive.Connection``.

    Raises:
        OSError: If the network connection fails.
    """
    auth = settings.HIVE_AUTH
    connect_kwargs: dict[str, Any] = {
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


def execute_hql(conn: Any, raw_hql: str) -> None:
    """Execute HiveQL statements, handling comments and statement splitting.

    Splits the HQL on semicolons, strips comment lines (starting with ``--``)
    before splitting to avoid empty fragments, then executes each statement
    sequentially on a single cursor.

    Args:
        conn: Open Hive connection.
        raw_hql: Raw HiveQL string (may contain multiple statements separated
            by semicolons).

    Raises:
        pyhive.exc.OperationalError: On HiveServer2 execution errors.
        pyhive.exc.ProgrammingError: On SQL errors.
        pyhive.exc.DatabaseError: On other DB-API errors.
    """
    # Strip comment lines before splitting on ';' to avoid empty fragments
    # produced by semicolons that may appear inside inline comments.
    stripped_lines = [
        line for line in raw_hql.splitlines() if not line.strip().startswith("--")
    ]
    statements = [s.strip() for s in "\n".join(stripped_lines).split(";") if s.strip()]

    cursor = conn.cursor()
    try:
        for sql in statements:
            LOGGER.info("Executing: %s", sql.split("\n", 1)[0][:120])
            cursor.execute(sql)
    finally:
        cursor.close()


def fetch_to_csv(conn: Any, table: str, csv_path: Path) -> None:
    """Fetch all rows from a Hive table and write them to a CSV file.

    Args:
        conn: Open Hive connection.
        table: Fully-qualified or bare table name to ``SELECT * FROM``.
        csv_path: Destination file path; parent directories are created if needed.

    Raises:
        pyhive.exc.OperationalError: If the table does not exist or query fails.
        OSError: If the output file cannot be written.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table}")
        headers = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
    finally:
        cursor.close()

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(headers)
        writer.writerows(rows)
    LOGGER.info("Wrote %d rows to %s.", len(rows), csv_path)
