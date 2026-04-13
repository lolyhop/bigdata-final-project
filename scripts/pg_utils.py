from pathlib import Path

import psycopg2

import settings

_REPO_ROOT: Path = Path(__file__).resolve().parent.parent
_SQL_DIR: Path = _REPO_ROOT / "sql"


def read_sql(rel_path: str, **kwargs: str) -> str:
    """Read a SQL file from the sql/ directory and apply placeholder substitution.

    Args:
        rel_path: File path relative to sql/ (e.g., ``'import_data.sql'``).
        **kwargs: Values forwarded to ``str.format`` on the file content.

    Returns:
        Formatted SQL string ready for execution.

    Raises:
        FileNotFoundError: If the SQL file does not exist.
    """
    path = _SQL_DIR / rel_path
    content = path.read_text(encoding="utf-8")
    return content.format(**kwargs) if kwargs else content


def open_connection() -> psycopg2.extensions.connection:
    """Open a PostgreSQL connection using ``settings``.

    Returns:
        Open psycopg2 connection.

    Raises:
        psycopg2.Error: If connection fails.
    """
    return psycopg2.connect(
        host=settings.PGHOST,
        port=settings.PGPORT,
        user=settings.PGUSER,
        password=settings.PGPASSWORD,
        dbname=settings.PGDATABASE,
    )
