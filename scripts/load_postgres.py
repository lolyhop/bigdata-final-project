import logging
import sys
from pathlib import Path

import psycopg2

import pg_utils
import settings

LOGGER = logging.getLogger(__name__)


def load(csv_path: Path, table_name: str) -> None:
    """Recreate the PostgreSQL table and bulk-load data from *csv_path*.

    Args:
        csv_path: Preprocessed CSV file produced by ``filter_dataset_for_pg.py``.
        table_name: Destination table name (must be a valid SQL identifier).

    Raises:
        FileNotFoundError: If *csv_path* does not exist.
        psycopg2.Error: On any database error.
    """
    if not csv_path.is_file():
        raise FileNotFoundError("Input CSV not found: {}".format(csv_path))

    create_table_sql = pg_utils.read_sql("create_table.sql", table_name=table_name)
    import_data_sql = pg_utils.read_sql("import_data.sql", table_name=table_name)

    conn = pg_utils.open_connection()
    try:
        with conn.cursor() as cur:
            LOGGER.info("Creating table %s ...", table_name)
            cur.execute(create_table_sql)
        conn.commit()

        import_statements = [s.strip() for s in import_data_sql.split(";") if s.strip()]
        copy_stmt = import_statements[0]
        post_import_stmts = import_statements[1:]

        with conn.cursor() as cur:
            LOGGER.info("Loading %s into %s ...", csv_path, table_name)
            with csv_path.open("r", encoding="utf-8") as fh:
                cur.copy_expert(copy_stmt, fh)
            for stmt in post_import_stmts:
                LOGGER.info("Executing: %s", stmt.split("\n", 1)[0][:120])
                cur.execute(stmt)
        conn.commit()
    finally:
        conn.close()


def main() -> int:
    """CLI entrypoint; configuration comes from the ``settings`` module.

    Returns:
        Process exit code (0 on success, 1 on error).
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        load(settings.DATASET_FILTERED_PATH, settings.TABLE_NAME)
    except FileNotFoundError as exc:
        LOGGER.error("%s", exc)
        return 1
    except (OSError, psycopg2.Error) as exc:
        LOGGER.error("Database or I/O error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
