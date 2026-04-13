import logging
import sys

from pyhive.exc import DatabaseError, OperationalError, ProgrammingError

import hive_utils
import settings

LOGGER = logging.getLogger(__name__)


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

    conn_default = hive_utils.open_connection(database="default")
    try:
        hive_utils.execute_hql(
            conn_default,
            "CREATE DATABASE IF NOT EXISTS {hive_db}".format(hive_db=hive_db),
        )
    finally:
        conn_default.close()

    conn_db = hive_utils.open_connection(database=hive_db)
    try:
        import_hql = hive_utils.read_hql(
            "import_data.hql",
            staging_table_1=staging_table_1,
            staging_table_2=staging_table_2,
            final_table=final_table,
            hdfs_data_dir=hdfs_data_dir,
            hive_warehouse_dir=hive_warehouse_dir,
        )
        hive_utils.execute_hql(conn_db, import_hql)
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
