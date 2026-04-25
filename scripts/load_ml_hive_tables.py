"""Register the six external ML-result Hive tables produced by Stage III."""

import logging
import os
import sys
from typing import Tuple

from pyhive.exc import DatabaseError, OperationalError, ProgrammingError

import hive_utils
import settings

LOGGER = logging.getLogger(__name__)

_ML_ENV_VARS: Tuple[str, ...] = (
    "ML_SPLIT_SUMMARY_PATH",
    "ML_FEATURE_INFO_PATH",
    "ML_LABEL_DISTRIBUTION_PATH",
    "ML_BASELINE_EVALUATION_PATH",
    "ML_EVALUATION_PATH",
    "ML_PREDICTION_DISTRIBUTION_PATH",
)


def _require_env(name: str) -> str:
    """Return the value of an environment variable or raise SettingsError.

    Args:
        name: Environment variable name.

    Returns:
        Non-empty string value of the variable.

    Raises:
        settings.SettingsError: If the variable is unset or empty.
    """
    value = os.environ.get(name, "").strip()
    if not value:
        raise settings.SettingsError(
            f"{name} must be set to a non-empty HDFS path."
        )
    return value


def create_ml_hive_tables() -> None:
    """Create (or replace) the six external ML-result Hive tables.

    Reads six HDFS directory paths from environment variables and registers
    external Hive tables that point to the CSV files written by Stage III.

    Raises:
        settings.SettingsError: If any required env var is missing.
        OperationalError: On HiveServer2 execution errors.
        ProgrammingError: On HQL syntax errors.
        DatabaseError: On other DB-API errors.
        OSError: On connection failures.
    """
    hive_database = settings.HIVE_DATABASE

    split_summary_path = _require_env("ML_SPLIT_SUMMARY_PATH")
    feature_info_path = _require_env("ML_FEATURE_INFO_PATH")
    label_distribution_path = _require_env("ML_LABEL_DISTRIBUTION_PATH")
    baseline_evaluation_path = _require_env("ML_BASELINE_EVALUATION_PATH")
    evaluation_path = _require_env("ML_EVALUATION_PATH")
    prediction_distribution_path = _require_env("ML_PREDICTION_DISTRIBUTION_PATH")

    LOGGER.info("Registering ML result tables in Hive database '%s'.", hive_database)

    hql = hive_utils.read_hql(
        "ml_results.hql",
        hive_database=hive_database,
        split_summary_path=split_summary_path,
        feature_info_path=feature_info_path,
        label_distribution_path=label_distribution_path,
        baseline_evaluation_path=baseline_evaluation_path,
        evaluation_path=evaluation_path,
        prediction_distribution_path=prediction_distribution_path,
    )

    conn = hive_utils.open_connection(database=hive_database)
    try:
        hive_utils.execute_hql(conn, hql)
    finally:
        conn.close()

    LOGGER.info("All six ML result tables registered successfully.")


def main() -> int:
    """CLI entrypoint; configuration comes from environment and the ``settings`` module.

    Returns:
        Process exit code (0 on success, 1 on error).
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        create_ml_hive_tables()
    except settings.SettingsError as exc:
        LOGGER.error("%s", exc)
        return 1
    except (OperationalError, ProgrammingError, DatabaseError) as exc:
        LOGGER.error("Hive error: %s", exc)
        return 1
    except OSError as exc:
        LOGGER.error("Connection error: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
