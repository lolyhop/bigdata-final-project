import logging
import sys
from pathlib import Path

from pyhive.exc import DatabaseError, OperationalError, ProgrammingError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hive_utils
import settings

_OUTPUT_DIR: Path = Path(__file__).resolve().parents[2] / "output"

LOGGER = logging.getLogger(__name__)


def main() -> int:
    """CLI entrypoint; configuration comes from ``settings``.

    Returns:
        Process exit code (0 on success, 1 on error).
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    conn = None
    try:
        raw = hive_utils.read_hql("eda/q4.hql")
        conn = hive_utils.open_connection(database=settings.HIVE_DATABASE)
        hive_utils.execute_hql(conn, raw)
        hive_utils.fetch_to_csv(conn, "q4_results", _OUTPUT_DIR / "q4.csv")
        LOGGER.info("q4_results table populated in %s.", settings.HIVE_DATABASE)
    except settings.SettingsError as exc:
        LOGGER.error("%s", exc)
        return 1
    except (OperationalError, ProgrammingError, DatabaseError) as exc:
        LOGGER.error("Hive error: %s", exc)
        return 1
    except OSError as exc:
        LOGGER.error("Connection error: %s", exc)
        return 1
    finally:
        if conn is not None:
            conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
