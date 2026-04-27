"""Run one HiveQL file and export a Hive result table to CSV."""

import argparse
import logging
import sys
from pathlib import Path

from pyhive.exc import DatabaseError, OperationalError, ProgrammingError

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import hive_utils  # pylint: disable=wrong-import-position
import settings  # pylint: disable=wrong-import-position

_OUTPUT_DIR = Path(__file__).resolve().parents[2] / "output"
LOGGER = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--hql", required=True, help="HQL path relative to sql/, e.g. eda/q3.hql"
    )
    parser.add_argument("--table", required=True, help="Hive result table to export")
    parser.add_argument("--output", required=True, help="CSV filename under output/")
    return parser.parse_args()


def main() -> int:
    """Run a HiveQL file and export the selected Hive table to CSV."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = parse_args()

    conn = None
    try:
        raw = hive_utils.read_hql(args.hql)
        conn = hive_utils.open_connection(database=settings.HIVE_DATABASE)
        hive_utils.execute_hql(conn, raw)
        hive_utils.fetch_to_csv(conn, args.table, _OUTPUT_DIR / args.output)

        LOGGER.info(
            "%s table populated in %s and exported to output/%s.",
            args.table,
            settings.HIVE_DATABASE,
            args.output,
        )
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
