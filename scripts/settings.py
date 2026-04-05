import os
import re
from pathlib import Path
from typing import Pattern

from typing_extensions import Final

_REPO_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
_ENV_FILE: Final[Path] = _REPO_ROOT / ".env"

_IDENTIFIER_RE: Final[Pattern[str]] = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


class SettingsError(ValueError):
    """Raised when required configuration is missing or invalid."""


def _load_dotenv(path: Path) -> None:
    """Merge ``path`` into ``os.environ`` with ``setdefault`` (existing keys win)."""
    if not path.is_file():
        return
    with path.open(encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, rest = line.partition("=")
            key = key.strip()
            if not key:
                continue
            value = rest.strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


_load_dotenv(_ENV_FILE)

if "PGPASSWORD" not in os.environ:
    raise SettingsError("PGPASSWORD must be set (can be empty for local trust auth).")

_raw_in = os.environ.get("DATASET_INPUT_PATH")
if _raw_in is None or not str(_raw_in).strip():
    raise SettingsError("DATASET_INPUT_PATH must be set to a non-empty value.")
DATASET_INPUT_PATH = Path(str(_raw_in).strip())

_raw_out = os.environ.get("DATASET_FILTERED_PATH")
if _raw_out is None or not str(_raw_out).strip():
    raise SettingsError("DATASET_FILTERED_PATH must be set to a non-empty value.")
DATASET_FILTERED_PATH = Path(str(_raw_out).strip())

_raw_table = os.environ.get("TABLE_NAME")
if _raw_table is None or not isinstance(_raw_table, str):
    raise SettingsError("TABLE_NAME must be set.")
TABLE_NAME = _raw_table.strip()
if not TABLE_NAME:
    raise SettingsError("TABLE_NAME must be set to a non-empty value.")
if not _IDENTIFIER_RE.match(TABLE_NAME):
    raise SettingsError(
        "TABLE_NAME must match [a-zA-Z_][a-zA-Z0-9_]*, got {0!r}.".format(TABLE_NAME)
    )

_raw_host = os.environ.get("PGHOST")
if _raw_host is None or not str(_raw_host).strip():
    PGHOST = "localhost"
else:
    PGHOST = str(_raw_host).strip()

_raw_port = os.environ.get("PGPORT")
if _raw_port is None or not str(_raw_port).strip():
    PGPORT = 5432
else:
    try:
        PGPORT = int(str(_raw_port).strip())
    except ValueError:
        raise SettingsError("PGPORT must be an integer, got {0!r}.".format(_raw_port))

_raw_user = os.environ.get("PGUSER")
if _raw_user is None or not isinstance(_raw_user, str) or not _raw_user.strip():
    raise SettingsError("PGUSER must be set to a non-empty string.")
PGUSER = _raw_user.strip()

PGPASSWORD = os.environ["PGPASSWORD"]

_raw_db = os.environ.get("PGDATABASE")
if _raw_db is None or not isinstance(_raw_db, str) or not _raw_db.strip():
    raise SettingsError("PGDATABASE must be set to a non-empty string.")
PGDATABASE = _raw_db.strip()
