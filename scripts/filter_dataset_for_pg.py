"""Stream raw loan CSV and emit a slim UTF-8 CSV for PostgreSQL bulk load."""

from __future__ import annotations

import csv
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Final

from loan_schema import LOAN_CSV_COLUMNS
from settings import settings

LOGGER = logging.getLogger(__name__)

REQUIRED_HEADERS: Final[frozenset[str]] = frozenset(LOAN_CSV_COLUMNS)

OUTPUT_FIELDNAMES: Final[list[str]] = list(LOAN_CSV_COLUMNS)

DATE_FMT_IN: Final[str] = "%b-%Y"


def _strip_percent(value: str) -> str:
    """Remove percent signs and surrounding whitespace from a numeric string.

    Args:
        value: Raw field from the CSV.

    Returns:
        Cleaned string, or empty if input is blank.
    """
    if not value or not str(value).strip():
        return ""
    return str(value).strip().replace("%", "")


def _parse_term_months(value: str) -> str:
    """Extract loan term in whole months from strings like ``' 36 months'``.

    Args:
        value: Raw ``term`` column.

    Returns:
        Decimal digits only, or empty if parsing fails.
    """
    if not value or not str(value).strip():
        return ""
    digits = re.sub(r"\D", "", str(value))
    return digits if digits else ""


def _parse_mon_year_to_iso(value: str) -> str:
    """Convert ``Mon-YYYY`` to ISO date ``YYYY-MM-01`` (first of month).

    Args:
        value: Raw date string from the dataset.

    Returns:
        ISO date string, or empty if missing or invalid.
    """
    if not value or not str(value).strip():
        return ""
    try:
        parsed = datetime.strptime(str(value).strip(), DATE_FMT_IN)
    except ValueError:
        return ""
    return parsed.strftime("%Y-%m-%d")


def _to_int_string(value: str) -> str:
    """Normalize a numeric field to an integer string for INTEGER columns.

    Args:
        value: Raw field (may be ``'3.0'``).

    Returns:
        Integer as decimal string, or empty if missing or invalid.
    """
    if value is None or str(value).strip() == "":
        return ""
    try:
        return str(int(float(str(value).strip())))
    except ValueError:
        return ""


def _to_numeric_string(value: str) -> str:
    """Normalize a field for NUMERIC columns (strip, no percent).

    Args:
        value: Raw field.

    Returns:
        String suitable for PostgreSQL NUMERIC, or empty.
    """
    if value is None or str(value).strip() == "":
        return ""
    return _strip_percent(str(value))


def _row_to_output(row: dict[str, str]) -> dict[str, str]:
    """Map one input row dict to output column dict.

    Args:
        row: Single row from ``DictReader`` (all values as strings).

    Returns:
        Dict aligned with ``OUTPUT_FIELDNAMES``.
    """
    return {
        "id": _to_int_string(row.get("id", "")),
        "loan_amnt": _to_numeric_string(row.get("loan_amnt", "")),
        "term": _parse_term_months(row.get("term", "")),
        "int_rate": _to_numeric_string(row.get("int_rate", "")),
        "installment": _to_numeric_string(row.get("installment", "")),
        "grade": (row.get("grade") or "").strip(),
        "sub_grade": (row.get("sub_grade") or "").strip(),
        "emp_length": (row.get("emp_length") or "").strip(),
        "home_ownership": (row.get("home_ownership") or "").strip(),
        "annual_inc": _to_numeric_string(row.get("annual_inc", "")),
        "verification_status": (row.get("verification_status") or "").strip(),
        "purpose": (row.get("purpose") or "").strip(),
        "dti": _to_numeric_string(row.get("dti", "")),
        "delinq_2yrs": _to_int_string(row.get("delinq_2yrs", "")),
        "inq_last_6mths": _to_int_string(row.get("inq_last_6mths", "")),
        "open_acc": _to_int_string(row.get("open_acc", "")),
        "pub_rec": _to_int_string(row.get("pub_rec", "")),
        "revol_bal": _to_int_string(row.get("revol_bal", "")),
        "revol_util": _to_numeric_string(row.get("revol_util", "")),
        "total_acc": _to_int_string(row.get("total_acc", "")),
        "application_type": (row.get("application_type") or "").strip(),
        "fico_range_low": _to_int_string(row.get("fico_range_low", "")),
        "fico_range_high": _to_int_string(row.get("fico_range_high", "")),
        "issue_d": _parse_mon_year_to_iso(row.get("issue_d", "")),
        "earliest_cr_line": _parse_mon_year_to_iso(row.get("earliest_cr_line", "")),
        "loan_status": (row.get("loan_status") or "").strip(),
    }


def _validate_headers(fieldnames: list[str] | None) -> None:
    """Ensure the source CSV contains every column we need.

    Args:
        fieldnames: Header row from the CSV reader.

    Raises:
        ValueError: If required columns are missing.
    """
    if not fieldnames:
        msg = "CSV has no header row."
        raise ValueError(msg)
    present = {h.strip() for h in fieldnames if h is not None}
    missing = sorted(REQUIRED_HEADERS - present)
    if missing:
        msg = f"Missing required columns: {', '.join(missing)}"
        raise ValueError(msg)


def filter_csv(input_path: Path, output_path: Path) -> tuple[int, int, int]:
    """Read ``input_path`` in a streaming fashion and write the core CSV.

    Args:
        input_path: Raw Lending Club-style dataset.
        output_path: Destination path (parent dirs created if needed).

    Returns:
        Tuple of ``(rows_written, rows_skipped_invalid_id, rows_skipped_bad_dates)``.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows_written = 0
    skipped_id = 0
    skipped_dates = 0

    with input_path.open(encoding="utf-8", errors="replace", newline="") as in_f:
        reader = csv.DictReader(in_f)
        _validate_headers(reader.fieldnames)

        with output_path.open("w", encoding="utf-8", newline="") as out_f:
            writer = csv.DictWriter(
                out_f,
                fieldnames=OUTPUT_FIELDNAMES,
                extrasaction="ignore",
                lineterminator="\n",
            )
            writer.writeheader()

            for row in reader:
                out = _row_to_output(row)
                if not out["id"]:
                    skipped_id += 1
                    continue
                raw_issue = (row.get("issue_d") or "").strip()
                raw_earliest = (row.get("earliest_cr_line") or "").strip()
                if raw_issue and not out["issue_d"]:
                    skipped_dates += 1
                    continue
                if raw_earliest and not out["earliest_cr_line"]:
                    skipped_dates += 1
                    continue
                writer.writerow(out)
                rows_written += 1

    return rows_written, skipped_id, skipped_dates


def main() -> int:
    """CLI entrypoint; paths come from ``settings`` (repo-root ``.env`` and environment).

    Returns:
        Process exit code (0 on success).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(levelname)s %(message)s",
    )
    input_path = settings.dataset_input_path
    output_path = settings.dataset_filtered_path
    if not input_path.is_file():
        LOGGER.error("Input file not found: %s", input_path)
        return 1
    try:
        written, skip_id, skip_dates = filter_csv(input_path, output_path)
    except (OSError, ValueError) as exc:
        LOGGER.error("%s", exc)
        return 1
    LOGGER.info(
        "Wrote %s rows to %s (skipped_id=%s skipped_bad_dates=%s)",
        written,
        output_path,
        skip_id,
        skip_dates,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
