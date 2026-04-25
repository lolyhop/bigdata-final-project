"""Download a public Yandex Disk share to a local file via the public API."""

import json
import logging
import sys
import urllib.parse as ul
import urllib.request
from pathlib import Path
from urllib.error import HTTPError, URLError

from typing_extensions import Final

LOGGER = logging.getLogger(__name__)

_API_BASE: Final[str] = (
    "https://cloud-api.yandex.net/v1/disk/public/resources/download?public_key="
)


def _fetch_download_href(public_share_url: str) -> str:
    """Resolve the direct download URL for a public Yandex Disk resource.

    Args:
        public_share_url: Browser share link (``https://disk.yandex...``).

    Returns:
        Temporary download ``href`` from the API JSON.

    Raises:
        ValueError: If the response is not valid JSON or has no ``href``.
        HTTPError: On HTTP failure from the Yandex API.
        URLError: On network errors.
    """
    api_url = f"{_API_BASE}{ul.quote_plus(public_share_url.strip())}"
    request = urllib.request.Request(
        api_url, headers={"User-Agent": "bigdata-final-project/1.0"}
    )
    with urllib.request.urlopen(request, timeout=120) as response:
        raw = response.read().decode("utf-8", errors="replace")
    try:
        payload = json.loads(raw)  # type: Dict[str, Any]
    except json.JSONDecodeError as exc:
        msg = "Yandex API returned non-JSON body."
        raise ValueError(msg) from exc
    href = payload.get("href")
    if not isinstance(href, str) or not href.strip():
        msg = "Yandex API JSON missing non-empty 'href'."
        raise ValueError(msg)
    return href.strip()


def download_public_file(public_share_url: str, output_path: Path) -> None:
    """Download the shared file to ``output_path`` (parent directories created).

    Args:
        public_share_url: Public share URL from Yandex Disk.
        output_path: Destination file path.

    Raises:
        ValueError: If ``public_share_url`` is empty.
        OSError: If the file cannot be written.
        HTTPError: On HTTP failure when downloading.
        URLError: On network errors.
    """
    if not public_share_url or not str(public_share_url).strip():
        msg = "public_share_url must be non-empty."
        raise ValueError(msg)
    href = _fetch_download_href(public_share_url)
    output_path = output_path.expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    request = urllib.request.Request(
        href, headers={"User-Agent": "bigdata-final-project/1.0"}
    )
    with urllib.request.urlopen(request, timeout=600) as response:
        data = response.read()
    output_path.write_bytes(data)
    LOGGER.info("Downloaded to %s (%s bytes)", output_path, len(data))


def main() -> int:
    """CLI: ``<share_url> <output_file_path>``.

    Returns:
        Process exit code (0 on success).
    """
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    if len(sys.argv) != 3:
        LOGGER.error(
            "Usage: python3 scripts/download_from_yandex_disk.py "
            "<DATASET_YANDEX_DISK_SHARE_URL> <output_zip_or_file_path>"
        )
        return 2
    public_url = sys.argv[1]
    dest = Path(sys.argv[2])
    try:
        download_public_file(public_url, dest)
    except (ValueError, OSError, HTTPError, URLError) as exc:
        LOGGER.error("%s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
