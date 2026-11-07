"""
Task 0 - Setup.

Idempotently downloads every raw dataset used by the later tasks from the
public ``cct-ds-code-challenge-input-data`` S3 bucket into ``data/raw/``.
Re-running this script is cheap: files that already exist locally (and
match the remote size) are skipped.

Usage
-----
    python -m src.task0_setup.download_data
"""
from __future__ import annotations

import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.aws_client import ensure_creds_downloaded  # noqa: E402
from src.common.config import RAW_FILES, S3_BASE_URL  # noqa: E402
from src.common.logging_utils import get_logger  # noqa: E402
from src.common.timing import timer  # noqa: E402

logger = get_logger("download_data")


def _download(url: str, destination: Path) -> None:
    """Stream-download ``url`` to ``destination``, skipping if already present
    and the same size as the remote object (cheap idempotency check)."""
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        remote_size = int(resp.headers.get("Content-Length", 0))

        if destination.exists() and destination.stat().st_size == remote_size and remote_size > 0:
            logger.info("SKIP  %s already present (%.2f MB)", destination.name, remote_size / 1e6)
            return

        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = destination.with_suffix(destination.suffix + ".part")
        downloaded = 0
        with open(tmp_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                fh.write(chunk)
                downloaded += len(chunk)
        tmp_path.replace(destination)
        logger.info(
            "DONE  %s (%.2f MB)", destination.name, downloaded / 1e6
        )


def main() -> None:
    with timer("download dummy AWS credentials", logger):
        creds_path = ensure_creds_downloaded()
        logger.info("Credentials cached at %s", creds_path)

    with timer("download all raw datasets", logger):
        for object_key, destination in RAW_FILES.items():
            url = f"{S3_BASE_URL}/{object_key}"
            with timer(f"download {object_key}", logger):
                _download(url, destination)

    logger.info("All raw datasets are available in %s", RAW_FILES["sr.csv.gz"].parent)


if __name__ == "__main__":
    main()
