"""Helpers for talking to the (public, read-only) challenge S3 bucket.

The bucket allows anonymous access, but the challenge also publishes a
"dummy" credentials file that grants the same access via a signed request.
We prefer the signed credentials because that's what the exercise (task 1)
revolves around (``S3 Select``, which is only reachable via the S3 API, not
plain HTTPS), and fall back to anonymous access if the credentials endpoint
is ever unreachable.
"""
from __future__ import annotations

import json
import logging
from functools import lru_cache
from pathlib import Path

import boto3
import requests
from botocore import UNSIGNED
from botocore.client import Config

from src.common.config import CREDS_PATH, CREDS_URL, S3_REGION

logger = logging.getLogger(__name__)


def ensure_creds_downloaded(path: Path = CREDS_PATH, url: str = CREDS_URL) -> Path:
    """Download the dummy credentials file if it isn't already cached locally."""
    if path.exists():
        return path
    path.parent.mkdir(parents=True, exist_ok=True)
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    path.write_bytes(resp.content)
    return path


@lru_cache(maxsize=1)
def get_s3_client():
    """Return a boto3 S3 client, preferring the challenge's dummy credentials.

    Falls back to an unsigned (anonymous) client if the credentials can't be
    fetched, since the bucket objects are also publicly readable.
    """
    try:
        creds_path = ensure_creds_downloaded()
        creds = json.loads(creds_path.read_text())["s3"]
        logger.info("Using challenge-supplied S3 credentials from %s", creds_path)
        return boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=creds["access_key"],
            aws_secret_access_key=creds["secret_key"],
        )
    except Exception as exc:  # noqa: BLE001 - deliberately broad: this is a fallback path
        logger.warning(
            "Could not obtain signed S3 credentials (%s); falling back to anonymous access",
            exc,
        )
        return boto3.client(
            "s3",
            region_name=S3_REGION,
            config=Config(signature_version=UNSIGNED),
        )
