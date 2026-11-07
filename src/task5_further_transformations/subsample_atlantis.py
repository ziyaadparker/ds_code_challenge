"""
Task 5.1 - Further Data Transformations (Data Engineering): Atlantis subsample.

Creates a subsample of ``sr_hex.csv.gz`` containing only the requests that
fall within 1 minute of arc (by definition, 1 nautical mile = 1,852 m) of the
centroid of the official City of Cape Town suburb of "Atlantis", which lies
in the North of the municipal area.

Method (fully programmatic, no hard-coded coordinates)
--------------------------------------------------------
1. The suburb's administrative boundary polygon is downloaded from
   OpenStreetMap via the Nominatim search API (a relation tagged
   ``place=town`` / suburb boundary for Atlantis, Cape Town).
2. The polygon's centroid is computed with Shapely's ``.centroid`` - i.e. the
   geometric centroid of the boundary polygon, not Nominatim's own
   (unspecified-method) lat/lon point. This is cached locally so repeated
   runs don't hit the Nominatim API.
3. Every request in ``sr_hex.csv.gz`` with a non-null Latitude/Longitude is
   scored by its great-circle (haversine) distance to that centroid, and
   rows within ``ATLANTIS_RADIUS_METERS`` (1,852 m) are kept.

Resilience: Nominatim is a shared, rate-limited public service. If it is
unreachable, we fall back to a previously cached boundary/centroid on disk,
so the script can still complete offline after the first successful run (see
``fetch_atlantis_boundary``).

Usage
-----
    python -m src.task5_further_transformations.subsample_atlantis
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb  # noqa: E402
import requests  # noqa: E402
from shapely.geometry import shape  # noqa: E402

from src.common.config import (  # noqa: E402
    ATLANTIS_BOUNDARY_PATH,
    ATLANTIS_CENTROID_PATH,
    ATLANTIS_NOMINATIM_QUERY,
    ATLANTIS_RADIUS_METERS,
    ATLANTIS_SUBSAMPLE_PATH,
    ATLANTIS_SUBURB_NAME,
    NOMINATIM_URL,
    NOMINATIM_USER_AGENT,
    RAW_FILES,
)
from src.common.logging_utils import get_logger  # noqa: E402
from src.common.timing import timer  # noqa: E402

logger = get_logger("task5_subsample_atlantis")

SR_HEX_PATH = RAW_FILES["sr_hex.csv.gz"]


def fetch_atlantis_boundary(max_retries: int = 3, backoff_seconds: float = 2.0) -> dict[str, Any]:
    """Fetch the Atlantis suburb boundary polygon from Nominatim, with caching.

    Nominatim's usage policy asks for a maximum of 1 request/second and a
    descriptive User-Agent, both of which are respected here. On failure we
    retry with exponential backoff, and ultimately fall back to a cached
    copy on disk if one exists, so a transient outage doesn't break the
    pipeline on a re-run.
    """
    if ATLANTIS_BOUNDARY_PATH.exists():
        logger.info("Using cached suburb boundary at %s", ATLANTIS_BOUNDARY_PATH)
        return json.loads(ATLANTIS_BOUNDARY_PATH.read_text())

    params = {
        "q": ATLANTIS_NOMINATIM_QUERY,
        "format": "jsonv2",
        "polygon_geojson": 1,
        "limit": 1,
    }
    headers = {"User-Agent": NOMINATIM_USER_AGENT}

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=30)
            resp.raise_for_status()
            results = resp.json()
            if not results:
                raise ValueError(f"Nominatim returned no results for '{ATLANTIS_NOMINATIM_QUERY}'")
            result = results[0]
            if "geojson" not in result:
                raise ValueError("Nominatim result did not include a geojson boundary")

            ATLANTIS_BOUNDARY_PATH.parent.mkdir(parents=True, exist_ok=True)
            ATLANTIS_BOUNDARY_PATH.write_text(json.dumps(result))
            logger.info(
                "Fetched boundary for '%s' (osm_type=%s, class=%s) from Nominatim",
                result.get("display_name"),
                result.get("osm_type"),
                result.get("class"),
            )
            return result
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning(
                "Nominatim request failed (attempt %d/%d): %s", attempt, max_retries, exc
            )
            if attempt < max_retries:
                time.sleep(backoff_seconds * attempt)

    raise RuntimeError(
        f"Could not fetch Atlantis boundary from Nominatim after {max_retries} attempts "
        f"and no local cache exists at {ATLANTIS_BOUNDARY_PATH}"
    ) from last_exc


def compute_centroid(boundary: dict[str, Any]) -> tuple[float, float]:
    """Compute the geometric centroid of the fetched boundary polygon."""
    if ATLANTIS_CENTROID_PATH.exists():
        cached = json.loads(ATLANTIS_CENTROID_PATH.read_text())
        logger.info("Using cached centroid %s", cached)
        return cached["lat"], cached["lon"]

    geom = shape(boundary["geojson"])
    centroid = geom.centroid
    lat, lon = centroid.y, centroid.x

    ATLANTIS_CENTROID_PATH.parent.mkdir(parents=True, exist_ok=True)
    ATLANTIS_CENTROID_PATH.write_text(
        json.dumps(
            {
                "suburb": ATLANTIS_SUBURB_NAME,
                "lat": lat,
                "lon": lon,
                "method": "shapely_polygon_centroid",
                "source": "OpenStreetMap via Nominatim",
            },
            indent=2,
        )
    )
    logger.info("Computed centroid of '%s': lat=%.6f lon=%.6f", ATLANTIS_SUBURB_NAME, lat, lon)
    return lat, lon


def build_subsample(con: duckdb.DuckDBPyConnection, centroid_lat: float, centroid_lon: float) -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE sr_hex AS
        SELECT * FROM read_csv_auto('{SR_HEX_PATH.as_posix()}', header = true)
        """
    )
    # Haversine great-circle distance (metres), computed directly in SQL so
    # the whole filter runs as one vectorised query rather than row-by-row
    # Python - this scales comfortably to the full ~940k-row dataset.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE sr_hex_atlantis AS
        WITH distances AS (
            SELECT
                *,
                2 * 6371000 * ASIN(
                    SQRT(
                        POWER(SIN(RADIANS(latitude - {centroid_lat}) / 2), 2)
                        + COS(RADIANS({centroid_lat})) * COS(RADIANS(latitude))
                          * POWER(SIN(RADIANS(longitude - {centroid_lon}) / 2), 2)
                    )
                ) AS distance_to_centroid_m
            FROM sr_hex
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        )
        SELECT * FROM distances
        WHERE distance_to_centroid_m <= {ATLANTIS_RADIUS_METERS}
        ORDER BY distance_to_centroid_m
        """
    )


def main() -> None:
    with timer("fetch Atlantis suburb boundary (Nominatim, cached)", logger):
        boundary = fetch_atlantis_boundary()

    with timer("compute suburb centroid", logger):
        centroid_lat, centroid_lon = compute_centroid(boundary)

    con = duckdb.connect()

    with timer("filter sr_hex.csv.gz to within 1' of arc of centroid", logger):
        build_subsample(con, centroid_lat, centroid_lon)

    n_total = con.execute("SELECT COUNT(*) FROM sr_hex").fetchone()[0]
    n_subsample = con.execute("SELECT COUNT(*) FROM sr_hex_atlantis").fetchone()[0]
    logger.info(
        "Subsample: %d / %d requests (%.4f%%) within %.0fm of (%.6f, %.6f)",
        n_subsample,
        n_total,
        100 * n_subsample / n_total,
        ATLANTIS_RADIUS_METERS,
        centroid_lat,
        centroid_lon,
    )

    with timer("write Atlantis subsample", logger):
        ATLANTIS_SUBSAMPLE_PATH.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY sr_hex_atlantis TO '{ATLANTIS_SUBSAMPLE_PATH.as_posix()}' (HEADER, DELIMITER ',')"
        )
        logger.info("Wrote %s", ATLANTIS_SUBSAMPLE_PATH)

    logger.info("Task 5.1 completed successfully.")


if __name__ == "__main__":
    main()
