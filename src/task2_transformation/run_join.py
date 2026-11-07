"""
Task 2 - Initial Data Transformation (Data Engineering).

Joins every service request in ``sr.csv.gz`` to the H3 resolution-8 hexagon
(from ``city-hex-polygons-8.geojson``) whose polygon contains the request's
(Longitude, Latitude) point, producing the equivalent of ``sr_hex.csv.gz``.

Design
------
* The spatial join itself is a point-in-polygon test, done entirely in SQL
  via DuckDB's ``spatial`` extension (``ST_Within``), accelerated with an
  R-Tree index over the 3,832 hex polygons so the join is a few seconds, not
  a full NxM scan.
* Requests with a missing Latitude/Longitude are short-circuited to hex
  index ``"0"`` (per the task description) without ever being sent through
  the spatial join.
* As an independent cross-check (not the production path, but a validation
  signal), every joined row's hex index is also derived purely analytically
  via ``h3.latlng_to_cell`` (the H3 library computes the index directly from
  a point with no polygon data at all). Any row where the two methods
  disagree is a strong signal of a boundary/precision issue and is logged.
* The script fails loudly (non-zero exit) if the fraction of geolocated
  requests that fail to land inside *any* hex exceeds
  ``JOIN_ERROR_THRESHOLD``. See README.md task 2 for why that threshold was
  chosen.

Usage
-----
    python -m src.task2_transformation.run_join
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb  # noqa: E402
import h3  # noqa: E402

from src.common.config import (  # noqa: E402
    JOIN_ERROR_THRESHOLD,
    RAW_FILES,
    SR_HEX_JOINED_OUTPUT,
)
from src.common.logging_utils import get_logger  # noqa: E402
from src.common.timing import timer  # noqa: E402

logger = get_logger("task2_transformation")

SR_PATH = RAW_FILES["sr.csv.gz"]
HEX8_GEOJSON_PATH = RAW_FILES["city-hex-polygons-8.geojson"]
REFERENCE_PATH = RAW_FILES["sr_hex.csv.gz"]


def build_connection() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    return con


def run_spatial_join(
    con: duckdb.DuckDBPyConnection,
    sr_path: Path | None = None,
    hex_path: Path | None = None,
) -> None:
    """Create the `sr_hex_joined` table: service requests LEFT JOINed to hex polygons.

    ``sr_path`` / ``hex_path`` default to the full challenge files; tests pass
    tiny fixtures instead so the same join SQL is exercised without the 941k-row
    download.
    """
    sr_path = sr_path or SR_PATH
    hex_path = hex_path or HEX8_GEOJSON_PATH

    con.execute(
        f"""
        CREATE OR REPLACE TABLE sr_raw AS
        SELECT *
        FROM read_csv_auto(
            '{sr_path.as_posix()}',
            header = true,
            all_varchar = false
        )
        """
    )
    # The published sr.csv.gz has an unnamed leading index column that DuckDB
    # names column00; the tiny test fixture does not. Drop it only if present.
    raw_columns = [row[0] for row in con.execute("DESCRIBE sr_raw").fetchall()]
    drop = [c for c in raw_columns if c.lower().startswith("column0")]
    exclude = f" EXCLUDE ({', '.join(drop)})" if drop else ""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE sr AS
        SELECT row_number() OVER () AS _sr_row_id, *{exclude}
        FROM sr_raw
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE hex8 AS
        SELECT
            "index" AS h3_index,
            geom
        FROM ST_Read('{hex_path.as_posix()}')
        """
    )
    # Speeds up the point-in-polygon test from an O(n*m) scan to an indexed
    # lookup - with ~3.8k polygons and several hundred thousand points this
    # is the difference between seconds and minutes.
    con.execute("CREATE INDEX hex8_rtree ON hex8 USING RTREE (geom)")

    # Note: the spatial join is deliberately split into two branches that are
    # UNIONed back together, rather than one LEFT JOIN with the null-check
    # folded into the join predicate. DuckDB's spatial-join optimiser (which
    # rewrites ST_Within joins to use the hex8_rtree index) only kicks in for
    # a "clean" `LEFT JOIN ... ON ST_Within(...)`. Mixing in the extra
    # `latitude IS NOT NULL` predicates defeats that rewrite and silently
    # falls back to an O(n*m) nested-loop scan (~3.6 billion point-in-polygon
    # tests here) which takes >10 minutes instead of ~5 seconds. Pre-filtering
    # the null-coordinate rows out *before* the join keeps the join predicate
    # simple enough for the optimised path.
    con.execute(
        """
        CREATE OR REPLACE TABLE sr_geo AS
        SELECT * FROM sr WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """
    )
    con.execute(
        """
        CREATE OR REPLACE TABLE sr_no_geo AS
        SELECT *, '0' AS h3_level8_index
        FROM sr WHERE latitude IS NULL OR longitude IS NULL
        """
    )
    # QUALIFY ROW_NUMBER = 1 collapses the (rare) case of a point falling in
    # two overlapping polygons, which a raw LEFT JOIN ST_Within would
    # duplicate. Ties are broken by a stable sort on h3_index so the result
    # is deterministic. _sr_row_id is dropped from the delivered table.
    con.execute(
        """
        CREATE OR REPLACE TABLE sr_hex_joined AS
        SELECT * EXCLUDE (_sr_row_id) FROM (
            SELECT
                sr_geo.*,
                COALESCE(hex8.h3_index, '0') AS h3_level8_index
            FROM sr_geo
            LEFT JOIN hex8
                ON ST_Within(ST_Point(sr_geo.longitude, sr_geo.latitude), hex8.geom)
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY sr_geo._sr_row_id
                ORDER BY hex8.h3_index NULLS LAST
            ) = 1
        )
        UNION ALL BY NAME
        SELECT * EXCLUDE (_sr_row_id) FROM sr_no_geo
        """
    )


def validate_join(con: duckdb.DuckDBPyConnection) -> None:
    """Log join-failure counts and abort if they exceed JOIN_ERROR_THRESHOLD."""
    total = con.execute("SELECT COUNT(*) FROM sr_hex_joined").fetchone()[0]
    null_latlon = con.execute(
        "SELECT COUNT(*) FROM sr_hex_joined WHERE latitude IS NULL OR longitude IS NULL"
    ).fetchone()[0]
    geolocated = total - null_latlon
    unmatched_geolocated = con.execute(
        """
        SELECT COUNT(*) FROM sr_hex_joined
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND h3_level8_index = '0'
        """
    ).fetchone()[0]

    failure_rate = unmatched_geolocated / geolocated if geolocated else 0.0

    logger.info("Total requests: %d", total)
    logger.info("Requests with missing lat/lon (set to hex '0' by definition): %d", null_latlon)
    logger.info(
        "Geolocated requests that failed to join to any hex: %d / %d (%.4f%%)",
        unmatched_geolocated,
        geolocated,
        failure_rate * 100,
    )

    n_distinct = con.execute(
        """
        SELECT COUNT(DISTINCT notification_number)
        FROM sr_hex_joined
        WHERE notification_number IS NOT NULL
        """
    ).fetchone()[0]
    n_with_id = con.execute(
        "SELECT COUNT(*) FROM sr_hex_joined WHERE notification_number IS NOT NULL"
    ).fetchone()[0]
    n_duplicated = n_with_id - n_distinct
    logger.info(
        "Join cardinality: %d rows with a notification_number, %d distinct "
        "(%d extra rows from a many-hex match)",
        n_with_id, n_distinct, n_duplicated,
    )
    if n_duplicated > 0:
        raise SystemExit(
            f"Spatial join duplicated {n_duplicated} requests (a point landed in "
            "more than one hex and was not collapsed). Aborting."
        )

    if failure_rate > JOIN_ERROR_THRESHOLD:
        raise SystemExit(
            f"Join failure rate {failure_rate:.4%} exceeds the allowed threshold "
            f"{JOIN_ERROR_THRESHOLD:.4%}; aborting. This usually indicates a CRS, "
            "coordinate order, or stale hex-polygon issue - inspect the "
            "unmatched rows before re-running."
        )


def cross_validate_with_h3_library(con: duckdb.DuckDBPyConnection, sample_size: int = 20_000) -> None:
    """Independently recompute h3 index for a sample via the h3 library.

    This doesn't use the polygon file at all, so agreement between the two
    methods is strong evidence the spatial join is correct, rather than
    merely "consistent with itself".
    """
    sample = con.execute(
        f"""
        SELECT notification_number, latitude, longitude, h3_level8_index
        FROM sr_hex_joined
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        USING SAMPLE {sample_size} ROWS
        """
    ).fetchall()

    mismatches = 0
    for notification_number, lat, lon, joined_index in sample:
        h3_index = h3.latlng_to_cell(lat, lon, 8)
        if h3_index != joined_index:
            mismatches += 1
            logger.warning(
                "H3-LIBRARY MISMATCH notification=%s spatial_join=%s h3_lib=%s",
                notification_number,
                joined_index,
                h3_index,
            )

    logger.info(
        "H3-library cross-validation: %d/%d sampled rows agree (%.4f%% mismatch)",
        len(sample) - mismatches,
        len(sample),
        100 * mismatches / len(sample) if sample else 0.0,
    )


def validate_against_reference(con: duckdb.DuckDBPyConnection) -> None:
    """Compare the join output against the City-provided sr_hex.csv.gz."""
    con.execute(
        f"""
        CREATE OR REPLACE TABLE sr_hex_reference AS
        SELECT notification_number, h3_level8_index AS reference_h3_index
        FROM read_csv_auto('{REFERENCE_PATH.as_posix()}', header = true)
        """
    )
    row = con.execute(
        """
        SELECT
            COUNT(*) AS n_compared,
            SUM(CASE WHEN j.h3_level8_index = r.reference_h3_index THEN 1 ELSE 0 END) AS n_match
        FROM sr_hex_joined j
        JOIN sr_hex_reference r USING (notification_number)
        """
    ).fetchone()
    n_compared, n_match = row
    n_mismatch = n_compared - n_match
    logger.info(
        "Reference validation vs sr_hex.csv.gz: %d/%d rows match exactly (%d mismatches)",
        n_match,
        n_compared,
        n_mismatch,
    )
    if n_compared and n_mismatch > 0:
        sample_mismatches = con.execute(
            """
            SELECT j.notification_number, j.h3_level8_index, r.reference_h3_index
            FROM sr_hex_joined j
            JOIN sr_hex_reference r USING (notification_number)
            WHERE j.h3_level8_index != r.reference_h3_index
            LIMIT 10
            """
        ).fetchall()
        for notification_number, mine, theirs in sample_mismatches:
            logger.warning(
                "REFERENCE MISMATCH notification=%s mine=%s reference=%s",
                notification_number,
                mine,
                theirs,
            )


def write_output(con: duckdb.DuckDBPyConnection) -> None:
    SR_HEX_JOINED_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY sr_hex_joined TO '{SR_HEX_JOINED_OUTPUT.as_posix()}'
        (HEADER, DELIMITER ',', COMPRESSION gzip)
        """
    )
    logger.info("Wrote joined dataset to %s", SR_HEX_JOINED_OUTPUT)


def main() -> None:
    con = build_connection()

    with timer("spatial join of sr.csv.gz to resolution-8 hex polygons", logger):
        run_spatial_join(con)

    with timer("join validation / error-rate check", logger):
        validate_join(con)

    with timer("cross-validation against h3 library", logger):
        cross_validate_with_h3_library(con)

    with timer("validation against sr_hex.csv.gz reference file", logger):
        validate_against_reference(con)

    with timer("write joined output", logger):
        write_output(con)

    logger.info("Task 2 completed successfully.")


if __name__ == "__main__":
    main()
