"""
Task 5.3 - Further Data Transformations (Data Engineering): anonymisation.

Anonymises the wind-augmented Atlantis subsample (task 5.2 output), while
preserving:
    * location accuracy to within ~500 m  -> generalise to the H3
      resolution-8 cell (``h3_level8_index``, already present), which has an
      average hexagon edge length of ~461 m. Exact latitude/longitude are
      dropped entirely.
    * temporal accuracy to within 6 hours -> floor both timestamps to the
      start of a UTC-aligned 6-hour window via integer epoch arithmetic.
      Exact timestamps are dropped.
    * Direct identifiers (``reference_number``, ``notification_number``) and
      remaining quasi-identifiers (suburb, directorate/department/branch/
      section, service-type codes, distance-to-centroid) are dropped from
      the *public* release. They are retained only on the internal review
      file.

Wind on the public file is not the hourly station reading from task 5.2.
An hourly value inside a 6-hour bucket would split the k-group and, because
the City wind series is itself public, would let someone match the number
back to the hour and undo the time generalisation. The release therefore
publishes one value per creation-time bucket: the mean speed and the
circular mean direction of the distinct hours in that bucket. Every row
in the bucket shares it, so wind cannot distinguish one resident from
another in the same cell.

On top of those generalisations, a k-anonymity check groups records by every
quasi-identifier that is actually on the public release -
``(h3_level8_index, creation_time_bucket, completion_time_bucket)`` - and
routes groups smaller than ``ANONYMISATION_K_THRESHOLD`` to a separate
human-review file. ``completion_time_bucket`` has to be in that partition,
not just the first two columns: it is its own independent 6-hour bucket, so
five records with the same hex and creation bucket could still land in five
different completion buckets, which would make each one distinguishable
again in the file that is actually published. That review file is **not**
a public deliverable. It still has the original hourly wind.

See README.md (task 5.3) for the <500 word justification write-up.

Usage
-----
    python -m src.task5_further_transformations.anonymise
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb  # noqa: E402

from src.common.config import (  # noqa: E402
    ANONYMISATION_K_THRESHOLD,
    ANONYMISATION_RELEASE_COLUMNS,
    ANONYMISATION_TIME_BUCKET_HOURS,
    ATLANTIS_ANONYMISED_PATH,
    ATLANTIS_ANONYMISED_REVIEW_PATH,
    ATLANTIS_WIND_AUGMENTED_PATH,
    REVIEW_FILE_NOTICE,
)
from src.common.logging_utils import get_logger  # noqa: E402
from src.common.timing import timer  # noqa: E402

logger = get_logger("task5_anonymise")

# Shared SQL fragment: floor a timestamptz column to a UTC N-hour bucket.
# CAST to BIGINT is load-bearing - DuckDB `//` on DOUBLE is float division.
TIME_BUCKET_SQL = """
to_timestamp(
    (CAST(epoch({column}::TIMESTAMPTZ) AS BIGINT) // ({hours} * 3600))
    * ({hours} * 3600)
)
"""


def time_bucket_sql(column: str, hours: int = ANONYMISATION_TIME_BUCKET_HOURS) -> str:
    return TIME_BUCKET_SQL.format(column=column, hours=hours)


def build_anonymised_tables(
    con: duckdb.DuckDBPyConnection,
    source_path: Path | None = None,
) -> None:
    source_path = source_path or ATLANTIS_WIND_AUGMENTED_PATH
    con.execute(
        f"""
        CREATE OR REPLACE TABLE wind_augmented AS
        SELECT * FROM read_csv_auto('{source_path.as_posix()}', header = true)
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE generalised AS
        SELECT
            *,
            {time_bucket_sql("creation_timestamp")} AS creation_time_bucket,
            {time_bucket_sql("completion_timestamp")} AS completion_time_bucket
        FROM wind_augmented
        """
    )

    # Partitioned on every quasi-identifier actually present on the public
    # release, not just the two that are easiest to think about. The public
    # file also carries completion_time_bucket (ANONYMISATION_RELEASE_COLUMNS)
    # - if it were left out here, five rows could share the same hex and
    # creation_time_bucket while having five different completion_time_bucket
    # values, making each one individually distinguishable again in the
    # column that was actually published. See AI_log.md for how this was
    # found and the alternative (dropping completion_time_bucket) that was
    # rejected.
    con.execute(
        """
        CREATE OR REPLACE TABLE with_group_size AS
        SELECT
            g.*,
            COUNT(*) OVER (
                PARTITION BY h3_level8_index, creation_time_bucket, completion_time_bucket
            ) AS quasi_id_group_size
        FROM generalised g
        """
    )

    # Hourly wind varies inside a 6-hour bucket and would (a) split the
    # k-anonymous group and (b) be matchable back to the public hourly wind
    # series, recovering the hour. Collapse to one station-level summary per
    # creation_time_bucket: unweighted mean of distinct hours (not weighted
    # by how many requests fell in that hour). Direction uses a circular mean
    # so 350° and 10° do not average to 180°.
    con.execute(
        f"""
        CREATE OR REPLACE TABLE bucket_wind AS
        SELECT
            creation_time_bucket,
            AVG(speed) AS wind_speed,
            CASE
                WHEN AVG(dir_sin) IS NULL THEN NULL
                ELSE (
                    DEGREES(ATAN2(AVG(dir_sin), AVG(dir_cos))) + 360
                ) % 360
            END AS wind_direction,
            BOOL_OR(available) AS wind_data_available
        FROM (
            SELECT DISTINCT
                creation_time_bucket,
                date_trunc('hour', creation_timestamp::TIMESTAMPTZ) AS creation_hour,
                TRY_CAST(wind_speed AS DOUBLE) AS speed,
                SIN(RADIANS(TRY_CAST(wind_direction AS DOUBLE))) AS dir_sin,
                COS(RADIANS(TRY_CAST(wind_direction AS DOUBLE))) AS dir_cos,
                COALESCE(wind_data_available, false) AS available
            FROM with_group_size
            WHERE quasi_id_group_size >= {ANONYMISATION_K_THRESHOLD}
        )
        GROUP BY creation_time_bucket
        """
    )

    non_wind = [
        c for c in ANONYMISATION_RELEASE_COLUMNS
        if c not in {"wind_speed", "wind_direction", "wind_data_available"}
    ]
    non_wind_sql = ", ".join(f"g.{c}" for c in non_wind)
    con.execute(
        f"""
        CREATE OR REPLACE TABLE anonymised_release AS
        SELECT
            {non_wind_sql},
            w.wind_speed,
            w.wind_direction,
            w.wind_data_available
        FROM with_group_size g
        JOIN bucket_wind w USING (creation_time_bucket)
        WHERE g.quasi_id_group_size >= {ANONYMISATION_K_THRESHOLD}
        """
    )

    con.execute(
        f"""
        CREATE OR REPLACE TABLE for_human_review AS
        SELECT w.*, s.quasi_id_group_size
        FROM with_group_size s
        JOIN wind_augmented w USING (notification_number)
        WHERE s.quasi_id_group_size < {ANONYMISATION_K_THRESHOLD}
        """
    )


def _write_review_notice(review_csv: Path) -> None:
    notice_path = review_csv.with_suffix(".INTERNAL_ONLY.txt")
    notice_path.write_text(REVIEW_FILE_NOTICE + "\n")
    logger.warning("%s — see %s", REVIEW_FILE_NOTICE, notice_path)


def main() -> None:
    con = duckdb.connect()

    with timer("generalise location/time and drop quasi-identifiers", logger):
        build_anonymised_tables(con)

    n_total = con.execute("SELECT COUNT(*) FROM with_group_size").fetchone()[0]
    n_released = con.execute("SELECT COUNT(*) FROM anonymised_release").fetchone()[0]
    n_review = n_total - n_released

    logger.info(
        "k-anonymity (k=%d) over (h3_level8_index, creation_time_bucket, "
        "completion_time_bucket): "
        "%d/%d records (%.2f%%) released; %d/%d (%.2f%%) routed to manual review",
        ANONYMISATION_K_THRESHOLD,
        n_released, n_total, 100 * n_released / n_total if n_total else 0,
        n_review, n_total, 100 * n_review / n_total if n_total else 0,
    )
    logger.info(
        "Public release columns: %s",
        ", ".join(ANONYMISATION_RELEASE_COLUMNS),
    )

    with timer("write anonymised release + review files", logger):
        ATLANTIS_ANONYMISED_PATH.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY anonymised_release TO '{ATLANTIS_ANONYMISED_PATH.as_posix()}' (HEADER, DELIMITER ',')"
        )
        logger.info("Wrote public anonymised dataset to %s", ATLANTIS_ANONYMISED_PATH)

        con.execute(
            f"COPY for_human_review TO '{ATLANTIS_ANONYMISED_REVIEW_PATH.as_posix()}' (HEADER, DELIMITER ',')"
        )
        _write_review_notice(ATLANTIS_ANONYMISED_REVIEW_PATH)
        logger.info(
            "Wrote %d INTERNAL-ONLY records for manual review to %s",
            n_review, ATLANTIS_ANONYMISED_REVIEW_PATH,
        )

    logger.info("Task 5.3 completed successfully.")


if __name__ == "__main__":
    main()
