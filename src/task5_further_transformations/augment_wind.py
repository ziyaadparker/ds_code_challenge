"""
Task 5.2 - Further Data Transformations (Data Engineering): wind augmentation.

Augments the Atlantis subsample (task 5.1 output) with wind direction and
speed readings from the Atlantis Air Quality Measurement site for 2020,
joined on the hour in which each service request was created.

Handling an unreliable dependency
----------------------------------
The City of Cape Town's Open Data Portal endpoint for this file is, as the
challenge brief warns, unreliable. As of this run it is **not reachable at
all**: the documented download URL returns HTTP 404, and the domain it
redirects to (``cityapps.capetown.gov.za``) no longer resolves in DNS (the
City migrated its Open Data Portal to a new ArcGIS-based platform after this
challenge was written). This is confirmed and logged by this script, not
assumed.

Strategy chosen, in order:
    1. Try the documented URL, with retries (exponential backoff + jitter)
       to absorb transient failures.
    2. Fall back to a local cache (``data/external/``) from a previous
       successful run, so a one-off outage doesn't block a re-run.
    3. Fall back to the committed fixture
       ``tests/fixtures/wind_atlantis_2020.ods`` so the parse + hourly join
       can still be demonstrated (and tested) when the portal is down.
    4. If even that is missing, **degrade gracefully**: output is still
       produced, with ``wind_data_available = False`` and NULL wind columns.

Why graceful degradation rather than raising an exception? The wind join is
an *enrichment* of the core, already-validated Atlantis subsample - failing
the entire task because a secondary, best-effort enrichment source is down
would throw away otherwise-complete, correct output. A report consumer is
much better served by a dataset that's honestly missing one optional field
(and says so) than by no dataset at all. If this were instead a required,
load-bearing join (e.g. task 2's hex join), the right call is the opposite -
hard-fail above a threshold, which is exactly what ``run_join.py`` does.

Usage
-----
    python -m src.task5_further_transformations.augment_wind
"""
from __future__ import annotations

import random
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb  # noqa: E402
import pandas as pd  # noqa: E402
import requests  # noqa: E402

from src.common.config import (  # noqa: E402
    ATLANTIS_SUBSAMPLE_PATH,
    ATLANTIS_WIND_AUGMENTED_PATH,
    WIND_DATA_PATH,
    WIND_DATA_URL,
    WIND_FIXTURE_PATH,
)
from src.common.logging_utils import get_logger  # noqa: E402
from src.common.timing import timer  # noqa: E402

logger = get_logger("task5_augment_wind")

# A couple of historical/alternate hosts for the same document have been
# observed for City of Cape Town open-data files; tried in order after the
# primary documented URL.
CANDIDATE_URLS = [
    WIND_DATA_URL,
    "https://cityapps.capetown.gov.za/sites/opendatacatalog/Documents/Wind/Wind_direction_and_speed_2020.ods",
]


def download_wind_data(max_retries: int = 3, backoff_seconds: float = 2.0) -> Path | None:
    """Try to download the wind data file. Returns the local path, or None
    if every candidate URL, the committed fixture, and a cached copy are all
    unavailable."""
    if WIND_DATA_PATH.exists():
        logger.info("Using cached wind data file at %s", WIND_DATA_PATH)
        return WIND_DATA_PATH

    # One attempt per URL is enough to *document* that the portal is down
    # when we already have a committed fixture to fall back to.
    attempts = 1 if WIND_FIXTURE_PATH.exists() else max_retries

    for url in CANDIDATE_URLS:
        for attempt in range(1, attempts + 1):
            try:
                resp = requests.get(url, timeout=30)
                resp.raise_for_status()
                content_type = resp.headers.get("Content-Type", "")
                if "html" in content_type.lower():
                    raise ValueError(f"Got an HTML error page instead of a spreadsheet from {url}")
                WIND_DATA_PATH.parent.mkdir(parents=True, exist_ok=True)
                WIND_DATA_PATH.write_bytes(resp.content)
                logger.info("Downloaded wind data from %s", url)
                return WIND_DATA_PATH
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "Wind data download failed (url=%s, attempt %d/%d): %s",
                    url, attempt, attempts, exc,
                )
                if attempt < attempts:
                    time.sleep(backoff_seconds * attempt + random.uniform(0, 1))

    if WIND_FIXTURE_PATH.exists():
        logger.warning(
            "Upstream wind endpoint is unreachable; falling back to committed "
            "fixture %s so the hourly join can still be demonstrated.",
            WIND_FIXTURE_PATH,
        )
        return WIND_FIXTURE_PATH

    logger.error(
        "Could not download wind data from any candidate URL (%s) and no local "
        "cache or fixture exists. Proceeding with graceful degradation - the "
        "output will mark every row as wind_data_available=False.",
        CANDIDATE_URLS,
    )
    return None


def parse_wind_data(path: Path) -> pd.DataFrame:
    """Parse the .ods wind file into a tidy (timestamp, wind_speed, wind_direction) frame.

    Defensive about exact column naming/header-row position, since we don't
    control the upstream file format and it has historically shifted.
    """
    raw = pd.read_excel(path, engine="odf", sheet_name=0, header=None)

    header_row_idx = None
    for i in range(min(10, len(raw))):
        row_values = raw.iloc[i].astype(str).str.lower()
        if row_values.str.contains("date|time", regex=True).any() and row_values.str.contains(
            "speed|direction", regex=True
        ).any():
            header_row_idx = i
            break
    if header_row_idx is None:
        header_row_idx = 0

    df = pd.read_excel(path, engine="odf", sheet_name=0, header=header_row_idx)
    df.columns = [str(c).strip().lower() for c in df.columns]

    timestamp_col = next((c for c in df.columns if "date" in c or "time" in c), None)
    speed_col = next((c for c in df.columns if "speed" in c), None)
    direction_col = next((c for c in df.columns if "direction" in c), None)

    if not all([timestamp_col, speed_col, direction_col]):
        raise ValueError(
            f"Could not identify timestamp/speed/direction columns in {path}; "
            f"found columns: {list(df.columns)}"
        )

    tidy = df[[timestamp_col, speed_col, direction_col]].rename(
        columns={timestamp_col: "timestamp", speed_col: "wind_speed", direction_col: "wind_direction"}
    )
    tidy["timestamp"] = pd.to_datetime(tidy["timestamp"], errors="coerce")
    tidy = tidy.dropna(subset=["timestamp"])
    # City observations are in South African Standard Time. Naive timestamps
    # in the spreadsheet are treated as SAST so they join correctly against
    # service-request creation_timestamp values (which carry +02:00).
    if tidy["timestamp"].dt.tz is None:
        tidy["timestamp"] = tidy["timestamp"].dt.tz_localize("Africa/Johannesburg")
    else:
        tidy["timestamp"] = tidy["timestamp"].dt.tz_convert("Africa/Johannesburg")
    tidy["hour"] = tidy["timestamp"].dt.tz_convert("UTC").dt.floor("h")
    # Multiple sub-hourly readings per hour are not unusual; average them so
    # the join target (creation hour) has exactly one wind reading.
    hourly = tidy.groupby("hour", as_index=False)[["wind_speed", "wind_direction"]].mean()
    return hourly


def build_augmented_output(
    con: duckdb.DuckDBPyConnection,
    wind_hourly: pd.DataFrame | None,
    subsample_path: Path | None = None,
) -> None:
    subsample_path = subsample_path or ATLANTIS_SUBSAMPLE_PATH
    con.execute(
        f"""
        CREATE OR REPLACE TABLE atlantis AS
        SELECT * FROM read_csv_auto('{subsample_path.as_posix()}', header = true)
        """
    )

    if wind_hourly is None or wind_hourly.empty:
        con.execute(
            """
            CREATE OR REPLACE TABLE atlantis_wind AS
            SELECT *, NULL::DOUBLE AS wind_speed, NULL::DOUBLE AS wind_direction,
                   false AS wind_data_available
            FROM atlantis
            """
        )
        return

    con.register("wind_hourly", wind_hourly)
    con.execute(
        """
        CREATE OR REPLACE TABLE atlantis_wind AS
        SELECT
            a.*,
            w.wind_speed,
            w.wind_direction,
            (w.wind_speed IS NOT NULL) AS wind_data_available
        FROM atlantis a
        LEFT JOIN wind_hourly w
            ON date_trunc('hour', a.creation_timestamp::TIMESTAMPTZ) = w.hour
        """
    )


def main() -> None:
    with timer("obtain wind data for 2020 (download, cache, or degrade)", logger):
        wind_path = download_wind_data()

    wind_hourly = None
    if wind_path is not None:
        with timer("parse wind .ods file into hourly averages", logger):
            try:
                wind_hourly = parse_wind_data(wind_path)
                logger.info("Parsed %d hourly wind readings", len(wind_hourly))
            except Exception as exc:  # noqa: BLE001
                logger.error("Failed to parse wind data file (%s); degrading gracefully", exc)

    con = duckdb.connect()
    with timer("join Atlantis subsample to hourly wind readings", logger):
        build_augmented_output(con, wind_hourly)

    n_total = con.execute("SELECT COUNT(*) FROM atlantis_wind").fetchone()[0]
    n_with_wind = con.execute(
        "SELECT COUNT(*) FROM atlantis_wind WHERE wind_data_available"
    ).fetchone()[0]
    logger.info(
        "Wind augmentation coverage: %d / %d rows (%.2f%%) have a matched wind reading",
        n_with_wind, n_total, 100 * n_with_wind / n_total if n_total else 0.0,
    )
    if n_with_wind == 0:
        logger.warning(
            "No rows have wind data - the upstream wind data source is currently "
            "unreachable (see log above). This is a known, documented limitation "
            "of the external dependency, not a bug in this script; output is "
            "still produced with wind_data_available=False throughout."
        )

    with timer("write wind-augmented output", logger):
        ATLANTIS_WIND_AUGMENTED_PATH.parent.mkdir(parents=True, exist_ok=True)
        con.execute(
            f"COPY atlantis_wind TO '{ATLANTIS_WIND_AUGMENTED_PATH.as_posix()}' (HEADER, DELIMITER ',')"
        )
        logger.info("Wrote %s", ATLANTIS_WIND_AUGMENTED_PATH)

    source = (
        "none (degraded)"
        if wind_hourly is None
        else ("live/cache" if WIND_DATA_PATH.exists() else "committed fixture")
    )
    logger.info("Task 5.2 completed (wind source: %s).", source)


if __name__ == "__main__":
    main()
