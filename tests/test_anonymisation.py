"""Unit tests for the Task 5.3 anonymisation SQL logic: time-bucket flooring
(a bug caught during development - see comment in anonymise.py) and the
k-anonymity grouping/routing decision."""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.task5_further_transformations.anonymise import time_bucket_sql  # noqa: E402


def _floor_to_bucket(con: duckdb.DuckDBPyConnection, timestamp_literal: str, bucket_hours: int) -> datetime:
    expr = time_bucket_sql(f"TIMESTAMPTZ '{timestamp_literal}'", hours=bucket_hours)
    row = con.execute(f"SELECT {expr}").fetchone()
    return row[0]


def _expected_utc_bucket(timestamp_literal: str, bucket_hours: int) -> datetime:
    """Independently (in plain Python, no DuckDB) compute the expected
    UTC-aligned bucket start, as ground truth for the SQL under test."""
    dt = datetime.fromisoformat(timestamp_literal).astimezone(timezone.utc)
    bucket_seconds = bucket_hours * 3600
    epoch_seconds = int(dt.timestamp())
    floored = (epoch_seconds // bucket_seconds) * bucket_seconds
    return datetime.fromtimestamp(floored, tz=timezone.utc)


@pytest.fixture()
def con():
    return duckdb.connect()


@pytest.mark.parametrize(
    "timestamp_literal",
    [
        "2020-11-10 09:57:49+02:00",
        "2020-11-10 00:00:00+02:00",
        "2020-11-10 05:59:59+02:00",
        "2020-11-10 06:00:00+02:00",
        "2020-11-10 23:59:59+02:00",
        "2020-01-01 00:00:01+02:00",
    ],
)
def test_six_hour_bucket_flooring_matches_independent_python_computation(con, timestamp_literal):
    """This is a regression test for a real bug found during development:
    DuckDB's `//` operator on a DOUBLE (what epoch() returns) performs
    ordinary float division, not floor division, so without the explicit
    CAST(... AS BIGINT) the 'bucket' silently equals the original exact
    timestamp - i.e. no anonymisation would actually happen at all. The
    expected value here is computed independently in plain Python (buckets
    are UTC-aligned, since epoch() is UTC-based) rather than via DuckDB, so
    this test doesn't just check DuckDB against itself."""
    actual = _floor_to_bucket(con, timestamp_literal, bucket_hours=6)
    expected = _expected_utc_bucket(timestamp_literal, bucket_hours=6)
    assert actual.astimezone(timezone.utc) == expected


def test_bucket_is_stable_within_the_same_six_hour_window(con):
    start = _floor_to_bucket(con, "2020-11-10 08:00:00+02:00", bucket_hours=6)
    just_before_end = _floor_to_bucket(con, "2020-11-10 13:59:59+02:00", bucket_hours=6)
    just_after_end = _floor_to_bucket(con, "2020-11-10 14:00:00+02:00", bucket_hours=6)
    assert start == just_before_end
    assert just_after_end == start + timedelta(hours=6)


def test_naive_float_floor_division_bug_reproduction(con):
    """Demonstrates the exact bug described above, so a future refactor that
    reintroduces it fails loudly."""
    buggy = con.execute(
        "SELECT (epoch(TIMESTAMPTZ '2020-11-10 09:57:49+02:00') // 21600) * 21600"
    ).fetchone()[0]
    correct = con.execute(
        "SELECT (CAST(epoch(TIMESTAMPTZ '2020-11-10 09:57:49+02:00') AS BIGINT) // 21600) * 21600"
    ).fetchone()[0]
    assert buggy != correct, "DuckDB may have changed `//` semantics for DOUBLE; re-check anonymise.py"


def test_k_anonymity_grouping_and_routing(con):
    con.execute(
        """
        CREATE TABLE t AS SELECT * FROM (VALUES
            ('hexA', 'bucket1'),
            ('hexA', 'bucket1'),
            ('hexA', 'bucket1'),
            ('hexA', 'bucket1'),
            ('hexA', 'bucket1'),
            ('hexB', 'bucket2')
        ) AS v(hex, bucket)
        """
    )
    grouped = con.execute(
        """
        SELECT hex, bucket, COUNT(*) OVER (PARTITION BY hex, bucket) AS group_size
        FROM t
        """
    ).fetchall()
    k = 5
    released = [row for row in grouped if row[2] >= k]
    review = [row for row in grouped if row[2] < k]
    assert len(released) == 5  # the 5 hexA/bucket1 rows
    assert len(review) == 1  # the lone hexB/bucket2 row


def test_public_release_schema_has_no_suburb_or_service_type():
    from src.common.config import ANONYMISATION_DROP_FROM_RELEASE, ANONYMISATION_RELEASE_COLUMNS

    leaked = {
        "official_suburb",
        "directorate",
        "department",
        "code",
        "code_group",
        "latitude",
        "longitude",
        "notification_number",
        "distance_to_centroid_m",
    }
    assert leaked <= set(ANONYMISATION_DROP_FROM_RELEASE)
    assert "official_suburb" not in ANONYMISATION_RELEASE_COLUMNS
    assert "code" not in ANONYMISATION_RELEASE_COLUMNS
