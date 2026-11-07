"""Tests for Task 5.2 wind parsing and hourly join, using the committed .ods fixture."""
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import WIND_FIXTURE_PATH  # noqa: E402
from src.task5_further_transformations.augment_wind import (  # noqa: E402
    build_augmented_output,
    parse_wind_data,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_fixture_ods_exists():
    assert WIND_FIXTURE_PATH.exists()
    assert WIND_FIXTURE_PATH.suffix == ".ods"


def test_parse_wind_data_averages_sub_hourly_readings():
    hourly = parse_wind_data(WIND_FIXTURE_PATH)
    assert list(hourly.columns) == ["hour", "wind_speed", "wind_direction"]
    # Two readings in 10:00 SAST / 08:00 UTC on 2020-01-01 should collapse to one hour.
    hour = pd.Timestamp("2020-01-01 08:00:00", tz="UTC")
    # pandas floor('h') may be tz-naive depending on the file; compare by wall-clock UTC.
    hourly = hourly.copy()
    hourly["hour_utc"] = pd.to_datetime(hourly["hour"], utc=True)
    match = hourly[hourly["hour_utc"] == hour]
    assert len(match) == 1
    # 5.0 and 7.0 m/s -> 6.0; 180 and 200 deg -> 190
    assert match.iloc[0]["wind_speed"] == 6.0
    assert match.iloc[0]["wind_direction"] == 190.0


def test_wind_join_sets_availability_flag_when_hour_matches():
    hourly = parse_wind_data(WIND_FIXTURE_PATH)
    con = duckdb.connect()
    build_augmented_output(con, hourly, subsample_path=FIXTURES / "atlantis_tiny.csv")
    n1 = con.execute(
        "SELECT wind_speed, wind_direction, wind_data_available "
        "FROM atlantis_wind WHERE notification_number = 'N1'"
    ).fetchone()
    assert n1[2] is True
    assert n1[0] == 6.0
    assert n1[1] == 190.0

    n6 = con.execute(
        "SELECT wind_data_available FROM atlantis_wind WHERE notification_number = 'N6'"
    ).fetchone()[0]
    assert n6 is True  # fixture includes 2020-11-10 08:00 UTC


def test_wind_join_degrades_when_hourly_frame_is_missing():
    con = duckdb.connect()
    build_augmented_output(con, None, subsample_path=FIXTURES / "atlantis_tiny.csv")
    flags = con.execute("SELECT DISTINCT wind_data_available FROM atlantis_wind").fetchall()
    assert flags == [(False,)]
