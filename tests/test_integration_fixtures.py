"""Integration tests against tiny committed fixtures (no network, no 941k-row files)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import ANONYMISATION_RELEASE_COLUMNS, ANONYMISATION_DROP_FROM_RELEASE  # noqa: E402
from src.task2_transformation.run_join import build_connection, run_spatial_join  # noqa: E402
from src.task5_further_transformations.anonymise import build_anonymised_tables  # noqa: E402
from src.task5_further_transformations.augment_wind import (  # noqa: E402
    build_augmented_output,
    parse_wind_data,
)
from src.common.config import WIND_FIXTURE_PATH  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


def test_end_to_end_join_wind_anonymise_on_fixtures(tmp_path):
    con = build_connection()
    run_spatial_join(
        con,
        sr_path=FIXTURES / "sr_tiny.csv",
        hex_path=FIXTURES / "hex_tiny.geojson",
    )
    wind_hourly = parse_wind_data(WIND_FIXTURE_PATH)
    build_augmented_output(
        con, wind_hourly, subsample_path=FIXTURES / "atlantis_tiny.csv"
    )
    n_with_wind = con.execute(
        "SELECT COUNT(*) FROM atlantis_wind WHERE wind_data_available"
    ).fetchone()[0]
    assert n_with_wind == 6, "every tiny Atlantis row should match a fixture wind hour"

    wind_csv = tmp_path / "atlantis_wind.csv"
    con.execute(f"COPY atlantis_wind TO '{wind_csv.as_posix()}' (HEADER, DELIMITER ',')")

    build_anonymised_tables(con, source_path=wind_csv)
    release_cols = [r[0] for r in con.execute("DESCRIBE anonymised_release").fetchall()]
    assert release_cols == ANONYMISATION_RELEASE_COLUMNS
    for leaked in ANONYMISATION_DROP_FROM_RELEASE:
        assert leaked not in release_cols

    n_released = con.execute("SELECT COUNT(*) FROM anonymised_release").fetchone()[0]
    n_review = con.execute("SELECT COUNT(*) FROM for_human_review").fetchone()[0]
    # Five of six tiny rows share hex 88ad360001fffff and the same 6-hour
    # UTC bucket (08:00 UTC on 2020-01-01), so k=5 releases them; N6 is alone.
    assert n_released == 5
    assert n_review == 1

    # The five released rows span three different hours (and three different
    # hourly wind readings) inside one 6-hour bucket. The public wind columns
    # must collapse to a single station summary, or the hourly value would
    # split the k-group and be matchable back to the hour.
    wind_variants = con.execute(
        """
        SELECT COUNT(DISTINCT wind_speed), COUNT(DISTINCT wind_direction),
               COUNT(DISTINCT wind_data_available)
        FROM anonymised_release
        WHERE h3_level8_index = '88ad360001fffff'
        """
    ).fetchone()
    assert wind_variants == (1, 1, 1)
    speed = con.execute(
        "SELECT wind_speed FROM anonymised_release WHERE h3_level8_index = '88ad360001fffff' LIMIT 1"
    ).fetchone()[0]
    assert speed == 4.0  # unweighted mean of the three distinct hours: (4+6+2)/3
