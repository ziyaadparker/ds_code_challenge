"""Unit tests for the Task 2 spatial-join SQL logic, using small synthetic
tables rather than the full ~940k row dataset, so these run in well under a
second."""
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.task2_transformation.run_join import build_connection, run_spatial_join  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture()
def con():
    connection = duckdb.connect()
    connection.execute("INSTALL spatial; LOAD spatial;")
    connection.execute(
        """
        CREATE TABLE hex8 AS
        SELECT 'test_hex_1' AS h3_index,
               ST_GeomFromText(
                   'POLYGON((18.0 -34.0, 19.0 -34.0, 19.0 -33.0, 18.0 -33.0, 18.0 -34.0))'
               ) AS geom
        """
    )
    return connection


def test_point_inside_polygon_matches(con):
    con.execute(
        "CREATE TABLE sr_geo AS SELECT 'A' AS id, -33.5 AS latitude, 18.5 AS longitude"
    )
    result = con.execute(
        """
        SELECT sr_geo.id, COALESCE(hex8.h3_index, '0') AS h3_level8_index
        FROM sr_geo
        LEFT JOIN hex8 ON ST_Within(ST_Point(sr_geo.longitude, sr_geo.latitude), hex8.geom)
        """
    ).fetchall()
    assert result == [("A", "test_hex_1")]


def test_point_outside_polygon_gets_zero(con):
    con.execute(
        "CREATE TABLE sr_geo AS SELECT 'B' AS id, 0.0 AS latitude, 0.0 AS longitude"
    )
    result = con.execute(
        """
        SELECT sr_geo.id, COALESCE(hex8.h3_index, '0') AS h3_level8_index
        FROM sr_geo
        LEFT JOIN hex8 ON ST_Within(ST_Point(sr_geo.longitude, sr_geo.latitude), hex8.geom)
        """
    ).fetchall()
    assert result == [("B", "0")]


def test_null_coordinates_are_routed_to_zero_without_spatial_join():
    """Mirrors run_join.py's `sr_no_geo` branch: null lat/lon never reach the
    spatial join at all, they're short-circuited to hex '0'."""
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE sr AS SELECT * FROM (VALUES ('C', NULL, NULL), ('D', 1.0, 1.0)) AS t(id, latitude, longitude)"
    )
    no_geo = con.execute(
        "SELECT id, '0' AS h3_level8_index FROM sr WHERE latitude IS NULL OR longitude IS NULL"
    ).fetchall()
    assert no_geo == [("C", "0")]


def test_overlapping_polygons_do_not_duplicate_requests():
    """hex_tiny.geojson has two identical polygons covering the same point.
    A naive LEFT JOIN ST_Within would emit two rows for N1/N2; the production
    join must collapse that to one row per request."""
    con = build_connection()
    run_spatial_join(
        con,
        sr_path=FIXTURES / "sr_tiny.csv",
        hex_path=FIXTURES / "hex_tiny.geojson",
    )
    rows = con.execute(
        """
        SELECT notification_number, h3_level8_index, COUNT(*) AS n
        FROM sr_hex_joined
        GROUP BY 1, 2
        ORDER BY 1
        """
    ).fetchall()
    counts = {n: c for n, _hex, c in rows}
    assert counts["N1"] == 1
    assert counts["N2"] == 1
    assert counts["N3"] == 1  # null coords -> hex 0
    assert counts["N4"] == 1  # outside both polygons -> hex 0

    total = con.execute("SELECT COUNT(*) FROM sr_hex_joined").fetchone()[0]
    distinct = con.execute(
        "SELECT COUNT(DISTINCT notification_number) FROM sr_hex_joined"
    ).fetchone()[0]
    assert total == distinct == 4

    n1_hex = con.execute(
        "SELECT h3_level8_index FROM sr_hex_joined WHERE notification_number = 'N1'"
    ).fetchone()[0]
    assert n1_hex in {"hex_a", "hex_b"}

    n3_hex = con.execute(
        "SELECT h3_level8_index FROM sr_hex_joined WHERE notification_number = 'N3'"
    ).fetchone()[0]
    assert n3_hex == "0"
