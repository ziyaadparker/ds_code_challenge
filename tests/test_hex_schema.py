"""Unit tests for the Task 1 schema-conformance logic (no network required)."""
import json
import sys
from pathlib import Path

import jsonschema
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.common.config import HEX8_SCHEMA_PATH  # noqa: E402

SCHEMA = json.loads(HEX8_SCHEMA_PATH.read_text())

GOOD_FEATURE = {
    "type": "Feature",
    "properties": {
        "index": "88ad361801fffff",
        "centroid_lat": -33.859427322761434,
        "centroid_lon": 18.677843311941835,
        "resolution": 8,
    },
    "geometry": {
        "type": "Polygon",
        "coordinates": [[[18.1, -33.1], [18.2, -33.2], [18.3, -33.3], [18.4, -33.4],
                          [18.5, -33.5], [18.6, -33.6], [18.1, -33.1]]],
    },
}


@pytest.fixture()
def validator():
    return jsonschema.Draft7Validator(SCHEMA)


def test_good_feature_is_valid(validator):
    errors = list(validator.iter_errors(GOOD_FEATURE))
    assert errors == []


def test_wrong_resolution_is_invalid(validator):
    bad = json.loads(json.dumps(GOOD_FEATURE))
    bad["properties"]["resolution"] = 9
    errors = list(validator.iter_errors(bad))
    assert errors


@pytest.mark.parametrize(
    "bad_index",
    [
        "not-a-valid-index",
        "99ad361801fffff",  # wrong resolution-digit prefix
        "88ad361801ffff",  # too short
        "88AD361801FFFFF",  # must be lower-case
    ],
)
def test_malformed_index_is_invalid(validator, bad_index):
    bad = json.loads(json.dumps(GOOD_FEATURE))
    bad["properties"]["index"] = bad_index
    errors = list(validator.iter_errors(bad))
    assert errors


def test_out_of_bounds_centroid_is_invalid(validator):
    bad = json.loads(json.dumps(GOOD_FEATURE))
    bad["properties"]["centroid_lat"] = 51.5  # London, not Cape Town
    errors = list(validator.iter_errors(bad))
    assert errors


def test_missing_geometry_is_invalid(validator):
    bad = json.loads(json.dumps(GOOD_FEATURE))
    del bad["geometry"]
    errors = list(validator.iter_errors(bad))
    assert errors
