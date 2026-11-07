"""
Task 1 - Data Extraction (Data Engineering).

Uses AWS S3 Select to read only the resolution-8 features out of the large
``city-hex-polygons-8-10.geojson`` object (which also contains resolution 9
and 10 data), without downloading or parsing the whole ~108 MB file.

The extracted features are:
    1. validated against the (small, authoritative) ``city-hex-polygons-8.geojson``
       file - for an exact match on the set of H3 indices and on each
       polygon's geometry;
    2. scored for conformance against ``config/hex_polygon_schema.json``, a
       standalone JSON Schema describing what a "reasonable" record should
       look like. The conformance score is the fraction of extracted
       records that validate cleanly against that schema. This is
       deliberately *not* a pass/fail boolean: a single malformed record
       out of 3,832 is a very different situation to a systemic problem
       affecting 50% of records, and the score communicates that.

Usage
-----
    python -m src.task1_extraction.run_extraction
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path
from typing import Any, Iterator

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import jsonschema  # noqa: E402

from src.common.aws_client import get_s3_client  # noqa: E402
from src.common.config import (  # noqa: E402
    HEX8_CONFORMANCE_THRESHOLD,
    HEX8_SCHEMA_PATH,
    HEX8_SELECT_OUTPUT,
    RAW_FILES,
    S3_BUCKET,
)
from src.common.logging_utils import get_logger  # noqa: E402
from src.common.timing import timer  # noqa: E402

logger = get_logger("task1_extraction")

SOURCE_KEY = "city-hex-polygons-8-10.geojson"
VALIDATION_KEY_LOCAL = RAW_FILES["city-hex-polygons-8.geojson"]

# S3 Select SQL. The file is a single JSON *document* (a FeatureCollection),
# not JSON-Lines, so InputSerialization Type must be DOCUMENT. The `[*]`
# wildcard flattens the `features` array into one "row" per GeoJSON Feature,
# and the WHERE clause pushes the resolution-8 filter down into S3 itself so
# we never pay to transfer resolution 9/10 rows.
SELECT_SQL = (
    "SELECT s.properties.\"index\" AS idx, "
    "s.properties.centroid_lat AS centroid_lat, "
    "s.properties.centroid_lon AS centroid_lon, "
    "s.properties.resolution AS resolution, "
    "s.geometry AS geometry "
    "FROM S3Object[*].features[*] s "
    "WHERE s.properties.resolution = 8"
)


def run_s3_select() -> Iterator[dict[str, Any]]:
    """Yield one dict per resolution-8 feature, streamed from S3 Select."""
    client = get_s3_client()
    response = client.select_object_content(
        Bucket=S3_BUCKET,
        Key=SOURCE_KEY,
        ExpressionType="SQL",
        Expression=SELECT_SQL,
        InputSerialization={"JSON": {"Type": "DOCUMENT"}},
        OutputSerialization={"JSON": {"RecordDelimiter": "\n"}},
    )

    buffer = ""
    stats = {}
    for event in response["Payload"]:
        if "Records" in event:
            buffer += event["Records"]["Payload"].decode("utf-8")
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                if line.strip():
                    yield json.loads(line)
        elif "Stats" in event:
            stats = event["Stats"]["Details"]
        elif "End" in event:
            logger.info(
                "S3 Select scan stats: %s MB scanned, %s MB returned",
                round(int(stats.get("BytesScanned", 0)) / 1e6, 2),
                round(int(stats.get("BytesReturned", 0)) / 1e6, 2),
            )
    if buffer.strip():
        yield json.loads(buffer)


def record_to_feature(record: dict[str, Any]) -> dict[str, Any]:
    """Re-assemble an S3 Select output row back into a standard GeoJSON Feature."""
    return {
        "type": "Feature",
        "properties": {
            "index": record["idx"],
            "centroid_lat": record.get("centroid_lat"),
            "centroid_lon": record.get("centroid_lon"),
            "resolution": record.get("resolution"),
        },
        "geometry": record["geometry"],
    }


def validate_schema_conformance(features: list[dict[str, Any]]) -> float:
    """Score the extracted features against config/hex_polygon_schema.json.

    Returns the fraction (0-1) of features that validate cleanly. Every
    failure is logged (with the specific validation error) so a human can
    triage exactly what is non-conformant, rather than just seeing a count.
    """
    schema = json.loads(HEX8_SCHEMA_PATH.read_text())
    validator = jsonschema.Draft7Validator(schema)

    n_failed = 0
    for feature in features:
        errors = sorted(validator.iter_errors(feature), key=lambda e: e.path)
        if errors:
            n_failed += 1
            idx = feature.get("properties", {}).get("index", "<unknown>")
            for err in errors:
                logger.warning(
                    "SCHEMA NON-CONFORMANCE idx=%s path=%s message=%s",
                    idx,
                    list(err.path),
                    err.message,
                )

    n_total = len(features)
    score = (n_total - n_failed) / n_total if n_total else 0.0
    logger.info(
        "Schema conformance: %d/%d records conformant (score=%.4f, threshold=%.4f)",
        n_total - n_failed,
        n_total,
        score,
        HEX8_CONFORMANCE_THRESHOLD,
    )
    return score


def validate_against_reference(features: list[dict[str, Any]]) -> None:
    """Cross-check the S3-Select extraction against city-hex-polygons-8.geojson.

    This is the "ground truth" file referenced in the task description. We
    check (a) the set of H3 indices match exactly and (b) every polygon's
    coordinate ring matches exactly (geometry equality, not just presence).
    """
    reference = json.loads(VALIDATION_KEY_LOCAL.read_text())
    ref_by_index = {f["properties"]["index"]: f for f in reference["features"]}

    extracted_indices = {f["properties"]["index"] for f in features}
    reference_indices = set(ref_by_index.keys())

    missing = reference_indices - extracted_indices
    unexpected = extracted_indices - reference_indices
    if missing:
        logger.error("%d hex indices present in reference but missing from S3 Select extraction", len(missing))
    if unexpected:
        logger.error("%d hex indices present in extraction but absent from reference file", len(unexpected))

    geometry_mismatches = 0
    for feature in features:
        idx = feature["properties"]["index"]
        ref_feature = ref_by_index.get(idx)
        if ref_feature is None:
            continue
        if feature["geometry"]["coordinates"] != ref_feature["geometry"]["coordinates"]:
            geometry_mismatches += 1
            logger.warning("GEOMETRY MISMATCH for hex index %s", idx)

    logger.info(
        "Reference validation: %d/%d indices match, %d geometry mismatches",
        len(extracted_indices & reference_indices),
        len(reference_indices),
        geometry_mismatches,
    )

    if missing or unexpected or geometry_mismatches:
        raise AssertionError(
            f"Extraction does not match reference file: "
            f"{len(missing)} missing, {len(unexpected)} unexpected, "
            f"{geometry_mismatches} geometry mismatches"
        )
    logger.info("Extraction matches city-hex-polygons-8.geojson exactly.")


def main() -> None:
    with timer("S3 Select extraction of resolution-8 features", logger):
        start = time.perf_counter()
        records = list(run_s3_select())
        elapsed = time.perf_counter() - start
        logger.info("Retrieved %d records in %.3fs via S3 Select", len(records), elapsed)

    features = [record_to_feature(r) for r in records]

    with timer("write extracted FeatureCollection", logger):
        feature_collection = {"type": "FeatureCollection", "features": features}
        HEX8_SELECT_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
        HEX8_SELECT_OUTPUT.write_text(json.dumps(feature_collection))
        logger.info("Wrote %s", HEX8_SELECT_OUTPUT)

    with timer("schema conformance validation", logger):
        score = validate_schema_conformance(features)

    with timer("reference-file validation", logger):
        validate_against_reference(features)

    if score < HEX8_CONFORMANCE_THRESHOLD:
        raise SystemExit(
            f"Schema conformance score {score:.4f} is below the "
            f"required threshold {HEX8_CONFORMANCE_THRESHOLD:.4f}"
        )

    logger.info("Task 1 completed successfully.")


if __name__ == "__main__":
    main()
