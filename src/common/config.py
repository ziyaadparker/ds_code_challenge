"""
Central configuration for the Data Engineering submission.

Keeping every path, S3 object key and tunable threshold in one module means
none of the task scripts hard-code "magic" values, and the whole pipeline can
be re-pointed at a different bucket / region / local layout by editing a
single file.
"""
from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Project layout
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"
EXTERNAL_DIR = DATA_DIR / "external"

LOG_DIR = PROJECT_ROOT / "logs"
CONFIG_DIR = PROJECT_ROOT / "config"

for _dir in (RAW_DIR, INTERIM_DIR, PROCESSED_DIR, EXTERNAL_DIR, LOG_DIR, CONFIG_DIR):
    _dir.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# Source data (City of Cape Town Data Science code challenge)
# ---------------------------------------------------------------------------
S3_BUCKET = "cct-ds-code-challenge-input-data"
S3_REGION = "af-south-1"
S3_BASE_URL = f"https://{S3_BUCKET}.s3.{S3_REGION}.amazonaws.com"

CREDS_URL = f"{S3_BASE_URL}/ds_code_challenge_creds.json"
CREDS_PATH = RAW_DIR / "ds_code_challenge_creds.json"

# object_key -> local filename (kept identical for traceability)
RAW_FILES = {
    "sr.csv.gz": RAW_DIR / "sr.csv.gz",
    "sr_hex.csv.gz": RAW_DIR / "sr_hex.csv.gz",
    "sr_hex_truncated.csv": RAW_DIR / "sr_hex_truncated.csv",
    "city-hex-polygons-8.geojson": RAW_DIR / "city-hex-polygons-8.geojson",
    "city-hex-polygons-8-10.geojson": RAW_DIR / "city-hex-polygons-8-10.geojson",
}

WIND_DATA_URL = (
    "https://www.capetown.gov.za/_layouts/OpenDataPortalHandler/DownloadHandler.ashx"
    "?DocumentName=Wind_direction_and_speed_2020.ods"
    "&DatasetDocument=https%3A%2F%2Fcityapps.capetown.gov.za%2Fsites%2F"
    "opendatacatalog%2FDocuments%2FWind%2FWind_direction_and_speed_2020.ods"
)
WIND_DATA_PATH = EXTERNAL_DIR / "Wind_direction_and_speed_2020.ods"
# Committed stand-in used when the City's Open Data endpoint is unreachable.
# Lets Task 5.2 still demonstrate a real parse + hourly join, and is the
# fixture the unit/integration tests parse (so those tests never need the
# live portal).
WIND_FIXTURE_PATH = PROJECT_ROOT / "tests" / "fixtures" / "wind_atlantis_2020.ods"

# OpenStreetMap Nominatim, used (politely, with caching) to geocode the
# centroid of the Atlantis-area suburbs for task 5.1.
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = "cct-ds-code-challenge-submission/1.0 (data-engineering task)"

# ---------------------------------------------------------------------------
# Task 1 - S3 Select extraction
# ---------------------------------------------------------------------------
HEX8_SCHEMA_PATH = CONFIG_DIR / "hex_polygon_schema.json"
HEX8_SELECT_OUTPUT = INTERIM_DIR / "city-hex-polygons-8-from-s3-select.geojson"
# Schema conformance below this fraction (0-1) will make the extraction
# script exit non-zero. See config/hex_polygon_schema.json for the schema
# itself and README.md s1 for the rationale behind the number chosen.
HEX8_CONFORMANCE_THRESHOLD = 0.98

# ---------------------------------------------------------------------------
# Task 2 - service-request / hex join
# ---------------------------------------------------------------------------
SR_HEX_JOINED_OUTPUT = PROCESSED_DIR / "sr_hex_joined.csv.gz"
# Fraction of records (with a non-null lat/lon) that are allowed to fail to
# land in a resolution-8 hex before we consider the join broken and abort.
# See README.md s2 for the justification of this particular number.
JOIN_ERROR_THRESHOLD = 0.01

# ---------------------------------------------------------------------------
# Task 5 - further transformations
# ---------------------------------------------------------------------------
# The official suburb (OSM/City boundary) used as the target for task 5.1.
# "Atlantis" itself is an official City of Cape Town suburb, located well to
# the North of the city centre, which matches the task description exactly -
# no need to guess at a set of "nearby" suburbs.
ATLANTIS_SUBURB_NAME = "Atlantis"
ATLANTIS_NOMINATIM_QUERY = "Atlantis, City of Cape Town, Western Cape, South Africa"

# 1 arc-minute of latitude is, by definition, 1 nautical mile = 1852 m. We
# use that as the (geodesic, not planar) radius around the suburb centroid.
ATLANTIS_RADIUS_METERS = 1852.0

ATLANTIS_CENTROID_PATH = EXTERNAL_DIR / "atlantis_suburb_centroid.json"
ATLANTIS_BOUNDARY_PATH = EXTERNAL_DIR / "atlantis_suburb_boundary.geojson"
ATLANTIS_SUBSAMPLE_PATH = INTERIM_DIR / "sr_hex_atlantis_subsample.csv"
ATLANTIS_WIND_AUGMENTED_PATH = INTERIM_DIR / "sr_hex_atlantis_wind.csv"
ATLANTIS_ANONYMISED_PATH = PROCESSED_DIR / "sr_hex_atlantis_anonymised.csv"
ATLANTIS_ANONYMISED_REVIEW_PATH = PROCESSED_DIR / "sr_hex_atlantis_anonymised_for_review.csv"

# H3 resolution whose edge length is close to, but a little coarser than,
# 500m, used to coarsen point locations for the anonymisation task.
ANONYMISATION_H3_RESOLUTION = 8  # ~461m average hexagon edge length
ANONYMISATION_TIME_BUCKET_HOURS = 6

# Minimum group size, for records sharing the same (coarsened location, time
# bucket) quasi-identifier, below which a record is routed to manual human
# review rather than released automatically. k=5 is a widely used baseline
# in the k-anonymity literature (e.g. it's the floor commonly applied to US
# Census and health-data releases) - see README.md task 5.3 for the full
# rationale.
ANONYMISATION_K_THRESHOLD = 5

# Columns dropped from the *public* anonymised release (kept on the
# internal-only human-review file). See README.md task 5.3: suburb,
# organisational geography, and service-type codes remain identifying in a
# 20-hex Atlantis extract even after lat/lon and timestamps are coarsened.
# distance_to_centroid_m is dropped because it is a 1-D location relative
# to a published point, finer than the ~500 m requirement.
ANONYMISATION_DROP_FROM_RELEASE = [
    "notification_number",
    "reference_number",
    "latitude",
    "longitude",
    "creation_timestamp",
    "completion_timestamp",
    "official_suburb",
    "directorate",
    "department",
    "branch",
    "section",
    "code_group",
    "code",
    "cause_code_group",
    "cause_code",
    "distance_to_centroid_m",
]

# wind_* on the public file are the 6-hour station summary (see anonymise.py),
# not the hourly reading. Hourly wind stays on the internal review file only.
ANONYMISATION_RELEASE_COLUMNS = [
    "h3_level8_index",
    "creation_time_bucket",
    "completion_time_bucket",
    "wind_speed",
    "wind_direction",
    "wind_data_available",
]

REVIEW_FILE_NOTICE = (
    "INTERNAL USE ONLY — not for public release. This file retains original "
    "identifiers, exact coordinates, timestamps, suburb and service-type "
    "fields so a human can anonymise leftover k<5 records by hand. The "
    "public deliverable is sr_hex_atlantis_anonymised.csv."
)
