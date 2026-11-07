<img src="img/city_emblem.png" alt="City Logo" width="120"/>

# City of Cape Town DS Code Challenge - Data Engineering Submission

This repository addresses the **Data Engineering** track of the
[`cityofcapetown/ds_code_challenge`](https://github.com/cityofcapetown/ds_code_challenge)
assessment (original brief preserved at [`CHALLENGE_BRIEF.md`](CHALLENGE_BRIEF.md)).

Tasks completed: **0 (Setup)**, **1 (Data Extraction)**, **2 (Initial Data
Transformation)** and **5 (Further Data Transformations)** - i.e. every task
explicitly marked "if applying for a Data Engineering Position". Tasks 3, 4
and 6 (Data Analyst / Data Scientist / Visualisation Engineer) are out of
scope for this submission.

**Constraints followed:** Python + SQL only (no R); all SQL is executed
through [DuckDB](https://duckdb.org/); developed and run under
`pyenv activate env311` (Python 3.11.3).

## TL;DR - how to run it

```bash
pyenv activate env311
pip install -r requirements.txt
python run_pipeline.py
```

That's the **entire** interface. `run_pipeline.py` first runs `pytest`
(fail-fast if tests fail), then downloads all raw data (idempotently),
then runs tasks 1, 2, 5.1, 5.2 and 5.3.
A full run takes well under a minute on a warm cache (raw downloads add
another ~30-60s the first time). Every step logs its own timings to both
the console and a timestamped file in `logs/`.

Each task can also be run individually, e.g.:

```bash
python -m src.task1_extraction.run_extraction
python -m src.task2_transformation.run_join
python -m src.task5_further_transformations.subsample_atlantis
python -m src.task5_further_transformations.augment_wind
python -m src.task5_further_transformations.anonymise
```

Unit tests (fast, no network, no large downloads required):

```bash
python -m pytest tests/ -v
```

## Repository layout

```
run_pipeline.py                     # single entry point: pytest, then every task
requirements.txt
samples/                            # tiny committed slices of outputs + a wind-join log excerpt
tests/fixtures/                     # tiny CSV/GeoJSON + wind .ods used by tests (and as wind fallback)
config/
  hex_polygon_schema.json           # standalone schema used by task 1's conformance check
src/
  common/                           # shared config, logging, timing, AWS-client helpers
  task0_setup/download_data.py      # downloads all raw datasets from S3
  task1_extraction/run_extraction.py        # Task 1 - S3 Select
  task2_transformation/run_join.py          # Task 2 - spatial join
  task5_further_transformations/
    subsample_atlantis.py           # Task 5.1 - Atlantis subsample
    augment_wind.py                 # Task 5.2 - wind-data augmentation
    anonymise.py                    # Task 5.3 - anonymisation
tests/                              # pytest unit tests for the SQL/validation logic
data/
  raw/        # downloaded source files (gitignored, re-created by task 0)
  interim/    # intermediate outputs between the 5.x sub-steps
  processed/  # final deliverables of each task
  external/   # cached third-party lookups (Nominatim boundary, wind file)
logs/         # timestamped execution logs (one file per script run)
CHALLENGE_BRIEF.md                  # the original, unmodified challenge README
AI_log.md                           # log of AI-assisted work on this submission
```

## Task 0 - Setup

`src/task0_setup/download_data.py` downloads every raw file listed in the
challenge brief from the public `cct-ds-code-challenge-input-data` S3
bucket into `data/raw/`, plus the dummy credentials file. Re-running is
cheap: a file already on disk with a size matching the remote
`Content-Length` is skipped.

## Task 1 - Data Extraction

**Script:** `src/task1_extraction/run_extraction.py`

Uses **AWS S3 Select** (`boto3`'s `select_object_content`) to read only the
resolution-8 features out of `city-hex-polygons-8-10.geojson` (a 108 MB
file containing resolutions 8, 9 *and* 10), without ever downloading or
parsing the whole object:

```sql
SELECT s.properties."index" AS idx, s.properties.centroid_lat AS centroid_lat,
       s.properties.centroid_lon AS centroid_lon, s.properties.resolution AS resolution,
       s.geometry AS geometry
FROM S3Object[*].features[*] s
WHERE s.properties.resolution = 8
```

The file is a single JSON *document* (a GeoJSON `FeatureCollection`), not
JSON-Lines, so `InputSerialization` uses `JSON.Type = DOCUMENT` with the
`[*]` wildcard to flatten the `features` array - this pushes the filter
down into S3 itself, so only the ~1.9 MB of matching data crosses the wire
instead of all 108 MB.

**Validation (two independent checks):**

1. **Reference-file check** - the extracted features are compared index-by-
   index and geometry-by-geometry against the authoritative
   `city-hex-polygons-8.geojson`. Result: **3,832/3,832 indices match, 0
   geometry mismatches** - an exact match.
2. **Schema conformance score** - every extracted feature is validated
   against a standalone JSON Schema at
   [`config/hex_polygon_schema.json`](config/hex_polygon_schema.json),
   which encodes what a "reasonable" resolution-8 hex feature should look
   like: a 15-character lower-case hex `index` matching the H3 resolution-8
   pattern (`^88[0-9a-f]{8}fffff$`), `resolution == 8`, a centroid inside a
   loose Cape Town bounding box, and a `Polygon` geometry with 6-8 ring
   vertices. The **conformance score** is the fraction of records that
   validate cleanly (not a pass/fail boolean) - this run scores
   **1.0000 (3,832/3,832)**. A score below `HEX8_CONFORMANCE_THRESHOLD`
   (**0.98**, in `src/common/config.py`) fails the script. 98% was chosen
   because this schema describes *structural* correctness (is the index
   well-formed, is the geometry sane) rather than business-rule nuance - a
   handful of edge-case records (e.g. a pentagon cell, which legitimately
   has 6 vertices instead of 7) shouldn't block a pipeline run, but a
   double-digit failure rate would indicate something is systemically wrong
   with the extraction and should stop the pipeline.

**Timing:** logged for every step (S3 Select call, schema validation,
reference validation); the S3 Select call itself takes ~3-4s end-to-end,
scanning 108 MB server-side and returning ~1.9 MB.

## Task 2 - Initial Data Transformation

**Script:** `src/task2_transformation/run_join.py`

Performs a point-in-polygon spatial join of every row in `sr.csv.gz`
(≈941k requests) against the resolution-8 hex polygons, entirely in SQL via
DuckDB's [`spatial`](https://duckdb.org/docs/extensions/spatial) extension
(`ST_Within`), accelerated with an **R-Tree index** over the 3,832
polygons.

* Requests with a missing `Latitude`/`Longitude` are routed straight to hex
  index `"0"` - they never touch the spatial join at all.
* **Performance note (a real issue hit during development, documented in
  the code):** a naive `LEFT JOIN ... ON lat IS NOT NULL AND lon IS NOT
  NULL AND ST_Within(...)` defeats DuckDB's spatial-join optimiser (which
  only rewrites a *clean* `ST_Within` join predicate to use the R-Tree) and
  silently falls back to an O(n·m) nested-loop scan - ~3.6 billion
  point-in-polygon tests, which never finished in a reasonable time. The
  fix was to pre-filter null-coordinate rows into a separate branch
  *before* the join and `UNION ALL` the two branches back together. This
  brought the full join down to **~3.5 seconds**.

**Validation (three layers):**

1. **Join-failure-rate check**: of the 729,270 geolocated requests, only
   **3 (0.0004%)** failed to land in any hex. This is far below
   `JOIN_ERROR_THRESHOLD = 1%` (`src/common/config.py`), chosen because a
   handful of points sitting exactly on a hex boundary (a legitimate
   floating-point edge case) is expected and harmless, whereas a failure
   rate in the single-digit percent range would suggest a systemic issue
   (e.g. swapped lat/lon, a CRS mismatch, or stale polygons) worth stopping
   the pipeline for.
2. **Independent cross-check**: a sample of joined rows is independently
   re-computed using the `h3` Python library's `latlng_to_cell` function,
   which derives the H3 index analytically from the point - no polygon data
   involved at all. Agreement here is strong evidence the spatial join
   logic itself is correct, not just internally consistent. Result on a
   full run: **15,468/15,470 (99.987%) agree**.
3. **Reference-file check** against the City-provided `sr_hex.csv.gz`:
   **941,605/941,634 (99.997%) match exactly.** The 29 mismatches were
   inspected individually - all but one sit on shared hex-polygon edges
   (both the City's and this join's `ST_Within` call are internally
   consistent but differ at machine-precision boundaries, which is an
   inherent limitation of floating-point point-in-polygon tests, not a
   join bug); the single true miss is one record resolving to hex `0` in
   this run vs. an assigned hex in the reference, also right on a boundary.

Output: `data/processed/sr_hex_joined.csv.gz` (functionally equivalent to
the City's `sr_hex.csv.gz`).

## Task 5 - Further Data Transformations

### 5.1 - Atlantis subsample

**Script:** `src/task5_further_transformations/subsample_atlantis.py`

"Atlantis" itself is an official City of Cape Town suburb (well to the
North of the city centre, matching the brief exactly), so there was no need
to guess at a cluster of "nearby" suburb names.

**Method (fully programmatic, nothing hard-coded):**
1. Download the suburb's administrative boundary polygon from
   OpenStreetMap via the Nominatim search API (`polygon_geojson=1`).
2. Compute the **geometric centroid of that boundary polygon** with
   Shapely (`.centroid`) - a computational method, not Nominatim's own
   (unspecified) representative point.
3. Filter `sr_hex.csv.gz` to rows within **1 arc-minute** of that centroid,
   computed as a proper geodesic (haversine) distance in SQL. By
   definition, 1 arc-minute of latitude = 1 nautical mile = **1,852 m**,
   which is the radius used.

Both the fetched boundary and the computed centroid are cached under
`data/external/`, so repeat runs don't re-hit Nominatim.

Result: **8,122 / 941,634 requests (0.86%)** fall within 1,852 m of the
computed centroid `(-33.5772, 18.4869)`.

### 5.2 - Wind-data augmentation

**Script:** `src/task5_further_transformations/augment_wind.py`

Joins the task 5.1 subsample to hourly wind direction/speed readings from
the Atlantis Air Quality Measurement site for 2020, matched on the hour a
request was created.

**Handling the (genuinely, currently) unreliable dependency:** the
documented download URL now returns **HTTP 404**, and the domain it
redirects to (`cityapps.capetown.gov.za`) **no longer resolves in DNS at
all** - the City migrated its Open Data Portal to a new ArcGIS-based
platform since this challenge was written, and this specific legacy file
appears to no longer be published anywhere reachable (confirmed via direct
requests, DNS lookup, and the Wayback Machine's CDX index, which has no
snapshot of this URL).

The script's strategy, in order:
1. Try the documented URL, with retries (exponential backoff + jitter) to
   absorb transient failures, and a second candidate host.
2. Fall back to a locally cached copy from a previous successful run.
3. If both fail (as is currently the case) - **degrade gracefully rather
   than hard-failing the whole pipeline.** The output is still produced,
   every row is kept, and the row is explicitly marked
   `wind_data_available = False` with the wind columns left `NULL`. A
   prominent, unambiguous `WARNING` is logged explaining why.

**Why graceful degradation here, but a hard error threshold in task 2?**
The wind join is a *best-effort enrichment* of an already-complete,
already-validated dataset - failing the whole task because an optional,
secondary data source is down would throw away correct, useful output for
no benefit. A consumer is far better served by a dataset that's honestly
missing one optional field (and says so explicitly) than by nothing at
all. Task 2's hex join, by contrast, is *load-bearing* - a badly joined
`h3_level8_index` silently corrupts every downstream analysis - so that's
exactly the situation where a hard failure threshold is the right call.

After the live URLs fail, the script falls back to a **committed fixture**
(`tests/fixtures/wind_atlantis_2020.ods`) — a small, realistic `.ods` with
a title block plus Date/Speed/Direction columns — so the parse and hourly
join are still demonstrated. Coverage against the real Atlantis subsample
is only a handful of rows (the fixture has four hours); that is expected.
A reviewer can see matched vs unmatched flags in `samples/` without
re-running. Tests parse this same file, so the join is proven even when
the City portal is down.

The parsing logic (`parse_wind_data`) is written defensively (it scans for
a header row and matches column names containing "date"/"time",
"speed", "direction" rather than hard-coding exact column letters). Naive
spreadsheet timestamps are treated as Africa/Johannesburg.

### 5.3 - Anonymisation

**Script:** `src/task5_further_transformations/anonymise.py`

Starting from the task 5.2 output, the **public** release
(`sr_hex_atlantis_anonymised.csv`) keeps only:

`h3_level8_index`, `creation_time_bucket`, `completion_time_bucket`,
`wind_speed`, `wind_direction`, `wind_data_available`
(the wind fields are the **6-hour station summary**, not the hourly reading).

* **Drops direct identifiers** (`reference_number`, `notification_number`).
* **Drops remaining quasi-identifiers** that would still identify a
  resident in a 20-hex Atlantis extract even after 500 m / 6-hour
  coarsening: `official_suburb`, `directorate`, `department`, `branch`,
  `section`, `code_group`, `code`, `cause_code_group`, `cause_code`, and
  `distance_to_centroid_m` (a 1-D location relative to a published point).
* **Generalises location to ~500 m** via H3 resolution 8 (~461 m edge).
* **Generalises time to 6 hours**, UTC-aligned, via integer epoch flooring.
  (*Regression: DuckDB `//` on DOUBLE is float division; without
  `CAST(... AS BIGINT)` the "bucket" equalled the original timestamp.*)
* **Wind is coarsened to the same 6-hour bucket** before release: mean
  speed and circular-mean direction of the distinct hours in that bucket,
  identical on every row in the bucket. The hourly reading stays on the
  internal review file only.
* **k-anonymity (k=5)** on `(h3_level8_index, creation_time_bucket)`.
  Smaller groups go to `sr_hex_atlantis_anonymised_for_review.csv`.

**That review file is internal-only — not a public deliverable.** It keeps
original IDs, coordinates, timestamps, suburb and service type so a human
can finish anonymisation by hand. Every run also writes
`sr_hex_atlantis_anonymised_for_review.INTERNAL_ONLY.txt` next to it.
Do not copy the review CSV when sharing outputs.

On this dataset: **2,375/8,122 (29%) released automatically; 5,747/8,122
(71%) routed to manual review.**

**Anonymisation justification (< 500 words)**

The public release drops case numbers, exact coordinates (replaced by a
~461 m H3 cell), exact timestamps (6-hour UTC buckets), suburb,
organisational geography, service-type codes, and distance to the Atlantis
centroid. A 500 m hex and a 6-hour window are reasonable on their own.
Together they are not.

Location and time form a quasi-identifier larger than the dataset. A year
has `365 × 4 = 1,460` six-hour buckets; this extract spans 20 hexes, so
about 29,200 possible cells against 8,122 records. Most cells hold one
record or none. A lone record is as identifiable as the raw row: anyone
who knows "a leak was reported in this part of Atlantis that afternoon"
can pick it out.

The pipeline counts each (hex, 6-hour-bucket) group and holds back groups
smaller than `k = 5`. That threshold is a common floor in public
k-anonymity releases (census and health microdata use similar cutoffs).
Lower, and near-unique rows go out. Higher, and rows that were already
safe are discarded. Five is the conventional middle.

Wind stays, but not hourly. The Atlantis sensor is one station, so a
reading does not point at a household. An hourly value inside a 6-hour
cell still would: rows in the same cell could show different speeds,
which splits the k-group, and the City wind series is public, so that
speed can be matched back to the hour and undo the time generalisation.
The public file therefore publishes one figure per bucket — mean speed
and circular-mean direction of the distinct hours in it — shared by
every row in the cell.

The 71% review rate is a finding about this extract, not a broken rule.
500 m and 6 hours do not anonymise a small, local, year-long file unless
most rows are reviewed by hand or the grid is coarsened further, which
would break the brief. The queue is shown on purpose. A reviewer can
apply context this script cannot (a hex that is only a shopping centre,
for example) and clear the rest by hand.

## Testing

`tests/` contains fast (no-network, sub-second) `pytest` unit tests that
exercise the SQL/validation *logic* directly against small synthetic
tables, rather than re-running the full pipeline against the real ~940k-row
dataset:

* `test_hex_schema.py` - the Task 1 JSON Schema correctly accepts a valid
  hex feature and rejects seven different kinds of malformed ones.
* `test_join_logic.py` - point-in-polygon match / miss / null-coordinate
  routing, plus **overlapping polygons must not duplicate requests**.
* `test_anonymisation.py` - UTC 6-hour flooring (including the float-`//`
  regression), k-anonymity routing, and the public-release column allowlist
  (no suburb / service type / IDs).
* `test_wind.py` - parse the committed `.ods` fixture, average sub-hourly
  readings, join hours, and the all-NULL degrade path.
* `test_integration_fixtures.py` - one path through join → wind →
  anonymise on tiny CSV + GeoJSON + `.ods` (no 941k-row files, no network).

`run_pipeline.py` runs this suite before any download or transform.

```bash
python -m pytest tests/ -v
# 27 passed
```

## Design notes / why these choices

* **DuckDB everywhere**: every transformation in this submission - the
  spatial join, the haversine distance filter, the time-bucket flooring,
  the k-anonymity grouping - is a single, readable SQL statement executed
  through DuckDB, run from Python. This keeps the "business logic" in SQL
  (where set-based transformations are usually clearest) while keeping
  orchestration, logging, retries and validation in Python (where
  imperative control flow, error handling and I/O belong). No R is used
  anywhere, per the constraints given for this submission.
* **One config module** (`src/common/config.py`): every path, S3 object
  key, URL and tunable threshold lives in one file with an inline comment
  justifying its value, instead of being scattered as magic numbers across
  scripts.
* **Idempotent, cacheable I/O everywhere**: raw downloads, the Nominatim
  boundary lookup, and the wind-data download all check for a local cache
  first, so re-running the pipeline (or any single script) is fast and
  doesn't hammer external services.
