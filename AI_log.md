# AI assistance log

I used two tools. I talked through the plan with ChatGPT before writing code. I then built the pipeline in Cursor, using a mix of Claude medium thinking and Grok high thinking. I decided the structure, the checks, and what to keep. The models drafted and edited code from my instructions.

## Token usage

I did not record Claude Sonnet token usage.

Grok usage is from my Cursor usage list for `cursor-grok-4.6-medium-fast`. Those requests add up to **2,645,600 tokens**.

## Planning

**Tool:** ChatGPT (OpenAI)
**Model:** GPT-6

I used this to think through how I would approach the assessment: what the brief was actually asking for, how I would keep the work reproducible, and how I would handle the awkward parts (a dead download, a privacy release). I checked anything I took from that conversation against the brief myself.

**What I was applying for.** I am submitting for Data Engineering, so I only did those tasks: setup, S3 Select, the hex join, and the Atlantis transformations. I left the analyst, science, and visualisation tasks alone.

**Languages.** The brief allows Python, R, and SQL. I stuck to Python for the scripts and DuckDB for the SQL. I did not use R.

**How someone else runs it.** I wanted one command. Dependencies go in `requirements.txt`. Large files download themselves. Imports stay at the top. Each step logs how long it took and whether the checks passed. That command is `python run_pipeline.py`, and it runs the tests before it touches the data.

**The wind file.** The brief says that endpoint is unreliable. I did not want a missing spreadsheet to throw away an otherwise good subsample, so I retry, then fall back to a small `.ods` I committed, and if even that is missing I still write the rows with `wind_data_available` set to false.

**Anonymisation.** Rounding location to about 500 m and time to 6 hours is not enough on its own in a small extract. I treat hex and time bucket together, hold groups smaller than 5 for a person to review, and I take suburb and service type off the public file. I also stopped publishing the hourly wind reading, because someone with the public wind series could match it back to the hour.

## Code

**Tool:** Cursor
**Models:** Claude medium thinking and Grok high thinking

I told Cursor to implement the Data Engineering tasks in Python and DuckDB, with no R, in `pyenv` env `env311`. It wrote the scripts I asked for: `download_data.py`, `run_extraction.py`, `run_join.py`, `subsample_atlantis.py`, `augment_wind.py`, `anonymise.py`, `run_pipeline.py`, the tests, and the README. I chose the layout and I decided which result was good enough.

I ran every draft. A few times the code looked fine and did not crash, and was still wrong. I saw that in the output and told it what to change.

### The spatial join hung

The first join put `lat IS NOT NULL AND lon IS NOT NULL AND ST_Within(...)` in one `ON` clause. A tiny example was fine. The full file never finished. DuckDB only uses the R-tree when the join is a plain `ST_Within`. The extra conditions made it test every point against every hex.

I had it split the null coordinates off, join the rest, and `UNION ALL` them back. That brought the full join down to about 3.5 seconds. I later also had it collapse a point that sits in two overlapping hexes to a single row.

### The schema rejected every hex

The first index pattern was `^88[0-9a-f]{11}fffff$`. I counted a real index, `88ad36958bfffff`, and it is 15 characters, not 18. Conformance came back as 0. I changed the pattern to `^88[0-9a-f]{8}fffff$`. The next run scored 1.0 and matched `city-hex-polygons-8.geojson` exactly.

### The 6-hour buckets were not buckets

The first floor was `(epoch(ts) // (6*3600)) * (6*3600)`. `epoch()` comes back as a double, and in DuckDB `//` on a double is ordinary division, not a floor. The "bucket" was the original timestamp. I noticed because I had about 8,121 different buckets for 8,122 rows.

I had it cast `epoch()` to an integer first. The anonymisation tests call that same SQL, including a regression test, so this does not slip back in.

### Hourly wind gave the hour away

Joining wind on the hour the request was created is the right join for task 5.2. Putting that hourly speed on the public file was not. Rows in the same 6-hour cell could show different speeds, which breaks the group, and the City publishes the wind series, so the speed can be looked up to the hour.

I changed the public file to one station summary per 6-hour bucket: mean speed and a circular mean of direction. The hourly values stay on the internal review file.

## What I kept for myself

I did not hand the design to the models. I used them to write and revise code against a plan I had already made. If a suggestion still made sense after I ran it, I kept it.

**How the repo is put together.** I laid this out before the files were filled in. One package per task under `src/`. Shared config and logging in `src/common/`. The hex schema in `config/`. Tests next to small fixtures. Big downloads stay out of git. A few sample rows live in `samples/` so someone can see the shape without a full run. `run_pipeline.py` runs the tests, then the tasks in order.

Task 1 filters in S3 instead of downloading 108 MB. Task 2 is a spatial join that stops if too many located rows miss a hex. Task 5 works out the Atlantis centroid, treats wind as optional, and only publishes a coarsened file. Steps that the rest of the pipeline depends on fail loudly. The wind join does not.

**The cutoffs.** I picked these, not the model. A conformance score rather than pass or fail. A 1% join-failure limit. One arc-minute as 1,852 m. k = 5. Suburb and service type come off the public file. Hourly wind stays off it too, for the reason above.

**Catching the mistakes.** I found the four problems above by reading timings, counts, and a few rows. The model did not flag them. The join that would not finish, the schema that scored zero, the time bucket that was still the exact timestamp, and the wind value that leaked the hour. I then said what to do: filter before `ST_Within`, count a real H3 index, cast `epoch()` to an integer, and publish one wind figure per 6-hour bucket.

**Tools.** I chose Python 3.11 in `pyenv` env `env311`, DuckDB for the SQL, boto3 for S3 Select, and pytest. No R. I also chose what goes in the repo: the code, the fixtures, and the samples. The large downloads stay out. The review file is for me to look at, not to publish.
