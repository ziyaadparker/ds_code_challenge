# Committed output samples

Full pipeline outputs live under `data/` (gitignored — they are large and
reproducible). This folder is a small committed slice so a reviewer can
see the public output shape and the wind-download log without running the
pipeline.

| File | What it shows |
| --- | --- |
| `sr_hex_atlantis_anonymised_sample.csv` | Public Task 5.3 release: hex, 6-hour buckets, and the 6-hour wind summary. No suburb, service type, IDs, or exact lat/lon/time. |
| `sr_hex_atlantis_anonymised_for_review.INTERNAL_ONLY.txt` | The notice the pipeline writes next to the review file. The review CSV itself is not in this folder. |
| `task5_augment_wind_log_excerpt.txt` | A live run: City wind URL 404, then DNS failure, then the committed fixture. |

Rows that still identify a request (notification number, exact coordinates, suburb, exact timestamp) are not committed. That includes the Task 2 join sample, the wind-join samples, and the k&lt;5 review queue.

The public file is `data/processed/sr_hex_atlantis_anonymised.csv` after `python run_pipeline.py`. The review file, `data/processed/sr_hex_atlantis_anonymised_for_review.csv`, is written beside it for a person to anonymise by hand. Both are gitignored. Do not publish the review file.
