"""
Single entry point that runs every Data Engineering task end-to-end, in
order, with no human interaction required:

    tests (pytest)                     # fail-fast on logic/fixture tests
    0. Download raw datasets           (src/task0_setup)
    1. S3 Select extraction            (src/task1_extraction)
    2. sr <-> hex8 spatial join         (src/task2_transformation)
    5.1 Atlantis subsample              (src/task5_further_transformations)
    5.2 Wind-data augmentation          (src/task5_further_transformations)
    5.3 Anonymisation                   (src/task5_further_transformations)

Tasks 1 and 2 are "hard" steps: they validate their own output and will
raise/exit non-zero if a correctness threshold isn't met (see each module's
docstring), which stops the pipeline. Task 5.2's wind download is a "soft"
dependency: per its own docstring, if the external wind-data source is
unreachable it degrades gracefully rather than failing the run, so tasks
5.3 still has something to anonymise.

Usage
-----
    pyenv activate env311
    python run_pipeline.py
"""
from __future__ import annotations

import sys
import time

import pytest

from src.common.config import PROJECT_ROOT
from src.common.logging_utils import get_logger

logger = get_logger("run_pipeline")


def run_tests() -> None:
    tests_dir = str(PROJECT_ROOT / "tests")
    logger.info("RUNNING pytest %s", tests_dir)
    exit_code = pytest.main(["-q", tests_dir])
    if exit_code != 0:
        raise SystemExit(f"pytest failed with exit code {exit_code}")
    logger.info("pytest passed")


def main() -> None:
    steps = [
        ("Task 0 - download raw datasets", "src.task0_setup.download_data"),
        ("Task 1 - S3 Select extraction", "src.task1_extraction.run_extraction"),
        ("Task 2 - sr/hex8 spatial join", "src.task2_transformation.run_join"),
        ("Task 5.1 - Atlantis subsample", "src.task5_further_transformations.subsample_atlantis"),
        ("Task 5.2 - wind-data augmentation", "src.task5_further_transformations.augment_wind"),
        ("Task 5.3 - anonymisation", "src.task5_further_transformations.anonymise"),
    ]

    overall_start = time.perf_counter()

    logger.info("=" * 80)
    logger.info("RUNNING unit + fixture tests (pytest)")
    logger.info("=" * 80)
    run_tests()

    for description, module_name in steps:
        logger.info("=" * 80)
        logger.info("RUNNING %s (%s)", description, module_name)
        logger.info("=" * 80)
        step_start = time.perf_counter()

        module = __import__(module_name, fromlist=["main"])
        module.main()

        logger.info(
            "FINISHED %s in %.2fs", description, time.perf_counter() - step_start
        )

    logger.info(
        "Pipeline completed successfully in %.2fs total.",
        time.perf_counter() - overall_start,
    )


if __name__ == "__main__":
    try:
        main()
    except SystemExit as exc:
        logger.error("Pipeline aborted: %s", exc)
        sys.exit(1)
