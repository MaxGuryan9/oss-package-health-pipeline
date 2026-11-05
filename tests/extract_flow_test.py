"""Run the real extract subflow on a one-week window into a temp folder. Costs about 50-150 API requests.

Run from the project root:  python tests/extract_flow_test.py
"""
import logging
import sys
import tempfile
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

# Save this test's output to logs/test_logs/<this file's name>.log (overwritten each run, so the file always
# holds the latest run). Same handler pattern as main.py's per-task logs; main's task loggers also write here.
TEST_LOG_DIR = main.LOG_DIR / "test_logs"
TEST_LOG_DIR.mkdir(exist_ok=True)
test_handler = logging.FileHandler(TEST_LOG_DIR / f"{Path(__file__).stem}.log", mode="w")
test_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
for name in ["extract", "load", "transform", "validate", "model", "plot"]:
    logging.getLogger(name).addHandler(test_handler)
test_log = logging.getLogger(Path(__file__).stem)
test_log.setLevel(logging.INFO)
test_log.propagate = False  # say() already prints to the screen; don't print each line twice
test_log.addHandler(test_handler)


def say(message: str) -> None:
    """Print a result line and save it to this test's log."""
    print(message)
    test_log.info(message)

TEST_WINDOW = "2026-10-25T00:00:00Z"
ENDPOINTS = {"commits", "pulls", "releases", "reviews"}


def test_extract_flow() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Point data paths at the temp folder so nothing lands in the real data/raw.
        # Log lines go to the real logs/extract.log (its handler is attached at import).
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.DB_PATH = Path(tmp) / "test.duckdb"

        first_run = main.extract(window_start=TEST_WINDOW)

        for repo in main.REPOS:
            repo_dir = main.RAW_DIR / repo.replace("/", "__")
            found = {d.name for d in repo_dir.iterdir()}
            assert found == ENDPOINTS, f"{repo}: folders {sorted(found)}, expected {sorted(ENDPOINTS)}"
        assert (main.LOG_DIR / "extract.log").exists(), "logs/extract.log was not written"

        rerun = main.extract(window_start=TEST_WINDOW)

        say(f"\nFirst run: {first_run} requests. Rerun: {rerun} requests.")
        say(f"Log file lines: {len((main.LOG_DIR / 'extract.log').read_text().splitlines())}")
        assert rerun == 0, f"Expected 0 requests on rerun, got {rerun}"
        say("All extract flow checks passed")


if __name__ == "__main__":
    try:
        test_extract_flow()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
