# INTERVIEW: GOAL OF THIS TEST: run the real extract subflow end to end on a one-week window. CONFIRMS: all four
# endpoint folders exist for all three repos (Task 1 'Required endpoints'), logs/extract.log is written (General
# 'Logging'), and a second run costs 0 requests because every page is already on disk (Task 1 'Resumability').
# CATCHES: a missing endpoint, or a resume that downloads pages again.
"""Run the real extract subflow on a one-week window into a temp folder. Costs about 50-150 API requests.

Run from the project root:  python tests/extract_flow_test.py
"""
import logging
import sys
import tempfile
from pathlib import Path

# INTERVIEW: The tests live in tests/ but main.py is one folder up, so the project root is added to Python's search
# path before `import main`. Importing main needs GH_PAT set, because main reads the token at import time.
# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

# INTERVIEW: Shared block, repeated in every test file (decision: no shared helper module, course style keeps code in
# single files). It saves this test's output to logs/test_logs/<file name>.log, opened with mode='w' so the file
# always holds the latest run and can be quoted as evidence in the README.
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


# INTERVIEW: Prints a result line and writes it to the test's log file, so what was on screen is also saved. propagate
# = False above stops each line from being printed a second time by the root logger.
def say(message: str) -> None:
    """Print a result line and save it to this test's log."""
    print(message)
    test_log.info(message)

TEST_WINDOW = "2026-10-25T00:00:00Z"
ENDPOINTS = {"commits", "pulls", "releases", "reviews"}


# INTERVIEW: Replaces main's path constants with a temp folder, so nothing lands in the real data/raw; main reads
# those constants when a function runs, so the swap takes effect. Load never runs here, so no _LOADED marker exists
# and the second extract is in resume mode, which is why it must cost 0 requests.
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


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_extract_flow()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
