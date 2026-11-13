# INTERVIEW: GOAL OF THIS TEST: the first and simplest check of Task 1 'Pagination'. CONFIRMS: with a page size of 10,
# fetch_pages returns more than 10 records, which is only possible if it followed the Link header to a second page.
# CATCHES: a fetch that stops after page 1. (Named before the <topic>_test.py convention was set, so its name differs
# from the other tests.)
"""Manual check that fetch_pages follows Link pagination. Costs about 5 API requests.

Run from the project root:  python tests/test_fetch_pages.py
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


# INTERVIEW: Landing files go to a temp folder so test pages never mix with real data. The newest and oldest records
# are printed to check by eye that results start at the present and reach back to the `since` date.
def test_fetch_pages() -> None:
    main.PER_PAGE = 10  # small pages force several Link rel="next" hops
    # Landing files go to a temp folder so test pages never mix with real data/raw/.
    with tempfile.TemporaryDirectory() as tmp:
        records, _ = main.fetch_pages(
            "dask/dask", "commits", {"since": "2026-06-01T00:00:00Z"}, out_dir=Path(tmp)
        )

    say(f"dask/dask commits since 2026-06-01: {len(records)} records")
    say(f"newest: {records[0]['sha'][:7]} {records[0]['commit']['author']['date']}")
    say(f"oldest: {records[-1]['sha'][:7]} {records[-1]['commit']['author']['date']}")
    assert len(records) > main.PER_PAGE, "Only one page returned; Link pagination did not continue"


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_fetch_pages()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
