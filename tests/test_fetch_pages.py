"""Manual check that fetch_pages follows Link pagination. Costs about 5 API requests.

Run from the project root:  python tests/test_fetch_pages.py
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


if __name__ == "__main__":
    try:
        test_fetch_pages()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
