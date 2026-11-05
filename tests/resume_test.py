"""Manual check that fetch_pages resumes from pages on disk. Costs about 7 API requests.

Run from the project root:  python tests/resume_test.py
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

REQUEST_COUNT = 0
_real_github_get = main.github_get


def counting_github_get(*args, **kwargs):
    """Wrap main.github_get to count API requests."""
    global REQUEST_COUNT
    REQUEST_COUNT += 1
    return _real_github_get(*args, **kwargs)


main.github_get = counting_github_get


def fetch(out_dir: Path) -> tuple[list[dict], int]:
    """Run fetch_pages and return (records, requests used)."""
    global REQUEST_COUNT
    REQUEST_COUNT = 0
    records, _ = main.fetch_pages(
        "dask/dask", "commits", {"since": "2026-06-01T00:00:00Z"}, out_dir=out_dir
    )
    return records, REQUEST_COUNT


def test_resume() -> None:
    main.PER_PAGE = 10  # small pages force several landing files
    with tempfile.TemporaryDirectory() as tmp:
        out_dir = Path(tmp)

        # 1. Full fetch.
        full, n = fetch(out_dir)
        pages = len(list(out_dir.glob("page_*.json")))
        say(f"1. Full fetch:      {len(full)} records, {pages} pages, {n} requests")
        assert n == pages

        # 2. Simulate a crash: last 2 pages missing, one half-written temp file left behind.
        (out_dir / f"page_{pages:04d}.json").unlink()
        (out_dir / f"page_{pages - 1:04d}.json").unlink()
        (out_dir / f"page_{pages - 1:04d}.tmp").write_text('{"repo": "dask/da')
        say(f"2. Simulated crash: deleted pages {pages - 1}-{pages}, wrote a half-written .tmp")

        # 3. Resume: only the 2 missing pages are fetched.
        resumed, n = fetch(out_dir)
        say(f"3. Resume:          {len(resumed)} records, {n} requests")
        assert n == 2, f"Expected 2 requests on resume, got {n}"
        assert [r["sha"] for r in resumed] == [r["sha"] for r in full], "Resumed records differ"
        assert len(list(out_dir.glob("page_*.json"))) == pages
        assert not list(out_dir.glob("*.tmp")), "Leftover .tmp file"

        # 4. Rerun on a finished endpoint: nothing to fetch.
        again, n = fetch(out_dir)
        say(f"4. Rerun complete:  {len(again)} records, {n} requests")
        assert n == 0, f"Expected 0 requests on a finished endpoint, got {n}"
        assert len(again) == len(full)

        say("All resume checks passed")


if __name__ == "__main__":
    try:
        test_resume()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
