"""Manual check that fetch_pages resumes from pages on disk. Costs about 7 API requests.

Run from the project root:  python tests/resume_test.py
"""
import sys
import tempfile
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

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
        print(f"1. Full fetch:      {len(full)} records, {pages} pages, {n} requests")
        assert n == pages

        # 2. Simulate a crash: last 2 pages missing, one half-written temp file left behind.
        (out_dir / f"page_{pages:04d}.json").unlink()
        (out_dir / f"page_{pages - 1:04d}.json").unlink()
        (out_dir / f"page_{pages - 1:04d}.tmp").write_text('{"repo": "dask/da')
        print(f"2. Simulated crash: deleted pages {pages - 1}-{pages}, wrote a half-written .tmp")

        # 3. Resume: only the 2 missing pages are fetched.
        resumed, n = fetch(out_dir)
        print(f"3. Resume:          {len(resumed)} records, {n} requests")
        assert n == 2, f"Expected 2 requests on resume, got {n}"
        assert [r["sha"] for r in resumed] == [r["sha"] for r in full], "Resumed records differ"
        assert len(list(out_dir.glob("page_*.json"))) == pages
        assert not list(out_dir.glob("*.tmp")), "Leftover .tmp file"

        # 4. Rerun on a finished endpoint: nothing to fetch.
        again, n = fetch(out_dir)
        print(f"4. Rerun complete:  {len(again)} records, {n} requests")
        assert n == 0, f"Expected 0 requests on a finished endpoint, got {n}"
        assert len(again) == len(full)

        print("All resume checks passed")


if __name__ == "__main__":
    test_resume()
