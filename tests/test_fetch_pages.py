"""Manual check that fetch_pages follows Link pagination. Costs about 5 API requests.

Run from the project root:  python tests/test_fetch_pages.py
"""
import sys
import tempfile
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main


def test_fetch_pages() -> None:
    main.PER_PAGE = 10  # small pages force several Link rel="next" hops
    # Landing files go to a temp folder so test pages never mix with real data/raw/.
    with tempfile.TemporaryDirectory() as tmp, main.make_client() as client:
        records = main.fetch_pages(
            client, "dask/dask", "commits", {"since": "2026-06-01T00:00:00Z"}, out_dir=Path(tmp)
        )

    print(f"dask/dask commits since 2026-06-01: {len(records)} records")
    print("newest:", records[0]["sha"][:7], records[0]["commit"]["author"]["date"])
    print("oldest:", records[-1]["sha"][:7], records[-1]["commit"]["author"]["date"])
    assert len(records) > main.PER_PAGE, "Only one page returned; Link pagination did not continue"


if __name__ == "__main__":
    test_fetch_pages()
