"""Run extract -> load twice in a temp folder and check the second run is incremental. Costs about 100-120 API requests.

Run from the project root:  python tests/incremental_test.py
"""
import json
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

TEST_WINDOW = "2026-10-25T00:00:00Z"


def table_counts() -> dict:
    """Row count of every raw table in the test database."""
    counts = {}
    with duckdb.connect(main.DB_PATH, read_only=True) as con:
        for spec in main.LOAD_SPECS.values():
            counts[spec["table"]] = con.execute(f"SELECT count(*) FROM {spec['table']}").fetchall()[0][0]
    return counts


def test_incremental() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Point data paths at the temp folder so nothing touches the real data/ or packages.duckdb.
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.ARCHIVE_DIR = Path(tmp) / "data" / "archive"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.DB_PATH = Path(tmp) / "test.duckdb"

        # 1. Full run: no _load_state yet, so everything since TEST_WINDOW is fetched.
        first = main.extract(window_start=TEST_WINDOW)
        main.load()
        counts1 = table_counts()
        print(f"\n1. Full run:        {first} requests, {counts1}")

        # 2. Incremental run: _LOADED exists, so data/raw is archived and only records since the watermark are fetched.
        watermarks = main.read_watermarks()
        second = main.extract(window_start=TEST_WINDOW)
        main.load()
        counts2 = table_counts()
        print(f"2. Incremental run: {second} requests, {counts2}")

        # 3. The commits fetch used the watermark as `since` (date only: colons may be URL-encoded).
        since = watermarks[("scikit-learn/scikit-learn", "commits")]
        page1 = json.loads((main.RAW_DIR / "scikit-learn__scikit-learn" / "commits" / "page_0001.json").read_text())
        print(f"3. sklearn commits watermark {since}; first page URL {page1['url']}")
        assert since[:10] in page1["url"], "Commits fetch did not use the watermark as 'since'"
        assert second < first, f"Incremental run used {second} requests, full run used {first}"
        for table in counts1:
            assert counts2[table] >= counts1[table], f"{table} lost rows: {counts1[table]} -> {counts2[table]}"

        # 4. The first run's files were moved to the archive, not deleted.
        archived = list(main.ARCHIVE_DIR.glob("raw_*"))
        print(f"4. Archive: {[a.name for a in archived]}")
        assert len(archived) == 1, f"Expected 1 archived raw_* folder, found {len(archived)}"
        assert list(archived[0].rglob("page_*.json")), "Archived folder has no page files"
        print("All incremental checks passed")


if __name__ == "__main__":
    test_incremental()
