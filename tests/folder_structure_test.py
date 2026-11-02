"""Manual check that fetch_pages writes landing files in the README format. Costs about 5 API requests.

Run from the project root:  python tests/folder_structure_test.py
Keep the files to inspect:  python tests/folder_structure_test.py --keep   (written to tests/output/)
"""
import json
import shutil
import sys
import tempfile
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

REQUIRED_KEYS = {"repo", "endpoint", "url", "page", "fetched_at", "next_url", "data"}
KEEP_DIR = Path(__file__).resolve().parent / "output"


def test_folder_structure(keep: bool = False) -> None:
    # landing_dir builds the README path data/raw/<owner>__<repo>/<endpoint> (no API call).
    assert main.landing_dir("dask/dask", "commits") == main.RAW_DIR / "dask__dask" / "commits"

    main.PER_PAGE = 10  # small pages force several landing files
    with tempfile.TemporaryDirectory() as tmp:
        if keep:
            shutil.rmtree(KEEP_DIR, ignore_errors=True)  # clear the last run so old pages can't mix in
            out_dir = KEEP_DIR / "dask__dask" / "commits"
        else:
            out_dir = Path(tmp)
        records, _ = main.fetch_pages(
            "dask/dask", "commits", {"since": "2026-06-01T00:00:00Z"}, out_dir=out_dir
        )

        files = sorted(out_dir.glob("page_*.json"))
        print(f"{len(records)} records in {len(files)} files: {files[0].name} .. {files[-1].name}")

        # One file per page, numbered page_0001, page_0002, ... with no gaps.
        expected = [f"page_{n:04d}.json" for n in range(1, len(files) + 1)]
        assert [f.name for f in files] == expected, f"Unexpected file names: {[f.name for f in files]}"

        # Every file has the README keys and matching metadata.
        total = 0
        for n, f in enumerate(files, start=1):
            body = json.loads(f.read_text())
            assert set(body) == REQUIRED_KEYS, f"{f.name} keys: {sorted(body)}"
            assert body["repo"] == "dask/dask" and body["endpoint"] == "commits" and body["page"] == n
            total += len(body["data"])
        assert total == len(records), f"Files hold {total} records, fetch_pages returned {len(records)}"

        # The rename step left no temp files behind.
        assert not list(out_dir.glob("*.tmp")), "Leftover .tmp files"

        print("page_0001.json url:", json.loads(files[0].read_text())["url"])
        print("All landing-file checks passed")
        if keep:
            print(f"Files kept in {out_dir}")


if __name__ == "__main__":
    test_folder_structure(keep="--keep" in sys.argv)
