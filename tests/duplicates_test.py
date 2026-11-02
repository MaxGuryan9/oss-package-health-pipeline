"""Check the landed files for records that appear more than once, and print where they are. Costs 0 API requests.

Run from the project root:  python tests/duplicates_test.py
"""
import shutil
import sys
import tempfile
from pathlib import Path

import duckdb

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main


def test_checker_finds_planted_duplicate() -> None:
    """Copy one pulls page under a second name; every PR on it must be reported with both file paths."""
    real_raw = main.RAW_DIR
    with tempfile.TemporaryDirectory() as tmp:
        pulls_dir = Path(tmp) / "dask__dask" / "pulls"
        pulls_dir.mkdir(parents=True)
        shutil.copy(real_raw / "dask__dask" / "pulls" / "page_0001.json", pulls_dir / "page_0001.json")
        shutil.copy(real_raw / "dask__dask" / "pulls" / "page_0001.json", pulls_dir / "page_0099.json")
        main.RAW_DIR = Path(tmp)
        try:
            duplicates = main.find_duplicates(duckdb.connect(), main.LOAD_SPECS["pulls"])
        finally:
            main.RAW_DIR = real_raw
    assert duplicates, "Checker missed a planted duplicate"
    repo, key, copies, files = duplicates[0]
    assert copies == 2 and files[0].endswith("page_0001.json") and files[1].endswith("page_0099.json")
    print(f"1. Planted duplicate found: {len(duplicates)} PRs x2, e.g. {repo} #{key} in "
          f"{Path(files[0]).name}, {Path(files[1]).name}")


def test_real_data_has_no_duplicates() -> None:
    """Scan every endpoint in data/raw. On failure, list each duplicated key and the files it is in."""
    con = duckdb.connect()
    found = {}
    print("2. Real data/raw:")
    for endpoint, spec in main.LOAD_SPECS.items():
        duplicates = main.find_duplicates(con, spec)
        print(f"   {endpoint:<9} {len(duplicates)} duplicated keys")
        if duplicates:
            found[endpoint] = duplicates
    for endpoint, duplicates in found.items():
        print(f"\n   {endpoint}: where to look")
        for repo, key, copies, files in duplicates:
            print(f"     {repo} id={key} x{copies}: {', '.join(files)}")
    assert not found, f"Duplicates in {sorted(found)}; see the file list above"


if __name__ == "__main__":
    test_checker_finds_planted_duplicate()
    test_real_data_has_no_duplicates()
    print("All duplicate checks passed")
