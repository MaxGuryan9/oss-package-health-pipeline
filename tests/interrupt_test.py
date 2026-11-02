"""Send a real Ctrl-C to a running extract and check it stops cleanly. Costs about 10-30 API requests.

Run from the project root:  python tests/interrupt_test.py
"""
import os
import signal
import sys
import tempfile
import threading
import time
from pathlib import Path

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

TEST_WINDOW = "2026-09-01T00:00:00Z"  # big enough that the run is still going after 3 seconds
INTERRUPT_AFTER = 3.0
MAX_STOP_SECONDS = 5.0


def test_interrupt() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Point every path at the temp folder so nothing lands in the real data/raw or logs/.
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.LOG_DIR = Path(tmp) / "logs"

        sent_at = {}

        def send_ctrl_c() -> None:
            sent_at["t"] = time.monotonic()
            os.kill(os.getpid(), signal.SIGINT)  # same signal the terminal sends on Ctrl-C

        threading.Timer(INTERRUPT_AFTER, send_ctrl_c).start()
        try:
            main.extract(window_start=TEST_WINDOW)
        except KeyboardInterrupt:
            stopped_in = time.monotonic() - sent_at["t"]
        else:
            raise AssertionError("extract finished before the Ctrl-C; move TEST_WINDOW earlier")

        print(f"\nStopped {stopped_in:.1f}s after Ctrl-C")
        assert stopped_in < MAX_STOP_SECONDS, f"Took {stopped_in:.1f}s to stop (limit {MAX_STOP_SECONDS}s)"
        assert not list(main.RAW_DIR.rglob("*.tmp")), "Leftover .tmp files"
        log_text = (main.LOG_DIR / "extract.log").read_text()
        assert "Ctrl-C received" in log_text, "Ctrl-C warning missing from extract.log"
        print("All interrupt checks passed")


if __name__ == "__main__":
    test_interrupt()
