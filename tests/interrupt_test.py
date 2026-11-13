# INTERVIEW: GOAL OF THIS TEST: send a real Ctrl-C to a running extract (Task 1 'Resumability'). CONFIRMS: the run
# stops in under 5 seconds, no half-written .tmp files are left, and the 'Ctrl-C received' warning is in the log.
# CATCHES: worker threads that keep running after Ctrl-C, or a page left half-written. It also backs the README note
# that the first full run's log is missing the Ctrl-C line (the warning was added after that run).
"""Send a real Ctrl-C to a running extract and check it stops cleanly. Costs about 10-30 API requests.

Run from the project root:  python tests/interrupt_test.py
"""
import logging
import os
import signal
import sys
import tempfile
import threading
import time
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

TEST_WINDOW = "2026-09-01T00:00:00Z"  # big enough that the run is still going after 3 seconds
INTERRUPT_AFTER = 3.0
MAX_STOP_SECONDS = 5.0


# INTERVIEW: A timer thread fires the interrupt 3 seconds after extract starts, and time.monotonic measures how long
# the stop takes. The `else` branch fails the test if extract finished before the interrupt, since then nothing was
# actually tested.
def test_interrupt() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        # Point data paths at the temp folder so nothing lands in the real data/raw.
        # Log lines go to the real logs/extract.log (its handler is attached at import).
        main.RAW_DIR = Path(tmp) / "data" / "raw"
        main.LOADED_MARKER = main.RAW_DIR / "_LOADED"
        main.DB_PATH = Path(tmp) / "test.duckdb"

        sent_at = {}

        # INTERVIEW: Sends SIGINT to this same process, the exact signal a terminal sends on Ctrl-C, so the test
        # exercises the real KeyboardInterrupt path in extract rather than a simulated one.
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

        say(f"\nStopped {stopped_in:.1f}s after Ctrl-C")
        assert stopped_in < MAX_STOP_SECONDS, f"Took {stopped_in:.1f}s to stop (limit {MAX_STOP_SECONDS}s)"
        assert not list(main.RAW_DIR.rglob("*.tmp")), "Leftover .tmp files"
        log_text = (main.LOG_DIR / "extract.log").read_text()
        assert "Ctrl-C received" in log_text, "Ctrl-C warning missing from extract.log"
        say("All interrupt checks passed")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_interrupt()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
