# INTERVIEW: GOAL OF THIS TEST: check github_get's rate-limit and error behavior against a fake GitHub, at 0 API
# requests (Task 1 'Primary rate limit' and 'Other HTTP errors'). CONFIRMS: no sleep with plenty of budget, a sleep
# until reset + 5 s when 3 requests remain, a 30 s sleep on Retry-After, an exception on 403/404/502/connection
# errors/broken JSON, and no request sent after Ctrl-C. CATCHES: running the budget to 0, ignoring Retry-After, or
# swallowing an error. It shows that errors are raised; whether Prefect then retries is decided by retry_if_transient,
# which this file does not test.
"""Check rate-limit sleeps and error types against a fake GitHub. Costs 0 API requests.

Run from the project root:  python tests/rate_limit_test.py
"""
import logging
import sys
import tempfile
import time
from pathlib import Path

import httpx

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

URL = "https://api.github.com/repos/dask/dask/commits"
SLEEPS: list[float] = []


# INTERVIEW: Replaces main.pause, so the test records how long the code would have slept instead of really waiting a
# minute or more.
def fake_sleep(seconds: float) -> None:
    """Record how long the code would have slept instead of waiting."""
    SLEEPS.append(seconds)


main.pause = fake_sleep


# INTERVIEW: Builds a stand-in for httpx.get that returns whatever status, headers and body the test asks for, or
# raises a connection error. This is how rate-limit and error responses are tested without real requests.
def fake_get(status: int, headers: dict | None = None, body: str = "[]", network_error: bool = False):
    """Stand-in for httpx.get that returns the given response (or a connection error)."""
    def get(url, **kwargs):
        request = httpx.Request("GET", url)
        if network_error:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(status, headers=headers or {}, text=body, request=request)
    return get


# INTERVIEW: Helper for the error cases: installs a fake response, calls github_get, and fails the test if no httpx
# error is raised. Returns the error message so it can be printed.
def expect_error(get) -> str:
    """Call github_get with a fake httpx.get and confirm it raises an httpx error. Returns the error message."""
    main.httpx.get = get
    try:
        main.github_get(URL)
    except httpx.HTTPError as exc:
        return str(exc)
    raise AssertionError("Expected an httpx.HTTPError")


# INTERVIEW: Nine numbered cases, one per behavior. Case 2 expects a sleep between 60 and 65 seconds because the fake
# reset is 60 s away and we add a 5 s margin. The `finally` clears STOP so the Ctrl-C flag set in case 9 does not leak
# into anything that runs afterwards.
def test_rate_limit_and_errors() -> None:
    reset_in_60s = str(int(time.time()) + 60)

    SLEEPS.clear()
    main.httpx.get = fake_get(200, {"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": reset_in_60s})
    main.github_get(URL)
    assert SLEEPS == [], f"Slept with plenty of budget: {SLEEPS}"
    say("1. 200, remaining 100     -> returned, no sleep")

    SLEEPS.clear()
    main.httpx.get = fake_get(200, {"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": reset_in_60s})
    main.github_get(URL)
    assert len(SLEEPS) == 1 and 60 < SLEEPS[0] <= 65, f"Expected one ~65s sleep, got {SLEEPS}"
    say(f"2. 200, remaining 3       -> slept {SLEEPS[0]:.0f}s until reset, returned")

    SLEEPS.clear()
    msg = expect_error(fake_get(429, {"Retry-After": "30"}))
    assert SLEEPS == [30], f"Expected a 30s Retry-After sleep, got {SLEEPS}"
    say(f"3. 429, Retry-After 30    -> slept 30s, raised: {msg[:60]}")

    msg = expect_error(fake_get(403, body='{"message": "API rate limit exceeded"}'))
    say(f"4. 403                    -> raised: {msg[:60]}")

    msg = expect_error(fake_get(502))
    say(f"5. 502                    -> raised: {msg[:60]}")

    msg = expect_error(fake_get(404, body='{"message": "Not Found"}'))
    say(f"6. 404                    -> raised: {msg[:60]}")

    msg = expect_error(fake_get(200, network_error=True))
    say(f"7. connection error       -> raised: {msg[:60]}")

    with tempfile.TemporaryDirectory() as tmp:
        main.httpx.get = fake_get(200, body="<html>not json")
        try:
            main.fetch_pages("dask/dask", "commits", {}, out_dir=Path(tmp))
        except ValueError as exc:
            say(f"8. 200, broken JSON body  -> ValueError: {str(exc)[:60]}")
        else:
            raise AssertionError("Expected ValueError for a broken JSON body")

    sent = []
    main.STOP.set()
    main.httpx.get = lambda url, **kw: sent.append(url) or httpx.Response(200)
    try:
        main.github_get(URL)
    except main.RunInterrupted:
        assert sent == [], "A request was sent after STOP was set"
        say("9. STOP set (Ctrl-C)       -> RunInterrupted, no request sent")
    else:
        raise AssertionError("Expected RunInterrupted when STOP is set")
    finally:
        main.STOP.clear()

    say("All rate-limit and error checks passed")


# INTERVIEW: Runs the test when the file is executed directly. A failure is written to the log with its traceback and
# then re-raised, so the log also records failed runs and the command still exits with an error.
if __name__ == "__main__":
    try:
        test_rate_limit_and_errors()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
