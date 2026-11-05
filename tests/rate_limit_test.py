"""Check rate-limit sleeps and error types against a fake GitHub. Costs 0 API requests.

Run from the project root:  python tests/rate_limit_test.py
"""
import logging
import sys
import tempfile
import time
from pathlib import Path

import httpx

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

URL = "https://api.github.com/repos/dask/dask/commits"
SLEEPS: list[float] = []


def fake_sleep(seconds: float) -> None:
    """Record how long the code would have slept instead of waiting."""
    SLEEPS.append(seconds)


main.pause = fake_sleep


def fake_get(status: int, headers: dict | None = None, body: str = "[]", network_error: bool = False):
    """Stand-in for httpx.get that returns the given response (or a connection error)."""
    def get(url, **kwargs):
        request = httpx.Request("GET", url)
        if network_error:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(status, headers=headers or {}, text=body, request=request)
    return get


def expect_error(get) -> str:
    """Call github_get with a fake httpx.get and confirm it raises an httpx error. Returns the error message."""
    main.httpx.get = get
    try:
        main.github_get(URL)
    except httpx.HTTPError as exc:
        return str(exc)
    raise AssertionError("Expected an httpx.HTTPError")


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


if __name__ == "__main__":
    try:
        test_rate_limit_and_errors()
    except Exception:
        test_log.exception("Test failed")  # save failures too, with the traceback
        raise
