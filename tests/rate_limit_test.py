"""Check rate-limit sleeps and error types against a fake GitHub. Costs 0 API requests.

Run from the project root:  python tests/rate_limit_test.py
"""
import sys
import tempfile
import time
from pathlib import Path

import httpx

# Make main.py (one folder up) importable when this file is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import main

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
    print("1. 200, remaining 100     -> returned, no sleep")

    SLEEPS.clear()
    main.httpx.get = fake_get(200, {"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": reset_in_60s})
    main.github_get(URL)
    assert len(SLEEPS) == 1 and 60 < SLEEPS[0] <= 65, f"Expected one ~65s sleep, got {SLEEPS}"
    print(f"2. 200, remaining 3       -> slept {SLEEPS[0]:.0f}s until reset, returned")

    SLEEPS.clear()
    msg = expect_error(fake_get(429, {"Retry-After": "30"}))
    assert SLEEPS == [30], f"Expected a 30s Retry-After sleep, got {SLEEPS}"
    print(f"3. 429, Retry-After 30    -> slept 30s, raised: {msg[:60]}")

    msg = expect_error(fake_get(403, body='{"message": "API rate limit exceeded"}'))
    print(f"4. 403                    -> raised: {msg[:60]}")

    msg = expect_error(fake_get(502))
    print(f"5. 502                    -> raised: {msg[:60]}")

    msg = expect_error(fake_get(404, body='{"message": "Not Found"}'))
    print(f"6. 404                    -> raised: {msg[:60]}")

    msg = expect_error(fake_get(200, network_error=True))
    print(f"7. connection error       -> raised: {msg[:60]}")

    with tempfile.TemporaryDirectory() as tmp:
        main.httpx.get = fake_get(200, body="<html>not json")
        try:
            main.fetch_pages("dask/dask", "commits", {}, out_dir=Path(tmp))
        except ValueError as exc:
            print(f"8. 200, broken JSON body  -> ValueError: {str(exc)[:60]}")
        else:
            raise AssertionError("Expected ValueError for a broken JSON body")

    sent = []
    main.STOP.set()
    main.httpx.get = lambda url, **kw: sent.append(url) or httpx.Response(200)
    try:
        main.github_get(URL)
    except main.RunInterrupted:
        assert sent == [], "A request was sent after STOP was set"
        print("9. STOP set (Ctrl-C)       -> RunInterrupted, no request sent")
    else:
        raise AssertionError("Expected RunInterrupted when STOP is set")
    finally:
        main.STOP.clear()

    print("All rate-limit and error checks passed")


if __name__ == "__main__":
    test_rate_limit_and_errors()
