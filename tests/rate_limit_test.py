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


def fake_client(status: int, headers: dict | None = None, body: str = "[]", network_error: bool = False):
    """httpx client whose every request returns the given response (or a connection error)."""
    def handler(request: httpx.Request) -> httpx.Response:
        if network_error:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(status, headers=headers or {}, text=body)
    return httpx.Client(transport=httpx.MockTransport(handler))


def expect_error(error_type, client) -> str:
    """Call github_get and confirm it raises error_type. Returns the error message."""
    try:
        main.github_get(client, URL)
    except error_type as exc:
        return str(exc)
    raise AssertionError(f"Expected {error_type.__name__}")


def test_rate_limit_and_errors() -> None:
    reset_in_60s = str(int(time.time()) + 60)

    SLEEPS.clear()
    main.github_get(fake_client(200, {"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": reset_in_60s}), URL)
    assert SLEEPS == [], f"Slept with plenty of budget: {SLEEPS}"
    print("1. 200, remaining 100     -> returned, no sleep")

    SLEEPS.clear()
    main.github_get(fake_client(200, {"X-RateLimit-Remaining": "3", "X-RateLimit-Reset": reset_in_60s}), URL)
    assert len(SLEEPS) == 1 and 60 < SLEEPS[0] <= 65, f"Expected one ~65s sleep, got {SLEEPS}"
    print(f"2. 200, remaining 3       -> slept {SLEEPS[0]:.0f}s until reset, returned")

    SLEEPS.clear()
    msg = expect_error(main.TransientHTTPError, fake_client(429, {"Retry-After": "30"}))
    assert SLEEPS == [30], f"Expected a 30s Retry-After sleep, got {SLEEPS}"
    print(f"3. 429, Retry-After 30    -> slept 30s, Transient: {msg}")

    msg = expect_error(main.TransientHTTPError, fake_client(403, body='{"message": "API rate limit exceeded"}'))
    print(f"4. 403, rate limit body   -> Transient: {msg[:60]}")

    msg = expect_error(main.PermanentHTTPError, fake_client(403, body='{"message": "Resource not accessible"}'))
    print(f"5. 403, permission body   -> Permanent: {msg[:60]}")

    msg = expect_error(main.TransientHTTPError, fake_client(502))
    print(f"6. 502                    -> Transient: {msg}")

    msg = expect_error(main.PermanentHTTPError, fake_client(404, body='{"message": "Not Found"}'))
    print(f"7. 404                    -> Permanent: {msg[:60]}")

    msg = expect_error(main.TransientHTTPError, fake_client(200, network_error=True))
    print(f"8. connection error       -> Transient: {msg[:60]}")

    with tempfile.TemporaryDirectory() as tmp:
        try:
            main.fetch_pages(fake_client(200, body="<html>not json"), "dask/dask", "commits", {}, out_dir=Path(tmp))
        except main.TransientHTTPError as exc:
            print(f"9. 200, broken JSON body  -> Transient: {str(exc)[:60]}")
        else:
            raise AssertionError("Expected TransientHTTPError for a broken JSON body")

    sent = []
    main.STOP.set()
    client = httpx.Client(transport=httpx.MockTransport(lambda req: sent.append(req) or httpx.Response(200)))
    try:
        main.github_get(client, URL)
    except main.RunInterrupted:
        assert sent == [], "A request was sent after STOP was set"
        print("10. STOP set (Ctrl-C)      -> RunInterrupted, no request sent")
    else:
        raise AssertionError("Expected RunInterrupted when STOP is set")
    finally:
        main.STOP.clear()

    print("All rate-limit and error checks passed")


if __name__ == "__main__":
    test_rate_limit_and_errors()
