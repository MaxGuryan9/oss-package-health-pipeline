# Open Source Package Health: GitHub Data Pipeline

DS3022 Data Project 2 · rhk2dx · Package set B. The assignment text is in [PROJECT_INSTRUCTIONS.md](PROJECT_INSTRUCTIONS.md).

## 1. Overview

One Prefect flow pulls GitHub activity for three Python data science packages (package set B) and ranks their development communities from healthiest to least healthy.

| Repo                        | PRs created since 2024-01-01 | Commits since 2024-01-01 |
|-----------------------------|------------------------------|--------------------------|
| `scikit-learn/scikit-learn` | 5,228                        | 3,364                    |
| `dask/dask`                 | 1,231                        | 1,371                    |
| `plotly/plotly.py`          | 690                          | 2,829                    |

Analysis window: 2024-01-01 through the time of each run (35 months, January 2024 to November 2026, as of the last run).

### The flow

`rhk2dx-package-health-flow` in `main.py` runs six tasks in order:

| #   | Task                | What it does                                                                         | Output                                               |
|-----|---------------------|--------------------------------------------------------------------------------------|------------------------------------------------------|
| 1   | `extract` (subflow) | Fetches commits, pull requests, PR reviews, and releases for the 3 repos in parallel | `data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json` |
| 2   | `load` (subflow)    | Upserts every landed record into one raw table per endpoint                          | `raw_*` tables, `_load_state`, `data/raw/_LOADED`    |
| 3   | `dbt_build`         | Runs `dbt build`: staging models, fact and mart tables, 43 tests                     | `stg_*`, `fct_*`, `mart_*`                           |
| 4   | `validate`          | Python checks from landed files to staging; outliers; descriptive statistics         | log only (fails the flow on a mismatch)              |
| 5   | `score_health`      | Scores 6 health dimensions from `mart_health_inputs`, weights them, ranks the repos  | `health_scores` table                                |
| 6   | `plot_health`       | Draws 3 plots                                                                        | `plots/*.png`                                        |

### Result

| Rank | Repo                        | Health score (0–1) |
|------|-----------------------------|--------------------|
| 1    | `plotly/plotly.py`          | 0.676              |
| 2    | `scikit-learn/scikit-learn` | 0.615              |
| 3    | `dask/dask`                 | 0.550              |

With equal weights the order is the same (0.666 / 0.603 / 0.517). Section 9 explains the metrics and weights. Section 11 covers dask's rank: its merging slowed sharply from July 2026 (1 merge each in August and September, against an average of 30.5 per month in 2024–25), and it has not merged a PR since 2026-09-29, though maintainers still review and close PRs.

## 2. Setup and run

### Requirements

- Python 3.12
- A GitHub personal access token. A classic token with no scopes is enough, because all three repos are public.
- All Python dependencies are pinned in `requirements.txt` (dbt-core 1.12.6, dbt-duckdb 1.11.1, duckdb 2.0.0, httpx 0.28.1, prefect 3.9.0, prefect-dbt 0.7.25).

### Setup

```bash
git clone https://github.com/MaxGuryan9/package-health-project-ds3022.git
cd package-health-project-ds3022
uv venv proj2 --python 3.12         # or: python3.12 -m venv proj2
source proj2/bin/activate
uv pip install -r requirements.txt  # or: pip install -r requirements.txt
export GH_PAT=<your token>
```

The token is read only from the `GH_PAT` environment variable (`main.py`). It is never written to a file, and `.gitignore` excludes `.env` files.

Nothing else needs to be set up: the flow runs `dbt deps` itself, and it creates `data/`, `logs/`, and `packages.duckdb` on the first run.

### Run

```bash
python main.py
```

This runs one Prefect flow, `rhk2dx-package-health-flow`: extract → load → dbt build → validate → score → plot.

### Expected runtime

| Run                                      | API requests | Time                                                                           | Source                                                                                                |
|------------------------------------------|--------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| First run (full window, nothing on disk) | 7,034        | Extract about 35 min; load, dbt, validation, scoring, and plots add under 15 s | `logs/extract_full_run.log` (2026-11-01, 17:59 → 18:35, including a 3-minute deliberate interruption) |
| Later runs (incremental)                 | 13–61        | About 20 s for the whole flow                                                  | `logs/extract.log` → `logs/plot.log` (2026-11-04: 33 requests, 19 s)                                  |

A first run needs more requests than the 5,000-per-hour limit. In the measured run, GitHub's hourly reset came partway through, so the flow never had to sleep. A run that starts with a full 5,000 budget will use it in about 24 minutes, then sleep until the reset. That makes a first run take up to about 70 minutes.

**Tip: start a full run shortly before your rate limit resets.** GitHub's limit is not reset on the clock hour. It resets one hour after the first request of the current window. A full run that starts shortly before a reset gets a fresh 5,000 requests early, so it has little or no sleep. That is what happened in the measured run: the budget reset about 10 minutes after the start, at 18:08:46 (`logs/extract_full_run.log`), so the run never slept. To check your remaining requests and reset time (this call does not count against the limit):

```bash
curl -s -H "Authorization: Bearer $GH_PAT" https://api.github.com/rate_limit | grep -A3 '"core"'
```

`reset` is a Unix timestamp; `date -r <reset>` (macOS) shows it as a local time.

### Outputs

- `packages.duckdb`: raw tables, dbt models, and the `health_scores` ranking
- `plots/*.png`: three plots
- `logs/`: one log per task (`extract`, `load`, `transform`, `validate`, `model`, `plot`), plus dbt's own `dbt.log`

## 3. Repository layout

```
package-health-project-ds3022/
├── main.py                        # the Prefect flow and all six tasks
├── requirements.txt               # pinned dependencies
│
├── dbt_project.yml                # dbt project: staging = views, marts = tables
├── profiles.yml                   # dbt connection: packages.duckdb, UTC (no secrets)
├── packages.yml                   # dbt package dbt_utils (installed by `dbt deps` in the flow)
├── package-lock.yml               # pinned dbt package version
├── models/
│   ├── staging/
│   │   ├── sources.yml            # raw_* tables declared as dbt sources
│   │   ├── schema.yml             # staging tests
│   │   ├── stg_commits.sql
│   │   ├── stg_pull_requests.sql
│   │   ├── stg_pr_reviews.sql
│   │   └── stg_releases.sql
│   └── marts/
│       ├── schema.yml             # descriptions and tests for every mart and column
│       ├── fct_pull_requests.sql
│       ├── fct_releases.sql
│       ├── mart_repo_monthly.sql
│       └── mart_health_inputs.sql
│
├── tests/
│   ├── assert_pr_closed_after_created.sql   # dbt singular test
│   ├── assert_review_after_pr_created.sql   # dbt singular test
│   └── *_test.py, test_fetch_pages.py       # Python tests (section 13)
│
├── plots/                         # the three PNGs written by Task 6
│   ├── dimension_profile.png
│   ├── monthly_trends.png
│   └── load_vs_rejection.png
│
│   # Template reference files (supplied with the assignment, not our work)
├── PROJECT_INSTRUCTIONS.md        # the assignment text (was the template's README.md)
├── CHECKLIST.md                   # submission checklist
├── ASSESSING-HEALTH.md            # background on measuring open source health
├── DBT-REFERENCE.md               # dbt layout, profiles, and PrefectDbtRunner reference
├── DEPLOYMENTS.md                 # Prefect deployment instructions (extended item)
├── EXTRAS.md                      # instructor tips
│
└── README.md                      # this write-up
```

dbt reads `tests/` for singular tests (`test-paths` in `dbt_project.yml`), so the dbt tests and the Python tests share that folder.

**Not committed** (in `.gitignore`), because each is created by running the flow, holds local state, or is not project output:

| Excluded                                | What it is                                                                                    |
|-----------------------------------------|-----------------------------------------------------------------------------------------------|
| `data/`                                 | Landed API pages (`data/raw/`) and earlier runs' pages (`data/archive/`)                      |
| `packages.duckdb`                       | The database                                                                                  |
| `logs/`                                 | Task logs, `dbt.log`, and test logs (`logs/test_logs/`)                                       |
| `target/`, `dbt_packages/`              | dbt build output and installed dbt packages                                                   |
| `proj2/`                                | The virtual environment                                                                       |
| `images/`                               | Example plots from the assignment template; kept locally as the style reference for our plots |
| `.env`, `__pycache__/`, `tests/output/` | Secrets, Python caches, test scratch files                                                    |

## 4. Pipeline-wide design

### Retries and error handling

Only Task 1 talks to the network, so it is the only task with retries.

| Failure                                                  | Where                                   | What happens                                                                                                                                                                                                                                                                                                                                                       |
|----------------------------------------------------------|-----------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Any failed request (4xx, 5xx, timeout, connection error) | `github_get`                            | `response.raise_for_status()` raises `httpx.HTTPError`                                                                                                                                                                                                                                                                                                             |
| Broken JSON in a response                                | `fetch_pages`                           | `response.json()` raises `ValueError`                                                                                                                                                                                                                                                                                                                              |
| Either of the above                                      | `fetch_pages`                           | Caught, logged with context (`[repo] endpoint page N failed: <error>`), then re-raised                                                                                                                                                                                                                                                                             |
| Secondary rate limit (403/429 with `Retry-After`)        | `github_get`                            | Sleeps the number of seconds GitHub asks for, then raises, so the retry happens after the wait                                                                                                                                                                                                                                                                     |
| The re-raised error                                      | `fetch_repo` task, `retry_if_transient` | Transient failures (403/429, 5xx, timeouts, connection errors, broken JSON) are retried up to 3 times, with increasing delays of 10, 30, and 60 s. Pages already on disk are skipped, so a retry costs only the requests that had not succeeded. Permanent failures (other 4xx, such as 404 or 401) fail at once, because they would fail the same way every time. |
| Ctrl-C                                                   | `extract`, `retry_if_transient`         | Workers stop at their next request or sleep. A Ctrl-C stop is never retried.                                                                                                                                                                                                                                                                                       |

Tasks 2–6 work on local files and the database. A failure there (a bad file, a failed dbt test, a validation mismatch) would fail again on a retry, so these tasks have no retries and fail the flow immediately.

### Logging

Each task has its own Python logger with a file handler, and the same logger is routed into Prefect (`PREFECT_LOGGING_EXTRA_LOGGERS`), so every line appears in the terminal, in the Prefect UI, and in a file:

| Task         | Log file                             | What the log records (purpose)                                                                                                                |
|--------------|--------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------|
| 1 Extract    | `logs/extract.log`                   | Every page fetched with its record count and remaining rate-limit budget; rate-limit sleeps; resume points; total API requests per run        |
| 2 Load       | `logs/load.log`                      | Row count before and after each raw table upsert, which shows loads are idempotent; `_load_state` update and `_LOADED` marker                 |
| 3 dbt        | `logs/transform.log`, `logs/dbt.log` | `transform.log`: start and finish of `dbt deps` and `dbt build`, with the pass count. `dbt.log`: every model built and every test's PASS/FAIL |
| 4 Validation | `logs/validate.log`                  | Completeness checks (landed files → raw → staging, commits every month), flagged outliers, descriptive statistics                             |
| 5 Model      | `logs/model.log`                     | The ranked table (score, rank, 6 subscores) and the equal-weight robustness check                                                             |
| 6 Plot       | `logs/plot.log`                      | The path of each saved PNG                                                                                                                    |

Python tests write their own logs to `logs/test_logs/` (section 13).

## 5. Task 1: Extract

### Endpoints and record counts (first full run, 2026-11-01)

| Endpoint      | Request                                                       | scikit-learn       | dask              | plotly.py       |
|---------------|---------------------------------------------------------------|--------------------|-------------------|-----------------|
| Pull requests | `/pulls?state=all&sort=updated&direction=desc`                | 6,400              | 1,500             | 900             |
| PR reviews    | `/pulls/{number}/reviews`, one call per PR created since 2024 | 18,260 (5,207 PRs) | 1,985 (1,224 PRs) | 1,592 (685 PRs) |
| Commits       | `/commits?since=2024-01-01T00:00:00Z`                         | 3,345              | 1,371             | 2,827           |
| Releases      | `/releases` (full history, small)                             | 51                 | 44                | 109             |

Total: 7,034 API requests. Every count above is logged per repo and endpoint in `logs/extract_full_run.log`.

- **Pull requests** have no date filter, so they are sorted by `updated` descending and paging stops once a page ends before the window start (`stop_before` in `fetch_pages`).
- **Reviews** are fetched for every PR created in the window, merged or not (`fetch_reviews`).
- **Pagination** follows the `Link` header's `rel="next"` URL with `per_page=100` until there is no next page.

### Rate limits

- **Primary limit:** `github_get` reads `X-RateLimit-Remaining` and `X-RateLimit-Reset` on every response. When 25 or fewer requests remain, it sleeps until the reset time plus 5 s. The 25-request buffer leaves room for the other two threads' requests that are already in flight.
- **Secondary limit:** a 403/429 with `Retry-After` sleeps that many seconds, then raises so Prefect retries (section 4).
- 5xx errors and timeouts are retried by Prefect (section 4).

### Concurrent fetching (extended item)

`extract` runs one `fetch_repo` task per repo in parallel (`fetch_repo.map(...)` on a `ThreadPoolTaskRunner` with 3 workers). One token means one shared budget, so all threads share `RATE_LOCK`: when one thread sleeps off the rate limit it holds the lock, and every other thread waits at its next request.

### Landing files

`data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json`, one file per response page, in the README format (`repo`, `endpoint`, `url`, `page`, `fetched_at`, `data`) plus one added field:

- `next_url`: the next page's URL (null on the last page). Resume uses it to continue from the last saved page without re-fetching.

Reviews are stored one folder per PR, `reviews/pr_<number>/page_NNNN.json`, so resume can tell which PRs are done.

Pages are written atomically: first to a `.tmp` file, then renamed (`write_page`). A crash mid-write leaves only a `.tmp` file, which the next run deletes; a half-written `.json` page can never exist.

### Resume after an interruption

When there is no `_LOADED` marker, the next run keeps `data/raw/`, skips every page already on disk, and continues from the saved `next_url`. Excerpt from the first full run, stopped with Ctrl-C after scikit-learn's pulls page 54 and restarted (`logs/extract_full_run.log`, lines 170–190):

```
18:00:01  [scikit-learn/scikit-learn] pulls page 54: 100 records (budget left 4437)
          ← Ctrl-C; restarted 2m 43s later
18:02:44  Extract starting (resume): 3 repos, window from 2024-01-01T00:00:00Z
18:02:45  [plotly/plotly.py] pulls: 9 pages on disk, already complete
18:02:45  [dask/dask] pulls: 15 pages on disk, already complete
18:02:45  [dask/dask] commits: 14 pages on disk, already complete
18:02:45  [scikit-learn/scikit-learn] commits: 34 pages on disk, already complete
...
18:02:45  [scikit-learn/scikit-learn] pulls: 54 pages on disk, resuming at page 55
18:02:47  [scikit-learn/scikit-learn] pulls page 55: 100 records (budget left 4430)
```

The budget went from 4,437 before the stop to 4,430 after the restart, so the pages already on disk cost no requests.

> **Note on the missing Ctrl-C line.** The warning `Ctrl-C received: stopping workers; pages on disk are kept for resume` was added to `extract` after this full run, so it does not appear in the excerpt above. It is confirmed by `tests/interrupt_test.py`, which sends a real Ctrl-C to a running extract (`logs/test_logs/interrupt_test.log`: the warning is logged and the workers stop 0.7 s later). The full run was not repeated to capture the line: the gap was found too close to the deadline, and a fresh full run costs about 7,000 requests and up to 70 minutes.

When `_LOADED` exists, everything on disk is already in DuckDB. The run moves `data/raw/` to `data/archive/raw_<UTC time>/` (kept for Task 4, instead of deleted) and fetches only records newer than the `_load_state` watermarks (section 6).

## 6. Task 2: Load

_To be written._

## 7. Task 3: dbt

_To be written._

## 8. Task 4: Validation

_To be written._

## 9. Task 5: Health model and ranking

_To be written._

## 10. Task 6: Plots

_To be written._

## 11. Data findings and limitations

_To be written._

## 12. Extended items

_To be written after the scheduled deployment and API odometer are built._

## 13. Tests

_To be written._
