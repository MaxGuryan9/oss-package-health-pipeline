# Open Source Package Health: GitHub Data Pipeline

DS3022 Data Project 2 · rhk2dx · Package set B. The assignment text is in [PROJECT_INSTRUCTIONS.md](PROJECT_INSTRUCTIONS.md).

## 1. Overview

One Prefect flow pulls GitHub activity for three Python data science packages (package set B) and ranks their development communities from healthiest to least healthy.

| Repo | PRs created since 2024-01-01 | Commits since 2024-01-01 |
|---|---|---|
| `scikit-learn/scikit-learn` | 5,228 | 3,364 |
| `dask/dask` | 1,231 | 1,371 |
| `plotly/plotly.py` | 690 | 2,829 |

Analysis window: 2024-01-01 through the time of each run (35 months, January 2024 to November 2026, as of the last run).

### The flow

`rhk2dx-package-health-flow` in `main.py` runs six tasks in order:

| # | Task | What it does | Output |
|---|---|---|---|
| 1 | `extract` (subflow) | Fetches commits, pull requests, PR reviews, and releases for the 3 repos in parallel | `data/raw/<owner>__<repo>/<endpoint>/page_NNNN.json` |
| 2 | `load` (subflow) | Upserts every landed record into one raw table per endpoint | `raw_*` tables, `_load_state`, `data/raw/_LOADED` |
| 3 | `dbt_build` | Runs `dbt build`: staging models, fact and mart tables, 43 tests | `stg_*`, `fct_*`, `mart_*` |
| 4 | `validate` | Python checks from landed files to staging; outliers; descriptive statistics | log only (fails the flow on a mismatch) |
| 5 | `score_health` | Scores 6 health dimensions from `mart_health_inputs`, weights them, ranks the repos | `health_scores` table |
| 6 | `plot_health` | Draws 3 plots | `plots/*.png` |

### Result

| Rank | Repo | Health score (0–1) |
|---|---|---|
| 1 | `plotly/plotly.py` | 0.676 |
| 2 | `scikit-learn/scikit-learn` | 0.615 |
| 3 | `dask/dask` | 0.550 |

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

| Run | API requests | Time | Source |
|---|---|---|---|
| First run (full window, nothing on disk) | 7,034 | Extract about 35 min; load, dbt, validation, scoring, and plots add under 15 s | `logs/extract_full_run.log` (2026-11-01, 17:59 → 18:35, including a 3-minute deliberate interruption) |
| Later runs (incremental) | 13–61 | About 20 s for the whole flow | `logs/extract.log` → `logs/plot.log` (2026-11-04: 33 requests, 19 s) |

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
main.py                     # the Prefect flow and all six tasks
requirements.txt            # pinned dependencies
dbt_project.yml             # dbt project: staging = views, marts = tables
profiles.yml                # dbt connection: DuckDB file packages.duckdb, UTC (no secrets)
packages.yml, package-lock.yml   # dbt package dbt_utils (installed by `dbt deps` in the flow)
models/
  staging/                  # sources.yml, stg_commits, stg_pull_requests, stg_pr_reviews, stg_releases, schema.yml
  marts/                    # fct_pull_requests, fct_releases, mart_repo_monthly, mart_health_inputs, schema.yml
tests/
  assert_*.sql              # dbt singular tests (dbt reads this folder: test-paths in dbt_project.yml)
  *_test.py, test_fetch_pages.py   # Python tests, run one at a time (section 13)
plots/                      # the three PNGs written by Task 6 (committed)

# Template reference files (supplied with the assignment, not our work)
PROJECT_INSTRUCTIONS.md     # the assignment text (was the template's README.md)
CHECKLIST.md                # submission checklist
ASSESSING-HEALTH.md         # background on measuring open source health
DBT-REFERENCE.md            # dbt layout, profiles, and PrefectDbtRunner reference
DEPLOYMENTS.md              # Prefect deployment instructions (extended item)
EXTRAS.md                   # instructor tips
```

**Not committed** (in `.gitignore`), because each is created by running the flow, holds local state, or is not project output:

| Excluded | What it is |
|---|---|
| `data/` | Landed API pages (`data/raw/`) and earlier runs' pages (`data/archive/`) |
| `packages.duckdb` | The database |
| `logs/` | Task logs, `dbt.log`, and test logs (`logs/test_logs/`) |
| `target/`, `dbt_packages/` | dbt build output and installed dbt packages |
| `proj2/` | The virtual environment |
| `images/` | Example plots from the assignment template; kept locally as the style reference for our plots |
| `.env`, `__pycache__/`, `tests/output/` | Secrets, Python caches, test scratch files |

## 4. Pipeline-wide design

_To be written._

## 5. Task 1: Extract

_To be written._

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
