# Open Source Package Health: GitHub Data Pipeline

DS3022 Data Project 2 · rhk2dx · Package set B. The assignment text is in [PROJECT_INSTRUCTIONS.md](PROJECT_INSTRUCTIONS.md).

## 1. Overview

_To be written._

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

_To be written._

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
