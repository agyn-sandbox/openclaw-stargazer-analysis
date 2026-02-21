# openclaw-stargazer-analysis

Analysis toolkit to fetch and analyze stargazers of `openclaw/openclaw`, store results in SQLite, and generate a bot-likelihood report.

## Overview

The pipeline provides:

- Deterministic fetch of stargazers via GitHub GraphQL with retry/backoff and REST enrichment support.
- Normalized SQLite persistence (repositories, users, stargazers, fetch runs, metrics).
- Bot-heuristic metrics (score 0–100) and activity sampling.
- Aggregated CSV outputs and rich figures for reporting.
- Markdown report summarising findings with visuals.

## Prerequisites

- Python 3.12+
- `pip` and basic build tooling (`libstdc++`, `zlib`).
- A GitHub personal access token with `public_repo` scope. App/installation tokens require an accurate system clock.

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

If NumPy fails to load due to missing native libraries, ensure `libstdc++` and `zlib` development packages are installed (e.g., `apt install libstdc++6 zlib1g`).

Copy `.env.example` to `.env` (or export the variables) and provide a valid `GITHUB_TOKEN`:

```bash
cp .env.example .env
```

## Usage

All commands assume the virtual environment is active and the repository root is on `PYTHONPATH` (e.g., `export PYTHONPATH=$(pwd)`).

1. **Fetch stargazers** (GraphQL primary, optional REST enrichment):
   ```bash
   python -m src.fetch --repo openclaw/openclaw --max-users 200 --rest-enrichment --events recent
   ```
   The fetch run is resumable via checkpoints stored in `fetch_runs`.

2. **Compute bot metrics** (requires fetch data):
   ```bash
   python -m src.metrics --repo openclaw/openclaw --metrics-version bot-v1
   ```

3. **Generate aggregates and CSVs**:
   ```bash
   python -m src.analyze --repo openclaw/openclaw --metrics-version bot-v1 --out-dir reports/data
   ```

4. **Render figures and Markdown report**:
   ```bash
   python -m src.report --repo openclaw/openclaw --metrics-version bot-v1 \
     --data-dir reports/data --fig-dir reports/figures --out-file reports/report.md
   ```

Outputs land in:

- SQLite database: `data/openclaw_stargazers.db`
- Analysis CSVs: `reports/data/*.csv`
- Figures: `reports/figures/*.png`
- Markdown report: `reports/report.md`

## Offline Demo / Sample Data

If GitHub access is unavailable, seed a deterministic demo dataset and run the downstream stages:

```bash
export GITHUB_TOKEN=offline
export PYTHONPATH=$(pwd)
python scripts/seed_sample_data.py
python -m src.metrics --repo openclaw/openclaw --metrics-version bot-v1
python -m src.analyze --repo openclaw/openclaw --metrics-version bot-v1 --out-dir reports/data
python -m src.report --repo openclaw/openclaw --metrics-version bot-v1 \
  --data-dir reports/data --fig-dir reports/figures --out-file reports/report.md
```

This generates a six-user dataset illustrating metric scoring, aggregate outputs, and the final report without calling the GitHub APIs.

## Troubleshooting

- **Invalid or expired token** – GitHub GraphQL returns `Timestamp outside allowed skew` when using installation tokens with a misconfigured clock. Use a PAT or fix clock skew.
- **Rate limiting** – The clients automatically backoff; rerun the fetch after the reset window or reduce `--max-users`.
- **Missing native libs** – Install `libstdc++`/`zlib` development packages if NumPy/Pandas cannot import.

## Ethics & Privacy

- Uses only public GitHub APIs.
- No raw emails are stored; only a boolean `email_public` flag.
- Bot heuristics are deterministic indicators, not definitive classifications.

## License

MIT
