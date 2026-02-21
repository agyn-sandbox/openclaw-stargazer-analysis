# openclaw-stargazer-analysis

Analysis toolkit to fetch and analyze stargazers of `openclaw/openclaw`, store results in SQLite, and generate a bot-likelihood report.

## Overview
This repository contains scripts to:
- Fetch stargazers via GitHub GraphQL (REST fallback) with pagination and rate-limit handling.
- Store user and stargazer data in a normalized SQLite database.
- Compute user metrics and a reproducible bot-likelihood score (0–100).
- Generate aggregate statistics and figures (locations, account age, activity, companies, bot-score histograms, time series).
- Produce a final report.

## Status
Initial scaffolding. Implementation will proceed in Issue #1 and a single PR per the workflow.

## Quick Start (planned)
1. Set `GITHUB_TOKEN` in your environment.
2. Create a virtual environment and install dependencies from `requirements.txt`.
3. Run fetch, metrics, analyze, and report scripts as documented in Issue #1.

## Ethics & Privacy
- Uses only public GitHub APIs.
- No raw emails are stored; only an `email_public` flag.
- Heuristic bot classification; results are probabilistic.

## License
MIT
