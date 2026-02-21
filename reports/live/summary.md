# Stargazer Bot Analysis for openclaw/openclaw

## Executive Summary
- Total stargazers analyzed: **1662**
- Likely bots (score ≥60): **7 (0.4%)**
- Suspicious (score 40–59): **48 (2.9%)**
- Likely human (score ≤39): **1607 (96.7%)**
- Top locations: London (23), Poland (9), India (9), Germany (8), Uk (8)

## Methodology
- Fetched stargazers via GitHub GraphQL with deterministic pagination and rate-limit handling.
- Stored normalized records in SQLite and computed metrics via `metrics.py` version `bot-v1`.
- Optional REST enrichment provided site admin status, websites, and public events sampling.

## Bot Model
- Heuristic scoring spanning account type, naming patterns, profile completeness, social graph,
  activity, and repository/gist presence.
- Key thresholds: +80 for GitHub Bot accounts, +25 for accounts younger than 7 days,
  +10 for blank profiles, −30 for site-admin verified staff.

## Results
- Average account age: **3004 days** (where available).
- Average followers: **57.1** (where available).
- Public event sampling coverage: **100.0%** of users.
- ![Bot labels](figures/bot_label_distribution.png)
- ![Bot scores](figures/bot_score_histogram.png)
- ![Account age](figures/account_age_histogram.png)
- ![Top locations](figures/top_locations.png)
- ![Stars over time](figures/stars_time_series.png)

## Validation
- Manual spot checks recommended for high-scoring accounts.
- Event sampling inspects recent public activity but omits private/org events.

## Limitations
- GitHub profiles may omit key signals (location, company).
- Heuristic classifier is deterministic and can mislabel niche communities.
- Event sampling capped to recent public events; dormant users may be legitimate.

## Appendix
- Data exports available in `reports/data`.
- Figures saved in `reports/figures`.
- Report generated at 2026-02-21 14:54 UTC.