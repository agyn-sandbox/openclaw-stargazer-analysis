"""Generate figures and a Markdown report summarizing the analysis."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Optional

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sqlalchemy.orm import sessionmaker

from .analyze import load_analysis_dataframe
from .config import AppConfig, load_config
from .db import init_db, session_scope
from .utils import setup_logging, utc_now


logger = logging.getLogger(__name__)


class Reporter:
    """Produces visualisations and a Markdown report."""

    def __init__(self, config: AppConfig, args: argparse.Namespace):
        self.config = config
        self.repo = args.repo
        self.metrics_version = args.metrics_version
        self.data_dir = Path(args.data_dir)
        self.fig_dir = Path(args.fig_dir)
        self.out_file = Path(args.out_file)
        engine = init_db(config.database_url)
        self.session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    def run(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.fig_dir.mkdir(parents=True, exist_ok=True)
        self.out_file.parent.mkdir(parents=True, exist_ok=True)

        with session_scope(self.session_factory) as session:
            df = load_analysis_dataframe(session, self.repo, self.metrics_version)

        if df.empty:
            logger.warning("No data found for report. Run fetch, metrics, and analyze first.")
            return

        sns.set_theme(style="whitegrid")

        figures = {}
        figures["bot_label_distribution"] = self._plot_bot_labels(df)
        figures["bot_score_histogram"] = self._plot_bot_scores(df)
        figures["account_age_histogram"] = self._plot_account_age(df)
        figures["top_locations"] = self._plot_locations(df)
        figures["stars_time_series"] = self._plot_time_series(df)

        report_content = self._build_report(df, figures)
        self.out_file.write_text(report_content)
        logger.info("Report written to %s", self.out_file)

    def _save_figure(self, fig: plt.Figure, name: str) -> str:
        path = self.fig_dir / f"{name}.png"
        fig.tight_layout()
        fig.savefig(path, dpi=200)
        plt.close(fig)
        return path.name

    def _plot_bot_labels(self, df: pd.DataFrame) -> str:
        counts = df["bot_label"].dropna().value_counts().reset_index()
        counts.columns = ["bot_label", "count"]
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.bar(counts["bot_label"], counts["count"], color="#2b8cbe")
        ax.set_title("Bot classification distribution")
        ax.set_xlabel("Bot label")
        ax.set_ylabel("Users")
        return self._save_figure(fig, "bot_label_distribution")

    def _plot_bot_scores(self, df: pd.DataFrame) -> str:
        fig, ax = plt.subplots(figsize=(6, 4))
        sns.histplot(df["bot_score"].dropna(), bins=10, color="#5b8def", ax=ax)
        ax.set_title("Bot score histogram")
        ax.set_xlabel("Bot score")
        ax.set_ylabel("Users")
        return self._save_figure(fig, "bot_score_histogram")

    def _plot_account_age(self, df: pd.DataFrame) -> str:
        ages = df["account_age_days"].dropna()
        if ages.empty:
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.text(0.5, 0.5, "No account age data", ha="center", va="center")
            ax.axis("off")
            return self._save_figure(fig, "account_age_histogram")

        fig, ax = plt.subplots(figsize=(6, 4))
        sns.histplot(ages, bins=20, color="#f77f00", ax=ax)
        ax.set_title("Account age distribution")
        ax.set_xlabel("Account age (days)")
        ax.set_ylabel("Users")
        return self._save_figure(fig, "account_age_histogram")

    def _plot_locations(self, df: pd.DataFrame) -> str:
        locations = (
            df["location"].dropna().astype(str).str.strip().replace({"": None}).dropna().str.title().value_counts().head(10)
        )
        if locations.empty:
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.text(0.5, 0.5, "No location data", ha="center", va="center")
            ax.axis("off")
            return self._save_figure(fig, "top_locations")

        fig, ax = plt.subplots(figsize=(7, 5))
        ax.barh(range(len(locations)), locations.values, color="#1b9e77")
        ax.set_yticks(range(len(locations)))
        ax.set_yticklabels(locations.index)
        ax.set_title("Top locations")
        ax.set_xlabel("Users")
        ax.set_ylabel("Location")
        return self._save_figure(fig, "top_locations")

    def _plot_time_series(self, df: pd.DataFrame) -> str:
        stars = df[["starred_at"]].dropna().copy()
        if stars.empty:
            fig, ax = plt.subplots(figsize=(6, 4))
            ax.text(0.5, 0.5, "No star timeline data", ha="center", va="center")
            ax.axis("off")
            return self._save_figure(fig, "stars_time_series")

        starred_series = stars["starred_at"]
        if starred_series.dt.tz is not None:
            starred_series = starred_series.dt.tz_localize(None)
        stars["month"] = starred_series.dt.to_period("M").dt.to_timestamp()
        timeline = stars.groupby("month").size().reset_index(name="stars")

        fig, ax = plt.subplots(figsize=(7, 4))
        sns.lineplot(data=timeline, x="month", y="stars", marker="o", ax=ax)
        ax.set_title("Stars over time")
        ax.set_xlabel("Month")
        ax.set_ylabel("Stars")
        ax.tick_params(axis="x", rotation=45)
        return self._save_figure(fig, "stars_time_series")

    def _build_report(self, df: pd.DataFrame, figures: dict[str, str]) -> str:
        total = len(df)
        enriched_df = df[df["counts_enriched"]]
        enriched_total = len(enriched_df)
        coverage_fraction = (enriched_total / total) if total else 0.0
        coverage_label = (
            f"{enriched_total:,}/{total:,} users ({coverage_fraction * 100:.2f}%)" if total else "0/0"
        )

        likely_bot = df.loc[df["bot_score"] >= 60]
        suspicious = df.loc[(df["bot_score"] >= 40) & (df["bot_score"] < 60)]
        likely_human = df.loc[df["bot_score"] < 40]

        bot_pct = (len(likely_bot) / total * 100) if total else 0
        suspicious_pct = (len(suspicious) / total * 100) if total else 0
        human_pct = (len(likely_human) / total * 100) if total else 0

        top_locations = (
            df["location"].dropna().astype(str).str.strip().replace({"": None}).dropna().str.title().value_counts().head(5)
        )
        location_summary = ", ".join(f"{loc} ({count})" for loc, count in top_locations.items()) or "None reported"

        account_age_series = enriched_df["account_age_days"].dropna()
        avg_account_age = float(account_age_series.mean()) if not account_age_series.empty else None
        follower_series = enriched_df["followers_count"].dropna()
        avg_followers = float(follower_series.mean()) if not follower_series.empty else None
        events_sampled = df["recent_event_count_90d"].dropna()
        events_coverage = len(events_sampled) / total * 100 if total else 0

        generated_at = utc_now().strftime("%Y-%m-%d %H:%M UTC")

        avg_account_age_str = f"{avg_account_age:.0f}" if avg_account_age is not None else "n/a"
        avg_followers_str = f"{avg_followers:.1f}" if avg_followers is not None else "n/a"

        return "\n".join(
            [
                f"# Stargazer Bot Analysis for {self.repo}",
                "",
                "## Executive Summary",
                f"- Total stargazers analyzed: **{total}**",
                f"- Enrichment coverage: **{coverage_label}**",
                f"- Likely bots (score ≥60): **{len(likely_bot)} ({bot_pct:.1f}%)**",
                f"- Suspicious (score 40–59): **{len(suspicious)} ({suspicious_pct:.1f}%)**",
                f"- Likely human (score ≤39): **{len(likely_human)} ({human_pct:.1f}%)**",
                f"- Top locations: {location_summary}",
                "",
                "## Methodology",
                "- Fetched stargazers via GitHub GraphQL with deterministic pagination and rate-limit handling.",
                f"- Stored normalized records in SQLite and computed metrics via `metrics.py` version `{self.metrics_version}`.",
                "- Optional REST enrichment provided site admin status, websites, and public events sampling.",
                "",
                "## Bot Model",
                "- Heuristic scoring spanning account type, naming patterns, profile completeness, social graph,",
                "  activity, and repository/gist presence.",
                "- Key thresholds: +80 for GitHub Bot accounts, +25 for accounts younger than 7 days,",
                "  +10 for blank profiles, −30 for site-admin verified staff.",
                "",
                "## Results",
                f"- Average account age: **{avg_account_age_str} days** (enriched sample).",
                f"- Average followers: **{avg_followers_str}** (enriched sample).",
                f"- Public event sampling coverage: **{events_coverage:.1f}%** of users.",
                f"- ![Bot labels](figures/{figures['bot_label_distribution']})",
                f"- ![Bot scores](figures/{figures['bot_score_histogram']})",
                f"- ![Account age](figures/{figures['account_age_histogram']})",
                f"- ![Top locations](figures/{figures['top_locations']})",
                f"- ![Stars over time](figures/{figures['stars_time_series']})",
                "",
                "## Validation",
                "- Manual spot checks recommended for high-scoring accounts.",
                "- Event sampling inspects recent public activity but omits private/org events.",
                "",
                "## Limitations",
                "- GitHub profiles may omit key signals (location, company).",
                "- Heuristic classifier is deterministic and can mislabel niche communities.",
                "- Event sampling capped to recent public events; dormant users may be legitimate.",
                "",
                "## Appendix",
                f"- Data exports available in `{self.data_dir}`.",
                f"- Figures saved in `{self.fig_dir}`.",
                f"- Report generated at {generated_at}.",
            ]
        )


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create visual report from stargazer metrics")
    parser.add_argument("--repo", default="openclaw/openclaw", help="Repository in owner/name format")
    parser.add_argument("--metrics-version", default="bot-v1", help="Metrics version to report on")
    parser.add_argument("--data-dir", default="reports/data", help="Directory containing analysis CSVs")
    parser.add_argument("--fig-dir", default="reports/figures", help="Directory for output figures")
    parser.add_argument("--out-file", default="reports/report.md", help="Path to Markdown report")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)
    config = load_config()
    reporter = Reporter(config, args)
    reporter.run()


if __name__ == "__main__":  # pragma: no cover
    main()
