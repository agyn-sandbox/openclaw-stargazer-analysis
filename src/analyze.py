"""Generate aggregate analytics from computed bot metrics."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import and_, select
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig, load_config
from .db import Repository, Stargazer, User, UserMetric, init_db, session_scope
from .utils import setup_logging, utc_now


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AnalyzerArgs:
    repo: str
    metrics_version: str
    out_dir: Path


def load_analysis_dataframe(session: Session, repo: str, metrics_version: str) -> pd.DataFrame:
    """Load a DataFrame of users, metrics, and stargazer metadata for a repo."""

    stmt = (
        select(
            User.id.label("user_id"),
            User.login.label("login"),
            User.type.label("account_type"),
            User.site_admin.label("site_admin"),
            User.company.label("company"),
            User.location.label("location"),
            User.created_at.label("created_at"),
            User.followers_count.label("followers_count"),
            User.following_count.label("following_count"),
            User.public_repos_count.label("public_repos_count"),
            User.public_gists_count.label("public_gists_count"),
            User.hireable.label("hireable"),
            User.verified_badge.label("verified_badge"),
            UserMetric.bot_score.label("bot_score"),
            UserMetric.bot_label.label("bot_label"),
            UserMetric.last_public_activity_date.label("last_public_activity_date"),
            UserMetric.recent_event_count_90d.label("recent_event_count_90d"),
            UserMetric.follower_following_ratio.label("follower_following_ratio"),
            Stargazer.starred_at.label("starred_at"),
        )
        .select_from(User)
        .join(Stargazer, Stargazer.user_id == User.id)
        .join(Repository, Repository.id == Stargazer.repository_id)
        .join(
            UserMetric,
            and_(
                UserMetric.user_id == User.id,
                UserMetric.metrics_version == metrics_version,
            ),
            isouter=True,
        )
        .where(Repository.full_name == repo)
    )

    rows = session.execute(stmt).all()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=[col.key for col in stmt.exported_columns])
    df["starred_at"] = pd.to_datetime(df["starred_at"], utc=True)
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    if "last_public_activity_date" in df.columns:
        df["last_public_activity_date"] = pd.to_datetime(df["last_public_activity_date"], utc=True)

    now = utc_now()
    df["account_age_days"] = df["created_at"].apply(lambda ts: (now - ts).days if pd.notnull(ts) else None)
    df["counts_enriched"] = (
        df[["followers_count", "following_count", "public_repos_count", "public_gists_count"]]
        .notnull()
        .all(axis=1)
    )
    return df


class Analyzer:
    """Creates CSV aggregates for downstream reporting."""

    def __init__(self, config: AppConfig, args: argparse.Namespace):
        self.config = config
        self.args = AnalyzerArgs(
            repo=args.repo,
            metrics_version=args.metrics_version,
            out_dir=Path(args.out_dir),
        )
        engine = init_db(config.database_url)
        self.session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    def run(self) -> None:
        self.args.out_dir.mkdir(parents=True, exist_ok=True)

        with session_scope(self.session_factory) as session:
            df = load_analysis_dataframe(session, self.args.repo, self.args.metrics_version)

        if df.empty:
            logger.warning("No metrics available. Run fetch and metrics before analyze.")
            return

        timestamp = utc_now().isoformat()
        raw_path = self.args.out_dir / "stargazers_raw.csv"
        df.to_csv(raw_path, index=False)
        logger.info("Wrote raw dataset to %s", raw_path)

        self._write_bot_distribution(df)
        self._write_locations(df)
        self._write_account_age(df)
        self._write_activity(df)
        self._write_companies(df)
        self._write_bot_score_histogram(df)
        self._write_stars_time_series(df)

        summary_path = self.args.out_dir / "analysis_metadata.txt"
        enriched_total = int(df["counts_enriched"].sum())
        coverage_fraction = enriched_total / len(df)
        logger.info(
            "Enriched profile coverage: %s/%s (%.2f%%)",
            enriched_total,
            len(df),
            coverage_fraction * 100,
        )
        summary_lines = [
            f"generated_at={timestamp}",
            f"rows={len(df)}",
            f"counts_enriched={enriched_total}",
            f"counts_coverage={coverage_fraction:.6f}",
        ]
        summary_path.write_text("\n".join(summary_lines) + "\n")

    def _write_bot_distribution(self, df: pd.DataFrame) -> None:
        if "bot_label" not in df.columns:
            return
        distribution = df["bot_label"].dropna().value_counts().rename_axis("bot_label").reset_index(name="count")
        path = self.args.out_dir / "bot_label_distribution.csv"
        distribution.to_csv(path, index=False)

    def _write_locations(self, df: pd.DataFrame) -> None:
        locations = (
            df["location"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace({"": None})
            .dropna()
            .str.title()
            .value_counts()
            .head(10)
            .rename_axis("location")
            .reset_index(name="count")
        )
        path = self.args.out_dir / "top_locations.csv"
        locations.to_csv(path, index=False)

    def _write_account_age(self, df: pd.DataFrame) -> None:
        ages = df["account_age_days"].dropna()
        if ages.empty:
            return
        bins = [0, 7, 30, 90, 365, 730, 1825, float("inf")]
        labels = ["<1w", "<1m", "<3m", "<1y", "<2y", "<5y", ">=5y"]
        categorized = pd.cut(ages, bins=bins, labels=labels, right=False)
        result = categorized.value_counts(sort=False).rename_axis("account_age").reset_index(name="count")
        path = self.args.out_dir / "account_age_distribution.csv"
        result.to_csv(path, index=False)

    def _write_activity(self, df: pd.DataFrame) -> None:
        activity = df[["recent_event_count_90d", "last_public_activity_date"]].copy()
        activity["recent_event_count_90d"] = activity["recent_event_count_90d"].fillna(0)
        now = utc_now()
        activity["days_since_last_activity"] = activity["last_public_activity_date"].apply(
            lambda ts: (now - ts).days if pd.notnull(ts) else None
        )
        summary = {
            "mean_recent_events": activity["recent_event_count_90d"].mean(),
            "median_recent_events": activity["recent_event_count_90d"].median(),
            "inactive_over_180d": activity["days_since_last_activity"].apply(lambda d: d is not None and d > 180).sum(),
        }
        path = self.args.out_dir / "activity_summary.csv"
        pd.Series(summary).to_csv(path)

    def _write_companies(self, df: pd.DataFrame) -> None:
        companies = (
            df["company"]
            .dropna()
            .astype(str)
            .str.strip()
            .replace({"": None})
            .dropna()
            .value_counts()
            .head(10)
            .rename_axis("company")
            .reset_index(name="count")
        )
        path = self.args.out_dir / "top_companies.csv"
        companies.to_csv(path, index=False)

    def _write_bot_score_histogram(self, df: pd.DataFrame) -> None:
        scores = df["bot_score"].dropna()
        if scores.empty:
            return
        bins = list(range(0, 101, 10))
        categorized = pd.cut(scores, bins=bins, right=False, include_lowest=True)
        histogram = categorized.value_counts(sort=False).rename_axis("score_bin").reset_index(name="count")
        path = self.args.out_dir / "bot_score_histogram.csv"
        histogram.to_csv(path, index=False)

    def _write_stars_time_series(self, df: pd.DataFrame) -> None:
        stars = df[["starred_at"]].dropna().copy()
        if stars.empty:
            return
        starred_series = stars["starred_at"]
        if starred_series.dt.tz is not None:
            starred_series = starred_series.dt.tz_localize(None)
        stars["month"] = starred_series.dt.to_period("M").dt.to_timestamp()
        timeline = stars.groupby("month").size().reset_index(name="stars")
        path = self.args.out_dir / "stars_time_series.csv"
        timeline.to_csv(path, index=False)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze stargazer bot metrics")
    parser.add_argument("--repo", default="openclaw/openclaw", help="Repository in owner/name format")
    parser.add_argument("--metrics-version", default="bot-v1", help="Metrics version to analyze")
    parser.add_argument("--out-dir", default="reports/data", help="Directory for CSV outputs")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)
    config = load_config()
    analyzer = Analyzer(config, args)
    analyzer.run()


if __name__ == "__main__":  # pragma: no cover
    main()
