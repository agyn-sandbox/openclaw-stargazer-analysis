"""Compute user metrics and bot likelihood scores."""

from __future__ import annotations

import argparse
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig, load_config
from .db import FetchRun, Repository, User, UserMetric, init_db, session_scope
from .utils import setup_logging, utc_now


logger = logging.getLogger(__name__)

BOT_LOGIN_PATTERN = re.compile(r"(bot|ci|auto|build|action)s?$", re.IGNORECASE)
BOT_COMPANY_PATTERN = re.compile(r"(bot|automation|ai|script)", re.IGNORECASE)


@dataclass(frozen=True)
class MetricInput:
    user: User
    events_metric: Optional[UserMetric]


@dataclass(frozen=True)
class MetricResult:
    follower_following_ratio: Optional[float]
    bot_score: int
    bot_label: str
    last_public_activity_date: Optional[datetime]
    recent_event_count_90d: Optional[int]


class MetricsRunner:
    """Encapsulates logic for computing bot scores."""

    def __init__(self, config: AppConfig, args: argparse.Namespace):
        self.config = config
        self.args = args
        engine = init_db(config.database_url)
        self.session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    def run(self) -> None:
        with session_scope(self.session_factory) as session:
            repository = self._get_repository(session)
            if repository is None:
                raise ValueError(f"Repository {self.args.repo} was not found. Run fetch first.")

            source_run = self._get_latest_fetch_run(session)
            events_metrics = self._load_events_metrics(session)

            stmt = (
                select(User)
                .join(User.stargazer_entries)
                .join(User.stargazer_entries.property.mapper.class_.repository)
                .where(Repository.full_name == self.args.repo)
                .distinct()
            )
            users = session.scalars(stmt).all()

            logger.info("Computing metrics for %s users", len(users))
            for user in users:
                events_metric = events_metrics.get(user.id)
                result = self._compute_metrics(MetricInput(user=user, events_metric=events_metric))
                self._persist_metric(session, user, result, source_run)

    def _get_repository(self, session: Session) -> Optional[Repository]:
        stmt = select(Repository).where(Repository.full_name == self.args.repo)
        return session.scalars(stmt).first()

    def _get_latest_fetch_run(self, session: Session) -> Optional[FetchRun]:
        stmt = (
            select(FetchRun)
            .where(FetchRun.repo_full_name == self.args.repo, FetchRun.success.is_(True))
            .order_by(FetchRun.started_at.desc())
        )
        return session.scalars(stmt).first()

    def _load_events_metrics(self, session: Session) -> Dict[int, UserMetric]:
        stmt = select(UserMetric).where(UserMetric.metrics_version == "events-sampled")
        return {metric.user_id: metric for metric in session.scalars(stmt)}

    def _compute_metrics(self, metric_input: MetricInput) -> MetricResult:
        user = metric_input.user

        ratio = self._compute_ratio(user)
        last_public_activity = (
            metric_input.events_metric.last_public_activity_date
            if metric_input.events_metric and metric_input.events_metric.last_public_activity_date
            else None
        )
        recent_event_count = (
            metric_input.events_metric.recent_event_count_90d if metric_input.events_metric else None
        )

        score = 0

        if user.type == "Bot":
            score += 80

        if BOT_LOGIN_PATTERN.search(user.login):
            score += 15

        account_age_days = self._account_age_days(user)
        if account_age_days is not None:
            if account_age_days < 7:
                score += 25
            elif account_age_days < 30:
                score += 15
            elif account_age_days < 90:
                score += 5

        if self._profile_blank(user):
            score += 10

        if self._low_social_presence(user):
            score += 10

        if ratio is not None and (ratio >= 10 or ratio <= 0.1):
            score += 10

        if recent_event_count is not None and recent_event_count == 0:
            score += 10
        elif last_public_activity is None:
            # missing data contributes zero
            pass

        if self._no_public_assets(user):
            score += 10

        if user.company and BOT_COMPANY_PATTERN.search(user.company):
            score += 10

        if user.site_admin:
            score -= 30

        score = max(0, min(100, score))
        label = self._score_to_label(score)

        return MetricResult(
            follower_following_ratio=ratio,
            bot_score=score,
            bot_label=label,
            last_public_activity_date=last_public_activity,
            recent_event_count_90d=recent_event_count,
        )

    def _persist_metric(
        self,
        session: Session,
        user: User,
        result: MetricResult,
        source_run: Optional[FetchRun],
    ) -> None:
        stmt = select(UserMetric).where(
            UserMetric.user_id == user.id,
            UserMetric.metrics_version == self.args.metrics_version,
        )
        metric = session.scalars(stmt).first()
        if metric is None:
            metric = UserMetric(
                user=user,
                metrics_version=self.args.metrics_version,
                updated_at=utc_now(),
                source_run=source_run,
            )
            session.add(metric)

        metric.follower_following_ratio = result.follower_following_ratio
        metric.bot_score = result.bot_score
        metric.bot_label = result.bot_label
        metric.updated_at = utc_now()
        metric.source_run = source_run

        if result.last_public_activity_date:
            metric.last_public_activity_date = result.last_public_activity_date
        if result.recent_event_count_90d is not None:
            metric.recent_event_count_90d = result.recent_event_count_90d

    def _compute_ratio(self, user: User) -> Optional[float]:
        followers = user.followers_count
        following = user.following_count
        if followers is None or following is None:
            return None
        if following == 0:
            return float("inf") if followers > 0 else None
        return round(followers / following, 2)

    def _account_age_days(self, user: User) -> Optional[int]:
        if user.created_at is None:
            return None
        created_at = user.created_at
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)
        delta = utc_now() - created_at
        return max(delta.days, 0)

    def _profile_blank(self, user: User) -> bool:
        return not any([user.name, user.bio, user.company])

    def _low_social_presence(self, user: User) -> bool:
        followers = user.followers_count
        following = user.following_count
        if followers is None or following is None:
            return False
        return followers <= 1 and following <= 1

    def _no_public_assets(self, user: User) -> bool:
        repos = user.public_repos_count
        gists = user.public_gists_count
        if repos is None or gists is None:
            return False
        return repos == 0 and gists == 0

    def _score_to_label(self, score: int) -> str:
        if score >= 60:
            return "likely_bot"
        if score >= 40:
            return "suspicious"
        return "likely_human"


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute bot metrics for stargazers")
    parser.add_argument("--repo", default="openclaw/openclaw", help="Repository in owner/name format")
    parser.add_argument("--metrics-version", default="bot-v1", help="Metrics version label")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)
    config = load_config()
    runner = MetricsRunner(config, args)
    runner.run()


if __name__ == "__main__":  # pragma: no cover
    main()
