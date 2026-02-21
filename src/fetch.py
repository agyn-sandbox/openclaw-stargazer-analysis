"""CLI entry point for fetching stargazers into SQLite."""

from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from .config import AppConfig, load_config
from .db import FetchRun, Repository, Stargazer, User, UserMetric, init_db, session_scope
from .github_graphql import GithubGraphQLClient, StargazerEdge
from .github_rest import GithubRestClient
from .utils import setup_logging, utc_now


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class UserSnapshot:
    github_id: int
    login: str
    account_type: str
    site_admin: Optional[bool]
    name: Optional[str]
    bio: Optional[str]
    company: Optional[str]
    location: Optional[str]
    created_at: Optional[datetime]
    updated_at: Optional[datetime]
    followers: Optional[int]
    following: Optional[int]
    public_repos: Optional[int]
    public_gists: Optional[int]
    hireable: Optional[bool]
    email_public: Optional[bool]
    verified: Optional[bool]
    site: Optional[str]


class FetchRunner:
    """Handles the stargazer fetching workflow."""

    def __init__(self, config: AppConfig, args: argparse.Namespace):
        self.config = config
        self.args = args
        self.graphql = GithubGraphQLClient(config)
        self.rest = GithubRestClient(config) if self._needs_rest_client else None
        engine = init_db(config.database_url)
        self.session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)

    @property
    def _needs_rest_client(self) -> bool:
        return self.args.rest_enrichment or self.args.events != "none"

    def run(self) -> None:
        with session_scope(self.session_factory) as session:
            repository = self._get_or_create_repository(session, self.args.repo)
            resume_cursor, resume_page = self._load_resume_state(session)
            fetch_run = self._start_fetch_run(session, resume_cursor, resume_page)
            session.flush()

            total_processed = 0
            page_number = resume_page
            try:
                for page in self.graphql.iter_stargazers(
                    repo_full_name=self.args.repo,
                    page_size=self.args.page_size or self.config.default_page_size,
                    start_cursor=resume_cursor,
                    max_users=self.args.max_users,
                ):
                    page_number += 1
                    processed = self._process_page(session, repository, fetch_run, page.edges)
                    total_processed += processed

                    fetch_run.cursor_checkpoint = page.end_cursor
                    fetch_run.page_checkpoint = page_number
                    if page.edges:
                        fetch_run.last_starred_at_seen = page.edges[-1].starred_at
                    fetch_run.rate_limit_limit = page.rate_limit.limit
                    fetch_run.rate_limit_remaining = page.rate_limit.remaining
                    fetch_run.rate_limit_reset_at = page.rate_limit.reset_at
                    rate_limit_used = page.rate_limit.used
                    if rate_limit_used is None:
                        rate_limit_used = page.rate_limit.cost
                    fetch_run.rate_limit_used = rate_limit_used
                    session.flush()

            except Exception as exc:
                fetch_run.success = False
                fetch_run.error_message = str(exc)
                fetch_run.ended_at = utc_now()
                logger.exception("Fetch run failed")
                raise
            else:
                fetch_run.success = True
                fetch_run.ended_at = utc_now()
                logger.info("Fetch complete: processed %s users", total_processed)

    def _get_or_create_repository(self, session: Session, repo_full_name: str) -> Repository:
        stmt = select(Repository).where(Repository.full_name == repo_full_name)
        repository = session.scalars(stmt).first()
        if repository is None:
            repository = Repository(full_name=repo_full_name)
            session.add(repository)
            session.flush()
        return repository

    def _load_resume_state(self, session: Session) -> tuple[Optional[str], int]:
        if not self.args.resume:
            return None, 0

        stmt = (
            select(FetchRun)
            .where(FetchRun.repo_full_name == self.args.repo, FetchRun.api == "graphql")
            .order_by(FetchRun.started_at.desc())
        )
        last_run = session.scalars(stmt).first()
        if last_run and last_run.cursor_checkpoint:
            logger.info("Resuming fetch from cursor %s (page %s)", last_run.cursor_checkpoint, last_run.page_checkpoint)
            return last_run.cursor_checkpoint, last_run.page_checkpoint or 0
        return None, 0

    def _start_fetch_run(
        self,
        session: Session,
        resume_cursor: Optional[str],
        resume_page: int,
    ) -> FetchRun:
        fetch_run = FetchRun(
            started_at=utc_now(),
            repo_full_name=self.args.repo,
            api="graphql",
            page_size=self.args.page_size or self.config.default_page_size,
            cursor_checkpoint=resume_cursor,
            page_checkpoint=resume_page,
            notes="resume" if resume_cursor else None,
        )
        session.add(fetch_run)
        return fetch_run

    def _process_page(
        self,
        session: Session,
        repository: Repository,
        fetch_run: FetchRun,
        edges: list[StargazerEdge],
    ) -> int:
        processed = 0
        for edge in edges:
            snapshot = self._build_snapshot(edge)
            if snapshot is None:
                continue

            user = self._upsert_user(session, snapshot)
            self._upsert_stargazer(session, repository, user, edge)

            if self.rest and self.args.rest_enrichment:
                self._apply_rest_enrichment(session, user, snapshot)

            if self.rest and self.args.events != "none" and snapshot.account_type == "User":
                self._collect_events(session, fetch_run, user)

            processed += 1

        return processed

    def _build_snapshot(self, edge: StargazerEdge) -> Optional[UserSnapshot]:
        node = edge.node
        github_id = node.get("github_id")
        login = node.get("login")
        account_type = node.get("type")

        if github_id is None or login is None or account_type is None:
            logger.debug("Skipping edge with missing identifiers: %s", node)
            return None

        return UserSnapshot(
            github_id=int(github_id),
            login=str(login),
            account_type=str(account_type),
            site_admin=node.get("site_admin"),
            name=node.get("name"),
            bio=node.get("bio"),
            company=node.get("company"),
            location=node.get("location"),
            created_at=node.get("created_at"),
            updated_at=node.get("updated_at"),
            followers=node.get("followers"),
            following=node.get("following"),
            public_repos=node.get("public_repos"),
            public_gists=node.get("public_gists"),
            hireable=node.get("hireable"),
            email_public=node.get("email_public"),
            verified=node.get("verified"),
            site=node.get("site"),
        )

    def _upsert_user(self, session: Session, snapshot: UserSnapshot) -> User:
        stmt = select(User).where(User.github_id_int == snapshot.github_id)
        user = session.scalars(stmt).first()

        if user is None:
            user = User(github_id_int=snapshot.github_id, login=snapshot.login, type=snapshot.account_type)
            session.add(user)
        else:
            user.login = snapshot.login
            user.type = snapshot.account_type

        if snapshot.site_admin is not None:
            user.site_admin = bool(snapshot.site_admin)
        user.name = snapshot.name
        user.bio = snapshot.bio
        user.company = snapshot.company
        user.location = snapshot.location
        user.created_at = snapshot.created_at
        user.updated_at = snapshot.updated_at
        user.followers_count = snapshot.followers
        user.following_count = snapshot.following
        user.public_repos_count = snapshot.public_repos
        user.public_gists_count = snapshot.public_gists
        user.hireable = snapshot.hireable
        user.email_public = snapshot.email_public
        if snapshot.verified is not None:
            user.verified_badge = snapshot.verified
        elif user.verified_badge is None:
            user.verified_badge = False
        if snapshot.site:
            user.site = snapshot.site

        return user

    def _upsert_stargazer(self, session: Session, repository: Repository, user: User, edge: StargazerEdge) -> None:
        stmt = select(Stargazer).where(
            Stargazer.repository_id == repository.id,
            Stargazer.user_id == user.id,
        )
        stargazer = session.scalars(stmt).first()
        now = utc_now()

        if stargazer is None:
            stargazer = Stargazer(
                repository=repository,
                user=user,
                starred_at=edge.starred_at,
                first_seen_at=edge.starred_at,
                last_seen_at=now,
                source="graphql",
            )
            session.add(stargazer)
        else:
            stargazer.starred_at = edge.starred_at
            stargazer.last_seen_at = now

    def _apply_rest_enrichment(self, session: Session, user: User, snapshot: UserSnapshot) -> None:
        if self.rest is None:
            return

        profile = self.rest.fetch_user_profile(user.login)
        if profile is None:
            return
        user.site_admin = bool(profile.get("site_admin", False))
        if profile.get("site"):
            user.site = profile["site"]
        profile_type = profile.get("type")
        if profile_type:
            user.type = str(profile_type)
        verified_badge = profile.get("verified_badge")
        if verified_badge is not None:
            user.verified_badge = bool(verified_badge)
        elif profile_type != "Organization" and user.verified_badge is None:
            user.verified_badge = False

        name = profile.get("name")
        if name is not None:
            user.name = name
        bio = profile.get("bio")
        if bio is not None:
            user.bio = bio
        company = profile.get("company")
        if company is not None:
            user.company = company
        location = profile.get("location")
        if location is not None:
            user.location = location

        created_at = profile.get("created_at")
        if created_at is not None:
            user.created_at = created_at
        updated_at = profile.get("updated_at")
        if updated_at is not None:
            user.updated_at = updated_at

        followers = profile.get("followers_count")
        if followers is not None:
            user.followers_count = followers
        following = profile.get("following_count")
        if following is not None:
            user.following_count = following
        public_repos = profile.get("public_repos_count")
        if public_repos is not None:
            user.public_repos_count = public_repos
        public_gists = profile.get("public_gists_count")
        if public_gists is not None:
            user.public_gists_count = public_gists

        hireable = profile.get("hireable")
        if hireable is not None:
            user.hireable = hireable  # already bool/None from API
        email_public = profile.get("email_public")
        if email_public is not None:
            user.email_public = bool(email_public)

    def _collect_events(self, session: Session, fetch_run: FetchRun, user: User) -> None:
        if self.rest is None:
            return

        max_pages = 1 if self.args.events == "recent" else 5
        last_activity, count_90d = self.rest.fetch_recent_public_events(user.login, max_pages=max_pages)

        metrics_version = "events-sampled"
        stmt = select(UserMetric).where(
            UserMetric.user_id == user.id,
            UserMetric.metrics_version == metrics_version,
        )
        metric = session.scalars(stmt).first()
        if metric is None:
            metric = UserMetric(
                user=user,
                metrics_version=metrics_version,
                updated_at=utc_now(),
                source_run=fetch_run,
            )
            session.add(metric)

        metric.last_public_activity_date = last_activity
        metric.recent_event_count_90d = count_90d
        metric.updated_at = utc_now()
        metric.source_run = fetch_run


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch stargazers from GitHub into SQLite")
    parser.add_argument("--repo", default="openclaw/openclaw", help="Repository in owner/name format")
    parser.add_argument("--page-size", type=int, default=None, help="GraphQL page size (default 100)")
    parser.add_argument("--max-users", type=int, default=None, help="Maximum number of users to fetch")
    parser.add_argument("--resume", action="store_true", help="Resume from last stored cursor")
    parser.add_argument(
        "--events",
        choices=["none", "recent", "full"],
        default="none",
        help="Optional public events sampling intensity",
    )
    parser.add_argument("--rest-enrichment", action="store_true", help="Enable REST enrichment for profiles")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args(argv)

    if args.page_size is not None and not (1 <= args.page_size <= 100):
        parser.error("--page-size must be between 1 and 100")

    if args.max_users is not None and args.max_users <= 0:
        parser.error("--max-users must be a positive integer")

    return args


def main(argv: Optional[list[str]] = None) -> None:
    args = parse_args(argv)
    setup_logging(verbose=args.verbose)
    config = load_config()
    runner = FetchRunner(config, args)
    runner.run()


if __name__ == "__main__":  # pragma: no cover
    main()
