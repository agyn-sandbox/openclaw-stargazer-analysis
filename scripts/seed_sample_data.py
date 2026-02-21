"""Seed the SQLite database with deterministic sample data for demos."""

from __future__ import annotations

from datetime import timedelta
from typing import List

from sqlalchemy import delete
from sqlalchemy.orm import sessionmaker

from src.config import load_config
from src.db import FetchRun, Repository, Stargazer, User, UserMetric, init_db, session_scope
from src.utils import utc_now


def seed_sample_data(session_factory: sessionmaker) -> None:
    now = utc_now()

    with session_scope(session_factory) as session:
        # Wipe existing data so repeated invocations remain deterministic.
        session.execute(delete(UserMetric))
        session.execute(delete(Stargazer))
        session.execute(delete(FetchRun))
        session.execute(delete(User))
        session.execute(delete(Repository))

        repo = Repository(full_name="openclaw/openclaw", github_repo_id=1103012935)
        session.add(repo)
        session.flush()

        users: List[User] = []
        users.append(
            User(
                github_id_int=1,
                login="organic-captain",
                type="User",
                name="Organic Captain",
                bio="Open source maintainer",
                company="OpenClaw Labs",
                location="San Francisco, CA",
                created_at=now - timedelta(days=1800),
                followers_count=320,
                following_count=150,
                public_repos_count=42,
                public_gists_count=4,
                hireable=True,
                email_public=False,
                verified_badge=True,
                site="https://organic.example.com",
            )
        )
        users.append(
            User(
                github_id_int=2,
                login="automation-spark-bot",
                type="Bot",
                name=None,
                bio=None,
                company="Automation Squad",
                location=None,
                created_at=now - timedelta(days=12),
                followers_count=0,
                following_count=0,
                public_repos_count=0,
                public_gists_count=0,
                hireable=False,
                email_public=False,
                verified_badge=False,
                site=None,
            )
        )
        users.append(
            User(
                github_id_int=3,
                login="quiet-watcher",
                type="User",
                name="Quinn Watcher",
                bio="Infrastructure at Scale",
                company=None,
                location="Berlin, Germany",
                created_at=now - timedelta(days=4000),
                followers_count=5,
                following_count=45,
                public_repos_count=12,
                public_gists_count=1,
                hireable=False,
                email_public=False,
                verified_badge=False,
                site="https://quiet.example.net",
            )
        )
        users.append(
            User(
                github_id_int=4,
                login="ci-helper",
                type="User",
                name="CI Helper",
                bio="CI automation account",
                company="OpenClaw Automation",
                location="Toronto, Canada",
                created_at=now - timedelta(days=30),
                followers_count=1,
                following_count=1,
                public_repos_count=1,
                public_gists_count=0,
                hireable=False,
                email_public=False,
                verified_badge=False,
                site=None,
            )
        )
        users.append(
            User(
                github_id_int=5,
                login="research-ally",
                type="User",
                name="Research Ally",
                bio="ML safety researcher",
                company="Safety Initiative",
                location="London, UK",
                created_at=now - timedelta(days=2500),
                followers_count=85,
                following_count=62,
                public_repos_count=18,
                public_gists_count=3,
                hireable=False,
                email_public=True,
                verified_badge=False,
                site="https://ally.example.org",
            )
        )
        users.append(
            User(
                github_id_int=6,
                login="silent-fork",
                type="User",
                name=None,
                bio=None,
                company=None,
                location=None,
                created_at=now - timedelta(days=5),
                followers_count=0,
                following_count=0,
                public_repos_count=0,
                public_gists_count=0,
                hireable=False,
                email_public=False,
                verified_badge=False,
                site=None,
            )
        )

        session.add_all(users)
        session.flush()

        stargazers = []
        for idx, user in enumerate(users, start=1):
            starred_at = now - timedelta(days=idx * 7)
            stargazers.append(
                Stargazer(
                    repository_id=repo.id,
                    user_id=user.id,
                    starred_at=starred_at,
                    first_seen_at=starred_at,
                    last_seen_at=starred_at,
                    source="seed",
                )
            )
        session.add_all(stargazers)

        fetch_run = FetchRun(
            started_at=now - timedelta(minutes=10),
            ended_at=now - timedelta(minutes=5),
            repo_full_name=repo.full_name,
            api="seed",
            page_size=100,
            page_checkpoint=len(stargazers),
            last_starred_at_seen=stargazers[0].starred_at,
            success=True,
            notes="Seeded demo dataset",
        )
        session.add(fetch_run)
        session.flush()

        metrics_seed = [
            UserMetric(
                user_id=users[0].id,
                metrics_version="events-sampled",
                updated_at=now,
                last_public_activity_date=now - timedelta(days=10),
                recent_event_count_90d=42,
                source_run_id=fetch_run.id,
            ),
            UserMetric(
                user_id=users[1].id,
                metrics_version="events-sampled",
                updated_at=now,
                last_public_activity_date=now - timedelta(days=120),
                recent_event_count_90d=0,
                source_run_id=fetch_run.id,
            ),
            UserMetric(
                user_id=users[2].id,
                metrics_version="events-sampled",
                updated_at=now,
                last_public_activity_date=now - timedelta(days=45),
                recent_event_count_90d=3,
                source_run_id=fetch_run.id,
            ),
            UserMetric(
                user_id=users[3].id,
                metrics_version="events-sampled",
                updated_at=now,
                last_public_activity_date=now - timedelta(days=2),
                recent_event_count_90d=18,
                source_run_id=fetch_run.id,
            ),
            UserMetric(
                user_id=users[4].id,
                metrics_version="events-sampled",
                updated_at=now,
                last_public_activity_date=now - timedelta(days=60),
                recent_event_count_90d=5,
                source_run_id=fetch_run.id,
            ),
            UserMetric(
                user_id=users[5].id,
                metrics_version="events-sampled",
                updated_at=now,
                last_public_activity_date=None,
                recent_event_count_90d=0,
                source_run_id=fetch_run.id,
            ),
        ]
        session.add_all(metrics_seed)


def main() -> None:
    config = load_config()
    engine = init_db(config.database_url)
    session_factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)
    seed_sample_data(session_factory)
    print("Seeded sample data set with 6 stargazers.")


if __name__ == "__main__":  # pragma: no cover
    main()

