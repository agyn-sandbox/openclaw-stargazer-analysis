"""Database models and helpers for stargazer persistence."""

from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
from typing import Iterator, Optional

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    event,
)
from sqlalchemy.engine import Engine
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship, sessionmaker


class Base(DeclarativeBase):
    """Base declarative class for ORM models."""


class Repository(Base):
    """Tracked GitHub repository."""

    __tablename__ = "repositories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    full_name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)
    github_repo_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    stargazers: Mapped[list["Stargazer"]] = relationship("Stargazer", back_populates="repository")


class User(Base):
    """GitHub account that starred the repository."""

    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    github_id_int: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    login: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    site_admin: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    name: Mapped[Optional[str]] = mapped_column(String(200))
    bio: Mapped[Optional[str]] = mapped_column(Text())
    company: Mapped[Optional[str]] = mapped_column(String(200))
    location: Mapped[Optional[str]] = mapped_column(String(200))
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    followers_count: Mapped[Optional[int]] = mapped_column(Integer)
    following_count: Mapped[Optional[int]] = mapped_column(Integer)
    public_repos_count: Mapped[Optional[int]] = mapped_column(Integer)
    public_gists_count: Mapped[Optional[int]] = mapped_column(Integer)
    hireable: Mapped[Optional[bool]] = mapped_column(Boolean)
    email_public: Mapped[Optional[bool]] = mapped_column(Boolean)
    verified_badge: Mapped[Optional[bool]] = mapped_column(Boolean)
    site: Mapped[Optional[str]] = mapped_column(String(300))

    stargazer_entries: Mapped[list["Stargazer"]] = relationship("Stargazer", back_populates="user")
    metrics: Mapped[list["UserMetric"]] = relationship("UserMetric", back_populates="user")


class Stargazer(Base):
    """Association table linking repositories and users with star metadata."""

    __tablename__ = "stargazers"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    repository_id: Mapped[int] = mapped_column(ForeignKey("repositories.id"), nullable=False)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    starred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False, default="graphql")

    repository: Mapped["Repository"] = relationship("Repository", back_populates="stargazers")
    user: Mapped["User"] = relationship("User", back_populates="stargazer_entries")

    __table_args__ = (UniqueConstraint("repository_id", "user_id", name="uq_stargazers_repo_user"),)


class FetchRun(Base):
    """Records the lifecycle of a single fetch execution."""

    __tablename__ = "fetch_runs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    repo_full_name: Mapped[str] = mapped_column(String(200), nullable=False)
    api: Mapped[str] = mapped_column(String(32), nullable=False)
    page_size: Mapped[int] = mapped_column(Integer, nullable=False)
    cursor_checkpoint: Mapped[Optional[str]] = mapped_column(String(256))
    page_checkpoint: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_starred_at_seen: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rate_limit_limit: Mapped[Optional[int]] = mapped_column(Integer)
    rate_limit_remaining: Mapped[Optional[int]] = mapped_column(Integer)
    rate_limit_reset_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    rate_limit_used: Mapped[Optional[int]] = mapped_column(Integer)
    success: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    error_message: Mapped[Optional[str]] = mapped_column(Text())
    notes: Mapped[Optional[str]] = mapped_column(Text())

    metrics: Mapped[list["UserMetric"]] = relationship("UserMetric", back_populates="source_run")


class UserMetric(Base):
    """Computed metrics and bot score for a user."""

    __tablename__ = "user_metrics"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id"), nullable=False)
    last_public_activity_date: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    recent_event_count_90d: Mapped[Optional[int]] = mapped_column(Integer)
    follower_following_ratio: Mapped[Optional[float]] = mapped_column(Float)
    bot_score: Mapped[Optional[int]] = mapped_column(Integer)
    bot_label: Mapped[Optional[str]] = mapped_column(String(32))
    metrics_version: Mapped[str] = mapped_column(String(32), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source_run_id: Mapped[Optional[int]] = mapped_column(ForeignKey("fetch_runs.id"))

    user: Mapped["User"] = relationship("User", back_populates="metrics")
    source_run: Mapped[Optional["FetchRun"]] = relationship("FetchRun", back_populates="metrics")

    __table_args__ = (UniqueConstraint("user_id", "metrics_version", name="uq_user_metrics_version"),)


def _ensure_sqlite_foreign_keys(engine: Engine) -> None:
    """Enable foreign key enforcement for SQLite connections."""

    if not engine.url.drivername.startswith("sqlite"):
        return

    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_connection, connection_record):  # type: ignore[unused-ignore]
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()


def create_db_engine(database_url: str) -> Engine:
    """Create a SQLAlchemy engine with sensible defaults."""

    engine = create_engine(database_url, echo=False, future=True)
    _ensure_sqlite_foreign_keys(engine)
    return engine


def create_session_factory(database_url: str) -> sessionmaker[Session]:
    """Create a configured session factory for the database URL."""

    engine = create_db_engine(database_url)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False, future=True)


def init_db(database_url: str) -> Engine:
    """Initialise the database by creating all known tables."""

    engine = create_db_engine(database_url)
    Base.metadata.create_all(engine)
    return engine


@contextmanager
def session_scope(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""

    session = session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
