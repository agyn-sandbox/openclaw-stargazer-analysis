"""Application configuration loading utilities.

This module is boundary-layer code responsible for reading environment
variables, validating configuration, and preparing directories used by the
pipeline. Internal modules consume the resulting ``AppConfig`` dataclass and
may assume that all invariants expressed here hold true.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DATA_DIR = BASE_DIR / "data"
DEFAULT_REPORTS_DIR = BASE_DIR / "reports"


@dataclass(frozen=True)
class AppConfig:
    """Runtime configuration for the stargazer analysis pipeline."""

    github_token: str
    database_url: str
    data_dir: Path
    reports_dir: Path
    request_timeout: int = 20
    max_retries: int = 5
    backoff_min_seconds: float = 1.0
    backoff_max_seconds: float = 30.0
    default_page_size: int = 100


def _resolve_database_url(raw_url: Optional[str], raw_path: Optional[str]) -> str:
    """Resolve the configured database URL, validating SQLite paths."""

    if raw_url:
        return raw_url

    path = Path(raw_path) if raw_path else DEFAULT_DATA_DIR / "openclaw_stargazers.db"
    if not path.is_absolute():
        path = BASE_DIR / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite:///{path}"


def load_config(env_file: Optional[Path] = None) -> AppConfig:
    """Load configuration from environment variables and optional ``.env`` file."""

    candidate_env = env_file if env_file is not None else BASE_DIR / ".env"
    if candidate_env.exists():
        load_dotenv(dotenv_path=candidate_env)
    else:
        load_dotenv()

    github_token = os.getenv("GITHUB_TOKEN")
    if not github_token:
        raise ValueError("GITHUB_TOKEN must be set in the environment before running the pipeline.")

    data_dir = Path(os.getenv("DATA_DIR", DEFAULT_DATA_DIR))
    if not data_dir.is_absolute():
        data_dir = BASE_DIR / data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    reports_dir = Path(os.getenv("REPORTS_DIR", DEFAULT_REPORTS_DIR))
    if not reports_dir.is_absolute():
        reports_dir = BASE_DIR / reports_dir
    reports_dir.mkdir(parents=True, exist_ok=True)

    database_url = _resolve_database_url(os.getenv("DATABASE_URL"), os.getenv("DATABASE_PATH"))

    request_timeout = int(os.getenv("GITHUB_REQUEST_TIMEOUT", "20"))
    max_retries = int(os.getenv("GITHUB_MAX_RETRIES", "5"))
    backoff_min_seconds = float(os.getenv("GITHUB_BACKOFF_MIN", "1.0"))
    backoff_max_seconds = float(os.getenv("GITHUB_BACKOFF_MAX", "30.0"))
    default_page_size = int(os.getenv("GITHUB_PAGE_SIZE", "100"))

    return AppConfig(
        github_token=github_token,
        database_url=database_url,
        data_dir=data_dir,
        reports_dir=reports_dir,
        request_timeout=request_timeout,
        max_retries=max_retries,
        backoff_min_seconds=backoff_min_seconds,
        backoff_max_seconds=backoff_max_seconds,
        default_page_size=default_page_size,
    )

