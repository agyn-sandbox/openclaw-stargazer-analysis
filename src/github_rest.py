"""GitHub REST API helpers for enrichment and events sampling."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple

import requests

from .config import AppConfig


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RateLimitState:
    limit: Optional[int]
    remaining: Optional[int]
    reset_at: Optional[datetime]


class GithubRestClient:
    """Thin wrapper around GitHub's REST APIs used for enrichment."""

    api_url = "https://api.github.com"

    def __init__(self, config: AppConfig):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {config.github_token}",
                "Accept": "application/vnd.github+json",
                "User-Agent": "openclaw-stargazer-analysis",
            }
        )
        self.timeout = config.request_timeout
        self.max_retries = config.max_retries
        self.backoff_min = config.backoff_min_seconds
        self.backoff_max = config.backoff_max_seconds
        self._last_rate_limit: Optional[RateLimitState] = None

    def fetch_user_profile(self, login: str) -> Optional[Dict[str, object]]:
        path = f"/users/{login}"
        response = self._request("GET", path, allow_not_found=True)
        if response is None:
            return None

        data = response.json()
        account_type = data.get("type")
        verified_badge = self._coerce_optional_bool(data.get("verified"))
        if verified_badge is None:
            verified_badge = self._coerce_optional_bool(data.get("is_verified"))

        if account_type == "Organization":
            org_path = f"/orgs/{login}"
            org_response = self._request("GET", org_path, allow_not_found=True)
            if org_response is not None:
                org_data = org_response.json()
                org_verified = self._coerce_optional_bool(org_data.get("is_verified"))
                if org_verified is None:
                    org_verified = self._coerce_optional_bool(org_data.get("verified"))
                if org_verified is not None:
                    verified_badge = org_verified

        return {
            "site_admin": bool(data.get("site_admin", False)),
            "site": data.get("blog") or data.get("html_url"),
            "type": account_type,
            "verified_badge": verified_badge,
            "name": data.get("name"),
            "bio": data.get("bio"),
            "company": data.get("company"),
            "location": data.get("location"),
            "created_at": self._parse_datetime(data.get("created_at")),
            "updated_at": self._parse_datetime(data.get("updated_at")),
            "followers_count": self._safe_int(data.get("followers")),
            "following_count": self._safe_int(data.get("following")),
            "public_repos_count": self._safe_int(data.get("public_repos")),
            "public_gists_count": self._safe_int(data.get("public_gists")),
            "hireable": data.get("hireable"),
            "email_public": bool(data.get("email")),
        }

    def fetch_recent_public_events(
        self,
        login: str,
        max_pages: int,
        per_page: int = 100,
    ) -> Tuple[Optional[datetime], int]:
        total_events = 0
        last_activity: Optional[datetime] = None

        for page in range(1, max_pages + 1):
            path = f"/users/{login}/events/public"
            response = self._request(
                "GET",
                path,
                params={"per_page": per_page, "page": page},
                allow_not_found=True,
            )
            if response is None:
                return last_activity, total_events
            events = response.json()

            if not isinstance(events, list):
                break

            for event in events:
                created_at_raw = event.get("created_at")
                created_at = self._parse_datetime(created_at_raw)
                if created_at is None:
                    continue
                if last_activity is None or created_at > last_activity:
                    last_activity = created_at
                if created_at >= datetime.now(timezone.utc) - timedelta(days=90):
                    total_events += 1

            if len(events) < per_page:
                break

        return last_activity, total_events

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, object]] = None,
        *,
        allow_not_found: bool = False,
    ) -> Optional[requests.Response]:
        attempt = 0
        url = f"{self.api_url}{path}"
        while True:
            attempt += 1
            response = self.session.request(method, url, params=params, timeout=self.timeout)
            rate_limit = self._parse_rate_limit(response.headers)
            self._last_rate_limit = rate_limit

            content_lower = response.text.lower() if response.text else ""
            if "timestamp outside allowed skew" in content_lower:
                wait = 60.0
                logger.warning(
                    "REST timestamp skew detected on %s. Sleeping %.1fs before retrying.",
                    path,
                    wait,
                )
                time.sleep(wait)
                attempt = 0
                continue

            if response.status_code == 403 and rate_limit.remaining == 0:
                self._sleep_until_reset(rate_limit)
                continue

            if allow_not_found and response.status_code in {404, 410}:
                logger.info("Optional REST resource %s returned %s. Skipping.", path, response.status_code)
                return None

            if response.status_code >= 500:
                if attempt >= self.max_retries:
                    response.raise_for_status()
                delay = self._compute_backoff(attempt)
                logger.warning("Transient REST error %s on %s. Retrying in %.1fs", response.status_code, path, delay)
                time.sleep(delay)
                continue

            response.raise_for_status()
            return response

    @property
    def last_rate_limit(self) -> Optional[RateLimitState]:
        return self._last_rate_limit

    def _parse_rate_limit(self, headers: Dict[str, str]) -> RateLimitState:
        limit = self._safe_int(headers.get("X-RateLimit-Limit"))
        remaining = self._safe_int(headers.get("X-RateLimit-Remaining"))
        reset_at = self._parse_reset(headers.get("X-RateLimit-Reset"))
        return RateLimitState(limit=limit, remaining=remaining, reset_at=reset_at)

    def _sleep_until_reset(self, rate_limit: RateLimitState) -> None:
        if rate_limit.reset_at is None:
            raise RuntimeError("Rate limited but no reset time present in headers.")
        delay = max((rate_limit.reset_at - datetime.now(timezone.utc)).total_seconds(), 1.0)
        logger.warning("Hit REST rate limit. Sleeping %.1fs", delay)
        time.sleep(delay)

    def _compute_backoff(self, attempt: int) -> float:
        ceiling = min(self.backoff_max, self.backoff_min * (2**attempt))
        return random.uniform(self.backoff_min, ceiling)

    @staticmethod
    def _coerce_optional_bool(value: Optional[object]) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes"}:
                return True
            if lowered in {"false", "0", "no"}:
                return False
        if isinstance(value, (int, float)):
            if value == 1:
                return True
            if value == 0:
                return False
        return None

    @staticmethod
    def _safe_int(value: Optional[object]) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, int):
            return value
        if isinstance(value, str) and not value.strip():
            return None
        try:
            return int(value)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _parse_reset(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            epoch = int(value)
        except ValueError:
            return None
        return datetime.fromtimestamp(epoch, tz=timezone.utc)

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
