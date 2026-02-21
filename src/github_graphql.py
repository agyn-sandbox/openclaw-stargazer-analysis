"""GitHub GraphQL client focused on stargazer retrieval."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Dict, Iterator, Optional

import requests
from requests import exceptions as requests_exceptions

from .config import AppConfig


logger = logging.getLogger(__name__)


STARGAZER_QUERY = """
query ($owner: String!, $name: String!, $cursor: String) {
  rateLimit {
    remaining
    resetAt
    cost
  }
  repository(owner: $owner, name: $name) {
    stargazers(first: 100, after: $cursor, orderBy: { field: STARRED_AT, direction: ASC }) {
      pageInfo {
        endCursor
        hasNextPage
      }
      edges {
        cursor
        starredAt
        node {
          __typename
          login
          databaseId
          ... on User {
            isHireable
          }
        }
      }
    }
  }
}
"""


@dataclass(frozen=True)
class RateLimitInfo:
    limit: Optional[int]
    remaining: Optional[int]
    reset_at: Optional[datetime]
    used: Optional[int]
    cost: Optional[int]


@dataclass(frozen=True)
class StargazerEdge:
    cursor: str
    starred_at: datetime
    node: Dict[str, object]


@dataclass(frozen=True)
class StargazerPage:
    edges: list[StargazerEdge]
    has_next_page: bool
    end_cursor: Optional[str]
    rate_limit: RateLimitInfo


class GithubGraphQLClient:
    """Lightweight client for GitHub GraphQL requests."""

    api_url = "https://api.github.com/graphql"

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
        self.pacing_min_interval = 0.55
        self.pacing_max_interval = 1.05
        self._last_request_time: Optional[float] = None

    def iter_stargazers(
        self,
        repo_full_name: str,
        page_size: int,
        start_cursor: Optional[str] = None,
        max_users: Optional[int] = None,
    ) -> Iterator[StargazerPage]:
        owner, name = repo_full_name.split("/", 1)
        cursor = start_cursor
        fetched = 0

        while True:
            page = self._fetch_page(owner, name, page_size, cursor)
            should_continue = True

            edges = page.edges
            if max_users is not None:
                remaining = max_users - fetched
                if remaining <= 0:
                    break
                if len(edges) > remaining:
                    edges = edges[:remaining]
                    page = replace(page, edges=edges)
                    should_continue = False

            yield page

            count = len(edges)
            fetched += count

            if max_users is not None and fetched >= max_users:
                break

            if not page.has_next_page or not page.end_cursor:
                break

            if should_continue:
                self._maybe_wait_for_rate_limit(page.rate_limit)

            cursor = page.end_cursor

    def _fetch_page(self, owner: str, name: str, _page_size: int, cursor: Optional[str]) -> StargazerPage:
        payload = {
            "query": STARGAZER_QUERY,
            "variables": {
                "owner": owner,
                "name": name,
                "cursor": cursor,
            },
        }

        attempt = 0
        while True:
            attempt += 1
            self._respect_pacing()
            try:
                response = self.session.post(self.api_url, json=payload, timeout=self.timeout)
            except requests_exceptions.RequestException as exc:
                if attempt >= self.max_retries:
                    raise RuntimeError("GraphQL request failed after retries due to connection error") from exc
                delay = self._compute_backoff(attempt)
                logger.warning("GraphQL request error %s. Retrying in %.1fs", exc.__class__.__name__, delay)
                time.sleep(delay)
                continue
            header_rate_limit = self._extract_rate_limit(response.headers)

            if response.status_code == 401:
                raise RuntimeError("GitHub GraphQL API returned 401 Unauthorized. Check token scope.")

            if response.status_code == 403 and header_rate_limit.remaining == 0:
                self._sleep_until_reset(header_rate_limit)
                continue

            if response.status_code >= 500:
                content_lower = response.text.lower() if response.text else ""
                if "timestamp outside allowed skew" in content_lower:
                    wait = 60.0
                    logger.warning(
                        "GraphQL server timestamp skew (HTTP %s). Sleeping %.1fs before retrying cursor %s.",
                        response.status_code,
                        wait,
                        cursor,
                    )
                    self._last_request_time = None
                    attempt = 0
                    time.sleep(wait)
                    continue
                if attempt >= self.max_retries:
                    response.raise_for_status()
                delay = self._compute_backoff(attempt)
                logger.warning("Transient GraphQL error %s. Retrying in %.1fs", response.status_code, delay)
                time.sleep(delay)
                continue

            response.raise_for_status()
            payload_json = response.json()

            if "errors" in payload_json:
                errors = payload_json["errors"]
                message = ", ".join(err.get("message", "Unknown error") for err in errors)
                if any("timestamp outside allowed skew" in err.get("message", "").lower() for err in errors):
                    wait = 60.0
                    logger.warning(
                        "GraphQL timestamp skew detected. Sleeping %.1fs before retrying cursor %s.",
                        wait,
                        cursor,
                    )
                    time.sleep(wait)
                    self._last_request_time = None
                    attempt = 0
                    continue
                rate_limited = any(
                    err.get("type") == "RATE_LIMITED" or "rate limit" in err.get("message", "").lower()
                    for err in errors
                )
                if rate_limited and header_rate_limit.reset_at is not None:
                    self._sleep_until_reset(header_rate_limit)
                    continue
                if attempt >= self.max_retries:
                    raise RuntimeError(f"GraphQL query failed after retries: {message}")
                delay = self._compute_backoff(attempt)
                logger.warning("GraphQL returned errors: %s. Retrying in %.1fs", message, delay)
                time.sleep(delay)
                continue

            data = payload_json["data"]
            repo = data.get("repository")
            if repo is None:
                raise RuntimeError("Repository not found or access denied in GraphQL response.")

            stargazers = repo["stargazers"]
            page_info = stargazers["pageInfo"]
            edges = [self._translate_edge(edge) for edge in stargazers["edges"]]

            rate_limit_block = data.get("rateLimit") or {}
            rate_limit = RateLimitInfo(
                remaining=rate_limit_block.get("remaining"),
                reset_at=self._parse_datetime(rate_limit_block.get("resetAt")),
                cost=rate_limit_block.get("cost"),
                limit=None,
                used=None,
            )

            return StargazerPage(
                edges=edges,
                has_next_page=page_info["hasNextPage"],
                end_cursor=page_info.get("endCursor"),
                rate_limit=rate_limit,
            )

    @staticmethod
    def _translate_edge(edge: Dict[str, object]) -> StargazerEdge:
        starred_at = GithubGraphQLClient._parse_datetime(edge["starredAt"])
        cursor = edge["cursor"]
        node_raw = edge["node"]
        node = GithubGraphQLClient._translate_node(node_raw)
        return StargazerEdge(cursor=cursor, starred_at=starred_at, node=node)

    @staticmethod
    def _translate_node(node: Dict[str, object]) -> Dict[str, object]:
        typename = node["__typename"]
        base: Dict[str, object] = {
            "type": typename,
            "login": node.get("login"),
            "github_id": node.get("databaseId"),
            "created_at": None,
            "updated_at": None,
            "name": None,
            "bio": None,
            "company": None,
            "location": None,
            "followers": None,
            "following": None,
            "public_repos": None,
            "public_gists": None,
            "hireable": None,
            "email_public": None,
            "verified": None,
            "site_admin": None,
            "site": None,
        }

        if typename == "User":
            base["hireable"] = node.get("isHireable")

        return base

    @staticmethod
    def _extract_rate_limit(headers: Dict[str, str]) -> RateLimitInfo:
        limit = GithubGraphQLClient._safe_int(headers.get("X-RateLimit-Limit"))
        remaining = GithubGraphQLClient._safe_int(headers.get("X-RateLimit-Remaining"))
        used = GithubGraphQLClient._safe_int(headers.get("X-RateLimit-Used"))
        reset_at = GithubGraphQLClient._parse_rate_limit_reset(headers.get("X-RateLimit-Reset"))
        return RateLimitInfo(limit=limit, remaining=remaining, reset_at=reset_at, used=used, cost=None)

    @staticmethod
    def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)

    @staticmethod
    def _parse_rate_limit_reset(value: Optional[str]) -> Optional[datetime]:
        if not value:
            return None
        try:
            timestamp = int(value)
        except ValueError:
            return None
        return datetime.fromtimestamp(timestamp, tz=timezone.utc)

    def _sleep_until_reset(self, rate_limit: RateLimitInfo) -> None:
        if rate_limit.reset_at is None:
            raise RuntimeError("Rate limited but reset time unavailable.")
        delta = (rate_limit.reset_at - datetime.now(timezone.utc)).total_seconds()
        wait_time = max(delta, 1.0)
        logger.warning("Hit GraphQL rate limit. Sleeping %.1fs until reset.", wait_time)
        time.sleep(wait_time)
        self._last_request_time = None

    def _compute_backoff(self, attempt: int) -> float:
        ceiling = min(self.backoff_max, self.backoff_min * (2 ** attempt))
        return random.uniform(self.backoff_min, ceiling)

    @staticmethod
    def _safe_int(value: Optional[str]) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    def _respect_pacing(self) -> None:
        now = time.monotonic()
        if self._last_request_time is None:
            self._last_request_time = now
            return

        target_interval = random.uniform(self.pacing_min_interval, self.pacing_max_interval)
        elapsed = now - self._last_request_time
        if elapsed < target_interval:
            time.sleep(target_interval - elapsed)
        self._last_request_time = time.monotonic()

    def _maybe_wait_for_rate_limit(self, info: RateLimitInfo) -> None:
        if info.remaining is None or info.cost is None:
            return

        if info.remaining <= max(info.cost, 1):
            self._sleep_until_reset(info)
