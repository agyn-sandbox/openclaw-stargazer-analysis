"""GitHub GraphQL client focused on stargazer retrieval."""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Dict, Iterator, Optional

import requests

from .config import AppConfig


logger = logging.getLogger(__name__)


STARGAZER_QUERY = """
query ($owner: String!, $name: String!, $page_size: Int!, $cursor: String) {
  rateLimit {
    cost
    limit
    remaining
    resetAt
    used
  }
  repository(owner: $owner, name: $name) {
    id
    databaseId
    stargazers(first: $page_size, after: $cursor, orderBy: {field: STARRED_AT, direction: DESC}) {
      pageInfo {
        endCursor
        hasNextPage
      }
      edges {
        cursor
        starredAt
        node {
          __typename
          ... on User {
            login
            databaseId
            name
            bio
            company
            location
            email
            createdAt
            updatedAt
            followers {
              totalCount
            }
            following {
              totalCount
            }
            repositories(first: 0) {
              totalCount
            }
            gists(first: 0) {
              totalCount
            }
            isHireable
            isSiteAdmin
            isVerified
            websiteUrl
          }
          ... on Organization {
            login
            databaseId
            name
            description
            company
            location
            email
            createdAt
            updatedAt
            isVerified
            websiteUrl
          }
          ... on Bot {
            login
            databaseId
            createdAt
            updatedAt
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

            edges = page.edges
            if max_users is not None:
                remaining = max_users - fetched
                if remaining <= 0:
                    break
                if len(edges) > remaining:
                    edges = edges[:remaining]
                    page = replace(page, edges=edges)

            yield page

            count = len(edges)
            fetched += count

            if max_users is not None and fetched >= max_users:
                break

            if not page.has_next_page or not page.end_cursor:
                break

            cursor = page.end_cursor

    def _fetch_page(self, owner: str, name: str, page_size: int, cursor: Optional[str]) -> StargazerPage:
        payload = {
            "query": STARGAZER_QUERY,
            "variables": {
                "owner": owner,
                "name": name,
                "page_size": page_size,
                "cursor": cursor,
            },
        }

        attempt = 0
        while True:
            attempt += 1
            response = self.session.post(self.api_url, json=payload, timeout=self.timeout)
            header_rate_limit = self._extract_rate_limit(response.headers)

            if response.status_code == 401:
                raise RuntimeError("GitHub GraphQL API returned 401 Unauthorized. Check token scope.")

            if response.status_code == 403 and header_rate_limit.remaining == 0:
                self._sleep_until_reset(header_rate_limit)
                continue

            if response.status_code >= 500:
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
                limit=rate_limit_block.get("limit"),
                remaining=rate_limit_block.get("remaining"),
                reset_at=self._parse_datetime(rate_limit_block.get("resetAt")),
                used=rate_limit_block.get("used"),
                cost=rate_limit_block.get("cost"),
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
        base = {
            "type": typename,
            "login": node.get("login"),
            "github_id": node.get("databaseId"),
            "created_at": GithubGraphQLClient._parse_datetime(node.get("createdAt")),
            "updated_at": GithubGraphQLClient._parse_datetime(node.get("updatedAt")),
        }

        if typename == "User":
            base.update(
                {
                    "name": node.get("name"),
                    "bio": node.get("bio"),
                    "company": node.get("company"),
                    "location": node.get("location"),
                    "followers": (node.get("followers") or {}).get("totalCount"),
                    "following": (node.get("following") or {}).get("totalCount"),
                    "public_repos": (node.get("repositories") or {}).get("totalCount"),
                    "public_gists": (node.get("gists") or {}).get("totalCount"),
                    "hireable": node.get("isHireable"),
                    "email_public": bool(node.get("email")),
                    "verified": node.get("isVerified"),
                    "site_admin": node.get("isSiteAdmin"),
                    "site": node.get("websiteUrl"),
                }
            )
        elif typename == "Organization":
            base.update(
                {
                    "name": node.get("name"),
                    "bio": node.get("description"),
                    "company": node.get("company"),
                    "location": node.get("location"),
                    "followers": None,
                    "following": None,
                    "public_repos": None,
                    "public_gists": None,
                    "hireable": None,
                    "email_public": bool(node.get("email")),
                    "verified": node.get("isVerified"),
                    "site_admin": False,
                    "site": node.get("websiteUrl"),
                }
            )
        else:  # Bot or other actor
            base.update(
                {
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
                    "site_admin": False,
                    "site": None,
                }
            )

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
