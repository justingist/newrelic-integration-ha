"""Minimal async client for the New Relic Metric and Event ingest APIs.

Docs:
- Metric API: https://docs.newrelic.com/docs/data-apis/ingest-apis/metric-api/report-metrics-metric-api/
- Event API:  https://docs.newrelic.com/docs/data-apis/ingest-apis/event-api/introduction-event-api/
"""
from __future__ import annotations

import asyncio
import gzip
import json
import logging
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

REGIONS = {
    "US": {
        "metrics": "https://metric-api.newrelic.com/metric/v1",
        "events": "https://insights-collector.newrelic.com/v1/accounts/events",
    },
    "EU": {
        "metrics": "https://metric-api.eu.newrelic.com/metric/v1",
        "events": "https://insights-collector.eu01.nr-data.net/v1/accounts/events",
    },
    "JP": {
        "metrics": "https://metric-api.jp.nr-data.net/metric/v1",
        "events": "https://insights-collector.jp.nr-data.net/v1/accounts/events",
    },
}

# Both APIs reject payloads of 10^6 bytes or more (measured after compression).
# Keep a margin.
MAX_PAYLOAD_BYTES = 900_000
# Event API: attribute values are capped at 4096 characters.
MAX_EVENT_ATTRIBUTE_LENGTH = 4096

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=10)
MAX_ATTEMPTS = 3


class NewRelicError(Exception):
    """Generic failure talking to New Relic."""


class InvalidAuth(NewRelicError):
    """The license key was rejected (HTTP 403)."""


class CannotConnect(NewRelicError):
    """Network-level failure."""


class NewRelicClient:
    def __init__(self, session: aiohttp.ClientSession, license_key: str, region: str = "US"):
        if region not in REGIONS:
            raise ValueError(f"Unknown New Relic region {region!r}, expected one of {list(REGIONS)}")
        self._session = session
        self._license_key = license_key
        self._urls = REGIONS[region]

    async def validate(self) -> None:
        """Send an empty metric batch to check that the key is accepted."""
        await self._post(self._urls["metrics"], [{"metrics": []}], retry=False)

    async def send_metrics(self, metrics: list[dict[str, Any]]) -> None:
        """Send gauge metrics, e.g. {"name", "type", "value", "timestamp", "attributes"}."""
        if metrics:
            await self._post_split(self._urls["metrics"], metrics, lambda chunk: [{"metrics": chunk}])

    async def send_events(self, events: list[dict[str, Any]]) -> None:
        """Send custom events; each one needs an "eventType"."""
        if events:
            await self._post_split(self._urls["events"], events, lambda chunk: chunk)

    async def _post_split(self, url: str, items: list, wrap) -> None:
        body = _encode(wrap(items))
        if len(body) > MAX_PAYLOAD_BYTES and len(items) > 1:
            middle = len(items) // 2
            await self._post_split(url, items[:middle], wrap)
            await self._post_split(url, items[middle:], wrap)
            return
        await self._post_encoded(url, body)

    async def _post(self, url: str, payload: Any, retry: bool = True) -> None:
        await self._post_encoded(url, _encode(payload), retry=retry)

    async def _post_encoded(self, url: str, body: bytes, retry: bool = True) -> None:
        headers = {
            "Api-Key": self._license_key,
            "Content-Type": "application/json",
            "Content-Encoding": "gzip",
        }
        attempts = MAX_ATTEMPTS if retry else 1
        for attempt in range(1, attempts + 1):
            try:
                async with self._session.post(url, data=body, headers=headers, timeout=REQUEST_TIMEOUT) as resp:
                    if resp.status in (200, 202):
                        return
                    text = (await resp.text())[:500]
                    if resp.status in (401, 403):
                        raise InvalidAuth(f"New Relic rejected the license key (HTTP {resp.status}): {text}")
                    if resp.status == 429 or resp.status >= 500:
                        error: NewRelicError = NewRelicError(f"HTTP {resp.status} from {url}: {text}")
                    else:
                        # 4xx other than auth/rate-limit: retrying won't help
                        raise NewRelicError(f"HTTP {resp.status} from {url}: {text}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as err:
                error = CannotConnect(f"Could not reach {url}: {err!r}")
            if attempt < attempts:
                await asyncio.sleep(2 ** attempt)
        raise error


def _encode(payload: Any) -> bytes:
    return gzip.compress(json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"))


def tags_to_attributes(tags: list[str]) -> dict[str, str]:
    """Convert Datadog-style "key:value" tags into New Relic attributes.

    New Relic attributes are key/value pairs with a single value per key, so
    repeated keys (e.g. two "entity_label:..." tags) are merged into one
    comma-separated, sorted string. Tags without a colon become key="true".
    """
    grouped: dict[str, set[str]] = {}
    for tag in tags:
        key, sep, value = tag.partition(":")
        key = key.strip()
        if not key:
            continue
        grouped.setdefault(key, set()).add(value if sep else "true")
    return {key: ",".join(sorted(values)) for key, values in grouped.items()}


def truncate(value: str, length: int = MAX_EVENT_ATTRIBUTE_LENGTH) -> str:
    return value if len(value) <= length else value[: length - 3] + "..."
