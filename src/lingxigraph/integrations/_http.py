"""Shared HTTP reliability helpers for provider integrations."""

from __future__ import annotations

import asyncio
import random
from collections.abc import Mapping
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}


def should_retry_status(status_code: int) -> bool:
    return status_code in RETRYABLE_STATUS


def retry_delay(
    attempt: int,
    headers: Mapping[str, Any] | None = None,
    *,
    base: float = 0.5,
    cap: float = 15.0,
) -> float:
    source = headers or {}
    retry_after = str(source.get("retry-after", source.get("Retry-After", ""))).strip()
    if retry_after:
        try:
            return min(cap, max(0.0, float(retry_after)))
        except ValueError:
            try:
                target = parsedate_to_datetime(retry_after)
                if target.tzinfo is None:
                    target = target.replace(tzinfo=UTC)
                return min(cap, max(0.0, (target - datetime.now(UTC)).total_seconds()))
            except (TypeError, ValueError, OverflowError):
                pass
    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    return raw + random.uniform(0, raw / 4 if raw else 0)


async def sleep_before_retry(
    attempt: int,
    headers: Mapping[str, Any] | None = None,
    *,
    base: float = 0.5,
) -> None:
    await asyncio.sleep(retry_delay(attempt, headers, base=base))


__all__ = ["RETRYABLE_STATUS", "retry_delay", "should_retry_status", "sleep_before_retry"]
