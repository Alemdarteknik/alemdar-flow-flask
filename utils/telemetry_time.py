from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from typing import Optional, Union
from zoneinfo import ZoneInfo

TimezoneInput = Union[str, tzinfo]


def coerce_timezone(value: TimezoneInput) -> tzinfo:
    if isinstance(value, str):
        return ZoneInfo(value)
    return value


def parse_watchpower_timestamp(
    raw_timestamp: object, timezone_value: TimezoneInput
) -> Optional[datetime]:
    if not isinstance(raw_timestamp, str):
        return None

    text = raw_timestamp.strip()
    if not text:
        return None

    tz = coerce_timezone(timezone_value)

    try:
        parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
        return parsed.replace(tzinfo=tz).astimezone(timezone.utc)
    except ValueError:
        pass

    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=tz)
    return parsed.astimezone(timezone.utc)
