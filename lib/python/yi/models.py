"""Data models used by the Yi client."""

from __future__ import annotations

from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple
from uuid import uuid4

from .errors import YiError


def _utc_now() -> str:
    """Return the current time as a UTC RFC3339 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


def _as_mapping(raw: Mapping[str, Any], what: str) -> MappingABC:
    if not isinstance(raw, MappingABC):
        raise YiError("{} must be a JSON object".format(what))
    return raw


def _as_int(raw: Mapping[str, Any], key: str) -> int:
    try:
        return int(raw.get(key, 0) or 0)
    except (TypeError, ValueError):
        return 0


@dataclass(frozen=True)
class Event:
    """An immutable record of something that happened.

    Attributes:
        id: Event identifier, unique per ``source``.
        source: Producer that emitted the event.
        types: One or more dot-separated event types.
        time: RFC3339 timestamp.
        data: JSON object payload.
    """

    id: str
    source: str
    types: Tuple[str, ...]
    time: str
    data: Dict[str, Any]

    @classmethod
    def new(
        cls,
        source: str,
        types: Sequence[str] = ("raw.captured",),
        data: Optional[Mapping[str, Any]] = None,
    ) -> "Event":
        """Build a new event with a random id and the current UTC time."""
        return cls(
            id=uuid4().hex,
            source=source,
            types=_normalize_types(types),
            time=_utc_now(),
            data=dict(data) if data is not None else {},
        )

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Event":
        """Parse an event from a decoded JSON object."""
        raw = _as_mapping(raw, "event")
        data = raw.get("data", {})
        if data is None:
            data = {}
        if not isinstance(data, MappingABC):
            raise YiError("event data must be a JSON object")

        types = raw.get("types")
        if types is None:
            legacy = raw.get("type")
            types = [legacy] if isinstance(legacy, str) and legacy else []
        if not isinstance(types, (list, tuple)):
            raise YiError("event types must be a JSON array")

        return cls(
            id=str(raw.get("id", "")),
            source=str(raw.get("source", "")),
            types=_normalize_types(types),
            time=str(raw.get("time", "")),
            data=dict(data),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return the event as a JSON-serializable dictionary."""
        return {
            "id": self.id,
            "source": self.source,
            "types": list(self.types),
            "time": self.time,
            "data": dict(self.data),
        }


def _normalize_types(types: Sequence[str]) -> Tuple[str, ...]:
    """Trim, drop empties, dedupe and sort event types."""
    out = []
    seen = set()
    for value in types:
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return tuple(sorted(out))


@dataclass(frozen=True)
class PublishResult:
    """The outcome of publishing one event."""

    duplicate: bool
    matched: int

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PublishResult":
        """Parse a publish result from a decoded JSON object."""
        raw = _as_mapping(raw, "publish result")
        return cls(
            duplicate=bool(raw.get("duplicate", False)),
            matched=_as_int(raw, "matched"),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return the result as a JSON-serializable dictionary."""
        return {"duplicate": self.duplicate, "matched": self.matched}


@dataclass(frozen=True)
class ConsumerStats:
    """A snapshot of a registered consumer's progress."""

    name: str
    types: List[str]
    cursor: int
    pending: int
    delivered: int
    failed: int
    last_error: Optional[str] = None
    last_event: Optional[str] = None

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ConsumerStats":
        """Parse consumer stats from a decoded JSON object."""
        raw = _as_mapping(raw, "consumer stats")

        types_raw = raw.get("types")
        if types_raw is None:
            types = []
        elif isinstance(types_raw, (list, tuple)):
            types = [str(item) for item in types_raw]
        else:
            raise YiError("consumer stats types must be a JSON array")

        last_error = raw.get("last_error")
        if last_error is not None:
            last_error = str(last_error)
        last_event = raw.get("last_event")
        if last_event is not None:
            last_event = str(last_event)

        return cls(
            name=str(raw.get("name", "")),
            types=types,
            cursor=_as_int(raw, "cursor"),
            pending=_as_int(raw, "pending"),
            delivered=_as_int(raw, "delivered"),
            failed=_as_int(raw, "failed"),
            last_error=last_error,
            last_event=last_event,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return the stats as a JSON-serializable dictionary."""
        out = {
            "name": self.name,
            "types": list(self.types),
            "cursor": self.cursor,
            "pending": self.pending,
            "delivered": self.delivered,
            "failed": self.failed,
        }
        if self.last_error is not None:
            out["last_error"] = self.last_error
        if self.last_event is not None:
            out["last_event"] = self.last_event
        return out


@dataclass(frozen=True)
class Failure:
    """A nacked event, as recorded by the server."""

    consumer: str
    event_id: str
    source: str
    error: str
    at: str

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "Failure":
        """Parse a failure record from a decoded JSON object."""
        raw = _as_mapping(raw, "failure")
        return cls(
            consumer=str(raw.get("consumer", "")),
            event_id=str(raw.get("event_id", "")),
            source=str(raw.get("source", "")),
            error=str(raw.get("error", "")),
            at=str(raw.get("at", "")),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return the failure as a JSON-serializable dictionary."""
        return {
            "consumer": self.consumer,
            "event_id": self.event_id,
            "source": self.source,
            "error": self.error,
            "at": self.at,
        }
