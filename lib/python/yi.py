"""Zero-dependency Python client for the Yi event service.

One module, two roles:

- :class:`Client` — producer and admin calls against the service port.
- :class:`Consumer` — pull-based consumer with explicit ack/nack.

The client is synchronous and uses only the Python standard library.
"""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from collections.abc import Mapping as MappingABC
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import quote
from uuid import uuid4

__version__ = "0.1.5"

__all__ = [
    "Client",
    "Consumer",
    "Message",
    "Event",
    "PublishResult",
    "ConsumerStats",
    "Failure",
    "YiError",
    "HTTPError",
    "ConflictError",
]


# ---- errors ----------------------------------------------------------------


class YiError(Exception):
    """Base class for every error raised by the Yi client."""


class HTTPError(YiError):
    """The server answered with a non-success HTTP status.

    Attributes:
        status: The numeric HTTP status code.
        body: The response body as text. It is decoded defensively because
            error responses are not guaranteed to be JSON.
    """

    def __init__(self, message: str, status: int, body: str = "") -> None:
        super().__init__(message)
        self.status = status
        self.body = body


class ConflictError(HTTPError):
    """The server answered with HTTP 409 Conflict."""


# ---- small helpers ---------------------------------------------------------


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


def _normalize_patterns(patterns: Optional[Sequence[str]]) -> List[str]:
    """Trim, drop empties, dedupe and sort subscription patterns.

    The server normalizes patterns the same way; doing it locally keeps the
    request body stable and makes an all-empty input fall back to omitting the
    field. Pattern syntax itself is validated by the server (Go RE2).
    """
    out = []
    seen = set()
    for value in patterns or ():
        text = str(value).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return sorted(out)


def _format_timeout(value: float) -> str:
    """Format a timeout for use in a query string."""
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return repr(number)


# ---- models ----------------------------------------------------------------


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
    """A snapshot of a registered consumer's progress.

    Attributes:
        patterns: The Go RE2 patterns this consumer subscribes to, if any.
            Older servers omit the field, in which case it is an empty tuple.
    """

    name: str
    types: List[str]
    cursor: int
    pending: int
    delivered: int
    failed: int
    last_error: Optional[str] = None
    last_event: Optional[str] = None
    patterns: Tuple[str, ...] = ()

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

        patterns_raw = raw.get("patterns")
        if patterns_raw is None:
            patterns: Tuple[str, ...] = ()
        elif isinstance(patterns_raw, (list, tuple)):
            patterns = tuple(str(item) for item in patterns_raw)
        else:
            raise YiError("consumer stats patterns must be a JSON array")

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
            patterns=patterns,
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
        if self.patterns:
            out["patterns"] = list(self.patterns)
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


# ---- client ----------------------------------------------------------------


class Client:
    """Client for the Yi JSON service API.

    Args:
        base_url: Root URL of the Yi service port.
        timeout: Socket timeout for ordinary requests, in seconds. For
            ``next()`` the socket timeout is extended past the server-side
            long-poll timeout so the server can answer 204 first.
        token: Optional bearer token. When non-empty, every request carries
            ``Authorization: Bearer <token>``. The empty default leaves
            requests untouched, which matches a server without ``auth-token``.
    """

    def __init__(
        self,
        base_url: str = "http://127.0.0.1:8081",
        timeout: float = 10.0,
        token: str = "",
    ) -> None:
        """Create a client for the service at ``base_url``."""
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)
        self._token = str(token or "")
        if "\r" in self._token or "\n" in self._token:
            raise YiError("token must not contain CR or LF")

    # ---- HTTP plumbing ----

    def _request(
        self,
        method: str,
        path: str,
        *,
        body: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
    ) -> Tuple[int, bytes]:
        """Perform one HTTP request and return ``(status, body_bytes)``.

        Every transport or HTTP-level failure is translated into a YiError
        (or one of its subclasses); urllib exceptions never escape.
        """
        url = self._base_url + path
        headers = {"Accept": "application/json"}
        if self._token:
            headers["Authorization"] = "Bearer " + self._token
        data = None
        if body is not None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"

        request = urllib.request.Request(
            url, data=data, headers=headers, method=method
        )
        socket_timeout = self._timeout if timeout is None else timeout

        try:
            with urllib.request.urlopen(request, timeout=socket_timeout) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as exc:
            raw_body = b""
            try:
                raw_body = exc.read()
            except OSError:
                pass
            finally:
                try:
                    exc.close()
                except OSError:
                    pass

            body_text = raw_body.decode("utf-8", errors="replace")
            message = self._error_message(body_text, exc.code, exc.reason)
            text = "{} {} failed with HTTP {}: {}".format(
                method, url, exc.code, message
            )
            if exc.code == 409:
                raise ConflictError(
                    text, status=exc.code, body=body_text
                ) from exc
            raise HTTPError(text, status=exc.code, body=body_text) from exc
        except (urllib.error.URLError, OSError) as exc:
            reason = getattr(exc, "reason", exc)
            timed_out = isinstance(exc, (socket.timeout, TimeoutError)) or isinstance(
                reason, (socket.timeout, TimeoutError)
            )
            if timed_out:
                raise YiError(
                    "{} {} timed out after {}s".format(method, url, socket_timeout)
                ) from exc
            raise YiError("{} {} failed: {}".format(method, url, reason)) from exc

    @staticmethod
    def _error_message(body: str, status: int, reason: Any) -> str:
        """Best-effort extraction of an error message from a response body."""
        text = body.strip()
        if text:
            try:
                parsed = json.loads(text)
            except ValueError:
                parsed = None
            if isinstance(parsed, dict) and isinstance(parsed.get("error"), str):
                text = parsed["error"]
            return text
        return "HTTP {}: {}".format(status, reason)

    @staticmethod
    def _parse_json(data: bytes, what: str) -> Any:
        """Decode a JSON response body, wrapping malformed input in YiError."""
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise YiError("{}: response is not valid UTF-8".format(what)) from exc
        try:
            return json.loads(text)
        except ValueError as exc:
            raise YiError("{}: response is not valid JSON".format(what)) from exc

    @classmethod
    def _parse_list(cls, data: bytes, what: str) -> List[Any]:
        parsed = cls._parse_json(data, what)
        if not isinstance(parsed, list):
            raise YiError("{}: expected a JSON array".format(what))
        return parsed

    @staticmethod
    def _consumer_path(name: str, suffix: str = "") -> str:
        return "/consumers/" + quote(str(name), safe="") + suffix

    # ---- service endpoints ----

    def health(self) -> str:
        """Return the plain-text health response, without trailing whitespace."""
        _, data = self._request("GET", "/healthz")
        return data.decode("utf-8", errors="replace").strip()

    def publish(self, event: Union[Event, Mapping[str, Any]]) -> PublishResult:
        """Publish an event and return the duplicate/match information."""
        if isinstance(event, Event):
            payload = event.to_dict()
        elif isinstance(event, MappingABC):
            payload = dict(event)
        else:
            raise TypeError("event must be an Event or a mapping")

        _, data = self._request("POST", "/events", body=payload)
        raw = self._parse_json(data, "publish")
        return PublishResult.from_dict(raw)

    def capture(
        self,
        text: str,
        *,
        source: str = "python",
        types: Sequence[str] = ("raw.captured",),
        **data: Any
    ) -> PublishResult:
        """Publish an event whose data contains ``text``."""
        payload: Dict[str, Any] = {"text": text}
        payload.update(data)
        return self.publish(Event.new(source=source, types=types, data=payload))

    def events(self, limit: int = 50) -> List[Event]:
        """Return the most recent events, oldest first."""
        _, data = self._request("GET", "/events?limit={}".format(int(limit)))
        return [Event.from_dict(item) for item in self._parse_list(data, "events")]

    def stats(self) -> List[ConsumerStats]:
        """Return progress snapshots for every registered consumer."""
        _, data = self._request("GET", "/stats")
        return [
            ConsumerStats.from_dict(item)
            for item in self._parse_list(data, "stats")
        ]

    def failures(self) -> List[Failure]:
        """Return the failures recorded by nacks."""
        _, data = self._request("GET", "/failures")
        return [Failure.from_dict(item) for item in self._parse_list(data, "failures")]

    def register(
        self,
        name: str,
        types: Sequence[str],
        patterns: Optional[Sequence[str]] = None,
    ) -> ConsumerStats:
        """Register a consumer, or verify an existing registration.

        ``types`` match event types exactly; ``patterns`` are Go RE2 regular
        expressions matched against each event type (unanchored unless the
        pattern uses ``^``/``$``). Subscriptions are the union of both.
        At least one non-empty type or pattern is required. The ``patterns``
        field is omitted from the request when it is empty, so existing
        servers that only understand ``types`` keep working.

        Raises:
            ConflictError: The name exists with a different subscription.
            HTTPError: The server rejected the body, for example an invalid
                pattern (HTTP 400).
        """
        payload: Dict[str, Any] = {"types": list(types)}
        normalized = _normalize_patterns(patterns)
        if normalized:
            payload["patterns"] = normalized
        _, data = self._request(
            "PUT", self._consumer_path(name), body=payload
        )
        raw = self._parse_json(data, "register")
        return ConsumerStats.from_dict(raw)

    def deregister(self, name: str) -> None:
        """Delete a consumer registration."""
        self._request("DELETE", self._consumer_path(name))

    def next(self, name: str, *, timeout: float = 25.0) -> Optional[Event]:
        """Fetch the next event for a consumer, or ``None`` on long-poll timeout.

        The server waits up to ``timeout`` seconds and answers 204 when no
        event arrives. A socket timeout is raised as ``YiError``.
        """
        wait = float(timeout)
        # Let the server reach its own 204 before the socket gives up.
        socket_timeout = max(self._timeout, wait + 5.0)
        path = self._consumer_path(
            name, "/next?timeout={}".format(_format_timeout(wait))
        )
        status, data = self._request("GET", path, timeout=socket_timeout)
        if status == 204:
            return None
        raw = self._parse_json(data, "next")
        if not isinstance(raw, dict) or "event" not in raw:
            raise YiError("next: response does not contain an event")
        return Event.from_dict(raw["event"])

    def ack(self, name: str, event_id: Optional[str] = None) -> None:
        """Acknowledge an event for a consumer.

        The ``id`` field is omitted when ``event_id`` is empty.
        """
        payload: Dict[str, Any] = {}
        if event_id:
            payload["id"] = event_id
        self._request("POST", self._consumer_path(name, "/ack"), body=payload)

    def nack(
        self, name: str, error: str, event_id: Optional[str] = None
    ) -> None:
        """Reject an event and record an error for a consumer.

        The ``id`` field is omitted when ``event_id`` is empty.
        """
        payload: Dict[str, Any] = {"error": str(error)}
        if event_id:
            payload["id"] = event_id
        self._request("POST", self._consumer_path(name, "/nack"), body=payload)


# ---- consumer --------------------------------------------------------------


class Message:
    """A delivered event together with the means to settle it."""

    def __init__(self, client: Client, consumer_name: str, event: Event) -> None:
        """Wrap ``event`` so it can be acked or nacked."""
        self.event = event
        self._client = client
        self._consumer_name = consumer_name

    def ack(self) -> None:
        """Acknowledge the event, advancing the consumer's cursor."""
        self._client.ack(self._consumer_name, self.event.id)

    def nack(self, error: Union[str, BaseException]) -> None:
        """Reject the event, recording ``error`` as a string failure."""
        self._client.nack(
            self._consumer_name, str(error), event_id=self.event.id
        )


class Consumer:
    """A pull-based consumer.

    Construction registers the consumer on the server immediately. Closing
    the object only stops local use; call :meth:`deregister` to remove the
    registration from the server.
    """

    def __init__(
        self,
        client: Client,
        name: str,
        types: Sequence[str],
        *,
        patterns: Sequence[str] = (),
        timeout: float = 25.0
    ) -> None:
        """Register ``name`` for ``types`` and/or ``patterns``.

        ``types`` are exact event types and ``patterns`` are Go RE2 regular
        expressions matched against event types. Either may be empty, but not
        both. The subscription is the union of the two.
        """
        self._client = client
        self._name = name
        self._types: List[str] = list(types)
        self._patterns: List[str] = _normalize_patterns(patterns)
        self._timeout = float(timeout)
        self._closed = False
        client.register(name, self._types, self._patterns)

    @property
    def patterns(self) -> Tuple[str, ...]:
        """The normalized regex patterns this consumer subscribes to."""
        return tuple(self._patterns)

    def __enter__(self) -> "Consumer":
        """Return this consumer for use in a ``with`` statement."""
        return self

    def __exit__(self, *exc: Any) -> None:
        """Close local use without deleting the server registration."""
        self.close()

    def close(self) -> None:
        """Stop local use. Further calls to :meth:`next` raise ``YiError``."""
        self._closed = True

    def deregister(self) -> None:
        """Delete the server registration and close this consumer locally."""
        self._client.deregister(self._name)
        self._closed = True

    def next(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Return the next message, or ``None`` if the long poll timed out."""
        if self._closed:
            raise YiError("consumer {!r} is closed".format(self._name))
        wait = self._timeout if timeout is None else float(timeout)
        event = self._client.next(self._name, timeout=wait)
        if event is None:
            return None
        return Message(self._client, self._name, event)

    def __iter__(self) -> Iterator[Message]:
        """Iterate forever, skipping idle timeouts, until :meth:`close`.

        Iteration ends cleanly after ``close()``. Calling :meth:`next`
        directly after ``close()`` still raises ``YiError``.
        """
        while not self._closed:
            try:
                message = self.next()
            except YiError:
                if self._closed:
                    return
                raise
            if message is not None:
                yield message
