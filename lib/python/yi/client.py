"""Synchronous HTTP client for the Yi service port."""

from __future__ import annotations

import json
import socket
import urllib.error
import urllib.request
from collections.abc import Mapping as MappingABC
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union
from urllib.parse import quote

from .errors import ConflictError, HTTPError, YiError
from .models import ConsumerStats, Event, Failure, PublishResult


def _format_timeout(value: float) -> str:
    """Format a timeout for use in a query string."""
    number = float(value)
    if number.is_integer():
        return str(int(number))
    return repr(number)


class Client:
    """Client for the Yi JSON service API.

    The client is synchronous and uses only the Python standard library.

    Args:
        base_url: Root URL of the Yi service port.
        timeout: Socket timeout for ordinary requests, in seconds. For
            ``next()`` the socket timeout is extended past the server-side
            long-poll timeout so the server can answer 204 first.
    """

    def __init__(
        self, base_url: str = "http://127.0.0.1:8081", timeout: float = 10.0
    ) -> None:
        """Create a client for the service at ``base_url``."""
        self._base_url = base_url.rstrip("/")
        self._timeout = float(timeout)

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

    def register(self, name: str, types: Sequence[str]) -> ConsumerStats:
        """Register a consumer, or verify an existing registration.

        Raises:
            ConflictError: The name exists with a different set of types.
        """
        payload = {"types": list(types)}
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
