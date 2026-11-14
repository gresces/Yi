"""In-memory Yi service used by the unit tests.

The fake implements the same HTTP contract as the real service port. It is
intentionally small: a list of events, a dictionary of consumers, and a
condition variable for long-polling.
"""

from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlsplit


def _rfc3339_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace(
        "+00:00", "Z"
    )


class FakeState:
    """Thread-safe in-memory state of the fake service."""

    def __init__(self) -> None:
        self.condition = threading.Condition(threading.RLock())
        self.events: List[Dict[str, Any]] = []
        self.seen = set()
        self.consumers: Dict[str, Dict[str, Any]] = {}
        self.failures: List[Dict[str, Any]] = []
        self.requests: List[Dict[str, Any]] = []
        # Test hooks: sleep before handling, or return a raw error response.
        self.delay = 0.0
        self.raw_error: Optional[Tuple[int, bytes]] = None

    def record(
        self, method: str, path: str, query: Dict[str, List[str]], raw_body: bytes
    ) -> Dict[str, Any]:
        try:
            body: Any = json.loads(raw_body.decode("utf-8")) if raw_body else None
        except (UnicodeDecodeError, ValueError):
            body = raw_body.decode("utf-8", errors="replace")
        entry = {
            "method": method,
            "path": path,
            "query": query,
            "body": body,
            "raw_body": raw_body,
            "status": None,
        }
        with self.condition:
            self.requests.append(entry)
        return entry

    def stats_locked(self, consumer: Dict[str, Any]) -> Dict[str, Any]:
        pending = len(self.events) - int(consumer["cursor"])
        if pending < 0:
            pending = 0
        stats: Dict[str, Any] = {
            "name": consumer["name"],
            "types": list(consumer["types"]),
            "cursor": consumer["cursor"],
            "pending": pending,
            "delivered": consumer["delivered"],
            "failed": consumer["failed"],
        }
        if consumer["last_error"]:
            stats["last_error"] = consumer["last_error"]
        if consumer["last_event"]:
            stats["last_event"] = consumer["last_event"]
        return stats


class _FakeHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep unittest output clean.
        pass

    @property
    def state(self) -> FakeState:
        return self.server.state  # type: ignore[attr-defined]

    # ---- request/response helpers ----

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length") or 0)
        return self.rfile.read(length) if length else b""

    def _handle(self, method: str) -> None:
        parsed = urlsplit(self.path)
        segments = [unquote(part) for part in parsed.path.split("/") if part]
        query = parse_qs(parsed.query)
        raw_body = self._read_body()
        self._entry = self.state.record(method, parsed.path, query, raw_body)

        if self.state.delay:
            time.sleep(self.state.delay)

        if self.state.raw_error is not None:
            status, body = self.state.raw_error
            self._write_raw(status, body)
            return

        try:
            self._dispatch(method, segments, query, raw_body)
        except (BrokenPipeError, ConnectionResetError):
            # The client may have hit its socket timeout.
            pass
        except OSError:
            pass

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")

    def do_PUT(self) -> None:
        self._handle("PUT")

    def do_DELETE(self) -> None:
        self._handle("DELETE")

    def _dispatch(
        self,
        method: str,
        segments: List[str],
        query: Dict[str, List[str]],
        raw_body: bytes,
    ) -> None:
        if segments == ["healthz"] and method == "GET":
            self._write_text(200, "ok\n")
            return

        if segments == ["events"]:
            if method == "POST":
                self._publish(raw_body)
                return
            if method == "GET":
                self._recent(query)
                return

        if segments == ["stats"] and method == "GET":
            self._all_stats()
            return

        if segments == ["failures"] and method == "GET":
            self._all_failures()
            return

        if segments == ["consumers"] and method == "GET":
            self._all_consumers()
            return

        if len(segments) == 2 and segments[0] == "consumers":
            name = segments[1]
            if method == "PUT":
                self._register(name, raw_body)
                return
            if method == "DELETE":
                self._deregister(name)
                return

        if len(segments) == 3 and segments[0] == "consumers":
            name, action = segments[1], segments[2]
            if method == "GET" and action == "next":
                self._next(name, query)
                return
            if method == "POST" and action == "ack":
                self._settle(name, raw_body, failed=False)
                return
            if method == "POST" and action == "nack":
                self._settle(name, raw_body, failed=True)
                return

        self._write_json(404, {"error": "not found"})

    # ---- response writers ----

    def _write_json(self, status: int, payload: Any) -> None:
        self._record_status(status)
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_text(self, status: int, text: str) -> None:
        self._record_status(status)
        body = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_empty(self, status: int) -> None:
        self._record_status(status)
        self.send_response(status)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def _write_raw(self, status: int, body: bytes) -> None:
        self._record_status(status)
        self.send_response(status)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _record_status(self, status: int) -> None:
        entry = getattr(self, "_entry", None)
        if entry is not None:
            entry["status"] = status

    # ---- endpoint implementations ----

    def _publish(self, raw_body: bytes) -> None:
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            self._write_json(400, {"error": "invalid json: {}".format(exc)})
            return

        if not isinstance(payload, dict):
            self._write_json(400, {"error": "event must be a JSON object"})
            return
        for field in ("id", "source", "time"):
            if not isinstance(payload.get(field), str) or not payload[field]:
                self._write_json(
                    400, {"error": "event needs a non-empty {}".format(field)}
                )
                return
        types = payload.get("types")
        if (
            not isinstance(types, list)
            or not types
            or not all(isinstance(item, str) and item for item in types)
        ):
            self._write_json(
                400, {"error": "event types must be a non-empty array of strings"}
            )
            return
        if not isinstance(payload.get("data"), dict):
            self._write_json(400, {"error": "event data must be a JSON object"})
            return

        event = dict(payload)
        with self.state.condition:
            key = (event["source"], event["id"])
            duplicate = key in self.state.seen
            if not duplicate:
                self.state.events.append(event)
                self.state.seen.add(key)
                self.state.condition.notify_all()
            event_types = set(event.get("types") or [])
            matched = sum(
                1
                for consumer in self.state.consumers.values()
                if event_types & set(consumer["types"])
            )
            status = 200 if duplicate else 201
            self._write_json(
                status, {"duplicate": duplicate, "matched": matched}
            )

    def _recent(self, query: Dict[str, List[str]]) -> None:
        raw_limit = query.get("limit", ["50"])[0]
        try:
            limit = int(raw_limit)
        except ValueError:
            limit = 50
        with self.state.condition:
            if limit <= 0:
                selected = list(self.state.events)
            else:
                selected = list(self.state.events[-limit:])
        self._write_json(200, selected)

    def _all_stats(self) -> None:
        with self.state.condition:
            payload = [
                self.state.stats_locked(self.state.consumers[name])
                for name in sorted(self.state.consumers)
            ]
        self._write_json(200, payload)

    def _all_failures(self) -> None:
        with self.state.condition:
            payload = list(self.state.failures)
        self._write_json(200, payload)

    def _all_consumers(self) -> None:
        with self.state.condition:
            payload = [
                self.state.stats_locked(self.state.consumers[name])
                for name in sorted(self.state.consumers)
            ]
        self._write_json(200, payload)

    def _register(self, name: str, raw_body: bytes) -> None:
        try:
            payload = json.loads(raw_body.decode("utf-8"))
        except (UnicodeDecodeError, ValueError) as exc:
            self._write_json(400, {"error": "invalid json: {}".format(exc)})
            return

        types = payload.get("types") if isinstance(payload, dict) else None
        if (
            not isinstance(types, list)
            or not types
            or not all(isinstance(item, str) and item for item in types)
        ):
            self._write_json(
                400, {"error": "types must be a non-empty array of strings"}
            )
            return

        with self.state.condition:
            existing = self.state.consumers.get(name)
            if existing is not None:
                if set(existing["types"]) == set(types):
                    self._write_json(200, self.state.stats_locked(existing))
                else:
                    self._write_json(
                        409,
                        {
                            "error": "consumer {!r} already exists with different types".format(
                                name
                            )
                        },
                    )
                return

            consumer = {
                "name": name,
                "types": list(types),
                "cursor": 0,
                "delivered": 0,
                "failed": 0,
                "last_error": "",
                "last_event": "",
                "inflight": None,
            }
            self.state.consumers[name] = consumer
            self._write_json(201, self.state.stats_locked(consumer))

    def _deregister(self, name: str) -> None:
        with self.state.condition:
            if name not in self.state.consumers:
                self._write_json(
                    404, {"error": "unknown consumer {!r}".format(name)}
                )
                return
            del self.state.consumers[name]
            self._write_empty(204)

    def _next(self, name: str, query: Dict[str, List[str]]) -> None:
        raw_timeout = query.get("timeout", ["25"])[0]
        try:
            wait = float(raw_timeout)
        except ValueError:
            wait = 25.0
        wait = max(0.0, min(wait, 60.0))

        with self.state.condition:
            consumer = self.state.consumers.get(name)
            if consumer is None:
                self._write_json(
                    404, {"error": "unknown consumer {!r}".format(name)}
                )
                return

            deadline = time.monotonic() + wait
            while True:
                if consumer["inflight"] is not None:
                    event = self.state.events[consumer["inflight"]]
                    self._write_json(200, {"event": event})
                    return

                index: Optional[int] = None
                for i in range(consumer["cursor"], len(self.state.events)):
                    event_types = set(self.state.events[i].get("types") or [])
                    if event_types & set(consumer["types"]):
                        index = i
                        break

                if index is None:
                    # Safe to skip events of other types, like the real bus.
                    if len(self.state.events) > consumer["cursor"]:
                        consumer["cursor"] = len(self.state.events)
                else:
                    consumer["inflight"] = index
                    self._write_json(200, {"event": self.state.events[index]})
                    return

                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self._write_empty(204)
                    return
                self.state.condition.wait(min(remaining, 0.05))

    def _settle(self, name: str, raw_body: bytes, failed: bool) -> None:
        try:
            payload = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        except (UnicodeDecodeError, ValueError) as exc:
            self._write_json(400, {"error": "invalid json: {}".format(exc)})
            return

        if not isinstance(payload, dict):
            self._write_json(409, {"error": "body must be a JSON object"})
            return

        with self.state.condition:
            consumer = self.state.consumers.get(name)
            if consumer is None:
                self._write_json(
                    404, {"error": "unknown consumer {!r}".format(name)}
                )
                return

            event_id = payload.get("id")
            if consumer["inflight"] is None:
                self._write_json(409, {"error": "no event is in flight"})
                return

            event = self.state.events[consumer["inflight"]]
            if event_id and event_id != event["id"]:
                self._write_json(
                    409, {"error": "event {!r} is not in flight".format(event_id)}
                )
                return

            consumer["cursor"] = consumer["inflight"] + 1
            consumer["inflight"] = None
            consumer["last_event"] = event["id"]

            if failed:
                error = payload.get("error", "")
                if not isinstance(error, str):
                    error = str(error)
                consumer["failed"] += 1
                consumer["last_error"] = error
                self.state.failures.append(
                    {
                        "consumer": name,
                        "event_id": event["id"],
                        "source": event["source"],
                        "error": error,
                        "at": _rfc3339_now(),
                    }
                )
            else:
                consumer["delivered"] += 1

            self._write_empty(204)


class FakeServer:
    """A fake Yi service bound to a random free localhost port."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.state = FakeState()
        self._httpd = ThreadingHTTPServer((host, port), _FakeHandler)
        self._httpd.state = self.state  # type: ignore[attr-defined]
        self._httpd.daemon_threads = True
        self._thread: Optional[threading.Thread] = None

    @property
    def url(self) -> str:
        """Base URL of the running fake service."""
        host, port = self._httpd.server_address[:2]
        return "http://{}:{}".format(host, port)

    def start(self) -> "FakeServer":
        """Start serving in a background thread and return ``self``."""
        if self._thread is None:
            self._thread = threading.Thread(
                target=lambda: self._httpd.serve_forever(poll_interval=0.05),
                daemon=True,
            )
            self._thread.start()
        return self

    def stop(self) -> None:
        """Stop the server and close the listening socket."""
        if self._thread is not None:
            self._httpd.shutdown()
            self._thread.join(timeout=2.0)
            self._thread = None
        self._httpd.server_close()

    def __enter__(self) -> "FakeServer":
        return self.start()

    def __exit__(self, *exc: Any) -> None:
        self.stop()
