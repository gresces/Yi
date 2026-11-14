"""Tests for the Yi HTTP client."""

from __future__ import annotations

import unittest
from datetime import datetime

from tests.fake_server import FakeServer
from yi import (
    Client,
    ConflictError,
    ConsumerStats,
    Event,
    Failure,
    HTTPError,
    PublishResult,
    YiError,
)


class ClientTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeServer().start()
        self.client = Client(self.server.url, timeout=2.0)

    def tearDown(self) -> None:
        self.server.stop()

    @property
    def last_request(self):
        return self.server.state.requests[-1]

    def test_health(self) -> None:
        self.assertEqual(self.client.health(), "ok")
        self.assertEqual(self.last_request["method"], "GET")
        self.assertEqual(self.last_request["path"], "/healthz")
        self.assertEqual(self.last_request["status"], 200)

    def test_publish_event_and_deduplicate(self) -> None:
        event = Event.new("phone", data={"text": "hello"})
        result = self.client.publish(event)

        self.assertIsInstance(result, PublishResult)
        self.assertFalse(result.duplicate)
        self.assertEqual(result.matched, 0)
        self.assertEqual(self.last_request["method"], "POST")
        self.assertEqual(self.last_request["path"], "/events")
        self.assertEqual(self.last_request["body"], event.to_dict())
        self.assertEqual(self.last_request["status"], 201)
        self.assertEqual(self.server.state.events, [event.to_dict()])

        duplicate = self.client.publish(event.to_dict())
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(self.last_request["status"], 200)
        self.assertEqual(len(self.server.state.events), 1)

    def test_publish_matched_counts_consumers(self) -> None:
        self.client.register("worker", ["raw.captured"])
        matched = self.client.publish(Event.new("phone"))
        self.assertEqual(matched.matched, 1)
        other = self.client.publish(Event.new("phone", types=["other.created"]))
        self.assertEqual(other.matched, 0)

    def test_capture_builds_data(self) -> None:
        result = self.client.capture(
            "buy milk", source="cli", types=["raw.captured"], mood="calm"
        )
        self.assertFalse(result.duplicate)

        request = self.last_request
        self.assertEqual(request["method"], "POST")
        self.assertEqual(request["path"], "/events")
        body = request["body"]
        self.assertEqual(body["source"], "cli")
        self.assertEqual(body["types"], ["raw.captured"])
        self.assertEqual(body["data"], {"text": "buy milk", "mood": "calm"})
        self.assertEqual(len(body["id"]), 32)
        parsed_time = datetime.fromisoformat(body["time"].replace("Z", "+00:00"))
        self.assertIsNotNone(parsed_time.tzinfo)

    def test_events_parses_and_limits(self) -> None:
        first = Event.new("one", data={"n": 1})
        second = Event.new("two", data={"n": 2})
        third = Event.new("three", data={"n": 3})
        for event in (first, second, third):
            self.client.publish(event)

        events = self.client.events(limit=2)
        self.assertEqual([event.id for event in events], [second.id, third.id])
        self.assertTrue(all(isinstance(event, Event) for event in events))
        self.assertEqual(events[0].data, {"n": 2})
        self.assertEqual(self.last_request["path"], "/events")
        self.assertEqual(self.last_request["query"], {"limit": ["2"]})

    def test_stats_and_failures(self) -> None:
        self.client.register("worker", ["raw.captured"])
        event = Event.new("phone")
        self.client.publish(event)

        delivered = self.client.next("worker", timeout=0.05)
        self.assertIsNotNone(delivered)
        self.client.nack("worker", "boom", delivered.id)

        stats = self.client.stats()
        self.assertEqual(len(stats), 1)
        self.assertIsInstance(stats[0], ConsumerStats)
        self.assertEqual(stats[0].name, "worker")
        self.assertEqual(stats[0].types, ["raw.captured"])
        self.assertEqual(stats[0].failed, 1)
        self.assertEqual(stats[0].last_error, "boom")
        self.assertEqual(stats[0].last_event, event.id)

        failures = self.client.failures()
        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0], Failure)
        self.assertEqual(failures[0].consumer, "worker")
        self.assertEqual(failures[0].event_id, event.id)
        self.assertEqual(failures[0].source, "phone")
        self.assertEqual(failures[0].error, "boom")
        self.assertIsNotNone(
            datetime.fromisoformat(failures[0].at.replace("Z", "+00:00")).tzinfo
        )

    def test_register_new_same_and_conflict(self) -> None:
        stats = self.client.register("worker", ["raw.captured"])
        self.assertIsInstance(stats, ConsumerStats)
        self.assertEqual(stats.name, "worker")
        self.assertEqual(stats.types, ["raw.captured"])
        self.assertEqual(stats.cursor, 0)
        self.assertEqual(self.last_request["method"], "PUT")
        self.assertEqual(self.last_request["path"], "/consumers/worker")
        self.assertEqual(self.last_request["body"], {"types": ["raw.captured"]})
        self.assertEqual(self.last_request["status"], 201)

        again = self.client.register("worker", ["raw.captured"])
        self.assertEqual(again.name, "worker")
        self.assertEqual(self.last_request["status"], 200)
        self.assertEqual(len(self.server.state.consumers), 1)

        with self.assertRaises(ConflictError) as raised:
            self.client.register("worker", ["other.created"])
        self.assertIsInstance(raised.exception, HTTPError)
        self.assertEqual(raised.exception.status, 409)
        self.assertIn("already exists", str(raised.exception))
        self.assertIn('"error"', raised.exception.body)
        self.assertEqual(self.last_request["status"], 409)
        self.assertEqual(
            self.last_request["body"], {"types": ["other.created"]}
        )

    def test_deregister(self) -> None:
        self.client.register("worker", ["raw.captured"])
        self.client.deregister("worker")
        self.assertEqual(self.last_request["method"], "DELETE")
        self.assertEqual(self.last_request["path"], "/consumers/worker")
        self.assertEqual(self.last_request["status"], 204)
        self.assertNotIn("worker", self.server.state.consumers)

        with self.assertRaises(HTTPError) as raised:
            self.client.deregister("worker")
        self.assertEqual(raised.exception.status, 404)

    def test_next_event_retry_and_timeout(self) -> None:
        self.client.register("worker", ["raw.captured"])
        nonmatching = Event.new("phone", types=["other.created"])
        matching = Event.new("phone", data={"text": "hello"})
        self.client.publish(nonmatching)
        self.client.publish(matching)

        first = self.client.next("worker", timeout=0.05)
        self.assertIsNotNone(first)
        self.assertEqual(first.id, matching.id)
        self.assertEqual(self.last_request["query"], {"timeout": ["0.05"]})
        self.assertEqual(self.last_request["status"], 200)

        # Not acked yet: the same event is delivered again.
        again = self.client.next("worker", timeout=0.05)
        self.assertIsNotNone(again)
        self.assertEqual(again.id, matching.id)

        self.client.ack("worker", matching.id)
        self.assertIsNone(self.client.next("worker", timeout=0.05))
        self.assertEqual(self.last_request["status"], 204)

        with self.assertRaises(HTTPError) as raised:
            self.client.next("missing", timeout=0.05)
        self.assertEqual(raised.exception.status, 404)
        self.assertIn("unknown consumer", raised.exception.body)

    def test_ack_and_nack_payloads(self) -> None:
        self.client.register("worker", ["raw.captured"])

        # An empty id is omitted, even though the server rejects the ack.
        with self.assertRaises(ConflictError):
            self.client.ack("worker")
        self.assertEqual(self.last_request["body"], {})
        self.assertEqual(self.last_request["status"], 409)

        first = Event.new("phone")
        self.client.publish(first)
        self.assertIsNotNone(self.client.next("worker", timeout=0.05))
        self.client.ack("worker", first.id)
        self.assertEqual(self.last_request["path"], "/consumers/worker/ack")
        self.assertEqual(self.last_request["body"], {"id": first.id})
        self.assertEqual(self.last_request["status"], 204)

        second = Event.new("phone")
        self.client.publish(second)
        self.assertIsNotNone(self.client.next("worker", timeout=0.05))
        self.client.nack("worker", "boom", second.id)
        self.assertEqual(self.last_request["path"], "/consumers/worker/nack")
        self.assertEqual(
            self.last_request["body"], {"id": second.id, "error": "boom"}
        )
        self.assertEqual(self.last_request["status"], 204)

        with self.assertRaises(ConflictError):
            self.client.nack("worker", "later")
        self.assertEqual(self.last_request["body"], {"error": "later"})
        self.assertEqual(self.last_request["status"], 409)

    def test_http_error_keeps_non_json_body(self) -> None:
        self.server.state.raw_error = (502, b"<html>bad gateway</html>")
        with self.assertRaises(HTTPError) as raised:
            self.client.health()
        error = raised.exception
        self.assertNotIsInstance(error, ConflictError)
        self.assertEqual(error.status, 502)
        self.assertEqual(error.body, "<html>bad gateway</html>")
        self.assertIn("bad gateway", str(error))

    def test_socket_timeout_is_yi_error(self) -> None:
        self.server.state.delay = 0.2
        fast_client = Client(self.server.url, timeout=0.02)
        with self.assertRaises(YiError) as raised:
            fast_client.health()
        self.assertNotIsInstance(raised.exception, HTTPError)

    def test_event_helpers(self) -> None:
        event = Event.new("phone", data={"n": 1})
        self.assertEqual(len(event.id), 32)
        parsed = datetime.fromisoformat(event.time.replace("Z", "+00:00"))
        self.assertIsNotNone(parsed.tzinfo)
        self.assertEqual(Event.from_dict(event.to_dict()), event)
        self.assertEqual(
            set(event.to_dict()),
            {"id", "source", "types", "time", "data"},
        )


if __name__ == "__main__":
    unittest.main()
