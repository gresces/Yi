"""Tests for the Yi consumer helper."""

from __future__ import annotations

import threading
import time
import unittest

from tests.fake_server import FakeServer
from yi import Client, ConflictError, Consumer, Event, Message, YiError


class ConsumerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = FakeServer().start()
        self.client = Client(self.server.url, timeout=2.0)

    def tearDown(self) -> None:
        self.server.stop()

    @property
    def last_request(self):
        return self.server.state.requests[-1]

    def make_consumer(
        self, types=("raw.captured",), patterns=(), timeout: float = 0.05
    ) -> Consumer:
        return Consumer(
            self.client,
            "worker",
            list(types),
            patterns=patterns,
            timeout=timeout,
        )

    def test_constructor_registers_immediately(self) -> None:
        consumer = self.make_consumer()
        self.assertIn("worker", self.server.state.consumers)
        self.assertEqual(self.last_request["method"], "PUT")
        self.assertEqual(self.last_request["path"], "/consumers/worker")
        self.assertEqual(
            self.last_request["body"], {"types": ["raw.captured"]}
        )
        self.assertEqual(self.last_request["status"], 201)
        consumer.close()

    def test_constructor_registers_patterns(self) -> None:
        consumer = Consumer(
            self.client,
            "worker",
            ["raw.captured"],
            patterns=[r"^mail\.", r"^note$"],
            timeout=0.05,
        )
        self.assertEqual(
            self.last_request["body"],
            {
                "types": ["raw.captured"],
                "patterns": [r"^mail\.", r"^note$"],
            },
        )
        self.assertEqual(consumer.patterns, (r"^mail\.", r"^note$"))
        consumer.close()

    def test_patterns_only_consumer_delivers_matching_events(self) -> None:
        consumer = Consumer(
            self.client, "worker", [], patterns=[r"^note\."], timeout=0.05
        )
        self.assertEqual(
            self.last_request["body"],
            {"types": [], "patterns": [r"^note\."]},
        )

        ignored = Event.new("phone", types=["mail.received"])
        matching = Event.new("phone", types=["note.created"])
        self.client.publish(ignored)
        self.client.publish(matching)

        message = consumer.next()
        self.assertIsNotNone(message)
        self.assertEqual(message.event.id, matching.id)
        message.ack()

        stats = self.client.stats()[0]
        self.assertEqual(stats.types, [])
        self.assertEqual(stats.patterns, (r"^note\.",))
        self.assertEqual(stats.delivered, 1)

    def test_constructor_propagates_conflict(self) -> None:
        self.make_consumer()
        with self.assertRaises(ConflictError):
            Consumer(self.client, "worker", ["other.created"])

    def test_next_and_message_ack(self) -> None:
        consumer = self.make_consumer()
        event = Event.new("phone", data={"text": "hello"})
        result = self.client.publish(event)
        self.assertEqual(result.matched, 1)

        message = consumer.next()
        self.assertIsInstance(message, Message)
        self.assertEqual(message.event, event)

        message.ack()
        self.assertEqual(self.last_request["path"], "/consumers/worker/ack")
        self.assertEqual(self.last_request["body"], {"id": event.id})
        self.assertEqual(self.last_request["status"], 204)

        stats = self.client.stats()[0]
        self.assertEqual(stats.delivered, 1)
        self.assertEqual(stats.cursor, 1)

    def test_next_returns_none_on_timeout(self) -> None:
        consumer = self.make_consumer(timeout=0.05)
        self.assertIsNone(consumer.next())
        self.assertEqual(self.last_request["status"], 204)

    def test_message_nack_string_and_exception(self) -> None:
        consumer = self.make_consumer()

        first = Event.new("phone")
        self.client.publish(first)
        message = consumer.next()
        self.assertIsNotNone(message)
        message.nack(ValueError("boom"))
        self.assertEqual(self.last_request["path"], "/consumers/worker/nack")
        self.assertEqual(
            self.last_request["body"], {"id": first.id, "error": "boom"}
        )
        self.assertEqual(self.last_request["status"], 204)

        second = Event.new("phone")
        self.client.publish(second)
        message = consumer.next()
        self.assertIsNotNone(message)
        message.nack("plain string")

        failures = self.client.failures()
        self.assertEqual(len(failures), 2)
        self.assertEqual(failures[0].error, "boom")
        self.assertEqual(failures[1].error, "plain string")
        self.assertEqual(failures[1].event_id, second.id)
        stats = self.client.stats()[0]
        self.assertEqual(stats.failed, 2)
        self.assertEqual(stats.last_error, "plain string")

    def test_iteration_survives_idle_timeout_and_stops_after_close(self) -> None:
        consumer = self.make_consumer(timeout=0.05)

        def publish_later() -> None:
            time.sleep(0.12)
            Client(self.server.url).publish(Event.new("clock"))

        thread = threading.Thread(target=publish_later)
        thread.start()
        try:
            iterator = iter(consumer)
            message = next(iterator)  # Must survive at least one 204.
            self.assertIsInstance(message, Message)
            self.assertEqual(message.event.source, "clock")
            message.ack()
        finally:
            thread.join(timeout=2.0)
        self.assertFalse(thread.is_alive())

        consumer.close()
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_close_stops_local_use_but_keeps_registration(self) -> None:
        consumer = self.make_consumer()
        consumer.close()

        with self.assertRaises(YiError):
            consumer.next()
        with self.assertRaises(StopIteration):
            next(iter(consumer))
        self.assertIn("worker", self.server.state.consumers)

    def test_context_manager_only_closes_locally(self) -> None:
        with self.make_consumer() as consumer:
            self.assertIn("worker", self.server.state.consumers)

        with self.assertRaises(YiError):
            consumer.next()
        with self.assertRaises(StopIteration):
            next(iter(consumer))
        self.assertIn("worker", self.server.state.consumers)

    def test_deregister_removes_registration_and_closes(self) -> None:
        consumer = self.make_consumer()
        consumer.deregister()

        self.assertNotIn("worker", self.server.state.consumers)
        self.assertEqual(self.last_request["method"], "DELETE")
        self.assertEqual(self.last_request["path"], "/consumers/worker")
        self.assertEqual(self.last_request["status"], 204)
        with self.assertRaises(YiError):
            consumer.next()
        with self.assertRaises(StopIteration):
            next(iter(consumer))


if __name__ == "__main__":
    unittest.main()
