"""Consumer helper built on top of :class:`yi.client.Client`."""

from __future__ import annotations

from typing import Any, Iterator, List, Optional, Sequence, Union

from .client import Client
from .errors import YiError
from .models import Event


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
        timeout: float = 25.0
    ) -> None:
        """Register ``name`` for ``types`` and create a local consumer."""
        self._client = client
        self._name = name
        self._types: List[str] = list(types)
        self._timeout = float(timeout)
        self._closed = False
        client.register(name, self._types)

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
