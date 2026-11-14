"""Zero-dependency Python client for the Yi event service."""

from __future__ import annotations

from .client import Client
from .consumer import Consumer, Message
from .errors import ConflictError, HTTPError, YiError
from .models import ConsumerStats, Event, Failure, PublishResult

__version__ = "0.1.0"

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
