"""Exceptions raised by the Yi client."""

from __future__ import annotations


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
