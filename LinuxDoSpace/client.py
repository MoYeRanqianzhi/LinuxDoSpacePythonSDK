"""LinuxDoSpace Python SDK client.

The SDK uses only the Python standard library so deployments can keep the
dependency surface minimal while still consuming the HTTPS newline-delimited
mail stream exposed by the backend.
"""

from __future__ import annotations

import base64
import json
import socket
import time
import urllib.error
import urllib.request
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from email.parser import BytesParser
from email.policy import default as default_email_policy
from email.utils import getaddresses, parsedate_to_datetime
from typing import Any, Callable, Iterator

from .enums import Suffix
from .exceptions import AuthenticationError, LinuxDoSpaceError, StreamError
from .models import MailMessage

_DEFAULT_BASE_URL = "https://api.linuxdo.space"
_DEFAULT_CONNECT_TIMEOUT_SECONDS = 10.0
_DEFAULT_STREAM_SOCKET_TIMEOUT_SECONDS = 30.0
_STREAM_PATH = "/v1/token/email/stream"
_READY_EVENT_TYPE = "ready"
_HEARTBEAT_EVENT_TYPE = "heartbeat"
_MAIL_EVENT_TYPE = "mail"
_LOCALHOST_NAMES = {"localhost", "127.0.0.1", "::1"}


@dataclass(slots=True)
class _StreamEvent:
    """Internal representation of one raw NDJSON stream event."""

    type: str
    payload: dict[str, Any]


class Client:
    """Top-level LinuxDoSpace SDK client.

    Parameters
    ----------
    token:
        The plaintext API token generated from the LinuxDoSpace web console.
    base_url:
        Backend base URL. Production defaults to `https://api.linuxdo.space`.
    connect_timeout:
        Timeout used when establishing one HTTPS connection.
    stream_socket_timeout:
        Per-read socket timeout for the live stream. The SDK reconnects when it
        expires before the caller's total `listen()` timeout has elapsed.
    """

    def __init__(
        self,
        token: str,
        *,
        base_url: str = _DEFAULT_BASE_URL,
        connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT_SECONDS,
        stream_socket_timeout: float = _DEFAULT_STREAM_SOCKET_TIMEOUT_SECONDS,
        _urlopen: Callable[..., Any] | None = None,
    ) -> None:
        self._token = token.strip()
        self._base_url = _normalize_base_url(base_url)
        self._connect_timeout = float(connect_timeout)
        self._stream_socket_timeout = float(stream_socket_timeout)
        self._urlopen = _urlopen or urllib.request.urlopen

        if not self._token:
            raise ValueError("token must not be empty")
        if not self._base_url:
            raise ValueError("base_url must not be empty")
        if self._connect_timeout <= 0:
            raise ValueError("connect_timeout must be greater than 0")
        if self._stream_socket_timeout <= 0:
            raise ValueError("stream_socket_timeout must be greater than 0")

    def mail(self, prefix: str, suffix: Suffix | str) -> "MailBox":
        """Create one mailbox listener bound to a concrete address."""

        normalized_prefix = prefix.strip().lower()
        normalized_suffix = str(suffix).strip().lower()
        if not normalized_prefix:
            raise ValueError("prefix must not be empty")
        if not normalized_suffix:
            raise ValueError("suffix must not be empty")

        return MailBox(
            client=self,
            prefix=normalized_prefix,
            suffix=normalized_suffix,
        )


class MailBox:
    """Context-managed mailbox stream helper.

    The object itself is lightweight. The HTTPS stream is only opened when
    `listen()` is iterated.
    """

    def __init__(self, *, client: Client, prefix: str, suffix: str) -> None:
        self._client = client
        self.prefix = prefix
        self.suffix = suffix
        self.address = f"{self.prefix}@{self.suffix}"
        self._closed = False

    def __enter__(self) -> "MailBox":
        """Return the active mailbox helper."""

        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Mark the helper as closed so future listens are rejected."""

        self.close()

    def close(self) -> None:
        """Close the logical mailbox helper."""

        self._closed = True

    def listen(self, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield parsed mail events for this mailbox.

        Parameters
        ----------
        timeout:
            Maximum total listen duration in seconds. A negative value means
            "listen forever".
        """

        self._ensure_open()

        deadline = None if timeout < 0 else time.monotonic() + float(timeout)

        while not self._closed:
            remaining_seconds = _remaining_seconds(deadline)
            if remaining_seconds is not None and remaining_seconds <= 0:
                return

            socket_timeout = self._client._stream_socket_timeout
            if remaining_seconds is not None:
                socket_timeout = max(0.1, min(socket_timeout, remaining_seconds))

            try:
                yield from self._listen_once(socket_timeout=socket_timeout, deadline=deadline)
                return
            except socket.timeout:
                if deadline is not None and time.monotonic() >= deadline:
                    return
                continue
            except TimeoutError:
                if deadline is not None and time.monotonic() >= deadline:
                    return
                continue

    def _listen_once(self, *, socket_timeout: float, deadline: float | None) -> Iterator[MailMessage]:
        """Open the HTTPS stream once and yield every matching mail event."""

        request = urllib.request.Request(
            url=f"{self._client._base_url}{_STREAM_PATH}",
            headers={
                "Authorization": f"Bearer {self._client._token}",
                "Accept": "application/x-ndjson",
                "User-Agent": "LinuxDoSpace Python SDK/0.1.0a1",
            },
            method="GET",
        )

        try:
            with self._client._urlopen(request, timeout=self._client._connect_timeout) as response:
                status_code = getattr(response, "status", 200)
                if status_code != 200:
                    raise StreamError(f"unexpected stream status code: {status_code}")
                _set_stream_response_timeout(response, socket_timeout)

                while not self._closed:
                    if deadline is not None and time.monotonic() >= deadline:
                        return

                    raw_line = response.readline()
                    if not raw_line:
                        return

                    stripped_line = raw_line.strip()
                    if not stripped_line:
                        continue

                    event = self._decode_stream_event(stripped_line)
                    if event.type in {_READY_EVENT_TYPE, _HEARTBEAT_EVENT_TYPE}:
                        continue
                    if event.type != _MAIL_EVENT_TYPE:
                        continue

                    message = self._mail_message_from_event(event)
                    if message is None:
                        continue
                    yield message
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403}:
                raise AuthenticationError("api token was rejected by the LinuxDoSpace backend") from exc
            raise StreamError(f"failed to open LinuxDoSpace mail stream: http {exc.code}") from exc
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, socket.timeout):
                raise socket.timeout() from exc
            raise StreamError(f"failed to connect to LinuxDoSpace mail stream: {exc.reason}") from exc

    def _decode_stream_event(self, raw_line: bytes) -> _StreamEvent:
        """Decode one NDJSON line into the internal event representation."""

        try:
            payload = json.loads(raw_line.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise StreamError("received invalid JSON from the LinuxDoSpace mail stream") from exc

        event_type = str(payload.get("type", "")).strip().lower()
        if not event_type:
            raise StreamError("received stream event without a type field")

        return _StreamEvent(type=event_type, payload=payload)

    def _mail_message_from_event(self, event: _StreamEvent) -> MailMessage | None:
        """Convert one raw stream event into the public `MailMessage` model."""

        recipient_candidates = tuple(
            str(value).strip().lower()
            for value in event.payload.get("original_recipients", [])
            if str(value).strip()
        )
        matched_address = next((value for value in recipient_candidates if value == self.address), None)
        if matched_address is None:
            return None

        raw_message_base64 = str(event.payload.get("raw_message_base64", "")).strip()
        if not raw_message_base64:
            raise StreamError("mail event did not include raw_message_base64")

        try:
            raw_bytes = base64.b64decode(raw_message_base64, validate=True)
        except (ValueError, TypeError) as exc:
            raise StreamError("mail event contained invalid base64 message data") from exc

        email_message = BytesParser(policy=default_email_policy).parsebytes(raw_bytes)
        text_body, html_body = _extract_message_bodies(email_message)

        return MailMessage(
            address=matched_address,
            sender=str(event.payload.get("original_envelope_from", "")).strip(),
            recipients=recipient_candidates,
            received_at=_parse_iso_datetime(str(event.payload.get("received_at", "")).strip()),
            subject=str(email_message.get("Subject", "")),
            message_id=_optional_header(email_message, "Message-ID"),
            date=_parse_email_datetime(_optional_header(email_message, "Date")),
            from_header=str(email_message.get("From", "")),
            to_header=str(email_message.get("To", "")),
            cc_header=str(email_message.get("Cc", "")),
            reply_to_header=str(email_message.get("Reply-To", "")),
            from_addresses=_parse_header_addresses(str(email_message.get("From", ""))),
            to_addresses=_parse_header_addresses(str(email_message.get("To", ""))),
            cc_addresses=_parse_header_addresses(str(email_message.get("Cc", ""))),
            reply_to_addresses=_parse_header_addresses(str(email_message.get("Reply-To", ""))),
            text=text_body,
            html=html_body,
            headers={key: str(value) for key, value in email_message.items()},
            raw=raw_bytes.decode("utf-8", errors="replace"),
            raw_bytes=raw_bytes,
            message=email_message,
        )

    def _ensure_open(self) -> None:
        """Reject listen attempts after the mailbox helper was explicitly closed."""

        if self._closed:
            raise LinuxDoSpaceError("mailbox stream is already closed")


def _remaining_seconds(deadline: float | None) -> float | None:
    """Return the remaining total listen time or `None` when unbounded."""

    if deadline is None:
        return None
    return deadline - time.monotonic()


def _parse_iso_datetime(value: str) -> datetime:
    """Parse one RFC3339-style timestamp returned by the backend."""

    normalized_value = value.strip()
    if not normalized_value:
        raise StreamError("mail event timestamp was empty")
    if normalized_value.endswith("Z"):
        normalized_value = normalized_value[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(normalized_value)
    except ValueError as exc:
        raise StreamError(f"invalid mail event timestamp: {value!r}") from exc


def _parse_email_datetime(value: str | None) -> datetime | None:
    """Parse one RFC2822 Date header when present."""

    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None


def _optional_header(message: Any, header_name: str) -> str | None:
    """Read one optional header as a plain string."""

    value = message.get(header_name)
    if value is None:
        return None
    normalized_value = str(value).strip()
    return normalized_value or None


def _parse_header_addresses(raw_value: str) -> tuple[str, ...]:
    """Extract bare email addresses from one RFC2822 address header."""

    addresses = []
    for _, address in getaddresses([raw_value]):
        normalized_address = address.strip().lower()
        if normalized_address:
            addresses.append(normalized_address)
    return tuple(addresses)


def _extract_message_bodies(message: Any) -> tuple[str, str]:
    """Extract text/plain and text/html bodies from one parsed MIME message."""

    text_parts: list[str] = []
    html_parts: list[str] = []

    if message.is_multipart():
        for part in message.walk():
            if part.is_multipart():
                continue
            content_disposition = str(part.get_content_disposition() or "").strip().lower()
            if content_disposition == "attachment":
                continue
            content_type = str(part.get_content_type()).strip().lower()
            payload = part.get_content()
            payload_text = payload if isinstance(payload, str) else str(payload)
            if content_type == "text/plain":
                text_parts.append(payload_text)
            elif content_type == "text/html":
                html_parts.append(payload_text)
    else:
        payload = message.get_content()
        payload_text = payload if isinstance(payload, str) else str(payload)
        content_type = str(message.get_content_type()).strip().lower()
        if content_type == "text/html":
            html_parts.append(payload_text)
        else:
            text_parts.append(payload_text)

    return "\n".join(text_parts).strip(), "\n".join(html_parts).strip()


def _normalize_base_url(raw_base_url: str) -> str:
    """Validate and normalize the backend base URL."""

    normalized_value = raw_base_url.strip().rstrip("/")
    if not normalized_value:
        raise ValueError("base_url must not be empty")

    parsed_url = urllib.parse.urlparse(normalized_value)
    if parsed_url.scheme not in {"https", "http"}:
        raise ValueError("base_url must use http or https")
    if not parsed_url.netloc:
        raise ValueError("base_url must include a host")

    hostname = (parsed_url.hostname or "").strip().lower()
    if parsed_url.scheme != "https" and hostname not in _LOCALHOST_NAMES and not hostname.endswith(".localhost"):
        raise ValueError("non-local base_url must use https")

    return normalized_value


def _set_stream_response_timeout(response: Any, timeout_seconds: float) -> None:
    """Best-effort read-timeout adjustment after the HTTPS connection is open."""

    candidate_paths = (
        ("fp", "raw", "_sock"),
        ("fp", "raw", "_fp", "fp", "raw", "_sock"),
    )

    for candidate_path in candidate_paths:
        current_object = response
        for attribute_name in candidate_path:
            current_object = getattr(current_object, attribute_name, None)
            if current_object is None:
                break
        if current_object is None:
            continue
        settimeout = getattr(current_object, "settimeout", None)
        if callable(settimeout):
            try:
                settimeout(timeout_seconds)
            except OSError:
                return
            return
