"""LinuxDoSpace Python SDK client.

This SDK intentionally keeps one architectural invariant:

- one `Client` owns exactly one upstream HTTPS stream
- the client parses every received mail event once
- the client then fan-outs parsed messages to local sub-listeners created by
  `client.mail(...)`

This matches the backend contract: the server knows only the API token, while
mailbox-level filtering and sub-dispatch happen entirely inside the client.
"""

from __future__ import annotations

import base64
import json
import queue
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
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
_RECONNECT_DELAY_SECONDS = 0.3
_WAIT_POLL_INTERVAL_SECONDS = 0.2
_CLOSE_SENTINEL = object()


@dataclass(slots=True)
class _StreamEvent:
    """Internal representation of one raw NDJSON stream event."""

    type: str
    payload: dict[str, Any]


@dataclass(slots=True)
class _StreamFailure:
    """Sent through subscriber queues when the shared stream fails fatally."""

    error: LinuxDoSpaceError


@dataclass(slots=True)
class _ParsedMailEnvelope:
    """Internal parsed representation shared across all local subscribers."""

    sender: str
    recipients: tuple[str, ...]
    received_at: datetime

    subject: str
    message_id: str | None
    date: datetime | None

    from_header: str
    to_header: str
    cc_header: str
    reply_to_header: str

    from_addresses: tuple[str, ...]
    to_addresses: tuple[str, ...]
    cc_addresses: tuple[str, ...]
    reply_to_addresses: tuple[str, ...]

    text: str
    html: str
    headers: dict[str, str]

    raw: str
    raw_bytes: bytes
    message: EmailMessage

    def to_message(self, address: str) -> MailMessage:
        """Project the parsed envelope into the public SDK model."""

        return MailMessage(
            address=address,
            sender=self.sender,
            recipients=self.recipients,
            received_at=self.received_at,
            subject=self.subject,
            message_id=self.message_id,
            date=self.date,
            from_header=self.from_header,
            to_header=self.to_header,
            cc_header=self.cc_header,
            reply_to_header=self.reply_to_header,
            from_addresses=self.from_addresses,
            to_addresses=self.to_addresses,
            cc_addresses=self.cc_addresses,
            reply_to_addresses=self.reply_to_addresses,
            text=self.text,
            html=self.html,
            headers=self.headers,
            raw=self.raw,
            raw_bytes=self.raw_bytes,
            message=self.message,
        )


class Client:
    """Top-level LinuxDoSpace SDK client.

    One client owns exactly one upstream HTTPS stream and keeps it alive in a
    background thread from the moment the client is created.

    Parameters
    ----------
    token:
        The plaintext API token generated from the LinuxDoSpace web console.
    base_url:
        Backend base URL. Production defaults to `https://api.linuxdo.space`.
    connect_timeout:
        Timeout used when establishing one HTTPS connection.
    stream_socket_timeout:
        Per-read socket timeout for the live stream. When the underlying
        connection stalls longer than this interval, the client reconnects.
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
        if self._connect_timeout <= 0:
            raise ValueError("connect_timeout must be greater than 0")
        if self._stream_socket_timeout <= 0:
            raise ValueError("stream_socket_timeout must be greater than 0")

        self._closed = False
        self._connected = False
        self._active_response_lock = threading.Lock()
        self._active_response: Any | None = None
        self._subscribers_lock = threading.Lock()
        self._all_subscribers: set[queue.Queue[object]] = set()
        self._mail_subscribers: dict[str, set[queue.Queue[object]]] = {}

        self._initial_connect_event = threading.Event()
        self._initial_connect_error: LinuxDoSpaceError | None = None
        self._fatal_error: LinuxDoSpaceError | None = None

        self._reader_thread = threading.Thread(
            target=self._run_stream_loop,
            name="LinuxDoSpaceClientStream",
            daemon=True,
        )
        self._reader_thread.start()

        wait_timeout = self._connect_timeout + 1.0
        if not self._initial_connect_event.wait(timeout=wait_timeout):
            self.close()
            raise StreamError("timed out while opening the LinuxDoSpace HTTPS mail stream")
        if self._initial_connect_error is not None:
            self.close()
            raise self._initial_connect_error

    def __enter__(self) -> "Client":
        """Allow the client itself to be managed with `with Client(...)`."""

        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Close the shared stream thread on context-manager exit."""

        self.close()

    def close(self) -> None:
        """Close the client and terminate all current local listeners."""

        if self._closed:
            return

        self._closed = True
        self._connected = False
        self._close_active_response()
        self._broadcast_control(_CLOSE_SENTINEL)
        self._reader_thread.join(timeout=self._connect_timeout + 1.0)

    @property
    def connected(self) -> bool:
        """Report whether the shared upstream HTTPS stream is currently alive."""

        return self._connected and not self._closed and self._fatal_error is None

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

    def listen(self, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield every mail event received by this client.

        This is the "full receive" interface: it exposes all events delivered
        to the current token stream, regardless of individual mailbox bindings.
        The lighter `mail(...).listen()` helper is implemented as a filtered
        local subscription on top of the same underlying client stream.
        """

        self._ensure_open()
        subscriber_queue, unregister = self._register_all_listener()
        try:
            yield from self._iterate_queue(subscriber_queue, timeout=timeout)
        finally:
            unregister()

    def _run_stream_loop(self) -> None:
        """Keep the single shared HTTPS stream alive in the background."""

        initial_attempt = True

        while not self._closed and self._fatal_error is None:
            try:
                self._consume_stream_once()
                self._connected = False
                if initial_attempt and not self._initial_connect_event.is_set():
                    self._initial_connect_event.set()
                initial_attempt = False
            except AuthenticationError as exc:
                self._connected = False
                self._fatal_error = exc
                if initial_attempt and not self._initial_connect_event.is_set():
                    self._initial_connect_error = exc
                    self._initial_connect_event.set()
                self._broadcast_control(_StreamFailure(exc))
                return
            except LinuxDoSpaceError as exc:
                self._connected = False
                if initial_attempt and not self._initial_connect_event.is_set():
                    self._initial_connect_error = exc
                    self._initial_connect_event.set()
                    return
                initial_attempt = False
            except Exception as exc:  # pragma: no cover - final safety net
                wrapped_error = StreamError(f"unexpected LinuxDoSpace SDK stream failure: {exc}")
                self._connected = False
                if initial_attempt and not self._initial_connect_event.is_set():
                    self._initial_connect_error = wrapped_error
                    self._initial_connect_event.set()
                    return
                self._fatal_error = wrapped_error
                self._broadcast_control(_StreamFailure(wrapped_error))
                return

            if self._closed:
                return
            time.sleep(_RECONNECT_DELAY_SECONDS)

    def _consume_stream_once(self) -> None:
        """Open the shared HTTPS stream once and consume it until it ends."""

        request = urllib.request.Request(
            url=f"{self._base_url}{_STREAM_PATH}",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/x-ndjson",
                "User-Agent": "LinuxDoSpace Python SDK/0.2.0a1",
            },
            method="GET",
        )

        try:
            with self._urlopen(request, timeout=self._connect_timeout) as response:
                with self._active_response_lock:
                    self._active_response = response
                status_code = getattr(response, "status", 200)
                if status_code != 200:
                    raise StreamError(f"unexpected stream status code: {status_code}")

                self._connected = True
                if not self._initial_connect_event.is_set():
                    self._initial_connect_event.set()

                _set_stream_response_timeout(response, self._stream_socket_timeout)

                while not self._closed:
                    raw_line = response.readline()
                    if not raw_line:
                        return

                    stripped_line = raw_line.strip()
                    if not stripped_line:
                        continue

                    event = _decode_stream_event(stripped_line)
                    if event.type in {_READY_EVENT_TYPE, _HEARTBEAT_EVENT_TYPE}:
                        continue
                    if event.type != _MAIL_EVENT_TYPE:
                        continue

                    parsed_envelope = _parse_mail_event(event)
                    self._dispatch_parsed_envelope(parsed_envelope)
        except urllib.error.HTTPError as exc:
            if exc.code in {401, 403}:
                raise AuthenticationError("api token was rejected by the LinuxDoSpace backend") from exc
            raise StreamError(f"failed to open LinuxDoSpace mail stream: http {exc.code}") from exc
        except urllib.error.URLError as exc:
            if isinstance(exc.reason, socket.timeout):
                raise StreamError("timed out while connecting to the LinuxDoSpace mail stream") from exc
            raise StreamError(f"failed to connect to LinuxDoSpace mail stream: {exc.reason}") from exc
        except socket.timeout as exc:
            raise StreamError("LinuxDoSpace mail stream stalled and will be reconnected") from exc
        finally:
            with self._active_response_lock:
                self._active_response = None

    def _dispatch_parsed_envelope(self, parsed_envelope: _ParsedMailEnvelope) -> None:
        """Fan-out one parsed event to the all-listener and mailbox listeners."""

        if parsed_envelope.recipients:
            primary_address = parsed_envelope.recipients[0]
        else:
            primary_address = ""

        self._broadcast_to_all(parsed_envelope.to_message(primary_address))

        delivered_addresses = set()
        for recipient in parsed_envelope.recipients:
            normalized_recipient = recipient.strip().lower()
            if not normalized_recipient or normalized_recipient in delivered_addresses:
                continue
            delivered_addresses.add(normalized_recipient)
            self._broadcast_to_mailbox(normalized_recipient, parsed_envelope.to_message(normalized_recipient))

    def _broadcast_to_all(self, item: object) -> None:
        """Send one message to every client-level listener."""

        with self._subscribers_lock:
            subscribers = list(self._all_subscribers)
        for subscriber_queue in subscribers:
            subscriber_queue.put_nowait(item)

    def _broadcast_to_mailbox(self, address: str, item: object) -> None:
        """Send one message to every mailbox listener registered for `address`."""

        with self._subscribers_lock:
            subscribers = list(self._mail_subscribers.get(address, set()))
        for subscriber_queue in subscribers:
            subscriber_queue.put_nowait(item)

    def _broadcast_control(self, item: object) -> None:
        """Send one control object to every current listener queue."""

        with self._subscribers_lock:
            all_subscribers = list(self._all_subscribers)
            mailbox_subscribers = [subscriber for subscribers in self._mail_subscribers.values() for subscriber in subscribers]

        for subscriber_queue in all_subscribers + mailbox_subscribers:
            subscriber_queue.put_nowait(item)

    def _register_all_listener(self) -> tuple[queue.Queue[object], Callable[[], None]]:
        """Register one client-level listener queue."""

        subscriber_queue: queue.Queue[object] = queue.Queue()
        with self._subscribers_lock:
            self._all_subscribers.add(subscriber_queue)

        def _unregister() -> None:
            with self._subscribers_lock:
                self._all_subscribers.discard(subscriber_queue)

        return subscriber_queue, _unregister

    def _register_mail_listener(self, address: str) -> tuple[queue.Queue[object], Callable[[], None]]:
        """Register one mailbox-level listener queue for a specific address."""

        normalized_address = address.strip().lower()
        subscriber_queue: queue.Queue[object] = queue.Queue()
        with self._subscribers_lock:
            self._mail_subscribers.setdefault(normalized_address, set()).add(subscriber_queue)

        def _unregister() -> None:
            with self._subscribers_lock:
                subscribers = self._mail_subscribers.get(normalized_address)
                if subscribers is None:
                    return
                subscribers.discard(subscriber_queue)
                if not subscribers:
                    self._mail_subscribers.pop(normalized_address, None)

        return subscriber_queue, _unregister

    def _iterate_queue(self, subscriber_queue: queue.Queue[object], *, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield items from one local subscriber queue with total timeout control."""

        deadline = None if timeout < 0 else time.monotonic() + float(timeout)

        while not self._closed:
            self._raise_fatal_error()

            remaining_seconds = _remaining_seconds(deadline)
            if remaining_seconds is not None and remaining_seconds <= 0:
                return

            wait_timeout = _WAIT_POLL_INTERVAL_SECONDS if remaining_seconds is None else max(0.01, min(_WAIT_POLL_INTERVAL_SECONDS, remaining_seconds))

            try:
                item = subscriber_queue.get(timeout=wait_timeout)
            except queue.Empty:
                continue

            if item is _CLOSE_SENTINEL:
                return
            if isinstance(item, _StreamFailure):
                raise item.error
            if isinstance(item, MailMessage):
                yield item

    def _ensure_open(self) -> None:
        """Reject listen attempts after the client has been closed."""

        if self._closed:
            raise LinuxDoSpaceError("client is already closed")
        self._raise_fatal_error()

    def _raise_fatal_error(self) -> None:
        """Raise any previously recorded fatal stream error."""

        if self._fatal_error is not None:
            raise self._fatal_error

    def _close_active_response(self) -> None:
        """Best-effort close of the live upstream response to unblock readers."""

        with self._active_response_lock:
            response = self._active_response
        if response is None:
            return

        close = getattr(response, "close", None)
        if callable(close):
            try:
                close()
            except OSError:
                return


class MailBox:
    """Context-managed mailbox listener bound to one concrete address."""

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
        """Mark the logical mailbox helper as closed."""

        self.close()

    def close(self) -> None:
        """Close the logical mailbox helper."""

        self._closed = True

    def listen(self, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield messages matching this mailbox address from the shared client stream."""

        if self._closed:
            raise LinuxDoSpaceError("mailbox stream is already closed")

        subscriber_queue, unregister = self._client._register_mail_listener(self.address)
        try:
            yield from self._client._iterate_queue(subscriber_queue, timeout=timeout)
        finally:
            unregister()


def _decode_stream_event(raw_line: bytes) -> _StreamEvent:
    """Decode one NDJSON line into the internal event representation."""

    try:
        payload = json.loads(raw_line.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise StreamError("received invalid JSON from the LinuxDoSpace mail stream") from exc

    event_type = str(payload.get("type", "")).strip().lower()
    if not event_type:
        raise StreamError("received stream event without a type field")

    return _StreamEvent(type=event_type, payload=payload)


def _parse_mail_event(event: _StreamEvent) -> _ParsedMailEnvelope:
    """Parse one raw stream event into a reusable internal mail envelope."""

    recipient_candidates = tuple(
        str(value).strip().lower()
        for value in event.payload.get("original_recipients", [])
        if str(value).strip()
    )
    raw_message_base64 = str(event.payload.get("raw_message_base64", "")).strip()
    if not raw_message_base64:
        raise StreamError("mail event did not include raw_message_base64")

    try:
        raw_bytes = base64.b64decode(raw_message_base64, validate=True)
    except (ValueError, TypeError) as exc:
        raise StreamError("mail event contained invalid base64 message data") from exc

    email_message = BytesParser(policy=default_email_policy).parsebytes(raw_bytes)
    text_body, html_body = _extract_message_bodies(email_message)

    return _ParsedMailEnvelope(
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


def _optional_header(message: EmailMessage, header_name: str) -> str | None:
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


def _extract_message_bodies(message: EmailMessage) -> tuple[str, str]:
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
