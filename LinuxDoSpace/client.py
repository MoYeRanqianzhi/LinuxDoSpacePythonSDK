"""LinuxDoSpace Python SDK client.

The SDK keeps one strict runtime architecture:

- one `Client` owns one upstream HTTPS stream
- the client parses every received mail event exactly once
- the client fans parsed messages out to local mailbox bindings in memory

This means the backend only knows about the API token. It does not need to know
which exact mailboxes or regex rules the local process subscribed to.
"""

from __future__ import annotations

import base64
import json
import queue
import re
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
    """Internal representation of one NDJSON stream event line."""

    type: str
    payload: dict[str, Any]


@dataclass(slots=True)
class _StreamFailure:
    """Sent to local listener queues when the shared stream fails fatally."""

    error: LinuxDoSpaceError


@dataclass(slots=True)
class _MailBinding:
    """One locally registered mailbox binding stored in creation order."""

    mode: str
    suffix: str
    allow_overlap: bool
    queue: queue.Queue[object]
    prefix: str | None = None
    compiled_pattern: re.Pattern[str] | None = None
    pattern_text: str | None = None

    def matches(self, local_part: str) -> bool:
        """Report whether this binding matches the provided mailbox local part."""

        if self.mode == "exact":
            return self.prefix == local_part
        if self.compiled_pattern is None:
            return False
        return self.compiled_pattern.fullmatch(local_part) is not None


@dataclass(slots=True)
class _ParsedMailEnvelope:
    """Internal parsed representation shared across all local listeners."""

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
        """Project the internal parsed envelope into the public SDK model."""

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

        self._listeners_lock = threading.Lock()
        self._all_listeners: list[queue.Queue[object]] = []
        self._mail_bindings_by_suffix: dict[str, list[_MailBinding]] = {}

        self._initial_connect_event = threading.Event()
        self._initial_connect_error: LinuxDoSpaceError | None = None
        self._fatal_error: LinuxDoSpaceError | None = None

        # `client.mail` is a callable facade object instead of a plain method.
        # This gives the SDK two intentionally different user-facing styles:
        #
        # - explicit registration: `client.mail.bind(...)`
        # - convenience sugar: `client.mail(...)`
        #
        # Both styles build the exact same `MailBox` object and therefore share
        # the same local ordered matching semantics.
        self.mail: MailBindingFacade = MailBindingFacade(self)

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
        """Close the shared stream when leaving the context manager."""

        self.close()

    @property
    def connected(self) -> bool:
        """Report whether the shared upstream HTTPS stream is currently alive."""

        return self._connected and not self._closed and self._fatal_error is None

    def close(self) -> None:
        """Close the client and terminate all current local listeners."""

        if self._closed:
            return

        self._closed = True
        self._connected = False
        self._close_active_response()
        self._broadcast_control(_CLOSE_SENTINEL)
        self._reader_thread.join(timeout=self._connect_timeout + 1.0)

    def listen(self, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield every mail event received by this client.

        This is the canonical "full intake" interface. It exposes all mail
        events delivered to the current token stream without any mailbox-level
        filtering.
        """

        self._ensure_open()
        listener_queue, unregister = self._register_all_listener()
        try:
            yield from self._iterate_queue(listener_queue, timeout=timeout)
        finally:
            unregister()

    def _build_mailbox(
        self,
        *,
        prefix: str | None = None,
        pattern: str | re.Pattern[str] | None = None,
        suffix: Suffix | str,
        allow_overlap: bool = False,
    ) -> "MailBox":
        """Create one mailbox binding on top of the shared client stream.

        Exactly one of `prefix` or `pattern` must be provided.
        Matching semantics are intentionally simple:

        - all bindings for the same suffix are checked strictly in creation order
        - exact and regex bindings live in the same ordered chain
        - when a binding matches:
          - it receives the message
          - if `allow_overlap` is false, matching stops immediately
          - if `allow_overlap` is true, scanning continues to later bindings
        """

        normalized_suffix = str(suffix).strip().lower()
        if not normalized_suffix:
            raise ValueError("suffix must not be empty")
        if (prefix is None) == (pattern is None):
            raise ValueError("exactly one of prefix or pattern must be provided")

        if prefix is not None:
            normalized_prefix = prefix.strip().lower()
            if not normalized_prefix:
                raise ValueError("prefix must not be empty")
            return MailBox(
                client=self,
                mode="exact",
                suffix=normalized_suffix,
                allow_overlap=allow_overlap,
                prefix=normalized_prefix,
                pattern_text=None,
                compiled_pattern=None,
            )

        return MailBox(
            client=self,
            mode="pattern",
            suffix=normalized_suffix,
            allow_overlap=allow_overlap,
            prefix=None,
            pattern_text=_normalize_pattern_text(pattern),
            compiled_pattern=_compile_pattern(pattern),
        )

    def catch_all(
        self,
        *,
        pattern: str | re.Pattern[str] = r".*",
        suffix: Suffix | str,
        allow_overlap: bool = False,
    ) -> "MailBox":
        """Create one catch-all helper based on regex mailbox matching.

        This method remains available as a short top-level helper, but the
        preferred explicit-registration style is now
        `client.mail.catch_all(...)` or `client.mail.bind(pattern=...)`.
        """

        return self.mail.catch_all(pattern=pattern, suffix=suffix, allow_overlap=allow_overlap)

    def _run_stream_loop(self) -> None:
        """Keep the shared HTTPS stream alive in the background."""

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
                "User-Agent": "LinuxDoSpace Python SDK/0.3.0a1",
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

                    self._dispatch_parsed_envelope(_parse_mail_event(event))
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
        """Fan out one parsed event to the full listener and mailbox bindings."""

        primary_address = parsed_envelope.recipients[0] if parsed_envelope.recipients else ""
        self._broadcast_to_all(parsed_envelope.to_message(primary_address))

        delivered_addresses = set()
        for recipient in parsed_envelope.recipients:
            normalized_recipient = recipient.strip().lower()
            if not normalized_recipient or normalized_recipient in delivered_addresses:
                continue
            delivered_addresses.add(normalized_recipient)
            self._dispatch_to_mail_bindings(normalized_recipient, parsed_envelope.to_message(normalized_recipient))

    def _dispatch_to_mail_bindings(self, address: str, message: MailMessage) -> None:
        """Dispatch one message through the ordered local mailbox-binding chain."""

        local_part, at_sign, suffix = address.partition("@")
        if not local_part or at_sign != "@":
            return

        with self._listeners_lock:
            bindings = list(self._mail_bindings_by_suffix.get(suffix, []))

        for binding in bindings:
            if not binding.matches(local_part):
                continue
            binding.queue.put_nowait(message)
            if not binding.allow_overlap:
                break

    def _broadcast_to_all(self, item: object) -> None:
        """Send one message to every client-level full listener."""

        with self._listeners_lock:
            listeners = list(self._all_listeners)
        for listener_queue in listeners:
            listener_queue.put_nowait(item)

    def _broadcast_control(self, item: object) -> None:
        """Send one control object to every currently registered local listener."""

        with self._listeners_lock:
            full_listeners = list(self._all_listeners)
            mail_listener_queues = [binding.queue for bindings in self._mail_bindings_by_suffix.values() for binding in bindings]

        for listener_queue in full_listeners + mail_listener_queues:
            listener_queue.put_nowait(item)

    def _register_all_listener(self) -> tuple[queue.Queue[object], Callable[[], None]]:
        """Register one client-level full listener queue."""

        listener_queue: queue.Queue[object] = queue.Queue()
        with self._listeners_lock:
            self._all_listeners.append(listener_queue)

        def _unregister() -> None:
            with self._listeners_lock:
                self._all_listeners = [item for item in self._all_listeners if item is not listener_queue]

        return listener_queue, _unregister

    def _register_mail_binding(
        self,
        *,
        mode: str,
        suffix: str,
        allow_overlap: bool,
        prefix: str | None,
        pattern_text: str | None,
        compiled_pattern: re.Pattern[str] | None,
    ) -> tuple[queue.Queue[object], Callable[[], None]]:
        """Register one mailbox binding in the ordered local dispatch chain."""

        listener_queue: queue.Queue[object] = queue.Queue()
        binding = _MailBinding(
            mode=mode,
            suffix=suffix,
            allow_overlap=allow_overlap,
            queue=listener_queue,
            prefix=prefix,
            compiled_pattern=compiled_pattern,
            pattern_text=pattern_text,
        )

        with self._listeners_lock:
            self._mail_bindings_by_suffix.setdefault(suffix, []).append(binding)

        def _unregister() -> None:
            with self._listeners_lock:
                bindings = self._mail_bindings_by_suffix.get(suffix)
                if bindings is None:
                    return
                bindings[:] = [item for item in bindings if item.queue is not listener_queue]
                if not bindings:
                    self._mail_bindings_by_suffix.pop(suffix, None)

        return listener_queue, _unregister

    def _iterate_queue(self, listener_queue: queue.Queue[object], *, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield items from one local listener queue with total timeout control."""

        deadline = None if timeout < 0 else time.monotonic() + float(timeout)

        while not self._closed:
            self._raise_fatal_error()

            remaining_seconds = _remaining_seconds(deadline)
            if remaining_seconds is not None and remaining_seconds <= 0:
                return

            wait_timeout = _WAIT_POLL_INTERVAL_SECONDS if remaining_seconds is None else max(0.01, min(_WAIT_POLL_INTERVAL_SECONDS, remaining_seconds))

            try:
                item = listener_queue.get(timeout=wait_timeout)
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
    """Context-managed mailbox binding living on top of one shared client stream."""

    def __init__(
        self,
        *,
        client: Client,
        mode: str,
        suffix: str,
        allow_overlap: bool,
        prefix: str | None,
        pattern_text: str | None,
        compiled_pattern: re.Pattern[str] | None,
    ) -> None:
        self._client = client
        self.mode = mode
        self.suffix = suffix
        self.allow_overlap = allow_overlap
        self.prefix = prefix
        self.pattern = pattern_text
        self._compiled_pattern = compiled_pattern
        self.address = f"{self.prefix}@{self.suffix}" if self.prefix is not None else None
        self._closed = False

    def __enter__(self) -> "MailBox":
        """Return the active mailbox binding helper."""

        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        """Mark the logical mailbox binding as closed."""

        self.close()

    def close(self) -> None:
        """Close the logical mailbox binding."""

        self._closed = True

    def listen(self, timeout: float = -1) -> Iterator[MailMessage]:
        """Yield messages matching this mailbox binding from the shared client stream."""

        if self._closed:
            raise LinuxDoSpaceError("mailbox stream is already closed")

        listener_queue, unregister = self._client._register_mail_binding(
            mode=self.mode,
            suffix=self.suffix,
            allow_overlap=self.allow_overlap,
            prefix=self.prefix,
            pattern_text=self.pattern,
            compiled_pattern=self._compiled_pattern,
        )
        try:
            yield from self._client._iterate_queue(listener_queue, timeout=timeout)
        finally:
            unregister()


class MailBindingFacade:
    """Callable mailbox-registration facade exposed as `client.mail`.

    The facade exists to make the API shape explicit:

    - `client.mail.bind(...)` is the primary, explicit registration form
    - `client.mail(...)` is only syntactic sugar over `bind(...)`

    Both forms intentionally return the same `MailBox` object so that `with`
    remains a natural context-managed convenience layer on top of explicit
    mailbox registration.
    """

    def __init__(self, client: Client) -> None:
        """Bind the facade to exactly one shared `Client` instance."""

        self._client = client

    def bind(
        self,
        *,
        prefix: str | None = None,
        pattern: str | re.Pattern[str] | None = None,
        suffix: Suffix | str,
        allow_overlap: bool = False,
    ) -> MailBox:
        """Register one mailbox binding explicitly.

        This is the preferred public API because it makes mailbox registration
        visually distinct from the later `listen(...)` step.
        """

        return self._client._build_mailbox(
            prefix=prefix,
            pattern=pattern,
            suffix=suffix,
            allow_overlap=allow_overlap,
        )

    def __call__(
        self,
        *,
        prefix: str | None = None,
        pattern: str | re.Pattern[str] | None = None,
        suffix: Suffix | str,
        allow_overlap: bool = False,
    ) -> MailBox:
        """Return the same mailbox binding as `bind(...)`.

        This keeps `client.mail(...)` and `with client.mail(...)` working as
        syntactic sugar without introducing a second code path.
        """

        return self.bind(
            prefix=prefix,
            pattern=pattern,
            suffix=suffix,
            allow_overlap=allow_overlap,
        )

    def catch_all(
        self,
        *,
        pattern: str | re.Pattern[str] = r".*",
        suffix: Suffix | str,
        allow_overlap: bool = False,
    ) -> MailBox:
        """Build one regex-based catch-all mailbox helper explicitly."""

        return self.bind(pattern=pattern, suffix=suffix, allow_overlap=allow_overlap)


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

    recipients = tuple(
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
        recipients=recipients,
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


def _compile_pattern(value: str | re.Pattern[str] | None) -> re.Pattern[str]:
    """Compile one mailbox regex pattern into a reusable regex object."""

    if value is None:
        raise ValueError("pattern must not be empty")
    if isinstance(value, re.Pattern):
        return value
    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError("pattern must not be empty")
    return re.compile(normalized_value)


def _normalize_pattern_text(value: str | re.Pattern[str] | None) -> str:
    """Normalize one pattern into a user-facing text representation."""

    if value is None:
        raise ValueError("pattern must not be empty")
    if isinstance(value, re.Pattern):
        return value.pattern
    normalized_value = value.strip()
    if not normalized_value:
        raise ValueError("pattern must not be empty")
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
