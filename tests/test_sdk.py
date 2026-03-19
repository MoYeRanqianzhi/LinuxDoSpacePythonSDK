"""Unit tests for the LinuxDoSpace Python SDK."""

from __future__ import annotations

import base64
import json
import queue
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from LinuxDoSpace import Client, Suffix


class _FakeResponse:
    """Minimal context-managed response object used by the SDK unit tests."""

    def __init__(self, lines: list[bytes], status: int = 200) -> None:
        self._lines = list(lines)
        self.status = status

    def __enter__(self) -> "_FakeResponse":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        return None

    def readline(self) -> bytes:
        if not self._lines:
            return b""
        return self._lines.pop(0)


class _RecordingTransport:
    """Callable test double that records the outgoing HTTPS request."""

    def __init__(self, responses: list[_FakeResponse]) -> None:
        self._responses = list(responses)
        self.urls: list[str] = []
        self.authorization_headers: list[str] = []

    def __call__(self, request: object, timeout: float | None = None) -> _FakeResponse:
        self.urls.append(getattr(request, "full_url"))
        self.authorization_headers.append(getattr(request, "headers").get("Authorization", ""))
        if not self._responses:
            raise AssertionError("test transport exhausted")
        return self._responses.pop(0)


class LinuxDoSpaceSDKTests(unittest.TestCase):
    """End-user-facing SDK behavior tests."""

    def test_suffix_string_matches_expected_domain(self) -> None:
        """The public enum should stringify to the real mailbox suffix."""

        self.assertEqual(str(Suffix.linuxdo_space), "linuxdo.space")

    def test_mail_listen_yields_parsed_message(self) -> None:
        """A matching `mail` event should become a fully parsed `MailMessage`."""

        raw_message = (
            b"From: Sender <sender@example.com>\r\n"
            b"To: Alice <alice@linuxdo.space>\r\n"
            b"Subject: Hello LinuxDoSpace\r\n"
            b"Message-ID: <msg-1@example.com>\r\n"
            b"\r\n"
            b"plain body"
        )
        response = _FakeResponse(
            [
                _event_line({"type": "ready", "token_public_id": "tok123"}),
                _event_line(
                    {
                        "type": "mail",
                        "original_envelope_from": "bounce@example.com",
                        "original_recipients": ["alice@linuxdo.space", "other@linuxdo.space"],
                        "received_at": "2026-03-19T14:15:16Z",
                        "raw_message_base64": base64.b64encode(raw_message).decode("ascii"),
                    }
                ),
            ]
        )
        transport = _RecordingTransport([response])
        client = Client(token="lds_pat.tok123.supersecret", _urlopen=transport)

        with client.mail("alice", Suffix.linuxdo_space) as mailbox:
            iterator = mailbox.listen(timeout=1)
            message = next(iterator)

        self.assertEqual(message.address, "alice@linuxdo.space")
        self.assertEqual(message.sender, "bounce@example.com")
        self.assertEqual(message.subject, "Hello LinuxDoSpace")
        self.assertEqual(message.message_id, "<msg-1@example.com>")
        self.assertEqual(message.text, "plain body")
        self.assertEqual(message.to_addresses, ("alice@linuxdo.space",))
        self.assertEqual(message.from_addresses, ("sender@example.com",))
        self.assertIn("/v1/token/email/stream", transport.urls[0])
        self.assertEqual(transport.authorization_headers[0], "Bearer lds_pat.tok123.supersecret")

    def test_mail_listen_skips_events_for_other_mailboxes(self) -> None:
        """The mailbox listener should ignore events that do not target its address."""

        raw_message = (
            b"From: Sender <sender@example.com>\r\n"
            b"To: Bob <bob@linuxdo.space>\r\n"
            b"Subject: Not For Alice\r\n"
            b"\r\n"
            b"ignored"
        )
        response = _FakeResponse(
            [
                _event_line({"type": "heartbeat", "token_public_id": "tok123"}),
                _event_line(
                    {
                        "type": "mail",
                        "original_envelope_from": "bounce@example.com",
                        "original_recipients": ["bob@linuxdo.space"],
                        "received_at": "2026-03-19T14:15:16Z",
                        "raw_message_base64": base64.b64encode(raw_message).decode("ascii"),
                    }
                ),
            ]
        )
        transport = _RecordingTransport([response])
        client = Client(token="lds_pat.tok123.supersecret", _urlopen=transport)

        with client.mail("alice", Suffix.linuxdo_space) as mailbox:
            messages = list(mailbox.listen(timeout=0.01))

        self.assertEqual(messages, [])

    def test_client_rejects_non_https_remote_base_url(self) -> None:
        """Remote non-local backend URLs should be rejected to protect the token."""

        with self.assertRaises(ValueError):
            Client(token="lds_pat.tok123.supersecret", base_url="http://example.com")

    def test_real_local_server_supports_parallel_mailboxes(self) -> None:
        """Multiple mailbox bindings should listen in parallel on one real local server."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )

        results: "queue.Queue[tuple[str, str]]" = queue.Queue()

        def _listen(prefix: str) -> None:
            with client.mail(prefix, Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    results.put((prefix, message.subject))
                    break

        listeners = [threading.Thread(target=_listen, args=(prefix,)) for prefix in ("alice", "bob", "carol")]
        for listener in listeners:
            listener.start()
        self.assertTrue(server.wait_for_subscribers(3, timeout=2.0))
        server.publish_mail(
            "alice@linuxdo.space",
            (
                b"From: Sender <sender@example.com>\r\n"
                b"To: Alice <alice@linuxdo.space>\r\n"
                b"Subject: Alice Mail\r\n\r\n"
                b"alice body"
            ),
        )
        server.publish_mail(
            "bob@linuxdo.space",
            (
                b"From: Sender <sender@example.com>\r\n"
                b"To: Bob <bob@linuxdo.space>\r\n"
                b"Subject: Bob Mail\r\n\r\n"
                b"bob body"
            ),
        )
        server.publish_mail(
            "carol@linuxdo.space",
            (
                b"From: Sender <sender@example.com>\r\n"
                b"To: Carol <carol@linuxdo.space>\r\n"
                b"Subject: Carol Mail\r\n\r\n"
                b"carol body"
            ),
        )
        for listener in listeners:
            listener.join(timeout=2.0)

        received = sorted(results.get_nowait() for _ in range(results.qsize()))
        self.assertEqual(
            received,
            [("alice", "Alice Mail"), ("bob", "Bob Mail"), ("carol", "Carol Mail")],
        )

    def test_real_local_server_stays_stable_under_burst(self) -> None:
        """One mailbox should remain stable under a short burst of many messages."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )

        received_subjects: list[str] = []
        def _listen() -> None:
            with client.mail("alice", Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.5):
                    received_subjects.append(message.subject)

        listener = threading.Thread(target=_listen)
        listener.start()
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))
        raw_message = (
            b"From: Sender <sender@example.com>\r\n"
            b"To: Alice <alice@linuxdo.space>\r\n"
            b"Subject: Burst Mail\r\n\r\n"
            b"burst body"
        )
        for _ in range(50):
            server.publish_mail("alice@linuxdo.space", raw_message)
        listener.join(timeout=2.0)

        self.assertEqual(len(received_subjects), 50)
        self.assertTrue(all(subject == "Burst Mail" for subject in received_subjects))


def _event_line(payload: dict[str, object]) -> bytes:
    """Encode one fake NDJSON stream event line."""

    return json.dumps(payload).encode("utf-8") + b"\n"


if __name__ == "__main__":
    unittest.main()


class _StreamingRequestHandler(BaseHTTPRequestHandler):
    """Tiny local broadcast NDJSON stream server used by the real-process SDK tests."""

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/v1/token/email/stream":
            self.send_response(404)
            self.end_headers()
            return

        self.send_response(200)
        self.send_header("Content-Type", "application/x-ndjson; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()

        try:
            self.wfile.write(_event_line({"type": "ready", "token_public_id": "tok123"}))
            self.wfile.flush()
        except OSError:
            return

        self.server.register_subscriber(self.wfile)
        try:
            while not self.server.stop_event.is_set():
                time.sleep(0.05)
        finally:
            self.server.unregister_subscriber(self.wfile)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A003
        """Silence the local integration server logs during tests."""

        return


class _ThreadingTestHTTPServer(ThreadingHTTPServer):
    """Typed local server that can broadcast one event to all live subscribers."""

    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler]) -> None:
        super().__init__(server_address, handler)
        self.stop_event = threading.Event()
        self._subscribers: list[object] = []
        self._subscribers_lock = threading.Lock()

    def register_subscriber(self, writer: object) -> None:
        with self._subscribers_lock:
            self._subscribers.append(writer)

    def unregister_subscriber(self, writer: object) -> None:
        with self._subscribers_lock:
            self._subscribers = [item for item in self._subscribers if item is not writer]

    def wait_for_subscribers(self, expected_count: int, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._subscribers_lock:
                if len(self._subscribers) >= expected_count:
                    return True
            time.sleep(0.01)
        return False

    def publish_mail(self, recipient: str, raw_message: bytes) -> None:
        event_line = _event_line(
            {
                "type": "mail",
                "original_envelope_from": "bounce@example.com",
                "original_recipients": [recipient],
                "received_at": "2026-03-19T14:15:16Z",
                "raw_message_base64": base64.b64encode(raw_message).decode("ascii"),
            }
        )

        with self._subscribers_lock:
            subscribers = list(self._subscribers)

        for writer in subscribers:
            try:
                writer.write(event_line)
                writer.flush()
            except OSError:
                self.unregister_subscriber(writer)

    def shutdown(self) -> None:
        self.stop_event.set()
        super().shutdown()


def _start_stream_server() -> tuple[_ThreadingTestHTTPServer, threading.Thread]:
    """Start one real local broadcast HTTP streaming server for integration-style tests."""

    server = _ThreadingTestHTTPServer(("127.0.0.1", 0), _StreamingRequestHandler)

    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _cleanup_stream_server(server: _ThreadingTestHTTPServer, thread: threading.Thread) -> None:
    """Stop the local test server in a Windows-friendly order."""

    server.shutdown()
    thread.join(timeout=1.0)
    server.server_close()
