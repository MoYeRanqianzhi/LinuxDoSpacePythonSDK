"""Integration-style tests for the LinuxDoSpace Python SDK."""

from __future__ import annotations

import base64
import json
import queue
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from LinuxDoSpace import Client, Suffix


class LinuxDoSpaceSDKTests(unittest.TestCase):
    """Validate the single-connection client architecture end to end."""

    def test_client_rejects_non_https_remote_base_url(self) -> None:
        """Remote non-local backend URLs should be rejected to protect the token."""

        with self.assertRaises(ValueError):
            Client(token="lds_pat.tok123.supersecret", base_url="http://example.com")

    def test_client_connects_immediately(self) -> None:
        """Constructing the client should open the single upstream stream immediately."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        self.assertTrue(server.wait_for_requests(1, timeout=2.0))
        self.assertEqual(server.request_count, 1)
        self.assertTrue(client.connected)

    def test_client_listen_receives_all_messages(self) -> None:
        """The full client-level listener should expose every message received by the token."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        collected: list[tuple[str, str]] = []

        def _consume() -> None:
            for message in client.listen(timeout=0.4):
                collected.append((message.address, message.subject))

        listener = threading.Thread(target=_consume)
        listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Alice Mail", "alice body"),
        )
        server.publish_mail(
            "bob@linuxdo.space",
            _raw_message("bob@linuxdo.space", "Bob Mail", "bob body"),
        )

        listener.join(timeout=2.0)

        self.assertEqual(
            collected,
            [
                ("alice@linuxdo.space", "Alice Mail"),
                ("bob@linuxdo.space", "Bob Mail"),
            ],
        )
        self.assertEqual(server.request_count, 1)

    def test_parallel_mailboxes_share_one_upstream_connection(self) -> None:
        """Multiple mailbox listeners should share the single client stream."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        results: "queue.Queue[tuple[str, str]]" = queue.Queue()

        def _listen(prefix: str) -> None:
            with client.mail(prefix, Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    results.put((prefix, message.subject))
                    break

        listeners = [threading.Thread(target=_listen, args=(prefix,)) for prefix in ("alice", "bob", "carol")]
        for listener in listeners:
            listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Alice Mail", "alice body"),
        )
        server.publish_mail(
            "bob@linuxdo.space",
            _raw_message("bob@linuxdo.space", "Bob Mail", "bob body"),
        )
        server.publish_mail(
            "carol@linuxdo.space",
            _raw_message("carol@linuxdo.space", "Carol Mail", "carol body"),
        )

        for listener in listeners:
            listener.join(timeout=2.0)

        received = sorted(results.get_nowait() for _ in range(results.qsize()))
        self.assertEqual(
            received,
            [("alice", "Alice Mail"), ("bob", "Bob Mail"), ("carol", "Carol Mail")],
        )
        self.assertEqual(server.request_count, 1)

    def test_mailboxes_filter_locally_without_server_side_mailbox_awareness(self) -> None:
        """Mailbox listeners should receive only their own messages while the server still sees one token stream."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        alice_subjects: list[str] = []
        bob_subjects: list[str] = []

        def _listen_alice() -> None:
            with client.mail("alice", Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    alice_subjects.append(message.subject)

        def _listen_bob() -> None:
            with client.mail("bob", Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    bob_subjects.append(message.subject)

        alice_listener = threading.Thread(target=_listen_alice)
        bob_listener = threading.Thread(target=_listen_bob)
        alice_listener.start()
        bob_listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Alice Only", "alice body"),
        )
        server.publish_mail(
            "bob@linuxdo.space",
            _raw_message("bob@linuxdo.space", "Bob Only", "bob body"),
        )
        server.publish_mail(
            "carol@linuxdo.space",
            _raw_message("carol@linuxdo.space", "Carol Only", "carol body"),
        )

        alice_listener.join(timeout=2.0)
        bob_listener.join(timeout=2.0)

        self.assertEqual(alice_subjects, ["Alice Only"])
        self.assertEqual(bob_subjects, ["Bob Only"])
        self.assertEqual(server.request_count, 1)

    def test_full_listener_and_mailbox_listeners_can_run_together(self) -> None:
        """The full listener and filtered mailbox listeners should share one upstream stream."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        all_subjects: list[str] = []
        alice_subjects: list[str] = []

        def _listen_all() -> None:
            for message in client.listen(timeout=0.4):
                all_subjects.append(message.subject)

        def _listen_alice() -> None:
            with client.mail("alice", Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    alice_subjects.append(message.subject)

        all_listener = threading.Thread(target=_listen_all)
        alice_listener = threading.Thread(target=_listen_alice)
        all_listener.start()
        alice_listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Alice Shared", "alice body"),
        )
        server.publish_mail(
            "bob@linuxdo.space",
            _raw_message("bob@linuxdo.space", "Bob Shared", "bob body"),
        )

        all_listener.join(timeout=2.0)
        alice_listener.join(timeout=2.0)

        self.assertEqual(all_subjects, ["Alice Shared", "Bob Shared"])
        self.assertEqual(alice_subjects, ["Alice Shared"])
        self.assertEqual(server.request_count, 1)

    def test_burst_delivery_remains_stable(self) -> None:
        """A short burst of many messages should be delivered without loss."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        received_subjects: list[str] = []

        def _consume() -> None:
            with client.mail("alice", Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.5):
                    received_subjects.append(message.subject)

        listener = threading.Thread(target=_consume)
        listener.start()
        time.sleep(0.05)

        for index in range(50):
            server.publish_mail(
                "alice@linuxdo.space",
                _raw_message("alice@linuxdo.space", f"Burst {index}", f"body {index}"),
            )

        listener.join(timeout=2.0)

        self.assertEqual(len(received_subjects), 50)
        self.assertEqual(received_subjects[0], "Burst 0")
        self.assertEqual(received_subjects[-1], "Burst 49")
        self.assertEqual(server.request_count, 1)


class _StreamingRequestHandler(BaseHTTPRequestHandler):
    """Broadcast NDJSON server used by local integration tests."""

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/v1/token/email/stream":
            self.send_response(404)
            self.end_headers()
            return

        self.server.record_request()
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
        """Silence test-server request logs."""

        return


class _ThreadingTestHTTPServer(ThreadingHTTPServer):
    """Local broadcast server with request counting and subscriber fan-out."""

    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], handler: type[BaseHTTPRequestHandler]) -> None:
        super().__init__(server_address, handler)
        self.stop_event = threading.Event()
        self._subscribers: list[object] = []
        self._subscribers_lock = threading.Lock()
        self._request_count = 0
        self._request_count_lock = threading.Lock()

    @property
    def request_count(self) -> int:
        """Return how many upstream client connections were accepted."""

        with self._request_count_lock:
            return self._request_count

    def record_request(self) -> None:
        """Increment the accepted request count."""

        with self._request_count_lock:
            self._request_count += 1

    def wait_for_requests(self, expected_count: int, timeout: float) -> bool:
        """Wait until the expected number of client connections has arrived."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.request_count >= expected_count:
                return True
            time.sleep(0.01)
        return False

    def register_subscriber(self, writer: object) -> None:
        """Register one live HTTP response writer for broadcast delivery."""

        with self._subscribers_lock:
            self._subscribers.append(writer)

    def unregister_subscriber(self, writer: object) -> None:
        """Remove one HTTP response writer from the broadcast set."""

        with self._subscribers_lock:
            self._subscribers = [item for item in self._subscribers if item is not writer]

    def publish_mail(self, recipient: str, raw_message: bytes) -> None:
        """Push one mail event to every currently connected client stream."""

        event_line = _event_line(
            {
                "type": "mail",
                "original_envelope_from": "bounce@example.com",
                "original_recipients": [recipient],
                "received_at": "2026-03-20T10:11:12Z",
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
        """Stop subscriber loops before shutting the HTTP server down."""

        self.stop_event.set()
        super().shutdown()


def _event_line(payload: dict[str, object]) -> bytes:
    """Encode one fake NDJSON stream line."""

    return json.dumps(payload).encode("utf-8") + b"\n"


def _raw_message(recipient: str, subject: str, body: str) -> bytes:
    """Build one minimal RFC 5322 message for local integration tests."""

    return (
        f"From: Sender <sender@example.com>\r\n"
        f"To: Receiver <{recipient}>\r\n"
        f"Subject: {subject}\r\n"
        "\r\n"
        f"{body}"
    ).encode("utf-8")


def _start_stream_server() -> tuple[_ThreadingTestHTTPServer, threading.Thread]:
    """Start the local broadcast HTTP stream server used by the integration tests."""

    server = _ThreadingTestHTTPServer(("127.0.0.1", 0), _StreamingRequestHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _cleanup_stream_server(server: _ThreadingTestHTTPServer, thread: threading.Thread) -> None:
    """Shut the local stream server down in a Windows-friendly order."""

    server.shutdown()
    thread.join(timeout=1.0)
    server.server_close()


if __name__ == "__main__":
    unittest.main()
