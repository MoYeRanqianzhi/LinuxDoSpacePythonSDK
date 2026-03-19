"""Integration-style tests for the LinuxDoSpace Python SDK."""

from __future__ import annotations

import base64
import json
import queue
import re
import threading
import time
import unittest
from datetime import datetime, timezone
from email.message import EmailMessage
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from LinuxDoSpace import Client, LinuxDoSpaceError, Suffix
from LinuxDoSpace.models import MailMessage


class LinuxDoSpaceSDKTests(unittest.TestCase):
    """Validate the single-stream shared-client architecture end to end."""

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
            with client.mail.bind(prefix=prefix, suffix=Suffix.linuxdo_space) as mailbox:
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

    def test_mailbox_registers_immediately_and_unbinds_on_with_exit(self) -> None:
        """Leaving the mailbox context should explicitly unregister that binding."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        probe_message = _sdk_message("alice@linuxdo.space")

        with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
            matched = client.mail.route(probe_message)
            self.assertEqual(matched, (mailbox,))
            self.assertFalse(mailbox.closed)

        self.assertEqual(client.mail.route(probe_message), ())
        self.assertTrue(mailbox.closed)

    def test_explicit_unbind_removes_active_binding(self) -> None:
        """The facade should support explicit mailbox unbinding outside `with`."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        probe_message = _sdk_message("alice@linuxdo.space")
        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)

        self.assertEqual(client.mail.route(probe_message), (mailbox,))
        client.mail.unbind(mailbox)
        self.assertEqual(client.mail.route(probe_message), ())
        self.assertTrue(mailbox.closed)

    def test_bind_many_registers_one_ordered_group(self) -> None:
        """Batch registration should preserve caller order exactly."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        with client.mail.bind_many(
            client.mail.spec(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True),
            client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
        ) as bindings:
            self.assertEqual(len(bindings), 2)
            matched = client.mail.route(_sdk_message("alice@linuxdo.space"))
            self.assertEqual(matched, (bindings[0], bindings[1]))

        self.assertEqual(client.mail.route(_sdk_message("alice@linuxdo.space")), ())

    def test_bind_many_rolls_back_if_any_spec_is_invalid(self) -> None:
        """A failed batch bind should not leave earlier mailbox bindings behind."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        with self.assertRaisesRegex(ValueError, "pattern must not be empty"):
            client.mail.bind_many(
                client.mail.spec(prefix="alice", suffix=Suffix.linuxdo_space),
                client.mail.spec(pattern="", suffix=Suffix.linuxdo_space),
            )

        self.assertEqual(client.mail.route(_sdk_message("alice@linuxdo.space")), ())

    def test_client_close_closes_registered_mailboxes(self) -> None:
        """Closing the client should mark bound mailboxes closed and unroutable."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )

        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.assertEqual(client.mail.route(_sdk_message("alice@linuxdo.space")), (mailbox,))

        client.close()

        self.assertTrue(mailbox.closed)
        self.assertEqual(client.mail.route(_sdk_message("alice@linuxdo.space")), ())
        with self.assertRaisesRegex(LinuxDoSpaceError, "client is already closed"):
            client.mail.bind(prefix="bob", suffix=Suffix.linuxdo_space)

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
            with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
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

    def test_pure_creation_order_applies_to_exact_and_pattern_bindings(self) -> None:
        """Mailbox matching should follow binding creation order, not exact-value priority."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        pattern_subjects: list[str] = []
        exact_subjects: list[str] = []

        def _listen_pattern() -> None:
            with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    pattern_subjects.append(message.subject)

        def _listen_exact() -> None:
            with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    exact_subjects.append(message.subject)

        pattern_listener = threading.Thread(target=_listen_pattern)
        exact_listener = threading.Thread(target=_listen_exact)
        pattern_listener.start()
        time.sleep(0.05)
        exact_listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Ordered Match", "alice body"),
        )

        pattern_listener.join(timeout=2.0)
        exact_listener.join(timeout=2.0)

        self.assertEqual(pattern_subjects, ["Ordered Match"])
        self.assertEqual(exact_subjects, [])
        self.assertEqual(server.request_count, 1)

    def test_route_helper_matches_full_listener_message_in_same_order(self) -> None:
        """`client.mail.route(...)` should mirror the local ordered binding chain."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        pattern_mailbox = client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)
        exact_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.addCleanup(pattern_mailbox.close)
        self.addCleanup(exact_mailbox.close)

        collected: list[MailMessage] = []

        def _consume() -> None:
            for message in client.listen(timeout=0.4):
                collected.append(message)

        listener = threading.Thread(target=_consume)
        listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Route Mirror", "alice body"),
        )

        listener.join(timeout=2.0)

        self.assertEqual(len(collected), 1)
        self.assertEqual(client.mail.route(collected[0]), (pattern_mailbox, exact_mailbox))

    def test_route_uses_message_address_instead_of_all_recipients(self) -> None:
        """Routing should follow the current message instance address only."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        alice_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        bob_mailbox = client.mail.bind(prefix="bob", suffix=Suffix.linuxdo_space)
        self.addCleanup(alice_mailbox.close)
        self.addCleanup(bob_mailbox.close)

        multi_recipient_message = _sdk_message(
            "alice@linuxdo.space",
            recipients=("alice@linuxdo.space", "bob@linuxdo.space"),
        )

        self.assertEqual(client.mail.route(multi_recipient_message), (alice_mailbox,))

    def test_allow_overlap_continues_to_later_bindings(self) -> None:
        """A matching binding with allow_overlap should let later bindings receive the message too."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        first_subjects: list[str] = []
        second_subjects: list[str] = []
        third_subjects: list[str] = []

        def _listen_first() -> None:
            with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    first_subjects.append(message.subject)

        def _listen_second() -> None:
            with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    second_subjects.append(message.subject)

        def _listen_third() -> None:
            with client.mail.bind(pattern=r"a.*", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    third_subjects.append(message.subject)

        first_listener = threading.Thread(target=_listen_first)
        second_listener = threading.Thread(target=_listen_second)
        third_listener = threading.Thread(target=_listen_third)
        first_listener.start()
        time.sleep(0.05)
        second_listener.start()
        time.sleep(0.05)
        third_listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Overlap Match", "alice body"),
        )

        first_listener.join(timeout=2.0)
        second_listener.join(timeout=2.0)
        third_listener.join(timeout=2.0)

        self.assertEqual(first_subjects, ["Overlap Match"])
        self.assertEqual(second_subjects, ["Overlap Match"])
        self.assertEqual(third_subjects, [])

    def test_multiple_overlap_bindings_all_receive(self) -> None:
        """When each matching binding allows overlap, they should all receive the same message."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        first_subjects: list[str] = []
        second_subjects: list[str] = []
        third_subjects: list[str] = []

        def _listen_first() -> None:
            with client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    first_subjects.append(message.subject)

        def _listen_second() -> None:
            with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    second_subjects.append(message.subject)

        def _listen_third() -> None:
            with client.mail.bind(pattern=re.compile(r".*e"), suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    third_subjects.append(message.subject)

        first_listener = threading.Thread(target=_listen_first)
        second_listener = threading.Thread(target=_listen_second)
        third_listener = threading.Thread(target=_listen_third)
        first_listener.start()
        time.sleep(0.05)
        second_listener.start()
        time.sleep(0.05)
        third_listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "All Receive", "alice body"),
        )

        first_listener.join(timeout=2.0)
        second_listener.join(timeout=2.0)
        third_listener.join(timeout=2.0)

        self.assertEqual(first_subjects, ["All Receive"])
        self.assertEqual(second_subjects, ["All Receive"])
        self.assertEqual(third_subjects, ["All Receive"])

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
            with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space) as mailbox:
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

    def test_mailbox_does_not_backfill_messages_sent_before_listen_starts(self) -> None:
        """Binding registration alone should not create a growing pre-listen backlog."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.addCleanup(mailbox.close)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Before Listen", "early body"),
        )

        late_subjects: list[str] = []

        def _consume() -> None:
            for message in mailbox.listen(timeout=0.4):
                late_subjects.append(message.subject)

        listener = threading.Thread(target=_consume)
        listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "After Listen", "late body"),
        )

        listener.join(timeout=2.0)

        self.assertEqual(late_subjects, ["After Listen"])

    def test_one_mailbox_rejects_multiple_concurrent_listeners(self) -> None:
        """A single mailbox instance should not split one queue across multiple listeners."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.addCleanup(mailbox.close)

        def _first_listener() -> None:
            for _ in mailbox.listen(timeout=1.0):
                break

        worker = threading.Thread(target=_first_listener)
        worker.start()
        deadline = time.monotonic() + 1.0
        while time.monotonic() < deadline and not mailbox._is_listening:
            time.sleep(0.01)

        self.assertTrue(mailbox._is_listening)

        with self.assertRaisesRegex(LinuxDoSpaceError, "mailbox already has an active listener"):
            next(mailbox.listen(timeout=0.1))

        mailbox.close()
        worker.join(timeout=2.0)

    def test_mail_call_remains_sugar_over_explicit_bind(self) -> None:
        """`client.mail(...)` should behave exactly like the explicit `bind(...)` form."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        explicit_subjects: list[str] = []
        sugar_subjects: list[str] = []

        def _listen_explicit() -> None:
            with client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    explicit_subjects.append(message.subject)

        def _listen_sugar() -> None:
            with client.mail(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True) as mailbox:
                for message in mailbox.listen(timeout=0.4):
                    sugar_subjects.append(message.subject)

        explicit_listener = threading.Thread(target=_listen_explicit)
        sugar_listener = threading.Thread(target=_listen_sugar)
        explicit_listener.start()
        time.sleep(0.05)
        sugar_listener.start()
        time.sleep(0.05)

        server.publish_mail(
            "alice@linuxdo.space",
            _raw_message("alice@linuxdo.space", "Sugar Match", "alice body"),
        )

        explicit_listener.join(timeout=2.0)
        sugar_listener.join(timeout=2.0)

        self.assertEqual(explicit_subjects, ["Sugar Match"])
        self.assertEqual(sugar_subjects, ["Sugar Match"])


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


def _sdk_message(address: str, *, recipients: tuple[str, ...] | None = None) -> MailMessage:
    """Build one minimal public SDK message object for routing-only tests."""

    message = EmailMessage()
    message["From"] = "sender@example.com"
    message["To"] = address
    message["Subject"] = "Probe"
    resolved_recipients = recipients or (address,)
    return MailMessage(
        address=address,
        sender="sender@example.com",
        recipients=resolved_recipients,
        received_at=datetime(2026, 3, 20, 10, 11, 12, tzinfo=timezone.utc),
        subject="Probe",
        message_id=None,
        date=None,
        from_header="sender@example.com",
        to_header=address,
        cc_header="",
        reply_to_header="",
        from_addresses=("sender@example.com",),
        to_addresses=resolved_recipients,
        cc_addresses=(),
        reply_to_addresses=(),
        text="probe",
        html="",
        headers={},
        raw="probe",
        raw_bytes=b"probe",
        message=message,
    )


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
