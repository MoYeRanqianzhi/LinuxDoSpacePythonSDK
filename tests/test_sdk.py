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
from typing import Callable

from LinuxDoSpace import Client, LinuxDoSpaceError, Suffix
from LinuxDoSpace.models import MailMessage

TEST_OWNER_USERNAME = "testuser"
TEST_NAMESPACE_SUFFIX = f"{TEST_OWNER_USERNAME}.linuxdo.space"


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

    def test_literal_string_suffix_remains_literal(self) -> None:
        """Plain string suffixes should stay literal mailbox domains."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        mailbox = client.mail.bind(prefix="alice", suffix="linuxdo.space")
        self.assertEqual(mailbox.address, "alice@linuxdo.space")

    def test_semantic_suffix_uses_mail_namespace_by_default(self) -> None:
        """The semantic suffix should now expose the owner's canonical mail namespace."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.assertEqual(mailbox.address, "alice@testuser-mail.linuxdo.space")

    def test_semantic_suffix_with_dynamic_fragment_registers_remote_filter(self) -> None:
        """Binding one dynamic semantic mail suffix should sync it to the backend."""

        server, thread = _start_stream_server()
        self.addCleanup(_cleanup_stream_server, server, thread)

        client = Client(
            token="lds_pat.tok123.supersecret",
            base_url=f"http://127.0.0.1:{server.server_port}",
            stream_socket_timeout=0.2,
        )
        self.addCleanup(client.close)

        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space.with_suffix("foo"))
        self.assertEqual(mailbox.address, "alice@testuser-mailfoo.linuxdo.space")
        self.assertTrue(
            server.wait_for_filter_suffixes(("foo",), timeout=2.0),
            f"expected synced suffix fragments ('foo',), got {server.latest_filter_suffixes}",
        )

        collected: list[str] = []

        def _consume() -> None:
            for item in mailbox.listen(timeout=0.4):
                collected.append(item.address)

        listener = threading.Thread(target=_consume)
        listener.start()
        _wait_for(lambda: mailbox._is_listening, timeout=2.0, description="dynamic suffix mailbox listener")

        server.publish_mail(
            "alice@testuser-mailfoo.linuxdo.space",
            _raw_message("alice@testuser-mailfoo.linuxdo.space", "Dynamic Foo", "body"),
        )

        listener.join(timeout=2.0)
        self.assertEqual(collected, ["alice@testuser-mailfoo.linuxdo.space"])

        mailbox.close()
        self.assertTrue(
            server.wait_for_filter_suffixes((), timeout=2.0),
            f"expected suffix fragments to be cleared after close, got {server.latest_filter_suffixes}",
        )

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
        _wait_for(lambda: len(client._all_listeners) == 1, timeout=2.0, description="full listener registration")
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Alice Mail", "alice body"),
        )
        server.publish_mail(
            "bob@testuser.linuxdo.space",
            _raw_message("bob@testuser.linuxdo.space", "Bob Mail", "bob body"),
        )

        listener.join(timeout=2.0)

        self.assertEqual(
            collected,
            [
                ("alice@testuser.linuxdo.space", "Alice Mail"),
                ("bob@testuser.linuxdo.space", "Bob Mail"),
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
        mailboxes = {
            prefix: client.mail.bind(prefix=prefix, suffix=Suffix.linuxdo_space)
            for prefix in ("alice", "bob", "carol")
        }
        for mailbox in mailboxes.values():
            self.addCleanup(mailbox.close)

        def _listen(prefix: str) -> None:
            mailbox = mailboxes[prefix]
            for message in mailbox.listen(timeout=0.4):
                results.put((prefix, message.subject))
                break

        listeners = [threading.Thread(target=_listen, args=(prefix,)) for prefix in ("alice", "bob", "carol")]
        for listener in listeners:
            listener.start()
        _wait_for(
            lambda: all(mailbox._is_listening for mailbox in mailboxes.values()),
            timeout=2.0,
            description="parallel mailbox listeners",
        )
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Alice Mail", "alice body"),
        )
        server.publish_mail(
            "bob@testuser.linuxdo.space",
            _raw_message("bob@testuser.linuxdo.space", "Bob Mail", "bob body"),
        )
        server.publish_mail(
            "carol@testuser.linuxdo.space",
            _raw_message("carol@testuser.linuxdo.space", "Carol Mail", "carol body"),
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

        probe_message = _sdk_message("alice@testuser.linuxdo.space")

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

        probe_message = _sdk_message("alice@testuser.linuxdo.space")
        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)

        self.assertEqual(client.mail.route(probe_message), (mailbox,))
        client.mail.unbind(mailbox)
        self.assertEqual(client.mail.route(probe_message), ())
        self.assertTrue(mailbox.closed)

    def test_semantic_suffix_also_matches_mail_namespace(self) -> None:
        """The original semantic suffix should transparently match the current mail namespace."""

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

        matched = client.mail.route(_sdk_message("alice@testuser-mail.linuxdo.space"))
        self.assertEqual(matched, (mailbox,))

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
            matched = client.mail.route(_sdk_message("alice@testuser.linuxdo.space"))
            self.assertEqual(matched, (bindings[0], bindings[1]))

        self.assertEqual(client.mail.route(_sdk_message("alice@testuser.linuxdo.space")), ())

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

        self.assertEqual(client.mail.route(_sdk_message("alice@testuser.linuxdo.space")), ())

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
        self.assertEqual(client.mail.route(_sdk_message("alice@testuser.linuxdo.space")), (mailbox,))

        client.close()

        self.assertTrue(mailbox.closed)
        self.assertEqual(client.mail.route(_sdk_message("alice@testuser.linuxdo.space")), ())
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
        alice_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.addCleanup(alice_mailbox.close)

        def _listen_all() -> None:
            for message in client.listen(timeout=0.4):
                all_subjects.append(message.subject)

        def _listen_alice() -> None:
            for message in alice_mailbox.listen(timeout=0.4):
                alice_subjects.append(message.subject)

        all_listener = threading.Thread(target=_listen_all)
        alice_listener = threading.Thread(target=_listen_alice)
        all_listener.start()
        alice_listener.start()
        _wait_for(lambda: len(client._all_listeners) == 1, timeout=2.0, description="shared full listener registration")
        _wait_for(lambda: alice_mailbox._is_listening, timeout=2.0, description="alice mailbox listener")
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Alice Shared", "alice body"),
        )
        server.publish_mail(
            "bob@testuser.linuxdo.space",
            _raw_message("bob@testuser.linuxdo.space", "Bob Shared", "bob body"),
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
        pattern_mailbox = client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space)
        exact_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.addCleanup(pattern_mailbox.close)
        self.addCleanup(exact_mailbox.close)

        def _listen_pattern() -> None:
            for message in pattern_mailbox.listen(timeout=0.4):
                pattern_subjects.append(message.subject)

        def _listen_exact() -> None:
            for message in exact_mailbox.listen(timeout=0.4):
                exact_subjects.append(message.subject)

        pattern_listener = threading.Thread(target=_listen_pattern)
        exact_listener = threading.Thread(target=_listen_exact)
        pattern_listener.start()
        exact_listener.start()
        _wait_for(lambda: pattern_mailbox._is_listening, timeout=2.0, description="pattern mailbox listener")
        _wait_for(lambda: exact_mailbox._is_listening, timeout=2.0, description="exact mailbox listener")
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Ordered Match", "alice body"),
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
        _wait_for(lambda: len(client._all_listeners) == 1, timeout=2.0, description="route helper full listener registration")
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Route Mirror", "alice body"),
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
            "alice@testuser.linuxdo.space",
            recipients=("alice@testuser.linuxdo.space", "bob@testuser.linuxdo.space"),
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
        first_mailbox = client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)
        second_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        third_mailbox = client.mail.bind(pattern=r"a.*", suffix=Suffix.linuxdo_space, allow_overlap=True)
        self.addCleanup(first_mailbox.close)
        self.addCleanup(second_mailbox.close)
        self.addCleanup(third_mailbox.close)

        def _listen_first() -> None:
            for message in first_mailbox.listen(timeout=0.4):
                first_subjects.append(message.subject)

        def _listen_second() -> None:
            for message in second_mailbox.listen(timeout=0.4):
                second_subjects.append(message.subject)

        def _listen_third() -> None:
            for message in third_mailbox.listen(timeout=0.4):
                third_subjects.append(message.subject)

        first_listener = threading.Thread(target=_listen_first)
        second_listener = threading.Thread(target=_listen_second)
        third_listener = threading.Thread(target=_listen_third)
        first_listener.start()
        second_listener.start()
        third_listener.start()
        _wait_for(
            lambda: first_mailbox._is_listening and second_mailbox._is_listening and third_mailbox._is_listening,
            timeout=2.0,
            description="overlap listener startup",
        )
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Overlap Match", "alice body"),
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
        first_mailbox = client.mail.bind(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)
        second_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space, allow_overlap=True)
        third_mailbox = client.mail.bind(pattern=re.compile(r".*e"), suffix=Suffix.linuxdo_space, allow_overlap=True)
        self.addCleanup(first_mailbox.close)
        self.addCleanup(second_mailbox.close)
        self.addCleanup(third_mailbox.close)

        def _listen_first() -> None:
            for message in first_mailbox.listen(timeout=0.4):
                first_subjects.append(message.subject)

        def _listen_second() -> None:
            for message in second_mailbox.listen(timeout=0.4):
                second_subjects.append(message.subject)

        def _listen_third() -> None:
            for message in third_mailbox.listen(timeout=0.4):
                third_subjects.append(message.subject)

        first_listener = threading.Thread(target=_listen_first)
        second_listener = threading.Thread(target=_listen_second)
        third_listener = threading.Thread(target=_listen_third)
        first_listener.start()
        second_listener.start()
        third_listener.start()
        _wait_for(
            lambda: first_mailbox._is_listening and second_mailbox._is_listening and third_mailbox._is_listening,
            timeout=2.0,
            description="multi-overlap listener startup",
        )
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "All Receive", "alice body"),
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
        mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space)
        self.addCleanup(mailbox.close)

        def _consume() -> None:
            for message in mailbox.listen(timeout=0.5):
                received_subjects.append(message.subject)

        listener = threading.Thread(target=_consume)
        listener.start()
        _wait_for(lambda: mailbox._is_listening, timeout=2.0, description="burst mailbox listener")
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        for index in range(50):
            server.publish_mail(
                "alice@testuser.linuxdo.space",
                _raw_message("alice@testuser.linuxdo.space", f"Burst {index}", f"body {index}"),
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
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        all_subjects: list[str] = []

        def _consume_all() -> None:
            for message in client.listen(timeout=0.4):
                all_subjects.append(message.subject)

        full_listener = threading.Thread(target=_consume_all)
        full_listener.start()

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Before Listen", "early body"),
        )
        _wait_for(
            lambda: all_subjects == ["Before Listen"],
            timeout=2.0,
            description="pre-listen full stream consumption",
        )

        late_subjects: list[str] = []

        def _consume() -> None:
            for message in mailbox.listen(timeout=0.4):
                late_subjects.append(message.subject)

        listener = threading.Thread(target=_consume)
        listener.start()
        _wait_for(lambda: mailbox._is_listening, timeout=2.0, description="late mailbox listener")
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "After Listen", "late body"),
        )

        listener.join(timeout=2.0)
        full_listener.join(timeout=2.0)

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
        _wait_for(lambda: mailbox._is_listening, timeout=1.0, description="single mailbox first listener")
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
        explicit_mailbox = client.mail.bind(prefix="alice", suffix=Suffix.linuxdo_space, allow_overlap=True)
        sugar_mailbox = client.mail(pattern=r".*", suffix=Suffix.linuxdo_space, allow_overlap=True)
        self.addCleanup(explicit_mailbox.close)
        self.addCleanup(sugar_mailbox.close)

        def _listen_explicit() -> None:
            for message in explicit_mailbox.listen(timeout=0.4):
                explicit_subjects.append(message.subject)

        def _listen_sugar() -> None:
            for message in sugar_mailbox.listen(timeout=0.4):
                sugar_subjects.append(message.subject)

        explicit_listener = threading.Thread(target=_listen_explicit)
        sugar_listener = threading.Thread(target=_listen_sugar)
        explicit_listener.start()
        sugar_listener.start()
        _wait_for(
            lambda: explicit_mailbox._is_listening and sugar_mailbox._is_listening,
            timeout=2.0,
            description="explicit and sugar mailbox listeners",
        )
        self.assertTrue(server.wait_for_subscribers(1, timeout=2.0))

        server.publish_mail(
            "alice@testuser.linuxdo.space",
            _raw_message("alice@testuser.linuxdo.space", "Sugar Match", "alice body"),
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
            self.wfile.write(
                _event_line(
                    {
                        "type": "ready",
                        "token_public_id": "tok123",
                        "owner_username": TEST_OWNER_USERNAME,
                    }
                )
            )
            self.wfile.flush()
        except OSError:
            return

        self.server.register_subscriber(self.wfile)
        try:
            while not self.server.stop_event.is_set():
                time.sleep(0.05)
        finally:
            self.server.unregister_subscriber(self.wfile)

    def do_PUT(self) -> None:  # noqa: N802
        if self.path != "/v1/token/email/filters":
            self.send_response(404)
            self.end_headers()
            return

        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
        payload = json.loads(raw_body.decode("utf-8"))
        suffixes = payload.get("suffixes", [])
        self.server.record_filter_update(tuple(str(item) for item in suffixes))

        response = json.dumps(
            {
                "suffixes": list(suffixes),
                "domains": [
                    f"{TEST_OWNER_USERNAME}-mail{str(item)}.linuxdo.space"
                    for item in suffixes
                ],
            }
        ).encode("utf-8")

        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)
        self.wfile.flush()

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
        self._latest_filter_suffixes: tuple[str, ...] = ()
        self._filter_lock = threading.Lock()

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

    def wait_for_subscribers(self, expected_count: int, timeout: float) -> bool:
        """Wait until the expected number of stream subscribers is connected."""

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._subscribers_lock:
                if len(self._subscribers) >= expected_count:
                    return True
            time.sleep(0.01)
        return False

    def unregister_subscriber(self, writer: object) -> None:
        """Remove one HTTP response writer from the broadcast set."""

        with self._subscribers_lock:
            self._subscribers = [item for item in self._subscribers if item is not writer]

    @property
    def latest_filter_suffixes(self) -> tuple[str, ...]:
        """Return the latest suffix fragment set synced by the SDK."""

        with self._filter_lock:
            return self._latest_filter_suffixes

    def record_filter_update(self, suffixes: tuple[str, ...]) -> None:
        """Persist the latest filter-sync payload observed by the test server."""

        with self._filter_lock:
            self._latest_filter_suffixes = tuple(sorted(suffixes))

    def wait_for_filter_suffixes(self, expected_suffixes: tuple[str, ...], timeout: float) -> bool:
        """Wait until the SDK synced the expected suffix-fragment set."""

        expected = tuple(sorted(expected_suffixes))
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._filter_lock:
                if self._latest_filter_suffixes == expected:
                    return True
            time.sleep(0.01)
        return False

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


def _wait_for(predicate: Callable[[], bool], *, timeout: float, description: str) -> None:
    """Wait until one boolean predicate becomes true or fail the test clearly."""

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.01)
    raise AssertionError(f"timed out while waiting for {description}")


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
