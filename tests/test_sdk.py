"""Unit tests for the LinuxDoSpace Python SDK."""

from __future__ import annotations

import base64
import json
import unittest

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


def _event_line(payload: dict[str, object]) -> bytes:
    """Encode one fake NDJSON stream event line."""

    return json.dumps(payload).encode("utf-8") + b"\n"


if __name__ == "__main__":
    unittest.main()
