"""Typed models returned by the Python SDK.

The goal of these dataclasses is straightforward: expose as many useful mail
attributes as named fields as possible so IDEs can provide strong completion
without forcing callers to manually decode MIME messages first.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from email.message import EmailMessage
from typing import Mapping


@dataclass(slots=True)
class MailMessage:
    """One parsed mail event received from the LinuxDoSpace HTTPS stream."""

    address: str
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
    headers: Mapping[str, str]

    raw: str
    raw_bytes: bytes
    message: EmailMessage
