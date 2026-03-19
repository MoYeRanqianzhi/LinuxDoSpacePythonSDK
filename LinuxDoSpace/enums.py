"""Public SDK enums.

The SDK intentionally models the mailbox suffix as a dedicated enum instead of
an untyped string so users get better IDE completion and fewer typo-driven bugs.
"""

from __future__ import annotations

from enum import Enum


class Suffix(str, Enum):
    """Known LinuxDoSpace mailbox suffixes."""

    linuxdo_space = "linuxdo.space"

    def __str__(self) -> str:
        """Return the actual mailbox suffix string."""

        return self.value
