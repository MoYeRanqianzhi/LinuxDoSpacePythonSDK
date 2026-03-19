"""LinuxDoSpace SDK exception hierarchy.

This module keeps the public exception types small and explicit so callers can
catch one broad SDK error or branch on narrower authentication/stream failures.
"""

from __future__ import annotations


class LinuxDoSpaceError(Exception):
    """Base exception for every SDK-level failure."""


class AuthenticationError(LinuxDoSpaceError):
    """Raised when the backend rejects the provided API token."""


class StreamError(LinuxDoSpaceError):
    """Raised when the HTTPS mail stream cannot be established or parsed."""
