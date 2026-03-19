"""Public entrypoints for the LinuxDoSpace Python SDK."""

from .client import Client, MailBox
from .enums import Suffix
from .exceptions import AuthenticationError, LinuxDoSpaceError, StreamError
from .models import MailMessage

__all__ = [
    "AuthenticationError",
    "Client",
    "LinuxDoSpaceError",
    "MailBox",
    "MailMessage",
    "StreamError",
    "Suffix",
]
