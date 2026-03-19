"""Public entrypoints for the LinuxDoSpace Python SDK."""

from .client import Client, MailBindingGroup, MailBindingSpec, MailBox
from .enums import Suffix
from .exceptions import AuthenticationError, LinuxDoSpaceError, StreamError
from .models import MailMessage

__all__ = [
    "AuthenticationError",
    "Client",
    "LinuxDoSpaceError",
    "MailBindingGroup",
    "MailBindingSpec",
    "MailBox",
    "MailMessage",
    "StreamError",
    "Suffix",
]
