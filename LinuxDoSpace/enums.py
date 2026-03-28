"""Public SDK enums.

The SDK intentionally models the mailbox suffix as a dedicated enum instead of
an untyped string so users get better IDE completion and fewer typo-driven bugs.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(slots=True, frozen=True)
class SemanticSuffix:
    """One semantic LinuxDoSpace mailbox suffix with an optional mail variant.

    The plain semantic root remains `linuxdo.space`, but mail clients can ask
    the SDK to derive the current owner's dedicated mail namespace by attaching
    one dynamic suffix fragment. For example:

    - `Suffix.linuxdo_space` -> `<owner>-mail.linuxdo.space`
    - `Suffix.linuxdo_space.with_suffix("foo")` -> `<owner>-mailfoo.linuxdo.space`
    """

    base: "Suffix"
    mail_suffix_fragment: str = ""

    def with_suffix(self, fragment: str) -> "SemanticSuffix":
        """Return the same semantic base with one normalized mail suffix fragment."""

        return SemanticSuffix(base=self.base, mail_suffix_fragment=_normalize_mail_suffix_fragment(fragment))

    def __str__(self) -> str:
        """Render the public semantic base string, not the owner-specific domain."""

        return self.base.value


class Suffix(str, Enum):
    """Known LinuxDoSpace mailbox suffixes."""

    linuxdo_space = "linuxdo.space"

    def with_suffix(self, fragment: str) -> SemanticSuffix:
        """Attach one dynamic mail suffix fragment to the semantic base."""

        return SemanticSuffix(base=self, mail_suffix_fragment=_normalize_mail_suffix_fragment(fragment))

    def __str__(self) -> str:
        """Return the actual mailbox suffix string."""

        return self.value


def _normalize_mail_suffix_fragment(raw: str) -> str:
    """Normalize one optional dynamic mail suffix fragment into DNS-safe text."""

    value = str(raw).strip().lower()
    if value == "":
        return ""

    normalized_parts: list[str] = []
    last_was_dash = False
    for character in value:
        if "a" <= character <= "z" or "0" <= character <= "9":
            normalized_parts.append(character)
            last_was_dash = False
            continue
        if not last_was_dash:
            normalized_parts.append("-")
            last_was_dash = True

    normalized = "".join(normalized_parts).strip("-")
    if normalized == "":
        raise ValueError("mail suffix fragment does not contain any valid dns characters")
    if "." in normalized:
        raise ValueError("mail suffix fragment must stay inside one dns label")
    if len(normalized) > 48:
        raise ValueError("mail suffix fragment must be 48 characters or fewer")
    return normalized
