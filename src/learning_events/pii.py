"""Pseudonymisation of personal data before anything is stored.

GDPR treats an email address as personal data. Learning analytics only needs a stable key to
join a learner's events, not the address itself, so emails are replaced by a keyed hash
(HMAC-SHA256 with a secret salt from the environment). Without the salt the hash cannot be
reversed by hashing a list of known emails, which a plain unsalted SHA-256 would allow.
Pseudonymised data is still personal data under GDPR, so deletion and retention still apply.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import re
from typing import Any

SALT_ENV = "LEVENTS_PII_SALT"
EMAIL = re.compile(r"[^@\s\"']+@[^@\s\"']+\.[^@\s\"']+")


class MissingSaltError(RuntimeError):
    """Raised instead of silently hashing with a default (guessable) salt."""


def salt_from_env() -> str:
    salt = os.environ.get(SALT_ENV, "")
    if len(salt) < 8:
        raise MissingSaltError(
            f"set {SALT_ENV} to a secret of at least 8 characters, "
            f"e.g. export {SALT_ENV}=$(openssl rand -hex 16)"
        )
    return salt


def keyed_hash(value: str, salt: str) -> str:
    """HMAC-SHA256 of a normalised value; the same email always gives the same key."""
    normalised = value.strip().lower().encode()
    return hmac.new(salt.encode(), normalised, hashlib.sha256).hexdigest()


def scrub(value: Any, salt: str) -> Any:
    """Return a copy with every email-looking string replaced by its hash.

    Applied to the whole payload, not only `actor.email`, because malformed events can put
    an email anywhere and dead-lettered payloads are stored too.
    """
    if isinstance(value, dict):
        return {key: scrub(item, salt) for key, item in value.items()}
    if isinstance(value, list):
        return [scrub(item, salt) for item in value]
    if isinstance(value, str) and EMAIL.search(value):
        return EMAIL.sub(lambda m: "hmac:" + keyed_hash(m.group(0), salt), value)
    return value


def pseudonymise_event(event: dict[str, Any], salt: str) -> tuple[dict[str, Any], str | None]:
    """Split an accepted event into (payload without the email, email hash or None)."""
    clean = scrub(event, salt)
    actor = dict(event.get("actor", {}))
    email = actor.pop("email", None)
    clean["actor"] = scrub(actor, salt)
    return clean, keyed_hash(email, salt) if isinstance(email, str) else None
