"""PII masking — Brazilian patterns, deterministic, dimension-preserving.

Each public ``mask_*`` function takes a raw value and a salt and returns a masked
value that preserves the input's structural shape (length, punctuation, format
markers). Same input + same salt → same output (deterministic) so masked values
can be joined across runs. See ``references/pii_masking.md`` for the full spec.

No external dependencies — pure stdlib (``re`` + ``hashlib``).
"""

from __future__ import annotations

import hashlib
import re

# ---------------------------------------------------------------------------
# Regex patterns (compiled once, reused)
# ---------------------------------------------------------------------------

# Brazilian mobile phone. Examples that match:
#   +5511988734012, 5511988734012, 11988734012, (11) 98873-4012, +55 11 98873 4012
# The "9" prefix on the local number is what distinguishes mobile from older
# landlines and from random 11-digit numbers.
PHONE_RE = re.compile(
    r"(?:\+?55\s?)?\(?\d{2}\)?\s?9\d{4}[-\s]?\d{4}"
)

# CPF — 11 digits, optionally formatted as XXX.XXX.XXX-XX.
# Word-boundary on the unformatted variant to avoid eating into longer digit runs.
CPF_RE = re.compile(r"\d{3}\.\d{3}\.\d{3}-\d{2}|\b\d{11}\b")

# CEP — 8 digits, optionally formatted as XXXXX-XXX.
CEP_RE = re.compile(r"\d{5}-\d{3}|\b\d{8}\b")

# Email.
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

# License plates.
#   Mercosul (post-2018): AAA0A00  (3 letters, digit, letter, 2 digits)
#   Old:                  AAA-0000 or AAA0000  (3 letters, optional dash, 4 digits)
PLATE_MERCOSUL_RE = re.compile(r"\b[A-Z]{3}\d[A-Z]\d{2}\b", re.IGNORECASE)
PLATE_OLD_RE = re.compile(r"\b[A-Z]{3}-?\d{4}\b", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _deterministic_digits(seed: str, salt: str, n: int) -> str:
    """Generate ``n`` decimal digits deterministically from ``seed + salt``.

    Algorithm: SHA-256 the seed+salt, walk the hex digest in 2-character chunks
    (each chunk = one byte ∈ [0, 255]), take ``% 10`` for a uniform-ish digit.
    If we run out of digest bytes (n > 32), re-hash with a counter for more.
    Stable across Python versions; no third-party deps.
    """
    if n <= 0:
        return ""
    digest = hashlib.sha256((seed + salt).encode()).hexdigest()
    out: list[str] = []
    i = 0
    while len(out) < n:
        if i + 2 > len(digest):
            # Extend the keystream by re-hashing with a counter.
            digest = hashlib.sha256((digest + salt + str(len(out))).encode()).hexdigest()
            i = 0
        byte = int(digest[i : i + 2], 16)
        out.append(str(byte % 10))
        i += 2
    return "".join(out)


def _deterministic_letters(seed: str, salt: str, n: int) -> str:
    """Same idea as ``_deterministic_digits`` but produces uppercase A–Z letters."""
    if n <= 0:
        return ""
    digest = hashlib.sha256((seed + salt + "LETTERS").encode()).hexdigest()
    out: list[str] = []
    i = 0
    while len(out) < n:
        if i + 2 > len(digest):
            digest = hashlib.sha256((digest + salt + str(len(out))).encode()).hexdigest()
            i = 0
        byte = int(digest[i : i + 2], 16)
        out.append(chr(ord("A") + (byte % 26)))
        i += 2
    return "".join(out)


def _normalize_phone(phone: str) -> str:
    """Canonical form of a Brazilian phone for stable hashing.

    Strips formatting characters (parens, spaces, dashes) and forces a leading
    ``+55`` country code. Output is digits-only after the ``+``.
    """
    digits = re.sub(r"\D", "", phone)
    # If user passed an 11-digit local number, prefix +55.
    if len(digits) == 11:
        digits = "55" + digits
    return "+" + digits


# ---------------------------------------------------------------------------
# Public maskers
# ---------------------------------------------------------------------------


def mask_phone(phone: str, salt: str) -> str:
    """Mask a Brazilian phone number, preserving the input's exact shape.

    The literal ``+55`` country code (if present) is kept; every other digit is
    replaced by a deterministic digit derived from the normalized form. Non-digit
    characters (parens, spaces, dashes) stay in their original positions so the
    output passes ``len(masked) == len(phone)``.

    Args:
        phone: Raw phone string in any common Brazilian format.
        salt: Project-wide salt (stored as a Databricks secret in prod).

    Returns:
        Masked phone with the same length and structure as ``phone``.
    """
    canonical = _normalize_phone(phone)
    has_cc = phone.startswith("+55")
    n_replace = sum(1 for c in phone if c.isdigit()) - (2 if has_cc else 0)
    replacement = _deterministic_digits(canonical, salt, max(n_replace, 0))

    out: list[str] = []
    cc_digits_left = 2 if has_cc else 0
    repl_i = 0
    for c in phone:
        if c.isdigit() and cc_digits_left > 0:
            out.append(c)
            cc_digits_left -= 1
        elif c.isdigit():
            out.append(replacement[repl_i])
            repl_i += 1
        else:
            out.append(c)
    return "".join(out)


def name_token_from_phone(phone: str, salt: str) -> str:
    """Stable opaque token for a lead, derived from their phone (NOT their name).

    Returns a fixed-shape ``LEAD_xxxxxxxx`` (13-char) token. The canonical PII
    rule keys the lead's display token off the phone because names are messy
    (typos, capitalization, accents, partial). The phone is the stable identity.

    Args:
        phone: Raw phone string. Normalized internally for stable hashing.
        salt: Project-wide salt.

    Returns:
        13-char token of the shape ``LEAD_xxxxxxxx``.
    """
    canonical = _normalize_phone(phone)
    digest = hashlib.sha256((canonical + salt).encode()).hexdigest()
    return f"LEAD_{digest[:8]}"


def mask_cpf(cpf: str, salt: str) -> str:
    """Mask a Brazilian CPF, preserving its exact shape.

    Accepts formatted (``123.456.789-00``) or unformatted (``12345678900``)
    input. Returns the same shape with 11 digits replaced deterministically.
    Returns the input unchanged if it isn't a valid 11-digit CPF.
    """
    canonical = re.sub(r"\D", "", cpf)
    if len(canonical) != 11:
        return cpf
    replacement = _deterministic_digits(canonical, salt + "CPF", 11)
    return _replace_digits_inplace(cpf, replacement)


def mask_cep(cep: str, salt: str) -> str:
    """Mask a Brazilian CEP (postal code), preserving its exact shape.

    Accepts formatted (``12345-678``) or unformatted (``12345678``) input.
    Returns the input unchanged if it isn't a valid 8-digit CEP.
    """
    canonical = re.sub(r"\D", "", cep)
    if len(canonical) != 8:
        return cep
    replacement = _deterministic_digits(canonical, salt + "CEP", 8)
    return _replace_digits_inplace(cep, replacement)


def mask_email(email: str, salt: str) -> str:
    """Mask the local part of an email, preserving the domain and total length.

    ``nome@dominio.com`` → ``<hash-token>@dominio.com`` where the token is the
    same length as the original local part (so ``len(masked) == len(email)``).
    Domain is preserved because Gold-layer analytics may want to bucket by
    provider domain. Returns the input unchanged if no ``@`` is present.
    """
    if "@" not in email:
        return email
    local, _, domain = email.partition("@")
    digest = hashlib.sha256((email + salt + "EMAIL").encode()).hexdigest()
    # Match the local-part length. SHA-256 hex is 64 chars; tile if local is longer.
    if len(local) <= len(digest):
        token = digest[: len(local)]
    else:
        repeats = (len(local) // len(digest)) + 1
        token = (digest * repeats)[: len(local)]
    return f"{token}@{domain}"


def mask_license_plate(plate: str, salt: str) -> str:
    """Mask a Brazilian license plate (Mercosul or old format), preserving shape.

    Detects Mercosul (``AAA0A00``) vs old (``AAA-0000`` / ``AAA0000``) by regex,
    then replaces letters with deterministic letters and digits with deterministic
    digits. Case is preserved per character. Hyphens stay in place. Returns the
    input unchanged if it doesn't look like a known plate format.
    """
    canonical = re.sub(r"\W", "", plate.upper())
    if not (PLATE_MERCOSUL_RE.fullmatch(canonical) or PLATE_OLD_RE.fullmatch(canonical)):
        return plate
    n_letters = sum(1 for c in plate if c.isalpha())
    n_digits = sum(1 for c in plate if c.isdigit())
    letters = _deterministic_letters(canonical, salt + "PLATE", n_letters)
    digits = _deterministic_digits(canonical, salt + "PLATE", n_digits)

    out: list[str] = []
    li, di = 0, 0
    for c in plate:
        if c.isalpha():
            replacement = letters[li]
            li += 1
            out.append(replacement if c.isupper() else replacement.lower())
        elif c.isdigit():
            out.append(digits[di])
            di += 1
        else:
            out.append(c)
    return "".join(out)


def mask_message_body(body: str, salt: str) -> str:
    """Sweep a free-text message body and mask every PII pattern in place.

    Order matters: phones first (specific 9-prefixed mobile pattern), then CPF
    and CEP (which would otherwise collide with unformatted phones on length),
    then email, then plates. Each substitution preserves the matched span's
    length so the masked body retains the original message dimensions.
    """
    body = PHONE_RE.sub(lambda m: mask_phone(m.group(0), salt), body)
    body = CPF_RE.sub(lambda m: mask_cpf(m.group(0), salt), body)
    body = CEP_RE.sub(lambda m: mask_cep(m.group(0), salt), body)
    body = EMAIL_RE.sub(lambda m: mask_email(m.group(0), salt), body)
    body = PLATE_MERCOSUL_RE.sub(lambda m: mask_license_plate(m.group(0), salt), body)
    body = PLATE_OLD_RE.sub(lambda m: mask_license_plate(m.group(0), salt), body)
    return body


# ---------------------------------------------------------------------------
# Tiny shared helper for the format-preserving digit-only maskers (CPF, CEP)
# ---------------------------------------------------------------------------


def _replace_digits_inplace(source: str, replacement_digits: str) -> str:
    """Walk ``source`` and substitute every digit with the next char from
    ``replacement_digits``. Non-digit characters are kept in place.
    """
    out: list[str] = []
    i = 0
    for c in source:
        if c.isdigit():
            out.append(replacement_digits[i])
            i += 1
        else:
            out.append(c)
    return "".join(out)
