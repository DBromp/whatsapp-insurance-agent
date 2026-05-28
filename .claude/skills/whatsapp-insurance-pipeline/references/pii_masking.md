# PII masking — dimension-preserving strategy

The brief requires PII to be masked with the same dimensions as the original. This is a hard constraint — masked values must have identical character count and structural shape so downstream code that pattern-matches on length/format doesn't break.

## Fields and patterns

| Field | Source pattern | Masked pattern | Strategy |
|---|---|---|---|
| sender_phone | `+5511988734012` | `+5500000000000` | Keep `+55`, replace digits with deterministic hash-derived digits |
| sender_name | "Ana Paula Ribeiro" | `LEAD_a1b2c3d4` (stable token) | SHA-256(phone)[:8] -> token; mapping table kept in `silver._pii_vault` (access-controlled) |
| CPF | `XXX.XXX.XXX-XX` (11 digits) | Same 11 digits, deterministically randomized | Format-preserving — keep dots and dash |
| CEP | `XXXXX-XXX` (8 digits) | Same shape, randomized digits | Keep hyphen |
| Email | `nome@dominio.com` | `<token>@<dominio>` (domain preserved for analytics) | Provider domain stays for parity — but we're skipping the email-provider Gold cut per the brief |
| License plate | `ABC1D23` (Mercosul) or `ABC-1234` (old) | Same shape, deterministically randomized | Detect format first |

## Where masking happens

- In-place on `message_body`: regex detection -> masked replacement
- On structured columns: `sender_phone` and `sender_name` masked into new columns `lead_phone_masked` and `lead_name_token`; originals dropped before persisting to Silver

## Determinism

Same plaintext input must produce the same masked output across runs — uses a project-wide salt stored as a Databricks secret. This lets us join the same lead across rows after masking.

## Reverse mapping

A `silver._pii_vault` Delta table stores `(token, original_hash)` for internal forensics if needed. Never exposed in Gold. Strict access controls.

## Brazilian-specific notes

- CPF can come with or without formatting: `12345678900`, `123.456.789-00`, `123 456 789 00`. Normalize first, then mask.
- Mercosul plates (since 2018): `AAA0A00`. Old plates: `AAA-0000` or `AAA0000`. Both must be detected.
- CEP can be `00000-000` or `00000000`. Normalize then mask preserving the user's chosen format.
- Phone may have or omit `+55`, may have area code in parens `(11)`. Normalize to E.164 first.
