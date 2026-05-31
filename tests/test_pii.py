"""Tests for src/pii.py — Brazilian PII masking.

Covers determinism, dimension preservation, no-passthrough, malformed inputs,
and edge cases for every masker. Plus integration tests for mask_message_body.

Mirrors tests/test_schema.py style — plain assert, no TestCase, no fixtures.
"""

from __future__ import annotations

import pytest

from src.pii import (
    mask_cep,
    mask_cpf,
    mask_email,
    mask_license_plate,
    mask_message_body,
    mask_phone,
    name_token_from_phone,
)

# Production salt is a 32-byte hex string (64 chars) from Databricks secrets;
# tests mimic that shape so we exercise the same key-length characteristics.
SALT = "test-salt-" + "x" * 54


# ---------------------------------------------------------------------------
# mask_phone
# ---------------------------------------------------------------------------


def test_mask_phone_is_deterministic():
    assert mask_phone("+5511988734012", SALT) == mask_phone("+5511988734012", SALT)


def test_mask_phone_preserves_length_and_structure():
    masked = mask_phone("+5511988734012", SALT)
    assert len(masked) == len("+5511988734012")
    assert masked.startswith("+55")              # country code preserved
    assert masked[3:].isdigit()                  # rest is digits


def test_mask_phone_does_not_passthrough():
    assert mask_phone("+5511988734012", SALT) != "+5511988734012"


def test_mask_phone_handles_formatted_input():
    formatted = "(11) 98873-4012"
    masked = mask_phone(formatted, SALT)
    assert len(masked) == len(formatted)
    # Structural markers stay in the same positions:
    assert masked[0] == "("
    assert masked[3] == ")"
    assert masked[4] == " "
    assert masked[10] == "-"


def test_mask_phone_handles_input_without_country_code():
    # Should still mask all 11 digits (no +55 to preserve literally).
    masked = mask_phone("11988734012", SALT)
    assert len(masked) == 11
    assert masked.isdigit()
    assert masked != "11988734012"


# ---------------------------------------------------------------------------
# name_token_from_phone
# ---------------------------------------------------------------------------


def test_name_token_is_deterministic():
    assert name_token_from_phone("+5511988734012", SALT) == name_token_from_phone(
        "+5511988734012", SALT
    )


def test_name_token_has_expected_shape():
    token = name_token_from_phone("+5511988734012", SALT)
    assert token.startswith("LEAD_")
    assert len(token) == 13                       # "LEAD_" (5) + 8-char hex
    assert all(c in "0123456789abcdef" for c in token[5:])


def test_name_token_is_phone_keyed_not_name_keyed():
    # Same phone in different formats normalizes to the same token.
    a = name_token_from_phone("+5511988734012", SALT)
    b = name_token_from_phone("(11) 98873-4012", SALT)
    assert a == b


def test_name_token_differs_across_phones():
    a = name_token_from_phone("+5511988734012", SALT)
    b = name_token_from_phone("+5521987654321", SALT)
    assert a != b


# ---------------------------------------------------------------------------
# mask_cpf
# ---------------------------------------------------------------------------


def test_mask_cpf_is_deterministic():
    assert mask_cpf("123.456.789-00", SALT) == mask_cpf("123.456.789-00", SALT)


def test_mask_cpf_preserves_formatting():
    masked = mask_cpf("123.456.789-00", SALT)
    assert len(masked) == len("123.456.789-00")
    assert masked[3] == "."
    assert masked[7] == "."
    assert masked[11] == "-"


def test_mask_cpf_handles_unformatted():
    masked = mask_cpf("12345678900", SALT)
    assert len(masked) == 11
    assert masked.isdigit()
    assert masked != "12345678900"


def test_mask_cpf_returns_input_unchanged_when_malformed():
    # Not 11 digits — masker leaves it alone.
    assert mask_cpf("not-a-cpf", SALT) == "not-a-cpf"
    assert mask_cpf("1234", SALT) == "1234"


def test_mask_cpf_does_not_passthrough_valid_input():
    assert mask_cpf("123.456.789-00", SALT) != "123.456.789-00"


# ---------------------------------------------------------------------------
# mask_cep
# ---------------------------------------------------------------------------


def test_mask_cep_is_deterministic():
    assert mask_cep("01310-100", SALT) == mask_cep("01310-100", SALT)


def test_mask_cep_preserves_hyphen():
    masked = mask_cep("01310-100", SALT)
    assert len(masked) == len("01310-100")
    assert masked[5] == "-"


def test_mask_cep_handles_unformatted():
    masked = mask_cep("01310100", SALT)
    assert len(masked) == 8
    assert masked.isdigit()
    assert masked != "01310100"


def test_mask_cep_returns_input_unchanged_when_malformed():
    assert mask_cep("123", SALT) == "123"


# ---------------------------------------------------------------------------
# mask_email
# ---------------------------------------------------------------------------


def test_mask_email_is_deterministic():
    assert mask_email("ana@gmail.com", SALT) == mask_email("ana@gmail.com", SALT)


def test_mask_email_preserves_domain_and_length():
    masked = mask_email("ana.paula@gmail.com", SALT)
    assert masked.endswith("@gmail.com")
    assert len(masked) == len("ana.paula@gmail.com")


def test_mask_email_replaces_local_part():
    masked = mask_email("ana.paula@gmail.com", SALT)
    assert not masked.startswith("ana.paula")


def test_mask_email_returns_input_unchanged_when_no_at_sign():
    assert mask_email("not-an-email", SALT) == "not-an-email"


# ---------------------------------------------------------------------------
# mask_license_plate
# ---------------------------------------------------------------------------


def test_mask_plate_mercosul_is_deterministic():
    assert mask_license_plate("ABC1D23", SALT) == mask_license_plate("ABC1D23", SALT)


def test_mask_plate_mercosul_preserves_shape():
    masked = mask_license_plate("ABC1D23", SALT)
    assert len(masked) == 7
    # Position-by-position type check: A A A D A D D (letter/letter/letter/digit/letter/digit/digit)
    assert masked[0].isalpha()
    assert masked[1].isalpha()
    assert masked[2].isalpha()
    assert masked[3].isdigit()
    assert masked[4].isalpha()
    assert masked[5].isdigit()
    assert masked[6].isdigit()


def test_mask_plate_old_format_with_hyphen():
    masked = mask_license_plate("ABC-1234", SALT)
    assert len(masked) == 8
    assert masked[3] == "-"
    assert masked[:3].isalpha()
    assert masked[4:].isdigit()


def test_mask_plate_returns_input_unchanged_when_not_a_plate():
    assert mask_license_plate("XYZ", SALT) == "XYZ"


def test_mask_plate_does_not_passthrough_valid_plate():
    assert mask_license_plate("ABC1D23", SALT) != "ABC1D23"


# ---------------------------------------------------------------------------
# mask_message_body — integration
# ---------------------------------------------------------------------------


def test_mask_message_body_handles_empty_string():
    assert mask_message_body("", SALT) == ""


def test_mask_message_body_masks_phone_in_natural_text():
    msg = "me liga no +5511988734012 por favor"
    masked = mask_message_body(msg, SALT)
    assert "+5511988734012" not in masked
    # Phone stand-in still has +55 and the same total length:
    assert len(masked) == len(msg)


def test_mask_message_body_masks_multiple_pii_types():
    msg = "CPF 123.456.789-00, CEP 01310-100, email ana@gmail.com, placa ABC1D23"
    masked = mask_message_body(msg, SALT)
    # Each piece of PII is gone; structural words remain.
    assert "123.456.789-00" not in masked
    assert "01310-100" not in masked
    assert "ana@gmail.com" not in masked
    assert "ABC1D23" not in masked
    # Surrounding text intact:
    assert "CPF " in masked
    assert ", CEP " in masked
    assert "placa " in masked


def test_mask_message_body_is_deterministic():
    msg = "meu CPF é 123.456.789-00 e meu email é teste@dominio.com"
    assert mask_message_body(msg, SALT) == mask_message_body(msg, SALT)


def test_mask_message_body_leaves_non_pii_text_alone():
    msg = "olá tudo bem? quero fazer uma cotação"
    assert mask_message_body(msg, SALT) == msg


# ---------------------------------------------------------------------------
# Cross-cutting: idempotency on already-masked input
# ---------------------------------------------------------------------------


def test_mask_phone_idempotency_on_already_masked_input():
    once = mask_phone("+5511988734012", SALT)
    twice = mask_phone(once, SALT)
    # Re-masking is stable (length and structure unchanged).
    assert len(twice) == len(once)
    assert twice.startswith("+55")


@pytest.mark.parametrize(
    "fn, sample",
    [
        (mask_phone, "+5511988734012"),
        (mask_cpf, "123.456.789-00"),
        (mask_cep, "01310-100"),
        (mask_email, "ana@gmail.com"),
        (mask_license_plate, "ABC1D23"),
    ],
)
def test_all_maskers_change_their_input(fn, sample):
    assert fn(sample, SALT) != sample
