# Entity definitions

## Lead

The prospective buyer being contacted via WhatsApp.

- Identity rule: `sender_phone` is the canonical lead identifier. In this dataset every phone maps to exactly one `sender_name`, but never trust `sender_name` for identity — the brief explicitly says it can be inconsistent across conversations. Defensive code should still cluster by phone.
- Primary key: `sender_phone` (E.164 format, `+55XXXXXXXXXXX`)
- Common filters: `direction = 'inbound'` when you want lead-authored messages only
- PII to mask: `sender_phone` (token, length-preserved), `sender_name` (stable token — per project decision)

## Agent

The human seller. Always initiates the conversation (rule from the dictionary — the seller always sends the first message of any `conversation_id`, which is outbound).

- Identity: `agent_id` (e.g., `agent_lucas_09`), 20 distinct agents
- `sender_name` is clean for agents (unlike leads)
- NOT PII — agent IDs and names stay unmasked (internal employees)

## Conversation

A complete WhatsApp thread between one seller and one lead about one quote or negotiation.

- PK: `conversation_id` (`conv_XXXXXXXX`)
- One lead can appear in multiple conversations (follow-ups) — though in this dataset they don't
- The conversation's `conversation_outcome` is the same value across all of its rows
- Length distribution shapes intent classification (see `data_profile.md`)

## Campaign

Marketing origin of the lead before the seller reached out.

- Identifier: `campaign_id` (`camp_XXX_fev2026`), 10 distinct campaigns
- Same `campaign_id` across all messages in a conversation
- Joins to `lead_source` in metadata (e.g., `camp_landing_fev2026` -> `google_ads`)
