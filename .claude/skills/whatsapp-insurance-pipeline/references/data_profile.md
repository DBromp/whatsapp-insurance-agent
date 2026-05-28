# Bronze data profile

Profiled from `conversations_bronze.parquet` (153,228 rows x 14 columns).

## Headline stats

- Conversations: 15,000 unique (`conversation_id`)
- Messages: 153,228
- Period: 2026-02-01 to 2026-03-01 (28 days)
- Agents: 20 unique
- Campaigns: 10 (all `*_fev2026`)
- States: all 27 BR UFs; SP dominates (~21%)
- Lead sources: 10 sources, roughly evenly distributed

## Conversation length buckets (matches the dictionary)

- 2 to 4 messages (cold/bounce): 33.0%
- 5 to 10 (short): 29.4%
- 11 to 20 (medium): 25.2%
- 21+ (long): 12.4%

## Outcome distribution (message-weighted)

- venda_fechada: 22.8%
- ghosting: 17.3%
- desistencia_lead: 13.2%
- perdido_concorrente: 12.7%
- em_negociacao: 12.5%
- proposta_enviada: 12.3%
- perdido_preco: 9.1%

## Agent skew

Top 5 of 20 agents handle ~46% of message volume — Lucas 09, Marcos 07, Julia 15, Rafael 03, Ana 12. Useful signal for the agent scorecard Gold table.

## Message types present

text (146,479), audio (4,108), image (903), document (624), sticker (420), contact (233), video (231), location (230)

## Status values present

delivered (89,175), read (53,380), sent (10,673). `failed` is absent in this snapshot.

## Deltas from the data dictionary (real data does not match dictionary in several places)

These quirks matter because the brief asks for defensive cleaning. Transforms must handle the documented behaviors even when they don't appear in this snapshot — future Bronze data could include them.

1. No name variants per phone. Dictionary claims each lead may appear under multiple spellings; in this dataset every phone maps to exactly one `sender_name`. Still build dedupe-by-fuzzy-name as a safety net.
2. No sent+delivered duplicates. Every `message_id` is unique. Build idempotent dedupe anyway.
3. No empty message bodies. Dictionary claims stickers/images produce null bodies; here all have text. Build null-safe transforms anyway.
4. No `failed` status records present. Build the status enum to include it.
5. Three extra `message_type` values not in the dictionary: `contact`, `video`, `location`. Schema must accept these.
6. `response_time_sec` is 51.8% null overall — but only because outbound messages don't carry it. Inbound rows have it 100%.
7. 15,000 conversations vs. 14,918 unique inbound phones — 82 conversations had zero inbound replies (true bounces).
