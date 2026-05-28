# Bronze schema reference

## Column inventory (14 columns)

| Column | Type | Notes |
|---|---|---|
| message_id | string | UUID hex, 12 chars — unique row PK |
| conversation_id | string | `conv_XXXXXXXX` — groups messages of one conversation |
| timestamp | string | `YYYY-MM-DD HH:MM:SS` — parse to datetime in Silver |
| direction | string | `outbound` (seller) or `inbound` (lead) |
| sender_phone | string | `+55XXXXXXXXXXX` — PII, must be masked |
| sender_name | string | Free text — PII; messy for leads, clean for agents |
| message_type | string | text / audio / image / document / sticker / contact / video / location |
| message_body | string | Free text with embedded PII and unstructured data |
| status | string | sent / delivered / read / failed |
| channel | string | Always `whatsapp` in this base |
| campaign_id | string | `camp_XXX_fev2026` |
| agent_id | string | `agent_<name>_NN` |
| conversation_outcome | string | 7-value enum (see below) |
| metadata | string | JSON blob — parse in Silver |

## Embedded JSON `metadata` fields

| Field | Type | Notes |
|---|---|---|
| device | string | android / iphone / desktop / web |
| city | string | Free text |
| state | string | 2-char UF |
| response_time_sec | int or null | Null on outbound messages |
| is_business_hours | bool | 08–18h Mon–Fri |
| lead_source | string | google_ads / instagram_ads / facebook_ads / youtube_ads / google_remarketing / organico / indicacao / whatsapp_broadcast / sms / base_clientes |

## conversation_outcome enum

`venda_fechada`, `perdido_preco`, `perdido_concorrente`, `ghosting`, `desistencia_lead`, `proposta_enviada`, `em_negociacao`

## Embedded unstructured data in `message_body`

The dictionary calls these out — they're the heart of Silver-layer extraction:

- CPF, CEP, email, phone, license plate (PII to mask)
- Vehicle: brand, model, year, plate (unstandardized order — "gol 2019 1.0 placa ABC1D23", "tenho um HB20 22", "Corolla 2021/2022 prata")
- Competitor mentions: Porto Seguro, Azul Seguros, Bradesco Seguros, SulAmérica, Liberty, Allianz, Tokio Marine, Mapfre, HDI
- Claim history (sinistros) in natural language
- Audio transcripts may contain ASR errors
