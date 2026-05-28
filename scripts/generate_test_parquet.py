"""Generate a small synthetic parquet that matches the Bronze schema.

Use this to feed the incremental smoke test (Day 1 task 1.9): upload the
output file to /Volumes/nmstx_whatsapp_pipeline/bronze/raw_files/conversations/
and confirm Auto Loader picks it up without manual intervention.

Usage:
    python scripts/generate_test_parquet.py --rows 100 --out test_increment.parquet
"""

from __future__ import annotations

import argparse
import json
import random
import string
import uuid
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd


AGENTS = ["agent_lucas_09", "agent_marcos_07", "agent_julia_15", "agent_diego_14"]
CAMPAIGNS = ["camp_landing_mar2026", "camp_instagram_mar2026"]
OUTCOMES = ["venda_fechada", "ghosting", "em_negociacao", "proposta_enviada"]
STATES = ["SP", "RJ", "MG", "RS", "PR"]
CITIES = {"SP": "Sao Paulo", "RJ": "Rio de Janeiro", "MG": "Belo Horizonte", "RS": "Porto Alegre", "PR": "Curitiba"}
SOURCES = ["google_ads", "instagram_ads", "indicacao", "organico"]


def random_phone() -> str:
    digits = "".join(random.choices(string.digits, k=11))
    return f"+55{digits}"


def random_message(direction: str) -> str:
    if direction == "outbound":
        return random.choice([
            "oi, vi seu cadastro no site. posso ajudar com a cotacao?",
            "beleza! preciso dos dados do veiculo. marca, modelo, ano?",
            "perfeito! qual seu cep pra eu cotar?",
            "te enviei a proposta por email, deu pra ver?",
        ])
    return random.choice([
        "ola, quero cotar um seguro",
        "tenho um civic 2020",
        "meu cep e 04567-890",
        "vou pensar e te aviso",
    ])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100)
    parser.add_argument("--out", type=Path, default=Path("test_increment.parquet"))
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    random.seed(args.seed)

    # Use conv_99* range so we never collide with the original 15k conversations
    base_ts = datetime(2026, 3, 2, 9, 0, 0)
    rows = []
    n_convs = max(1, args.rows // 5)

    for c in range(n_convs):
        conv_id = f"conv_99{c:06d}"
        agent = random.choice(AGENTS)
        campaign = random.choice(CAMPAIGNS)
        outcome = random.choice(OUTCOMES)
        state = random.choice(STATES)
        city = CITIES[state]
        source = random.choice(SOURCES)
        lead_phone = random_phone()
        agent_phone = random_phone()
        conv_start = base_ts + timedelta(minutes=random.randint(0, 60 * 24 * 5))

        n_msgs = min(random.randint(2, 8), args.rows - len(rows))
        for i in range(n_msgs):
            direction = "outbound" if i % 2 == 0 else "inbound"
            phone = agent_phone if direction == "outbound" else lead_phone
            name = "Carlos Vendedor" if direction == "outbound" else "Maria Lead"
            meta = {
                "device": random.choice(["android", "iphone", "desktop"]),
                "city": city,
                "state": state,
                "response_time_sec": random.randint(30, 400) if direction == "inbound" else None,
                "is_business_hours": True,
                "lead_source": source,
            }
            rows.append({
                "message_id": uuid.uuid4().hex[:12],
                "conversation_id": conv_id,
                "timestamp": (conv_start + timedelta(seconds=i * 90)).strftime("%Y-%m-%d %H:%M:%S"),
                "direction": direction,
                "sender_phone": phone,
                "sender_name": name,
                "message_type": "text",
                "message_body": random_message(direction),
                "status": random.choice(["sent", "delivered", "read"]),
                "channel": "whatsapp",
                "campaign_id": campaign,
                "agent_id": agent,
                "conversation_outcome": outcome,
                "metadata": json.dumps(meta),
            })
            if len(rows) >= args.rows:
                break
        if len(rows) >= args.rows:
            break

    df = pd.DataFrame(rows)
    df.to_parquet(args.out, index=False)
    print(f"Wrote {len(df)} rows across {df['conversation_id'].nunique()} conversations to {args.out}")


if __name__ == "__main__":
    main()
