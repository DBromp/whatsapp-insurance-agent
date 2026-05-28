# Gold-layer insights

Eight tables, refreshed incrementally from Silver. Each one is documented below with what it shows, key columns, derivation logic, and notes.

## 1. agent_scorecard

Performance dashboard per seller for coaching and routing decisions.

Key columns: `agent_id`, `n_conversations`, `close_rate`, `avg_response_time_sec`, `ghosting_rate`, `competitor_loss_rate`, `avg_messages_to_close`, `total_revenue_proxy`

Logic: Aggregate `silver.conversations` grouped by `agent_id`. `close_rate = COUNT(outcome='venda_fechada') / COUNT(*)`. `avg_response_time_sec` from inbound message metadata, filtered to first-response in each conversation.

Watch out: Agents with <30 conversations get a `low_sample` flag — don't surface them in leaderboards.

## 2. funnel_stages

Each conversation classified by deepest stage reached.

Stage taxonomy: `greeting -> vehicle_collection -> quote_request -> proposal_sent -> negotiation -> close_or_loss`

Key columns: `conversation_id`, `max_stage_reached`, `stages_traversed[]`, `time_in_each_stage_sec`, `dropoff_stage`

Logic: Gemini classifies each message into a stage; aggregate to conversation level. Use a curated few-shot prompt with ~20 examples per stage.

## 3. objection_taxonomy

Cluster lead objections to find dominant rejection themes.

Categories: `price`, `coverage_gaps`, `trust_credibility`, `bad_timing`, `existing_insurer_satisfaction`, `vehicle_age_mismatch`, `other`

Key columns: `conversation_id`, `primary_objection`, `secondary_objections[]`, `objection_severity` (low/medium/high), `objection_resolved` (bool)

Logic: Gemini extracts objection signals from inbound messages in conversations with outcome in `(perdido_preco, perdido_concorrente, desistencia_lead, ghosting)`. Resolved = objection raised but conversation ended in `venda_fechada`.

## 4. competitor_matrix

Where leads compare and what wins/loses against whom.

Key columns: `competitor`, `mentions`, `won_against`, `lost_to`, `win_rate`, `avg_quote_gap_brl` (when extractable)

Logic: Extract competitor mentions from inbound text (regex + LLM verification — competitors listed in `schema.md`). Cross-tabulate with `conversation_outcome`. `lost_to(X) = conversations mentioning X AND outcome='perdido_concorrente'`.

## 5. ghosting_predictors

Features that correlate with leads dropping off mid-conversation.

Key columns: `feature`, `correlation_with_ghosting`, `lift_vs_baseline`, `sample_size`

Logic: Train a simple logistic model (or compute correlations) over Silver features predicting `outcome='ghosting'`. Candidate features: `response_time_sec` of first reply, `is_business_hours`, `device`, `state`, `lead_source`, `n_messages_before_quote_request`, message_type at dropoff.

## 6. vehicle_cohorts

Segments by vehicle profile vs. close rate.

Key columns: `vehicle_brand`, `vehicle_segment` (compact/sedan/SUV/luxury), `year_band` (<=5y / 6-10y / 11+y), `n_quotes`, `close_rate`, `avg_competitor_mentions`

Logic: LLM extracts (brand, model, year) from `message_body`. Hardcoded mapping `brand+model -> segment` (Hyundai HB20 = compact; Toyota Corolla = sedan; etc.). Group and aggregate.

## 7. lead_intent_score

Composite score predicting buying intent, computed at conversation level.

Key columns: `conversation_id`, `intent_score` (0-100), `intent_tier` (cold/warm/hot), `top_signals[]`

Logic: Weighted combination of: response_time (faster = warmer), n_messages (more = warmer), proposal_request explicit ask, document/personal data shared, business-hours engagement, sentiment trajectory.

Calibration: Validate against actual `outcome` — `hot` tier should have at least 3x the close rate of `cold`.

## 8. conversation_quality

Coaching score for agent performance per conversation.

Key columns: `conversation_id`, `agent_id`, `quality_score` (0-100), `dimensions` (responsiveness / clarity / personalization / objection_handling / closing), `coaching_notes` (LLM-generated 2-sentence summary)

Logic: Gemini scores each conversation against a rubric. Average per agent for coaching dashboards. Use a strict rubric in the system prompt to keep scores stable across runs.

## Cross-cutting notes

- Always exclude `outcome='em_negociacao'` from rate calculations (close_rate, ghosting_rate, etc.) — those conversations aren't terminal.
- Sample-size guards: any aggregate with <30 underlying conversations gets a `low_sample` flag.
- Refresh cadence: Gold tables refresh every 15 min via Workflow trigger when Silver's `_updated_at` watermark advances.
