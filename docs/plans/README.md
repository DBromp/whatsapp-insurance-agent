# Project plans

Each file here is one durable plan. Plans are the contract: read here before starting work, update status as you progress.

## Convention

- **Filename:** `NN-short-name.md` where `NN` is a 2-digit sequence (`00`, `01`, `02`...)
- **Status header:** every plan starts with `**Status:** Draft | Approved | In Progress | Done`
- One plan per stream — don\'t merge multiple concerns into one file
- When complete, set status to `Done` and leave the file as historical record

## Workflow for Claude sessions

Before starting work:

1. Read every file in `docs/plans/` with status `Approved` or `In Progress`
2. Read `docs/decisions.md` for the ADR log
3. Read `.claude/skills/whatsapp-insurance-pipeline/SKILL.md` and the relevant references

When you start a chunk of work:

- Flip the plan\'s status to `In Progress` and add who\'s working it (`@cowork` or `@vscode`) plus start timestamp
- Commit that status flip alone before touching code — gives the other Claude a clear "do not touch this file" signal

When you finish:

- Flip status to `Done`
- Add a short "Outcome" section at the bottom summarising what landed (file paths, commit SHAs)

## Current plans

| File | Status | Owner |
|---|---|---|
| `00-project-overview.md` | In Progress | Daniel |
| `01-bronze-fixes.md` | Approved | TBD |
| `02-day2-silver.md` | Approved | TBD |
