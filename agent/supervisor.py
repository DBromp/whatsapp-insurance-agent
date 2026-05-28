"""Supervising agent for the WhatsApp insurance pipeline.

Architecture (built incrementally; fully wired on Day 4):

  1. `monitor_loop()` polls the Databricks Jobs API for recent runs
  2. On failure: `capture_failure()` collects stderr, notebook source, last N log lines
  3. `diagnose()` sends the captured context to Gemini and returns a structured patch proposal
  4. `classify_patch()` decides between safe-auto-apply and human-required
  5. Safe patches go through `apply_patch()`; otherwise `escalate()` writes a structured
     alert to `nmstx_whatsapp_pipeline.bronze._pipeline_alerts`

For now this file establishes the dataclasses and method signatures so the rest of
the codebase can reference them. Implementations land on Day 4.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


@dataclass
class JobFailure:
    """Captured context about a failed Databricks JobRun."""
    run_id: str
    job_id: str
    task_key: str
    notebook_path: str
    stderr: str
    notebook_source: str
    failed_at: str  # ISO timestamp


@dataclass
class PatchProposal:
    """A Gemini-generated patch proposal for a failed job."""
    failure: JobFailure
    diagnosis: str
    patch_kind: Literal[
        "schema_addition",
        "null_handling",
        "retry_config",
        "logic_change",
        "data_quality",
        "unknown",
    ]
    suggested_diff: str  # unified diff against notebook_source
    confidence: float  # 0.0 to 1.0
    safe_to_auto_apply: bool
    rationale: str


@dataclass
class SupervisorState:
    """Runtime state for the supervising agent."""
    seen_run_ids: set[str] = field(default_factory=set)
    open_alerts: list[PatchProposal] = field(default_factory=list)
    auto_applied: list[PatchProposal] = field(default_factory=list)
    poll_interval_sec: int = 60


class Supervisor:
    """Coordinates monitoring, diagnosis, and (safe) patching of pipeline jobs.

    Construct with a `databricks.sdk.WorkspaceClient` and a Gemini client.
    """

    def __init__(self, workspace_client, gemini_client):
        self.ws = workspace_client
        self.gemini = gemini_client
        self.state = SupervisorState()

    # ---- Public API (called from notebooks/04_agent_supervisor.py) ----

    def monitor_loop(self) -> None:
        """Main control loop. Runs continuously until interrupted."""
        raise NotImplementedError("Day 4 — implements polling + dispatch")

    def handle_failure(self, run_id: str) -> PatchProposal:
        """Capture, diagnose, and act on a single failed JobRun."""
        raise NotImplementedError("Day 4")

    # ---- Internal pipeline (each method becomes its own unit-testable function) ----

    def capture_failure(self, run_id: str) -> JobFailure:
        raise NotImplementedError("Day 4")

    def diagnose(self, failure: JobFailure) -> PatchProposal:
        raise NotImplementedError("Day 4")

    def classify_patch(self, proposal: PatchProposal) -> PatchProposal:
        raise NotImplementedError("Day 4")

    def apply_patch(self, proposal: PatchProposal) -> bool:
        raise NotImplementedError("Day 4")

    def escalate(self, proposal: PatchProposal) -> None:
        raise NotImplementedError("Day 4")
