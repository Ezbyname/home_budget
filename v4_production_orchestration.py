"""
Unified Commitments — Phase 2E
Production Orchestration Service

RESPONSIBILITY:
  Single entry point for the atomic V4 production pipeline.
  Composes Phase 2A, 2B, and 2D2 inside a single BEGIN IMMEDIATE transaction.

TRANSACTION CONTRACT:
  BEGIN IMMEDIATE
    → persist_run_on_connection()   (Phase 2A — writes v4_run_results, pattern_families)
    → link_phase2b()                (Phase 2B — writes link tables)
    → orchestrate_authority_adjustment()  (Phase 2D2 — read-only, no writes)
  COMMIT
  ROLLBACK on any failure (zero partial rows).

PRODUCTION AUTHORIZATION:
  production_write_enabled must be the literal boolean True.
  It is sourced exclusively from server-side configuration.
  It is NEVER derived from request JSON, query params, or form input.
  Enforcement: _is_production_path() guard + is not True strict check.
  Non-production DB paths are not blocked by the flag.

DOES NOT:
  - Open an HTTP request context
  - Read from Flask request
  - Commit sub-transactions
  - Modify production_write_enabled
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from analyze_home_budget_v4 import (
    REVIEWED_TARGETS,
    PATTERN_OVERRIDES,
    INCOME_BASELINES,
)
from intelligence.v4_contracts import ClassificationReport
from intelligence.v4_cashflow_engine import run_analysis
from v4_persistence import PersistenceReport, persist_run_on_connection, _is_production_path
from v4_linking import LinkReport, link_phase2b
from v4_authority_orchestration import orchestrate_authority_adjustment
from v4_authority_runtime import AuthorityAdjustedAnalysis


# ── Result wrapper ────────────────────────────────────────────────────────────

@dataclass
class ProductionPipelineResult:
    """
    Immutable result from run_v4_production_pipeline().

    persistence:   Phase 2A outcome
    link:          Phase 2B outcome
    adjusted:      Phase 2D2 authority-adjusted analysis
    run_id:        shared run identity across all phases
    """
    persistence: PersistenceReport
    link:        LinkReport
    adjusted:    AuthorityAdjustedAnalysis
    run_id:      str


# ── Production authorization guard ───────────────────────────────────────────

def _check_production_authorization(db_path: str, production_write_enabled: object) -> None:
    """
    Enforce production write authorization.

    For a production DB path: requires production_write_enabled is True (strict).
    For a non-production path: no restriction from this guard.

    Raises RuntimeError if production path detected and flag is not literal True.
    """
    if _is_production_path(db_path) and production_write_enabled is not True:
        raise RuntimeError(
            "Production writes require explicit authorization: "
            "production_write_enabled must be literal True. "
            f"Got: {production_write_enabled!r}"
        )


# ── Public entry point ────────────────────────────────────────────────────────

def run_v4_production_pipeline(
    db_path: str,
    *,
    user_id: int,
    run_id: str | None = None,
    production_write_enabled: object = False,
) -> ProductionPipelineResult:
    """
    Execute the atomic Phase 2E V4 production pipeline.

    Atomic transaction wraps Phase 1 (analysis) → Phase 2A (persist) → Phase 2B (link) →
    Phase 2D2 (authority adjust) in a single BEGIN IMMEDIATE / COMMIT transaction.
    Any failure rolls back all phases completely (zero partial rows).

    Family Review baselines (REVIEWED_TARGETS, PATTERN_OVERRIDES, INCOME_BASELINES)
    are applied inside the transaction to ensure consistency.

    Parameters
    ----------
    db_path:
        Path to the SQLite database.
    user_id:
        User whose patterns are being analyzed and persisted.
    run_id:
        Optional caller-supplied run identity.  None → UUID4 generated once
        and shared across all phases.
    production_write_enabled:
        Server-side authorization flag.  Must be the literal boolean True to
        allow writes to a production DB path.  Never pass a value sourced from
        request JSON/query/form input.  Default is False (fail-closed).

    Returns
    -------
    ProductionPipelineResult

    Raises
    ------
    RuntimeError
        If db_path is a known production path and production_write_enabled
        is not literal True.
    Any exception from analysis, Phase 2A, 2B, or 2D2 propagates after full rollback.
    """
    _check_production_authorization(db_path, production_write_enabled)

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        conn.execute("BEGIN IMMEDIATE")

        # Phase 1 — run analysis with Family Review baselines inside transaction
        analysis_report = run_analysis(
            db_path,
            user_id=user_id,
            reviewed_targets=REVIEWED_TARGETS,
            pattern_overrides=PATTERN_OVERRIDES,
            income_baselines=INCOME_BASELINES,
        )

        # Phase 2A — persist raw evidence
        persistence = persist_run_on_connection(
            conn,
            analysis_report,
            user_id=user_id,
            run_id=run_id,
        )

        # Phase 2B — link families to existing commitments
        link = link_phase2b(
            conn,
            analysis_report,
            persistence,
            user_id=user_id,
        )

        # Phase 2D2 — apply authority adjustments (read-only, no DB writes)
        adjusted = orchestrate_authority_adjustment(
            conn,
            analysis_report,
            persistence,
            link,
            user_id=user_id,
        )

        conn.execute("COMMIT")

    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.close()

    return ProductionPipelineResult(
        persistence=persistence,
        link=link,
        adjusted=adjusted,
        run_id=persistence.run_id,
    )
