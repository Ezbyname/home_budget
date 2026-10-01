"""
Unified Commitments — Phase 2D2
Authority Orchestration: Composition Boundary

DESIGN:
  This module composes Phase 2D1 (authority read) with Phase 2D2 (runtime apply).
  It does NOT own persistence, linking, or transaction control.
  The caller provides: ClassificationReport, PersistenceReport, LinkReport, connection.
  This orchestrator only reads authority and produces an adjusted result.

RESPONSIBILITY:
  ✓ Read authority from commitment_authority table
  ✓ Resolve precedence (MANUAL_OVERRIDE > FAMILY_REVIEW)
  ✓ Apply resolved values to effective patterns
  ✓ Recompute derived fields
  ✓ Return immutable AuthorityAdjustedAnalysis

  ✗ Does NOT persist anything
  ✗ Does NOT link anything
  ✗ Does NOT run the classifier
  ✗ Does NOT own the transaction

TRANSACTION MODEL:
  Caller owns connection and transaction.
  Phase 2D1 uses its own read SAVEPOINT.
  Phase 2D2 orchestrator does not commit/rollback.
"""

from __future__ import annotations

import sqlite3

from v4_authority_read import resolve_authority_from_reports
from v4_authority_runtime import apply_authority_to_analysis, AuthorityAdjustedAnalysis
from intelligence.v4_contracts import ClassificationReport
from v4_persistence import PersistenceReport
from v4_linking import LinkReport


def orchestrate_authority_adjustment(
    conn: sqlite3.Connection,
    analysis_report: ClassificationReport,
    persistence_report: PersistenceReport,
    link_report: LinkReport,
    *,
    user_id: int,
) -> AuthorityAdjustedAnalysis:
    """
    Compose Phase 2D1 (authority read) with Phase 2D2 (authority runtime apply).

    This orchestrator:
    1. Resolves active authority from commitment_authority table (Phase 2D1)
    2. Applies resolved authority to effective patterns (Phase 2D2)
    3. Recomputes derived financial values
    4. Returns authority-adjusted result

    Does NOT perform:
    - persist_run() (Phase 2A)
    - link_phase2b() (Phase 2B)
    - persist_family_review_authority() (Phase 2C)
    - Transaction commit/rollback (caller owns transaction)

    Parameters:
        conn               — open SQLite connection; caller owns transaction
        analysis_report    — ClassificationReport from run_analysis()
        persistence_report — PersistenceReport from persist_run()
        link_report        — LinkReport from link_phase2b()
        user_id            — user context for authority resolution

    Returns:
        AuthorityAdjustedAnalysis containing base_report, final_report, authority_report

    Raises:
        ValueError if Phase 2D1 resolver encounters hard errors (duplicate authority,
        invalid enum, NULL value, etc.)
    """
    # Build baseline pattern map from effective patterns
    baseline_patterns = {p.description_key: p for p in analysis_report.effective.patterns}

    # Phase 2D1: Resolve active authority with precedence
    authority_report = resolve_authority_from_reports(
        conn,
        persistence_report,
        link_report,
        user_id=user_id,
        baseline_patterns=baseline_patterns,
    )

    # Phase 2D2: Apply authority to analysis
    adjusted = apply_authority_to_analysis(
        analysis_report,
        authority_report,
    )

    return adjusted
