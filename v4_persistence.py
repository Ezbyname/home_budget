"""
Unified Commitments — Phase 2A
V4 Evidence Persistence + Pattern Family Identity

Companion to intelligence/v4_cashflow_engine.py.  That module is and remains
read-only (ZERO DB WRITES enforced by URI mode).  This module owns all DB
writes for V4 raw-evidence persistence.

ALLOWED WRITES (Phase 2A only):
    v4_run_results
    pattern_families

OUT OF SCOPE (Phase 2B or later):
    commitments, commitment_occurrences, commitment_installment_meta,
    commitment_expense_links, commitment_classifier_snapshots,
    commitment_authority, commitment_suggestions, commitment_link_conflicts,
    description_key_aliases.

Production DB guard: rejects the known production path before any write.
"""

from __future__ import annotations

import json
import os
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Optional

from intelligence.v4_contracts import ClassificationReport, PatternResult

# ── Production DB guard ───────────────────────────────────────────────────────

_KNOWN_PRODUCTION_PATHS = [
    r"C:\Users\erezg\.budget_tracker_data\budget.db",
    "/c/users/erezg/.budget_tracker_data/budget.db",
]

_PRODUCTION_FINGERPRINTS = frozenset(
    p.lower().replace("\\", "/").rstrip("/")
    for p in _KNOWN_PRODUCTION_PATHS
)


def _canonicalise(path: str) -> str:
    return os.path.normpath(path).lower().replace("\\", "/")


def _is_production_path(db_path: str) -> bool:
    canon = _canonicalise(db_path)
    for fp in _PRODUCTION_FINGERPRINTS:
        if canon == fp or canon.endswith("/" + fp.lstrip("/")):
            return True
    abs_canon = _canonicalise(os.path.abspath(db_path))
    for fp in _PRODUCTION_FINGERPRINTS:
        if abs_canon == fp:
            return True
    return False


# ── Money conversion ──────────────────────────────────────────────────────────

def _to_agorot(amount: Optional[Decimal]) -> Optional[int]:
    """Convert Decimal NIS amount to exact INTEGER agorot, or None."""
    if amount is None:
        return None
    d = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int(d * 100)


# ── Result types ──────────────────────────────────────────────────────────────

class FamilyResolution(str, Enum):
    MATCHED_EXISTING    = "MATCHED_EXISTING"     # reused an ACTIVE non-split family
    CREATED_NEW         = "CREATED_NEW"          # created a new ACTIVE non-split family
    UNRESOLVED_PARALLEL = "UNRESOLVED_PARALLEL"  # parallel streams — no safe discriminator


@dataclass
class RunResultOutcome:
    run_result_id:     str
    description_key:   str
    stream_index:      int
    family_id:         Optional[str]
    family_resolution: FamilyResolution


@dataclass
class PersistenceReport:
    run_id:   str
    user_id:  int
    outcomes: list[RunResultOutcome] = field(default_factory=list)

    def counts(self):
        from collections import Counter
        return Counter(o.family_resolution.value for o in self.outcomes)

    def ids_by_resolution(self):
        from collections import defaultdict
        d = defaultdict(list)
        for o in self.outcomes:
            d[o.family_resolution.value].append(o.run_result_id)
        return dict(d)


# ── Deterministic run_result identity ─────────────────────────────────────────
#
# UUID5 derived from (run_id, description_key, stream_index) guarantees:
#   same run_id + same stream  →  same result ID  →  idempotent INSERT OR IGNORE
#   new run_id                 →  new result IDs  →  new historical evidence

def _run_result_id(run_id: str, description_key: str, stream_index: int) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS,
                          f"{run_id}:{description_key}:{stream_index}"))


# ── Family matching / creation (ACTIVE non-split families only) ───────────────

def _find_active_single_family(
    conn: sqlite3.Connection, user_id: int, description_key: str
) -> Optional[str]:
    """Return id of ACTIVE non-split family for (user_id, description_key), or None."""
    row = conn.execute("""
        SELECT id FROM pattern_families
        WHERE user_id = ?
          AND primary_description_key = ?
          AND is_split_discriminator = 0
          AND family_status = 'ACTIVE'
    """, (user_id, description_key)).fetchone()
    return row[0] if row else None


def _create_single_family(
    conn: sqlite3.Connection, user_id: int, description_key: str, now_iso: str
) -> str:
    """Create a new ACTIVE non-split family; return its id."""
    fid = str(uuid.uuid4())
    conn.execute("""
        INSERT INTO pattern_families
            (id, user_id, primary_description_key,
             is_split_discriminator, amount_cluster_agorot,
             window_start, window_end, commitment_id,
             is_primary, linked_by, family_status,
             superseded_at, superseded_by_event_id,
             created_at, updated_at)
        VALUES (?,?,?, 0, NULL, NULL, NULL, NULL, 1, 'AUTO', 'ACTIVE',
                NULL, NULL, ?, ?)
    """, (fid, user_id, description_key, now_iso, now_iso))
    return fid


# ── Core: persist one PatternResult → one v4_run_results row ─────────────────

def _persist_run_result(
    conn: sqlite3.Connection,
    pattern: PatternResult,
    run_id: str,
    user_id: int,
    stream_index: int,
    family_id: Optional[str],
    now_iso: str,
) -> str:
    """INSERT OR IGNORE one raw run result. Returns the deterministic run_result_id."""
    rrid = _run_result_id(run_id, pattern.description_key, stream_index)

    planning_agorot = _to_agorot(pattern.planning_amount)
    monthly_agorot  = _to_agorot(pattern.monthly_reserve_contrib) or 0

    review_reasons_json = json.dumps([
        r.value if hasattr(r, "value") else str(r)
        for r in pattern.review_reasons
    ])

    conn.execute("""
        INSERT OR IGNORE INTO v4_run_results
            (id, run_id, user_id, family_id,
             description_key, stream_index, label,
             planning_amount_agorot, cadence,
             recurrence_status, commitment_status,
             classifier_lifecycle_status, budget_class,
             reserve_eligible, monthly_reserve_contrib_agorot,
             cadence_coverage, evidence_month_count,
             review_required, review_reasons, created_at)
        VALUES (?,?,?,?, ?,?,?, ?,?, ?,?, ?,?, ?,?, NULL,NULL, ?,?,?)
    """, (
        rrid, run_id, user_id, family_id,
        pattern.description_key, stream_index, pattern.label,
        planning_agorot,
        pattern.cadence.value,
        pattern.recurrence_status.value,
        pattern.commitment_status.value,
        pattern.lifecycle_status.value,
        pattern.budget_class.value,
        1 if pattern.reserve_eligible else 0,
        monthly_agorot,
        1 if pattern.family_review_required else 0,
        review_reasons_json,
        now_iso,
    ))
    return rrid


# ── Caller-owned connection primitive (Phase 2E) ──────────────────────────────

def persist_run_on_connection(
    conn: sqlite3.Connection,
    report: ClassificationReport,
    *,
    user_id: int,
    run_id: Optional[str] = None,
) -> PersistenceReport:
    """
    Persist raw V4 classifier evidence using a caller-supplied connection.

    The caller owns the connection and its outer transaction
    (BEGIN IMMEDIATE / COMMIT / ROLLBACK).  This function:
      - does NOT open a connection
      - does NOT close the connection
      - does NOT call conn.commit()
      - does NOT call conn.rollback() on the caller's outer transaction

    A local SAVEPOINT (sp_v4_persist) is used for internal atomicity;
    on failure it is rolled back and released before re-raising, so the
    caller's outer transaction remains in a clean, rollback-able state.

    All other behavior is identical to persist_run():
      run_id, family matching, INSERT OR IGNORE idempotence, tables written,
      PersistenceReport / RunResultOutcome structure.

    Does NOT enforce a db_path production guard (path is not known here).
    Production authorization is the caller's responsibility.
    """
    if run_id is None:
        run_id = str(uuid.uuid4())

    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    out = PersistenceReport(run_id=run_id, user_id=user_id)

    # Group raw patterns by description_key to detect parallel streams.
    by_key: dict[str, list[PatternResult]] = {}
    for p in report.raw.patterns:
        by_key.setdefault(p.description_key, []).append(p)

    sp = "sp_v4_persist"
    conn.execute(f"SAVEPOINT {sp}")
    try:
        for description_key, streams in by_key.items():
            if len(streams) > 1:
                # Parallel streams: no deterministic split discriminator
                # available from PatternResult alone → fail closed.
                for stream_index, pattern in enumerate(streams):
                    rrid = _persist_run_result(
                        conn, pattern, run_id, user_id,
                        stream_index, None, now_iso,
                    )
                    out.outcomes.append(RunResultOutcome(
                        run_result_id=rrid,
                        description_key=description_key,
                        stream_index=stream_index,
                        family_id=None,
                        family_resolution=FamilyResolution.UNRESOLVED_PARALLEL,
                    ))
            else:
                # Single stream: match or create ACTIVE non-split family.
                pattern = streams[0]
                family_id = _find_active_single_family(
                    conn, user_id, description_key
                )
                if family_id is not None:
                    resolution = FamilyResolution.MATCHED_EXISTING
                else:
                    family_id = _create_single_family(
                        conn, user_id, description_key, now_iso
                    )
                    resolution = FamilyResolution.CREATED_NEW

                rrid = _persist_run_result(
                    conn, pattern, run_id, user_id, 0, family_id, now_iso,
                )
                out.outcomes.append(RunResultOutcome(
                    run_result_id=rrid,
                    description_key=description_key,
                    stream_index=0,
                    family_id=family_id,
                    family_resolution=resolution,
                ))

        conn.execute(f"RELEASE {sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO {sp}")
            conn.execute(f"RELEASE {sp}")
        except Exception:
            pass
        raise

    return out


# ── Public entry point ────────────────────────────────────────────────────────

def persist_run(
    db_path: str,
    report: ClassificationReport,
    *,
    user_id: int,
    run_id: Optional[str] = None,
) -> PersistenceReport:
    """
    Persist raw V4 classifier evidence from report.raw.patterns into
    v4_run_results and pattern_families.

    - run_id=None → generate UUID4 once for this call.
    - run_id=R supplied → use R (caller controls identity; enables retry).
    - Calling twice with the same run_id → ZERO duplicates (idempotent).
    - Calling with a new run_id → new historical evidence rows.

    Family matching:
    - Single stream per description_key → match or create ACTIVE non-split family.
    - Multiple streams (parallel) per description_key → family_id = NULL,
      resolution = UNRESOLVED_PARALLEL (fail-closed; no unsafe family invented).

    Only ACTIVE families participate in new matching.
    SUPERSEDED families are never matched.

    Raises RuntimeError if db_path resolves to the known production DB.
    All writes are wrapped in a single SAVEPOINT; failure rolls back cleanly.
    """
    if _is_production_path(db_path):
        raise RuntimeError(
            f"PRODUCTION SAFETY ABORT: refusing to write to production DB: {db_path!r}"
        )

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        out = persist_run_on_connection(conn, report, user_id=user_id, run_id=run_id)
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        conn.close()

    return out
