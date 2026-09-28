"""
Unified Commitments — Phase 2B
Deterministic V4 → Existing Commitment Linking

MODEL A — SAME-RUN BRIDGE
PatternResult.member_ids are in-memory only (not persisted).
This module receives both the ClassificationReport and the Phase 2A
PersistenceReport, correlates them deterministically, and resolves
each pattern to a link decision.

ALLOWED WRITES:
    pattern_families.commitment_id
    pattern_families.is_primary
    commitment_classifier_snapshots
    commitment_suggestions
    commitment_link_conflicts
    commitment_link_events

NEVER writes:
    commitments, commitment_expense_links, commitment_occurrences,
    commitment_installment_meta, commitment_authority, description_key_aliases,
    v4_run_results
"""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional

from intelligence.v4_contracts import (
    ClassificationReport,
    PatternResult,
    RecurrenceStatus,
)
from v4_persistence import PersistenceReport, RunResultOutcome, _run_result_id

# ── Production DB guard ───────────────────────────────────────────────────────

import os

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


# ── Result types ──────────────────────────────────────────────────────────────

class LinkOutcome(str, Enum):
    LINKED                 = "LINKED"
    ALREADY_LINKED         = "ALREADY_LINKED"
    NEW_RECURRING_SUGGESTED = "NEW_RECURRING_SUGGESTED"
    AMBIGUOUS_SUGGESTED    = "AMBIGUOUS_SUGGESTED"
    CONFLICT               = "CONFLICT"
    SKIPPED_SUPERSEDED     = "SKIPPED_SUPERSEDED"
    NO_ACTION              = "NO_ACTION"


@dataclass
class PatternLinkResult:
    description_key:  str
    stream_index:     int
    run_result_id:    str
    family_id:        Optional[str]
    outcome:          LinkOutcome
    commitment_id:    Optional[str] = None
    detail:           dict          = field(default_factory=dict)


@dataclass
class LinkReport:
    run_id:   str
    user_id:  int
    results:  list[PatternLinkResult] = field(default_factory=list)

    def by_outcome(self) -> dict[str, list[PatternLinkResult]]:
        from collections import defaultdict
        d: dict[str, list[PatternLinkResult]] = defaultdict(list)
        for r in self.results:
            d[r.outcome.value].append(r)
        return dict(d)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


def _is_qualifying_recurring(pattern: PatternResult) -> bool:
    return pattern.recurrence_status == RecurrenceStatus.RECURRING


def _load_family(conn: sqlite3.Connection, family_id: str, user_id: int) -> Optional[dict]:
    row = conn.execute(
        "SELECT id, commitment_id, is_primary, family_status, user_id "
        "FROM pattern_families WHERE id = ? AND user_id = ?",
        (family_id, user_id),
    ).fetchone()
    if row is None:
        return None
    return {
        "id":            row[0],
        "commitment_id": row[1],
        "is_primary":    row[2],
        "family_status": row[3],
        "user_id":       row[4],
    }


def _find_active_primary_family_for_commitment(
    conn: sqlite3.Connection, commitment_id: str, user_id: int, exclude_family_id: Optional[str] = None
) -> Optional[str]:
    q = (
        "SELECT id FROM pattern_families "
        "WHERE commitment_id = ? AND user_id = ? "
        "AND family_status = 'ACTIVE' AND is_primary = 1"
    )
    params: list = [commitment_id, user_id]
    if exclude_family_id is not None:
        q += " AND id != ?"
        params.append(exclude_family_id)
    row = conn.execute(q, params).fetchone()
    return row[0] if row else None


def _resolve_candidate_commitment(
    conn: sqlite3.Connection, member_ids: tuple[str, ...], user_id: int
) -> tuple[Optional[str], str]:
    """
    Query CEL for member_ids ownership.
    Returns (commitment_id_or_None, reason).
    reason is one of: 'single', 'zero', 'multiple', 'user_drift'.
    """
    if not member_ids:
        return None, "zero"

    rows = conn.execute(
        f"SELECT DISTINCT cel.commitment_id, e.user_id "
        f"FROM commitment_expense_links cel "
        f"JOIN expenses e ON e.id = cel.expense_id "
        f"WHERE cel.expense_id IN ({','.join('?' for _ in member_ids)}) "
        f"AND cel.membership_type IN ('MEMBER', 'OCCURRENCE_CONFIRMED')",
        list(member_ids),
    ).fetchall()

    if not rows:
        return None, "zero"

    # Check for user drift: expense belongs to a different user
    for _, exp_user_id in rows:
        if exp_user_id != user_id:
            return None, "user_drift"

    commitment_ids = {r[0] for r in rows}
    if len(commitment_ids) == 1:
        return commitment_ids.pop(), "single"
    return None, "multiple"


def _snapshot_exists(conn: sqlite3.Connection, commitment_id: str, run_result_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM commitment_classifier_snapshots "
        "WHERE commitment_id = ? AND representative_run_result_id = ? "
        "AND snapshot_type = 'V4_SINGLE'",
        (commitment_id, run_result_id),
    ).fetchone()
    return row is not None


def _insert_snapshot(
    conn: sqlite3.Connection,
    commitment_id: str,
    user_id: int,
    run_result_id: str,
    pattern: PatternResult,
    now: str,
) -> bool:
    """INSERT OR IGNORE snapshot. Returns True if a new row was created."""
    from decimal import Decimal
    monthly_agorot: Optional[int] = None
    if pattern.monthly_reserve_contrib is not None:
        d = Decimal(str(pattern.monthly_reserve_contrib))
        from decimal import ROUND_HALF_UP
        monthly_agorot = int(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) * 100)

    conn.execute(
        "INSERT OR IGNORE INTO commitment_classifier_snapshots "
        "(commitment_id, user_id, snapshot_type, representative_run_result_id, "
        " recurrence_status, commitment_status, classifier_lifecycle_status, "
        " budget_class, reserve_eligible, monthly_reserve_contrib_agorot, cadence, "
        " created_at) "
        "VALUES (?,?, 'V4_SINGLE', ?, ?,?,?, ?,?,?,?, ?)",
        (
            commitment_id, user_id, run_result_id,
            pattern.recurrence_status.value,
            pattern.commitment_status.value,
            pattern.lifecycle_status.value,
            pattern.budget_class.value,
            1 if pattern.reserve_eligible else 0,
            monthly_agorot,
            pattern.cadence.value,
            now,
        ),
    )
    return conn.execute("SELECT changes()").fetchone()[0] > 0


def _insert_suggestion(
    conn: sqlite3.Connection,
    user_id: int,
    suggestion_type: str,
    description_key: str,
    family_id: Optional[str],
    run_result_id: str,
    candidate_commitment_id: Optional[str],
    detail: dict,
    now: str,
) -> bool:
    """INSERT OR IGNORE suggestion (Phase 0.4 dedup indexes). Returns True if new."""
    conn.execute(
        "INSERT OR IGNORE INTO commitment_suggestions "
        "(user_id, suggestion_type, description_key, family_id, run_result_id, "
        " candidate_commitment_id, detail, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (
            user_id, suggestion_type, description_key,
            family_id, run_result_id,
            candidate_commitment_id,
            json.dumps(detail),
            now,
        ),
    )
    return conn.execute("SELECT changes()").fetchone()[0] > 0


def _insert_conflict(
    conn: sqlite3.Connection,
    user_id: int,
    run_id: str,
    conflict_type: str,
    family_id: Optional[str],
    commitment_id: Optional[str],
    detail: dict,
    now: str,
) -> bool:
    """INSERT OR IGNORE conflict (Phase 0.4 dedup indexes). Returns True if new."""
    conn.execute(
        "INSERT OR IGNORE INTO commitment_link_conflicts "
        "(commitment_id, user_id, family_id, run_id, conflict_type, detail, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (
            commitment_id, user_id, family_id, run_id,
            conflict_type,
            json.dumps(detail),
            now,
        ),
    )
    return conn.execute("SELECT changes()").fetchone()[0] > 0


def _insert_event(
    conn: sqlite3.Connection,
    user_id: int,
    event_type: str,
    commitment_id: Optional[str],
    family_id: Optional[str],
    detail: dict,
    now: str,
) -> None:
    conn.execute(
        "INSERT INTO commitment_link_events "
        "(commitment_id, user_id, family_id, event_type, detail, created_at) "
        "VALUES (?,?,?,?,?,?)",
        (commitment_id, user_id, family_id, event_type, json.dumps(detail), now),
    )


# ── Per-pattern decision (one SAVEPOINT) ────────────────────────────────────

def _process_one(
    conn: sqlite3.Connection,
    pattern: PatternResult,
    outcome: RunResultOutcome,
    run_id: str,
    user_id: int,
    now: str,
) -> PatternLinkResult:
    sp = f"sp_p2b_{outcome.run_result_id.replace('-', '')}"
    conn.execute(f"SAVEPOINT {sp}")
    try:
        result = _decide(conn, pattern, outcome, run_id, user_id, now)
        conn.execute(f"RELEASE {sp}")
        return result
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO {sp}")
            conn.execute(f"RELEASE {sp}")
        except Exception:
            pass
        raise


def _decide(
    conn: sqlite3.Connection,
    pattern: PatternResult,
    outcome: RunResultOutcome,
    run_id: str,
    user_id: int,
    now: str,
) -> PatternLinkResult:
    rrid       = outcome.run_result_id
    family_id  = outcome.family_id
    desc_key   = pattern.description_key
    member_ids = pattern.member_ids

    # ── UNRESOLVED PARALLEL ─────────────────────────────────────────────────
    if family_id is None:
        _insert_suggestion(
            conn, user_id, "AMBIGUOUS_FAMILY", desc_key,
            None, rrid, None,
            {"reason": "UNRESOLVED_PARALLEL", "stream_index": outcome.stream_index,
             "description_key": desc_key},
            now,
        )
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, None,
            LinkOutcome.AMBIGUOUS_SUGGESTED,
            detail={"reason": "UNRESOLVED_PARALLEL"},
        )

    # ── Load family ─────────────────────────────────────────────────────────
    family = _load_family(conn, family_id, user_id)
    if family is None or family["family_status"] == "SUPERSEDED":
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.SKIPPED_SUPERSEDED,
        )

    # ── Resolve deterministic candidate ─────────────────────────────────────
    candidate_id, reason = _resolve_candidate_commitment(conn, member_ids, user_id)

    # ── USER_ID_DRIFT ───────────────────────────────────────────────────────
    if reason == "user_drift":
        new_conflict = _insert_conflict(
            conn, user_id, run_id, "USER_ID_DRIFT",
            family_id, None,
            {"description_key": desc_key, "run_result_id": rrid},
            now,
        )
        if new_conflict:
            _insert_event(conn, user_id, "CONFLICT_DETECTED", None, family_id,
                          {"conflict_type": "USER_ID_DRIFT"}, now)
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.CONFLICT, detail={"conflict_type": "USER_ID_DRIFT"},
        )

    # ── MULTIPLE (contradictory deterministic evidence) ──────────────────────
    # CASE B: member_ids resolve to >1 distinct commitment.
    # Contradictory evidence — write CONFLICT, never a suggestion.
    if reason == "multiple":
        new_conflict = _insert_conflict(
            conn, user_id, run_id, "AMBIGUOUS_FAMILY",
            family_id, None,
            {"description_key": desc_key, "run_result_id": rrid},
            now,
        )
        if new_conflict:
            _insert_event(conn, user_id, "CONFLICT_DETECTED", None, family_id,
                          {"conflict_type": "AMBIGUOUS_FAMILY"}, now)
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.CONFLICT, detail={"conflict_type": "AMBIGUOUS_FAMILY"},
        )

    # ── ZERO deterministic candidates ───────────────────────────────────────
    if candidate_id is None:
        if _is_qualifying_recurring(pattern):
            _insert_suggestion(
                conn, user_id, "NEW_RECURRING", desc_key,
                family_id, rrid, None, {}, now,
            )
            return PatternLinkResult(
                desc_key, outcome.stream_index, rrid, family_id,
                LinkOutcome.NEW_RECURRING_SUGGESTED,
            )
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.NO_ACTION, detail={"reason": "no_deterministic_candidate"},
        )

    # ── ONE deterministic candidate ─────────────────────────────────────────
    # Check existing family link
    existing_link = family["commitment_id"]

    if existing_link is not None and existing_link != candidate_id:
        # Family already linked to a DIFFERENT commitment → OVERLAPPING_WINDOW
        new_conflict = _insert_conflict(
            conn, user_id, run_id, "OVERLAPPING_WINDOW",
            family_id, candidate_id,
            {"description_key": desc_key, "existing_commitment_id": existing_link,
             "candidate_commitment_id": candidate_id},
            now,
        )
        if new_conflict:
            _insert_event(conn, user_id, "CONFLICT_DETECTED", candidate_id, family_id,
                          {"conflict_type": "OVERLAPPING_WINDOW",
                           "existing_commitment_id": existing_link}, now)
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.CONFLICT, commitment_id=candidate_id,
            detail={"conflict_type": "OVERLAPPING_WINDOW",
                    "existing_commitment_id": existing_link},
        )

    if existing_link == candidate_id:
        # Already linked to the same commitment — idempotent
        new_snap = _insert_snapshot(conn, candidate_id, user_id, rrid, pattern, now)
        if new_snap:
            _insert_event(conn, user_id, "SNAPSHOT_CREATED", candidate_id, family_id,
                          {"run_result_id": rrid}, now)
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.ALREADY_LINKED, commitment_id=candidate_id,
        )

    # family.commitment_id is NULL — attempt fresh link
    # Check: does candidate already have a different primary family?
    other_primary = _find_active_primary_family_for_commitment(
        conn, candidate_id, user_id, exclude_family_id=family_id
    )
    if other_primary is not None:
        # Primary conflict — write AMBIGUOUS_FAMILY suggestion, not a link
        _insert_suggestion(
            conn, user_id, "AMBIGUOUS_FAMILY", desc_key,
            family_id, rrid, candidate_id,
            {"reason": "candidate_has_existing_primary_family",
             "existing_primary_family_id": other_primary},
            now,
        )
        return PatternLinkResult(
            desc_key, outcome.stream_index, rrid, family_id,
            LinkOutcome.AMBIGUOUS_SUGGESTED, commitment_id=candidate_id,
            detail={"reason": "candidate_has_existing_primary_family",
                    "existing_primary_family_id": other_primary},
        )

    # All conditions satisfied — perform link
    conn.execute(
        "UPDATE pattern_families SET commitment_id = ?, is_primary = 1 "
        "WHERE id = ? AND user_id = ?",
        (candidate_id, family_id, user_id),
    )
    _insert_event(conn, user_id, "FAMILY_LINKED", candidate_id, family_id,
                  {"description_key": desc_key, "run_result_id": rrid}, now)

    new_snap = _insert_snapshot(conn, candidate_id, user_id, rrid, pattern, now)
    if new_snap:
        _insert_event(conn, user_id, "SNAPSHOT_CREATED", candidate_id, family_id,
                      {"run_result_id": rrid}, now)

    return PatternLinkResult(
        desc_key, outcome.stream_index, rrid, family_id,
        LinkOutcome.LINKED, commitment_id=candidate_id,
    )


# ── Correlation helper ────────────────────────────────────────────────────────

def _correlate(
    report: ClassificationReport,
    persistence: PersistenceReport,
) -> list[tuple[PatternResult, RunResultOutcome]]:
    """
    Deterministically pair each raw PatternResult with its persisted RunResultOutcome.
    Uses the same UUID5 identity as Phase 2A: (run_id, description_key, stream_index).
    """
    by_rrid: dict[str, RunResultOutcome] = {o.run_result_id: o for o in persistence.outcomes}

    pairs: list[tuple[PatternResult, RunResultOutcome]] = []
    by_key: dict[str, list[PatternResult]] = {}
    for p in report.raw.patterns:
        by_key.setdefault(p.description_key, []).append(p)

    for desc_key, streams in by_key.items():
        for stream_index, pattern in enumerate(streams):
            rrid = _run_result_id(persistence.run_id, desc_key, stream_index)
            outcome = by_rrid.get(rrid)
            if outcome is None:
                raise ValueError(
                    f"No persisted run result for description_key={desc_key!r} "
                    f"stream_index={stream_index} run_id={persistence.run_id!r}. "
                    "Ensure persistence report matches this classification report."
                )
            pairs.append((pattern, outcome))

    return pairs


# ── Public API ─────────────────────────────────────────────────────────────────

def link_phase2b(
    conn: sqlite3.Connection,
    classification_report: ClassificationReport,
    persistence_report: PersistenceReport,
    *,
    user_id: int,
) -> LinkReport:
    """
    Phase 2B: deterministically link V4 pattern families to existing commitments.

    Receives:
        conn                    — open SQLite connection (PRAGMA foreign_keys=ON recommended)
        classification_report   — output of run_analysis(); member_ids live here only
        persistence_report      — output of persist_run(); provides run identity
        user_id                 — must match persistence_report.user_id

    Writes only:
        pattern_families.commitment_id / is_primary
        commitment_classifier_snapshots
        commitment_suggestions
        commitment_link_conflicts
        commitment_link_events

    Never writes:
        commitments, commitment_expense_links, commitment_authority,
        commitment_occurrences, commitment_installment_meta,
        description_key_aliases, v4_run_results

    Each pattern decision is atomic (SAVEPOINT). Failures in one do not
    affect other patterns.

    Returns LinkReport with per-pattern outcomes.
    """
    if user_id != persistence_report.user_id:
        raise ValueError(
            f"user_id mismatch: caller supplied {user_id}, "
            f"persistence_report carries {persistence_report.user_id}"
        )

    run_id = persistence_report.run_id
    out    = LinkReport(run_id=run_id, user_id=user_id)
    now    = _now_iso()

    pairs = _correlate(classification_report, persistence_report)

    for pattern, outcome in pairs:
        result = _process_one(conn, pattern, outcome, run_id, user_id, now)
        out.results.append(result)

    return out


def link_phase2b_from_path(
    db_path: str,
    classification_report: ClassificationReport,
    persistence_report: PersistenceReport,
    *,
    user_id: int,
) -> LinkReport:
    """
    Convenience wrapper that opens a connection, enforces production guard,
    calls link_phase2b, commits, and closes.
    """
    if _is_production_path(db_path):
        raise RuntimeError(
            f"PRODUCTION SAFETY ABORT: refusing to write to production DB: {db_path!r}"
        )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        report = link_phase2b(conn, classification_report, persistence_report,
                              user_id=user_id)
        conn.commit()
        return report
    finally:
        conn.close()
