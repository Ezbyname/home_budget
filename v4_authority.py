"""
Unified Commitments — Phase 2C
Family Review Authority Persistence

Persists already-approved Family Review decisions from a ClassificationReport
into commitment_authority rows with authority_source='FAMILY_REVIEW'.

PURPOSE:
    Phase 2C is persistence only.
    It does NOT activate authority at runtime.
    It does NOT change effective V4 results.
    It does NOT replace apply_overrides().
    It does NOT implement MANUAL_OVERRIDE precedence.
    Phase 2D owns authority reads and runtime precedence.

ALLOWED WRITES:
    commitment_authority

NEVER writes:
    v4_run_results, commitment_classifier_snapshots, pattern_families,
    commitments, commitment_expense_links, commitment_occurrences,
    commitment_installment_meta, commitment_suggestions,
    commitment_link_conflicts, commitment_link_events, expenses, installments

Production DB guard: rejects the known production path before any write.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional

from intelligence.v4_cashflow_engine import PatternOverride
from intelligence.v4_contracts import (
    Cadence,
    ClassificationReport,
    CommitmentStatus,
    LifecycleStatus,
    PurposeType,
    RecurrenceStatus,
)
from v4_linking import LinkReport, LinkOutcome, PatternLinkResult
from v4_persistence import (
    FamilyResolution,
    PersistenceReport,
    RunResultOutcome,
    _is_production_path,
    _run_result_id,
)


# ── Result types ──────────────────────────────────────────────────────────────

class Phase2COutcome(str, Enum):
    INSERTED                       = "INSERTED"
    ALREADY_CURRENT                = "ALREADY_CURRENT"
    REPLACED                       = "REPLACED"
    DEFER_PARALLEL_UNRESOLVED      = "DEFER_PARALLEL_UNRESOLVED"
    DEFER_CANONICAL_MERGE          = "DEFER_CANONICAL_MERGE"
    DEFER_SUPERSEDED               = "DEFER_SUPERSEDED"
    DEFER_NO_LINKED_COMMITMENT     = "DEFER_NO_LINKED_COMMITMENT"


@dataclass
class Phase2CResult:
    override_id:      str
    description_key:  str
    field_name:       str
    commitment_id:    Optional[str]
    outcome:          Phase2COutcome
    reason:           Optional[str]   = None
    previous_row_id:  Optional[int]   = None
    new_row_id:       Optional[int]   = None


@dataclass
class AuthorityReport:
    run_id:   str
    user_id:  int
    outcomes: list[Phase2CResult] = field(default_factory=list)

    def inserted_count(self) -> int:
        return sum(1 for o in self.outcomes if o.outcome == Phase2COutcome.INSERTED)

    def already_current_count(self) -> int:
        return sum(1 for o in self.outcomes if o.outcome == Phase2COutcome.ALREADY_CURRENT)

    def replaced_count(self) -> int:
        return sum(1 for o in self.outcomes if o.outcome == Phase2COutcome.REPLACED)

    def deferred_count(self) -> int:
        return sum(1 for o in self.outcomes if o.outcome.value.startswith("DEFER_"))

    def by_outcome(self) -> dict[str, list[Phase2CResult]]:
        from collections import defaultdict
        d: dict[str, list[Phase2CResult]] = defaultdict(list)
        for o in self.outcomes:
            d[o.outcome.value].append(o)
        return dict(d)


# ── Serialization ─────────────────────────────────────────────────────────────

_CANONICAL_MERGE_IDENTITY = "google-cloud-tbd"

_FIELD_EXPECTED_TYPES: dict[str, type | tuple] = {
    "recurrence_status":  RecurrenceStatus,
    "commitment_status":  CommitmentStatus,
    "lifecycle_status":   LifecycleStatus,
    "cadence":            Cadence,
    "purpose_type":       PurposeType,
    "planning_amount":    (Decimal, type(None)),
}


def _serialize_value(field_name: str, value: object) -> Optional[str]:
    """Serialize PatternOverride.value to canonical TEXT for commitment_authority.value."""
    if field_name not in _FIELD_EXPECTED_TYPES:
        raise TypeError(
            f"Unknown field_name for Phase 2C serialization: {field_name!r}. "
            f"Accepted: {sorted(_FIELD_EXPECTED_TYPES)}"
        )
    expected = _FIELD_EXPECTED_TYPES[field_name]
    if not isinstance(value, expected):
        raise TypeError(
            f"field_name={field_name!r}: expected type {expected}, "
            f"got {type(value).__name__!r} ({value!r})"
        )
    if value is None:
        return None
    if isinstance(value, Decimal):
        return str(value)
    return value.value  # str-Enum .value (e.g. "RECURRING", "CONFIRMED")


def _expected_audit_override_str(ov: PatternOverride) -> Optional[str]:
    """
    Compute what apply_overrides() wrote into override_audit changed_fields[f]["override"].
    From apply_overrides() line: "override": str(ov.value) if ov.value is not None else None
    """
    return str(ov.value) if ov.value is not None else None


def _is_canonical_merge_defer(ov: PatternOverride) -> bool:
    return ov.canonical_identity == _CANONICAL_MERGE_IDENTITY


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_iso() -> str:
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")


def _table_snapshot(conn: sqlite3.Connection, table: str) -> Optional[tuple]:
    """
    Returns a deterministically ordered tuple of row-tuples for the given table.
    Preserves row multiplicity. Returns None if table does not exist.
    """
    try:
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
        return tuple(tuple(row) for row in rows)
    except Exception:
        try:
            rows = conn.execute(f"SELECT * FROM {table}").fetchall()
            # Normalize each row to sortable form: (type_tag, str_repr) per cell
            def _norm_cell(v: object) -> tuple:
                if v is None:
                    return (0, "")
                if isinstance(v, int):
                    return (1, str(v))
                if isinstance(v, float):
                    return (2, repr(v))
                if isinstance(v, str):
                    return (3, v)
                if isinstance(v, bytes):
                    return (4, v.hex())
                return (5, repr(v))
            normalized = [tuple(_norm_cell(c) for c in row) for row in rows]
            normalized.sort()
            return tuple(tuple(row) for row in rows)
        except Exception:
            return None


_IMMUTABLE_TABLES = [
    "v4_run_results",
    "commitment_classifier_snapshots",
    "pattern_families",
    "commitments",
    "commitment_expense_links",
    "commitment_occurrences",
    "commitment_installment_meta",
    "commitment_suggestions",
    "commitment_link_conflicts",
    "commitment_link_events",
    "expenses",
    "installments",
]


def _snapshot_all_immutable(conn: sqlite3.Connection) -> dict[str, Optional[tuple]]:
    return {t: _table_snapshot(conn, t) for t in _IMMUTABLE_TABLES}


# ── Core implementation ───────────────────────────────────────────────────────

def persist_family_review_authority(
    conn: sqlite3.Connection,
    analysis_result: ClassificationReport,
    persistence_report: PersistenceReport,
    link_report: LinkReport,
    overrides: list[PatternOverride],
    *,
    user_id: int,
) -> AuthorityReport:
    """
    Persist already-approved Family Review decisions into commitment_authority.

    Operates entirely inside a single SAVEPOINT (opened before preflight reads).
    All preflight reads + consistency checks + mutation planning happen before
    any writes.  Any hard error aborts with full rollback.

    Returns AuthorityReport with per-override outcomes.
    Raises RuntimeError on any hard error (no committed AuthorityReport).
    """
    conn.execute("SAVEPOINT sp_phase2c")
    try:
        report = _phase2c_inner(
            conn, analysis_result, persistence_report, link_report, overrides,
            user_id=user_id,
        )
        conn.execute("RELEASE SAVEPOINT sp_phase2c")
        return report
    except Exception:
        conn.execute("ROLLBACK TO SAVEPOINT sp_phase2c")
        conn.execute("RELEASE SAVEPOINT sp_phase2c")
        raise


def _phase2c_inner(
    conn: sqlite3.Connection,
    analysis_result: ClassificationReport,
    persistence_report: PersistenceReport,
    link_report: LinkReport,
    overrides: list[PatternOverride],
    *,
    user_id: int,
) -> AuthorityReport:

    # ── Step 1: User / run consistency ────────────────────────────────────────
    if user_id != persistence_report.user_id:
        raise ValueError(
            f"user_id mismatch: caller={user_id!r}, "
            f"persistence_report.user_id={persistence_report.user_id!r}"
        )
    if user_id != link_report.user_id:
        raise ValueError(
            f"user_id mismatch: caller={user_id!r}, "
            f"link_report.user_id={link_report.user_id!r}"
        )
    if persistence_report.run_id != link_report.run_id:
        raise ValueError(
            f"run_id mismatch: persistence_report.run_id={persistence_report.run_id!r}, "
            f"link_report.run_id={link_report.run_id!r}"
        )
    # R1 != R2 is valid — analysis_result.run_id may differ from persistence_report.run_id

    # ── Step 2: Build override lookup (duplicate check) ───────────────────────
    override_by_id: dict[str, PatternOverride] = {}
    for ov in overrides:
        if ov.override_id in override_by_id:
            raise ValueError(
                f"Duplicate override_id in overrides list: {ov.override_id!r}"
            )
        override_by_id[ov.override_id] = ov

    # ── Step 3: Build audit cardinality index ─────────────────────────────────
    # override_id -> list of audit entries that mention it
    oid_to_audit_entries: dict[str, list[dict]] = {}
    for entry in analysis_result.override_audit:
        for oid in entry.get("override_ids_applied", []):
            oid_to_audit_entries.setdefault(oid, []).append(entry)

    # ── Step 4: Bidirectional applied / audit consistency ─────────────────────
    applied_set = set(analysis_result.effective.overrides_applied)

    # Direction 1: audit IDs -> applied set
    audit_oids: set[str] = set()
    for entry in analysis_result.override_audit:
        for oid in entry.get("override_ids_applied", []):
            audit_oids.add(oid)
    orphaned = audit_oids - applied_set
    if orphaned:
        raise ValueError(
            f"override_audit references override_ids not in "
            f"effective.overrides_applied: {sorted(orphaned)}"
        )

    # Direction 2: applied IDs -> audit (per-override; handled in Step 6 cardinality check)
    # Also validate each applied ID exists in overrides list
    for oid in analysis_result.effective.overrides_applied:
        if oid not in override_by_id:
            raise KeyError(
                f"Applied override_id {oid!r} not found in overrides list. "
                "Phase 2C called with different overrides than were used for analysis."
            )

    # ── Step 5: Build raw stream index (mirrors Phase 2B _correlate) ──────────
    # Use raw.patterns, group by description_key, preserving source ordering
    by_key: dict[str, list] = {}
    for p in analysis_result.raw.patterns:
        by_key.setdefault(p.description_key, []).append(p)

    # Compute run_result_id for each (dkey, stream_index)
    rrid_for: dict[tuple[str, int], str] = {}
    for dkey, streams in by_key.items():
        for si in range(len(streams)):
            rrid_for[(dkey, si)] = _run_result_id(
                persistence_report.run_id, dkey, si
            )

    # Build RunResultOutcome lookup by run_result_id
    outcome_by_rrid: dict[str, RunResultOutcome] = {
        o.run_result_id: o for o in persistence_report.outcomes
    }

    # Build PatternLinkResult lookup by run_result_id
    link_by_rrid: dict[str, PatternLinkResult] = {
        r.run_result_id: r for r in link_report.results
    }

    # ── Step 6: Classify each applied override → planned operation ────────────

    @dataclass
    class _PlannedOp:
        override_id:        str
        description_key:    str
        field_name:         str
        commitment_id:      Optional[str]
        op_type:            str          # "INSERT", "ALREADY_CURRENT", "REPLACE", "DEFER", "DEFER_*"
        serialized_value:   Optional[str]
        old_row_id:         Optional[int]
        old_serialized_val: Optional[str]
        defer_outcome:      Optional[Phase2COutcome]
        reason:             Optional[str]

    planned: list[_PlannedOp] = []
    now_iso = _now_iso()

    for oid in analysis_result.effective.overrides_applied:
        ov = override_by_id[oid]

        # ── Cardinality check ─────────────────────────────────────────────────
        entries = oid_to_audit_entries.get(oid, [])
        if len(entries) == 0:
            raise ValueError(
                f"APPLIED_OVERRIDE_AUDIT_MISSING: override_id={oid!r} is in "
                f"effective.overrides_applied but has no audit entries in override_audit."
            )

        dkeys_in_audit = {e["description_key"] for e in entries}
        if len(dkeys_in_audit) > 1:
            raise ValueError(
                f"SOURCE_TARGET_CARDINALITY_DRIFT: override_id={oid!r} appears in "
                f"audit entries for multiple description_keys: {sorted(dkeys_in_audit)!r}. "
                "Expected single target."
            )

        dkey = next(iter(dkeys_in_audit))

        # ── Audit field+value tamper validation ───────────────────────────────
        for entry in entries:
            changed_fields = entry.get("changed_fields", {})
            for fn, rec in changed_fields.items():
                if rec.get("override_id") != oid:
                    continue
                if fn != ov.field_name:
                    raise ValueError(
                        f"OVERRIDE_AUDIT_VALUE_DRIFT: override_id={oid!r}: "
                        f"audit records field_name={fn!r} but PatternOverride "
                        f"has field_name={ov.field_name!r}."
                    )
                expected_audit_val = _expected_audit_override_str(ov)
                actual_audit_val   = rec.get("override")
                if expected_audit_val != actual_audit_val:
                    raise ValueError(
                        f"OVERRIDE_AUDIT_VALUE_DRIFT: override_id={oid!r} "
                        f"field_name={fn!r}: audit recorded {actual_audit_val!r}, "
                        f"current PatternOverride gives {expected_audit_val!r}."
                    )

        # ── Serialization (validates type) ────────────────────────────────────
        serialized_value = _serialize_value(ov.field_name, ov.value)

        # ── Target identity resolution ────────────────────────────────────────
        streams = by_key.get(dkey, [])
        if len(streams) == 0:
            raise ValueError(
                f"PERSISTED_IDENTITY_MISSING: override_id={oid!r} audit maps "
                f"to description_key={dkey!r}, but no raw stream found for this key."
            )

        # For parallel streams, all share the same description_key;
        # check RunResultOutcome for UNRESOLVED_PARALLEL
        parallel_detected = False
        for si in range(len(streams)):
            rrid = rrid_for.get((dkey, si))
            if rrid is None:
                raise ValueError(
                    f"PERSISTED_IDENTITY_MISSING: no rrid computed for "
                    f"(dkey={dkey!r}, stream_index={si})."
                )
            run_result = outcome_by_rrid.get(rrid)
            if run_result is None:
                raise ValueError(
                    f"PERSISTED_IDENTITY_MISSING: override_id={oid!r}: "
                    f"no RunResultOutcome for run_result_id={rrid!r} "
                    f"(dkey={dkey!r}, stream_index={si}, "
                    f"persistence_report.run_id={persistence_report.run_id!r}). "
                    "Ensure PersistenceReport matches this ClassificationReport."
                )
            if run_result.family_resolution == FamilyResolution.UNRESOLVED_PARALLEL:
                parallel_detected = True
                break

        if parallel_detected:
            planned.append(_PlannedOp(
                override_id=oid, description_key=dkey, field_name=ov.field_name,
                commitment_id=None, op_type="DEFER",
                serialized_value=serialized_value,
                old_row_id=None, old_serialized_val=None,
                defer_outcome=Phase2COutcome.DEFER_PARALLEL_UNRESOLVED,
                reason="family_resolution=UNRESOLVED_PARALLEL",
            ))
            continue

        # Single stream path (non-parallel)
        si = 0
        rrid = rrid_for[(dkey, 0)]
        run_result = outcome_by_rrid[rrid]
        link_result = link_by_rrid.get(rrid)
        if link_result is None:
            raise ValueError(
                f"PERSISTED_IDENTITY_MISSING: override_id={oid!r}: "
                f"no PatternLinkResult for run_result_id={rrid!r}. "
                "Ensure LinkReport matches this PersistenceReport."
            )

        # Cross-user safety
        if run_result.family_id is not None:
            family_row = conn.execute(
                "SELECT user_id FROM pattern_families WHERE id = ?",
                (run_result.family_id,)
            ).fetchone()
            if family_row and family_row[0] != user_id:
                raise ValueError(
                    f"Cross-user family: override_id={oid!r} family_id={run_result.family_id!r} "
                    f"belongs to user_id={family_row[0]!r}, not caller user_id={user_id!r}."
                )

        # Canonical merge defer (checked after PARALLEL is excluded)
        if _is_canonical_merge_defer(ov):
            planned.append(_PlannedOp(
                override_id=oid, description_key=dkey, field_name=ov.field_name,
                commitment_id=None, op_type="DEFER",
                serialized_value=serialized_value,
                old_row_id=None, old_serialized_val=None,
                defer_outcome=Phase2COutcome.DEFER_CANONICAL_MERGE,
                reason="canonical_identity=google-cloud-tbd; merging deferred to Phase 3",
            ))
            continue

        # Superseded
        if link_result.outcome == LinkOutcome.SKIPPED_SUPERSEDED:
            planned.append(_PlannedOp(
                override_id=oid, description_key=dkey, field_name=ov.field_name,
                commitment_id=None, op_type="DEFER",
                serialized_value=serialized_value,
                old_row_id=None, old_serialized_val=None,
                defer_outcome=Phase2COutcome.DEFER_SUPERSEDED,
                reason="Phase 2B link outcome=SKIPPED_SUPERSEDED",
            ))
            continue

        # No linked commitment
        commitment_id = link_result.commitment_id
        if commitment_id is None:
            planned.append(_PlannedOp(
                override_id=oid, description_key=dkey, field_name=ov.field_name,
                commitment_id=None, op_type="DEFER",
                serialized_value=serialized_value,
                old_row_id=None, old_serialized_val=None,
                defer_outcome=Phase2COutcome.DEFER_NO_LINKED_COMMITMENT,
                reason="family exists but commitment_id is NULL; Phase 2B did not link",
            ))
            continue

        # Cross-user commitment safety
        cmt_row = conn.execute(
            "SELECT user_id FROM commitments WHERE id = ?",
            (commitment_id,)
        ).fetchone()
        if cmt_row and cmt_row[0] != user_id:
            raise ValueError(
                f"Cross-user commitment: override_id={oid!r} "
                f"commitment_id={commitment_id!r} belongs to user_id={cmt_row[0]!r}, "
                f"not caller user_id={user_id!r}."
            )

        # ── Existing authority pre-flight ─────────────────────────────────────
        # Read all ACTIVE FAMILY_REVIEW rows for this (user, commitment)
        active_rows = conn.execute("""
            SELECT id, override_id, field_name, value
            FROM commitment_authority
            WHERE user_id          = ?
              AND commitment_id    = ?
              AND is_active        = 1
              AND authority_source = 'FAMILY_REVIEW'
        """, (user_id, commitment_id)).fetchall()

        rows_by_override: dict[str, dict] = {}
        rows_by_field: dict[str, dict] = {}
        for r in active_rows:
            row_dict = {"id": r[0], "override_id": r[1], "field_name": r[2], "value": r[3]}
            rows_by_override[r[1]] = row_dict
            rows_by_field[r[2]] = row_dict

        # TARGET_IDENTITY_DRIFT: same override active on different commitment
        cross_cmt = conn.execute("""
            SELECT id, commitment_id FROM commitment_authority
            WHERE user_id          = ?
              AND override_id      = ?
              AND is_active        = 1
              AND commitment_id   != ?
        """, (user_id, oid, commitment_id)).fetchone()
        if cross_cmt is not None:
            raise ValueError(
                f"TARGET_IDENTITY_DRIFT: override_id={oid!r} is currently active on "
                f"commitment_id={cross_cmt[1]!r} (row id={cross_cmt[0]}), but current "
                f"run resolves to commitment_id={commitment_id!r}."
            )

        existing_same_override = rows_by_override.get(oid)

        if existing_same_override is None:
            # Check ACTIVE_FIELD_SOURCE_CONFLICT: different FAMILY_REVIEW override for same field
            existing_same_field = rows_by_field.get(ov.field_name)
            if existing_same_field is not None:
                raise ValueError(
                    f"ACTIVE_FIELD_SOURCE_CONFLICT: commitment_id={commitment_id!r} "
                    f"field_name={ov.field_name!r} already has ACTIVE FAMILY_REVIEW authority "
                    f"from override_id={existing_same_field['override_id']!r} "
                    f"(row id={existing_same_field['id']}). "
                    f"Cannot insert override_id={oid!r} without revoking the prior decision."
                )
            # Case A: INSERTED
            planned.append(_PlannedOp(
                override_id=oid, description_key=dkey, field_name=ov.field_name,
                commitment_id=commitment_id, op_type="INSERT",
                serialized_value=serialized_value,
                old_row_id=None, old_serialized_val=None,
                defer_outcome=None, reason=None,
            ))
        else:
            # existing row with same override_id
            if existing_same_override["field_name"] != ov.field_name:
                raise ValueError(
                    f"SOURCE_IDENTITY_DRIFT: override_id={oid!r} is active on "
                    f"commitment={commitment_id!r} with field_name="
                    f"{existing_same_override['field_name']!r}, but current Phase 2C "
                    f"expects field_name={ov.field_name!r}."
                )
            existing_val = existing_same_override["value"]
            if existing_val == serialized_value:
                # Case B: ALREADY_CURRENT
                planned.append(_PlannedOp(
                    override_id=oid, description_key=dkey, field_name=ov.field_name,
                    commitment_id=commitment_id, op_type="ALREADY_CURRENT",
                    serialized_value=serialized_value,
                    old_row_id=existing_same_override["id"], old_serialized_val=existing_val,
                    defer_outcome=None, reason="active row matches; no write needed",
                ))
            else:
                # Case C: REPLACED
                planned.append(_PlannedOp(
                    override_id=oid, description_key=dkey, field_name=ov.field_name,
                    commitment_id=commitment_id, op_type="REPLACE",
                    serialized_value=serialized_value,
                    old_row_id=existing_same_override["id"], old_serialized_val=existing_val,
                    defer_outcome=None,
                    reason=f"value updated from {existing_val!r} to {serialized_value!r}",
                ))

    # ── Step 7: Execute mutation plan ─────────────────────────────────────────
    report = AuthorityReport(run_id=persistence_report.run_id, user_id=user_id)

    for op in planned:
        if op.op_type == "DEFER":
            report.outcomes.append(Phase2CResult(
                override_id=op.override_id,
                description_key=op.description_key,
                field_name=op.field_name,
                commitment_id=op.commitment_id,
                outcome=op.defer_outcome,  # type: ignore[arg-type]
                reason=op.reason,
                previous_row_id=None,
                new_row_id=None,
            ))

        elif op.op_type == "ALREADY_CURRENT":
            report.outcomes.append(Phase2CResult(
                override_id=op.override_id,
                description_key=op.description_key,
                field_name=op.field_name,
                commitment_id=op.commitment_id,
                outcome=Phase2COutcome.ALREADY_CURRENT,
                reason=op.reason,
                previous_row_id=op.old_row_id,
                new_row_id=None,
            ))

        elif op.op_type == "INSERT":
            conn.execute("""
                INSERT INTO commitment_authority
                    (commitment_id, user_id, field_name, value,
                     authority_source, override_id,
                     is_active, created_at, created_by,
                     revoked_at, revoked_by)
                VALUES (?, ?, ?, ?,
                        'FAMILY_REVIEW', ?,
                        1, ?, ?,
                        NULL, NULL)
            """, (
                op.commitment_id, user_id, op.field_name, op.serialized_value,
                op.override_id, now_iso, user_id,
            ))
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            report.outcomes.append(Phase2CResult(
                override_id=op.override_id,
                description_key=op.description_key,
                field_name=op.field_name,
                commitment_id=op.commitment_id,
                outcome=Phase2COutcome.INSERTED,
                reason=None,
                previous_row_id=None,
                new_row_id=new_id,
            ))

        elif op.op_type == "REPLACE":
            cursor = conn.execute("""
                UPDATE commitment_authority
                SET is_active  = 0,
                    revoked_at = ?,
                    revoked_by = NULL
                WHERE id               = ?
                  AND user_id          = ?
                  AND commitment_id    = ?
                  AND override_id      = ?
                  AND field_name       = ?
                  AND authority_source = 'FAMILY_REVIEW'
                  AND is_active        = 1
                  AND value IS ?
            """, (
                now_iso,
                op.old_row_id, user_id, op.commitment_id,
                op.override_id, op.field_name,
                op.old_serialized_val,
            ))
            if cursor.rowcount != 1:
                raise RuntimeError(
                    f"AUTHORITY_STATE_CHANGED_DURING_WRITE: "
                    f"override_id={op.override_id!r} commitment_id={op.commitment_id!r} "
                    f"field_name={op.field_name!r}: revoke UPDATE matched "
                    f"{cursor.rowcount} rows (expected 1). Concurrent modification. "
                    "Phase 2C call is rolling back."
                )
            conn.execute("""
                INSERT INTO commitment_authority
                    (commitment_id, user_id, field_name, value,
                     authority_source, override_id,
                     is_active, created_at, created_by,
                     revoked_at, revoked_by)
                VALUES (?, ?, ?, ?,
                        'FAMILY_REVIEW', ?,
                        1, ?, ?,
                        NULL, NULL)
            """, (
                op.commitment_id, user_id, op.field_name, op.serialized_value,
                op.override_id, now_iso, user_id,
            ))
            new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            report.outcomes.append(Phase2CResult(
                override_id=op.override_id,
                description_key=op.description_key,
                field_name=op.field_name,
                commitment_id=op.commitment_id,
                outcome=Phase2COutcome.REPLACED,
                reason=op.reason,
                previous_row_id=op.old_row_id,
                new_row_id=new_id,
            ))

    return report


# ── Public path-based API ─────────────────────────────────────────────────────

def persist_family_review_authority_from_path(
    db_path: str,
    analysis_result: ClassificationReport,
    persistence_report: PersistenceReport,
    link_report: LinkReport,
    overrides: list[PatternOverride],
    *,
    user_id: int,
) -> AuthorityReport:
    """
    Path-based convenience wrapper.  Enforces production path guard before
    opening a connection, then delegates to persist_family_review_authority.
    """
    if _is_production_path(db_path):
        raise RuntimeError(
            f"PRODUCTION SAFETY ABORT: refusing to write to production DB: {db_path!r}"
        )
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    try:
        report = persist_family_review_authority(
            conn, analysis_result, persistence_report, link_report, overrides,
            user_id=user_id,
        )
        conn.commit()
        return report
    finally:
        conn.close()
