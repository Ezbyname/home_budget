"""
Unified Commitments — Phase 2D1
Authority Read / Resolution / Precedence

Reads active commitment_authority rows for the current analysis run and resolves
which source wins for each (commitment_id, field_name) pair, producing typed
AuthorityFieldResolution records.

PURPOSE:
    Phase 2D1 is read-only and current-run-scoped.
    It resolves precedence but does NOT apply resolved values to runtime
    financial output.  That is Phase 2D2.

NEVER writes:
    commitment_authority, v4_run_results, commitments, pattern_families,
    or any other table.

PRECEDENCE:
    MANUAL_OVERRIDE > FAMILY_REVIEW > baseline (no active authority)

Production DB guard: inherited convention from Phase 2A/2B/2C.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional

from intelligence.v4_contracts import (
    Cadence,
    CommitmentStatus,
    LifecycleStatus,
    PurposeType,
    RecurrenceStatus,
)
from v4_linking import LinkOutcome, LinkReport, PatternLinkResult
from v4_persistence import PersistenceReport, _is_production_path


# ── Supported authority field names ──────────────────────────────────────────

_SUPPORTED_FIELDS: frozenset[str] = frozenset({
    "recurrence_status",
    "commitment_status",
    "lifecycle_status",
    "cadence",
    "purpose_type",
    "planning_amount",
})


# ── Authority source precedence ───────────────────────────────────────────────
# Higher number = higher precedence.

_SOURCE_PRECEDENCE: dict[str, int] = {
    "FAMILY_REVIEW":    1,
    "MANUAL_OVERRIDE":  2,
}


# ── Outcome taxonomy ──────────────────────────────────────────────────────────

class AuthorityOutcome(str, Enum):
    BASELINE_NO_AUTHORITY       = "BASELINE_NO_AUTHORITY"
    APPLIED_FAMILY_REVIEW       = "APPLIED_FAMILY_REVIEW"
    APPLIED_MANUAL_OVERRIDE     = "APPLIED_MANUAL_OVERRIDE"
    DEFER_SUPERSEDED            = "DEFER_SUPERSEDED"
    DEFER_NO_LINKED_COMMITMENT  = "DEFER_NO_LINKED_COMMITMENT"
    DEFER_PARALLEL_UNRESOLVED   = "DEFER_PARALLEL_UNRESOLVED"


# ── Per-field resolution record ───────────────────────────────────────────────

@dataclass
class AuthorityFieldResolution:
    user_id:              int
    commitment_id:        str
    run_result_id:        str
    description_key:      str
    field_name:           str

    baseline_value:       object         # raw PatternResult field value (untyped)

    family_review_row_id: Optional[int]
    family_review_value:  object         # deserialized or None

    manual_override_row_id: Optional[int]
    manual_override_value:  object       # deserialized or None

    winning_source:       Optional[str]  # "FAMILY_REVIEW" | "MANUAL_OVERRIDE" | None
    resolved_value:       object         # winning value, or baseline_value if no authority

    override_id:          Optional[str]  # override_id of winning row
    outcome:              AuthorityOutcome
    reason:               Optional[str]  = None


# ── Per-pattern skip record ───────────────────────────────────────────────────

@dataclass
class AuthoritySkip:
    run_result_id:  str
    description_key: str
    stream_index:   int
    outcome:        AuthorityOutcome
    reason:         str


# ── Top-level resolution report ───────────────────────────────────────────────

@dataclass
class AuthorityResolutionReport:
    run_id:      str
    user_id:     int
    resolutions: list[AuthorityFieldResolution] = field(default_factory=list)
    skips:       list[AuthoritySkip]            = field(default_factory=list)

    def by_commitment(self) -> dict[str, list[AuthorityFieldResolution]]:
        from collections import defaultdict
        d: dict[str, list[AuthorityFieldResolution]] = defaultdict(list)
        for r in self.resolutions:
            d[r.commitment_id].append(r)
        return dict(d)

    def applied_count(self) -> int:
        return sum(
            1 for r in self.resolutions
            if r.outcome in (
                AuthorityOutcome.APPLIED_FAMILY_REVIEW,
                AuthorityOutcome.APPLIED_MANUAL_OVERRIDE,
            )
        )

    def baseline_count(self) -> int:
        return sum(
            1 for r in self.resolutions
            if r.outcome == AuthorityOutcome.BASELINE_NO_AUTHORITY
        )


# ── Deserialization ───────────────────────────────────────────────────────────

def _deserialize_authority_value(field_name: str, raw: Optional[str]) -> object:
    """
    Deserialize a TEXT value from commitment_authority.value to its typed form.

    Raises ValueError on unknown field_name, invalid enum member, or invalid
    Decimal.  Never uses float.  None is only valid for planning_amount.
    """
    if field_name not in _SUPPORTED_FIELDS:
        raise ValueError(
            f"UNKNOWN_AUTHORITY_FIELD: {field_name!r} is not a supported authority "
            f"field. Supported: {sorted(_SUPPORTED_FIELDS)}"
        )

    if field_name == "planning_amount":
        if raw is None:
            return None
        try:
            return Decimal(raw)
        except InvalidOperation:
            raise ValueError(
                f"INVALID_DECIMAL for planning_amount: {raw!r}"
            )

    # All remaining fields are str-Enum types; None is not valid here.
    if raw is None:
        raise ValueError(
            f"NULL_VALUE for non-nullable field {field_name!r}"
        )

    _enum_map: dict[str, type] = {
        "recurrence_status": RecurrenceStatus,
        "commitment_status": CommitmentStatus,
        "lifecycle_status":  LifecycleStatus,
        "cadence":           Cadence,
        "purpose_type":      PurposeType,
    }
    enum_cls = _enum_map[field_name]
    try:
        return enum_cls(raw)
    except ValueError:
        raise ValueError(
            f"INVALID_ENUM for {field_name!r}: {raw!r} is not a valid "
            f"{enum_cls.__name__} value. Valid: {[e.value for e in enum_cls]}"
        )


# ── Active row fetch (batch) ──────────────────────────────────────────────────

_CHUNK_SIZE = 200  # safe SQLite IN-clause limit


@dataclass
class _ActiveRow:
    rowid:            int
    commitment_id:    str
    field_name:       str
    value:            Optional[str]
    authority_source: str
    override_id:      str


def _fetch_active_authority(
    conn: sqlite3.Connection,
    user_id: int,
    commitment_ids: list[str],
) -> list[_ActiveRow]:
    """
    Batch-fetch all active authority rows for the given commitment_ids / user_id.
    Chunks the IN-clause to stay within SQLite's variable limit.
    """
    rows: list[_ActiveRow] = []
    ids = list(commitment_ids)
    for start in range(0, max(len(ids), 1), _CHUNK_SIZE):
        chunk = ids[start : start + _CHUNK_SIZE]
        if not chunk:
            break
        placeholders = ",".join("?" * len(chunk))
        sql = (
            "SELECT id, commitment_id, field_name, value, authority_source, override_id "
            "FROM commitment_authority "
            f"WHERE user_id = ? AND is_active = 1 AND commitment_id IN ({placeholders})"
        )
        for raw in conn.execute(sql, [user_id, *chunk]).fetchall():
            rows.append(_ActiveRow(
                rowid=raw[0],
                commitment_id=raw[1],
                field_name=raw[2],
                value=raw[3],
                authority_source=raw[4],
                override_id=raw[5],
            ))
    return rows


# ── Core resolver ─────────────────────────────────────────────────────────────

def resolve_authority_from_reports(
    conn: sqlite3.Connection,
    persistence_report: PersistenceReport,
    link_report: LinkReport,
    *,
    user_id: int,
    baseline_patterns: Optional[dict[str, object]] = None,
) -> AuthorityResolutionReport:
    """
    Resolve active authority for all current-run commitments.

    Steps:
    1. Validate cross-report consistency (user_id, run_id).
    2. Derive exact commitment_ids from LinkReport (LINKED / ALREADY_LINKED only).
    3. Batch-fetch all active authority rows for those commitments.
    4. Validate cardinality: no duplicate active (commitment_id, field_name,
       authority_source) — hard error DUPLICATE_ACTIVE_SOURCE_AUTHORITY.
    5. Validate field_name is supported — hard error on unknown active field.
    6. Deserialize values — hard error on invalid enum / Decimal.
    7. Resolve precedence per (commitment_id, field_name).
    8. Emit AuthorityFieldResolution for each (commitment_id, field_name) that
       has at least one active authority row.
    9. Record AuthoritySkip for SKIPPED_SUPERSEDED / no linked commitment.

    Parameters:
        conn               — open SQLite connection (read-only operations only)
        persistence_report — from persist_run(); provides run identity
        link_report        — from link_phase2b(); provides commitment_id per run_result
        user_id            — must match both reports
        baseline_patterns  — optional dict keyed by description_key with PatternResult-like
                             objects; used to populate baseline_value in resolutions.
                             May be None (baseline_value will be None for all resolutions).

    Raises:
        ValueError on cross-report mismatch, duplicate active authority, unknown
        field, or deserialization failure.
    """
    # ── Step 1: Cross-report consistency ─────────────────────────────────────
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

    report = AuthorityResolutionReport(
        run_id=persistence_report.run_id,
        user_id=user_id,
    )

    # ── Step 2: Classify each link result ────────────────────────────────────
    # rrid → PatternLinkResult
    link_by_rrid: dict[str, PatternLinkResult] = {
        r.run_result_id: r for r in link_report.results
    }

    # Collect commitment_ids for batch read; record skips for non-linked
    linked_rrids: list[str] = []         # rrids with a real commitment_id
    commitment_id_for: dict[str, str] = {}  # rrid → commitment_id

    for lr in link_report.results:
        if lr.outcome == LinkOutcome.SKIPPED_SUPERSEDED:
            report.skips.append(AuthoritySkip(
                run_result_id=lr.run_result_id,
                description_key=lr.description_key,
                stream_index=lr.stream_index,
                outcome=AuthorityOutcome.DEFER_SUPERSEDED,
                reason="link_outcome=SKIPPED_SUPERSEDED",
            ))
        elif lr.commitment_id is None:
            report.skips.append(AuthoritySkip(
                run_result_id=lr.run_result_id,
                description_key=lr.description_key,
                stream_index=lr.stream_index,
                outcome=AuthorityOutcome.DEFER_NO_LINKED_COMMITMENT,
                reason=f"link_outcome={lr.outcome.value}, commitment_id=None",
            ))
        else:
            linked_rrids.append(lr.run_result_id)
            commitment_id_for[lr.run_result_id] = lr.commitment_id

    if not linked_rrids:
        return report

    # Unique commitment_ids for this run
    unique_cids = list(dict.fromkeys(commitment_id_for.values()))

    # ── Step 3: Batch read under a read-only SAVEPOINT ────────────────────────
    conn.execute("SAVEPOINT sp_phase2d1_read")
    try:
        active_rows = _fetch_active_authority(conn, user_id, unique_cids)
    finally:
        conn.execute("RELEASE SAVEPOINT sp_phase2d1_read")

    # ── Step 4: Group rows by (commitment_id, field_name, authority_source) ───
    # key → list[_ActiveRow]; more than 1 row = hard error
    from collections import defaultdict
    grouped: dict[tuple[str, str, str], list[_ActiveRow]] = defaultdict(list)
    for row in active_rows:
        grouped[(row.commitment_id, row.field_name, row.authority_source)].append(row)

    # Cardinality check
    for (cid, fn, src), rows in grouped.items():
        if len(rows) > 1:
            raise ValueError(
                f"DUPLICATE_ACTIVE_SOURCE_AUTHORITY: commitment_id={cid!r}, "
                f"field_name={fn!r}, authority_source={src!r} has "
                f"{len(rows)} active rows (ids: {[r.rowid for r in rows]}). "
                "Schema constraint violation — do not silently select one."
            )

    # ── Step 5+6: Validate field_name + deserialize ───────────────────────────
    # Build per-(commitment_id, field_name) → {source: (rowid, deserialized, override_id)}
    resolved_by: dict[tuple[str, str], dict[str, tuple[int, object, str]]] = defaultdict(dict)
    for (cid, fn, src), rows in grouped.items():
        if fn not in _SUPPORTED_FIELDS:
            raise ValueError(
                f"UNKNOWN_ACTIVE_AUTHORITY_FIELD: commitment_id={cid!r} has active "
                f"authority for unsupported field {fn!r}. "
                f"Supported: {sorted(_SUPPORTED_FIELDS)}"
            )
        row = rows[0]
        typed_value = _deserialize_authority_value(fn, row.value)
        resolved_by[(cid, fn)][src] = (row.rowid, typed_value, row.override_id)

    # ── Step 7: Emit resolutions for fields that have authority ──────────────
    # For each (rrid, cid) pair, emit per-field resolutions
    # Build rrid → description_key from link_report
    dkey_for_rrid: dict[str, str] = {
        lr.run_result_id: lr.description_key for lr in link_report.results
    }

    # Collect fields that have authority per commitment_id (avoid N² iteration)
    fields_for_cid: dict[str, set[str]] = defaultdict(set)
    for (cid, fn) in resolved_by:
        fields_for_cid[cid].add(fn)

    # For each linked rrid, emit one AuthorityFieldResolution per active field
    seen: set[tuple[str, str]] = set()  # (cid, fn) already emitted
    for rrid in linked_rrids:
        cid = commitment_id_for[rrid]
        dkey = dkey_for_rrid.get(rrid, "")

        # baseline_value lookup
        baseline_pattern = (
            baseline_patterns.get(dkey) if baseline_patterns else None
        )

        for fn in fields_for_cid.get(cid, set()):
            pair = (cid, fn)
            if pair in seen:
                continue
            seen.add(pair)

            sources = resolved_by[pair]
            baseline_val = (
                getattr(baseline_pattern, fn, None)
                if baseline_pattern is not None else None
            )

            fr_rowid, fr_value, fr_oid = sources.get("FAMILY_REVIEW", (None, None, None))
            mo_rowid, mo_value, mo_oid = sources.get("MANUAL_OVERRIDE", (None, None, None))

            # Precedence resolution
            if mo_rowid is not None:
                winning_source = "MANUAL_OVERRIDE"
                resolved_value = mo_value
                winning_oid    = mo_oid
                outcome = AuthorityOutcome.APPLIED_MANUAL_OVERRIDE
            elif fr_rowid is not None:
                winning_source = "FAMILY_REVIEW"
                resolved_value = fr_value
                winning_oid    = fr_oid
                outcome = AuthorityOutcome.APPLIED_FAMILY_REVIEW
            else:
                winning_source = None
                resolved_value = baseline_val
                winning_oid    = None
                outcome = AuthorityOutcome.BASELINE_NO_AUTHORITY

            report.resolutions.append(AuthorityFieldResolution(
                user_id=user_id,
                commitment_id=cid,
                run_result_id=rrid,
                description_key=dkey,
                field_name=fn,
                baseline_value=baseline_val,
                family_review_row_id=fr_rowid,
                family_review_value=fr_value,
                manual_override_row_id=mo_rowid,
                manual_override_value=mo_value,
                winning_source=winning_source,
                resolved_value=resolved_value,
                override_id=winning_oid,
                outcome=outcome,
            ))

    return report
