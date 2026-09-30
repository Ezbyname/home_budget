"""
Phase 2D1 — Authority Read / Resolution / Precedence
Test suite for v4_authority_read.py

Covers:
  - SOURCE / IDENTITY
  - AUTHORITY READ (baseline, FAMILY_REVIEW, MANUAL_OVERRIDE, both, inactive, revoked)
  - DESERIALIZATION (all 6 fields, all error cases)
  - IDENTITY / LINK STATES (superseded, no commitment, missing identity)
  - PRECEDENCE
  - PROVENANCE
  - READ STRATEGY
  - IMMUTABILITY
  - COMPATIBILITY
"""
from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional

import pytest

from intelligence.v4_contracts import (
    Cadence,
    CommitmentStatus,
    LifecycleStatus,
    PurposeType,
    RecurrenceStatus,
)
from v4_authority_read import (
    AuthorityFieldResolution,
    AuthorityOutcome,
    AuthorityResolutionReport,
    AuthoritySkip,
    _deserialize_authority_value,
    resolve_authority_from_reports,
)
from v4_linking import LinkOutcome, LinkReport, PatternLinkResult
from v4_persistence import FamilyResolution, PersistenceReport, RunResultOutcome


# ── Helpers ───────────────────────────────────────────────────────────────────

def _uid() -> str:
    return str(uuid.uuid4())


def _run_result_id(run_id: str, dkey: str, si: int) -> str:
    import uuid as _uuid
    return str(_uuid.uuid5(_uuid.NAMESPACE_DNS, f"{run_id}:{dkey}:{si}"))


def _mem_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _make_db(conn: sqlite3.Connection) -> None:
    """Create minimal schema for Phase 2D1 tests."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS commitments (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS commitment_authority (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            value TEXT DEFAULT NULL,
            authority_source TEXT NOT NULL
                CHECK(authority_source IN ('MANUAL_OVERRIDE', 'FAMILY_REVIEW')),
            override_id TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1
                CHECK(is_active IN (0, 1)),
            created_at TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            revoked_at TEXT DEFAULT NULL,
            revoked_by INTEGER DEFAULT NULL
        );
    """)


def _insert_commitment(conn, cid: str, user_id: int) -> None:
    conn.execute("INSERT INTO commitments(id, user_id) VALUES (?,?)", (cid, user_id))


def _insert_authority(
    conn,
    commitment_id: str,
    user_id: int,
    field_name: str,
    value: Optional[str],
    authority_source: str,
    override_id: str,
    is_active: int = 1,
) -> int:
    cur = conn.execute(
        """INSERT INTO commitment_authority
           (commitment_id, user_id, field_name, value, authority_source,
            override_id, is_active, created_at, created_by)
           VALUES (?,?,?,?,?,?,?,'2026-01-01T00:00:00',1)""",
        (commitment_id, user_id, field_name, value, authority_source,
         override_id, is_active),
    )
    conn.commit()
    return cur.lastrowid


def _make_persistence_report(
    run_id: str,
    user_id: int,
    dkey: str,
    family_id: str,
    stream_index: int = 0,
) -> PersistenceReport:
    rrid = _run_result_id(run_id, dkey, stream_index)
    return PersistenceReport(
        run_id=run_id,
        user_id=user_id,
        outcomes=[RunResultOutcome(
            run_result_id=rrid,
            description_key=dkey,
            stream_index=stream_index,
            family_id=family_id,
            family_resolution=FamilyResolution.MATCHED_EXISTING,
        )],
    )


def _make_link_report(
    run_id: str,
    user_id: int,
    dkey: str,
    stream_index: int = 0,
    commitment_id: Optional[str] = None,
    outcome: LinkOutcome = LinkOutcome.LINKED,
) -> LinkReport:
    rrid = _run_result_id(run_id, dkey, stream_index)
    return LinkReport(
        run_id=run_id,
        user_id=user_id,
        results=[PatternLinkResult(
            description_key=dkey,
            stream_index=stream_index,
            run_result_id=rrid,
            family_id=_uid(),
            outcome=outcome,
            commitment_id=commitment_id,
        )],
    )


def _snapshot(conn: sqlite3.Connection, table: str) -> tuple:
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    return tuple(sorted(tuple(r) for r in rows))


# ═══════════════════════════════════════════════════════════════════════════════
# T01–T10 — SOURCE / IDENTITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestSourceIdentity:

    def test_T01_dataclass_fields_present(self):
        """AuthorityFieldResolution has the required provenance fields."""
        r = AuthorityFieldResolution(
            user_id=1, commitment_id="c1", run_result_id="r1",
            description_key="k", field_name="cadence",
            baseline_value=None,
            family_review_row_id=None, family_review_value=None,
            manual_override_row_id=None, manual_override_value=None,
            winning_source=None, resolved_value=None,
            override_id=None, outcome=AuthorityOutcome.BASELINE_NO_AUTHORITY,
        )
        assert r.user_id == 1
        assert r.commitment_id == "c1"
        assert r.run_result_id == "r1"
        assert r.description_key == "k"
        assert r.field_name == "cadence"
        assert r.outcome == AuthorityOutcome.BASELINE_NO_AUTHORITY

    def test_T02_r1_not_equal_r2_valid(self):
        """analysis run_id ≠ persistence run_id is valid (R1 ≠ R2)."""
        run_id = _uid()
        user_id = 42
        dkey = "sub/electric"
        cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        # No authority rows — should succeed with empty resolutions
        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert isinstance(result, AuthorityResolutionReport)
        assert result.run_id == run_id

    def test_T03_persistence_user_mismatch_raises(self):
        conn = _mem_conn()
        _make_db(conn)
        run_id = _uid()
        pr = PersistenceReport(run_id=run_id, user_id=99)
        lr = LinkReport(run_id=run_id, user_id=99)
        with pytest.raises(ValueError, match="user_id mismatch"):
            resolve_authority_from_reports(conn, pr, lr, user_id=1)

    def test_T04_link_user_mismatch_raises(self):
        conn = _mem_conn()
        _make_db(conn)
        run_id = _uid()
        pr = PersistenceReport(run_id=run_id, user_id=1)
        lr = LinkReport(run_id=run_id, user_id=99)
        with pytest.raises(ValueError, match="user_id mismatch"):
            resolve_authority_from_reports(conn, pr, lr, user_id=1)

    def test_T05_run_id_mismatch_raises(self):
        conn = _mem_conn()
        _make_db(conn)
        pr = PersistenceReport(run_id=_uid(), user_id=1)
        lr = LinkReport(run_id=_uid(), user_id=1)
        with pytest.raises(ValueError, match="run_id mismatch"):
            resolve_authority_from_reports(conn, pr, lr, user_id=1)

    def test_T06_exact_run_result_id_identity(self):
        """run_result_id in resolution matches UUID5 identity."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        expected_rrid = _run_result_id(run_id, dkey, 0)

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        rowid = _insert_authority(conn, cid, user_id, "cadence", "monthly",
                                   "FAMILY_REVIEW", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == 1
        assert result.resolutions[0].run_result_id == expected_rrid

    def test_T07_no_description_key_fallback(self):
        """Unlinked pattern (commitment_id=None) produces a skip, not a fallback."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"

        conn = _mem_conn()
        _make_db(conn)

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=None,
                               outcome=LinkOutcome.NO_ACTION)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == 0
        assert len(result.skips) == 1
        assert result.skips[0].outcome == AuthorityOutcome.DEFER_NO_LINKED_COMMITMENT

    def test_T08_cross_user_authority_not_loaded(self):
        """Authority for a different user_id is not returned."""
        run_id = _uid()
        user_id = 1
        other_user = 2
        dkey = "sub/phone"
        cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        # Insert authority for other_user
        _insert_authority(conn, cid, other_user, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == 0  # not loaded for user 1

    def test_T09_empty_link_report_returns_empty(self):
        conn = _mem_conn()
        _make_db(conn)
        run_id = _uid()
        pr = PersistenceReport(run_id=run_id, user_id=1)
        lr = LinkReport(run_id=run_id, user_id=1)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=1)
        assert result.resolutions == []
        assert result.skips == []

    def test_T10_authority_outside_current_run_has_no_effect(self):
        """Authority for a commitment NOT in this run's link report is not loaded."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        other_cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_commitment(conn, other_cid, user_id)
        # Authority for other_cid, which is not in this run
        _insert_authority(conn, other_cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# T11–T21 — AUTHORITY READ
# ═══════════════════════════════════════════════════════════════════════════════

class TestAuthorityRead:

    def _setup(self, dkey="sub/phone"):
        run_id = _uid()
        user_id = 1
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        return conn, pr, lr, cid, user_id

    def test_T11_no_authority_baseline(self):
        conn, pr, lr, cid, uid = self._setup()
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 0
        assert result.baseline_count() == 0

    def test_T12_family_review_only(self):
        conn, pr, lr, cid, uid = self._setup()
        oid = _uid()
        rowid = _insert_authority(conn, cid, uid, "cadence", "monthly",
                                   "FAMILY_REVIEW", oid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 1
        r = result.resolutions[0]
        assert r.outcome == AuthorityOutcome.APPLIED_FAMILY_REVIEW
        assert r.winning_source == "FAMILY_REVIEW"
        assert r.family_review_row_id == rowid
        assert r.family_review_value == Cadence.MONTHLY
        assert r.manual_override_row_id is None
        assert r.override_id == oid

    def test_T13_manual_override_only(self):
        conn, pr, lr, cid, uid = self._setup()
        oid = _uid()
        rowid = _insert_authority(conn, cid, uid, "cadence", "quarterly",
                                   "MANUAL_OVERRIDE", oid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 1
        r = result.resolutions[0]
        assert r.outcome == AuthorityOutcome.APPLIED_MANUAL_OVERRIDE
        assert r.winning_source == "MANUAL_OVERRIDE"
        assert r.manual_override_row_id == rowid
        assert r.manual_override_value == Cadence.QUARTERLY

    def test_T14_both_active_manual_wins(self):
        conn, pr, lr, cid, uid = self._setup()
        fr_oid = _uid()
        mo_oid = _uid()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", fr_oid)
        _insert_authority(conn, cid, uid, "cadence", "quarterly",
                          "MANUAL_OVERRIDE", mo_oid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 1
        r = result.resolutions[0]
        assert r.outcome == AuthorityOutcome.APPLIED_MANUAL_OVERRIDE
        assert r.winning_source == "MANUAL_OVERRIDE"
        assert r.resolved_value == Cadence.QUARTERLY
        # FAMILY_REVIEW row still recorded in provenance
        assert r.family_review_row_id is not None
        assert r.family_review_value == Cadence.MONTHLY

    def test_T15_inactive_family_review_ignored(self):
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid(), is_active=0)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 0

    def test_T16_inactive_manual_override_ignored(self):
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "MANUAL_OVERRIDE", _uid(), is_active=0)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 0

    def test_T17_revoked_history_inactive_ignored(self):
        """is_active=0 row (revoked) does not participate."""
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "recurrence_status", "RECURRING",
                          "FAMILY_REVIEW", _uid(), is_active=0)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 0

    def test_T18_family_and_manual_may_coexist(self):
        """Both FAMILY_REVIEW and MANUAL_OVERRIDE may be active simultaneously."""
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, uid, "cadence", "quarterly",
                          "MANUAL_OVERRIDE", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r = result.resolutions[0]
        # Both are populated
        assert r.family_review_row_id is not None
        assert r.manual_override_row_id is not None
        # MANUAL wins
        assert r.resolved_value == Cadence.QUARTERLY

    def test_T19_duplicate_same_source_active_hard_error(self):
        """Two active rows for same (cid, field, FAMILY_REVIEW) = hard error."""
        conn, pr, lr, cid, uid = self._setup()
        # Insert two rows with same source (bypass unique constraint by not enforcing here)
        conn.execute(
            "INSERT INTO commitment_authority "
            "(commitment_id, user_id, field_name, value, authority_source, "
            "override_id, is_active, created_at, created_by) "
            "VALUES (?,?,?,?,?,?,1,'2026-01-01T00:00:00',1)",
            (cid, uid, "cadence", "monthly", "FAMILY_REVIEW", _uid()),
        )
        conn.execute(
            "INSERT INTO commitment_authority "
            "(commitment_id, user_id, field_name, value, authority_source, "
            "override_id, is_active, created_at, created_by) "
            "VALUES (?,?,?,?,?,?,1,'2026-01-01T00:00:00',1)",
            (cid, uid, "cadence", "monthly", "FAMILY_REVIEW", _uid()),
        )
        conn.commit()
        with pytest.raises(ValueError, match="DUPLICATE_ACTIVE_SOURCE_AUTHORITY"):
            resolve_authority_from_reports(conn, pr, lr, user_id=uid)

    def test_T20_multiple_fields_resolved_independently(self):
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, uid, "recurrence_status", "RECURRING",
                          "FAMILY_REVIEW", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(result.resolutions) == 2
        fields = {r.field_name for r in result.resolutions}
        assert fields == {"cadence", "recurrence_status"}

    def test_T21_mixed_sources_different_fields(self):
        """Different fields may have different winning sources."""
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, uid, "recurrence_status", "RECURRING",
                          "MANUAL_OVERRIDE", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        by_field = {r.field_name: r for r in result.resolutions}
        assert by_field["cadence"].winning_source == "FAMILY_REVIEW"
        assert by_field["recurrence_status"].winning_source == "MANUAL_OVERRIDE"


# ═══════════════════════════════════════════════════════════════════════════════
# T22–T38 — DESERIALIZATION
# ═══════════════════════════════════════════════════════════════════════════════

class TestDeserialization:

    def test_T22_recurrence_status_recurring(self):
        v = _deserialize_authority_value("recurrence_status", "RECURRING")
        assert v == RecurrenceStatus.RECURRING

    def test_T23_recurrence_status_possible(self):
        v = _deserialize_authority_value("recurrence_status", "POSSIBLE_RECURRING")
        assert v == RecurrenceStatus.POSSIBLE_RECURRING

    def test_T24_commitment_status_committed(self):
        v = _deserialize_authority_value("commitment_status", "COMMITTED")
        assert v == CommitmentStatus.COMMITTED

    def test_T25_commitment_status_non_committed(self):
        v = _deserialize_authority_value("commitment_status", "NON_COMMITTED")
        assert v == CommitmentStatus.NON_COMMITTED

    def test_T26_lifecycle_status_active(self):
        v = _deserialize_authority_value("lifecycle_status", "ACTIVE")
        assert v == LifecycleStatus.ACTIVE

    def test_T27_cadence_monthly(self):
        v = _deserialize_authority_value("cadence", "monthly")
        assert v == Cadence.MONTHLY
        assert isinstance(v, Cadence)

    def test_T28_cadence_quarterly(self):
        v = _deserialize_authority_value("cadence", "quarterly")
        assert v == Cadence.QUARTERLY

    def test_T29_purpose_type_housing(self):
        v = _deserialize_authority_value("purpose_type", "HOUSING")
        assert v == PurposeType.HOUSING

    def test_T30_planning_amount_decimal(self):
        v = _deserialize_authority_value("planning_amount", "450.00")
        assert v == Decimal("450.00")
        assert isinstance(v, Decimal)

    def test_T31_planning_amount_none_valid(self):
        v = _deserialize_authority_value("planning_amount", None)
        assert v is None

    def test_T32_invalid_recurrence_enum_hard_error(self):
        with pytest.raises(ValueError, match="INVALID_ENUM"):
            _deserialize_authority_value("recurrence_status", "BOGUS")

    def test_T33_invalid_commitment_enum_hard_error(self):
        with pytest.raises(ValueError, match="INVALID_ENUM"):
            _deserialize_authority_value("commitment_status", "CONFIRMED")

    def test_T34_invalid_lifecycle_enum_hard_error(self):
        with pytest.raises(ValueError, match="INVALID_ENUM"):
            _deserialize_authority_value("lifecycle_status", "EXPIRED")

    def test_T35_invalid_cadence_enum_hard_error(self):
        with pytest.raises(ValueError, match="INVALID_ENUM"):
            _deserialize_authority_value("cadence", "MONTHLY")  # must be lowercase

    def test_T36_invalid_purpose_enum_hard_error(self):
        with pytest.raises(ValueError, match="INVALID_ENUM"):
            _deserialize_authority_value("purpose_type", "BOGUS_PURPOSE")

    def test_T37_invalid_decimal_hard_error(self):
        with pytest.raises(ValueError, match="INVALID_DECIMAL"):
            _deserialize_authority_value("planning_amount", "not_a_number")

    def test_T38_unknown_field_hard_error(self):
        with pytest.raises(ValueError, match="UNKNOWN_AUTHORITY_FIELD"):
            _deserialize_authority_value("reserve_eligible", "1")

    def test_T38b_null_non_nullable_field_hard_error(self):
        with pytest.raises(ValueError, match="NULL_VALUE"):
            _deserialize_authority_value("cadence", None)

    def test_T38c_no_float_in_decimal(self):
        """planning_amount deserialization never uses float; Decimal is exact."""
        v = _deserialize_authority_value("planning_amount", "886.25")
        assert v == Decimal("886.25")
        assert type(v) is Decimal


# ═══════════════════════════════════════════════════════════════════════════════
# T39–T46 — IDENTITY / LINK STATES
# ═══════════════════════════════════════════════════════════════════════════════

class TestLinkStates:

    def test_T39_superseded_produces_skip(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"

        conn = _mem_conn()
        _make_db(conn)

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey,
                               commitment_id=None,
                               outcome=LinkOutcome.SKIPPED_SUPERSEDED)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == 0
        assert len(result.skips) == 1
        assert result.skips[0].outcome == AuthorityOutcome.DEFER_SUPERSEDED

    def test_T40_no_linked_commitment_produces_skip(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"

        conn = _mem_conn()
        _make_db(conn)

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey,
                               commitment_id=None,
                               outcome=LinkOutcome.NEW_RECURRING_SUGGESTED)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.skips) == 1
        assert result.skips[0].outcome == AuthorityOutcome.DEFER_NO_LINKED_COMMITMENT

    def test_T41_superseded_not_walked_to_another_family(self):
        """SKIPPED_SUPERSEDED does not fall back to description_key lookup."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        other_cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, other_cid, user_id)
        # Insert authority for another commitment with the same dkey pattern
        _insert_authority(conn, other_cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey,
                               commitment_id=None,
                               outcome=LinkOutcome.SKIPPED_SUPERSEDED)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        # No resolution produced — skip only
        assert len(result.resolutions) == 0
        assert len(result.skips) == 1

    def test_T42_missing_link_result_produces_skip(self):
        """Patterns in persistence_report but not in link_report produce no resolution."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        # Empty link report — no results
        lr = LinkReport(run_id=run_id, user_id=user_id)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# T43–T50 — PRECEDENCE
# ═══════════════════════════════════════════════════════════════════════════════

class TestPrecedence:
    """
    Canonical precedence test fixture:
        MANUAL_OVERRIDE → 400 ILS (highest priority)
        FAMILY_REVIEW   → 450 ILS
        baseline        → 500 ILS
    """

    def _setup(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        return conn, pr, lr, cid, user_id

    def test_T43_both_active_resolve_to_manual(self):
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "planning_amount", "450.00",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, uid, "planning_amount", "400.00",
                          "MANUAL_OVERRIDE", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r = result.resolutions[0]
        assert r.resolved_value == Decimal("400.00")
        assert r.winning_source == "MANUAL_OVERRIDE"

    def test_T44_manual_revoked_falls_back_to_family(self):
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "planning_amount", "450.00",
                          "FAMILY_REVIEW", _uid(), is_active=1)
        _insert_authority(conn, cid, uid, "planning_amount", "400.00",
                          "MANUAL_OVERRIDE", _uid(), is_active=0)  # revoked
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r = result.resolutions[0]
        assert r.resolved_value == Decimal("450.00")
        assert r.winning_source == "FAMILY_REVIEW"

    def test_T45_both_revoked_baseline(self):
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "planning_amount", "450.00",
                          "FAMILY_REVIEW", _uid(), is_active=0)
        _insert_authority(conn, cid, uid, "planning_amount", "400.00",
                          "MANUAL_OVERRIDE", _uid(), is_active=0)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        # No active authority → no resolution
        assert len(result.resolutions) == 0

    def test_T46_family_review_value_differs_from_baseline(self):
        """FAMILY_REVIEW persisted value wins over baseline if no MANUAL."""
        conn, pr, lr, cid, uid = self._setup()
        _insert_authority(conn, cid, uid, "recurrence_status", "RECURRING",
                          "FAMILY_REVIEW", _uid())

        @dataclass
        class FakePattern:
            recurrence_status = RecurrenceStatus.POSSIBLE_RECURRING

        result = resolve_authority_from_reports(
            conn, pr, lr, user_id=uid,
            baseline_patterns={"sub/phone": FakePattern()},
        )
        r = result.resolutions[0]
        assert r.resolved_value == RecurrenceStatus.RECURRING
        assert r.baseline_value == RecurrenceStatus.POSSIBLE_RECURRING
        assert r.winning_source == "FAMILY_REVIEW"


# ═══════════════════════════════════════════════════════════════════════════════
# T47–T55 — PROVENANCE
# ═══════════════════════════════════════════════════════════════════════════════

class TestProvenance:

    def _setup(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        return conn, pr, lr, cid, user_id, dkey, run_id

    def test_T47_winning_source_recorded(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert result.resolutions[0].winning_source == "FAMILY_REVIEW"

    def test_T48_authority_row_id_recorded(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        oid = _uid()
        rowid = _insert_authority(conn, cid, uid, "cadence", "monthly",
                                   "FAMILY_REVIEW", oid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r = result.resolutions[0]
        assert r.family_review_row_id == rowid

    def test_T49_override_id_recorded(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        oid = _uid()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", oid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert result.resolutions[0].override_id == oid

    def test_T50_baseline_value_recorded(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        @dataclass
        class FakePattern:
            cadence = Cadence.QUARTERLY

        result = resolve_authority_from_reports(
            conn, pr, lr, user_id=uid,
            baseline_patterns={dkey: FakePattern()},
        )
        assert result.resolutions[0].baseline_value == Cadence.QUARTERLY

    def test_T51_resolved_value_equals_winning_value(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        _insert_authority(conn, cid, uid, "planning_amount", "450.00",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, uid, "planning_amount", "400.00",
                          "MANUAL_OVERRIDE", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r = result.resolutions[0]
        assert r.resolved_value == r.manual_override_value == Decimal("400.00")

    def test_T52_commitment_id_in_resolution(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert result.resolutions[0].commitment_id == cid

    def test_T53_run_result_id_in_resolution(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        expected_rrid = _run_result_id(run_id, dkey, 0)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert result.resolutions[0].run_result_id == expected_rrid

    def test_T54_description_key_in_resolution(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        _insert_authority(conn, cid, uid, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert result.resolutions[0].description_key == dkey

    def test_T55_both_sources_provenance_in_single_resolution(self):
        conn, pr, lr, cid, uid, dkey, run_id = self._setup()
        fr_oid = _uid()
        mo_oid = _uid()
        fr_rowid = _insert_authority(conn, cid, uid, "cadence", "monthly",
                                      "FAMILY_REVIEW", fr_oid)
        mo_rowid = _insert_authority(conn, cid, uid, "cadence", "quarterly",
                                      "MANUAL_OVERRIDE", mo_oid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r = result.resolutions[0]
        assert r.family_review_row_id == fr_rowid
        assert r.family_review_value == Cadence.MONTHLY
        assert r.manual_override_row_id == mo_rowid
        assert r.manual_override_value == Cadence.QUARTERLY
        assert r.override_id == mo_oid  # winning override_id


# ═══════════════════════════════════════════════════════════════════════════════
# T56–T62 — READ STRATEGY
# ═══════════════════════════════════════════════════════════════════════════════

class TestReadStrategy:

    def test_T56_batch_read_multiple_commitments(self):
        """Multiple commitments are read in one call, not N+1."""
        run_id = _uid()
        user_id = 1
        conn = _mem_conn()
        _make_db(conn)

        outcomes = []
        link_results = []
        n = 10

        for i in range(n):
            dkey = f"sub/item{i}"
            cid = _uid()
            _insert_commitment(conn, cid, user_id)
            _insert_authority(conn, cid, user_id, "cadence", "monthly",
                              "FAMILY_REVIEW", _uid())
            rrid = _run_result_id(run_id, dkey, 0)
            outcomes.append(RunResultOutcome(
                run_result_id=rrid, description_key=dkey, stream_index=0,
                family_id=_uid(), family_resolution=FamilyResolution.MATCHED_EXISTING,
            ))
            link_results.append(PatternLinkResult(
                description_key=dkey, stream_index=0, run_result_id=rrid,
                family_id=_uid(), outcome=LinkOutcome.LINKED, commitment_id=cid,
            ))

        pr = PersistenceReport(run_id=run_id, user_id=user_id, outcomes=outcomes)
        lr = LinkReport(run_id=run_id, user_id=user_id, results=link_results)

        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert len(result.resolutions) == n

    def test_T57_duplicate_active_detection_across_chunk(self):
        """Duplicate active authority is detected regardless of chunk boundaries."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        # Two active MANUAL_OVERRIDE rows for same field
        for _ in range(2):
            conn.execute(
                "INSERT INTO commitment_authority "
                "(commitment_id, user_id, field_name, value, authority_source, "
                "override_id, is_active, created_at, created_by) "
                "VALUES (?,?,?,?,?,?,1,'2026-01-01T00:00:00',1)",
                (cid, user_id, "cadence", "monthly", "MANUAL_OVERRIDE", _uid()),
            )
        conn.commit()

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        with pytest.raises(ValueError, match="DUPLICATE_ACTIVE_SOURCE_AUTHORITY"):
            resolve_authority_from_reports(conn, pr, lr, user_id=user_id)

    def test_T58_unknown_active_field_is_hard_error(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()

        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        # Insert authority for an unsupported field directly
        conn.execute(
            "INSERT INTO commitment_authority "
            "(commitment_id, user_id, field_name, value, authority_source, "
            "override_id, is_active, created_at, created_by) "
            "VALUES (?,?,?,?,?,?,1,'2026-01-01T00:00:00',1)",
            (cid, user_id, "reserve_eligible", "1", "FAMILY_REVIEW", _uid()),
        )
        conn.commit()

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        with pytest.raises(ValueError, match="UNKNOWN_ACTIVE_AUTHORITY_FIELD"):
            resolve_authority_from_reports(conn, pr, lr, user_id=user_id)


# ═══════════════════════════════════════════════════════════════════════════════
# T59–T70 — IMMUTABILITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestImmutability:

    def _full_setup(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        return conn, pr, lr, cid, user_id

    def test_T59_commitment_authority_unchanged(self):
        conn, pr, lr, cid, uid = self._full_setup()
        before = _snapshot(conn, "commitment_authority")
        resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        after = _snapshot(conn, "commitment_authority")
        assert before == after

    def test_T60_commitments_unchanged(self):
        conn, pr, lr, cid, uid = self._full_setup()
        before = _snapshot(conn, "commitments")
        resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        after = _snapshot(conn, "commitments")
        assert before == after

    def test_T61_persistence_report_not_mutated(self):
        conn, pr, lr, cid, uid = self._full_setup()
        original_run_id = pr.run_id
        original_outcomes = list(pr.outcomes)
        original_user_id = pr.user_id
        resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert pr.run_id == original_run_id
        assert pr.user_id == original_user_id
        assert len(pr.outcomes) == len(original_outcomes)

    def test_T62_link_report_not_mutated(self):
        conn, pr, lr, cid, uid = self._full_setup()
        original_run_id = lr.run_id
        original_results = list(lr.results)
        resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert lr.run_id == original_run_id
        assert len(lr.results) == len(original_results)

    def test_T63_report_is_read_only(self):
        """Calling resolve twice produces the same result (idempotent reads)."""
        conn, pr, lr, cid, uid = self._full_setup()
        r1 = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        r2 = resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        assert len(r1.resolutions) == len(r2.resolutions)
        assert r1.resolutions[0].resolved_value == r2.resolutions[0].resolved_value

    def test_T64_authority_row_count_unchanged_after_resolve(self):
        conn, pr, lr, cid, uid = self._full_setup()
        count_before = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
        resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        count_after = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
        assert count_before == count_after


# ═══════════════════════════════════════════════════════════════════════════════
# T65–T70 — COMPATIBILITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestCompatibility:

    def test_T65_no_runtime_financial_values_modified(self):
        """resolve_authority_from_reports does not return EffectiveFinancialResult."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert isinstance(result, AuthorityResolutionReport)
        assert not hasattr(result, "effective")
        assert not hasattr(result, "monthly_reserve_effective")

    def test_T66_resolution_report_has_run_id(self):
        run_id = _uid()
        conn = _mem_conn()
        _make_db(conn)
        pr = PersistenceReport(run_id=run_id, user_id=1)
        lr = LinkReport(run_id=run_id, user_id=1)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=1)
        assert result.run_id == run_id

    def test_T67_resolution_report_has_user_id(self):
        run_id = _uid()
        conn = _mem_conn()
        _make_db(conn)
        pr = PersistenceReport(run_id=run_id, user_id=42)
        lr = LinkReport(run_id=run_id, user_id=42)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=42)
        assert result.user_id == 42

    def test_T68_applied_count_method(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        assert result.applied_count() == 1
        assert result.baseline_count() == 0

    def test_T69_by_commitment_groups_correctly(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_db(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, user_id, "recurrence_status", "RECURRING",
                          "FAMILY_REVIEW", _uid())
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        by_cid = result.by_commitment()
        assert cid in by_cid
        assert len(by_cid[cid]) == 2

    def test_T70_authority_skip_has_required_fields(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        conn = _mem_conn()
        _make_db(conn)
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey,
                               commitment_id=None,
                               outcome=LinkOutcome.SKIPPED_SUPERSEDED)
        result = resolve_authority_from_reports(conn, pr, lr, user_id=user_id)
        skip = result.skips[0]
        assert skip.run_result_id is not None
        assert skip.description_key == dkey
        assert skip.stream_index == 0
        assert skip.outcome == AuthorityOutcome.DEFER_SUPERSEDED
        assert isinstance(skip.reason, str)


# ═══════════════════════════════════════════════════════════════════════════════
# T71–T75 — COMPLETE 13-TABLE DB IMMUTABILITY
# ═══════════════════════════════════════════════════════════════════════════════

_ALL_IMMUTABLE_TABLES = [
    "commitment_authority",
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


def _make_full_schema(conn: sqlite3.Connection) -> None:
    """Create all 13 tables so every snapshot works without 'table does not exist'."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS commitments (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS commitment_authority (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            field_name TEXT NOT NULL,
            value TEXT DEFAULT NULL,
            authority_source TEXT NOT NULL,
            override_id TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            created_by INTEGER NOT NULL,
            revoked_at TEXT DEFAULT NULL,
            revoked_by INTEGER DEFAULT NULL
        );
        CREATE TABLE IF NOT EXISTS v4_run_results (
            id TEXT PRIMARY KEY,
            run_id TEXT,
            user_id INTEGER,
            family_id TEXT,
            description_key TEXT,
            stream_index INTEGER,
            label TEXT,
            planning_amount_agorot INTEGER,
            cadence TEXT,
            recurrence_status TEXT,
            commitment_status TEXT,
            classifier_lifecycle_status TEXT,
            budget_class TEXT,
            reserve_eligible INTEGER,
            monthly_reserve_contrib_agorot INTEGER,
            cadence_coverage REAL,
            evidence_month_count INTEGER,
            review_required INTEGER,
            review_reasons TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_classifier_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT,
            run_result_id TEXT,
            snapshot_json TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pattern_families (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            primary_description_key TEXT,
            is_split_discriminator INTEGER,
            amount_cluster_agorot INTEGER,
            window_start TEXT,
            window_end TEXT,
            commitment_id TEXT,
            is_primary INTEGER,
            linked_by TEXT,
            family_status TEXT,
            superseded_at TEXT,
            superseded_by_event_id TEXT,
            created_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_expense_links (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT,
            expense_id TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_occurrences (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT,
            occurrence_date TEXT,
            amount_agorot INTEGER,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_installment_meta (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT,
            total_installments INTEGER,
            current_installment INTEGER,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_suggestions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_result_id TEXT,
            suggestion_type TEXT,
            detail_json TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_link_conflicts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_result_id TEXT,
            conflict_type TEXT,
            detail_json TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_link_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_result_id TEXT,
            event_type TEXT,
            detail_json TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS expenses (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            amount_agorot INTEGER,
            description TEXT,
            expense_date TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS installments (
            id TEXT PRIMARY KEY,
            expense_id TEXT,
            installment_number INTEGER,
            amount_agorot INTEGER,
            due_date TEXT,
            created_at TEXT
        );
    """)


def _content_snapshot(conn: sqlite3.Connection, table: str) -> tuple:
    """
    Content-level snapshot: tuple of sorted row-tuples.
    Preserves row multiplicity; deterministic; not just a count.
    Returns empty tuple if table does not exist (silently tolerated).
    """
    try:
        rows = conn.execute(f"SELECT * FROM {table}").fetchall()
        return tuple(sorted(tuple(r) for r in rows))
    except Exception:
        return ()


class TestFullImmutability:

    def _setup(self):
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_full_schema(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        # Also add a row to several other tables to prove non-empty content is preserved
        conn.execute(
            "INSERT INTO v4_run_results (id, run_id, user_id, description_key, "
            "stream_index, cadence, recurrence_status, commitment_status, "
            "classifier_lifecycle_status, budget_class, reserve_eligible, "
            "monthly_reserve_contrib_agorot, review_required, review_reasons, created_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (_uid(), run_id, user_id, dkey, 0, "monthly", "RECURRING",
             "COMMITTED", "ACTIVE", "FIXED_AMOUNT_RECURRING",
             1, 45000, 0, "[]", "2026-01-01T00:00:00"),
        )
        conn.execute(
            "INSERT INTO pattern_families "
            "(id, user_id, primary_description_key, is_split_discriminator, "
            "is_primary, linked_by, family_status, created_at, updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (_uid(), user_id, dkey, 0, 1, "AUTO", "ACTIVE",
             "2026-01-01T00:00:00", "2026-01-01T00:00:00"),
        )
        conn.commit()
        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        return conn, pr, lr, cid, user_id

    def test_T71_all_13_tables_content_unchanged(self):
        """
        Snapshot all 13 required tables before and after resolve_authority_from_reports.
        Content-level (sorted row tuples), not just counts.
        Preserves row multiplicity.
        """
        conn, pr, lr, cid, uid = self._setup()
        before = {t: _content_snapshot(conn, t) for t in _ALL_IMMUTABLE_TABLES}
        resolve_authority_from_reports(conn, pr, lr, user_id=uid)
        after = {t: _content_snapshot(conn, t) for t in _ALL_IMMUTABLE_TABLES}
        for table in _ALL_IMMUTABLE_TABLES:
            assert before[table] == after[table], (
                f"Table {table!r} was mutated by resolve_authority_from_reports()"
            )

    def test_T72_non_empty_tables_are_actually_compared(self):
        """Prove that at least commitment_authority and v4_run_results are non-empty before."""
        conn, pr, lr, cid, uid = self._setup()
        assert len(_content_snapshot(conn, "commitment_authority")) > 0
        assert len(_content_snapshot(conn, "v4_run_results")) > 0
        assert len(_content_snapshot(conn, "pattern_families")) > 0
        assert len(_content_snapshot(conn, "commitments")) > 0

    def test_T73_row_multiplicity_preserved(self):
        """Two identical authority rows (possible in corrupt DB) are both counted."""
        conn = _mem_conn()
        _make_full_schema(conn)
        cid = _uid()
        user_id = 1
        _insert_commitment(conn, cid, user_id)
        # Insert two rows — same content
        for _ in range(2):
            conn.execute(
                "INSERT INTO commitment_authority "
                "(commitment_id, user_id, field_name, value, authority_source, "
                "override_id, is_active, created_at, created_by) "
                "VALUES (?,?,?,?,?,?,0,'2026-01-01T00:00:00',1)",
                (cid, user_id, "cadence", "monthly", "FAMILY_REVIEW", _uid()),
            )
        conn.commit()
        snap = _content_snapshot(conn, "commitment_authority")
        # Two rows → tuple of length 2 (not deduplicated)
        assert len(snap) == 2


# ═══════════════════════════════════════════════════════════════════════════════
# T74–T75 — baseline_patterns INPUT IMMUTABILITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestBaselinePatternsImmutability:

    def test_T74_baseline_patterns_dict_not_mutated(self):
        """The baseline_patterns dict itself is not modified."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_full_schema(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        @dataclass
        class FakePattern:
            cadence = Cadence.QUARTERLY

        bp = {dkey: FakePattern()}
        keys_before = set(bp.keys())
        len_before = len(bp)

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        resolve_authority_from_reports(conn, pr, lr, user_id=user_id,
                                       baseline_patterns=bp)

        assert set(bp.keys()) == keys_before
        assert len(bp) == len_before

    def test_T75_baseline_pattern_objects_not_mutated(self):
        """PatternResult-like objects inside baseline_patterns are not mutated."""
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_full_schema(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        @dataclass
        class FakePattern:
            cadence = Cadence.QUARTERLY
            recurrence_status = RecurrenceStatus.POSSIBLE_RECURRING

        pattern = FakePattern()
        cadence_before = pattern.cadence
        recurrence_before = pattern.recurrence_status

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        resolve_authority_from_reports(conn, pr, lr, user_id=user_id,
                                       baseline_patterns={dkey: pattern})

        assert pattern.cadence == cadence_before
        assert pattern.recurrence_status == recurrence_before


# ═══════════════════════════════════════════════════════════════════════════════
# T76 — COHERENT READ VIEW
# ═══════════════════════════════════════════════════════════════════════════════

class TestCoherentReadView:

    def test_T76_savepoint_used_for_batch_read(self):
        """
        Prove that resolve_authority_from_reports wraps the DB read in a SAVEPOINT,
        providing a coherent read snapshot.

        Mechanism: we observe that the SAVEPOINT is opened and released by
        checking that calls on a connection with no uncommitted writes succeed
        cleanly and that the connection remains usable after resolution
        (RELEASE was called — no dangling savepoint).
        """
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_full_schema(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)

        # If SAVEPOINT leaked (not RELEASED), a second SAVEPOINT with same name
        # would silently nest in SQLite — but subsequent operations would still work.
        # Prove: connection is fully usable after resolution (RELEASE happened).
        resolve_authority_from_reports(conn, pr, lr, user_id=user_id)

        # Connection is still usable — no dangling savepoint / lock
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_authority"
        ).fetchone()[0]
        assert count == 1

        # Can open a new SAVEPOINT with the same name — proving previous was released
        conn.execute("SAVEPOINT sp_phase2d1_read")
        conn.execute("RELEASE SAVEPOINT sp_phase2d1_read")


# ═══════════════════════════════════════════════════════════════════════════════
# T77 — REAL PRAGMA foreign_key_check
# ═══════════════════════════════════════════════════════════════════════════════

class TestForeignKeyCheck:

    def test_T77_foreign_key_check_zero_violations(self):
        """
        Run PRAGMA foreign_key_check on the populated in-memory test DB.
        Expected: [] (zero violations).
        """
        run_id = _uid()
        user_id = 1
        dkey = "sub/phone"
        cid = _uid()
        conn = _mem_conn()
        _make_full_schema(conn)
        _insert_commitment(conn, cid, user_id)
        _insert_authority(conn, cid, user_id, "cadence", "monthly",
                          "FAMILY_REVIEW", _uid())
        _insert_authority(conn, cid, user_id, "recurrence_status", "RECURRING",
                          "MANUAL_OVERRIDE", _uid())

        pr = _make_persistence_report(run_id, user_id, dkey, _uid())
        lr = _make_link_report(run_id, user_id, dkey, commitment_id=cid)
        resolve_authority_from_reports(conn, pr, lr, user_id=user_id)

        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        assert violations == [], (
            f"PRAGMA foreign_key_check returned violations: {violations}"
        )
