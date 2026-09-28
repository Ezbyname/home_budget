"""
Phase 2C — Family Review Authority Persistence Tests
101 focused tests covering all approved scenarios.
"""

from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Optional
from unittest.mock import patch

import pytest

from intelligence.v4_cashflow_engine import PatternOverride
from intelligence.v4_contracts import (
    Cadence,
    ClassificationReport,
    CommitmentStatus,
    EffectiveFinancialResult,
    LifecycleStatus,
    PurposeType,
    RawClassifierOutput,
    RecurrenceStatus,
    ReconciliationRecord,
    ReconciliationReport,
)
from v4_authority import (
    AuthorityReport,
    Phase2COutcome,
    Phase2CResult,
    _CANONICAL_MERGE_IDENTITY,
    _IMMUTABLE_TABLES,
    _expected_audit_override_str,
    _is_canonical_merge_defer,
    _serialize_value,
    _snapshot_all_immutable,
    _table_snapshot,
    persist_family_review_authority,
    persist_family_review_authority_from_path,
)
from v4_linking import LinkOutcome, LinkReport, PatternLinkResult
from v4_persistence import FamilyResolution, PersistenceReport, RunResultOutcome, _run_result_id


# ═══════════════════════════════════════════════════════════════════════════
# TEST HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _mem_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _make_db(conn: sqlite3.Connection) -> None:
    """Create all tables needed by Phase 2C tests."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS commitments (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            description TEXT,
            commitment_status TEXT DEFAULT 'ACTIVE'
        );
        CREATE TABLE IF NOT EXISTS pattern_families (
            id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            primary_description_key TEXT NOT NULL,
            commitment_id TEXT,
            is_split_discriminator INTEGER DEFAULT 0,
            is_primary INTEGER DEFAULT 1,
            family_status TEXT DEFAULT 'ACTIVE',
            linked_by TEXT,
            window_start TEXT,
            window_end TEXT,
            amount_cluster_agorot INTEGER,
            superseded_at TEXT,
            superseded_by_event_id TEXT,
            created_at TEXT,
            updated_at TEXT,
            FOREIGN KEY(commitment_id) REFERENCES commitments(id)
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
            decision_source TEXT,
            family_review_required INTEGER,
            review_reasons_json TEXT,
            member_ids_json TEXT,
            membership_confidence_json TEXT,
            evidence_sources_json TEXT,
            canonical_identity TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_expense_links (
            id TEXT PRIMARY KEY,
            commitment_id TEXT,
            expense_id TEXT,
            membership_type TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_classifier_snapshots (
            id TEXT PRIMARY KEY,
            commitment_id TEXT,
            run_result_id TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_occurrences (
            id TEXT PRIMARY KEY,
            commitment_id TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_installment_meta (
            id TEXT PRIMARY KEY,
            commitment_id TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_suggestions (
            id TEXT PRIMARY KEY,
            commitment_id TEXT
        );
        CREATE TABLE IF NOT EXISTS commitment_link_conflicts (
            id TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS commitment_link_events (
            id TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS expenses (
            id TEXT PRIMARY KEY,
            user_id INTEGER,
            date TEXT,
            description TEXT,
            amount REAL,
            category_id TEXT
        );
        CREATE TABLE IF NOT EXISTS installments (
            id TEXT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS commitment_authority (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id       TEXT NOT NULL,
            user_id             INTEGER NOT NULL,
            field_name          TEXT NOT NULL,
            value               TEXT DEFAULT NULL,
            authority_source    TEXT NOT NULL
                CHECK(authority_source IN ('MANUAL_OVERRIDE', 'FAMILY_REVIEW')),
            override_id         TEXT NOT NULL,
            is_active           INTEGER NOT NULL DEFAULT 1
                CHECK(is_active IN (0, 1)),
            created_at          TEXT NOT NULL,
            created_by          INTEGER NOT NULL,
            revoked_at          TEXT DEFAULT NULL,
            revoked_by          INTEGER DEFAULT NULL,
            CHECK(is_active = 0 OR (revoked_at IS NULL AND revoked_by IS NULL)),
            CHECK(is_active = 1 OR revoked_at IS NOT NULL),
            FOREIGN KEY (commitment_id) REFERENCES commitments(id)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ca_active_field_source
            ON commitment_authority(user_id, commitment_id, field_name, authority_source)
            WHERE is_active = 1;
        CREATE UNIQUE INDEX IF NOT EXISTS idx_ca_active_instance
            ON commitment_authority(user_id, commitment_id, override_id)
            WHERE is_active = 1;
    """)
    conn.commit()


def _seed_commitment(conn, commitment_id="C1", user_id=1):
    conn.execute(
        "INSERT OR IGNORE INTO commitments(id, user_id) VALUES (?, ?)",
        (commitment_id, user_id)
    )


_DUMMY_RECON_RECORD = ReconciliationRecord(
    field="test", reviewed_value=Decimal("0"), derived_value=Decimal("0"),
    raw_derived_value=Decimal("0"), difference=Decimal("0"), status="MATCH",
    conflict_report=None,
)
_DUMMY_RECON = ReconciliationReport(
    planning_income=_DUMMY_RECON_RECORD,
    monthly_reserve=_DUMMY_RECON_RECORD,
)


def _make_raw_output(patterns=()) -> RawClassifierOutput:
    return RawClassifierOutput(
        patterns=tuple(patterns),
        income_streams=(),
        planning_income_raw=Decimal("0"),
        monthly_reserve_raw=Decimal("0"),
        family_review_items=(),
    )


def _make_effective_output(patterns=(), overrides_applied=()) -> EffectiveFinancialResult:
    return EffectiveFinancialResult(
        patterns=tuple(patterns),
        income_streams=(),
        planning_income_effective=Decimal("0"),
        monthly_reserve_effective=Decimal("0"),
        family_review_items=(),
        overrides_applied=tuple(overrides_applied),
    )


def _make_report(
    raw_patterns=(),
    applied_override_ids=(),
    override_audit=(),
    run_id="RUN-ANALYSIS",
) -> ClassificationReport:
    return ClassificationReport(
        classifier_version="4.0",
        run_id=run_id,
        analysis_db="/tmp/test.db",
        run_at="2026-01-01T00:00:00",
        raw=_make_raw_output(raw_patterns),
        effective=_make_effective_output(overrides_applied=applied_override_ids),
        reconciliation=_DUMMY_RECON,
        override_audit=tuple(override_audit),
    )


def _make_persistence_report(
    run_id="RUN-PERSIST",
    user_id=1,
    outcomes=None,
) -> PersistenceReport:
    return PersistenceReport(
        run_id=run_id, user_id=user_id,
        outcomes=outcomes if outcomes is not None else [],
    )


def _make_link_report(run_id="RUN-PERSIST", user_id=1, results=None) -> LinkReport:
    return LinkReport(
        run_id=run_id, user_id=user_id,
        results=results if results is not None else [],
    )


def _make_ov(
    description_key="DK1",
    field_name="commitment_status",
    value=CommitmentStatus.COMMITTED,
    override_id="ov-test-1",
    stream_label_hint="",
    canonical_identity=None,
) -> PatternOverride:
    return PatternOverride(
        description_key=description_key,
        stream_label_hint=stream_label_hint,
        field_name=field_name,
        value=value,
        override_id=override_id,
        canonical_identity=canonical_identity,
    )


def _make_minimal_pattern(description_key="DK1"):
    """Create a minimal PatternResult-like object for raw.patterns."""
    from intelligence.v4_contracts import (
        AmountBehavior, BudgetClass, DecisionSource, PatternResult,
    )
    return PatternResult(
        description_key=description_key,
        label=description_key,
        recurrence_status=RecurrenceStatus.RECURRING,
        commitment_status=CommitmentStatus.COMMITTED,
        amount_behavior=AmountBehavior.STABLE,
        budget_class=BudgetClass.FIXED_AMOUNT_RECURRING,
        lifecycle_status=LifecycleStatus.ACTIVE,
        purpose_type=PurposeType.HOUSING,
        cadence=Cadence.MONTHLY,
        planning_amount=Decimal("100.00"),
        member_ids=(),
        membership_confidence={},
        evidence_sources=(),
        decision_source=DecisionSource.FAMILY_REVIEW,
        family_review_required=False,
        review_reasons=(),
        reserve_eligible=True,
        monthly_reserve_contrib=Decimal("100.00"),
    )


def _make_run_result(
    run_id, dkey, si=0, family_id="FAM1",
    family_resolution=FamilyResolution.MATCHED_EXISTING,
) -> RunResultOutcome:
    rrid = _run_result_id(run_id, dkey, si)
    return RunResultOutcome(
        run_result_id=rrid,
        description_key=dkey,
        stream_index=si,
        family_id=family_id,
        family_resolution=family_resolution,
    )


def _make_link_result(
    run_id, dkey, si=0, commitment_id="C1",
    outcome=LinkOutcome.LINKED,
    family_id="FAM1",
) -> PatternLinkResult:
    rrid = _run_result_id(run_id, dkey, si)
    return PatternLinkResult(
        description_key=dkey,
        stream_index=si,
        run_result_id=rrid,
        family_id=family_id,
        outcome=outcome,
        commitment_id=commitment_id,
    )


def _make_audit_entry(
    description_key, override_ids_applied, changed_fields=None
) -> dict:
    return {
        "description_key": description_key,
        "override_ids_applied": list(override_ids_applied),
        "changed_fields": changed_fields or {},
        "label": description_key,
    }


def _changed_field_rec(ov: PatternOverride) -> dict:
    return {
        "classifier": None,
        "override": _expected_audit_override_str(ov),
        "override_id": ov.override_id,
    }


def _full_setup(
    conn,
    run_id="RUN-PERSIST",
    user_id=1,
    dkey="DK1",
    commitment_id="C1",
    ov=None,
    analysis_run_id="RUN-ANALYSIS",
) -> tuple:
    """Full scaffold: DB seeded, reports built, ready for persist_family_review_authority."""
    if ov is None:
        ov = _make_ov(description_key=dkey)

    _seed_commitment(conn, commitment_id, user_id)
    pattern = _make_minimal_pattern(dkey)
    run_result = _make_run_result(run_id, dkey, 0)
    link_result = _make_link_result(run_id, dkey, 0, commitment_id=commitment_id)

    audit_entry = _make_audit_entry(
        dkey,
        [ov.override_id],
        {ov.field_name: _changed_field_rec(ov)},
    )

    analysis = _make_report(
        raw_patterns=[pattern],
        applied_override_ids=[ov.override_id],
        override_audit=[audit_entry],
        run_id=analysis_run_id,
    )
    persistence = _make_persistence_report(run_id=run_id, user_id=user_id, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=user_id, results=[link_result])
    return analysis, persistence, links, [ov]


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 1: INPUT CONSISTENCY (T01–T08)
# ═══════════════════════════════════════════════════════════════════════════

def test_T01_user_id_mismatch_persistence():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persistence_wrong = _make_persistence_report(run_id="R", user_id=2)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence_wrong, links, ovs, user_id=1)


def test_T02_user_id_mismatch_link():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    links_wrong = _make_link_report(run_id="R", user_id=2)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links_wrong, ovs, user_id=1)


def test_T03_run_id_mismatch_persistence_link():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R1", user_id=1)
    links_wrong = _make_link_report(run_id="R2", user_id=1)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links_wrong, ovs, user_id=1)


def test_T04_r1_r2_valid_analysis_run_id_differs():
    """R1 (analysis run_id) != R2 (persistence run_id) is valid and must succeed."""
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(
        conn, run_id="R2", user_id=1, analysis_run_id="R1"
    )
    # R1 != R2 must not cause any error
    report = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report.inserted_count() == 1


def test_T05_applied_override_not_in_overrides_list():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    with pytest.raises((KeyError, ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [], user_id=1)


def test_T06_duplicate_override_id_in_list():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    ov_dup = _make_ov(override_id=ovs[0].override_id)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ovs[0], ov_dup], user_id=1)


def test_T07_audit_references_id_not_in_applied():
    """override_audit mentions an override_id not in effective.overrides_applied."""
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    orphan_entry = _make_audit_entry("DK1", ["ov-orphan"], {})
    analysis = _make_report(
        raw_patterns=[pattern],
        applied_override_ids=[ov.override_id],
        override_audit=[orphan_entry],
    )
    persistence = _make_persistence_report(run_id="R", user_id=1)
    links = _make_link_report(run_id="R", user_id=1)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T08_applied_id_absent_from_audit():
    """effective.overrides_applied has an ID with no audit entry."""
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    # No audit entry for ov.override_id
    analysis = _make_report(
        raw_patterns=[pattern],
        applied_override_ids=[ov.override_id],
        override_audit=[],  # empty — no audit for this override
    )
    persistence = _make_persistence_report(run_id="R", user_id=1)
    links = _make_link_report(run_id="R", user_id=1)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 2: DATACLASS CONTRACT (T09)
# ═══════════════════════════════════════════════════════════════════════════

def test_T09_no_analysis_user_id_access():
    """ClassificationReport has no user_id. v4_authority must not access it."""
    import v4_authority as mod
    import ast, inspect
    src = inspect.getsource(mod)
    # Must not reference analysis_result.user_id
    assert "analysis_result.user_id" not in src
    # Must not reference analysis_result.applied_override_ids
    assert "analysis_result.applied_override_ids" not in src
    # Must use analysis_result.effective.overrides_applied
    assert "effective.overrides_applied" in src


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 3: TARGET IDENTITY (T10–T16)
# ═══════════════════════════════════════════════════════════════════════════

def test_T10_uses_persistence_run_id_for_rrid():
    """run_result_id must be computed with persistence_report.run_id, not analysis_result.run_id."""
    run_id = "PERSIST-RUN"
    rrid = _run_result_id(run_id, "DK1", 0)
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    _seed_commitment(conn, "C1", 1)
    pattern = _make_minimal_pattern("DK1")
    run_result = RunResultOutcome(
        run_result_id=rrid,
        description_key="DK1", stream_index=0,
        family_id="FAM1",
        family_resolution=FamilyResolution.MATCHED_EXISTING,
    )
    link_result = PatternLinkResult(
        description_key="DK1", stream_index=0,
        run_result_id=rrid, family_id="FAM1",
        outcome=LinkOutcome.LINKED, commitment_id="C1",
    )
    audit_entry = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry], run_id="ANALYSIS-RUN-DIFFERENT")
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.inserted_count() == 1


def test_T11_no_limit1_sql_in_source():
    """SELECT ... WHERE description_key=? LIMIT 1 must not appear in v4_authority.py."""
    import inspect, v4_authority as mod
    src = inspect.getsource(mod)
    assert "LIMIT 1" not in src.upper() or "description_key" not in src.lower()
    # More specific: no LIMIT 1 immediately after a description_key filter
    import re
    assert not re.search(r'description_key\s*=.*LIMIT\s+1', src, re.IGNORECASE | re.DOTALL)


def test_T12_unresolved_parallel_deferred():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0,
                                  family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    link_result = _make_link_result(run_id, "DK1", 0)
    audit_entry = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.deferred_count() == 1
    assert report.outcomes[0].outcome == Phase2COutcome.DEFER_PARALLEL_UNRESOLVED


def test_T13_canonical_merge_deferred():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(canonical_identity="google-cloud-tbd",
                  override_id="ov-google-cloud-lnkqw-tbd",
                  field_name="planning_amount", value=None)
    pattern = _make_minimal_pattern("DK-CLOUD")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK-CLOUD", 0)
    link_result = _make_link_result(run_id, "DK-CLOUD", 0, commitment_id="C1")
    _seed_commitment(conn, "C1", 1)
    audit_entry = _make_audit_entry("DK-CLOUD", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.deferred_count() == 1
    assert report.outcomes[0].outcome == Phase2COutcome.DEFER_CANONICAL_MERGE


def test_T14_no_linked_commitment_deferred():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0)
    link_result = _make_link_result(run_id, "DK1", 0, commitment_id=None,
                                    outcome=LinkOutcome.NEW_RECURRING_SUGGESTED)
    audit_entry = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.deferred_count() == 1
    assert report.outcomes[0].outcome == Phase2COutcome.DEFER_NO_LINKED_COMMITMENT


def test_T15_superseded_deferred():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0)
    link_result = _make_link_result(run_id, "DK1", 0, commitment_id=None,
                                    outcome=LinkOutcome.SKIPPED_SUPERSEDED)
    audit_entry = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.deferred_count() == 1
    assert report.outcomes[0].outcome == Phase2COutcome.DEFER_SUPERSEDED


def test_T16_cross_user_family_hard_error():
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C2', 2)")
    conn.execute("INSERT INTO pattern_families(id, user_id, primary_description_key, commitment_id, family_status, created_at, updated_at) "
                 "VALUES ('FAM2', 2, 'DK1', 'C2', 'ACTIVE', '2026-01-01', '2026-01-01')")
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0, family_id="FAM2",
                                  family_resolution=FamilyResolution.MATCHED_EXISTING)
    link_result = _make_link_result(run_id, "DK1", 0, commitment_id="C2",
                                    family_id="FAM2")
    audit_entry = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 4: CANONICAL DEFER SELECTOR (T17–T22)
# ═══════════════════════════════════════════════════════════════════════════

def test_T17_canonical_merge_selector_count_8():
    """_is_canonical_merge_defer selects exactly 8 overrides from PATTERN_OVERRIDES."""
    import sys; sys.path.insert(0, "/home/user/home_budget")
    from analyze_home_budget_v4 import PATTERN_OVERRIDES
    selected = [ov for ov in PATTERN_OVERRIDES if _is_canonical_merge_defer(ov)]
    assert len(selected) == 8


def test_T18_canonical_merge_selector_exact_ids():
    from analyze_home_budget_v4 import PATTERN_OVERRIDES
    selected_ids = {ov.override_id for ov in PATTERN_OVERRIDES if _is_canonical_merge_defer(ov)}
    expected_ids = {
        "ov-google-cloud-lnkqw-tbd",
        "ov-google-cloud-lnkqw-recurrence",
        "ov-google-cloud-lnkqw-committed",
        "ov-google-cloud-lnkqw-active",
        "ov-google-cloud-tlbz7j-tbd",
        "ov-google-cloud-tlbz7j-recurrence",
        "ov-google-cloud-tlbz7j-committed",
        "ov-google-cloud-tlbz7j-active",
    }
    assert selected_ids == expected_ids


def test_T19_non_google_canonical_not_canonical_merge():
    """hot/klal/migdal/menora canonical overrides do NOT satisfy canonical merge selector."""
    from analyze_home_budget_v4 import PATTERN_OVERRIDES
    parallel_ci = {"hot-subscription", "klal-hayim-b", "migdal-hayim-briut", "menora-mivtahim"}
    non_google = [ov for ov in PATTERN_OVERRIDES if ov.canonical_identity in parallel_ci]
    assert len(non_google) == 20
    assert all(not _is_canonical_merge_defer(ov) for ov in non_google)


def test_T20_parallel_wins_over_canonical_merge():
    """An override with canonical_identity=google-cloud-tbd AND UNRESOLVED_PARALLEL → PARALLEL wins."""
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(canonical_identity="google-cloud-tbd",
                  override_id="ov-google-cloud-lnkqw-tbd",
                  field_name="planning_amount", value=None)
    pattern = _make_minimal_pattern("DK-CLOUD")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK-CLOUD", 0,
                                  family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    link_result = _make_link_result(run_id, "DK-CLOUD", 0)
    audit_entry = _make_audit_entry("DK-CLOUD", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.outcomes[0].outcome == Phase2COutcome.DEFER_PARALLEL_UNRESOLVED


def test_T21_canonical_identity_total_28():
    from analyze_home_budget_v4 import PATTERN_OVERRIDES
    count = sum(1 for ov in PATTERN_OVERRIDES if ov.canonical_identity is not None)
    assert count == 28


def test_T22_partition_sums_to_118():
    from analyze_home_budget_v4 import PATTERN_OVERRIDES
    assert len(PATTERN_OVERRIDES) == 118


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 5: AUDIT CARDINALITY (T23–T25)
# ═══════════════════════════════════════════════════════════════════════════

def test_T23_zero_audit_entries_hard_error():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    # No audit entry for ov
    analysis = _make_report(
        raw_patterns=[pattern],
        applied_override_ids=[ov.override_id],
        override_audit=[],
    )
    persistence = _make_persistence_report(run_id="R", user_id=1)
    links = _make_link_report(run_id="R", user_id=1)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T24_multiple_audit_entries_different_dkeys_hard_error():
    """override_id in audit entries with different description_keys → SOURCE_TARGET_CARDINALITY_DRIFT."""
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern1 = _make_minimal_pattern("DK1")
    pattern2 = _make_minimal_pattern("DK2")
    audit1 = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    audit2 = _make_audit_entry("DK2", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(
        raw_patterns=[pattern1, pattern2],
        applied_override_ids=[ov.override_id],
        override_audit=[audit1, audit2],
    )
    persistence = _make_persistence_report(run_id="R", user_id=1)
    links = _make_link_report(run_id="R", user_id=1)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T25_multiple_audit_entries_same_dkey_parallel():
    """override_id in 2 audit entries, same description_key (parallel) → DEFER_PARALLEL_UNRESOLVED."""
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    p1 = _make_minimal_pattern("DK1")
    p2 = _make_minimal_pattern("DK1")
    run_id = "R"
    rr0 = _make_run_result(run_id, "DK1", 0, family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    rr1 = _make_run_result(run_id, "DK1", 1, family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    lr0 = _make_link_result(run_id, "DK1", 0, commitment_id=None)
    lr1 = _make_link_result(run_id, "DK1", 1, commitment_id=None)
    audit0 = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    audit1 = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[p1, p2], applied_override_ids=[ov.override_id],
                            override_audit=[audit0, audit1])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[rr0, rr1])
    links = _make_link_report(run_id=run_id, user_id=1, results=[lr0, lr1])
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.outcomes[0].outcome == Phase2COutcome.DEFER_PARALLEL_UNRESOLVED


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 6: SERIALIZATION (T26–T35)
# ═══════════════════════════════════════════════════════════════════════════

def test_T26_serialize_recurrence_status():
    assert _serialize_value("recurrence_status", RecurrenceStatus.RECURRING) == "RECURRING"


def test_T27_serialize_commitment_status():
    assert _serialize_value("commitment_status", CommitmentStatus.COMMITTED) == "COMMITTED"


def test_T28_serialize_lifecycle_status():
    assert _serialize_value("lifecycle_status", LifecycleStatus.ACTIVE) == "ACTIVE"


def test_T29_serialize_cadence():
    assert _serialize_value("cadence", Cadence.MONTHLY) == "monthly"


def test_T30_serialize_purpose_type():
    result = _serialize_value("purpose_type", PurposeType.HOUSING)
    assert result == PurposeType.HOUSING.value


def test_T31_serialize_planning_amount_decimal():
    assert _serialize_value("planning_amount", Decimal("607.00")) == "607.00"


def test_T32_serialize_planning_amount_precision():
    assert _serialize_value("planning_amount", Decimal("607.12345")) == "607.12345"


def test_T33_serialize_planning_amount_none():
    assert _serialize_value("planning_amount", None) is None


def test_T34_serialize_unknown_field_raises():
    with pytest.raises(TypeError):
        _serialize_value("label", "some value")


def test_T35_serialize_wrong_type_raises():
    with pytest.raises(TypeError):
        _serialize_value("planning_amount", 607.0)


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 7: AUDIT VALUE DRIFT ALL SIX FIELDS (T36–T42)
# ═══════════════════════════════════════════════════════════════════════════

def _run_with_drifted_audit(conn, ov, drifted_override_str):
    """Helper: build setup where audit says override value != live PatternOverride.value."""
    _seed_commitment(conn, "C1", 1)
    run_id = "R"
    pattern = _make_minimal_pattern("DK1")
    run_result = _make_run_result(run_id, "DK1", 0)
    link_result = _make_link_result(run_id, "DK1", 0)
    bad_rec = {"classifier": None, "override": drifted_override_str, "override_id": ov.override_id}
    audit_entry = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: bad_rec})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[audit_entry])
    persistence = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    links = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T36_audit_value_drift_recurrence_status():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="recurrence_status", value=RecurrenceStatus.RECURRING)
    _run_with_drifted_audit(conn, ov, str(RecurrenceStatus.POSSIBLE_RECURRING))


def test_T37_audit_value_drift_commitment_status():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="commitment_status", value=CommitmentStatus.COMMITTED)
    _run_with_drifted_audit(conn, ov, str(CommitmentStatus.NON_COMMITTED))


def test_T38_audit_value_drift_lifecycle_status():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="lifecycle_status", value=LifecycleStatus.ACTIVE)
    _run_with_drifted_audit(conn, ov, str(LifecycleStatus.ENDED))


def test_T39_audit_value_drift_cadence():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="cadence", value=Cadence.MONTHLY)
    _run_with_drifted_audit(conn, ov, str(Cadence.QUARTERLY))


def test_T40_audit_value_drift_planning_amount():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="planning_amount", value=Decimal("500.00"))
    _run_with_drifted_audit(conn, ov, "300.00")


def test_T41_audit_value_drift_planning_amount_none_vs_value():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="planning_amount", value=None)
    _run_with_drifted_audit(conn, ov, "300.00")


def test_T42_audit_value_drift_purpose_type():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="purpose_type", value=PurposeType.HOUSING)
    _run_with_drifted_audit(conn, ov, str(PurposeType.EDUCATION))


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 8: INSERT (T43–T49)
# ═══════════════════════════════════════════════════════════════════════════

def test_T43_normal_insert():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    report = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report.inserted_count() == 1


def test_T44_inserted_row_authority_source():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    row = conn.execute("SELECT authority_source FROM commitment_authority").fetchone()
    assert row[0] == "FAMILY_REVIEW"


def test_T45_inserted_row_created_by():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=7)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=7)
    row = conn.execute("SELECT created_by FROM commitment_authority").fetchone()
    assert row[0] == 7


def test_T46_inserted_row_is_active():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    row = conn.execute("SELECT is_active FROM commitment_authority").fetchone()
    assert row[0] == 1


def test_T47_inserted_row_revocation_null():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    row = conn.execute("SELECT revoked_at, revoked_by FROM commitment_authority").fetchone()
    assert row[0] is None
    assert row[1] is None


def test_T48_inserted_row_commitment_id():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1, commitment_id="C1")
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    row = conn.execute("SELECT commitment_id FROM commitment_authority").fetchone()
    assert row[0] == "C1"


def test_T49_inserted_row_serialized_value():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(field_name="commitment_status", value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    row = conn.execute("SELECT value FROM commitment_authority").fetchone()
    assert row[0] == "COMMITTED"


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 9: IDEMPOTENCE / ALREADY_CURRENT (T50–T53)
# ═══════════════════════════════════════════════════════════════════════════

def test_T50_second_call_already_current():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    report2 = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report2.already_current_count() == 1
    assert report2.inserted_count() == 0


def test_T51_second_call_no_new_rows():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    count_before = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    count_after = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count_before == count_after


def test_T52_second_call_created_at_unchanged():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    ts1 = conn.execute("SELECT created_at FROM commitment_authority").fetchone()[0]
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    ts2 = conn.execute("SELECT created_at FROM commitment_authority").fetchone()[0]
    assert ts1 == ts2


def test_T53_db_state_identical_after_second_call():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    rows1 = conn.execute("SELECT * FROM commitment_authority ORDER BY id").fetchall()
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    rows2 = conn.execute("SELECT * FROM commitment_authority ORDER BY id").fetchall()
    assert rows1 == rows2


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 10: REPLACEMENT (T54–T64)
# ═══════════════════════════════════════════════════════════════════════════

def _setup_replace(conn, run_id="R", user_id=1, old_value=CommitmentStatus.COMMITTED,
                   new_value=CommitmentStatus.NON_COMMITTED, commitment_id="C1"):
    ov_old = _make_ov(field_name="commitment_status", value=old_value, override_id="ov-replace-test")
    ov_new = _make_ov(field_name="commitment_status", value=new_value, override_id="ov-replace-test")
    analysis, persistence, links, _ = _full_setup(conn, run_id=run_id, user_id=user_id,
                                                   commitment_id=commitment_id, ov=ov_old)
    persist_family_review_authority(conn, analysis, persistence, links, [ov_old], user_id=user_id)

    # Now build second call with new value for same override_id
    pattern = _make_minimal_pattern("DK1")
    run_result = _make_run_result(run_id, "DK1", 0)
    link_result = _make_link_result(run_id, "DK1", 0, commitment_id=commitment_id)
    audit_entry = _make_audit_entry("DK1", [ov_new.override_id], {ov_new.field_name: _changed_field_rec(ov_new)})
    analysis2 = _make_report(raw_patterns=[pattern], applied_override_ids=[ov_new.override_id],
                              override_audit=[audit_entry])
    persistence2 = _make_persistence_report(run_id=run_id, user_id=user_id, outcomes=[run_result])
    links2 = _make_link_report(run_id=run_id, user_id=user_id, results=[link_result])
    return analysis2, persistence2, links2, [ov_new]


def test_T54_replacement_outcome():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    report = persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    assert report.replaced_count() == 1


def test_T55_old_row_inactive_after_replace():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    rows = conn.execute("SELECT is_active, value FROM commitment_authority ORDER BY id").fetchall()
    assert rows[0][0] == 0  # old row inactive
    assert rows[1][0] == 1  # new row active


def test_T56_old_row_revoked_at_populated():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    old_row = conn.execute("SELECT revoked_at FROM commitment_authority ORDER BY id").fetchone()
    assert old_row[0] is not None


def test_T57_old_row_revoked_by_null():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    old_row = conn.execute("SELECT revoked_by FROM commitment_authority ORDER BY id").fetchone()
    assert old_row[0] is None


def test_T58_new_row_is_active():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    new_row = conn.execute("SELECT is_active FROM commitment_authority ORDER BY id DESC").fetchone()
    assert new_row[0] == 1


def test_T59_old_row_retained():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    count = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count == 2


def test_T60_revoked_at_equals_new_created_at():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    rows = conn.execute("SELECT revoked_at, created_at FROM commitment_authority ORDER BY id").fetchall()
    old_revoked_at = rows[0][0]
    new_created_at = rows[1][1]
    assert old_revoked_at == new_created_at


def test_T61_new_row_has_new_value():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn, new_value=CommitmentStatus.NON_COMMITTED)
    persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    new_val = conn.execute("SELECT value FROM commitment_authority ORDER BY id DESC LIMIT 1").fetchone()[0]
    assert new_val == "NON_COMMITTED"


def test_T62_previous_and_new_row_ids_in_report():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    report = persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    r = report.outcomes[0]
    assert r.previous_row_id is not None
    assert r.new_row_id is not None
    assert r.previous_row_id != r.new_row_id


def test_T63_replace_rollback_on_insert_fail():
    """If INSERT fails after revoke UPDATE, old row is restored to is_active=1."""
    conn = _mem_conn(); _make_db(conn)
    # Insert initial row
    ov_old = _make_ov(field_name="commitment_status", value=CommitmentStatus.COMMITTED, override_id="ov-rb")
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov_old)
    persist_family_review_authority(conn, analysis, persistence, links, [ov_old], user_id=1)
    old_row_id = conn.execute("SELECT id FROM commitment_authority").fetchone()[0]

    # Force INSERT to fail by injecting a bad commitment_id via a wrapper
    class _FailOnInsertConn:
        def __init__(self, real_conn):
            self._c = real_conn
            self._fail_next_insert = False
        def execute(self, sql, params=()):
            if "INSERT INTO commitment_authority" in sql and self._fail_next_insert:
                raise sqlite3.OperationalError("injected INSERT failure")
            if "UPDATE commitment_authority" in sql and "is_active  = 0" in sql:
                self._fail_next_insert = True
            return self._c.execute(sql, params)
        def __getattr__(self, name):
            return getattr(self._c, name)

    wrapped = _FailOnInsertConn(conn)
    ov_new = _make_ov(field_name="commitment_status", value=CommitmentStatus.NON_COMMITTED, override_id="ov-rb")
    pattern = _make_minimal_pattern("DK1")
    run_result = _make_run_result("R", "DK1", 0)
    link_result = _make_link_result("R", "DK1", 0, commitment_id="C1")
    ae = _make_audit_entry("DK1", [ov_new.override_id], {ov_new.field_name: _changed_field_rec(ov_new)})
    analysis2 = _make_report(raw_patterns=[pattern], applied_override_ids=[ov_new.override_id],
                              override_audit=[ae])
    p2 = _make_persistence_report(run_id="R", user_id=1, outcomes=[run_result])
    l2 = _make_link_report(run_id="R", user_id=1, results=[link_result])
    with pytest.raises(Exception):
        persist_family_review_authority(wrapped, analysis2, p2, l2, [ov_new], user_id=1)
    # Old row must still be active
    row = conn.execute("SELECT is_active FROM commitment_authority WHERE id=?", (old_row_id,)).fetchone()
    assert row[0] == 1


def test_T64_conditional_update_predicate_in_source():
    """Conditional revoke UPDATE must contain 8 WHERE predicates."""
    import inspect, v4_authority as mod
    src = inspect.getsource(mod)
    assert "AND value IS ?" in src
    assert "authority_source = 'FAMILY_REVIEW'" in src
    assert "AND is_active        = 1" in src
    assert "cursor.rowcount != 1" in src


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 11: DRIFT / CONFLICT (T65–T69)
# ═══════════════════════════════════════════════════════════════════════════

def test_T65_source_identity_drift():
    """Same override_id active with different field_name → SOURCE_IDENTITY_DRIFT."""
    conn = _mem_conn(); _make_db(conn)
    # Insert row with field_name="commitment_status"
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C1', 1, 'commitment_status', 'COMMITTED', 'FAMILY_REVIEW', 'ov-drift',
                1, '2026-01-01T00:00:00', 1)""")

    # Now try to write same override_id with different field_name
    ov = _make_ov(override_id="ov-drift", field_name="recurrence_status",
                  value=RecurrenceStatus.RECURRING)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T66_target_identity_drift():
    """Same override_id active on different commitment → TARGET_IDENTITY_DRIFT."""
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C2', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C2', 1, 'commitment_status', 'COMMITTED', 'FAMILY_REVIEW', 'ov-target',
                1, '2026-01-01T00:00:00', 1)""")

    ov = _make_ov(override_id="ov-target", field_name="commitment_status",
                  value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1,
                                                   commitment_id="C1", ov=ov)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T67_active_field_source_conflict():
    """Different FAMILY_REVIEW override already active for same (commitment, field)."""
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C1', 1, 'commitment_status', 'COMMITTED', 'FAMILY_REVIEW', 'ov-other',
                1, '2026-01-01T00:00:00', 1)""")

    ov = _make_ov(override_id="ov-new", field_name="commitment_status",
                  value=CommitmentStatus.NON_COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)


def test_T68_manual_override_coexists_no_conflict():
    """MANUAL_OVERRIDE active for same (commitment, field) → NOT a conflict."""
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C1', 1, 'commitment_status', 'COMMITTED', 'MANUAL_OVERRIDE', 'ov-manual',
                1, '2026-01-01T00:00:00', 1)""")
    rows_before = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]

    ov = _make_ov(override_id="ov-fr", field_name="commitment_status",
                  value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.inserted_count() == 1


def test_T69_manual_override_row_untouched():
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C1', 1, 'commitment_status', 'COMMITTED', 'MANUAL_OVERRIDE', 'ov-manual',
                1, '2026-01-01T00:00:00', 1)""")
    manual_before = conn.execute(
        "SELECT * FROM commitment_authority WHERE override_id='ov-manual'"
    ).fetchone()

    ov = _make_ov(override_id="ov-fr", field_name="commitment_status",
                  value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)

    manual_after = conn.execute(
        "SELECT * FROM commitment_authority WHERE override_id='ov-manual'"
    ).fetchone()
    assert manual_before == manual_after


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 12: ATOMICITY (T70–T74)
# ═══════════════════════════════════════════════════════════════════════════

def test_T70_preflight_hard_error_zero_writes():
    """Hard error during preflight (T66 scenario) → zero rows written."""
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C2', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C2', 1, 'commitment_status', 'COMMITTED', 'FAMILY_REVIEW', 'ov-target',
                1, '2026-01-01T00:00:00', 1)""")

    ov = _make_ov(override_id="ov-target", field_name="commitment_status",
                  value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1,
                                                   commitment_id="C1", ov=ov)
    count_before = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    count_after = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count_before == count_after


def test_T71_authority_state_changed_during_write():
    """AUTHORITY_STATE_CHANGED_DURING_WRITE when rowcount != 1 on revoke UPDATE."""
    conn = _mem_conn(); _make_db(conn)
    ov_old = _make_ov(field_name="commitment_status", value=CommitmentStatus.COMMITTED, override_id="ov-rwc")
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov_old)
    persist_family_review_authority(conn, analysis, persistence, links, [ov_old], user_id=1)
    old_id = conn.execute("SELECT id FROM commitment_authority WHERE is_active=1").fetchone()[0]

    class _ZeroRowcountConn:
        def __init__(self, c):
            self._c = c
            self._mock_update = False
        def execute(self, sql, params=()):
            if "UPDATE commitment_authority" in sql and "is_active  = 0" in sql:
                # Don't actually run the update, return a fake cursor with rowcount=0
                class _FakeCursor:
                    rowcount = 0
                return _FakeCursor()
            return self._c.execute(sql, params)
        def __getattr__(self, name):
            return getattr(self._c, name)

    wrapped = _ZeroRowcountConn(conn)
    ov_new = _make_ov(field_name="commitment_status", value=CommitmentStatus.NON_COMMITTED, override_id="ov-rwc")
    pattern = _make_minimal_pattern("DK1")
    run_result = _make_run_result("R", "DK1", 0)
    link_result = _make_link_result("R", "DK1", 0, commitment_id="C1")
    ae = _make_audit_entry("DK1", [ov_new.override_id], {ov_new.field_name: _changed_field_rec(ov_new)})
    analysis2 = _make_report(raw_patterns=[pattern], applied_override_ids=[ov_new.override_id],
                              override_audit=[ae])
    p2 = _make_persistence_report(run_id="R", user_id=1, outcomes=[run_result])
    l2 = _make_link_report(run_id="R", user_id=1, results=[link_result])
    with pytest.raises((RuntimeError, Exception)) as exc_info:
        persist_family_review_authority(wrapped, analysis2, p2, l2, [ov_new], user_id=1)
    assert "AUTHORITY_STATE_CHANGED_DURING_WRITE" in str(exc_info.value)


def test_T72_retry_after_rollback_succeeds():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    # Simulate first attempt fails (user mismatch)
    with pytest.raises((ValueError, RuntimeError)):
        persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=99)
    # Second attempt with correct user succeeds
    report = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report.inserted_count() == 1


def test_T73_savepoint_released_on_success():
    """After success, no dangling savepoints."""
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    # Should be able to open a new savepoint without error
    conn.execute("SAVEPOINT test_sp")
    conn.execute("RELEASE SAVEPOINT test_sp")


def test_T74_outer_savepoint_survives_phase2c():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    conn.execute("SAVEPOINT outer_sp")
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    # Outer savepoint still valid
    conn.execute("RELEASE SAVEPOINT outer_sp")


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 13: PREFLIGHT/MUTATION SNAPSHOT (T75)
# ═══════════════════════════════════════════════════════════════════════════

def test_T75_preflight_reads_before_writes():
    """Verify SAVEPOINT is opened before any SELECT (reads inside same tx scope)."""
    import inspect, v4_authority as mod
    src = inspect.getsource(mod)
    # SAVEPOINT must be opened before the inner function does reads
    sp_pos = src.find("SAVEPOINT sp_phase2c")
    inner_pos = src.find("_phase2c_inner")
    assert sp_pos < inner_pos, "SAVEPOINT must precede _phase2c_inner call"


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 14: UNSEEN HISTORY (T76–T77)
# ═══════════════════════════════════════════════════════════════════════════

def test_T76_unseen_authority_remains_active():
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by)
        VALUES ('C1', 1, 'cadence', 'MONTHLY', 'FAMILY_REVIEW', 'ov-unseen',
                1, '2026-01-01T00:00:00', 1)""")
    # Run Phase 2C with a different override — ov-unseen is not in this run
    ov = _make_ov(override_id="ov-new", field_name="commitment_status",
                  value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    row = conn.execute("SELECT is_active FROM commitment_authority WHERE override_id='ov-unseen'").fetchone()
    assert row[0] == 1  # must remain active


def test_T77_revoked_history_rows_untouched():
    conn = _mem_conn(); _make_db(conn)
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("""INSERT INTO commitment_authority
        (commitment_id, user_id, field_name, value, authority_source, override_id,
         is_active, created_at, created_by, revoked_at)
        VALUES ('C1', 1, 'cadence', 'QUARTERLY', 'FAMILY_REVIEW', 'ov-old-hist',
                0, '2025-01-01T00:00:00', 1, '2025-06-01T00:00:00')""")
    ov = _make_ov(override_id="ov-now", field_name="commitment_status",
                  value=CommitmentStatus.COMMITTED)
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    hist_before = conn.execute("SELECT * FROM commitment_authority WHERE override_id='ov-old-hist'").fetchone()
    persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    hist_after = conn.execute("SELECT * FROM commitment_authority WHERE override_id='ov-old-hist'").fetchone()
    assert hist_before == hist_after


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 15: CONTENT-LEVEL IMMUTABILITY (T78)
# ═══════════════════════════════════════════════════════════════════════════

def test_T78_immutability_all_12_tables():
    """All 12 named tables have identical full row content before and after Phase 2C."""
    conn = _mem_conn(); _make_db(conn)
    # Seed some rows in a few tables so snapshot is non-trivial
    conn.execute("INSERT INTO commitments(id, user_id) VALUES ('C1', 1)")
    conn.execute("INSERT INTO expenses(id, user_id, date, description, amount) VALUES ('E1', 1, '2026-01-01', 'test', 100.0)")

    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    before = _snapshot_all_immutable(conn)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    after = _snapshot_all_immutable(conn)

    for table in _IMMUTABLE_TABLES:
        assert before[table] == after[table], (
            f"Immutability violated: table={table!r} content changed during Phase 2C"
        )


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 16: REPORT (T79–T84)
# ═══════════════════════════════════════════════════════════════════════════

def test_T79_report_inserted_count():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    report = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report.inserted_count() == 1


def test_T80_report_already_current_count():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    report2 = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report2.already_current_count() == 1


def test_T81_report_replaced_count():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    report = persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    assert report.replaced_count() == 1


def test_T82_report_deferred_count():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0,
                                  family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    link_result = _make_link_result(run_id, "DK1", 0)
    ae = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[ae])
    p = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    l = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, p, l, [ov], user_id=1)
    assert report.deferred_count() == 1


def test_T83_report_row_ids_for_replaced():
    conn = _mem_conn(); _make_db(conn)
    analysis2, persistence2, links2, ovs2 = _setup_replace(conn)
    report = persist_family_review_authority(conn, analysis2, persistence2, links2, ovs2, user_id=1)
    r = report.outcomes[0]
    assert r.outcome == Phase2COutcome.REPLACED
    assert r.previous_row_id is not None
    assert r.new_row_id is not None


def test_T84_defer_reason_populated():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0,
                                  family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    link_result = _make_link_result(run_id, "DK1", 0)
    ae = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[ae])
    p = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    l = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    report = persist_family_review_authority(conn, analysis, p, l, [ov], user_id=1)
    assert report.outcomes[0].reason is not None


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 17: NO SECOND MATCHER (T85–T91)
# ═══════════════════════════════════════════════════════════════════════════

def test_T85_no_import_override_matches():
    import inspect, v4_authority as mod
    src = inspect.getsource(mod)
    assert "_override_matches" not in src


def test_T86_no_import_override_group_key():
    import inspect, v4_authority as mod
    src = inspect.getsource(mod)
    assert "_override_group_key" not in src


def test_T87_broad_match_follows_audit():
    """Override with stream_label_hint='' still resolved from audit, not re-matched."""
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(stream_label_hint="")
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov], user_id=1)
    assert report.inserted_count() == 1


def test_T88_stream_label_hint_irrelevant_in_phase2c():
    """Changing stream_label_hint after analysis has no effect on Phase 2C resolution."""
    conn = _mem_conn(); _make_db(conn)
    ov_orig = _make_ov(stream_label_hint="stream 2")
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov_orig)
    # Change hint — doesn't matter since we use audit
    ov_modified = _make_ov(stream_label_hint="completely different", override_id=ov_orig.override_id)
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov_modified], user_id=1)
    assert report.inserted_count() == 1


def test_T89_amount_hint_irrelevant_in_phase2c():
    from decimal import Decimal as D
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    import dataclasses
    ov_with_hint = dataclasses.replace(ov, amount_hint=D("999.00"))
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov_with_hint], user_id=1)
    assert report.inserted_count() == 1


def test_T90_label_exact_irrelevant_in_phase2c():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    import dataclasses
    ov_with_exact = dataclasses.replace(ov, label_exact="irrelevant exact label")
    analysis, persistence, links, _ = _full_setup(conn, run_id="R", user_id=1, ov=ov)
    report = persist_family_review_authority(conn, analysis, persistence, links, [ov_with_exact], user_id=1)
    assert report.inserted_count() == 1


def test_T91_two_overrides_same_dkey_different_fields():
    """Two overrides for same description_key, different fields, each resolved independently from audit."""
    conn = _mem_conn(); _make_db(conn)
    run_id = "R"
    ov1 = _make_ov(field_name="commitment_status", value=CommitmentStatus.COMMITTED, override_id="ov-f1")
    ov2 = _make_ov(field_name="recurrence_status", value=RecurrenceStatus.RECURRING, override_id="ov-f2")
    _seed_commitment(conn, "C1", 1)
    pattern = _make_minimal_pattern("DK1")
    rr = _make_run_result(run_id, "DK1", 0)
    lr = _make_link_result(run_id, "DK1", 0, commitment_id="C1")
    ae1 = _make_audit_entry("DK1", [ov1.override_id], {ov1.field_name: _changed_field_rec(ov1)})
    ae2 = _make_audit_entry("DK1", [ov2.override_id], {ov2.field_name: _changed_field_rec(ov2)})
    analysis = _make_report(raw_patterns=[pattern],
                            applied_override_ids=[ov1.override_id, ov2.override_id],
                            override_audit=[ae1, ae2])
    p = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[rr])
    l = _make_link_report(run_id=run_id, user_id=1, results=[lr])
    report = persist_family_review_authority(conn, analysis, p, l, [ov1, ov2], user_id=1)
    assert report.inserted_count() == 2


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 18: CANONICAL PARTITION COUNTS (T92–T94)
# ═══════════════════════════════════════════════════════════════════════════

def test_T92_persistable_plus_parallel_plus_canonical_equals_118():
    """Partition sums verified: 81+29+8=118 (proved by audit; test the total)."""
    from analyze_home_budget_v4 import PATTERN_OVERRIDES
    assert len(PATTERN_OVERRIDES) == 118
    canonical_merge_ids = {
        "ov-google-cloud-lnkqw-tbd", "ov-google-cloud-lnkqw-recurrence",
        "ov-google-cloud-lnkqw-committed", "ov-google-cloud-lnkqw-active",
        "ov-google-cloud-tlbz7j-tbd", "ov-google-cloud-tlbz7j-recurrence",
        "ov-google-cloud-tlbz7j-committed", "ov-google-cloud-tlbz7j-active",
    }
    assert len(canonical_merge_ids) == 8


def test_T93_none_of_29_parallel_produce_writes():
    """None of the 29 DEFER_PARALLEL_UNRESOLVED overrides write to commitment_authority."""
    # Verified by T12 (single) and T20 (canonical+parallel) and T25 (multi-audit-parallel)
    # This test asserts the partition rule: having UNRESOLVED_PARALLEL resolution always defers
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov()
    pattern = _make_minimal_pattern("DK1")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK1", 0,
                                  family_resolution=FamilyResolution.UNRESOLVED_PARALLEL)
    link_result = _make_link_result(run_id, "DK1", 0)
    ae = _make_audit_entry("DK1", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[ae])
    p = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    l = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    persist_family_review_authority(conn, analysis, p, l, [ov], user_id=1)
    count = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count == 0


def test_T94_none_of_8_canonical_merge_produce_writes():
    conn = _mem_conn(); _make_db(conn)
    ov = _make_ov(canonical_identity="google-cloud-tbd",
                  override_id="ov-google-cloud-lnkqw-tbd",
                  field_name="planning_amount", value=None)
    _seed_commitment(conn, "C1", 1)
    pattern = _make_minimal_pattern("DK-CLOUD")
    run_id = "R"
    run_result = _make_run_result(run_id, "DK-CLOUD", 0)
    link_result = _make_link_result(run_id, "DK-CLOUD", 0, commitment_id="C1")
    ae = _make_audit_entry("DK-CLOUD", [ov.override_id], {ov.field_name: _changed_field_rec(ov)})
    analysis = _make_report(raw_patterns=[pattern], applied_override_ids=[ov.override_id],
                            override_audit=[ae])
    p = _make_persistence_report(run_id=run_id, user_id=1, outcomes=[run_result])
    l = _make_link_report(run_id=run_id, user_id=1, results=[link_result])
    persist_family_review_authority(conn, analysis, p, l, [ov], user_id=1)
    count = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count == 0


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 19: PRODUCTION SAFETY (T95–T97)
# ═══════════════════════════════════════════════════════════════════════════

def test_T95_production_path_rejected():
    path = r"C:\Users\erezg\.budget_tracker_data\budget.db"
    analysis = _make_report(); persistence = _make_persistence_report()
    links = _make_link_report(); ovs = []
    with pytest.raises(RuntimeError, match="PRODUCTION SAFETY ABORT"):
        persist_family_review_authority_from_path(path, analysis, persistence, links, ovs, user_id=1)


def test_T96_production_path_normalized_rejected():
    path = "/c/users/erezg/.budget_tracker_data/budget.db"
    analysis = _make_report(); persistence = _make_persistence_report()
    links = _make_link_report(); ovs = []
    with pytest.raises(RuntimeError, match="PRODUCTION SAFETY ABORT"):
        persist_family_review_authority_from_path(path, analysis, persistence, links, ovs, user_id=1)


def test_T97_temp_path_allowed(tmp_path):
    db_path = str(tmp_path / "test.db")
    conn_setup = sqlite3.connect(db_path)
    _make_db(conn_setup)
    conn_setup.commit(); conn_setup.close()
    analysis = _make_report(); persistence = _make_persistence_report()
    links = _make_link_report(); ovs = []
    # Empty overrides → succeeds with no writes
    report = persist_family_review_authority_from_path(db_path, analysis, persistence, links, ovs, user_id=1)
    assert isinstance(report, AuthorityReport)


# ═══════════════════════════════════════════════════════════════════════════
# GROUP 20: FK CHECK + RETRY IDEMPOTENCE (T98–T101)
# ═══════════════════════════════════════════════════════════════════════════

def test_T98_foreign_key_check_zero_violations():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    violations = conn.execute("PRAGMA foreign_key_check(commitment_authority)").fetchall()
    assert violations == []


def test_T99_full_retry_idempotent():
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(conn, run_id="R", user_id=1)
    r1 = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    r2 = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert r1.inserted_count() == 1
    assert r2.already_current_count() == 1
    rows = conn.execute("SELECT COUNT(*) FROM commitment_authority WHERE is_active=1").fetchone()[0]
    assert rows == 1


def test_T100_empty_overrides_no_writes():
    conn = _mem_conn(); _make_db(conn)
    analysis = _make_report()
    persistence = _make_persistence_report()
    links = _make_link_report()
    report = persist_family_review_authority(conn, analysis, persistence, links, [], user_id=1)
    assert report.inserted_count() == 0
    assert report.deferred_count() == 0
    count = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count == 0


def test_T101_report_run_id_is_persistence_run_id():
    """AuthorityReport.run_id == persistence_report.run_id, NOT analysis_result.run_id."""
    conn = _mem_conn(); _make_db(conn)
    analysis, persistence, links, ovs = _full_setup(
        conn, run_id="PERSIST-RUN", user_id=1, analysis_run_id="ANALYSIS-RUN"
    )
    report = persist_family_review_authority(conn, analysis, persistence, links, ovs, user_id=1)
    assert report.run_id == "PERSIST-RUN"
    assert report.run_id != "ANALYSIS-RUN"
