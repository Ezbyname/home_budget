"""
Phase 2A — V4 Evidence Persistence + Pattern Family Identity
Test suite for v4_persistence.py

All tests use temporary SQLite databases.
Zero production DB access.
"""

from __future__ import annotations

import importlib
import json
import sqlite3
import sys
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Optional

import pytest

# ── Module fixtures ───────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def app_mod():
    import app as a
    return a

@pytest.fixture(scope="session")
def persist_mod():
    import v4_persistence as m
    return m

@pytest.fixture(scope="session")
def contracts_mod():
    from intelligence import v4_contracts as c
    return c

# ── DB helpers ────────────────────────────────────────────────────────────────

def _fresh_db(app_mod, tmp_path: Path) -> str:
    db = str(tmp_path / "test.db")
    orig = app_mod.DB_PATH
    app_mod.DB_PATH = db
    app_mod.init_db()
    app_mod.DB_PATH = orig
    return db

def _conn(db: str) -> sqlite3.Connection:
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys = ON")
    return c

def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]

# ── Minimal PatternResult / ClassificationReport factory ──────────────────────

def _make_pattern(contracts_mod, description_key: str, *,
                  recurrence=None, commitment=None, cadence=None,
                  lifecycle=None, budget_class=None, amount_behavior=None,
                  planning_amount=None, reserve_eligible=False,
                  monthly_reserve_contrib=None, family_review_required=False,
                  review_reasons=(), label: str = "", stream_index: int = 0):
    c = contracts_mod
    return c.PatternResult(
        description_key=description_key,
        label=label or description_key,
        recurrence_status=recurrence or c.RecurrenceStatus.UNKNOWN,
        commitment_status=commitment or c.CommitmentStatus.UNCERTAIN,
        amount_behavior=amount_behavior or c.AmountBehavior.UNKNOWN,
        budget_class=budget_class or c.BudgetClass.NON_RECURRING_EXPENSE,
        lifecycle_status=lifecycle or c.LifecycleStatus.UNKNOWN,
        purpose_type=c.PurposeType.UNKNOWN,
        cadence=cadence or c.Cadence.UNKNOWN,
        planning_amount=planning_amount,
        member_ids=(),
        membership_confidence={},
        evidence_sources=(),
        decision_source=c.DecisionSource.CLASSIFIER,
        family_review_required=family_review_required,
        review_reasons=review_reasons,
        reserve_eligible=reserve_eligible,
        monthly_reserve_contrib=monthly_reserve_contrib or Decimal("0"),
        canonical_identity=None,
    )

def _make_report(contracts_mod, patterns, *, run_id: Optional[str] = None):
    c = contracts_mod
    zero = Decimal("0")
    rec_rec = c.make_reconciliation_record(
        field="planning_income", reviewed_value=zero, derived_value=zero, raw_derived_value=zero
    )
    raw = c.RawClassifierOutput(
        patterns=tuple(patterns),
        income_streams=(),
        planning_income_raw=zero,
        monthly_reserve_raw=zero,
        family_review_items=(),
    )
    eff = c.EffectiveFinancialResult(
        patterns=tuple(patterns),
        income_streams=(),
        planning_income_effective=zero,
        monthly_reserve_effective=zero,
        family_review_items=(),
        overrides_applied=(),
    )
    rec = c.ReconciliationReport(
        planning_income=rec_rec,
        monthly_reserve=c.make_reconciliation_record(
            field="monthly_reserve", reviewed_value=zero, derived_value=zero, raw_derived_value=zero
        ),
    )
    return c.ClassificationReport(
        classifier_version="v4.0-test",
        run_id=run_id or str(uuid.uuid4()),
        analysis_db=":memory:",
        run_at="2024-01-01T00:00:00",
        raw=raw,
        effective=eff,
        reconciliation=rec,
        override_audit=(),
    )

# ═════════════════════════════════════════════════════════════════════════════
# RUN ID TESTS (1–4)
# ═════════════════════════════════════════════════════════════════════════════

class TestRunId:
    def test_missing_run_id_generates_uuid4(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """No run_id supplied → PersistenceReport.run_id is a valid UUID."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p])
        result = persist_mod.persist_run(db, report, user_id=1)
        uuid.UUID(result.run_id)  # raises if not valid UUID

    def test_supplied_run_id_preserved(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Supplied run_id is kept exactly."""
        db = _fresh_db(app_mod, tmp_path)
        fixed = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p], run_id=fixed)
        result = persist_mod.persist_run(db, report, user_id=1, run_id=fixed)
        assert result.run_id == fixed

    def test_same_run_id_twice_no_duplicate_evidence(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Second persist with same run_id → no new rows."""
        db = _fresh_db(app_mod, tmp_path)
        fixed = str(uuid.uuid4())
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p], run_id=fixed)
        persist_mod.persist_run(db, report, user_id=1, run_id=fixed)
        persist_mod.persist_run(db, report, user_id=1, run_id=fixed)
        c = _conn(db)
        assert _count(c, "v4_run_results") == 1
        assert _count(c, "pattern_families") == 1
        c.close()

    def test_different_run_id_creates_new_evidence(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Two different run_ids → two v4_run_results rows, one shared family."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "netflix")
        r1 = _make_report(contracts_mod, [p])
        r2 = _make_report(contracts_mod, [p])
        assert r1.run_id != r2.run_id
        persist_mod.persist_run(db, r1, user_id=1)
        persist_mod.persist_run(db, r2, user_id=1)
        c = _conn(db)
        assert _count(c, "v4_run_results") == 2   # two historical records
        assert _count(c, "pattern_families") == 1  # same family matched on second run
        c.close()

# ═════════════════════════════════════════════════════════════════════════════
# RAW EVIDENCE TESTS (5–7)
# ═════════════════════════════════════════════════════════════════════════════

class TestRawEvidence:
    def test_run_result_receives_raw_classifier_values(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """v4_run_results row stores actual PatternResult field values."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(
            contracts_mod, "mortgage",
            recurrence=contracts_mod.RecurrenceStatus.RECURRING,
            commitment=contracts_mod.CommitmentStatus.COMMITTED,
            cadence=contracts_mod.Cadence.MONTHLY,
            lifecycle=contracts_mod.LifecycleStatus.ACTIVE,
            budget_class=contracts_mod.BudgetClass.FIXED_AMOUNT_RECURRING,
            planning_amount=Decimal("1500.00"),
            reserve_eligible=True,
            monthly_reserve_contrib=Decimal("1500.00"),
        )
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        cur = c.execute("SELECT * FROM v4_run_results")
        col = [d[0] for d in cur.description]
        row = cur.fetchone()
        c.close()
        d = dict(zip(col, row))
        assert d["description_key"] == "mortgage"
        assert d["recurrence_status"] == "RECURRING"
        assert d["commitment_status"] == "COMMITTED"
        assert d["cadence"] == "monthly"
        assert d["classifier_lifecycle_status"] == "ACTIVE"
        assert d["budget_class"] == "FIXED_AMOUNT_RECURRING"
        assert d["planning_amount_agorot"] == 150000
        assert d["reserve_eligible"] == 1
        assert d["monthly_reserve_contrib_agorot"] == 150000

    def test_raw_values_not_modified_by_authority(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Persisted row reflects raw classifier output, not effective authority."""
        db = _fresh_db(app_mod, tmp_path)
        # Pattern with CLASSIFIER decision_source (not FAMILY_REVIEW)
        p = _make_pattern(
            contracts_mod, "spotify",
            recurrence=contracts_mod.RecurrenceStatus.POSSIBLE_RECURRING,
            commitment=contracts_mod.CommitmentStatus.UNCERTAIN,
        )
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        row = c.execute(
            "SELECT recurrence_status, commitment_status FROM v4_run_results"
        ).fetchone()
        c.close()
        assert row[0] == "POSSIBLE_RECURRING"
        assert row[1] == "UNCERTAIN"

    def test_unknown_defaults_preserved(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """UNKNOWN/UNCERTAIN safe defaults written as exact raw classifier values."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "mystery_vendor")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        row = c.execute(
            "SELECT recurrence_status, commitment_status, cadence, "
            "classifier_lifecycle_status, reserve_eligible, review_required "
            "FROM v4_run_results"
        ).fetchone()
        c.close()
        assert row[0] == "UNKNOWN"
        assert row[1] == "UNCERTAIN"
        assert row[2] == "unknown"
        # Phase 0.3: schema now accepts UNKNOWN exactly — no coercion to ACTIVE
        assert row[3] == "UNKNOWN"
        assert row[4] == 0
        assert row[5] == 0

# ═════════════════════════════════════════════════════════════════════════════
# LIFECYCLE RAW PERSISTENCE TESTS
# Prove that every actual V4 LifecycleStatus value is persisted exactly,
# with no coercion.  Phase 0.3 expanded the schema CHECK to allow all five
# V4 lifecycle values (plus PAUSED as a forward-compat schema value).
# ═════════════════════════════════════════════════════════════════════════════

class TestLifecycleRawPersistence:
    @pytest.mark.parametrize("lc_name,expected", [
        ("ACTIVE",           "ACTIVE"),
        ("POSSIBLY_STOPPED", "POSSIBLY_STOPPED"),
        ("CANCELLED",        "CANCELLED"),
        ("ENDED",            "ENDED"),
        ("UNKNOWN",          "UNKNOWN"),
    ])
    def test_lifecycle_persisted_exactly(
            self, lc_name, expected,
            persist_mod, app_mod, contracts_mod, tmp_path):
        """Each V4 LifecycleStatus value is stored verbatim — no coercion."""
        db = _fresh_db(app_mod, tmp_path)
        lc = getattr(contracts_mod.LifecycleStatus, lc_name)
        p = _make_pattern(contracts_mod, f"vendor_{lc_name}", lifecycle=lc)
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        stored = c.execute(
            "SELECT classifier_lifecycle_status FROM v4_run_results"
        ).fetchone()[0]
        c.close()
        assert stored == expected, (
            f"LifecycleStatus.{lc_name} should persist as {expected!r}, got {stored!r}"
        )

    def test_no_lifecycle_coerced_to_active(
            self, persist_mod, app_mod, contracts_mod, tmp_path):
        """UNKNOWN and POSSIBLY_STOPPED must NOT be stored as ACTIVE."""
        db = _fresh_db(app_mod, tmp_path)
        for lc in (contracts_mod.LifecycleStatus.UNKNOWN,
                   contracts_mod.LifecycleStatus.POSSIBLY_STOPPED):
            p = _make_pattern(contracts_mod, f"coerce_test_{lc.value}", lifecycle=lc)
            report = _make_report(contracts_mod, [p])
            persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        stored = {
            r[0]
            for r in c.execute("SELECT classifier_lifecycle_status FROM v4_run_results")
        }
        c.close()
        assert "ACTIVE" not in stored, (
            "Neither UNKNOWN nor POSSIBLY_STOPPED should be coerced to ACTIVE"
        )
        assert "UNKNOWN" in stored
        assert "POSSIBLY_STOPPED" in stored


# ═════════════════════════════════════════════════════════════════════════════
# SINGLE FAMILY TESTS (8–10)
# ═════════════════════════════════════════════════════════════════════════════

class TestSingleFamily:
    def test_one_stream_creates_one_active_family(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Single stream → exactly one ACTIVE non-split family created."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "hot_mobile")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        families = c.execute(
            "SELECT id, family_status, is_split_discriminator FROM pattern_families"
        ).fetchall()
        c.close()
        assert len(families) == 1
        assert families[0][1] == "ACTIVE"
        assert families[0][2] == 0

    def test_same_stream_rerun_matches_same_family(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Second run with same description_key reuses existing ACTIVE family."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "hot_mobile")
        r1 = _make_report(contracts_mod, [p])
        r2 = _make_report(contracts_mod, [p])
        out1 = persist_mod.persist_run(db, r1, user_id=1)
        out2 = persist_mod.persist_run(db, r2, user_id=1)
        assert out1.outcomes[0].family_id == out2.outcomes[0].family_id
        c = _conn(db)
        assert _count(c, "pattern_families") == 1
        c.close()

    def test_no_duplicate_family_on_same_run_retry(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Same run_id retried → single family, single run_result."""
        db = _fresh_db(app_mod, tmp_path)
        fixed_run = str(uuid.uuid4())
        p = _make_pattern(contracts_mod, "hot_mobile")
        report = _make_report(contracts_mod, [p], run_id=fixed_run)
        persist_mod.persist_run(db, report, user_id=1, run_id=fixed_run)
        persist_mod.persist_run(db, report, user_id=1, run_id=fixed_run)
        c = _conn(db)
        assert _count(c, "pattern_families") == 1
        assert _count(c, "v4_run_results") == 1
        c.close()

# ═════════════════════════════════════════════════════════════════════════════
# PARALLEL STREAM TESTS (11–14)
# ═════════════════════════════════════════════════════════════════════════════

class TestParallelStreams:
    def _two_parallel_patterns(self, contracts_mod):
        p1 = _make_pattern(contracts_mod, "gal_naomi",
                           planning_amount=Decimal("607"), label="gal naomi stream 1")
        p2 = _make_pattern(contracts_mod, "gal_naomi",
                           planning_amount=Decimal("2000"), label="gal naomi stream 2")
        return p1, p2

    def test_parallel_streams_not_collapsed_into_one_nonsplit_family(
            self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Two streams on same description_key → no non-split family created."""
        db = _fresh_db(app_mod, tmp_path)
        p1, p2 = self._two_parallel_patterns(contracts_mod)
        report = _make_report(contracts_mod, [p1, p2])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        nonsplit = c.execute(
            "SELECT COUNT(*) FROM pattern_families WHERE is_split_discriminator=0"
        ).fetchone()[0]
        c.close()
        assert nonsplit == 0

    def test_parallel_streams_family_id_null(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Parallel streams get family_id=NULL in v4_run_results."""
        db = _fresh_db(app_mod, tmp_path)
        p1, p2 = self._two_parallel_patterns(contracts_mod)
        report = _make_report(contracts_mod, [p1, p2])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        nulls = c.execute(
            "SELECT COUNT(*) FROM v4_run_results WHERE family_id IS NULL"
        ).fetchone()[0]
        c.close()
        assert nulls == 2

    def test_parallel_streams_resolution_unresolved(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Parallel streams reported as UNRESOLVED_PARALLEL."""
        db = _fresh_db(app_mod, tmp_path)
        p1, p2 = self._two_parallel_patterns(contracts_mod)
        report = _make_report(contracts_mod, [p1, p2])
        out = persist_mod.persist_run(db, report, user_id=1)
        assert all(
            o.family_resolution == persist_mod.FamilyResolution.UNRESOLVED_PARALLEL
            for o in out.outcomes
        )

    def test_parallel_streams_no_invented_amount_cluster(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """No split family with amount_cluster_agorot invented for parallel streams."""
        db = _fresh_db(app_mod, tmp_path)
        p1, p2 = self._two_parallel_patterns(contracts_mod)
        report = _make_report(contracts_mod, [p1, p2])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        split_with_amount = c.execute(
            "SELECT COUNT(*) FROM pattern_families "
            "WHERE is_split_discriminator=1 AND amount_cluster_agorot IS NOT NULL"
        ).fetchone()[0]
        c.close()
        assert split_with_amount == 0

# ═════════════════════════════════════════════════════════════════════════════
# LIFECYCLE TESTS (15–17)
# ═════════════════════════════════════════════════════════════════════════════

class TestLifecycle:
    def _seed_superseded_family(self, db: str, user_id: int, description_key: str) -> str:
        """Insert a SUPERSEDED family directly and return its id."""
        fid = str(uuid.uuid4())
        now = "2024-01-01T00:00:00"
        c = _conn(db)
        c.execute("""
            INSERT INTO pattern_families
                (id, user_id, primary_description_key, is_split_discriminator,
                 amount_cluster_agorot, window_start, window_end,
                 commitment_id, is_primary, linked_by, family_status,
                 superseded_at, superseded_by_event_id, created_at, updated_at)
            VALUES (?,?,?,0,NULL,NULL,NULL,NULL,1,'AUTO','SUPERSEDED',?,NULL,?,?)
        """, (fid, user_id, description_key, now, now, now))
        c.commit()
        c.close()
        return fid

    def test_superseded_family_ignored_by_matcher(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """New run creates a fresh ACTIVE family, ignores existing SUPERSEDED one."""
        db = _fresh_db(app_mod, tmp_path)
        sup_id = self._seed_superseded_family(db, 1, "electricity")
        p = _make_pattern(contracts_mod, "electricity")
        report = _make_report(contracts_mod, [p])
        out = persist_mod.persist_run(db, report, user_id=1)
        new_family_id = out.outcomes[0].family_id
        assert new_family_id != sup_id
        c = _conn(db)
        status = c.execute(
            "SELECT family_status FROM pattern_families WHERE id=?", (new_family_id,)
        ).fetchone()[0]
        c.close()
        assert status == "ACTIVE"

    def test_historical_run_results_remain_on_superseded_family(
            self, persist_mod, app_mod, contracts_mod, tmp_path):
        """v4_run_results rows pointing to a superseded family stay unchanged."""
        db = _fresh_db(app_mod, tmp_path)
        sup_id = self._seed_superseded_family(db, 1, "electricity")
        # Manually plant a run_result on the superseded family
        now = "2024-01-01T00:00:00"
        old_rrid = str(uuid.uuid4())
        c = _conn(db)
        c.execute("""
            INSERT INTO v4_run_results
                (id, run_id, user_id, family_id, description_key, stream_index,
                 label, planning_amount_agorot, cadence, recurrence_status,
                 commitment_status, classifier_lifecycle_status, budget_class,
                 reserve_eligible, monthly_reserve_contrib_agorot,
                 cadence_coverage, evidence_month_count,
                 review_required, review_reasons, created_at)
            VALUES (?,?,?,?,?,0,'old',NULL,'unknown','UNKNOWN','UNCERTAIN',
                    'ACTIVE','NON_RECURRING_EXPENSE',0,0,NULL,NULL,0,'[]',?)
        """, (old_rrid, str(uuid.uuid4()), 1, sup_id, "electricity", now))
        c.commit(); c.close()

        # New run
        p = _make_pattern(contracts_mod, "electricity")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)

        c = _conn(db)
        attached = c.execute(
            "SELECT family_id FROM v4_run_results WHERE id=?", (old_rrid,)
        ).fetchone()[0]
        c.close()
        assert attached == sup_id  # historical attachment unchanged

    def test_active_family_remains_matchable(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Third run on same key still finds the ACTIVE family (not creating a third)."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "water")
        r1 = _make_report(contracts_mod, [p])
        r2 = _make_report(contracts_mod, [p])
        r3 = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, r1, user_id=1)
        persist_mod.persist_run(db, r2, user_id=1)
        persist_mod.persist_run(db, r3, user_id=1)
        c = _conn(db)
        assert _count(c, "pattern_families") == 1
        assert _count(c, "v4_run_results") == 3
        c.close()

# ═════════════════════════════════════════════════════════════════════════════
# OWNERSHIP TESTS (18–19)
# ═════════════════════════════════════════════════════════════════════════════

class TestOwnership:
    def test_cross_user_family_matching_never_occurs(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """User 1's family is not reused for user 2's run."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "netflix")
        r1 = _make_report(contracts_mod, [p])
        r2 = _make_report(contracts_mod, [p])
        out1 = persist_mod.persist_run(db, r1, user_id=1)
        out2 = persist_mod.persist_run(db, r2, user_id=2)
        assert out1.outcomes[0].family_id != out2.outcomes[0].family_id
        c = _conn(db)
        assert _count(c, "pattern_families") == 2
        c.close()

    def test_user_a_result_cannot_attach_to_user_b_family(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Run result for user 2 has user_id=2, never user_id=1."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=2)
        c = _conn(db)
        uid = c.execute("SELECT user_id FROM v4_run_results").fetchone()[0]
        c.close()
        assert uid == 2

# ═════════════════════════════════════════════════════════════════════════════
# COMMITMENT BOUNDARY TESTS (20–23)
# ═════════════════════════════════════════════════════════════════════════════

class TestCommitmentBoundary:
    def test_commitments_count_unchanged(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Phase 2A writes ZERO new commitments."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "mortgage",
                          recurrence=contracts_mod.RecurrenceStatus.RECURRING,
                          commitment=contracts_mod.CommitmentStatus.COMMITTED)
        report = _make_report(contracts_mod, [p])
        c = _conn(db)
        before = _count(c, "commitments")
        c.close()
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        after = _count(c, "commitments")
        c.close()
        assert before == after == 0

    def test_migrated_installment_commitments_unchanged(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """A pre-existing migrated commitment is untouched by Phase 2A."""
        db = _fresh_db(app_mod, tmp_path)
        # Manually insert a migrated installment commitment
        cid = str(uuid.uuid4())
        now = "2024-01-01T00:00:00"
        c = _conn(db)
        c.execute("""
            INSERT INTO users (id, username, password_hash, email)
            VALUES (1, 'u', 'x', 'u@u.com')
        """)
        c.execute("""
            INSERT INTO commitments
                (id, user_id, canonical_label, source_type, is_finite,
                 total_occurrences, linked_legacy_installment_id, created_at, updated_at)
            VALUES (?,1,'mortgage','MIGRATED',1,12,NULL,?,?)
        """, (cid, now, now))
        c.commit(); c.close()

        p = _make_pattern(contracts_mod, "mortgage")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)

        c = _conn(db)
        rows = c.execute("SELECT id, source_type FROM commitments").fetchall()
        c.close()
        assert len(rows) == 1
        assert rows[0][0] == cid
        assert rows[0][1] == "MIGRATED"

    def test_commitment_occurrences_unchanged(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Phase 2A does not touch commitment_occurrences."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "any")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        assert _count(c, "commitment_occurrences") == 0
        c.close()

    def test_commitment_expense_links_unchanged(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Phase 2A does not touch commitment_expense_links."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "any")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        assert _count(c, "commitment_expense_links") == 0
        c.close()

# ═════════════════════════════════════════════════════════════════════════════
# OTHER TABLE BOUNDARY TESTS (24–28)
# ═════════════════════════════════════════════════════════════════════════════

class TestOtherTableBoundaries:
    def _persist_one(self, persist_mod, app_mod, contracts_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "netflix",
                          recurrence=contracts_mod.RecurrenceStatus.RECURRING,
                          commitment=contracts_mod.CommitmentStatus.COMMITTED)
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        return db

    def test_commitment_authority_zero_rows(self, persist_mod, app_mod, contracts_mod, tmp_path):
        db = self._persist_one(persist_mod, app_mod, contracts_mod, tmp_path)
        c = _conn(db)
        assert _count(c, "commitment_authority") == 0
        c.close()

    def test_commitment_classifier_snapshots_zero_rows(self, persist_mod, app_mod, contracts_mod, tmp_path):
        db = self._persist_one(persist_mod, app_mod, contracts_mod, tmp_path)
        c = _conn(db)
        assert _count(c, "commitment_classifier_snapshots") == 0
        c.close()

    def test_commitment_suggestions_zero_rows(self, persist_mod, app_mod, contracts_mod, tmp_path):
        db = self._persist_one(persist_mod, app_mod, contracts_mod, tmp_path)
        c = _conn(db)
        assert _count(c, "commitment_suggestions") == 0
        c.close()

    def test_commitment_link_conflicts_zero_rows(self, persist_mod, app_mod, contracts_mod, tmp_path):
        db = self._persist_one(persist_mod, app_mod, contracts_mod, tmp_path)
        c = _conn(db)
        assert _count(c, "commitment_link_conflicts") == 0
        c.close()

    def test_family_creation_not_based_solely_on_merchant_string(
            self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Two description_keys with same merchant prefix → two distinct families."""
        db = _fresh_db(app_mod, tmp_path)
        p1 = _make_pattern(contracts_mod, "bank_loan_personal")
        p2 = _make_pattern(contracts_mod, "bank_loan_mortgage")
        r1 = _make_report(contracts_mod, [p1])
        r2 = _make_report(contracts_mod, [p2])
        persist_mod.persist_run(db, r1, user_id=1)
        persist_mod.persist_run(db, r2, user_id=1)
        c = _conn(db)
        fids = c.execute("SELECT id FROM pattern_families").fetchall()
        c.close()
        assert len(fids) == 2  # two distinct families, not merged

# ═════════════════════════════════════════════════════════════════════════════
# SAFETY TESTS (29–32)
# ═════════════════════════════════════════════════════════════════════════════

class TestSafety:
    def test_production_db_path_rejected(self, persist_mod, contracts_mod):
        """Known production DB path raises RuntimeError before any write."""
        prod = r"C:\Users\erezg\.budget_tracker_data\budget.db"
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p])
        with pytest.raises(RuntimeError, match="PRODUCTION SAFETY ABORT"):
            persist_mod.persist_run(prod, report, user_id=1)

    def test_foreign_key_check_zero(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """After Phase 2A persistence, no FK violations."""
        db = _fresh_db(app_mod, tmp_path)
        p1 = _make_pattern(contracts_mod, "netflix")
        p2 = _make_pattern(contracts_mod, "electricity")
        report = _make_report(contracts_mod, [p1, p2])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        violations = c.execute("PRAGMA foreign_key_check").fetchall()
        c.close()
        assert violations == []

    def test_legacy_tables_unchanged(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Phase 2A writes nothing to any legacy table."""
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p])
        persist_mod.persist_run(db, report, user_id=1)
        c = _conn(db)
        for table in ("expenses", "income", "installments",
                      "installment_transaction_links", "installment_suggestions"):
            assert _count(c, table) == 0, f"Unexpected rows in legacy table {table}"
        c.close()

    def test_persistence_failure_rolls_back_safely(self, persist_mod, app_mod, contracts_mod, tmp_path):
        """Simulated failure inside persistence leaves DB unchanged."""
        import v4_persistence as m_orig
        db = _fresh_db(app_mod, tmp_path)

        # Monkey-patch _persist_run_result to raise on second call
        call_count = [0]
        original_fn = m_orig._persist_run_result

        def failing_fn(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] >= 2:
                raise RuntimeError("Simulated write failure")
            return original_fn(*args, **kwargs)

        p1 = _make_pattern(contracts_mod, "netflix")
        p2 = _make_pattern(contracts_mod, "electricity")
        report = _make_report(contracts_mod, [p1, p2])

        m_orig._persist_run_result = failing_fn
        try:
            with pytest.raises(RuntimeError, match="Simulated write failure"):
                m_orig.persist_run(db, report, user_id=1)
        finally:
            m_orig._persist_run_result = original_fn

        # DB must be clean — no half-written state
        c = _conn(db)
        assert _count(c, "v4_run_results") == 0
        assert _count(c, "pattern_families") == 0
        c.close()
