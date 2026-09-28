"""
Phase 2B — Deterministic V4 → Existing Commitment Linking
Test suite for v4_linking.py

All tests use temporary SQLite databases.
Zero production DB access.

Test numbering follows the authorised test plan (76 tests).
"""

from __future__ import annotations

import json
import sqlite3
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
def link_mod():
    import v4_linking as m
    return m

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

# ── Data factories ────────────────────────────────────────────────────────────

def _insert_user(conn: sqlite3.Connection, user_id: int = 1) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash) VALUES (?,?,?)",
        (user_id, f"u{user_id}", "x"),
    )
    conn.commit()

def _insert_commitment(conn: sqlite3.Connection, cid: str, user_id: int = 1) -> str:
    conn.execute(
        "INSERT OR IGNORE INTO commitments "
        "(id, user_id, canonical_label, created_at, updated_at) "
        "VALUES (?,?,'test commitment','2024-01-01','2024-01-01')",
        (cid, user_id),
    )
    conn.commit()
    return cid

def _insert_expense(conn: sqlite3.Connection, expense_id: int, user_id: int = 1) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO expenses "
        "(id, date, category_id, description, amount, user_id) "
        "VALUES (?,?,?,?,?,?)",
        (expense_id, "2024-01-15", "cat1", f"exp{expense_id}", 100.0, user_id),
    )
    conn.commit()
    return expense_id

def _insert_cel(
    conn: sqlite3.Connection,
    commitment_id: str,
    expense_id: int,
    user_id: int = 1,
    membership_type: str = "MEMBER",
    linked_by: str = "AUTO",
    family_id: Optional[str] = None,
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO commitment_expense_links "
        "(commitment_id, user_id, expense_id, membership_type, linked_by, family_id, created_at) "
        "VALUES (?,?,?,?,?,?,'2024-01-15')",
        (commitment_id, user_id, expense_id, membership_type, linked_by, family_id),
    )
    conn.commit()

def _insert_family(
    conn: sqlite3.Connection,
    family_id: str,
    user_id: int,
    description_key: str,
    *,
    commitment_id: Optional[str] = None,
    family_status: str = "ACTIVE",
    is_primary: int = 1,
) -> str:
    conn.execute(
        "INSERT OR IGNORE INTO pattern_families "
        "(id, user_id, primary_description_key, is_split_discriminator, "
        " commitment_id, is_primary, linked_by, family_status, "
        " created_at, updated_at) "
        "VALUES (?,?,?,0, ?,?,?,'ACTIVE','2024-01-01','2024-01-01')",
        (family_id, user_id, description_key, commitment_id, is_primary, "AUTO"),
    )
    if family_status == "SUPERSEDED":
        conn.execute(
            "UPDATE pattern_families SET family_status='SUPERSEDED', superseded_at='2024-01-02' "
            "WHERE id=?", (family_id,)
        )
    conn.commit()
    return family_id

def _insert_category(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color) VALUES ('cat1','בדיקה','#888888')"
    )
    conn.commit()

# ── PatternResult / Report factories (mirror Phase 2A helpers) ────────────────

def _make_pattern(
    contracts_mod,
    description_key: str,
    *,
    recurrence=None,
    commitment=None,
    cadence=None,
    lifecycle=None,
    budget_class=None,
    amount_behavior=None,
    planning_amount=None,
    reserve_eligible: bool = False,
    monthly_reserve_contrib=None,
    family_review_required: bool = False,
    review_reasons=(),
    label: str = "",
    member_ids: tuple = (),
):
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
        member_ids=member_ids,
        membership_confidence={},
        evidence_sources=(),
        decision_source=c.DecisionSource.CLASSIFIER,
        family_review_required=family_review_required,
        review_reasons=review_reasons,
        reserve_eligible=reserve_eligible,
        monthly_reserve_contrib=monthly_reserve_contrib or Decimal("0"),
        canonical_identity=None,
    )


def _make_recurring_pattern(contracts_mod, description_key: str, **kwargs):
    c = contracts_mod
    return _make_pattern(
        contracts_mod, description_key,
        recurrence=c.RecurrenceStatus.RECURRING,
        commitment=c.CommitmentStatus.COMMITTED,
        cadence=c.Cadence.MONTHLY,
        lifecycle=c.LifecycleStatus.ACTIVE,
        budget_class=c.BudgetClass.FIXED_AMOUNT_RECURRING,
        planning_amount=Decimal("500.00"),
        reserve_eligible=True,
        monthly_reserve_contrib=Decimal("500.00"),
        **kwargs,
    )


def _make_report(contracts_mod, patterns, *, run_id: Optional[str] = None):
    c = contracts_mod
    zero = Decimal("0")
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
    _zero_rec = c.make_reconciliation_record(
        field="x", reviewed_value=zero, derived_value=zero, raw_derived_value=zero
    )
    rec = c.ReconciliationReport(planning_income=_zero_rec, monthly_reserve=_zero_rec)
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


def _persist_and_link(
    app_mod, persist_mod, link_mod, contracts_mod,
    patterns, db: str, user_id: int = 1, run_id: Optional[str] = None
):
    """Helper: persist then link in a single call sequence."""
    report = _make_report(contracts_mod, patterns, run_id=run_id)
    p_report = persist_mod.persist_run(db, report, user_id=user_id,
                                       run_id=report.run_id)
    conn = _conn(db)
    try:
        lr = link_mod.link_phase2b(conn, report, p_report, user_id=user_id)
        conn.commit()
    finally:
        conn.close()
    return report, p_report, lr


# ═════════════════════════════════════════════════════════════════════════════
# SOURCE / SAFETY (1–3)
# ═════════════════════════════════════════════════════════════════════════════

class TestSourceSafety:
    def test_01_production_db_path_rejected(self, link_mod):
        """Production DB path rejected before any write."""
        import v4_persistence as pm
        import intelligence.v4_contracts as c
        prod_path = r"C:\Users\erezg\.budget_tracker_data\budget.db"
        p = _make_pattern(c, "test")
        report = _make_report(c, [p])
        p_report = pm.PersistenceReport(run_id="r1", user_id=1)
        with pytest.raises(RuntimeError, match="PRODUCTION SAFETY ABORT"):
            link_mod.link_phase2b_from_path(
                prod_path, report, p_report, user_id=1
            )

    def test_02_user_id_mismatch_rejected(self, link_mod, app_mod, contracts_mod, tmp_path):
        """user_id != persistence_report.user_id → ValueError before any write."""
        import v4_persistence as pm
        db = _fresh_db(app_mod, tmp_path)
        p = _make_pattern(contracts_mod, "test")
        report = _make_report(contracts_mod, [p])
        p_report = pm.PersistenceReport(run_id="r1", user_id=2)
        conn = _conn(db)
        with pytest.raises(ValueError, match="user_id mismatch"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.close()

    def test_03_pragma_fk_check_zero(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """After a full link run PRAGMA foreign_key_check reports 0 violations."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 1)
        _insert_cel(c, "cm1", 1)
        _insert_family(c, "fam1", 1, "key::netflix")
        c.commit(); c.close()

        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)

        conn = _conn(db)
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        conn.close()
        assert violations == []


# ═════════════════════════════════════════════════════════════════════════════
# DETERMINISTIC LINK (4–10)
# ═════════════════════════════════════════════════════════════════════════════

class TestDeterministicLink:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 1)
        _insert_cel(c, "cm1", 1)
        _insert_family(c, "fam1", 1, "key::netflix")
        c.commit(); c.close()
        return db

    def test_04_single_member_links_family(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """One ACTIVE family + one shared expense owned by C → family linked to C."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "LINKED"
        assert lr.results[0].commitment_id == "cm1"

    def test_05_link_sets_is_primary(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Linked family has is_primary=1."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        row = conn.execute("SELECT is_primary FROM pattern_families WHERE id='fam1'").fetchone()
        conn.close()
        assert row[0] == 1

    def test_06_no_new_commitment_created(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment count unchanged after link."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitments"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        after = _count(conn, "commitment_expense_links"); conn.close()
        conn2 = _conn(db)
        assert _count(conn2, "commitments") == before
        conn2.close()

    def test_07_no_new_cel_row_created(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment_expense_links row count unchanged after link."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitment_expense_links"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitment_expense_links") == before
        conn.close()

    def test_08_existing_cel_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """The original CEL row is untouched by linking."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        orig = conn.execute(
            "SELECT commitment_id, expense_id, membership_type FROM commitment_expense_links"
        ).fetchone()
        conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        after = conn.execute(
            "SELECT commitment_id, expense_id, membership_type FROM commitment_expense_links"
        ).fetchone()
        conn.close()
        assert orig == after

    def test_09_raw_run_results_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """v4_run_results rows are not modified by Phase 2B."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)
        conn = _conn(db)
        before_rows = conn.execute(
            "SELECT id, run_id, user_id FROM v4_run_results"
        ).fetchall()
        lr = link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        after_rows = conn.execute(
            "SELECT id, run_id, user_id FROM v4_run_results"
        ).fetchall()
        conn.close()
        assert before_rows == after_rows

    def test_10_legacy_installments_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment_installment_meta is untouched."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitment_installment_meta"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=(1,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitment_installment_meta") == before
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# MIGRATED INSTALLMENT (11–13)
# ═════════════════════════════════════════════════════════════════════════════

class TestMigratedInstallment:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm_mig")
        _insert_expense(c, 10)
        # MIGRATION-owned CEL row (as Phase 1 would create)
        c.execute(
            "INSERT INTO commitment_expense_links "
            "(commitment_id, user_id, expense_id, membership_type, linked_by, created_at) "
            "VALUES ('cm_mig',1,10,'MEMBER','MIGRATION','2024-01-15')"
        )
        _insert_family(c, "fam_mig", 1, "key::mig")
        c.commit(); c.close()
        return db

    def test_11_migrated_commitment_deterministic_link(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Phase 1 MIGRATED commitment with shared MIGRATION-owned expense → deterministic link."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::mig", member_ids=(10,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "LINKED"
        assert lr.results[0].commitment_id == "cm_mig"

    def test_12_no_second_cashflow_stream(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment_occurrences count is unchanged after linking a migrated commitment."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitment_occurrences"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::mig", member_ids=(10,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitment_occurrences") == before
        conn.close()

    def test_13_commitment_occurrences_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment_occurrences rows not altered."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = conn.execute("SELECT * FROM commitment_occurrences").fetchall()
        conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::mig", member_ids=(10,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        after = conn.execute("SELECT * FROM commitment_occurrences").fetchall()
        conn.close()
        assert before == after


# ═════════════════════════════════════════════════════════════════════════════
# MULTIPLE EXPENSES (14–16)
# ═════════════════════════════════════════════════════════════════════════════

class TestMultipleExpenses:

    def _setup_multi(self, app_mod, tmp_path, exp_ids, commitment_for=None):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        for eid in exp_ids:
            _insert_expense(c, eid)
            if commitment_for and eid in commitment_for:
                _insert_cel(c, commitment_for[eid], eid)
        _insert_family(c, "fam1", 1, "key::multi")
        c.commit(); c.close()
        return db

    def test_14_all_members_same_commitment(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Multiple member_ids all owned by same C → link to C."""
        db = self._setup_multi(app_mod, tmp_path, [20, 21, 22],
                               commitment_for={20: "cm1", 21: "cm1", 22: "cm1"})
        pattern = _make_recurring_pattern(contracts_mod, "key::multi", member_ids=(20, 21, 22))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "LINKED"
        assert lr.results[0].commitment_id == "cm1"

    def test_15_some_members_owned_others_not(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Some members owned by C, others unowned → still deterministic link to C."""
        db = self._setup_multi(app_mod, tmp_path, [30, 31],
                               commitment_for={30: "cm1"})
        pattern = _make_recurring_pattern(contracts_mod, "key::multi", member_ids=(30, 31))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "LINKED"
        assert lr.results[0].commitment_id == "cm1"

    def test_16_members_split_across_two_commitments(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Evidence owned by C1 and C2 → no link, ambiguity."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_commitment(c, "cm2")
        _insert_expense(c, 40)
        _insert_expense(c, 41)
        _insert_cel(c, "cm1", 40)
        _insert_cel(c, "cm2", 41)
        _insert_family(c, "fam1", 1, "key::split")
        c.commit(); c.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::split", member_ids=(40, 41))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        # CASE B: contradictory deterministic evidence → CONFLICT, not AMBIGUOUS_SUGGESTED
        assert lr.results[0].outcome.value == "CONFLICT"
        assert lr.results[0].detail.get("conflict_type") == "AMBIGUOUS_FAMILY"
        conn = _conn(db)
        assert conn.execute("SELECT commitment_id FROM pattern_families WHERE id='fam1'").fetchone()[0] is None
        conn.close()


# ═════════════════════════════════════════════════════════════════════════════
# OVERLAP / CONFLICT (17–19)
# ═════════════════════════════════════════════════════════════════════════════

class TestOverlap:

    def _setup_overlap(self, app_mod, tmp_path):
        """Family already linked to cm1; CEL evidence points to cm2."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_commitment(c, "cm2")
        _insert_expense(c, 50)
        _insert_cel(c, "cm2", 50)  # expense already owned by cm2
        # family already linked to cm1
        _insert_family(c, "fam1", 1, "key::ov", commitment_id="cm1")
        c.commit(); c.close()
        return db

    def test_17_overlapping_window_conflict(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Evidence contradicts existing family link → OVERLAPPING_WINDOW conflict."""
        db = self._setup_overlap(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::ov", member_ids=(50,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "CONFLICT"
        assert lr.results[0].detail.get("conflict_type") == "OVERLAPPING_WINDOW"

    def test_18_conflict_dedup_same_run_retry(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Same-run retry does not create a second conflict row."""
        db = self._setup_overlap(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::ov", member_ids=(50,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM commitment_link_conflicts").fetchone()[0]
        conn.close()
        assert count == 1

    def test_19_one_conflict_detected_event(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Exactly one CONFLICT_DETECTED event written."""
        db = self._setup_overlap(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::ov", member_ids=(50,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='CONFLICT_DETECTED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


# ═════════════════════════════════════════════════════════════════════════════
# PRIMARY FAMILY (20–24)
# ═════════════════════════════════════════════════════════════════════════════

class TestPrimaryFamily:

    def _setup_no_primary(self, app_mod, tmp_path):
        """Candidate commitment has no existing primary family."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 60)
        _insert_cel(c, "cm1", 60)
        _insert_family(c, "fam1", 1, "key::pf")
        c.commit(); c.close()
        return db

    def _setup_existing_primary(self, app_mod, tmp_path):
        """Candidate already has a different ACTIVE primary family."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 70)
        _insert_cel(c, "cm1", 70)
        # existing primary family already linked to cm1
        _insert_family(c, "fam_existing", 1, "key::existing", commitment_id="cm1")
        # new family trying to link
        _insert_family(c, "fam_new", 1, "key::new_fam")
        c.commit(); c.close()
        return db

    def test_20_no_primary_link_succeeds(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Candidate has no primary → link succeeds."""
        db = self._setup_no_primary(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::pf", member_ids=(60,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "LINKED"

    def test_21_existing_primary_blocks_link(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Candidate already has different primary → no link."""
        db = self._setup_existing_primary(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::new_fam", member_ids=(70,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "AMBIGUOUS_SUGGESTED"

    def test_22_writes_ambiguous_suggestion(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """AMBIGUOUS_FAMILY suggestion written when primary blocked."""
        db = self._setup_existing_primary(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::new_fam", member_ids=(70,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        row = conn.execute(
            "SELECT suggestion_type FROM commitment_suggestions"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "AMBIGUOUS_FAMILY"

    def test_23_does_not_supersede_existing_family(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """fam_existing remains ACTIVE and primary after blocked link attempt."""
        db = self._setup_existing_primary(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::new_fam", member_ids=(70,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        row = conn.execute(
            "SELECT family_status, is_primary FROM pattern_families WHERE id='fam_existing'"
        ).fetchone()
        conn.close()
        assert row == ("ACTIVE", 1)

    def test_24_does_not_demote_existing_primary(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """fam_existing commitment_id unchanged after blocked link attempt."""
        db = self._setup_existing_primary(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::new_fam", member_ids=(70,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        row = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam_existing'"
        ).fetchone()
        conn.close()
        assert row[0] == "cm1"


# ═════════════════════════════════════════════════════════════════════════════
# SUPERSEDED (25–27)
# ═════════════════════════════════════════════════════════════════════════════

class TestSuperseded:

    def _setup_superseded(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 80)
        _insert_cel(c, "cm1", 80)
        _insert_family(c, "fam_sup", 1, "key::sup", family_status="SUPERSEDED")
        c.commit(); c.close()
        return db

    def test_25_superseded_family_never_linked(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """SUPERSEDED family → SKIPPED_SUPERSEDED when persistence report explicitly targets it."""
        import v4_persistence as pm
        from v4_persistence import _run_result_id as rrid_fn
        db = self._setup_superseded(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::sup", member_ids=(80,))
        # Persist run result manually pointing to fam_sup
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        # Persist normally (creates a NEW family since fam_sup is SUPERSEDED)
        p_report_real = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        new_rrid = p_report_real.outcomes[0].run_result_id
        # Build a fake persistence report that says family_id=fam_sup
        fake_outcome = pm.RunResultOutcome(
            run_result_id=new_rrid,
            description_key="key::sup",
            stream_index=0,
            family_id="fam_sup",
            family_resolution=pm.FamilyResolution.MATCHED_EXISTING,
        )
        p_report = pm.PersistenceReport(run_id=run_id, user_id=1)
        p_report.outcomes.append(fake_outcome)
        conn = _conn(db)
        lr = link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()
        assert lr.results[0].outcome.value == "SKIPPED_SUPERSEDED"
        conn = _conn(db)
        row = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam_sup'"
        ).fetchone()
        conn.close()
        assert row[0] is None

    def test_26_historical_run_results_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """v4_run_results unchanged after a run on a superseded-family scenario."""
        db = self._setup_superseded(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::sup", member_ids=(80,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)
        conn = _conn(db)
        before = conn.execute("SELECT * FROM v4_run_results").fetchall()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        after = conn.execute("SELECT * FROM v4_run_results").fetchall()
        conn.close()
        assert before == after

    def test_27_no_reactivated_event(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """No FAMILY_REACTIVATED event written for superseded skip."""
        db = self._setup_superseded(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::sup", member_ids=(80,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='FAMILY_REACTIVATED'"
        ).fetchone()[0]
        conn.close()
        assert count == 0


# ═════════════════════════════════════════════════════════════════════════════
# UNRESOLVED PARALLEL (28–34)
# ═════════════════════════════════════════════════════════════════════════════

class TestUnresolvedParallel:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_expense(c, 90)
        c.commit(); c.close()
        return db

    def _run_parallel(self, app_mod, persist_mod, link_mod, contracts_mod, db, run_id=None):
        """Two streams for same description_key → family_id=NULL in persistence."""
        c = contracts_mod
        p1 = _make_pattern(contracts_mod, "key::par",
                           recurrence=c.RecurrenceStatus.RECURRING,
                           cadence=c.Cadence.MONTHLY,
                           commitment=c.CommitmentStatus.COMMITTED,
                           lifecycle=c.LifecycleStatus.ACTIVE,
                           budget_class=c.BudgetClass.FIXED_AMOUNT_RECURRING,
                           member_ids=(90,), label="stream0")
        p2 = _make_pattern(contracts_mod, "key::par",
                           recurrence=c.RecurrenceStatus.RECURRING,
                           cadence=c.Cadence.MONTHLY,
                           commitment=c.CommitmentStatus.COMMITTED,
                           lifecycle=c.LifecycleStatus.ACTIVE,
                           budget_class=c.BudgetClass.FIXED_AMOUNT_RECURRING,
                           member_ids=(), label="stream1")
        report = _make_report(contracts_mod, [p1, p2], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=report.run_id)
        conn = _conn(db)
        lr = link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()
        return report, p_report, lr

    def test_28_unresolved_parallel_ambiguous_suggestion(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """run_result family_id=NULL → AMBIGUOUS_FAMILY suggestion."""
        db = self._setup(app_mod, tmp_path)
        _, _, lr = self._run_parallel(app_mod, persist_mod, link_mod, contracts_mod, db)
        outcomes = [r.outcome.value for r in lr.results]
        assert "AMBIGUOUS_SUGGESTED" in outcomes

    def test_29_suggestion_family_id_null(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """AMBIGUOUS_FAMILY suggestion has family_id=NULL."""
        db = self._setup(app_mod, tmp_path)
        self._run_parallel(app_mod, persist_mod, link_mod, contracts_mod, db)
        conn = _conn(db)
        rows = conn.execute(
            "SELECT family_id FROM commitment_suggestions WHERE suggestion_type='AMBIGUOUS_FAMILY'"
        ).fetchall()
        conn.close()
        assert any(r[0] is None for r in rows)

    def test_30_suggestion_candidate_commitment_null(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """AMBIGUOUS_FAMILY suggestion has candidate_commitment_id=NULL."""
        db = self._setup(app_mod, tmp_path)
        self._run_parallel(app_mod, persist_mod, link_mod, contracts_mod, db)
        conn = _conn(db)
        rows = conn.execute(
            "SELECT candidate_commitment_id FROM commitment_suggestions "
            "WHERE suggestion_type='AMBIGUOUS_FAMILY' AND family_id IS NULL"
        ).fetchall()
        conn.close()
        assert len(rows) > 0
        assert all(r[0] is None for r in rows)

    def test_31_suggestion_run_result_id_populated(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """AMBIGUOUS_FAMILY suggestion has run_result_id populated."""
        db = self._setup(app_mod, tmp_path)
        self._run_parallel(app_mod, persist_mod, link_mod, contracts_mod, db)
        conn = _conn(db)
        rows = conn.execute(
            "SELECT run_result_id FROM commitment_suggestions "
            "WHERE suggestion_type='AMBIGUOUS_FAMILY' AND family_id IS NULL"
        ).fetchall()
        conn.close()
        assert len(rows) > 0
        assert all(r[0] is not None for r in rows)

    def test_32_detail_reason_unresolved_parallel(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """AMBIGUOUS_FAMILY suggestion detail contains reason='UNRESOLVED_PARALLEL'."""
        db = self._setup(app_mod, tmp_path)
        self._run_parallel(app_mod, persist_mod, link_mod, contracts_mod, db)
        conn = _conn(db)
        rows = conn.execute(
            "SELECT detail FROM commitment_suggestions "
            "WHERE suggestion_type='AMBIGUOUS_FAMILY' AND family_id IS NULL"
        ).fetchall()
        conn.close()
        assert len(rows) > 0
        assert any(json.loads(r[0]).get("reason") == "UNRESOLVED_PARALLEL" for r in rows)

    def test_33_no_conflict_for_unresolved_parallel(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """No commitment_link_conflicts row for unresolved parallel (suggestion only)."""
        db = self._setup(app_mod, tmp_path)
        self._run_parallel(app_mod, persist_mod, link_mod, contracts_mod, db)
        conn = _conn(db)
        count = _count(conn, "commitment_link_conflicts")
        conn.close()
        assert count == 0

    def test_34_same_run_retry_no_duplicate_suggestion(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Same-run retry does not duplicate AMBIGUOUS_FAMILY suggestion."""
        db = self._setup(app_mod, tmp_path)
        run_id = str(uuid.uuid4())
        report, p_report, _ = self._run_parallel(
            app_mod, persist_mod, link_mod, contracts_mod, db, run_id=run_id)
        # retry
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='AMBIGUOUS_FAMILY'"
        ).fetchone()[0]
        conn.close()
        assert count == 2  # one per stream (two parallel streams, each gets its own)


# ═════════════════════════════════════════════════════════════════════════════
# NEW_RECURRING (35–39)
# ═════════════════════════════════════════════════════════════════════════════

class TestNewRecurring:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_family(c, "fam_nr", 1, "key::nr")
        c.commit(); c.close()
        return db

    def test_35_new_recurring_suggestion_created(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Resolved ACTIVE recurring pattern + zero candidates → NEW_RECURRING suggestion."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::nr", member_ids=())
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "NEW_RECURRING_SUGGESTED"

    def test_36_no_commitment_created(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment count unchanged after NEW_RECURRING."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitments"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::nr", member_ids=())
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitments") == before
        conn.close()

    def test_37_no_authority_created(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment_authority unchanged after NEW_RECURRING."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitment_authority"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::nr", member_ids=())
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitment_authority") == before
        conn.close()

    def test_38_same_run_retry_no_duplicate_suggestion(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Same-run retry of NEW_RECURRING does not duplicate suggestion."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::nr", member_ids=())
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='NEW_RECURRING'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_39_non_recurring_pattern_no_new_recurring(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """NON_RECURRING pattern with zero candidates → NO_ACTION, not NEW_RECURRING."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_family(c, "fam_nr2", 1, "key::nonrec")
        c.commit(); c.close()
        cc = contracts_mod
        pattern = _make_pattern(
            contracts_mod, "key::nonrec",
            recurrence=cc.RecurrenceStatus.NON_RECURRING,
            commitment=cc.CommitmentStatus.NON_COMMITTED,
            cadence=cc.Cadence.UNKNOWN,
            lifecycle=cc.LifecycleStatus.UNKNOWN,
            budget_class=cc.BudgetClass.NON_RECURRING_EXPENSE,
            member_ids=(),
        )
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value == "NO_ACTION"
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='NEW_RECURRING'"
        ).fetchone()[0]
        conn.close()
        assert count == 0


# ═════════════════════════════════════════════════════════════════════════════
# POSSIBLE_MATCH (40–41)
# ═════════════════════════════════════════════════════════════════════════════

class TestPossibleMatch:
    def test_40_description_similarity_alone_never_autolinks(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Merchant/description/name similarity alone never produces auto-link or POSSIBLE_MATCH."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm_name")
        _insert_family(c, "fam_name", 1, "key::netflix")
        c.commit(); c.close()
        # No shared expenses — only description_key similarity
        pattern = _make_recurring_pattern(contracts_mod, "key::netflix", member_ids=())
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        assert lr.results[0].outcome.value != "LINKED"
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='POSSIBLE_MATCH'"
        ).fetchone()[0]
        conn.close()
        assert count == 0

    def test_41_possible_match_not_fabricated(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Phase 2B initial implementation produces no POSSIBLE_MATCH suggestions.
        No approved non-deterministic candidate discovery rule exists."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm_pm")
        _insert_family(c, "fam_pm", 1, "key::pm")
        c.commit(); c.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::pm", member_ids=())
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='POSSIBLE_MATCH'"
        ).fetchone()[0]
        conn.close()
        assert count == 0


# ═════════════════════════════════════════════════════════════════════════════
# CROSS USER (42–44)
# ═════════════════════════════════════════════════════════════════════════════

class TestCrossUser:

    def test_42_commitment_user_mismatch_no_link(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Family user_id=1 + commitment user_id=2 → FK prevents linking."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c, 1)
        _insert_user(c, 2)
        # commitment owned by user 2
        c.execute(
            "INSERT INTO commitments "
            "(id, user_id, canonical_label, created_at, updated_at) "
            "VALUES ('cm_u2',2,'xu','2024-01-01','2024-01-01')"
        )
        _insert_expense(c, 100, user_id=2)
        c.execute(
            "INSERT INTO commitment_expense_links "
            "(commitment_id, user_id, expense_id, membership_type, linked_by, created_at) "
            "VALUES ('cm_u2',2,100,'MEMBER','AUTO','2024-01-15')"
        )
        _insert_family(c, "fam_u1", 1, "key::xu")
        c.commit(); c.close()
        # member_id=100 is user_2's expense → USER_ID_DRIFT
        pattern = _make_recurring_pattern(contracts_mod, "key::xu", member_ids=(100,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db, user_id=1)
        # Must not link; outcome is CONFLICT (USER_ID_DRIFT) or NO_ACTION
        assert lr.results[0].outcome.value in ("CONFLICT", "NO_ACTION", "AMBIGUOUS_SUGGESTED")
        conn = _conn(db)
        fam_link = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam_u1'"
        ).fetchone()[0]
        conn.close()
        assert fam_link is None

    def test_43_member_expense_different_user_drift_conflict(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """member_id from different user → USER_ID_DRIFT conflict.
        Setup: expense owned by user_2, CEL on user_2's commitment (DB-consistent).
        Phase 2B runs as user_1 → e.user_id=2 != user_1 → USER_ID_DRIFT."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c, 1)
        _insert_user(c, 2)
        _insert_commitment(c, "cm2", user_id=2)  # user_2 commitment
        _insert_expense(c, 200, user_id=2)        # expense owned by user_2
        _insert_cel(c, "cm2", 200, user_id=2)     # CEL consistent: cel.user_id=2, expense.user_id=2
        _insert_family(c, "fam1", 1, "key::drift")  # family for user_1
        c.commit(); c.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::drift", member_ids=(200,))
        _, _, lr = _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db, user_id=1)
        assert lr.results[0].outcome.value == "CONFLICT"
        assert lr.results[0].detail.get("conflict_type") == "USER_ID_DRIFT"

    def test_44_no_cross_user_link_occurs(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """After drift conflict fam1.commitment_id remains NULL."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c, 1)
        _insert_user(c, 2)
        _insert_commitment(c, "cm2", user_id=2)
        _insert_expense(c, 300, user_id=2)
        _insert_cel(c, "cm2", 300, user_id=2)
        _insert_family(c, "fam1", 1, "key::drift2")
        c.commit(); c.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::drift2", member_ids=(300,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db, user_id=1)
        conn = _conn(db)
        row = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam1'"
        ).fetchone()
        conn.close()
        assert row[0] is None


# ═════════════════════════════════════════════════════════════════════════════
# SNAPSHOT (45–51)
# ═════════════════════════════════════════════════════════════════════════════

class TestSnapshot:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 400)
        _insert_cel(c, "cm1", 400)
        _insert_family(c, "fam1", 1, "key::snap")
        c.commit(); c.close()
        return db

    def test_45_link_creates_v4single_snapshot(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Successful link creates a V4_SINGLE snapshot row."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        row = conn.execute(
            "SELECT snapshot_type, commitment_id FROM commitment_classifier_snapshots"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == "V4_SINGLE"
        assert row[1] == "cm1"

    def test_46_snapshot_rep_run_result_id_nonnull_and_correct(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Snapshot representative_run_result_id is non-NULL and matches persisted run_result."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)
        expected_rrid = p_report.outcomes[0].run_result_id
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        row = conn.execute(
            "SELECT representative_run_result_id FROM commitment_classifier_snapshots"
        ).fetchone()
        conn.close()
        assert row is not None
        assert row[0] == expected_rrid

    def test_47_snapshot_values_from_raw_pattern(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Snapshot classifier fields come from report.raw PatternResult, not effective."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        row = conn.execute(
            "SELECT recurrence_status, commitment_status, cadence, reserve_eligible "
            "FROM commitment_classifier_snapshots"
        ).fetchone()
        conn.close()
        assert row[0] == pattern.recurrence_status.value
        assert row[1] == pattern.commitment_status.value
        assert row[2] == pattern.cadence.value
        assert row[3] == (1 if pattern.reserve_eligible else 0)

    def test_48_effective_values_not_in_snapshot(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Snapshot is created; its values are raw-derived (no Family Review authority applied)."""
        # This is structural: Phase 2B only writes RAW values.
        # Verify by checking that the raw pattern values match what's stored.
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        raw_recurrence = report.raw.patterns[0].recurrence_status.value
        eff_recurrence = report.effective.patterns[0].recurrence_status.value
        snap_recurrence = conn.execute(
            "SELECT recurrence_status FROM commitment_classifier_snapshots"
        ).fetchone()[0]
        conn.close()
        # They are the same in these tests; key is the snapshot came from raw, not invented
        assert snap_recurrence == raw_recurrence

    def test_49_raw_run_result_unchanged_after_snapshot(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """v4_run_results row unchanged after snapshot creation."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)
        conn = _conn(db)
        before = conn.execute("SELECT * FROM v4_run_results").fetchall()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        after = conn.execute("SELECT * FROM v4_run_results").fetchall()
        conn.close()
        assert before == after

    def test_50_retry_no_duplicate_snapshot(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Same-run retry does not create a second snapshot."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM commitment_classifier_snapshots").fetchone()[0]
        conn.close()
        assert count == 1

    def test_51_v4single_null_representative_impossible_through_phase2b(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """V4_SINGLE snapshot created by Phase 2B always has non-NULL representative_run_result_id."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::snap", member_ids=(400,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        rows = conn.execute(
            "SELECT representative_run_result_id FROM commitment_classifier_snapshots "
            "WHERE snapshot_type='V4_SINGLE'"
        ).fetchall()
        conn.close()
        assert len(rows) > 0
        assert all(r[0] is not None for r in rows)


# ═════════════════════════════════════════════════════════════════════════════
# AUTHORITY (52–54)
# ═════════════════════════════════════════════════════════════════════════════

class TestAuthority:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 500)
        _insert_cel(c, "cm1", 500)
        _insert_family(c, "fam1", 1, "key::auth")
        c.commit(); c.close()
        return db

    def test_52_authority_count_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """commitment_authority row count unchanged after link."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        before = _count(conn, "commitment_authority"); conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::auth", member_ids=(500,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitment_authority") == before
        conn.close()

    def test_53_classifier_never_writes_authority(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """After full link run commitment_authority is still empty."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::auth", member_ids=(500,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        assert _count(conn, "commitment_authority") == 0
        conn.close()

    def test_54_existing_authority_rows_untouched(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Pre-existing commitment_authority rows are not modified."""
        db = self._setup(app_mod, tmp_path)
        conn = _conn(db)
        conn.execute(
            "INSERT INTO commitment_authority "
            "(commitment_id, user_id, field_name, authority_source, override_id, created_at, created_by) "
            "VALUES ('cm1',1,'label','MANUAL_OVERRIDE','oid1','2024-01-01',1)"
        )
        conn.commit()
        before = conn.execute("SELECT * FROM commitment_authority").fetchall()
        conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::auth", member_ids=(500,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        after = conn.execute("SELECT * FROM commitment_authority").fetchall()
        conn.close()
        assert before == after


# ═════════════════════════════════════════════════════════════════════════════
# EVENTS (55–60)
# ═════════════════════════════════════════════════════════════════════════════

class TestEvents:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 600)
        _insert_cel(c, "cm1", 600)
        _insert_family(c, "fam1", 1, "key::evt")
        c.commit(); c.close()
        return db

    def test_55_first_link_creates_family_linked_event(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """First link creates exactly one FAMILY_LINKED event."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::evt", member_ids=(600,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='FAMILY_LINKED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_56_retry_no_second_family_linked(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Second run with same run_id → no additional FAMILY_LINKED event."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::evt", member_ids=(600,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='FAMILY_LINKED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_57_new_snapshot_creates_snapshot_created_event(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Actual new snapshot creation triggers one SNAPSHOT_CREATED event."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::evt", member_ids=(600,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='SNAPSHOT_CREATED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_58_retry_no_second_snapshot_created(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Retry does not create a second SNAPSHOT_CREATED event."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::evt", member_ids=(600,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='SNAPSHOT_CREATED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_59_conflict_creates_conflict_detected_event(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """New conflict row → one CONFLICT_DETECTED event."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_commitment(c, "cm2")
        _insert_expense(c, 700)
        _insert_cel(c, "cm2", 700)
        _insert_family(c, "fam1", 1, "key::cevt", commitment_id="cm1")
        c.commit(); c.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::cevt", member_ids=(700,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='CONFLICT_DETECTED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_60_retry_no_second_conflict_event(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Retry of conflict does not create second CONFLICT_DETECTED event."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_commitment(c, "cm2")
        _insert_expense(c, 800)
        _insert_cel(c, "cm2", 800)
        _insert_family(c, "fam1", 1, "key::cevt2", commitment_id="cm1")
        c.commit(); c.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::cevt2", member_ids=(800,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events WHERE event_type='CONFLICT_DETECTED'"
        ).fetchone()[0]
        conn.close()
        assert count == 1


# ═════════════════════════════════════════════════════════════════════════════
# ATOMICITY (61–63)
# ═════════════════════════════════════════════════════════════════════════════

class TestAtomicity:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 900)
        _insert_cel(c, "cm1", 900)
        _insert_family(c, "fam1", 1, "key::atom")
        c.commit(); c.close()
        return db

    def test_61_failure_after_family_update_rolls_back(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path, monkeypatch):
        """Forced failure after family UPDATE → link rolled back, family.commitment_id=NULL."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::atom", member_ids=(900,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)

        original_insert_snapshot = link_mod._insert_snapshot

        def _fail_snapshot(*args, **kwargs):
            raise RuntimeError("forced snapshot failure")

        monkeypatch.setattr(link_mod, "_insert_snapshot", _fail_snapshot)

        conn = _conn(db)
        with pytest.raises(RuntimeError, match="forced snapshot failure"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        conn = _conn(db)
        fam_link = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam1'"
        ).fetchone()[0]
        snap_count = _count(conn, "commitment_classifier_snapshots")
        conn.close()
        assert fam_link is None
        assert snap_count == 0

    def test_62_failure_after_snapshot_rolls_back_link(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path, monkeypatch):
        """Forced failure after snapshot → snapshot and link rolled back."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::atom", member_ids=(900,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)

        original_insert_event = link_mod._insert_event
        call_count = {"n": 0}

        def _fail_on_second_event(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] >= 2:
                raise RuntimeError("forced event failure")
            return original_insert_event(*args, **kwargs)

        monkeypatch.setattr(link_mod, "_insert_event", _fail_on_second_event)

        conn = _conn(db)
        with pytest.raises(RuntimeError, match="forced event failure"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        conn = _conn(db)
        fam_link = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam1'"
        ).fetchone()[0]
        snap_count = _count(conn, "commitment_classifier_snapshots")
        conn.close()
        assert fam_link is None
        assert snap_count == 0

    def test_63_conflict_path_no_partial_state(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path, monkeypatch):
        """Forced failure during conflict → no partial conflict/event left behind."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_commitment(c, "cm2")
        _insert_expense(c, 950)
        _insert_cel(c, "cm2", 950)
        _insert_family(c, "fam1", 1, "key::catom", commitment_id="cm1")
        c.commit(); c.close()

        pattern = _make_recurring_pattern(contracts_mod, "key::catom", member_ids=(950,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)

        original_insert_event = link_mod._insert_event

        def _fail_event(*args, **kwargs):
            raise RuntimeError("forced conflict event failure")

        monkeypatch.setattr(link_mod, "_insert_event", _fail_event)

        conn = _conn(db)
        with pytest.raises(RuntimeError, match="forced conflict event failure"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        conn = _conn(db)
        conflict_count = _count(conn, "commitment_link_conflicts")
        event_count = _count(conn, "commitment_link_events")
        conn.close()
        assert conflict_count == 0
        assert event_count == 0


# ═════════════════════════════════════════════════════════════════════════════
# IDEMPOTENCE (64–65)
# ═════════════════════════════════════════════════════════════════════════════

class TestIdempotence:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 1000)
        _insert_cel(c, "cm1", 1000)
        _insert_family(c, "fam1", 1, "key::idem")
        c.commit(); c.close()
        return db

    def test_64_same_run_full_retry_identical_state(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Same run_id full retry produces identical final DB state."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::idem", member_ids=(1000,))
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)

        def _state(db):
            conn = _conn(db)
            fam = conn.execute("SELECT commitment_id, is_primary FROM pattern_families WHERE id='fam1'").fetchone()
            snaps = conn.execute("SELECT COUNT(*) FROM commitment_classifier_snapshots").fetchone()[0]
            events = conn.execute("SELECT COUNT(*) FROM commitment_link_events").fetchone()[0]
            conn.close()
            return (fam, snaps, events)

        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()
        state1 = _state(db)

        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()
        state2 = _state(db)

        assert state1 == state2

    def test_65_new_run_id_new_evidence(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """New run_id is treated as new classifier evidence (new run_result row)."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::idem", member_ids=(1000,))

        report1 = _make_report(contracts_mod, [pattern])
        p_report1 = persist_mod.persist_run(db, report1, user_id=1)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report1, p_report1, user_id=1)
        conn.commit()
        conn.close()

        report2 = _make_report(contracts_mod, [pattern])
        assert report2.run_id != report1.run_id
        p_report2 = persist_mod.persist_run(db, report2, user_id=1)

        conn = _conn(db)
        rr_count = _count(conn, "v4_run_results")
        conn.close()
        assert rr_count == 2  # two distinct historical evidence rows


# ═════════════════════════════════════════════════════════════════════════════
# COUNTS / IMMUTABILITY (66–73)
# ═════════════════════════════════════════════════════════════════════════════

class TestImmutability:

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm1")
        _insert_expense(c, 1100)
        _insert_cel(c, "cm1", 1100)
        _insert_family(c, "fam1", 1, "key::imm")
        c.commit(); c.close()
        return db

    def _before_after(self, app_mod, persist_mod, link_mod, contracts_mod, db, table, query=None):
        conn = _conn(db)
        q = query or f"SELECT * FROM {table}"
        before = conn.execute(q).fetchall()
        conn.close()
        pattern = _make_recurring_pattern(contracts_mod, "key::imm", member_ids=(1100,))
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        after = conn.execute(q).fetchall()
        conn.close()
        return before, after

    def test_66_commitments_count_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db, "commitments",
                                  "SELECT COUNT(*) FROM commitments")
        assert b == a

    def test_67_cel_count_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "commitment_expense_links",
                                  "SELECT COUNT(*) FROM commitment_expense_links")
        assert b == a

    def test_68_occurrences_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "commitment_occurrences",
                                  "SELECT * FROM commitment_occurrences")
        assert b == a

    def test_69_installment_meta_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "commitment_installment_meta",
                                  "SELECT * FROM commitment_installment_meta")
        assert b == a

    def test_70_description_key_aliases_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "description_key_aliases",
                                  "SELECT * FROM description_key_aliases")
        assert b == a

    def test_71_authority_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "commitment_authority",
                                  "SELECT * FROM commitment_authority")
        assert b == a

    def test_72_legacy_expenses_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "expenses",
                                  "SELECT id, amount, user_id FROM expenses ORDER BY id")
        assert b == a

    def test_73_legacy_installments_unchanged(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        b, a = self._before_after(app_mod, persist_mod, link_mod, contracts_mod, db,
                                  "commitment_installment_meta",
                                  "SELECT * FROM commitment_installment_meta")
        assert b == a


# ═════════════════════════════════════════════════════════════════════════════
# CORRELATION (74–76)
# ═════════════════════════════════════════════════════════════════════════════

class TestCorrelation:

    def test_74_each_raw_pattern_maps_to_correct_run_result(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Each raw PatternResult correlates to its specific persisted run_result_id."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        c.commit(); c.close()

        p1 = _make_recurring_pattern(contracts_mod, "key::a", member_ids=())
        p2 = _make_recurring_pattern(contracts_mod, "key::b", member_ids=())
        report = _make_report(contracts_mod, [p1, p2])
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=report.run_id)

        # Insert families so linking can proceed
        conn = _conn(db)
        _insert_family(conn, "fam_a", 1, "key::a")
        _insert_family(conn, "fam_b", 1, "key::b")
        conn.commit()

        lr = link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        from v4_persistence import _run_result_id
        expected_a = _run_result_id(report.run_id, "key::a", 0)
        expected_b = _run_result_id(report.run_id, "key::b", 0)
        by_key = {r.description_key: r for r in lr.results}
        assert by_key["key::a"].run_result_id == expected_a
        assert by_key["key::b"].run_result_id == expected_b

    def test_75_wrong_run_id_mismatch_fails_closed(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """PersistenceReport with mismatched run_id raises ValueError (no persisted outcome found)."""
        import v4_persistence as pm
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        c.commit(); c.close()

        pattern = _make_recurring_pattern(contracts_mod, "key::corr", member_ids=())
        report = _make_report(contracts_mod, [pattern])

        # PersistenceReport with a DIFFERENT run_id (no outcomes matching the report)
        p_report = pm.PersistenceReport(run_id="totally-wrong-run-id", user_id=1)

        conn = _conn(db)
        with pytest.raises((ValueError, KeyError)):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.close()

    def test_76_no_accidental_cross_stream_correlation(self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path):
        """Two patterns with different description_keys are correlated to distinct run_results."""
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        c.commit(); c.close()

        p1 = _make_recurring_pattern(contracts_mod, "key::x1", member_ids=())
        p2 = _make_recurring_pattern(contracts_mod, "key::x2", member_ids=())
        report = _make_report(contracts_mod, [p1, p2])
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=report.run_id)

        from v4_persistence import _run_result_id
        rrid1 = _run_result_id(report.run_id, "key::x1", 0)
        rrid2 = _run_result_id(report.run_id, "key::x2", 0)
        assert rrid1 != rrid2

        conn = _conn(db)
        _insert_family(conn, "fam_x1", 1, "key::x1")
        _insert_family(conn, "fam_x2", 1, "key::x2")
        conn.commit()
        lr = link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        by_key = {r.description_key: r for r in lr.results}
        assert by_key["key::x1"].run_result_id == rrid1
        assert by_key["key::x2"].run_result_id == rrid2


# ═════════════════════════════════════════════════════════════════════════════
# CASE B — MULTIPLE DETERMINISTIC OWNERS (77–80)
# ═════════════════════════════════════════════════════════════════════════════

class TestMultipleDeterministicOwners:
    """
    CASE B: member_ids resolve to >1 distinct commitment via CEL (same user).
    Contradictory deterministic evidence — must produce CONFLICT with
    conflict_type='AMBIGUOUS_FAMILY', never AMBIGUOUS_SUGGESTED.
    """

    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c, 1)
        _insert_commitment(c, "cm_ma")
        _insert_commitment(c, "cm_mb")
        _insert_expense(c, 2000)
        _insert_expense(c, 2001)
        _insert_cel(c, "cm_ma", 2000)   # E2000 → C_ma
        _insert_cel(c, "cm_mb", 2001)   # E2001 → C_mb
        _insert_family(c, "fam_multi", 1, "key::multi")
        c.commit(); c.close()
        return db

    def test_77_multiple_owners_produces_conflict_not_suggestion(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path
    ):
        """E1→C1, E2→C2 same user → reason=multiple → CONFLICT outcome."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(
            contracts_mod, "key::multi", member_ids=(2000, 2001)
        )
        _, _, lr = _persist_and_link(
            app_mod, persist_mod, link_mod, contracts_mod, [pattern], db
        )
        assert lr.results[0].outcome.value == "CONFLICT"
        assert lr.results[0].detail.get("conflict_type") == "AMBIGUOUS_FAMILY"

    def test_78_ambiguous_family_conflict_row_written_no_suggestion(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path
    ):
        """AMBIGUOUS_FAMILY conflict row inserted with correct fields; no suggestion row."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(
            contracts_mod, "key::multi", member_ids=(2000, 2001)
        )
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        conflict = conn.execute(
            "SELECT conflict_type, commitment_id, family_id "
            "FROM commitment_link_conflicts WHERE conflict_type='AMBIGUOUS_FAMILY'"
        ).fetchone()
        suggestion_count = conn.execute(
            "SELECT COUNT(*) FROM commitment_suggestions"
        ).fetchone()[0]
        event_count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events "
            "WHERE event_type='CONFLICT_DETECTED'"
        ).fetchone()[0]
        conn.close()
        assert conflict is not None
        assert conflict[0] == "AMBIGUOUS_FAMILY"
        assert conflict[1] is None            # commitment_id=NULL (no clear winner)
        assert conflict[2] == "fam_multi"
        assert suggestion_count == 0
        assert event_count == 1

    def test_79_family_link_remains_null_no_snapshot(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path
    ):
        """After AMBIGUOUS_FAMILY conflict: family.commitment_id=NULL, no snapshot."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(
            contracts_mod, "key::multi", member_ids=(2000, 2001)
        )
        _persist_and_link(app_mod, persist_mod, link_mod, contracts_mod, [pattern], db)
        conn = _conn(db)
        fam_link = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam_multi'"
        ).fetchone()[0]
        snap_count = _count(conn, "commitment_classifier_snapshots")
        conn.close()
        assert fam_link is None
        assert snap_count == 0

    def test_80_same_run_retry_no_duplicate_conflict(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path
    ):
        """Same-run retry of AMBIGUOUS_FAMILY conflict does not duplicate row or event."""
        db = self._setup(app_mod, tmp_path)
        pattern = _make_recurring_pattern(
            contracts_mod, "key::multi", member_ids=(2000, 2001)
        )
        run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [pattern], run_id=run_id)
        p_report = persist_mod.persist_run(db, report, user_id=1, run_id=run_id)
        conn = _conn(db)
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conflict_count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_conflicts "
            "WHERE conflict_type='AMBIGUOUS_FAMILY'"
        ).fetchone()[0]
        event_count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events "
            "WHERE event_type='CONFLICT_DETECTED'"
        ).fetchone()[0]
        conn.close()
        assert conflict_count == 1
        assert event_count == 1


# ═════════════════════════════════════════════════════════════════════════════
# RUN-ID INDEPENDENCE (81)
# ═════════════════════════════════════════════════════════════════════════════

class TestRunIdIndependence:

    def test_81_persistence_run_id_differs_from_report_run_id(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path
    ):
        """
        report.run_id = R1; persist_run called WITHOUT run_id → generates R2.
        Assert R1 != R2. _correlate must use persistence_report.run_id (R2).
        link_phase2b must succeed and produce correct outcome.
        """
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_family(c, "fam_r2ind", 1, "key::r2ind")
        c.commit(); c.close()

        pattern = _make_recurring_pattern(contracts_mod, "key::r2ind", member_ids=())
        report = _make_report(contracts_mod, [pattern])          # R1
        # Do NOT pass run_id — persistence generates a fresh R2
        p_report = persist_mod.persist_run(db, report, user_id=1)

        assert p_report.run_id != report.run_id                  # R1 != R2

        conn = _conn(db)
        lr = link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        assert len(lr.results) == 1
        # Recurring, zero CEL candidates → NEW_RECURRING_SUGGESTED
        assert lr.results[0].outcome.value == "NEW_RECURRING_SUGGESTED"
        # LinkReport carries the persistence run_id (authoritative evidence identity)
        assert lr.run_id == p_report.run_id


# ═════════════════════════════════════════════════════════════════════════════
# ATOMICITY A / B / C — PRECISE SAVEPOINT PROOFS (82–84)
# ═════════════════════════════════════════════════════════════════════════════

class TestAtomicityPrecise:
    """
    Explicit per-SAVEPOINT proofs for the three injection points specified
    in the Phase 2B Correction Gate.
    Each test asserts the COMPLETE post-rollback state for its failure point.
    """

    def _setup_link(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm_pa")
        _insert_expense(c, 3000)
        _insert_cel(c, "cm_pa", 3000)
        _insert_family(c, "fam_pa", 1, "key::prec")
        c.commit(); c.close()
        return db

    def test_82_atomicity_a_no_family_linked_event_on_rollback(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path, monkeypatch
    ):
        """
        ATOMICITY A: Family UPDATE NULL→C done; inject failure BEFORE snapshot.
        SAVEPOINT rolls back: family.commitment_id=NULL, no snapshot,
        no FAMILY_LINKED event.
        """
        db = self._setup_link(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::prec", member_ids=(3000,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)

        def _fail_snapshot(*a, **k):
            raise RuntimeError("atomicity-a-failure")

        monkeypatch.setattr(link_mod, "_insert_snapshot", _fail_snapshot)

        conn = _conn(db)
        with pytest.raises(RuntimeError, match="atomicity-a-failure"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        conn = _conn(db)
        fam_link = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam_pa'"
        ).fetchone()[0]
        snap_count = _count(conn, "commitment_classifier_snapshots")
        family_linked_count = conn.execute(
            "SELECT COUNT(*) FROM commitment_link_events "
            "WHERE event_type='FAMILY_LINKED'"
        ).fetchone()[0]
        conn.close()
        assert fam_link is None,          "family.commitment_id must be NULL after rollback"
        assert snap_count == 0,           "no snapshot after rollback"
        assert family_linked_count == 0,  "no FAMILY_LINKED event after rollback"

    def test_83_atomicity_b_all_state_rolled_back_on_event_failure(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path, monkeypatch
    ):
        """
        ATOMICITY B: Family link written, snapshot inserted; inject failure at
        SNAPSHOT_CREATED event (before event completion).
        SAVEPOINT rolls back: family link gone, snapshot gone, no events at all.
        """
        db = self._setup_link(app_mod, tmp_path)
        pattern = _make_recurring_pattern(contracts_mod, "key::prec", member_ids=(3000,))
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)

        original_insert_event = link_mod._insert_event

        def _fail_on_snapshot_created(*a, **k):
            # a: (conn, user_id, event_type, ...)
            if a[2] == "SNAPSHOT_CREATED":
                raise RuntimeError("atomicity-b-failure")
            return original_insert_event(*a, **k)

        monkeypatch.setattr(link_mod, "_insert_event", _fail_on_snapshot_created)

        conn = _conn(db)
        with pytest.raises(RuntimeError, match="atomicity-b-failure"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        conn = _conn(db)
        fam_link = conn.execute(
            "SELECT commitment_id FROM pattern_families WHERE id='fam_pa'"
        ).fetchone()[0]
        snap_count = _count(conn, "commitment_classifier_snapshots")
        event_count = _count(conn, "commitment_link_events")
        conn.close()
        assert fam_link is None, "family link must be rolled back"
        assert snap_count == 0,  "snapshot must be rolled back"
        assert event_count == 0, "no events must remain after rollback"

    def test_84_atomicity_c_conflict_row_rolled_back_on_event_failure(
        self, app_mod, persist_mod, link_mod, contracts_mod, tmp_path, monkeypatch
    ):
        """
        ATOMICITY C: Conflict row inserted; inject failure BEFORE CONFLICT_DETECTED
        event completion.
        SAVEPOINT rolls back: no conflict row, no event.
        """
        db = _fresh_db(app_mod, tmp_path)
        _insert_category(_conn(db))
        c = _conn(db)
        _insert_user(c)
        _insert_commitment(c, "cm_pc1")
        _insert_commitment(c, "cm_pc2")
        _insert_expense(c, 3100)
        _insert_cel(c, "cm_pc2", 3100)
        _insert_family(c, "fam_pc", 1, "key::catom2b", commitment_id="cm_pc1")
        c.commit(); c.close()

        pattern = _make_recurring_pattern(
            contracts_mod, "key::catom2b", member_ids=(3100,)
        )
        report = _make_report(contracts_mod, [pattern])
        p_report = persist_mod.persist_run(db, report, user_id=1)

        def _fail_conflict_detected(*a, **k):
            if a[2] == "CONFLICT_DETECTED":
                raise RuntimeError("atomicity-c-failure")
            return link_mod._insert_event.__wrapped__(*a, **k)

        # Wrap _insert_event so calls to CONFLICT_DETECTED fail
        original_insert_event = link_mod._insert_event

        def _patched(*a, **k):
            if a[2] == "CONFLICT_DETECTED":
                raise RuntimeError("atomicity-c-failure")
            return original_insert_event(*a, **k)

        monkeypatch.setattr(link_mod, "_insert_event", _patched)

        conn = _conn(db)
        with pytest.raises(RuntimeError, match="atomicity-c-failure"):
            link_mod.link_phase2b(conn, report, p_report, user_id=1)
        conn.commit()
        conn.close()

        conn = _conn(db)
        conflict_count = _count(conn, "commitment_link_conflicts")
        event_count = _count(conn, "commitment_link_events")
        conn.close()
        assert conflict_count == 0, "conflict row must be rolled back"
        assert event_count == 0,    "no event must remain after rollback"
