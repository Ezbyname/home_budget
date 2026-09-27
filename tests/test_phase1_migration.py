"""
Phase 1 — Installment migration + finite occurrence generator tests.

All tests use isolated temporary SQLite databases.
The production DB is never touched.
"""

from __future__ import annotations

import os
import sys
import shutil
import sqlite3
import tempfile
import uuid
from datetime import date
from decimal import Decimal

import pytest

# ── Module isolation ──────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def _session_home():
    d = tempfile.mkdtemp(prefix="phase1_home_")
    orig = os.environ.get("HOME")
    os.environ["HOME"] = d
    yield d
    if orig is None:
        os.environ.pop("HOME", None)
    else:
        os.environ["HOME"] = orig
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope="session")
def app_mod(_session_home):
    for m in list(sys.modules):
        if m == "app" or m.startswith("app."):
            del sys.modules[m]
    import app as _app
    return _app


@pytest.fixture(scope="session")
def mig_mod():
    for m in list(sys.modules):
        if m == "commitment_migration" or m.startswith("commitment_migration."):
            del sys.modules[m]
    import commitment_migration as cm
    return cm


# ── Isolated DB helpers ───────────────────────────────────────────────────────

def _fresh_db(app_mod, tmp_path) -> str:
    db_path = str(tmp_path / "test.db")
    orig = app_mod.DB_PATH
    app_mod.DB_PATH = db_path
    app_mod.init_db()
    app_mod.DB_PATH = orig
    return db_path


def _conn(db_path: str) -> sqlite3.Connection:
    c = sqlite3.connect(db_path)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    return c


_CATEGORY_ID = "arnona"  # always present after init_db()


def _add_expense(c, exp_id, user_id, amount=100.0, date_="2024-01-15"):
    c.execute("""
        INSERT OR IGNORE INTO expenses (id, date, category_id, amount, user_id)
        VALUES (?, ?, ?, ?, ?)
    """, (exp_id, date_, _CATEGORY_ID, amount, user_id))


def _add_installment(c, inst_id, user_id=1, total=12, paid=0,
                     monthly=100.0, total_amount=1200.0,
                     status="active", start="2024-01-15",
                     description="Test Plan"):
    c.execute("""
        INSERT OR IGNORE INTO installments
            (id, description, store, total_amount, total_payments,
             payments_made, monthly_payment, start_date, user_id, status)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (inst_id, description, "Shop", total_amount, total, paid,
          monthly, start, user_id, status))


def _add_link(c, inst_id, exp_id, user_id=1, status="confirmed"):
    c.execute("""
        INSERT OR IGNORE INTO installment_transaction_links
            (user_id, installment_id, expense_id, status)
        VALUES (?,?,?,?)
    """, (user_id, inst_id, exp_id, status))


def _count(c, table) -> int:
    return c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ── Calendar helper tests ─────────────────────────────────────────────────────

class TestCalendarHelper:
    def test_anchor_31_jan_to_feb_nonleap(self, mig_mod):
        result = mig_mod._add_months(date(2023, 1, 31), 1)
        assert result == date(2023, 2, 28)

    def test_anchor_31_jan_to_feb_leap(self, mig_mod):
        result = mig_mod._add_months(date(2024, 1, 31), 1)
        assert result == date(2024, 2, 29)

    def test_march_after_feb_leap_returns_to_anchor(self, mig_mod):
        # Jan 31 → Feb 29 → Mar 31
        d = mig_mod._add_months(date(2024, 1, 31), 2)
        assert d == date(2024, 3, 31)

    def test_march_after_feb_nonleap_returns_to_anchor(self, mig_mod):
        # Jan 31 → Feb 28 → Mar 31
        d = mig_mod._add_months(date(2023, 1, 31), 2)
        assert d == date(2023, 3, 31)

    def test_anchor_30_feb_clamps(self, mig_mod):
        result = mig_mod._add_months(date(2023, 1, 30), 1)
        assert result == date(2023, 2, 28)

    def test_anchor_30_mar_restores(self, mig_mod):
        result = mig_mod._add_months(date(2023, 1, 30), 2)
        assert result == date(2023, 3, 30)

    def test_anchor_15_preserved(self, mig_mod):
        result = mig_mod._add_months(date(2024, 3, 15), 3)
        assert result == date(2024, 6, 15)

    def test_anchor_1_preserved(self, mig_mod):
        result = mig_mod._add_months(date(2024, 1, 1), 5)
        assert result == date(2024, 6, 1)


# ── Money conversion tests ────────────────────────────────────────────────────

class TestMoneyConversion:
    def test_exact_integer(self, mig_mod):
        assert mig_mod._to_agorot(100.0) == 10000

    def test_fractional_45(self, mig_mod):
        assert mig_mod._to_agorot(123.45) == 12345

    def test_fractional_01(self, mig_mod):
        assert mig_mod._to_agorot(0.01) == 1

    def test_no_float_error(self, mig_mod):
        # 0.1+0.2 float trap: must still be exact via Decimal path
        result = mig_mod._to_agorot("83.33")
        assert result == 8333

    def test_decimal_input(self, mig_mod):
        assert mig_mod._to_agorot(Decimal("999.99")) == 99999


# ── Occurrence generator tests ────────────────────────────────────────────────

class TestOccurrenceGenerator:
    def test_m0_n12_generates_12(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 15), 12, 0, 10000
        )
        assert len(rows) == 12

    def test_m1_n12_generates_11(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 15), 12, 1, 10000
        )
        assert len(rows) == 11

    def test_m11_n12_generates_1(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 15), 12, 11, 10000
        )
        assert len(rows) == 1

    def test_m12_n12_generates_0(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 15), 12, 12, 10000
        )
        assert rows == []

    def test_m_gt_n_raises(self, mig_mod):
        with pytest.raises(ValueError, match="payments_made"):
            mig_mod.generate_future_occurrences(
                "C1", 1, date(2024, 1, 15), 12, 13, 10000
            )

    def test_indexes_are_m_plus_1_to_n(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 3, 1), 5, 2, 5000
        )
        assert [r["occurrence_index"] for r in rows] == [3, 4, 5]

    def test_no_index_zero(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 1), 3, 0, 1000
        )
        assert all(r["occurrence_index"] >= 1 for r in rows)

    def test_no_index_greater_than_n(self, mig_mod):
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 1), 5, 0, 1000
        )
        assert all(r["occurrence_index"] <= 5 for r in rows)

    def test_calendar_dates_correct(self, mig_mod):
        # First payment Jan 31; future indexes 1,2,3
        rows = mig_mod.generate_future_occurrences(
            "C1", 1, date(2024, 1, 31), 3, 0, 1000
        )
        dates = [r["occurrence_date"] for r in rows]
        assert dates == ["2024-01-31", "2024-02-29", "2024-03-31"]

    def test_n_zero_raises(self, mig_mod):
        with pytest.raises(ValueError):
            mig_mod.generate_future_occurrences("C1", 1, date(2024, 1, 1), 0, 0, 1000)


# ── Production-path safety tests ─────────────────────────────────────────────

class TestProductionPathGuard:
    def test_production_path_is_rejected(self, mig_mod, tmp_path):
        prod = r"C:\Users\erezg\.budget_tracker_data\budget.db"
        with pytest.raises(RuntimeError, match="PRODUCTION SAFETY ABORT"):
            mig_mod.migrate_installments(prod)

    def test_temp_path_accepted(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=0)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10, user_id=1)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert any(o.result.value in ("MIGRATED", "ALREADY_MIGRATED")
                   for o in report.outcomes)

    def test_is_production_path_windows(self, mig_mod):
        assert mig_mod._is_production_path(
            r"C:\Users\erezg\.budget_tracker_data\budget.db"
        )

    def test_is_production_path_random_path_not_flagged(self, mig_mod):
        assert not mig_mod._is_production_path("/tmp/test.db")


# ── Ownership resolution tests ────────────────────────────────────────────────

class TestOwnershipResolution:
    def test_single_user_migrates(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1)
        _add_expense(c, 10, user_id=1, amount=100.0)
        _add_link(c, 1, 10, user_id=1)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED

    def test_multiple_same_user_expenses_migrates(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=1, date_="2024-02-15")
        _add_link(c, 1, 10); _add_link(c, 1, 11)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED

    def test_multi_user_expenses_conflict(self, mig_mod, app_mod, tmp_path):
        # Policy A: installment.user_id=1, expense owners={1,2} → CONFLICT (mismatch)
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=6, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=2, date_="2024-02-15")
        _add_link(c, 1, 10); _add_link(c, 1, 11, user_id=2)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.CONFLICT

    def test_no_links_user_id_zero_skipped_no_owner(self, mig_mod, app_mod, tmp_path):
        # Policy A: user_id=0 sentinel + no links → SKIPPED_NO_OWNER
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=6, paid=0)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.SKIPPED_NO_OWNER

    def test_installment_owner_trusted_without_links(self, mig_mod, app_mod, tmp_path):
        # Policy A: user_id=1 > 0, no expense links → MIGRATE using installment owner
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=6, paid=0)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED


# ── Eligibility tests ─────────────────────────────────────────────────────────

class TestEligibility:
    def test_active_migrates(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, status="active", total=6, paid=0)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED

    def test_completed_skipped(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, status="completed", total=6, paid=6)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.SKIPPED_STATUS_NOT_ELIGIBLE

    def test_cancelled_skipped(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, status="cancelled", total=6, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.SKIPPED_STATUS_NOT_ELIGIBLE

    def test_unknown_status_skipped(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, status="weird_status", total=6, paid=0)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.SKIPPED_STATUS_NOT_ELIGIBLE


# ── 1:1 invariant and idempotence tests ──────────────────────────────────────

class TestIdempotenceAnd11:
    def _setup(self, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        return db

    def test_one_legacy_one_commitment(self, mig_mod, app_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        mig_mod.migrate_installments(db)
        c = _conn(db)
        assert _count(c, "commitments") == 1
        c.close()

    def test_second_run_already_migrated(self, mig_mod, app_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        mig_mod.migrate_installments(db)
        report2 = mig_mod.migrate_installments(db)
        assert report2.outcomes[0].result == mig_mod.MigrationResult.ALREADY_MIGRATED

    def test_run_twice_same_commitment_count(self, mig_mod, app_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        mig_mod.migrate_installments(db)
        c = _conn(db); n1 = _count(c, "commitments"); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db); n2 = _count(c, "commitments"); c.close()
        assert n1 == n2

    def test_run_twice_same_meta_count(self, mig_mod, app_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        mig_mod.migrate_installments(db)
        c = _conn(db); n1 = _count(c, "commitment_installment_meta"); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db); n2 = _count(c, "commitment_installment_meta"); c.close()
        assert n1 == n2

    def test_run_twice_same_link_count(self, mig_mod, app_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        mig_mod.migrate_installments(db)
        c = _conn(db); n1 = _count(c, "commitment_expense_links"); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db); n2 = _count(c, "commitment_expense_links"); c.close()
        assert n1 == n2

    def test_run_twice_same_occurrence_count(self, mig_mod, app_mod, tmp_path):
        db = self._setup(app_mod, tmp_path)
        mig_mod.migrate_installments(db)
        c = _conn(db); n1 = _count(c, "commitment_occurrences"); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db); n2 = _count(c, "commitment_occurrences"); c.close()
        assert n1 == n2


# ── Atomicity tests ───────────────────────────────────────────────────────────

class TestAtomicity:
    def test_forced_failure_rolls_back(self, mig_mod, app_mod, tmp_path, monkeypatch):
        """Force a failure after commitment insert; verify full rollback."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=0)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()

        orig_gen = mig_mod.generate_future_occurrences

        def _boom(*a, **kw):
            raise RuntimeError("Injected failure")

        monkeypatch.setattr(mig_mod, "generate_future_occurrences", _boom)
        report = mig_mod.migrate_installments(db)
        monkeypatch.setattr(mig_mod, "generate_future_occurrences", orig_gen)

        assert report.outcomes[0].result == mig_mod.MigrationResult.FAILED

        c = _conn(db)
        assert _count(c, "commitments") == 0
        assert _count(c, "commitment_installment_meta") == 0
        assert _count(c, "commitment_expense_links") == 0
        assert _count(c, "commitment_occurrences") == 0
        c.close()

    def test_no_half_created_commitment(self, mig_mod, app_mod, tmp_path, monkeypatch):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=0)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()

        orig_gen = mig_mod.generate_future_occurrences
        monkeypatch.setattr(mig_mod, "generate_future_occurrences",
                            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")))
        mig_mod.migrate_installments(db)
        monkeypatch.setattr(mig_mod, "generate_future_occurrences", orig_gen)

        c = _conn(db)
        assert _count(c, "commitments") == 0
        c.close()

    def test_independent_installment_still_migrates_after_skip(
            self, mig_mod, app_mod, tmp_path, monkeypatch):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        # Installment 1: user_id=0 + no links → SKIPPED_NO_OWNER under Policy A
        _add_installment(c, 1, user_id=0, total=6, paid=0)
        # Installment 2: valid, should succeed
        _add_installment(c, 2, user_id=1, total=3, paid=0, description="Plan 2")
        _add_expense(c, 20, user_id=1)
        _add_link(c, 2, 20)
        c.commit(); c.close()

        report = mig_mod.migrate_installments(db)
        results = {o.legacy_id: o.result for o in report.outcomes}
        assert results[1] == mig_mod.MigrationResult.SKIPPED_NO_OWNER
        assert results[2] == mig_mod.MigrationResult.MIGRATED


# ── Money agorot tests ────────────────────────────────────────────────────────

class TestMoneyAgorot:
    def test_agorot_exact(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=1, paid=0,
                         monthly=123.45, total_amount=123.45)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        meta = dict(c.execute("SELECT * FROM commitment_installment_meta").fetchone())
        c.close()
        assert meta["payment_agorot"] == 12345

    def test_agorot_123_45(self, mig_mod):
        assert mig_mod._to_agorot(123.45) == 12345

    def test_no_float_arithmetic(self, mig_mod):
        # 0.1+0.2 in float = 0.30000000000000004, must still be 30
        result = mig_mod._to_agorot(0.30)
        assert result == 30


# ── Expense link tests ────────────────────────────────────────────────────────

class TestExpenseLinks:
    def test_legacy_links_produce_cel_rows(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=1, date_="2024-02-15")
        _add_link(c, 1, 10); _add_link(c, 1, 11)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        assert _count(c, "commitment_expense_links") == 2
        c.close()

    def test_same_expense_cannot_fund_two_commitments(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1, description="Plan A")
        _add_installment(c, 2, total=4, paid=1, description="Plan B")
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10); _add_link(c, 2, 10)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        results = {o.legacy_id: o.result for o in report.outcomes}
        migrated = sum(1 for r in results.values() if r == mig_mod.MigrationResult.MIGRATED)
        conflict = sum(1 for r in results.values() if r == mig_mod.MigrationResult.CONFLICT)
        # Exactly one migrates, one gets CONFLICT
        assert migrated == 1
        assert conflict == 1

    def test_conflict_rolls_back_installment(self, mig_mod, app_mod, tmp_path):
        """After conflict, no extra commitments, links or occurrences for the failed one."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=3, paid=1, description="Plan A")
        _add_installment(c, 2, total=3, paid=1, description="Plan B")
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10); _add_link(c, 2, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        assert _count(c, "commitments") == 1
        c.close()

    def test_cross_user_expense_conflict(self, mig_mod, app_mod, tmp_path):
        """Policy A: installment.user_id=1, expenses owned by users 1 and 2 → CONFLICT."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=2, date_="2024-02-15")
        _add_installment(c, 1, user_id=1, total=6, paid=2)
        _add_link(c, 1, 10, user_id=1)
        _add_link(c, 1, 11, user_id=2)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.CONFLICT


# ── Legacy immutability tests ─────────────────────────────────────────────────

class TestLegacyImmutability:
    LEGACY_TABLES = [
        "expenses", "income", "installments",
        "installment_transaction_links", "installment_suggestions",
    ]

    def _snapshot(self, c, table):
        cols = [r[1] for r in c.execute(f"PRAGMA table_info('{table}')").fetchall()]
        idxs = {r[1]: r[2] for r in c.execute(f"PRAGMA index_list('{table}')").fetchall()}
        trigs = {r[0]: r[1] for r in c.execute(
            f"SELECT name, sql FROM sqlite_master WHERE type='trigger' AND tbl_name='{table}'"
        ).fetchall()}
        rows = c.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return {"cols": cols, "idxs": idxs, "trigs": trigs, "rows": rows}

    def _run_migration(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit()
        before = {t: self._snapshot(c, t) for t in self.LEGACY_TABLES}
        c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        after = {t: self._snapshot(c, t) for t in self.LEGACY_TABLES}
        c.close()
        return before, after

    def test_expenses_rows_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        assert b["expenses"]["rows"] == a["expenses"]["rows"]

    def test_installments_rows_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        assert b["installments"]["rows"] == a["installments"]["rows"]

    def test_itl_rows_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        assert b["installment_transaction_links"]["rows"] == a["installment_transaction_links"]["rows"]

    def test_income_rows_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        assert b["income"]["rows"] == a["income"]["rows"]

    def test_installment_suggestions_rows_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        assert b["installment_suggestions"]["rows"] == a["installment_suggestions"]["rows"]

    def test_all_legacy_columns_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        for t in self.LEGACY_TABLES:
            assert b[t]["cols"] == a[t]["cols"], f"Columns changed for {t}"

    def test_all_legacy_indexes_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        for t in self.LEGACY_TABLES:
            assert b[t]["idxs"] == a[t]["idxs"], f"Indexes changed for {t}"

    def test_all_legacy_triggers_unchanged(self, mig_mod, app_mod, tmp_path):
        b, a = self._run_migration(mig_mod, app_mod, tmp_path)
        for t in self.LEGACY_TABLES:
            assert b[t]["trigs"] == a[t]["trigs"], f"Triggers changed for {t}"


# ── Pattern families boundary ─────────────────────────────────────────────────

class TestPatternFamiliesBoundary:
    def test_zero_pattern_families_created(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        assert _count(c, "pattern_families") == 0
        c.close()

    def test_merchant_alone_never_creates_family(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1, description="Apple Store")
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        assert _count(c, "pattern_families") == 0
        c.close()


# ── Reporting completeness tests ──────────────────────────────────────────────

class TestReportingCompleteness:
    def test_every_installment_has_result(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=6, paid=0)  # user_id=0 + no links → SKIPPED_NO_OWNER
        _add_installment(c, 2, total=3, paid=0, status="completed")  # SKIPPED_STATUS
        _add_installment(c, 3, total=4, paid=1, description="Active")
        _add_expense(c, 10, user_id=1)
        _add_link(c, 3, 10)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert len(report.outcomes) == 3
        ids = {o.legacy_id for o in report.outcomes}
        assert ids == {1, 2, 3}

    def test_result_counts_reconcile(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        _add_installment(c, 2, status="completed", total=3, paid=3)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        counts = report.counts()
        total = sum(counts.values())
        assert total == 2


# ── Isolated fixture proof ────────────────────────────────────────────────────

class TestIsolatedFixtureProof:
    def test_full_fixture_proof(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)

        # Two active installments with different users
        _add_installment(c, 1, user_id=1, total=12, paid=3,
                         monthly=500.0, total_amount=6000.0, start="2024-01-15")
        _add_installment(c, 2, user_id=1, total=6, paid=1,
                         monthly=123.45, total_amount=740.70, start="2024-03-31",
                         description="Plan B")
        # Completed (should be skipped)
        _add_installment(c, 3, user_id=1, total=3, paid=3, status="completed")

        _add_expense(c, 10, user_id=1, amount=500.0, date_="2024-01-15")
        _add_expense(c, 11, user_id=1, amount=500.0, date_="2024-02-15")
        _add_expense(c, 12, user_id=1, amount=500.0, date_="2024-03-15")
        _add_expense(c, 20, user_id=1, amount=123.45, date_="2024-03-31")

        _add_link(c, 1, 10); _add_link(c, 1, 11); _add_link(c, 1, 12)
        _add_link(c, 2, 20)

        c.commit()

        # BEFORE snapshot
        before = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["expenses", "installments", "installment_transaction_links",
                            "installment_suggestions", "income"]}
        uc_before = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                     for t in ["commitments", "commitment_installment_meta",
                               "commitment_expense_links", "commitment_occurrences"]}
        c.close()

        # Run 1
        report1 = mig_mod.migrate_installments(db)
        c = _conn(db)
        after1 = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["expenses", "installments", "installment_transaction_links",
                            "installment_suggestions", "income"]}
        uc_after1 = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                     for t in ["commitments", "commitment_installment_meta",
                               "commitment_expense_links", "commitment_occurrences"]}
        fk1 = c.execute("PRAGMA foreign_key_check").fetchall()
        c.close()

        # Run 2
        report2 = mig_mod.migrate_installments(db)
        c = _conn(db)
        after2 = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ["expenses", "installments", "installment_transaction_links",
                            "installment_suggestions", "income"]}
        uc_after2 = {t: c.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                     for t in ["commitments", "commitment_installment_meta",
                               "commitment_expense_links", "commitment_occurrences"]}
        fk2 = c.execute("PRAGMA foreign_key_check").fetchall()
        c.close()

        # Legacy rows unchanged across both runs
        assert before == after1 == after2

        # UC rows increased on run 1 (2 migrated)
        assert uc_after1["commitments"] == uc_before["commitments"] + 2
        assert uc_after1["commitment_installment_meta"] == uc_before["commitment_installment_meta"] + 2
        # Plan A: paid=3, total=12 → 9 future; Plan B: paid=1, total=6 → 5 future
        assert uc_after1["commitment_occurrences"] == uc_before["commitment_occurrences"] + 9 + 5
        # Links: 3 for plan A, 1 for plan B
        assert uc_after1["commitment_expense_links"] == uc_before["commitment_expense_links"] + 4

        # No duplicates on run 2
        assert uc_after1 == uc_after2

        # FK integrity
        assert fk1 == []
        assert fk2 == []

        # Check results
        res = {o.legacy_id: o.result for o in report1.outcomes}
        assert res[1] == mig_mod.MigrationResult.MIGRATED
        assert res[2] == mig_mod.MigrationResult.MIGRATED
        assert res[3] == mig_mod.MigrationResult.SKIPPED_STATUS_NOT_ELIGIBLE

        # Check payment_agorot is exact
        c = _conn(db)
        meta = {r["commitment_id"]: dict(r)
                for r in c.execute("SELECT * FROM commitment_installment_meta")}
        c.close()
        # Find plan B (123.45/month)
        plan_b_meta = next(
            m for m in meta.values() if m["total_payments"] == 6
        )
        assert plan_b_meta["payment_agorot"] == 12345

        # Verify occurrence indexes for plan A: should be 4..12
        c = _conn(db)
        plan_a_cid = next(o.commitment_id for o in report1.outcomes if o.legacy_id == 1)
        a_idxs = sorted(
            r[0] for r in c.execute(
                "SELECT occurrence_index FROM commitment_occurrences WHERE commitment_id=?",
                (plan_a_cid,)
            ).fetchall()
        )
        c.close()
        assert a_idxs == list(range(4, 13))  # M+1=4 .. N=12

    def test_foreign_key_check_zero(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, total=6, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        violations = c.execute("PRAGMA foreign_key_check").fetchall()
        c.close()
        assert violations == []


# ── Policy A ownership tests ──────────────────────────────────────────────────

class TestOwnershipPolicyA:
    """Exhaustive coverage of the Policy A ownership matrix."""

    def test_uid_gt0_no_links_migrates(self, mig_mod, app_mod, tmp_path):
        """user_id > 0, no expense links → MIGRATE using installment owner."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=5, total=3, paid=0)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED

    def test_uid_gt0_no_links_commitment_has_correct_user(self, mig_mod, app_mod, tmp_path):
        """Commitment created for installment owner, not some default."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=7, total=3, paid=0)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        row = c.execute("SELECT user_id FROM commitments").fetchone()
        c.close()
        assert row[0] == 7

    def test_uid_gt0_matching_links_migrates(self, mig_mod, app_mod, tmp_path):
        """user_id > 0, all expenses owned by same user → MIGRATE."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=1, date_="2024-02-15")
        _add_link(c, 1, 10); _add_link(c, 1, 11)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED

    def test_uid_gt0_single_mismatching_expense_conflict(self, mig_mod, app_mod, tmp_path):
        """user_id > 0, linked expense owned by different user → CONFLICT."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=1)
        _add_expense(c, 10, user_id=9)           # different owner
        _add_link(c, 1, 10, user_id=9)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.CONFLICT

    def test_uid_gt0_multiple_linked_owners_conflict(self, mig_mod, app_mod, tmp_path):
        """user_id > 0, multiple distinct expense owners → CONFLICT."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=2, date_="2024-02-15")
        _add_link(c, 1, 10); _add_link(c, 1, 11, user_id=2)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.CONFLICT

    def test_uid_zero_one_expense_owner_migrates(self, mig_mod, app_mod, tmp_path):
        """user_id=0 sentinel + exactly one expense owner → MIGRATE using expense owner."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=3, paid=1)
        _add_expense(c, 10, user_id=3)
        _add_link(c, 1, 10, user_id=3)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.MIGRATED

    def test_uid_zero_one_expense_owner_commitment_user(self, mig_mod, app_mod, tmp_path):
        """Commitment user_id comes from expense graph, not installment sentinel."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=3, paid=1)
        _add_expense(c, 10, user_id=4)
        _add_link(c, 1, 10, user_id=4)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        row = c.execute("SELECT user_id FROM commitments").fetchone()
        c.close()
        assert row[0] == 4

    def test_uid_zero_no_links_skipped_no_owner(self, mig_mod, app_mod, tmp_path):
        """user_id=0, no expense links → SKIPPED_NO_OWNER."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=3, paid=0)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.SKIPPED_NO_OWNER

    def test_uid_zero_multiple_expense_owners_skipped_multi(self, mig_mod, app_mod, tmp_path):
        """user_id=0, two distinct expense owners → SKIPPED_MULTI_OWNER."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=3, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=2, date_="2024-02-15")
        _add_link(c, 1, 10, user_id=1)
        _add_link(c, 1, 11, user_id=2)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.SKIPPED_MULTI_OWNER

    def test_migration_never_changes_installment_user_id(self, mig_mod, app_mod, tmp_path):
        """Legacy installments.user_id must be unchanged after migration."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=5, total=3, paid=0)
        c.commit()
        uid_before = c.execute("SELECT user_id FROM installments WHERE id=1").fetchone()[0]
        c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        uid_after = c.execute("SELECT user_id FROM installments WHERE id=1").fetchone()[0]
        c.close()
        assert uid_before == uid_after == 5

    def test_migration_never_changes_expense_user_id(self, mig_mod, app_mod, tmp_path):
        """Legacy expenses.user_id must be unchanged after migration."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit()
        uid_before = c.execute("SELECT user_id FROM expenses WHERE id=10").fetchone()[0]
        c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        uid_after = c.execute("SELECT user_id FROM expenses WHERE id=10").fetchone()[0]
        c.close()
        assert uid_before == uid_after == 1

    def test_conflict_leaves_zero_partial_rows(self, mig_mod, app_mod, tmp_path):
        """After CONFLICT, zero commitments, meta, links, or occurrences created."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=1)
        _add_expense(c, 10, user_id=9)           # owner mismatch → CONFLICT
        _add_link(c, 1, 10, user_id=9)
        c.commit(); c.close()
        report = mig_mod.migrate_installments(db)
        assert report.outcomes[0].result == mig_mod.MigrationResult.CONFLICT
        c = _conn(db)
        assert _count(c, "commitments") == 0
        assert _count(c, "commitment_installment_meta") == 0
        assert _count(c, "commitment_expense_links") == 0
        assert _count(c, "commitment_occurrences") == 0
        c.close()


# ── Migration provenance tests ────────────────────────────────────────────────

class TestMigrationProvenance:
    """commitment_expense_links.linked_by must be 'MIGRATION' for all migrated rows."""

    def test_all_cel_rows_have_migration_provenance(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=6, paid=2)
        _add_expense(c, 10, user_id=1)
        _add_expense(c, 11, user_id=1, date_="2024-02-15")
        _add_link(c, 1, 10); _add_link(c, 1, 11)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        rows = c.execute("SELECT linked_by FROM commitment_expense_links").fetchall()
        c.close()
        assert len(rows) == 2
        assert all(r[0] == "MIGRATION" for r in rows), \
            f"Expected all MIGRATION, got: {[r[0] for r in rows]}"

    def test_no_cel_row_has_manual_provenance(self, mig_mod, app_mod, tmp_path):
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        manual_count = c.execute(
            "SELECT COUNT(*) FROM commitment_expense_links WHERE linked_by='MANUAL'"
        ).fetchone()[0]
        c.close()
        assert manual_count == 0

    def test_migration_provenance_survives_idempotent_run(self, mig_mod, app_mod, tmp_path):
        """Second run does not alter linked_by of already-migrated rows."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=1, total=3, paid=1)
        _add_expense(c, 10, user_id=1)
        _add_link(c, 1, 10)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        mig_mod.migrate_installments(db)
        c = _conn(db)
        rows = c.execute("SELECT linked_by FROM commitment_expense_links").fetchall()
        c.close()
        assert all(r[0] == "MIGRATION" for r in rows)

    def test_uid_zero_fallback_also_uses_migration_provenance(self, mig_mod, app_mod, tmp_path):
        """user_id=0 path (expense graph fallback) also writes MIGRATION."""
        db = _fresh_db(app_mod, tmp_path)
        c = _conn(db)
        _add_installment(c, 1, user_id=0, total=3, paid=1)
        _add_expense(c, 10, user_id=3)
        _add_link(c, 1, 10, user_id=3)
        c.commit(); c.close()
        mig_mod.migrate_installments(db)
        c = _conn(db)
        row = c.execute("SELECT linked_by FROM commitment_expense_links").fetchone()
        c.close()
        assert row[0] == "MIGRATION"
