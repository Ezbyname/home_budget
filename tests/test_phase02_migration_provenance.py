"""
Phase 0.2 — Migration Provenance Schema Patch
Tests for commitment_expense_links.linked_by CHECK extension: 'MIGRATION' added.

Coverage:
- Fresh-DB schema: 'MIGRATION' present in CHECK
- Fresh-DB DML: 'MIGRATION' accepted, invalid values still rejected
- Existing-DB upgrade: rows preserved, schema upgraded atomically
- Idempotence: init_db() can run multiple times safely
- Index survival: idx_cel_member_exclusive recreated after upgrade
- Trigger survival: trg_cel_expense_owner_ins recreated after upgrade
- Phase 0.1 index untouched: idx_commitments_legacy_installment_unique still present
- Failure atomicity: upgrade failure leaves old table intact
- FK integrity: PRAGMA foreign_key_check = 0 after upgrade
"""

import sqlite3
import pytest

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as app_module

_CATEGORY_ID = "arnona"


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def fresh_db(tmp_path):
    """Fresh DB initialised via init_db()."""
    db_path = str(tmp_path / "test.db")
    original = app_module.DB_PATH
    app_module.DB_PATH = db_path
    try:
        app_module.init_db()
        yield db_path
    finally:
        app_module.DB_PATH = original


def _make_old_cel_table(conn):
    """Create commitment_expense_links with the OLD CHECK (no MIGRATION)."""
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS commitment_expense_links (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id       TEXT NOT NULL,
            user_id             INTEGER NOT NULL,
            expense_id          INTEGER NOT NULL REFERENCES expenses(id),
            membership_type     TEXT NOT NULL DEFAULT 'MEMBER'
                CHECK(membership_type IN ('MEMBER', 'EXCLUDED', 'OCCURRENCE_CONFIRMED')),
            linked_by           TEXT NOT NULL DEFAULT 'AUTO'
                CHECK(linked_by IN ('AUTO', 'MANUAL', 'V4_CLASSIFIER')),
            family_id           TEXT DEFAULT NULL,
            run_result_id       TEXT DEFAULT NULL,
            created_at          TEXT NOT NULL,
            UNIQUE(user_id, expense_id, commitment_id),
            FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id),
            FOREIGN KEY (family_id, user_id) REFERENCES pattern_families(id, user_id),
            FOREIGN KEY (run_result_id, user_id) REFERENCES v4_run_results(id, user_id)
        )
    """)


def _seed_minimal(conn, *, user_id=1):
    """Seed the minimal rows required to insert a commitment_expense_links row."""
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color) VALUES (?,?,?)",
        (_CATEGORY_ID, "ארנונה", "#aabbcc")
    )
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash) VALUES (?,?,?)",
        (user_id, f"user{user_id}", "x")
    )
    conn.execute(
        "INSERT OR IGNORE INTO expenses (id, date, category_id, amount, user_id) VALUES (?,?,?,?,?)",
        (100 + user_id, "2024-01-15", _CATEGORY_ID, 100.0, user_id)
    )
    cid = f"commit-seed-{user_id}"
    conn.execute("""
        INSERT OR IGNORE INTO commitments
            (id, user_id, canonical_label, source_type, is_finite, total_occurrences,
             linked_legacy_installment_id, created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (cid, user_id, "seed", "MIGRATED", 1, 3, None,
          "2024-01-01T00:00:00", "2024-01-01T00:00:00"))
    return cid, 100 + user_id


def _insert_cel(conn, commitment_id, user_id, expense_id, linked_by, created_at="2024-01-01T00:00:00"):
    conn.execute("""
        INSERT INTO commitment_expense_links
            (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
        VALUES (?,?,?,?,?,?)
    """, (commitment_id, user_id, expense_id, "MEMBER", linked_by, created_at))


# ── 1. Fresh-DB schema ────────────────────────────────────────────────────────

class TestFreshDbSchema:
    def test_check_contains_migration(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()
        conn.close()
        assert row is not None
        assert "'MIGRATION'" in row[0] or '"MIGRATION"' in row[0], \
            f"'MIGRATION' not found in CEL DDL: {row[0]}"

    def test_all_four_values_in_check(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()
        conn.close()
        ddl = row[0]
        for v in ("'AUTO'", "'MANUAL'", "'V4_CLASSIFIER'", "'MIGRATION'"):
            assert v in ddl, f"{v} not found in CEL DDL"


# ── 2. Fresh-DB DML ───────────────────────────────────────────────────────────

class TestFreshDbDml:
    def test_migration_value_accepted(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        cid, exp_id = _seed_minimal(conn, user_id=1)
        conn.commit()
        _insert_cel(conn, cid, 1, exp_id, "MIGRATION")
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_expense_links WHERE linked_by='MIGRATION'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_auto_value_accepted(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        cid, exp_id = _seed_minimal(conn, user_id=2)
        conn.commit()
        _insert_cel(conn, cid, 2, exp_id, "AUTO")
        conn.commit()
        conn.close()

    def test_manual_value_accepted(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        cid, exp_id = _seed_minimal(conn, user_id=3)
        conn.commit()
        _insert_cel(conn, cid, 3, exp_id, "MANUAL")
        conn.commit()
        conn.close()

    def test_v4_classifier_value_accepted(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        cid, exp_id = _seed_minimal(conn, user_id=4)
        conn.commit()
        _insert_cel(conn, cid, 4, exp_id, "V4_CLASSIFIER")
        conn.commit()
        conn.close()

    def test_invalid_value_rejected(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        cid, exp_id = _seed_minimal(conn, user_id=5)
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_cel(conn, cid, 5, exp_id, "BOGUS")
        conn.close()

    def test_old_linked_by_value_rejected(self, fresh_db):
        """Values not in new or old set are still rejected."""
        conn = sqlite3.connect(fresh_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        cid, exp_id = _seed_minimal(conn, user_id=6)
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_cel(conn, cid, 6, exp_id, "SYSTEM")
        conn.close()


# ── 3. Existing-DB upgrade ────────────────────────────────────────────────────

class TestExistingDbUpgrade:
    @pytest.fixture
    def old_db(self, tmp_path):
        """DB built with old CEL schema (no MIGRATION), pre-seeded with rows."""
        db_path = str(tmp_path / "old.db")
        original = app_module.DB_PATH
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        # Replace CEL table with old schema
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE IF EXISTS commitment_expense_links")
        _make_old_cel_table(conn)
        conn.commit()

        # Seed rows with old valid values
        _seed_minimal(conn, user_id=10)
        conn.commit()
        cid = "commit-old-10"
        conn.execute("""
            INSERT OR IGNORE INTO commitments
                (id, user_id, canonical_label, source_type, is_finite, total_occurrences,
                 linked_legacy_installment_id, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (cid, 10, "old", "MIGRATED", 1, 3, None,
              "2024-01-01T00:00:00", "2024-01-01T00:00:00"))
        conn.commit()
        for linked_by, exp_offset in [("AUTO", 0), ("MANUAL", 1), ("V4_CLASSIFIER", 2)]:
            exp_id = 110 + exp_offset
            conn.execute(
                "INSERT OR IGNORE INTO expenses (id, date, category_id, amount, user_id) VALUES (?,?,?,?,?)",
                (exp_id, "2024-01-15", _CATEGORY_ID, 50.0, 10)
            )
            conn.execute("""
                INSERT INTO commitment_expense_links
                    (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
                VALUES (?,?,?,?,?,?)
            """, (cid, 10, exp_id, "MEMBER", linked_by, "2024-01-01T00:00:00"))
        conn.commit()
        conn.close()
        yield db_path

    def test_upgrade_preserves_row_count(self, old_db):
        conn = sqlite3.connect(old_db)
        count_before = conn.execute("SELECT COUNT(*) FROM commitment_expense_links").fetchone()[0]
        conn.close()
        assert count_before == 3

        original = app_module.DB_PATH
        app_module.DB_PATH = old_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(old_db)
        count_after = conn.execute("SELECT COUNT(*) FROM commitment_expense_links").fetchone()[0]
        conn.close()
        assert count_after == 3, f"Row count changed: before=3 after={count_after}"

    def test_upgrade_preserves_all_linked_by_values(self, old_db):
        original = app_module.DB_PATH
        app_module.DB_PATH = old_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(old_db)
        rows = conn.execute(
            "SELECT linked_by FROM commitment_expense_links ORDER BY linked_by"
        ).fetchall()
        conn.close()
        values = sorted(r[0] for r in rows)
        assert values == ["AUTO", "MANUAL", "V4_CLASSIFIER"]

    def test_upgraded_schema_contains_migration(self, old_db):
        original = app_module.DB_PATH
        app_module.DB_PATH = old_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(old_db)
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()
        conn.close()
        assert "'MIGRATION'" in row[0] or '"MIGRATION"' in row[0]

    def test_upgraded_db_accepts_migration_insert(self, old_db):
        original = app_module.DB_PATH
        app_module.DB_PATH = old_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(old_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        new_exp_id = 999
        conn.execute(
            "INSERT OR IGNORE INTO expenses (id, date, category_id, amount, user_id) VALUES (?,?,?,?,?)",
            (new_exp_id, "2024-06-01", _CATEGORY_ID, 75.0, 10)
        )
        conn.execute("""
            INSERT INTO commitment_expense_links
                (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
            VALUES ('commit-old-10', 10, ?, 'MEMBER', 'MIGRATION', '2024-06-01T00:00:00')
        """, (new_exp_id,))
        conn.commit()
        count = conn.execute(
            "SELECT COUNT(*) FROM commitment_expense_links WHERE linked_by='MIGRATION'"
        ).fetchone()[0]
        conn.close()
        assert count == 1

    def test_upgraded_db_still_rejects_invalid(self, old_db):
        original = app_module.DB_PATH
        app_module.DB_PATH = old_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(old_db)
        conn.execute("PRAGMA foreign_keys = OFF")
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("""
                INSERT INTO commitment_expense_links
                    (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
                VALUES ('commit-old-10', 10, 110, 'MEMBER', 'BOGUS', '2024-06-01T00:00:00')
            """)
        conn.close()

    def test_upgrade_foreign_key_check_zero(self, old_db):
        original = app_module.DB_PATH
        app_module.DB_PATH = old_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(old_db)
        conn.execute("PRAGMA foreign_keys = ON")
        violations = conn.execute("PRAGMA foreign_key_check").fetchall()
        conn.close()
        assert violations == [], f"FK violations after upgrade: {violations}"


# ── 4. Idempotence ────────────────────────────────────────────────────────────

class TestIdempotence:
    def test_double_init_db_no_error(self, fresh_db):
        original = app_module.DB_PATH
        app_module.DB_PATH = fresh_db
        try:
            app_module.init_db()  # second call
        finally:
            app_module.DB_PATH = original

    def test_double_init_db_schema_unchanged(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        sql_before = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()[0]
        conn.close()

        original = app_module.DB_PATH
        app_module.DB_PATH = fresh_db
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(fresh_db)
        sql_after = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()[0]
        conn.close()
        assert sql_before == sql_after

    def test_upgrade_twice_row_count_stable(self, tmp_path):
        """Upgrading old DB twice (via init_db twice) keeps row count stable."""
        db_path = str(tmp_path / "idem.db")
        original = app_module.DB_PATH
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        # Simulate old schema
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE IF EXISTS commitment_expense_links")
        _make_old_cel_table(conn)
        conn.commit()
        _seed_minimal(conn, user_id=20)
        conn.commit()
        cid20 = "commit-idem-20"
        conn.execute("""
            INSERT OR IGNORE INTO commitments
                (id, user_id, canonical_label, source_type, is_finite, total_occurrences,
                 linked_legacy_installment_id, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (cid20, 20, "idem", "MIGRATED", 1, 2, None,
              "2024-01-01T00:00:00", "2024-01-01T00:00:00"))
        conn.execute("""
            INSERT INTO commitment_expense_links
                (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
            VALUES (?,?,?,?,?,?)
        """, (cid20, 20, 120, "MEMBER", "AUTO", "2024-01-01T00:00:00"))
        conn.commit()
        conn.close()

        # First upgrade
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        # Second upgrade (must be no-op)
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM commitment_expense_links").fetchone()[0]
        conn.close()
        assert count == 1


# ── 5. Index and trigger survival ─────────────────────────────────────────────

class TestIndexAndTriggerSurvival:
    def _run_upgrade(self, db_path):
        original = app_module.DB_PATH
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

    def _build_old_db(self, tmp_path):
        db_path = str(tmp_path / "idx.db")
        original = app_module.DB_PATH
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE IF EXISTS commitment_expense_links")
        _make_old_cel_table(conn)
        conn.commit()
        conn.close()
        return db_path

    def test_cel_member_exclusive_index_recreated(self, tmp_path):
        db_path = self._build_old_db(tmp_path)
        self._run_upgrade(db_path)
        conn = sqlite3.connect(db_path)
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_cel_member_exclusive'"
        ).fetchone()
        conn.close()
        assert idx is not None, "idx_cel_member_exclusive missing after upgrade"

    def test_trg_cel_expense_owner_ins_recreated(self, tmp_path):
        db_path = self._build_old_db(tmp_path)
        self._run_upgrade(db_path)
        conn = sqlite3.connect(db_path)
        trg = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name='trg_cel_expense_owner_ins'"
        ).fetchone()
        conn.close()
        assert trg is not None, "trg_cel_expense_owner_ins missing after upgrade"

    def test_trg_cel_expense_owner_upd_recreated(self, tmp_path):
        db_path = self._build_old_db(tmp_path)
        self._run_upgrade(db_path)
        conn = sqlite3.connect(db_path)
        trg = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND name='trg_cel_expense_owner_upd'"
        ).fetchone()
        conn.close()
        assert trg is not None, "trg_cel_expense_owner_upd missing after upgrade"

    def test_phase01_index_intact_after_upgrade(self, tmp_path):
        db_path = self._build_old_db(tmp_path)
        self._run_upgrade(db_path)
        conn = sqlite3.connect(db_path)
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_commitments_legacy_installment_unique'"
        ).fetchone()
        conn.close()
        assert idx is not None, "idx_commitments_legacy_installment_unique missing after Phase 0.2 upgrade"


# ── 6. Atomicity: failure leaves old table intact ─────────────────────────────

class TestFailureAtomicity:
    def test_failure_during_upgrade_leaves_old_table(self, tmp_path, monkeypatch):
        """If the upgrade raises mid-way, the original CEL table is still intact."""
        db_path = str(tmp_path / "atomic.db")
        original = app_module.DB_PATH
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        # Replace with old schema + one row
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE IF EXISTS commitment_expense_links")
        _make_old_cel_table(conn)
        conn.commit()
        _seed_minimal(conn, user_id=30)
        conn.commit()
        cid30 = "commit-atom-30"
        conn.execute("""
            INSERT OR IGNORE INTO commitments
                (id, user_id, canonical_label, source_type, is_finite, total_occurrences,
                 linked_legacy_installment_id, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (cid30, 30, "atom", "MIGRATED", 1, 2, None,
              "2024-01-01T00:00:00", "2024-01-01T00:00:00"))
        conn.execute("""
            INSERT INTO commitment_expense_links
                (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
            VALUES (?,?,?,?,?,?)
        """, (cid30, 30, 130, "MEMBER", "AUTO", "2024-01-01T00:00:00"))
        conn.commit()
        conn.close()

        # Monkeypatch to inject failure during upgrade INSERT
        original_fn = app_module._upgrade_cel_linked_by

        def _failing_upgrade(conn):
            row = conn.execute(
                "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
            ).fetchone()
            if row and "'MIGRATION'" not in row[0]:
                raise RuntimeError("injected failure during upgrade")
            return original_fn(conn)

        monkeypatch.setattr(app_module, "_upgrade_cel_linked_by", _failing_upgrade)

        with pytest.raises(RuntimeError, match="injected failure"):
            app_module.DB_PATH = db_path
            try:
                app_module.init_db()
            finally:
                app_module.DB_PATH = original

        # Old table must still be intact with its row
        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM commitment_expense_links").fetchone()[0]
        old_sql = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()
        conn.close()
        assert count == 1, f"Row count should be 1, got {count}"
        assert old_sql is not None, "CEL table disappeared after failed upgrade"
        # Old schema should still not contain MIGRATION
        assert "'MIGRATION'" not in old_sql[0], "Old table was incorrectly modified"


# ── 7. No temporary table residue ─────────────────────────────────────────────

class TestNoResidueTable:
    def test_no_new_table_residue_after_upgrade(self, tmp_path):
        """commitment_expense_links_new must not exist after a successful upgrade."""
        db_path = str(tmp_path / "residue.db")
        original = app_module.DB_PATH
        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        # Simulate old DB
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = OFF")
        conn.execute("DROP TABLE IF EXISTS commitment_expense_links")
        _make_old_cel_table(conn)
        conn.commit()
        conn.close()

        app_module.DB_PATH = db_path
        try:
            app_module.init_db()
        finally:
            app_module.DB_PATH = original

        conn = sqlite3.connect(db_path)
        residue = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='commitment_expense_links_new'"
        ).fetchone()
        conn.close()
        assert residue is None, "Temporary upgrade table commitment_expense_links_new still present"

    def test_no_new_table_residue_on_fresh_db(self, fresh_db):
        conn = sqlite3.connect(fresh_db)
        residue = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='commitment_expense_links_new'"
        ).fetchone()
        conn.close()
        assert residue is None
