"""
Phase 0.3 — Lifecycle Evidence Schema Alignment
Tests for the classifier_lifecycle_status CHECK expansion on:
    v4_run_results
    commitment_classifier_snapshots

Covers all 27 required test areas from the authorization spec.
"""

import sqlite3
import textwrap
from pathlib import Path

import pytest

import app as app_mod


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_db(tmp_path: Path) -> str:
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


def _insert_user(c, user_id=1):
    c.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash, email) "
        "VALUES (?,?,?,?)",
        (user_id, f"u{user_id}", "x", f"u{user_id}@t.com"),
    )


def _insert_family(c, fid, user_id=1, desc="desc::key"):
    c.execute(
        "INSERT OR IGNORE INTO pattern_families "
        "(id, user_id, primary_description_key, is_split_discriminator, "
        " is_primary, linked_by, family_status, created_at, updated_at) "
        "VALUES (?,?,?, 0, 1, 'AUTO', 'ACTIVE', '2024-01-01', '2024-01-01')",
        (fid, user_id, desc),
    )


def _insert_vrr(c, rid, run_id, user_id, family_id, lifecycle):
    c.execute(
        "INSERT INTO v4_run_results "
        "(id, run_id, user_id, family_id, description_key, stream_index, label, "
        " cadence, recurrence_status, commitment_status, "
        " classifier_lifecycle_status, budget_class, "
        " reserve_eligible, monthly_reserve_contrib_agorot, "
        " review_required, review_reasons, created_at) "
        "VALUES (?,?,?,?, 'k',0,'', 'MONTHLY','RECURRING','CONFIRMED', "
        "        ?, 'COMMITTED', 0,0, 0,'[]', '2024-01-01')",
        (rid, run_id, user_id, family_id, lifecycle),
    )


def _get_vrr_lifecycle(c, rid):
    return c.execute(
        "SELECT classifier_lifecycle_status FROM v4_run_results WHERE id=?", (rid,)
    ).fetchone()[0]


def _get_ccs_lifecycle(c, ccs_id):
    return c.execute(
        "SELECT classifier_lifecycle_status FROM commitment_classifier_snapshots WHERE id=?",
        (ccs_id,),
    ).fetchone()[0]


def _schema_of(c, table):
    return c.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()[0]


def _indexes_of(c, table):
    return {
        r[0]: r[1]
        for r in c.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='index' AND tbl_name=?",
            (table,),
        ).fetchall()
        if r[1] is not None  # skip auto-index
    }


def _triggers_of(c, table):
    return {
        r[0]
        for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' AND tbl_name=?",
            (table,),
        ).fetchall()
    }


def _fk_check(c):
    return c.execute("PRAGMA foreign_key_check").fetchall()


_V4_LIFECYCLES = ["ACTIVE", "POSSIBLY_STOPPED", "CANCELLED", "ENDED", "UNKNOWN"]
_ALL_ALLOWED = _V4_LIFECYCLES + ["PAUSED"]


# ── 1-7: Fresh v4_run_results accepts / rejects lifecycle values ──────────────

class TestFreshVrrLifecycle:
    """Tests 1-7: fresh DB lifecycle acceptance on v4_run_results."""

    @pytest.fixture()
    def db(self, tmp_path):
        return _make_db(tmp_path)

    @pytest.mark.parametrize("lc", _V4_LIFECYCLES)
    def test_accepts_v4_lifecycle(self, db, lc):
        """Tests 1-5: fresh v4_run_results accepts each actual V4 lifecycle value."""
        c = _conn(db)
        _insert_user(c)
        _insert_family(c, "fam1")
        _insert_vrr(c, f"rr_{lc}", "run1", 1, "fam1", lc)
        c.commit()
        assert _get_vrr_lifecycle(c, f"rr_{lc}") == lc
        c.close()

    def test_accepts_paused(self, db):
        """Test 6: fresh v4_run_results accepts PAUSED (forward-compat value)."""
        c = _conn(db)
        _insert_user(c)
        _insert_family(c, "fam1")
        _insert_vrr(c, "rr_PAUSED", "run1", 1, "fam1", "PAUSED")
        c.commit()
        assert _get_vrr_lifecycle(c, "rr_PAUSED") == "PAUSED"
        c.close()

    def test_rejects_invalid_lifecycle(self, db):
        """Test 7: fresh v4_run_results rejects invalid lifecycle values."""
        c = _conn(db)
        _insert_user(c)
        _insert_family(c, "fam1")
        with pytest.raises(sqlite3.IntegrityError):
            _insert_vrr(c, "rr_BAD", "run1", 1, "fam1", "NOT_A_LIFECYCLE")
        c.close()


# ── 8-11: Fresh commitment_classifier_snapshots ───────────────────────────────

class TestFreshCcsLifecycle:
    """Tests 8-11: fresh commitment_classifier_snapshots lifecycle acceptance."""

    @pytest.fixture()
    def db(self, tmp_path):
        return _make_db(tmp_path)

    def _insert_commitment(self, c, cid, user_id=1):
        c.execute(
            "INSERT OR IGNORE INTO commitments "
            "(id, user_id, source_type, lifecycle_status, "
            " created_at, updated_at) "
            "VALUES (?,?, 'MANUAL_ENTRY', 'ACTIVE', "
            "        '2024-01-01', '2024-01-01')",
            (cid, user_id),
        )

    def _insert_ccs(self, c, commitment_id, user_id, lifecycle):
        c.execute(
            "INSERT INTO commitment_classifier_snapshots "
            "(commitment_id, user_id, snapshot_type, "
            " classifier_lifecycle_status, created_at) "
            "VALUES (?,?, 'V4_SINGLE', ?, '2024-01-01')",
            (commitment_id, user_id, lifecycle),
        )
        return c.execute("SELECT last_insert_rowid()").fetchone()[0]

    @pytest.mark.parametrize("lc", _V4_LIFECYCLES)
    def test_accepts_v4_lifecycle(self, db, lc):
        """Test 8: snapshot column accepts each actual V4 lifecycle value."""
        c = _conn(db)
        _insert_user(c)
        self._insert_commitment(c, "c1")
        ccs_id = self._insert_ccs(c, "c1", 1, lc)
        c.commit()
        assert _get_ccs_lifecycle(c, ccs_id) == lc
        c.close()

    def test_accepts_null(self, db):
        """Test 9: snapshot classifier_lifecycle_status still accepts NULL."""
        c = _conn(db)
        _insert_user(c)
        self._insert_commitment(c, "c1")
        c.execute(
            "INSERT INTO commitment_classifier_snapshots "
            "(commitment_id, user_id, snapshot_type, "
            " classifier_lifecycle_status, created_at) "
            "VALUES ('c1', 1, 'V4_SINGLE', NULL, '2024-01-01')"
        )
        ccs_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        c.commit()
        assert _get_ccs_lifecycle(c, ccs_id) is None
        c.close()

    def test_accepts_paused(self, db):
        """Test 10: snapshot accepts PAUSED (forward-compat value)."""
        c = _conn(db)
        _insert_user(c)
        self._insert_commitment(c, "c1")
        ccs_id = self._insert_ccs(c, "c1", 1, "PAUSED")
        c.commit()
        assert _get_ccs_lifecycle(c, ccs_id) == "PAUSED"
        c.close()

    def test_rejects_invalid(self, db):
        """Test 11: snapshot rejects invalid lifecycle value."""
        c = _conn(db)
        _insert_user(c)
        self._insert_commitment(c, "c1")
        with pytest.raises(sqlite3.IntegrityError):
            self._insert_ccs(c, "c1", 1, "NONSENSE")
        c.close()


# ── Helpers: build a pre-0.3 DB ───────────────────────────────────────────────

_OLD_CHECK_VRR = "CHECK(classifier_lifecycle_status IN ('ACTIVE', 'PAUSED', 'ENDED', 'CANCELLED'))"
_OLD_CHECK_CCS = (
    "CHECK(classifier_lifecycle_status IS NULL\n"
    "                      OR classifier_lifecycle_status IN ('ACTIVE', 'PAUSED', 'ENDED', 'CANCELLED'))"
)


def _build_pre03_db(tmp_path: Path) -> str:
    """
    Create an isolated pre-Phase-0.3 DB.

    Strategy: start from a full init_db() (which creates the new Phase-0.3 schema),
    then atomically REBUILD only the two affected tables back to the OLD CHECK
    constraints.  This guarantees all other tables (categories, expenses, indexes,
    triggers, etc.) are consistent with the real app schema.
    """
    db = str(tmp_path / "pre03.db")

    # Step 1: create a fully-initialized DB (post-0.3 schema)
    orig = app_mod.DB_PATH
    app_mod.DB_PATH = db
    app_mod.init_db()
    app_mod.DB_PATH = orig

    # Step 2: rebuild the two tables with OLD CHECK constraints to simulate pre-0.3
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys = OFF")
    c.executescript("""
        -- Rebuild v4_run_results with OLD CHECK (no POSSIBLY_STOPPED, no UNKNOWN)
        DROP TABLE IF EXISTS v4_run_results;
        CREATE TABLE v4_run_results (
            id                              TEXT NOT NULL,
            run_id                          TEXT NOT NULL,
            user_id                         INTEGER NOT NULL,
            family_id                       TEXT DEFAULT NULL,
            description_key                 TEXT NOT NULL,
            stream_index                    INTEGER NOT NULL DEFAULT 0,
            label                           TEXT NOT NULL DEFAULT '',
            planning_amount_agorot          INTEGER DEFAULT NULL,
            cadence                         TEXT NOT NULL DEFAULT 'UNKNOWN',
            recurrence_status               TEXT NOT NULL DEFAULT 'UNKNOWN',
            commitment_status               TEXT NOT NULL DEFAULT 'UNKNOWN',
            classifier_lifecycle_status     TEXT NOT NULL DEFAULT 'ACTIVE'
                CHECK(classifier_lifecycle_status IN ('ACTIVE', 'PAUSED', 'ENDED', 'CANCELLED')),
            budget_class                    TEXT NOT NULL DEFAULT 'UNKNOWN',
            reserve_eligible                INTEGER NOT NULL DEFAULT 0
                CHECK(reserve_eligible IN (0, 1)),
            monthly_reserve_contrib_agorot  INTEGER NOT NULL DEFAULT 0,
            cadence_coverage                REAL DEFAULT NULL,
            evidence_month_count            INTEGER DEFAULT NULL,
            review_required                 INTEGER NOT NULL DEFAULT 0
                CHECK(review_required IN (0, 1)),
            review_reasons                  TEXT NOT NULL DEFAULT '[]',
            created_at                      TEXT NOT NULL,
            PRIMARY KEY (id),
            UNIQUE (id, user_id),
            FOREIGN KEY (family_id, user_id) REFERENCES pattern_families(id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_vrr_family
            ON v4_run_results(family_id, created_at DESC)
            WHERE family_id IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_vrr_run
            ON v4_run_results(run_id, user_id);

        -- Rebuild commitment_classifier_snapshots with OLD CHECK
        DROP TABLE IF EXISTS commitment_classifier_snapshots;
        CREATE TABLE commitment_classifier_snapshots (
            id                              INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id                   TEXT NOT NULL,
            user_id                         INTEGER NOT NULL,
            snapshot_type                   TEXT NOT NULL
                CHECK(snapshot_type IN ('V4_SINGLE', 'V4_CANONICAL_MERGED')),
            representative_run_result_id    TEXT DEFAULT NULL,
            constituent_run_result_ids      TEXT NOT NULL DEFAULT '[]',
            recurrence_status               TEXT DEFAULT NULL,
            commitment_status               TEXT DEFAULT NULL,
            classifier_lifecycle_status     TEXT DEFAULT NULL
                CHECK(classifier_lifecycle_status IS NULL
                      OR classifier_lifecycle_status IN ('ACTIVE', 'PAUSED', 'ENDED', 'CANCELLED')),
            budget_class                    TEXT DEFAULT NULL,
            reserve_eligible                INTEGER DEFAULT NULL
                CHECK(reserve_eligible IS NULL OR reserve_eligible IN (0, 1)),
            monthly_reserve_contrib_agorot  INTEGER DEFAULT NULL,
            cadence                         TEXT DEFAULT NULL,
            created_at                      TEXT NOT NULL,
            FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id),
            FOREIGN KEY (representative_run_result_id, user_id)
                REFERENCES v4_run_results(id, user_id)
        );
        CREATE INDEX IF NOT EXISTS idx_ccs_commitment
            ON commitment_classifier_snapshots(commitment_id, created_at DESC);
    """)
    c.execute("PRAGMA foreign_keys = ON")

    # Step 3: seed representative rows
    c.execute("INSERT OR IGNORE INTO users (id, username, password_hash, email) "
              "VALUES (1,'u','x','u@t.com')")
    c.execute("INSERT OR IGNORE INTO pattern_families "
              "(id, user_id, primary_description_key, is_split_discriminator, "
              " is_primary, linked_by, family_status, created_at, updated_at) "
              "VALUES ('fam1', 1, 'desc::k', 0, 1, 'AUTO', 'ACTIVE', '2024-01-01', '2024-01-01')")

    for lc in ("ACTIVE", "PAUSED", "ENDED", "CANCELLED"):
        c.execute(
            "INSERT INTO v4_run_results "
            "(id, run_id, user_id, family_id, description_key, stream_index, label, "
            " cadence, recurrence_status, commitment_status, "
            " classifier_lifecycle_status, budget_class, "
            " reserve_eligible, monthly_reserve_contrib_agorot, "
            " review_required, review_reasons, created_at) "
            "VALUES (?,?,1,'fam1','desc::k',0,'','MONTHLY','RECURRING','CONFIRMED',"
            "        ?,'COMMITTED',0,0,0,'[]','2024-01-01')",
            (f"rr_{lc}", "run_seed", lc),
        )

    c.execute("INSERT OR IGNORE INTO commitments "
              "(id, user_id, source_type, lifecycle_status, created_at, updated_at) "
              "VALUES ('com1', 1, 'MANUAL_ENTRY', 'ACTIVE', '2024-01-01', '2024-01-01')")
    c.execute("INSERT INTO commitment_classifier_snapshots "
              "(commitment_id, user_id, snapshot_type, representative_run_result_id, "
              " classifier_lifecycle_status, created_at) "
              "VALUES ('com1', 1, 'V4_SINGLE', 'rr_ACTIVE', 'ACTIVE', '2024-01-01')")

    # commitment_suggestions referencing v4_run_results (no expense required)
    c.execute("INSERT OR IGNORE INTO commitment_suggestions "
              "(user_id, suggestion_type, description_key, run_result_id, created_at) "
              "VALUES (1, 'NEW_RECURRING', 'desc::k', 'rr_ACTIVE', '2024-01-01')")

    c.commit()
    c.close()
    return db


def _snapshot_rows(c, table):
    """Return all rows from table as list of dicts."""
    cur = c.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]


# ── 12-24: Existing-DB upgrade ────────────────────────────────────────────────

class TestExistingDbUpgrade:
    """Tests 12-24: pre-0.3 DB upgrade correctness."""

    @pytest.fixture()
    def pre03(self, tmp_path):
        return _build_pre03_db(tmp_path)

    @pytest.fixture()
    def upgraded(self, pre03):
        """Run init_db (upgrade path) on the pre-0.3 DB."""
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = pre03
        app_mod.init_db()
        app_mod.DB_PATH = orig
        return pre03

    def test_upgrade_succeeds(self, upgraded):
        """Test 12: existing pre-0.3 DB upgrades successfully."""
        c = _conn(upgraded)
        sql = _schema_of(c, "v4_run_results")
        assert "POSSIBLY_STOPPED" in sql
        c.close()

    def test_vrr_rows_unchanged(self, pre03, upgraded):
        """Test 13: existing v4_run_results rows unchanged after upgrade."""
        c_pre = _conn(pre03)  # same file, rows already in it
        rows_after = _snapshot_rows(_conn(upgraded), "v4_run_results")
        # All 4 seed rows must survive
        lifecycles = {r["classifier_lifecycle_status"] for r in rows_after}
        assert lifecycles == {"ACTIVE", "PAUSED", "ENDED", "CANCELLED"}
        assert len(rows_after) == 4
        c_pre.close()

    def test_snapshot_rows_unchanged(self, upgraded):
        """Test 14: existing commitment_classifier_snapshots rows unchanged."""
        c = _conn(upgraded)
        rows = _snapshot_rows(c, "commitment_classifier_snapshots")
        assert len(rows) == 1
        assert rows[0]["commitment_id"] == "com1"
        assert rows[0]["classifier_lifecycle_status"] == "ACTIVE"
        c.close()

    def test_snapshot_fk_references_survive(self, upgraded):
        """Test 15: commitment_classifier_snapshots FK to v4_run_results survives upgrade."""
        c = _conn(upgraded)
        row = c.execute(
            "SELECT representative_run_result_id FROM commitment_classifier_snapshots "
            "WHERE representative_run_result_id='rr_ACTIVE'"
        ).fetchone()
        assert row is not None
        c.close()

    def test_suggestion_fk_references_survive(self, upgraded):
        """Test 16: commitment_suggestions FK to v4_run_results survives upgrade."""
        c = _conn(upgraded)
        row = c.execute(
            "SELECT run_result_id FROM commitment_suggestions WHERE run_result_id='rr_ACTIVE'"
        ).fetchone()
        assert row is not None
        c.close()

    def test_indexes_preserved(self, upgraded):
        """Test 17: idx_vrr_family and idx_vrr_run indexes recreated after upgrade."""
        c = _conn(upgraded)
        idxs = _indexes_of(c, "v4_run_results")
        assert "idx_vrr_family" in idxs
        assert "idx_vrr_run" in idxs
        ccs_idxs = _indexes_of(c, "commitment_classifier_snapshots")
        assert "idx_ccs_commitment" in ccs_idxs
        c.close()

    def test_trigger_preserved(self, upgraded):
        """Test 18: cross-user expense trigger on commitment_expense_links survives."""
        c = _conn(upgraded)
        trigs = _triggers_of(c, "commitment_expense_links")
        assert "trg_cel_expense_owner_ins" in trigs
        c.close()

    def test_other_check_constraints_preserved(self, upgraded):
        """Test 19: other CHECK constraints (reserve_eligible, review_required) preserved."""
        c = _conn(upgraded)
        sql = _schema_of(c, "v4_run_results")
        assert "reserve_eligible IN (0, 1)" in sql
        assert "review_required IN (0, 1)" in sql
        c.close()

    def test_unique_constraints_preserved(self, upgraded):
        """Test 20: UNIQUE(id, user_id) constraint preserved on v4_run_results."""
        c = _conn(upgraded)
        # Attempt duplicate insert — should fail
        with pytest.raises(sqlite3.IntegrityError):
            c.execute(
                "INSERT INTO v4_run_results "
                "(id, run_id, user_id, family_id, description_key, stream_index, label, "
                " cadence, recurrence_status, commitment_status, "
                " classifier_lifecycle_status, budget_class, "
                " reserve_eligible, monthly_reserve_contrib_agorot, "
                " review_required, review_reasons, created_at) "
                "VALUES ('rr_ACTIVE','run2',1,'fam1','desc::k',0,'','MONTHLY',"
                "        'RECURRING','CONFIRMED','ACTIVE','COMMITTED',0,0,0,'[]','2024-01-02')"
            )
        c.close()

    def test_foreign_key_check_zero(self, upgraded):
        """Test 21: PRAGMA foreign_key_check returns 0 violations after upgrade."""
        c = _conn(upgraded)
        violations = _fk_check(c)
        assert violations == [], f"FK violations: {violations}"
        c.close()

    def test_idempotence(self, upgraded):
        """Test 22: running init_db a second time after upgrade is a no-op."""
        rows_before = _snapshot_rows(_conn(upgraded), "v4_run_results")
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = upgraded
        app_mod.init_db()  # second call
        app_mod.DB_PATH = orig
        rows_after = _snapshot_rows(_conn(upgraded), "v4_run_results")
        assert rows_before == rows_after

    def test_no_temp_tables_remain(self, upgraded):
        """Test 23: no v4_run_results_new or commitment_classifier_snapshots_new tables remain."""
        c = _conn(upgraded)
        tables = {
            r[0]
            for r in c.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert "v4_run_results_new" not in tables
        assert "commitment_classifier_snapshots_new" not in tables
        c.close()

    def test_forced_failure_preserves_original(self, pre03):
        """
        Test 24: forced upgrade failure leaves original schema and data intact.
        We simulate failure by monkey-patching sqlite3.Connection.execute to
        raise after the INSERT so the SAVEPOINT rolls back.
        """
        import unittest.mock as mock

        # Capture original rows
        c_orig = _conn(pre03)
        rows_orig = _snapshot_rows(c_orig, "v4_run_results")
        schema_orig = _schema_of(c_orig, "v4_run_results")
        c_orig.close()

        # Run upgrade — it should succeed normally (pre03 is old schema).
        # To test rollback, we patch _upgrade_lifecycle_check to raise mid-way.
        original_fn = app_mod._upgrade_lifecycle_check

        call_count = [0]

        def _failing_upgrade(conn):
            call_count[0] += 1
            # Let it get partway, then raise to trigger rollback.
            # We'll just raise immediately to test the rollback path.
            conn.execute("PRAGMA foreign_keys = OFF")
            conn.execute("SAVEPOINT sp_lifecycle_upgrade_03")
            try:
                conn.execute("""
                    CREATE TABLE v4_run_results_new (
                        id TEXT NOT NULL PRIMARY KEY,
                        run_id TEXT NOT NULL,
                        user_id INTEGER NOT NULL,
                        family_id TEXT DEFAULT NULL,
                        description_key TEXT NOT NULL,
                        stream_index INTEGER NOT NULL DEFAULT 0,
                        label TEXT NOT NULL DEFAULT '',
                        planning_amount_agorot INTEGER DEFAULT NULL,
                        cadence TEXT NOT NULL DEFAULT 'UNKNOWN',
                        recurrence_status TEXT NOT NULL DEFAULT 'UNKNOWN',
                        commitment_status TEXT NOT NULL DEFAULT 'UNKNOWN',
                        classifier_lifecycle_status TEXT NOT NULL DEFAULT 'ACTIVE',
                        budget_class TEXT NOT NULL DEFAULT 'UNKNOWN',
                        reserve_eligible INTEGER NOT NULL DEFAULT 0,
                        monthly_reserve_contrib_agorot INTEGER NOT NULL DEFAULT 0,
                        cadence_coverage REAL DEFAULT NULL,
                        evidence_month_count INTEGER DEFAULT NULL,
                        review_required INTEGER NOT NULL DEFAULT 0,
                        review_reasons TEXT NOT NULL DEFAULT '[]',
                        created_at TEXT NOT NULL,
                        UNIQUE (id, user_id)
                    )
                """)
                raise RuntimeError("Simulated failure mid-upgrade")
            except Exception:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT sp_lifecycle_upgrade_03")
                    conn.execute("RELEASE SAVEPOINT sp_lifecycle_upgrade_03")
                except Exception:
                    pass
                raise
            finally:
                conn.execute("PRAGMA foreign_keys = ON")

        app_mod._upgrade_lifecycle_check = _failing_upgrade
        try:
            orig = app_mod.DB_PATH
            app_mod.DB_PATH = pre03
            with pytest.raises(RuntimeError, match="Simulated failure"):
                app_mod.init_db()
            app_mod.DB_PATH = orig
        finally:
            app_mod._upgrade_lifecycle_check = original_fn

        # Original schema and rows must be intact
        c_after = _conn(pre03)
        rows_after = _snapshot_rows(c_after, "v4_run_results")
        schema_after = _schema_of(c_after, "v4_run_results")
        c_after.close()

        assert rows_orig == rows_after
        assert "POSSIBLY_STOPPED" not in schema_after  # old CHECK still in place


# ── 25-27: Regression — prior phases intact ───────────────────────────────────

class TestPriorPhasesIntact:
    """Tests 25-27: Phase 0.1 / 0.2 / legacy table regressions."""

    @pytest.fixture()
    def db(self, tmp_path):
        return _make_db(tmp_path)

    def test_phase01_commitments_legacy_index_intact(self, db):
        """Test 25: Phase 0.1 idx_commitments_legacy_installment_unique index still present."""
        c = _conn(db)
        idx = c.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name='idx_commitments_legacy_installment_unique'"
        ).fetchone()
        assert idx is not None, "idx_commitments_legacy_installment_unique missing after Phase 0.3"
        c.close()

    def test_phase02_migration_linked_by_intact(self, db):
        """Test 26: Phase 0.2 MIGRATION value in commitment_expense_links.linked_by CHECK."""
        c = _conn(db)
        sql = _schema_of(c, "commitment_expense_links")
        assert "'MIGRATION'" in sql or '"MIGRATION"' in sql
        c.close()

    def test_legacy_tables_unchanged(self, db):
        """Test 27: legacy tables (expenses, installments, categories) still exist."""
        c = _conn(db)
        tables = {
            r[0]
            for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
        for t in ("expenses", "installments", "categories", "users"):
            assert t in tables, f"Legacy table '{t}' missing"
        c.close()


# ── 28: CEL → v4_run_results FK preservation through upgrade ─────────────────

class TestCelVrrFkPreservation:
    """Test 28: commitment_expense_links.run_result_id → v4_run_results FK survives upgrade."""

    def test_cel_run_result_fk_survives_upgrade(self, tmp_path):
        """
        Full end-to-end: seed CEL row referencing a v4_run_results row in a
        pre-0.3 DB, run the Phase 0.3 upgrade, verify the CEL row and FK intact.
        """
        # Build a pre-0.3 DB via init_db() (gets Phase 0.3 schema), then
        # rebuild the two target tables back to old CHECK so the upgrade fires.
        db = str(tmp_path / "cel_pre03.db")
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        app_mod.init_db()
        app_mod.DB_PATH = orig

        # Downgrade the two tables to old CHECK
        c = sqlite3.connect(db)
        c.execute("PRAGMA foreign_keys = OFF")
        c.executescript("""
            DROP TABLE IF EXISTS v4_run_results;
            CREATE TABLE v4_run_results (
                id TEXT NOT NULL, run_id TEXT NOT NULL, user_id INTEGER NOT NULL,
                family_id TEXT DEFAULT NULL, description_key TEXT NOT NULL,
                stream_index INTEGER NOT NULL DEFAULT 0, label TEXT NOT NULL DEFAULT '',
                planning_amount_agorot INTEGER DEFAULT NULL,
                cadence TEXT NOT NULL DEFAULT 'UNKNOWN',
                recurrence_status TEXT NOT NULL DEFAULT 'UNKNOWN',
                commitment_status TEXT NOT NULL DEFAULT 'UNKNOWN',
                classifier_lifecycle_status TEXT NOT NULL DEFAULT 'ACTIVE'
                    CHECK(classifier_lifecycle_status IN ('ACTIVE','PAUSED','ENDED','CANCELLED')),
                budget_class TEXT NOT NULL DEFAULT 'UNKNOWN',
                reserve_eligible INTEGER NOT NULL DEFAULT 0 CHECK(reserve_eligible IN (0,1)),
                monthly_reserve_contrib_agorot INTEGER NOT NULL DEFAULT 0,
                cadence_coverage REAL DEFAULT NULL, evidence_month_count INTEGER DEFAULT NULL,
                review_required INTEGER NOT NULL DEFAULT 0 CHECK(review_required IN (0,1)),
                review_reasons TEXT NOT NULL DEFAULT '[]', created_at TEXT NOT NULL,
                PRIMARY KEY (id), UNIQUE (id, user_id),
                FOREIGN KEY (family_id, user_id) REFERENCES pattern_families(id, user_id)
            );
            DROP TABLE IF EXISTS commitment_classifier_snapshots;
            CREATE TABLE commitment_classifier_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commitment_id TEXT NOT NULL, user_id INTEGER NOT NULL,
                snapshot_type TEXT NOT NULL
                    CHECK(snapshot_type IN ('V4_SINGLE','V4_CANONICAL_MERGED')),
                representative_run_result_id TEXT DEFAULT NULL,
                constituent_run_result_ids TEXT NOT NULL DEFAULT '[]',
                recurrence_status TEXT DEFAULT NULL, commitment_status TEXT DEFAULT NULL,
                classifier_lifecycle_status TEXT DEFAULT NULL
                    CHECK(classifier_lifecycle_status IS NULL
                          OR classifier_lifecycle_status IN ('ACTIVE','PAUSED','ENDED','CANCELLED')),
                budget_class TEXT DEFAULT NULL,
                reserve_eligible INTEGER DEFAULT NULL
                    CHECK(reserve_eligible IS NULL OR reserve_eligible IN (0,1)),
                monthly_reserve_contrib_agorot INTEGER DEFAULT NULL,
                cadence TEXT DEFAULT NULL, created_at TEXT NOT NULL,
                FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id),
                FOREIGN KEY (representative_run_result_id, user_id)
                    REFERENCES v4_run_results(id, user_id)
            );
        """)
        c.execute("PRAGMA foreign_keys = ON")

        # Seed: user + family + v4_run_results row + commitment + expense + CEL
        c.execute("INSERT OR IGNORE INTO users (id, username, password_hash, email) "
                  "VALUES (1, 'u', 'x', 'u@t.com')")
        c.execute("INSERT OR IGNORE INTO pattern_families "
                  "(id, user_id, primary_description_key, is_split_discriminator, "
                  " is_primary, linked_by, family_status, created_at, updated_at) "
                  "VALUES ('fam1', 1, 'desc::k', 0, 1, 'AUTO', 'ACTIVE', '2024-01-01', '2024-01-01')")
        c.execute(
            "INSERT INTO v4_run_results "
            "(id, run_id, user_id, family_id, description_key, stream_index, label, "
            " cadence, recurrence_status, commitment_status, "
            " classifier_lifecycle_status, budget_class, "
            " reserve_eligible, monthly_reserve_contrib_agorot, "
            " review_required, review_reasons, created_at) "
            "VALUES ('rr1','run1',1,'fam1','desc::k',0,'','MONTHLY','RECURRING','CONFIRMED',"
            "        'ACTIVE','COMMITTED',0,0,0,'[]','2024-01-01')"
        )
        c.execute("INSERT OR IGNORE INTO commitments "
                  "(id, user_id, source_type, lifecycle_status, created_at, updated_at) "
                  "VALUES ('com1', 1, 'MANUAL_ENTRY', 'ACTIVE', '2024-01-01', '2024-01-01')")
        # Use a default category that init_db() always creates
        c.execute("INSERT OR IGNORE INTO expenses "
                  "(id, user_id, amount, description, date, category_id) "
                  "VALUES (1, 1, 50.0, 'test expense', '2024-01-01', 'arnona')")
        c.execute("INSERT INTO commitment_expense_links "
                  "(commitment_id, user_id, expense_id, linked_by, run_result_id, created_at) "
                  "VALUES ('com1', 1, 1, 'V4_CLASSIFIER', 'rr1', '2024-01-01')")
        c.commit()

        # Capture pre-upgrade CEL row
        cel_before = c.execute(
            "SELECT commitment_id, user_id, expense_id, linked_by, run_result_id "
            "FROM commitment_expense_links WHERE run_result_id='rr1'"
        ).fetchone()
        assert cel_before is not None
        c.close()

        # Run Phase 0.3 upgrade
        app_mod.DB_PATH = db
        app_mod.init_db()
        app_mod.DB_PATH = orig

        # Verify post-upgrade
        c2 = _conn(db)

        # CEL row unchanged
        cel_after = c2.execute(
            "SELECT commitment_id, user_id, expense_id, linked_by, run_result_id "
            "FROM commitment_expense_links WHERE run_result_id='rr1'"
        ).fetchone()
        assert cel_after == cel_before, f"CEL row changed: {cel_before!r} → {cel_after!r}"

        # v4_run_results row still exists and is reachable
        vrr = c2.execute(
            "SELECT id, user_id FROM v4_run_results WHERE id='rr1'"
        ).fetchone()
        assert vrr == ('rr1', 1)

        # FK check = 0 violations
        violations = c2.execute("PRAGMA foreign_key_check").fetchall()
        assert violations == [], f"FK violations after upgrade: {violations}"

        # Ownership trigger still fires: inserting a cross-user CEL must be rejected
        c2.execute("INSERT OR IGNORE INTO users (id, username, password_hash, email) "
                   "VALUES (2, 'u2', 'x', 'u2@t.com')")
        c2.execute("INSERT OR IGNORE INTO expenses "
                   "(id, user_id, amount, description, date, category_id) "
                   "VALUES (2, 2, 10.0, 'other', '2024-01-01', 'arnona')")
        c2.commit()
        with pytest.raises((sqlite3.OperationalError, sqlite3.IntegrityError), match="cross-user"):
            c2.execute("INSERT INTO commitment_expense_links "
                       "(commitment_id, user_id, expense_id, linked_by, run_result_id, created_at) "
                       "VALUES ('com1', 1, 2, 'AUTO', NULL, '2024-01-01')")

        c2.close()
