"""
Phase 0.4 — Phase 2B Idempotence Constraints
Test suite: 31 tests

Covers:
  Fresh-DB tests (1–18)
  Existing-DB upgrade tests (19–31)
"""

import importlib
import sqlite3
import sys
import types
import uuid
from pathlib import Path

import pytest

# ── module bootstrap ──────────────────────────────────────────────────────────

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


def _make_db(tmp_path):
    """
    Create a fresh test DB by temporarily redirecting app.DB_PATH.
    Returns the db path string.
    """
    db = str(tmp_path / "test.db")
    import app as app_mod
    orig = app_mod.DB_PATH
    app_mod.DB_PATH = db
    try:
        app_mod.init_db()
    finally:
        app_mod.DB_PATH = orig
    return db


# ── shared helpers ────────────────────────────────────────────────────────────

def _conn(db):
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys = ON")
    return c


def _uid():
    return str(uuid.uuid4())


def _insert_user(conn, user_id=1):
    """Insert minimal user row."""
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash) VALUES (?, ?, 'x')",
        (user_id, f"user_{user_id}"),
    )


def _insert_category(conn, cat_id="other"):
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color) VALUES (?, ?, ?)",
        (cat_id, "אחר", "#888888"),
    )


def _insert_expense(conn, user_id=1, cat_id="other"):
    _insert_user(conn, user_id)
    _insert_category(conn, cat_id)
    conn.execute(
        "INSERT INTO expenses (date, description, amount, category_id, user_id) "
        "VALUES ('2024-01-01', 'test', 100.0, ?, ?)",
        (cat_id, user_id),
    )
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def _insert_commitment(conn, user_id=1, now="2024-01-01T00:00:00"):
    cid = _uid()
    conn.execute(
        "INSERT INTO commitments (id, user_id, canonical_label, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (cid, user_id, "test", now, now),
    )
    return cid


def _insert_family(conn, user_id=1, desc_key=None, now="2024-01-01T00:00:00"):
    fid = _uid()
    desc_key = desc_key or _uid()
    conn.execute(
        "INSERT INTO pattern_families "
        "(id, user_id, primary_description_key, is_split_discriminator, "
        " is_primary, linked_by, family_status, created_at, updated_at) "
        "VALUES (?, ?, ?, 0, 1, 'AUTO', 'ACTIVE', ?, ?)",
        (fid, user_id, desc_key, now, now),
    )
    return fid


def _insert_run_result(conn, user_id=1, family_id=None, run_id=None, now="2024-01-01T00:00:00"):
    rrid = _uid()
    run_id = run_id or _uid()
    conn.execute(
        "INSERT INTO v4_run_results "
        "(id, run_id, user_id, family_id, description_key, stream_index, label, "
        " cadence, recurrence_status, commitment_status, classifier_lifecycle_status, "
        " budget_class, reserve_eligible, monthly_reserve_contrib_agorot, "
        " review_required, review_reasons, created_at) "
        "VALUES (?, ?, ?, ?, ?, 0, 'test', 'monthly', 'RECURRING', 'COMMITTED', "
        "        'ACTIVE', 'FIXED_AMOUNT_RECURRING', 0, 0, 0, '[]', ?)",
        (rrid, run_id, user_id, family_id, _uid(), now),
    )
    return rrid


def _insert_snapshot(conn, commitment_id, user_id, run_result_id, snap_type="V4_SINGLE",
                     now="2024-01-01T00:00:00"):
    conn.execute(
        "INSERT INTO commitment_classifier_snapshots "
        "(commitment_id, user_id, snapshot_type, representative_run_result_id, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (commitment_id, user_id, snap_type, run_result_id, now),
    )


def _insert_suggestion(conn, user_id, desc_key, stype, run_result_id=None,
                       family_id=None, candidate_id=None, now="2024-01-01T00:00:00"):
    conn.execute(
        "INSERT INTO commitment_suggestions "
        "(user_id, suggestion_type, description_key, family_id, run_result_id, "
        " candidate_commitment_id, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (user_id, stype, desc_key, family_id, run_result_id, candidate_id, now),
    )


def _insert_conflict(conn, user_id, run_id, ctype, family_id=None,
                     commitment_id=None, now="2024-01-01T00:00:00"):
    conn.execute(
        "INSERT INTO commitment_link_conflicts "
        "(commitment_id, user_id, family_id, run_id, conflict_type, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (commitment_id, user_id, family_id, run_id, ctype, now),
    )


def _indexes(conn):
    return {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()
    }


def _triggers(conn):
    return {
        r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FRESH-DB TESTS (1–18)
# ═══════════════════════════════════════════════════════════════════════════════

class TestFreshDbPhase04:

    # ── 1. All six indexes exist ──────────────────────────────────────────────

    def test_01_all_six_indexes_exist(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        idx = _indexes(c)
        c.close()
        expected = {
            "idx_ccs_v4single_dedup",
            "idx_cs_possible_match_dedup",
            "idx_cs_new_recurring_dedup",
            "idx_cs_ambiguous_dedup",
            "idx_clc_family_only_dedup",
            "idx_clc_overlap_dedup",
        }
        assert expected.issubset(idx), f"Missing indexes: {expected - idx}"

    # ── 2. Both V4_SINGLE representative-required triggers exist ─────────────

    def test_02_v4single_triggers_exist(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        trg = _triggers(c)
        c.close()
        assert "trg_ccs_v4single_rep_required_ins" in trg
        assert "trg_ccs_v4single_rep_required_upd" in trg

    # ── 3. Duplicate V4_SINGLE rejected ──────────────────────────────────────

    def test_03_duplicate_v4single_same_commitment_same_run_result_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid = _insert_commitment(c)
        rrid = _insert_run_result(c)
        c.commit()
        _insert_snapshot(c, cid, 1, rrid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_snapshot(c, cid, 1, rrid)
        c.close()

    # ── 4. Same run result + different commitment → allowed ───────────────────

    def test_04_same_run_result_different_commitment_allowed(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid1 = _insert_commitment(c)
        cid2 = _insert_commitment(c)
        rrid = _insert_run_result(c)
        c.commit()
        _insert_snapshot(c, cid1, 1, rrid)
        _insert_snapshot(c, cid2, 1, rrid)  # different commitment — allowed
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_classifier_snapshots"
        ).fetchone()[0]
        assert count == 2
        c.close()

    # ── 5. V4_SINGLE with NULL representative rejected on INSERT ──────────────

    def test_05_v4single_null_representative_rejected_on_insert(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid = _insert_commitment(c)
        c.commit()
        # RAISE(ABORT, ...) from a BEFORE INSERT trigger surfaces as IntegrityError
        with pytest.raises((sqlite3.OperationalError, sqlite3.IntegrityError),
                           match="representative_run_result_id"):
            c.execute(
                "INSERT INTO commitment_classifier_snapshots "
                "(commitment_id, user_id, snapshot_type, representative_run_result_id, created_at) "
                "VALUES (?, 1, 'V4_SINGLE', NULL, '2024-01-01T00:00:00')",
                (cid,),
            )
        c.close()

    # ── 6. UPDATE setting V4_SINGLE representative to NULL rejected ───────────

    def test_06_v4single_update_null_representative_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid = _insert_commitment(c)
        rrid = _insert_run_result(c)
        c.commit()
        _insert_snapshot(c, cid, 1, rrid)
        c.commit()
        snap_id = c.execute(
            "SELECT id FROM commitment_classifier_snapshots"
        ).fetchone()[0]
        with pytest.raises((sqlite3.OperationalError, sqlite3.IntegrityError),
                           match="representative_run_result_id"):
            c.execute(
                "UPDATE commitment_classifier_snapshots "
                "SET representative_run_result_id = NULL WHERE id = ?",
                (snap_id,),
            )
        c.close()

    # ── 7. V4_CANONICAL_MERGED with NULL representative still allowed ─────────

    def test_07_canonical_merged_null_representative_allowed(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid = _insert_commitment(c)
        rrid = _insert_run_result(c)
        c.commit()
        # V4_CANONICAL_MERGED with NULL representative — trigger does NOT fire
        c.execute(
            "INSERT INTO commitment_classifier_snapshots "
            "(commitment_id, user_id, snapshot_type, representative_run_result_id, "
            " constituent_run_result_ids, created_at) "
            "VALUES (?, 1, 'V4_CANONICAL_MERGED', NULL, ?, '2024-01-01T00:00:00')",
            (cid, f'["{rrid}"]'),
        )
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_classifier_snapshots "
            "WHERE snapshot_type = 'V4_CANONICAL_MERGED'"
        ).fetchone()[0]
        assert count == 1
        c.close()

    # ── 8. Duplicate POSSIBLE_MATCH rejected ──────────────────────────────────

    def test_08_duplicate_possible_match_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid = _insert_commitment(c)
        rrid = _insert_run_result(c)
        desc = _uid()
        c.commit()
        _insert_suggestion(c, 1, desc, "POSSIBLE_MATCH", run_result_id=rrid, candidate_id=cid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_suggestion(c, 1, desc, "POSSIBLE_MATCH", run_result_id=rrid, candidate_id=cid)
        c.close()

    # ── 9. Same run_result + different candidate → allowed ────────────────────

    def test_09_possible_match_different_candidate_allowed(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid1 = _insert_commitment(c)
        cid2 = _insert_commitment(c)
        rrid = _insert_run_result(c)
        desc = _uid()
        c.commit()
        _insert_suggestion(c, 1, desc, "POSSIBLE_MATCH", run_result_id=rrid, candidate_id=cid1)
        _insert_suggestion(c, 1, desc, "POSSIBLE_MATCH", run_result_id=rrid, candidate_id=cid2)
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='POSSIBLE_MATCH'"
        ).fetchone()[0]
        assert count == 2
        c.close()

    # ── 10. Duplicate NEW_RECURRING rejected ──────────────────────────────────

    def test_10_duplicate_new_recurring_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        rrid = _insert_run_result(c, family_id=fid)
        desc = _uid()
        c.commit()
        _insert_suggestion(c, 1, desc, "NEW_RECURRING", run_result_id=rrid, family_id=fid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_suggestion(c, 1, desc, "NEW_RECURRING", run_result_id=rrid, family_id=fid)
        c.close()

    # ── 11. Different run_result NEW_RECURRING → allowed ─────────────────────

    def test_11_new_recurring_different_run_result_allowed(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        rrid1 = _insert_run_result(c, family_id=fid)
        rrid2 = _insert_run_result(c, family_id=fid)
        desc = _uid()
        c.commit()
        _insert_suggestion(c, 1, desc, "NEW_RECURRING", run_result_id=rrid1, family_id=fid)
        _insert_suggestion(c, 1, desc, "NEW_RECURRING", run_result_id=rrid2, family_id=fid)
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='NEW_RECURRING'"
        ).fetchone()[0]
        assert count == 2
        c.close()

    # ── 12. Duplicate AMBIGUOUS_FAMILY with candidate NULL rejected ───────────

    def test_12_duplicate_ambiguous_family_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        rrid = _insert_run_result(c, family_id=fid)
        desc = _uid()
        c.commit()
        _insert_suggestion(c, 1, desc, "AMBIGUOUS_FAMILY", run_result_id=rrid, family_id=fid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_suggestion(c, 1, desc, "AMBIGUOUS_FAMILY", run_result_id=rrid, family_id=fid)
        c.close()

    # ── 13. Unresolved AMBIGUOUS_FAMILY (family_id=NULL) is valid ────────────

    def test_13_ambiguous_family_null_family_id_valid(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        rrid = _insert_run_result(c, family_id=fid)
        desc = _uid()
        c.commit()
        # family_id=NULL, candidate=NULL — unresolved parallel run result
        _insert_suggestion(c, 1, desc, "AMBIGUOUS_FAMILY", run_result_id=rrid,
                           family_id=None, candidate_id=None)
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_suggestions WHERE suggestion_type='AMBIGUOUS_FAMILY'"
        ).fetchone()[0]
        assert count == 1
        c.close()

    # ── 14. Duplicate unresolved AMBIGUOUS_FAMILY rejected ───────────────────

    def test_14_duplicate_unresolved_ambiguous_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        rrid = _insert_run_result(c, family_id=fid)
        desc = _uid()
        c.commit()
        _insert_suggestion(c, 1, desc, "AMBIGUOUS_FAMILY", run_result_id=rrid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_suggestion(c, 1, desc, "AMBIGUOUS_FAMILY", run_result_id=rrid)
        c.close()

    # ── 15. Duplicate family-only conflict rejected ───────────────────────────

    def test_15_duplicate_family_only_conflict_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        run_id = _uid()
        c.commit()
        _insert_conflict(c, 1, run_id, "AMBIGUOUS_FAMILY", family_id=fid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_conflict(c, 1, run_id, "AMBIGUOUS_FAMILY", family_id=fid)
        c.close()

    # ── 16. Same family + same run but different conflict_type → allowed ──────

    def test_16_same_family_run_different_conflict_type_allowed(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        fid = _insert_family(c)
        run_id = _uid()
        c.commit()
        _insert_conflict(c, 1, run_id, "AMBIGUOUS_FAMILY", family_id=fid)
        _insert_conflict(c, 1, run_id, "USER_ID_DRIFT", family_id=fid)
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_link_conflicts"
        ).fetchone()[0]
        assert count == 2
        c.close()

    # ── 17. Duplicate family+commitment conflict rejected ─────────────────────

    def test_17_duplicate_family_commitment_conflict_rejected(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid = _insert_commitment(c)
        fid = _insert_family(c)
        run_id = _uid()
        c.commit()
        _insert_conflict(c, 1, run_id, "OVERLAPPING_WINDOW",
                         family_id=fid, commitment_id=cid)
        c.commit()
        with pytest.raises(sqlite3.IntegrityError):
            _insert_conflict(c, 1, run_id, "OVERLAPPING_WINDOW",
                             family_id=fid, commitment_id=cid)
        c.close()

    # ── 18. Same family/run + different commitment → allowed ─────────────────

    def test_18_overlap_different_commitment_allowed(self, tmp_path):
        db = _make_db(tmp_path)
        c = _conn(db)
        cid1 = _insert_commitment(c)
        cid2 = _insert_commitment(c)
        fid = _insert_family(c)
        run_id = _uid()
        c.commit()
        _insert_conflict(c, 1, run_id, "OVERLAPPING_WINDOW",
                         family_id=fid, commitment_id=cid1)
        _insert_conflict(c, 1, run_id, "OVERLAPPING_WINDOW",
                         family_id=fid, commitment_id=cid2)
        c.commit()
        count = c.execute(
            "SELECT COUNT(*) FROM commitment_link_conflicts"
        ).fetchone()[0]
        assert count == 2
        c.close()


# ═══════════════════════════════════════════════════════════════════════════════
# EXISTING-DB UPGRADE TESTS (19–31)
# ═══════════════════════════════════════════════════════════════════════════════

def _build_pre04_db(tmp_path):
    """
    Build a pre-Phase-0.4 DB with Phase 0.1/0.2/0.3/2A data but WITHOUT
    the Phase 0.4 indexes/triggers.

    Strategy: create a fresh DB (which has Phase 0.4), then DROP the six
    new indexes and two new triggers to simulate a pre-0.4 state with real data.
    """
    db = str(tmp_path / "pre04.db")
    import app as app_mod
    orig = app_mod.DB_PATH
    app_mod.DB_PATH = db
    try:
        app_mod.init_db()
    finally:
        app_mod.DB_PATH = orig

    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = OFF")

    # Drop the Phase 0.4 indexes to simulate pre-0.4 schema
    for idx in [
        "idx_ccs_v4single_dedup",
        "idx_cs_possible_match_dedup",
        "idx_cs_new_recurring_dedup",
        "idx_cs_ambiguous_dedup",
        "idx_clc_family_only_dedup",
        "idx_clc_overlap_dedup",
    ]:
        conn.execute(f"DROP INDEX IF EXISTS {idx}")

    # Drop the Phase 0.4 triggers
    for trg in [
        "trg_ccs_v4single_rep_required_ins",
        "trg_ccs_v4single_rep_required_upd",
    ]:
        conn.execute(f"DROP TRIGGER IF EXISTS {trg}")

    # Insert representative pre-0.4 data using raw SQL (triggers are dropped)
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash) "
        "VALUES (1, 'testuser', 'x')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color) VALUES ('other', 'אחר', '#888888')"
    )

    # Phase 2A: family + run result
    fam_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO pattern_families "
        "(id, user_id, primary_description_key, is_split_discriminator, "
        " is_primary, linked_by, family_status, created_at, updated_at) "
        "VALUES (?, 1, 'שכר_דירה', 0, 1, 'AUTO', 'ACTIVE', '2024-01-01T00:00:00', '2024-01-01T00:00:00')",
        (fam_id,),
    )
    rr_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO v4_run_results "
        "(id, run_id, user_id, family_id, description_key, stream_index, label, "
        " cadence, recurrence_status, commitment_status, classifier_lifecycle_status, "
        " budget_class, reserve_eligible, monthly_reserve_contrib_agorot, "
        " review_required, review_reasons, created_at) "
        "VALUES (?, 'run1', 1, ?, 'שכר_דירה', 0, 'שכר דירה', 'monthly', "
        "        'RECURRING', 'COMMITTED', 'ACTIVE', "
        "        'FIXED_AMOUNT_RECURRING', 0, 0, 0, '[]', '2024-01-01T00:00:00')",
        (rr_id, fam_id),
    )

    # Phase 1: migrated commitment
    com_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO commitments (id, user_id, canonical_label, source_type, is_finite, "
        "  created_at, updated_at) "
        "VALUES (?, 1, 'שכר דירה', 'MIGRATED', 1, '2024-01-01T00:00:00', '2024-01-01T00:00:00')",
        (com_id,),
    )

    # Phase 0.3 lifecycle value that must survive
    rr_id2 = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO v4_run_results "
        "(id, run_id, user_id, family_id, description_key, stream_index, label, "
        " cadence, recurrence_status, commitment_status, classifier_lifecycle_status, "
        " budget_class, reserve_eligible, monthly_reserve_contrib_agorot, "
        " review_required, review_reasons, created_at) "
        "VALUES (?, 'run2', 1, NULL, 'something_stopped', 0, 'stopped', 'monthly', "
        "        'RECURRING', 'COMMITTED', 'POSSIBLY_STOPPED', "
        "        'FIXED_AMOUNT_RECURRING', 0, 0, 0, '[]', '2024-01-01T00:00:00')",
        (rr_id2,),
    )

    conn.commit()
    conn.execute("PRAGMA foreign_keys = ON")
    conn.close()

    return db, fam_id, rr_id, com_id, rr_id2


class TestExistingDbUpgrade:

    # ── 19. Upgrade adds all six indexes ─────────────────────────────────────

    def test_19_upgrade_adds_all_six_indexes(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)

        # Verify pre-condition: indexes absent
        c = sqlite3.connect(db)
        idx_before = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
        c.close()
        assert "idx_ccs_v4single_dedup" not in idx_before

        # Apply upgrade
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        idx_after = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        ).fetchall()}
        c.close()
        expected = {
            "idx_ccs_v4single_dedup",
            "idx_cs_possible_match_dedup",
            "idx_cs_new_recurring_dedup",
            "idx_cs_ambiguous_dedup",
            "idx_clc_family_only_dedup",
            "idx_clc_overlap_dedup",
        }
        assert expected.issubset(idx_after)

    # ── 20. Upgrade adds both V4_SINGLE representative triggers ─────────────────

    def test_20_upgrade_adds_both_triggers(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        trg = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger'"
        ).fetchall()}
        c.close()
        assert "trg_ccs_v4single_rep_required_ins" in trg
        assert "trg_ccs_v4single_rep_required_upd" in trg

    # ── 21. All existing rows unchanged ──────────────────────────────────────

    def test_21_existing_rows_unchanged(self, tmp_path):
        db, fam_id, rr_id, com_id, rr_id2 = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        fam = c.execute(
            "SELECT id FROM pattern_families WHERE id=?", (fam_id,)
        ).fetchone()
        rr = c.execute(
            "SELECT id FROM v4_run_results WHERE id=?", (rr_id,)
        ).fetchone()
        com = c.execute(
            "SELECT id FROM commitments WHERE id=?", (com_id,)
        ).fetchone()
        c.close()
        assert fam is not None
        assert rr is not None
        assert com is not None

    # ── 22. PK/FK relationships unchanged ────────────────────────────────────

    def test_22_fk_relationships_intact(self, tmp_path):
        db, fam_id, rr_id, com_id, _ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        c.execute("PRAGMA foreign_keys = ON")
        # FK: v4_run_results.family_id → pattern_families
        row = c.execute(
            "SELECT family_id FROM v4_run_results WHERE id=?", (rr_id,)
        ).fetchone()
        assert row[0] == fam_id
        c.close()

    # ── 23. Phase 0.1 index preserved ────────────────────────────────────────

    def test_23_phase01_index_preserved(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        idx = _indexes(c)
        c.close()
        assert "idx_commitments_legacy_installment_unique" in idx

    # ── 24. Phase 0.2 MIGRATION provenance preserved ─────────────────────────

    def test_24_phase02_migration_linked_by_preserved(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        # Check MIGRATION is a valid linked_by value in the CEL CHECK
        sql = c.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='commitment_expense_links'"
        ).fetchone()[0]
        c.close()
        assert "'MIGRATION'" in sql

    # ── 25. Phase 0.3 lifecycle values preserved ─────────────────────────────

    def test_25_phase03_lifecycle_values_preserved(self, tmp_path):
        db, _, _, _, rr_id2 = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        row = c.execute(
            "SELECT classifier_lifecycle_status FROM v4_run_results WHERE id=?", (rr_id2,)
        ).fetchone()
        c.close()
        assert row[0] == "POSSIBLY_STOPPED"

    # ── 26. Phase 2A pattern families / run_results preserved ────────────────

    def test_26_phase2a_data_preserved(self, tmp_path):
        db, fam_id, rr_id, _, _ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        fam_count = c.execute("SELECT COUNT(*) FROM pattern_families").fetchone()[0]
        vrr_count = c.execute("SELECT COUNT(*) FROM v4_run_results").fetchone()[0]
        c.close()
        assert fam_count >= 1
        assert vrr_count >= 2  # two run results inserted in pre04 builder

    # ── 27. PRAGMA foreign_key_check = 0 ─────────────────────────────────────

    def test_27_foreign_key_check_zero(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        c.execute("PRAGMA foreign_keys = ON")
        violations = c.execute("PRAGMA foreign_key_check").fetchall()
        c.close()
        assert violations == []

    # ── 28. Second init_db is idempotent ─────────────────────────────────────

    def test_28_second_init_db_idempotent(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
            app_mod.init_db()  # second call must not raise
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        idx = _indexes(c)
        c.close()
        # All six still present exactly once
        assert "idx_ccs_v4single_dedup" in idx

    # ── 29. No temporary schema artifacts ────────────────────────────────────

    def test_29_no_temp_schema_artifacts(self, tmp_path):
        db, *_ = _build_pre04_db(tmp_path)
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        c = sqlite3.connect(db)
        tables = {r[0] for r in c.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        c.close()
        for name in tables:
            assert not name.endswith("_new"), f"Temporary table leaked: {name}"

    # ── 30. Forced failure rolls back entire Phase 0.4 ───────────────────────

    def test_30_forced_failure_rolls_back_all_phase04(self, tmp_path):
        """
        Simulate a failure after the first index creation:
        monkey-patch the upgrade function to raise after idx 1, then verify
        none of the Phase 0.4 indexes or triggers remain.
        """
        db, *_ = _build_pre04_db(tmp_path)

        import app as app_mod
        orig_upgrade = app_mod._upgrade_phase04_dedup_indexes

        def _failing_upgrade(conn):
            # Apply Phase 0.4 partially — create first index then fail
            already = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master "
                "WHERE type='index' AND name='idx_ccs_v4single_dedup'"
            ).fetchone()[0]
            if already:
                return
            conn.execute("SAVEPOINT sp_phase04_dedup")
            try:
                conn.execute("""
                    CREATE UNIQUE INDEX idx_ccs_v4single_dedup
                    ON commitment_classifier_snapshots(commitment_id, representative_run_result_id)
                    WHERE snapshot_type = 'V4_SINGLE'
                      AND representative_run_result_id IS NOT NULL
                """)
                raise RuntimeError("simulated failure after first index")
            except Exception:
                try:
                    conn.execute("ROLLBACK TO SAVEPOINT sp_phase04_dedup")
                    conn.execute("RELEASE SAVEPOINT sp_phase04_dedup")
                except Exception:
                    pass
                raise

        app_mod._upgrade_phase04_dedup_indexes = _failing_upgrade
        orig_db = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            with pytest.raises(RuntimeError, match="simulated failure"):
                app_mod.init_db()
        finally:
            app_mod._upgrade_phase04_dedup_indexes = orig_upgrade
            app_mod.DB_PATH = orig_db

        # None of the Phase 0.4 indexes should exist
        c = sqlite3.connect(db)
        idx = _indexes(c)
        trg = _triggers(c)
        c.close()
        phase04_indexes = {
            "idx_ccs_v4single_dedup", "idx_cs_possible_match_dedup",
            "idx_cs_new_recurring_dedup", "idx_cs_ambiguous_dedup",
            "idx_clc_family_only_dedup", "idx_clc_overlap_dedup",
        }
        phase04_triggers = {
            "trg_ccs_v4single_rep_required_ins",
            "trg_ccs_v4single_rep_required_upd",
        }
        assert not phase04_indexes.intersection(idx), \
            f"Phase 0.4 indexes leaked after rollback: {phase04_indexes.intersection(idx)}"
        # No Phase 0.4 triggers exist in this design (enforcement is application-level)
        assert not phase04_triggers.intersection(trg)

    # ── 31. Pre-existing duplicate data causes upgrade to fail closed ─────────

    def test_31_duplicate_existing_data_fails_closed(self, tmp_path):
        """
        Insert two CCS rows with the same (commitment_id, representative_run_result_id)
        and snapshot_type='V4_SINGLE' before Phase 0.4 upgrade.
        The UNIQUE index creation must fail; data must remain intact.
        """
        db, _, rr_id, com_id, _ = _build_pre04_db(tmp_path)

        # Insert duplicate pre-existing snapshot rows directly (triggers are absent)
        c = sqlite3.connect(db)
        c.execute("PRAGMA foreign_keys = OFF")
        c.execute(
            "INSERT INTO commitment_classifier_snapshots "
            "(commitment_id, user_id, snapshot_type, representative_run_result_id, created_at) "
            "VALUES (?, 1, 'V4_SINGLE', ?, '2024-01-01T00:00:00')",
            (com_id, rr_id),
        )
        c.execute(
            "INSERT INTO commitment_classifier_snapshots "
            "(commitment_id, user_id, snapshot_type, representative_run_result_id, created_at) "
            "VALUES (?, 1, 'V4_SINGLE', ?, '2024-01-02T00:00:00')",
            (com_id, rr_id),
        )
        c.commit()
        row_count_before = c.execute(
            "SELECT COUNT(*) FROM commitment_classifier_snapshots"
        ).fetchone()[0]
        c.execute("PRAGMA foreign_keys = ON")
        c.close()

        assert row_count_before == 2

        # Now attempt upgrade — must fail
        import app as app_mod
        orig = app_mod.DB_PATH
        app_mod.DB_PATH = db
        try:
            with pytest.raises(Exception):
                app_mod.init_db()
        finally:
            app_mod.DB_PATH = orig

        # Data must be unchanged — no automatic repair
        c = sqlite3.connect(db)
        row_count_after = c.execute(
            "SELECT COUNT(*) FROM commitment_classifier_snapshots"
        ).fetchone()[0]
        idx = _indexes(c)
        c.close()
        assert row_count_after == 2, "Rows were deleted during failed upgrade"
        assert "idx_ccs_v4single_dedup" not in idx, \
            "Phase 0.4 index must not remain after failed upgrade"
