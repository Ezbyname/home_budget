"""
Phase 0 schema foundation tests for Unified Commitments.

All tests use isolated temporary SQLite databases — the production DB is never touched.
Tests call init_db() via a redirected HOME so DB_PATH resolves to a temp dir.
"""

import os
import sys
import shutil
import sqlite3
import tempfile
import pytest

# ── Production path guard ──────────────────────────────────────────────────────
_PROTECTED_PATHS = [
    os.path.normcase(os.path.normpath(os.path.expanduser('~/.budget_tracker_data/budget.db'))),
]

def _hard_guard(path: str) -> None:
    norm = os.path.normcase(os.path.normpath(path))
    for p in _PROTECTED_PATHS:
        if norm == p or norm.startswith(p.rstrip('budget.db')):
            pytest.fail(f"HARD STOP: path matches production DB: {path!r}")


# ── Session-scoped HOME redirect + app import ──────────────────────────────────

@pytest.fixture(scope='session')
def _session_home():
    d = tempfile.mkdtemp(prefix='phase0_home_')
    orig_home = os.environ.get('HOME')
    orig_up = os.environ.get('USERPROFILE')
    os.environ['HOME'] = d
    os.environ['USERPROFILE'] = d
    yield d
    if orig_home is None:
        os.environ.pop('HOME', None)
    else:
        os.environ['HOME'] = orig_home
    if orig_up is None:
        os.environ.pop('USERPROFILE', None)
    else:
        os.environ['USERPROFILE'] = orig_up
    shutil.rmtree(d, ignore_errors=True)


@pytest.fixture(scope='session')
def app_module(_session_home):
    for mod_name in list(sys.modules):
        if mod_name == 'app' or mod_name.startswith('app.'):
            del sys.modules[mod_name]
    import app as _app
    _hard_guard(_app.DB_PATH)
    assert _session_home in _app.DB_PATH
    return _app


# ── Per-test isolated DB helper ────────────────────────────────────────────────

@pytest.fixture
def fresh_db(app_module, tmp_path):
    """Return (db_path, app_module) with a fresh isolated DB after init_db()."""
    db_path = str(tmp_path / 'test.db')
    orig = app_module.DB_PATH
    app_module.DB_PATH = db_path
    app_module.init_db()
    yield db_path, app_module
    app_module.DB_PATH = orig


def open_conn(db_path: str, foreign_keys: bool = True) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    if foreign_keys:
        conn.execute("PRAGMA foreign_keys = ON")
    return conn


# ── Expected tables / indexes / triggers ──────────────────────────────────────

UC_TABLES = [
    'commitments',
    'pattern_families',
    'description_key_aliases',
    'v4_run_results',
    'commitment_classifier_snapshots',
    'commitment_authority',
    'commitment_installment_meta',
    'commitment_occurrences',
    'commitment_expense_links',
    'commitment_suggestions',
    'commitment_link_conflicts',
    'commitment_link_events',
]

UC_INDEXES = [
    'idx_commitments_user',
    'idx_pf_one_primary',
    'idx_pf_ongoing',
    'idx_pf_split_amount',
    'idx_pf_split_window',
    'idx_pf_split_full',
    'idx_pf_commitment',
    'idx_pf_active',
    'idx_vrr_family',
    'idx_vrr_run',
    'idx_ccs_commitment',
    'idx_ca_resolve',
    'idx_co_lookup',
    'idx_co_finite',
    'idx_co_indefinite',
    'idx_co_linked_expense',
    'idx_cel_member_exclusive',
]

UC_TRIGGERS = [
    'trg_co_expense_owner_ins',
    'trg_co_expense_owner_upd',
    'trg_cel_expense_owner_ins',
    'trg_cel_expense_owner_upd',
    'trg_cs_expense_owner_ins',
]


# ========== A. DDL ==========================================================

def test_init_db_accepts_fresh_db(fresh_db):
    """Fresh SQLite DB accepts all final DDL via init_db()."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    for tbl in UC_TABLES:
        assert tbl in tables, f"Missing table: {tbl}"


def test_init_db_idempotent(fresh_db, app_module, tmp_path):
    """Calling init_db() twice produces no error."""
    db_path, mod = fresh_db
    mod.init_db()  # second call — must not raise


def test_all_uc_tables_present(fresh_db):
    """All 12 Unified Commitments tables present in sqlite_master."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    conn.close()
    assert len(UC_TABLES) == 12
    for tbl in UC_TABLES:
        assert tbl in tables, f"Missing table: {tbl}"


def test_all_uc_indexes_present(fresh_db):
    """All named indexes present in sqlite_master."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    indexes = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
    conn.close()
    for idx in UC_INDEXES:
        assert idx in indexes, f"Missing index: {idx}"


def test_all_uc_triggers_present(fresh_db):
    """All 5 triggers present in sqlite_master."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    triggers = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='trigger'").fetchall()}
    conn.close()
    assert len(UC_TRIGGERS) == 5
    for trg in UC_TRIGGERS:
        assert trg in triggers, f"Missing trigger: {trg}"


# ========== B. Money types ==================================================

def test_agorot_fields_are_integer(fresh_db):
    """Every *_agorot field in every UC table is INTEGER."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    agorot_tables = {
        'v4_run_results': ['planning_amount_agorot', 'monthly_reserve_contrib_agorot'],
        'commitment_classifier_snapshots': ['monthly_reserve_contrib_agorot'],
        'commitment_installment_meta': ['payment_agorot', 'total_purchase_agorot'],
        'commitment_occurrences': ['expected_agorot'],
    }
    for tbl, cols in agorot_tables.items():
        info = {r[1]: r[2].upper() for r in conn.execute(f"PRAGMA table_info({tbl})").fetchall()}
        for col in cols:
            assert 'INTEGER' in info.get(col, ''), f"{tbl}.{col} is not INTEGER, got: {info.get(col)}"
    conn.close()


def test_cadence_coverage_is_real(fresh_db):
    """cadence_coverage REAL is explicitly permitted."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    info = {r[1]: r[2].upper() for r in conn.execute("PRAGMA table_info(v4_run_results)").fetchall()}
    assert 'REAL' in info.get('cadence_coverage', ''), f"cadence_coverage type: {info.get('cadence_coverage')}"
    conn.close()


# ========== C. FK / ownership ===============================================

def test_foreign_keys_pragma_on(fresh_db):
    """PRAGMA foreign_keys = ON when test connection is opened."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    val = conn.execute("PRAGMA foreign_keys").fetchone()[0]
    conn.close()
    assert val == 1


def test_foreign_key_check_clean(fresh_db):
    """PRAGMA foreign_key_check returns 0 rows on fresh DB."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    rows = conn.execute("PRAGMA foreign_key_check").fetchall()
    conn.close()
    assert rows == [], f"FK violations: {rows}"


def _insert_user(conn, uid=1, username='user1'):
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash) VALUES (?,?,?)",
        (uid, username, 'x')
    )


def _insert_commitment(conn, cid, uid):
    conn.execute(
        """INSERT INTO commitments (id, user_id, created_at, updated_at)
           VALUES (?, ?, '2024-01-01', '2024-01-01')""",
        (cid, uid)
    )


def _insert_pattern_family(conn, fid, uid, desc_key='pay_electricity', commitment_id=None):
    conn.execute(
        """INSERT INTO pattern_families
           (id, user_id, primary_description_key, created_at, updated_at)
           VALUES (?, ?, ?, '2024-01-01', '2024-01-01')""",
        (fid, uid, desc_key)
    )


def test_cross_user_commitment_child_rejected(fresh_db):
    """Cross-user commitment child reference rejected."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_user(conn, uid=2, username='u2')
    _insert_commitment(conn, 'c1', 1)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        # commitment belongs to user 1, but child says user 2
        conn.execute(
            """INSERT INTO commitment_classifier_snapshots
               (commitment_id, user_id, snapshot_type, constituent_run_result_ids, created_at)
               VALUES ('c1', 2, 'V4_SINGLE', '[]', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_cross_user_family_child_rejected(fresh_db):
    """Cross-user family child reference rejected."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_user(conn, uid=2, username='u2')
    _insert_pattern_family(conn, 'f1', 1)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO description_key_aliases
               (family_id, user_id, description_key, first_seen_at)
               VALUES ('f1', 2, 'alias_key', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def _insert_category(conn):
    conn.execute("INSERT OR IGNORE INTO categories (id, name_he, color) VALUES ('misc','מגוון','#888')")


def _insert_expense(conn, eid, uid):
    conn.execute(
        """INSERT INTO expenses (id, date, category_id, amount, user_id)
           VALUES (?, '2024-01-01', 'misc', 100, ?)""",
        (eid, uid)
    )


def test_cross_user_expense_trigger_co(fresh_db):
    """Cross-user expense trigger fires for commitment_occurrences.linked_expense_id."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_user(conn, uid=2, username='u2')
    _insert_category(conn)
    _insert_expense(conn, 10, 1)  # expense owned by user 1
    _insert_commitment(conn, 'c1', 2)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_occurrences
               (commitment_id, user_id, occurrence_date, expected_agorot, linked_expense_id, generated_at)
               VALUES ('c1', 2, '2024-01-01', 1000, 10, '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_cross_user_expense_trigger_cel(fresh_db):
    """Cross-user expense trigger fires for commitment_expense_links.expense_id."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_user(conn, uid=2, username='u2')
    _insert_category(conn)
    _insert_expense(conn, 20, 1)  # expense owned by user 1
    _insert_commitment(conn, 'c2', 2)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_expense_links
               (commitment_id, user_id, expense_id, created_at)
               VALUES ('c2', 2, 20, '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_cross_user_run_result_composite_fk(fresh_db):
    """Cross-user run_result composite FK fires."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_user(conn, uid=2, username='u2')
    _insert_pattern_family(conn, 'f1', 1)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        # family belongs to user 1 but run_result says user 2
        conn.execute(
            """INSERT INTO v4_run_results
               (id, run_id, user_id, family_id, description_key, created_at)
               VALUES ('rr1', 'run1', 2, 'f1', 'desc', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


# ========== D. Pattern families =============================================

def test_duplicate_ongoing_family_rejected(fresh_db):
    """Duplicate ongoing family rejected (idx_pf_ongoing)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_pattern_family(conn, 'f1', 1, 'electric')
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        _insert_pattern_family(conn, 'f2', 1, 'electric')  # same user+desc_key, is_split_discriminator=0
        conn.commit()
    conn.close()


def test_split_families_distinct_windows_accepted(fresh_db):
    """Split families with distinct windows accepted."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    conn.execute(
        """INSERT INTO pattern_families
           (id, user_id, primary_description_key, is_split_discriminator, window_start, window_end, created_at, updated_at)
           VALUES ('f1', 1, 'rent', 1, '2023-01-01', '2023-06-30', '2024-01-01', '2024-01-01')"""
    )
    conn.execute(
        """INSERT INTO pattern_families
           (id, user_id, primary_description_key, is_split_discriminator, window_start, window_end, created_at, updated_at)
           VALUES ('f2', 1, 'rent', 1, '2023-07-01', '2023-12-31', '2024-01-01', '2024-01-01')"""
    )
    conn.commit()
    conn.close()


def test_half_open_window_rejected(fresh_db):
    """Half-open window rejected (window_start without window_end)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO pattern_families
               (id, user_id, primary_description_key, is_split_discriminator, window_start, window_end, created_at, updated_at)
               VALUES ('f1', 1, 'rent', 1, '2023-01-01', NULL, '2024-01-01', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_split_without_discriminator_rejected(fresh_db):
    """Non-split family with amount_cluster_agorot rejected."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO pattern_families
               (id, user_id, primary_description_key, is_split_discriminator, amount_cluster_agorot, created_at, updated_at)
               VALUES ('f1', 1, 'rent', 0, 50000, '2024-01-01', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_active_family_survives_amount_drift(fresh_db):
    """Active family survives amount drift — new run results can be inserted."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_pattern_family(conn, 'f1', 1, 'electric')
    conn.commit()
    # Adding new run result with different amounts is allowed
    conn.execute(
        """INSERT INTO v4_run_results
           (id, run_id, user_id, family_id, description_key, planning_amount_agorot, created_at)
           VALUES ('rr1', 'run1', 1, 'f1', 'electric', 12000, '2024-01-01')"""
    )
    conn.execute(
        """INSERT INTO v4_run_results
           (id, run_id, user_id, family_id, description_key, planning_amount_agorot, created_at)
           VALUES ('rr2', 'run2', 1, 'f1', 'electric', 15000, '2024-02-01')"""
    )
    conn.commit()
    conn.close()


def test_superseded_family_allows_new_active_primary(fresh_db):
    """New active primary accepted after old family is SUPERSEDED."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    # Insert active family and link a commitment
    _insert_commitment(conn, 'c1', 1)
    conn.execute(
        """INSERT INTO pattern_families
           (id, user_id, primary_description_key, is_primary, family_status, commitment_id, created_at, updated_at)
           VALUES ('f1', 1, 'electric', 1, 'ACTIVE', 'c1', '2024-01-01', '2024-01-01')"""
    )
    conn.commit()
    # Supersede it
    conn.execute(
        "UPDATE pattern_families SET family_status='SUPERSEDED', superseded_at='2024-06-01' WHERE id='f1'"
    )
    conn.commit()
    # Now a new primary can be inserted for same commitment
    conn.execute(
        """INSERT INTO pattern_families
           (id, user_id, primary_description_key, is_primary, family_status, commitment_id, created_at, updated_at)
           VALUES ('f2', 1, 'electric2', 1, 'ACTIVE', 'c1', '2024-06-01', '2024-06-01')"""
    )
    conn.commit()
    conn.close()


def test_two_active_primaries_for_same_commitment_rejected(fresh_db):
    """Two active primaries for same commitment rejected (idx_pf_one_primary)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_commitment(conn, 'c1', 1)
    conn.execute(
        """INSERT INTO pattern_families
           (id, user_id, primary_description_key, is_primary, family_status, commitment_id, created_at, updated_at)
           VALUES ('f1', 1, 'electric', 1, 'ACTIVE', 'c1', '2024-01-01', '2024-01-01')"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO pattern_families
               (id, user_id, primary_description_key, is_primary, family_status, commitment_id, created_at, updated_at)
               VALUES ('f2', 1, 'electric2', 1, 'ACTIVE', 'c1', '2024-01-01', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


# ========== E. Authority ====================================================

def test_import_signal_authority_source_rejected(fresh_db):
    """IMPORT_SIGNAL authority_source rejected by CHECK constraint."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_commitment(conn, 'c1', 1)
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_authority
               (commitment_id, user_id, field_name, authority_source, override_id, created_at, created_by)
               VALUES ('c1', 1, 'cashflow_role', 'IMPORT_SIGNAL', 'oid1', '2024-01-01', 1)"""
        )
        conn.commit()
    conn.close()


def test_duplicate_override_id_rejected(fresh_db):
    """Duplicate override_id rejected."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_commitment(conn, 'c1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_authority
           (commitment_id, user_id, field_name, authority_source, override_id, created_at, created_by)
           VALUES ('c1', 1, 'cashflow_role', 'MANUAL_OVERRIDE', 'oid_dup', '2024-01-01', 1)"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_authority
               (commitment_id, user_id, field_name, authority_source, override_id, created_at, created_by)
               VALUES ('c1', 1, 'payment_mechanism', 'MANUAL_OVERRIDE', 'oid_dup', '2024-01-01', 1)"""
        )
        conn.commit()
    conn.close()


def test_default_cashflow_role_is_unclassified(fresh_db):
    """Default cashflow_role = 'UNCLASSIFIED'."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    conn.execute(
        """INSERT INTO commitments (id, user_id, created_at, updated_at)
           VALUES ('c1', 1, '2024-01-01', '2024-01-01')"""
    )
    conn.commit()
    row = conn.execute("SELECT cashflow_role FROM commitments WHERE id='c1'").fetchone()
    conn.close()
    assert row['cashflow_role'] == 'UNCLASSIFIED'


def test_default_commitment_kind_is_unknown(fresh_db):
    """Default commitment_kind = 'UNKNOWN'."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    conn.execute(
        """INSERT INTO commitments (id, user_id, created_at, updated_at)
           VALUES ('c1', 1, '2024-01-01', '2024-01-01')"""
    )
    conn.commit()
    row = conn.execute("SELECT commitment_kind FROM commitments WHERE id='c1'").fetchone()
    conn.close()
    assert row['commitment_kind'] == 'UNKNOWN'


# ========== F. Expenses / occurrences =======================================

def test_member_double_funding_rejected(fresh_db):
    """MEMBER double-funding rejected (idx_cel_member_exclusive)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_category(conn)
    _insert_expense(conn, 100, 1)
    _insert_commitment(conn, 'c1', 1)
    _insert_commitment(conn, 'c2', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_expense_links
           (commitment_id, user_id, expense_id, membership_type, created_at)
           VALUES ('c1', 1, 100, 'MEMBER', '2024-01-01')"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_expense_links
               (commitment_id, user_id, expense_id, membership_type, created_at)
               VALUES ('c2', 1, 100, 'MEMBER', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_excluded_allows_same_expense_in_second_link(fresh_db):
    """EXCLUDED membership_type allows same expense_id in second link."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_category(conn)
    _insert_expense(conn, 100, 1)
    _insert_commitment(conn, 'c1', 1)
    _insert_commitment(conn, 'c2', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_expense_links
           (commitment_id, user_id, expense_id, membership_type, created_at)
           VALUES ('c1', 1, 100, 'MEMBER', '2024-01-01')"""
    )
    conn.execute(
        """INSERT INTO commitment_expense_links
           (commitment_id, user_id, expense_id, membership_type, created_at)
           VALUES ('c2', 1, 100, 'EXCLUDED', '2024-01-01')"""
    )
    conn.commit()
    conn.close()


def test_same_linked_expense_confirms_two_occurrences_rejected(fresh_db):
    """Same linked_expense_id confirming two occurrences rejected (idx_co_linked_expense)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_category(conn)
    _insert_expense(conn, 200, 1)
    _insert_commitment(conn, 'c1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_occurrences
           (commitment_id, user_id, occurrence_date, expected_agorot, linked_expense_id, generated_at)
           VALUES ('c1', 1, '2024-01-01', 1000, 200, '2024-01-01')"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_occurrences
               (commitment_id, user_id, occurrence_date, expected_agorot, linked_expense_id, generated_at)
               VALUES ('c1', 1, '2024-02-01', 1000, 200, '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_finite_occurrence_index_uniqueness(fresh_db):
    """Finite occurrence index uniqueness enforced (idx_co_finite)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_commitment(conn, 'c1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_occurrences
           (commitment_id, user_id, occurrence_date, occurrence_index, expected_agorot, generated_at)
           VALUES ('c1', 1, '2024-01-01', 1, 1000, '2024-01-01')"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_occurrences
               (commitment_id, user_id, occurrence_date, occurrence_index, expected_agorot, generated_at)
               VALUES ('c1', 1, '2024-02-01', 1, 1000, '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_indefinite_occurrence_date_uniqueness(fresh_db):
    """Indefinite occurrence date uniqueness enforced (idx_co_indefinite)."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_commitment(conn, 'c1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_occurrences
           (commitment_id, user_id, occurrence_date, occurrence_index, expected_agorot, generated_at)
           VALUES ('c1', 1, '2024-01-15', NULL, 1000, '2024-01-01')"""
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_occurrences
               (commitment_id, user_id, occurrence_date, occurrence_index, expected_agorot, generated_at)
               VALUES ('c1', 1, '2024-01-15', NULL, 1200, '2024-01-01')"""
        )
        conn.commit()
    conn.close()


# ========== G. Events / conflicts ===========================================

def test_family_created_null_commitment_id_succeeds(fresh_db):
    """FAMILY_CREATED with NULL commitment_id succeeds when family_id is valid."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_pattern_family(conn, 'f1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_link_events
           (commitment_id, user_id, family_id, event_type, detail, created_at)
           VALUES (NULL, 1, 'f1', 'FAMILY_CREATED', '{}', '2024-01-01')"""
    )
    conn.commit()
    conn.close()


def test_double_null_anchor_rejected(fresh_db):
    """Both commitment_id and family_id NULL rejected by CHECK."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            """INSERT INTO commitment_link_events
               (commitment_id, user_id, family_id, event_type, detail, created_at)
               VALUES (NULL, 1, NULL, 'FAMILY_CREATED', '{}', '2024-01-01')"""
        )
        conn.commit()
    conn.close()


def test_family_superseded_event_type_accepted(fresh_db):
    """FAMILY_SUPERSEDED event_type accepted."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_pattern_family(conn, 'f1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_link_events
           (commitment_id, user_id, family_id, event_type, detail, created_at)
           VALUES (NULL, 1, 'f1', 'FAMILY_SUPERSEDED', '{}', '2024-01-01')"""
    )
    conn.commit()
    conn.close()


def test_family_reactivated_event_type_accepted(fresh_db):
    """FAMILY_REACTIVATED event_type accepted."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=True)
    _insert_user(conn, uid=1, username='u1')
    _insert_pattern_family(conn, 'f1', 1)
    conn.commit()
    conn.execute(
        """INSERT INTO commitment_link_events
           (commitment_id, user_id, family_id, event_type, detail, created_at)
           VALUES (NULL, 1, 'f1', 'FAMILY_REACTIVATED', '{}', '2024-01-01')"""
    )
    conn.commit()
    conn.close()


# ========== H. Legacy safety ================================================

def test_expenses_table_unchanged(fresh_db):
    """PRAGMA table_info(expenses) unchanged by Phase 0."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(expenses)").fetchall()]
    conn.close()
    # Core legacy columns must all be present
    for col in ['id', 'date', 'category_id', 'amount', 'user_id']:
        assert col in cols, f"expenses missing column: {col}"
    # No unexpected UC columns added
    for col in cols:
        assert not col.startswith('commitment'), f"Unexpected commitment col in expenses: {col}"


def test_income_table_unchanged(fresh_db):
    """PRAGMA table_info(income) unchanged by Phase 0."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(income)").fetchall()]
    conn.close()
    for col in ['id', 'date', 'person', 'source', 'amount', 'user_id']:
        assert col in cols, f"income missing column: {col}"
    for col in cols:
        assert not col.startswith('commitment'), f"Unexpected commitment col in income: {col}"


def test_installments_table_unchanged(fresh_db):
    """PRAGMA table_info(installments) unchanged by Phase 0."""
    db_path, _ = fresh_db
    conn = open_conn(db_path, foreign_keys=False)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(installments)").fetchall()]
    conn.close()
    for col in ['id', 'user_id']:
        assert col in cols, f"installments missing column: {col}"
    for col in cols:
        assert not col.startswith('commitment'), f"Unexpected commitment col in installments: {col}"
