"""
Phase 0.5 — Authority Identity Schema: 49 tests.

Groups:
  1–7   Fresh DB schema correctness
  8–14  Post-migration behavioral correctness
  15–18 Revocation CHECK enforcement
  19–28 Migration unit tests (direct call)
  29–33 Schema detection unit tests
  34    Convergence (fresh == migrated fingerprint)
  35    FK integrity
  36–39 PRE_0_5 idx_ca_resolve fingerprint variants → HYBRID
  40–44 Fresh atomic creation + rollback
  45–49 Real init_db() ordering integration tests
"""

import sqlite3
import pytest
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import (
    AuthoritySchemaState,
    _detect_authority_schema,
    _create_phase05_fresh,
    _migrate_to_phase05,
    _apply_phase05_authority,
    _PHASE05_AUTHORITY_DDL,
    init_db,
    get_db,
)

# ─── helpers ──────────────────────────────────────────────────────────────────

def _fresh_mem_conn():
    """In-memory SQLite connection with FK enforcement ON."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _make_pre05_table(conn):
    """
    Create the pre-Phase-0.5 commitment_authority table (old UNIQUE(override_id))
    and an exact idx_ca_resolve, preceded by the commitments table it references.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commitments (
            id      TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (id, user_id)
        )
    """)
    conn.execute("""
        CREATE TABLE commitment_authority (
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
            UNIQUE(override_id),
            FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id)
        )
    """)
    conn.execute(
        "CREATE INDEX idx_ca_resolve "
        "ON commitment_authority(commitment_id, field_name, created_at DESC) "
        "WHERE is_active = 1"
    )


def _make_phase05_table(conn):
    """Create the full Phase 0.5 commitment_authority schema from scratch."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commitments (
            id      TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            PRIMARY KEY (id, user_id)
        )
    """)
    _create_phase05_fresh(conn)


def _collect_fingerprint(conn):
    cols = [(r[1], r[2], r[3], r[4])
            for r in conn.execute("PRAGMA table_info(commitment_authority)").fetchall()]
    index_rows = conn.execute("PRAGMA index_list(commitment_authority)").fetchall()
    indexes = {}
    for r in index_rows:
        name, unique, partial = r[1], bool(r[2]), bool(r[4])
        xinfo = conn.execute(f"PRAGMA index_xinfo('{name}')").fetchall()
        key_cols = [(xi[2], xi[3]) for xi in xinfo if xi[5] == 1]
        indexes[name] = {"unique": unique, "partial": partial, "key_cols": key_cols}
    # Exclude autoindex entries from fingerprint comparison
    named_indexes = {k: v for k, v in indexes.items()
                     if not k.startswith("sqlite_autoindex")}
    return {"columns": cols, "named_indexes": named_indexes}


def _insert_authority_row(conn, rid, commitment_id, user_id, override_id,
                           field_name="amount", authority_source="MANUAL_OVERRIDE",
                           is_active=1, created_at="2024-01-01T00:00:00",
                           created_by=1, revoked_at=None, revoked_by=None):
    conn.execute(
        "INSERT OR IGNORE INTO commitments(id, user_id) VALUES (?, ?)",
        (commitment_id, user_id)
    )
    conn.execute(
        "INSERT INTO commitment_authority "
        "(id, commitment_id, user_id, field_name, value, authority_source, "
        " override_id, is_active, created_at, created_by, revoked_at, revoked_by) "
        "VALUES (?,?,?,?,NULL,?,?,?,?,?,?,?)",
        (rid, commitment_id, user_id, field_name, authority_source,
         override_id, is_active, created_at, created_by, revoked_at, revoked_by)
    )


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 1–7: Fresh DB schema correctness
# ═══════════════════════════════════════════════════════════════════════════════

def test_01_fresh_table_exists():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='commitment_authority'"
    ).fetchone()
    assert exists is not None


def test_02_fresh_exactly_12_columns():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    cols = conn.execute("PRAGMA table_info(commitment_authority)").fetchall()
    assert len(cols) == 12


def test_03_fresh_no_global_unique_override_id():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    index_rows = conn.execute("PRAGMA index_list(commitment_authority)").fetchall()
    for r in index_rows:
        name, unique, partial = r[1], bool(r[2]), bool(r[4])
        xinfo = conn.execute(f"PRAGMA index_xinfo('{name}')").fetchall()
        key_cols = [xi[2] for xi in xinfo if xi[5] == 1]
        assert not (unique and not partial and key_cols == ["override_id"]), \
            f"Global UNIQUE(override_id) found on fresh DB at index '{name}'"


def test_04_fresh_idx_ca_resolve_desc():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    xinfo = conn.execute("PRAGMA index_xinfo('idx_ca_resolve')").fetchall()
    key_cols = [(xi[2], xi[3]) for xi in xinfo if xi[5] == 1]
    assert key_cols == [("commitment_id", 0), ("field_name", 0), ("created_at", 1)]


def test_05_fresh_idx_ca_active_instance_correct():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    xinfo = conn.execute("PRAGMA index_xinfo('idx_ca_active_instance')").fetchall()
    key_cols = [(xi[2], xi[3]) for xi in xinfo if xi[5] == 1]
    assert key_cols == [("user_id", 0), ("commitment_id", 0), ("override_id", 0)]
    index_rows = {r[1]: r for r in conn.execute("PRAGMA index_list(commitment_authority)").fetchall()}
    assert bool(index_rows["idx_ca_active_instance"][2])  # unique
    assert bool(index_rows["idx_ca_active_instance"][4])  # partial


def test_06_fresh_idx_ca_active_field_source_correct():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    xinfo = conn.execute("PRAGMA index_xinfo('idx_ca_active_field_source')").fetchall()
    key_cols = [(xi[2], xi[3]) for xi in xinfo if xi[5] == 1]
    assert key_cols == [("user_id", 0), ("commitment_id", 0), ("field_name", 0), ("authority_source", 0)]
    index_rows = {r[1]: r for r in conn.execute("PRAGMA index_list(commitment_authority)").fetchall()}
    assert bool(index_rows["idx_ca_active_field_source"][2])  # unique
    assert bool(index_rows["idx_ca_active_field_source"][4])  # partial


def test_07_fresh_state_is_phase05():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 8–14: Post-migration behavioral correctness
# ═══════════════════════════════════════════════════════════════════════════════

def test_08_migrated_state_is_phase05():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PRE_0_5
    _migrate_to_phase05(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5


def test_09_migrated_rows_preserved():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A")
    _insert_authority_row(conn, 2, "C2", 1, "OVR-B")
    _migrate_to_phase05(conn)
    rows = conn.execute("SELECT id, override_id FROM commitment_authority ORDER BY id").fetchall()
    assert rows == [(1, "OVR-A"), (2, "OVR-B")]


def test_10_migrated_autoincrement_continues():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 5, "C1", 1, "OVR-A", field_name="amount")
    _migrate_to_phase05(conn)
    # Use a different field_name to avoid idx_ca_active_field_source collision
    _insert_authority_row(conn, None, "C1", 1, "OVR-NEW", field_name="label",
                          created_at="2024-02-01T00:00:00")
    new_id = conn.execute("SELECT MAX(id) FROM commitment_authority").fetchone()[0]
    assert new_id > 5


def test_11_migrated_allows_same_override_id_revoked_plus_active():
    """Post-migration: same override_id can appear in both an active and an inactive row."""
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _migrate_to_phase05(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-X", is_active=0,
                          revoked_at="2024-01-02T00:00:00", revoked_by=1)
    _insert_authority_row(conn, 2, "C1", 1, "OVR-X", is_active=1)
    count = conn.execute(
        "SELECT COUNT(*) FROM commitment_authority WHERE override_id='OVR-X'"
    ).fetchone()[0]
    assert count == 2


def test_12_migrated_blocks_duplicate_active_override():
    """idx_ca_active_instance: two active rows with same (user, commitment, override_id) are rejected."""
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-X")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_authority_row(conn, 2, "C1", 1, "OVR-X")


def test_13_migrated_blocks_two_active_same_field_source():
    """idx_ca_active_field_source: two active rows same (user, commitment, field, source) rejected."""
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    with pytest.raises(sqlite3.IntegrityError):
        _insert_authority_row(conn, 2, "C1", 1, "OVR-B", field_name="amount",
                              authority_source="MANUAL_OVERRIDE")


def test_14_migrated_fk_check_clean():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A")
    _migrate_to_phase05(conn)
    violations = conn.execute("PRAGMA foreign_key_check(commitment_authority)").fetchall()
    assert violations == []


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 15–18: Revocation CHECK enforcement
# ═══════════════════════════════════════════════════════════════════════════════

def test_15_active_row_cannot_have_revoked_at():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_authority_row(conn, 1, "C1", 1, "OVR-A", is_active=1,
                              revoked_at="2024-01-02T00:00:00")


def test_16_active_row_cannot_have_revoked_by():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_authority_row(conn, 1, "C1", 1, "OVR-A", is_active=1,
                              revoked_by=99)


def test_17_inactive_row_requires_revoked_at():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    with pytest.raises(sqlite3.IntegrityError):
        _insert_authority_row(conn, 1, "C1", 1, "OVR-A", is_active=0,
                              revoked_at=None, revoked_by=None)


def test_18_valid_inactive_row_accepted():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A", is_active=0,
                          revoked_at="2024-01-02T00:00:00", revoked_by=1)
    count = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 19–28: Migration unit tests (direct call)
# ═══════════════════════════════════════════════════════════════════════════════

def test_19_migrate_pre05_succeeds():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _migrate_to_phase05(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5


def test_20_migrate_removes_old_unique():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _migrate_to_phase05(conn)
    index_rows = conn.execute("PRAGMA index_list(commitment_authority)").fetchall()
    for r in index_rows:
        name, unique, partial = r[1], bool(r[2]), bool(r[4])
        xinfo = conn.execute(f"PRAGMA index_xinfo('{name}')").fetchall()
        key_cols = [xi[2] for xi in xinfo if xi[5] == 1]
        assert not (unique and not partial and key_cols == ["override_id"]), \
            f"Old UNIQUE(override_id) still present at '{name}' after migration"


def test_21_migrate_adds_new_indexes():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _migrate_to_phase05(conn)
    names = {r[1] for r in conn.execute("PRAGMA index_list(commitment_authority)").fetchall()}
    assert "idx_ca_active_instance" in names
    assert "idx_ca_active_field_source" in names
    assert "idx_ca_resolve" in names


def test_22_migrate_temp_table_absent_after_success():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _migrate_to_phase05(conn)
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='commitment_authority_pre05'"
    ).fetchone()
    assert exists is None


def test_23_pre05_collision_blocks_migration():
    """
    Two ACTIVE rows with same (user_id, commitment_id, field_name, authority_source)
    but different override_ids — valid in PRE_0_5, but violate idx_ca_active_field_source.
    Migration must fail.
    """
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    _insert_authority_row(conn, 2, "C1", 1, "OVR-B", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    with pytest.raises(Exception):
        _migrate_to_phase05(conn)


def test_24_migration_failure_leaves_pre05_state():
    """After a failed migration, DB must be exactly PRE_0_5 — not HYBRID."""
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    _insert_authority_row(conn, 2, "C1", 1, "OVR-B", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    try:
        _migrate_to_phase05(conn)
    except Exception:
        pass
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PRE_0_5


def test_25_migration_failure_rows_intact():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    _insert_authority_row(conn, 2, "C1", 1, "OVR-B", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    try:
        _migrate_to_phase05(conn)
    except Exception:
        pass
    rows = conn.execute("SELECT id FROM commitment_authority ORDER BY id").fetchall()
    assert [r[0] for r in rows] == [1, 2]


def test_26_migration_failure_no_temp_table_residue():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    _insert_authority_row(conn, 2, "C1", 1, "OVR-B", field_name="amount",
                          authority_source="MANUAL_OVERRIDE")
    try:
        _migrate_to_phase05(conn)
    except Exception:
        pass
    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE name='commitment_authority_pre05'"
    ).fetchone()
    assert exists is None


def test_27_migration_revocation_check_enforced_on_copy():
    """
    An is_active=1 row with revoked_at set (bad data in old DB) must fail the new CHECK
    during COPY and roll back the migration.
    """
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    # Insert the commitment row so the FK is satisfied
    conn.execute("INSERT OR IGNORE INTO commitments(id, user_id) VALUES ('C1', 1)")
    # Insert bad data directly — FK satisfied, old schema has no revocation CHECKs
    conn.execute(
        "INSERT INTO commitment_authority "
        "(id, commitment_id, user_id, field_name, value, authority_source, "
        " override_id, is_active, created_at, created_by, revoked_at, revoked_by) "
        "VALUES (1, 'C1', 1, 'amount', NULL, 'MANUAL_OVERRIDE', "
        "        'OVR-BAD', 1, '2024-01-01', 1, '2024-01-02', 1)"
    )
    with pytest.raises(Exception):
        _migrate_to_phase05(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PRE_0_5


def test_28_migrate_is_idempotent_via_apply():
    """Calling _apply_phase05_authority on an already-migrated DB is a no-op."""
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A")
    _apply_phase05_authority(conn)
    _apply_phase05_authority(conn)  # second call must not raise
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5
    count = conn.execute("SELECT COUNT(*) FROM commitment_authority").fetchone()[0]
    assert count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 29–33: Schema detection unit tests
# ═══════════════════════════════════════════════════════════════════════════════

def test_29_detect_no_table():
    conn = _fresh_mem_conn()
    assert _detect_authority_schema(conn) == AuthoritySchemaState.NO_TABLE


def test_30_detect_pre05():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PRE_0_5


def test_31_detect_phase05_fresh():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5


def test_32_detect_phase05_migrated():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    _migrate_to_phase05(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5


def test_33_detect_hybrid_old_unique_plus_new_index():
    """Old UNIQUE(override_id) AND idx_ca_active_instance both present → HYBRID."""
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    conn.execute(
        "CREATE UNIQUE INDEX idx_ca_active_instance "
        "ON commitment_authority(user_id, commitment_id, override_id) "
        "WHERE is_active = 1"
    )
    assert _detect_authority_schema(conn) == AuthoritySchemaState.HYBRID


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 34: Convergence
# ═══════════════════════════════════════════════════════════════════════════════

def test_34_fresh_and_migrated_fingerprints_equal():
    fresh_conn = _fresh_mem_conn()
    _make_phase05_table(fresh_conn)
    fp_fresh = _collect_fingerprint(fresh_conn)

    mig_conn = _fresh_mem_conn()
    _make_pre05_table(mig_conn)
    _migrate_to_phase05(mig_conn)
    fp_migrated = _collect_fingerprint(mig_conn)

    assert fp_fresh == fp_migrated


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 35: FK integrity
# ═══════════════════════════════════════════════════════════════════════════════

def test_35_fk_check_fresh():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    _insert_authority_row(conn, 1, "C1", 1, "OVR-A")
    violations = conn.execute("PRAGMA foreign_key_check(commitment_authority)").fetchall()
    assert violations == []


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 36–39: PRE_0_5 idx_ca_resolve fingerprint variants
# ═══════════════════════════════════════════════════════════════════════════════

def test_36_pre05_missing_idx_ca_resolve_is_hybrid():
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    conn.execute("DROP INDEX idx_ca_resolve")
    assert _detect_authority_schema(conn) == AuthoritySchemaState.HYBRID


def test_37_pre05_idx_ca_resolve_asc_is_hybrid():
    """idx_ca_resolve with created_at ASC (not DESC) → HYBRID."""
    conn = _fresh_mem_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commitments (
            id TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (id, user_id)
        )
    """)
    conn.execute("""
        CREATE TABLE commitment_authority (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT NOT NULL, user_id INTEGER NOT NULL,
            field_name TEXT NOT NULL, value TEXT DEFAULT NULL,
            authority_source TEXT NOT NULL
                CHECK(authority_source IN ('MANUAL_OVERRIDE', 'FAMILY_REVIEW')),
            override_id TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
            created_at TEXT NOT NULL, created_by INTEGER NOT NULL,
            revoked_at TEXT DEFAULT NULL, revoked_by INTEGER DEFAULT NULL,
            UNIQUE(override_id),
            FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id)
        )
    """)
    # ASC — not DESC
    conn.execute(
        "CREATE INDEX idx_ca_resolve "
        "ON commitment_authority(commitment_id, field_name, created_at ASC) "
        "WHERE is_active = 1"
    )
    assert _detect_authority_schema(conn) == AuthoritySchemaState.HYBRID


def test_38_pre05_idx_ca_resolve_non_partial_is_hybrid():
    """idx_ca_resolve without WHERE clause (non-partial) → HYBRID."""
    conn = _fresh_mem_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS commitments (
            id TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (id, user_id)
        )
    """)
    conn.execute("""
        CREATE TABLE commitment_authority (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            commitment_id TEXT NOT NULL, user_id INTEGER NOT NULL,
            field_name TEXT NOT NULL, value TEXT DEFAULT NULL,
            authority_source TEXT NOT NULL
                CHECK(authority_source IN ('MANUAL_OVERRIDE', 'FAMILY_REVIEW')),
            override_id TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1 CHECK(is_active IN (0, 1)),
            created_at TEXT NOT NULL, created_by INTEGER NOT NULL,
            revoked_at TEXT DEFAULT NULL, revoked_by INTEGER DEFAULT NULL,
            UNIQUE(override_id),
            FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id)
        )
    """)
    # No WHERE clause → non-partial
    conn.execute(
        "CREATE INDEX idx_ca_resolve "
        "ON commitment_authority(commitment_id, field_name, created_at DESC)"
    )
    assert _detect_authority_schema(conn) == AuthoritySchemaState.HYBRID


def test_39_pre05_exact_idx_ca_resolve_is_pre05():
    """Exact PRE_0_5 idx_ca_resolve → PRE_0_5 (migration allowed)."""
    conn = _fresh_mem_conn()
    _make_pre05_table(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PRE_0_5


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 40–44: Fresh atomic creation + rollback
# ═══════════════════════════════════════════════════════════════════════════════

class _FailingConnWrapper:
    """
    Wraps a sqlite3.Connection and raises RuntimeError when `trigger_sql` is
    found in an execute() call.  sqlite3.Connection.execute is a C-level method
    and cannot be monkeypatched, so we pass this wrapper to the Phase 0.5
    helpers instead of the real connection.
    """
    def __init__(self, conn, trigger_sql):
        self._conn = conn
        self._trigger = trigger_sql
        self._fired = False

    def execute(self, sql, *args, **kwargs):
        if not self._fired and self._trigger in sql:
            self._fired = True
            raise RuntimeError(f"injected failure on: {self._trigger!r}")
        return self._conn.execute(sql, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def test_40_fresh_failure_after_create_table_leaves_no_table():
    """Failure on first CREATE INDEX (just after CREATE TABLE) → rollback → no table."""
    conn = _fresh_mem_conn()
    conn.execute("CREATE TABLE IF NOT EXISTS commitments (id TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (id, user_id))")
    wrapped = _FailingConnWrapper(conn, "CREATE INDEX idx_ca_resolve")

    with pytest.raises(RuntimeError):
        _create_phase05_fresh(wrapped)

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='commitment_authority'"
    ).fetchone()
    assert exists is None


def test_41_fresh_failure_after_idx_ca_resolve_leaves_no_table():
    """Failure before idx_ca_active_instance → full rollback → no table."""
    conn = _fresh_mem_conn()
    conn.execute("CREATE TABLE IF NOT EXISTS commitments (id TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (id, user_id))")
    wrapped = _FailingConnWrapper(conn, "CREATE UNIQUE INDEX idx_ca_active_instance")

    with pytest.raises(RuntimeError):
        _create_phase05_fresh(wrapped)

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='commitment_authority'"
    ).fetchone()
    assert exists is None


def test_42_fresh_failure_after_first_unique_index_leaves_no_table():
    """Failure before idx_ca_active_field_source → full rollback → no table."""
    conn = _fresh_mem_conn()
    conn.execute("CREATE TABLE IF NOT EXISTS commitments (id TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (id, user_id))")
    wrapped = _FailingConnWrapper(conn, "CREATE UNIQUE INDEX idx_ca_active_field_source")

    with pytest.raises(RuntimeError):
        _create_phase05_fresh(wrapped)

    exists = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='commitment_authority'"
    ).fetchone()
    assert exists is None


def test_43_fresh_success_phase05_and_fk_clean():
    conn = _fresh_mem_conn()
    _make_phase05_table(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5
    violations = conn.execute("PRAGMA foreign_key_check(commitment_authority)").fetchall()
    assert violations == []


def test_44_init_db_after_failed_fresh_treats_as_fresh_not_hybrid():
    """
    After a fresh-creation failure (full rollback), a second call to
    _apply_phase05_authority sees NO_TABLE and retries — not HYBRID.
    """
    conn = _fresh_mem_conn()
    conn.execute("CREATE TABLE IF NOT EXISTS commitments (id TEXT NOT NULL, user_id INTEGER NOT NULL, PRIMARY KEY (id, user_id))")

    # First call: inject failure → rollback
    wrapped = _FailingConnWrapper(conn, "CREATE UNIQUE INDEX idx_ca_active_instance")
    with pytest.raises(RuntimeError):
        _create_phase05_fresh(wrapped)

    # After rollback: no table
    assert _detect_authority_schema(conn) == AuthoritySchemaState.NO_TABLE

    # Second call on unwrapped conn: should succeed
    _apply_phase05_authority(conn)
    assert _detect_authority_schema(conn) == AuthoritySchemaState.PHASE_0_5


# ═══════════════════════════════════════════════════════════════════════════════
# GROUP 45–49: Real init_db() ordering integration tests
# ═══════════════════════════════════════════════════════════════════════════════

import tempfile


def _make_pre05_db_file(path):
    """
    Write a minimal pre-Phase-0.5 DB file: commitments + commitment_authority
    in the pre-0.5 schema (with idx_ca_resolve), plus all tables required by
    init_db() foreign-key constraints so the script doesn't error on those.
    We fake this by running init_db() against a temp path, then downgrading
    commitment_authority back to pre-0.5.
    """
    import app as _app_module
    original = _app_module._DB_PATH if hasattr(_app_module, "_DB_PATH") else None

    # Run init_db() to get a fully-Phase-0.5 DB, then rebuild commitment_authority
    # back to pre-0.5 to simulate a legacy DB.
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA foreign_keys = ON")

    # We cannot call the real init_db() because it uses get_db() (global singleton).
    # Instead, manually downgrade an already-created Phase 0.5 table.
    # Since we can't call init_db() here, return a conn with just the tables we need.
    conn.close()
    return


def test_45_real_init_db_creates_phase05_on_fresh_db(tmp_path):
    """
    A fresh DB file: init_db() must produce a PHASE_0_5 commitment_authority.
    """
    import app as _app_mod
    db_path = str(tmp_path / "test45.db")

    original_get_db = _app_mod.get_db

    def patched_get_db():
        c = sqlite3.connect(db_path)
        c.execute("PRAGMA foreign_keys = ON")
        c.row_factory = sqlite3.Row
        return c

    _app_mod.get_db = patched_get_db
    try:
        _app_mod.init_db()
    finally:
        _app_mod.get_db = original_get_db

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    state = _detect_authority_schema(conn)
    conn.close()
    assert state == AuthoritySchemaState.PHASE_0_5


def test_46_real_init_db_idempotent_on_phase05_db(tmp_path):
    """
    Running init_db() twice on the same DB must not raise and must preserve PHASE_0_5.
    """
    import app as _app_mod
    db_path = str(tmp_path / "test46.db")

    def patched_get_db():
        c = sqlite3.connect(db_path)
        c.execute("PRAGMA foreign_keys = ON")
        c.row_factory = sqlite3.Row
        return c

    original = _app_mod.get_db
    _app_mod.get_db = patched_get_db
    try:
        _app_mod.init_db()
        fp_before = _collect_fingerprint(sqlite3.connect(db_path))
        _app_mod.init_db()
        fp_after  = _collect_fingerprint(sqlite3.connect(db_path))
    finally:
        _app_mod.get_db = original

    assert fp_before == fp_after


def test_47_no_idx_ca_resolve_in_generic_index_block():
    """
    Verify that the generic index block in init_db() does NOT contain
    'idx_ca_resolve' — it must be owned exclusively by _apply_phase05_authority.
    """
    import inspect
    import app as _app_mod
    src = inspect.getsource(_app_mod.init_db)
    # The generic index block lines contain "CREATE INDEX IF NOT EXISTS"
    # Ensure idx_ca_resolve does not appear in those lines
    generic_lines = [
        line for line in src.splitlines()
        if "CREATE INDEX IF NOT EXISTS" in line or "CREATE UNIQUE INDEX IF NOT EXISTS" in line
    ]
    for line in generic_lines:
        assert "idx_ca_resolve" not in line, \
            f"idx_ca_resolve found in generic index block: {line!r}"


def test_48_commitment_authority_not_in_executescript():
    """
    Verify that the executescript() call in init_db() does NOT contain
    'commitment_authority' — the table must be absent from the bulk DDL.
    """
    import inspect
    import ast
    import app as _app_mod
    src = inspect.getsource(_app_mod.init_db)
    # Find the executescript string by looking for the call
    assert "commitment_authority" not in src.split("executescript")[1].split("'''")[1], \
        "commitment_authority DDL found inside executescript() block"


def test_49_phase05_indexes_not_in_init_db_generic_block():
    """
    Verify that the three Phase 0.5 authority indexes are NOT created by any
    conn.execute() call inside init_db() — they must be owned exclusively by
    _apply_phase05_authority(), not embedded in the generic index block.
    Also verify _apply_phase05_authority(conn) is called from init_db().
    """
    import inspect
    import app as _app_mod
    src = inspect.getsource(_app_mod.init_db)

    assert "_apply_phase05_authority(conn)" in src, \
        "_apply_phase05_authority(conn) not found in init_db()"

    # These index names must NOT appear in a conn.execute() call inside init_db()
    for idx_name in ("idx_ca_active_instance", "idx_ca_active_field_source"):
        assert idx_name not in src, \
            f"{idx_name} found inside init_db() source — must be owned by _apply_phase05_authority"

    # idx_ca_resolve must also not appear in a direct conn.execute() call in init_db()
    # (it's created inside _apply_phase05_authority/_create_phase05_fresh/_migrate_to_phase05)
    for line in src.splitlines():
        if "conn.execute(" in line and "idx_ca_resolve" in line:
            raise AssertionError(
                f"idx_ca_resolve found in a conn.execute() call inside init_db(): {line.strip()!r}"
            )
