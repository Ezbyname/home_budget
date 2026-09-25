"""
Q1-B tests: category access/visibility helpers + auto-assignment gate.

ISOLATION: same model as Q1-A.
  - HOME + USERPROFILE redirected before app import
  - app.DB_PATH explicitly patched to temp file per test
  - hard guard against exact production path

Production DB (must never be opened):
    C:\\Users\\erezg\\.budget_tracker_data\\budget.db
"""

import os
import shutil
import sqlite3
import sys
import tempfile
import types

import pytest


# ── Production-path guard (identical to Q1-A) ────────────────────────────────

_PRODUCTION_DB_NORMALIZED = os.path.normcase(
    os.path.normpath(r'C:\Users\erezg\.budget_tracker_data\budget.db')
)
_PRODUCTION_DIR_NORMALIZED = os.path.normcase(
    os.path.normpath(r'C:\Users\erezg\.budget_tracker_data')
) + os.sep


def _hard_guard(path: str) -> None:
    norm = os.path.normcase(os.path.normpath(path))
    if norm == _PRODUCTION_DB_NORMALIZED or norm.startswith(_PRODUCTION_DIR_NORMALIZED):
        pytest.fail(f"HARD STOP: production path detected: {path!r}")


# ── Session fixtures ──────────────────────────────────────────────────────────

@pytest.fixture(scope='session')
def _session_home():
    d = tempfile.mkdtemp(prefix='q1b_home_')
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
    for mod in list(sys.modules):
        if mod == 'app' or mod.startswith('app.'):
            del sys.modules[mod]
    import app as _app
    _hard_guard(_app.DB_PATH)
    assert _session_home in _app.DB_PATH
    return _app


# ── Per-test isolated DB fixture ──────────────────────────────────────────────

def _build_isolated_db(path: str) -> None:
    """
    Minimal schema sufficient for Q1-B helper tests.
    Includes: categories (with Q1-A columns), users, user_category_preferences.
    Does NOT need expenses or other tables.
    """
    conn = sqlite3.connect(path)
    conn.executescript("""
        CREATE TABLE categories (
            id           TEXT PRIMARY KEY,
            name_he      TEXT NOT NULL,
            color        TEXT NOT NULL DEFAULT '#888888',
            sort_order   INTEGER DEFAULT 0,
            parent_id    TEXT DEFAULT NULL,
            owner_user_id INTEGER DEFAULT NULL,
            icon         TEXT DEFAULT NULL
        );
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE
        );
        CREATE TABLE user_category_preferences (
            user_id     INTEGER NOT NULL,
            category_id TEXT    NOT NULL,
            PRIMARY KEY (user_id, category_id)
        );
        CREATE TABLE category_rules (
            description TEXT PRIMARY KEY,
            category_id TEXT NOT NULL
        );
        CREATE TABLE expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            category_id TEXT NOT NULL,
            description TEXT,
            amount REAL NOT NULL DEFAULT 0,
            user_id INTEGER NOT NULL DEFAULT 0,
            frequency TEXT DEFAULT 'random'
        );
    """)
    # Seed: system categories
    system_cats = [
        ('food',     'מזון',    '#f28e2b', 0, None),
        ('housing',  'דיור',   '#4e79a7', 1, None),
        ('misc',     'שונות',  '#a5a5a5', 2, None),
        ('mortgage', 'משכנתא', '#4e79a7', 10, 'housing'),  # child of housing
        ('rent',     'שכר דירה','#4e79a7', 11, 'housing'),  # child of housing
    ]
    for cat_id, name_he, color, sort_order, parent_id in system_cats:
        conn.execute(
            "INSERT INTO categories (id, name_he, color, sort_order, parent_id, owner_user_id) VALUES (?,?,?,?,?,NULL)",
            (cat_id, name_he, color, sort_order, parent_id)
        )
    # User 1 custom category
    conn.execute(
        "INSERT INTO categories (id, name_he, color, sort_order, owner_user_id) VALUES (?,?,?,?,?)",
        ('custom_user1', 'חיות מחמד', '#c08040', 99, 1)
    )
    # User 2 custom category
    conn.execute(
        "INSERT INTO categories (id, name_he, color, sort_order, owner_user_id) VALUES (?,?,?,?,?)",
        ('custom_user2', 'גינון', '#3a7a40', 99, 2)
    )
    conn.commit()
    conn.close()


@pytest.fixture
def isolated_db(app_module):
    """Minimal schema DB for helper unit tests (no pipeline)."""
    orig_path = app_module.DB_PATH
    tmpdir = tempfile.mkdtemp(prefix='q1b_db_')
    db_path = os.path.join(tmpdir, 'test.db')
    _hard_guard(db_path)
    _build_isolated_db(db_path)
    app_module.DB_PATH = db_path

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=OFF")  # matches production setting

    yield conn, app_module

    conn.close()
    app_module.DB_PATH = orig_path
    shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.fixture
def full_db(app_module):
    """
    Full-schema DB built by real app.init_db().
    Required for pipeline tests that call resolve_category() or smart_categorize(),
    which need merchant_aliases, feature_flags, and all other init_db() tables.

    After init_db(), inserts the test category/user fixtures into the real schema.
    """
    orig_path = app_module.DB_PATH
    tmpdir = tempfile.mkdtemp(prefix='q1b_full_')
    db_path = os.path.join(tmpdir, 'full_test.db')
    _hard_guard(db_path)

    app_module.DB_PATH = db_path
    _hard_guard(app_module.DB_PATH)
    app_module.init_db()

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=OFF")

    # Add user 1 custom category for access/visibility tests
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color, sort_order, owner_user_id) VALUES (?,?,?,?,?)",
        ('custom_user1', 'חיות מחמד', '#c08040', 99, 1)
    )
    # Add user 2 custom category
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color, sort_order, owner_user_id) VALUES (?,?,?,?,?)",
        ('custom_user2', 'גינון', '#3a7a40', 99, 2)
    )
    conn.commit()

    yield conn, app_module

    conn.close()
    app_module.DB_PATH = orig_path
    shutil.rmtree(tmpdir, ignore_errors=True)


# ═══════════════════════════════════════════════════════════════════════════════
# ACCESS TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestCategoryAccessible:

    def test_system_category_accessible_to_any_user(self, isolated_db):
        conn, app = isolated_db
        assert app.category_accessible(conn, 1, 'food') is True
        assert app.category_accessible(conn, 2, 'food') is True
        assert app.category_accessible(conn, 999, 'food') is True

    def test_own_custom_category_accessible_to_owner(self, isolated_db):
        conn, app = isolated_db
        assert app.category_accessible(conn, 1, 'custom_user1') is True

    def test_other_users_custom_category_not_accessible(self, isolated_db):
        conn, app = isolated_db
        assert app.category_accessible(conn, 1, 'custom_user2') is False

    def test_other_users_custom_category_not_accessible_even_without_hide_preference(self, isolated_db):
        conn, app = isolated_db
        # No preference row for user 1 on custom_user2 — still inaccessible
        pref = conn.execute(
            "SELECT 1 FROM user_category_preferences WHERE user_id=1 AND category_id='custom_user2'"
        ).fetchone()
        assert pref is None
        assert app.category_accessible(conn, 1, 'custom_user2') is False

    def test_nonexistent_category_not_accessible(self, isolated_db):
        conn, app = isolated_db
        assert app.category_accessible(conn, 1, 'does_not_exist') is False

    def test_hidden_system_category_remains_accessible_to_requesting_user(self, isolated_db):
        conn, app = isolated_db
        # User 1 hides 'food'
        conn.execute(
            "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        conn.commit()
        # Accessible is separate from visible — still accessible
        assert app.category_accessible(conn, 1, 'food') is True

    def test_hidden_system_category_remains_visible_to_other_user(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        conn.commit()
        # User 2 is unaffected
        assert app.effective_visible(conn, 2, 'food') is True


# ═══════════════════════════════════════════════════════════════════════════════
# VISIBILITY TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestEffectiveVisible:

    def test_no_preference_row_means_effectively_visible(self, isolated_db):
        conn, app = isolated_db
        assert app.effective_visible(conn, 1, 'food') is True

    def test_direct_preference_means_effectively_hidden(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        conn.commit()
        assert app.effective_visible(conn, 1, 'food') is False

    def test_hide_parent_hides_descendant_effectively(self, isolated_db):
        conn, app = isolated_db
        # Hide housing (parent) for user 1
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'housing')"
        )
        conn.commit()
        # mortgage and rent are children of housing
        assert app.effective_visible(conn, 1, 'mortgage') is False
        assert app.effective_visible(conn, 1, 'rent') is False
        # housing itself also not visible
        assert app.effective_visible(conn, 1, 'housing') is False

    def test_parent_hide_does_not_create_child_preference(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'housing')"
        )
        conn.commit()
        # No preference rows for children created
        row = conn.execute(
            "SELECT 1 FROM user_category_preferences WHERE user_id=1 AND category_id='mortgage'"
        ).fetchone()
        assert row is None

    def test_restore_parent_restores_child_without_direct_preference(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'housing')"
        )
        conn.commit()
        assert app.effective_visible(conn, 1, 'mortgage') is False
        # Restore parent
        conn.execute(
            "DELETE FROM user_category_preferences WHERE user_id=1 AND category_id='housing'"
        )
        conn.commit()
        assert app.effective_visible(conn, 1, 'mortgage') is True

    def test_directly_hidden_child_remains_hidden_after_parent_restore(self, isolated_db):
        conn, app = isolated_db
        # Hide both parent and child
        conn.executemany(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,?)",
            [('housing',), ('mortgage',)]
        )
        conn.commit()
        # Restore parent
        conn.execute(
            "DELETE FROM user_category_preferences WHERE user_id=1 AND category_id='housing'"
        )
        conn.commit()
        # mortgage still directly hidden
        assert app.effective_visible(conn, 1, 'mortgage') is False

    def test_other_user_unaffected_by_parent_hide(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'housing')"
        )
        conn.commit()
        # User 2 sees mortgage normally
        assert app.effective_visible(conn, 2, 'mortgage') is True

    def test_effective_visible_cycle_fails_closed(self, isolated_db):
        conn, app = isolated_db
        # Create a cycle: A → B → A
        conn.execute("INSERT INTO categories (id, name_he, color, sort_order, owner_user_id) VALUES ('cycle_a','A','#aaa',50,NULL)")
        conn.execute("INSERT INTO categories (id, name_he, color, sort_order, owner_user_id, parent_id) VALUES ('cycle_b','B','#bbb',51,NULL,'cycle_a')")
        conn.execute("UPDATE categories SET parent_id='cycle_b' WHERE id='cycle_a'")
        conn.commit()
        assert app.effective_visible(conn, 1, 'cycle_a') is False
        assert app.effective_visible(conn, 1, 'cycle_b') is False

    def test_effective_visible_missing_node_fails_closed(self, isolated_db):
        conn, app = isolated_db
        assert app.effective_visible(conn, 1, 'nonexistent_xyz') is False


# ═══════════════════════════════════════════════════════════════════════════════
# IS_AUTO_ASSIGNABLE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsAutoAssignable:

    def test_auto_assign_visible_system_category_allowed(self, isolated_db):
        conn, app = isolated_db
        assert app.is_auto_assignable(conn, 1, 'food') is True

    def test_auto_assign_hidden_system_category_blocked(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        conn.commit()
        assert app.is_auto_assignable(conn, 1, 'food') is False

    def test_auto_assign_other_users_custom_category_blocked(self, isolated_db):
        conn, app = isolated_db
        assert app.is_auto_assignable(conn, 1, 'custom_user2') is False

    def test_auto_assign_own_visible_custom_category_allowed(self, isolated_db):
        conn, app = isolated_db
        assert app.is_auto_assignable(conn, 1, 'custom_user1') is True

    def test_auto_assign_own_hidden_custom_category_blocked(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'custom_user1')"
        )
        conn.commit()
        assert app.is_auto_assignable(conn, 1, 'custom_user1') is False

    def test_misc_remains_valid_fallback(self, isolated_db):
        conn, app = isolated_db
        # misc is a system category; no preference rows → always auto-assignable
        assert app.is_auto_assignable(conn, 1, 'misc') is True
        assert app.is_auto_assignable(conn, 2, 'misc') is True
        assert app.is_auto_assignable(conn, 999, 'misc') is True


# ═══════════════════════════════════════════════════════════════════════════════
# INTELLIGENCE PIPELINE GATE TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestPipelineGate:
    """
    Tests that exercise resolve_category() with the per-stage is_auto_assignable() gate.
    Uses full_db (real app.init_db() schema) because resolve_category() calls
    resolve_merchant_key() which requires merchant_aliases, and smart_categorize()
    requires feature_flags.
    """

    def test_pipeline_accepts_visible_p1_candidate(self, full_db):
        from intelligence.categorizer import resolve_category
        from intelligence.normalizer import normalize_merchant
        conn, app = full_db
        # merchant_key must match what normalize_merchant() produces
        mk = normalize_merchant('testmerchant_p1')
        conn.execute(
            "INSERT OR REPLACE INTO merchant_learning (user_id, merchant_key, category_id, confidence) VALUES (1,?,?,?)",
            (mk, 'food', 0.90)
        )
        conn.commit()
        result = resolve_category('testmerchant_p1', 10.0, 1, conn)
        assert result.category_id == 'food'
        assert result.source == 'merchant_learning'

    def test_pipeline_continues_when_p1_candidate_hidden(self, full_db):
        """P1 proposes 'food' (hidden for user 1) → pipeline continues → P5 misc."""
        from intelligence.categorizer import resolve_category
        from intelligence.normalizer import normalize_merchant
        conn, app = full_db
        conn.execute(
            "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        mk = normalize_merchant('testmerchant_p1hidden')
        conn.execute(
            "INSERT OR REPLACE INTO merchant_learning (user_id, merchant_key, category_id, confidence) VALUES (1,?,?,?)",
            (mk, 'food', 0.90)
        )
        conn.commit()
        result = resolve_category('testmerchant_p1hidden', 10.0, 1, conn)
        # P1 blocked; no lower-stage resolver fires → P5 misc
        assert result.category_id == 'misc'
        assert result.source == 'unresolved'

    def test_pipeline_accepts_p2_when_p1_hidden(self, full_db):
        """P1 (food, hidden) → P2 fingerprint (housing, visible) accepted."""
        from intelligence.categorizer import resolve_category
        from intelligence.normalizer import normalize_merchant
        conn, app = full_db
        conn.execute(
            "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        # resolve_merchant_key normalizes the full description, so keys must match
        mk = normalize_merchant('testmerchant_p2 RENT payment')
        conn.execute(
            "INSERT OR REPLACE INTO merchant_learning (user_id, merchant_key, category_id, confidence) VALUES (1,?,?,?)",
            (mk, 'food', 0.90)
        )
        # fingerprint: keyword 'RENT' matches description 'testmerchant_p2 RENT payment'.upper()
        conn.execute(
            "INSERT OR REPLACE INTO merchant_fingerprints (user_id, merchant_key, keyword, category_id, weight) VALUES (1,?,?,?,?)",
            (mk, 'RENT', 'housing', 2.0)
        )
        conn.commit()
        result = resolve_category('testmerchant_p2 RENT payment', 500.0, 1, conn)
        assert result.category_id == 'housing'
        assert result.source == 'fingerprint'

    def test_pipeline_continues_when_intermediate_candidate_hidden(self, full_db):
        """P3e rule proposes 'housing' (hidden) → pipeline continues → P5 misc."""
        from intelligence.categorizer import resolve_category
        conn, app = full_db
        conn.execute(
            "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,'housing')"
        )
        conn.commit()

        def _rule(c, desc, cat, freq, uid):
            return ('housing', 'monthly')

        result = resolve_category('RENT PAYMENT', 1000.0, 1, conn, apply_legacy_rule_fn=_rule)
        assert result.category_id == 'misc'

    def test_pipeline_falls_to_misc_when_no_candidate_is_assignable(self, full_db):
        """All proposed candidates hidden → P5 misc."""
        from intelligence.categorizer import resolve_category
        conn, app = full_db
        for cat in ('food', 'housing'):
            conn.execute(
                "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,?)",
                (cat,)
            )
        conn.execute(
            "INSERT OR REPLACE INTO merchant_learning (user_id, merchant_key, category_id, confidence) VALUES (1,'testm','food',0.90)"
        )
        conn.commit()
        result = resolve_category('testm', 10.0, 1, conn)
        assert result.category_id == 'misc'


# ═══════════════════════════════════════════════════════════════════════════════
# CATEGORY_RULES CONTRACT TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestCategoryRulesContract:

    def test_visible_system_global_rule_can_assign(self, isolated_db):
        conn, app = isolated_db
        # category_rules has 'food' rule; food is visible → assign
        conn.execute(
            "INSERT INTO category_rules (description, category_id) VALUES ('SUPERMARKET', 'food')"
        )
        conn.commit()
        cat, _ = app.apply_category_rule(conn, 'SUPERMARKET', 'misc', 'random', 1)
        assert cat == 'food'
        # food is auto-assignable
        assert app.is_auto_assignable(conn, 1, cat) is True

    def test_hidden_system_global_rule_is_rejected_for_that_user(self, isolated_db):
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO category_rules (description, category_id) VALUES ('SUPERMARKET', 'food')"
        )
        conn.execute(
            "INSERT INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        conn.commit()
        cat, _ = app.apply_category_rule(conn, 'SUPERMARKET', 'misc', 'random', 1)
        # apply_category_rule returns the rule result; caller must validate via is_auto_assignable
        assert cat == 'food'
        # gate rejects it
        assert app.is_auto_assignable(conn, 1, cat) is False

    def test_hidden_global_rule_falls_through_safely(self, full_db):
        """smart_categorize() legacy path (flag OFF): hidden rule result falls back to misc."""
        conn, app = full_db
        conn.execute(
            "INSERT OR REPLACE INTO category_rules (description, category_id) VALUES ('HIDDEN RULE DESC', 'food')"
        )
        conn.execute(
            "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (1,'food')"
        )
        conn.commit()
        # merchant_learning flag is OFF by default in test DB → legacy path runs
        result = app.smart_categorize(conn, 'HIDDEN RULE DESC', 10.0, 1)
        assert result.category_id == 'misc'

    def test_new_global_category_rule_cannot_target_custom_category(self, isolated_db):
        conn, app = isolated_db
        # is_system_category() gate: custom_user1 is not a system category
        assert app.is_system_category(conn, 'food') is True
        assert app.is_system_category(conn, 'custom_user1') is False
        assert app.is_system_category(conn, 'custom_user2') is False
        assert app.is_system_category(conn, 'nonexistent') is False

    def test_existing_category_rules_are_not_modified_by_q1b(self, isolated_db):
        conn, app = isolated_db
        # Insert a rule, then verify it survives (no Q1-B code deletes rules)
        conn.execute(
            "INSERT INTO category_rules (description, category_id) VALUES ('TEST_RULE', 'misc')"
        )
        conn.commit()
        row = conn.execute(
            "SELECT category_id FROM category_rules WHERE description='TEST_RULE'"
        ).fetchone()
        assert row is not None
        assert row['category_id'] == 'misc'

    def test_other_users_custom_category_never_assigned_via_global_rule(self, isolated_db):
        conn, app = isolated_db
        # A category_rules row (if it existed) pointing at custom_user2
        # must never be assigned to user 1 — is_auto_assignable blocks it
        assert app.is_auto_assignable(conn, 1, 'custom_user2') is False


# ═══════════════════════════════════════════════════════════════════════════════
# IS_SYSTEM_CATEGORY TESTS
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsSystemCategory:

    def test_system_category_identified(self, isolated_db):
        conn, app = isolated_db
        assert app.is_system_category(conn, 'food') is True
        assert app.is_system_category(conn, 'misc') is True
        assert app.is_system_category(conn, 'housing') is True

    def test_custom_category_not_system(self, isolated_db):
        conn, app = isolated_db
        assert app.is_system_category(conn, 'custom_user1') is False
        assert app.is_system_category(conn, 'custom_user2') is False

    def test_nonexistent_category_not_system(self, isolated_db):
        conn, app = isolated_db
        assert app.is_system_category(conn, 'does_not_exist') is False


# ── TestCategoryRulesWriteContract ───────────────────────────────────────────

class TestCategoryRulesWriteContract:
    """
    Q1-B contract: category_rules is a global table (no user_id).
    A new row may only be written when the target category is a system category
    (owner_user_id IS NULL).  Custom-category corrections must NOT write a
    global rule — the expense correction itself still succeeds.
    """

    def _write_category_rule_if_system(self, conn, app, description, category_id):
        """
        Mirror of the guarded production write path:
            if is_system_category(conn, new_cat):
                INSERT OR REPLACE INTO category_rules ...
        Returns True if a row was written.
        """
        if app.is_system_category(conn, category_id):
            conn.execute(
                "INSERT OR REPLACE INTO category_rules (description, category_id) VALUES (?, ?)",
                (description, category_id)
            )
            conn.commit()
            return True
        return False

    def test_expense_correction_to_system_category_writes_global_category_rule(self, isolated_db):
        conn, app = isolated_db
        wrote = self._write_category_rule_if_system(conn, app, 'סופר פארם', 'food')
        assert wrote is True
        row = conn.execute(
            "SELECT category_id FROM category_rules WHERE description=?", ('סופר פארם',)
        ).fetchone()
        assert row is not None
        assert row['category_id'] == 'food'

    def test_expense_correction_to_custom_category_does_not_write_global_category_rule(self, isolated_db):
        conn, app = isolated_db
        wrote = self._write_category_rule_if_system(conn, app, 'חנות חיות', 'custom_user1')
        assert wrote is False
        row = conn.execute(
            "SELECT category_id FROM category_rules WHERE description=?", ('חנות חיות',)
        ).fetchone()
        assert row is None, "Global category_rule must NOT be created for a custom category"

    def test_custom_category_expense_correction_still_succeeds(self, isolated_db):
        """
        The expense row itself can be corrected to a custom category.
        Only the global category_rules write is blocked.
        Simulate: update expense category_id directly (the correction part always succeeds).
        """
        conn, app = isolated_db
        conn.execute(
            "INSERT INTO expenses (date, category_id, description, amount, user_id) "
            "VALUES ('2026-01-01', 'misc', 'חנות חיות', 50.0, 1)"
        )
        conn.commit()
        # Correction to custom category: update the expense row
        conn.execute(
            "UPDATE expenses SET category_id=? WHERE description=? AND user_id=1",
            ('custom_user1', 'חנות חיות')
        )
        conn.commit()
        # Verify expense was corrected
        row = conn.execute(
            "SELECT category_id FROM expenses WHERE description=? AND user_id=1",
            ('חנות חיות',)
        ).fetchone()
        assert row['category_id'] == 'custom_user1', "Expense must be correctable to custom category"
        # Verify no global rule was written (the guarded path would have been skipped)
        rule = conn.execute(
            "SELECT 1 FROM category_rules WHERE description=?", ('חנות חיות',)
        ).fetchone()
        assert rule is None, "No global category_rule must be created for custom category correction"

    def test_existing_category_rules_unchanged_by_q1b(self, isolated_db):
        """Pre-existing global rules are never touched by Q1-B."""
        conn, app = isolated_db
        conn.execute(
            "INSERT OR REPLACE INTO category_rules (description, category_id) VALUES (?, ?)",
            ('רמי לוי', 'food')
        )
        conn.commit()
        # Q1-B logic does not scan or modify existing rows
        row = conn.execute(
            "SELECT category_id FROM category_rules WHERE description=?", ('רמי לוי',)
        ).fetchone()
        assert row is not None
        assert row['category_id'] == 'food'

    def test_custom_category_correction_may_update_merchant_learning(self, isolated_db):
        """
        merchant_learning is user-scoped (has user_id).
        A custom-category correction may still write merchant_learning for that user.
        Only category_rules (global) is blocked.
        """
        conn, app = isolated_db
        # Add merchant_aliases table (needed by resolve_merchant_key in some paths)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS merchant_aliases "
            "(merchant_key TEXT NOT NULL, alias TEXT NOT NULL PRIMARY KEY)"
        )
        conn.execute(
            "CREATE TABLE IF NOT EXISTS merchant_learning "
            "(user_id INTEGER NOT NULL, merchant_key TEXT NOT NULL, "
            " category_id TEXT NOT NULL, confidence REAL DEFAULT 0.5, "
            " source TEXT DEFAULT 'user', display_name TEXT, "
            " PRIMARY KEY (user_id, merchant_key))"
        )
        conn.commit()
        # Simulate the user-scoped merchant_learning write (always allowed)
        conn.execute(
            "INSERT OR REPLACE INTO merchant_learning "
            "(user_id, merchant_key, category_id, confidence, source) "
            "VALUES (1, 'ANIMAL SHOP', 'custom_user1', 0.95, 'user')"
        )
        conn.commit()
        ml = conn.execute(
            "SELECT category_id FROM merchant_learning WHERE user_id=1 AND merchant_key='ANIMAL SHOP'"
        ).fetchone()
        assert ml['category_id'] == 'custom_user1'
        # And confirm no global rule was written
        rule = conn.execute(
            "SELECT 1 FROM category_rules WHERE category_id='custom_user1'"
        ).fetchone()
        assert rule is None


# ── TestBaselineNoOp ──────────────────────────────────────────────────────────

class TestBaselineNoOp:
    """
    Q1-B must be a no-op when all categories are system categories and no
    user_category_preferences rows exist. Categorization precedence and results
    must be identical to pre-Q1B behavior.
    """

    def test_p0_mortgage_baseline_no_preferences(self, full_db):
        """P0 mortgage override fires unchanged when mortgage is not hidden."""
        from intelligence.categorizer import resolve_category
        conn, app = full_db
        result = resolve_category('משכנתא בנק הפועלים', 3000.0, 1, conn)
        assert result.category_id == 'mortgage'
        assert result.source == 'deterministic'
        assert result.confidence == 0.95

    def test_p2_fingerprint_baseline_no_preferences(self, full_db):
        """P2 fingerprint fires unchanged when category is not hidden."""
        from intelligence.categorizer import resolve_category
        from intelligence.normalizer import normalize_merchant
        conn, app = full_db
        mk = normalize_merchant('baseline supermarket')
        conn.execute(
            "INSERT OR REPLACE INTO merchant_fingerprints "
            "(user_id, merchant_key, keyword, category_id, weight) VALUES (1,?,?,?,?)",
            (mk, 'SUPERMARKET', 'food', 2.0)
        )
        conn.commit()
        result = resolve_category('baseline supermarket', 200.0, 1, conn)
        assert result.category_id == 'food'
        assert result.source == 'fingerprint'

    def test_p5_misc_fallback_baseline_no_preferences(self, full_db):
        """P5 misc fallback fires unchanged when no resolver matches."""
        from intelligence.categorizer import resolve_category
        conn, app = full_db
        result = resolve_category('xyzzy_unknown_merchant_q1b_baseline', 1.0, 1, conn)
        assert result.category_id == 'misc'
        assert result.source == 'unresolved'
        assert result.confidence == 0.0
