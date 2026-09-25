"""
Q1-B route-level integration tests: category_rules write contract via
the real PUT /api/expenses/<id> route.

All tests use isolated tmp_path DBs via the same _make_app pattern as
test_stage_db.py.  No production DB is opened.

Protected production path: C:\\Users\\erezg\\.budget_tracker_data\\budget.db
"""

import os
import sqlite3
import sys
import uuid

import pytest


# ── Production-path guard ────────────────────────────────────────────────────

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


# ── App / client factory (matches test_stage_db.py pattern) ─────────────────

def _make_app(tmp_path):
    env = {
        'APP_ENV': 'production',
        'SECRET_KEY': 's' * 64,
        'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
        'ADMIN_EMAIL': None,
        'ADMIN_PASSWORD': None,
        'ADMIN_USERNAME': None,
    }
    orig = {k: os.environ.get(k) for k in env}
    for k, v in env.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v
    for mod in list(sys.modules):
        if mod == 'app' or mod.startswith('app.'):
            del sys.modules[mod]
    try:
        import app as m
        _hard_guard(m.DB_PATH)
        m.app.config['TESTING'] = True
        return m
    finally:
        for k, orig_v in orig.items():
            if orig_v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig_v


def _make_client(mod, is_admin=False):
    """Create a user, log in, return (client, user_id)."""
    uname = 'u_' + uuid.uuid4().hex[:8]
    conn = mod.get_db()
    pw = mod.hash_password('pw')
    conn.execute(
        "INSERT INTO users (username, password_hash, email, verified, is_admin) "
        "VALUES (?,?,?,1,?)",
        (uname, pw, f'{uname}@test.com', 1 if is_admin else 0),
    )
    conn.commit()
    uid = conn.execute("SELECT id FROM users WHERE username=?", (uname,)).fetchone()['id']
    conn.close()

    client = mod.app.test_client()
    r = client.post('/api/auth/login', json={'username': uname, 'password': 'pw'})
    assert r.status_code == 200, f"Login failed: {r.data}"
    return client, uid


def _create_expense(mod, user_id, description, category_id='misc', merchant_key=''):
    """Insert an expense directly and return its id."""
    conn = mod.get_db()
    conn.execute(
        "INSERT INTO expenses (date, category_id, description, amount, user_id, merchant_key) "
        "VALUES ('2026-01-01', ?, ?, 50.0, ?, ?)",
        (category_id, description, user_id, merchant_key),
    )
    conn.commit()
    eid = conn.execute(
        "SELECT id FROM expenses WHERE description=? AND user_id=? ORDER BY id DESC LIMIT 1",
        (description, user_id),
    ).fetchone()['id']
    conn.close()
    return eid


def _add_custom_category(mod, owner_user_id, cat_id='custom_pet', name_he='חיות מחמד'):
    conn = mod.get_db()
    conn.execute(
        "INSERT OR IGNORE INTO categories (id, name_he, color, sort_order, owner_user_id) "
        "VALUES (?,?,?,99,?)",
        (cat_id, name_he, '#c08040', owner_user_id),
    )
    conn.commit()
    conn.close()


# ── Tests ────────────────────────────────────────────────────────────────────

class TestExpensePatchCategoryRulesRoute:
    """
    Route-level integration tests for the Q1-B category_rules write guard.
    All tests drive the real PUT /api/expenses/<id> route.
    """

    def test_system_category_patch_writes_global_category_rule(self, tmp_path):
        """
        PATCH expense to a system category → HTTP 200, expense updated,
        category_rules global row written.
        """
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        desc = 'שופרסל_route_test_' + uuid.uuid4().hex[:6]
        eid = _create_expense(mod, uid, desc, category_id='misc')

        r = client.put(f'/api/expenses/{eid}', json={'category_id': 'food'})
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.data}"

        conn = mod.get_db()

        # expense row updated
        exp = conn.execute("SELECT category_id FROM expenses WHERE id=?", (eid,)).fetchone()
        assert exp['category_id'] == 'food'

        # global category_rule written (description-keyed, system target)
        rule = conn.execute(
            "SELECT category_id FROM category_rules WHERE description=?", (desc,)
        ).fetchone()
        assert rule is not None, "category_rules row must be written for system category correction"
        assert rule['category_id'] == 'food'

        conn.close()

    def test_custom_category_patch_does_not_write_global_category_rule(self, tmp_path):
        """
        PATCH expense to user's own custom category → HTTP 200, expense updated,
        NO category_rules global row created.
        """
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = 'custom_pet_' + uuid.uuid4().hex[:4]
        _add_custom_category(mod, uid, cat_id=cat_id)

        desc = 'חנות_חיות_route_' + uuid.uuid4().hex[:6]
        eid = _create_expense(mod, uid, desc, category_id='misc')

        r = client.put(f'/api/expenses/{eid}', json={'category_id': cat_id})
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.data}"

        conn = mod.get_db()

        # expense row updated to custom category
        exp = conn.execute("SELECT category_id FROM expenses WHERE id=?", (eid,)).fetchone()
        assert exp['category_id'] == cat_id, "Expense must be correctable to own custom category"

        # NO global category_rule written
        rule = conn.execute(
            "SELECT 1 FROM category_rules WHERE description=?", (desc,)
        ).fetchone()
        assert rule is None, "category_rules must NOT be written for custom category correction"

        conn.close()

    def test_custom_category_patch_still_writes_merchant_learning(self, tmp_path):
        """
        PATCH expense to custom category with a non-empty merchant_key →
        merchant_learning IS written (user-scoped), category_rules is NOT.
        """
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = 'custom_garden_' + uuid.uuid4().hex[:4]
        _add_custom_category(mod, uid, cat_id=cat_id, name_he='גינון')

        mkey = 'GARDEN_STORE_' + uuid.uuid4().hex[:4]
        desc = 'חנות_גינון_' + uuid.uuid4().hex[:6]
        eid = _create_expense(mod, uid, desc, category_id='misc', merchant_key=mkey)

        r = client.put(f'/api/expenses/{eid}', json={'category_id': cat_id})
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.data}"

        conn = mod.get_db()

        # merchant_learning written for this user (user-scoped, always allowed)
        ml = conn.execute(
            "SELECT category_id, confidence FROM merchant_learning "
            "WHERE user_id=? AND merchant_key=?",
            (uid, mkey),
        ).fetchone()
        assert ml is not None, "merchant_learning must be written even for custom category"
        assert ml['category_id'] == cat_id
        assert ml['confidence'] == 0.95

        # category_rules NOT written (custom target, global table)
        rule = conn.execute(
            "SELECT 1 FROM category_rules WHERE description=?", (desc,)
        ).fetchone()
        assert rule is None, "category_rules must NOT be written for custom category correction"

        conn.close()

    def test_cross_user_custom_category_ownership(self, tmp_path):
        """
        Security check: User A attempts to PATCH their expense to User B's
        custom category. Report whether the route accepts or rejects this.

        Current Q1-B scope does not add ownership validation to the PUT route.
        This test documents the current behavior without enforcing a block
        (enforcement belongs to Q1-C).
        """
        mod = _make_app(tmp_path)

        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)

        cat_b_id = 'custom_b_' + uuid.uuid4().hex[:4]
        _add_custom_category(mod, uid_b, cat_id=cat_b_id, name_he='קטגוריה של B')

        desc = 'הוצאה_של_A_' + uuid.uuid4().hex[:6]
        eid = _create_expense(mod, uid_a, desc, category_id='misc')

        r = client_a.put(f'/api/expenses/{eid}', json={'category_id': cat_b_id})

        # Document current behavior: route does not yet validate category ownership.
        # A 200 here means the route accepted it (no Q1-C guard yet).
        # A non-200 means an existing guard already blocked it.
        conn = mod.get_db()
        exp = conn.execute("SELECT category_id FROM expenses WHERE id=?", (eid,)).fetchone()
        conn.close()

        # Either way: no global category_rules row must be created for a custom category
        conn = mod.get_db()
        rule = conn.execute(
            "SELECT 1 FROM category_rules WHERE description=?", (desc,)
        ).fetchone()
        conn.close()
        assert rule is None, (
            "category_rules must never be written for a custom category target, "
            "regardless of whether the cross-user correction is accepted or rejected"
        )

        # Record the current behavior for documentation purposes
        if r.status_code == 200:
            # Route accepted cross-user custom category assignment (no ownership guard yet)
            assert exp['category_id'] == cat_b_id  # documents current permissive behavior
        else:
            # Route already rejects cross-user custom category (existing guard present)
            assert exp['category_id'] == 'misc'    # expense unchanged
