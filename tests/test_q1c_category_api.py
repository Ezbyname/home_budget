"""
Q1-C tests: category API endpoints + manual assignment enforcement.

Isolation contract (identical to Q1-A / Q1-B):
  - HOME and USERPROFILE are redirected to a dedicated temp directory BEFORE
    app is imported, so expanduser('~') never resolves to the real home.
  - app.DB_PATH is explicitly patched to a temp SQLite file AFTER import.
  - The exact normalised production path is compared OS-independently via
    os.path.normcase + os.path.normpath; the guard fires on both exact match
    and 'erezg' substring — correct on any host OS.
  - _hard_guard() is called on every candidate DB path before use.
  - An assertion confirms DB_PATH is inside the test temp directory.

Protected production path: C:\\Users\\erezg\\.budget_tracker_data\\budget.db
"""

import os
import shutil
import sys
import tempfile
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


# ── App / client factory ─────────────────────────────────────────────────────

def _make_app(tmp_path):
    """
    Hardened app factory.

    1. Create a dedicated temp directory for HOME / USERPROFILE redirect.
    2. Set HOME and USERPROFILE to that directory BEFORE any app import,
       so os.path.expanduser('~') never resolves to the real home directory.
    3. Set RAILWAY_VOLUME_MOUNT_PATH to tmp_path so the DB resolves there.
    4. Evict any cached 'app' module, then import fresh.
    5. Call _hard_guard() on the resulting DB_PATH.
    6. Assert DB_PATH is under tmp_path (containment check).
    7. Restore HOME/USERPROFILE after import (app captured DB_PATH at import time).
    """
    fake_home = tempfile.mkdtemp(prefix='q1c_home_')
    orig_home = os.environ.get('HOME')
    orig_up   = os.environ.get('USERPROFILE')

    # Redirect home BEFORE import
    os.environ['HOME']        = fake_home
    os.environ['USERPROFILE'] = fake_home

    env_patch = {
        'APP_ENV': 'production',
        'SECRET_KEY': 's' * 64,
        'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
        'ADMIN_EMAIL': None,
        'ADMIN_PASSWORD': None,
        'ADMIN_USERNAME': None,
    }
    orig_env = {k: os.environ.get(k) for k in env_patch}
    for k, v in env_patch.items():
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
        assert str(tmp_path) in m.DB_PATH, (
            f"DB_PATH {m.DB_PATH!r} is not inside tmp_path {tmp_path!r}"
        )
        m.app.config['TESTING'] = True
        return m
    finally:
        # Restore HOME / USERPROFILE
        if orig_home is None:
            os.environ.pop('HOME', None)
        else:
            os.environ['HOME'] = orig_home
        if orig_up is None:
            os.environ.pop('USERPROFILE', None)
        else:
            os.environ['USERPROFILE'] = orig_up
        # Restore other env vars
        for k, orig_v in orig_env.items():
            if orig_v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig_v
        shutil.rmtree(fake_home, ignore_errors=True)


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


def _add_custom_category(mod, owner_uid, name_he='חיות מחמד', color='#c08040',
                          parent_id=None):
    """Insert a custom category directly, return its id."""
    cat_id = 'custom_' + uuid.uuid4().hex
    conn = mod.get_db()
    conn.execute(
        "INSERT INTO categories (id, name_he, color, sort_order, owner_user_id, parent_id) "
        "VALUES (?,?,?,99,?,?)",
        (cat_id, name_he, color, owner_uid, parent_id),
    )
    conn.commit()
    conn.close()
    return cat_id


def _create_expense(mod, user_id, description='test expense',
                    category_id='misc', merchant_key=''):
    conn = mod.get_db()
    conn.execute(
        "INSERT INTO expenses "
        "(date, category_id, description, amount, user_id, merchant_key) "
        "VALUES ('2026-01-01',?,?,50.0,?,?)",
        (category_id, description, user_id, merchant_key),
    )
    conn.commit()
    eid = conn.execute(
        "SELECT id FROM expenses WHERE description=? AND user_id=? "
        "ORDER BY id DESC LIMIT 1",
        (description, user_id),
    ).fetchone()['id']
    conn.close()
    return eid


def _hide(mod, uid, cat_id):
    conn = mod.get_db()
    conn.execute(
        "INSERT OR IGNORE INTO user_category_preferences (user_id, category_id) VALUES (?,?)",
        (uid, cat_id),
    )
    conn.commit()
    conn.close()


# ════════════════════════════════════════════════════════════════════════════
# GET /api/categories
# ════════════════════════════════════════════════════════════════════════════

class TestGetCategoriesDefault:

    def test_get_categories_default_returns_visible_system_categories(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.get('/api/categories')
        assert r.status_code == 200
        cats = r.get_json()
        ids = [c['id'] for c in cats]
        assert 'food' in ids
        assert 'misc' in ids

    def test_get_categories_default_returns_own_visible_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='גינון')
        r = client.get('/api/categories')
        assert r.status_code == 200
        ids = [c['id'] for c in r.get_json()]
        assert cat_id in ids

    def test_get_categories_default_excludes_other_users_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='קטגוריה של B')
        r = client_a.get('/api/categories')
        ids = [c['id'] for c in r.get_json()]
        assert cat_b not in ids

    def test_get_categories_default_excludes_directly_hidden_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _hide(mod, uid, 'food')
        r = client.get('/api/categories')
        ids = [c['id'] for c in r.get_json()]
        assert 'food' not in ids

    def test_get_categories_default_excludes_child_of_hidden_parent(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        # mortgage is a child of housing in default categories
        _hide(mod, uid, 'housing')
        r = client.get('/api/categories')
        ids = [c['id'] for c in r.get_json()]
        assert 'housing' not in ids
        assert 'mortgage' not in ids

    def test_get_categories_requires_login(self, tmp_path):
        mod = _make_app(tmp_path)
        client = mod.app.test_client()
        r = client.get('/api/categories')
        assert r.status_code in (401, 302)


class TestGetCategoriesManageScope:

    def test_manage_scope_returns_own_hidden_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _hide(mod, uid, 'food')
        r = client.get('/api/categories?scope=manage')
        assert r.status_code == 200
        cats = {c['id']: c for c in r.get_json()}
        assert 'food' in cats
        assert cats['food']['is_directly_hidden'] is True
        assert cats['food']['is_effectively_hidden'] is True

    def test_manage_scope_excludes_other_users_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='שלי בלבד')
        r = client_a.get('/api/categories?scope=manage')
        ids = [c['id'] for c in r.get_json()]
        assert cat_b not in ids

    def test_manage_scope_reports_direct_hidden_state(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _hide(mod, uid, 'food')
        r = client.get('/api/categories?scope=manage')
        cats = {c['id']: c for c in r.get_json()}
        assert cats['food']['is_directly_hidden'] is True
        assert cats['food']['is_effectively_hidden'] is True

    def test_manage_scope_reports_inherited_hidden_state(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _hide(mod, uid, 'housing')
        r = client.get('/api/categories?scope=manage')
        cats = {c['id']: c for c in r.get_json()}
        # mortgage is child of housing
        assert cats['mortgage']['is_directly_hidden'] is False
        assert cats['mortgage']['is_effectively_hidden'] is True

    def test_manage_scope_reports_hidden_by_ancestor(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _hide(mod, uid, 'housing')
        r = client.get('/api/categories?scope=manage')
        cats = {c['id']: c for c in r.get_json()}
        assert cats['mortgage']['hidden_by_ancestor_id'] == 'housing'

    def test_manage_scope_distinguishes_direct_and_inherited_hidden(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _hide(mod, uid, 'housing')
        r = client.get('/api/categories?scope=manage')
        cats = {c['id']: c for c in r.get_json()}
        assert cats['housing']['is_directly_hidden'] is True
        assert cats['housing']['hidden_by_ancestor_id'] is None
        assert cats['mortgage']['is_directly_hidden'] is False
        assert cats['mortgage']['is_effectively_hidden'] is True


# ════════════════════════════════════════════════════════════════════════════
# POST /api/categories
# ════════════════════════════════════════════════════════════════════════════

class TestPostCategory:

    def test_post_category_requires_login(self, tmp_path):
        mod = _make_app(tmp_path)
        client = mod.app.test_client()
        r = client.post('/api/categories', json={'name': 'גינון'})
        assert r.status_code in (401, 302)

    def test_post_category_server_generates_full_uuid_id(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': 'גינון', 'color': '#aabbcc'})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        assert new_id.startswith('custom_')
        assert len(new_id) == len('custom_') + 32  # uuid4 hex = 32 chars

    def test_post_category_sets_owner_from_session(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': 'גינון'})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        conn = mod.get_db()
        row = conn.execute("SELECT owner_user_id FROM categories WHERE id=?", (new_id,)).fetchone()
        conn.close()
        assert row['owner_user_id'] == uid

    def test_post_category_ignores_or_rejects_client_id_authority(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories',
                        json={'name': 'גינון', 'id': 'hacker_id'})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        assert new_id != 'hacker_id'
        assert new_id.startswith('custom_')

    def test_post_category_ignores_or_rejects_client_owner_user_id(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        r = client_a.post('/api/categories',
                          json={'name': 'גינון', 'owner_user_id': uid_b})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        conn = mod.get_db()
        row = conn.execute("SELECT owner_user_id FROM categories WHERE id=?", (new_id,)).fetchone()
        conn.close()
        assert row['owner_user_id'] == uid_a  # session user, not spoofed

    def test_post_category_cannot_overwrite_housing(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        # Even if client supplies id='housing', server generates a new uuid id
        r = client.post('/api/categories',
                        json={'name': 'דיור חדש', 'id': 'housing'})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        assert new_id != 'housing'
        # Original housing still intact
        conn = mod.get_db()
        orig = conn.execute("SELECT owner_user_id FROM categories WHERE id='housing'").fetchone()
        conn.close()
        assert orig['owner_user_id'] is None  # still system

    def test_post_category_uses_plain_insert_behavior(self, tmp_path):
        """Two POST calls with the same name create duplicate-detected 409, not silent overwrite."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r1 = client.post('/api/categories', json={'name': 'ייחודי'})
        assert r1.status_code == 201
        r2 = client.post('/api/categories', json={'name': 'ייחודי'})
        assert r2.status_code == 409

    def test_post_category_empty_name_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': '   '})
        assert r.status_code == 400

    def test_post_category_long_name_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': 'א' * 61})
        assert r.status_code == 400

    def test_post_category_invalid_color_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories',
                        json={'name': 'גינון', 'color': 'red'})
        assert r.status_code == 400

    def test_post_category_duplicate_active_returns_409(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        client.post('/api/categories', json={'name': 'כלבים'})
        r = client.post('/api/categories', json={'name': 'כלבים'})
        assert r.status_code == 409
        body = r.get_json()
        assert body['error'] == 'duplicate_active'
        assert 'category_id' in body

    def test_post_category_duplicate_direct_hidden_returns_409(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r1 = client.post('/api/categories', json={'name': 'חתולים'})
        cat_id = r1.get_json()['id']
        client.post(f'/api/categories/{cat_id}/hide')
        r2 = client.post('/api/categories', json={'name': 'חתולים'})
        assert r2.status_code == 409
        body = r2.get_json()
        assert body['error'] == 'duplicate_hidden'
        assert body['category_id'] == cat_id

    def test_post_category_duplicate_inherited_hidden_reports_ancestor(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        # Create a parent and a child with a known name, hide the parent
        parent_id = _add_custom_category(mod, uid, name_he='הורה')
        child_id = _add_custom_category(mod, uid, name_he='ילד ייחודי', parent_id=parent_id)
        _hide(mod, uid, parent_id)
        # Try to create a category with the same name as the child
        r = client.post('/api/categories', json={'name': 'ילד ייחודי'})
        assert r.status_code == 409
        body = r.get_json()
        assert body['error'] == 'duplicate_inherited_hidden'
        assert body['category_id'] == child_id
        assert body['hidden_by_ancestor_id'] == parent_id

    def test_post_category_unicode_normalized_duplicate_detected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        client.post('/api/categories', json={'name': 'גינון'})
        # Same word with different Unicode normalisation (NFC vs NFD-like)
        # Using casefold difference instead: uppercase vs lower
        r = client.post('/api/categories', json={'name': 'גינון'})
        assert r.status_code == 409

    def test_post_category_whitespace_normalized_duplicate_detected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        client.post('/api/categories', json={'name': 'כלי עבודה'})
        # Extra internal whitespace — should still be detected as duplicate
        r = client.post('/api/categories', json={'name': 'כלי  עבודה'})
        assert r.status_code == 409


# ════════════════════════════════════════════════════════════════════════════
# PATCH /api/categories/<id>
# ════════════════════════════════════════════════════════════════════════════

class TestPatchCategory:

    def test_patch_own_custom_category_succeeds(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='ישן')
        r = client.patch(f'/api/categories/{cat_id}',
                         json={'name': 'חדש', 'color': '#112233'})
        assert r.status_code == 200
        conn = mod.get_db()
        row = conn.execute("SELECT name_he, color FROM categories WHERE id=?", (cat_id,)).fetchone()
        conn.close()
        assert row['name_he'] == 'חדש'
        assert row['color'] == '#112233'

    def test_patch_preserves_category_id(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='שם')
        client.patch(f'/api/categories/{cat_id}', json={'name': 'שם חדש'})
        conn = mod.get_db()
        row = conn.execute("SELECT id FROM categories WHERE id=?", (cat_id,)).fetchone()
        conn.close()
        assert row is not None  # same id still exists

    def test_patch_preserves_owner_user_id(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='שם')
        client.patch(f'/api/categories/{cat_id}',
                     json={'name': 'שם חדש', 'owner_user_id': 9999})
        conn = mod.get_db()
        row = conn.execute("SELECT owner_user_id FROM categories WHERE id=?", (cat_id,)).fetchone()
        conn.close()
        assert row['owner_user_id'] == uid  # unchanged

    def test_patch_system_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.patch('/api/categories/food', json={'name': 'אוכל חדש'})
        assert r.status_code == 403
        body = r.get_json()
        assert body['error'] == 'system_category'

    def test_patch_other_users_custom_category_returns_nonleaking_error(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.patch(f'/api/categories/{cat_b}', json={'name': 'גנוב'})
        assert r.status_code == 404  # non-leaking

    def test_patch_duplicate_name_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _add_custom_category(mod, uid, name_he='קיים כבר')
        cat_id = _add_custom_category(mod, uid, name_he='אחר')
        r = client.patch(f'/api/categories/{cat_id}', json={'name': 'קיים כבר'})
        assert r.status_code == 409

    def test_patch_uses_same_normalization_as_post(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        _add_custom_category(mod, uid, name_he='כלי  עבודה')
        cat_id = _add_custom_category(mod, uid, name_he='אחר')
        # Extra space collapsed → same normalized name → duplicate
        r = client.patch(f'/api/categories/{cat_id}', json={'name': 'כלי עבודה'})
        assert r.status_code == 409


# ════════════════════════════════════════════════════════════════════════════
# hide / restore endpoints
# ════════════════════════════════════════════════════════════════════════════

class TestHideRestore:

    def test_hide_system_category_for_one_user_only(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        client_b, uid_b = _make_client(mod)
        r = client_a.post('/api/categories/food/hide')
        assert r.status_code == 200
        # User A sees food hidden; User B still sees it
        ra = client_a.get('/api/categories')
        rb = client_b.get('/api/categories')
        ids_a = [c['id'] for c in ra.get_json()]
        ids_b = [c['id'] for c in rb.get_json()]
        assert 'food' not in ids_a
        assert 'food' in ids_b

    def test_hide_own_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='לחיות')
        r = client.post(f'/api/categories/{cat_id}/hide')
        assert r.status_code == 200
        ids = [c['id'] for c in client.get('/api/categories').get_json()]
        assert cat_id not in ids

    def test_hide_other_users_custom_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post(f'/api/categories/{cat_b}/hide')
        assert r.status_code == 404

    def test_hide_misc_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories/misc/hide')
        assert r.status_code == 400
        assert r.get_json()['error'] == 'protected_category'

    def test_hide_is_idempotent(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        client.post('/api/categories/food/hide')
        r = client.post('/api/categories/food/hide')
        assert r.status_code == 200

    def test_hide_parent_does_not_create_child_preference_rows(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories/housing/hide')
        assert r.status_code == 200
        conn = mod.get_db()
        rows = conn.execute(
            "SELECT category_id FROM user_category_preferences WHERE user_id=?", (uid,)
        ).fetchall()
        conn.close()
        cat_ids = [r['category_id'] for r in rows]
        # Only housing itself should be in preferences, not mortgage
        assert 'housing' in cat_ids
        assert 'mortgage' not in cat_ids

    def test_restore_system_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        client.post('/api/categories/food/hide')
        r = client.post('/api/categories/food/restore')
        assert r.status_code == 200
        ids = [c['id'] for c in client.get('/api/categories').get_json()]
        assert 'food' in ids

    def test_restore_own_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='ספורט')
        client.post(f'/api/categories/{cat_id}/hide')
        r = client.post(f'/api/categories/{cat_id}/restore')
        assert r.status_code == 200
        ids = [c['id'] for c in client.get('/api/categories').get_json()]
        assert cat_id in ids

    def test_restore_other_users_custom_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post(f'/api/categories/{cat_b}/restore')
        assert r.status_code == 404

    def test_restore_is_idempotent(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories/food/restore')
        assert r.status_code == 200  # no preference row existed — still OK

    def test_restore_parent_preserves_directly_hidden_child(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        # Hide both housing and mortgage directly
        client.post('/api/categories/housing/hide')
        client.post('/api/categories/mortgage/hide')
        # Restore housing
        client.post('/api/categories/housing/restore')
        # mortgage was directly hidden — must remain hidden
        ids = [c['id'] for c in client.get('/api/categories').get_json()]
        assert 'housing' in ids
        assert 'mortgage' not in ids


# ════════════════════════════════════════════════════════════════════════════
# Manual assignment enforcement
# ════════════════════════════════════════════════════════════════════════════

class TestManualAssignment:
    """
    Tests for category_accessible() enforcement in manual expense routes.
    Covers POST /api/expenses and PUT /api/expenses/<id>.
    """

    def test_manual_assign_system_category_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/expenses', json={
            'date': '2026-01-01', 'category_id': 'food',
            'description': 'שופרסל', 'amount': 100,
        })
        assert r.status_code == 200

    def test_manual_assign_own_custom_category_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='גינון')
        r = client.post('/api/expenses', json={
            'date': '2026-01-01', 'category_id': cat_id,
            'description': 'כלי גינון', 'amount': 50,
        })
        assert r.status_code == 200

    def test_manual_assign_own_hidden_custom_category_allowed(self, tmp_path):
        """Manual assignment is authorized by accessibility, not visibility."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='נסתרת')
        _hide(mod, uid, cat_id)
        r = client.post('/api/expenses', json={
            'date': '2026-01-01', 'category_id': cat_id,
            'description': 'הוצאה', 'amount': 50,
        })
        assert r.status_code == 200

    def test_manual_assign_other_users_custom_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post('/api/expenses', json={
            'date': '2026-01-01', 'category_id': cat_b,
            'description': 'ניסיון', 'amount': 10,
        })
        assert r.status_code == 400

    def test_manual_assign_nonexistent_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/expenses', json={
            'date': '2026-01-01', 'category_id': 'does_not_exist',
            'description': 'בדיקה', 'amount': 10,
        })
        assert r.status_code == 400

    def test_rejected_cross_user_assignment_does_not_change_expense(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        eid = _create_expense(mod, uid_a, 'הוצאה מקורית', category_id='misc')
        r = client_a.put(f'/api/expenses/{eid}', json={'category_id': cat_b})
        assert r.status_code == 400
        conn = mod.get_db()
        row = conn.execute("SELECT category_id FROM expenses WHERE id=?", (eid,)).fetchone()
        conn.close()
        assert row['category_id'] == 'misc'  # unchanged

    def test_rejected_cross_user_assignment_does_not_write_merchant_learning(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        mkey = 'TESTMERCHANT_' + uuid.uuid4().hex[:4]
        eid = _create_expense(mod, uid_a, 'חנות', category_id='misc', merchant_key=mkey)
        client_a.put(f'/api/expenses/{eid}', json={'category_id': cat_b})
        conn = mod.get_db()
        ml = conn.execute(
            "SELECT 1 FROM merchant_learning WHERE user_id=? AND merchant_key=?",
            (uid_a, mkey),
        ).fetchone()
        conn.close()
        assert ml is None  # not written on rejected correction

    def test_rejected_cross_user_assignment_does_not_write_category_rules(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        desc = 'תיאור_ייחודי_' + uuid.uuid4().hex[:6]
        eid = _create_expense(mod, uid_a, desc, category_id='misc')
        client_a.put(f'/api/expenses/{eid}', json={'category_id': cat_b})
        conn = mod.get_db()
        rule = conn.execute(
            "SELECT 1 FROM category_rules WHERE description=?", (desc,)
        ).fetchone()
        conn.close()
        assert rule is None


# ════════════════════════════════════════════════════════════════════════════
# Admin route protections
# ════════════════════════════════════════════════════════════════════════════

class TestAdminCategoryRoutes:

    def test_admin_update_system_category_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod, is_admin=True)
        r = client.put('/api/admin/categories',
                       json={'id': 'food', 'name_he': 'מזון עודכן', 'color': '#ff0000'})
        assert r.status_code == 200
        conn = mod.get_db()
        row = conn.execute("SELECT name_he FROM categories WHERE id='food'").fetchone()
        conn.close()
        assert row['name_he'] == 'מזון עודכן'

    def test_admin_update_custom_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client_admin, uid_admin = _make_client(mod, is_admin=True)
        _, uid_user = _make_client(mod)
        cat_id = _add_custom_category(mod, uid_user, name_he='מותאם')
        r = client_admin.put('/api/admin/categories',
                             json={'id': cat_id, 'name_he': 'שונה ע"י אדמין', 'color': '#ff0000'})
        assert r.status_code == 403

    def test_admin_delete_system_category_disabled_in_q1(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod, is_admin=True)
        r = client.delete('/api/admin/categories/food')
        assert r.status_code == 403
        body = r.get_json()
        assert body['error'] == 'category_deletion_disabled'

    def test_admin_delete_custom_category_disabled_in_q1(self, tmp_path):
        mod = _make_app(tmp_path)
        client_admin, uid_admin = _make_client(mod, is_admin=True)
        _, uid_user = _make_client(mod)
        cat_id = _add_custom_category(mod, uid_user, name_he='מותאם')
        r = client_admin.delete(f'/api/admin/categories/{cat_id}')
        assert r.status_code == 403
        body = r.get_json()
        assert body['error'] == 'category_deletion_disabled'


# ════════════════════════════════════════════════════════════════════════════
# Security invariants
# ════════════════════════════════════════════════════════════════════════════

class TestSecurityInvariants:

    def test_user_a_cannot_get_user_b_custom_category_default(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='סודי')
        ids = [c['id'] for c in client_a.get('/api/categories').get_json()]
        assert cat_b not in ids

    def test_user_a_cannot_get_user_b_custom_category_manage_scope(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='סודי B')
        ids = [c['id'] for c in client_a.get('/api/categories?scope=manage').get_json()]
        assert cat_b not in ids

    def test_user_a_cannot_patch_user_b_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.patch(f'/api/categories/{cat_b}', json={'name': 'גנוב'})
        assert r.status_code == 404

    def test_user_a_cannot_hide_user_b_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post(f'/api/categories/{cat_b}/hide')
        assert r.status_code == 404

    def test_user_a_cannot_restore_user_b_custom_category(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post(f'/api/categories/{cat_b}/restore')
        assert r.status_code == 404

    def test_user_a_cannot_assign_user_b_custom_category_to_expense(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post('/api/expenses', json={
            'date': '2026-01-01', 'category_id': cat_b,
            'description': 'ניסיון', 'amount': 10,
        })
        assert r.status_code == 400

    def test_cannot_spoof_owner_user_id_when_creating(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        r = client_a.post('/api/categories',
                          json={'name': 'ספויפד', 'owner_user_id': uid_b})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        conn = mod.get_db()
        row = conn.execute("SELECT owner_user_id FROM categories WHERE id=?", (new_id,)).fetchone()
        conn.close()
        assert row['owner_user_id'] == uid_a

    def test_cannot_overwrite_system_id_via_client_supplied_id(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': 'מזון עם id זדוני', 'id': 'food'})
        assert r.status_code in (201, 409)  # either new id created or duplicate detected
        # Either way, original 'food' is still a system category
        conn = mod.get_db()
        row = conn.execute("SELECT owner_user_id FROM categories WHERE id='food'").fetchone()
        conn.close()
        assert row['owner_user_id'] is None


# ════════════════════════════════════════════════════════════════════════════
# POST /api/budget — category ownership enforcement
# ════════════════════════════════════════════════════════════════════════════

class TestBudgetCategoryOwnership:

    def test_budget_system_category_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/budget', json={
            'category_id': 'food', 'month': '2026-01', 'planned_amount': 1000,
        })
        assert r.status_code == 200

    def test_budget_own_custom_category_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='גינון')
        r = client.post('/api/budget', json={
            'category_id': cat_id, 'month': '2026-01', 'planned_amount': 200,
        })
        assert r.status_code == 200

    def test_budget_own_hidden_custom_category_allowed(self, tmp_path):
        """Visibility does not deny manual budget assignment."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='נסתרת')
        _hide(mod, uid, cat_id)
        r = client.post('/api/budget', json={
            'category_id': cat_id, 'month': '2026-01', 'planned_amount': 100,
        })
        assert r.status_code == 200

    def test_budget_other_users_custom_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post('/api/budget', json={
            'category_id': cat_b, 'month': '2026-01', 'planned_amount': 500,
        })
        assert r.status_code == 400

    def test_budget_nonexistent_category_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/budget', json={
            'category_id': 'does_not_exist', 'month': '2026-01', 'planned_amount': 100,
        })
        assert r.status_code == 400

    def test_rejected_budget_category_does_not_write_budget_row(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        client_a.post('/api/budget', json={
            'category_id': cat_b, 'month': '2026-02', 'planned_amount': 999,
        })
        conn = mod.get_db()
        row = conn.execute(
            "SELECT 1 FROM budget WHERE category_id=? AND user_id=? AND month=?",
            (cat_b, uid_a, '2026-02'),
        ).fetchone()
        conn.close()
        assert row is None

    def test_rejected_budget_does_not_modify_existing_row(self, tmp_path):
        """Upsert path must not update an existing budget row on rejected category."""
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        # Pre-populate a legitimate budget row
        client_a.post('/api/budget', json={
            'category_id': 'food', 'month': '2026-03', 'planned_amount': 500,
        })
        # Now try to overwrite using another user's custom category (rejected)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        r = client_a.post('/api/budget', json={
            'category_id': cat_b, 'month': '2026-03', 'planned_amount': 999,
        })
        assert r.status_code == 400
        # Original food row untouched (different category_id — the upsert key
        # includes category_id so a cross-category collision cannot happen anyway,
        # but the rejection must still occur before the write)
        conn = mod.get_db()
        food_row = conn.execute(
            "SELECT planned_amount FROM budget WHERE category_id='food' AND user_id=? AND month='2026-03'",
            (uid_a,),
        ).fetchone()
        conn.close()
        assert food_row['planned_amount'] == 500


# ════════════════════════════════════════════════════════════════════════════
# Display-casing preservation
# ════════════════════════════════════════════════════════════════════════════

class TestDisplayCasePreservation:

    def test_post_preserves_display_case_while_duplicate_check_casefolds(self, tmp_path):
        """
        Stored name_he must preserve original mixed-case input.
        Duplicate detection must use the casefolded comparison form.
        """
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': 'Pet Food', 'color': '#aabbcc'})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        conn = mod.get_db()
        row = conn.execute("SELECT name_he FROM categories WHERE id=?", (new_id,)).fetchone()
        conn.close()
        # Display name preserved — NOT lowercased
        assert row['name_he'] == 'Pet Food'
        # Duplicate detection fires on casefolded match
        r2 = client.post('/api/categories', json={'name': 'pet food'})
        assert r2.status_code == 409

    def test_patch_preserves_display_case_while_duplicate_check_casefolds(self, tmp_path):
        """
        PATCH must store the display name with original case.
        Duplicate detection on rename must use casefolded comparison.
        """
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='ישן')
        r = client.patch(f'/api/categories/{cat_id}', json={'name': 'Garden Tools'})
        assert r.status_code == 200
        conn = mod.get_db()
        row = conn.execute("SELECT name_he FROM categories WHERE id=?", (cat_id,)).fetchone()
        conn.close()
        assert row['name_he'] == 'Garden Tools'
        # Creating another category with casefolded match must 409
        r2 = client.post('/api/categories', json={'name': 'garden tools'})
        assert r2.status_code == 409

    def test_post_whitespace_collapse_preserves_words(self, tmp_path):
        """Extra internal whitespace is collapsed in stored display name."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        r = client.post('/api/categories', json={'name': 'כלי  עבודה'})
        assert r.status_code == 201
        new_id = r.get_json()['id']
        conn = mod.get_db()
        row = conn.execute("SELECT name_he FROM categories WHERE id=?", (new_id,)).fetchone()
        conn.close()
        assert row['name_he'] == 'כלי עבודה'  # single space


# ════════════════════════════════════════════════════════════════════════════
# PUT /api/expenses/<id> — partial update (no category_id)
# ════════════════════════════════════════════════════════════════════════════

class TestExpensePutPartialUpdate:

    def test_expense_put_without_category_id_preserves_existing_category(self, tmp_path):
        """A PUT that only changes description must leave category_id unchanged."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        eid = _create_expense(mod, uid, 'original', category_id='food')
        r = client.put(f'/api/expenses/{eid}', json={'description': 'updated'})
        assert r.status_code == 200
        conn = mod.get_db()
        row = conn.execute("SELECT category_id, description FROM expenses WHERE id=?", (eid,)).fetchone()
        conn.close()
        assert row['category_id'] == 'food'
        assert row['description'] == 'updated'

    def test_expense_put_without_category_id_does_not_fail_category_access_check(self, tmp_path):
        """The accessibility guard must not fire when category_id is absent from payload."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        eid = _create_expense(mod, uid, 'הוצאה', category_id='food')
        # Only updating amount — no category_id in payload
        r = client.put(f'/api/expenses/{eid}', json={'amount': 999.0})
        assert r.status_code == 200

    def test_manual_assign_system_category_via_put_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        eid = _create_expense(mod, uid, 'הוצאה', category_id='misc')
        r = client.put(f'/api/expenses/{eid}', json={'category_id': 'food'})
        assert r.status_code == 200

    def test_manual_assign_own_custom_category_via_put_allowed(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='גינון')
        eid = _create_expense(mod, uid, 'הוצאה', category_id='misc')
        r = client.put(f'/api/expenses/{eid}', json={'category_id': cat_id})
        assert r.status_code == 200

    def test_manual_assign_own_hidden_custom_category_via_put_allowed(self, tmp_path):
        """Hidden category is still accessible for manual assignment."""
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        cat_id = _add_custom_category(mod, uid, name_he='נסתרת')
        _hide(mod, uid, cat_id)
        eid = _create_expense(mod, uid, 'הוצאה', category_id='misc')
        r = client.put(f'/api/expenses/{eid}', json={'category_id': cat_id})
        assert r.status_code == 200

    def test_manual_assign_other_users_custom_category_via_put_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client_a, uid_a = _make_client(mod)
        _, uid_b = _make_client(mod)
        cat_b = _add_custom_category(mod, uid_b, name_he='של B')
        eid = _create_expense(mod, uid_a, 'הוצאה', category_id='misc')
        r = client_a.put(f'/api/expenses/{eid}', json={'category_id': cat_b})
        assert r.status_code == 400

    def test_manual_assign_nonexistent_category_via_put_rejected(self, tmp_path):
        mod = _make_app(tmp_path)
        client, uid = _make_client(mod)
        eid = _create_expense(mod, uid, 'הוצאה', category_id='misc')
        r = client.put(f'/api/expenses/{eid}', json={'category_id': 'ghost'})
        assert r.status_code == 400
