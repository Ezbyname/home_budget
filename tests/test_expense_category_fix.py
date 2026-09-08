"""
Regression tests for expense/category join fix.

Root cause proven:
  POST persisted category_id='__new__' (frontend sentinel, never valid in DB)
  GET /api/expenses used INNER JOIN → row disappeared silently

Fixes:
  A. GET: INNER JOIN → LEFT JOIN  (broken category ref no longer hides expense)
  B. POST: reject '__new__' and any nonexistent category_id with 400
  C. Frontend guard (not directly testable here — covered by B)
"""
import io
import os
import sys
import uuid
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_app(tmp_path):
    env = {
        'APP_ENV': 'production',
        'SECRET_KEY': 'z' * 64,
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
        return m
    except Exception:
        for mod in list(sys.modules):
            if mod == 'app' or mod.startswith('app.'):
                del sys.modules[mod]
        raise
    finally:
        for k, orig_v in orig.items():
            if orig_v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig_v


def _make_client(tmp_path):
    mod = _load_app(tmp_path)
    mod.app.config['TESTING'] = True
    uname = 'user_' + uuid.uuid4().hex[:6]
    conn = mod.get_db()
    pw = mod.hash_password('pw')
    conn.execute(
        "INSERT INTO users (username, password_hash, email, verified, is_admin)"
        " VALUES (?,?,?,1,1)",
        (uname, pw, f'{uname}@test.com')
    )
    conn.commit()
    conn.close()
    client = mod.app.test_client()
    r = client.post('/api/auth/login', json={'username': uname, 'password': 'pw'})
    assert r.status_code == 200
    return client, mod


def _first_category_id(mod):
    """Return the id of the first seeded category, or None."""
    conn = mod.get_db()
    row = conn.execute("SELECT id FROM categories ORDER BY sort_order LIMIT 1").fetchone()
    conn.close()
    return row['id'] if row else None


# ---------------------------------------------------------------------------
# 1. Fresh cloud DB has seeded categories
# ---------------------------------------------------------------------------

class TestFreshCloudCategories:

    def test_init_db_seeds_categories(self, tmp_path):
        """A fresh cloud DB must contain at least one category after init_db()."""
        mod = _load_app(tmp_path)
        conn = mod.get_db()
        count = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        conn.close()
        assert count > 0, \
            f"Fresh cloud DB has 0 categories — init_db() seeding is broken"

    def test_default_categories_have_required_columns(self, tmp_path):
        mod = _load_app(tmp_path)
        conn = mod.get_db()
        row = conn.execute("SELECT id, name_he, color FROM categories LIMIT 1").fetchone()
        conn.close()
        assert row is not None
        assert row['name_he'], "category name_he is empty"
        assert row['color'], "category color is empty"


# ---------------------------------------------------------------------------
# 2. POST /api/expenses validation
# ---------------------------------------------------------------------------

class TestPostExpenseValidation:

    def test_valid_category_id_persists(self, tmp_path):
        client, mod = _make_client(tmp_path)
        cat_id = _first_category_id(mod)
        assert cat_id, "No seeded categories — cannot run test"
        r = client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': cat_id,
            'amount': 50.0, 'description': 'valid test'
        })
        assert r.status_code == 200
        assert r.get_json().get('status') == 'ok'
        conn = mod.get_db()
        count = conn.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
        conn.close()
        assert count == 1, "Expense was not inserted"

    def test_sentinel_new_rejected_with_400(self, tmp_path):
        client, mod = _make_client(tmp_path)
        r = client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': '__new__',
            'amount': 50.0, 'description': 'sentinel test'
        })
        assert r.status_code == 400, f"Expected 400, got {r.status_code}"
        assert 'error' in r.get_json()

    def test_sentinel_new_produces_no_db_row(self, tmp_path):
        client, mod = _make_client(tmp_path)
        client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': '__new__',
            'amount': 99.0, 'description': 'should not persist'
        })
        conn = mod.get_db()
        count = conn.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
        conn.close()
        assert count == 0, f"Row was inserted despite '__new__' sentinel — count={count}"

    def test_nonexistent_category_id_rejected(self, tmp_path):
        client, mod = _make_client(tmp_path)
        r = client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': 'definitely_not_a_real_cat_999',
            'amount': 10.0, 'description': 'invalid cat'
        })
        assert r.status_code == 400
        assert 'error' in r.get_json()

    def test_nonexistent_category_produces_no_db_row(self, tmp_path):
        client, mod = _make_client(tmp_path)
        client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': 'fake_cat',
            'amount': 5.0, 'description': 'should not persist'
        })
        conn = mod.get_db()
        count = conn.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
        conn.close()
        assert count == 0


# ---------------------------------------------------------------------------
# 3. GET /api/expenses — LEFT JOIN behavior
# ---------------------------------------------------------------------------

class TestGetExpensesLeftJoin:

    def _insert_expense_raw(self, mod, category_id):
        """Bypass validation to inject a row with an arbitrary category_id."""
        conn = mod.get_db()
        uid = conn.execute("SELECT id FROM users ORDER BY id LIMIT 1").fetchone()['id']
        conn.execute(
            "INSERT INTO expenses (date, category_id, description, amount, source, frequency, user_id)"
            " VALUES ('2026-09-05', ?, 'raw insert', 1.23, 'manual', 'random', ?)",
            (category_id, uid)
        )
        conn.commit()
        conn.close()

    def test_expense_with_broken_category_ref_returned_by_get(self, tmp_path):
        """The existing Railway smoke-test row: category_id='__new__' must now appear."""
        client, mod = _make_client(tmp_path)
        self._insert_expense_raw(mod, '__new__')

        r = client.get('/api/expenses')
        assert r.status_code == 200
        rows = r.get_json()
        assert len(rows) == 1, \
            f"Expected 1 row (LEFT JOIN preserves broken ref), got {len(rows)}"
        assert rows[0]['category_name'] is None or rows[0].get('category_name') in (None, ''), \
            "category_name should be null for unresolved category"

    def test_expense_with_broken_category_not_silently_dropped(self, tmp_path):
        """Regression: INNER JOIN previously returned [] for this case."""
        client, mod = _make_client(tmp_path)
        self._insert_expense_raw(mod, 'nonexistent_category_xyz')

        r = client.get('/api/expenses')
        assert r.status_code == 200
        rows = r.get_json()
        assert len(rows) == 1, \
            f"Expense with broken category ref must not disappear from GET, got {len(rows)}"

    def test_normal_expense_still_returned(self, tmp_path):
        """Normal path: valid category_id expense appears with category_name."""
        client, mod = _make_client(tmp_path)
        cat_id = _first_category_id(mod)
        assert cat_id
        client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': cat_id,
            'amount': 42.0, 'description': 'normal expense'
        })
        r = client.get('/api/expenses')
        assert r.status_code == 200
        rows = r.get_json()
        assert len(rows) == 1
        assert rows[0]['category_name'] is not None
        assert rows[0]['amount'] == 42.0

    def test_mixed_valid_and_broken_refs_both_returned(self, tmp_path):
        """Both a valid and a broken-ref expense appear in GET results."""
        client, mod = _make_client(tmp_path)
        cat_id = _first_category_id(mod)
        assert cat_id
        # Valid expense via POST
        client.post('/api/expenses', json={
            'date': '2026-09-07', 'category_id': cat_id,
            'amount': 10.0, 'description': 'valid'
        })
        # Broken-ref expense via raw insert
        self._insert_expense_raw(mod, '__new__')

        r = client.get('/api/expenses')
        assert r.status_code == 200
        rows = r.get_json()
        assert len(rows) == 2, f"Expected 2 rows, got {len(rows)}"
