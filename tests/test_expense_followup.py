"""
Expense Followup V1 — backend tests.

Branch: feature/expense-followup-v1
Base:   main @ 7304371

Covers: GET /api/followup
  - authentication
  - empty state
  - recurring_candidate detection and user isolation
  - unresolved_category detection, strict P5-only, 90-day window,
    is_unusual independence, display cap vs true count, user isolation
  - mutation resolution (recurring + unresolved)
"""
import os
import sys
import json
import datetime
import pytest


# ---------------------------------------------------------------------------
# Infrastructure helpers (mirrors test_phase0b.py pattern)
# ---------------------------------------------------------------------------

def _make_app(tmp_path):
    env = {
        'APP_ENV': None,
        'SECRET_KEY': None,
        'RAILWAY_VOLUME_MOUNT_PATH': None,
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
        import app as flask_app
        return flask_app.app
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


@pytest.fixture
def app(tmp_path):
    flask_app = _make_app(tmp_path)
    flask_app.config['TESTING'] = True
    return flask_app


def _create_user(username, email, password='pass123'):
    import app as mod
    conn = mod.get_db()
    pw_hash = mod.hash_password(password)
    conn.execute(
        "INSERT OR REPLACE INTO users (username, password_hash, email, verified, is_admin) "
        "VALUES (?, ?, ?, 1, 0)",
        (username, pw_hash, email),
    )
    conn.commit()
    uid = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()[0]
    conn.close()
    return uid


def _login(client, username, password='pass123'):
    r = client.post('/api/auth/login', json={'username': username, 'password': password})
    assert r.status_code == 200, f"login failed: {r.data}"


def _insert_expense(conn, user_id, description, amount, date, source='bank_csv',
                    frequency='random', category_source='legacy', category_id='misc',
                    merchant_key='', is_unusual=0):
    conn.execute(
        "INSERT INTO expenses "
        "(user_id, description, amount, date, source, frequency, category_id, "
        " category_source, merchant_key, is_unusual) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (user_id, description, amount, date, source, frequency,
         category_id, category_source, merchant_key, is_unusual),
    )
    conn.commit()


def _today_minus(days):
    return (datetime.date.today() - datetime.timedelta(days=days)).isoformat()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestFollowupAuthentication:

    def test_followup_requires_authentication(self, app):
        with app.test_client() as c:
            r = c.get('/api/followup')
        assert r.status_code == 401


class TestFollowupEmptyState:

    def test_followup_empty_when_no_signals(self, app):
        with app.test_client() as c:
            _create_user('empty_user', 'empty@test.com')
            _login(c, 'empty_user')
            r = c.get('/api/followup')
        assert r.status_code == 200
        data = json.loads(r.data)
        assert data['items'] == []
        assert data['counts']['recurring_candidate'] == 0
        assert data['counts']['unresolved_category'] == 0
        assert data['counts']['total'] == 0


class TestFollowupRecurring:

    def test_followup_returns_recurring_candidate(self, app):
        """Two months of same description with low variance → one recurring group."""
        with app.test_client() as c:
            uid = _create_user('rec_user', 'rec@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'חשמל', 100.0, '2026-07-15')
            _insert_expense(conn, uid, 'חשמל', 102.0, '2026-08-15')
            conn.close()
            _login(c, 'rec_user')
            r = c.get('/api/followup')
        assert r.status_code == 200
        data = json.loads(r.data)
        recurring = [i for i in data['items'] if i['type'] == 'recurring_candidate']
        assert len(recurring) == 1
        assert recurring[0]['description'] == 'חשמל'
        assert data['counts']['recurring_candidate'] == 1

    def test_followup_single_month_does_not_qualify(self, app):
        """Expense in only one month must not become a recurring candidate."""
        with app.test_client() as c:
            uid = _create_user('single_month', 'single@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'ארנונה', 500.0, '2026-08-01')
            conn.close()
            _login(c, 'single_month')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        recurring = [i for i in data['items'] if i['type'] == 'recurring_candidate']
        assert len(recurring) == 0

    def test_followup_high_variance_does_not_qualify(self, app):
        """Amount variance > 15 % of average must not qualify."""
        with app.test_client() as c:
            uid = _create_user('high_var', 'highvar@test.com')
            import app as mod
            conn = mod.get_db()
            # avg=150, spread=100 → spread/avg ≈ 66 % → exceeds 15 %
            _insert_expense(conn, uid, 'שונות גבוה', 100.0, '2026-07-10')
            _insert_expense(conn, uid, 'שונות גבוה', 200.0, '2026-08-10')
            conn.close()
            _login(c, 'high_var')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        recurring = [i for i in data['items'] if i['type'] == 'recurring_candidate']
        assert len(recurring) == 0

    def test_followup_recurring_key_derives_from_description(self, app):
        with app.test_client() as c:
            uid = _create_user('key_user', 'key@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'מנוי נטפליקס', 50.0, '2026-07-01')
            _insert_expense(conn, uid, 'מנוי נטפליקס', 50.0, '2026-08-01')
            conn.close()
            _login(c, 'key_user')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        item = next(i for i in data['items'] if i['type'] == 'recurring_candidate')
        assert 'מנוי נטפליקס' in item['key']
        assert 'merchant_key' not in item['key']

    def test_followup_recurring_user_isolation(self, app):
        """Recurring candidates of user A must not appear in user B response."""
        with app.test_client() as c:
            uid_a = _create_user('rec_a', 'reca@test.com')
            _create_user('rec_b', 'recb@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid_a, 'ביטוח', 200.0, '2026-07-05')
            _insert_expense(conn, uid_a, 'ביטוח', 200.0, '2026-08-05')
            conn.close()
            _login(c, 'rec_b')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['recurring_candidate'] == 0


class TestFollowupUnresolvedCategory:

    def test_followup_returns_recent_unresolved_category(self, app):
        with app.test_client() as c:
            uid = _create_user('unres_user', 'unres@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'קנייה לא מזוהה', 75.0, _today_minus(5),
                            category_source='unresolved')
            conn.close()
            _login(c, 'unres_user')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        unres = [i for i in data['items'] if i['type'] == 'unresolved_category']
        assert len(unres) == 1
        assert data['counts']['unresolved_category'] == 1

    def test_followup_strict_p5_excludes_user_source(self, app):
        """category_source='user' with category_id='misc' must NOT appear."""
        with app.test_client() as c:
            uid = _create_user('user_src', 'usersrc@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'קנייה ידנית', 50.0, _today_minus(3),
                            category_source='user', category_id='misc')
            conn.close()
            _login(c, 'user_src')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 0

    def test_followup_strict_p5_excludes_legacy_source(self, app):
        """category_source='legacy' with category_id='misc' must NOT appear."""
        with app.test_client() as c:
            uid = _create_user('leg_src', 'legsrc@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'ישן', 30.0, _today_minus(10),
                            category_source='legacy', category_id='misc')
            conn.close()
            _login(c, 'leg_src')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 0

    def test_followup_strict_p5_excludes_null_source(self, app):
        """category_source=NULL must NOT appear."""
        with app.test_client() as c:
            uid = _create_user('null_src', 'nullsrc@test.com')
            import app as mod
            conn = mod.get_db()
            conn.execute(
                "INSERT INTO expenses "
                "(user_id, description, amount, date, source, frequency, category_id, "
                " category_source, merchant_key) "
                "VALUES (?, ?, ?, ?, 'bank_csv', 'random', 'misc', NULL, '')",
                (uid, 'ללא מקור', 20.0, _today_minus(2)),
            )
            conn.commit()
            conn.close()
            _login(c, 'null_src')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 0

    def test_followup_90_day_window_includes_within(self, app):
        with app.test_client() as c:
            uid = _create_user('win_in', 'winin@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'בתוך חלון', 10.0, _today_minus(89),
                            category_source='unresolved')
            conn.close()
            _login(c, 'win_in')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 1

    def test_followup_90_day_window_excludes_older(self, app):
        with app.test_client() as c:
            uid = _create_user('win_out', 'winout@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'מחוץ לחלון', 10.0, _today_minus(91),
                            category_source='unresolved')
            conn.close()
            _login(c, 'win_out')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 0

    def test_followup_unusual_independence(self, app):
        """is_unusual=1 unresolved expense must still appear (orthogonal dimensions)."""
        with app.test_client() as c:
            uid = _create_user('unusual_u', 'unusual@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'הוצאה חריגה לא מזוהה', 9999.0,
                            _today_minus(5), category_source='unresolved', is_unusual=1)
            conn.close()
            _login(c, 'unusual_u')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 1

    def test_followup_display_cap_vs_true_count(self, app):
        """25 qualifying rows → items capped at 20 but count reflects all 25."""
        with app.test_client() as c:
            uid = _create_user('cap_user', 'cap@test.com')
            import app as mod
            conn = mod.get_db()
            for i in range(25):
                _insert_expense(conn, uid, f'הוצאה {i}', float(10 + i),
                                _today_minus(i % 89 + 1), category_source='unresolved')
            conn.close()
            _login(c, 'cap_user')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        unres_items = [i for i in data['items'] if i['type'] == 'unresolved_category']
        assert len(unres_items) == 20
        assert data['counts']['unresolved_category'] == 25
        assert data['counts']['total'] >= 25

    def test_followup_unresolved_user_isolation(self, app):
        """Unresolved expenses of user A must not appear in user B response."""
        with app.test_client() as c:
            uid_a = _create_user('iso_a', 'isoa@test.com')
            _create_user('iso_b', 'isob@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid_a, 'של א', 50.0, _today_minus(3),
                            category_source='unresolved')
            conn.close()
            _login(c, 'iso_b')
            r = c.get('/api/followup')
        data = json.loads(r.data)
        assert data['counts']['unresolved_category'] == 0


class TestFollowupMutationResolution:

    def test_recurring_resolves_after_set_recurring(self, app):
        """POST set-recurring → recurring candidate disappears from followup."""
        with app.test_client() as c:
            uid = _create_user('res_rec', 'resrec@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'חשמל חוזר', 100.0, '2026-07-10')
            _insert_expense(conn, uid, 'חשמל חוזר', 101.0, '2026-08-10')
            conn.close()
            _login(c, 'res_rec')
            # Confirm candidate present
            r = c.get('/api/followup')
            data = json.loads(r.data)
            assert data['counts']['recurring_candidate'] == 1
            # Resolve via mutation
            r2 = c.post('/api/expenses/set-recurring',
                        json={'description': 'חשמל חוזר', 'frequency': 'monthly'})
            assert r2.status_code == 200
            # Candidate must be gone: expenses now have frequency='monthly', not 'random'
            r3 = c.get('/api/followup')
            data3 = json.loads(r3.data)
            assert data3['counts']['recurring_candidate'] == 0

    def test_recurring_resolves_after_mark_once(self, app):
        """POST set-recurring with frequency='once' → candidate absent from followup.

        Regression for the blocker where 'random' was sent instead of 'once':
        detect_recurring() queries frequency='random', so writing 'random' leaves
        the candidate in the pool and the card reappears immediately.
        Writing 'once' removes it from the detection pool correctly.
        """
        with app.test_client() as c:
            uid = _create_user('res_once', 'resonce@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'ביטוח חד פעמי', 200.0, '2026-07-20')
            _insert_expense(conn, uid, 'ביטוח חד פעמי', 200.0, '2026-08-20')
            conn.close()
            _login(c, 'res_once')
            # Confirm candidate present before action
            r = c.get('/api/followup')
            data = json.loads(r.data)
            assert data['counts']['recurring_candidate'] == 1
            # Mark as חד פעמי → must write 'once', not 'random'
            r2 = c.post('/api/expenses/set-recurring',
                        json={'description': 'ביטוח חד פעמי', 'frequency': 'once'})
            assert r2.status_code == 200
            # Candidate must be gone: 'once' is excluded from detection predicate
            r3 = c.get('/api/followup')
            data3 = json.loads(r3.data)
            assert data3['counts']['recurring_candidate'] == 0, (
                "Writing frequency='once' must remove candidate; "
                "if 'random' were sent instead, the card would reappear"
            )

    def test_unresolved_resolves_after_category_update(self, app):
        """PUT /api/expenses/<id> with category → unresolved item disappears."""
        with app.test_client() as c:
            uid = _create_user('res_unres', 'resunres@test.com')
            import app as mod
            conn = mod.get_db()
            _insert_expense(conn, uid, 'לא מזוהה', 55.0, _today_minus(5),
                            category_source='unresolved')
            eid = conn.execute(
                "SELECT id FROM expenses WHERE user_id=? AND description='לא מזוהה'",
                (uid,)
            ).fetchone()[0]
            conn.close()
            _login(c, 'res_unres')
            r = c.get('/api/followup')
            assert json.loads(r.data)['counts']['unresolved_category'] == 1
            # Resolve via category update
            r2 = c.put(f'/api/expenses/{eid}',
                       json={'category_id': 'food'})
            assert r2.status_code == 200
            r3 = c.get('/api/followup')
            assert json.loads(r3.data)['counts']['unresolved_category'] == 0
