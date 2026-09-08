"""
Phase 0b targeted tests: environment modes, session security, SMTP/AI auth, OTP, SQLite concurrency.
All tests run without network access and without touching production data.
"""
import os
import sys
import json
import sqlite3
import tempfile
import threading
import importlib
import types
import pytest

# ---------------------------------------------------------------------------
# Helpers: isolate app imports from real filesystem side-effects
# ---------------------------------------------------------------------------

def _make_app(env_vars: dict, frozen: bool = False):
    """
    Import app in a controlled environment with specified env vars.
    Returns the Flask app object.
    Raises RuntimeError/EnvironmentError if startup validation fails (expected for some tests).
    """
    # Stash originals
    orig_env = {k: os.environ.get(k) for k in env_vars}
    orig_frozen = None

    # Patch env
    for k, v in env_vars.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    # Remove any cached module so re-import runs fresh startup code
    for mod in list(sys.modules.keys()):
        if mod == 'app' or mod.startswith('app.'):
            del sys.modules[mod]

    try:
        if frozen:
            # Simulate frozen exe: patch sys.frozen before import
            sys.frozen = True
        import app as flask_app
        return flask_app.app
    except Exception:
        # On failure clean up the module so the next test gets a fresh import
        for mod in list(sys.modules.keys()):
            if mod == 'app' or mod.startswith('app.'):
                del sys.modules[mod]
        raise
    finally:
        # Restore env vars (but keep the module in sys.modules so helpers that do
        # `import app as mod` get the same DATA_DIR / DB_PATH as the returned app)
        for k, orig in orig_env.items():
            if orig is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig
        if frozen and hasattr(sys, 'frozen'):
            del sys.frozen


# ---------------------------------------------------------------------------
# 1. Environment / startup validation
# ---------------------------------------------------------------------------

class TestEnvironmentModes:

    def test_local_dev_starts_without_cloud_vars(self, tmp_path):
        """Local development: python app.py works without APP_ENV or SECRET_KEY."""
        env = {
            'APP_ENV': None,
            'SECRET_KEY': None,
            'RAILWAY_VOLUME_MOUNT_PATH': None,
        }
        # Should not raise
        flask_app = _make_app(env)
        assert flask_app is not None

    def test_cloud_missing_volume_raises(self, tmp_path):
        """Cloud mode: missing RAILWAY_VOLUME_MOUNT_PATH → RuntimeError at startup."""
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'a' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': None,
        }
        with pytest.raises(RuntimeError, match="RAILWAY_VOLUME_MOUNT_PATH is not set"):
            _make_app(env)

    def test_cloud_nonexistent_volume_path_raises(self, tmp_path):
        """Cloud mode: RAILWAY_VOLUME_MOUNT_PATH pointing to nonexistent dir → RuntimeError."""
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'a' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': '/nonexistent/path/that/cannot/exist/xyz123',
        }
        with pytest.raises(RuntimeError, match="does not exist or is not a directory"):
            _make_app(env)

    def test_cloud_missing_secret_key_raises(self, tmp_path):
        """Cloud mode: missing SECRET_KEY → RuntimeError at startup."""
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': None,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
        }
        with pytest.raises(RuntimeError, match="SECRET_KEY environment variable must be set"):
            _make_app(env)

    def test_cloud_valid_config_starts(self, tmp_path):
        """Cloud mode: valid RAILWAY_VOLUME_MOUNT_PATH + SECRET_KEY → starts successfully."""
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'b' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
        }
        flask_app = _make_app(env)
        assert flask_app is not None
        assert flask_app.config['SESSION_COOKIE_SECURE'] is True
        assert flask_app.config['SESSION_COOKIE_HTTPONLY'] is True
        assert flask_app.config['SESSION_COOKIE_SAMESITE'] == 'Lax'
        assert flask_app.config['SESSION_COOKIE_NAME'] == 'budget_session'

    def test_local_dev_no_secure_cookies(self):
        """Local dev: secure cookie flags must NOT be set (HTTP localhost would break)."""
        env = {'APP_ENV': None, 'SECRET_KEY': None, 'RAILWAY_VOLUME_MOUNT_PATH': None}
        flask_app = _make_app(env)
        assert flask_app.config.get('SESSION_COOKIE_SECURE') is not True

    def test_cloud_secret_key_is_stable(self, tmp_path):
        """Cloud: two imports with same SECRET_KEY produce same secret_key value."""
        key = 'c' * 64
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': key,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
        }
        app1 = _make_app(env)
        sk1 = app1.secret_key
        app2 = _make_app(env)
        sk2 = app2.secret_key
        assert sk1 == sk2 == key


# ---------------------------------------------------------------------------
# 2. Session security via Flask test client
# ---------------------------------------------------------------------------

@pytest.fixture
def cloud_app(tmp_path):
    env = {
        'APP_ENV': 'production',
        'SECRET_KEY': 'd' * 64,
        'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
    }
    flask_app = _make_app(env)
    flask_app.config['TESTING'] = True
    return flask_app


@pytest.fixture
def local_app(tmp_path):
    env = {'APP_ENV': None, 'SECRET_KEY': None, 'RAILWAY_VOLUME_MOUNT_PATH': None}
    flask_app = _make_app(env)
    flask_app.config['TESTING'] = True
    return flask_app


def _create_admin_user(flask_app):
    """Insert a test admin user into the app's database. Returns (username, password)."""
    import app as mod
    conn = mod.get_db()
    import hashlib, secrets as _secrets
    pw = 'testpass1'
    pw_hash = mod.hash_password(pw)
    conn.execute(
        "INSERT OR REPLACE INTO users (username, password_hash, email, verified, is_admin) "
        "VALUES (?, ?, ?, 1, 1)",
        ('testadmin', pw_hash, 'admin@test.com')
    )
    conn.commit()
    conn.close()
    return 'testadmin', pw


def _create_normal_user(flask_app):
    import app as mod
    conn = mod.get_db()
    pw = 'testpass2'
    pw_hash = mod.hash_password(pw)
    conn.execute(
        "INSERT OR REPLACE INTO users (username, password_hash, email, verified, is_admin) "
        "VALUES (?, ?, ?, 1, 0)",
        ('testuser', pw_hash, 'user@test.com')
    )
    conn.commit()
    conn.close()
    return 'testuser', pw


class TestSMTPAuthorization:
    """SMTP config endpoints must enforce login + admin."""

    def test_smtp_get_unauthenticated_returns_401(self, cloud_app):
        with cloud_app.test_client() as c:
            r = c.get('/api/auth/smtp-config')
            assert r.status_code == 401

    def test_smtp_post_unauthenticated_returns_401(self, cloud_app):
        with cloud_app.test_client() as c:
            r = c.post('/api/auth/smtp-config',
                       json={'smtp_server': 'evil.com'})
            assert r.status_code == 401

    def test_smtp_get_non_admin_returns_403(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_normal_user(cloud_app)
            r = c.post('/api/auth/login', json={'username': username, 'password': pw})
            assert r.status_code == 200
            r2 = c.get('/api/auth/smtp-config')
            assert r2.status_code == 403

    def test_smtp_post_non_admin_returns_403(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_normal_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.post('/api/auth/smtp-config', json={'smtp_server': 'evil.com'})
            assert r.status_code == 403

    def test_smtp_get_admin_returns_200(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_admin_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.get('/api/auth/smtp-config')
            assert r.status_code == 200

    def test_smtp_post_admin_returns_200(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_admin_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.post('/api/auth/smtp-config',
                       json={'smtp_server': 'smtp.gmail.com', 'smtp_port': 587,
                             'smtp_user': '', 'smtp_password': '', 'from_email': '',
                             'twilio_sid': '', 'twilio_token': '', 'twilio_from': ''})
            assert r.status_code == 200

    def test_smtp_post_admin_writes_config_file(self, cloud_app, tmp_path):
        """Admin POST persists dummy values to isolated SMTP_CONFIG_PATH under tmp_path."""
        import app as mod
        smtp_config_path = mod.SMTP_CONFIG_PATH

        # 5. Must be under the isolated tmp_path, never under /data or real dirs
        assert str(smtp_config_path).startswith(str(tmp_path)), (
            f"SMTP_CONFIG_PATH {smtp_config_path!r} is not under tmp_path {tmp_path!r}"
        )

        dummy = {
            'smtp_server': 'smtp.test.invalid',
            'smtp_port': 587,
            'smtp_user': 'phase0b-test-user',
            'smtp_password': 'phase0b-test-password',
            'from_email': 'phase0b@example.invalid',
            'twilio_sid': '',
            'twilio_token': '',
            'twilio_from': '',
        }

        with cloud_app.test_client() as c:
            # 1. Authenticate as admin
            username, pw = _create_admin_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})

            # 2+3. POST dummy SMTP data; assert HTTP 200
            r = c.post('/api/auth/smtp-config', json=dummy)
            assert r.status_code == 200

        # 4. File must exist after the POST
        assert os.path.exists(smtp_config_path), (
            f"SMTP_CONFIG_PATH {smtp_config_path!r} was not created after admin POST"
        )

        # 6. Read and assert expected dummy values persisted
        with open(smtp_config_path, 'r') as f:
            written = json.load(f)
        assert written.get('smtp_server') == 'smtp.test.invalid'
        assert written.get('smtp_port') == 587
        assert written.get('smtp_user') == 'phase0b-test-user'
        assert written.get('smtp_password') == 'phase0b-test-password'
        assert written.get('from_email') == 'phase0b@example.invalid'

    def test_smtp_post_non_admin_does_not_create_config(self, cloud_app):
        """Non-admin POST returns 403 and must not create or modify SMTP_CONFIG_PATH."""
        import app as mod
        smtp_config_path = mod.SMTP_CONFIG_PATH
        existed_before = os.path.exists(smtp_config_path)

        with cloud_app.test_client() as c:
            username, pw = _create_normal_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.post('/api/auth/smtp-config', json={'smtp_server': 'evil.com'})
            assert r.status_code == 403

        # File must not have been created by the rejected request
        if not existed_before:
            assert not os.path.exists(smtp_config_path), (
                "SMTP_CONFIG_PATH was created despite 403 rejection"
            )


# ---------------------------------------------------------------------------
# 3. AI settings authorization
# ---------------------------------------------------------------------------

class TestAISettingsAuthorization:

    def test_ai_get_requires_login(self, cloud_app):
        with cloud_app.test_client() as c:
            r = c.get('/api/settings/ai')
            assert r.status_code == 401

    def test_ai_post_non_admin_returns_403(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_normal_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.post('/api/settings/ai', json={'api_key': 'sk-ant-evil'})
            assert r.status_code == 403

    def test_ai_post_admin_succeeds(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_admin_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.post('/api/settings/ai', json={'api_key': 'sk-ant-test-key'})
            assert r.status_code == 200

    def test_ai_key_written_to_config_file(self, cloud_app):
        """Verify admin POST actually persists the key to AI_CONFIG_PATH."""
        import app as mod
        with cloud_app.test_client() as c:
            username, pw = _create_admin_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            c.post('/api/settings/ai', json={'api_key': 'sk-ant-persistent'})
        assert mod._get_ai_key() == 'sk-ant-persistent'

    def test_ai_get_logged_in_non_admin_sees_status(self, cloud_app):
        """Non-admin can still read whether AI is configured (no key revealed)."""
        with cloud_app.test_client() as c:
            username, pw = _create_normal_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            r = c.get('/api/settings/ai')
            assert r.status_code == 200
            data = r.get_json()
            assert 'has_key' in data
            # Key itself must not be exposed
            assert 'sk-ant' not in json.dumps(data)


# ---------------------------------------------------------------------------
# 4. OTP fail-closed on cloud
# ---------------------------------------------------------------------------

class TestOTPBehavior:

    def test_cloud_otp_failure_returns_503(self, cloud_app):
        """Cloud: when OTP send fails, signup returns 503 and user is not created."""
        import app as mod
        import uuid
        uname = f'otp_cloud_{uuid.uuid4().hex[:8]}'
        # Patch send functions to simulate delivery failure
        orig_email = mod.send_email_otp
        orig_sms = mod.send_sms_otp
        mod.send_email_otp = lambda *a, **k: (False, 'SMTP not configured')
        mod.send_sms_otp = lambda *a, **k: (False, 'SMS not configured')
        try:
            with cloud_app.test_client() as c:
                r = c.post('/api/auth/signup', json={
                    'username': uname,
                    'password': 'password123',
                    'email': 'test@example.com',
                    'verification_method': 'email',
                })
                assert r.status_code == 503
                # User must not exist in DB
                conn = mod.get_db()
                row = conn.execute(
                    "SELECT id FROM users WHERE username=?", (uname,)
                ).fetchone()
                conn.close()
                assert row is None, "User was created despite cloud OTP failure"
        finally:
            mod.send_email_otp = orig_email
            mod.send_sms_otp = orig_sms

    def test_local_dev_otp_failure_auto_verifies(self, local_app):
        """Local dev: when OTP send fails, user is auto-verified (offline mode preserved)."""
        import app as mod
        import uuid
        uname = f'otp_local_{uuid.uuid4().hex[:8]}'
        orig_email = mod.send_email_otp
        orig_sms = mod.send_sms_otp
        mod.send_email_otp = lambda *a, **k: (False, 'no SMTP')
        mod.send_sms_otp = lambda *a, **k: (False, 'no SMS')
        try:
            with local_app.test_client() as c:
                r = c.post('/api/auth/signup', json={
                    'username': uname,
                    'password': 'password123',
                    'email': 'local@example.com',
                    'verification_method': 'email',
                })
                # Should succeed with auto_verified
                data = r.get_json()
                assert r.status_code == 200
                assert data.get('auto_verified') is True
                # User must be verified in DB
                conn = mod.get_db()
                row = conn.execute(
                    "SELECT verified FROM users WHERE username=?", (uname,)
                ).fetchone()
                conn.close()
                assert row is not None
                assert row['verified'] == 1
        finally:
            mod.send_email_otp = orig_email
            mod.send_sms_otp = orig_sms


# ---------------------------------------------------------------------------
# 5. SQLite busy_timeout
# ---------------------------------------------------------------------------

class TestSQLiteBusyTimeout:

    def test_busy_timeout_pragma_set(self, local_app):
        """PRAGMA busy_timeout=5000 must be set on every connection."""
        import app as mod
        conn = mod.get_db()
        result = conn.execute("PRAGMA busy_timeout").fetchone()[0]
        conn.close()
        assert result == 5000

    def test_concurrent_writes_no_operational_error(self, local_app, tmp_path):
        """Two concurrent writers complete without OperationalError."""
        import app as mod
        import uuid
        run_id = uuid.uuid4().hex  # unique per test run to avoid cross-test pollution
        errors = []

        def write_expense(tag):
            try:
                conn = mod.get_db()
                conn.execute(
                    "INSERT INTO expenses (date, description, amount, category_id, user_id, source, frequency) "
                    "VALUES (date('now'), ?, 1.0, 'food', 1, 'bank', 'random')",
                    (f'concurrent-{run_id}-{tag}',)
                )
                conn.commit()
                conn.close()
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=write_expense, args=(i,)) for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent write errors: {errors}"

        # Verify all 5 rows written (no lost write)
        conn = mod.get_db()
        count = conn.execute(
            "SELECT COUNT(*) FROM expenses WHERE description LIKE ?",
            (f'concurrent-{run_id}-%',)
        ).fetchone()[0]
        conn.close()
        assert count == 5, f"Expected 5 concurrent writes, got {count}"


# ---------------------------------------------------------------------------
# 6. Login / session basics
# ---------------------------------------------------------------------------

class TestSessionBehavior:

    def test_login_creates_session(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_admin_user(cloud_app)
            r = c.post('/api/auth/login', json={'username': username, 'password': pw})
            assert r.status_code == 200
            # Session cookie must be present (Flask 3.x API)
            assert c.get_cookie('budget_session') is not None

    def test_logout_clears_session(self, cloud_app):
        with cloud_app.test_client() as c:
            username, pw = _create_admin_user(cloud_app)
            c.post('/api/auth/login', json={'username': username, 'password': pw})
            # Protected route works
            r1 = c.get('/api/auth/status')
            assert r1.get_json()['logged_in'] is True
            # Logout
            c.post('/api/auth/logout')
            r2 = c.get('/api/auth/status')
            assert r2.get_json()['logged_in'] is False

    def test_protected_route_requires_auth(self, cloud_app):
        with cloud_app.test_client() as c:
            r = c.get('/api/expenses')
            assert r.status_code == 401
