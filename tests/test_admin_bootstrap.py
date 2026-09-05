"""
Targeted tests for admin-bootstrap behavior:
  Cloud mode  → ADMIN_SECRETS from env vars only
  Local mode  → ADMIN_SECRETS from secrets file only; env vars have no effect
"""
import os
import sys
import json
import pytest


def _make_app(env_vars: dict):
    """Import app fresh with given env vars. Returns the Flask app object."""
    orig_env = {k: os.environ.get(k) for k in env_vars}
    for k, v in env_vars.items():
        if v is None:
            os.environ.pop(k, None)
        else:
            os.environ[k] = v

    for mod in list(sys.modules.keys()):
        if mod == 'app' or mod.startswith('app.'):
            del sys.modules[mod]

    try:
        import app as flask_app
        return flask_app.app
    except Exception:
        for mod in list(sys.modules.keys()):
            if mod == 'app' or mod.startswith('app.'):
                del sys.modules[mod]
        raise
    finally:
        for k, orig in orig_env.items():
            if orig is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = orig


def _get_admin_user(flask_app, username='admin'):
    import app as mod
    conn = mod.get_db()
    row = conn.execute(
        "SELECT username, is_admin FROM users WHERE username=?", (username,)
    ).fetchone()
    conn.close()
    return row


# ---------------------------------------------------------------------------
# 1. Cloud + ADMIN_EMAIL + ADMIN_PASSWORD → admin user created, is_admin=1
# ---------------------------------------------------------------------------

class TestCloudAdminBootstrap:

    def test_cloud_with_credentials_creates_admin(self, tmp_path):
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'x' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
            'ADMIN_EMAIL': 'admin@example.com',
            'ADMIN_PASSWORD': 'securepass123',
            'ADMIN_USERNAME': 'cloudadmin',
        }
        _make_app(env)
        row = _get_admin_user(None, 'cloudadmin')
        assert row is not None, "Admin user was not created"
        assert row['is_admin'] == 1

    def test_cloud_default_username_is_admin(self, tmp_path):
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'x' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
            'ADMIN_EMAIL': 'admin@example.com',
            'ADMIN_PASSWORD': 'securepass123',
            'ADMIN_USERNAME': None,  # not set → default "admin"
        }
        _make_app(env)
        row = _get_admin_user(None, 'admin')
        assert row is not None, "Default admin user was not created"
        assert row['is_admin'] == 1

    # -----------------------------------------------------------------------
    # 2. Cloud + missing ADMIN_EMAIL → no bootstrap user
    # -----------------------------------------------------------------------

    def test_cloud_missing_email_no_bootstrap(self, tmp_path):
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'x' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
            'ADMIN_EMAIL': None,
            'ADMIN_PASSWORD': 'securepass123',
            'ADMIN_USERNAME': None,
        }
        _make_app(env)
        import app as mod
        conn = mod.get_db()
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()
        assert count == 0, "User was created despite missing ADMIN_EMAIL"

    # -----------------------------------------------------------------------
    # 3. Cloud + missing ADMIN_PASSWORD → no bootstrap user
    # -----------------------------------------------------------------------

    def test_cloud_missing_password_no_bootstrap(self, tmp_path):
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'x' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
            'ADMIN_EMAIL': 'admin@example.com',
            'ADMIN_PASSWORD': None,
            'ADMIN_USERNAME': None,
        }
        _make_app(env)
        import app as mod
        conn = mod.get_db()
        count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        conn.close()
        assert count == 0, "User was created despite missing ADMIN_PASSWORD"

    # -----------------------------------------------------------------------
    # 4. Local dev + ADMIN_* env vars → env vars do NOT activate bootstrap
    # -----------------------------------------------------------------------

    def test_local_dev_env_vars_have_no_effect(self, tmp_path):
        """In local mode, ADMIN_EMAIL/ADMIN_PASSWORD env vars are ignored."""
        env = {
            'APP_ENV': None,
            'SECRET_KEY': None,
            'RAILWAY_VOLUME_MOUNT_PATH': None,
            'ADMIN_EMAIL': 'hacker@example.com',
            'ADMIN_PASSWORD': 'evilpassword',
            'ADMIN_USERNAME': 'hacker',
        }
        _make_app(env)
        import app as mod
        conn = mod.get_db()
        # The specific env-var username must NOT exist — env vars must be ignored in local mode
        row = conn.execute("SELECT id FROM users WHERE username=?", ('hacker',)).fetchone()
        conn.close()
        assert row is None, "Env vars triggered bootstrap in local mode — must not happen"

    # -----------------------------------------------------------------------
    # 5. Desktop/local secrets-file behavior unchanged
    # -----------------------------------------------------------------------

    def test_local_secrets_file_creates_admin(self, tmp_path, monkeypatch):
        """Local mode: ~/.budget_tracker_secrets.json still works."""
        secrets = {
            'ADMIN_EMAIL': 'file_admin@example.com',
            'ADMIN_PASSWORD': 'filepassword',
            'ADMIN_USERNAME': 'fileadmin',
        }
        secrets_file = tmp_path / '.budget_tracker_secrets.json'
        secrets_file.write_text(json.dumps(secrets))

        # Redirect ~ to tmp_path so the secrets file is found
        monkeypatch.setenv('HOME', str(tmp_path))
        monkeypatch.setenv('USERPROFILE', str(tmp_path))

        env = {
            'APP_ENV': None,
            'SECRET_KEY': None,
            'RAILWAY_VOLUME_MOUNT_PATH': None,
            'ADMIN_EMAIL': None,
            'ADMIN_PASSWORD': None,
            'ADMIN_USERNAME': None,
        }
        _make_app(env)
        import app as mod
        conn = mod.get_db()
        row = conn.execute(
            "SELECT username, is_admin FROM users WHERE username=?", ('fileadmin',)
        ).fetchone()
        conn.close()
        assert row is not None, "Secrets-file admin was not created in local mode"
        assert row['is_admin'] == 1

    # -----------------------------------------------------------------------
    # 6. No admin password appears in any logged/accessible attribute
    # -----------------------------------------------------------------------

    def test_cloud_password_not_in_app_attributes(self, tmp_path):
        """Verify the plaintext admin password is not stored on the app object."""
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'x' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
            'ADMIN_EMAIL': 'admin@example.com',
            'ADMIN_PASSWORD': 'supersecretpassword',
            'ADMIN_USERNAME': None,
        }
        flask_app = _make_app(env)
        # Scan all string-valued attributes of the Flask app for the plaintext password
        password = 'supersecretpassword'
        for attr in dir(flask_app):
            try:
                val = getattr(flask_app, attr)
                if isinstance(val, str) and password in val:
                    pytest.fail(f"Plaintext password found in flask_app.{attr}")
            except Exception:
                pass
        # Also verify it's not in the Flask config dict
        for k, v in flask_app.config.items():
            if isinstance(v, str) and password in v:
                pytest.fail(f"Plaintext password found in flask_app.config['{k}']")

    # -----------------------------------------------------------------------
    # 7. Cloud admin can log in after bootstrap
    # -----------------------------------------------------------------------

    def test_cloud_bootstrapped_admin_can_login(self, tmp_path):
        env = {
            'APP_ENV': 'production',
            'SECRET_KEY': 'y' * 64,
            'RAILWAY_VOLUME_MOUNT_PATH': str(tmp_path),
            'ADMIN_EMAIL': 'admin@example.com',
            'ADMIN_PASSWORD': 'loginpass123',
            'ADMIN_USERNAME': 'loginadmin',
        }
        flask_app = _make_app(env)
        flask_app.config['TESTING'] = True
        with flask_app.test_client() as c:
            r = c.post('/api/auth/login', json={
                'username': 'loginadmin',
                'password': 'loginpass123',
            })
            assert r.status_code == 200
            r2 = c.get('/api/auth/status')
            data2 = r2.get_json()
            assert data2.get('logged_in') is True
            assert data2.get('is_admin') is True
