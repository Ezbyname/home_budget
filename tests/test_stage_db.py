"""
Tests for POST /api/admin/stage-db and GET /api/admin/stage-db.

All tests use isolated tmp_path directories; no real budget.db is referenced.
"""
import hashlib
import io
import os
import sqlite3
import sys
import uuid
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SQLITE_MAGIC = b'SQLite format 3\x00'


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


def _make_client(tmp_path, is_admin=True):
    mod = _make_app(tmp_path)
    mod.app.config['TESTING'] = True
    uname = 'u_' + uuid.uuid4().hex[:6]
    conn = mod.get_db()
    pw = mod.hash_password('pw')
    conn.execute(
        "INSERT INTO users (username, password_hash, email, verified, is_admin)"
        " VALUES (?,?,?,1,?)",
        (uname, pw, f'{uname}@test.com', 1 if is_admin else 0)
    )
    conn.commit()
    conn.close()
    client = mod.app.test_client()
    r = client.post('/api/auth/login', json={'username': uname, 'password': 'pw'})
    assert r.status_code == 200
    return client, mod


def _make_valid_sqlite(path: str) -> bytes:
    """Create a minimal valid SQLite DB at path and return its bytes."""
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)")
    conn.commit()
    conn.close()
    with open(path, 'rb') as f:
        return f.read()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _post_stage(client, data: bytes, expected_sha256: str | None, filename='budget.db'):
    fields = {}
    if expected_sha256 is not None:
        fields['expected_sha256'] = expected_sha256
    return client.post(
        '/api/admin/stage-db',
        data={**fields, 'file': (io.BytesIO(data), filename)},
        content_type='multipart/form-data',
    )


# ---------------------------------------------------------------------------
# 1. Authentication / authorization / environment gate
# ---------------------------------------------------------------------------

class TestStageDbAuth:

    def test_anonymous_get_returns_401(self, tmp_path):
        mod = _make_app(tmp_path)
        mod.app.config['TESTING'] = True
        with mod.app.test_client() as c:
            r = c.get('/api/admin/stage-db')
            assert r.status_code == 401

    def test_anonymous_post_returns_401(self, tmp_path):
        mod = _make_app(tmp_path)
        mod.app.config['TESTING'] = True
        with mod.app.test_client() as c:
            r = c.post('/api/admin/stage-db', data={}, content_type='multipart/form-data')
            assert r.status_code == 401

    def test_non_admin_get_returns_403(self, tmp_path):
        client, _ = _make_client(tmp_path, is_admin=False)
        r = client.get('/api/admin/stage-db')
        assert r.status_code == 403

    def test_non_admin_post_returns_403(self, tmp_path):
        client, _ = _make_client(tmp_path, is_admin=False)
        r = client.post('/api/admin/stage-db', data={}, content_type='multipart/form-data')
        assert r.status_code == 403

    def test_non_cloud_post_returns_403(self, tmp_path):
        """In local (non-cloud) mode the endpoint must return 403."""
        env = {
            'APP_ENV': None,
            'SECRET_KEY': None,
            'RAILWAY_VOLUME_MOUNT_PATH': None,
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
            import app as local_mod
            local_mod.app.config['TESTING'] = True
            uname = 'adm_' + uuid.uuid4().hex[:6]
            conn = local_mod.get_db()
            pw = local_mod.hash_password('pw')
            conn.execute(
                "INSERT INTO users (username, password_hash, email, verified, is_admin)"
                " VALUES (?,?,?,1,1)",
                (uname, pw, f'{uname}@test.com')
            )
            conn.commit()
            conn.close()
            with local_mod.app.test_client() as c:
                c.post('/api/auth/login', json={'username': uname, 'password': 'pw'})
                r = c.post('/api/admin/stage-db', data={}, content_type='multipart/form-data')
                assert r.status_code == 403, f"Expected 403 in local mode, got {r.status_code}"
        finally:
            for k, orig_v in orig.items():
                if orig_v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = orig_v
            for mod in list(sys.modules):
                if mod == 'app' or mod.startswith('app.'):
                    del sys.modules[mod]

    def test_admin_get_returns_html(self, tmp_path):
        client, _ = _make_client(tmp_path, is_admin=True)
        r = client.get('/api/admin/stage-db')
        assert r.status_code == 200
        assert b'stage-db' in r.data.lower() or b'staging' in r.data.lower()
        assert b'<form' in r.data


# ---------------------------------------------------------------------------
# 2. POST validation rejections
# ---------------------------------------------------------------------------

class TestStageDbRejections:

    def test_missing_file_returns_400(self, tmp_path):
        client, mod = _make_client(tmp_path)
        r = client.post(
            '/api/admin/stage-db',
            data={'expected_sha256': 'a' * 64},
            content_type='multipart/form-data',
        )
        assert r.status_code == 400
        assert 'error' in r.get_json()

    def test_missing_expected_sha256_returns_400(self, tmp_path):
        client, mod = _make_client(tmp_path)
        data = b'\x00' * 32
        r = client.post(
            '/api/admin/stage-db',
            data={'file': (io.BytesIO(data), 'test.db')},
            content_type='multipart/form-data',
        )
        assert r.status_code == 400
        body = r.get_json()
        assert 'expected_sha256' in body.get('error', '')

    def test_malformed_sha256_too_short_returns_400(self, tmp_path):
        client, _ = _make_client(tmp_path)
        data = b'\x00' * 32
        r = _post_stage(client, data, 'abc123')
        assert r.status_code == 400
        assert 'hex' in r.get_json().get('error', '').lower() or '64' in r.get_json().get('error', '')

    def test_malformed_sha256_non_hex_returns_400(self, tmp_path):
        client, _ = _make_client(tmp_path)
        data = b'\x00' * 32
        r = _post_stage(client, data, 'z' * 64)
        assert r.status_code == 400

    def test_oversized_upload_returns_400(self, tmp_path):
        client, mod = _make_client(tmp_path)
        large = _SQLITE_MAGIC + b'\x00' * (mod._STAGING_MAX_BYTES + 1)
        r = _post_stage(client, large, _sha256(large))
        assert r.status_code == 400
        assert 'limit' in r.get_json().get('error', '').lower() or 'exceed' in r.get_json().get('error', '').lower()

    def test_non_sqlite_file_returns_400(self, tmp_path):
        client, _ = _make_client(tmp_path)
        data = b'This is just a text file, not SQLite'
        r = _post_stage(client, data, _sha256(data))
        assert r.status_code == 400
        assert 'magic' in r.get_json().get('error', '').lower() or 'sqlite' in r.get_json().get('error', '').lower()

    def test_wrong_sha256_returns_400(self, tmp_path, tmp_path_factory):
        client, _ = _make_client(tmp_path)
        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))
        wrong_hash = 'a' * 64
        r = _post_stage(client, data, wrong_hash)
        assert r.status_code == 400
        body = r.get_json()
        assert 'mismatch' in body.get('error', '').lower()

    def test_corrupt_sqlite_returns_400(self, tmp_path):
        """SQLite magic OK but content is corrupt — integrity_check must catch it."""
        client, _ = _make_client(tmp_path)
        # Valid magic, but garbage content after header
        data = _SQLITE_MAGIC + b'\xff' * 500
        r = _post_stage(client, data, _sha256(data))
        assert r.status_code == 400
        body = r.get_json()
        assert 'error' in body

    def test_client_filename_is_ignored(self, tmp_path, tmp_path_factory):
        """Malicious filename with path traversal must not affect staging destination."""
        client, mod = _make_client(tmp_path)
        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))
        correct_hash = _sha256(data)
        r = _post_stage(client, data, correct_hash, filename='../../evil/path.db')
        # Either it succeeds (and stages to budget.incoming.db, not evil path)
        # or it fails — but it must NEVER write outside DATA_DIR
        if r.status_code == 200:
            assert os.path.exists(mod._STAGING_PATH)
            assert not os.path.exists(str(tmp_path / '../../evil/path.db'))
        # No path traversal artifacts anywhere outside tmp_path
        evil_path = os.path.normpath(os.path.join(str(tmp_path), '../../evil'))
        assert not os.path.exists(evil_path)


# ---------------------------------------------------------------------------
# 3. Successful staging
# ---------------------------------------------------------------------------

class TestStageDbSuccess:

    def test_valid_sqlite_staged_successfully(self, tmp_path, tmp_path_factory):
        client, mod = _make_client(tmp_path)
        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))
        correct_hash = _sha256(data)

        r = _post_stage(client, data, correct_hash)
        assert r.status_code == 200, f"Expected 200, got {r.status_code}: {r.get_json()}"

        body = r.get_json()
        assert body['staged_file'] == 'budget.incoming.db'
        assert body['file_size'] == len(data)
        assert body['sha256'] == correct_hash
        assert body['integrity_check'] == 'ok'
        assert body['quick_check'] == 'ok'

    def test_staged_file_exists_at_correct_path(self, tmp_path, tmp_path_factory):
        client, mod = _make_client(tmp_path)
        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))

        _post_stage(client, data, _sha256(data))

        assert os.path.exists(mod._STAGING_PATH), "budget.incoming.db was not created"
        # Must be within the isolated DATA_DIR (tmp_path), not /data
        assert str(mod._STAGING_PATH).startswith(str(tmp_path))
        assert '/data/' not in str(mod._STAGING_PATH).replace('\\', '/')

    def test_live_db_is_not_modified(self, tmp_path, tmp_path_factory):
        """Staging must never touch budget.db."""
        client, mod = _make_client(tmp_path)
        live_db_path = mod.DB_PATH
        mtime_before = os.path.getmtime(live_db_path)
        size_before = os.path.getsize(live_db_path)

        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))
        _post_stage(client, data, _sha256(data))

        assert os.path.getmtime(live_db_path) == mtime_before, "budget.db mtime changed"
        assert os.path.getsize(live_db_path) == size_before, "budget.db size changed"

    def test_failed_upload_does_not_destroy_existing_valid_staged_db(self, tmp_path, tmp_path_factory):
        """A failed second upload must not delete or corrupt a previously valid staged DB."""
        client, mod = _make_client(tmp_path)

        # First: stage a valid DB
        db_file = tmp_path_factory.mktemp('dbs') / 'first.db'
        data = _make_valid_sqlite(str(db_file))
        r = _post_stage(client, data, _sha256(data))
        assert r.status_code == 200
        valid_sha = r.get_json()['sha256']

        # Second: attempt to stage a bad (non-SQLite) file
        bad_data = b'this is not sqlite at all'
        _post_stage(client, bad_data, _sha256(bad_data))

        # The previously staged DB must still exist and be intact
        assert os.path.exists(mod._STAGING_PATH), "Valid staged DB was destroyed by failed upload"
        with open(mod._STAGING_PATH, 'rb') as f:
            remaining = f.read()
        assert hashlib.sha256(remaining).hexdigest() == valid_sha, "Staged DB content was corrupted"

    def test_sha256_case_insensitive(self, tmp_path, tmp_path_factory):
        """Uppercase expected_sha256 must be accepted (normalized to lowercase)."""
        client, mod = _make_client(tmp_path)
        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))
        upper_hash = _sha256(data).upper()

        r = _post_stage(client, data, upper_hash)
        assert r.status_code == 200

    def test_response_contains_no_full_server_path(self, tmp_path, tmp_path_factory):
        """Response must not expose full filesystem paths."""
        client, mod = _make_client(tmp_path)
        db_file = tmp_path_factory.mktemp('dbs') / 'valid.db'
        data = _make_valid_sqlite(str(db_file))
        r = _post_stage(client, data, _sha256(data))
        body_str = r.get_data(as_text=True)
        # The DATA_DIR (which contains /tmp or similar) must not appear in the response
        assert str(tmp_path) not in body_str

    def test_sqlite_validation_error_does_not_leak_exception_and_preserves_staged_db(
            self, tmp_path, tmp_path_factory):
        """If SQLite validation raises after connect(), the temp file is cleaned up,
        the existing staged DB is untouched, and the raw exception is not in the response."""
        client, mod = _make_client(tmp_path)

        # First: place a valid staged DB so we can confirm it survives the failure
        db_file = tmp_path_factory.mktemp('dbs') / 'first.db'
        first_data = _make_valid_sqlite(str(db_file))
        r = _post_stage(client, first_data, _sha256(first_data))
        assert r.status_code == 200
        valid_sha = r.get_json()['sha256']

        # Second: upload valid magic + correct SHA256 but corrupt body so
        # integrity_check raises sqlite3.DatabaseError after connect() succeeds.
        # 100 bytes of 0xff after the magic is enough to open but fail PRAGMA.
        corrupt = _SQLITE_MAGIC + b'\xff' * 100
        r2 = _post_stage(client, corrupt, _sha256(corrupt))
        assert r2.status_code == 400
        body = r2.get_json()
        # Must contain 'error' key
        assert 'error' in body
        # Must NOT expose raw SQLite internals in the response
        assert 'traceback' not in r2.get_data(as_text=True).lower()
        assert 'sqlite3.' not in r2.get_data(as_text=True)
        # No temp files left behind (.staging_tmp suffix)
        leftover = [
            f for f in os.listdir(str(tmp_path))
            if f.endswith('.staging_tmp')
        ]
        assert leftover == [], f"Temp files not cleaned up: {leftover}"
        # Previously valid staged DB must be intact
        assert os.path.exists(mod._STAGING_PATH), "Valid staged DB was destroyed"
        with open(mod._STAGING_PATH, 'rb') as f:
            remaining = f.read()
        assert hashlib.sha256(remaining).hexdigest() == valid_sha, "Staged DB content corrupted"
