"""Build script — Windows distribution for Home Budget Tracker.

MANDATORY SAFETY ORDER:
  1.  Verify Windows environment
  2.  Verify application is closed
  3.  Locate existing user DB
  4.  Fingerprint DB (path, size, mtime, SHA256)
  5.  Backup DB (timestamped, verified)
  6.  Record DB row counts (read-only)
  7.  Preserve current stable EXE (rollback copy, verified)
  8.  Install PyInstaller if needed
  9.  Run PyInstaller
  10. Verify versioned EXE (exists, size > 0)
  11. Copy versioned EXE to temporary stable path, verify
  12. Atomically replace dist/expense-tracker.exe
  13. Create/update Desktop shortcut
  14. Read back and verify shortcut
  15. Print final report

If steps 1-12 fail, the original expense-tracker.exe is left untouched.
DB backup and EXE rollback copies are never deleted by this script.
"""
import subprocess
import sys
import os
import re
import shutil
import hashlib
import sqlite3
import platform
import datetime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE = os.path.dirname(os.path.abspath(__file__))
DIST = os.path.join(BASE, 'dist')
ROLLBACK_DIR = os.path.join(DIST, 'rollback')

# Read APP_VERSION from app.py (single source of truth)
with open(os.path.join(BASE, 'app.py'), encoding='utf-8') as _f:
    _match = re.search(r"APP_VERSION\s*=\s*'([^']+)'", _f.read())
if not _match:
    print('ERROR: APP_VERSION not found in app.py')
    sys.exit(1)
VERSION = _match.group(1)

EXE_NAME      = f'HomeBudget-{VERSION}-Setup'
VERSIONED_EXE = os.path.join(DIST, f'{EXE_NAME}.exe')
STABLE_EXE    = os.path.join(DIST, 'expense-tracker.exe')
STABLE_APP    = 'expense-tracker.exe'   # process name for tasklist check

TIMESTAMP     = datetime.datetime.now().strftime('%Y%m%d-%H%M%S-%f')
DB_DATA_DIR   = os.path.join(os.path.expanduser('~'), '.budget_tracker_data')
DB_PATH       = os.path.join(DB_DATA_DIR, 'budget.db')
DB_BACKUP_DIR = os.path.join(DB_DATA_DIR, 'backups')
DB_BACKUP     = os.path.join(DB_BACKUP_DIR, f'budget.db.prebuild-{VERSION}-{TIMESTAMP}')

SHORTCUT_PATH = os.path.join(os.path.expanduser('~'), 'Desktop', 'Family Budget Tracker.lnk')
SHORTCUT_TARGET   = STABLE_EXE
SHORTCUT_WORKDIR  = BASE

print(f'=== Home Budget Build v{VERSION} ===')
print(f'    Base:        {BASE}')
print(f'    DB:          {DB_PATH}')
print(f'    Versioned:   {VERSIONED_EXE}')
print(f'    Stable:      {STABLE_EXE}')
print()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def sha256_file(path):
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(65536), b''):
            h.update(chunk)
    return h.hexdigest()


def stop(msg):
    print(f'\nBUILD STOPPED: {msg}')
    sys.exit(1)


# ---------------------------------------------------------------------------
# Step 1 — Verify Windows environment
# ---------------------------------------------------------------------------
print('[1/15] Checking platform...')
if platform.system() != 'Windows':
    stop(
        'This build workflow must be executed on Windows.\n'
        f'       Current platform: {platform.system()}'
    )
print(f'  Platform: {platform.system()} {platform.version()}')


# ---------------------------------------------------------------------------
# Step 2 — Verify application is closed
# ---------------------------------------------------------------------------
print('[2/15] Checking that application is not running...')
try:
    result = subprocess.run(
        ['tasklist', '/FI', f'IMAGENAME eq {STABLE_APP}', '/NH'],
        capture_output=True, text=True
    )
    if STABLE_APP.lower() in result.stdout.lower():
        stop(
            'Close Family Budget Tracker before building.\n'
            f'       Process found: {STABLE_APP}'
        )
except FileNotFoundError:
    stop('tasklist not found — cannot verify the application is closed.')
print(f'  {STABLE_APP} is not running.')


# ---------------------------------------------------------------------------
# Step 3 — Locate existing user DB
# ---------------------------------------------------------------------------
print('[3/15] Locating user database...')
if not os.path.exists(DB_PATH):
    stop(
        f'User database not found: {DB_PATH}\n'
        '       This build workflow requires an existing installation with user data.\n'
        '       Do not build on a fresh machine without first migrating the database.'
    )
db_size = os.path.getsize(DB_PATH)
db_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(DB_PATH)).isoformat(timespec='seconds')
print(f'  DB path:     {DB_PATH}')
print(f'  DB size:     {db_size:,} bytes')
print(f'  DB modified: {db_mtime}')


# ---------------------------------------------------------------------------
# Step 4 — Fingerprint DB
# ---------------------------------------------------------------------------
print('[4/15] Fingerprinting database...')
db_sha256 = sha256_file(DB_PATH)
print(f'  SHA256: {db_sha256}')


# ---------------------------------------------------------------------------
# Step 5 — Backup DB
# ---------------------------------------------------------------------------
print('[5/15] Backing up database...')
os.makedirs(DB_BACKUP_DIR, exist_ok=True)
if os.path.exists(DB_BACKUP):
    stop(f'Backup already exists (timestamp collision): {DB_BACKUP}')
shutil.copy2(DB_PATH, DB_BACKUP)
print(f'  DB backup created: {DB_BACKUP}')


# ---------------------------------------------------------------------------
# Step 6 — Verify DB backup
# ---------------------------------------------------------------------------
print('[6/15] Verifying database backup...')
if not os.path.exists(DB_BACKUP):
    stop(f'Backup file not found after copy: {DB_BACKUP}')
backup_size = os.path.getsize(DB_BACKUP)
if backup_size != db_size:
    stop(f'Backup size mismatch: source={db_size}, backup={backup_size}')
backup_sha256 = sha256_file(DB_BACKUP)
if backup_sha256 != db_sha256:
    stop(f'Backup SHA256 mismatch:\n  source={db_sha256}\n  backup={backup_sha256}')
print(f'  DB backup verification: PASS')
print(f'  Size match:  {backup_size:,} bytes')
print(f'  SHA256 match: {backup_sha256[:16]}...')


# ---------------------------------------------------------------------------
# Step 7 — Record DB row counts (read-only)
# ---------------------------------------------------------------------------
print('[7/15] Recording pre-build row counts...')
USER_TABLES = ['users', 'expenses', 'categories', 'merchant_learning', 'installments']
try:
    conn = sqlite3.connect(f'file:{DB_PATH}?mode=ro', uri=True)
    all_tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()]
    print('  Tables:')
    for tbl in all_tables:
        try:
            count = conn.execute(f'SELECT COUNT(*) FROM "{tbl}"').fetchone()[0]
            marker = ' ← key table' if tbl in USER_TABLES else ''
            print(f'    {tbl}: {count}{marker}')
        except Exception as e:
            print(f'    {tbl}: ERROR ({e})')
    conn.close()
except Exception as e:
    # Non-fatal: print warning but do not stop — DB already backed up
    print(f'  WARNING: Could not open DB read-only: {e}')
    print('  Proceeding (DB backup already verified).')


# ---------------------------------------------------------------------------
# Step 8 — Preserve current stable EXE (rollback)
# ---------------------------------------------------------------------------
print('[8/15] Preserving current stable EXE...')
os.makedirs(ROLLBACK_DIR, exist_ok=True)
if os.path.exists(STABLE_EXE):
    rollback_name = f'expense-tracker.pre-{VERSION}-{TIMESTAMP}.exe'
    rollback_path = os.path.join(ROLLBACK_DIR, rollback_name)
    if os.path.exists(rollback_path):
        stop(f'Rollback file already exists (timestamp collision): {rollback_path}')
    old_exe_size   = os.path.getsize(STABLE_EXE)
    old_exe_sha256 = sha256_file(STABLE_EXE)
    shutil.copy2(STABLE_EXE, rollback_path)
    if not os.path.exists(rollback_path):
        stop(f'Rollback copy not found after write: {rollback_path}')
    rb_size   = os.path.getsize(rollback_path)
    rb_sha256 = sha256_file(rollback_path)
    if rb_size != old_exe_size or rb_sha256 != old_exe_sha256:
        stop(
            f'Rollback verification failed:\n'
            f'  source size={old_exe_size}, rollback size={rb_size}\n'
            f'  source sha256={old_exe_sha256}\n'
            f'  rollback sha256={rb_sha256}'
        )
    print(f'  Rollback copy: {rollback_path}')
    print(f'  Rollback verification: PASS')
else:
    rollback_path = None
    print('  No existing stable EXE to roll back (first build).')


# ---------------------------------------------------------------------------
# Step 9 — Install PyInstaller if needed
# ---------------------------------------------------------------------------
print('[9/15] Checking PyInstaller...')
try:
    import PyInstaller  # noqa: F401
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyinstaller'])


# ---------------------------------------------------------------------------
# Step 10 — Run PyInstaller
# ---------------------------------------------------------------------------
print('[10/15] Building versioned EXE...')
subprocess.check_call([
    sys.executable, '-m', 'PyInstaller',
    '--noconfirm',
    '--onefile',
    '--windowed',
    '--name', EXE_NAME,
    '--icon', 'app.ico',
    '--add-data', f'static{os.pathsep}static',
    '--hidden-import', 'openpyxl',
    '--hidden-import', 'xlrd',
    '--hidden-import', 'fitz',
    '--hidden-import', 'intelligence',
    '--hidden-import', 'intelligence.normalizer',
    '--hidden-import', 'intelligence.income_normalizer',
    '--hidden-import', 'intelligence.categorizer',
    '--hidden-import', 'intelligence.merchant_seed_loader',
    '--hidden-import', 'webview.platforms.edgechromium',
    '--hidden-import', 'webview.platforms.winforms',
    '--collect-data', 'webview',
    '--add-data', f'intelligence{os.pathsep}intelligence',
    'app.py',
], cwd=BASE)


# ---------------------------------------------------------------------------
# Step 11 — Verify versioned EXE
# ---------------------------------------------------------------------------
print('[11/15] Verifying versioned EXE...')
if not os.path.exists(VERSIONED_EXE):
    stop(f'Expected versioned EXE not found after build: {VERSIONED_EXE}')
versioned_size = os.path.getsize(VERSIONED_EXE)
if versioned_size == 0:
    stop(f'Versioned EXE is empty: {VERSIONED_EXE}')
print(f'  {VERSIONED_EXE}')
print(f'  Size: {versioned_size / (1024 * 1024):.1f} MB')


# ---------------------------------------------------------------------------
# Step 12 — Copy to temporary stable path and verify, then atomic replace
# ---------------------------------------------------------------------------
print('[12/15] Replacing stable EXE...')
temp_stable = STABLE_EXE + '.new'
shutil.copy2(VERSIONED_EXE, temp_stable)

# Verify the temp copy before touching the live stable EXE
if not os.path.exists(temp_stable):
    stop(f'Temporary stable copy not found: {temp_stable}')
temp_size   = os.path.getsize(temp_stable)
temp_sha256 = sha256_file(temp_stable)
ver_sha256  = sha256_file(VERSIONED_EXE)
if temp_size != versioned_size or temp_sha256 != ver_sha256:
    try:
        os.remove(temp_stable)
    except OSError:
        pass
    stop(
        f'Temporary copy verification failed:\n'
        f'  versioned size={versioned_size}, temp size={temp_size}\n'
        f'  versioned sha256={ver_sha256}\n'
        f'  temp sha256={temp_sha256}'
    )

try:
    os.replace(temp_stable, STABLE_EXE)
except OSError as e:
    try:
        os.remove(temp_stable)
    except OSError:
        pass
    stop(
        f'Atomic replace failed: {e}\n'
        f'  Old stable EXE is still intact.\n'
        f'  Rollback copy: {rollback_path}'
    )

stable_size = os.path.getsize(STABLE_EXE)
print(f'  Stable EXE updated: {STABLE_EXE}')
print(f'  Size: {stable_size / (1024 * 1024):.1f} MB')


# ---------------------------------------------------------------------------
# Step 13 — Create/update Desktop shortcut
# ---------------------------------------------------------------------------
print('[13/15] Updating Desktop shortcut...')
target_escaped   = SHORTCUT_TARGET.replace("'", "''")
workdir_escaped  = SHORTCUT_WORKDIR.replace("'", "''")
lnk_escaped      = SHORTCUT_PATH.replace("'", "''")

ps_create = (
    f"$ws = New-Object -ComObject WScript.Shell; "
    f"$s = $ws.CreateShortcut('{lnk_escaped}'); "
    f"$s.TargetPath = '{target_escaped}'; "
    f"$s.WorkingDirectory = '{workdir_escaped}'; "
    f"$s.Save()"
)
try:
    subprocess.check_call(
        ['powershell', '-NonInteractive', '-NoProfile', '-Command', ps_create]
    )
except subprocess.CalledProcessError as e:
    stop(
        f'Shortcut creation failed: {e}\n'
        f'  Stable EXE was already updated.\n'
        f'  Rollback copy: {rollback_path}\n'
        f'  DB backup:     {DB_BACKUP}'
    )
print(f'  Shortcut written: {SHORTCUT_PATH}')


# ---------------------------------------------------------------------------
# Step 14 — Read back and verify shortcut
# ---------------------------------------------------------------------------
print('[14/15] Verifying shortcut...')
ps_read = (
    f"$ws = New-Object -ComObject WScript.Shell; "
    f"$s = $ws.CreateShortcut('{lnk_escaped}'); "
    f"Write-Output $s.TargetPath; "
    f"Write-Output $s.WorkingDirectory"
)
try:
    result = subprocess.run(
        ['powershell', '-NonInteractive', '-NoProfile', '-Command', ps_read],
        capture_output=True, text=True, check=True
    )
    lines = [l.strip() for l in result.stdout.strip().splitlines() if l.strip()]
    actual_target  = lines[0] if len(lines) > 0 else ''
    actual_workdir = lines[1] if len(lines) > 1 else ''
except subprocess.CalledProcessError as e:
    stop(f'Could not read back shortcut for verification: {e}')

target_ok  = actual_target.lower()  == SHORTCUT_TARGET.lower()
workdir_ok = actual_workdir.lower() == SHORTCUT_WORKDIR.lower()

print(f'  TargetPath:      {actual_target}')
print(f'  WorkingDirectory:{actual_workdir}')

if not target_ok or not workdir_ok:
    stop(
        f'Shortcut verification failed:\n'
        f'  Expected TargetPath:      {SHORTCUT_TARGET}\n'
        f'  Actual   TargetPath:      {actual_target}\n'
        f'  Expected WorkingDirectory:{SHORTCUT_WORKDIR}\n'
        f'  Actual   WorkingDirectory:{actual_workdir}'
    )
print('  Shortcut verification: PASS')


# ---------------------------------------------------------------------------
# Step 15 — Final report
# ---------------------------------------------------------------------------
print()
print('=' * 65)
print('BUILD COMPLETE')
print('=' * 65)
print(f'Version:           {VERSION}')
print(f'Versioned EXE:     {VERSIONED_EXE}')
print(f'  Size:            {versioned_size / (1024 * 1024):.1f} MB')
print(f'Stable EXE:        {STABLE_EXE}')
print(f'  Size:            {stable_size / (1024 * 1024):.1f} MB')
print(f'DB backup:         {DB_BACKUP}')
print(f'  SHA256:          {db_sha256[:32]}...')
if rollback_path:
    print(f'EXE rollback:      {rollback_path}')
print(f'Desktop shortcut:  {SHORTCUT_PATH}')
print(f'  Target:          {actual_target}')
print(f'  Working dir:     {actual_workdir}')
print('=' * 65)
