"""Build script - creates a distributable .exe with no personal data."""
import subprocess
import sys
import os
import re

BASE = os.path.dirname(os.path.abspath(__file__))
DIST = os.path.join(BASE, 'dist')

# Read version from app.py (single source of truth)
with open(os.path.join(BASE, 'app.py'), encoding='utf-8') as f:
    match = re.search(r"APP_VERSION\s*=\s*'([^']+)'", f.read())
VERSION = match.group(1) if match else 'unknown'

EXE_NAME = f'HomeBudget-{VERSION}-Setup'

print(f'=== Building Home Budget v{VERSION} ===')

# 1. Install PyInstaller if needed
print('[1/3] Checking PyInstaller...')
try:
    import PyInstaller
except ImportError:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pyinstaller'])

# 2. Run PyInstaller
print('[2/3] Building .exe ...')
subprocess.check_call([
    sys.executable, '-m', 'PyInstaller',
    '--noconfirm',
    '--onefile',
    '--console',
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
    '--add-data', f'intelligence{os.pathsep}intelligence',
    'app.py',
], cwd=BASE)

# 3. Summary
exe_path = os.path.join(DIST, f'{EXE_NAME}.exe')
if os.path.exists(exe_path):
    size_mb = os.path.getsize(exe_path) / (1024 * 1024)
    print(f'\n[3/3] Done! Installer created:')
    print(f'  {exe_path}')
    print(f'  Size: {size_mb:.1f} MB')
    print(f'\nTag the release:')
    print(f'  git tag v{VERSION}')
    print(f'  git push origin v{VERSION}')
    print(f'\nThen upload {EXE_NAME}.exe to the GitHub release.')
else:
    print('ERROR: Build failed, .exe not found')
    sys.exit(1)
