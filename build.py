"""Build script - creates a distributable .exe with no personal data."""
import subprocess
import sys
import os
import shutil

BASE = os.path.dirname(os.path.abspath(__file__))
DIST = os.path.join(BASE, 'dist')
OUTPUT = os.path.join(DIST, 'expense-tracker')

print('=== Building Expense Tracker ===')

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
    '--name', 'expense-tracker',
    '--icon', 'app.ico',
    '--add-data', f'static{os.pathsep}static',
    '--hidden-import', 'openpyxl',
    '--hidden-import', 'xlrd',
    '--hidden-import', 'fitz',
    'app.py',
], cwd=BASE)

# 3. Summary
exe_path = os.path.join(DIST, 'expense-tracker.exe')
if os.path.exists(exe_path):
    size_mb = os.path.getsize(exe_path) / (1024 * 1024)
    print(f'\n[3/3] Done! exe created:')
    print(f'  {exe_path}')
    print(f'  Size: {size_mb:.1f} MB')
    print(f'\nSend this single file. The recipient double-clicks it,')
    print(f'the browser opens automatically, and they can start importing their own data.')
    print(f'\nNo personal data is included - database is created fresh on first run.')
else:
    print('ERROR: Build failed, .exe not found')
    sys.exit(1)
