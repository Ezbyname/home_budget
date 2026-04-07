import os
import csv
import io
import sqlite3
import json
import random
import smtplib
import hashlib
import secrets
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import threading
import time as _time
from datetime import datetime, date, timedelta
from functools import wraps
from flask import Flask, request, jsonify, send_from_directory, send_file, session, redirect
import xlrd
import openpyxl

import sys

# When running as a PyInstaller exe, use the exe's directory for data files
if getattr(sys, 'frozen', False):
    BASE_DIR = os.path.dirname(sys.executable)
    STATIC_DIR = os.path.join(sys._MEIPASS, 'static')
else:
    BASE_DIR = os.path.dirname(__file__)
    STATIC_DIR = os.path.join(BASE_DIR, 'static')

app = Flask(__name__, static_folder=STATIC_DIR)
app.config['UPLOAD_FOLDER'] = os.path.join(BASE_DIR, 'uploads')
DB_PATH = os.path.join(BASE_DIR, 'budget.db')

# Session config
SECRET_FILE = os.path.join(BASE_DIR, '.secret_key')
if os.path.exists(SECRET_FILE):
    with open(SECRET_FILE, 'r') as f:
        app.secret_key = f.read().strip()
else:
    app.secret_key = secrets.token_hex(32)
    with open(SECRET_FILE, 'w') as f:
        f.write(app.secret_key)
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)

# SMTP config file (created on first email setup)
SMTP_CONFIG_PATH = os.path.join(BASE_DIR, 'smtp_config.json')

os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# Hebrew category mapping from the XLS structure
CATEGORY_MAP = {
    'דיור ואחזקת בית': 'housing',
    'מזון': 'food',
    'הורים': 'parents',
    'ילדים': 'children',
    'רכב': 'vehicle',
    'תקשורת': 'communication',
    'מוצרי טיפוח ובריאות': 'health_beauty',
    'ריפוי': 'medical',
    'ביטוחים': 'insurance',
    'בילוי,פנאי ובידור': 'entertainment',
    'אישי': 'personal',
    'חיסכון והתחייבויות': 'savings',
    'תשלומים שונים': 'misc',
}

DEFAULT_CATEGORIES = [
    ('housing', 'דיור ואחזקת בית', '#4e79a7'),
    ('food', 'מזון', '#f28e2b'),
    ('children', 'ילדים', '#e15759'),
    ('vehicle', 'רכב', '#76b7b2'),
    ('communication', 'תקשורת', '#59a14f'),
    ('health_beauty', 'טיפוח ובריאות', '#edc948'),
    ('medical', 'ריפוי', '#b07aa1'),
    ('insurance', 'ביטוחים', '#ff9da7'),
    ('entertainment', 'בילוי ופנאי', '#9c755f'),
    ('personal', 'אישי', '#bab0ac'),
    ('savings', 'חיסכון והתחייבויות', '#4dc9f6'),
    ('misc', 'שונות', '#a5a5a5'),
    ('parents', 'הורים', '#d4a373'),
]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def apply_category_rule(conn, description, category_id, frequency='random'):
    """Check if user has a saved category/frequency rule for this description."""
    if description:
        rule = conn.execute(
            "SELECT category_id FROM category_rules WHERE description=?", (description,)
        ).fetchone()
        if rule:
            category_id = rule['category_id']
        # Also check if existing expenses with same description have a non-random frequency
        freq_row = conn.execute(
            "SELECT frequency FROM expenses WHERE description=? AND frequency != 'random' LIMIT 1",
            (description,)
        ).fetchone()
        if freq_row:
            frequency = freq_row['frequency']
    return category_id, frequency


def init_db():
    conn = get_db()
    conn.executescript('''
        CREATE TABLE IF NOT EXISTS categories (
            id TEXT PRIMARY KEY,
            name_he TEXT NOT NULL,
            color TEXT NOT NULL DEFAULT '#888888',
            sort_order INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            category_id TEXT NOT NULL,
            subcategory TEXT,
            description TEXT,
            amount REAL NOT NULL,
            source TEXT DEFAULT 'manual',
            frequency TEXT DEFAULT 'random',
            card TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS income (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT NOT NULL,
            person TEXT NOT NULL,
            source TEXT NOT NULL,
            amount REAL NOT NULL,
            description TEXT,
            is_recurring INTEGER DEFAULT 0,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS budget_plans (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL DEFAULT 'תקציב 1'
        );

        CREATE TABLE IF NOT EXISTS budget (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            category_id TEXT NOT NULL,
            month TEXT NOT NULL,
            planned_amount REAL NOT NULL DEFAULT 0,
            plan_id INTEGER NOT NULL DEFAULT 1,
            UNIQUE(category_id, month, plan_id),
            FOREIGN KEY (category_id) REFERENCES categories(id),
            FOREIGN KEY (plan_id) REFERENCES budget_plans(id)
        );

        INSERT OR IGNORE INTO budget_plans (id, name) VALUES (1, 'תקציב 1');

        CREATE INDEX IF NOT EXISTS idx_expenses_date ON expenses(date);
        CREATE INDEX IF NOT EXISTS idx_expenses_category ON expenses(category_id);
        CREATE INDEX IF NOT EXISTS idx_income_date ON income(date);

        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            email TEXT DEFAULT '',
            phone TEXT DEFAULT '',
            verified INTEGER DEFAULT 0,
            verification_method TEXT DEFAULT '',
            otp_code TEXT DEFAULT '',
            otp_expires TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS financial_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            subtype TEXT NOT NULL DEFAULT '',
            company TEXT NOT NULL DEFAULT '',
            name TEXT NOT NULL DEFAULT '',
            policy_number TEXT DEFAULT '',
            monthly_cost REAL DEFAULT 0,
            coverage_amount REAL DEFAULT 0,
            balance REAL DEFAULT 0,
            balance_date TEXT DEFAULT '',
            employee_pct REAL DEFAULT 0,
            employer_pct REAL DEFAULT 0,
            return_rate REAL DEFAULT 0,
            start_date TEXT DEFAULT '',
            renewal_date TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            expense_pattern TEXT DEFAULT '',
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS reminders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL DEFAULT 'תזכורת ייבוא',
            method TEXT NOT NULL DEFAULT 'email',
            destination TEXT NOT NULL DEFAULT '',
            frequency TEXT NOT NULL DEFAULT 'monthly',
            day_of_month INTEGER DEFAULT 1,
            day_of_week INTEGER DEFAULT 0,
            hour INTEGER DEFAULT 9,
            minute INTEGER DEFAULT 0,
            message TEXT DEFAULT '',
            enabled INTEGER DEFAULT 1,
            last_sent TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS category_rules (
            description TEXT PRIMARY KEY,
            category_id TEXT NOT NULL,
            FOREIGN KEY (category_id) REFERENCES categories(id)
        );

        CREATE TABLE IF NOT EXISTS installments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            description TEXT NOT NULL,
            store TEXT DEFAULT '',
            total_amount REAL NOT NULL,
            total_payments INTEGER NOT NULL,
            payments_made INTEGER NOT NULL DEFAULT 0,
            monthly_payment REAL NOT NULL,
            start_date TEXT NOT NULL,
            card TEXT DEFAULT '',
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    ''')

    # Migrate budget table: add plan_id column and fix unique constraint
    cols = [r[1] for r in conn.execute("PRAGMA table_info(budget)").fetchall()]
    if 'plan_id' not in cols:
        conn.execute("ALTER TABLE budget RENAME TO budget_old")
        conn.execute("""
            CREATE TABLE budget (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                category_id TEXT NOT NULL,
                month TEXT NOT NULL,
                planned_amount REAL NOT NULL DEFAULT 0,
                plan_id INTEGER NOT NULL DEFAULT 1,
                UNIQUE(category_id, month, plan_id),
                FOREIGN KEY (category_id) REFERENCES categories(id),
                FOREIGN KEY (plan_id) REFERENCES budget_plans(id)
            )
        """)
        conn.execute("INSERT INTO budget (id, category_id, month, planned_amount, plan_id) SELECT id, category_id, month, planned_amount, 1 FROM budget_old")
        conn.execute("DROP TABLE budget_old")
        conn.commit()

    # Insert default categories if empty
    existing = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
    if existing == 0:
        for i, (cat_id, name_he, color) in enumerate(DEFAULT_CATEGORIES):
            conn.execute(
                "INSERT INTO categories (id, name_he, color, sort_order) VALUES (?, ?, ?, ?)",
                (cat_id, name_he, color, i)
            )
    conn.commit()
    conn.close()


init_db()


# ============================================================
# Authentication
# ============================================================

def hash_password(password):
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return salt + ':' + h.hex()


def verify_password(password, stored):
    salt, h = stored.split(':')
    check = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000)
    return check.hex() == h


def generate_otp():
    return str(random.randint(100000, 999999))


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        # If no users exist yet, allow access (first-time setup)
        if not has_any_users():
            return f(*args, **kwargs)
        if 'user_id' not in session:
            return jsonify({'error': 'Not authenticated'}), 401
        return f(*args, **kwargs)
    return decorated


def has_any_users():
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    conn.close()
    return count > 0


def send_email_otp(to_email, otp_code):
    """Send OTP via email. Returns True on success."""
    if not os.path.exists(SMTP_CONFIG_PATH):
        return False, 'SMTP not configured'
    with open(SMTP_CONFIG_PATH, 'r') as f:
        cfg = json.load(f)
    try:
        msg = MIMEMultipart()
        msg['From'] = cfg['from_email']
        msg['To'] = to_email
        msg['Subject'] = 'Family Budget Tracker - Verification Code'
        body = f"""
        <div dir="rtl" style="font-family:Arial;text-align:center;padding:20px">
            <h2>קוד אימות</h2>
            <p>הקוד שלך להרשמה למערכת ניהול תקציב:</p>
            <div style="font-size:36px;font-weight:bold;letter-spacing:8px;
                        background:#f0f5ff;padding:20px;border-radius:12px;
                        color:#2563eb;margin:20px 0">{otp_code}</div>
            <p style="color:#666">הקוד תקף ל-10 דקות</p>
        </div>
        """
        msg.attach(MIMEText(body, 'html'))
        server = smtplib.SMTP(cfg['smtp_server'], cfg.get('smtp_port', 587))
        server.starttls()
        server.login(cfg['smtp_user'], cfg['smtp_password'])
        server.send_message(msg)
        server.quit()
        return True, 'OK'
    except Exception as e:
        return False, str(e)


def send_sms_otp(phone, otp_code):
    """Send OTP via SMS (Twilio). Returns True on success."""
    if not os.path.exists(SMTP_CONFIG_PATH):
        return False, 'SMS not configured'
    with open(SMTP_CONFIG_PATH, 'r') as f:
        cfg = json.load(f)
    if not cfg.get('twilio_sid'):
        return False, 'Twilio not configured'
    try:
        import http.client
        import urllib.parse
        conn_http = http.client.HTTPSConnection("api.twilio.com")
        data = urllib.parse.urlencode({
            'To': phone,
            'From': cfg['twilio_from'],
            'Body': f'Family Budget Tracker - Your code is: {otp_code}'
        })
        auth = (cfg['twilio_sid'] + ':' + cfg['twilio_token']).encode()
        import base64
        auth_header = 'Basic ' + base64.b64encode(auth).decode()
        conn_http.request('POST',
            f'/2010-04-01/Accounts/{cfg["twilio_sid"]}/Messages.json',
            body=data,
            headers={
                'Authorization': auth_header,
                'Content-Type': 'application/x-www-form-urlencoded'
            })
        resp = conn_http.getresponse()
        if resp.status in (200, 201):
            return True, 'OK'
        return False, resp.read().decode()
    except Exception as e:
        return False, str(e)


# --- Auth Routes ---

@app.route('/auth')
def auth_page():
    """Serve the login/signup page."""
    return send_from_directory('static', 'auth.html')


@app.route('/api/auth/status', methods=['GET'])
def auth_status():
    """Check if user is logged in and if any users exist."""
    has_users = has_any_users()
    logged_in = 'user_id' in session
    username = session.get('username', '')
    return jsonify({
        'has_users': has_users,
        'logged_in': logged_in,
        'username': username,
    })


@app.route('/api/auth/signup', methods=['POST'])
def auth_signup():
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '')
    email = data.get('email', '').strip()
    phone = data.get('phone', '').strip()
    method = data.get('verification_method', 'email')  # 'email' or 'sms'

    if not username or not password:
        return jsonify({'error': 'Username and password required'}), 400
    if len(password) < 6:
        return jsonify({'error': 'Password must be at least 6 characters'}), 400
    if method == 'email' and not email:
        return jsonify({'error': 'Email required for email verification'}), 400
    if method == 'sms' and not phone:
        return jsonify({'error': 'Phone required for SMS verification'}), 400

    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()
    if existing:
        conn.close()
        return jsonify({'error': 'Username already exists'}), 400

    otp = generate_otp()
    otp_expires = (datetime.now() + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')

    # Try to send OTP
    if method == 'email':
        success, msg = send_email_otp(email, otp)
    else:
        success, msg = send_sms_otp(phone, otp)

    if not success:
        # If sending fails, allow skip verification (for local/offline use)
        # Store user as unverified but let them set up verification later
        pass

    pw_hash = hash_password(password)
    conn.execute("""
        INSERT INTO users (username, password_hash, email, phone, verification_method, otp_code, otp_expires)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (username, pw_hash, email, phone, method, otp, otp_expires))
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE username=?", (username,)).fetchone()[0]
    conn.close()

    if not success:
        # Auto-verify if we can't send OTP (offline mode)
        conn = get_db()
        conn.execute("UPDATE users SET verified=1 WHERE id=?", (user_id,))
        conn.commit()
        conn.close()
        session.permanent = True
        session['user_id'] = user_id
        session['username'] = username
        return jsonify({
            'status': 'ok',
            'auto_verified': True,
            'message': f'Verification skipped ({msg}). Account created.',
        })

    return jsonify({
        'status': 'verification_sent',
        'method': method,
        'user_id': user_id,
        'destination': email if method == 'email' else phone[-4:],
    })


@app.route('/api/auth/verify', methods=['POST'])
def auth_verify():
    data = request.json
    user_id = data.get('user_id')
    code = data.get('code', '').strip()

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return jsonify({'error': 'User not found'}), 404

    if user['otp_code'] != code:
        conn.close()
        return jsonify({'error': 'Invalid code'}), 400

    if user['otp_expires'] and datetime.now().strftime('%Y-%m-%d %H:%M:%S') > user['otp_expires']:
        conn.close()
        return jsonify({'error': 'Code expired. Please request a new one.'}), 400

    conn.execute("UPDATE users SET verified=1, otp_code='', otp_expires='' WHERE id=?", (user_id,))
    conn.commit()

    session.permanent = True
    session['user_id'] = user_id
    session['username'] = user['username']
    conn.close()
    return jsonify({'status': 'ok', 'username': user['username']})


@app.route('/api/auth/resend', methods=['POST'])
def auth_resend():
    data = request.json
    user_id = data.get('user_id')

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return jsonify({'error': 'User not found'}), 404

    otp = generate_otp()
    otp_expires = (datetime.now() + timedelta(minutes=10)).strftime('%Y-%m-%d %H:%M:%S')
    conn.execute("UPDATE users SET otp_code=?, otp_expires=? WHERE id=?", (otp, otp_expires, user_id))
    conn.commit()

    if user['verification_method'] == 'email':
        success, msg = send_email_otp(user['email'], otp)
    else:
        success, msg = send_sms_otp(user['phone'], otp)

    conn.close()
    if success:
        return jsonify({'status': 'ok'})
    return jsonify({'error': f'Failed to send: {msg}'}), 500


@app.route('/api/auth/login', methods=['POST'])
def auth_login():
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '')

    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    conn.close()

    if not user or not verify_password(password, user['password_hash']):
        return jsonify({'error': 'Invalid username or password'}), 401

    if not user['verified']:
        return jsonify({'error': 'Account not verified', 'needs_verification': True, 'user_id': user['id']}), 403

    session.permanent = True
    session['user_id'] = user['id']
    session['username'] = user['username']
    return jsonify({'status': 'ok', 'username': user['username']})


@app.route('/api/auth/logout', methods=['POST'])
def auth_logout():
    session.clear()
    return jsonify({'status': 'ok'})


@app.route('/api/auth/smtp-config', methods=['GET'])
def get_smtp_config():
    if os.path.exists(SMTP_CONFIG_PATH):
        with open(SMTP_CONFIG_PATH, 'r') as f:
            cfg = json.load(f)
        # Mask sensitive fields
        return jsonify({
            'configured': True,
            'smtp_server': cfg.get('smtp_server', ''),
            'from_email': cfg.get('from_email', ''),
            'has_twilio': bool(cfg.get('twilio_sid')),
        })
    return jsonify({'configured': False})


@app.route('/api/auth/smtp-config', methods=['POST'])
def set_smtp_config():
    data = request.json
    cfg = {
        'smtp_server': data.get('smtp_server', 'smtp.gmail.com'),
        'smtp_port': int(data.get('smtp_port', 587)),
        'smtp_user': data.get('smtp_user', ''),
        'smtp_password': data.get('smtp_password', ''),
        'from_email': data.get('from_email', ''),
        'twilio_sid': data.get('twilio_sid', ''),
        'twilio_token': data.get('twilio_token', ''),
        'twilio_from': data.get('twilio_from', ''),
    }
    with open(SMTP_CONFIG_PATH, 'w') as f:
        json.dump(cfg, f)
    return jsonify({'status': 'ok'})


# ============================================================
# Reminders
# ============================================================

REMINDER_MESSAGES = {
    'bank': 'תזכורת: הגיע הזמן להוריד את דוח התנועות מהבנק ולייבא אותו למערכת ניהול התקציב.',
    'visa': 'תזכורת: הגיע הזמן להוריד את פירוט כרטיס האשראי ולייבא אותו למערכת ניהול התקציב.',
    'general': 'תזכורת: הגיע הזמן לעדכן את נתוני ההוצאות וההכנסות במערכת ניהול התקציב.',
}


def send_reminder(reminder):
    """Send a reminder via email or SMS. Returns (success, msg)."""
    dest = reminder['destination']
    text = reminder['message'] or REMINDER_MESSAGES.get('general', '')
    name = reminder['name']

    if reminder['method'] == 'email':
        if not os.path.exists(SMTP_CONFIG_PATH):
            return False, 'SMTP not configured'
        with open(SMTP_CONFIG_PATH, 'r') as f:
            cfg = json.load(f)
        try:
            msg = MIMEMultipart()
            msg['From'] = cfg['from_email']
            msg['To'] = dest
            msg['Subject'] = f'Family Budget Tracker - {name}'
            body = f"""
            <div dir="rtl" style="font-family:Arial;text-align:center;padding:20px">
                <h2>⏰ {name}</h2>
                <p style="font-size:16px;line-height:1.8">{text}</p>
                <div style="margin-top:20px;padding:15px;background:#f0f5ff;border-radius:12px">
                    <p style="color:#64748b;font-size:14px">
                        💡 היכנס לאתר הבנק או חברת האשראי, הורד את הדוח, ולחץ על "ייבוא" באפליקציה.
                    </p>
                </div>
            </div>
            """
            msg.attach(MIMEText(body, 'html'))
            server = smtplib.SMTP(cfg['smtp_server'], cfg.get('smtp_port', 587))
            server.starttls()
            server.login(cfg['smtp_user'], cfg['smtp_password'])
            server.sendmail(cfg['from_email'], dest, msg.as_string())
            server.quit()
            return True, 'Email sent'
        except Exception as e:
            return False, str(e)
    elif reminder['method'] == 'sms':
        return send_sms_otp(dest, text)
    return False, 'Unknown method'


def reminder_scheduler():
    """Background thread: check every 60s if any reminder should fire."""
    while True:
        _time.sleep(60)
        try:
            now = datetime.now()
            conn = get_db()
            reminders = conn.execute("SELECT * FROM reminders WHERE enabled=1").fetchall()
            for r in reminders:
                should_send = False
                last = r['last_sent']
                today_str = now.strftime('%Y-%m-%d')

                # Skip if already sent today
                if last and last[:10] == today_str:
                    continue

                # Check time
                if now.hour < r['hour'] or (now.hour == r['hour'] and now.minute < r['minute']):
                    continue

                freq = r['frequency']
                if freq == 'daily':
                    should_send = True
                elif freq == 'weekly':
                    if now.weekday() == (r['day_of_week'] or 0):
                        should_send = True
                elif freq == 'monthly':
                    if now.day == (r['day_of_month'] or 1):
                        should_send = True
                elif freq == 'biweekly':
                    # Send on 1st and 15th
                    if now.day in (1, 15):
                        should_send = True

                if should_send:
                    success, msg = send_reminder(dict(r))
                    if success:
                        conn.execute(
                            "UPDATE reminders SET last_sent=? WHERE id=?",
                            (now.strftime('%Y-%m-%d %H:%M:%S'), r['id'])
                        )
                        conn.commit()
            conn.close()
        except Exception:
            pass


# Start scheduler thread
_scheduler_thread = threading.Thread(target=reminder_scheduler, daemon=True)
_scheduler_thread.start()


@app.route('/api/reminders', methods=['GET'])
@login_required
def get_reminders():
    conn = get_db()
    rows = conn.execute("SELECT * FROM reminders ORDER BY created_at DESC").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/reminders', methods=['POST'])
@login_required
def add_reminder():
    data = request.json
    conn = get_db()
    conn.execute("""
        INSERT INTO reminders (name, method, destination, frequency, day_of_month, day_of_week, hour, minute, message, enabled)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        data.get('name', 'תזכורת ייבוא'),
        data.get('method', 'email'),
        data.get('destination', ''),
        data.get('frequency', 'monthly'),
        data.get('day_of_month', 1),
        data.get('day_of_week', 0),
        data.get('hour', 9),
        data.get('minute', 0),
        data.get('message', ''),
        1
    ))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/reminders/<int:rid>', methods=['PUT'])
@login_required
def update_reminder(rid):
    data = request.json
    conn = get_db()
    conn.execute("""
        UPDATE reminders SET name=?, method=?, destination=?, frequency=?,
        day_of_month=?, day_of_week=?, hour=?, minute=?, message=?, enabled=?
        WHERE id=?
    """, (
        data.get('name', ''),
        data.get('method', 'email'),
        data.get('destination', ''),
        data.get('frequency', 'monthly'),
        data.get('day_of_month', 1),
        data.get('day_of_week', 0),
        data.get('hour', 9),
        data.get('minute', 0),
        data.get('message', ''),
        data.get('enabled', 1),
        rid
    ))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/reminders/<int:rid>', methods=['DELETE'])
@login_required
def delete_reminder(rid):
    conn = get_db()
    conn.execute("DELETE FROM reminders WHERE id=?", (rid,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/reminders/<int:rid>/test', methods=['POST'])
@login_required
def test_reminder(rid):
    """Send a test reminder immediately."""
    conn = get_db()
    r = conn.execute("SELECT * FROM reminders WHERE id=?", (rid,)).fetchone()
    conn.close()
    if not r:
        return jsonify({'error': 'Reminder not found'}), 404
    success, msg = send_reminder(dict(r))
    if success:
        return jsonify({'status': 'ok', 'message': msg})
    return jsonify({'error': msg}), 500


# --- Static files ---
@app.route('/')
def index():
    if has_any_users() and 'user_id' not in session:
        return redirect('/auth')
    resp = send_from_directory('static', 'index.html')
    resp.headers['Cache-Control'] = 'no-cache, no-store, must-revalidate'
    return resp


@app.route('/static/<path:path>')
def static_files(path):
    return send_from_directory('static', path)


# --- Categories API ---
@app.route('/api/categories', methods=['GET'])
@login_required
def get_categories():
    conn = get_db()
    rows = conn.execute("SELECT * FROM categories ORDER BY sort_order").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/categories', methods=['POST'])
@login_required
def add_category():
    data = request.json
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO categories (id, name_he, color, sort_order) VALUES (?, ?, ?, ?)",
        (data['id'], data['name_he'], data.get('color', '#888888'), data.get('sort_order', 99))
    )
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


# --- Standing Orders API ---
@app.route('/api/standing-orders', methods=['GET'])
@login_required
def get_standing_orders():
    """Return latest occurrence of each monthly expense (grouped by description)."""
    conn = get_db()
    rows = conn.execute("""
        SELECT e.description, e.category_id, c.name_he as category_name,
               c.color as category_color, e.card, e.amount, MAX(e.date) as last_date
        FROM expenses e
        JOIN categories c ON e.category_id = c.id
        WHERE e.frequency = 'monthly'
        GROUP BY e.description
        ORDER BY e.amount DESC
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


# --- Expenses API ---
@app.route('/api/expenses', methods=['GET'])
@login_required
def get_expenses():
    conn = get_db()
    month = request.args.get('month')  # format: YYYY-MM
    if month:
        rows = conn.execute(
            """SELECT e.*, c.name_he as category_name, c.color as category_color
               FROM expenses e JOIN categories c ON e.category_id = c.id
               WHERE e.date LIKE ? ORDER BY e.date DESC""",
            (month + '%',)
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT e.*, c.name_he as category_name, c.color as category_color
               FROM expenses e JOIN categories c ON e.category_id = c.id
               ORDER BY e.date DESC LIMIT 500"""
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/expenses', methods=['POST'])
@login_required
def add_expense():
    data = request.json
    conn = get_db()
    conn.execute(
        "INSERT INTO expenses (date, category_id, subcategory, description, amount, source, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (data['date'], data['category_id'], data.get('subcategory', ''),
         data.get('description', ''), data['amount'], data.get('source', 'manual'),
         data.get('frequency', 'random'))
    )
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/expenses/<int:expense_id>', methods=['PUT'])
@login_required
def update_expense(expense_id):
    data = request.json
    conn = get_db()
    fields = []
    values = []
    for col in ('date', 'category_id', 'subcategory', 'description', 'amount', 'frequency', 'card'):
        if col in data:
            fields.append(f"{col}=?")
            values.append(data[col])
    if not fields:
        conn.close()
        return jsonify({'error': 'No fields to update'}), 400
    values.append(expense_id)
    conn.execute(f"UPDATE expenses SET {','.join(fields)} WHERE id=?", values)

    # If category changed, update ALL expenses with the same description and save a rule
    if 'category_id' in data:
        exp = conn.execute("SELECT description FROM expenses WHERE id=?", (expense_id,)).fetchone()
        if exp and exp['description']:
            desc = exp['description']
            conn.execute(
                "UPDATE expenses SET category_id=? WHERE description=?",
                (data['category_id'], desc)
            )
            conn.execute(
                "INSERT OR REPLACE INTO category_rules (description, category_id) VALUES (?, ?)",
                (desc, data['category_id'])
            )

    # If frequency changed, update ALL expenses with the same description
    if 'frequency' in data:
        exp = conn.execute("SELECT description FROM expenses WHERE id=?", (expense_id,)).fetchone()
        if exp and exp['description']:
            conn.execute(
                "UPDATE expenses SET frequency=? WHERE description=?",
                (data['frequency'], exp['description'])
            )

    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/expenses/<int:expense_id>', methods=['DELETE'])
@login_required
def delete_expense(expense_id):
    conn = get_db()
    conn.execute("DELETE FROM expenses WHERE id = ?", (expense_id,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


# --- Income API ---
@app.route('/api/income', methods=['GET'])
@login_required
def get_income():
    conn = get_db()
    month = request.args.get('month')
    if month:
        rows = conn.execute(
            "SELECT * FROM income WHERE date LIKE ? ORDER BY date DESC",
            (month + '%',)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM income ORDER BY date DESC LIMIT 200").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/income', methods=['POST'])
@login_required
def add_income():
    data = request.json
    conn = get_db()
    conn.execute(
        "INSERT INTO income (date, person, source, amount, description, is_recurring) VALUES (?, ?, ?, ?, ?, ?)",
        (data['date'], data['person'], data['source'], data['amount'],
         data.get('description', ''), data.get('is_recurring', 0))
    )
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/income/<int:income_id>', methods=['DELETE'])
@login_required
def delete_income(income_id):
    conn = get_db()
    conn.execute("DELETE FROM income WHERE id = ?", (income_id,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


# --- Budget API ---
@app.route('/api/available-months', methods=['GET'])
@login_required
def get_available_months():
    """Return all months that have expense data, sorted ascending."""
    conn = get_db()
    rows = conn.execute(
        "SELECT DISTINCT substr(date, 1, 7) as month FROM expenses ORDER BY month"
    ).fetchall()
    conn.close()
    return jsonify([r['month'] for r in rows])


@app.route('/api/category-averages', methods=['GET'])
@login_required
def get_category_averages():
    """Return monthly average expense per category. Optional ?from=YYYY-MM filter."""
    conn = get_db()
    from_month = request.args.get('from')
    if from_month and from_month != 'all':
        rows = conn.execute("""
            SELECT e.category_id,
                   SUM(e.amount) as total,
                   COUNT(DISTINCT substr(e.date, 1, 7)) as months
            FROM expenses e
            WHERE substr(e.date, 1, 7) >= ?
            GROUP BY e.category_id
        """, (from_month,)).fetchall()
    else:
        rows = conn.execute("""
            SELECT e.category_id,
                   SUM(e.amount) as total,
                   COUNT(DISTINCT substr(e.date, 1, 7)) as months
            FROM expenses e
            GROUP BY e.category_id
        """).fetchall()
    conn.close()
    result = {}
    for r in rows:
        months = max(r['months'], 1)
        result[r['category_id']] = round(r['total'] / months, 0)
    return jsonify(result)


@app.route('/api/budget', methods=['GET'])
@login_required
def get_budget():
    conn = get_db()
    month = request.args.get('month')
    plan_id = request.args.get('plan', '1')
    if month:
        rows = conn.execute(
            """SELECT b.*, c.name_he, c.color FROM budget b
               JOIN categories c ON b.category_id = c.id
               WHERE b.month = ? AND b.plan_id = ? ORDER BY c.sort_order""",
            (month, plan_id)
        ).fetchall()
    else:
        rows = conn.execute(
            """SELECT b.*, c.name_he, c.color FROM budget b
               JOIN categories c ON b.category_id = c.id
               WHERE b.plan_id = ?
               ORDER BY b.month DESC, c.sort_order""",
            (plan_id,)
        ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/budget', methods=['POST'])
@login_required
def set_budget():
    data = request.json
    plan_id = data.get('plan_id', 1)
    conn = get_db()
    conn.execute(
        """INSERT INTO budget (category_id, month, planned_amount, plan_id)
           VALUES (?, ?, ?, ?)
           ON CONFLICT(category_id, month, plan_id)
           DO UPDATE SET planned_amount = excluded.planned_amount""",
        (data['category_id'], data['month'], data['planned_amount'], plan_id)
    )
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/budget-plans', methods=['GET'])
@login_required
def get_budget_plans():
    conn = get_db()
    rows = conn.execute("SELECT * FROM budget_plans ORDER BY id").fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.route('/api/budget-plans', methods=['POST'])
@login_required
def save_budget_plan():
    data = request.json
    conn = get_db()
    count = conn.execute("SELECT COUNT(*) FROM budget_plans").fetchone()[0]
    if data.get('id'):
        conn.execute("UPDATE budget_plans SET name = ? WHERE id = ?", (data['name'], data['id']))
    elif count < 3:
        new_id = count + 1
        conn.execute("INSERT INTO budget_plans (id, name) VALUES (?, ?)", (new_id, data['name']))
    else:
        conn.close()
        return jsonify({'error': 'max 3 plans'}), 400
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/budget-plans/<int:plan_id>', methods=['DELETE'])
@login_required
def delete_budget_plan(plan_id):
    if plan_id == 1:
        return jsonify({'error': 'cannot delete default plan'}), 400
    conn = get_db()
    conn.execute("DELETE FROM budget WHERE plan_id = ?", (plan_id,))
    conn.execute("DELETE FROM budget_plans WHERE id = ?", (plan_id,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


# --- Dashboard summary ---
@app.route('/api/summary', methods=['GET'])
@login_required
def get_summary():
    conn = get_db()
    month = request.args.get('month', date.today().strftime('%Y-%m'))

    # Expenses by category
    cat_expenses = conn.execute(
        """SELECT c.id, c.name_he, c.color, COALESCE(SUM(e.amount), 0) as total
           FROM categories c LEFT JOIN expenses e ON c.id = e.category_id AND e.date LIKE ?
           GROUP BY c.id ORDER BY total DESC""",
        (month + '%',)
    ).fetchall()

    # Daily expenses for the month
    daily = conn.execute(
        """SELECT date, SUM(amount) as total FROM expenses
           WHERE date LIKE ? GROUP BY date ORDER BY date""",
        (month + '%',)
    ).fetchall()

    # Total income for the month
    income_total = conn.execute(
        "SELECT COALESCE(SUM(amount), 0) as total FROM income WHERE date LIKE ?",
        (month + '%',)
    ).fetchone()['total']

    # Income by person
    income_by_person = conn.execute(
        """SELECT person, SUM(amount) as total FROM income
           WHERE date LIKE ? GROUP BY person""",
        (month + '%',)
    ).fetchall()

    # Budget vs actual
    plan_id = request.args.get('plan', '1')
    budget_vs_actual = conn.execute(
        """SELECT c.id, c.name_he, c.color,
                  COALESCE(b.planned_amount, 0) as planned,
                  COALESCE(SUM(e.amount), 0) as actual
           FROM categories c
           LEFT JOIN budget b ON c.id = b.category_id AND b.month = ? AND b.plan_id = ?
           LEFT JOIN expenses e ON c.id = e.category_id AND e.date LIKE ?
           GROUP BY c.id
           HAVING planned > 0 OR actual > 0
           ORDER BY c.sort_order""",
        (month, plan_id, month + '%')
    ).fetchall()

    # Monthly trend (last 6 months)
    monthly_trend = conn.execute(
        """SELECT substr(date, 1, 7) as month, SUM(amount) as total
           FROM expenses GROUP BY substr(date, 1, 7)
           ORDER BY month DESC LIMIT 6"""
    ).fetchall()

    # Expenses by card
    by_card = conn.execute(
        """SELECT CASE WHEN card = '' THEN 'אחר' ELSE card END as card_name,
                  SUM(amount) as total, COUNT(*) as count
           FROM expenses WHERE date LIKE ?
           GROUP BY card_name ORDER BY total DESC""",
        (month + '%',)
    ).fetchall()

    # Expenses by frequency
    by_frequency = conn.execute(
        """SELECT frequency, SUM(amount) as total, COUNT(*) as count
           FROM expenses WHERE date LIKE ?
           GROUP BY frequency""",
        (month + '%',)
    ).fetchall()

    expense_total = sum(r['total'] for r in cat_expenses)

    conn.close()
    return jsonify({
        'month': month,
        'expense_total': expense_total,
        'income_total': income_total,
        'balance': income_total - expense_total,
        'by_category': [dict(r) for r in cat_expenses if r['total'] > 0],
        'daily': [dict(r) for r in daily],
        'income_by_person': [dict(r) for r in income_by_person],
        'budget_vs_actual': [dict(r) for r in budget_vs_actual],
        'monthly_trend': [dict(r) for r in reversed(list(monthly_trend))],
        'by_card': [dict(r) for r in by_card],
        'by_frequency': [dict(r) for r in by_frequency],
    })


# --- File Import ---
@app.route('/api/import', methods=['POST'])
@login_required
def import_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file uploaded'}), 400

    file = request.files['file']
    if not file.filename:
        return jsonify({'error': 'No file selected'}), 400

    filepath = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(filepath)

    try:
        if file.filename.endswith('.xls'):
            result = parse_budget_xls(filepath)
        elif file.filename.endswith('.xlsx'):
            result = parse_visa_xlsx(filepath)
        elif file.filename.endswith('.csv'):
            result = parse_bank_csv(filepath)
        else:
            return jsonify({'error': 'Unsupported file format. Use .xls, .xlsx or .csv'}), 400
        return jsonify(result)
    except Exception as e:
        return jsonify({'error': str(e)}), 400


def parse_budget_xls(filepath):
    """Parse the Hebrew budget XLS format and import expenses."""
    wb = xlrd.open_workbook(filepath)
    sheet = wb.sheets()[0]
    conn = get_db()

    imported = 0
    current_category = None
    category_id = None

    # Try to extract month from cell B1 (Excel date serial)
    month_str = None
    try:
        date_val = sheet.cell_value(1, 1)
        if date_val and isinstance(date_val, float):
            dt = xlrd.xldate_as_datetime(date_val, wb.datemode)
            month_str = dt.strftime('%Y-%m')
    except Exception:
        pass

    if not month_str:
        month_str = date.today().strftime('%Y-%m')

    # Category row markers (col 0 values that indicate category headers)
    category_markers = {
        'דיור': 'housing',
        'מזון': 'food',
        'הורים': 'parents',
        'ילדים': 'children',
        'רכב': 'vehicle',
        'תקשורת': 'communication',
        'טיפוח': 'health_beauty',
        'ריפוי': 'medical',
        'ביטוחים': 'insurance',
        'בילוי': 'entertainment',
        'אישי': 'personal',
        'חיסכון': 'savings',
        'תשלומים': 'misc',
        'נוספים': 'misc',
    }

    for row in range(2, sheet.nrows):
        col0 = str(sheet.cell_value(row, 0)).strip()
        col1 = str(sheet.cell_value(row, 1)).strip()

        # Check if this is a category header row
        if col0 and not col0.startswith('סה'):
            for marker, cat_id in category_markers.items():
                if marker in col0:
                    current_category = col0
                    category_id = cat_id
                    break

        # Skip header/summary rows
        if col0.startswith('סה') or col0 == 'סוג ההוצאה' or not category_id:
            continue

        # Extract amounts from weekly columns (actual columns: 3, 6, 9, 12)
        subcategory = col1 if col1 else current_category
        week_cols = [(3, 1), (6, 8), (9, 16), (12, 24)]  # (col_index, day_start)

        for amount_col, day_start in week_cols:
            try:
                amount = float(sheet.cell_value(row, amount_col))
                if amount > 0:
                    # Use middle of the week as the date
                    day = min(day_start + 3, 28)
                    expense_date = f"{month_str}-{day:02d}"
                    conn.execute(
                        """INSERT INTO expenses (date, category_id, subcategory, description, amount, source)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (expense_date, category_id, subcategory, f'Imported from XLS', amount, 'xls_import')
                    )
                    imported += 1
            except (ValueError, TypeError, IndexError):
                continue

    conn.commit()
    conn.close()
    return {'status': 'ok', 'imported': imported, 'month': month_str}


# Visa category (ענף) to our category mapping
VISA_CATEGORY_MAP = {
    'מזון ומשקאות': 'food',
    'מזון מהיר': 'food',
    'מסעדות': 'food',
    'סופרמרקט': 'food',
    'אופנה': 'personal',
    'הלבשה': 'personal',
    'פנאי בילוי': 'entertainment',
    'פנאי ובילוי': 'entertainment',
    'אנרגיה': 'vehicle',
    'דלק': 'vehicle',
    'רכב ותחבורה': 'vehicle',
    'חינוך': 'children',
    'תקשורת ומחשבים': 'communication',
    'תקשורת': 'communication',
    'בריאות': 'medical',
    'קוסמטיקה': 'health_beauty',
    'ביטוח': 'insurance',
    'ביטוח ופיננסים': 'insurance',
    'ממשלה ומוניציפלי': 'housing',
    'מוסדות': 'housing',
    'שירותים': 'misc',
    'שונות': 'misc',
    'ציוד ומשרד': 'personal',
    'ריהוט ובית': 'housing',
    'ריהוט': 'housing',
    'בית וגן': 'housing',
    'ספורט': 'entertainment',
    'נסיעות ותיירות': 'entertainment',
    'תיירות': 'entertainment',
    'מלונאות ואירוח': 'entertainment',
    'טיפוח ויופי': 'health_beauty',
    'רפואה ובריאות': 'medical',
    'ילדים': 'children',
}


def parse_visa_xlsx(filepath):
    """Parse Visa credit card XLSX export and import expenses."""
    import re
    wb = openpyxl.load_workbook(filepath)
    sheet = wb.active
    conn = get_db()

    imported = 0
    skipped = 0

    # Detect card number from row 1 or filename
    card_label = ''
    row1 = str(sheet.cell(1, 1).value or '')
    card_match = re.search(r'(\d{4})\s*$', row1)
    if card_match:
        card_label = 'ויזה ' + card_match.group(1)
    else:
        fname_match = re.search(r'(\d{4})', os.path.basename(filepath))
        if fname_match:
            card_label = 'ויזה ' + fname_match.group(1)

    # Find the header row (contains 'תאריך')
    data_start = None
    for row in range(1, min(sheet.max_row + 1, 10)):
        val = str(sheet.cell(row, 1).value or '')
        if 'תאריך' in val:
            data_start = row + 1
            break

    # If no header found, data likely starts at row 5 or 6
    if data_start is None:
        for row in range(4, 8):
            val = sheet.cell(row, 1).value
            if val and hasattr(val, 'strftime'):
                data_start = row
                break

    if data_start is None:
        data_start = 6

    for row in range(data_start, sheet.max_row + 1):
        date_val = sheet.cell(row, 1).value
        business = sheet.cell(row, 2).value
        amount_val = sheet.cell(row, 4).value or sheet.cell(row, 3).value  # prefer charge amount
        visa_category = str(sheet.cell(row, 6).value or '').strip()

        if not date_val or not amount_val:
            continue

        # Parse date
        if hasattr(date_val, 'strftime'):
            expense_date = date_val.strftime('%Y-%m-%d')
        else:
            continue

        # Parse amount
        try:
            amount = float(str(amount_val).replace(',', ''))
            if amount <= 0:
                continue
        except (ValueError, TypeError):
            continue

        # Map visa category to our category
        category_id = 'misc'
        for visa_key, cat_id in VISA_CATEGORY_MAP.items():
            if visa_key in visa_category:
                category_id = cat_id
                break

        description = str(business or '').strip()
        subcategory = visa_category

        # If still misc, try to reclassify by description
        if category_id == 'misc' and description:
            for pattern, cat_id, subcat in VISA_DESCRIPTION_MAP:
                if pattern in description:
                    category_id = cat_id
                    subcategory = subcat
                    break

        # Apply user's saved category/frequency rules
        category_id, freq = apply_category_rule(conn, description, category_id)

        conn.execute(
            """INSERT INTO expenses (date, category_id, subcategory, description, amount, source, card, frequency)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (expense_date, category_id, subcategory, description, amount, 'visa_import', card_label, freq)
        )
        imported += 1

    conn.commit()
    conn.close()
    return {'status': 'ok', 'imported': imported, 'source': 'visa'}


# ---- Bank CSV Import ----

# Patterns to SKIP (already covered by Visa/credit card imports)
BANK_SKIP_PATTERNS = [
    'חיוב לכרטיס ויזה',
    'זיכוי הנחות מפתח מכרטיס',
    'החזרת חיוב דיסקונט למשכנתאות',  # mortgage charge-back (paired with charge)
]

# Income patterns: (pattern_in_description, person, source, is_recurring)
BANK_INCOME_PATTERNS = [
    ('בנק פועלים משכורת', 'husband', 'salary', 1),
    ('בנק לאומי משכורת', 'wife', 'salary', 1),
    ('ביטוח לאומי - ילדים', 'family', 'child_allowance', 1),
    ('בטוח לאומי', 'family', 'child_allowance', 0),
    ('מס-הכנסה ה', 'family', 'other', 0),
    ('העברה מגל נעמי', 'wife', 'other', 0),
    ('העברה מליגת כדורסל', 'husband', 'other', 0),
    ('העברה מר.ס.ג.ר', 'family', 'other', 0),
    ('קבלת תשלום על יתרת זכות', 'family', 'other', 0),
    ('החזרת חיוב ישראכרט', 'family', 'other', 0),
    ('החזרת חיוב הראל', 'family', 'other', 0),
    ('מור גמל ופ', 'husband', 'other', 0),  # positive = withdrawal from provident
]

# Expense patterns: (pattern, category_id, subcategory, frequency)
# frequency: 'monthly', 'bimonthly', 'random'
BANK_EXPENSE_PATTERNS = [
    ('דסק-משכנתא', 'housing', 'משכנתא', 'monthly'),
    ('לועד מקומי', 'housing', 'ועד מקומי', 'monthly'),
    ('לגל נעמי', 'housing', 'העברה קבועה', 'monthly'),
    ('מ.א. חוף ה', 'housing', 'ארנונה', 'bimonthly'),
    ('הראל פנסיה', 'savings', 'פנסיה הראל', 'monthly'),
    ('הראל בטוח', 'insurance', 'ביטוח הראל', 'monthly'),
    ('הראלהלואה', 'savings', 'הלוואה הראל', 'monthly'),
    ('כלל חיים/ב', 'insurance', 'ביטוח חיים כלל', 'monthly'),
    ('חיובי הלוו', 'savings', 'הלוואה', 'monthly'),
    ('דיינרס קלו', 'misc', 'כרטיס דיינרס', 'monthly'),
    ('ישראכרט חיוב', 'misc', 'כרטיס ישראכרט', 'monthly'),
    ('ישראכרט גביה', 'misc', 'כרטיס ישראכרט', 'monthly'),
    ('ישראכרט', 'misc', 'כרטיס ישראכרט', 'monthly'),
    ('השתלמות אג', 'savings', 'קרן השתלמות', 'monthly'),
    ('מור גמל ופ חיוב', 'savings', 'גמל מור', 'monthly'),
    ('ריבית על משיכת יתר', 'misc', 'ריבית מינוס', 'monthly'),
    ('תשלום מס במקור', 'misc', 'מס במקור', 'monthly'),
    ('כספומט', 'misc', 'משיכת מזומן', 'random'),
    ('מש\' מכספומט', 'misc', 'משיכת מזומן', 'random'),
    ('משיכה מכספומט', 'misc', 'משיכת מזומן', 'random'),
    ('משיכת שיק', 'misc', 'שיק', 'random'),
    ('עמלת פנקסי', 'misc', 'עמלת בנק', 'random'),
    ('עמלת החזר', 'misc', 'עמלת בנק', 'random'),
    ('העברה לגל', 'housing', 'העברה משפחתית', 'random'),
    ('הע. ל', 'misc', 'העברה', 'random'),
    ('העברה ל', 'misc', 'העברה', 'random'),
    ('דמי כרטיס', 'misc', 'עמלת בנק', 'monthly'),
]

# Extra description-based classification for Visa "שונות" (misc) entries
VISA_DESCRIPTION_MAP = [
    ('NETFLIX', 'entertainment', 'נטפליקס'),
    ('Netflix', 'entertainment', 'נטפליקס'),
    ('AMAZON', 'personal', 'אמזון'),
    ('חניון', 'vehicle', 'חניה'),
    ('חניה', 'vehicle', 'חניה'),
    ('חצב חניון', 'vehicle', 'חניה'),
    ('מנהרות', 'vehicle', 'מנהרות'),
    ('פנגו', 'vehicle', 'חניה'),
    ('מוביט', 'vehicle', 'תחבורה'),
    ('מי חוף הכרמל', 'housing', 'מים'),
    ('מועצה אזורית', 'housing', 'מועצה אזורית'),
    ('המועצה האזורית', 'housing', 'מועצה אזורית'),
    ('BIT', 'misc', 'העברות BIT'),
    ('PAYBOX', 'misc', 'העברות PAYBOX'),
    ('העברה ב BIT', 'misc', 'העברות BIT'),
    ('דמי כרטיס', 'misc', 'עמלת בנק'),
    ('PAYPAL', 'personal', 'PayPal'),
    ('צומת ספרים', 'children', 'ספרים'),
    ('סיגטס', 'children', 'ילדים'),
    ('כפר נופש', 'entertainment', 'חופשה'),
    ('אקספו', 'entertainment', 'אירועים'),
    ('ספלנדו', 'personal', 'מתנות'),
    ('משקי רם', 'housing', 'משקי רם'),
]


def parse_bank_csv(filepath):
    """Parse Israeli bank CSV (עובר ושב) and import expenses + income."""
    # Try different encodings
    content = None
    for enc in ('utf-8-sig', 'utf-8', 'windows-1255', 'iso-8859-8'):
        try:
            with open(filepath, 'r', encoding=enc) as f:
                content = f.read()
            break
        except (UnicodeDecodeError, UnicodeError):
            continue

    if not content:
        return {'error': 'Could not decode CSV file'}

    reader = csv.reader(io.StringIO(content))
    header = next(reader)  # skip header row

    conn = get_db()
    imported_expenses = 0
    imported_income = 0
    skipped_visa = 0
    skipped_other = 0

    for row in reader:
        if len(row) < 5:
            continue

        date_str = row[0].strip().strip('"')
        description = row[2].strip().strip('"')
        amount_str = row[3].strip().strip('"').replace(',', '')

        if not date_str or not amount_str:
            continue

        # Parse date (DD/MM/YY format)
        try:
            parts = date_str.split('/')
            day, month_num, year = int(parts[0]), int(parts[1]), int(parts[2])
            if year < 100:
                year += 2000
            expense_date = f"{year}-{month_num:02d}-{day:02d}"
        except (ValueError, IndexError):
            continue

        # Parse amount
        try:
            amount = float(amount_str)
        except ValueError:
            continue

        # Skip zero amounts
        if amount == 0:
            continue

        # Check if should skip (Visa charges already imported)
        should_skip = False
        for pattern in BANK_SKIP_PATTERNS:
            if pattern in description:
                should_skip = True
                skipped_visa += 1
                break

        if should_skip:
            continue

        # POSITIVE amounts = income
        if amount > 0:
            person = 'family'
            source = 'other'
            is_recurring = 0

            for pattern, p, s, rec in BANK_INCOME_PATTERNS:
                if pattern in description:
                    person = p
                    source = s
                    is_recurring = rec
                    break

            conn.execute(
                """INSERT INTO income (date, person, source, amount, description, is_recurring)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (expense_date, person, source, amount, description, is_recurring)
            )
            imported_income += 1

        # NEGATIVE amounts = expenses
        else:
            abs_amount = abs(amount)
            category_id = 'misc'
            subcategory = ''
            frequency = 'random'

            matched = False
            for pattern, cat, subcat, freq in BANK_EXPENSE_PATTERNS:
                if pattern in description:
                    category_id = cat
                    subcategory = subcat
                    frequency = freq
                    matched = True
                    break

            if not matched:
                subcategory = description
                skipped_other += 1

            # Apply user's saved category/frequency rules
            category_id, frequency = apply_category_rule(conn, description, category_id, frequency)

            conn.execute(
                """INSERT INTO expenses (date, category_id, subcategory, description, amount, source, frequency, card)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (expense_date, category_id, subcategory, description, abs_amount, 'bank_csv', frequency, 'בנק דיסקונט')
            )
            imported_expenses += 1

    conn.commit()
    conn.close()
    return {
        'status': 'ok',
        'imported_expenses': imported_expenses,
        'imported_income': imported_income,
        'skipped_visa': skipped_visa,
        'skipped_unmatched': skipped_other,
        'source': 'bank_csv'
    }


# ---- Budget Tips & Insights ----

@app.route('/api/tips', methods=['GET'])
@login_required
def get_tips():
    conn = get_db()
    month = request.args.get('month', date.today().strftime('%Y-%m'))
    tips = []

    # Helper: get total months of data
    months_list = [r[0] for r in conn.execute(
        "SELECT DISTINCT substr(date,1,7) FROM expenses ORDER BY substr(date,1,7)"
    ).fetchall()]
    num_months = max(len(months_list), 1)

    # Current month data
    exp_total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE date LIKE ?", (month+'%',)
    ).fetchone()[0]
    inc_total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM income WHERE date LIKE ?", (month+'%',)
    ).fetchone()[0]

    # ---- TIP 1: Overdraft interest ----
    overdraft = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE subcategory='ריבית מינוס'"
    ).fetchone()[0] or 0
    if overdraft > 0:
        monthly_avg = overdraft / num_months
        yearly_est = monthly_avg * 12
        tips.append({
            'id': 'overdraft',
            'icon': 'bi-exclamation-triangle-fill',
            'color': '#dc2626',
            'title': 'ריבית מינוס - כסף שנזרק לפח',
            'summary': f'שילמתם {overdraft:,.0f} ש"ח ריבית על מינוס. זה כ-{yearly_est:,.0f} ש"ח בשנה!',
            'detail': 'ריבית מינוס היא הוצאה שאפשר לבטל לחלוטין. כמה אפשרויות:\n\n'
                      '1. **הלוואה אישית** - ריבית נמוכה בהרבה ממינוס (4-6% במקום 12-18%)\n'
                      '2. **העברת חלק מהחיסכון** - אם יש כסף בקרנות/חסכונות, שחרור חלק יחסוך את הריבית\n'
                      '3. **תזמון הוצאות** - לדחות הוצאות גדולות לאחרי קבלת משכורת\n'
                      f'4. **חיסכון שנתי פוטנציאלי: {yearly_est:,.0f} ש"ח**',
            'savings': yearly_est,
            'priority': 'high',
        })

    # ---- TIP 2: Cash withdrawals - untracked spending ----
    cash = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE subcategory='משיכת מזומן'"
    ).fetchone()[0] or 0
    if cash > 500:
        monthly_cash = cash / num_months
        tips.append({
            'id': 'cash',
            'icon': 'bi-cash',
            'color': '#f59e0b',
            'title': 'הוצאות מזומן - הכסף "נעלם"',
            'summary': f'משכתם {cash:,.0f} ש"ח מזומן ({monthly_cash:,.0f} ש"ח/חודש). אי אפשר לעקוב לאן הכסף הולך.',
            'detail': 'מזומן הוא האויב של תקציב מסודר - אי אפשר לעקוב אחריו.\n\n'
                      '1. **עברו לתשלום בכרטיס/ביט** - כל הוצאה תתועד אוטומטית\n'
                      '2. **הגדירו תקרת מזומן** - לא יותר מ-500 ש"ח בחודש\n'
                      '3. **רשמו הוצאות מזומן** - תפתחו הערה בנייד ותרשמו מיד\n'
                      f'4. **חיסכון פוטנציאלי: {monthly_cash*0.3:,.0f} ש"ח/חודש** (30% מהמזומן בד"כ הולך על דברים לא הכרחיים)',
            'savings': monthly_cash * 0.3 * 12,
            'priority': 'medium',
        })

    # ---- TIP 3: Multiple credit cards ----
    diners = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE subcategory LIKE '%דיינרס%'"
    ).fetchone()[0] or 0
    isracard = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE subcategory LIKE '%ישראכרט%'"
    ).fetchone()[0] or 0
    if diners > 0 and isracard > 0:
        tips.append({
            'id': 'cards',
            'icon': 'bi-credit-card-2-back',
            'color': '#8b5cf6',
            'title': 'יותר מדי כרטיסי אשראי',
            'summary': f'יש לכם ויזה, ישראכרט ({isracard:,.0f} ש"ח) ודיינרס ({diners:,.0f} ש"ח). ריכוז יחסוך כסף.',
            'detail': 'ריבוי כרטיסים = דמי כרטיס כפולים + קושי לעקוב אחרי הוצאות.\n\n'
                      '1. **בטלו כרטיסים מיותרים** - דיינרס וישראכרט עולים 10-20 ש"ח/חודש כל אחד\n'
                      '2. **רכזו הכל בויזה אחת** - קל יותר לעקוב, יותר נקודות/הטבות\n'
                      '3. **בדקו הטבות** - לפעמים כרטיס אחד נותן cashback טוב יותר\n'
                      '4. **חיסכון: 240-480 ש"ח/שנה** בדמי כרטיס + שליטה טובה יותר',
            'savings': 480,
            'priority': 'medium',
        })

    # ---- TIP 4: Food spending analysis ----
    food_total = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE category_id='food'"
    ).fetchone()[0] or 0
    food_monthly = food_total / num_months if num_months else 0
    restaurants = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE category_id='food' AND (subcategory LIKE '%מסעד%' OR subcategory LIKE '%מזון מהיר%')"
    ).fetchone()[0] or 0
    if food_monthly > 2000:
        rest_pct = (restaurants / food_total * 100) if food_total else 0
        tips.append({
            'id': 'food',
            'icon': 'bi-cart4',
            'color': '#f28e2b',
            'title': 'הוצאות מזון - יש מקום לחסוך',
            'summary': f'ממוצע חודשי על מזון: {food_monthly:,.0f} ש"ח. מתוכם {rest_pct:.0f}% על מסעדות/מזון מהיר.',
            'detail': 'מזון הוא תחום שקל לחסוך בו בלי לוותר על איכות חיים:\n\n'
                      '1. **תכננו תפריט שבועי** - מונע קניות אימפולסיביות ובזבוז אוכל\n'
                      '2. **קנו במבצעים** - השוו מחירים בין סופרים, קנו מותג פרטי\n'
                      '3. **בישול ביתי** - ארוחה ביתית עולה 15-30 ש"ח, במסעדה 60-120 ש"ח\n'
                      '4. **הפחיתו מסעדות ב-50%** - תאכלו בחוץ פעם בשבוע במקום 2-3\n'
                      f'5. **חיסכון פוטנציאלי: {food_monthly*0.2:,.0f} ש"ח/חודש** (20% מהוצאות מזון)',
            'savings': food_monthly * 0.2 * 12,
            'priority': 'medium',
        })

    # ---- TIP 5: Entertainment spending ----
    ent_total = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE category_id='entertainment'"
    ).fetchone()[0] or 0
    ent_monthly = ent_total / num_months if num_months else 0
    if ent_monthly > 1000:
        tips.append({
            'id': 'entertainment',
            'icon': 'bi-film',
            'color': '#9c755f',
            'title': 'בילויים ופנאי - ליהנות בחכמה',
            'summary': f'ממוצע חודשי: {ent_monthly:,.0f} ש"ח על בילויים ופנאי.',
            'detail': 'בילויים חשובים לאיכות חיים, אבל אפשר ליהנות בפחות:\n\n'
                      '1. **הגדירו תקציב בילויים** - 1,000-1,500 ש"ח/חודש למשפחה\n'
                      '2. **חפשו חלופות חינמיות** - פארקים, חופים, טיולים בטבע, אירועי עירייה\n'
                      '3. **השתמשו בהנחות** - כרטיסים מוזלים, גרופון, מבצעי שעות מוקדמות\n'
                      '4. **תכננו מראש** - הזמנה מוקדמת תמיד זולה יותר\n'
                      f'5. **חיסכון פוטנציאלי: {max(ent_monthly-1200, 0):,.0f} ש"ח/חודש**',
            'savings': max(ent_monthly - 1200, 0) * 12,
            'priority': 'low',
        })

    # ---- TIP 6: Insurance optimization ----
    insurance = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE category_id='insurance'"
    ).fetchone()[0] or 0
    ins_monthly = insurance / num_months if num_months else 0
    if ins_monthly > 500:
        tips.append({
            'id': 'insurance',
            'icon': 'bi-shield-check',
            'color': '#ff9da7',
            'title': 'ביטוחים - בדקו כפילויות',
            'summary': f'משלמים כ-{ins_monthly:,.0f} ש"ח/חודש על ביטוחים. ייתכן שיש כפילויות!',
            'detail': 'ביטוחים הם תחום שרוב המשפחות משלמות עליו יותר מדי:\n\n'
                      '1. **בדקו כפילויות** - ביטוח חיים דרך העבודה + פרטי = כפול\n'
                      '2. **השוו מחירים** - סוכן ביטוח עצמאי יכול לחסוך 20-30%\n'
                      '3. **בטלו ביטוחים מיותרים** - ביטוח מכשיר סלולרי, ביטוח נסיעות שנתי\n'
                      '4. **העלו השתתפות עצמית** - מוריד פרמיה משמעותית\n'
                      f'5. **חיסכון פוטנציאלי: {ins_monthly*0.2:,.0f} ש"ח/חודש** (20% מהביטוחים)',
            'savings': ins_monthly * 0.2 * 12,
            'priority': 'medium',
        })

    # ---- TIP 7: Savings rate ----
    total_income = conn.execute("SELECT COALESCE(SUM(amount),0) FROM income").fetchone()[0]
    savings_exp = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE category_id='savings'"
    ).fetchone()[0] or 0
    if total_income > 0:
        savings_rate = savings_exp / total_income * 100
        tips.append({
            'id': 'savings',
            'icon': 'bi-piggy-bank',
            'color': '#4dc9f6',
            'title': 'שיעור חיסכון - תכננו לעתיד',
            'summary': f'שיעור החיסכון שלכם (פנסיה+קרנות+השתלמות): {savings_rate:.1f}% מההכנסה.',
            'detail': 'מומלץ לחסוך 20% מההכנסה ברוטו. הנה תוכנית:\n\n'
                      '1. **פנסיה** - ודאו שאתם מפרישים את המקסימום (עד 7% עובד + 7.5% מעביד)\n'
                      '2. **קרן השתלמות** - הכלי הכי טוב בישראל! פטור ממס אחרי 6 שנים\n'
                      '3. **חיסכון חירום** - 3-6 חודשי הוצאות נזילים (כ-100,000-200,000 ש"ח)\n'
                      '4. **השקעות** - אחרי שיש חיסכון חירום:\n'
                      '   - **קרן מחקה S&P500** - תשואה ממוצעת 10% בשנה\n'
                      '   - **תיק השקעות מנוהל** - בנק/בית השקעות, מ-50,000 ש"ח\n'
                      '   - **קופת גמל להשקעה** - הטבת מס, נזילות אחרי 15 שנה\n'
                      '5. **כלל 50/30/20** - 50% צרכים, 30% רצונות, 20% חיסכון',
            'savings': 0,
            'priority': 'high',
        })

    # ---- TIP 8: BIT/PAYBOX untracked transfers ----
    bit_total = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE subcategory LIKE '%BIT%' OR subcategory LIKE '%PAYBOX%'"
    ).fetchone()[0] or 0
    if bit_total > 500:
        tips.append({
            'id': 'transfers',
            'icon': 'bi-phone',
            'color': '#59a14f',
            'title': 'העברות BIT/PAYBOX - לאן הכסף הולך?',
            'summary': f'העברתם {bit_total:,.0f} ש"ח דרך BIT/PAYBOX. ההוצאות האלה לא מסווגות.',
            'detail': 'העברות דיגיטליות קשות לעקוב כי אין להן קטגוריה:\n\n'
                      '1. **רשמו הערה** - כשמעבירים ב-BIT, כתבו למה (מטפלת, חוג, חלוקת חשבון)\n'
                      '2. **הוסיפו ידנית** - את ההוצאות הגדולות הוסיפו ידנית לקטגוריה הנכונה\n'
                      '3. **הגדירו תקרה** - לא יותר מ-1,000 ש"ח/חודש בהעברות\n'
                      '4. **בדקו חיובים חוזרים** - אולי יש מנוי שמשולם ב-BIT שאפשר לבטל',
            'savings': bit_total * 0.15,
            'priority': 'low',
        })

    # ---- TIP 9: Negative balance months ----
    all_months = conn.execute("""
        SELECT substr(date,1,7) as m, SUM(amount) as exp_total
        FROM expenses GROUP BY m
    """).fetchall()
    neg_months = 0
    for row in all_months:
        m = row[0]
        inc = conn.execute(
            "SELECT COALESCE(SUM(amount),0) FROM income WHERE substr(date,1,7)=?", (m,)
        ).fetchone()[0]
        if inc - row[1] < 0:
            neg_months += 1
    if neg_months > 1:
        tips.append({
            'id': 'deficit',
            'icon': 'bi-graph-down-arrow',
            'color': '#dc2626',
            'title': f'גרעון תקציבי - {neg_months} חודשים במינוס',
            'summary': f'ב-{neg_months} מתוך {num_months} חודשים הוצאתם יותר ממה שהרווחתם.',
            'detail': 'גרעון חודשי חוזר הוא הגורם העיקרי לחוב ולריבית מינוס:\n\n'
                      '1. **הגדירו תקציב חודשי מחייב** - לא רק לעקוב, אלא להגביל\n'
                      '2. **שיטת המעטפות** - חלקו את הכסף לקטגוריות בתחילת החודש\n'
                      '3. **חוק 24 שעות** - לפני קנייה מעל 200 ש"ח, חכו יום\n'
                      '4. **הפחיתו הוצאות קבועות** - הן הכי משפיעות כי חוזרות כל חודש\n'
                      '5. **הגדילו הכנסה** - עבודה נוספת, פרילנס, מכירת דברים מיותרים',
            'savings': 0,
            'priority': 'high',
        })

    # Sort by priority
    priority_order = {'high': 0, 'medium': 1, 'low': 2}
    tips.sort(key=lambda t: priority_order.get(t['priority'], 99))

    conn.close()
    return jsonify(tips)


# ---- Budget Agent - Deep Analysis ----

@app.route('/api/analyze', methods=['GET'])
@login_required
def analyze_budget():
    """Deep analysis engine that processes all expenses and generates conclusions."""
    conn = get_db()

    # Gather all data
    months_data = conn.execute("""
        SELECT m,
               exp_total,
               COALESCE((SELECT SUM(amount) FROM income WHERE substr(date,1,7)=m), 0) as inc_total
        FROM (SELECT substr(date,1,7) as m, SUM(amount) as exp_total
              FROM expenses GROUP BY m ORDER BY m)
    """).fetchall()

    categories = conn.execute("""
        SELECT c.name_he, c.id, SUM(e.amount) as total,
               COUNT(DISTINCT substr(e.date,1,7)) as months_active
        FROM expenses e JOIN categories c ON e.category_id=c.id
        GROUP BY c.id ORDER BY total DESC
    """).fetchall()

    num_months = len(months_data)
    total_income = sum(r[2] for r in months_data)
    total_expense = sum(r[1] for r in months_data)
    avg_income = total_income / num_months if num_months else 0
    avg_expense = total_expense / num_months if num_months else 0

    # Fixed vs variable
    fixed = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE frequency='monthly'"
    ).fetchone()[0] or 0
    fixed_monthly = fixed / num_months if num_months else 0

    variable = conn.execute(
        "SELECT SUM(amount) FROM expenses WHERE frequency='random'"
    ).fetchone()[0] or 0
    variable_monthly = variable / num_months if num_months else 0

    # Category averages
    cat_analysis = []
    for cat in categories:
        monthly_avg = cat[2] / cat[3] if cat[3] else 0
        pct = cat[2] / total_expense * 100 if total_expense else 0
        cat_analysis.append({
            'name': cat[0],
            'id': cat[1],
            'total': cat[2],
            'monthly_avg': monthly_avg,
            'percentage': pct,
        })

    # Budget health score (0-100)
    score = 50  # base
    savings_rate = 0

    # Positive factors
    savings_exp = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE category_id='savings'"
    ).fetchone()[0]
    if total_income > 0:
        savings_rate = savings_exp / total_income * 100
        if savings_rate >= 10:
            score += 10
        if savings_rate >= 20:
            score += 10

    # Surplus months
    surplus_months = sum(1 for r in months_data if r[2] > r[1])
    deficit_months = num_months - surplus_months
    score += min(surplus_months * 3, 15)
    score -= min(deficit_months * 3, 15)

    # Overdraft penalty
    overdraft = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE subcategory='ריבית מינוס'"
    ).fetchone()[0]
    if overdraft > 0:
        score -= 10

    # Fixed expenses ratio
    if avg_income > 0:
        fixed_ratio = fixed_monthly / avg_income * 100
        if fixed_ratio < 50:
            score += 5
        elif fixed_ratio > 70:
            score -= 10

    score = max(0, min(100, score))

    # Recommended budget (50/30/20 rule adapted)
    recommended = {}
    if avg_income > 0:
        recommended = {
            'needs': avg_income * 0.50,  # housing, food, transport, insurance
            'wants': avg_income * 0.30,  # entertainment, personal, dining out
            'savings': avg_income * 0.20,  # savings, investments, emergency fund
            'needs_categories': ['דיור ואחזקת בית', 'מזון', 'רכב', 'ביטוחים', 'ריפוי', 'תקשורת'],
            'wants_categories': ['בילוי ופנאי', 'אישי', 'טיפוח ובריאות', 'ילדים'],
            'savings_categories': ['חיסכון והתחייבויות'],
        }

    # Generate textual analysis
    analysis_sections = []

    # Overall health
    if score >= 70:
        health_text = 'המצב הכלכלי שלכם סביר. יש בסיס טוב לעבוד איתו.'
    elif score >= 50:
        health_text = 'יש מקום לשיפור משמעותי. כמה שינויים קטנים יכולים לעשות הבדל גדול.'
    else:
        health_text = 'המצב דורש תשומת לב מיידית. חשוב להתחיל בשינויים היום.'

    analysis_sections.append({
        'title': 'בריאות תקציבית',
        'icon': 'bi-heart-pulse',
        'text': health_text,
        'score': score,
    })

    # Income vs Expense
    if avg_income > avg_expense:
        ie_text = f'בממוצע אתם מרוויחים {avg_income-avg_expense:,.0f} ש"ח יותר ממה שאתם מוציאים. מצוין!'
    else:
        ie_text = f'בממוצע אתם מוציאים {avg_expense-avg_income:,.0f} ש"ח יותר ממה שאתם מרוויחים. זה חייב להשתנות.'

    analysis_sections.append({
        'title': 'הכנסות מול הוצאות',
        'icon': 'bi-arrow-left-right',
        'text': f'הכנסה ממוצעת: {avg_income:,.0f} ש"ח | הוצאה ממוצעת: {avg_expense:,.0f} ש"ח\n{ie_text}',
    })

    # Fixed vs Variable
    analysis_sections.append({
        'title': 'הוצאות קבועות מול משתנות',
        'icon': 'bi-pie-chart',
        'text': f'הוצאות קבועות: {fixed_monthly:,.0f} ש"ח/חודש ({fixed_monthly/avg_expense*100:.0f}% מההוצאות)\n'
                f'הוצאות משתנות: {variable_monthly:,.0f} ש"ח/חודש ({variable_monthly/avg_expense*100:.0f}% מההוצאות)\n\n'
                + ('ההוצאות הקבועות גבוהות מדי (מעל 60%). נסו לצמצם חיובים חודשיים.' if fixed_monthly/avg_expense > 0.6
                   else 'יחס סביר בין הוצאות קבועות למשתנות.'),
    })

    # Top 3 expense categories
    top3 = cat_analysis[:3]
    top3_text = '\n'.join([f'{i+1}. **{c["name"]}**: {c["monthly_avg"]:,.0f} ש"ח/חודש ({c["percentage"]:.1f}%)' for i, c in enumerate(top3)])
    analysis_sections.append({
        'title': 'הקטגוריות הגדולות',
        'icon': 'bi-bar-chart',
        'text': f'שלוש הקטגוריות הכי יקרות:\n{top3_text}',
    })

    # Investment recommendations
    invest_text = ''
    if avg_income > 0:
        monthly_surplus = avg_income - avg_expense
        if monthly_surplus > 2000:
            invest_text = (
                f'יש לכם עודף ממוצע של {monthly_surplus:,.0f} ש"ח/חודש. הנה מה לעשות איתו:\n\n'
                '1. **חיסכון חירום** (עדיפות ראשונה) - 3 חודשי הוצאות בפיקדון/ממ"ש\n'
                '2. **קרן השתלמות** - אם לא ממקסמים, השלימו. הטבת מס משמעותית\n'
                '3. **קופת גמל להשקעה** - עד 70,000 ש"ח/שנה, הטבת מס במשיכה\n'
                '4. **תיק השקעות** - קרן מחקה מדד (S&P500 / ת"א 125) לטווח ארוך'
            )
        elif monthly_surplus > 0:
            invest_text = (
                f'העודף הממוצע שלכם ({monthly_surplus:,.0f} ש"ח/חודש) קטן מדי להשקעה משמעותית.\n'
                'קודם כל צמצמו הוצאות כדי להגדיל את העודף ל-2,000 ש"ח לפחות.'
            )
        else:
            invest_text = (
                'כרגע אתם בגירעון - אין מקום להשקעות.\n'
                'שלב 1: צמצמו הוצאות והגיעו לאיזון.\n'
                'שלב 2: בנו חיסכון חירום.\n'
                'שלב 3: רק אז תתחילו להשקיע.'
            )
    analysis_sections.append({
        'title': 'המלצות השקעה',
        'icon': 'bi-graph-up-arrow',
        'text': invest_text,
    })

    conn.close()

    return jsonify({
        'score': score,
        'avg_income': avg_income,
        'avg_expense': avg_expense,
        'savings_rate': savings_rate,
        'num_months': num_months,
        'surplus_months': surplus_months,
        'deficit_months': deficit_months,
        'fixed_monthly': fixed_monthly,
        'variable_monthly': variable_monthly,
        'categories': cat_analysis,
        'recommended': recommended,
        'sections': analysis_sections,
    })


# ---- Export to Excel ----

FREQ_HE = {'monthly': 'חודשי', 'bimonthly': 'דו-חודשי', 'random': 'לא קבוע'}
SOURCE_HE = {'manual': 'ידני', 'visa_import': 'ויזה', 'xls_import': 'XLS', 'bank_csv': 'בנק'}
PERSON_HE = {'husband': 'בעל', 'wife': 'אישה', 'family': 'משפחה', 'other': 'אחר'}
INCOME_SRC_HE = {'salary': 'משכורת', 'bonus': 'בונוס', 'freelance': 'פרילנס',
                 'child_allowance': 'קצבת ילדים', 'rental': 'שכירות', 'other': 'אחר'}


def _style_header(ws, col_count):
    """Apply header styling to first row."""
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    header_font = Font(bold=True, color='FFFFFF', size=11)
    header_fill = PatternFill(start_color='2563EB', end_color='2563EB', fill_type='solid')
    thin_border = Border(bottom=Side(style='thin', color='CCCCCC'))
    for col in range(1, col_count + 1):
        cell = ws.cell(1, col)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center', vertical='center')
        cell.border = thin_border


def _auto_width(ws):
    """Auto-fit column widths."""
    for col in ws.columns:
        max_len = 0
        col_letter = col[0].column_letter
        for cell in col:
            val = str(cell.value or '')
            max_len = max(max_len, len(val))
        ws.column_dimensions[col_letter].width = min(max_len + 4, 40)


def _add_number_format(ws, col_idx, start_row, end_row, fmt='#,##0.00'):
    """Apply number format to a column range."""
    for row in range(start_row, end_row + 1):
        ws.cell(row, col_idx).number_format = fmt


@app.route('/api/export', methods=['GET'])
@login_required
def export_excel():
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.chart import PieChart, BarChart, Reference, LineChart
    from openpyxl.utils import get_column_letter

    month = request.args.get('month', date.today().strftime('%Y-%m'))
    conn = get_db()

    wb = openpyxl.Workbook()

    # ========== Sheet 1: Summary ==========
    ws_sum = wb.active
    ws_sum.title = 'סיכום'
    ws_sum.sheet_view.rightToLeft = True

    # Get summary data
    income_total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM income WHERE date LIKE ?", (month+'%',)
    ).fetchone()[0]
    expense_total = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE date LIKE ?", (month+'%',)
    ).fetchone()[0]

    title_font = Font(bold=True, size=14, color='1E40AF')
    ws_sum.cell(1, 1, 'דוח תקציב משפחתי').font = title_font
    ws_sum.cell(2, 1, f'חודש: {month}').font = Font(size=12, color='64748B')

    ws_sum.cell(4, 1, 'סה"כ הכנסות').font = Font(bold=True)
    ws_sum.cell(4, 2, income_total).number_format = '#,##0.00'
    ws_sum.cell(5, 1, 'סה"כ הוצאות').font = Font(bold=True)
    ws_sum.cell(5, 2, expense_total).number_format = '#,##0.00'
    ws_sum.cell(6, 1, 'יתרה').font = Font(bold=True, color='16A34A' if income_total >= expense_total else 'DC2626')
    ws_sum.cell(6, 2, income_total - expense_total).number_format = '#,##0.00'

    # By category summary
    ws_sum.cell(8, 1, 'הוצאות לפי קטגוריה').font = Font(bold=True, size=12)
    ws_sum.cell(9, 1, 'קטגוריה')
    ws_sum.cell(9, 2, 'סכום')
    ws_sum.cell(9, 3, 'אחוז')
    _style_header_row(ws_sum, 9, 3)

    cat_rows = conn.execute(
        """SELECT c.name_he, COALESCE(SUM(e.amount),0) as total
           FROM categories c LEFT JOIN expenses e ON c.id=e.category_id AND e.date LIKE ?
           GROUP BY c.id HAVING total>0 ORDER BY total DESC""",
        (month+'%',)
    ).fetchall()

    for i, r in enumerate(cat_rows):
        row = 10 + i
        ws_sum.cell(row, 1, r[0])
        ws_sum.cell(row, 2, r[1]).number_format = '#,##0.00'
        ws_sum.cell(row, 3, r[1]/expense_total if expense_total else 0).number_format = '0.0%'

    # By frequency
    freq_start = 10 + len(cat_rows) + 2
    ws_sum.cell(freq_start, 1, 'הוצאות לפי תדירות').font = Font(bold=True, size=12)
    ws_sum.cell(freq_start+1, 1, 'תדירות')
    ws_sum.cell(freq_start+1, 2, 'סכום')
    ws_sum.cell(freq_start+1, 3, 'מספר פריטים')
    _style_header_row(ws_sum, freq_start+1, 3)

    freq_rows = conn.execute(
        """SELECT frequency, SUM(amount), COUNT(*) FROM expenses
           WHERE date LIKE ? GROUP BY frequency ORDER BY SUM(amount) DESC""",
        (month+'%',)
    ).fetchall()
    for i, r in enumerate(freq_rows):
        row = freq_start + 2 + i
        ws_sum.cell(row, 1, FREQ_HE.get(r[0], r[0]))
        ws_sum.cell(row, 2, r[1]).number_format = '#,##0.00'
        ws_sum.cell(row, 3, r[2])

    # By card
    card_start = freq_start + 2 + len(freq_rows) + 2
    ws_sum.cell(card_start, 1, 'הוצאות לפי כרטיס').font = Font(bold=True, size=12)
    ws_sum.cell(card_start+1, 1, 'כרטיס')
    ws_sum.cell(card_start+1, 2, 'סכום')
    ws_sum.cell(card_start+1, 3, 'מספר פריטים')
    _style_header_row(ws_sum, card_start+1, 3)

    card_rows = conn.execute(
        """SELECT CASE WHEN card='' THEN 'אחר' ELSE card END, SUM(amount), COUNT(*)
           FROM expenses WHERE date LIKE ? GROUP BY card ORDER BY SUM(amount) DESC""",
        (month+'%',)
    ).fetchall()
    for i, r in enumerate(card_rows):
        row = card_start + 2 + i
        ws_sum.cell(row, 1, r[0])
        ws_sum.cell(row, 2, r[1]).number_format = '#,##0.00'
        ws_sum.cell(row, 3, r[2])

    # Income by person
    inc_start = card_start + 2 + len(card_rows) + 2
    ws_sum.cell(inc_start, 1, 'הכנסות לפי בן/בת זוג').font = Font(bold=True, size=12)
    ws_sum.cell(inc_start+1, 1, 'בן/בת זוג')
    ws_sum.cell(inc_start+1, 2, 'סכום')
    _style_header_row(ws_sum, inc_start+1, 2)

    inc_person_rows = conn.execute(
        """SELECT person, SUM(amount) FROM income
           WHERE date LIKE ? GROUP BY person ORDER BY SUM(amount) DESC""",
        (month+'%',)
    ).fetchall()
    for i, r in enumerate(inc_person_rows):
        row = inc_start + 2 + i
        ws_sum.cell(row, 1, PERSON_HE.get(r[0], r[0]))
        ws_sum.cell(row, 2, r[1]).number_format = '#,##0.00'

    _auto_width(ws_sum)

    # ========== Sheet 2: All Expenses ==========
    ws_exp = wb.create_sheet('הוצאות')
    ws_exp.sheet_view.rightToLeft = True
    exp_headers = ['תאריך', 'קטגוריה', 'תת-קטגוריה', 'תיאור', 'סכום', 'תדירות', 'כרטיס', 'מקור']
    for i, h in enumerate(exp_headers, 1):
        ws_exp.cell(1, i, h)
    _style_header(ws_exp, len(exp_headers))

    expenses = conn.execute(
        """SELECT e.date, c.name_he, e.subcategory, e.description, e.amount,
                  e.frequency, e.card, e.source
           FROM expenses e JOIN categories c ON e.category_id=c.id
           WHERE e.date LIKE ? ORDER BY e.date DESC""",
        (month+'%',)
    ).fetchall()
    for i, r in enumerate(expenses):
        row = i + 2
        ws_exp.cell(row, 1, r[0])
        ws_exp.cell(row, 2, r[1])
        ws_exp.cell(row, 3, r[2] or '')
        ws_exp.cell(row, 4, r[3] or '')
        ws_exp.cell(row, 5, r[4]).number_format = '#,##0.00'
        ws_exp.cell(row, 6, FREQ_HE.get(r[5], r[5] or ''))
        ws_exp.cell(row, 7, r[6] or '')
        ws_exp.cell(row, 8, SOURCE_HE.get(r[7], r[7] or ''))
    _auto_width(ws_exp)

    # ========== Sheet 3: Income ==========
    ws_inc = wb.create_sheet('הכנסות')
    ws_inc.sheet_view.rightToLeft = True
    inc_headers = ['תאריך', 'בן/בת זוג', 'מקור', 'סכום', 'תיאור', 'חוזר']
    for i, h in enumerate(inc_headers, 1):
        ws_inc.cell(1, i, h)
    _style_header(ws_inc, len(inc_headers))

    incomes = conn.execute(
        "SELECT date, person, source, amount, description, is_recurring FROM income WHERE date LIKE ? ORDER BY date DESC",
        (month+'%',)
    ).fetchall()
    for i, r in enumerate(incomes):
        row = i + 2
        ws_inc.cell(row, 1, r[0])
        ws_inc.cell(row, 2, PERSON_HE.get(r[1], r[1]))
        ws_inc.cell(row, 3, INCOME_SRC_HE.get(r[2], r[2]))
        ws_inc.cell(row, 4, r[3]).number_format = '#,##0.00'
        ws_inc.cell(row, 5, r[4] or '')
        ws_inc.cell(row, 6, 'כן' if r[5] else 'לא')
    _auto_width(ws_inc)

    # ========== Sheet 4: Daily Breakdown ==========
    ws_daily = wb.create_sheet('יומי')
    ws_daily.sheet_view.rightToLeft = True
    daily_headers = ['תאריך', 'סה"כ הוצאות', 'מספר עסקאות']
    for i, h in enumerate(daily_headers, 1):
        ws_daily.cell(1, i, h)
    _style_header(ws_daily, len(daily_headers))

    daily_rows = conn.execute(
        """SELECT date, SUM(amount), COUNT(*) FROM expenses
           WHERE date LIKE ? GROUP BY date ORDER BY date""",
        (month+'%',)
    ).fetchall()
    for i, r in enumerate(daily_rows):
        row = i + 2
        ws_daily.cell(row, 1, r[0])
        ws_daily.cell(row, 2, r[1]).number_format = '#,##0.00'
        ws_daily.cell(row, 3, r[2])
    _auto_width(ws_daily)

    # ========== Sheet 5: Budget vs Actual ==========
    ws_bud = wb.create_sheet('תקציב מול ביצוע')
    ws_bud.sheet_view.rightToLeft = True
    bud_headers = ['קטגוריה', 'מתוכנן', 'בפועל', 'הפרש', 'אחוז ניצול']
    for i, h in enumerate(bud_headers, 1):
        ws_bud.cell(1, i, h)
    _style_header(ws_bud, len(bud_headers))

    bud_rows = conn.execute(
        """SELECT c.name_he, COALESCE(b.planned_amount,0), COALESCE(SUM(e.amount),0)
           FROM categories c
           LEFT JOIN budget b ON c.id=b.category_id AND b.month=?
           LEFT JOIN expenses e ON c.id=e.category_id AND e.date LIKE ?
           GROUP BY c.id HAVING COALESCE(b.planned_amount,0)>0 OR COALESCE(SUM(e.amount),0)>0
           ORDER BY c.sort_order""",
        (month, month+'%')
    ).fetchall()

    over_fmt = Font(color='DC2626', bold=True)
    under_fmt = Font(color='16A34A')
    for i, r in enumerate(bud_rows):
        row = i + 2
        planned, actual = r[1], r[2]
        diff = planned - actual
        ws_bud.cell(row, 1, r[0])
        ws_bud.cell(row, 2, planned).number_format = '#,##0.00'
        ws_bud.cell(row, 3, actual).number_format = '#,##0.00'
        c = ws_bud.cell(row, 4, diff)
        c.number_format = '#,##0.00'
        c.font = over_fmt if diff < 0 else under_fmt
        ws_bud.cell(row, 5, actual/planned if planned else 0).number_format = '0.0%'
    _auto_width(ws_bud)

    # ========== Sheet 6: Monthly Trend ==========
    ws_trend = wb.create_sheet('מגמה חודשית')
    ws_trend.sheet_view.rightToLeft = True
    trend_headers = ['חודש', 'הוצאות', 'הכנסות', 'יתרה']
    for i, h in enumerate(trend_headers, 1):
        ws_trend.cell(1, i, h)
    _style_header(ws_trend, len(trend_headers))

    trend_rows = conn.execute(
        """SELECT m, exp_total,
                  COALESCE((SELECT SUM(amount) FROM income WHERE substr(date,1,7)=m), 0),
                  COALESCE((SELECT SUM(amount) FROM income WHERE substr(date,1,7)=m), 0) - exp_total
           FROM (SELECT substr(date,1,7) as m, SUM(amount) as exp_total
                 FROM expenses GROUP BY m ORDER BY m)"""
    ).fetchall()
    for i, r in enumerate(trend_rows):
        row = i + 2
        ws_trend.cell(row, 1, r[0])
        ws_trend.cell(row, 2, r[1]).number_format = '#,##0.00'
        ws_trend.cell(row, 3, r[2]).number_format = '#,##0.00'
        c = ws_trend.cell(row, 4, r[3])
        c.number_format = '#,##0.00'
        c.font = Font(color='16A34A' if r[3] >= 0 else 'DC2626', bold=True)
    _auto_width(ws_trend)

    conn.close()

    # Save to BytesIO and send
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f'budget_report_{month}.xlsx'
    return send_file(output, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, download_name=filename)


def _style_header_row(ws, row_num, col_count):
    """Apply header styling to a specific row."""
    from openpyxl.styles import Font, PatternFill, Alignment
    header_font = Font(bold=True, color='FFFFFF', size=10)
    header_fill = PatternFill(start_color='2563EB', end_color='2563EB', fill_type='solid')
    for col in range(1, col_count + 1):
        cell = ws.cell(row_num, col)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal='center')


# ---- Financial Products (Insurance, Pension, Funds) ----

# Auto-detect patterns: (expense_pattern, type, subtype, company, name)
FINANCIAL_DETECT_PATTERNS = [
    # Insurance
    ('הראל בטוח', 'insurance', 'general', 'הראל', 'ביטוח הראל'),
    ('כלל חיים/ב', 'insurance', 'life', 'כלל', 'ביטוח חיים כלל'),
    ('הפניקס חיים', 'insurance', 'life', 'הפניקס', 'ביטוח חיים הפניקס'),
    ('הפניקס חיים וברי', 'insurance', 'health', 'הפניקס', 'ביטוח בריאות הפניקס'),
    ('מגדל ביטוח', 'insurance', 'general', 'מגדל', 'ביטוח מגדל'),
    ('ביטוח ישיר', 'insurance', 'car', 'ביטוח ישיר', 'ביטוח רכב'),
    ('איילון ביטוח', 'insurance', 'general', 'איילון', 'ביטוח איילון'),
    ('שומרה ביטוח', 'insurance', 'general', 'שומרה', 'ביטוח שומרה'),
    ('מנורה ביטוח', 'insurance', 'general', 'מנורה', 'ביטוח מנורה'),
    # Pension
    ('הראל פנסיה', 'pension', 'comprehensive', 'הראל', 'פנסיה הראל'),
    ('מגדל פנסיה', 'pension', 'comprehensive', 'מגדל', 'פנסיה מגדל'),
    ('מנורה פנסיה', 'pension', 'comprehensive', 'מנורה', 'פנסיה מנורה'),
    ('הפניקס פנסיה', 'pension', 'comprehensive', 'הפניקס', 'פנסיה הפניקס'),
    ('כלל פנסיה', 'pension', 'comprehensive', 'כלל', 'פנסיה כלל'),
    ('מיטב פנסיה', 'pension', 'comprehensive', 'מיטב', 'פנסיה מיטב'),
    # Provident / Gemel
    ('מור גמל ופ חיוב', 'fund', 'gemel', 'מור', 'קופת גמל מור'),
    ('מור גמל', 'fund', 'gemel', 'מור', 'קופת גמל מור'),
    ('הראל גמל', 'fund', 'gemel', 'הראל', 'קופת גמל הראל'),
    ('אלטשולר גמל', 'fund', 'gemel', 'אלטשולר שחם', 'קופת גמל אלטשולר'),
    # Hishtalmut (education fund)
    ('השתלמות אג', 'fund', 'hishtalmut', '', 'קרן השתלמות'),
    ('השתלמות חיוב', 'fund', 'hishtalmut', '', 'קרן השתלמות'),
    ('הראל השתלמות', 'fund', 'hishtalmut', 'הראל', 'קרן השתלמות הראל'),
    # Loans linked to savings
    ('הראלהלואה', 'fund', 'loan', 'הראל', 'הלוואה מהראל'),
    ('חיובי הלוו', 'fund', 'loan', '', 'הלוואה'),
]

PRODUCT_TYPE_HE = {
    'insurance': 'ביטוח',
    'pension': 'פנסיה',
    'fund': 'קרן/קופה',
}

PRODUCT_SUBTYPE_HE = {
    'life': 'ביטוח חיים',
    'health': 'ביטוח בריאות',
    'home': 'ביטוח דירה',
    'car': 'ביטוח רכב',
    'general': 'ביטוח כללי',
    'comprehensive': 'פנסיה מקיפה',
    'gemel': 'קופת גמל',
    'hishtalmut': 'קרן השתלמות',
    'investment': 'קופת גמל להשקעה',
    'loan': 'הלוואה',
}


@app.route('/api/financial/detect', methods=['POST'])
@login_required
def financial_detect():
    """Scan expenses and auto-detect financial products."""
    conn = get_db()

    # Get all unique expense descriptions/subcategories
    expenses = conn.execute("""
        SELECT DISTINCT description, subcategory, AVG(amount) as avg_amt,
               COUNT(DISTINCT substr(date,1,7)) as months
        FROM expenses
        WHERE category_id IN ('insurance', 'savings')
        GROUP BY description
        HAVING months >= 1
    """).fetchall()

    # Also check bank patterns
    bank_expenses = conn.execute("""
        SELECT DISTINCT description, subcategory, AVG(amount) as avg_amt,
               COUNT(DISTINCT substr(date,1,7)) as months
        FROM expenses
        WHERE source = 'bank_csv' AND frequency = 'monthly'
        GROUP BY description
        HAVING months >= 1
    """).fetchall()

    all_expenses = list(expenses) + list(bank_expenses)
    seen_patterns = set()
    detected = []

    # Check existing products to avoid duplicates
    existing = conn.execute("SELECT expense_pattern FROM financial_products").fetchall()
    existing_patterns = {r[0] for r in existing if r[0]}

    for exp in all_expenses:
        desc = exp[0] or ''
        subcat = exp[1] or ''
        text = desc + ' ' + subcat

        for pattern, ptype, psubtype, company, name in FINANCIAL_DETECT_PATTERNS:
            if pattern in text and pattern not in seen_patterns and pattern not in existing_patterns:
                seen_patterns.add(pattern)
                detected.append({
                    'type': ptype,
                    'subtype': psubtype,
                    'company': company,
                    'name': name,
                    'expense_pattern': pattern,
                    'monthly_cost': round(exp[2], 0),
                    'months_seen': exp[3],
                    'source_desc': desc,
                })

    conn.close()
    return jsonify({
        'detected': detected,
        'count': len(detected),
        'already_tracked': len(existing_patterns),
    })


@app.route('/api/financial/auto-add', methods=['POST'])
@login_required
def financial_auto_add():
    """Add detected products to the database."""
    data = request.json
    conn = get_db()
    added = 0

    for item in data.get('items', []):
        # Check if already exists
        existing = conn.execute(
            "SELECT id FROM financial_products WHERE expense_pattern=?",
            (item['expense_pattern'],)
        ).fetchone()
        if existing:
            continue

        conn.execute("""
            INSERT INTO financial_products (type, subtype, company, name, monthly_cost, expense_pattern, status)
            VALUES (?, ?, ?, ?, ?, ?, 'active')
        """, (item['type'], item['subtype'], item['company'], item['name'],
              item['monthly_cost'], item['expense_pattern']))
        added += 1

    conn.commit()
    conn.close()
    return jsonify({'status': 'ok', 'added': added})


@app.route('/api/financial/products', methods=['GET'])
@login_required
def financial_list():
    """List all financial products, optionally filtered by type."""
    conn = get_db()
    ptype = request.args.get('type')
    if ptype:
        rows = conn.execute(
            "SELECT * FROM financial_products WHERE type=? AND status='active' ORDER BY type, company",
            (ptype,)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM financial_products WHERE status='active' ORDER BY type, company"
        ).fetchall()

    products = []
    for r in rows:
        p = dict(r)
        # Enrich with actual expense data
        if p['expense_pattern']:
            actual = conn.execute("""
                SELECT AVG(amount) as avg_amt, COUNT(DISTINCT substr(date,1,7)) as months,
                       MAX(date) as last_payment
                FROM expenses WHERE description LIKE ? OR subcategory LIKE ?
            """, ('%' + p['expense_pattern'] + '%', '%' + p['expense_pattern'] + '%')).fetchone()
            p['actual_monthly'] = round(actual[0], 0) if actual[0] else 0
            p['months_tracked'] = actual[1] or 0
            p['last_payment'] = actual[2] or ''
        else:
            p['actual_monthly'] = 0
            p['months_tracked'] = 0
            p['last_payment'] = ''
        products.append(p)

    conn.close()
    return jsonify(products)


@app.route('/api/financial/products', methods=['POST'])
@login_required
def financial_add():
    """Add or update a financial product."""
    data = request.json
    conn = get_db()

    if data.get('id'):
        conn.execute("""
            UPDATE financial_products SET
                type=?, subtype=?, company=?, name=?, policy_number=?,
                monthly_cost=?, coverage_amount=?, balance=?, balance_date=?,
                employee_pct=?, employer_pct=?, return_rate=?,
                start_date=?, renewal_date=?, notes=?, expense_pattern=?,
                status=?, updated_at=CURRENT_TIMESTAMP
            WHERE id=?
        """, (data['type'], data.get('subtype', ''), data.get('company', ''),
              data.get('name', ''), data.get('policy_number', ''),
              data.get('monthly_cost', 0), data.get('coverage_amount', 0),
              data.get('balance', 0), data.get('balance_date', ''),
              data.get('employee_pct', 0), data.get('employer_pct', 0),
              data.get('return_rate', 0),
              data.get('start_date', ''), data.get('renewal_date', ''),
              data.get('notes', ''), data.get('expense_pattern', ''),
              data.get('status', 'active'), data['id']))
    else:
        conn.execute("""
            INSERT INTO financial_products (type, subtype, company, name, policy_number,
                monthly_cost, coverage_amount, balance, balance_date,
                employee_pct, employer_pct, return_rate,
                start_date, renewal_date, notes, expense_pattern, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (data['type'], data.get('subtype', ''), data.get('company', ''),
              data.get('name', ''), data.get('policy_number', ''),
              data.get('monthly_cost', 0), data.get('coverage_amount', 0),
              data.get('balance', 0), data.get('balance_date', ''),
              data.get('employee_pct', 0), data.get('employer_pct', 0),
              data.get('return_rate', 0),
              data.get('start_date', ''), data.get('renewal_date', ''),
              data.get('notes', ''), data.get('expense_pattern', ''),
              data.get('status', 'active')))

    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/financial/products/<int:product_id>', methods=['DELETE'])
@login_required
def financial_delete(product_id):
    conn = get_db()
    conn.execute("UPDATE financial_products SET status='deleted' WHERE id=?", (product_id,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/financial/summary', methods=['GET'])
@login_required
def financial_summary():
    """Dashboard summary of all financial products."""
    conn = get_db()

    products = conn.execute(
        "SELECT * FROM financial_products WHERE status='active'"
    ).fetchall()

    insurance_items = [dict(r) for r in products if r['type'] == 'insurance']
    pension_items = [dict(r) for r in products if r['type'] == 'pension']
    fund_items = [dict(r) for r in products if r['type'] == 'fund']

    insurance_monthly = sum(r['monthly_cost'] for r in insurance_items)
    pension_monthly = sum(r['monthly_cost'] for r in pension_items)
    fund_monthly = sum(r['monthly_cost'] for r in fund_items)
    total_monthly = insurance_monthly + pension_monthly + fund_monthly

    total_coverage = sum(r['coverage_amount'] for r in insurance_items)
    total_balance = sum(r['balance'] for r in pension_items + fund_items)

    # Projection: project fund/pension balances 10/20/30 years
    projections = {}
    for years in [10, 20, 30]:
        projected = 0
        for p in pension_items + fund_items:
            bal = p['balance'] or 0
            monthly = p['monthly_cost'] or 0
            rate = (p['return_rate'] or 5) / 100
            # Future value with monthly contributions
            for y in range(years):
                bal = bal * (1 + rate) + monthly * 12
            projected += bal
        projections[years] = round(projected, 0)

    # Coverage check
    coverage_gaps = []
    subtypes_found = {r['subtype'] for r in insurance_items}
    essential = [
        ('life', 'ביטוח חיים'),
        ('health', 'ביטוח בריאות'),
        ('home', 'ביטוח דירה'),
        ('car', 'ביטוח רכב'),
    ]
    for sub_id, sub_name in essential:
        if sub_id not in subtypes_found:
            coverage_gaps.append(sub_name)

    # Renewal alerts (next 60 days)
    upcoming_renewals = []
    today_str = date.today().strftime('%Y-%m-%d')
    for r in products:
        if r['renewal_date'] and r['renewal_date'] >= today_str:
            try:
                rd = datetime.strptime(r['renewal_date'], '%Y-%m-%d').date()
                days_until = (rd - date.today()).days
                if days_until <= 60:
                    upcoming_renewals.append({
                        'name': r['name'],
                        'company': r['company'],
                        'renewal_date': r['renewal_date'],
                        'days_until': days_until,
                    })
            except ValueError:
                pass

    conn.close()
    return jsonify({
        'insurance': {
            'count': len(insurance_items),
            'monthly_cost': round(insurance_monthly, 0),
            'total_coverage': round(total_coverage, 0),
        },
        'pension': {
            'count': len(pension_items),
            'monthly_cost': round(pension_monthly, 0),
            'total_balance': round(sum(r['balance'] for r in pension_items), 0),
        },
        'funds': {
            'count': len(fund_items),
            'monthly_cost': round(fund_monthly, 0),
            'total_balance': round(sum(r['balance'] for r in fund_items), 0),
        },
        'total_monthly': round(total_monthly, 0),
        'total_balance': round(total_balance, 0),
        'projections': projections,
        'coverage_gaps': coverage_gaps,
        'upcoming_renewals': upcoming_renewals,
    })


# ---- Insights API Endpoints ----

# Israeli CBS average household spending percentages (2024 data approximation)
CBS_AVERAGES = {
    'housing': 25.2,
    'food': 17.1,
    'vehicle': 13.5,
    'children': 7.5,
    'communication': 4.2,
    'health_beauty': 3.8,
    'medical': 5.1,
    'insurance': 5.5,
    'entertainment': 6.3,
    'personal': 4.8,
    'savings': 5.0,
    'misc': 2.0,
}


@app.route('/api/insights/heatmap', methods=['GET'])
@login_required
def insights_heatmap():
    """Calendar heatmap: daily spending intensity for the month."""
    conn = get_db()
    month = request.args.get('month', date.today().strftime('%Y-%m'))
    year, mon = map(int, month.split('-'))

    import calendar
    days_in_month = calendar.monthrange(year, mon)[1]
    first_weekday = calendar.monthrange(year, mon)[0]  # 0=Mon 6=Sun
    # Convert to Sunday-start (Israeli week): Sun=0
    first_weekday = (first_weekday + 1) % 7

    daily = conn.execute(
        "SELECT date, SUM(amount) as total FROM expenses WHERE date LIKE ? GROUP BY date ORDER BY date",
        (month + '%',)
    ).fetchall()

    daily_map = {r[0]: r[1] for r in daily}
    amounts = [r[1] for r in daily] if daily else [0]
    max_amount = max(amounts) if amounts else 1

    days = []
    for d in range(1, days_in_month + 1):
        ds = f"{month}-{d:02d}"
        amt = daily_map.get(ds, 0)
        intensity = min(amt / max_amount, 1.0) if max_amount > 0 else 0
        days.append({'date': ds, 'day': d, 'amount': amt, 'intensity': round(intensity, 3)})

    conn.close()
    return jsonify({
        'month': month,
        'days_in_month': days_in_month,
        'first_weekday': first_weekday,
        'days': days,
        'max_amount': max_amount,
    })


@app.route('/api/insights/burnrate', methods=['GET'])
@login_required
def insights_burnrate():
    """Burn rate gauge: projected month-end spending based on current pace."""
    conn = get_db()
    month = request.args.get('month', date.today().strftime('%Y-%m'))
    year, mon = map(int, month.split('-'))

    import calendar
    days_in_month = calendar.monthrange(year, mon)[1]

    today = date.today()
    if today.year == year and today.month == mon:
        day_of_month = today.day
    else:
        day_of_month = days_in_month

    spent = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE date LIKE ?", (month + '%',)
    ).fetchone()[0]

    income = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM income WHERE date LIKE ?", (month + '%',)
    ).fetchone()[0]

    daily_avg = spent / day_of_month if day_of_month > 0 else 0
    projected = daily_avg * days_in_month
    pct_of_income = (projected / income * 100) if income > 0 else 0

    # Previous month average for comparison
    prev_months = conn.execute("""
        SELECT AVG(total) FROM (
            SELECT SUM(amount) as total FROM expenses
            WHERE substr(date,1,7) != ? GROUP BY substr(date,1,7)
        )
    """, (month,)).fetchone()[0] or 0

    conn.close()
    return jsonify({
        'month': month,
        'day_of_month': day_of_month,
        'days_in_month': days_in_month,
        'spent_so_far': spent,
        'daily_average': round(daily_avg, 0),
        'projected_total': round(projected, 0),
        'income': income,
        'pct_of_income': round(pct_of_income, 1),
        'prev_month_avg': round(prev_months, 0),
        'on_track': projected <= income if income > 0 else projected <= prev_months,
    })


@app.route('/api/insights/latte', methods=['GET'])
@login_required
def insights_latte():
    """Latte factor: show what small recurring costs become over time if invested."""
    conn = get_db()
    # Find small recurring expenses (< 200 NIS each, appearing 3+ months)
    candidates = conn.execute("""
        SELECT description, AVG(amount) as avg_amt, COUNT(DISTINCT substr(date,1,7)) as months,
               category_id, subcategory
        FROM expenses
        WHERE amount < 200 AND amount > 5 AND description != ''
        GROUP BY description
        HAVING months >= 2
        ORDER BY avg_amt * months DESC
        LIMIT 15
    """).fetchall()

    items = []
    for r in candidates:
        monthly = r[1]
        yearly = monthly * 12
        # Compound interest projections at 7% annual
        y5 = sum(yearly * (1.07 ** i) for i in range(5))
        y10 = sum(yearly * (1.07 ** i) for i in range(10))
        y20 = sum(yearly * (1.07 ** i) for i in range(20))
        items.append({
            'description': r[0],
            'monthly_avg': round(r[1], 0),
            'months_seen': r[2],
            'category': r[3],
            'yearly': round(yearly, 0),
            'invested_5y': round(y5, 0),
            'invested_10y': round(y10, 0),
            'invested_20y': round(y20, 0),
        })

    # Also allow custom calculation via query params
    custom = None
    custom_amount = request.args.get('amount')
    custom_freq = request.args.get('freq', 'monthly')
    if custom_amount:
        amt = float(custom_amount)
        if custom_freq == 'daily':
            monthly = amt * 30
        elif custom_freq == 'weekly':
            monthly = amt * 4.33
        else:
            monthly = amt
        yearly = monthly * 12
        y5 = sum(yearly * (1.07 ** i) for i in range(5))
        y10 = sum(yearly * (1.07 ** i) for i in range(10))
        y20 = sum(yearly * (1.07 ** i) for i in range(20))
        custom = {
            'monthly': round(monthly, 0),
            'yearly': round(yearly, 0),
            'invested_5y': round(y5, 0),
            'invested_10y': round(y10, 0),
            'invested_20y': round(y20, 0),
        }

    conn.close()
    return jsonify({'items': items, 'custom': custom})


@app.route('/api/insights/anomalies', methods=['GET'])
@login_required
def insights_anomalies():
    """Detect spending anomalies: categories significantly above their average."""
    conn = get_db()
    month = request.args.get('month', date.today().strftime('%Y-%m'))

    # Get current month totals per category
    current = conn.execute("""
        SELECT c.id, c.name_he, c.color, COALESCE(SUM(e.amount),0) as total
        FROM categories c LEFT JOIN expenses e ON c.id=e.category_id AND e.date LIKE ?
        GROUP BY c.id HAVING total > 0
    """, (month + '%',)).fetchall()

    anomalies = []
    for r in current:
        cat_id, name, color, cur_total = r[0], r[1], r[2], r[3]
        # Get average of other months
        hist = conn.execute("""
            SELECT AVG(monthly_total) FROM (
                SELECT SUM(amount) as monthly_total FROM expenses
                WHERE category_id=? AND substr(date,1,7) != ?
                GROUP BY substr(date,1,7)
            )
        """, (cat_id, month)).fetchone()[0]

        if hist and hist > 0:
            pct_change = ((cur_total - hist) / hist) * 100
            if abs(pct_change) > 25:
                # Find top contributors to the change
                top_items = conn.execute("""
                    SELECT description, SUM(amount) as total FROM expenses
                    WHERE category_id=? AND date LIKE ? AND description != ''
                    GROUP BY description ORDER BY total DESC LIMIT 3
                """, (cat_id, month + '%')).fetchall()

                anomalies.append({
                    'category': name,
                    'category_id': cat_id,
                    'color': color,
                    'current': round(cur_total, 0),
                    'average': round(hist, 0),
                    'pct_change': round(pct_change, 1),
                    'direction': 'up' if pct_change > 0 else 'down',
                    'top_items': [{'desc': t[0], 'amount': round(t[1], 0)} for t in top_items],
                })

    anomalies.sort(key=lambda a: abs(a['pct_change']), reverse=True)
    conn.close()
    return jsonify(anomalies)


@app.route('/api/insights/recurring', methods=['GET'])
@login_required
def insights_recurring():
    """Auto-detect recurring expenses by pattern analysis."""
    conn = get_db()

    # Find descriptions that appear in 3+ different months
    recurring = conn.execute("""
        SELECT description, category_id, AVG(amount) as avg_amt,
               MIN(amount) as min_amt, MAX(amount) as max_amt,
               COUNT(*) as count, COUNT(DISTINCT substr(date,1,7)) as months,
               GROUP_CONCAT(DISTINCT substr(date,1,7)) as month_list
        FROM expenses
        WHERE description != '' AND description IS NOT NULL
        GROUP BY description
        HAVING months >= 3
        ORDER BY avg_amt DESC
    """).fetchall()

    # Also find by subcategory for those without description
    recurring_sub = conn.execute("""
        SELECT subcategory, category_id, AVG(amount) as avg_amt,
               MIN(amount) as min_amt, MAX(amount) as max_amt,
               COUNT(*) as count, COUNT(DISTINCT substr(date,1,7)) as months,
               GROUP_CONCAT(DISTINCT substr(date,1,7)) as month_list
        FROM expenses
        WHERE (description = '' OR description IS NULL) AND subcategory != ''
        GROUP BY subcategory, category_id
        HAVING months >= 3
        ORDER BY avg_amt DESC
    """).fetchall()

    # Get category names
    cat_names = {r[0]: r[1] for r in conn.execute("SELECT id, name_he FROM categories").fetchall()}

    items = []
    seen = set()
    for r in list(recurring) + list(recurring_sub):
        name = r[0]
        if name in seen:
            continue
        seen.add(name)
        months_list = r[7].split(',') if r[7] else []
        # Determine frequency pattern
        if len(months_list) >= 2:
            sorted_months = sorted(months_list)
            gaps = []
            for i in range(1, len(sorted_months)):
                y1, m1 = map(int, sorted_months[i-1].split('-'))
                y2, m2 = map(int, sorted_months[i].split('-'))
                gap = (y2 - y1) * 12 + (m2 - m1)
                gaps.append(gap)
            avg_gap = sum(gaps) / len(gaps) if gaps else 1
            if avg_gap <= 1.2:
                pattern = 'monthly'
            elif avg_gap <= 2.5:
                pattern = 'bimonthly'
            else:
                pattern = 'irregular'
        else:
            pattern = 'unknown'

        items.append({
            'name': name,
            'category': cat_names.get(r[1], r[1]),
            'category_id': r[1],
            'avg_amount': round(r[2], 0),
            'min_amount': round(r[3], 0),
            'max_amount': round(r[4], 0),
            'count': r[5],
            'months': r[6],
            'pattern': pattern,
            'yearly_cost': round(r[2] * 12 if pattern == 'monthly' else r[2] * 6 if pattern == 'bimonthly' else r[2] * r[6], 0),
        })

    total_monthly = sum(i['avg_amount'] for i in items if i['pattern'] == 'monthly')
    total_yearly = sum(i['yearly_cost'] for i in items)

    conn.close()
    return jsonify({
        'items': items,
        'total_monthly': round(total_monthly, 0),
        'total_yearly': round(total_yearly, 0),
        'count': len(items),
    })


@app.route('/api/insights/whatif', methods=['GET'])
@login_required
def insights_whatif():
    """What-if simulator: provide baseline data for interactive sliders."""
    conn = get_db()

    months_count = conn.execute(
        "SELECT COUNT(DISTINCT substr(date,1,7)) FROM expenses"
    ).fetchone()[0] or 1

    cats = conn.execute("""
        SELECT c.id, c.name_he, c.color, COALESCE(SUM(e.amount),0) as total
        FROM categories c LEFT JOIN expenses e ON c.id=e.category_id
        GROUP BY c.id HAVING total > 0
        ORDER BY total DESC
    """).fetchall()

    income_monthly = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM income"
    ).fetchone()[0] / months_count

    categories_data = []
    for r in cats:
        monthly_avg = r[3] / months_count
        categories_data.append({
            'id': r[0],
            'name': r[1],
            'color': r[2],
            'monthly_avg': round(monthly_avg, 0),
            'total': round(r[3], 0),
        })

    total_monthly = sum(c['monthly_avg'] for c in categories_data)
    conn.close()
    return jsonify({
        'categories': categories_data,
        'total_monthly_expense': round(total_monthly, 0),
        'monthly_income': round(income_monthly, 0),
        'months_analyzed': months_count,
    })


@app.route('/api/insights/weekly-pulse', methods=['GET'])
@login_required
def insights_weekly_pulse():
    """Average spending by day of the week."""
    conn = get_db()

    # SQLite strftime('%w') = 0(Sun) to 6(Sat)
    raw = conn.execute(
        "SELECT CAST(strftime('%w', date) AS INTEGER) as dow,"
        " SUM(amount) as total, COUNT(*) as count,"
        " COUNT(DISTINCT date) as days_count"
        " FROM expenses GROUP BY dow ORDER BY dow"
    ).fetchall()

    day_names = ['ראשון', 'שני', 'שלישי', 'רביעי', 'חמישי', 'שישי', 'שבת']
    days = []
    amounts = []
    for r in raw:
        avg_per_day = r[1] / r[3] if r[3] > 0 else 0
        days.append({
            'dow': r[0],
            'name': day_names[r[0]],
            'total': round(r[1], 0),
            'avg_per_day': round(avg_per_day, 0),
            'transaction_count': r[2],
            'days_count': r[3],
        })
        amounts.append(avg_per_day)

    # Find peak and low days
    if amounts:
        max_idx = amounts.index(max(amounts))
        min_idx = amounts.index(min(amounts))
        peak_day = days[max_idx]['name'] if days else ''
        low_day = days[min_idx]['name'] if days else ''
    else:
        peak_day = low_day = ''

    conn.close()
    return jsonify({
        'days': days,
        'peak_day': peak_day,
        'low_day': low_day,
    })


@app.route('/api/insights/projection', methods=['GET'])
@login_required
def insights_projection():
    """12-month forward projection based on trends."""
    conn = get_db()

    # Get monthly data
    monthly = conn.execute("""
        SELECT substr(date,1,7) as m, SUM(amount) as total
        FROM expenses GROUP BY m ORDER BY m
    """).fetchall()

    income_monthly = conn.execute("""
        SELECT substr(date,1,7) as m, SUM(amount) as total
        FROM income GROUP BY m ORDER BY m
    """).fetchall()

    inc_map = {r[0]: r[1] for r in income_monthly}

    if len(monthly) < 2:
        conn.close()
        return jsonify({'error': 'Not enough data for projection', 'history': [], 'projection': []})

    # Calculate trend using simple linear regression
    exp_values = [r[1] for r in monthly]
    n = len(exp_values)
    x_mean = (n - 1) / 2
    y_mean = sum(exp_values) / n
    numerator = sum((i - x_mean) * (v - y_mean) for i, v in enumerate(exp_values))
    denominator = sum((i - x_mean) ** 2 for i in range(n))
    slope = numerator / denominator if denominator else 0
    intercept = y_mean - slope * x_mean

    # Average income
    inc_values = list(inc_map.values())
    avg_income = sum(inc_values) / len(inc_values) if inc_values else 0

    # Build history
    history = []
    for r in monthly:
        inc = inc_map.get(r[0], 0)
        history.append({
            'month': r[0],
            'expenses': round(r[1], 0),
            'income': round(inc, 0),
            'balance': round(inc - r[1], 0),
        })

    # Project 12 months forward
    last_month = monthly[-1][0]
    ly, lm = map(int, last_month.split('-'))
    projection = []
    cumulative_savings = 0
    for i in range(1, 13):
        pm = lm + i
        py = ly
        while pm > 12:
            pm -= 12
            py += 1
        proj_month = f"{py}-{pm:02d}"
        proj_expense = max(intercept + slope * (n - 1 + i), 0)
        proj_balance = avg_income - proj_expense
        cumulative_savings += proj_balance
        projection.append({
            'month': proj_month,
            'expenses': round(proj_expense, 0),
            'income': round(avg_income, 0),
            'balance': round(proj_balance, 0),
            'cumulative': round(cumulative_savings, 0),
        })

    conn.close()
    return jsonify({
        'history': history,
        'projection': projection,
        'trend_direction': 'up' if slope > 50 else 'down' if slope < -50 else 'stable',
        'monthly_trend': round(slope, 0),
        'avg_income': round(avg_income, 0),
        'projected_yearly_savings': round(sum(p['balance'] for p in projection), 0),
    })


@app.route('/api/insights/comparison', methods=['GET'])
@login_required
def insights_comparison():
    """Compare spending to Israeli CBS household averages."""
    conn = get_db()

    months_count = conn.execute(
        "SELECT COUNT(DISTINCT substr(date,1,7)) FROM expenses"
    ).fetchone()[0] or 1

    total_expense = conn.execute(
        "SELECT COALESCE(SUM(amount),0) FROM expenses"
    ).fetchone()[0]
    monthly_total = total_expense / months_count

    cats = conn.execute("""
        SELECT c.id, c.name_he, c.color, COALESCE(SUM(e.amount),0) as total
        FROM categories c LEFT JOIN expenses e ON c.id=e.category_id
        GROUP BY c.id HAVING total > 0
        ORDER BY total DESC
    """).fetchall()

    comparisons = []
    for r in cats:
        cat_id, name, color, total = r[0], r[1], r[2], r[3]
        my_pct = (total / total_expense * 100) if total_expense > 0 else 0
        cbs_pct = CBS_AVERAGES.get(cat_id, 0)
        diff = my_pct - cbs_pct
        monthly = total / months_count

        comparisons.append({
            'category': name,
            'category_id': cat_id,
            'color': color,
            'my_pct': round(my_pct, 1),
            'cbs_pct': cbs_pct,
            'diff': round(diff, 1),
            'monthly_avg': round(monthly, 0),
            'status': 'high' if diff > 5 else 'low' if diff < -5 else 'normal',
        })

    comparisons.sort(key=lambda c: abs(c['diff']), reverse=True)
    conn.close()
    return jsonify({
        'comparisons': comparisons,
        'monthly_total': round(monthly_total, 0),
        'months_analyzed': months_count,
    })


@app.route('/api/insights/achievements', methods=['GET'])
@login_required
def insights_achievements():
    """Budget streaks and gamification achievements."""
    conn = get_db()

    achievements = []

    # Get monthly data
    monthly = conn.execute("""
        SELECT substr(date,1,7) as m, SUM(amount) as total
        FROM expenses GROUP BY m ORDER BY m
    """).fetchall()

    # Income by month
    income_data = conn.execute("""
        SELECT substr(date,1,7) as m, SUM(amount) as total
        FROM income GROUP BY m ORDER BY m
    """).fetchall()
    inc_map = {r[0]: r[1] for r in income_data}

    # 1. Surplus streak
    surplus_streak = 0
    max_surplus_streak = 0
    for r in monthly:
        inc = inc_map.get(r[0], 0)
        if inc > r[1]:
            surplus_streak += 1
            max_surplus_streak = max(max_surplus_streak, surplus_streak)
        else:
            surplus_streak = 0

    if max_surplus_streak >= 2:
        achievements.append({
            'id': 'surplus_streak',
            'icon': 'bi-trophy',
            'color': '#f59e0b',
            'title': f'רצף חיובי - {max_surplus_streak} חודשים ברצף בעודף!',
            'description': f'הצלחתם לסיים {max_surplus_streak} חודשים ברצף עם יותר הכנסות מהוצאות.',
            'current': surplus_streak,
            'best': max_surplus_streak,
            'type': 'streak',
            'unlocked': True,
        })

    # 2. Best savings month
    best_savings = None
    best_savings_month = ''
    for r in monthly:
        inc = inc_map.get(r[0], 0)
        savings = inc - r[1]
        if best_savings is None or savings > best_savings:
            best_savings = savings
            best_savings_month = r[0]

    if best_savings and best_savings > 0:
        achievements.append({
            'id': 'best_savings',
            'icon': 'bi-star',
            'color': '#16a34a',
            'title': f'שיא חיסכון - {best_savings:,.0f} ש"ח!',
            'description': f'החודש הכי טוב שלכם היה {best_savings_month} עם חיסכון של {best_savings:,.0f} ש"ח.',
            'value': best_savings,
            'month': best_savings_month,
            'type': 'record',
            'unlocked': True,
        })

    # 3. Lowest expense month
    if monthly:
        lowest = min(monthly, key=lambda r: r[1])
        achievements.append({
            'id': 'lowest_expense',
            'icon': 'bi-arrow-down-circle',
            'color': '#2563eb',
            'title': f'חודש חסכוני - {lowest[1]:,.0f} ש"ח בלבד!',
            'description': f'ב-{lowest[0]} הוצאתם הכי מעט.',
            'value': lowest[1],
            'month': lowest[0],
            'type': 'record',
            'unlocked': True,
        })

    # 4. No overdraft streak
    overdraft_months = set()
    od_data = conn.execute("""
        SELECT DISTINCT substr(date,1,7) FROM expenses WHERE subcategory='ריבית מינוס'
    """).fetchall()
    overdraft_months = {r[0] for r in od_data}

    all_months = [r[0] for r in monthly]
    no_od_streak = 0
    max_no_od = 0
    for m in all_months:
        if m not in overdraft_months:
            no_od_streak += 1
            max_no_od = max(max_no_od, no_od_streak)
        else:
            no_od_streak = 0

    if max_no_od >= 2:
        achievements.append({
            'id': 'no_overdraft',
            'icon': 'bi-shield-check',
            'color': '#8b5cf6',
            'title': f'ללא מינוס - {max_no_od} חודשים!',
            'description': f'{max_no_od} חודשים בלי ריבית מינוס. כל שקל ריבית שנחסך = כסף בכיס!',
            'current': no_od_streak,
            'best': max_no_od,
            'type': 'streak',
            'unlocked': True,
        })

    # 5. Category improvement: check if any category went down vs 3-month average
    if len(all_months) >= 4:
        latest = all_months[-1]
        prev3 = all_months[-4:-1]
        cats = conn.execute("SELECT id, name_he FROM categories").fetchall()
        for cat_id, cat_name in cats:
            cur = conn.execute(
                "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE category_id=? AND substr(date,1,7)=?",
                (cat_id, latest)
            ).fetchone()[0]
            prev_avg_val = 0
            for pm in prev3:
                prev_avg_val += conn.execute(
                    "SELECT COALESCE(SUM(amount),0) FROM expenses WHERE category_id=? AND substr(date,1,7)=?",
                    (cat_id, pm)
                ).fetchone()[0]
            prev_avg_val /= 3
            if prev_avg_val > 500 and cur < prev_avg_val * 0.7:
                saved = prev_avg_val - cur
                achievements.append({
                    'id': f'improved_{cat_id}',
                    'icon': 'bi-graph-down-arrow',
                    'color': '#16a34a',
                    'title': f'שיפור ב{cat_name} - חסכתם {saved:,.0f} ש"ח!',
                    'description': f'הוצאתם ב-{cat_name} ירדו מממוצע {prev_avg_val:,.0f} ל-{cur:,.0f} ש"ח.',
                    'value': saved,
                    'type': 'improvement',
                    'unlocked': True,
                })

    # 6. Locked achievements (goals to work toward)
    if max_surplus_streak < 6:
        achievements.append({
            'id': 'surplus_6',
            'icon': 'bi-lock',
            'color': '#94a3b8',
            'title': 'אתגר: 6 חודשים ברצף בעודף',
            'description': f'עוד {6 - max_surplus_streak} חודשים בעודף כדי לפתוח את ההישג הזה!',
            'progress': max_surplus_streak,
            'target': 6,
            'type': 'challenge',
            'unlocked': False,
        })

    total_income = sum(inc_map.values())
    total_expense = sum(r[1] for r in monthly)
    savings_rate = ((total_income - total_expense) / total_income * 100) if total_income > 0 else 0

    if savings_rate < 20:
        achievements.append({
            'id': 'savings_20',
            'icon': 'bi-lock',
            'color': '#94a3b8',
            'title': 'אתגר: 20% חיסכון',
            'description': f'שיעור החיסכון שלכם {savings_rate:.1f}%. הגיעו ל-20% כדי לפתוח!',
            'progress': round(savings_rate, 1),
            'target': 20,
            'type': 'challenge',
            'unlocked': False,
        })

    conn.close()
    return jsonify(achievements)


# ============================================================
# Installment Payments
# ============================================================

@app.route('/api/cards', methods=['GET'])
@login_required
def cards_list():
    conn = get_db()
    rows = conn.execute("SELECT DISTINCT card FROM expenses WHERE card != '' ORDER BY card").fetchall()
    conn.close()
    return jsonify([r['card'] for r in rows])


@app.route('/api/installments', methods=['GET'])
@login_required
def installments_list():
    conn = get_db()
    rows = conn.execute("SELECT * FROM installments ORDER BY start_date DESC").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['payments_remaining'] = d['total_payments'] - d['payments_made']
        d['remaining_amount'] = round(d['monthly_payment'] * d['payments_remaining'], 2)
        result.append(d)
    conn.close()
    return jsonify(result)


@app.route('/api/installments', methods=['POST'])
@login_required
def installments_add():
    data = request.json
    total_amount = float(data.get('total_amount', 0))
    total_payments = int(data.get('total_payments', 1))
    monthly_payment = round(total_amount / total_payments, 2) if total_payments > 0 else total_amount
    conn = get_db()

    if data.get('id'):
        conn.execute("""
            UPDATE installments SET description=?, store=?, total_amount=?,
                total_payments=?, payments_made=?, monthly_payment=?,
                start_date=?, card=?, notes=?
            WHERE id=?
        """, (data['description'], data.get('store', ''), total_amount,
              total_payments, int(data.get('payments_made', 0)), monthly_payment,
              data.get('start_date', ''), data.get('card', ''),
              data.get('notes', ''), data['id']))
    else:
        conn.execute("""
            INSERT INTO installments (description, store, total_amount, total_payments,
                payments_made, monthly_payment, start_date, card, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (data['description'], data.get('store', ''), total_amount,
              total_payments, int(data.get('payments_made', 0)), monthly_payment,
              data.get('start_date', ''), data.get('card', ''),
              data.get('notes', '')))

    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


@app.route('/api/installments/<int:inst_id>', methods=['DELETE'])
@login_required
def installments_delete(inst_id):
    conn = get_db()
    conn.execute("DELETE FROM installments WHERE id=?", (inst_id,))
    conn.commit()
    conn.close()
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    if getattr(sys, 'frozen', False):
        import webbrowser
        import threading
        threading.Timer(1.5, lambda: webbrowser.open('http://127.0.0.1:5000')).start()
        print('=== מעקב הוצאות משפחתי ===')
        print('האפליקציה רצה בכתובת: http://127.0.0.1:5000')
        print('לסגירה: סגרו חלון זה או לחצו Ctrl+C')
        app.run(debug=False, port=5000)
    else:
        app.run(debug=True, port=5000)
