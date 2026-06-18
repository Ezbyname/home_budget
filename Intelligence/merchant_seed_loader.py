"""
Merchant seed loader.

Loads the canonical merchant → category seed data into merchant_learning
for a given user on first use (or re-seed on demand).

Seeds are system-level defaults (source='seed'). User corrections always
override them (source='user' has higher priority in the categorizer).

Idempotent: INSERT OR IGNORE, never overwrites existing user data.
"""

import sqlite3
from typing import Optional

# ── Canonical seed: (merchant_key, display_name, category_id, confidence) ────
# All confidences start at 0.80 — high enough to be used at P1 but lower than
# user-confirmed entries (which are pushed to 0.90+).
MERCHANT_SEEDS: list[tuple[str, str, str, float]] = [
    # Supermarkets → food
    ('SHUFERSAL',       'שופרסל',           'food',         0.90),
    ('RAMI_LEVY',       'רמי לוי',          'food',         0.90),
    ('VICTORY',         'ויקטורי',          'food',         0.85),
    ('MEGA',            'מגה',              'food',         0.85),
    ('YOCHANANOF',      'יוחננוף',          'food',         0.85),
    ('OSHER_AD',        'אושר עד',          'food',         0.85),
    ('MAHSANE_HASHUK',  'מחסני השוק',       'food',         0.85),
    ('TIVTAAM',         'טיב טעם',          'food',         0.85),
    ('AM_PM',           'AM:PM',            'food',         0.80),
    # Pharmacy → health_beauty
    ('SUPER_PHARM',     'סופר פארם',        'health_beauty', 0.90),
    ('NEW_PHARM',       'ניו פארם',         'health_beauty', 0.85),
    ('BOOTS',           'בוטס',             'health_beauty', 0.85),
    # Fuel → vehicle (top-level; fingerprints resolve PAZ market vs. fuel)
    ('PAZ',             'פז',               'vehicle',      0.75),
    ('SONOL',           'סונול',            'vehicle',      0.85),
    ('DELEK_STATIONS',  'דלק תחנות',        'vehicle',      0.85),
    ('ORL_FUEL',        'ORL',              'vehicle',      0.80),
    ('YELLOW_FUEL',     'ילו',              'vehicle',      0.80),
    # Communications → communication
    ('HOT',             'הוט',              'communication', 0.90),
    ('BEZEQ',           'בזק',              'communication', 0.90),
    ('PARTNER',         'פרטנר',            'communication', 0.90),
    ('CELLCOM',         'סלקום',            'communication', 0.90),
    ('PELEPHONE',       'פלאפון',           'communication', 0.90),
    ('BEZEQ_INT',       'בזק בינלאומי',     'communication', 0.85),
    # Streaming → subscriptions
    ('NETFLIX',         'נטפליקס',          'subscriptions', 0.95),
    ('SPOTIFY',         'ספוטיפיי',         'subscriptions', 0.95),
    ('APPLE',           'אפל',              'subscriptions', 0.85),
    ('GOOGLE',          'גוגל',             'subscriptions', 0.80),
    ('YOUTUBE',         'יוטיוב',           'subscriptions', 0.85),
    ('DISNEY_PLUS',     'דיסני+',           'subscriptions', 0.95),
    ('AMAZON_PRIME',    'אמזון פריים',      'subscriptions', 0.90),
    ('YES_TV',          'yes',              'subscriptions', 0.90),
    # Clothing → clothing
    ('HM',              'H&M',              'clothing',     0.90),
    ('ZARA',            'זארה',             'clothing',     0.90),
    ('FOX',             'פוקס',             'clothing',     0.85),
    ('GOLF',            'גולף',             'clothing',     0.85),
    ('CASTRO',          'קסטרו',            'clothing',     0.85),
    ('SHEIN',           'שיין',             'clothing',     0.85),
    ('ADIDAS',          'אדידס',            'clothing',     0.85),
    ('NIKE',            'נייק',             'clothing',     0.85),
    # Food delivery → dining_out
    ('WOLT',            'וולט',             'dining_out',   0.95),
    ('TEN_BIS',         '10bis',            'dining_out',   0.90),
    # Transfers (often internal) → misc
    ('BIT_TRANSFER',    'ביט',              'misc',         0.70),
    ('PAYBOX',          'פייבוקס',          'misc',         0.70),
    ('PEPPER_PAY',      'פפר',              'misc',         0.70),
    # Insurance → insurance
    ('HAREL',           'הראל',             'insurance',    0.85),
    ('CLAL_INSURANCE',  'כלל ביטוח',        'insurance',    0.85),
    ('MIGDAL',          'מגדל',             'insurance',    0.85),
    ('PHOENIX',         'פניקס',            'insurance',    0.85),
    ('MENORA',          'מנורה',            'insurance',    0.85),
    # Mortgage → always handled by the mortgage override rule (before P1)
    # Listed here only so they appear in the merchant management screen
    ('MORTGAGE',        'משכנתא',           'mortgage',     0.99),
    ('BANK_HAPOALIM_MORTGAGE', 'משכנתא הפועלים', 'mortgage', 0.99),
]

# Fingerprint seeds for ambiguous merchants
FINGERPRINT_SEEDS: list[tuple[str, str, Optional[float], Optional[float], str, float]] = [
    # (merchant_key, keyword, amount_min, amount_max, category_id, weight)
    ('PAZ', 'דלק',   40.0,  800.0, 'vehicle',  2.0),
    ('PAZ', 'fuel',  40.0,  800.0, 'vehicle',  2.0),
    ('PAZ', 'תחנה',  40.0,  800.0, 'vehicle',  2.0),
    ('PAZ', 'מרקט',  5.0,   150.0, 'food',     2.0),
    ('PAZ', 'market',5.0,   150.0, 'food',     2.0),
    ('PAZ', 'קיוסק', 5.0,   100.0, 'food',     1.5),
]


def seed_merchant_learning(user_id: int, conn: sqlite3.Connection,
                            force: bool = False) -> int:
    """
    Seed canonical merchant_learning entries for a user.

    Parameters
    ----------
    user_id : target user
    conn : open DB connection (caller commits)
    force : if True, update existing seed entries (never overwrites user-confirmed ones)

    Returns
    -------
    int : number of rows inserted
    """
    inserted = 0
    for merchant_key, display_name, category_id, confidence in MERCHANT_SEEDS:
        if force:
            # Only update if source is still 'seed' — never overwrite user/ai/document
            conn.execute(
                """INSERT INTO merchant_learning
                   (user_id, merchant_key, display_name, category_id, confidence, source, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'seed', CURRENT_TIMESTAMP)
                   ON CONFLICT(user_id, merchant_key)
                   DO UPDATE SET display_name=excluded.display_name,
                       category_id=excluded.category_id,
                       confidence=excluded.confidence,
                       updated_at=CURRENT_TIMESTAMP
                   WHERE source='seed'""",
                (user_id, merchant_key, display_name, category_id, confidence)
            )
        else:
            conn.execute(
                """INSERT OR IGNORE INTO merchant_learning
                   (user_id, merchant_key, display_name, category_id, confidence, source)
                   VALUES (?, ?, ?, ?, ?, 'seed')""",
                (user_id, merchant_key, display_name, category_id, confidence)
            )
        inserted += 1

    return inserted


def seed_merchant_fingerprints(user_id: int, conn: sqlite3.Connection) -> int:
    """Seed fingerprint rows for ambiguous merchants. Idempotent (INSERT OR IGNORE)."""
    inserted = 0
    for merchant_key, keyword, amt_min, amt_max, category_id, weight in FINGERPRINT_SEEDS:
        conn.execute(
            """INSERT OR IGNORE INTO merchant_fingerprints
               (user_id, merchant_key, keyword, amount_min, amount_max, category_id, weight)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (user_id, merchant_key, keyword, amt_min, amt_max, category_id, weight)
        )
        inserted += 1
    return inserted


def is_seeded(user_id: int, conn: sqlite3.Connection) -> bool:
    """Return True if this user already has seed entries."""
    row = conn.execute(
        "SELECT 1 FROM merchant_learning WHERE user_id=? AND source='seed' LIMIT 1",
        (user_id,)
    ).fetchone()
    return row is not None


def ensure_seeded(user_id: int, conn: sqlite3.Connection) -> None:
    """
    Seed merchant data for a user if not already done.
    Called lazily on first import — never on app startup (too slow for cold start).
    """
    if not is_seeded(user_id, conn):
        seed_merchant_learning(user_id, conn)
        seed_merchant_fingerprints(user_id, conn)
        conn.commit()
