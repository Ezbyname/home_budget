"""
Merchant normalization engine.

Converts raw bank/card description strings into a stable canonical merchant key
that can be used as a dictionary key across all intelligence tables.

Pipeline (applied in order):
  1. Uppercase + strip
  2. Remove legal suffixes (בע"מ, LTD, INC, CO, ...)
  3. Remove transaction noise (branch numbers, date fragments, trailing digits)
  4. Collapse whitespace
  5. Lookup in merchant_aliases table (user-specific, then global seed)
  6. If no alias → return the cleaned string as the tentative key

The normalizer NEVER writes to the DB — that is the categorizer's job.
"""

import re
import sqlite3
from functools import lru_cache
from typing import Optional

# ── Regex patterns applied in order ──────────────────────────────────────────

_LEGAL_SUFFIXES = re.compile(
    r'\b(בע"מ|בעמ|ב\.ע\.מ|LTD\.?|INC\.?|CO\.?|LLC\.?|PLC\.?|GmbH|S\.A\.?)\b',
    re.IGNORECASE
)

# Branch / store numbers that appear after the name (e.g. "שופרסל 452", "RAMI LEVY 009")
_BRANCH_NUMBERS = re.compile(r'\s+\d{2,5}$')

# Date fragments appended by some banks (e.g. "MERCHANT 12/06", "MERCHANT 12.06.26")
_DATE_FRAGMENTS = re.compile(r'\s+\d{1,2}[./]\d{2}([./]\d{2,4})?$')

# Transaction reference codes (e.g. "TXN#123456", "#REF 99887")
_REF_CODES = re.compile(r'\s*(#|REF\s*|TXN\s*)\d+$', re.IGNORECASE)

# Collapse multiple spaces to single
_MULTI_SPACE = re.compile(r'\s{2,}')

# ── Known merchant aliases (seed — institution-level, not user-specific) ─────
# Format: (raw_pattern_or_prefix, canonical_key)
# These fire AFTER normalization of the raw string so patterns are already clean.
_SEED_ALIASES: list[tuple[str, str]] = [
    # Supermarkets
    ('SHUFERSAL',       'SHUFERSAL'),
    ('שופרסל',          'SHUFERSAL'),
    ('SUPER SAL',       'SHUFERSAL'),
    ('SUPERSAL',        'SHUFERSAL'),
    ('RAMI LEVY',       'RAMI_LEVY'),
    ('רמי לוי',         'RAMI_LEVY'),
    ('RAMIYLEVY',       'RAMI_LEVY'),
    ('VICTORY',         'VICTORY'),
    ('ויקטורי',         'VICTORY'),
    ('MEGA',            'MEGA'),
    ('מגה',             'MEGA'),
    ('YOCHANANOF',      'YOCHANANOF'),
    ('יוחננוף',         'YOCHANANOF'),
    ('OSHER AD',        'OSHER_AD'),
    ('אושר עד',         'OSHER_AD'),
    ('MAHSANE HASHUK',  'MAHSANE_HASHUK'),
    ('מחסני השוק',      'MAHSANE_HASHUK'),
    ('TIVTAAM',         'TIVTAAM'),
    ('טיב טעם',         'TIVTAAM'),
    ('AM PM',           'AM_PM'),
    ('אמ פמ',           'AM_PM'),
    # Pharmacy
    ('SUPER PHARM',     'SUPER_PHARM'),
    ('SUPER-PHARM',     'SUPER_PHARM'),
    ('סופר פארם',       'SUPER_PHARM'),
    ('סופר-פארם',       'SUPER_PHARM'),
    ('SUPER_PHARM',     'SUPER_PHARM'),
    ('NEW PHARM',       'NEW_PHARM'),
    ('ניו פארם',        'NEW_PHARM'),
    ('BOOTS',           'BOOTS'),
    ('DRUGSTORE',       'SUPER_PHARM'),
    ('דראגסטור',        'SUPER_PHARM'),
    ('בי דראגסטורס',    'SUPER_PHARM'),
    # Fuel / Energy
    ('PAZ',             'PAZ'),
    ('פז',              'PAZ'),
    ('SONOL',           'SONOL'),
    ('סונול',           'SONOL'),
    ('DELEK',           'DELEK'),
    ('דלק',             'DELEK_STATIONS'),
    ('DORS',            'DORS'),
    ('ORL',             'ORL_FUEL'),
    ('YELLOW',          'YELLOW_FUEL'),
    # Communications
    ('HOT ',            'HOT'),
    ('HOT.CO',          'HOT'),
    ('הוט',             'HOT'),
    ('BEZEQ',           'BEZEQ'),
    ('בזק',             'BEZEQ'),
    ('PARTNER',         'PARTNER'),
    ('פרטנר',           'PARTNER'),
    ('CELLCOM',         'CELLCOM'),
    ('סלקום',           'CELLCOM'),
    ('PELEPHONE',       'PELEPHONE'),
    ('פלאפון',          'PELEPHONE'),
    ('012',             'BEZEQ_INT'),
    ('013',             'BEZEQ_INT'),
    # Streaming / Subscriptions
    ('NETFLIX',         'NETFLIX'),
    ('SPOTIFY',         'SPOTIFY'),
    ('APPLE.COM',       'APPLE'),
    ('APPLE ',          'APPLE'),
    ('GOOGLE ',         'GOOGLE'),
    ('YOUTUBE',         'YOUTUBE'),
    ('DISNEY+',         'DISNEY_PLUS'),
    ('AMAZON PRIME',    'AMAZON_PRIME'),
    ('YES ',            'YES_TV'),
    ('OPENAI',          'OPENAI_CHATGPT'),
    ('CHATGPT',         'OPENAI_CHATGPT'),
    ('MICROSOFT',       'MICROSOFT'),
    ('CANVA',           'CANVA'),
    ('DROPBOX',         'DROPBOX'),
    ('ICLOUD',          'APPLE'),
    ('ADOBE',           'ADOBE'),
    # Clothing
    ('H&M',             'HM'),
    ('ZARA',            'ZARA'),
    ('FOX ',            'FOX'),
    ('פוקס',            'FOX'),
    ('אתר פוקס',        'FOX'),
    ('GOLF ',           'GOLF'),
    ('גולף',            'GOLF'),
    ('CASTRO',          'CASTRO'),
    ('קסטרו',           'CASTRO'),
    ('SHEIN',           'SHEIN'),
    ('ADIDAS',          'ADIDAS'),
    ('NIKE',            'NIKE'),
    ('MAX FASHION',     'MAX_FASHION'),
    ('10 מקס',          'MAX_FASHION'),
    ('מקס פשיון',       'MAX_FASHION'),
    ('RENUAR',          'RENUAR'),
    ('רנואר',           'RENUAR'),
    ('KENVELO',         'KENVELO'),
    ('TERMINAL X',      'TERMINAL_X'),
    ('טרמינל X',        'TERMINAL_X'),
    # Food delivery
    ('WOLT',            'WOLT'),
    ('10BIS',           'TEN_BIS'),
    ('10 BIS',          'TEN_BIS'),
    ('עשר ביס',         'TEN_BIS'),
    # Supermarkets - additional
    ('סיטי מרקט',       'CITY_MARKET'),
    ('CITY MARKET',     'CITY_MARKET'),
    ('קיוסק מרקט',      'KIOSK_MARKET'),
    ('סופר עבאדי',      'SHUFERSAL'),   # franchise of Shufersal
    ('KOLBO',           'KOLBO'),
    ('כלבו',            'KOLBO'),
    # Health / Beauty
    ('IHERB',           'IHERB'),
    ('HAIR COSMETICS',  'HAIR_COSMETICS'),
    # Transport — public / parking
    ('דן חברה לתחבורה', 'DAN_BUS'),
    ('אגד',             'EGGED_BUS'),
    ('EGGED',           'EGGED_BUS'),
    ('רכבת ישראל',      'ISRAEL_RAIL'),
    ('ISRAEL RAIL',     'ISRAEL_RAIL'),
    ('פנגו',            'PANGO_PARKING'),
    ('PANGO',           'PANGO_PARKING'),
    ('דור אלון',        'DOR_ALON'),
    ('DOR ALON',        'DOR_ALON'),
    ('מ. התחבורה',      'DAN_BUS'),
    # Education / Culture
    ('אגוש',            'AGUSH_EDU'),
    ('מורים',           'AGUSH_EDU'),
    # Entertainment / Cinema
    ('לבידור',          'ENTERTAINMENT'),
    ('קשרת',            'ENTERTAINMENT'),
    ('מובילנד',         'CINEMA'),
    ('YES TV',          'YES_TV'),
    # Sport / Gym
    ('ספייס ',          'GYM'),
    ('מועדון כושר',     'GYM'),
    # Insurance — additional
    ('כלל רכב',         'CLAL_INSURANCE'),
    ('כלל דירה',        'CLAL_INSURANCE'),
    # Electronics
    ('KSP',             'KSP_ELECTRONICS'),
    # Subscriptions — additional
    ('CLAUDE.AI',       'ANTHROPIC_CLAUDE'),
    ('ANTHROPIC',       'ANTHROPIC_CLAUDE'),
    ('GOOGLE*',         'GOOGLE'),
    ('GOOGLE CLOUD',    'GOOGLE'),
    # Clothing — Max branches
    ('מקס ',            'MAX_FASHION'),
    ('MAX ',            'MAX_FASHION'),
    # Credit card fees
    ('ישראכרט גביה',    'CREDIT_CARD_FEE'),
    ('דמי כרטיס',       'CREDIT_CARD_FEE'),
    # Cash back
    ('החזר CASHPRO',    'CASHBACK'),
    ('CASHPRO',         'CASHBACK'),
    # Finance / Transfers
    ('BIT',             'BIT_TRANSFER'),
    ('PAYBOX',          'PAYBOX'),
    ('PEPPER',          'PEPPER_PAY'),
    # Insurance
    ('הראל',            'HAREL'),
    ('HAREL',           'HAREL'),
    ('כלל ביטוח',       'CLAL_INSURANCE'),
    ('מגדל',            'MIGDAL'),
    ('פניקס',           'PHOENIX'),
    ('מנורה',           'MENORA'),
    # Mortgage / banks
    ('דסק-משכנתא',      'BANK_HAPOALIM_MORTGAGE'),
    ('MORTGAGE',        'MORTGAGE'),
    ('משכנתא',          'MORTGAGE'),
]

# Build prefix-lookup dict from seed (longest match wins)
_SEED_MAP: dict[str, str] = {k.upper(): v for k, v in _SEED_ALIASES}


def normalize_merchant(raw: str) -> str:
    """
    Return a canonical merchant key for a raw bank/card description string.
    Pure string transformation — no DB access.
    """
    if not raw:
        return 'UNKNOWN'

    text = raw.strip().upper()

    # Remove legal suffixes
    text = _LEGAL_SUFFIXES.sub('', text)
    # Remove date fragments
    text = _DATE_FRAGMENTS.sub('', text)
    # Remove trailing reference codes
    text = _REF_CODES.sub('', text)
    # Remove trailing branch numbers (2–5 digits at end)
    text = _BRANCH_NUMBERS.sub('', text)
    # Collapse whitespace
    text = _MULTI_SPACE.sub(' ', text).strip()

    # Seed alias lookup: try longest prefix match
    best_key: Optional[str] = None
    best_len = 0
    for pattern, canonical in _SEED_MAP.items():
        if text.startswith(pattern) and len(pattern) > best_len:
            best_key = canonical
            best_len = len(pattern)

    return best_key if best_key else text


def resolve_merchant_key(raw: str, user_id: int, conn: sqlite3.Connection) -> str:
    """
    Full resolution: normalize → check user's merchant_aliases table → return key.
    If no alias exists, the normalized string is returned as the tentative key.
    The caller decides whether to create an alias record.
    """
    normalized = normalize_merchant(raw)

    # Check user-specific alias first
    row = conn.execute(
        "SELECT merchant_key FROM merchant_aliases WHERE user_id=? AND raw_text=?",
        (user_id, raw.strip())
    ).fetchone()
    if row:
        return row['merchant_key']

    # Check alias by normalized form
    row = conn.execute(
        "SELECT merchant_key FROM merchant_aliases WHERE user_id=? AND raw_text=?",
        (user_id, normalized)
    ).fetchone()
    if row:
        return row['merchant_key']

    return normalized
