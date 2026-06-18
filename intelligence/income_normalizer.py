"""
Income source normalization and type resolution.

Direction resolution order (strict — never skip ahead):
  1. Explicit sign (+/-) — handled by caller before this module is invoked
  2. Ledger column semantics (זכות/חובה) — handled by caller
  3. Structural position in sheet — handled by caller
  4. Keyword match (this module, INCOME_PATTERNS)
  5. income_learning table lookup (this module)
  6. AI queue — caller handles after this module returns None
"""

import re
import sqlite3
from typing import Optional

# ── Income type constants ─────────────────────────────────────────────────────
INCOME_TYPES = frozenset([
    'salary', 'business', 'rental', 'investment', 'pension',
    'government', 'tax_refund', 'insurance_claim', 'gift', 'internal',
    'other_income',
])

INCOME_TYPE_LABELS_HE = {
    'salary':           'משכורת',
    'business':         'הכנסה עסקית',
    'rental':           'שכירות',
    'investment':       'השקעות',
    'pension':          'קצבת פנסיה',
    'government':       'קצבה ממשלתית',
    'tax_refund':       'החזר מס',
    'insurance_claim':  'פיצוי ביטוח',
    'gift':             'מתנה',
    'internal':         'העברה פנימית',
    'other_income':     'הכנסה אחרת',
}

# ── Keyword patterns for income type detection ───────────────────────────────
# Order matters: more specific patterns first.
INCOME_PATTERNS: list[tuple[list[str], str]] = [
    (['משכורת', 'שכר', 'SALARY', 'PAYROLL', 'תלוש'],                     'salary'),
    (['ביטוח לאומי', 'מל"ל', 'קצבת ילדים', 'הבטחת הכנסה', 'דמי אבטלה',
      'BITUACH LEUMI', 'NATIONAL INSURANCE'],                              'government'),
    (['קצבה', 'פנסיה', 'גמלה', 'קצבת זקנה', 'PENSION'],                 'pension'),
    (['החזר מס', 'רשות המסים', 'TAX REFUND', 'INCOME TAX REFUND'],       'tax_refund'),
    (['זיכוי ביטוח', 'פיצוי ביטוח', 'תגמול ביטוח', 'INSURANCE CLAIM'],  'insurance_claim'),
    (['שכירות', 'דמי שכירות', 'RENTAL', 'AIRBNB', 'BOOKING'],            'rental'),
    (['דיבידנד', 'ריבית זכות', 'תשואה', 'DIVIDEND', 'INTEREST'],         'investment'),
    (['עסק', 'עצמאי', 'חשבונית', 'PAYPAL', 'STRIPE', 'BUSINESS'],       'business'),
    (['מתנה', 'GIFT'],                                                     'gift'),
    (['העברה עצמית', 'העברה מחשבון', 'TRANSFER FROM OWN',
      'SELF TRANSFER', 'INTERNAL'],                                        'internal'),
]

# Legal/noise suffixes same as merchant normalizer
_LEGAL_SUFFIXES = re.compile(
    r'\b(בע"מ|בעמ|ב\.ע\.מ|LTD\.?|INC\.?|CO\.?|LLC\.?)\b',
    re.IGNORECASE
)
_MULTI_SPACE = re.compile(r'\s{2,}')


def normalize_income_source(raw: str) -> str:
    """
    Return a stable canonical key for an income description.
    Pure string transformation — no DB access.
    """
    if not raw:
        return 'UNKNOWN_INCOME'
    text = raw.strip().upper()
    text = _LEGAL_SUFFIXES.sub('', text)
    text = _MULTI_SPACE.sub(' ', text).strip()
    # Replace spaces with underscores for canonical key style
    return re.sub(r'\s+', '_', text)


def detect_income_type_from_keywords(raw: str) -> Optional[str]:
    """
    Step 4 of direction resolution: keyword-based income type detection.
    Returns an income_type string or None if no keyword matches.
    """
    upper = raw.upper()
    for keywords, income_type in INCOME_PATTERNS:
        if any(kw.upper() in upper for kw in keywords):
            return income_type
    return None


def resolve_income_source(raw: str, user_id: int,
                          conn: sqlite3.Connection) -> tuple[Optional[str], str, float]:
    """
    Full resolution pipeline (steps 4–5 of direction order).

    Returns:
        (income_type, source_key, confidence)
        income_type is None if unresolved (caller should enqueue for AI).
    """
    source_key = normalize_income_source(raw)

    # Step 4: keyword match
    kw_type = detect_income_type_from_keywords(raw)
    if kw_type:
        return kw_type, source_key, 0.70

    # Step 5: income_learning lookup (user alias first, then direct key)
    alias_row = conn.execute(
        "SELECT source_key FROM income_aliases WHERE user_id=? AND raw_text=?",
        (user_id, raw.strip())
    ).fetchone()
    lookup_key = alias_row['source_key'] if alias_row else source_key

    learning_row = conn.execute(
        "SELECT income_type, confidence FROM income_learning WHERE user_id=? AND source_key=?",
        (user_id, lookup_key)
    ).fetchone()
    if learning_row and learning_row['confidence'] >= 0.40:
        return learning_row['income_type'], lookup_key, learning_row['confidence']

    return None, source_key, 0.0
