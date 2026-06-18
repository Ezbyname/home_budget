"""
Expense categorization engine.

Priority order (strict — never skip, never reorder):
  P1  merchant_learning  (user-confirmed, confidence >= 0.65)
  P2  merchant_fingerprints  (keyword + amount scoring, score >= 0.6)
  P3  deterministic rules  (VISA_CATEGORY_MAP, BANK_EXPENSE_PATTERNS, mortgage rule)
  P4  merchant_learning low confidence  (0.40 <= confidence < 0.65)
  P5  unresolved → 'misc', enqueue for AI

The engine returns a CategoryResult dataclass — never just a string.
Callers are responsible for writing the result to expenses.category_source,
expenses.categorization_confidence, and expenses.merchant_key.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from intelligence.normalizer import normalize_merchant, resolve_merchant_key

# ── Result object ─────────────────────────────────────────────────────────────

@dataclass
class CategoryResult:
    category_id: str
    source: str          # merchant_learning|fingerprint|deterministic|low_confidence|unresolved
    confidence: float    # 0.0–1.0
    merchant_key: str
    subcategory: str = ''
    frequency: str = 'random'

    @property
    def is_resolved(self) -> bool:
        return self.source != 'unresolved'


# ── Mortgage / housing hardcoded rule (Phase 1b hotfix) ──────────────────────
# These patterns ALWAYS map to mortgage regardless of anything else.
_MORTGAGE_PATTERNS = [
    'משכנתא', 'MORTGAGE', 'משכנ', 'BANK_HAPOALIM_MORTGAGE',
    'דסק-משכנתא', 'DISCOUNT MORTGAGE', 'LEUMI MORTGAGE',
]

def _is_mortgage(description: str, merchant_key: str) -> bool:
    upper_desc = description.upper()
    upper_key = merchant_key.upper()
    return any(p.upper() in upper_desc or p.upper() in upper_key
               for p in _MORTGAGE_PATTERNS)


# ── Fingerprint scoring ───────────────────────────────────────────────────────

def _score_fingerprints(merchant_key: str, description: str, amount: float,
                        user_id: int, conn: sqlite3.Connection) -> Optional[tuple[str, float]]:
    """
    Score all fingerprint rows for this merchant_key against the transaction context.
    Returns (category_id, score) of the best match, or None if score < 0.6.
    """
    rows = conn.execute(
        "SELECT keyword, amount_min, amount_max, category_id, weight "
        "FROM merchant_fingerprints WHERE user_id=? AND merchant_key=?",
        (user_id, merchant_key)
    ).fetchall()
    if not rows:
        return None

    scores: dict[str, float] = {}
    desc_upper = description.upper()

    for fp in rows:
        cat = fp['category_id']
        score = 0.0
        if fp['keyword'].upper() in desc_upper:
            score += fp['weight']
        if fp['amount_min'] is not None and amount < fp['amount_min']:
            score -= 0.5
        if fp['amount_max'] is not None and amount > fp['amount_max']:
            score -= 0.5
        scores[cat] = scores.get(cat, 0.0) + score

    if not scores:
        return None
    best_cat = max(scores, key=scores.__getitem__)
    best_score = scores[best_cat]
    if best_score < 0.6:
        return None
    # Normalize to 0–1 confidence range (cap at 0.95)
    confidence = min(0.95, 0.6 + (best_score - 0.6) * 0.1)
    return best_cat, confidence


# ── Main resolution function ──────────────────────────────────────────────────

def resolve_category(
    description: str,
    amount: float,
    user_id: int,
    conn: sqlite3.Connection,
    *,
    visa_category_map: dict = None,
    bank_expense_patterns: list = None,
    visa_description_map: list = None,
    apply_legacy_rule_fn=None,
) -> CategoryResult:
    """
    Resolve the best category for an expense transaction.

    Parameters
    ----------
    description : raw transaction description from bank/card
    amount : transaction amount (positive = expense)
    user_id : current user id
    conn : open DB connection
    visa_category_map : optional dict mapping Visa category strings → category_id
    bank_expense_patterns : optional list of (pattern, cat_id, subcat, freq) tuples
    visa_description_map : optional list of (pattern, cat_id, subcat) tuples
    apply_legacy_rule_fn : optional callable(conn, desc, cat, freq, uid) → (cat, freq)
        for backward compat with the existing category_rules table

    Returns
    -------
    CategoryResult
    """
    merchant_key = resolve_merchant_key(description, user_id, conn)

    # ── Mortgage override (highest priority — before all learning) ────────────
    if _is_mortgage(description, merchant_key):
        return CategoryResult(
            category_id='mortgage',
            source='deterministic',
            confidence=0.95,
            merchant_key=merchant_key,
            subcategory='משכנתא',
            frequency='monthly',
        )

    # ── P1: merchant_learning (high confidence) ───────────────────────────────
    ml_row = conn.execute(
        "SELECT category_id, confidence FROM merchant_learning "
        "WHERE user_id=? AND merchant_key=?",
        (user_id, merchant_key)
    ).fetchone()
    if ml_row and ml_row['confidence'] >= 0.65:
        return CategoryResult(
            category_id=ml_row['category_id'],
            source='merchant_learning',
            confidence=ml_row['confidence'],
            merchant_key=merchant_key,
        )

    # ── P2: merchant fingerprints ─────────────────────────────────────────────
    fp_result = _score_fingerprints(merchant_key, description, amount, user_id, conn)
    if fp_result:
        cat_id, fp_confidence = fp_result
        return CategoryResult(
            category_id=cat_id,
            source='fingerprint',
            confidence=fp_confidence,
            merchant_key=merchant_key,
        )

    # ── P3: deterministic rules ───────────────────────────────────────────────

    # P3b: bank expense patterns (prefix/substring match)
    if bank_expense_patterns:
        for pattern, cat_id, subcat, freq in bank_expense_patterns:
            if pattern.upper() in description.upper():
                return CategoryResult(
                    category_id=cat_id,
                    source='deterministic',
                    confidence=0.78,
                    merchant_key=merchant_key,
                    subcategory=subcat,
                    frequency=freq,
                )

    # P3c: Visa category map (Hebrew category string from card statement)
    if visa_category_map:
        for visa_key, cat_id in visa_category_map.items():
            if visa_key in description:
                return CategoryResult(
                    category_id=cat_id,
                    source='deterministic',
                    confidence=0.75,
                    merchant_key=merchant_key,
                )

    # P3d: Visa description map
    if visa_description_map:
        for pattern, cat_id, subcat in visa_description_map:
            if pattern.upper() in description.upper():
                return CategoryResult(
                    category_id=cat_id,
                    source='deterministic',
                    confidence=0.75,
                    merchant_key=merchant_key,
                    subcategory=subcat,
                )

    # P3e: legacy category_rules table
    if apply_legacy_rule_fn:
        new_cat, new_freq = apply_legacy_rule_fn(conn, description, 'misc', 'random', user_id)
        if new_cat and new_cat != 'misc':
            return CategoryResult(
                category_id=new_cat,
                source='deterministic',
                confidence=0.72,
                merchant_key=merchant_key,
                frequency=new_freq,
            )

    # ── P4: merchant_learning (low confidence) ────────────────────────────────
    if ml_row and ml_row['confidence'] >= 0.40:
        return CategoryResult(
            category_id=ml_row['category_id'],
            source='low_confidence',
            confidence=ml_row['confidence'],
            merchant_key=merchant_key,
        )

    # ── P5: unresolved ────────────────────────────────────────────────────────
    return CategoryResult(
        category_id='misc',
        source='unresolved',
        confidence=0.0,
        merchant_key=merchant_key,
    )
