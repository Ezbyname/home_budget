"""
V4 Classifier — Phase B (read-only, pure domain, no DB writes).

Pipeline per expense group:
  normalize descriptions
  → detect settlements / transfers
  → detect parallel streams (bimodal split)
  → classify recurrence (cadence-normalized coverage)
  → classify commitment (keyword rules)
  → classify amount behavior (CV)
  → classify lifecycle (recency gap)
  → classify purpose (category + keyword)
  → estimate planning amount (stable regime)
  → emit PatternResult(s)

Income pipeline:
  group by (person, source, norm_desc)
  → classify recurrence, reliability, income_type
  → set explicit planning_baseline
  → emit IncomeStreamResult

All monetary values use Decimal.
No DB writes anywhere in this module.
"""

from __future__ import annotations

import re
import statistics
import uuid
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal
from typing import Optional

from intelligence.v4_contracts import (
    AmountBehavior, BudgetClass, Cadence, CashflowRole, CommitmentStatus,
    DecisionSource, IncomeStreamResult, IncomeType, LifecycleStatus,
    PatternResult, PurposeType, RecurrenceStatus, ReliabilityStatus,
    ReviewReason, CADENCE_OCCURRENCES_PER_YEAR,
    derive_budget_class, is_reserve_eligible, make_income_stream_key,
    monthly_equivalent, quantize_ils, decimal_from_db,
)

CLASSIFIER_VERSION = "v4.0-phase-b"


# ═══════════════════════════════════════════════════════════════════════════
# INPUT ROW TYPES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ExpenseRow:
    id: int
    date: str          # YYYY-MM-DD
    category_id: str
    description: str
    amount: Decimal    # positive = expense
    source: str        # 'visa'|'bank'|'manual'
    frequency: str
    card: str
    user_id: int


@dataclass
class IncomeRow:
    id: int
    date: str
    person: str
    source: str
    amount: Decimal
    description: str
    is_recurring: int
    user_id: int


# ═══════════════════════════════════════════════════════════════════════════
# DESCRIPTION NORMALIZATION
# ═══════════════════════════════════════════════════════════════════════════

_LEGAL_STRIP = re.compile(
    r'\s+(בע"מ|בעמ|LTD|INC|CO\b|LLC|PLC|GmbH|S\.A\.)',
    re.IGNORECASE,
)
_DATE_STRIP = re.compile(r'\s+\d{1,2}[./]\d{1,2}([./]\d{2,4})?$')
_REF_STRIP   = re.compile(r'\s+(#|REF|TXN)\d+$', re.IGNORECASE)
_DIGITS_TRAIL = re.compile(r'\s+\d{2,5}$')
_SPACES = re.compile(r'\s+')


def normalize_description(raw: str) -> str:
    """
    Conservative normalization for grouping.
    Strips legal suffixes, date fragments, trailing digits, collapses spaces.
    Does NOT stem Hebrew words or apply alias maps — that happens in the
    full merchant resolver. Purpose here is to deduplicate the same merchant
    across slightly different description formats.
    """
    if not raw:
        return "UNKNOWN"
    s = raw.upper().strip()
    s = _LEGAL_STRIP.sub("", s)
    s = _DATE_STRIP.sub("", s)
    s = _REF_STRIP.sub("", s)
    s = _DIGITS_TRAIL.sub("", s)
    s = _SPACES.sub(" ", s).strip()
    return s or "UNKNOWN"


# ═══════════════════════════════════════════════════════════════════════════
# PAYMENT RAIL CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

_SETTLEMENT_KEYWORDS = [
    "DINERS", "ISRACARD", "דיינרס", "ישראכרט",
    "כרטיסי אשראי", "CREDIT CARD", "CAL ", "כאל",
    "MAX ", "מקס ", "ויזה CAL", "VISA CAL",
    "חיוב כרטיס", "תשלום כרטיס",
    "MASTERCARD", "AMEX", "AMERICAN EXPRESS",
]

_TRANSFER_KEYWORDS = [
    "העברה", "TRANSFER", "WIRE", "STANDING ORDER",
    "DIRECT DEBIT TO", "BANK TRANSFER",
]

_INCOME_CREDIT_KEYWORDS = [
    "CREDIT", "זיכוי", "החזר", "REFUND", "CASHBACK",
]


def classify_payment_rail(row: ExpenseRow) -> str:
    """
    Returns 'settlement' | 'transfer' | 'economic'.
    Settlements are credit-card statement debits; they must not be double-counted.
    """
    desc_upper = (row.description or "").upper()
    for kw in _SETTLEMENT_KEYWORDS:
        if kw.upper() in desc_upper:
            return "settlement"
    for kw in _TRANSFER_KEYWORDS:
        if kw.upper() in desc_upper:
            return "transfer"
    # Bank-source large round amounts are often settlements
    if row.source == "bank" and row.amount >= Decimal("500") and row.amount % Decimal("1") == 0:
        pass  # too broad; leave as economic unless matched above
    return "economic"


# ═══════════════════════════════════════════════════════════════════════════
# CADENCE DETECTION
# ═══════════════════════════════════════════════════════════════════════════

_CADENCE_THRESHOLDS = [
    (Cadence.BIWEEKLY,       10,  20),   # 14 ± 4 days
    (Cadence.MONTHLY,        24,  40),   # 30 ± 6 days
    (Cadence.EVERY_2_MONTHS, 50,  75),   # 60 ± 10 days
    (Cadence.QUARTERLY,      80, 100),   # 90 ± 10 days
    (Cadence.SEMIANNUAL,    165, 200),   # 180 ± 15 days
    (Cadence.YEARLY,        330, 395),   # 365 ± 30 days
]


def detect_cadence(dates: list[str]) -> Cadence:
    """
    Infer cadence from a list of ISO date strings.
    Uses median gap between consecutive dates.
    """
    if len(dates) < 2:
        return Cadence.UNKNOWN
    sorted_dates = sorted(date.fromisoformat(d) for d in dates)
    gaps = [(sorted_dates[i + 1] - sorted_dates[i]).days for i in range(len(sorted_dates) - 1)]
    if not gaps:
        return Cadence.UNKNOWN
    median_gap = statistics.median(gaps)
    for cadence, lo, hi in _CADENCE_THRESHOLDS:
        if lo <= median_gap <= hi:
            return cadence
    return Cadence.IRREGULAR


# ═══════════════════════════════════════════════════════════════════════════
# CADENCE COVERAGE (recurrence evidence quality)
# ═══════════════════════════════════════════════════════════════════════════

def cadence_coverage(dates: list[str], cadence: Cadence) -> float:
    """
    Fraction of expected cadence slots that have at least one transaction.
    Returns 0.0 for IRREGULAR/UNKNOWN.

    Example: monthly cadence over 12 months, 10 distinct months present → 0.83
    """
    if cadence not in CADENCE_OCCURRENCES_PER_YEAR:
        return 0.0
    if len(dates) < 2:
        return 0.0
    sorted_dates = sorted(date.fromisoformat(d) for d in dates)
    span_days = (sorted_dates[-1] - sorted_dates[0]).days
    if span_days <= 0:
        return 0.0
    occ_per_year = CADENCE_OCCURRENCES_PER_YEAR[cadence]
    expected_slots = max(1, round(span_days / 365.25 * occ_per_year))
    return min(1.0, len(dates) / expected_slots)


# ═══════════════════════════════════════════════════════════════════════════
# AMOUNT BEHAVIOR
# ═══════════════════════════════════════════════════════════════════════════

def classify_amount_behavior(amounts: list[Decimal]) -> AmountBehavior:
    if len(amounts) < 2:
        return AmountBehavior.UNKNOWN
    mean = sum(amounts) / len(amounts)
    if mean == 0:
        return AmountBehavior.UNKNOWN
    stdev = Decimal(str(statistics.stdev(float(a) for a in amounts)))
    cv = float(stdev / mean)
    if cv < 0.05:
        return AmountBehavior.VERY_STABLE
    if cv < 0.25:
        return AmountBehavior.STABLE
    if cv < 0.60:
        return AmountBehavior.VARIABLE
    return AmountBehavior.HIGHLY_VARIABLE


# ═══════════════════════════════════════════════════════════════════════════
# PARALLEL STREAM DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def _split_bimodal(amounts: list[Decimal]) -> list[list[Decimal]] | None:
    """
    Detect if a list of amounts has two clearly separated clusters.
    Returns [cluster_a, cluster_b] or None if no clear split.

    Algorithm:
      Sort amounts. Find the largest relative gap. If the gap is > 40% of
      the total range AND each cluster has at least 2 members, split there.
    """
    if len(amounts) < 4:
        return None
    sorted_a = sorted(amounts)
    total_range = float(sorted_a[-1] - sorted_a[0])
    if total_range < 1.0:
        return None
    max_gap = 0.0
    split_idx = -1
    for i in range(1, len(sorted_a)):
        gap = float(sorted_a[i] - sorted_a[i - 1])
        if gap > max_gap:
            max_gap = gap
            split_idx = i
    if max_gap / total_range < 0.40:
        return None
    lo_cluster = sorted_a[:split_idx]
    hi_cluster = sorted_a[split_idx:]
    if len(lo_cluster) < 2 or len(hi_cluster) < 2:
        return None
    return [lo_cluster, hi_cluster]


def split_parallel_streams(
    rows: list[ExpenseRow],
) -> list[list[ExpenseRow]]:
    """
    Split rows into 1 or 2 parallel streams based on bimodal amount distribution.
    Returns a list of groups (usually 1, occasionally 2).
    """
    if len(rows) < 4:
        return [rows]
    amounts = [r.amount for r in rows]
    clusters = _split_bimodal(amounts)
    if clusters is None:
        return [rows]
    lo_set = set(clusters[0])
    hi_set = set(clusters[1])
    group_lo = [r for r in rows if r.amount in lo_set]
    group_hi = [r for r in rows if r.amount in hi_set]
    remainder = [r for r in rows if r.amount not in lo_set and r.amount not in hi_set]
    # assign remainder to nearest cluster
    if remainder:
        lo_mean = sum(clusters[0]) / len(clusters[0])
        hi_mean = sum(clusters[1]) / len(clusters[1])
        for r in remainder:
            if abs(r.amount - lo_mean) <= abs(r.amount - hi_mean):
                group_lo.append(r)
            else:
                group_hi.append(r)
    return [group_lo, group_hi]


# ═══════════════════════════════════════════════════════════════════════════
# STABLE CORE + EXTRAS DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def detect_stable_core(amounts: list[Decimal]) -> tuple[list[Decimal], list[Decimal]]:
    """
    Separate a recurring stable core from occasional extras within one group.

    Algorithm:
      1. Find the modal amount cluster (amounts within 10% of the median).
      2. Amounts ≥ 2× the modal median are classified as extras.
      3. Returns (core_amounts, extra_amounts).
    """
    if not amounts:
        return [], []
    sorted_a = sorted(amounts)
    median = Decimal(str(statistics.median(float(a) for a in sorted_a)))
    if median == 0:
        return list(amounts), []
    core = [a for a in amounts if abs(a - median) / median <= 0.10]
    extras = [a for a in amounts if a >= median * Decimal("2")]
    if len(core) < 2:
        return list(amounts), []
    return core, extras


# ═══════════════════════════════════════════════════════════════════════════
# PRICE CHANGE DETECTION
# ═══════════════════════════════════════════════════════════════════════════

def detect_stable_regime(
    rows: list[ExpenseRow],
    recency_months: int = 4,
) -> Optional[Decimal]:
    """
    Detect a recent stable price regime.

    Prefers the most recent stable window over the lifetime average.
    Returns the median of the recent stable window if it passes stability
    criteria, otherwise returns the overall median.
    """
    if not rows:
        return None
    sorted_rows = sorted(rows, key=lambda r: r.date)
    if not sorted_rows:
        return None
    latest = date.fromisoformat(sorted_rows[-1].date)
    cutoff = latest - timedelta(days=recency_months * 30)
    recent = [r for r in sorted_rows if date.fromisoformat(r.date) >= cutoff]
    if len(recent) >= 2:
        recent_amounts = [r.amount for r in recent]
        recent_behavior = classify_amount_behavior(recent_amounts)
        if recent_behavior in (AmountBehavior.VERY_STABLE, AmountBehavior.STABLE):
            return quantize_ils(
                Decimal(str(statistics.median(float(a) for a in recent_amounts)))
            )
    all_amounts = [r.amount for r in sorted_rows]
    return quantize_ils(
        Decimal(str(statistics.median(float(a) for a in all_amounts)))
    )


# ═══════════════════════════════════════════════════════════════════════════
# COMMITMENT CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

_COMMITTED_CATEGORY_IDS = {
    "housing", "mortgage", "insurance", "utilities", "electricity",
    "water", "internet", "phone", "rent", "arnona", "tax",
    "health_insurance", "pension", "training_fund",
    "loan", "credit", "subscription",
}

_COMMITTED_KEYWORDS = [
    "משכנתא", "ביטוח", "INSURANCE", "חשמל", "מים", "ארנונה",
    "סלולר", "אינטרנט", "NETFLIX", "SPOTIFY", "APPLE",
    "GOOGLE", "חינוך", "גן", "MORTGAGE", "ועד בית",
    "קרן השתלמות", "פנסיה", "גמל", "הלוואה",
]

_NON_COMMITTED_KEYWORDS = [
    "קפה", "מסעדה", "RESTAURANT", "CAFE", "אוכל", "סופר",
    "קניות", "בגדים", "SHOPPING",
    # Transport is recurring but NOT a contractual commitment (no cancellation penalty)
    "רב קו", "RAV KAV", "BUS", "TRAIN", "METRO", "דלק", "FUEL",
]


def classify_commitment(
    rows: list[ExpenseRow],
    norm_desc: str,
    cat_id: Optional[str],
) -> CommitmentStatus:
    desc_upper = norm_desc.upper()
    # Category-based committed
    if cat_id and any(c in (cat_id or "").lower() for c in _COMMITTED_CATEGORY_IDS):
        return CommitmentStatus.COMMITTED
    # Keyword committed
    for kw in _COMMITTED_KEYWORDS:
        if kw.upper() in desc_upper:
            return CommitmentStatus.COMMITTED
    # Keyword non-committed
    for kw in _NON_COMMITTED_KEYWORDS:
        if kw.upper() in desc_upper:
            return CommitmentStatus.NON_COMMITTED
    # Recurring with very stable amount → likely committed
    amounts = [r.amount for r in rows]
    behavior = classify_amount_behavior(amounts)
    if behavior == AmountBehavior.VERY_STABLE and len(rows) >= 4:
        return CommitmentStatus.COMMITTED
    return CommitmentStatus.UNCERTAIN


# ═══════════════════════════════════════════════════════════════════════════
# LIFECYCLE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

# Today reference is injected at call time for testability
_REFERENCE_DATE_OVERRIDE: Optional[date] = None


def _today() -> date:
    return _REFERENCE_DATE_OVERRIDE or date.today()


def classify_lifecycle(
    rows: list[ExpenseRow],
    cadence: Cadence,
) -> LifecycleStatus:
    """
    Classify lifecycle based on recency of last transaction.
    """
    if not rows:
        return LifecycleStatus.UNKNOWN
    sorted_rows = sorted(rows, key=lambda r: r.date)
    last_date = date.fromisoformat(sorted_rows[-1].date)
    today = _today()
    gap_days = (today - last_date).days

    # Expected gap for this cadence (in days)
    cadence_days: dict[Cadence, float] = {
        Cadence.MONTHLY:        30,
        Cadence.BIWEEKLY:       14,
        Cadence.EVERY_2_MONTHS: 60,
        Cadence.QUARTERLY:      90,
        Cadence.SEMIANNUAL:    180,
        Cadence.YEARLY:        365,
    }
    expected = cadence_days.get(cadence, 45.0)

    if gap_days <= expected * 1.5:
        return LifecycleStatus.ACTIVE
    if gap_days <= expected * 3:
        return LifecycleStatus.POSSIBLY_STOPPED
    # Check if there was a cluster of activity, then nothing
    first_date = date.fromisoformat(sorted_rows[0].date)
    total_span = (last_date - first_date).days
    if total_span < 90:
        return LifecycleStatus.ENDED
    return LifecycleStatus.POSSIBLY_STOPPED


# ═══════════════════════════════════════════════════════════════════════════
# RECURRENCE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════
#
# Approved V4 recurrence contract:
#
#   0 observations  → UNKNOWN
#   1 observation   → UNKNOWN (single data-point proves nothing)
#   2 observations  → at most POSSIBLE_RECURRING automatically
#   3+ observations → RECURRING only when BOTH hold:
#                       (a) cadence evidence: coverage >= 0.70
#                       (b) recurring semantic plausibility (see below)
#                     POSSIBLE_RECURRING when cadence evidence but ambiguous semantics
#                     NON_RECURRING when coverage < 0.40
#
# Recurring semantic plausibility is INDEPENDENT of CommitmentStatus.
# A utility/subscription/transport payment is semantically plausible as
# recurring even when NON_COMMITTED (e.g. bus fare, gym membership).
# Random discretionary shopping at the same store is NOT plausible as
# recurring even if cadence happens to look regular.
#
# _recurring_semantic_plausibility(norm_desc, cat_id) → True|False|None
#   True  = semantics strongly support recurrence (subscription, utility, etc.)
#   False = semantics argue against recurrence (one-off, shopping)
#   None  = ambiguous / unknown → stays POSSIBLE_RECURRING at 3+
#
# Note: IRREGULAR/UNKNOWN cadence with 6+ observations → POSSIBLE_RECURRING
# (frequency is real but period is irregular — bus, coffee, gym).

# Categories and keywords that indicate recurring plausibility (NOT commitment)
_RECURRING_PLAUSIBLE_CATEGORIES = {
    "mortgage", "rent", "utilities", "electricity", "water", "internet",
    "phone", "insurance", "health_insurance", "pension", "training_fund",
    "loan", "credit", "subscription", "transport", "arnona", "tax",
    "gym", "childcare",
    # Income sources — employer salary / government benefits are recurring
    "employer", "ביטוח לאומי", "government", "social_security",
}
_RECURRING_PLAUSIBLE_KEYWORDS = [
    "משכנתא", "שכר דירה", "חשמל", "מים", "גז", "ארנונה", "ביטוח",
    "אינטרנט", "סלולר", "NETFLIX", "SPOTIFY", "APPLE", "GOOGLE",
    "HOT", "YES", "AMAZON PRIME", "DISNEY",
    "קרן השתלמות", "פנסיה", "גמל", "הלוואה", "ליסינג",
    "קופת חולים", "כללית", "מכבי",
    "רב קו", "RAV KAV", "חניה חודשית",
    "MONTHLY", "SUBSCRIPTION", "חיוב חודשי", "חיוב קבוע",
    # Income-side recurring sources
    "משכורת", "SALARY", "שכר", "WAGE",
    "קצבה", "BENEFIT", "גמלה",
]
# Categories/keywords that argue AGAINST recurring plausibility
_ONE_OFF_CATEGORIES = {"shopping", "entertainment", "restaurant", "travel"}
_ONE_OFF_KEYWORDS = [
    "קניון", "AMAZON.CO", "ZARA", "H&M", "ALIEXPRESS", "תיאטרון", "קולנוע",
    "מסעדה", "RESTAURANT", "CAFE", "קפה", "טיסה", "FLIGHT", "HOTEL",
    "BOOKING", "AIRBNB",
]


def _recurring_semantic_plausibility(
    norm_desc: str,
    cat_id: Optional[str],
) -> Optional[bool]:
    """
    Return True if description/category strongly suggests recurring behaviour,
    False if it suggests one-off behaviour, None if ambiguous.

    This is INDEPENDENT of CommitmentStatus. Transport, utilities and gym
    memberships are semantically recurring even when uncommitted.
    """
    desc_upper = norm_desc.upper()
    cat_lower  = (cat_id or "").lower()

    if cat_lower in _ONE_OFF_CATEGORIES:
        return False
    for kw in _ONE_OFF_KEYWORDS:
        if kw.upper() in desc_upper:
            return False

    if cat_lower in _RECURRING_PLAUSIBLE_CATEGORIES:
        return True
    for kw in _RECURRING_PLAUSIBLE_KEYWORDS:
        if kw.upper() in desc_upper:
            return True

    return None  # ambiguous


def classify_recurrence(
    rows: list[ExpenseRow],
    cadence: Cadence,
    norm_desc: str = "",
    cat_id: Optional[str] = None,
) -> RecurrenceStatus:
    """
    Classify recurrence using cadence evidence + semantic plausibility.

    Approved V4 contract:

      0–1 observations → UNKNOWN  (single data-point proves nothing)
      2   observations → POSSIBLE_RECURRING at most

      3+ with recognized cadence (MONTHLY, QUARTERLY, …):
        semantic=False              → NON_RECURRING
        coverage < 0.40             → POSSIBLE_RECURRING (weak evidence, not NON_RECURRING)
        coverage 0.40–0.69          → POSSIBLE_RECURRING
        coverage >= 0.70, sem=True  → RECURRING
        coverage >= 0.70, sem=None  → POSSIBLE_RECURRING

      3+ with IRREGULAR or UNKNOWN cadence:
        NON_RECURRING requires explicit one-off/negative semantics.
        Absence of detectable cadence is NOT evidence of non-recurrence.
        semantic=True  → POSSIBLE_RECURRING
        semantic=None  → POSSIBLE_RECURRING  (irregular but real spend pattern)
        semantic=False → NON_RECURRING

    Note: no arbitrary row-count threshold is used for IRREGULAR cadence.
    """
    n = len(rows)
    if n == 0:
        return RecurrenceStatus.UNKNOWN
    if n == 1:
        return RecurrenceStatus.UNKNOWN
    if n == 2:
        return RecurrenceStatus.POSSIBLE_RECURRING

    # 3+ observations
    semantic = _recurring_semantic_plausibility(norm_desc, cat_id)

    if cadence in (Cadence.IRREGULAR, Cadence.UNKNOWN):
        # Lack of detectable cadence ≠ non-recurring.
        # Only explicit one-off semantics justify NON_RECURRING here.
        if semantic is False:
            return RecurrenceStatus.NON_RECURRING
        return RecurrenceStatus.POSSIBLE_RECURRING

    # Recognized cadence — use coverage
    coverage = cadence_coverage([r.date for r in rows], cadence)

    if semantic is False:
        return RecurrenceStatus.NON_RECURRING
    if coverage < 0.70:
        # Weak/sparse cadence evidence — avoid strong negative conclusion
        return RecurrenceStatus.POSSIBLE_RECURRING

    # coverage >= 0.70 — sufficient cadence evidence
    if semantic is True:
        return RecurrenceStatus.RECURRING
    # semantic is None (ambiguous) → POSSIBLE_RECURRING
    return RecurrenceStatus.POSSIBLE_RECURRING


# ═══════════════════════════════════════════════════════════════════════════
# PURPOSE CLASSIFICATION
# ═══════════════════════════════════════════════════════════════════════════

_PURPOSE_MAP: list[tuple[PurposeType, list[str]]] = [
    (PurposeType.HOUSING,            ["משכנתא", "MORTGAGE", "שכר דירה", "RENT", "ועד בית"]),
    (PurposeType.UTILITY,            ["חשמל", "מים", "גז", "ELECTRICITY", "WATER", "GAS", "ארנונה", "ARNONA"]),
    (PurposeType.INSURANCE,          ["ביטוח", "INSURANCE", "הראל", "מנורה", "מגדל", "כלל ביטוח", "HAREL", "MENORA"]),
    (PurposeType.LOAN,               ["הלוואה", "LOAN", "ליסינג", "LEASING"]),
    (PurposeType.SUBSCRIPTION,       ["NETFLIX", "SPOTIFY", "APPLE", "GOOGLE", "AMAZON", "DISNEY", "HOT", "YES"]),
    (PurposeType.HEALTH,             ["קופת חולים", "כללית", "מכבי", "CLALIT", "MACCABI", "רופא", "רפואה", "תרופה", "PHARMACY"]),
    (PurposeType.CHILDREN,           ["גן", "חינוך", "SCHOOL", "חוג", "ילדים", "CHILDREN", "KINDERGARTEN"]),
    (PurposeType.EDUCATION,          ["אוניברסיטה", "UNIVERSITY", "COLLEGE", "מכללה", "לימודים", "קורס", "COURSE"]),
    (PurposeType.TRANSPORT,          ["תחבורה", "TRANSPORT", "אוטובוס", "BUS", "רכבת", "TRAIN", "METRO", "רב קו", "RAV KAV", "דלק", "FUEL", "חניה", "PARKING"]),
    (PurposeType.SAVINGS_INVESTMENT, ["קרן השתלמות", "TRAINING FUND", "פנסיה", "PENSION", "גמל", "GEMEL", "חיסכון", "SAVINGS", "INVESTMENT", "השקעה"]),
    (PurposeType.FINANCIAL_FEE,      ["עמלה", "COMMISSION", "BANK FEE", "FEE", "דמי ניהול", "כרטיס אשראי עמלה"]),
    (PurposeType.TAX,                ["מס", "TAX", "ארנונה"]),
    (PurposeType.FOOD,               ["סופר", "SUPERMARKET", "שוק", "MARKET", "FOOD", "מזון"]),
    (PurposeType.SHOPPING,           ["AMAZON", "ZARA", "H&M", "ONLINE SHOPPING", "קניון"]),
    (PurposeType.ENTERTAINMENT,      ["ENTERTAINMENT", "SPORT", "ספורט", "קולנוע", "CINEMA", "תיאטרון", "THEATER"]),
    (PurposeType.TRANSFER,           ["העברה", "TRANSFER"]),
    (PurposeType.CREDIT_CARD_SETTLEMENT, ["DINERS", "ISRACARD", "ישראכרט", "כרטיסי אשראי", "CREDIT CARD PAYMENT"]),
]

_CATEGORY_PURPOSE_MAP: dict[str, PurposeType] = {
    "housing": PurposeType.HOUSING,
    "mortgage": PurposeType.HOUSING,
    "rent": PurposeType.HOUSING,
    "utilities": PurposeType.UTILITY,
    "electricity": PurposeType.UTILITY,
    "water": PurposeType.UTILITY,
    "insurance": PurposeType.INSURANCE,
    "health_insurance": PurposeType.INSURANCE,
    "health": PurposeType.HEALTH,
    "transport": PurposeType.TRANSPORT,
    "education": PurposeType.EDUCATION,
    "children": PurposeType.CHILDREN,
    "savings": PurposeType.SAVINGS_INVESTMENT,
    "pension": PurposeType.SAVINGS_INVESTMENT,
    "training_fund": PurposeType.SAVINGS_INVESTMENT,
    "bank_fees": PurposeType.FINANCIAL_FEE,
    "subscription": PurposeType.SUBSCRIPTION,
    "entertainment": PurposeType.ENTERTAINMENT,
    "food": PurposeType.FOOD,
    "shopping": PurposeType.SHOPPING,
    "tax": PurposeType.TAX,
}


def classify_purpose(norm_desc: str, cat_id: Optional[str]) -> PurposeType:
    if cat_id:
        for cat_key, purpose in _CATEGORY_PURPOSE_MAP.items():
            if cat_key in cat_id.lower():
                return purpose
    desc_upper = norm_desc.upper()
    for purpose, keywords in _PURPOSE_MAP:
        for kw in keywords:
            if kw.upper() in desc_upper:
                return purpose
    return PurposeType.OTHER


# ═══════════════════════════════════════════════════════════════════════════
# REVIEW REASONS DERIVATION
# ═══════════════════════════════════════════════════════════════════════════

def derive_review_reasons(
    recurrence: RecurrenceStatus,
    commitment: CommitmentStatus,
    amount_behavior: AmountBehavior,
    planning_amount: Optional[Decimal],
    membership_confidence_values: list[float],
) -> tuple[bool, tuple[ReviewReason, ...]]:
    reasons: list[ReviewReason] = []
    if recurrence == RecurrenceStatus.POSSIBLE_RECURRING:
        reasons.append(ReviewReason.POSSIBLE_RECURRING)
    if commitment == CommitmentStatus.UNCERTAIN:
        reasons.append(ReviewReason.UNCERTAIN_COMMITMENT)
    if planning_amount is None:
        reasons.append(ReviewReason.AMOUNT_TBD)
    if amount_behavior == AmountBehavior.HIGHLY_VARIABLE:
        reasons.append(ReviewReason.HIGH_CV)
    if membership_confidence_values and min(membership_confidence_values) < 0.7:
        reasons.append(ReviewReason.LOW_CONFIDENCE_MEMBERSHIP)
    return bool(reasons), tuple(reasons)


# ═══════════════════════════════════════════════════════════════════════════
# CORE CLASSIFIER — ONE STREAM
# ═══════════════════════════════════════════════════════════════════════════

def classify_stream(
    rows: list[ExpenseRow],
    description_key: str,
    label: str,
    cat_id: Optional[str],
    stream_index: int = 0,
) -> PatternResult:
    """
    Classify a single stream (already split from parallel detection).
    Returns a PatternResult.
    """
    member_ids = tuple(str(r.id) for r in rows)
    membership_confidence = {str(r.id): 1.0 for r in rows}
    dates = [r.date for r in rows]
    amounts = [r.amount for r in rows]

    # Cadence
    cadence = detect_cadence(dates)

    # Stable core detection — if extras exist, classify core only
    core_amounts, _extras = detect_stable_core(amounts)
    effective_amounts = core_amounts if len(core_amounts) >= 2 else amounts

    # Amount behavior on effective amounts
    amount_behavior = classify_amount_behavior(effective_amounts)

    # Planning amount
    planning_amount: Optional[Decimal] = None
    if len(rows) >= 2:
        regime = detect_stable_regime(rows)
        if regime is not None and regime > Decimal("0"):
            planning_amount = regime

    # Recurrence — pass norm_desc and cat_id for semantic plausibility check
    recurrence = classify_recurrence(rows, cadence, norm_desc=description_key, cat_id=cat_id)

    # Commitment
    commitment = classify_commitment(rows, description_key, cat_id)

    # Lifecycle
    lifecycle = classify_lifecycle(rows, cadence)

    # Purpose
    purpose = classify_purpose(description_key, cat_id)

    # Budget class
    budget_class = derive_budget_class(recurrence, commitment, amount_behavior)

    # Reserve eligibility
    reserve_eligible = is_reserve_eligible(recurrence, commitment, lifecycle, planning_amount)
    monthly_contrib = Decimal("0.00")
    if reserve_eligible and planning_amount is not None:
        if cadence in CADENCE_OCCURRENCES_PER_YEAR:
            monthly_contrib = monthly_equivalent(planning_amount, cadence)
        else:
            monthly_contrib = planning_amount

    # Review reasons
    family_review_required, review_reasons = derive_review_reasons(
        recurrence, commitment, amount_behavior, planning_amount,
        list(membership_confidence.values()),
    )

    stream_label = label if stream_index == 0 else f"{label} (stream {stream_index + 1})"

    return PatternResult(
        description_key=description_key,
        label=stream_label,
        recurrence_status=recurrence,
        commitment_status=commitment,
        amount_behavior=amount_behavior,
        budget_class=budget_class,
        lifecycle_status=lifecycle,
        purpose_type=purpose,
        cadence=cadence,
        planning_amount=planning_amount,
        member_ids=member_ids,
        membership_confidence=membership_confidence,
        evidence_sources=tuple(set(r.source for r in rows)),
        decision_source=DecisionSource.CLASSIFIER,
        family_review_required=family_review_required,
        review_reasons=review_reasons,
        reserve_eligible=reserve_eligible,
        monthly_reserve_contrib=monthly_contrib,
    )


# ═══════════════════════════════════════════════════════════════════════════
# GROUP CLASSIFIER (handles parallel streams)
# ═══════════════════════════════════════════════════════════════════════════

def classify_group(
    rows: list[ExpenseRow],
    description_key: str,
    label: str,
    cat_id: Optional[str],
) -> list[PatternResult]:
    """
    Classify one description group, potentially returning multiple PatternResults
    for parallel streams.
    """
    if not rows:
        return []
    streams = split_parallel_streams(rows)
    results = []
    for i, stream_rows in enumerate(streams):
        if stream_rows:
            results.append(classify_stream(stream_rows, description_key, label, cat_id, i))
    return results


# ═══════════════════════════════════════════════════════════════════════════
# MAIN EXPENSE CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class SettlementRecord:
    """A credit-card settlement detected during classification."""
    description_key: str
    label: str
    member_ids: tuple[str, ...]
    total_amount: Decimal
    months: tuple[str, ...]
    evidence_sources: tuple[str, ...]


def classify_expenses(
    rows: list[ExpenseRow],
    cat_map: Optional[dict[int, str]] = None,
) -> tuple[list[PatternResult], list[SettlementRecord]]:
    """
    Classify all expense rows.
    Returns (patterns, settlements).

    Settlements are separated and tracked but NOT included in reserve totals
    or flexible budget, preventing double-counting.
    """
    if cat_map is None:
        cat_map = {}

    settlements: list[SettlementRecord] = []
    economic_by_group: dict[str, list[ExpenseRow]] = {}
    cat_by_group: dict[str, Optional[str]] = {}

    for row in rows:
        rail = classify_payment_rail(row)
        norm = normalize_description(row.description or "")

        if rail == "settlement":
            # Track as settlement, don't classify as recurring pattern
            months_set: set[str] = set()
            for r2 in rows:
                if normalize_description(r2.description or "") == norm:
                    months_set.add(r2.date[:7])
            # build minimal settlement record
            srows = [r for r in rows if normalize_description(r.description or "") == norm]
            settlements.append(SettlementRecord(
                description_key=norm,
                label=row.description or norm,
                member_ids=tuple(str(r.id) for r in srows),
                total_amount=quantize_ils(sum(r.amount for r in srows)),
                months=tuple(sorted(months_set)),
                evidence_sources=tuple(set(r.source for r in srows)),
            ))
            continue

        if norm not in economic_by_group:
            economic_by_group[norm] = []
            cat_by_group[norm] = cat_map.get(row.id)
        economic_by_group[norm].append(row)

    # Deduplicate settlement entries
    seen_settlements: set[str] = set()
    unique_settlements = []
    for s in settlements:
        if s.description_key not in seen_settlements:
            seen_settlements.add(s.description_key)
            unique_settlements.append(s)

    patterns: list[PatternResult] = []
    for norm_desc, group_rows in economic_by_group.items():
        cat_id = cat_by_group.get(norm_desc)
        label = group_rows[0].description or norm_desc
        patterns.extend(classify_group(group_rows, norm_desc, label, cat_id))

    return patterns, unique_settlements


# ═══════════════════════════════════════════════════════════════════════════
# INCOME CLASSIFIER
# ═══════════════════════════════════════════════════════════════════════════

_SALARY_KEYWORDS   = ["SALARY", "PAYCHECK", "EMPLOYER", "שכר", "משכורת", "מעסיק"]
_BENEFIT_KEYWORDS  = ["GOVERNMENT", "BENEFIT", "ALLOWANCE", "קצבה", "ביטוח לאומי", "BITUACH_LEUMI", "BITUACH LEUMI"]
_TRANSFER_KW_INC   = ["TRANSFER", "FAMILY", "העברה", "HAAVARAH"]
_BONUS_KW          = ["BONUS", "בונוס", "INCENTIVE", "COMMISSION"]


def _classify_income_type(person: str, source: str, description: str) -> IncomeType:
    combined = f"{person} {source} {description}".upper()
    for kw in _BENEFIT_KEYWORDS:
        if kw.upper() in combined:
            return IncomeType.GOVERNMENT_BENEFIT
    for kw in _BONUS_KW:
        if kw.upper() in combined:
            return IncomeType.BONUS
    for kw in _TRANSFER_KW_INC:
        if kw.upper() in combined:
            return IncomeType.FAMILY_TRANSFER
    for kw in _SALARY_KEYWORDS:
        if kw.upper() in combined:
            return IncomeType.SALARY
    # Recurring reliable income from employer-like source → probably salary
    return IncomeType.SALARY


def _classify_income_reliability(
    rows: list[IncomeRow],
    income_type: IncomeType,
    recurrence: RecurrenceStatus,
) -> ReliabilityStatus:
    if income_type == IncomeType.GOVERNMENT_BENEFIT:
        return ReliabilityStatus.RELIABLE
    if income_type in (IncomeType.FAMILY_TRANSFER, IncomeType.BONUS):
        return ReliabilityStatus.UNRELIABLE
    if recurrence == RecurrenceStatus.RECURRING and len(rows) >= 3:
        return ReliabilityStatus.RELIABLE
    if recurrence == RecurrenceStatus.POSSIBLE_RECURRING:
        return ReliabilityStatus.UNKNOWN
    if len(rows) < 2:
        return ReliabilityStatus.UNKNOWN
    return ReliabilityStatus.UNRELIABLE


def classify_income(
    rows: list[IncomeRow],
    reviewed_baselines: Optional[dict[str, Decimal]] = None,
) -> list[IncomeStreamResult]:
    """
    Classify income rows into streams grouped by (person, source, norm_desc).
    reviewed_baselines: stream_key → Decimal override for planning_baseline.
    """
    if reviewed_baselines is None:
        reviewed_baselines = {}

    groups: dict[str, list[IncomeRow]] = {}
    for row in rows:
        norm_desc = normalize_description(row.description or "")
        key = make_income_stream_key(row.person or "", row.source or "", norm_desc)
        if key not in groups:
            groups[key] = []
        groups[key].append(row)

    results: list[IncomeStreamResult] = []
    for stream_key, stream_rows in groups.items():
        sample = stream_rows[0]
        norm_desc = normalize_description(sample.description or "")
        dates = [r.date for r in stream_rows]
        amounts = [r.amount for r in stream_rows]

        cadence = detect_cadence(dates)
        recurrence = classify_recurrence(
            # income rows wrapped as minimal duck-typed objects for recurrence API
            [type("_R", (), {"date": r.date, "amount": r.amount, "source": r.source})()
             for r in stream_rows],
            cadence,
            norm_desc=norm_desc,
            cat_id=sample.source or "",  # income source as category hint
        )
        income_type = _classify_income_type(
            sample.person or "", sample.source or "", sample.description or ""
        )
        reliability = _classify_income_reliability(stream_rows, income_type, recurrence)
        amount_behavior = classify_amount_behavior(amounts)

        # planning_baseline: reviewed override takes priority
        if stream_key in reviewed_baselines:
            planning_baseline = reviewed_baselines[stream_key]
            decision_source = DecisionSource.FAMILY_REVIEW
        elif income_type in (IncomeType.FAMILY_TRANSFER, IncomeType.BONUS):
            planning_baseline = Decimal("0.00")
            decision_source = DecisionSource.CLASSIFIER
        else:
            regime = detect_stable_regime(
                [type("_R", (), {
                    "date": r.date, "amount": r.amount, "source": "income",
                    "id": r.id,
                })() for r in stream_rows]
            )
            planning_baseline = regime if regime is not None else Decimal("0.00")
            decision_source = DecisionSource.CLASSIFIER

        # Review reasons
        review_reasons: list[ReviewReason] = []
        family_review_required = False
        if recurrence == RecurrenceStatus.POSSIBLE_RECURRING:
            review_reasons.append(ReviewReason.POSSIBLE_RECURRING)
            family_review_required = True
        if planning_baseline == Decimal("0.00") and income_type == IncomeType.SALARY:
            review_reasons.append(ReviewReason.AMOUNT_TBD)
            family_review_required = True

        results.append(IncomeStreamResult(
            stream_key=stream_key,
            person=sample.person or "",
            source=sample.source or "",
            description_key=norm_desc,
            income_type=income_type,
            recurrence_status=recurrence,
            reliability_status=reliability,
            amount_behavior=amount_behavior,
            cadence=cadence,
            planning_baseline=planning_baseline,
            member_ids=tuple(str(r.id) for r in stream_rows),
            evidence_sources=tuple(set(r.source for r in stream_rows)),
            decision_source=decision_source,
            family_review_required=family_review_required,
            review_reasons=tuple(review_reasons),
        ))

    return results
