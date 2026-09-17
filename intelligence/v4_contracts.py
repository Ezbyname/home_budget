"""
V4 Domain Contracts — Phase A (contracts only, no DB/API/UI code).

Defines the canonical enums, dataclasses, domain functions, and protocols
for the V4 Recurring & Cashflow Intelligence system.

Key design decisions recorded here:
  - Pattern identity (cashflow_pattern) is separate from versioned financial
    state (cashflow_pattern_state). No financial attributes live on the
    identity record.
  - State history is strictly append-only / immutable. Rows are never
    UPDATEd or DELETEd after insertion. Transitions are new INSERT rows
    with supersedes_state_id pointing to the prior state.
  - Reserve eligibility is derived from financial state only:
    RECURRING + COMMITTED + ACTIVE + known planning_amount.
    purpose_type (SAVINGS_INVESTMENT, FINANCIAL_FEE, etc.) does NOT
    exclude a pattern from the reserve.
  - Raw classifier output and effective reviewed output are distinct.
    Reconciliation targets the effective result; raw differences are
    auditable, not automatic failures.
  - Household ownership: household_id is nullable only during
    migration/bootstrap. Normal operation requires a resolved household.

MONEY REPRESENTATION
  All monetary amounts use Decimal, never float, to support exact
  agora-level reconciliation.

  When values originate from SQLite REAL columns:
      Decimal(str(db_value))   # correct: via string
      Decimal(db_float)        # WRONG: inherits float error

  Rounding / quantization policy:
      - Intermediate calculations: full Decimal precision.
      - Financial boundary values (reserve totals, income totals,
        reconciliation fields): quantize to TWO_PLACES = Decimal("0.01")
        using ROUND_HALF_UP at the point where a value leaves the
        domain layer and enters persistence, display, or comparison.
      - membership_confidence scores are float (not money).
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from enum import Enum
from typing import Optional, Protocol, runtime_checkable


# ═══════════════════════════════════════════════════════════════════════════
# MONEY CONSTANTS
# ═══════════════════════════════════════════════════════════════════════════

# ILS monetary rounding policy (applies to all domain-layer money operations):
#   1. All monetary arithmetic uses Decimal — never float.
#   2. SQLite REAL → Decimal via Decimal(str(value)); never Decimal(float_value).
#   3. Intermediate calculations are not rounded prematurely.
#   4. Final ILS monetary values (reserve totals, income totals,
#      reconciliation fields) are quantized to Decimal("0.01").
#   5. Rounding mode is ALWAYS ROUND_HALF_UP.
#
# Rounding examples:
#   quantize_ils(Decimal("1.005"))   → Decimal("1.01")   (half rounds up)
#   quantize_ils(Decimal("1.004"))   → Decimal("1.00")   (below half stays down)
#   quantize_ils(Decimal("443.005")) → Decimal("443.01") (consistent with rule 5)

TWO_PLACES = Decimal("0.01")
ILS_ROUNDING = ROUND_HALF_UP


def quantize_ils(amount: Decimal) -> Decimal:
    """Round a Decimal to agora precision using ROUND_HALF_UP."""
    return amount.quantize(TWO_PLACES, rounding=ILS_ROUNDING)


def decimal_from_db(db_value) -> Decimal:
    """
    Convert a SQLite REAL value to Decimal without inheriting float error.
    Always route through str() — never pass the float directly to Decimal().
    """
    return Decimal(str(db_value))


# ═══════════════════════════════════════════════════════════════════════════
# ENUMS
# ═══════════════════════════════════════════════════════════════════════════

class RecurrenceStatus(str, Enum):
    RECURRING          = "RECURRING"
    POSSIBLE_RECURRING = "POSSIBLE_RECURRING"
    NON_RECURRING      = "NON_RECURRING"
    UNKNOWN            = "UNKNOWN"


class CommitmentStatus(str, Enum):
    COMMITTED     = "COMMITTED"
    NON_COMMITTED = "NON_COMMITTED"
    UNCERTAIN     = "UNCERTAIN"


class AmountBehavior(str, Enum):
    VERY_STABLE     = "VERY_STABLE"      # CV < 0.05
    STABLE          = "STABLE"           # CV 0.05–0.25
    VARIABLE        = "VARIABLE"         # CV 0.25–0.60
    HIGHLY_VARIABLE = "HIGHLY_VARIABLE"  # CV >= 0.60
    UNKNOWN         = "UNKNOWN"          # n < 2


class BudgetClass(str, Enum):
    FIXED_AMOUNT_RECURRING    = "FIXED_AMOUNT_RECURRING"
    VARIABLE_AMOUNT_RECURRING = "VARIABLE_AMOUNT_RECURRING"
    RECURRING_NON_COMMITMENT  = "RECURRING_NON_COMMITMENT"
    NON_RECURRING_EXPENSE     = "NON_RECURRING_EXPENSE"
    UNCERTAIN                 = "UNCERTAIN"


class LifecycleStatus(str, Enum):
    ACTIVE           = "ACTIVE"
    POSSIBLY_STOPPED = "POSSIBLY_STOPPED"
    CANCELLED        = "CANCELLED"
    ENDED            = "ENDED"
    UNKNOWN          = "UNKNOWN"
    # NOT_APPROVED: TBD (use planning_amount=None + ReviewReason.AMOUNT_TBD)
    # NOT_APPROVED: PAUSED (requires separate Product decision)


class PurposeType(str, Enum):
    HOUSING                = "HOUSING"
    UTILITY                = "UTILITY"
    INSURANCE              = "INSURANCE"
    LOAN                   = "LOAN"
    SUBSCRIPTION           = "SUBSCRIPTION"
    HEALTH                 = "HEALTH"
    CHILDREN               = "CHILDREN"
    EDUCATION              = "EDUCATION"
    TRANSPORT              = "TRANSPORT"
    SAVINGS_INVESTMENT     = "SAVINGS_INVESTMENT"
    FINANCIAL_FEE          = "FINANCIAL_FEE"
    TAX                    = "TAX"
    FOOD                   = "FOOD"
    SHOPPING               = "SHOPPING"
    ENTERTAINMENT          = "ENTERTAINMENT"
    TRANSFER               = "TRANSFER"
    CREDIT_CARD_SETTLEMENT = "CREDIT_CARD_SETTLEMENT"
    OTHER                  = "OTHER"
    UNKNOWN                = "UNKNOWN"


class CashflowRole(str, Enum):
    """
    Reporting / grouping label. Does NOT determine reserve eligibility.
    Reserve eligibility is determined by financial state:
    RECURRING + COMMITTED + ACTIVE + known planning_amount.
    A pattern with purpose_type=SAVINGS_INVESTMENT can have
    cashflow_role=RESERVE if it is a committed recurring obligation.
    """
    RESERVE    = "RESERVE"
    FLEXIBLE   = "FLEXIBLE"
    SAVINGS    = "SAVINGS"
    FEE        = "FEE"
    SETTLEMENT = "SETTLEMENT"
    TRANSFER   = "TRANSFER"
    INCOME     = "INCOME"


class ReliabilityStatus(str, Enum):
    """
    Economic reliability of an income stream.

    Seasonality is a temporal / cadence property that belongs to the
    cadence/pattern dimension, not here.  A stream may be both seasonal
    AND reliable (e.g. a guaranteed annual bonus).  Use UNKNOWN when the
    classifier lacks sufficient evidence to decide.
    """
    RELIABLE   = "RELIABLE"    # employer / government obligation
    UNRELIABLE = "UNRELIABLE"  # freelance / sporadic
    UNKNOWN    = "UNKNOWN"     # insufficient evidence to classify


class IncomeType(str, Enum):
    """
    Semantic economic type of an income stream.
    Independent from recurrence_status, reliability_status, amount_behavior.

    Examples:
      husband salary:  income_type=SALARY, reliability=RELIABLE, amount_behavior=VARIABLE
      child allowance: income_type=GOVERNMENT_BENEFIT, reliability=RELIABLE
      family support:  income_type=FAMILY_TRANSFER (not included in
                       reliable planning baseline by default)
      bonus:           income_type=BONUS (not included in normal reliable
                       planning baseline by default)
    """
    SALARY             = "SALARY"
    GOVERNMENT_BENEFIT = "GOVERNMENT_BENEFIT"
    FAMILY_TRANSFER    = "FAMILY_TRANSFER"
    BONUS              = "BONUS"
    OTHER              = "OTHER"
    UNKNOWN            = "UNKNOWN"


class Cadence(str, Enum):
    MONTHLY        = "monthly"
    BIWEEKLY       = "biweekly"       # ~26 payments/year (every 2 weeks)
    EVERY_2_MONTHS = "every_2_months" # 6 payments/year (arnona, water)
    QUARTERLY      = "quarterly"      # 4 payments/year
    SEMIANNUAL     = "semiannual"     # 2 payments/year
    YEARLY         = "yearly"         # 1 payment/year
    IRREGULAR      = "irregular"      # no automatic normalization
    UNKNOWN        = "unknown"        # no automatic normalization


class DecisionSource(str, Enum):
    CLASSIFIER      = "classifier"
    FAMILY_REVIEW   = "family_review"
    IMPORT_SIGNAL   = "import_signal"
    MANUAL_OVERRIDE = "manual_override"


class ReviewReason(str, Enum):
    AMOUNT_TBD                = "AMOUNT_TBD"
    POSSIBLE_RECURRING        = "POSSIBLE_RECURRING"
    UNCERTAIN_COMMITMENT      = "UNCERTAIN_COMMITMENT"
    LOW_CONFIDENCE_MEMBERSHIP = "LOW_CONFIDENCE_MEMBERSHIP"
    RECONCILIATION_CONFLICT   = "RECONCILIATION_CONFLICT"
    HIGH_CV                   = "HIGH_CV"
    UNKNOWN_MERCHANT          = "UNKNOWN_MERCHANT"
    POSSIBLY_STOPPED          = "POSSIBLY_STOPPED"
    UNRECONCILED_SETTLEMENT   = "UNRECONCILED_SETTLEMENT"
    NEW_EVIDENCE_FOUND        = "NEW_EVIDENCE_FOUND"


# ═══════════════════════════════════════════════════════════════════════════
# CADENCE — OCCURRENCES PER YEAR AND MONTHLY NORMALIZATION
#
# Model: monthly_equivalent = payment_amount × occurrences_per_year / 12
#
# IRREGULAR and UNKNOWN have no automatic normalization factor.
# Do not add them to CADENCE_OCCURRENCES_PER_YEAR.
# ═══════════════════════════════════════════════════════════════════════════

CADENCE_OCCURRENCES_PER_YEAR: dict[Cadence, int] = {
    Cadence.MONTHLY:        12,
    Cadence.BIWEEKLY:       26,  # every 2 weeks → 26 payments/year
    Cadence.EVERY_2_MONTHS:  6,  # arnona, water
    Cadence.QUARTERLY:       4,
    Cadence.SEMIANNUAL:      2,
    Cadence.YEARLY:          1,
}


def monthly_equivalent(payment_amount: Decimal, cadence: Cadence) -> Decimal:
    """
    Convert a single payment_amount to its monthly equivalent.

    Raises ValueError for IRREGULAR and UNKNOWN cadences — those require
    manual or statistical estimation, not automatic normalization.

    Example: ₪886 bimonthly (EVERY_2_MONTHS) → ₪443.00/month
    """
    if cadence not in CADENCE_OCCURRENCES_PER_YEAR:
        raise ValueError(
            f"Cadence {cadence} has no automatic monthly normalization. "
            "Estimate manually or statistically."
        )
    occ = CADENCE_OCCURRENCES_PER_YEAR[cadence]
    return quantize_ils(payment_amount * occ / 12)


# ═══════════════════════════════════════════════════════════════════════════
# DOMAIN DATACLASSES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass(frozen=True)
class PatternStateContract:
    """
    One immutable versioned state of a cashflow pattern.

    Write invariant: rows are NEVER updated or deleted after initial INSERT.
    State transitions are new INSERT rows with supersedes_state_id set.
    The effective end of a prior state is derived logically from the
    next state's valid_from — it is not stored in a valid_until field.

    Current state of pattern P:
        SELECT * FROM cashflow_pattern_state s WHERE s.pattern_id = P
        AND NOT EXISTS (
          SELECT 1 FROM cashflow_pattern_state s2
          WHERE s2.supersedes_state_id = s.state_id
        )
    """
    state_id:             str
    pattern_id:           str
    valid_from:           str                     # YYYY-MM-DD; set at INSERT; never changed
    supersedes_state_id:  Optional[str]           # None for the first state of a pattern
    recurrence_status:    RecurrenceStatus
    commitment_status:    CommitmentStatus
    amount_behavior:      AmountBehavior
    budget_class:         BudgetClass
    lifecycle_status:     LifecycleStatus
    cadence:              Cadence
    planning_amount:      Optional[Decimal]        # None = amount TBD; Decimal for precision
    purpose_type:         PurposeType
    evidence_sources:     tuple[str, ...]
    decision_source:      DecisionSource           # origin of this state row as a whole
    changed_by:           str                      # user_id or "classifier"
    change_reason:        Optional[str]


@dataclass(frozen=True)
class PatternIdentityContract:
    """
    Stable identity of a cashflow pattern.
    Contains NO mutable/versioned financial attributes.
    All financial state lives in PatternStateContract.

    household_id is nullable only during migration/bootstrap.
    Production writes must have a resolved, non-null household_id.
    """
    id:              str
    household_id:    Optional[str]   # nullable ONLY during migration/bootstrap
    label:           str
    description_key: str
    category_id:     Optional[str]
    created_at:      str


@dataclass(frozen=True)
class PatternResult:
    """
    Output of the V4 classifier for one detected recurring stream.
    Represents RAW classifier output — before human overrides are applied.
    Contains transaction membership for this stream.

    Multiple PatternResults may share a description_key when parallel
    streams are detected (e.g. Gal Naomi ₪607 + ₪2,000).
    """
    description_key:          str
    label:                    str
    recurrence_status:        RecurrenceStatus
    commitment_status:        CommitmentStatus
    amount_behavior:          AmountBehavior
    budget_class:             BudgetClass
    lifecycle_status:         LifecycleStatus
    purpose_type:             PurposeType
    cadence:                  Cadence
    planning_amount:          Optional[Decimal]    # None = TBD; Decimal for precision
    member_ids:               tuple[str, ...]      # expense_id list (this stream only)
    membership_confidence:    dict[str, float]     # expense_id → 0.0–1.0 (float OK, not money)
    evidence_sources:         tuple[str, ...]
    decision_source:          DecisionSource
    family_review_required:   bool
    review_reasons:           tuple[ReviewReason, ...]
    reserve_eligible:         bool                 # derived; see is_reserve_eligible()
    monthly_reserve_contrib:  Decimal              # Decimal(0) if not reserve_eligible


@dataclass(frozen=True)
class IncomeStreamResult:
    """
    Output of the V4 classifier for one income stream.
    Streams are grouped by (person, source, normalized_description).
    Two salaries with the same description but different persons
    produce two distinct IncomeStreamResults.

    planning_baseline is an explicit Decimal amount — never computed as amount × weight.
    VARIABLE amount_behavior does NOT reduce planning_baseline for RELIABLE streams.

    income_type is the semantic economic category (SALARY, GOVERNMENT_BENEFIT, etc.),
    independent from recurrence_status and reliability_status.
    FAMILY_TRANSFER and BONUS are not included in reliable planning baseline by default.
    """
    stream_key:             str             # hash(person, source, norm_desc)
    person:                 str
    source:                 str
    description_key:        str
    income_type:            IncomeType      # semantic economic category
    recurrence_status:      RecurrenceStatus
    reliability_status:     ReliabilityStatus
    amount_behavior:        AmountBehavior
    cadence:                Cadence
    planning_baseline:      Decimal         # explicit reviewed/derived amount; never float
    member_ids:             tuple[str, ...]
    evidence_sources:       tuple[str, ...]
    decision_source:        DecisionSource
    family_review_required: bool
    review_reasons:         tuple[ReviewReason, ...]


@dataclass(frozen=True)
class RawClassifierOutput:
    """
    What the V4 classifier derives from transaction evidence alone.
    No human overrides applied.
    """
    patterns:              tuple[PatternResult, ...]
    income_streams:        tuple[IncomeStreamResult, ...]
    planning_income_raw:   Decimal          # sum of income stream baselines
    monthly_reserve_raw:   Decimal          # sum of reserve-eligible monthly contribs
    family_review_items:   tuple[str, ...]  # pattern labels needing review


@dataclass(frozen=True)
class EffectiveFinancialResult:
    """
    RawClassifierOutput after human overrides (cashflow_override table) are applied.
    This is what the Cashflow Engine consumes.
    Reconciliation targets this result, not the raw output.
    """
    patterns:                    tuple[PatternResult, ...]
    income_streams:              tuple[IncomeStreamResult, ...]
    planning_income_effective:   Decimal
    monthly_reserve_effective:   Decimal
    family_review_items:         tuple[str, ...]
    overrides_applied:           tuple[str, ...]   # override_ids applied


@dataclass(frozen=True)
class ReconciliationRecord:
    """
    Per-value reconciliation result.
    Any non-zero difference → status = "CONFLICT".
    Conflicts are never auto-resolved by adjusting classification.
    All monetary fields are Decimal for exact agora-level comparison.
    """
    field:              str
    reviewed_value:     Decimal           # Family Review ground truth
    derived_value:      Decimal           # from effective result
    raw_derived_value:  Decimal           # from raw classifier (before overrides)
    difference:         Decimal           # derived_value - reviewed_value; 0.00 = MATCH
    status:             str               # "MATCH" | "CONFLICT"
    conflict_report:    Optional[dict]    # full breakdown when CONFLICT


@dataclass(frozen=True)
class ReconciliationReport:
    planning_income:  ReconciliationRecord
    monthly_reserve:  ReconciliationRecord


@dataclass(frozen=True)
class ClassificationReport:
    """
    Full output of a V4 classification run.
    Preserves both raw and effective views for auditability.
    Phase B produces this; Phase C reviews it.

    override_audit: per-pattern audit trail for every pattern where
    FAMILY_REVIEW overrides were applied. Each entry shows:
      - classifier_result: what the pure classifier derived
      - changed_fields: field-by-field before/after per override
      - effective_result: final state after all overrides

    This preserves three-layer auditability (classifier → override → effective)
    without hiding or overwriting the original classifier evidence.
    """
    classifier_version: str
    run_id:             str
    analysis_db:        str
    run_at:             str
    raw:                RawClassifierOutput
    effective:          EffectiveFinancialResult
    reconciliation:     ReconciliationReport
    override_audit:     tuple[dict, ...]    # per-pattern audit records


# ═══════════════════════════════════════════════════════════════════════════
# DOMAIN FUNCTIONS
# ═══════════════════════════════════════════════════════════════════════════

def is_reserve_eligible(
    recurrence: RecurrenceStatus,
    commitment: CommitmentStatus,
    lifecycle: LifecycleStatus,
    planning_amount: Optional[Decimal],
) -> bool:
    """
    Reserve eligibility is derived from financial state only.
    purpose_type (SAVINGS_INVESTMENT, FINANCIAL_FEE, etc.) is NOT a factor.

    A committed, recurring, active obligation with a known amount is
    reserve-eligible regardless of purpose.

    Examples:
      Training fund (קרן השתלמות):
        purpose=SAVINGS_INVESTMENT, recurrence=RECURRING,
        commitment=COMMITTED, lifecycle=ACTIVE, amount=Decimal("137.59") → ELIGIBLE
      Discount bank card fee:
        purpose=FINANCIAL_FEE, recurrence=RECURRING,
        commitment=COMMITTED, lifecycle=ACTIVE, amount=Decimal("39.60") → ELIGIBLE
      Round-up savings:
        purpose=SAVINGS_INVESTMENT, commitment=NON_COMMITTED → NOT ELIGIBLE
    """
    return (
        recurrence == RecurrenceStatus.RECURRING
        and commitment == CommitmentStatus.COMMITTED
        and lifecycle == LifecycleStatus.ACTIVE
        and planning_amount is not None
        and planning_amount > Decimal("0")
    )


def derive_budget_class(
    recurrence: RecurrenceStatus,
    commitment: CommitmentStatus,
    amount_behavior: AmountBehavior,
) -> BudgetClass:
    """
    V3-validated derivation logic, carried forward verbatim.
    POSSIBLE_RECURRING → always UNCERTAIN (never enters reserve).
    UNKNOWN recurrence → always UNCERTAIN.
    """
    if recurrence in (RecurrenceStatus.POSSIBLE_RECURRING, RecurrenceStatus.UNKNOWN):
        return BudgetClass.UNCERTAIN
    if recurrence == RecurrenceStatus.NON_RECURRING:
        return BudgetClass.NON_RECURRING_EXPENSE
    # recurrence == RECURRING
    if commitment == CommitmentStatus.UNCERTAIN:
        return BudgetClass.UNCERTAIN
    if commitment == CommitmentStatus.NON_COMMITTED:
        return BudgetClass.RECURRING_NON_COMMITMENT
    # COMMITTED
    if amount_behavior in (AmountBehavior.VERY_STABLE, AmountBehavior.STABLE):
        return BudgetClass.FIXED_AMOUNT_RECURRING
    return BudgetClass.VARIABLE_AMOUNT_RECURRING


def make_income_stream_key(person: str, source: str, norm_desc: str) -> str:
    """
    Deterministic stream key for grouping income rows.
    Grouping by (person, source, norm_desc) — NOT description alone —
    prevents two salaries with the same description merging into one stream.
    """
    raw = f"{person.strip().lower()}|{source.strip().lower()}|{norm_desc.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def derive_cashflow_role(
    recurrence: RecurrenceStatus,
    commitment: CommitmentStatus,
    lifecycle: LifecycleStatus,
    planning_amount: Optional[Decimal],
    purpose: PurposeType,
) -> CashflowRole:
    """
    Derives the reporting CashflowRole from financial state.
    Reserve eligibility check takes precedence over purpose-based grouping:
    a SAVINGS_INVESTMENT or FINANCIAL_FEE pattern that is RECURRING+COMMITTED+ACTIVE
    is labelled RESERVE, not SAVINGS or FEE.
    """
    if is_reserve_eligible(recurrence, commitment, lifecycle, planning_amount):
        return CashflowRole.RESERVE
    if recurrence == RecurrenceStatus.RECURRING and commitment == CommitmentStatus.NON_COMMITTED:
        if purpose == PurposeType.SAVINGS_INVESTMENT:
            return CashflowRole.SAVINGS
        if purpose == PurposeType.FINANCIAL_FEE:
            return CashflowRole.FEE
        return CashflowRole.FLEXIBLE
    return CashflowRole.FLEXIBLE


def make_reconciliation_record(
    field: str,
    reviewed_value: Decimal,
    derived_value: Decimal,
    raw_derived_value: Decimal,
) -> ReconciliationRecord:
    """
    Build a ReconciliationRecord with exact Decimal arithmetic.
    difference = quantize_ils(derived_value - reviewed_value).
    status = "MATCH" iff difference == Decimal("0.00").
    """
    diff = quantize_ils(derived_value - reviewed_value)
    status = "MATCH" if diff == Decimal("0.00") else "CONFLICT"
    return ReconciliationRecord(
        field=field,
        reviewed_value=reviewed_value,
        derived_value=derived_value,
        raw_derived_value=raw_derived_value,
        difference=diff,
        status=status,
        conflict_report=None if status == "MATCH" else {
            "field": field,
            "reviewed_value": str(reviewed_value),
            "derived_value": str(derived_value),
            "difference": str(diff),
        },
    )


# ═══════════════════════════════════════════════════════════════════════════
# IMMUTABLE STATE GRAPH — INVARIANT VALIDATOR
# ═══════════════════════════════════════════════════════════════════════════

def validate_state_graph(states: list[PatternStateContract]) -> list[str]:
    """
    Validates the immutable append-only state graph for a single pattern.
    All states must share the same pattern_id.

    Returns a list of violation strings. Empty list = valid graph.

    Invariants enforced:
    1. All states belong to the same pattern_id.
    2. Exactly one current state (leaf — not superseded by any successor).
    3. Each state is superseded by at most one successor (no branching).
    4. successor.valid_from > predecessor.valid_from.
    5. No cycles in the supersedes chain.
    6. No self-supersede (state_id != supersedes_state_id).
    """
    violations: list[str] = []
    if not states:
        return violations

    by_id = {s.state_id: s for s in states}

    # 1. All same pattern_id
    pattern_ids = {s.pattern_id for s in states}
    if len(pattern_ids) > 1:
        violations.append(f"States span multiple pattern_ids: {pattern_ids}")

    # 6. No self-supersede
    for s in states:
        if s.supersedes_state_id == s.state_id:
            violations.append(f"State {s.state_id} supersedes itself")

    # 3. Each state superseded by at most one successor (no branching)
    superseded_counts: dict[str, int] = {}
    for s in states:
        if s.supersedes_state_id:
            superseded_counts[s.supersedes_state_id] = (
                superseded_counts.get(s.supersedes_state_id, 0) + 1
            )
    for sid, count in superseded_counts.items():
        if count > 1:
            violations.append(
                f"State {sid} is superseded by {count} successors (branching not allowed)"
            )

    # 2. Exactly one current state (leaf = not in superseded_counts)
    superseded_set = set(superseded_counts.keys())
    leaves = [s for s in states if s.state_id not in superseded_set]
    if len(leaves) != 1:
        leaf_ids = [s.state_id for s in leaves]
        violations.append(
            f"Expected exactly 1 current state; found {len(leaves)}: {leaf_ids}"
        )

    # 4. successor.valid_from > predecessor.valid_from
    for s in states:
        if s.supersedes_state_id and s.supersedes_state_id in by_id:
            pred = by_id[s.supersedes_state_id]
            if s.valid_from <= pred.valid_from:
                violations.append(
                    f"State {s.state_id} valid_from={s.valid_from} is not after "
                    f"predecessor {pred.state_id} valid_from={pred.valid_from}"
                )

    # 5. No cycles (DFS through supersedes chain)
    def has_cycle(start_id: str) -> bool:
        visited: set[str] = set()
        current = start_id
        while current:
            if current in visited:
                return True
            visited.add(current)
            node = by_id.get(current)
            current = node.supersedes_state_id if node else None
        return False

    for s in states:
        if has_cycle(s.state_id):
            violations.append(f"Cycle detected in supersedes chain at state {s.state_id}")
            break  # one report is enough

    return violations


# ═══════════════════════════════════════════════════════════════════════════
# OVERRIDE RESOLUTION PROTOCOL
# ═══════════════════════════════════════════════════════════════════════════

@runtime_checkable
class OverrideResolver(Protocol):
    """
    Field-level override resolution.

    Reading rule for any field F on a pattern:
      1. Check cashflow_override for the latest override where override_field = F.
         If found, that value wins.
      2. Otherwise, use the value from the raw PatternResult (classifier-derived).

    decision_source on a PatternStateContract describes the origin of THAT
    STATE ROW as a whole. It does not claim all fields share the same provenance.
    A pattern may simultaneously have:
      recurrence_status  → classifier
      commitment_status  → family_review
      lifecycle_status   → manual_override
      planning_amount    → transaction_evidence
    """

    def resolve_field(
        self,
        pattern_id: str,
        field_name: str,
        raw_value: object,
    ) -> object:
        """
        Returns the effective value for field_name, applying any override.
        raw_value is the classifier-derived value; returned unchanged if no
        override exists for this field.
        """
        ...

    def get_applied_overrides(self, pattern_id: str) -> list[str]:
        """Returns override_ids applied to this pattern, for audit."""
        ...


# ═══════════════════════════════════════════════════════════════════════════
# CLASSIFIER PROTOCOL (Phase B will implement this)
# ═══════════════════════════════════════════════════════════════════════════

@runtime_checkable
class ExpenseClassifierProtocol(Protocol):
    """
    Phase B must implement this protocol.
    classify_expenses() is side-effect free: no DB writes.
    """

    def classify_expenses(
        self,
        rows: list,
        cat_map: dict,
    ) -> list[PatternResult]:
        """
        Groups rows by normalized description_key.
        For each group, calls classify_group() → list[PatternResult].
        Returns flat list of all PatternResults.
        Multiple PatternResults per description_key = parallel streams.
        NO DB writes.
        """
        ...

    def classify_group(
        self,
        rows: list,
        cat_map: dict,
    ) -> list[PatternResult]:
        """
        Detects parallel streams via bi-modal analysis.
        Returns 1–N PatternResults. Never merges N streams into 1 result.
        """
        ...

    def classify_income(
        self,
        rows: list,
    ) -> list[IncomeStreamResult]:
        """
        Groups by (person, source, norm_desc) — NOT description alone.
        Returns one IncomeStreamResult per distinct stream.
        NO DB writes.
        """
        ...


# ═══════════════════════════════════════════════════════════════════════════
# HOUSEHOLD OWNERSHIP GUARD
# ═══════════════════════════════════════════════════════════════════════════

class HouseholdResolutionError(ValueError):
    """
    Raised when a Phase D write is attempted without a resolved household_id.
    household_id=None is permitted only during bootstrap/migration.
    Production writes must refuse to create pattern rows for an unknown household.
    """


def assert_household_resolved(household_id: Optional[str], context: str = "") -> None:
    """
    Call before any Phase D write of V4 financial pattern data.
    Raises HouseholdResolutionError if household_id is None.
    """
    if household_id is None:
        msg = "household_id must be resolved before writing V4 financial data"
        if context:
            msg += f" ({context})"
        raise HouseholdResolutionError(msg)
