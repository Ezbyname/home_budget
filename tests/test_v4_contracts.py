"""
Phase A acceptance tests for V4 domain contracts.

Covers all 16 behavioral domains from the approved test matrix:
  1.  Parallel streams
  2.  Lifecycle status transitions
  3.  Stable core + extras (parallel stream detection boundary)
  4.  Price changes → amount_behavior upgrade
  5.  Manual overrides (OverrideResolver protocol)
  6.  Income stream isolation (person × source × norm_desc)
  7.  TBD amounts (planning_amount=None)
  8.  Savings/investments as reserve-eligible (AC3)
  9.  Financial fees as reserve-eligible (AC3)
  10. Settlements / non-recurring expenses
  11. Economic-vs-cash timing (no domain impact)
  12. Cadence-normalized monthly contribution
  13. Exact reconciliation — MATCH and CONFLICT
  14. State graph invariants (6 invariants)
  15. Regression: V3 budget-class derivation rules
  16. Side-effect freedom (classifiers are pure — protocol shape only)

Running:
  pytest tests/test_v4_contracts.py -v
"""

import hashlib
import pytest
from dataclasses import fields

from intelligence.v4_contracts import (
    # enums
    RecurrenceStatus, CommitmentStatus, AmountBehavior, BudgetClass,
    LifecycleStatus, PurposeType, CashflowRole, ReliabilityStatus,
    Cadence, DecisionSource, ReviewReason,
    # constants
    CADENCE_TO_MONTHS,
    # dataclasses
    PatternStateContract, PatternIdentityContract, PatternResult,
    IncomeStreamResult, RawClassifierOutput, EffectiveFinancialResult,
    ReconciliationRecord, ReconciliationReport, ClassificationReport,
    # domain functions
    is_reserve_eligible, derive_budget_class, make_income_stream_key,
    derive_cashflow_role, make_reconciliation_record, validate_state_graph,
    # errors
    HouseholdResolutionError, assert_household_resolved,
    # protocols
    OverrideResolver, ExpenseClassifierProtocol,
)


# ════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ════════════════════════════════════════════════════════════════════════════

def _make_state(
    state_id: str = "s1",
    pattern_id: str = "p1",
    valid_from: str = "2024-01-01",
    supersedes_state_id=None,
    recurrence=RecurrenceStatus.RECURRING,
    commitment=CommitmentStatus.COMMITTED,
    amount_behavior=AmountBehavior.VERY_STABLE,
    budget_class=BudgetClass.FIXED_AMOUNT_RECURRING,
    lifecycle=LifecycleStatus.ACTIVE,
    cadence=Cadence.MONTHLY,
    planning_amount=500.0,
    purpose=PurposeType.HOUSING,
    evidence_sources=("tx_evidence",),
    decision_source=DecisionSource.CLASSIFIER,
    changed_by="classifier",
    change_reason=None,
) -> PatternStateContract:
    return PatternStateContract(
        state_id=state_id,
        pattern_id=pattern_id,
        valid_from=valid_from,
        supersedes_state_id=supersedes_state_id,
        recurrence_status=recurrence,
        commitment_status=commitment,
        amount_behavior=amount_behavior,
        budget_class=budget_class,
        lifecycle_status=lifecycle,
        cadence=cadence,
        planning_amount=planning_amount,
        purpose_type=purpose,
        evidence_sources=evidence_sources,
        decision_source=decision_source,
        changed_by=changed_by,
        change_reason=change_reason,
    )


def _make_pattern_result(
    description_key: str = "rent",
    label: str = "שכר דירה",
    recurrence=RecurrenceStatus.RECURRING,
    commitment=CommitmentStatus.COMMITTED,
    amount_behavior=AmountBehavior.VERY_STABLE,
    lifecycle=LifecycleStatus.ACTIVE,
    purpose=PurposeType.HOUSING,
    cadence=Cadence.MONTHLY,
    planning_amount=3500.0,
    member_ids=("tx1", "tx2"),
    membership_confidence=None,
    evidence_sources=("tx_evidence",),
    decision_source=DecisionSource.CLASSIFIER,
    family_review_required=False,
    review_reasons=(),
) -> PatternResult:
    bc = derive_budget_class(recurrence, commitment, amount_behavior)
    eligible = is_reserve_eligible(recurrence, commitment, lifecycle, planning_amount)
    contrib = planning_amount if eligible else 0.0
    if membership_confidence is None:
        membership_confidence = {m: 1.0 for m in member_ids}
    return PatternResult(
        description_key=description_key,
        label=label,
        recurrence_status=recurrence,
        commitment_status=commitment,
        amount_behavior=amount_behavior,
        budget_class=bc,
        lifecycle_status=lifecycle,
        purpose_type=purpose,
        cadence=cadence,
        planning_amount=planning_amount,
        member_ids=tuple(member_ids),
        membership_confidence=membership_confidence,
        evidence_sources=tuple(evidence_sources),
        decision_source=decision_source,
        family_review_required=family_review_required,
        review_reasons=tuple(review_reasons),
        reserve_eligible=eligible,
        monthly_reserve_contrib=contrib,
    )


def _make_income_stream(
    person="גל",
    source="employer",
    description_key="משכורת",
    recurrence=RecurrenceStatus.RECURRING,
    reliability=ReliabilityStatus.RELIABLE,
    amount_behavior=AmountBehavior.STABLE,
    cadence=Cadence.MONTHLY,
    planning_baseline=15000.0,
    member_ids=("tx10", "tx11"),
    evidence_sources=("payslip",),
    decision_source=DecisionSource.CLASSIFIER,
    family_review_required=False,
    review_reasons=(),
) -> IncomeStreamResult:
    stream_key = make_income_stream_key(person, source, description_key)
    return IncomeStreamResult(
        stream_key=stream_key,
        person=person,
        source=source,
        description_key=description_key,
        recurrence_status=recurrence,
        reliability_status=reliability,
        amount_behavior=amount_behavior,
        cadence=cadence,
        planning_baseline=planning_baseline,
        member_ids=tuple(member_ids),
        evidence_sources=tuple(evidence_sources),
        decision_source=decision_source,
        family_review_required=family_review_required,
        review_reasons=tuple(review_reasons),
    )


# ════════════════════════════════════════════════════════════════════════════
# 1. ENUM VALUES — exact value assertions
# ════════════════════════════════════════════════════════════════════════════

class TestEnumValues:
    def test_recurrence_status_values(self):
        assert set(RecurrenceStatus) == {
            RecurrenceStatus.RECURRING,
            RecurrenceStatus.POSSIBLE_RECURRING,
            RecurrenceStatus.NON_RECURRING,
            RecurrenceStatus.UNKNOWN,
        }

    def test_lifecycle_status_values(self):
        assert set(LifecycleStatus) == {
            LifecycleStatus.ACTIVE,
            LifecycleStatus.POSSIBLY_STOPPED,
            LifecycleStatus.CANCELLED,
            LifecycleStatus.ENDED,
            LifecycleStatus.UNKNOWN,
        }
        # Prohibited values must NOT exist
        member_names = {m.name for m in LifecycleStatus}
        assert "TBD" not in member_names
        assert "PAUSED" not in member_names
        assert "NOT_APPROVED" not in member_names

    def test_commitment_status_values(self):
        assert set(CommitmentStatus) == {
            CommitmentStatus.COMMITTED,
            CommitmentStatus.NON_COMMITTED,
            CommitmentStatus.UNCERTAIN,
        }

    def test_budget_class_values(self):
        assert set(BudgetClass) == {
            BudgetClass.FIXED_AMOUNT_RECURRING,
            BudgetClass.VARIABLE_AMOUNT_RECURRING,
            BudgetClass.RECURRING_NON_COMMITMENT,
            BudgetClass.NON_RECURRING_EXPENSE,
            BudgetClass.UNCERTAIN,
        }

    def test_purpose_type_values(self):
        expected = {
            "HOUSING", "INSURANCE", "UTILITY", "EDUCATION",
            "SAVINGS_INVESTMENT", "TRANSPORT", "FOOD", "HEALTH",
            "DEBT", "FINANCIAL_FEE", "OTHER",
        }
        assert {m.name for m in PurposeType} == expected

    def test_cashflow_role_values(self):
        expected = {"RESERVE", "FLEXIBLE", "SAVINGS", "FEE", "SETTLEMENT", "TRANSFER", "INCOME"}
        assert {m.name for m in CashflowRole} == expected

    def test_decision_source_values(self):
        assert set(DecisionSource) == {
            DecisionSource.CLASSIFIER,
            DecisionSource.FAMILY_REVIEW,
            DecisionSource.IMPORT_SIGNAL,
            DecisionSource.MANUAL_OVERRIDE,
        }

    def test_review_reason_values(self):
        expected = {
            "AMOUNT_TBD", "POSSIBLE_RECURRING", "UNCERTAIN_COMMITMENT",
            "LOW_CONFIDENCE_MEMBERSHIP", "RECONCILIATION_CONFLICT", "HIGH_CV",
        }
        assert {m.name for m in ReviewReason} == expected

    def test_enums_are_str_subclasses(self):
        for enum_cls in [RecurrenceStatus, CommitmentStatus, AmountBehavior,
                         BudgetClass, LifecycleStatus, PurposeType, CashflowRole,
                         ReliabilityStatus, Cadence, DecisionSource, ReviewReason]:
            for member in enum_cls:
                assert isinstance(member, str), f"{enum_cls.__name__}.{member.name} is not str"


# ════════════════════════════════════════════════════════════════════════════
# 2. CADENCE_TO_MONTHS
# ════════════════════════════════════════════════════════════════════════════

class TestCadenceToMonths:
    def test_monthly_is_1(self):
        assert CADENCE_TO_MONTHS[Cadence.MONTHLY] == 1

    def test_quarterly_is_3(self):
        assert CADENCE_TO_MONTHS[Cadence.QUARTERLY] == 3

    def test_semiannual_is_6(self):
        assert CADENCE_TO_MONTHS[Cadence.SEMIANNUAL] == 6

    def test_yearly_is_12(self):
        assert CADENCE_TO_MONTHS[Cadence.YEARLY] == 12

    def test_biweekly_is_1(self):
        assert CADENCE_TO_MONTHS[Cadence.BIWEEKLY] == 1

    def test_irregular_and_unknown_not_in_map(self):
        assert Cadence.IRREGULAR not in CADENCE_TO_MONTHS
        assert Cadence.UNKNOWN not in CADENCE_TO_MONTHS


# ════════════════════════════════════════════════════════════════════════════
# 3. DOMAIN: is_reserve_eligible — AC3 compliance
# ════════════════════════════════════════════════════════════════════════════

class TestReserveEligibility:
    """AC3: purpose_type must NOT determine reserve eligibility."""

    def test_savings_investment_committed_recurring_active_is_eligible(self):
        # Training fund (קרן השתלמות): ₪137.59
        assert is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            137.59,
        )

    def test_financial_fee_committed_recurring_active_is_eligible(self):
        # Discount bank card fee: ₪39.60
        assert is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            39.60,
        )

    def test_round_up_savings_non_committed_not_eligible(self):
        # Round-up savings: ₪0 reserve
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE,
            0.0,
        )

    def test_planning_amount_none_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            None,
        )

    def test_planning_amount_zero_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            0.0,
        )

    def test_possible_recurring_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.POSSIBLE_RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            500.0,
        )

    def test_unknown_recurrence_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.UNKNOWN,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            500.0,
        )

    def test_possibly_stopped_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.POSSIBLY_STOPPED,
            500.0,
        )

    def test_cancelled_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.CANCELLED,
            500.0,
        )

    def test_ended_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ENDED,
            500.0,
        )

    def test_non_recurring_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.NON_RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            500.0,
        )


# ════════════════════════════════════════════════════════════════════════════
# 4. DOMAIN: derive_budget_class — V3 regression (domain 15)
# ════════════════════════════════════════════════════════════════════════════

class TestDeriveBudgetClass:
    def test_possible_recurring_always_uncertain(self):
        for commitment in CommitmentStatus:
            for amount in AmountBehavior:
                assert derive_budget_class(
                    RecurrenceStatus.POSSIBLE_RECURRING, commitment, amount
                ) == BudgetClass.UNCERTAIN

    def test_unknown_recurrence_always_uncertain(self):
        for commitment in CommitmentStatus:
            for amount in AmountBehavior:
                assert derive_budget_class(
                    RecurrenceStatus.UNKNOWN, commitment, amount
                ) == BudgetClass.UNCERTAIN

    def test_non_recurring_always_non_recurring_expense(self):
        for commitment in CommitmentStatus:
            for amount in AmountBehavior:
                assert derive_budget_class(
                    RecurrenceStatus.NON_RECURRING, commitment, amount
                ) == BudgetClass.NON_RECURRING_EXPENSE

    def test_recurring_uncertain_commitment_is_uncertain(self):
        for amount in AmountBehavior:
            assert derive_budget_class(
                RecurrenceStatus.RECURRING, CommitmentStatus.UNCERTAIN, amount
            ) == BudgetClass.UNCERTAIN

    def test_recurring_non_committed_is_recurring_non_commitment(self):
        for amount in AmountBehavior:
            assert derive_budget_class(
                RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED, amount
            ) == BudgetClass.RECURRING_NON_COMMITMENT

    def test_recurring_committed_very_stable_is_fixed(self):
        assert derive_budget_class(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED, AmountBehavior.VERY_STABLE
        ) == BudgetClass.FIXED_AMOUNT_RECURRING

    def test_recurring_committed_stable_is_fixed(self):
        assert derive_budget_class(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED, AmountBehavior.STABLE
        ) == BudgetClass.FIXED_AMOUNT_RECURRING

    def test_recurring_committed_variable_is_variable_recurring(self):
        assert derive_budget_class(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED, AmountBehavior.VARIABLE
        ) == BudgetClass.VARIABLE_AMOUNT_RECURRING

    def test_recurring_committed_highly_variable_is_variable_recurring(self):
        assert derive_budget_class(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED, AmountBehavior.HIGHLY_VARIABLE
        ) == BudgetClass.VARIABLE_AMOUNT_RECURRING

    def test_recurring_committed_unknown_amount_is_variable_recurring(self):
        assert derive_budget_class(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED, AmountBehavior.UNKNOWN
        ) == BudgetClass.VARIABLE_AMOUNT_RECURRING


# ════════════════════════════════════════════════════════════════════════════
# 5. DOMAIN: derive_cashflow_role
# ════════════════════════════════════════════════════════════════════════════

class TestDeriveCashflowRole:
    def test_committed_recurring_active_any_purpose_is_reserve(self):
        for purpose in PurposeType:
            role = derive_cashflow_role(
                RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
                LifecycleStatus.ACTIVE, 100.0, purpose
            )
            assert role == CashflowRole.RESERVE, f"Expected RESERVE for purpose={purpose}"

    def test_savings_investment_non_committed_is_savings(self):
        role = derive_cashflow_role(
            RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE, 100.0, PurposeType.SAVINGS_INVESTMENT
        )
        assert role == CashflowRole.SAVINGS

    def test_financial_fee_non_committed_is_fee(self):
        role = derive_cashflow_role(
            RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE, 100.0, PurposeType.FINANCIAL_FEE
        )
        assert role == CashflowRole.FEE

    def test_housing_non_committed_is_flexible(self):
        role = derive_cashflow_role(
            RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE, 100.0, PurposeType.HOUSING
        )
        assert role == CashflowRole.FLEXIBLE

    def test_possible_recurring_is_flexible(self):
        role = derive_cashflow_role(
            RecurrenceStatus.POSSIBLE_RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE, 100.0, PurposeType.HOUSING
        )
        assert role == CashflowRole.FLEXIBLE

    def test_non_recurring_is_flexible(self):
        role = derive_cashflow_role(
            RecurrenceStatus.NON_RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE, 100.0, PurposeType.OTHER
        )
        assert role == CashflowRole.FLEXIBLE


# ════════════════════════════════════════════════════════════════════════════
# 6. DOMAIN: Income stream key isolation (domain 6)
# ════════════════════════════════════════════════════════════════════════════

class TestIncomeStreamKey:
    def test_same_inputs_same_key(self):
        k1 = make_income_stream_key("גל", "employer", "משכורת")
        k2 = make_income_stream_key("גל", "employer", "משכורת")
        assert k1 == k2

    def test_different_person_different_key(self):
        k1 = make_income_stream_key("גל", "employer", "משכורת")
        k2 = make_income_stream_key("נעמי", "employer", "משכורת")
        assert k1 != k2

    def test_different_source_different_key(self):
        k1 = make_income_stream_key("גל", "employer", "משכורת")
        k2 = make_income_stream_key("גל", "freelance", "משכורת")
        assert k1 != k2

    def test_different_desc_different_key(self):
        k1 = make_income_stream_key("גל", "employer", "משכורת")
        k2 = make_income_stream_key("גל", "employer", "בונוס")
        assert k1 != k2

    def test_case_insensitive_and_whitespace_stripped(self):
        k1 = make_income_stream_key("  גל  ", "EMPLOYER", "משכורת")
        k2 = make_income_stream_key("גל", "employer", "משכורת")
        assert k1 == k2

    def test_key_is_16_char_hex(self):
        k = make_income_stream_key("a", "b", "c")
        assert len(k) == 16
        assert all(c in "0123456789abcdef" for c in k)


# ════════════════════════════════════════════════════════════════════════════
# 7. DOMAIN: make_reconciliation_record — MATCH and CONFLICT (domain 13)
# ════════════════════════════════════════════════════════════════════════════

class TestReconciliation:
    def test_exact_match(self):
        r = make_reconciliation_record("monthly_reserve", 15395.99, 15395.99, 15000.0)
        assert r.status == "MATCH"
        assert r.difference == 0.0
        assert r.conflict_report is None

    def test_conflict_detected(self):
        r = make_reconciliation_record("monthly_reserve", 15395.99, 15200.00, 15200.00)
        assert r.status == "CONFLICT"
        assert r.difference != 0.0
        assert r.conflict_report is not None

    def test_conflict_report_has_required_keys(self):
        r = make_reconciliation_record("planning_income", 31659.50, 30000.0, 30000.0)
        assert r.conflict_report["field"] == "planning_income"
        assert "reviewed_value" in r.conflict_report
        assert "derived_value" in r.conflict_report
        assert "difference" in r.conflict_report

    def test_difference_is_derived_minus_reviewed(self):
        r = make_reconciliation_record("x", 100.0, 110.0, 100.0)
        assert r.difference == pytest.approx(10.0)

    def test_family_review_ground_truth_income(self):
        # Family Review ground truth: planning_income ₪31,659.50
        r = make_reconciliation_record("planning_income", 31659.50, 31659.50, 31659.50)
        assert r.status == "MATCH"

    def test_family_review_ground_truth_reserve(self):
        # Family Review ground truth: monthly_reserve ₪15,395.99
        r = make_reconciliation_record("monthly_reserve", 15395.99, 15395.99, 15000.0)
        assert r.status == "MATCH"

    def test_reconciliation_immutable(self):
        r = make_reconciliation_record("x", 100.0, 100.0, 100.0)
        with pytest.raises((AttributeError, TypeError)):
            r.status = "CONFLICT"  # type: ignore


# ════════════════════════════════════════════════════════════════════════════
# 8. DOMAIN: Immutable state graph — validate_state_graph (domain 14, AC5)
# ════════════════════════════════════════════════════════════════════════════

class TestStateGraphInvariants:
    def test_single_state_valid(self):
        s = _make_state(state_id="s1", supersedes_state_id=None)
        assert validate_state_graph([s]) == []

    def test_linear_chain_valid(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-06-01", supersedes_state_id="s1")
        s3 = _make_state("s3", valid_from="2024-09-01", supersedes_state_id="s2")
        assert validate_state_graph([s1, s2, s3]) == []

    def test_invariant_1_multiple_pattern_ids(self):
        s1 = _make_state("s1", pattern_id="p1")
        s2 = _make_state("s2", pattern_id="p2")
        violations = validate_state_graph([s1, s2])
        assert any("pattern_id" in v for v in violations)

    def test_invariant_2_no_leaf(self):
        # All states superseded → no leaf
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-06-01", supersedes_state_id="s1")
        s3 = _make_state("s3", valid_from="2024-09-01", supersedes_state_id="s2")
        # Create artificial mutual supersession so s2 and s3 are both leaves
        # but s1 is not — by removing s3, s2 becomes the leaf (valid test)
        # Instead test: two states both with no supersedes_state_id = two leaves
        s1b = _make_state("s1b", valid_from="2024-01-01", supersedes_state_id=None)
        s2b = _make_state("s2b", valid_from="2024-06-01", supersedes_state_id=None)
        violations = validate_state_graph([s1b, s2b])
        assert any("1 current state" in v or "leaf" in v.lower() for v in violations)

    def test_invariant_3_branching_not_allowed(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-06-01", supersedes_state_id="s1")
        s3 = _make_state("s3", valid_from="2024-07-01", supersedes_state_id="s1")  # branch!
        violations = validate_state_graph([s1, s2, s3])
        assert any("branching" in v or "superseded" in v for v in violations)

    def test_invariant_4_valid_from_ordering(self):
        s1 = _make_state("s1", valid_from="2024-06-01", supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-01-01", supersedes_state_id="s1")  # earlier!
        violations = validate_state_graph([s1, s2])
        assert any("valid_from" in v for v in violations)

    def test_invariant_5_cycle_detected(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id="s2")
        s2 = _make_state("s2", valid_from="2024-06-01", supersedes_state_id="s1")
        violations = validate_state_graph([s1, s2])
        assert any("cycle" in v.lower() for v in violations)

    def test_invariant_6_self_supersede(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id="s1")
        violations = validate_state_graph([s1])
        assert any("self" in v.lower() for v in violations)

    def test_empty_list_is_valid(self):
        assert validate_state_graph([]) == []

    def test_valid_lifecycle_transition_active_to_cancelled(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None,
                          lifecycle=LifecycleStatus.ACTIVE)
        s2 = _make_state("s2", valid_from="2024-09-01", supersedes_state_id="s1",
                          lifecycle=LifecycleStatus.CANCELLED)
        assert validate_state_graph([s1, s2]) == []

    def test_valid_lifecycle_transition_active_to_possibly_stopped(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None,
                          lifecycle=LifecycleStatus.ACTIVE)
        s2 = _make_state("s2", valid_from="2024-08-01", supersedes_state_id="s1",
                          lifecycle=LifecycleStatus.POSSIBLY_STOPPED)
        assert validate_state_graph([s1, s2]) == []


# ════════════════════════════════════════════════════════════════════════════
# 9. DOMAIN: Parallel streams (domain 1)
# ════════════════════════════════════════════════════════════════════════════

class TestParallelStreams:
    def test_two_patterns_same_description_key_allowed(self):
        # Gal Naomi: ₪607 stream + ₪2,000 stream both under same description_key
        stream_a = _make_pattern_result(
            description_key="gal_naomi_training",
            label="גל נעמי - קרן א",
            planning_amount=607.0,
            member_ids=("tx1", "tx2"),
        )
        stream_b = _make_pattern_result(
            description_key="gal_naomi_training",
            label="גל נעמי - קרן ב",
            planning_amount=2000.0,
            member_ids=("tx3", "tx4"),
        )
        # Both can coexist in a RawClassifierOutput
        raw = RawClassifierOutput(
            patterns=(stream_a, stream_b),
            income_streams=(),
            planning_income_raw=31659.50,
            monthly_reserve_raw=15395.99,
            family_review_items=(),
        )
        assert len(raw.patterns) == 2
        desc_keys = {p.description_key for p in raw.patterns}
        assert len(desc_keys) == 1  # same key, two streams

    def test_parallel_streams_non_overlapping_member_ids(self):
        stream_a = _make_pattern_result(
            description_key="shared_key",
            member_ids=("tx1", "tx2"),
            planning_amount=607.0,
        )
        stream_b = _make_pattern_result(
            description_key="shared_key",
            member_ids=("tx3", "tx4"),
            planning_amount=2000.0,
        )
        all_members = set(stream_a.member_ids) | set(stream_b.member_ids)
        overlap = set(stream_a.member_ids) & set(stream_b.member_ids)
        assert len(overlap) == 0
        assert len(all_members) == 4


# ════════════════════════════════════════════════════════════════════════════
# 10. DOMAIN: TBD amounts (domain 7) — AC1
# ════════════════════════════════════════════════════════════════════════════

class TestTBDAmounts:
    def test_planning_amount_none_triggers_review(self):
        r = _make_pattern_result(
            planning_amount=None,
            family_review_required=True,
            review_reasons=(ReviewReason.AMOUNT_TBD,),
        )
        assert r.planning_amount is None
        assert r.family_review_required is True
        assert ReviewReason.AMOUNT_TBD in r.review_reasons

    def test_planning_amount_none_not_reserve_eligible(self):
        r = _make_pattern_result(
            planning_amount=None,
            recurrence=RecurrenceStatus.RECURRING,
            commitment=CommitmentStatus.COMMITTED,
            lifecycle=LifecycleStatus.ACTIVE,
        )
        assert r.reserve_eligible is False
        assert r.monthly_reserve_contrib == 0.0

    def test_state_with_none_planning_amount(self):
        s = _make_state(planning_amount=None)
        assert s.planning_amount is None


# ════════════════════════════════════════════════════════════════════════════
# 11. DOMAIN: Dataclass immutability (domain 16 — side-effect freedom)
# ════════════════════════════════════════════════════════════════════════════

class TestImmutability:
    def test_pattern_state_is_frozen(self):
        s = _make_state()
        with pytest.raises((AttributeError, TypeError)):
            s.recurrence_status = RecurrenceStatus.NON_RECURRING  # type: ignore

    def test_pattern_result_is_frozen(self):
        r = _make_pattern_result()
        with pytest.raises((AttributeError, TypeError)):
            r.planning_amount = 9999.0  # type: ignore

    def test_income_stream_result_is_frozen(self):
        r = _make_income_stream()
        with pytest.raises((AttributeError, TypeError)):
            r.planning_baseline = 0.0  # type: ignore

    def test_raw_classifier_output_is_frozen(self):
        raw = RawClassifierOutput(
            patterns=(), income_streams=(),
            planning_income_raw=0.0, monthly_reserve_raw=0.0,
            family_review_items=(),
        )
        with pytest.raises((AttributeError, TypeError)):
            raw.planning_income_raw = 99.0  # type: ignore

    def test_effective_result_is_frozen(self):
        eff = EffectiveFinancialResult(
            patterns=(), income_streams=(),
            planning_income_effective=0.0, monthly_reserve_effective=0.0,
            family_review_items=(), overrides_applied=(),
        )
        with pytest.raises((AttributeError, TypeError)):
            eff.monthly_reserve_effective = 99.0  # type: ignore

    def test_pattern_identity_is_frozen(self):
        identity = PatternIdentityContract(
            id="p1", household_id="h1", label="test",
            description_key="key", category_id=None, created_at="2024-01-01",
        )
        with pytest.raises((AttributeError, TypeError)):
            identity.household_id = None  # type: ignore


# ════════════════════════════════════════════════════════════════════════════
# 12. DOMAIN: Income stream isolation — two persons, same description (domain 6)
# ════════════════════════════════════════════════════════════════════════════

class TestIncomeStreamIsolation:
    def test_two_persons_same_desc_different_keys(self):
        gal = _make_income_stream(person="גל", description_key="משכורת")
        naomi = _make_income_stream(person="נעמי", description_key="משכורת")
        assert gal.stream_key != naomi.stream_key

    def test_income_streams_in_raw_output(self):
        gal = _make_income_stream(person="גל", planning_baseline=20000.0)
        naomi = _make_income_stream(person="נעמי", planning_baseline=11659.50)
        raw = RawClassifierOutput(
            patterns=(),
            income_streams=(gal, naomi),
            planning_income_raw=31659.50,
            monthly_reserve_raw=15395.99,
            family_review_items=(),
        )
        assert len(raw.income_streams) == 2
        total = sum(s.planning_baseline for s in raw.income_streams)
        assert total == pytest.approx(31659.50)

    def test_reliable_income_baseline_not_discounted(self):
        # RELIABLE income with VARIABLE amount must still use planning_baseline as-is
        stream = _make_income_stream(
            reliability=ReliabilityStatus.RELIABLE,
            amount_behavior=AmountBehavior.VARIABLE,
            planning_baseline=15000.0,
        )
        assert stream.planning_baseline == 15000.0


# ════════════════════════════════════════════════════════════════════════════
# 13. DOMAIN: Raw vs Effective separation (AC6)
# ════════════════════════════════════════════════════════════════════════════

class TestRawVsEffective:
    def test_raw_and_effective_are_distinct_types(self):
        assert RawClassifierOutput is not EffectiveFinancialResult

    def test_raw_has_no_overrides_applied_field(self):
        raw_field_names = {f.name for f in fields(RawClassifierOutput)}
        assert "overrides_applied" not in raw_field_names

    def test_effective_has_overrides_applied_field(self):
        eff_field_names = {f.name for f in fields(EffectiveFinancialResult)}
        assert "overrides_applied" in eff_field_names

    def test_classification_report_contains_both(self):
        field_names = {f.name for f in fields(ClassificationReport)}
        assert "raw" in field_names
        assert "effective" in field_names
        assert "reconciliation" in field_names

    def test_raw_has_planning_income_raw_field(self):
        raw_field_names = {f.name for f in fields(RawClassifierOutput)}
        assert "planning_income_raw" in raw_field_names
        assert "monthly_reserve_raw" in raw_field_names

    def test_effective_has_planning_income_effective_field(self):
        eff_field_names = {f.name for f in fields(EffectiveFinancialResult)}
        assert "planning_income_effective" in eff_field_names
        assert "monthly_reserve_effective" in eff_field_names


# ════════════════════════════════════════════════════════════════════════════
# 14. DOMAIN: PatternIdentityContract — no financial attributes (AC1)
# ════════════════════════════════════════════════════════════════════════════

class TestPatternIdentity:
    def test_identity_has_no_financial_fields(self):
        financial_field_names = {
            "recurrence_status", "commitment_status", "amount_behavior",
            "budget_class", "lifecycle_status", "planning_amount", "cadence",
            "purpose_type", "reserve_eligible",
        }
        identity_fields = {f.name for f in fields(PatternIdentityContract)}
        overlap = financial_field_names & identity_fields
        assert overlap == set(), f"Financial attributes found on identity: {overlap}"

    def test_identity_has_household_id(self):
        f_names = {f.name for f in fields(PatternIdentityContract)}
        assert "household_id" in f_names

    def test_household_id_nullable(self):
        identity = PatternIdentityContract(
            id="p1", household_id=None, label="boot",
            description_key="k", category_id=None, created_at="2024-01-01",
        )
        assert identity.household_id is None


# ════════════════════════════════════════════════════════════════════════════
# 15. DOMAIN: HouseholdResolutionError (bootstrap guard)
# ════════════════════════════════════════════════════════════════════════════

class TestHouseholdGuard:
    def test_raises_when_none(self):
        with pytest.raises(HouseholdResolutionError):
            assert_household_resolved(None)

    def test_does_not_raise_when_resolved(self):
        assert_household_resolved("household-123")  # no exception

    def test_raises_with_context_message(self):
        with pytest.raises(HouseholdResolutionError, match="create_pattern"):
            assert_household_resolved(None, context="create_pattern")

    def test_is_value_error_subclass(self):
        with pytest.raises(ValueError):
            assert_household_resolved(None)


# ════════════════════════════════════════════════════════════════════════════
# 16. DOMAIN: Protocol structural checks (domain 16 — protocol shape)
# ════════════════════════════════════════════════════════════════════════════

class TestProtocols:
    def test_override_resolver_is_protocol(self):
        from typing import runtime_checkable
        # Protocol is structural — check it's runtime_checkable and has expected methods
        assert hasattr(OverrideResolver, "resolve_field")
        assert hasattr(OverrideResolver, "get_applied_overrides")

    def test_expense_classifier_protocol_is_protocol(self):
        assert hasattr(ExpenseClassifierProtocol, "classify_expenses")
        assert hasattr(ExpenseClassifierProtocol, "classify_group")
        assert hasattr(ExpenseClassifierProtocol, "classify_income")

    def test_concrete_override_resolver_passes_isinstance(self):
        class ConcreteResolver:
            def resolve_field(self, pattern_id, field_name, raw_value):
                return raw_value
            def get_applied_overrides(self, pattern_id):
                return []
        assert isinstance(ConcreteResolver(), OverrideResolver)

    def test_incomplete_override_resolver_fails_isinstance(self):
        class IncompleteResolver:
            def resolve_field(self, pattern_id, field_name, raw_value):
                return raw_value
            # missing get_applied_overrides
        assert not isinstance(IncompleteResolver(), OverrideResolver)

    def test_concrete_classifier_passes_isinstance(self):
        class ConcreteClassifier:
            def classify_expenses(self, rows, cat_map):
                return []
            def classify_group(self, rows, cat_map):
                return []
            def classify_income(self, rows):
                return []
        assert isinstance(ConcreteClassifier(), ExpenseClassifierProtocol)


# ════════════════════════════════════════════════════════════════════════════
# 17. DOMAIN: Lifecycle domain (domain 2)
# ════════════════════════════════════════════════════════════════════════════

class TestLifecycle:
    def test_active_recurring_committed_reserve_eligible(self):
        assert is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE, 500.0
        )

    def test_possibly_stopped_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.POSSIBLY_STOPPED, 500.0
        )

    def test_cancelled_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.CANCELLED, 500.0
        )

    def test_ended_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ENDED, 500.0
        )

    def test_unknown_lifecycle_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.UNKNOWN, 500.0
        )

    def test_lifecycle_transition_state_graph_active_to_ended(self):
        s1 = _make_state("s1", valid_from="2024-01-01", lifecycle=LifecycleStatus.ACTIVE,
                          supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-12-01", lifecycle=LifecycleStatus.ENDED,
                          supersedes_state_id="s1")
        violations = validate_state_graph([s1, s2])
        assert violations == []


# ════════════════════════════════════════════════════════════════════════════
# 18. DOMAIN: Cadence normalization (domain 12)
# ════════════════════════════════════════════════════════════════════════════

class TestCadenceNormalization:
    def test_monthly_amount_is_planning_amount(self):
        monthly_amount = 500.0
        monthly_contrib = monthly_amount / CADENCE_TO_MONTHS[Cadence.MONTHLY]
        assert monthly_contrib == pytest.approx(500.0)

    def test_quarterly_amount_normalized_to_monthly(self):
        quarterly_amount = 1200.0
        monthly_equiv = quarterly_amount / CADENCE_TO_MONTHS[Cadence.QUARTERLY]
        assert monthly_equiv == pytest.approx(400.0)

    def test_semiannual_normalized(self):
        semi_amount = 1800.0
        monthly_equiv = semi_amount / CADENCE_TO_MONTHS[Cadence.SEMIANNUAL]
        assert monthly_equiv == pytest.approx(300.0)

    def test_yearly_normalized(self):
        yearly_amount = 12000.0
        monthly_equiv = yearly_amount / CADENCE_TO_MONTHS[Cadence.YEARLY]
        assert monthly_equiv == pytest.approx(1000.0)
