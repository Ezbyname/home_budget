"""
Phase A acceptance tests for V4 domain contracts (correction pass).

Covers all approved behavioral domains:
  1.  Parallel streams
  2.  Lifecycle status transitions
  3.  Cadence normalization — EVERY_2_MONTHS, BIWEEKLY, etc.
  4.  Money representation — Decimal, not float
  5.  Manual overrides (OverrideResolver protocol)
  6.  Income stream isolation (person × source × norm_desc)
  7.  TBD amounts (planning_amount=None)
  8.  Savings/investments as reserve-eligible (AC3)
  9.  Financial fees as reserve-eligible (AC3) — named regression tests
  10. Settlements / non-recurring expenses
  11. Income semantic type (IncomeType enum)
  12. Exact Decimal reconciliation — MATCH and CONFLICT
  13. State graph invariants (6 invariants)
  14. Regression: V3 budget-class derivation rules
  15. Side-effect freedom (protocols are structural-only)
  16. Full enum taxonomy completeness

Running:
  pytest tests/test_v4_contracts.py -v
"""

import hashlib
from dataclasses import fields
from decimal import Decimal

import pytest

from intelligence.v4_contracts import (
    # money helpers
    TWO_PLACES, quantize_ils, decimal_from_db,
    # enums
    RecurrenceStatus, CommitmentStatus, AmountBehavior, BudgetClass,
    LifecycleStatus, PurposeType, CashflowRole, ReliabilityStatus,
    IncomeType, Cadence, DecisionSource, ReviewReason,
    # cadence table
    CADENCE_OCCURRENCES_PER_YEAR, monthly_equivalent,
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
    planning_amount=Decimal("500.00"),
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
    planning_amount=Decimal("3500.00"),
    member_ids=("tx1", "tx2"),
    membership_confidence=None,
    evidence_sources=("tx_evidence",),
    decision_source=DecisionSource.CLASSIFIER,
    family_review_required=False,
    review_reasons=(),
) -> PatternResult:
    bc = derive_budget_class(recurrence, commitment, amount_behavior)
    eligible = is_reserve_eligible(recurrence, commitment, lifecycle, planning_amount)
    contrib = planning_amount if (eligible and planning_amount is not None) else Decimal("0.00")
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
    income_type=IncomeType.SALARY,
    recurrence=RecurrenceStatus.RECURRING,
    reliability=ReliabilityStatus.RELIABLE,
    amount_behavior=AmountBehavior.STABLE,
    cadence=Cadence.MONTHLY,
    planning_baseline=Decimal("15000.00"),
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
        income_type=income_type,
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

    def test_purpose_type_includes_required_values(self):
        required = {
            "HOUSING", "UTILITY", "INSURANCE", "LOAN", "SUBSCRIPTION",
            "HEALTH", "CHILDREN", "EDUCATION", "TRANSPORT",
            "SAVINGS_INVESTMENT", "FINANCIAL_FEE", "TAX", "FOOD",
            "SHOPPING", "ENTERTAINMENT", "TRANSFER",
            "CREDIT_CARD_SETTLEMENT", "OTHER", "UNKNOWN",
        }
        actual = {m.name for m in PurposeType}
        missing = required - actual
        assert missing == set(), f"PurposeType missing: {missing}"

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

    def test_review_reason_includes_required_values(self):
        required = {
            "AMOUNT_TBD", "POSSIBLE_RECURRING", "UNCERTAIN_COMMITMENT",
            "LOW_CONFIDENCE_MEMBERSHIP", "RECONCILIATION_CONFLICT", "HIGH_CV",
            "UNKNOWN_MERCHANT", "POSSIBLY_STOPPED",
            "UNRECONCILED_SETTLEMENT", "NEW_EVIDENCE_FOUND",
        }
        actual = {m.name for m in ReviewReason}
        missing = required - actual
        assert missing == set(), f"ReviewReason missing: {missing}"

    def test_income_type_values(self):
        required = {
            "SALARY", "GOVERNMENT_BENEFIT", "FAMILY_TRANSFER",
            "BONUS", "OTHER", "UNKNOWN",
        }
        actual = {m.name for m in IncomeType}
        assert required == actual

    def test_cadence_includes_every_2_months(self):
        assert Cadence.EVERY_2_MONTHS in set(Cadence)
        assert Cadence.EVERY_2_MONTHS.value == "every_2_months"

    def test_enums_are_str_subclasses(self):
        for enum_cls in [RecurrenceStatus, CommitmentStatus, AmountBehavior,
                         BudgetClass, LifecycleStatus, PurposeType, CashflowRole,
                         ReliabilityStatus, IncomeType, Cadence,
                         DecisionSource, ReviewReason]:
            for member in enum_cls:
                assert isinstance(member, str), f"{enum_cls.__name__}.{member.name} is not str"


# ════════════════════════════════════════════════════════════════════════════
# 2. MONEY REPRESENTATION — Decimal contracts
# ════════════════════════════════════════════════════════════════════════════

class TestMoneyRepresentation:
    def test_two_places_constant(self):
        assert TWO_PLACES == Decimal("0.01")

    def test_quantize_ils_rounds_to_two_places(self):
        assert quantize_ils(Decimal("0.005")) == Decimal("0.01")  # ROUND_HALF_UP
        assert quantize_ils(Decimal("0.004")) == Decimal("0.00")
        assert quantize_ils(Decimal("100.125")) == Decimal("100.13")

    def test_decimal_from_db_avoids_float_error(self):
        # 0.1 + 0.2 has float error; via str it is exact
        db_value = 0.1 + 0.2  # float: 0.30000000000000004
        result = decimal_from_db(db_value)
        # The string representation of the float is used, not its exact binary value
        assert isinstance(result, Decimal)

    def test_decimal_from_db_integer(self):
        assert decimal_from_db(137) == Decimal("137")

    def test_decimal_from_db_string_float(self):
        assert decimal_from_db("39.60") == Decimal("39.60")

    def test_planning_amount_is_decimal_or_none(self):
        state = _make_state(planning_amount=Decimal("500.00"))
        assert isinstance(state.planning_amount, Decimal)

    def test_planning_amount_none_allowed(self):
        state = _make_state(planning_amount=None)
        assert state.planning_amount is None

    def test_monthly_reserve_contrib_is_decimal(self):
        r = _make_pattern_result(planning_amount=Decimal("500.00"))
        assert isinstance(r.monthly_reserve_contrib, Decimal)

    def test_planning_baseline_is_decimal(self):
        s = _make_income_stream(planning_baseline=Decimal("15000.00"))
        assert isinstance(s.planning_baseline, Decimal)

    def test_reconciliation_reviewed_value_is_decimal(self):
        r = make_reconciliation_record(
            "monthly_reserve",
            Decimal("15395.99"),
            Decimal("15395.99"),
            Decimal("15000.00"),
        )
        assert isinstance(r.reviewed_value, Decimal)
        assert isinstance(r.derived_value, Decimal)
        assert isinstance(r.difference, Decimal)

    def test_membership_confidence_remains_float(self):
        r = _make_pattern_result(
            member_ids=("tx1",),
            membership_confidence={"tx1": 0.95},
        )
        assert isinstance(r.membership_confidence["tx1"], float)


# ════════════════════════════════════════════════════════════════════════════
# 3. CADENCE — occurrences per year and monthly normalization
# ════════════════════════════════════════════════════════════════════════════

class TestCadenceOccurrences:
    def test_monthly_12_per_year(self):
        assert CADENCE_OCCURRENCES_PER_YEAR[Cadence.MONTHLY] == 12

    def test_biweekly_26_per_year(self):
        assert CADENCE_OCCURRENCES_PER_YEAR[Cadence.BIWEEKLY] == 26

    def test_every_2_months_6_per_year(self):
        assert CADENCE_OCCURRENCES_PER_YEAR[Cadence.EVERY_2_MONTHS] == 6

    def test_quarterly_4_per_year(self):
        assert CADENCE_OCCURRENCES_PER_YEAR[Cadence.QUARTERLY] == 4

    def test_semiannual_2_per_year(self):
        assert CADENCE_OCCURRENCES_PER_YEAR[Cadence.SEMIANNUAL] == 2

    def test_yearly_1_per_year(self):
        assert CADENCE_OCCURRENCES_PER_YEAR[Cadence.YEARLY] == 1

    def test_irregular_not_in_table(self):
        assert Cadence.IRREGULAR not in CADENCE_OCCURRENCES_PER_YEAR

    def test_unknown_not_in_table(self):
        assert Cadence.UNKNOWN not in CADENCE_OCCURRENCES_PER_YEAR


class TestMonthlyEquivalent:
    def test_monthly_payment_unchanged(self):
        assert monthly_equivalent(Decimal("1000.00"), Cadence.MONTHLY) == Decimal("1000.00")

    def test_bimonthly_arnona_886_gives_443(self):
        # ₪886 EVERY_2_MONTHS → 886 × 6 / 12 = ₪443.00
        result = monthly_equivalent(Decimal("886.00"), Cadence.EVERY_2_MONTHS)
        assert result == Decimal("443.00")

    def test_quarterly_1200_gives_400(self):
        assert monthly_equivalent(Decimal("1200.00"), Cadence.QUARTERLY) == Decimal("400.00")

    def test_semiannual_1800_gives_300(self):
        assert monthly_equivalent(Decimal("1800.00"), Cadence.SEMIANNUAL) == Decimal("300.00")

    def test_yearly_12000_gives_1000(self):
        assert monthly_equivalent(Decimal("12000.00"), Cadence.YEARLY) == Decimal("1000.00")

    def test_biweekly_correct_factor(self):
        # e.g. ₪500 every 2 weeks → 500 × 26 / 12 = ₪1083.33
        result = monthly_equivalent(Decimal("500.00"), Cadence.BIWEEKLY)
        assert result == Decimal("1083.33")

    def test_irregular_raises(self):
        with pytest.raises(ValueError, match="no automatic monthly normalization"):
            monthly_equivalent(Decimal("100.00"), Cadence.IRREGULAR)

    def test_unknown_cadence_raises(self):
        with pytest.raises(ValueError, match="no automatic monthly normalization"):
            monthly_equivalent(Decimal("100.00"), Cadence.UNKNOWN)

    def test_result_is_decimal(self):
        result = monthly_equivalent(Decimal("100.00"), Cadence.MONTHLY)
        assert isinstance(result, Decimal)

    def test_result_quantized_to_two_places(self):
        result = monthly_equivalent(Decimal("100.00"), Cadence.BIWEEKLY)
        assert result == result.quantize(TWO_PLACES)


# ════════════════════════════════════════════════════════════════════════════
# 4. DOMAIN: is_reserve_eligible — AC3 compliance + named regressions
# ════════════════════════════════════════════════════════════════════════════

class TestReserveEligibility:
    """AC3: purpose_type must NOT determine reserve eligibility."""

    # ── Named regression tests required by the review ──────────────────────

    def test_training_fund_savings_is_still_reserve_eligible(self):
        """
        קרן השתלמות: SAVINGS_INVESTMENT + RECURRING + COMMITTED + ACTIVE + ₪137.59
        Must remain reserve-eligible regardless of purpose classification.
        """
        assert is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            Decimal("137.59"),
        )

    def test_discount_card_fee_is_still_reserve_eligible(self):
        """
        עמלת כרטיס דיסקונט: FINANCIAL_FEE + RECURRING + COMMITTED + ACTIVE + ₪39.60
        Must remain reserve-eligible regardless of purpose classification.
        """
        assert is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            Decimal("39.60"),
        )

    # ── General eligibility matrix ──────────────────────────────────────────

    def test_round_up_savings_non_committed_not_eligible(self):
        # Round-up savings: NON_COMMITTED → ₪0 reserve
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE,
            Decimal("0"),
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
            Decimal("0"),
        )

    def test_possible_recurring_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.POSSIBLE_RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            Decimal("500.00"),
        )

    def test_unknown_recurrence_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.UNKNOWN,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            Decimal("500.00"),
        )

    def test_possibly_stopped_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.POSSIBLY_STOPPED,
            Decimal("500.00"),
        )

    def test_cancelled_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.CANCELLED,
            Decimal("500.00"),
        )

    def test_ended_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ENDED,
            Decimal("500.00"),
        )

    def test_non_recurring_not_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.NON_RECURRING,
            CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE,
            Decimal("500.00"),
        )


# ════════════════════════════════════════════════════════════════════════════
# 5. DOMAIN: derive_budget_class — V3 regression
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
# 6. DOMAIN: derive_cashflow_role
# ════════════════════════════════════════════════════════════════════════════

class TestDeriveCashflowRole:
    def test_committed_recurring_active_any_purpose_is_reserve(self):
        for purpose in PurposeType:
            role = derive_cashflow_role(
                RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
                LifecycleStatus.ACTIVE, Decimal("100.00"), purpose
            )
            assert role == CashflowRole.RESERVE, f"Expected RESERVE for purpose={purpose}"

    def test_savings_investment_non_committed_is_savings(self):
        role = derive_cashflow_role(
            RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE, Decimal("100.00"), PurposeType.SAVINGS_INVESTMENT
        )
        assert role == CashflowRole.SAVINGS

    def test_financial_fee_non_committed_is_fee(self):
        role = derive_cashflow_role(
            RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE, Decimal("100.00"), PurposeType.FINANCIAL_FEE
        )
        assert role == CashflowRole.FEE

    def test_housing_non_committed_is_flexible(self):
        role = derive_cashflow_role(
            RecurrenceStatus.RECURRING, CommitmentStatus.NON_COMMITTED,
            LifecycleStatus.ACTIVE, Decimal("100.00"), PurposeType.HOUSING
        )
        assert role == CashflowRole.FLEXIBLE

    def test_possible_recurring_is_flexible(self):
        role = derive_cashflow_role(
            RecurrenceStatus.POSSIBLE_RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE, Decimal("100.00"), PurposeType.HOUSING
        )
        assert role == CashflowRole.FLEXIBLE

    def test_non_recurring_is_flexible(self):
        role = derive_cashflow_role(
            RecurrenceStatus.NON_RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE, Decimal("100.00"), PurposeType.OTHER
        )
        assert role == CashflowRole.FLEXIBLE


# ════════════════════════════════════════════════════════════════════════════
# 7. DOMAIN: Income stream key isolation
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
# 8. DOMAIN: Decimal reconciliation — MATCH and CONFLICT
# ════════════════════════════════════════════════════════════════════════════

class TestReconciliation:
    def test_exact_match(self):
        r = make_reconciliation_record(
            "monthly_reserve",
            Decimal("15395.99"), Decimal("15395.99"), Decimal("15000.00"),
        )
        assert r.status == "MATCH"
        assert r.difference == Decimal("0.00")
        assert r.conflict_report is None

    def test_conflict_detected(self):
        r = make_reconciliation_record(
            "monthly_reserve",
            Decimal("15395.99"), Decimal("15200.00"), Decimal("15200.00"),
        )
        assert r.status == "CONFLICT"
        assert r.difference != Decimal("0.00")
        assert r.conflict_report is not None

    def test_conflict_report_has_required_keys(self):
        r = make_reconciliation_record(
            "planning_income",
            Decimal("31659.50"), Decimal("30000.00"), Decimal("30000.00"),
        )
        assert r.conflict_report["field"] == "planning_income"
        assert "reviewed_value" in r.conflict_report
        assert "derived_value" in r.conflict_report
        assert "difference" in r.conflict_report

    def test_difference_is_derived_minus_reviewed(self):
        r = make_reconciliation_record(
            "x", Decimal("100.00"), Decimal("110.00"), Decimal("100.00"),
        )
        assert r.difference == Decimal("10.00")

    def test_family_review_ground_truth_income_match(self):
        # Family Review ground truth: planning_income ₪31,659.50
        r = make_reconciliation_record(
            "planning_income",
            Decimal("31659.50"), Decimal("31659.50"), Decimal("31659.50"),
        )
        assert r.status == "MATCH"

    def test_family_review_ground_truth_reserve_match(self):
        # Family Review ground truth: monthly_reserve ₪15,395.99
        r = make_reconciliation_record(
            "monthly_reserve",
            Decimal("15395.99"), Decimal("15395.99"), Decimal("15000.00"),
        )
        assert r.status == "MATCH"

    def test_no_float_tolerance_in_reconciliation(self):
        # Difference of 1 agora is a CONFLICT, not forgiven
        r = make_reconciliation_record(
            "monthly_reserve",
            Decimal("15395.99"), Decimal("15396.00"), Decimal("15396.00"),
        )
        assert r.status == "CONFLICT"
        assert r.difference == Decimal("0.01")

    def test_reconciliation_immutable(self):
        r = make_reconciliation_record(
            "x", Decimal("100.00"), Decimal("100.00"), Decimal("100.00"),
        )
        with pytest.raises((AttributeError, TypeError)):
            r.status = "CONFLICT"  # type: ignore

    def test_conflict_report_values_are_strings(self):
        # Decimal values in conflict_report are stored as str for safe serialization
        r = make_reconciliation_record(
            "x", Decimal("100.00"), Decimal("110.00"), Decimal("100.00"),
        )
        assert isinstance(r.conflict_report["reviewed_value"], str)
        assert isinstance(r.conflict_report["derived_value"], str)


# ════════════════════════════════════════════════════════════════════════════
# 9. DOMAIN: Immutable state graph — validate_state_graph
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

    def test_invariant_2_two_leaves(self):
        s1b = _make_state("s1b", valid_from="2024-01-01", supersedes_state_id=None)
        s2b = _make_state("s2b", valid_from="2024-06-01", supersedes_state_id=None)
        violations = validate_state_graph([s1b, s2b])
        assert any("1 current state" in v or "leaf" in v.lower() for v in violations)

    def test_invariant_3_branching_not_allowed(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-06-01", supersedes_state_id="s1")
        s3 = _make_state("s3", valid_from="2024-07-01", supersedes_state_id="s1")
        violations = validate_state_graph([s1, s2, s3])
        assert any("branching" in v or "superseded" in v for v in violations)

    def test_invariant_4_valid_from_ordering(self):
        s1 = _make_state("s1", valid_from="2024-06-01", supersedes_state_id=None)
        s2 = _make_state("s2", valid_from="2024-01-01", supersedes_state_id="s1")
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

    def test_valid_lifecycle_transition_active_to_ended(self):
        s1 = _make_state("s1", valid_from="2024-01-01", supersedes_state_id=None,
                          lifecycle=LifecycleStatus.ACTIVE)
        s2 = _make_state("s2", valid_from="2024-12-01", supersedes_state_id="s1",
                          lifecycle=LifecycleStatus.ENDED)
        assert validate_state_graph([s1, s2]) == []


# ════════════════════════════════════════════════════════════════════════════
# 10. DOMAIN: Parallel streams
# ════════════════════════════════════════════════════════════════════════════

class TestParallelStreams:
    def test_two_patterns_same_description_key_allowed(self):
        stream_a = _make_pattern_result(
            description_key="gal_naomi_training",
            label="גל נעמי - קרן א",
            planning_amount=Decimal("607.00"),
            member_ids=("tx1", "tx2"),
        )
        stream_b = _make_pattern_result(
            description_key="gal_naomi_training",
            label="גל נעמי - קרן ב",
            planning_amount=Decimal("2000.00"),
            member_ids=("tx3", "tx4"),
        )
        raw = RawClassifierOutput(
            patterns=(stream_a, stream_b),
            income_streams=(),
            planning_income_raw=Decimal("31659.50"),
            monthly_reserve_raw=Decimal("15395.99"),
            family_review_items=(),
        )
        assert len(raw.patterns) == 2
        desc_keys = {p.description_key for p in raw.patterns}
        assert len(desc_keys) == 1

    def test_parallel_streams_non_overlapping_member_ids(self):
        stream_a = _make_pattern_result(
            description_key="shared_key",
            member_ids=("tx1", "tx2"),
            planning_amount=Decimal("607.00"),
        )
        stream_b = _make_pattern_result(
            description_key="shared_key",
            member_ids=("tx3", "tx4"),
            planning_amount=Decimal("2000.00"),
        )
        overlap = set(stream_a.member_ids) & set(stream_b.member_ids)
        assert len(overlap) == 0


# ════════════════════════════════════════════════════════════════════════════
# 11. DOMAIN: TBD amounts
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
        assert r.monthly_reserve_contrib == Decimal("0.00")

    def test_state_with_none_planning_amount(self):
        s = _make_state(planning_amount=None)
        assert s.planning_amount is None


# ════════════════════════════════════════════════════════════════════════════
# 12. DOMAIN: Dataclass immutability (side-effect freedom)
# ════════════════════════════════════════════════════════════════════════════

class TestImmutability:
    def test_pattern_state_is_frozen(self):
        s = _make_state()
        with pytest.raises((AttributeError, TypeError)):
            s.recurrence_status = RecurrenceStatus.NON_RECURRING  # type: ignore

    def test_pattern_result_is_frozen(self):
        r = _make_pattern_result()
        with pytest.raises((AttributeError, TypeError)):
            r.planning_amount = Decimal("9999.00")  # type: ignore

    def test_income_stream_result_is_frozen(self):
        r = _make_income_stream()
        with pytest.raises((AttributeError, TypeError)):
            r.planning_baseline = Decimal("0.00")  # type: ignore

    def test_raw_classifier_output_is_frozen(self):
        raw = RawClassifierOutput(
            patterns=(), income_streams=(),
            planning_income_raw=Decimal("0.00"), monthly_reserve_raw=Decimal("0.00"),
            family_review_items=(),
        )
        with pytest.raises((AttributeError, TypeError)):
            raw.planning_income_raw = Decimal("99.00")  # type: ignore

    def test_effective_result_is_frozen(self):
        eff = EffectiveFinancialResult(
            patterns=(), income_streams=(),
            planning_income_effective=Decimal("0.00"),
            monthly_reserve_effective=Decimal("0.00"),
            family_review_items=(), overrides_applied=(),
        )
        with pytest.raises((AttributeError, TypeError)):
            eff.monthly_reserve_effective = Decimal("99.00")  # type: ignore

    def test_pattern_identity_is_frozen(self):
        identity = PatternIdentityContract(
            id="p1", household_id="h1", label="test",
            description_key="key", category_id=None, created_at="2024-01-01",
        )
        with pytest.raises((AttributeError, TypeError)):
            identity.household_id = None  # type: ignore


# ════════════════════════════════════════════════════════════════════════════
# 13. DOMAIN: Income stream isolation — two persons, same description
# ════════════════════════════════════════════════════════════════════════════

class TestIncomeStreamIsolation:
    def test_two_persons_same_desc_different_keys(self):
        gal = _make_income_stream(person="גל", description_key="משכורת")
        naomi = _make_income_stream(person="נעמי", description_key="משכורת")
        assert gal.stream_key != naomi.stream_key

    def test_income_streams_in_raw_output(self):
        gal = _make_income_stream(person="גל", planning_baseline=Decimal("20000.00"))
        naomi = _make_income_stream(person="נעמי", planning_baseline=Decimal("11659.50"))
        raw = RawClassifierOutput(
            patterns=(),
            income_streams=(gal, naomi),
            planning_income_raw=Decimal("31659.50"),
            monthly_reserve_raw=Decimal("15395.99"),
            family_review_items=(),
        )
        assert len(raw.income_streams) == 2
        total = sum(s.planning_baseline for s in raw.income_streams)
        assert total == Decimal("31659.50")

    def test_reliable_income_baseline_not_discounted(self):
        stream = _make_income_stream(
            reliability=ReliabilityStatus.RELIABLE,
            amount_behavior=AmountBehavior.VARIABLE,
            planning_baseline=Decimal("15000.00"),
        )
        assert stream.planning_baseline == Decimal("15000.00")


# ════════════════════════════════════════════════════════════════════════════
# 14. DOMAIN: IncomeType semantic type
# ════════════════════════════════════════════════════════════════════════════

class TestIncomeType:
    def test_salary_on_income_stream(self):
        s = _make_income_stream(income_type=IncomeType.SALARY)
        assert s.income_type == IncomeType.SALARY

    def test_government_benefit_on_income_stream(self):
        # קצבת ילדים
        s = _make_income_stream(
            person="משפחה",
            source="bitouach_leumi",
            description_key="קצבת ילדים",
            income_type=IncomeType.GOVERNMENT_BENEFIT,
            reliability=ReliabilityStatus.RELIABLE,
        )
        assert s.income_type == IncomeType.GOVERNMENT_BENEFIT

    def test_family_transfer_income_type(self):
        s = _make_income_stream(
            description_key="העברה מגל נעמי",
            income_type=IncomeType.FAMILY_TRANSFER,
        )
        assert s.income_type == IncomeType.FAMILY_TRANSFER

    def test_bonus_income_type(self):
        s = _make_income_stream(
            description_key="בונוס",
            income_type=IncomeType.BONUS,
        )
        assert s.income_type == IncomeType.BONUS

    def test_income_type_independent_from_reliability(self):
        # SALARY + RELIABLE + VARIABLE = husband's salary
        s = _make_income_stream(
            income_type=IncomeType.SALARY,
            reliability=ReliabilityStatus.RELIABLE,
            amount_behavior=AmountBehavior.VARIABLE,
        )
        assert s.income_type == IncomeType.SALARY
        assert s.reliability_status == ReliabilityStatus.RELIABLE
        assert s.amount_behavior == AmountBehavior.VARIABLE

    def test_income_stream_has_income_type_field(self):
        f_names = {f.name for f in fields(IncomeStreamResult)}
        assert "income_type" in f_names


# ════════════════════════════════════════════════════════════════════════════
# 15. DOMAIN: Raw vs Effective separation (AC6)
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
# 16. DOMAIN: PatternIdentityContract — no financial attributes (AC1)
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
# 17. DOMAIN: HouseholdResolutionError (bootstrap guard)
# ════════════════════════════════════════════════════════════════════════════

class TestHouseholdGuard:
    def test_raises_when_none(self):
        with pytest.raises(HouseholdResolutionError):
            assert_household_resolved(None)

    def test_does_not_raise_when_resolved(self):
        assert_household_resolved("household-123")

    def test_raises_with_context_message(self):
        with pytest.raises(HouseholdResolutionError, match="create_pattern"):
            assert_household_resolved(None, context="create_pattern")

    def test_is_value_error_subclass(self):
        with pytest.raises(ValueError):
            assert_household_resolved(None)


# ════════════════════════════════════════════════════════════════════════════
# 18. DOMAIN: Protocol structural checks
# ════════════════════════════════════════════════════════════════════════════

class TestProtocols:
    def test_override_resolver_has_expected_methods(self):
        assert hasattr(OverrideResolver, "resolve_field")
        assert hasattr(OverrideResolver, "get_applied_overrides")

    def test_expense_classifier_has_expected_methods(self):
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
# 19. DOMAIN: Lifecycle status (all transitions valid)
# ════════════════════════════════════════════════════════════════════════════

class TestLifecycle:
    def test_active_recurring_committed_reserve_eligible(self):
        assert is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ACTIVE, Decimal("500.00")
        )

    def test_possibly_stopped_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.POSSIBLY_STOPPED, Decimal("500.00")
        )

    def test_cancelled_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.CANCELLED, Decimal("500.00")
        )

    def test_ended_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.ENDED, Decimal("500.00")
        )

    def test_unknown_lifecycle_not_reserve_eligible(self):
        assert not is_reserve_eligible(
            RecurrenceStatus.RECURRING, CommitmentStatus.COMMITTED,
            LifecycleStatus.UNKNOWN, Decimal("500.00")
        )
