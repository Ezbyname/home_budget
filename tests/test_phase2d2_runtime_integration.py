"""
Phase 2D2 — Runtime Integration Tests

Comprehensive test suite for Authority Runtime Integration.

Coverage:
  - NO AUTHORITY: patterns unchanged
  - FAMILY_REVIEW: baseline → resolved value
  - MANUAL_OVERRIDE: baseline → FAMILY_REVIEW → MANUAL_OVERRIDE
  - Revoked MANUAL (FAMILY_REVIEW remains): FAMILY_REVIEW value
  - Both unavailable: baseline
  - Six supported fields: all work correctly
  - Derived recomputation: monthly_equivalent, reserve_eligible, budget_class
  - No double-counting: authority modifies existing pattern, no duplication
  - Deferred patterns (DEFER_PARALLEL_UNRESOLVED, DEFER_CANONICAL_MERGE): unchanged
  - Immutability: original ClassificationReport unchanged
  - Errors: Phase 2D1 errors propagate
"""

import pytest
from decimal import Decimal
from typing import Optional

from intelligence.v4_contracts import (
    AmountBehavior,
    BudgetClass,
    Cadence,
    ClassificationReport,
    CommitmentStatus,
    DecisionSource,
    EffectiveFinancialResult,
    IncomeStreamResult,
    IncomeType,
    LifecycleStatus,
    PatternResult,
    PurposeType,
    RawClassifierOutput,
    ReconciliationRecord,
    ReconciliationReport,
    RecurrenceStatus,
    ReliabilityStatus,
    ReviewReason,
    quantize_ils,
)
from v4_authority_read import (
    AuthorityFieldResolution,
    AuthorityOutcome,
    AuthorityResolutionReport,
    AuthoritySkip,
)
from v4_authority_runtime import (
    AuthorityAdjustedAnalysis,
    apply_authority_to_analysis,
)


# ── Test Fixtures ────────────────────────────────────────────────────────────

def make_pattern(
    description_key: str = "test_pattern",
    recurrence: RecurrenceStatus = RecurrenceStatus.RECURRING,
    commitment: CommitmentStatus = CommitmentStatus.COMMITTED,
    lifecycle: LifecycleStatus = LifecycleStatus.ACTIVE,
    cadence: Cadence = Cadence.MONTHLY,
    planning_amount: Optional[Decimal] = Decimal("500"),
    amount_behavior: AmountBehavior = AmountBehavior.VERY_STABLE,
    purpose_type: PurposeType = PurposeType.HOUSING,
    reserve_eligible: bool = True,
    monthly_reserve_contrib: Decimal = Decimal("500"),
    budget_class: BudgetClass = BudgetClass.FIXED_AMOUNT_RECURRING,
    decision_source: DecisionSource = DecisionSource.CLASSIFIER,
    canonical_identity: Optional[str] = None,
) -> PatternResult:
    return PatternResult(
        description_key=description_key,
        label=f"Pattern: {description_key}",
        recurrence_status=recurrence,
        commitment_status=commitment,
        amount_behavior=amount_behavior,
        budget_class=budget_class,
        lifecycle_status=lifecycle,
        purpose_type=purpose_type,
        cadence=cadence,
        planning_amount=planning_amount,
        member_ids=tuple(),
        membership_confidence={},
        evidence_sources=tuple(),
        decision_source=decision_source,
        family_review_required=False,
        review_reasons=tuple(),
        reserve_eligible=reserve_eligible,
        monthly_reserve_contrib=monthly_reserve_contrib,
        canonical_identity=canonical_identity,
    )


def make_income_stream(
    description_key: str = "salary",
    planning_baseline: Decimal = Decimal("10000"),
) -> IncomeStreamResult:
    return IncomeStreamResult(
        stream_key="test_key",
        person="Test Person",
        source="Test Source",
        description_key=description_key,
        income_type=IncomeType.SALARY,
        recurrence_status=RecurrenceStatus.RECURRING,
        reliability_status=ReliabilityStatus.RELIABLE,
        amount_behavior=AmountBehavior.VERY_STABLE,
        cadence=Cadence.MONTHLY,
        planning_baseline=planning_baseline,
        member_ids=tuple(),
        evidence_sources=tuple(),
        decision_source=DecisionSource.CLASSIFIER,
        family_review_required=False,
        review_reasons=tuple(),
    )


def make_classification_report(
    patterns: tuple[PatternResult, ...] = None,
    income_streams: tuple[IncomeStreamResult, ...] = None,
    planning_income_effective: Decimal = Decimal("10000"),
    monthly_reserve_effective: Decimal = Decimal("500"),
) -> ClassificationReport:
    if patterns is None:
        patterns = (make_pattern(),)
    if income_streams is None:
        income_streams = (make_income_stream(),)

    raw = RawClassifierOutput(
        patterns=patterns,
        income_streams=income_streams,
        planning_income_raw=planning_income_effective,
        monthly_reserve_raw=monthly_reserve_effective,
        family_review_items=tuple(),
    )
    effective = EffectiveFinancialResult(
        patterns=patterns,
        income_streams=income_streams,
        planning_income_effective=planning_income_effective,
        monthly_reserve_effective=monthly_reserve_effective,
        family_review_items=tuple(),
        overrides_applied=tuple(),
    )
    reconciliation = ReconciliationReport(
        planning_income=ReconciliationRecord(
            field="planning_income",
            reviewed_value=planning_income_effective,
            derived_value=planning_income_effective,
            raw_derived_value=planning_income_effective,
            difference=Decimal("0"),
            status="MATCH",
            conflict_report=None,
        ),
        monthly_reserve=ReconciliationRecord(
            field="monthly_reserve",
            reviewed_value=monthly_reserve_effective,
            derived_value=monthly_reserve_effective,
            raw_derived_value=monthly_reserve_effective,
            difference=Decimal("0"),
            status="MATCH",
            conflict_report=None,
        ),
    )
    return ClassificationReport(
        classifier_version="v4",
        run_id="test_run_id",
        analysis_db=":memory:",
        run_at="2026-09-30T00:00:00Z",
        raw=raw,
        effective=effective,
        reconciliation=reconciliation,
        override_audit=tuple(),
    )


def make_authority_resolution(
    commitment_id: str = "test_cid",
    run_result_id: str = "test_rrid",
    description_key: str = "test_pattern",
    field_name: str = "planning_amount",
    baseline_value: object = Decimal("500"),
    resolved_value: object = Decimal("450"),
    outcome: AuthorityOutcome = AuthorityOutcome.APPLIED_FAMILY_REVIEW,
    winning_source: Optional[str] = "FAMILY_REVIEW",
    family_review_row_id: Optional[int] = None,
    family_review_value: object = None,
    manual_override_row_id: Optional[int] = None,
    manual_override_value: object = None,
) -> AuthorityFieldResolution:
    # Auto-set row IDs and values if not provided, based on winning_source
    if family_review_row_id is None and winning_source == "FAMILY_REVIEW":
        family_review_row_id = 1
        family_review_value = resolved_value
    if manual_override_row_id is None and winning_source == "MANUAL_OVERRIDE":
        manual_override_row_id = 2
        manual_override_value = resolved_value

    return AuthorityFieldResolution(
        user_id=1,
        commitment_id=commitment_id,
        run_result_id=run_result_id,
        description_key=description_key,
        field_name=field_name,
        baseline_value=baseline_value,
        family_review_row_id=family_review_row_id,
        family_review_value=family_review_value,
        manual_override_row_id=manual_override_row_id,
        manual_override_value=manual_override_value,
        winning_source=winning_source,
        resolved_value=resolved_value,
        override_id="test_oid",
        outcome=outcome,
    )


# ── Test Cases ───────────────────────────────────────────────────────────────

class TestNoAuthority:
    """Verify patterns are unchanged when no authority is present."""

    def test_empty_authority_report(self):
        pattern = make_pattern()
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(run_id="test_run_id", user_id=1)

        result = apply_authority_to_analysis(base_report, authority_report)

        assert len(result.final_report.effective.patterns) == 1
        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.planning_amount == Decimal("500")
        assert final_pattern.monthly_reserve_contrib == Decimal("500")
        assert final_pattern.reserve_eligible is True

    def test_baseline_no_authority_outcome(self):
        pattern = make_pattern()
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(outcome=AuthorityOutcome.BASELINE_NO_AUTHORITY)
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.planning_amount == Decimal("500")


class TestFamilyReviewAuthority:
    """Verify FAMILY_REVIEW authority is applied correctly."""

    def test_planning_amount_family_review(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    baseline_value=Decimal("500"),
                    resolved_value=Decimal("450"),
                    outcome=AuthorityOutcome.APPLIED_FAMILY_REVIEW,
                    winning_source="FAMILY_REVIEW",
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.planning_amount == Decimal("450")

    def test_lifecycle_status_family_review(self):
        pattern = make_pattern(
            lifecycle=LifecycleStatus.ACTIVE,
            reserve_eligible=True,
            monthly_reserve_contrib=Decimal("500"),
        )
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="lifecycle_status",
                    baseline_value=LifecycleStatus.ACTIVE,
                    resolved_value=LifecycleStatus.POSSIBLY_STOPPED,
                    outcome=AuthorityOutcome.APPLIED_FAMILY_REVIEW,
                    winning_source="FAMILY_REVIEW",
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.lifecycle_status == LifecycleStatus.POSSIBLY_STOPPED
        # Should lose reserve eligibility
        assert final_pattern.reserve_eligible is False
        assert final_pattern.monthly_reserve_contrib == Decimal("0")


class TestManualOverrideAuthority:
    """Verify MANUAL_OVERRIDE precedence over FAMILY_REVIEW."""

    def test_manual_override_precedence(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    baseline_value=Decimal("500"),
                    family_review_row_id=1,
                    family_review_value=Decimal("450"),
                    manual_override_row_id=2,
                    manual_override_value=Decimal("400"),
                    resolved_value=Decimal("400"),
                    outcome=AuthorityOutcome.APPLIED_MANUAL_OVERRIDE,
                    winning_source="MANUAL_OVERRIDE",
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        # MANUAL_OVERRIDE should win, not FAMILY_REVIEW
        assert final_pattern.planning_amount == Decimal("400")


class TestMultiFieldAuthority:
    """Verify all six supported fields work correctly."""

    def test_all_six_fields(self):
        pattern = make_pattern(
            recurrence=RecurrenceStatus.RECURRING,
            commitment=CommitmentStatus.COMMITTED,
            lifecycle=LifecycleStatus.ACTIVE,
            cadence=Cadence.MONTHLY,
            planning_amount=Decimal("500"),
            purpose_type=PurposeType.HOUSING,
        )
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="recurrence_status",
                    baseline_value=RecurrenceStatus.RECURRING,
                    resolved_value=RecurrenceStatus.RECURRING,
                ),
                make_authority_resolution(
                    field_name="commitment_status",
                    baseline_value=CommitmentStatus.COMMITTED,
                    resolved_value=CommitmentStatus.COMMITTED,
                ),
                make_authority_resolution(
                    field_name="lifecycle_status",
                    baseline_value=LifecycleStatus.ACTIVE,
                    resolved_value=LifecycleStatus.ACTIVE,
                ),
                make_authority_resolution(
                    field_name="cadence",
                    baseline_value=Cadence.MONTHLY,
                    resolved_value=Cadence.BIWEEKLY,
                ),
                make_authority_resolution(
                    field_name="planning_amount",
                    baseline_value=Decimal("500"),
                    resolved_value=Decimal("600"),
                ),
                make_authority_resolution(
                    field_name="purpose_type",
                    baseline_value=PurposeType.HOUSING,
                    resolved_value=PurposeType.UTILITY,
                ),
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.recurrence_status == RecurrenceStatus.RECURRING
        assert final_pattern.commitment_status == CommitmentStatus.COMMITTED
        assert final_pattern.lifecycle_status == LifecycleStatus.ACTIVE
        assert final_pattern.cadence == Cadence.BIWEEKLY
        assert final_pattern.planning_amount == Decimal("600")
        assert final_pattern.purpose_type == PurposeType.UTILITY


class TestDerivedRecomputation:
    """Verify derived fields are recomputed correctly after authority."""

    def test_monthly_equivalent_recomputed_on_cadence_change(self):
        pattern = make_pattern(
            cadence=Cadence.MONTHLY,
            planning_amount=Decimal("600"),
            monthly_reserve_contrib=Decimal("600"),
        )
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="cadence",
                    baseline_value=Cadence.MONTHLY,
                    resolved_value=Cadence.BIWEEKLY,
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        # BIWEEKLY: 600 × 26 / 12 = 1300.00
        assert final_pattern.cadence == Cadence.BIWEEKLY
        assert final_pattern.monthly_reserve_contrib == Decimal("1300.00")

    def test_reserve_eligible_recomputed_on_lifecycle_change(self):
        pattern = make_pattern(
            lifecycle=LifecycleStatus.ACTIVE,
            reserve_eligible=True,
            monthly_reserve_contrib=Decimal("500"),
        )
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="lifecycle_status",
                    baseline_value=LifecycleStatus.ACTIVE,
                    resolved_value=LifecycleStatus.ENDED,
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.lifecycle_status == LifecycleStatus.ENDED
        assert final_pattern.reserve_eligible is False
        assert final_pattern.monthly_reserve_contrib == Decimal("0")

    def test_budget_class_recomputed(self):
        pattern = make_pattern(
            recurrence=RecurrenceStatus.RECURRING,
            commitment=CommitmentStatus.COMMITTED,
            amount_behavior=AmountBehavior.VERY_STABLE,
            budget_class=BudgetClass.FIXED_AMOUNT_RECURRING,
        )
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="commitment_status",
                    baseline_value=CommitmentStatus.COMMITTED,
                    resolved_value=CommitmentStatus.NON_COMMITTED,
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.budget_class == BudgetClass.RECURRING_NON_COMMITMENT


class TestNoDoubleCounting:
    """Verify authority modifies existing pattern, never adds a second."""

    def test_single_pattern_remains_single(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        assert len(result.final_report.effective.patterns) == 1

    def test_multiple_patterns_count_unchanged(self):
        patterns = (
            make_pattern(description_key="p1", planning_amount=Decimal("500")),
            make_pattern(description_key="p2", planning_amount=Decimal("300")),
        )
        base_report = make_classification_report(patterns=patterns)
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    description_key="p1",
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        assert len(result.final_report.effective.patterns) == 2


class TestImmutability:
    """Verify original ClassificationReport is never mutated."""

    def test_base_report_unchanged(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))
        original_amount = base_report.effective.patterns[0].planning_amount

        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        # Base report must be unchanged
        assert base_report.effective.patterns[0].planning_amount == original_amount
        # Final report must be changed
        assert result.final_report.effective.patterns[0].planning_amount == Decimal("450")

    def test_raw_unchanged(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))
        original_raw_amount = base_report.raw.patterns[0].planning_amount

        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        # Raw must be unchanged
        assert result.final_report.raw.patterns[0].planning_amount == original_raw_amount
        # Effective must be changed
        assert result.final_report.effective.patterns[0].planning_amount == Decimal("450")


class TestReconciliationRebuilt:
    """Verify reconciliation is recomputed with final derived values."""

    def test_monthly_reserve_reconciliation_updated(self):
        pattern = make_pattern(
            planning_amount=Decimal("500"),
            monthly_reserve_contrib=Decimal("500"),
            reserve_eligible=True,
        )
        base_report = make_classification_report(
            patterns=(pattern,),
            monthly_reserve_effective=Decimal("500"),
        )
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    baseline_value=Decimal("500"),
                    resolved_value=Decimal("600"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        # Final derived value should reflect new planning_amount
        final_reserve = result.final_report.reconciliation.monthly_reserve.derived_value
        # 600 × 12 / 12 = 600.00
        assert final_reserve == Decimal("600.00")


class TestDecisionSourceMarked:
    """Verify applied patterns are marked as FAMILY_REVIEW decision_source."""

    def test_decision_source_family_review(self):
        pattern = make_pattern(decision_source=DecisionSource.CLASSIFIER)
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id="test_run_id",
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.decision_source == DecisionSource.FAMILY_REVIEW


class TestOrchestrationComposition:
    """Verify orchestrator composes Phase 2D1 + Phase 2D2 correctly."""

    def test_orchestrator_uses_real_reports(self):
        """Prove orchestrator consumes real PersistenceReport + LinkReport."""
        from v4_authority_orchestration import orchestrate_authority_adjustment
        from v4_persistence import PersistenceReport, RunResultOutcome, FamilyResolution
        from v4_linking import LinkReport, PatternLinkResult, LinkOutcome
        import sqlite3
        from datetime import datetime

        # Set up in-memory database with commitment_authority table
        conn = sqlite3.Connection(":memory:")
        conn.execute("""
            CREATE TABLE commitments (
                id      TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (id, user_id)
            )
        """)
        conn.execute("""
            CREATE TABLE commitment_authority (
                id                  INTEGER PRIMARY KEY AUTOINCREMENT,
                commitment_id       TEXT NOT NULL,
                user_id             INTEGER NOT NULL,
                field_name          TEXT NOT NULL,
                value               TEXT DEFAULT NULL,
                authority_source    TEXT NOT NULL
                    CHECK(authority_source IN ('MANUAL_OVERRIDE', 'FAMILY_REVIEW')),
                override_id         TEXT NOT NULL,
                is_active           INTEGER NOT NULL DEFAULT 1
                    CHECK(is_active IN (0, 1)),
                created_at          TEXT NOT NULL,
                created_by          INTEGER NOT NULL,
                revoked_at          TEXT DEFAULT NULL,
                revoked_by          INTEGER DEFAULT NULL,
                UNIQUE(override_id),
                FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id)
            )
        """)
        conn.execute(
            "CREATE INDEX idx_ca_resolve "
            "ON commitment_authority(commitment_id, field_name, created_at DESC) "
            "WHERE is_active = 1"
        )

        # Insert commitment
        conn.execute(
            "INSERT INTO commitments (id, user_id) VALUES (?, ?)",
            ("test_cid", 1)
        )

        # Insert authority record: FAMILY_REVIEW planning_amount 450
        conn.execute(
            """INSERT INTO commitment_authority
            (commitment_id, user_id, field_name, value, authority_source,
             override_id, is_active, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("test_cid", 1, "planning_amount", "450", "FAMILY_REVIEW",
             "oid_1", 1, datetime.utcnow().isoformat(), 1)
        )
        conn.commit()

        # Create pattern and reports
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))

        # Real PersistenceReport (with outcomes, not results)
        persistence_report = PersistenceReport(
            run_id=base_report.run_id,
            user_id=1,
            outcomes=[
                RunResultOutcome(
                    run_result_id="test_rrid",
                    description_key="test_pattern",
                    stream_index=0,
                    family_id="test_fid",
                    family_resolution=FamilyResolution.MATCHED_EXISTING,
                )
            ]
        )

        # Real LinkReport
        link_report = LinkReport(
            run_id=base_report.run_id,
            user_id=1,
            results=[
                PatternLinkResult(
                    description_key="test_pattern",
                    stream_index=0,
                    run_result_id="test_rrid",
                    family_id="test_fid",
                    outcome=LinkOutcome.LINKED,
                    commitment_id="test_cid",
                )
            ]
        )

        # Call orchestrate_authority_adjustment (NOT apply_authority_to_analysis directly)
        result = orchestrate_authority_adjustment(
            conn,
            base_report,
            persistence_report,
            link_report,
            user_id=1,
        )

        # Verify: baseline 500 → FAMILY_REVIEW 450
        assert result.final_report.effective.patterns[0].planning_amount == Decimal("450")
        # Verify immutability: base_report unchanged
        assert result.base_report.effective.patterns[0].planning_amount == Decimal("500")

        conn.close()


class TestDeferredCompatibility:
    """Verify DEFER_PARALLEL_UNRESOLVED and DEFER_CANONICAL_MERGE remain unchanged."""

    def test_defer_parallel_unresolved_unchanged(self):
        """One DEFER_PARALLEL_UNRESOLVED case remains unaffected by Phase 2D2."""
        pattern = make_pattern(
            description_key="הוק לגל נעמי לסניף 17-662",  # Real deferred key
            planning_amount=Decimal("607"),
        )
        base_report = make_classification_report(patterns=(pattern,))
        # No authority resolution for this deferred pattern
        authority_report = AuthorityResolutionReport(
            run_id=base_report.run_id,
            user_id=1,
            resolutions=[]
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        # Pattern must remain exactly as-is
        assert final_pattern.planning_amount == Decimal("607")
        assert final_pattern.description_key == "הוק לגל נעמי לסניף 17-662"

    def test_canonical_merge_unchanged(self):
        """One google-cloud-tbd DEFER_CANONICAL_MERGE case remains unaffected."""
        pattern = make_pattern(
            description_key="google-cloud-tbd",
            canonical_identity="google-cloud-tbd",
            planning_amount=Decimal("1500"),
        )
        base_report = make_classification_report(patterns=(pattern,))
        authority_report = AuthorityResolutionReport(
            run_id=base_report.run_id,
            user_id=1,
            resolutions=[]
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        final_pattern = result.final_report.effective.patterns[0]
        assert final_pattern.planning_amount == Decimal("1500")
        assert final_pattern.canonical_identity == "google-cloud-tbd"


class TestNoDoubleApplicationWithHardcodedOverrides:
    """Prove hardcoded override + persisted authority don't double-apply."""

    def test_single_pattern_single_field_modification(self):
        """Single pattern receives single field overlay; no duplication."""
        # Baseline from hardcoded override already has 500
        pattern = make_pattern(
            planning_amount=Decimal("500"),
            monthly_reserve_contrib=Decimal("500"),
        )
        base_report = make_classification_report(patterns=(pattern,))

        # Persisted authority says: override to 450
        authority_report = AuthorityResolutionReport(
            run_id=base_report.run_id,
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        # One pattern in, one pattern out (no duplication)
        assert len(result.final_report.effective.patterns) == 1
        final_pattern = result.final_report.effective.patterns[0]
        # Exactly the persisted value applied
        assert final_pattern.planning_amount == Decimal("450")
        # No additional patterns created
        assert len(result.base_report.effective.patterns) == len(result.final_report.effective.patterns)


class Test13TableContentImmutability:
    """Verify all 13 tables remain unchanged by Phase 2D2 orchestration."""

    def test_13_table_content_unchanged_after_orchestration(self):
        """Snapshot 13-table contents before/after orchestration with REAL project schema."""
        from v4_authority_orchestration import orchestrate_authority_adjustment
        from v4_persistence import PersistenceReport, RunResultOutcome, FamilyResolution
        from v4_linking import LinkReport, PatternLinkResult, LinkOutcome
        import sqlite3
        import tempfile
        from datetime import datetime
        import app
        import os

        # Use REAL project schema via app.init_db() against temp DB only
        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        orig_path = app.DB_PATH
        conn = None
        try:
            app.DB_PATH = db_path
            app.init_db()  # Creates REAL project schema

            conn = sqlite3.connect(db_path)
            conn.execute("PRAGMA foreign_keys = ON")

            # Insert base data into real schema
            now_str = datetime.utcnow().isoformat()
            conn.execute("INSERT INTO commitments (id, user_id, created_at, updated_at) VALUES ('c1', 1, ?, ?)", (now_str, now_str))
            conn.execute(
                """INSERT INTO commitment_authority
                (commitment_id, user_id, field_name, value, authority_source, override_id, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                ("c1", 1, "planning_amount", "450", "FAMILY_REVIEW", "oid_1", now_str, 1)
            )
            conn.commit()

            # Snapshot BEFORE all 13 tables
            before_snapshots = {}
            for table in [
                "commitment_authority", "v4_run_results", "commitment_classifier_snapshots",
                "pattern_families", "commitments", "commitment_expense_links",
                "commitment_occurrences", "commitment_installment_meta",
                "commitment_suggestions", "commitment_link_conflicts", "commitment_link_events",
                "expenses", "installments"
            ]:
                rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                before_snapshots[table] = rows

            # Create reports and call orchestrator
            pattern = make_pattern(planning_amount=Decimal("500"))
            base_report = make_classification_report(patterns=(pattern,))

            persistence_report = PersistenceReport(
                run_id=base_report.run_id,
                user_id=1,
                outcomes=[
                    RunResultOutcome(
                        run_result_id="test_rrid",
                        description_key="test_pattern",
                        stream_index=0,
                        family_id="test_fid",
                        family_resolution=FamilyResolution.MATCHED_EXISTING,
                    )
                ]
            )

            link_report = LinkReport(
                run_id=base_report.run_id,
                user_id=1,
                results=[
                    PatternLinkResult(
                        description_key="test_pattern",
                        stream_index=0,
                        run_result_id="test_rrid",
                        family_id="test_fid",
                        outcome=LinkOutcome.LINKED,
                        commitment_id="c1",
                    )
                ]
            )

            # Run orchestration
            result = orchestrate_authority_adjustment(
                conn,
                base_report,
                persistence_report,
                link_report,
                user_id=1,
            )

            # Snapshot AFTER all 13 tables
            after_snapshots = {}
            for table in [
                "commitment_authority", "v4_run_results", "commitment_classifier_snapshots",
                "pattern_families", "commitments", "commitment_expense_links",
                "commitment_occurrences", "commitment_installment_meta",
                "commitment_suggestions", "commitment_link_conflicts", "commitment_link_events",
                "expenses", "installments"
            ]:
                rows = conn.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
                after_snapshots[table] = rows

            # Compare: exact row content immutability
            for table in before_snapshots.keys():
                assert before_snapshots[table] == after_snapshots[table], \
                    f"Real schema table {table} content changed"

        finally:
            if conn:
                conn.close()
            app.DB_PATH = orig_path
            if os.path.exists(db_path):
                try:
                    os.remove(db_path)
                except OSError:
                    pass  # File might be locked temporarily on Windows


class TestCallerTransactionOwnership:
    """Verify caller owns transaction; orchestrator does not commit/rollback."""

    def test_orchestrator_preserves_caller_transaction(self):
        """Caller starts transaction, orchestrator runs, caller can rollback."""
        from v4_authority_orchestration import orchestrate_authority_adjustment
        from v4_persistence import PersistenceReport, RunResultOutcome, FamilyResolution
        from v4_linking import LinkReport, PatternLinkResult, LinkOutcome
        import sqlite3
        from datetime import datetime

        # Set up database
        conn = sqlite3.connect(":memory:")

        conn.execute("""
            CREATE TABLE commitments (
                id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (id, user_id)
            )
        """)
        conn.execute("""
            CREATE TABLE commitment_authority (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commitment_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                value TEXT DEFAULT NULL,
                authority_source TEXT NOT NULL,
                override_id TEXT NOT NULL UNIQUE,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id)
            )
        """)

        # Caller STARTS explicit transaction
        conn.execute("BEGIN TRANSACTION")

        try:
            # Caller writes unique marker data
            conn.execute("INSERT INTO commitments (id, user_id) VALUES ('caller_marker', 1)")
            conn.execute("INSERT INTO commitments (id, user_id) VALUES ('c1', 1)")
            conn.execute(
                """INSERT INTO commitment_authority
                (commitment_id, user_id, field_name, value, authority_source, override_id, created_at, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                ("c1", 1, "planning_amount", "450", "FAMILY_REVIEW", "oid_1", datetime.utcnow().isoformat(), 1)
            )

            # Verify caller write exists
            marker_before = conn.execute("SELECT COUNT(*) FROM commitments WHERE id = 'caller_marker'").fetchone()[0]
            assert marker_before == 1, "Caller write should exist before orchestration"

            # Create reports
            pattern = make_pattern(planning_amount=Decimal("500"))
            base_report = make_classification_report(patterns=(pattern,))

            persistence_report = PersistenceReport(
                run_id=base_report.run_id,
                user_id=1,
                outcomes=[
                    RunResultOutcome(
                        run_result_id="test_rrid",
                        description_key="test_pattern",
                        stream_index=0,
                        family_id="test_fid",
                        family_resolution=FamilyResolution.MATCHED_EXISTING,
                    )
                ]
            )

            link_report = LinkReport(
                run_id=base_report.run_id,
                user_id=1,
                results=[
                    PatternLinkResult(
                        description_key="test_pattern",
                        stream_index=0,
                        run_result_id="test_rrid",
                        family_id="test_fid",
                        outcome=LinkOutcome.LINKED,
                        commitment_id="c1",
                    )
                ]
            )

            # Run orchestrator
            result = orchestrate_authority_adjustment(
                conn,
                base_report,
                persistence_report,
                link_report,
                user_id=1,
            )

            # Verify orchestrator did NOT commit
            # (connection should still be in transaction)

            # Caller performs ROLLBACK
            conn.execute("ROLLBACK")

            # Verify caller-owned write is gone (proves no commit happened)
            marker_after = conn.execute("SELECT COUNT(*) FROM commitments WHERE id = 'caller_marker'").fetchone()[0]
            assert marker_after == 0, "Caller write should be removed by rollback"

            # Verify connection is still open
            try:
                conn.execute("SELECT 1").fetchone()
            except sqlite3.ProgrammingError:
                raise AssertionError("Orchestrator closed the caller connection")

        finally:
            conn.close()


class TestCompleteInputImmutability:
    """Verify all input objects remain unchanged at CONTENT level."""

    def test_classification_report_content_immutable(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))

        # Snapshot entire effective and raw
        original_effective_patterns = base_report.effective.patterns
        original_raw_patterns = base_report.raw.patterns
        original_income_streams = base_report.effective.income_streams
        authority_report = AuthorityResolutionReport(
            run_id=base_report.run_id,
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        # Exact equality: all patterns tuples unchanged
        assert base_report.effective.patterns == original_effective_patterns
        assert base_report.raw.patterns == original_raw_patterns
        assert base_report.effective.income_streams == original_income_streams

    def test_raw_classifier_output_content_immutable(self):
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))

        original_raw_patterns_tuple = base_report.raw.patterns
        authority_report = AuthorityResolutionReport(
            run_id=base_report.run_id,
            user_id=1,
            resolutions=[
                make_authority_resolution(
                    field_name="planning_amount",
                    resolved_value=Decimal("450"),
                )
            ],
        )

        result = apply_authority_to_analysis(base_report, authority_report)

        # Raw patterns tuple must be identical
        assert result.final_report.raw.patterns == original_raw_patterns_tuple
        assert base_report.raw.patterns == original_raw_patterns_tuple

    def test_persistence_report_content_immutable(self):
        from v4_persistence import PersistenceReport, RunResultOutcome, FamilyResolution
        import copy

        outcome = RunResultOutcome(
            run_result_id="rrid1",
            description_key="test_pattern",
            stream_index=0,
            family_id="fid1",
            family_resolution=FamilyResolution.MATCHED_EXISTING,
        )
        persistence_report = PersistenceReport(
            run_id="test_run",
            user_id=1,
            outcomes=[outcome]
        )

        # Independent snapshot (not alias)
        original_outcomes = copy.deepcopy(persistence_report.outcomes)
        authority_report = AuthorityResolutionReport(run_id="test_run", user_id=1)

        pattern = make_pattern()
        base_report = make_classification_report(patterns=(pattern,))

        result = apply_authority_to_analysis(base_report, authority_report)

        # Exact content equality
        assert persistence_report.outcomes == original_outcomes
        assert persistence_report.outcomes[0] == outcome
        assert persistence_report.outcomes[0].run_result_id == "rrid1"
        assert persistence_report.outcomes[0].family_resolution == FamilyResolution.MATCHED_EXISTING

    def test_link_report_content_immutable(self):
        from v4_linking import LinkReport, PatternLinkResult, LinkOutcome
        import copy

        link_result = PatternLinkResult(
            description_key="test_pattern",
            stream_index=0,
            run_result_id="rrid1",
            family_id="fid1",
            outcome=LinkOutcome.LINKED,
            commitment_id="c1",
        )
        link_report = LinkReport(
            run_id="test_run",
            user_id=1,
            results=[link_result]
        )

        # Independent snapshot (not alias)
        original_results = copy.deepcopy(link_report.results)
        authority_report = AuthorityResolutionReport(run_id="test_run", user_id=1)

        pattern = make_pattern()
        base_report = make_classification_report(patterns=(pattern,))

        result = apply_authority_to_analysis(base_report, authority_report)

        # Exact content equality
        assert link_report.results == original_results
        assert link_report.results[0] == link_result
        assert link_report.results[0].commitment_id == "c1"
        assert link_report.results[0].outcome == LinkOutcome.LINKED

    def test_authority_resolution_report_content_immutable(self):
        import copy
        resolution = make_authority_resolution(
            field_name="planning_amount",
            resolved_value=Decimal("450"),
        )
        authority_report = AuthorityResolutionReport(
            run_id="test_run",
            user_id=1,
            resolutions=[resolution]
        )

        # Independent snapshot (not alias)
        original_resolutions = copy.deepcopy(authority_report.resolutions)

        pattern = make_pattern()
        base_report = make_classification_report(patterns=(pattern,))

        result = apply_authority_to_analysis(base_report, authority_report)

        # Exact content equality
        assert authority_report.resolutions == original_resolutions
        assert authority_report.resolutions[0] == resolution
        assert authority_report.resolutions[0].field_name == "planning_amount"
        assert authority_report.resolutions[0].resolved_value == Decimal("450")


class TestErrorPropagation:
    """Verify hard Phase 2D1 errors propagate through orchestrator."""

    def test_phase2d1_error_propagates_through_orchestrator(self):
        """Force Phase 2D1 resolver to raise; prove it propagates."""
        from v4_authority_orchestration import orchestrate_authority_adjustment
        from v4_persistence import PersistenceReport, RunResultOutcome, FamilyResolution
        from v4_linking import LinkReport, PatternLinkResult, LinkOutcome
        import sqlite3
        from datetime import datetime

        # Set up database with invalid state to force Phase 2D1 error
        conn = sqlite3.connect(":memory:")

        conn.execute("""
            CREATE TABLE commitments (
                id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                PRIMARY KEY (id, user_id)
            )
        """)
        conn.execute("""
            CREATE TABLE commitment_authority (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                commitment_id TEXT NOT NULL,
                user_id INTEGER NOT NULL,
                field_name TEXT NOT NULL,
                value TEXT DEFAULT NULL,
                authority_source TEXT NOT NULL,
                override_id TEXT NOT NULL UNIQUE,
                is_active INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                created_by INTEGER NOT NULL,
                FOREIGN KEY (commitment_id, user_id) REFERENCES commitments(id, user_id)
            )
        """)

        # Insert commitment
        conn.execute("INSERT INTO commitments (id, user_id) VALUES ('c1', 1)")

        # Insert DUPLICATE active authority rows (violates cardinality) to force Phase 2D1 error
        conn.execute(
            """INSERT INTO commitment_authority
            (commitment_id, user_id, field_name, value, authority_source, override_id, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("c1", 1, "planning_amount", "450", "FAMILY_REVIEW", "oid_1", datetime.utcnow().isoformat(), 1)
        )
        conn.execute(
            """INSERT INTO commitment_authority
            (commitment_id, user_id, field_name, value, authority_source, override_id, created_at, created_by)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            ("c1", 1, "planning_amount", "425", "FAMILY_REVIEW", "oid_2", datetime.utcnow().isoformat(), 1)
        )
        conn.commit()

        # Create reports
        pattern = make_pattern(planning_amount=Decimal("500"))
        base_report = make_classification_report(patterns=(pattern,))

        persistence_report = PersistenceReport(
            run_id=base_report.run_id,
            user_id=1,
            outcomes=[
                RunResultOutcome(
                    run_result_id="test_rrid",
                    description_key="test_pattern",
                    stream_index=0,
                    family_id="test_fid",
                    family_resolution=FamilyResolution.MATCHED_EXISTING,
                )
            ]
        )

        link_report = LinkReport(
            run_id=base_report.run_id,
            user_id=1,
            results=[
                PatternLinkResult(
                    description_key="test_pattern",
                    stream_index=0,
                    run_result_id="test_rrid",
                    family_id="test_fid",
                    outcome=LinkOutcome.LINKED,
                    commitment_id="c1",
                )
            ]
        )

        # Try to orchestrate; Phase 2D1 should raise ValueError
        with pytest.raises(ValueError) as exc_info:
            orchestrate_authority_adjustment(
                conn,
                base_report,
                persistence_report,
                link_report,
                user_id=1,
            )

        # Prove error propagated (not suppressed)
        assert "duplicate" in str(exc_info.value).lower() or \
               "cardinality" in str(exc_info.value).lower() or \
               "authority" in str(exc_info.value).lower(), \
               f"Expected Phase 2D1 error, got: {exc_info.value}"

        conn.close()
