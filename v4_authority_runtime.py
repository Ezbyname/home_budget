"""
Unified Commitments — Phase 2D2
Authority Runtime Integration + Derived-Value Recomputation

Consumes AuthorityResolutionReport (Phase 2D1) and applies resolved authority
values to effective patterns, recomputing dependent derived fields.

PURPOSE:
    Phase 2D2 overlays resolved authority on the current effective baseline,
    producing an authority-adjusted ClassificationReport.

    Does NOT write to any table.
    Does NOT feed adjusted result back into persist_run() or link_phase2b().
    No feedback loop.

RUNTIME ORDER:
    1. run_analysis()           → raw + effective baseline
    2. persist_run()            → Phase 2A persisted identity
    3. link_phase2b()           → exact commitment identity
    4. resolve_authority_from_reports()  → Phase 2D1 precedence result
    5. apply_authority_to_analysis()     → Phase 2D2 overlay + recompute
    6. produce final_report (authority-adjusted)

LOCKED ARCHITECTURE:
    - Precedence: MANUAL_OVERRIDE > FAMILY_REVIEW > baseline
    - apply_overrides() baseline remains the starting point
    - Authority changes field VALUE on existing pattern (no duplication)
    - Six supported fields only
    - Reuse existing helpers (never copy formulas)
"""

from __future__ import annotations

from dataclasses import dataclass, replace
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
    LifecycleStatus,
    PatternResult,
    PurposeType,
    RawClassifierOutput,
    ReconciliationReport,
    RecurrenceStatus,
    derive_budget_class,
    is_reserve_eligible,
    make_reconciliation_record,
    monthly_equivalent,
    quantize_ils,
)
from v4_authority_read import AuthorityResolutionReport, AuthorityOutcome


# ── Supported authority field names ──────────────────────────────────────────

_AUTHORITY_FIELDS: frozenset[str] = frozenset({
    "recurrence_status",
    "commitment_status",
    "lifecycle_status",
    "cadence",
    "planning_amount",
    "purpose_type",
})


# ── Result wrapper ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class AuthorityAdjustedAnalysis:
    """
    Wrapper containing base report, authority-adjusted final report, and
    authority provenance.

    base_report:
        original ClassificationReport (unchanged)
    final_report:
        new ClassificationReport with authority overlays applied and
        derived values recomputed
    authority_report:
        Phase 2D1 resolution results (provenance)
    """
    base_report:        ClassificationReport
    final_report:       ClassificationReport
    authority_report:   AuthorityResolutionReport


# ── Per-pattern authority overlay ────────────────────────────────────────────

def _apply_authority_to_pattern(
    pattern: PatternResult,
    field_overrides: dict[str, object],
) -> PatternResult:
    """
    Apply field-level authority overrides to a single pattern and recompute
    derived fields.

    Steps:
    1. Update allowed fields (recurrence_status, commitment_status, lifecycle_status,
       cadence, planning_amount, purpose_type).
    2. Recompute derived fields:
       - reserve_eligible: is_reserve_eligible(recurrence, commitment, lifecycle, amount)
       - monthly_reserve_contrib: Decimal(0) if not eligible, else monthly_equivalent(amount, cadence)
       - budget_class: derive_budget_class(recurrence, commitment, amount_behavior)
    3. Mark decision_source as FAMILY_REVIEW (overlay provenance).
    4. Return new PatternResult with updated fields.

    Does NOT mutate the original pattern.
    Does NOT add a second pattern copy.
    """
    # Extract overrides (those we care about)
    new_recurrence = field_overrides.get("recurrence_status", pattern.recurrence_status)
    new_commitment = field_overrides.get("commitment_status", pattern.commitment_status)
    new_lifecycle = field_overrides.get("lifecycle_status", pattern.lifecycle_status)
    new_cadence = field_overrides.get("cadence", pattern.cadence)
    new_planning_amount = field_overrides.get("planning_amount", pattern.planning_amount)
    new_purpose_type = field_overrides.get("purpose_type", pattern.purpose_type)

    # Recompute reserve eligibility
    new_reserve_eligible = is_reserve_eligible(
        new_recurrence,
        new_commitment,
        new_lifecycle,
        new_planning_amount,
    )

    # Recompute monthly reserve contribution
    if new_reserve_eligible and new_cadence in ["monthly", "biweekly", "every_2_months", "quarterly", "semiannual", "yearly"]:
        try:
            new_monthly_reserve_contrib = monthly_equivalent(new_planning_amount, new_cadence)
        except ValueError:
            new_monthly_reserve_contrib = Decimal("0")
    else:
        new_monthly_reserve_contrib = Decimal("0")

    # Recompute budget class
    new_budget_class = derive_budget_class(
        new_recurrence,
        new_commitment,
        pattern.amount_behavior,
    )

    # Apply changes via dataclass replace
    updated = replace(
        pattern,
        recurrence_status=new_recurrence,
        commitment_status=new_commitment,
        lifecycle_status=new_lifecycle,
        cadence=new_cadence,
        planning_amount=new_planning_amount,
        purpose_type=new_purpose_type,
        reserve_eligible=new_reserve_eligible,
        monthly_reserve_contrib=new_monthly_reserve_contrib,
        budget_class=new_budget_class,
        decision_source=DecisionSource.FAMILY_REVIEW,
    )
    return updated


# ── Full analysis authority application ──────────────────────────────────────

def apply_authority_to_analysis(
    base_report: ClassificationReport,
    authority_report: AuthorityResolutionReport,
    *,
    reviewed_targets: Optional[object] = None,
) -> AuthorityAdjustedAnalysis:
    """
    Apply Phase 2D1 authority resolutions to the base ClassificationReport.

    Steps:
    1. Group authority resolutions by commitment_id.
    2. For each pattern in effective.patterns:
       - Find resolutions for its commitment (via description_key → commitment_id mapping).
       - Collect field overrides from APPLIED resolutions.
       - Call _apply_authority_to_pattern() to overlay and recompute.
    3. Rebuild effective patterns list with updated patterns.
    4. Recompute aggregate financial values:
       - planning_income_effective: sum of income_streams.planning_baseline
       - monthly_reserve_effective: compute_monthly_reserve(final_patterns)
    5. Rebuild reconciliation records with fresh derived_value.
    6. Create new ClassificationReport with authority-adjusted effective result.
    7. Return AuthorityAdjustedAnalysis wrapper.

    Parameters:
        base_report         — original ClassificationReport (never modified)
        authority_report    — Phase 2D1 resolution results
        reviewed_targets    — optional reviewed_targets object (future use)

    Returns:
        AuthorityAdjustedAnalysis with base_report, final_report, authority_report

    Raises:
        ValueError if authority resolution produced errors
    """
    from intelligence.v4_cashflow_engine import compute_monthly_reserve

    # ── Step 1: Group resolutions by commitment_id ───────────────────────────
    resolutions_by_commitment = authority_report.by_commitment()

    # ── Step 2: Build pattern → field overrides mapping ─────────────────────
    # We need a way to match patterns to commitments. Use description_key as proxy
    # (patterns with same description_key may share authority via same commitment).
    patterns_by_dkey: dict[str, list[PatternResult]] = {}
    for p in base_report.effective.patterns:
        if p.description_key not in patterns_by_dkey:
            patterns_by_dkey[p.description_key] = []
        patterns_by_dkey[p.description_key].append(p)

    # Collect all unique commitments that have authority for this run
    # (assumption: one commitment per description_key via link_phase2b)
    # For patterns with multiple resolutions, collect all field overrides
    overrides_by_dkey: dict[str, dict[str, object]] = {}
    for resolution in authority_report.resolutions:
        dkey = resolution.description_key
        if resolution.outcome in (
            AuthorityOutcome.APPLIED_FAMILY_REVIEW,
            AuthorityOutcome.APPLIED_MANUAL_OVERRIDE,
        ):
            if dkey not in overrides_by_dkey:
                overrides_by_dkey[dkey] = {}
            # Build field override map: field_name → resolved_value
            overrides_by_dkey[dkey][resolution.field_name] = resolution.resolved_value

    # ── Step 3: Apply authority to each pattern ──────────────────────────────
    final_patterns: list[PatternResult] = []
    for pattern in base_report.effective.patterns:
        field_overrides = overrides_by_dkey.get(pattern.description_key, {})
        if field_overrides:
            updated_pattern = _apply_authority_to_pattern(pattern, field_overrides)
            final_patterns.append(updated_pattern)
        else:
            # No authority for this pattern; include unchanged
            final_patterns.append(pattern)

    final_patterns_tuple = tuple(final_patterns)

    # ── Step 4: Recompute aggregate values ───────────────────────────────────
    # planning_income_effective: sum of income_streams (unchanged)
    final_planning_income = quantize_ils(
        sum(
            (s.planning_baseline for s in base_report.effective.income_streams),
            Decimal("0"),
        )
    )

    # monthly_reserve_effective: from final_patterns
    final_monthly_reserve = compute_monthly_reserve(final_patterns_tuple)

    # ── Step 5: Rebuild reconciliation ───────────────────────────────────────
    # Use reviewed_targets if provided; otherwise fall back to base_report
    if reviewed_targets is not None:
        reviewed_planning_income = getattr(
            reviewed_targets, "planning_income", base_report.reconciliation.planning_income.reviewed_value
        )
        reviewed_monthly_reserve = getattr(
            reviewed_targets, "monthly_reserve", base_report.reconciliation.monthly_reserve.reviewed_value
        )
    else:
        reviewed_planning_income = base_report.reconciliation.planning_income.reviewed_value
        reviewed_monthly_reserve = base_report.reconciliation.monthly_reserve.reviewed_value

    final_reconciliation = ReconciliationReport(
        planning_income=make_reconciliation_record(
            field="planning_income",
            reviewed_value=reviewed_planning_income,
            derived_value=final_planning_income,
            raw_derived_value=base_report.reconciliation.planning_income.raw_derived_value,
        ),
        monthly_reserve=make_reconciliation_record(
            field="monthly_reserve",
            reviewed_value=reviewed_monthly_reserve,
            derived_value=final_monthly_reserve,
            raw_derived_value=base_report.reconciliation.monthly_reserve.raw_derived_value,
        ),
    )

    # ── Step 6: Build final EffectiveFinancialResult ──────────────────────────
    final_effective = EffectiveFinancialResult(
        patterns=final_patterns_tuple,
        income_streams=base_report.effective.income_streams,
        planning_income_effective=final_planning_income,
        monthly_reserve_effective=final_monthly_reserve,
        family_review_items=base_report.effective.family_review_items,
        overrides_applied=base_report.effective.overrides_applied,
    )

    # ── Step 7: Create final ClassificationReport ────────────────────────────
    final_report = ClassificationReport(
        classifier_version=base_report.classifier_version,
        run_id=base_report.run_id,
        analysis_db=base_report.analysis_db,
        run_at=base_report.run_at,
        raw=base_report.raw,                   # unchanged
        effective=final_effective,             # authority-adjusted
        reconciliation=final_reconciliation,   # recomputed
        override_audit=base_report.override_audit,  # unchanged
    )

    # ── Step 8: Return wrapper ───────────────────────────────────────────────
    return AuthorityAdjustedAnalysis(
        base_report=base_report,
        final_report=final_report,
        authority_report=authority_report,
    )
