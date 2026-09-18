"""
V4 Cashflow Engine — Phase B (read-only orchestration, no DB writes).

Responsibilities:
  - Open SQLite in read-only URI mode
  - Load expense and income rows
  - Drive the V4 classifier
  - Apply in-memory reviewed overrides
  - Compute exact Decimal reconciliation
  - Produce ClassificationReport (raw + effective + reconciliation)
  - Serialize to JSON and Markdown

ZERO DB WRITES: this module may not execute INSERT, UPDATE, DELETE,
ALTER, CREATE, DROP, REPLACE, VACUUM, or any PRAGMA that modifies state.
The read-only URI guarantee is enforced by open_readonly_db().
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Optional

from intelligence.v4_classifier import (
    CLASSIFIER_VERSION,
    ExpenseRow, IncomeRow, SettlementRecord,
    classify_expenses, classify_income,
    decimal_from_db,
)
from intelligence.v4_contracts import (
    ClassificationReport, DecisionSource, EffectiveFinancialResult,
    FamilyReviewMappingConflict, IncomeStreamResult, PatternResult,
    RawClassifierOutput, ReconciliationReport, ReviewReason,
    make_reconciliation_record, quantize_ils,
)
# Re-export for convenience (tests import FamilyReviewMappingConflict from here)
__all__ = ["FamilyReviewMappingConflict", "PatternOverride", "ReviewedTargets"]

# ═══════════════════════════════════════════════════════════════════════════
# READ-ONLY DB ACCESS
# ═══════════════════════════════════════════════════════════════════════════

def open_readonly_db(db_path: str) -> sqlite3.Connection:
    """
    Open SQLite database in read-only mode using URI.
    Raises sqlite3.OperationalError if the file does not exist or is
    not readable.  Any attempt to write through this connection will
    raise OperationalError at the SQLite level.
    """
    uri = f"file:{db_path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    # Paranoid guard: no writes possible after this
    conn.execute("PRAGMA query_only = ON;")
    return conn


def _load_expenses(conn: sqlite3.Connection, user_id: Optional[int] = None) -> list[ExpenseRow]:
    """
    Load expense rows. Amount converted via Decimal(str()) to avoid float error.
    """
    if user_id is not None:
        rows = conn.execute(
            "SELECT id, date, category_id, description, amount, source, frequency, card, user_id "
            "FROM expenses WHERE user_id = ? ORDER BY date",
            (user_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, date, category_id, description, amount, source, frequency, card, user_id "
            "FROM expenses ORDER BY date"
        ).fetchall()
    return [
        ExpenseRow(
            id=r["id"],
            date=r["date"],
            category_id=r["category_id"] or "",
            description=r["description"] or "",
            amount=decimal_from_db(r["amount"]),
            source=r["source"] or "manual",
            frequency=r["frequency"] or "random",
            card=r["card"] or "",
            user_id=r["user_id"],
        )
        for r in rows
    ]


def _load_income(conn: sqlite3.Connection, user_id: Optional[int] = None) -> list[IncomeRow]:
    if user_id is not None:
        rows = conn.execute(
            "SELECT id, date, person, source, amount, description, is_recurring, user_id "
            "FROM income WHERE user_id = ? ORDER BY date",
            (user_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, date, person, source, amount, description, is_recurring, user_id "
            "FROM income ORDER BY date"
        ).fetchall()
    return [
        IncomeRow(
            id=r["id"],
            date=r["date"],
            person=r["person"] or "",
            source=r["source"] or "",
            amount=decimal_from_db(r["amount"]),
            description=r["description"] or "",
            is_recurring=r["is_recurring"] or 0,
            user_id=r["user_id"],
        )
        for r in rows
    ]


# ═══════════════════════════════════════════════════════════════════════════
# IN-MEMORY OVERRIDE LAYER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class PatternOverride:
    """
    A single field-level override to apply over a raw PatternResult.
    Family Review decisions are represented as a list of these, not persisted to DB.

    Matching rules (all must hold):
      1. description_key == pattern.description_key  (exact, never fuzzy)
      2. if stream_label_hint != "": stream_label_hint in pattern.label
      3. if label_exact is not None: pattern.label == label_exact  (exact equality)
      4. if amount_hint is not None: pattern.planning_amount == amount_hint
         (checked against RAW classifier planning_amount before any overrides)

    label_exact vs stream_label_hint:
      stream_label_hint — substring match, useful for "stream 2" suffix patterns
      label_exact       — exact equality, use when two segments share the same
                          description_key but have distinguishable exact labels
                          (e.g. "חיובי הלוו חיוב" vs "חיובי הלוו חיוב (stream 2)")

    Validation:
      expected_match_count — if not None, the number of distinct patterns that this
      override's group key must match.
      0 or wrong count → FamilyReviewMappingConflict raised by apply_overrides().

    TBD deduplication:
      canonical_identity — when set, TBD patterns across multiple description_keys
      that share the same canonical_identity are counted as ONE TBD commitment in
      the report (e.g. two Google Cloud raw keys → one canonical TBD entry).
    """
    description_key: str               # matches PatternResult.description_key
    stream_label_hint: str             # optional label fragment to distinguish parallel streams
    field_name: str                    # field on PatternResult to override
    value: object                      # new value (must be the correct type)
    override_id: str                   # human-readable ID for audit
    source: DecisionSource = DecisionSource.FAMILY_REVIEW
    expected_match_count: Optional[int] = None   # None = "don't care"
    canonical_identity: Optional[str] = None     # for TBD canonical deduplication
    amount_hint: Optional[Decimal] = None        # discriminate by classifier planning_amount
    label_exact: Optional[str] = None            # exact pattern.label equality (stronger than hint)


def _override_matches(ov: PatternOverride, pattern: PatternResult) -> bool:
    """
    Return True if an override matches a pattern.

    All criteria must hold:
      1. description_key exact match
      2. stream_label_hint substring match (empty = wildcard)
      3. label_exact exact equality match (None = wildcard)
      4. amount_hint exact match against RAW classifier planning_amount (None = wildcard)
    """
    if ov.description_key != pattern.description_key:
        return False
    if ov.stream_label_hint and ov.stream_label_hint not in pattern.label:
        return False
    if ov.label_exact is not None and pattern.label != ov.label_exact:
        return False
    if ov.amount_hint is not None and pattern.planning_amount != ov.amount_hint:
        return False
    return True


def _override_group_key(ov: PatternOverride) -> tuple:
    """Canonical key that identifies a validation/deduplication group."""
    return (ov.description_key, ov.stream_label_hint, ov.label_exact, ov.amount_hint)


def apply_overrides(
    patterns: tuple[PatternResult, ...],
    overrides: list[PatternOverride],
) -> tuple[tuple[PatternResult, ...], tuple[str, ...], list[dict]]:
    """
    Apply in-memory overrides to raw patterns.
    Returns (updated_patterns, applied_override_ids, override_audit).

    Override matching (all must hold):
      1. description_key exact match (never fuzzy)
      2. stream_label_hint substring match against pattern.label (empty = wildcard)
      3. amount_hint exact match against RAW classifier planning_amount (None = wildcard)

    After applying all overrides, validates each override group whose
    expected_match_count is not None.  Raises FamilyReviewMappingConflict
    if the actual matched count differs from expected_match_count.

    override_audit: one dict per overridden pattern showing classifier
    raw state, override_ids applied, and final effective state — for
    auditability without hiding the original classifier result.
    """
    from intelligence.v4_contracts import (
        FamilyReviewMappingConflict,
        derive_budget_class, is_reserve_eligible,
        CADENCE_OCCURRENCES_PER_YEAR, monthly_equivalent,
    )

    applied: list[str] = []
    updated: list[PatternResult] = []
    audit: list[dict] = []

    # --- Phase 1: count matches per override group (against RAW patterns) ---
    group_match_counts: dict[tuple, int] = {}   # group_key → matched pattern count
    group_canonical: dict[tuple, Optional[str]] = {}  # group_key → canonical_identity

    for pattern in patterns:
        matched_groups: set[tuple] = set()
        for ov in overrides:
            if _override_matches(ov, pattern):
                gk = _override_group_key(ov)
                if gk not in matched_groups:
                    matched_groups.add(gk)
                    group_match_counts[gk] = group_match_counts.get(gk, 0) + 1
                if ov.canonical_identity is not None:
                    group_canonical[gk] = ov.canonical_identity

    # --- Phase 2: apply overrides pattern by pattern ---
    for pattern in patterns:
        relevant = [ov for ov in overrides if _override_matches(ov, pattern)]
        if not relevant:
            updated.append(pattern)
            continue

        # Apply overrides field by field using dataclass replacement
        kwargs: dict = {
            "description_key": pattern.description_key,
            "label": pattern.label,
            "recurrence_status": pattern.recurrence_status,
            "commitment_status": pattern.commitment_status,
            "amount_behavior": pattern.amount_behavior,
            "budget_class": pattern.budget_class,
            "lifecycle_status": pattern.lifecycle_status,
            "purpose_type": pattern.purpose_type,
            "cadence": pattern.cadence,
            "planning_amount": pattern.planning_amount,
            "member_ids": pattern.member_ids,
            "membership_confidence": pattern.membership_confidence,
            "evidence_sources": pattern.evidence_sources,
            "decision_source": pattern.decision_source,
            "family_review_required": pattern.family_review_required,
            "review_reasons": pattern.review_reasons,
            "reserve_eligible": pattern.reserve_eligible,
            "monthly_reserve_contrib": pattern.monthly_reserve_contrib,
            "canonical_identity": pattern.canonical_identity,
        }
        pattern_applied: list[str] = []
        changed_fields: dict[str, dict] = {}
        for ov in relevant:
            if ov.field_name in kwargs:
                old_val = kwargs[ov.field_name]
                kwargs[ov.field_name] = ov.value
                applied.append(ov.override_id)
                pattern_applied.append(ov.override_id)
                changed_fields[ov.field_name] = {
                    "classifier": str(old_val) if old_val is not None else None,
                    "override": str(ov.value) if ov.value is not None else None,
                    "override_id": ov.override_id,
                }
            # Propagate canonical_identity from any matching override
            if ov.canonical_identity is not None and kwargs["canonical_identity"] is None:
                kwargs["canonical_identity"] = ov.canonical_identity

        # Override authority: at least one override applied → FAMILY_REVIEW
        kwargs["decision_source"] = DecisionSource.FAMILY_REVIEW

        # Recompute derived fields after overrides
        kwargs["budget_class"] = derive_budget_class(
            kwargs["recurrence_status"],
            kwargs["commitment_status"],
            kwargs["amount_behavior"],
        )
        kwargs["reserve_eligible"] = is_reserve_eligible(
            kwargs["recurrence_status"],
            kwargs["commitment_status"],
            kwargs["lifecycle_status"],
            kwargs["planning_amount"],
        )
        if kwargs["reserve_eligible"] and kwargs["planning_amount"] is not None:
            cadence = kwargs["cadence"]
            if cadence in CADENCE_OCCURRENCES_PER_YEAR:
                kwargs["monthly_reserve_contrib"] = monthly_equivalent(
                    kwargs["planning_amount"], cadence
                )
            else:
                kwargs["monthly_reserve_contrib"] = kwargs["planning_amount"]
        else:
            kwargs["monthly_reserve_contrib"] = Decimal("0.00")

        effective = PatternResult(**kwargs)
        updated.append(effective)

        # Per-pattern audit record: classifier raw → overrides → effective
        audit.append({
            "label": pattern.label,
            "description_key": pattern.description_key,
            "override_ids_applied": pattern_applied,
            "changed_fields": changed_fields,
            "classifier_result": {
                "recurrence_status": pattern.recurrence_status.value,
                "commitment_status": pattern.commitment_status.value,
                "lifecycle_status": pattern.lifecycle_status.value,
                "cadence": pattern.cadence.value,
                "planning_amount": str(pattern.planning_amount) if pattern.planning_amount is not None else None,
                "reserve_eligible": pattern.reserve_eligible,
                "monthly_reserve_contrib": str(pattern.monthly_reserve_contrib),
            },
            "effective_result": {
                "recurrence_status": effective.recurrence_status.value,
                "commitment_status": effective.commitment_status.value,
                "lifecycle_status": effective.lifecycle_status.value,
                "cadence": effective.cadence.value,
                "planning_amount": str(effective.planning_amount) if effective.planning_amount is not None else None,
                "reserve_eligible": effective.reserve_eligible,
                "monthly_reserve_contrib": str(effective.monthly_reserve_contrib),
                "decision_source": effective.decision_source.value,
            },
        })

    # --- Phase 3: fail-closed validation ---
    validated_groups: set[tuple] = set()
    conflicts: list[str] = []
    for ov in overrides:
        if ov.expected_match_count is None:
            continue
        gk = _override_group_key(ov)
        if gk in validated_groups:
            continue
        validated_groups.add(gk)
        actual = group_match_counts.get(gk, 0)
        if actual != ov.expected_match_count:
            conflicts.append(
                f"  override key=({ov.description_key!r}, hint={ov.stream_label_hint!r}, "
                f"label_exact={ov.label_exact!r}, amount={ov.amount_hint}): "
                f"expected {ov.expected_match_count} match(es), got {actual}"
            )
    if conflicts:
        raise FamilyReviewMappingConflict(
            "Family Review override mapping conflict(s) detected — "
            "description_key, stream_label_hint, label_exact, or amount_hint may be stale:\n"
            + "\n".join(conflicts)
        )

    return tuple(updated), tuple(applied), audit


# ═══════════════════════════════════════════════════════════════════════════
# AGGREGATION
# ═══════════════════════════════════════════════════════════════════════════

def compute_monthly_reserve(patterns: tuple[PatternResult, ...]) -> Decimal:
    # Deduplicate by canonical_identity: multi-stream items share one economic
    # commitment; only the first eligible stream per identity is counted.
    seen_canonical: set[str] = set()
    total = Decimal("0")
    for p in patterns:
        if not p.reserve_eligible:
            continue
        if p.canonical_identity is not None:
            if p.canonical_identity in seen_canonical:
                continue
            seen_canonical.add(p.canonical_identity)
        total += p.monthly_reserve_contrib
    return quantize_ils(total)


def compute_planning_income(income_streams: tuple[IncomeStreamResult, ...]) -> Decimal:
    """Sum planning_baseline for RELIABLE streams (SALARY + GOVERNMENT_BENEFIT)."""
    from intelligence.v4_contracts import IncomeType, ReliabilityStatus
    return quantize_ils(sum(
        (s.planning_baseline for s in income_streams
         if s.reliability_status == ReliabilityStatus.RELIABLE
         and s.income_type not in (IncomeType.FAMILY_TRANSFER, IncomeType.BONUS)),
        Decimal("0"),
    ))


# ═══════════════════════════════════════════════════════════════════════════
# MAIN ANALYSIS RUNNER
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class ReviewedTargets:
    """
    Family Review ground-truth targets for exact reconciliation.
    Injected as input to the analysis; not derived from transactions.
    """
    planning_income: Decimal     # e.g. Decimal("31659.50")
    monthly_reserve: Decimal     # e.g. Decimal("15395.99")


def run_analysis(
    db_path: str,
    user_id: Optional[int] = None,
    reviewed_targets: Optional[ReviewedTargets] = None,
    pattern_overrides: Optional[list[PatternOverride]] = None,
    income_baselines: Optional[dict[str, Decimal]] = None,
) -> tuple[ClassificationReport, list[SettlementRecord]]:
    """
    Full V4 read-only analysis pipeline.

    Opens the DB read-only, classifies, applies overrides, reconciles.
    Returns (ClassificationReport, settlements).
    No DB writes occur.
    """
    if pattern_overrides is None:
        pattern_overrides = []
    if income_baselines is None:
        income_baselines = {}

    conn = open_readonly_db(db_path)
    try:
        expense_rows = _load_expenses(conn, user_id)
        income_rows = _load_income(conn, user_id)
    finally:
        conn.close()

    # — RAW classification —
    raw_patterns, settlements = classify_expenses(expense_rows)
    raw_income = classify_income(income_rows, reviewed_baselines=income_baselines)

    raw_planning_income = compute_planning_income(tuple(raw_income))
    raw_monthly_reserve = compute_monthly_reserve(tuple(raw_patterns))

    raw_review_items = tuple(
        p.label for p in raw_patterns if p.family_review_required
    )

    raw = RawClassifierOutput(
        patterns=tuple(raw_patterns),
        income_streams=tuple(raw_income),
        planning_income_raw=raw_planning_income,
        monthly_reserve_raw=raw_monthly_reserve,
        family_review_items=raw_review_items,
    )

    # — EFFECTIVE result (after overrides) —
    eff_patterns, applied_overrides, override_audit = apply_overrides(raw.patterns, pattern_overrides)
    eff_income = raw.income_streams  # income overrides via income_baselines already applied

    eff_planning_income = compute_planning_income(eff_income)
    eff_monthly_reserve = compute_monthly_reserve(eff_patterns)
    eff_review_items = tuple(p.label for p in eff_patterns if p.family_review_required)

    effective = EffectiveFinancialResult(
        patterns=eff_patterns,
        income_streams=eff_income,
        planning_income_effective=eff_planning_income,
        monthly_reserve_effective=eff_monthly_reserve,
        family_review_items=eff_review_items,
        overrides_applied=applied_overrides,
    )

    # — RECONCILIATION —
    if reviewed_targets:
        income_rec = make_reconciliation_record(
            "planning_income",
            reviewed_targets.planning_income,
            eff_planning_income,
            raw_planning_income,
        )
        reserve_rec = make_reconciliation_record(
            "monthly_reserve",
            reviewed_targets.monthly_reserve,
            eff_monthly_reserve,
            raw_monthly_reserve,
        )
    else:
        # No targets → synthetic MATCH placeholder
        income_rec = make_reconciliation_record(
            "planning_income",
            eff_planning_income, eff_planning_income, raw_planning_income,
        )
        reserve_rec = make_reconciliation_record(
            "monthly_reserve",
            eff_monthly_reserve, eff_monthly_reserve, raw_monthly_reserve,
        )

    reconciliation = ReconciliationReport(
        planning_income=income_rec,
        monthly_reserve=reserve_rec,
    )

    report = ClassificationReport(
        classifier_version=CLASSIFIER_VERSION,
        run_id=str(uuid.uuid4()),
        analysis_db=db_path,
        run_at=datetime.utcnow().isoformat() + "Z",
        raw=raw,
        effective=effective,
        reconciliation=reconciliation,
        override_audit=tuple(override_audit),
    )

    return report, settlements


# ═══════════════════════════════════════════════════════════════════════════
# SERIALIZATION
# ═══════════════════════════════════════════════════════════════════════════

def _decimal_default(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


def _pattern_to_dict(p: PatternResult) -> dict:
    return {
        "description_key": p.description_key,
        "label": p.label,
        "recurrence_status": p.recurrence_status.value,
        "commitment_status": p.commitment_status.value,
        "amount_behavior": p.amount_behavior.value,
        "budget_class": p.budget_class.value,
        "lifecycle_status": p.lifecycle_status.value,
        "purpose_type": p.purpose_type.value,
        "cadence": p.cadence.value,
        "planning_amount": str(p.planning_amount) if p.planning_amount is not None else None,
        "member_count": len(p.member_ids),
        "member_ids": list(p.member_ids),
        "evidence_sources": list(p.evidence_sources),
        "decision_source": p.decision_source.value,
        "family_review_required": p.family_review_required,
        "review_reasons": [r.value for r in p.review_reasons],
        "reserve_eligible": p.reserve_eligible,
        "monthly_reserve_contrib": str(p.monthly_reserve_contrib),
    }


def _income_to_dict(s: IncomeStreamResult) -> dict:
    return {
        "stream_key": s.stream_key,
        "person": s.person,
        "source": s.source,
        "description_key": s.description_key,
        "income_type": s.income_type.value,
        "recurrence_status": s.recurrence_status.value,
        "reliability_status": s.reliability_status.value,
        "amount_behavior": s.amount_behavior.value,
        "cadence": s.cadence.value,
        "planning_baseline": str(s.planning_baseline),
        "member_count": len(s.member_ids),
        "member_ids": list(s.member_ids),
        "evidence_sources": list(s.evidence_sources),
        "decision_source": s.decision_source.value,
        "family_review_required": s.family_review_required,
        "review_reasons": [r.value for r in s.review_reasons],
    }


def _settlement_to_dict(s: SettlementRecord) -> dict:
    return {
        "description_key": s.description_key,
        "label": s.label,
        "member_count": len(s.member_ids),
        "total_amount": str(s.total_amount),
        "months": list(s.months),
        "evidence_sources": list(s.evidence_sources),
        "note": "SETTLEMENT: economic expense contribution = 0",
    }


def _rec_record_to_dict(r) -> dict:
    return {
        "field": r.field,
        "reviewed_value": str(r.reviewed_value),
        "derived_value": str(r.derived_value),
        "raw_derived_value": str(r.raw_derived_value),
        "difference": str(r.difference),
        "status": r.status,
        "conflict_report": r.conflict_report,
    }


def report_to_json(
    report: ClassificationReport,
    settlements: list[SettlementRecord],
) -> str:
    effective_reserve = report.effective.monthly_reserve_effective
    effective_income = report.effective.planning_income_effective
    flexible = quantize_ils(effective_income - effective_reserve)

    # Patterns that are COMMITTED+RECURRING+ACTIVE but amount=TBD (planning_amount=None).
    # These represent a genuine UNKNOWN contribution to the reserve — not zero.
    # Patterns sharing the same canonical_identity are deduplicated for count purposes
    # (e.g. two Google Cloud raw description_keys → one canonical TBD commitment).
    tbd_reserve_patterns_raw = [
        p for p in report.effective.patterns
        if (not p.reserve_eligible
            and p.planning_amount is None
            and p.commitment_status.value == "COMMITTED"
            and p.lifecycle_status.value == "ACTIVE"
            and p.recurrence_status.value == "RECURRING")
    ]
    # Deduplicate by canonical_identity; patterns without one are each counted separately.
    seen_canonical: set[str] = set()
    tbd_reserve_patterns: list = []
    for p in tbd_reserve_patterns_raw:
        if p.canonical_identity is not None:
            if p.canonical_identity in seen_canonical:
                continue
            seen_canonical.add(p.canonical_identity)
        tbd_reserve_patterns.append(p)
    reserve_is_lower_bound = len(tbd_reserve_patterns) > 0

    payload = {
        "metadata": {
            "classifier_version": report.classifier_version,
            "run_id": report.run_id,
            "analysis_db": report.analysis_db,
            "run_at": report.run_at,
        },
        "raw_patterns": [_pattern_to_dict(p) for p in report.raw.patterns],
        "effective_patterns": [_pattern_to_dict(p) for p in report.effective.patterns],
        "income_streams": [_income_to_dict(s) for s in report.effective.income_streams],
        "settlements": [_settlement_to_dict(s) for s in settlements],
        "reviews": {
            "items": list(report.effective.family_review_items),
            "count": len(report.effective.family_review_items),
        },
        "aggregates": {
            "effective_planning_income": str(effective_income),
            "effective_monthly_reserve": str(effective_reserve),
            "effective_monthly_reserve_note": (
                "LOWER_BOUND: excludes TBD-amount committed patterns" if reserve_is_lower_bound
                else "EXACT"
            ),
            "effective_flexible": str(flexible),
            "raw_planning_income": str(report.raw.planning_income_raw),
            "raw_monthly_reserve": str(report.raw.monthly_reserve_raw),
            "overrides_applied": list(report.effective.overrides_applied),
            "pattern_count": len(report.effective.patterns),
            "income_stream_count": len(report.effective.income_streams),
            "settlement_count": len(settlements),
            "reserve_eligible_count": sum(
                1 for p in report.effective.patterns if p.reserve_eligible
            ),
            "reserve_tbd_count": len(tbd_reserve_patterns),
            "reserve_is_lower_bound": reserve_is_lower_bound,
        },
        "reconciliation": {
            "planning_income": _rec_record_to_dict(report.reconciliation.planning_income),
            "monthly_reserve": _rec_record_to_dict(report.reconciliation.monthly_reserve),
        },
        "override_audit": list(report.override_audit),
        "reserve_tbd_patterns": [
            {
                "label": p.label,
                "description_key": p.description_key,
                "commitment_status": p.commitment_status.value,
                "recurrence_status": p.recurrence_status.value,
                "lifecycle_status": p.lifecycle_status.value,
                "planning_amount": None,
                "monthly_reserve_contrib": "UNKNOWN",
                "note": "Committed recurring obligation; amount not yet reviewed. Reserve total is a lower bound.",
            }
            for p in tbd_reserve_patterns
        ],
    }
    return json.dumps(payload, ensure_ascii=False, indent=2, default=_decimal_default)


def report_to_markdown(
    report: ClassificationReport,
    settlements: list[SettlementRecord],
) -> str:
    eff = report.effective
    rec = report.reconciliation
    income_status = "✅ MATCH" if rec.planning_income.status == "MATCH" else f"❌ CONFLICT (diff={rec.planning_income.difference})"
    reserve_status = "✅ MATCH" if rec.monthly_reserve.status == "MATCH" else f"❌ CONFLICT (diff={rec.monthly_reserve.difference})"
    flexible = quantize_ils(eff.planning_income_effective - eff.monthly_reserve_effective)

    lines: list[str] = []
    lines.append(f"# V4 Cashflow Analysis — {report.run_at[:10]}")
    lines.append(f"\n**Classifier:** {report.classifier_version} | **Run:** {report.run_id[:8]}")
    lines.append(f"\n## Aggregates\n")
    lines.append(f"| Field | Effective | Raw |")
    lines.append(f"|-------|-----------|-----|")
    lines.append(f"| Planning income | ₪{eff.planning_income_effective} | ₪{report.raw.planning_income_raw} |")
    lines.append(f"| Monthly reserve | ₪{eff.monthly_reserve_effective} | ₪{report.raw.monthly_reserve_raw} |")
    lines.append(f"| Flexible budget | ₪{flexible} | — |")
    lines.append(f"\n## Reconciliation\n")
    lines.append(f"- Planning income: {income_status}")
    lines.append(f"- Monthly reserve: {reserve_status}")

    lines.append(f"\n## Income Streams ({len(eff.income_streams)})\n")
    lines.append("| Person | Type | Reliability | Baseline | Cadence |")
    lines.append("|--------|------|-------------|----------|---------|")
    for s in eff.income_streams:
        lines.append(f"| {s.person} | {s.income_type.value} | {s.reliability_status.value} "
                     f"| ₪{s.planning_baseline} | {s.cadence.value} |")

    _tbd_raw_md = [
        p for p in eff.patterns
        if (not p.reserve_eligible and p.planning_amount is None
            and p.commitment_status.value == "COMMITTED"
            and p.lifecycle_status.value == "ACTIVE"
            and p.recurrence_status.value == "RECURRING")
    ]
    _seen_md: set[str] = set()
    tbd_reserve = []
    for _p in _tbd_raw_md:
        if _p.canonical_identity is not None:
            if _p.canonical_identity in _seen_md:
                continue
            _seen_md.add(_p.canonical_identity)
        tbd_reserve.append(_p)
    reserve_note = " *(LOWER BOUND — TBD patterns excluded)*" if tbd_reserve else ""

    lines.append(f"\n## Reserve-Eligible Patterns{reserve_note}\n")
    reserve_patterns = [p for p in eff.patterns if p.reserve_eligible]
    lines.append("| Label | Amount | Cadence | Monthly | Source |")
    lines.append("|-------|--------|---------|---------|--------|")
    for p in reserve_patterns:
        src = "FAMILY_REVIEW" if p.decision_source.value == "FAMILY_REVIEW" else "CLASSIFIER"
        lines.append(f"| {p.label} | ₪{p.planning_amount} | {p.cadence.value} "
                     f"| ₪{p.monthly_reserve_contrib} | {src} |")

    if tbd_reserve:
        lines.append(f"\n## Reserve TBD Patterns (amount UNKNOWN — not included in reserve total)\n")
        lines.append("| Label | Commitment | Lifecycle | Note |")
        lines.append("|-------|------------|-----------|------|")
        for p in tbd_reserve:
            lines.append(f"| {p.label} | {p.commitment_status.value} "
                         f"| {p.lifecycle_status.value} | amount not yet reviewed |")

    lines.append(f"\n## Patterns Requiring Review ({len(eff.family_review_items)})\n")
    for item in eff.family_review_items:
        lines.append(f"- {item}")

    lines.append(f"\n## All Patterns ({len(eff.patterns)})\n")
    lines.append("| Label | Recurrence | Commitment | Budget Class | Lifecycle | Amount | Review |")
    lines.append("|-------|------------|------------|--------------|-----------|--------|--------|")
    for p in sorted(eff.patterns, key=lambda x: x.label):
        review = "⚠️" if p.family_review_required else ""
        lines.append(
            f"| {p.label} | {p.recurrence_status.value} | {p.commitment_status.value} "
            f"| {p.budget_class.value} | {p.lifecycle_status.value} "
            f"| ₪{p.planning_amount or '?'} | {review} |"
        )

    if report.override_audit:
        lines.append(f"\n## Override Audit — Family Review Decisions ({len(report.override_audit)} patterns)\n")
        lines.append("Each entry shows: classifier raw result → override fields → effective result.\n")
        for entry in report.override_audit:
            lines.append(f"### {entry['label']}")
            cr = entry['classifier_result']
            er = entry['effective_result']
            lines.append(f"\n**Classifier result:** rec={cr['recurrence_status']} "
                         f"com={cr['commitment_status']} lc={cr['lifecycle_status']} "
                         f"amount={cr['planning_amount'] or 'UNKNOWN'} "
                         f"reserve={cr['reserve_eligible']}")
            if entry['changed_fields']:
                lines.append(f"\n**Overrides applied:** {', '.join(entry['override_ids_applied'])}")
                for field, change in entry['changed_fields'].items():
                    lines.append(f"  - `{field}`: `{change['classifier']}` → `{change['override']}`")
            lines.append(f"\n**Effective result:** rec={er['recurrence_status']} "
                         f"com={er['commitment_status']} lc={er['lifecycle_status']} "
                         f"amount={er['planning_amount'] or 'UNKNOWN'} "
                         f"reserve={er['reserve_eligible']} "
                         f"monthly={er['monthly_reserve_contrib']} "
                         f"source={er['decision_source']}\n")

    lines.append(f"\n## Settlements ({len(settlements)})\n")
    for s in settlements:
        lines.append(f"- **{s.label}** — ₪{s.total_amount} across {len(s.months)} months *(economic contribution = 0)*")

    return "\n".join(lines)
