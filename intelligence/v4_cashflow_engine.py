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
    IncomeStreamResult, PatternResult, RawClassifierOutput,
    ReconciliationReport, ReviewReason,
    make_reconciliation_record, quantize_ils,
)

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
    """
    description_key: str     # matches PatternResult.description_key
    stream_label_hint: str   # optional label fragment to distinguish parallel streams
    field_name: str          # field on PatternResult to override
    value: object            # new value (must be the correct type)
    override_id: str         # human-readable ID for audit
    source: DecisionSource = DecisionSource.FAMILY_REVIEW


def apply_overrides(
    patterns: tuple[PatternResult, ...],
    overrides: list[PatternOverride],
) -> tuple[tuple[PatternResult, ...], tuple[str, ...]]:
    """
    Apply in-memory overrides to raw patterns.
    Returns (updated_patterns, applied_override_ids).

    Override matching: description_key must match exactly.
    If stream_label_hint is non-empty, the pattern label must contain it.
    """
    applied: list[str] = []
    updated: list[PatternResult] = []

    for pattern in patterns:
        relevant = [
            ov for ov in overrides
            if ov.description_key == pattern.description_key
            and (not ov.stream_label_hint or ov.stream_label_hint in pattern.label)
        ]
        if not relevant:
            updated.append(pattern)
            continue

        # Apply overrides field by field using dataclass replacement
        kwargs = {
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
        }
        for ov in relevant:
            if ov.field_name in kwargs:
                kwargs[ov.field_name] = ov.value
                applied.append(ov.override_id)
        # Override authority: at least one override applied → FAMILY_REVIEW
        kwargs["decision_source"] = DecisionSource.FAMILY_REVIEW

        # Recompute derived fields after overrides
        from intelligence.v4_contracts import (
            derive_budget_class, is_reserve_eligible,
            CADENCE_OCCURRENCES_PER_YEAR, monthly_equivalent,
        )
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

        updated.append(PatternResult(**kwargs))

    return tuple(updated), tuple(applied)


# ═══════════════════════════════════════════════════════════════════════════
# AGGREGATION
# ═══════════════════════════════════════════════════════════════════════════

def compute_monthly_reserve(patterns: tuple[PatternResult, ...]) -> Decimal:
    return quantize_ils(sum(
        (p.monthly_reserve_contrib for p in patterns if p.reserve_eligible),
        Decimal("0"),
    ))


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
    eff_patterns, applied_overrides = apply_overrides(raw.patterns, pattern_overrides)
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
        },
        "reconciliation": {
            "planning_income": _rec_record_to_dict(report.reconciliation.planning_income),
            "monthly_reserve": _rec_record_to_dict(report.reconciliation.monthly_reserve),
        },
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

    lines.append(f"\n## Reserve-Eligible Patterns\n")
    reserve_patterns = [p for p in eff.patterns if p.reserve_eligible]
    lines.append("| Label | Amount | Cadence | Monthly |")
    lines.append("|-------|--------|---------|---------|")
    for p in reserve_patterns:
        lines.append(f"| {p.label} | ₪{p.planning_amount} | {p.cadence.value} "
                     f"| ₪{p.monthly_reserve_contrib} |")

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

    lines.append(f"\n## Settlements ({len(settlements)})\n")
    for s in settlements:
        lines.append(f"- **{s.label}** — ₪{s.total_amount} across {len(s.months)} months *(economic contribution = 0)*")

    return "\n".join(lines)
