"""
analyze_home_budget_v4.py — Phase B read-only analysis CLI.

Usage:
  python analyze_home_budget_v4.py --db /path/to/budget.db [--user-id N]
  python analyze_home_budget_v4.py --db /path/to/budget.db --test-only

Options:
  --db PATH         Path to SQLite budget database (required)
  --user-id N       Analyze only this user ID (default: all users)
  --test-only       Validate DB connectivity and read-only access; do not run
                    full analysis (use this first on Railway)
  --out-dir DIR     Directory for output files (default: /tmp)
  --no-reviewed     Skip family-reviewed overrides/targets (raw classifier only)

Outputs (unless --test-only):
  {out_dir}/home_budget_v4_analysis.json
  {out_dir}/home_budget_v4_analysis.md

SAFETY CONTRACT:
  - Opens SQLite with mode=ro URI — no writes possible at the engine level
  - PRAGMA query_only = ON enforced inside open_readonly_db()
  - No INSERT/UPDATE/DELETE/ALTER/CREATE/DROP appears in this file or
    v4_cashflow_engine.py or v4_classifier.py

SHA256 of this file can be verified with:
  python -c "import hashlib,sys; print(hashlib.sha256(open(sys.argv[1],'rb').read()).hexdigest())" analyze_home_budget_v4.py
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from decimal import Decimal
from pathlib import Path


# ═══════════════════════════════════════════════════════════════════════════
# FAMILY-REVIEWED GROUND TRUTH (in-memory, not persisted)
#
# These values represent the Family Review decisions.
# They are NOT derived from the classifier; they are injected as known truth.
# The classifier is expected to reconcile against these targets.
#
# Income baselines keyed by stream_key (hash of person|source|norm_desc).
# PatternOverrides listed explicitly for known review decisions.
# ═══════════════════════════════════════════════════════════════════════════

from intelligence.v4_contracts import (
    Cadence, CommitmentStatus, DecisionSource, IncomeType, LifecycleStatus,
    PurposeType, RecurrenceStatus, ReliabilityStatus,
    make_income_stream_key,
)
from intelligence.v4_cashflow_engine import (
    PatternOverride, ReviewedTargets,
    open_readonly_db, run_analysis, report_to_json, report_to_markdown,
)

REVIEWED_TARGETS = ReviewedTargets(
    planning_income=Decimal("31659.50"),
    monthly_reserve=Decimal("15395.99"),
)

# Income stream baselines (Family Review ground truth)
# Keys computed from (person, source, norm_desc) — verified against runtime DB output.
# norm_desc is normalize_description(raw_description) from the actual income rows.
_INCOME_BASELINES_RAW: list[tuple[str, str, str, Decimal]] = [
    ("wife",   "salary",        "בנק לאומי משכורת",   Decimal("16623.00")),
    ("husband","salary",        "בנק פועלים משכורת",  Decimal("14446.00")),
    ("family", "child_allowance","ביטוח לאומי - ילדים", Decimal("590.50")),
    # family support and bonuses: planning_baseline = 0 by policy
]

INCOME_BASELINES: dict[str, Decimal] = {
    make_income_stream_key(person, source, desc): baseline
    for person, source, desc, baseline in _INCOME_BASELINES_RAW
}

# Pattern-level overrides for known Family Review decisions.
# description_key = normalize_description(raw_description) — verified against runtime output.
# stream_label_hint = substring that must appear in pattern.label (empty = any stream).
PATTERN_OVERRIDES: list[PatternOverride] = [

    # ══════════════════════════════════════════════════════════════════════
    # CONFIRMED RUNTIME description_keys (verified against normalize_description()
    # output from a real-data run on the budget DB).
    # ══════════════════════════════════════════════════════════════════════

    # ── Gal Naomi (הוק לגל נעמי לסניף 17-662) ────────────────────────────
    # Two parallel debt-repayment streams (~607 and ~2000). Both active recurring
    # committed. No amount override — classifier detects per-stream amounts.
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",   # applies to all streams (both are active LOAN)
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-gal-naomi-recurrence",
        expected_match_count=None,  # 2 streams expected, but don't hard-fail on count
    ),
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-gal-naomi-committed",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-gal-naomi-active",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.LOAN,
        override_id="ov-gal-naomi-purpose",
        expected_match_count=None,
    ),

    # ── Arnona (מ.א. חוף ה חיוב) ──────────────────────────────────────────
    # Every-2-months, planning_amount=886.10, monthly reserve=443.05
    PatternOverride(
        description_key="מ.א. חוף ה חיוב",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.EVERY_2_MONTHS,
        override_id="ov-arnona-cadence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מ.א. חוף ה חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("886.10"),
        override_id="ov-arnona-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מ.א. חוף ה חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-arnona-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מ.א. חוף ה חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-arnona-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מ.א. חוף ה חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-arnona-active",
        expected_match_count=1,
    ),

    # ── Harel Loan (הראלהלואה חיוב) ──────────────────────────────────────
    PatternOverride(
        description_key="הראלהלואה חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("390.80"),
        override_id="ov-harel-loan-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הראלהלואה חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-harel-loan-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הראלהלואה חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-harel-loan-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הראלהלואה חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-harel-loan-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הראלהלואה חיוב",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.LOAN,
        override_id="ov-harel-loan-purpose",
        expected_match_count=1,
    ),

    # ── Hiyuvei Halo (חיובי הלוו חיוב) — TWO STREAMS, active 222.12 only ─
    # The active stream has planning_amount=222.12 (verified).
    # The inactive stream has a different classifier amount; it is NOT overridden
    # here and keeps the classifier's lifecycle (ENDED / POSSIBLY_STOPPED).
    # amount_hint discriminates between the two streams without fuzzy matching.
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("222.12"),
        override_id="ov-hiyuvei-halo-amount",
        expected_match_count=1,
        amount_hint=Decimal("222.12"),
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-hiyuvei-halo-committed",
        expected_match_count=1,
        amount_hint=Decimal("222.12"),
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-hiyuvei-halo-recurrence",
        expected_match_count=1,
        amount_hint=Decimal("222.12"),
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-hiyuvei-halo-active",
        expected_match_count=1,
        amount_hint=Decimal("222.12"),
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.LOAN,
        override_id="ov-hiyuvei-halo-purpose",
        expected_match_count=1,
        amount_hint=Decimal("222.12"),
    ),

    # ── Harel Insurance (הראל בטוח חיוב) — two parallel streams ─────────
    # Stream 1 (lower, ~231.35, no label suffix): recurrence/commitment/lifecycle
    # Stream 2 (higher, 346.12, label contains "stream 2"): all + amount override
    # Broad overrides (no hint) apply to BOTH streams.
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-harel-ins-recurrence",
        expected_match_count=None,  # 2 streams; don't hard-fail
    ),
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-harel-ins-committed",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-harel-ins-active",
        expected_match_count=None,
    ),
    # Stream 2 specific — amount + commitment
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="stream 2",
        field_name="planning_amount",
        value=Decimal("346.12"),
        override_id="ov-harel-ins-amount-stream2",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="stream 2",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-harel-ins-committed-stream2",
        expected_match_count=1,
    ),

    # ── Mei Hof HaKarmel (מי חוף הכרמל) — every-2-months, variable ───────
    # Reviewed: 410.99 every 2 months (observed median).
    # monthly_equivalent(410.99, EVERY_2_MONTHS) = 410.99 * 6/12 = 205.495 → 205.50
    PatternOverride(
        description_key="מי חוף הכרמל",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.EVERY_2_MONTHS,
        override_id="ov-mei-hof-cadence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מי חוף הכרמל",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("410.99"),
        override_id="ov-mei-hof-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מי חוף הכרמל",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-mei-hof-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מי חוף הכרמל",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-mei-hof-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מי חוף הכרמל",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-mei-hof-active",
        expected_match_count=1,
    ),

    # ── Mishki Ram (משקי רם — normalized from משקי רם בע"מ) ──────────────
    PatternOverride(
        description_key="משקי רם",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("787.61"),
        override_id="ov-mishki-ram-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="משקי רם",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-mishki-ram-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="משקי רם",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-mishki-ram-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="משקי רם",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-mishki-ram-active",
        expected_match_count=1,
    ),

    # ── Training Fund (השתלמות אג חיוב) — SAVINGS_INVESTMENT, reserve-eligible ─
    # PROVEN runtime key: "השתלמות אג חיוב" (was wrong: "קרן השתלמות").
    # planning_amount=137.59; SAVINGS_INVESTMENT purpose override.
    PatternOverride(
        description_key="השתלמות אג חיוב",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.SAVINGS_INVESTMENT,
        override_id="ov-training-fund-purpose",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="השתלמות אג חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("137.59"),
        override_id="ov-training-fund-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="השתלמות אג חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-training-fund-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="השתלמות אג חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-training-fund-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="השתלמות אג חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-training-fund-active",
        expected_match_count=1,
    ),

    # ── Efrat Rosenberg (אפרת רוזנברג בריאות וכושר) — TWO STREAMS ────────
    # PROVEN runtime key: "אפרת רוזנברג בריאות וכושר" (was: "אפרת רוזנברג").
    # Historical stream 0: 300.00 (ended) — NO override; classifier keeps ENDED lifecycle.
    # Active stream 1 ("stream 2"): 350.00 — RECURRING + COMMITTED + ACTIVE + 350.
    PatternOverride(
        description_key="אפרת רוזנברג בריאות וכושר",
        stream_label_hint="stream 2",
        field_name="planning_amount",
        value=Decimal("350.00"),
        override_id="ov-efrat-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="אפרת רוזנברג בריאות וכושר",
        stream_label_hint="stream 2",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-efrat-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="אפרת רוזנברג בריאות וכושר",
        stream_label_hint="stream 2",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-efrat-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="אפרת רוזנברג בריאות וכושר",
        stream_label_hint="stream 2",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-efrat-active",
        expected_match_count=1,
    ),

    # ── Cancelled / Ended ─────────────────────────────────────────────────
    # Noy Lenz — confirmed matched in runtime
    PatternOverride(
        description_key="נוי לנץ",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ENDED,
        override_id="ov-noy-lenz-ended",
        expected_match_count=None,  # may not appear in all DB snapshots
    ),
    # STP — cancelled service
    PatternOverride(
        description_key="STP",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.CANCELLED,
        override_id="ov-stp-cancelled",
        expected_match_count=None,  # may not appear in all DB snapshots
    ),

    # ── Google Cloud — TWO raw description_keys, ONE canonical TBD commitment ─
    # PROVEN runtime keys: "GOOGLE*CLOUD LN7KQW" and "GOOGLE*CLOUD TLBZ7J".
    # Both represent the same reviewed commitment; canonical_identity deduplicates them
    # so they count as ONE TBD in the reserve report (not two).
    PatternOverride(
        description_key="GOOGLE*CLOUD LN7KQW",
        stream_label_hint="",
        field_name="planning_amount",
        value=None,
        override_id="ov-google-cloud-lnkqw-tbd",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD LN7KQW",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-google-cloud-lnkqw-recurrence",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD LN7KQW",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-google-cloud-lnkqw-committed",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD LN7KQW",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-google-cloud-lnkqw-active",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD TLBZ7J",
        stream_label_hint="",
        field_name="planning_amount",
        value=None,
        override_id="ov-google-cloud-tlbz7j-tbd",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD TLBZ7J",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-google-cloud-tlbz7j-recurrence",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD TLBZ7J",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-google-cloud-tlbz7j-committed",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),
    PatternOverride(
        description_key="GOOGLE*CLOUD TLBZ7J",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-google-cloud-tlbz7j-active",
        expected_match_count=1,
        canonical_identity="google-cloud-tbd",
    ),

    # ── Mor Gemel (מור גמל ופ חיוב) — TBD savings ────────────────────────
    # PROVEN runtime key: "מור גמל ופ חיוב" (was: "מור גמל").
    # Classifier may produce ~90.08 from limited data.
    # Family Review: planning_amount=None (TBD) — do NOT use classifier's amount.
    PatternOverride(
        description_key="מור גמל ופ חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=None,
        override_id="ov-mor-gemel-tbd",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מור גמל ופ חיוב",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.SAVINGS_INVESTMENT,
        override_id="ov-mor-gemel-purpose",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מור גמל ופ חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-mor-gemel-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מור גמל ופ חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-mor-gemel-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="מור גמל ופ חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-mor-gemel-active",
        expected_match_count=1,
    ),

    # ── Space Gym (ספייס מועדוני כושר - טירת הכרמל הו"ק) ────────────────
    # PROVEN runtime key: "ספייס מועדוני כושר - טירת הכרמל הו\"ק" (was: "SPACE GYM").
    # Family Review: recurring committed active, 149.00/month.
    PatternOverride(
        description_key='ספייס מועדוני כושר - טירת הכרמל הו"ק',
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-space-gym-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key='ספייס מועדוני כושר - טירת הכרמל הו"ק',
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-space-gym-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key='ספייס מועדוני כושר - טירת הכרמל הו"ק',
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-space-gym-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key='ספייס מועדוני כושר - טירת הכרמל הו"ק',
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("149.00"),
        override_id="ov-space-gym-amount",
        expected_match_count=1,
    ),

    # ── Local committee (הוק לועד מקומי החותר לסניף 12-703) — TWO STREAMS ─
    # PROVEN runtime key: "הוק לועד מקומי החותר לסניף 12-703" (was: "ועד בית").
    # Historical stream 0: 629.50 (ended) — NO override; classifier keeps ENDED lifecycle.
    # Active stream 1 ("stream 2"): 743.64 — RECURRING + COMMITTED + ACTIVE + 743.64.
    PatternOverride(
        description_key="הוק לועד מקומי החותר לסניף 12-703",
        stream_label_hint="stream 2",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-vaad-bayit-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הוק לועד מקומי החותר לסניף 12-703",
        stream_label_hint="stream 2",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-vaad-bayit-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הוק לועד מקומי החותר לסניף 12-703",
        stream_label_hint="stream 2",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-vaad-bayit-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הוק לועד מקומי החותר לסניף 12-703",
        stream_label_hint="stream 2",
        field_name="planning_amount",
        value=Decimal("743.64"),
        override_id="ov-vaad-bayit-amount",
        expected_match_count=1,
    ),

    # ── Nursing insurance (סיעוד) ─────────────────────────────────────────
    # Family Review: recurring committed active, 128.23/month.
    PatternOverride(
        description_key="סיעוד",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-siyud-recurrence",
        expected_match_count=None,  # description_key not yet confirmed from runtime
    ),
    PatternOverride(
        description_key="סיעוד",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-siyud-committed",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="סיעוד",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-siyud-active",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="סיעוד",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("128.23"),
        override_id="ov-siyud-amount",
        expected_match_count=None,
    ),

    # ── Sewage (ביוב) ─────────────────────────────────────────────────────
    # Family Review: recurring committed active, 174/month.
    PatternOverride(
        description_key="ביוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-biyuv-recurrence",
        expected_match_count=None,  # description_key not yet confirmed from runtime
    ),
    PatternOverride(
        description_key="ביוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-biyuv-committed",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="ביוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-biyuv-active",
        expected_match_count=None,
    ),
    PatternOverride(
        description_key="ביוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("174.00"),
        override_id="ov-biyuv-amount",
        expected_match_count=None,
    ),

    # ── Pango / Moovit (מ. התחבורה - פנגו מוביט) — NON_COMMITTED ─────────
    # PROVEN runtime key: "מ. התחבורה - פנגו מוביט" (combined, was: separate "פנגו"/"מוביט").
    # Family Review: NON_COMMITTED transport. Reserve contribution = 0.
    PatternOverride(
        description_key="מ. התחבורה - פנגו מוביט",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.NON_COMMITTED,
        override_id="ov-pango-moovit-non-committed",
        expected_match_count=1,
    ),

    # ── Sports association (עמותת ספורט חוף הכרמל) — TBD ─────────────────
    # PROVEN runtime key: "עמותת ספורט חוף הכרמל" (was: "אגודת ספורט").
    # Family Review: recurring committed active, amount TBD.
    PatternOverride(
        description_key="עמותת ספורט חוף הכרמל",
        stream_label_hint="",
        field_name="planning_amount",
        value=None,
        override_id="ov-sports-assoc-tbd",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="עמותת ספורט חוף הכרמל",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-sports-assoc-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="עמותת ספורט חוף הכרמל",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-sports-assoc-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="עמותת ספורט חוף הכרמל",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-sports-assoc-active",
        expected_match_count=1,
    ),

    # ── Discount card fees (דמי כרטיס בנק דיסקונט) — 39.60/month total ──
    # PROVEN runtime key: "דמי כרטיס בנק דיסקונט" (was: "דמי כרטיס").
    # Family Review: two 19.80 charges per month = 39.60/month total.
    # All payments share the same description_key and same amount (19.80), so bimodal
    # split does NOT occur → one stream.
    # Cadence override = MONTHLY (classifier may detect BIWEEKLY from 2 payments/month).
    # planning_amount = 39.60 (total monthly, not per-charge).
    PatternOverride(
        description_key="דמי כרטיס בנק דיסקונט",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.FINANCIAL_FEE,
        override_id="ov-discount-fee-purpose",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="דמי כרטיס בנק דיסקונט",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.MONTHLY,
        override_id="ov-discount-fee-cadence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="דמי כרטיס בנק דיסקונט",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-discount-fee-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="דמי כרטיס בנק דיסקונט",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-discount-fee-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="דמי כרטיס בנק דיסקונט",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-discount-fee-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="דמי כרטיס בנק דיסקונט",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("39.60"),
        override_id="ov-discount-fee-amount",
        expected_match_count=1,
    ),

    # ── Ituran (איתוראן) — RECURRING + COMMITTED + ACTIVE ────────────────
    # No existing override. Family Review: recurring committed active, 74.01/month.
    PatternOverride(
        description_key="איתוראן",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-ituran-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="איתוראן",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-ituran-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="איתוראן",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-ituran-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="איתוראן",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("74.01"),
        override_id="ov-ituran-amount",
        expected_match_count=1,
    ),
]


# ═══════════════════════════════════════════════════════════════════════════
# TEST-ONLY MODE
# ═══════════════════════════════════════════════════════════════════════════

def run_test_only(db_path: str) -> int:
    """
    Validate DB connectivity and read-only mode.
    Returns exit code (0 = ok, 1 = error).
    """
    print(f"[test-only] Opening DB in read-only mode: {db_path}")
    try:
        conn = open_readonly_db(db_path)
    except Exception as e:
        print(f"[test-only] FAILED to open DB: {e}")
        return 1

    try:
        # Verify tables exist
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        print(f"[test-only] Tables found: {tables}")

        # Verify read-only (attempt write should fail)
        try:
            conn.execute("INSERT INTO expenses (date,category_id,description,amount,user_id) "
                         "VALUES ('2099-01-01','test','test',0,99999)")
            conn.commit()
            print("[test-only] CRITICAL: write succeeded — read-only not enforced!")
            return 1
        except Exception:
            print("[test-only] ✅ Write correctly rejected — read-only confirmed")

        # Row counts
        expense_count = conn.execute("SELECT COUNT(*) FROM expenses").fetchone()[0]
        income_count  = conn.execute("SELECT COUNT(*) FROM income").fetchone()[0]
        print(f"[test-only] expense rows: {expense_count}, income rows: {income_count}")
        print("[test-only] ✅ DB connectivity OK")
        return 0
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════

def self_sha256() -> str:
    path = Path(__file__).resolve()
    return hashlib.sha256(path.read_bytes()).hexdigest()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="V4 Home Budget Analysis (read-only)")
    p.add_argument("--db", required=True, help="Path to budget.db")
    p.add_argument("--user-id", type=int, default=None, dest="user_id")
    p.add_argument("--test-only", action="store_true", dest="test_only")
    p.add_argument("--out-dir", default="/tmp", dest="out_dir")
    p.add_argument("--no-reviewed", action="store_true", dest="no_reviewed",
                   help="Skip family-reviewed overrides; run raw classifier only")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db).resolve())

    print(f"V4 Classifier  | SHA256: {self_sha256()[:16]}…")
    print(f"DB path        | {db_path}")
    print(f"User filter    | {args.user_id or 'all'}")

    if args.test_only:
        return run_test_only(db_path)

    targets = REVIEWED_TARGETS if not args.no_reviewed else None
    overrides = PATTERN_OVERRIDES if not args.no_reviewed else []
    baselines = INCOME_BASELINES if not args.no_reviewed else {}

    print("Running V4 analysis …")
    report, settlements = run_analysis(
        db_path=db_path,
        user_id=args.user_id,
        reviewed_targets=targets,
        pattern_overrides=overrides,
        income_baselines=baselines,
    )

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "home_budget_v4_analysis.json"
    md_path   = out_dir / "home_budget_v4_analysis.md"

    json_path.write_text(report_to_json(report, settlements), encoding="utf-8")
    md_path.write_text(report_to_markdown(report, settlements), encoding="utf-8")

    print(f"JSON output    | {json_path}")
    print(f"MD output      | {md_path}")
    print(f"Patterns       | {len(report.effective.patterns)}")
    print(f"Income streams | {len(report.effective.income_streams)}")
    print(f"Settlements    | {len(settlements)}")
    print(f"Reserve        | ₪{report.effective.monthly_reserve_effective}")
    print(f"Income         | ₪{report.effective.planning_income_effective}")
    print(f"Income recon   | {report.reconciliation.planning_income.status}")
    print(f"Reserve recon  | {report.reconciliation.monthly_reserve.status}")

    rc_income = report.reconciliation.planning_income.status
    rc_reserve = report.reconciliation.monthly_reserve.status
    if rc_income == "CONFLICT" or rc_reserve == "CONFLICT":
        print("\n⚠  RECONCILIATION CONFLICTS detected — see JSON for details")
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
