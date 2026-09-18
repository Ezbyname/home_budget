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
    # Two parallel debt-repayment streams (reviewed: 607.00 and 2000.00).
    # Both active recurring committed. No amount override — classifier detects
    # per-stream amounts. expected_match_count=2: exactly two streams must match.
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",   # applies to all streams (both are active LOAN)
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-gal-naomi-recurrence",
        expected_match_count=2,
    ),
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-gal-naomi-committed",
        expected_match_count=2,
    ),
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-gal-naomi-active",
        expected_match_count=2,
    ),
    PatternOverride(
        description_key="הוק לגל נעמי לסניף 17-662",
        stream_label_hint="",
        field_name="purpose_type",
        value=PurposeType.LOAN,
        override_id="ov-gal-naomi-purpose",
        expected_match_count=2,
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

    # ── Hiyuvei Halo (חיובי הלוו חיוב) — TWO SEGMENTS, current only ────────
    # The classifier split one continuous loan into two segments when the per-payment
    # amount shifted slightly:
    #   Current segment  label="חיובי הלוו חיוב"           plan=221.15  ACTIVE
    #   Historical seg.  label="חיובי הלוו חיוב (stream 2)" plan=222.21  POSSIBLY_STOPPED
    # Both share description_key="חיובי הלוו חיוב".
    # Family Review: ONE active loan commitment, 222.12/month.
    # label_exact="חיובי הלוו חיוב" selects ONLY the current segment by exact label
    # equality, excluding the historical "(stream 2)" segment.
    # amount_hint is NOT used — classifier amounts (221.15 / 222.21) are observations,
    # not stable identity.
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        label_exact="חיובי הלוו חיוב",
        field_name="planning_amount",
        value=Decimal("222.12"),
        override_id="ov-hiyuvei-halo-amount",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        label_exact="חיובי הלוו חיוב",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-hiyuvei-halo-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        label_exact="חיובי הלוו חיוב",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-hiyuvei-halo-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        label_exact="חיובי הלוו חיוב",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-hiyuvei-halo-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="חיובי הלוו חיוב",
        stream_label_hint="",
        label_exact="חיובי הלוו חיוב",
        field_name="purpose_type",
        value=PurposeType.LOAN,
        override_id="ov-hiyuvei-halo-purpose",
        expected_match_count=1,
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
        expected_match_count=2,  # exactly 2 streams (stream 1 ~231.35, stream 2 346.12)
    ),
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-harel-ins-committed",
        expected_match_count=2,
    ),
    PatternOverride(
        description_key="הראל בטוח חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-harel-ins-active",
        expected_match_count=2,
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

    # ── Cancelled / Ended (defensive lifecycle guardrails) ───────────────
    # These differ from reserve commitment overrides: if the pattern is absent
    # from the DB, there is no pattern to mis-classify and no reserve risk.
    # expected_match_count=None is correct here — the override fires only when
    # the pattern is present; absence is not an error.
    # Noy Lenz — confirmed matched in runtime
    PatternOverride(
        description_key="נוי לנץ",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ENDED,
        override_id="ov-noy-lenz-ended",
        expected_match_count=None,  # defensive guardrail: absence = no risk
    ),
    # STP — cancelled service
    PatternOverride(
        description_key="STP",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.CANCELLED,
        override_id="ov-stp-cancelled",
        expected_match_count=None,  # defensive guardrail: absence = no risk
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

    # ── Nursing insurance (סעוד הראל -כללית) ────────────────────────────────
    # PROVEN runtime key: "סעוד הראל -כללית" (was wrong: "סיעוד").
    # Proven from prior Railway Phase B output: description_key="סעוד הראל -כללית",
    # planning_amount=128.23, cadence=MONTHLY.
    # normalize_description("סעוד הראל -כללית") == "סעוד הראל -כללית" (no transformation).
    PatternOverride(
        description_key="סעוד הראל -כללית",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-siyud-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="סעוד הראל -כללית",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-siyud-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="סעוד הראל -כללית",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-siyud-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="סעוד הראל -כללית",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("128.23"),
        override_id="ov-siyud-amount",
        expected_match_count=1,
    ),

    # ── Sewage (מועצה אזורית חוף הכרמל הו"ק) ────────────────────────────
    # PROVEN runtime key: 'מועצה אזורית חוף הכרמל הו"ק'
    # Proven from prior Railway PatternResult:
    #   label = description_key = 'מועצה אזורית חוף הכרמל הו"ק'
    #   planning_amount = 174.00, cadence = MONTHLY
    #   DB evidence: 2025-11-03 174.00, 2025-12-11 174.00 (member_ids 323, 229)
    # normalize_description('מועצה אזורית חוף הכרמל הו"ק') == itself (no transformation).
    # Classifier state was: POSSIBLE_RECURRING / UNCERTAIN / ENDED — NOT reserve-eligible.
    # Family Review is the higher authority; audit trail preserves classifier evidence.
    PatternOverride(
        description_key='מועצה אזורית חוף הכרמל הו"ק',
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-biyuv-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key='מועצה אזורית חוף הכרמל הו"ק',
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-biyuv-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key='מועצה אזורית חוף הכרמל הו"ק',
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-biyuv-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key='מועצה אזורית חוף הכרמל הו"ק',
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("174.00"),
        override_id="ov-biyuv-amount",
        expected_match_count=1,
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

    # ── Ituran (איתוראן איתור ושליטה הוראות קבע) ──────────────────────────
    # PROVEN runtime key: "איתוראן איתור ושליטה הוראות קבע".
    # Prior Railway output: description_key="איתוראן איתור ושליטה הוראות קבע",
    # label="איתוראן איתור ושליטה בע\"מ הוראות קבע", planning_amount=74.01.
    # normalize_description strips the legal suffix "בע\"מ":
    #   "איתוראן איתור ושליטה בע\"מ הוראות קבע" -> "איתוראן איתור ושליטה הוראות קבע"
    # Family Review: recurring committed active, 74.01/month.
    PatternOverride(
        description_key="איתוראן איתור ושליטה הוראות קבע",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-ituran-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="איתוראן איתור ושליטה הוראות קבע",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-ituran-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="איתוראן איתור ושליטה הוראות קבע",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-ituran-active",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="איתוראן איתור ושליטה הוראות קבע",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("74.01"),
        override_id="ov-ituran-amount",
        expected_match_count=1,
    ),

    # ── Legacy reserve preservation ──────────────────────────────────────────
    #
    # These six items were part of the previously accepted Family Review reserve
    # baseline.  The classifier detected them but either under-classified their
    # recurrence (POSSIBLE_RECURRING instead of RECURRING) or, for the mortgage,
    # derived a planning_amount that drifted from the reviewed value.
    #
    # Evidence source: Phase B Railway run against
    #   /tmp/home_budget_v4_migrate_home/.budget_tracker_data/budget.db
    # Raw transaction chronology inspected via expenses table (member IDs from
    # PatternResult) to confirm economic identity and amount evidence.
    #
    # Architecture:
    #   Single-stream items (Mortgage, Phoenix): plain field-level overrides,
    #     expected_match_count=1.
    #   Multi-stream items (Clal, Migdal, Menora, HOT): ALL classifier streams
    #     receive recurrence/commitment/planning_amount overrides and share the
    #     same canonical_identity.  compute_monthly_reserve deduplicates by
    #     canonical_identity so the economic commitment is counted exactly once.
    #     Raw classifier lifecycle evidence is preserved (overrides do NOT touch
    #     lifecycle_status), so streams the classifier marked ENDED or
    #     POSSIBLY_STOPPED remain non-reserve-eligible by is_reserve_eligible().
    #     expected_match_count validates cardinality fail-closed.

    # ── Mortgage: דסק-משכנתא חיוב — single stream ────────────────────────────
    # Classifier: RECURRING + COMMITTED + ACTIVE, amount=6635.37.
    # Transaction median (6 months): Decimal("6641.585") → ROUND_HALF_UP = 6641.59.
    # Override planning_amount only; all other axes already correct.
    PatternOverride(
        description_key="דסק-משכנתא חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("6641.59"),
        override_id="ov-mortgage-reviewed-amount",
        expected_match_count=1,
    ),

    # ── הפניקס חיים ובריאות — single stream ──────────────────────────────────
    # Classifier: POSSIBLE_RECURRING + COMMITTED + ACTIVE, amount=171.58.
    # Transaction median (6 observations): 171.67.
    # Fix: recurrence + commitment + lock planning_amount.
    PatternOverride(
        description_key="הפניקס חיים ובריאות",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-phoenix-recurrence",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הפניקס חיים ובריאות",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-phoenix-committed",
        expected_match_count=1,
    ),
    PatternOverride(
        description_key="הפניקס חיים ובריאות",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("171.67"),
        override_id="ov-phoenix-amount",
        expected_match_count=1,
    ),

    # ── כלל חיים/ב חיוב — canonical group, 2 streams ─────────────────────────
    # Transactions interleave month-by-month (Oct–Mar): one continuous economic
    # commitment split by classifier into two streams.
    # Stream 1: ACTIVE monthly; stream 2: POSSIBLY_STOPPED every_2_months.
    # Family Review declares: one ACTIVE MONTHLY commitment at 443.70/month.
    # All four reviewed fields (recurrence, commitment, lifecycle, cadence,
    # planning_amount) are explicit overrides → canonical record is authoritative
    # from Family Review, not inherited from any raw classifier stream.
    # Phase 2b consolidates to one canonical PatternResult; raw streams in audit.
    # Transaction median: 443.70.
    PatternOverride(
        description_key="כלל חיים/ב חיוב",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-klal-hayim-recurrence",
        expected_match_count=2,
        canonical_identity="klal-hayim-b",
    ),
    PatternOverride(
        description_key="כלל חיים/ב חיוב",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-klal-hayim-committed",
        expected_match_count=2,
        canonical_identity="klal-hayim-b",
    ),
    PatternOverride(
        description_key="כלל חיים/ב חיוב",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-klal-hayim-lifecycle",
        expected_match_count=2,
        canonical_identity="klal-hayim-b",
    ),
    PatternOverride(
        description_key="כלל חיים/ב חיוב",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.MONTHLY,
        override_id="ov-klal-hayim-cadence",
        expected_match_count=2,
        canonical_identity="klal-hayim-b",
    ),
    PatternOverride(
        description_key="כלל חיים/ב חיוב",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("443.70"),
        override_id="ov-klal-hayim-amount",
        expected_match_count=2,
        canonical_identity="klal-hayim-b",
    ),

    # ── מגדל חיים/בריאות — canonical group, 2 streams ────────────────────────
    # One continuous monthly insurance commitment; premium jumped Jan 2026.
    # Classifier splits into stream 1 (ENDED, pre-Jan 2026) and stream 2
    # (ACTIVE, Jan 2026+).  Family Review declares: one ACTIVE MONTHLY
    # commitment at 107.28/month.  All reviewed fields are explicit overrides →
    # canonical record authority is Family Review, not classifier inheritance.
    # Phase 2b consolidates to one canonical PatternResult; raw streams in audit.
    # Full-history median ((95.77 + 118.79) / 2): 107.28.
    PatternOverride(
        description_key="מגדל חיים/בריאות",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-migdal-recurrence",
        expected_match_count=2,
        canonical_identity="migdal-hayim-briut",
    ),
    PatternOverride(
        description_key="מגדל חיים/בריאות",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-migdal-committed",
        expected_match_count=2,
        canonical_identity="migdal-hayim-briut",
    ),
    PatternOverride(
        description_key="מגדל חיים/בריאות",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-migdal-lifecycle",
        expected_match_count=2,
        canonical_identity="migdal-hayim-briut",
    ),
    PatternOverride(
        description_key="מגדל חיים/בריאות",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.MONTHLY,
        override_id="ov-migdal-cadence",
        expected_match_count=2,
        canonical_identity="migdal-hayim-briut",
    ),
    PatternOverride(
        description_key="מגדל חיים/בריאות",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("107.28"),
        override_id="ov-migdal-amount",
        expected_match_count=2,
        canonical_identity="migdal-hayim-briut",
    ),

    # ── מנורה מבטחים-חיים/בריאות — canonical group, 2 streams ───────────────
    # One continuous monthly insurance commitment; premium jumped Feb 2026.
    # Classifier splits into stream 1 (POSSIBLY_STOPPED, pre-Feb 2026) and
    # stream 2 (ACTIVE, Feb 2026+).  Family Review declares: one ACTIVE MONTHLY
    # commitment at 95.38/month.  All reviewed fields are explicit overrides →
    # canonical record authority is Family Review, not classifier inheritance.
    # Phase 2b consolidates to one canonical PatternResult; raw streams in audit.
    # Reviewed median: 95.38.
    PatternOverride(
        description_key="מנורה מבטחים-חיים/בריאות",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-menora-recurrence",
        expected_match_count=2,
        canonical_identity="menora-mivtahim",
    ),
    PatternOverride(
        description_key="מנורה מבטחים-חיים/בריאות",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-menora-committed",
        expected_match_count=2,
        canonical_identity="menora-mivtahim",
    ),
    PatternOverride(
        description_key="מנורה מבטחים-חיים/בריאות",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-menora-lifecycle",
        expected_match_count=2,
        canonical_identity="menora-mivtahim",
    ),
    PatternOverride(
        description_key="מנורה מבטחים-חיים/בריאות",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.MONTHLY,
        override_id="ov-menora-cadence",
        expected_match_count=2,
        canonical_identity="menora-mivtahim",
    ),
    PatternOverride(
        description_key="מנורה מבטחים-חיים/בריאות",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("95.38"),
        override_id="ov-menora-amount",
        expected_match_count=2,
        canonical_identity="menora-mivtahim",
    ),

    # ── HOT — canonical group, 2 streams ──────────────────────────────────────
    # One monthly subscription split by classifier into two streams due to
    # amount variation (58.37 vs 67–79 NIS per payment).
    # Transaction chronology: one payment per month — not two concurrent services.
    # Family Review declares: one ACTIVE MONTHLY commitment at 67.20/month.
    # All reviewed fields (recurrence, commitment, lifecycle, cadence,
    # planning_amount) are explicit overrides → canonical record authority is
    # Family Review, not classifier inheritance.
    # Phase 2b consolidates to one canonical PatternResult; raw streams in audit.
    # Reviewed median (Dec 2025 reference): 67.20.
    PatternOverride(
        description_key="HOT",
        stream_label_hint="",
        field_name="recurrence_status",
        value=RecurrenceStatus.RECURRING,
        override_id="ov-hot-recurrence",
        expected_match_count=2,
        canonical_identity="hot-subscription",
    ),
    PatternOverride(
        description_key="HOT",
        stream_label_hint="",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-hot-committed",
        expected_match_count=2,
        canonical_identity="hot-subscription",
    ),
    PatternOverride(
        description_key="HOT",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ACTIVE,
        override_id="ov-hot-lifecycle",
        expected_match_count=2,
        canonical_identity="hot-subscription",
    ),
    PatternOverride(
        description_key="HOT",
        stream_label_hint="",
        field_name="cadence",
        value=Cadence.MONTHLY,
        override_id="ov-hot-cadence",
        expected_match_count=2,
        canonical_identity="hot-subscription",
    ),
    PatternOverride(
        description_key="HOT",
        stream_label_hint="",
        field_name="planning_amount",
        value=Decimal("67.20"),
        override_id="ov-hot-amount",
        expected_match_count=2,
        canonical_identity="hot-subscription",
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
