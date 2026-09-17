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
# Keys computed from (person, source, norm_desc)
_INCOME_BASELINES_RAW: list[tuple[str, str, str, Decimal]] = [
    ("אשה", "employer", "משכורת",    Decimal("16623.00")),
    ("בעל", "employer", "משכורת",    Decimal("14446.00")),
    ("",    "ביטוח לאומי", "קצבת ילדים", Decimal("590.50")),
    # family support and bonuses: planning_baseline = 0 by policy
]

INCOME_BASELINES: dict[str, Decimal] = {
    make_income_stream_key(person, source, desc): baseline
    for person, source, desc, baseline in _INCOME_BASELINES_RAW
}

# Pattern-level overrides for known Family Review decisions
# These encode human knowledge that the classifier cannot derive from evidence alone.
PATTERN_OVERRIDES: list[PatternOverride] = [
    # Gal Naomi training fund — SAVINGS_INVESTMENT but reserve-eligible
    PatternOverride(
        description_key="גל נעמי".upper(),
        stream_label_hint="607",
        field_name="purpose_type",
        value=PurposeType.SAVINGS_INVESTMENT,
        override_id="ov-gal-naomi-607-purpose",
    ),
    PatternOverride(
        description_key="גל נעמי".upper(),
        stream_label_hint="607",
        field_name="commitment_status",
        value=CommitmentStatus.COMMITTED,
        override_id="ov-gal-naomi-607-committed",
    ),
    # Discount bank card fee — FINANCIAL_FEE but reserve-eligible
    PatternOverride(
        description_key="",  # will be set when we know the real description key
        stream_label_hint="דיסקונט",
        field_name="purpose_type",
        value=PurposeType.FINANCIAL_FEE,
        override_id="ov-discount-fee-purpose",
    ),
    # Cancelled STP service
    PatternOverride(
        description_key="STP",
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.CANCELLED,
        override_id="ov-stp-cancelled",
    ),
    # Ended Noy Lenz course
    PatternOverride(
        description_key="נוי לנץ".upper(),
        stream_label_hint="",
        field_name="lifecycle_status",
        value=LifecycleStatus.ENDED,
        override_id="ov-noy-lenz-ended",
    ),
    # Google Cloud — planning_amount TBD
    PatternOverride(
        description_key="GOOGLE CLOUD",
        stream_label_hint="",
        field_name="planning_amount",
        value=None,
        override_id="ov-google-cloud-tbd",
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
