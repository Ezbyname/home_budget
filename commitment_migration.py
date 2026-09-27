"""
Unified Commitments — Phase 1 Migration
Installment migration + finite occurrence generator.

This module migrates existing legacy installment plans into the Unified
Commitments schema.  It is intentionally NOT wired into app startup —
production migration requires a separate explicit invocation gate.

All writes go only to Unified Commitment tables:
    commitments
    commitment_installment_meta
    commitment_expense_links
    commitment_occurrences

Legacy tables (installments, installment_transaction_links, expenses,
installment_suggestions, income) are READ-ONLY in this module.
"""

from __future__ import annotations

import os
import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import List, Optional

# ── Production DB guard ───────────────────────────────────────────────────────

_KNOWN_PRODUCTION_PATHS = [
    # Windows primary path — matched case-insensitively after normalisation
    r"C:\Users\erezg\.budget_tracker_data\budget.db",
    "/c/users/erezg/.budget_tracker_data/budget.db",
]

# Canonicalised fingerprints (lowercase, forward-slashes, no trailing sep)
_PRODUCTION_FINGERPRINTS = frozenset(
    p.lower().replace("\\", "/").rstrip("/")
    for p in _KNOWN_PRODUCTION_PATHS
)


def _canonicalise(path: str) -> str:
    return os.path.normpath(path).lower().replace("\\", "/")


def _is_production_path(db_path: str) -> bool:
    canon = _canonicalise(db_path)
    for fp in _PRODUCTION_FINGERPRINTS:
        if canon == fp or canon.endswith("/" + fp.lstrip("/")):
            return True
    # Also check via os.path.abspath for relative paths
    abs_canon = _canonicalise(os.path.abspath(db_path))
    for fp in _PRODUCTION_FINGERPRINTS:
        if abs_canon == fp:
            return True
    return False


# ── Result states ─────────────────────────────────────────────────────────────

class MigrationResult(str, Enum):
    MIGRATED                  = "MIGRATED"
    ALREADY_MIGRATED          = "ALREADY_MIGRATED"
    SKIPPED_NO_OWNER          = "SKIPPED_NO_OWNER"
    SKIPPED_MULTI_OWNER       = "SKIPPED_MULTI_OWNER"
    SKIPPED_STATUS_NOT_ELIGIBLE = "SKIPPED_STATUS_NOT_ELIGIBLE"
    INVALID_DATA              = "INVALID_DATA"
    CONFLICT                  = "CONFLICT"
    FAILED                    = "FAILED"


# Statuses eligible for Phase 1 migration.
# Only 'active' installment plans are migrated.
ELIGIBLE_STATUSES = {"active"}


@dataclass
class InstallmentRecord:
    """Holds relevant legacy installment fields resolved before migration."""
    id: int
    description: str
    store: str
    total_amount: float
    total_payments: int
    payments_made: int
    monthly_payment: float
    start_date: str
    card: str
    notes: str
    user_id: int
    status: str
    source: str
    vendor_normalized: str


@dataclass
class MigrationOutcome:
    legacy_id: int
    result: MigrationResult
    commitment_id: Optional[str] = None
    detail: str = ""


@dataclass
class MigrationReport:
    outcomes: List[MigrationOutcome] = field(default_factory=list)

    def counts(self):
        from collections import Counter
        return Counter(o.result.value for o in self.outcomes)

    def non_success_ids(self):
        ok = {MigrationResult.MIGRATED, MigrationResult.ALREADY_MIGRATED}
        return {o.result.value: [o.legacy_id for o in self.outcomes if o.result == o.result and o.result not in ok]
                for r in MigrationResult if r not in ok
                for o in []}  # filled below

    def ids_by_result(self):
        from collections import defaultdict
        d = defaultdict(list)
        for o in self.outcomes:
            d[o.result.value].append(o.legacy_id)
        return dict(d)


# ── Calendar-month arithmetic ─────────────────────────────────────────────────

def _add_months(d: date, months: int) -> date:
    """Return d + months calendar months, clamping to end-of-month correctly."""
    target_month = d.month + months
    target_year  = d.year + (target_month - 1) // 12
    target_month = (target_month - 1) % 12 + 1
    import calendar
    last_day = calendar.monthrange(target_year, target_month)[1]
    return date(target_year, target_month, min(d.day, last_day))


# ── Money conversion ──────────────────────────────────────────────────────────

def _to_agorot(amount_nis) -> int:
    """Convert a NIS amount (float/str/Decimal) to exact INTEGER agorot."""
    d = Decimal(str(amount_nis)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return int(d * 100)


# ── Occurrence generator ──────────────────────────────────────────────────────

def generate_future_occurrences(
    commitment_id: str,
    user_id: int,
    first_payment_date: date,
    total_payments: int,
    payments_made: int,
    payment_agorot: int,
) -> List[dict]:
    """
    Generate future expected occurrence dicts for occurrence indexes M+1 .. N.

    Occurrence K has date = first_payment_date + (K-1) calendar months.
    Returns [] when payments_made == total_payments.
    Raises ValueError if payments_made > total_payments or other invariant fails.
    """
    N = total_payments
    M = payments_made

    if N <= 0:
        raise ValueError(f"total_payments must be > 0, got {N}")
    if M < 0:
        raise ValueError(f"payments_made must be >= 0, got {M}")
    if M > N:
        raise ValueError(
            f"payments_made ({M}) > total_payments ({N}): invalid installment state"
        )

    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    rows = []
    for k in range(M + 1, N + 1):
        occ_date = _add_months(first_payment_date, k - 1)
        rows.append({
            "commitment_id":  commitment_id,
            "user_id":        user_id,
            "occurrence_date": occ_date.isoformat(),
            "occurrence_index": k,
            "expected_agorot": payment_agorot,
            "status":         "expected",
            "generated_at":   now_iso,
        })
    return rows


# ── Ownership resolution — Policy A ──────────────────────────────────────────
#
# installments.user_id is primary (set by authenticated session, NOT NULL).
# Expense graph is consistency validation / fallback for user_id=0 sentinel.
#
# Matrix:
#   inst.user_id > 0, no links          → MIGRATE using installment owner
#   inst.user_id > 0, all match         → MIGRATE
#   inst.user_id > 0, owner mismatch    → CONFLICT (fail closed)
#   inst.user_id > 0, multiple owners   → CONFLICT (fail closed)
#   inst.user_id = 0, one expense owner → MIGRATE using expense graph owner
#   inst.user_id = 0, no links          → SKIPPED_NO_OWNER
#   inst.user_id = 0, multiple owners   → SKIPPED_MULTI_OWNER

def _resolve_expense_owners(conn: sqlite3.Connection, installment_id: int) -> list:
    """Return list of DISTINCT expense user_ids from confirmed/auto_matched links."""
    rows = conn.execute("""
        SELECT DISTINCT e.user_id
        FROM installment_transaction_links itl
        JOIN expenses e ON e.id = itl.expense_id
        WHERE itl.installment_id = ?
          AND itl.status IN ('confirmed', 'auto_matched')
    """, (installment_id,)).fetchall()
    return [r[0] for r in rows]


# ── Core migration ────────────────────────────────────────────────────────────

def _migrate_one(
    conn: sqlite3.Connection,
    inst: InstallmentRecord,
    dry_run: bool = False,
) -> MigrationOutcome:
    """
    Migrate one legacy installment atomically via SAVEPOINT.
    Returns a MigrationOutcome.  Never raises — all exceptions are caught.
    """
    lid = inst.id

    # 1. Resolve ownership — Policy A
    inst_uid = inst.user_id
    expense_owners = _resolve_expense_owners(conn, lid)

    if inst_uid > 0:
        if len(expense_owners) == 0:
            user_id = inst_uid                               # no links — trust installment
        elif len(set(expense_owners)) > 1:
            return MigrationOutcome(lid, MigrationResult.CONFLICT,
                                    detail=f"multiple expense owners={sorted(set(expense_owners))}")
        elif expense_owners[0] != inst_uid:
            return MigrationOutcome(lid, MigrationResult.CONFLICT,
                                    detail=f"expense owner {expense_owners[0]} != installment owner {inst_uid}")
        else:
            user_id = inst_uid                               # expense graph confirms
    else:
        # user_id=0: sentinel — fall back to expense graph
        if len(expense_owners) == 0:
            return MigrationOutcome(lid, MigrationResult.SKIPPED_NO_OWNER)
        unique_owners = set(expense_owners)
        if len(unique_owners) > 1:
            return MigrationOutcome(lid, MigrationResult.SKIPPED_MULTI_OWNER,
                                    detail=f"owners={sorted(unique_owners)}")
        owner = expense_owners[0]
        if owner <= 0:
            return MigrationOutcome(lid, MigrationResult.SKIPPED_NO_OWNER,
                                    detail="expense owner is also <=0")
        user_id = owner

    # 2. Check 1:1 — already migrated?
    existing = conn.execute(
        "SELECT id FROM commitments WHERE linked_legacy_installment_id = ?", (lid,)
    ).fetchone()
    if existing:
        return MigrationOutcome(lid, MigrationResult.ALREADY_MIGRATED,
                                commitment_id=existing[0])

    # 3. Validate data
    try:
        N = inst.total_payments
        M = inst.payments_made
        if N <= 0:
            return MigrationOutcome(lid, MigrationResult.INVALID_DATA,
                                    detail=f"total_payments={N}")
        if M < 0 or M > N:
            return MigrationOutcome(lid, MigrationResult.INVALID_DATA,
                                    detail=f"payments_made={M} total={N}")

        payment_agorot       = _to_agorot(inst.monthly_payment)
        total_purchase_agorot = _to_agorot(inst.total_amount)

        if payment_agorot <= 0:
            return MigrationOutcome(lid, MigrationResult.INVALID_DATA,
                                    detail=f"payment_agorot={payment_agorot}")

        # Parse first_payment_date from start_date
        first_payment_date = date.fromisoformat(inst.start_date)
    except Exception as exc:
        return MigrationOutcome(lid, MigrationResult.INVALID_DATA,
                                detail=str(exc))

    if dry_run:
        return MigrationOutcome(lid, MigrationResult.MIGRATED,
                                detail="dry_run=True")

    # 4. Collect legacy expense links
    link_rows = conn.execute("""
        SELECT itl.expense_id, e.user_id
        FROM installment_transaction_links itl
        JOIN expenses e ON e.id = itl.expense_id
        WHERE itl.installment_id = ?
          AND itl.status IN ('confirmed', 'auto_matched')
        ORDER BY itl.id
    """, (lid,)).fetchall()

    # 5. Check for expense conflicts (same expense already owned by another commitment)
    for exp_id, exp_uid in link_rows:
        if exp_uid != user_id:
            return MigrationOutcome(lid, MigrationResult.CONFLICT,
                                    detail=f"expense {exp_id} user_id mismatch: {exp_uid}!={user_id}")
        conflict = conn.execute("""
            SELECT commitment_id FROM commitment_expense_links
            WHERE expense_id = ? AND membership_type IN ('MEMBER','OCCURRENCE_CONFIRMED')
        """, (exp_id,)).fetchone()
        if conflict:
            return MigrationOutcome(
                lid, MigrationResult.CONFLICT,
                detail=f"expense {exp_id} already owned by commitment {conflict[0]}"
            )

    # 6. Everything looks good — atomic migration via SAVEPOINT
    commitment_id = str(uuid.uuid4())
    now_iso = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
    anchor_day = first_payment_date.day

    sp = f"sp_inst_{lid}"
    try:
        occurrences = generate_future_occurrences(
            commitment_id, user_id, first_payment_date, N, M, payment_agorot
        )
        conn.execute(f"SAVEPOINT {sp}")

        # Insert commitment
        conn.execute("""
            INSERT INTO commitments
                (id, user_id, canonical_label, source_type, is_finite,
                 total_occurrences, linked_legacy_installment_id, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (commitment_id, user_id, inst.description or inst.vendor_normalized or '',
              'MIGRATED', 1, N, lid, now_iso, now_iso))

        # Insert installment meta
        conn.execute("""
            INSERT INTO commitment_installment_meta
                (commitment_id, user_id, total_payments, payments_made,
                 payment_agorot, total_purchase_agorot, first_payment_date,
                 anchor_day_of_month, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (commitment_id, user_id, N, M, payment_agorot,
              total_purchase_agorot, first_payment_date.isoformat(),
              anchor_day, now_iso))

        # Insert expense links
        for exp_id, _exp_uid in link_rows:
            conn.execute("""
                INSERT INTO commitment_expense_links
                    (commitment_id, user_id, expense_id, membership_type, linked_by, created_at)
                VALUES (?,?,?,?,?,?)
            """, (commitment_id, user_id, exp_id, 'MEMBER', 'MIGRATION', now_iso))

        # Insert future occurrences
        for occ in occurrences:
            conn.execute("""
                INSERT INTO commitment_occurrences
                    (commitment_id, user_id, occurrence_date, occurrence_index,
                     expected_agorot, status, generated_at)
                VALUES (?,?,?,?,?,?,?)
            """, (occ["commitment_id"], occ["user_id"], occ["occurrence_date"],
                  occ["occurrence_index"], occ["expected_agorot"],
                  occ["status"], occ["generated_at"]))

        conn.execute(f"RELEASE {sp}")
        return MigrationOutcome(lid, MigrationResult.MIGRATED,
                                commitment_id=commitment_id)

    except Exception as exc:
        try:
            conn.execute(f"ROLLBACK TO {sp}")
            conn.execute(f"RELEASE {sp}")
        except Exception:
            pass
        return MigrationOutcome(lid, MigrationResult.FAILED, detail=str(exc))


# ── Public entry point ────────────────────────────────────────────────────────

def migrate_installments(
    db_path: str,
    *,
    dry_run: bool = False,
    eligible_statuses: frozenset = frozenset(ELIGIBLE_STATUSES),
) -> MigrationReport:
    """
    Migrate all eligible legacy installment plans in the given database.

    Raises RuntimeError if db_path resolves to the known production DB.
    All writes are committed only if not dry_run.
    """
    if _is_production_path(db_path):
        raise RuntimeError(
            f"PRODUCTION SAFETY ABORT: refusing to write to production DB: {db_path!r}"
        )

    report = MigrationReport()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")

    try:
        # Fetch all installments
        all_insts = conn.execute(
            "SELECT * FROM installments ORDER BY id"
        ).fetchall()

        for row in all_insts:
            d = dict(row)
            status = (d.get("status") or "active").lower()
            if status not in eligible_statuses:
                report.outcomes.append(MigrationOutcome(
                    d["id"],
                    MigrationResult.SKIPPED_STATUS_NOT_ELIGIBLE,
                    detail=f"status={status!r}",
                ))
                continue

            inst = InstallmentRecord(
                id=d["id"],
                description=d.get("description") or "",
                store=d.get("store") or "",
                total_amount=d.get("total_amount") or 0,
                total_payments=d.get("total_payments") or 0,
                payments_made=d.get("payments_made") or 0,
                monthly_payment=d.get("monthly_payment") or 0,
                start_date=d.get("start_date") or "",
                card=d.get("card") or "",
                notes=d.get("notes") or "",
                user_id=d.get("user_id") or 0,
                status=status,
                source=d.get("source") or "manual",
                vendor_normalized=d.get("vendor_normalized") or "",
            )
            outcome = _migrate_one(conn, inst, dry_run=dry_run)
            report.outcomes.append(outcome)

        if not dry_run:
            conn.commit()
    finally:
        conn.close()

    return report
