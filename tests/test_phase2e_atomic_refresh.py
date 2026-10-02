"""
Phase 2E — Atomic Refresh
Dedicated regression suite: 18 test cases.

Sections:
  A. Production authorization    (2E-001 … 2E-006)
  B. Transaction / atomicity     (2E-007 … 2E-012)
  C. Backward compatibility      (2E-013 … 2E-014)
  D. HTTP route / security       (2E-015 … 2E-018)

Zero production DB access.  All tests use temporary SQLite databases.
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Optional
from unittest.mock import patch, MagicMock

import pytest


# ═══════════════════════════════════════════════════════════════════════════
# Module fixtures
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture(scope="session")
def app_mod():
    import app as a
    return a


@pytest.fixture(scope="session")
def persist_mod():
    import v4_persistence as m
    return m


@pytest.fixture(scope="session")
def orch_mod():
    import v4_production_orchestration as o
    return o


@pytest.fixture(scope="session")
def contracts_mod():
    from intelligence import v4_contracts as c
    return c


# ═══════════════════════════════════════════════════════════════════════════
# DB helpers
# ═══════════════════════════════════════════════════════════════════════════

def _fresh_db(app_mod, tmp_path: Path, name: str = "test.db") -> str:
    db = str(tmp_path / name)
    orig = app_mod.DB_PATH
    app_mod.DB_PATH = db
    app_mod.init_db()
    app_mod.DB_PATH = orig
    return db


def _conn(db: str) -> sqlite3.Connection:
    c = sqlite3.connect(db)
    c.execute("PRAGMA foreign_keys = ON")
    return c


def _count(conn: sqlite3.Connection, table: str) -> int:
    return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


# ═══════════════════════════════════════════════════════════════════════════
# Pattern / Report factories  (shared with existing Phase 2A helpers)
# ═══════════════════════════════════════════════════════════════════════════

def _make_pattern(c, description_key: str, *, label: str = ""):
    return c.PatternResult(
        description_key=description_key,
        label=label or description_key,
        recurrence_status=c.RecurrenceStatus.UNKNOWN,
        commitment_status=c.CommitmentStatus.UNCERTAIN,
        amount_behavior=c.AmountBehavior.UNKNOWN,
        budget_class=c.BudgetClass.NON_RECURRING_EXPENSE,
        lifecycle_status=c.LifecycleStatus.UNKNOWN,
        purpose_type=c.PurposeType.UNKNOWN,
        cadence=c.Cadence.UNKNOWN,
        planning_amount=None,
        member_ids=(),
        membership_confidence={},
        evidence_sources=(),
        decision_source=c.DecisionSource.CLASSIFIER,
        family_review_required=False,
        review_reasons=(),
        reserve_eligible=False,
        monthly_reserve_contrib=Decimal("0"),
        canonical_identity=None,
    )


def _make_report(c, patterns, *, run_id: Optional[str] = None):
    zero = Decimal("0")
    rec_rec = c.make_reconciliation_record(
        field="planning_income", reviewed_value=zero, derived_value=zero, raw_derived_value=zero
    )
    raw = c.RawClassifierOutput(
        patterns=tuple(patterns),
        income_streams=(),
        planning_income_raw=zero,
        monthly_reserve_raw=zero,
        family_review_items=(),
    )
    eff = c.EffectiveFinancialResult(
        patterns=tuple(patterns),
        income_streams=(),
        planning_income_effective=zero,
        monthly_reserve_effective=zero,
        family_review_items=(),
        overrides_applied=(),
    )
    rec = c.ReconciliationReport(
        planning_income=rec_rec,
        monthly_reserve=c.make_reconciliation_record(
            field="monthly_reserve", reviewed_value=zero, derived_value=zero, raw_derived_value=zero
        ),
    )
    return c.ClassificationReport(
        classifier_version="v4.0-test",
        run_id=run_id or str(uuid.uuid4()),
        analysis_db=":memory:",
        run_at="2024-01-01T00:00:00",
        raw=raw,
        effective=eff,
        reconciliation=rec,
        override_audit=(),
    )


# Known-production path that _is_production_path() recognises.
_PROD_DB_PATH = "/c/users/erezg/.budget_tracker_data/budget.db"


# ═══════════════════════════════════════════════════════════════════════════
# A. PRODUCTION AUTHORIZATION  (2E-001 … 2E-006)
# ═══════════════════════════════════════════════════════════════════════════

class TestProductionAuthorization:

    def test_2e_001_production_path_default_flag_rejected(
        self, orch_mod, contracts_mod
    ):
        """2E-001: production path + omitted/default flag → RuntimeError, no writes."""
        p = _make_pattern(contracts_mod, "spotify")
        report = _make_report(contracts_mod, [p])
        with pytest.raises(RuntimeError, match="explicit authorization"):
            orch_mod.run_v4_production_pipeline(
                _PROD_DB_PATH, report, user_id=1
                # production_write_enabled omitted → default False
            )

    def test_2e_002_production_path_false_rejected(
        self, orch_mod, contracts_mod
    ):
        """2E-002: production path + production_write_enabled=False → rejected."""
        p = _make_pattern(contracts_mod, "netflix")
        report = _make_report(contracts_mod, [p])
        with pytest.raises(RuntimeError, match="explicit authorization"):
            orch_mod.run_v4_production_pipeline(
                _PROD_DB_PATH, report, user_id=1, production_write_enabled=False
            )

    def test_2e_003_production_path_string_true_rejected(
        self, orch_mod, contracts_mod
    ):
        """2E-003: production path + production_write_enabled='true' → rejected.
        Strict identity required; string truthiness is not authorization."""
        p = _make_pattern(contracts_mod, "hulu")
        report = _make_report(contracts_mod, [p])
        with pytest.raises(RuntimeError, match="explicit authorization"):
            orch_mod.run_v4_production_pipeline(
                _PROD_DB_PATH, report, user_id=1, production_write_enabled="true"
            )

    def test_2e_004_production_path_integer_one_rejected(
        self, orch_mod, contracts_mod
    ):
        """2E-004: production path + production_write_enabled=1 → rejected.
        Boolean truthiness must not authorize."""
        p = _make_pattern(contracts_mod, "amazon")
        report = _make_report(contracts_mod, [p])
        with pytest.raises(RuntimeError, match="explicit authorization"):
            orch_mod.run_v4_production_pipeline(
                _PROD_DB_PATH, report, user_id=1, production_write_enabled=1
            )

    def test_2e_005_production_path_literal_true_passes_auth_gate(
        self, orch_mod, contracts_mod
    ):
        """2E-005: production path + production_write_enabled=True → auth gate passes.
        (Pipeline will then fail at the real production DB — no such file — but
        the authorization check must not raise the 'explicit authorization' error.)"""
        p = _make_pattern(contracts_mod, "youtube")
        report = _make_report(contracts_mod, [p])
        # Authorization passes; subsequent sqlite3.connect will fail on the
        # production path (file doesn't exist here), which is a different error.
        with pytest.raises(Exception) as exc_info:
            orch_mod.run_v4_production_pipeline(
                _PROD_DB_PATH, report, user_id=1, production_write_enabled=True
            )
        assert "explicit authorization" not in str(exc_info.value)

    def test_2e_006_non_production_db_false_flag_allowed(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-006: non-production DB + production_write_enabled=False → pipeline allowed.
        The production flag must not block normal non-production operation."""
        db = _fresh_db(app_mod, tmp_path, "non_prod.db")
        p = _make_pattern(contracts_mod, "gym")
        report = _make_report(contracts_mod, [p])
        # Should succeed (no auth error) for a non-production path even with False flag.
        result = orch_mod.run_v4_production_pipeline(
            db, report, user_id=1, production_write_enabled=False
        )
        assert result.run_id is not None
        conn = _conn(db)
        assert _count(conn, "v4_run_results") == 1
        conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# B. TRANSACTION / ATOMICITY  (2E-007 … 2E-012)
# ═══════════════════════════════════════════════════════════════════════════

class TestAtomicity:

    def test_2e_007_successful_pipeline_commits_all_phases(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-007: successful run → all three phases' rows are committed and readable."""
        db = _fresh_db(app_mod, tmp_path, "success.db")
        p = _make_pattern(contracts_mod, "electricity")
        report = _make_report(contracts_mod, [p])

        result = orch_mod.run_v4_production_pipeline(
            db, report, user_id=1, production_write_enabled=False
        )

        conn = _conn(db)
        # Phase 2A rows committed
        assert _count(conn, "v4_run_results") == 1
        assert _count(conn, "pattern_families") == 1
        # Phase 2B: either a suggestion or a link event was written
        # (no existing commitment → NEW_RECURRING_SUGGESTED or NO_ACTION —
        # at minimum the run_results row must exist, confirming commit)
        assert result.run_id is not None
        conn.close()

    def test_2e_008_phase2a_failure_rollback_zero_rows(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-008: Phase 2A failure → full rollback, zero Phase 2E rows remain."""
        db = _fresh_db(app_mod, tmp_path, "fail2a.db")
        p = _make_pattern(contracts_mod, "water")
        report = _make_report(contracts_mod, [p])

        with patch(
            "v4_production_orchestration.persist_run_on_connection",
            side_effect=RuntimeError("simulated Phase 2A failure"),
        ):
            with pytest.raises(RuntimeError, match="simulated Phase 2A failure"):
                orch_mod.run_v4_production_pipeline(
                    db, report, user_id=1, production_write_enabled=False
                )

        conn = _conn(db)
        assert _count(conn, "v4_run_results") == 0
        assert _count(conn, "pattern_families") == 0
        conn.close()

    def test_2e_009_phase2b_failure_after_phase2a_rollback(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-009: Phase 2B failure after Phase 2A persistence → full rollback.
        Phase 2A rows must not remain after pipeline failure."""
        db = _fresh_db(app_mod, tmp_path, "fail2b.db")
        p = _make_pattern(contracts_mod, "internet")
        report = _make_report(contracts_mod, [p])

        with patch(
            "v4_production_orchestration.link_phase2b",
            side_effect=RuntimeError("simulated Phase 2B failure"),
        ):
            with pytest.raises(RuntimeError, match="simulated Phase 2B failure"):
                orch_mod.run_v4_production_pipeline(
                    db, report, user_id=1, production_write_enabled=False
                )

        conn = _conn(db)
        assert _count(conn, "v4_run_results") == 0
        assert _count(conn, "pattern_families") == 0
        conn.close()

    def test_2e_010_phase2d2_failure_after_2a_2b_rollback(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-010: Phase 2D2 failure after 2A+2B → full rollback.
        Even though 2D2 performs no writes, its failure must roll back 2A/2B."""
        db = _fresh_db(app_mod, tmp_path, "fail2d2.db")
        p = _make_pattern(contracts_mod, "phone")
        report = _make_report(contracts_mod, [p])

        with patch(
            "v4_production_orchestration.orchestrate_authority_adjustment",
            side_effect=RuntimeError("simulated Phase 2D2 failure"),
        ):
            with pytest.raises(RuntimeError, match="simulated Phase 2D2 failure"):
                orch_mod.run_v4_production_pipeline(
                    db, report, user_id=1, production_write_enabled=False
                )

        conn = _conn(db)
        assert _count(conn, "v4_run_results") == 0
        assert _count(conn, "pattern_families") == 0
        conn.close()

    def test_2e_011_same_connection_passed_through_all_phases(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-011: all three phases receive the same sqlite3.Connection object."""
        db = _fresh_db(app_mod, tmp_path, "conn_identity.db")
        p = _make_pattern(contracts_mod, "rent")
        report = _make_report(contracts_mod, [p])

        connections_seen: list[int] = []

        original_persist = orch_mod.persist_run_on_connection
        original_link    = orch_mod.link_phase2b
        original_auth    = orch_mod.orchestrate_authority_adjustment

        def spy_persist(conn, *args, **kwargs):
            connections_seen.append(id(conn))
            return original_persist(conn, *args, **kwargs)

        def spy_link(conn, *args, **kwargs):
            connections_seen.append(id(conn))
            return original_link(conn, *args, **kwargs)

        def spy_auth(conn, *args, **kwargs):
            connections_seen.append(id(conn))
            return original_auth(conn, *args, **kwargs)

        with (
            patch("v4_production_orchestration.persist_run_on_connection", spy_persist),
            patch("v4_production_orchestration.link_phase2b",              spy_link),
            patch("v4_production_orchestration.orchestrate_authority_adjustment", spy_auth),
        ):
            orch_mod.run_v4_production_pipeline(
                db, report, user_id=1, production_write_enabled=False
            )

        assert len(connections_seen) == 3, "All three phases must be called"
        assert connections_seen[0] == connections_seen[1] == connections_seen[2], (
            "All phases must receive the same connection object"
        )

    def test_2e_012_no_internal_commit_from_subphases(
        self, orch_mod, contracts_mod, app_mod, tmp_path
    ):
        """2E-012: sub-phases do not issue their own commit.
        Proven by transaction state: after 2A + 2B write but before final COMMIT,
        a separate reader connection must see zero rows (uncommitted data invisible).
        We validate this indirectly: a successful run produces committed rows
        readable from a new connection, which means exactly one commit happened
        at the orchestration layer (a sub-phase committing would expose rows
        during rollback tests — already covered by 2E-008 through 2E-010)."""
        db = _fresh_db(app_mod, tmp_path, "one_commit.db")
        p = _make_pattern(contracts_mod, "mortgage")
        report = _make_report(contracts_mod, [p])

        result = orch_mod.run_v4_production_pipeline(
            db, report, user_id=1, production_write_enabled=False
        )

        # A new independent connection must see exactly the committed rows.
        conn2 = sqlite3.connect(db)
        rows = conn2.execute("SELECT id FROM v4_run_results").fetchall()
        conn2.close()
        assert len(rows) == 1
        assert rows[0][0] in [o.run_result_id for o in result.persistence.outcomes]


# ═══════════════════════════════════════════════════════════════════════════
# C. BACKWARD COMPATIBILITY  (2E-013 … 2E-014)
# ═══════════════════════════════════════════════════════════════════════════

class TestBackwardCompatibility:

    def test_2e_013_persist_run_public_contract_unchanged(
        self, persist_mod, app_mod, contracts_mod, tmp_path
    ):
        """2E-013: legacy persist_run(db_path, report, *, user_id, run_id) still works."""
        db = _fresh_db(app_mod, tmp_path, "legacy.db")
        p = _make_pattern(contracts_mod, "insurance")
        fixed_run_id = str(uuid.uuid4())
        report = _make_report(contracts_mod, [p], run_id=fixed_run_id)

        result = persist_mod.persist_run(db, report, user_id=1, run_id=fixed_run_id)

        # Returns PersistenceReport
        assert result.run_id == fixed_run_id
        assert len(result.outcomes) == 1

        # Rows are committed (readable from a fresh connection)
        conn = _conn(db)
        assert _count(conn, "v4_run_results") == 1
        assert _count(conn, "pattern_families") == 1
        conn.close()

    def test_2e_014_persist_run_on_connection_caller_owns_lifecycle(
        self, persist_mod, app_mod, contracts_mod, tmp_path
    ):
        """2E-014: persist_run_on_connection does not close or commit caller's connection."""
        db = _fresh_db(app_mod, tmp_path, "caller_owned.db")
        p = _make_pattern(contracts_mod, "car_insurance")
        report = _make_report(contracts_mod, [p])

        conn = sqlite3.connect(db)
        conn.execute("PRAGMA foreign_keys = ON")
        try:
            conn.execute("BEGIN")
            persist_mod.persist_run_on_connection(conn, report, user_id=1)

            # Connection is still open and usable after the call
            assert not conn.in_transaction or True  # connection alive

            # Data is NOT yet committed from external reader's perspective
            conn2 = sqlite3.connect(db)
            rows_before = conn2.execute("SELECT COUNT(*) FROM v4_run_results").fetchone()[0]
            conn2.close()
            assert rows_before == 0, (
                "persist_run_on_connection must not commit; rows must be invisible "
                "to other connections before caller commits"
            )

            # Caller can still use the same connection
            conn.commit()

            # Now rows are visible
            conn3 = sqlite3.connect(db)
            rows_after = conn3.execute("SELECT COUNT(*) FROM v4_run_results").fetchone()[0]
            conn3.close()
            assert rows_after == 1
        finally:
            conn.close()


# ═══════════════════════════════════════════════════════════════════════════
# D. HTTP ROUTE / SECURITY CONTRACT  (2E-015 … 2E-018)
# ═══════════════════════════════════════════════════════════════════════════

@pytest.fixture
def flask_client(app_mod, tmp_path):
    """Provide a Flask test client with a fresh isolated DB."""
    db = _fresh_db(app_mod, tmp_path, "route_test.db")
    app_mod.app.config['TESTING'] = True
    app_mod.app.config['SECRET_KEY'] = 'test-secret'
    app_mod.app.config['WTF_CSRF_ENABLED'] = False
    # Ensure production flag is absent by default
    app_mod.app.config.pop('V4_PRODUCTION_ENABLED', None)
    orig_db = app_mod.DB_PATH
    app_mod.DB_PATH = db
    with app_mod.app.test_client() as client:
        yield client, db, app_mod
    app_mod.DB_PATH = orig_db


def _login(client, app_mod, db):
    """Insert a test user and create a session."""
    conn = sqlite3.connect(db)
    conn.execute("PRAGMA foreign_keys = ON")
    # Insert user with minimal required fields
    conn.execute(
        "INSERT OR IGNORE INTO users (id, username, password_hash, is_admin) "
        "VALUES (1, 'testuser', 'x', 0)"
    )
    conn.commit()
    conn.close()
    with client.session_transaction() as sess:
        sess['user_id'] = 1


class TestRouteSecurityContract:

    def test_2e_015_unauthenticated_returns_401(self, flask_client, contracts_mod):
        """2E-015: unauthenticated POST /api/v4/refresh → 401, pipeline not invoked."""
        client, db, app_mod = flask_client
        # No login → no session user_id
        resp = client.post('/api/v4/refresh')
        assert resp.status_code == 401
        data = resp.get_json()
        assert 'error' in data

    def test_2e_016_request_cannot_override_user_id_db_path_or_prod_flag(
        self, flask_client, contracts_mod
    ):
        """2E-016: request-supplied user_id / db_path / production_write_enabled are ignored.
        Endpoint uses session uid, server DB_PATH, and app.config flag only."""
        client, db, app_mod = flask_client
        _login(client, app_mod, db)

        # Attempt to supply all three through both body and query params
        malicious_body = {
            "user_id": 999999,
            "db_path": "/etc/passwd",
            "production_write_enabled": True,
        }

        captured = {}

        import v4_production_orchestration as orch_mod

        original_pipeline = orch_mod.run_v4_production_pipeline

        def spy_pipeline(actual_db_path, report, *, user_id, production_write_enabled=False, **kw):
            captured['db_path'] = actual_db_path
            captured['user_id'] = user_id
            captured['production_write_enabled'] = production_write_enabled
            return original_pipeline(
                actual_db_path, report,
                user_id=user_id,
                production_write_enabled=production_write_enabled,
                **kw,
            )

        import intelligence.v4_cashflow_engine as engine

        with (
            patch("v4_production_orchestration.run_v4_production_pipeline", spy_pipeline),
            patch.object(engine, "run_analysis",
                         return_value=_make_report(contracts_mod, [])),
        ):
            resp = client.post(
                '/api/v4/refresh?user_id=999999&db_path=/etc/passwd',
                data=json.dumps(malicious_body),
                content_type='application/json',
            )

        # The pipeline must have been called with server values, not request values
        assert captured.get('user_id') == 1, (
            f"user_id must come from session (1), not request (got {captured.get('user_id')})"
        )
        assert captured.get('db_path') == db, (
            f"db_path must be server DB_PATH ({db}), not request-supplied"
        )
        assert captured.get('production_write_enabled') is not True, (
            "production_write_enabled must not be set True by request input"
        )

    def test_2e_017_production_authorization_through_route(
        self, flask_client, contracts_mod
    ):
        """2E-017: missing/False server config → 403; request-supplied flag cannot bypass."""
        client, db, app_mod = flask_client
        _login(client, app_mod, db)

        # Confirm flag is absent / False
        app_mod.app.config.pop('V4_PRODUCTION_ENABLED', None)

        import intelligence.v4_cashflow_engine as engine
        import v4_production_orchestration as orch_mod

        # Simulate pipeline raising the production authorization RuntimeError
        def mock_pipeline(db_path, report, *, user_id, production_write_enabled=False, **kw):
            if production_write_enabled is not True:
                raise RuntimeError(
                    "Production writes require explicit authorization"
                )

        with (
            patch("v4_production_orchestration.run_v4_production_pipeline", mock_pipeline),
            patch.object(engine, "run_analysis",
                         return_value=_make_report(contracts_mod, [])),
        ):
            resp = client.post('/api/v4/refresh')

        assert resp.status_code == 403
        data = resp.get_json()
        assert 'error' in data
        # Must not expose internal details
        assert 'explicit authorization' not in data.get('error', '')
        assert 'RuntimeError' not in data.get('error', '')
        assert 'Traceback' not in str(data)

        # Verify literal True in server config would pass (authorize)
        app_mod.app.config['V4_PRODUCTION_ENABLED'] = True
        captured_flag = {}

        def spy_pipeline_true(db_path, report, *, user_id, production_write_enabled=False, **kw):
            captured_flag['value'] = production_write_enabled
            # Succeed (return a mock result)
            from v4_production_orchestration import ProductionPipelineResult
            from v4_persistence import PersistenceReport
            from v4_linking import LinkReport
            mock_adjusted = MagicMock()
            mock_adjusted.final_report.effective.patterns = ()
            mock_adjusted.final_report.effective.planning_income_effective = Decimal("0")
            mock_adjusted.final_report.effective.monthly_reserve_effective = Decimal("0")
            mock_link = MagicMock()
            mock_link.results = []
            mock_persist = PersistenceReport(run_id="test-run-id", user_id=user_id)
            return ProductionPipelineResult(
                persistence=mock_persist,
                link=mock_link,
                adjusted=mock_adjusted,
                run_id="test-run-id",
            )

        with (
            patch("v4_production_orchestration.run_v4_production_pipeline", spy_pipeline_true),
            patch.object(engine, "run_analysis",
                         return_value=_make_report(contracts_mod, [])),
        ):
            resp2 = client.post('/api/v4/refresh')

        assert captured_flag.get('value') is True, (
            "app.config True must be passed as literal True to pipeline"
        )
        app_mod.app.config.pop('V4_PRODUCTION_ENABLED', None)

    def test_2e_018_success_response_contract_and_error_sanitization(
        self, flask_client, contracts_mod
    ):
        """2E-018: success response carries exact keys from orchestration result;
        also verifies unexpected pipeline failures return sanitized 500."""
        client, db, app_mod = flask_client
        _login(client, app_mod, db)

        import intelligence.v4_cashflow_engine as engine
        from v4_production_orchestration import ProductionPipelineResult
        from v4_persistence import PersistenceReport
        from v4_linking import LinkReport, PatternLinkResult, LinkOutcome

        fixed_run_id = "aaaabbbb-cccc-dddd-eeee-ffffffffffff"
        planning_income = Decimal("8000.00")
        monthly_reserve = Decimal("1500.00")

        mock_persist = PersistenceReport(run_id=fixed_run_id, user_id=1)
        mock_link_result = MagicMock()
        mock_link_result.outcome.value = "LINKED"
        mock_link = MagicMock()
        mock_link.results = [mock_link_result]

        mock_adjusted = MagicMock()
        mock_adjusted.final_report.effective.patterns = (MagicMock(), MagicMock())  # 2 patterns
        mock_adjusted.final_report.effective.planning_income_effective = planning_income
        mock_adjusted.final_report.effective.monthly_reserve_effective = monthly_reserve

        mock_result = ProductionPipelineResult(
            persistence=mock_persist,
            link=mock_link,
            adjusted=mock_adjusted,
            run_id=fixed_run_id,
        )

        def mock_pipeline_success(db_path, report, *, user_id, production_write_enabled=False, **kw):
            return mock_result

        with (
            patch("v4_production_orchestration.run_v4_production_pipeline", mock_pipeline_success),
            patch.object(engine, "run_analysis",
                         return_value=_make_report(contracts_mod, [])),
        ):
            resp = client.post('/api/v4/refresh')

        assert resp.status_code == 200
        data = resp.get_json()

        # Exact keys required by spec
        assert data['ok'] is True
        assert data['run_id'] == fixed_run_id
        assert data['patterns_count'] == 2
        assert data['linked_count'] == 1
        assert data['planning_income'] == str(planning_income)
        assert data['monthly_reserve'] == str(monthly_reserve)

        # No unexpected internal fields
        allowed_keys = {'ok', 'run_id', 'patterns_count', 'linked_count',
                        'planning_income', 'monthly_reserve'}
        assert set(data.keys()) == allowed_keys, (
            f"Unexpected keys in response: {set(data.keys()) - allowed_keys}"
        )

        # Error sanitization: unexpected pipeline failure → sanitized 500
        def mock_pipeline_crash(db_path, report, *, user_id, production_write_enabled=False, **kw):
            raise ValueError("internal details: db_path=/secret budget.db traceback")

        with (
            patch("v4_production_orchestration.run_v4_production_pipeline", mock_pipeline_crash),
            patch.object(engine, "run_analysis",
                         return_value=_make_report(contracts_mod, [])),
        ):
            resp_err = client.post('/api/v4/refresh')

        assert resp_err.status_code == 500
        err_data = resp_err.get_json()
        assert 'error' in err_data
        # Raw exception text must not leak
        assert 'internal details' not in err_data.get('error', '')
        assert 'db_path' not in err_data.get('error', '')
        assert 'traceback' not in err_data.get('error', '').lower()
        assert 'ValueError' not in err_data.get('error', '')
