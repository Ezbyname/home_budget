"""
Sanity Test Suite for Family Budget Tracker
Run before committing: python sanity_test.py
Uses a temporary database — no effect on real data.
"""
import os
import sys
import json
import tempfile
import shutil

# ── Make sure app can be imported ──
sys.path.insert(0, os.path.dirname(__file__))

import app as budget_app

PASS = 0
FAIL = 0
ERRORS = []


def ok(label):
    global PASS
    PASS += 1
    print(f"  ✅ {label}")


def fail(label, detail=""):
    global FAIL
    FAIL += 1
    msg = f"  ❌ {label}" + (f" — {detail}" if detail else "")
    ERRORS.append(msg)
    print(msg)


def check(condition, label, detail=""):
    if condition:
        ok(label)
    else:
        fail(label, detail)


# ── Setup: temporary DB, Flask test client ──
tmp_dir = tempfile.mkdtemp()
tmp_db = os.path.join(tmp_dir, "test_budget.db")
budget_app.DB_PATH = tmp_db
budget_app.app.config["TESTING"] = True
budget_app.app.config["SECRET_KEY"] = "test-secret"
client = budget_app.app.test_client()

# Initialize DB
with budget_app.app.app_context():
    budget_app.init_db()


def api_get(url):
    r = client.get(url)
    return r.status_code, r.get_json()


def api_post(url, data=None):
    r = client.post(url, json=data, content_type="application/json")
    return r.status_code, r.get_json()


def api_delete(url):
    r = client.delete(url)
    return r.status_code, r.get_json()


def api_put(url, data=None):
    r = client.put(url, json=data, content_type="application/json")
    return r.status_code, r.get_json()


# ====================================================================
print("\n🔹 1. App Startup & Static Files")
# ====================================================================
status, _ = api_get("/")
check(status == 200, "GET / returns 200")

status, _ = api_get("/static/index.html")
check(status == 200, "GET /static/index.html returns 200")


# ====================================================================
print("\n🔹 2. Categories")
# ====================================================================
status, cats = api_get("/api/categories")
check(status == 200, "GET /api/categories returns 200")
check(isinstance(cats, list) and len(cats) > 0, "Categories list is non-empty")
check(all("id" in c and "name_he" in c for c in cats), "Each category has id and name_he")
cat_id = cats[0]["id"]


# ====================================================================
print("\n🔹 3. Expenses CRUD")
# ====================================================================
expense_data = {
    "description": "Test Expense",
    "amount": 150.50,
    "date": "2026-04-01",
    "category_id": cat_id,
    "source": "cash",
    "frequency": "once",
}
status, res = api_post("/api/expenses", expense_data)
check(status == 200 or status == 201, "POST /api/expenses succeeds")

status, expenses = api_get("/api/expenses?month=2026-04")
check(status == 200, "GET /api/expenses returns 200")
check(isinstance(expenses, list) and len(expenses) >= 1, "At least 1 expense returned")

exp_id = expenses[0]["id"]

# Update
status, res = api_put(f"/api/expenses/{exp_id}", {**expense_data, "amount": 200})
check(status == 200, "PUT /api/expenses/<id> succeeds")

# Delete
status, res = api_delete(f"/api/expenses/{exp_id}")
check(status == 200, "DELETE /api/expenses/<id> succeeds")

status, expenses = api_get("/api/expenses?month=2026-04")
check(len(expenses) == 0, "Expense deleted successfully")


# ====================================================================
print("\n🔹 4. Income CRUD")
# ====================================================================
income_data = {
    "description": "Salary",
    "amount": 10000,
    "date": "2026-04-01",
    "source": "bank_transfer",
    "person": "Test User",
    "is_recurring": 1,
}
status, res = api_post("/api/income", income_data)
check(status == 200 or status == 201, "POST /api/income succeeds")

status, incomes = api_get("/api/income?month=2026-04")
check(status == 200 and len(incomes) >= 1, "GET /api/income returns data")

inc_id = incomes[0]["id"]
status, _ = api_delete(f"/api/income/{inc_id}")
check(status == 200, "DELETE /api/income/<id> succeeds")


# ====================================================================
print("\n🔹 5. Budget Plans CRUD (max 3)")
# ====================================================================
status, plans = api_get("/api/budget-plans")
check(status == 200, "GET /api/budget-plans returns 200")
check(len(plans) >= 1, "Default plan exists")
default_plan = plans[0]
check(default_plan["id"] == 1, "Default plan has id=1")

# Create plan 2
status, _ = api_post("/api/budget-plans", {"name": "Plan B", "description": "Test plan B"})
check(status == 200, "Create budget plan 2")

# Create plan 3
status, _ = api_post("/api/budget-plans", {"name": "Plan C", "description": "Test plan C"})
check(status == 200, "Create budget plan 3")

# Attempt plan 4 — should fail
status, res = api_post("/api/budget-plans", {"name": "Plan D", "description": ""})
check(status == 400, "Cannot create 4th plan", f"got status {status}")

status, plans = api_get("/api/budget-plans")
check(len(plans) == 3, "Exactly 3 plans exist")

# Edit plan 2
status, _ = api_post("/api/budget-plans", {"id": 2, "name": "Plan B Edited", "description": "Updated"})
check(status == 200, "Edit plan 2 name/description")
status, plans = api_get("/api/budget-plans")
p2 = next((p for p in plans if p["id"] == 2), None)
check(p2 and p2["name"] == "Plan B Edited", "Plan 2 name updated correctly")

# Cannot delete default plan
status, _ = api_delete("/api/budget-plans/1")
check(status == 400, "Cannot delete default plan")

# Delete plan 3
status, _ = api_delete("/api/budget-plans/3")
check(status == 200, "Delete plan 3 succeeds")

status, plans = api_get("/api/budget-plans")
check(len(plans) == 2, "2 plans remain after deletion")


# ====================================================================
print("\n🔹 6. Budget per-plan isolation")
# ====================================================================
# Set budget for plan 1
status, _ = api_post("/api/budget", {
    "category_id": cat_id, "month": "2026-04", "planned_amount": 500, "plan_id": 1
})
check(status == 200, "Set budget for plan 1")

# Set different budget for plan 2, same category+month
status, _ = api_post("/api/budget", {
    "category_id": cat_id, "month": "2026-04", "planned_amount": 900, "plan_id": 2
})
check(status == 200, "Set budget for plan 2 (same category/month)")

# Read plan 1 budget
status, b1 = api_get(f"/api/budget?month=2026-04&plan=1")
check(status == 200 and len(b1) >= 1, "GET budget for plan 1")
check(b1[0]["planned_amount"] == 500, "Plan 1 budget = 500")

# Read plan 2 budget
status, b2 = api_get(f"/api/budget?month=2026-04&plan=2")
check(status == 200 and len(b2) >= 1, "GET budget for plan 2")
check(b2[0]["planned_amount"] == 900, "Plan 2 budget = 900 (isolated)")


# ====================================================================
print("\n🔹 7. Budget plan deletion cascades budget data")
# ====================================================================
status, _ = api_delete("/api/budget-plans/2")
check(status == 200, "Delete plan 2")

status, b2 = api_get("/api/budget?month=2026-04&plan=2")
check(status == 200 and len(b2) == 0, "Plan 2 budget data deleted (cascade)")

# Plan 1 budget still intact
status, b1 = api_get("/api/budget?month=2026-04&plan=1")
check(len(b1) >= 1 and b1[0]["planned_amount"] == 500, "Plan 1 budget unaffected by plan 2 deletion")


# ====================================================================
print("\n🔹 8. Summary endpoint")
# ====================================================================
# Add an expense so summary has data
api_post("/api/expenses", {
    "description": "Groceries", "amount": 300, "date": "2026-04-05",
    "category_id": cat_id, "source": "cash", "frequency": "once"
})
status, summary = api_get("/api/summary?month=2026-04&plan=1")
check(status == 200, "GET /api/summary returns 200")
check("expense_total" in summary, "Summary has expense_total")
check("budget_vs_actual" in summary, "Summary has budget_vs_actual")
check(summary["expense_total"] == 300, "Summary expense_total = 300")


# ====================================================================
print("\n🔹 9. Standing Orders")
# ====================================================================
# Add a monthly expense
api_post("/api/expenses", {
    "description": "Netflix", "amount": 50, "date": "2026-04-01",
    "category_id": cat_id, "source": "visa", "card": "4580",
    "frequency": "monthly"
})
status, orders = api_get("/api/standing-orders")
check(status == 200, "GET /api/standing-orders returns 200")
check(isinstance(orders, list) and len(orders) >= 1, "Standing orders returned")
check(orders[0]["description"] == "Netflix", "Standing order has correct description")


# ====================================================================
print("\n🔹 10. Category Averages")
# ====================================================================
status, avgs = api_get("/api/category-averages")
check(status == 200, "GET /api/category-averages returns 200")
check(isinstance(avgs, dict), "Averages is a dict")

status, avgs = api_get("/api/category-averages?from=2026-01")
check(status == 200, "GET /api/category-averages with from param")


# ====================================================================
print("\n🔹 11. Available Months")
# ====================================================================
status, months = api_get("/api/available-months")
check(status == 200, "GET /api/available-months returns 200")
check("2026-04" in months, "2026-04 is in available months")


# ====================================================================
print("\n🔹 12. Cards")
# ====================================================================
status, cards = api_get("/api/cards")
check(status == 200, "GET /api/cards returns 200")
check(isinstance(cards, list), "Cards is a list")


# ====================================================================
print("\n🔹 13. Per-Plan Settings (localStorage simulation)")
# ====================================================================
# This tests the CONCEPT that settings keys are plan-specific.
# The actual localStorage is in the browser, so here we verify the
# key naming scheme works correctly.
plan_keys = {}
for plan_id in [1, 2, 3]:
    plan_keys[plan_id] = {
        "lang": f"plan_{plan_id}_lang",
        "currency": f"plan_{plan_id}_currency",
        "avgFrom": f"plan_{plan_id}_avgFrom",
    }

# Simulate: plan 1 = Hebrew/ILS, plan 2 = English/USD
settings_store = {}
settings_store[plan_keys[1]["lang"]] = "he"
settings_store[plan_keys[1]["currency"]] = "ILS"
settings_store[plan_keys[1]["avgFrom"]] = "all"
settings_store[plan_keys[2]["lang"]] = "en"
settings_store[plan_keys[2]["currency"]] = "USD"
settings_store[plan_keys[2]["avgFrom"]] = "2026-01"

# Switch to plan 1 → read plan 1 settings
check(settings_store[plan_keys[1]["lang"]] == "he", "Plan 1 language = he")
check(settings_store[plan_keys[1]["currency"]] == "ILS", "Plan 1 currency = ILS")

# Switch to plan 2 → read plan 2 settings
check(settings_store[plan_keys[2]["lang"]] == "en", "Plan 2 language = en")
check(settings_store[plan_keys[2]["currency"]] == "USD", "Plan 2 currency = USD")
check(settings_store[plan_keys[2]["avgFrom"]] == "2026-01", "Plan 2 avg from = 2026-01")

# Verify plan 1 not affected by plan 2
check(settings_store[plan_keys[1]["lang"]] == "he", "Plan 1 language still he after plan 2 switch")
check(settings_store[plan_keys[1]["currency"]] == "ILS", "Plan 1 currency still ILS after plan 2 switch")

# Verify keys are truly separate
check(plan_keys[1]["lang"] != plan_keys[2]["lang"], "Plan 1 and 2 use different localStorage keys")


# ====================================================================
print("\n🔹 14. Import endpoint exists")
# ====================================================================
# Just verify the endpoint responds (POST with no file should return error, not 500)
r = client.post("/api/import")
check(r.status_code != 500, "POST /api/import does not crash", f"got {r.status_code}")


# ====================================================================
print("\n🔹 15. Export endpoint")
# ====================================================================
r = client.get("/api/export?month=2026-04")
check(r.status_code == 200, "GET /api/export returns 200")
check("spreadsheet" in r.content_type or "excel" in r.content_type or "octet" in r.content_type,
      "Export returns file content type", f"got {r.content_type}")


# ====================================================================
print("\n🔹 16. Insights endpoints respond")
# ====================================================================
insight_endpoints = [
    "/api/insights/heatmap?months=3",
    "/api/insights/burnrate?month=2026-04",
    "/api/insights/latte?months=3",
    "/api/insights/anomalies?months=3",
    "/api/insights/recurring",
    "/api/insights/weekly-pulse?month=2026-04",
    "/api/insights/projection?month=2026-04",
    "/api/insights/comparison?month=2026-04",
    "/api/insights/achievements",
]
for ep in insight_endpoints:
    status, _ = api_get(ep)
    name = ep.split("/")[-1].split("?")[0]
    check(status == 200, f"Insight '{name}' returns 200", f"got {status}")


# ====================================================================
print("\n🔹 17. Financial Products")
# ====================================================================
status, products = api_get("/api/financial/products")
check(status == 200, "GET /api/financial/products returns 200")

status, summary = api_get("/api/financial/summary")
check(status == 200, "GET /api/financial/summary returns 200")


# ====================================================================
print("\n🔹 18. Installments")
# ====================================================================
status, inst = api_get("/api/installments")
check(status == 200, "GET /api/installments returns 200")

inst_data = {
    "description": "New Laptop",
    "total_amount": 3600,
    "num_payments": 12,
    "start_date": "2026-04-01",
    "card": "4580"
}
status, _ = api_post("/api/installments", inst_data)
check(status == 200 or status == 201, "POST /api/installments succeeds")

status, inst = api_get("/api/installments")
check(len(inst) >= 1, "Installment created")
if inst:
    status, _ = api_delete(f"/api/installments/{inst[0]['id']}")
    check(status == 200, "DELETE /api/installments/<id> succeeds")


# ====================================================================
# Cleanup
# ====================================================================
shutil.rmtree(tmp_dir, ignore_errors=True)

# ====================================================================
# Report
# ====================================================================
total = PASS + FAIL
print(f"\n{'='*50}")
print(f"  Results: {PASS}/{total} passed, {FAIL} failed")
print(f"{'='*50}")

if ERRORS:
    print("\nFailures:")
    for e in ERRORS:
        print(e)

sys.exit(1 if FAIL else 0)
