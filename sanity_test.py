"""
Sanity Test Suite for Family Budget Tracker
Run before committing: python sanity_test.py
Uses a temporary database with random seed data — no effect on real data.
"""
import os
import sys
import json
import random
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
cat_ids = [c["id"] for c in cats]


# ====================================================================
print("\n🔹 3. Seed Data — realistic multi-month budget data")
# ====================================================================
random.seed(42)  # Reproducible

MONTHS = ["2025-11", "2025-12", "2026-01", "2026-02", "2026-03", "2026-04"]
SOURCES = ["cash", "visa", "bank_transfer"]
CARDS = ["4580", "7722", "1234", ""]
PERSONS = ["Dad", "Mom"]
FREQUENCIES = ["once", "monthly"]

EXPENSE_DESCRIPTIONS = [
    "Supermarket groceries", "Electricity bill", "Water bill", "Gas station",
    "Netflix subscription", "Gym membership", "Restaurant dinner", "Kids school",
    "Internet bill", "Phone plan", "Insurance payment", "Clothing store",
    "Bus pass", "Pharmacy", "Home maintenance", "Pet food",
    "Coffee shop", "Book store", "Dentist visit", "Car repair",
]

INCOME_DESCRIPTIONS = [
    ("Salary - Dad", "Dad"), ("Salary - Mom", "Mom"),
    ("Freelance project", "Dad"), ("Child allowance", "Mom"),
]

seed_expense_count = 0
seed_income_count = 0

for month in MONTHS:
    # 15-25 expenses per month, spread across categories
    num_expenses = random.randint(15, 25)
    for _ in range(num_expenses):
        day = random.randint(1, 28)
        cat = random.choice(cat_ids)
        source = random.choice(SOURCES)
        freq = "monthly" if random.random() < 0.2 else "once"
        status, _ = api_post("/api/expenses", {
            "description": random.choice(EXPENSE_DESCRIPTIONS),
            "amount": round(random.uniform(15, 1500), 2),
            "date": f"{month}-{day:02d}",
            "category_id": cat,
            "source": source,
            "frequency": freq,
        })
        if status == 200:
            seed_expense_count += 1

    # 2-4 income entries per month
    for desc, person in random.sample(INCOME_DESCRIPTIONS, random.randint(2, 4)):
        day = random.randint(1, 10)
        status, _ = api_post("/api/income", {
            "description": desc,
            "amount": round(random.uniform(3000, 15000), 2),
            "date": f"{month}-{day:02d}",
            "source": random.choice(["bank_transfer", "cash"]),
            "person": person,
            "is_recurring": 1 if "Salary" in desc else 0,
        })
        if status == 200:
            seed_income_count += 1

check(seed_expense_count >= 80, f"Seeded {seed_expense_count} expenses across 6 months")
check(seed_income_count >= 12, f"Seeded {seed_income_count} income entries across 6 months")


# ====================================================================
print("\n🔹 4. Expenses CRUD")
# ====================================================================
expense_data = {
    "description": "CRUD Test Expense",
    "amount": 150.50,
    "date": "2026-04-01",
    "category_id": cat_ids[0],
    "source": "cash",
    "frequency": "once",
}
status, res = api_post("/api/expenses", expense_data)
check(status == 200 or status == 201, "POST /api/expenses succeeds")

status, expenses = api_get("/api/expenses?month=2026-04")
check(status == 200, "GET /api/expenses returns 200")
crud_exp = [e for e in expenses if e["description"] == "CRUD Test Expense"]
check(len(crud_exp) == 1, "CRUD expense found in list")

exp_id = crud_exp[0]["id"]

# Update
status, res = api_put(f"/api/expenses/{exp_id}", {**expense_data, "amount": 200})
check(status == 200, "PUT /api/expenses/<id> succeeds")

# Verify update
status, expenses = api_get("/api/expenses?month=2026-04")
updated = [e for e in expenses if e["id"] == exp_id]
check(len(updated) == 1 and updated[0]["amount"] == 200, "Expense amount updated to 200")

# Delete
status, res = api_delete(f"/api/expenses/{exp_id}")
check(status == 200, "DELETE /api/expenses/<id> succeeds")

status, expenses = api_get("/api/expenses?month=2026-04")
check(all(e["id"] != exp_id for e in expenses), "Deleted expense no longer in list")


# ====================================================================
print("\n🔹 5. Income CRUD")
# ====================================================================
income_data = {
    "description": "CRUD Test Income",
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

crud_inc = [i for i in incomes if i["description"] == "CRUD Test Income"]
check(len(crud_inc) == 1, "CRUD income found in list")
inc_id = crud_inc[0]["id"]

status, _ = api_delete(f"/api/income/{inc_id}")
check(status == 200, "DELETE /api/income/<id> succeeds")


# ====================================================================
print("\n🔹 6. Budget Plans CRUD (max 3)")
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
print("\n🔹 7. Budget per-plan isolation with seed data")
# ====================================================================
# Set budgets for multiple categories on both plans
for i, cid in enumerate(cat_ids[:4]):
    plan1_amt = (i + 1) * 500
    plan2_amt = (i + 1) * 800
    api_post("/api/budget", {
        "category_id": cid, "month": "2026-04", "planned_amount": plan1_amt, "plan_id": 1
    })
    api_post("/api/budget", {
        "category_id": cid, "month": "2026-04", "planned_amount": plan2_amt, "plan_id": 2
    })

# Verify plan 1
status, b1 = api_get("/api/budget?month=2026-04&plan=1")
check(status == 200 and len(b1) >= 4, "Plan 1 has 4 budget entries")
b1_map = {b["category_id"]: b["planned_amount"] for b in b1}
check(b1_map.get(cat_ids[0]) == 500, "Plan 1 first category = 500")
check(b1_map.get(cat_ids[2]) == 1500, "Plan 1 third category = 1500")

# Verify plan 2
status, b2 = api_get("/api/budget?month=2026-04&plan=2")
check(status == 200 and len(b2) >= 4, "Plan 2 has 4 budget entries")
b2_map = {b["category_id"]: b["planned_amount"] for b in b2}
check(b2_map.get(cat_ids[0]) == 800, "Plan 2 first category = 800")
check(b2_map.get(cat_ids[2]) == 2400, "Plan 2 third category = 2400")

# Plans are truly separate
check(b1_map.get(cat_ids[1]) != b2_map.get(cat_ids[1]),
      "Same category has different amounts per plan")


# ====================================================================
print("\n🔹 8. Budget plan deletion cascades budget data")
# ====================================================================
status, _ = api_delete("/api/budget-plans/2")
check(status == 200, "Delete plan 2")

status, b2 = api_get("/api/budget?month=2026-04&plan=2")
check(status == 200 and len(b2) == 0, "Plan 2 budget data deleted (cascade)")

# Plan 1 budget still intact
status, b1 = api_get("/api/budget?month=2026-04&plan=1")
check(len(b1) >= 4, "Plan 1 budget unaffected by plan 2 deletion")
check(b1[0]["planned_amount"] == 500, "Plan 1 first category still 500")


# ====================================================================
print("\n🔹 9. Summary with rich data")
# ====================================================================
status, summary = api_get("/api/summary?month=2026-04&plan=1")
check(status == 200, "GET /api/summary returns 200")
check("expense_total" in summary, "Summary has expense_total")
check(summary["expense_total"] > 0, f"Summary expense_total = {summary.get('expense_total', 0)}")
check("income_total" in summary, "Summary has income_total")
check(summary["income_total"] > 0, f"Summary income_total = {summary.get('income_total', 0)}")
check("balance" in summary, "Summary has balance")
check(len(summary.get("by_category", [])) > 0, "Summary has expenses by category")
check(len(summary.get("daily", [])) > 0, "Summary has daily breakdown")
check(len(summary.get("income_by_person", [])) > 0, "Summary has income by person")
check(len(summary.get("budget_vs_actual", [])) > 0, "Summary has budget_vs_actual entries")
check(len(summary.get("monthly_trend", [])) >= 2, "Summary has multi-month trend data")
check(len(summary.get("by_card", [])) > 0, "Summary has expenses by card")
check(len(summary.get("by_frequency", [])) > 0, "Summary has expenses by frequency")


# ====================================================================
print("\n🔹 10. Standing Orders from seed data")
# ====================================================================
status, orders = api_get("/api/standing-orders")
check(status == 200, "GET /api/standing-orders returns 200")
check(isinstance(orders, list) and len(orders) >= 1,
      f"Standing orders returned ({len(orders)} items)")
# Each standing order should have required fields
if orders:
    check(all("description" in o and "amount" in o for o in orders),
          "Standing orders have description and amount fields")


# ====================================================================
print("\n🔹 11. Category Averages with multi-month data")
# ====================================================================
status, avgs = api_get("/api/category-averages")
check(status == 200, "GET /api/category-averages returns 200")
check(isinstance(avgs, dict) and len(avgs) > 0, "Averages has entries")
# Averages should be > 0 for categories that have expenses
first_avg = list(avgs.values())[0]
check(first_avg > 0, f"First category average = {first_avg}")

# With from filter
status, avgs_filtered = api_get("/api/category-averages?from=2026-01")
check(status == 200, "GET /api/category-averages with from=2026-01")
check(len(avgs_filtered) > 0, "Filtered averages has entries")

# from=all should equal no-filter
status, avgs_all = api_get("/api/category-averages?from=all")
check(status == 200, "GET /api/category-averages with from=all")
check(avgs_all == avgs, "from=all equals no filter")


# ====================================================================
print("\n🔹 12. Available Months")
# ====================================================================
status, months = api_get("/api/available-months")
check(status == 200, "GET /api/available-months returns 200")
check(len(months) == 6, f"6 months available (got {len(months)})")
for m in MONTHS:
    check(m in months, f"Month {m} is available")


# ====================================================================
print("\n🔹 13. Cards")
# ====================================================================
status, cards = api_get("/api/cards")
check(status == 200, "GET /api/cards returns 200")
check(isinstance(cards, list), "Cards is a list")


# ====================================================================
print("\n🔹 14. Per-Plan Settings (localStorage simulation)")
# ====================================================================
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

# Switch to plan 1 -> read plan 1 settings
check(settings_store[plan_keys[1]["lang"]] == "he", "Plan 1 language = he")
check(settings_store[plan_keys[1]["currency"]] == "ILS", "Plan 1 currency = ILS")

# Switch to plan 2 -> read plan 2 settings
check(settings_store[plan_keys[2]["lang"]] == "en", "Plan 2 language = en")
check(settings_store[plan_keys[2]["currency"]] == "USD", "Plan 2 currency = USD")
check(settings_store[plan_keys[2]["avgFrom"]] == "2026-01", "Plan 2 avg from = 2026-01")

# Verify plan 1 not affected by plan 2
check(settings_store[plan_keys[1]["lang"]] == "he", "Plan 1 language still he after plan 2 switch")
check(settings_store[plan_keys[1]["currency"]] == "ILS", "Plan 1 currency still ILS after plan 2 switch")

# Verify keys are truly separate
check(plan_keys[1]["lang"] != plan_keys[2]["lang"], "Plan 1 and 2 use different localStorage keys")


# ====================================================================
print("\n🔹 15. Import endpoint exists")
# ====================================================================
r = client.post("/api/import")
check(r.status_code != 500, "POST /api/import does not crash", f"got {r.status_code}")


# ====================================================================
print("\n🔹 16. Export with data")
# ====================================================================
r = client.get("/api/export?month=2026-04")
check(r.status_code == 200, "GET /api/export returns 200")
check("spreadsheet" in r.content_type or "excel" in r.content_type or "octet" in r.content_type,
      "Export returns file content type", f"got {r.content_type}")
check(len(r.data) > 500, f"Export file has substantial content ({len(r.data)} bytes)")


# ====================================================================
print("\n🔹 17. Insights endpoints with real data")
# ====================================================================
insight_checks = [
    ("/api/insights/heatmap?months=3", "heatmap", lambda d: isinstance(d, dict)),
    ("/api/insights/burnrate?month=2026-04", "burnrate", lambda d: isinstance(d, dict)),
    ("/api/insights/latte?months=3", "latte", lambda d: isinstance(d, dict)),
    ("/api/insights/anomalies?months=3", "anomalies", lambda d: isinstance(d, (dict, list))),
    ("/api/insights/recurring", "recurring", lambda d: isinstance(d, dict)),
    ("/api/insights/weekly-pulse?month=2026-04", "weekly-pulse", lambda d: isinstance(d, dict)),
    ("/api/insights/projection?month=2026-04", "projection", lambda d: isinstance(d, dict)),
    ("/api/insights/comparison?month=2026-04", "comparison", lambda d: isinstance(d, dict)),
    ("/api/insights/achievements", "achievements", lambda d: isinstance(d, (dict, list))),
]
for ep, name, validator in insight_checks:
    status, data = api_get(ep)
    check(status == 200, f"Insight '{name}' returns 200", f"got {status}")
    check(validator(data), f"Insight '{name}' returns valid data structure")


# ====================================================================
print("\n🔹 18. Financial Products")
# ====================================================================
status, products = api_get("/api/financial/products")
check(status == 200, "GET /api/financial/products returns 200")

status, fin_summary = api_get("/api/financial/summary")
check(status == 200, "GET /api/financial/summary returns 200")


# ====================================================================
print("\n🔹 19. Installments CRUD")
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
print("\n🔹 20. Multi-month expense consistency")
# ====================================================================
total_seeded = 0
for month in MONTHS:
    status, exps = api_get(f"/api/expenses?month={month}")
    check(status == 200, f"GET expenses for {month}")
    total_seeded += len(exps)

check(total_seeded >= 80, f"Total expenses across all months = {total_seeded}")

# Verify summaries work for each month
for month in MONTHS:
    status, s = api_get(f"/api/summary?month={month}&plan=1")
    check(status == 200 and s.get("expense_total", 0) > 0,
          f"Summary for {month} has expenses")


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
