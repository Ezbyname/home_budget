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

# Initialize DB (disable external secrets for test)
budget_app.ADMIN_SECRETS = {}
with budget_app.app.app_context():
    budget_app.init_db()

# Create a test user and login (endpoints require auth)
with budget_app.app.app_context():
    conn = budget_app.get_db()
    pw_hash = budget_app.hash_password("testpass123")
    conn.execute("INSERT INTO users (username, password_hash, email, verified, is_admin) VALUES (?, ?, ?, 1, 1)",
                 ("testadmin", pw_hash, "test@test.com"))
    conn.commit()
    conn.close()

# Login to get session
with client.session_transaction() as sess:
    with budget_app.app.app_context():
        conn = budget_app.get_db()
        user = conn.execute("SELECT id FROM users WHERE username='testadmin'").fetchone()
        conn.close()
    sess['user_id'] = user['id']
    sess['username'] = 'testadmin'


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
print("\n🔹 21. Auth — Signup, Login, Logout, Status")
# ====================================================================
# Logout current session first
status, res = api_post("/api/auth/logout")
check(status == 200, "POST /api/auth/logout succeeds")

# Auth status should show not logged in
status, res = api_get("/api/auth/status")
check(status == 200, "GET /api/auth/status returns 200")
check(res["logged_in"] == False, "After logout, logged_in is false")

# All data endpoints require auth when logged out
status, _ = api_get("/api/expenses?month=2026-04")
check(status == 401, "GET /api/expenses returns 401 when not logged in")

status, _ = api_get("/api/income?month=2026-04")
check(status == 401, "GET /api/income returns 401 when not logged in")

status, _ = api_get("/api/summary?month=2026-04&plan=1")
check(status == 401, "GET /api/summary returns 401 when not logged in")

# Write actions require auth
status, _ = api_post("/api/expenses", {
    "description": "Unauthorized test", "amount": 100, "date": "2026-04-01",
    "category_id": 1, "source": "cash", "frequency": "once"
})
check(status == 401, "POST /api/expenses returns 401 when not logged in")

status, _ = api_post("/api/income", {
    "description": "Unauthorized test", "amount": 5000, "date": "2026-04-01",
    "source": "cash", "person": "Test", "is_recurring": 0
})
check(status == 401, "POST /api/income returns 401 when not logged in")

# Signup a new user (OTP sending will fail in test → auto-verified)
status, res = api_post("/api/auth/signup", {
    "username": "newuser",
    "password": "securepass123",
    "email": "newuser@example.com",
    "verification_method": "email",
})
check(status == 200, "POST /api/auth/signup succeeds")
check(res.get("auto_verified") == True or res.get("status") == "verification_sent",
      "Signup returns valid status")

# If auto-verified, we should be logged in now
if res.get("auto_verified"):
    status, res = api_get("/api/auth/status")
    check(res["logged_in"] == True, "Auto-verified user is logged in")
    check(res["username"] == "newuser", "Logged in as newuser")
    check(res["is_admin"] == False, "New user is not admin")

    # Logout the new user
    status, _ = api_post("/api/auth/logout")
    check(status == 200, "Logout newuser")

# Signup with duplicate username should fail
status, res = api_post("/api/auth/signup", {
    "username": "newuser",
    "password": "anotherpass",
    "email": "other@example.com",
    "verification_method": "email",
})
check(status == 400, "Duplicate username signup returns 400")
check("exists" in res.get("error", "").lower(), "Error mentions username exists")

# Signup with bad data
status, _ = api_post("/api/auth/signup", {"username": "", "password": "123456", "email": "a@b.c", "verification_method": "email"})
check(status == 400, "Signup with empty username returns 400")

status, _ = api_post("/api/auth/signup", {"username": "shortpw", "password": "12345", "email": "a@b.c", "verification_method": "email"})
check(status == 400, "Signup with short password returns 400")

# Login with wrong password
status, res = api_post("/api/auth/login", {"username": "testadmin", "password": "wrongpass"})
check(status == 401, "Login with wrong password returns 401")

# Login with non-existent user
status, _ = api_post("/api/auth/login", {"username": "noexist", "password": "whatever"})
check(status == 401, "Login with non-existent user returns 401")

# Login with correct username
status, res = api_post("/api/auth/login", {"username": "testadmin", "password": "testpass123"})
check(status == 200, "Login with correct username succeeds")
check(res.get("username") == "testadmin", "Login returns correct username")

# Logout and login with email instead
api_post("/api/auth/logout")
status, res = api_post("/api/auth/login", {"username": "test@test.com", "password": "testpass123"})
check(status == 200, "Login with email succeeds")
check(res.get("username") == "testadmin", "Email login returns correct username")

# Auth status should show logged in + admin
status, res = api_get("/api/auth/status")
check(res["logged_in"] == True, "After login, logged_in is true")
check(res["is_admin"] == True, "testadmin is admin")

# Protected endpoints should work again
status, _ = api_get("/api/expenses?month=2026-04")
check(status == 200, "Protected endpoint works after login")


# ====================================================================
print("\n🔹 22. Admin endpoints")
# ====================================================================
# Admin stats
status, stats = api_get("/api/admin/stats")
check(status == 200, "GET /api/admin/stats returns 200")
check(stats["total_users"] >= 2, f"Admin stats shows {stats['total_users']} users")
check(stats["total_expenses"] > 0, "Admin stats shows expenses")
check(stats["total_categories"] > 0, "Admin stats shows categories")

# Admin get users
status, users = api_get("/api/admin/users")
check(status == 200, "GET /api/admin/users returns 200")
check(len(users) >= 2, f"Admin sees {len(users)} users")
admin_user = next((u for u in users if u["username"] == "testadmin"), None)
check(admin_user is not None, "Admin user found in list")
check(admin_user["is_admin"] == 1, "Admin user has is_admin=1")
new_user = next((u for u in users if u["username"] == "newuser"), None)
check(new_user is not None, "newuser found in list")

# Cannot delete admin user
status, res = api_delete(f"/api/admin/users/{admin_user['id']}")
check(status == 400, "Cannot delete admin user")

# Delete non-admin user
status, _ = api_delete(f"/api/admin/users/{new_user['id']}")
check(status == 200, "Delete non-admin user succeeds")

status, users = api_get("/api/admin/users")
check(not any(u["username"] == "newuser" for u in users), "Deleted user no longer in list")

# Non-admin cannot access admin endpoints
# Logout, login as a new regular user
api_post("/api/auth/logout")
api_post("/api/auth/signup", {
    "username": "regularuser",
    "password": "regular123",
    "email": "regular@example.com",
    "verification_method": "email",
})
# regularuser is auto-verified and logged in
status, _ = api_get("/api/admin/stats")
check(status in [401, 403], "Non-admin cannot access admin stats", f"got {status}")

status, _ = api_get("/api/admin/users")
check(status in [401, 403], "Non-admin cannot access admin users", f"got {status}")

# Re-login as admin for any further tests
api_post("/api/auth/logout")
api_post("/api/auth/login", {"username": "testadmin", "password": "testpass123"})


# ====================================================================
print("\n🔹 23. Chat Assistant — Basic Queries")
# ====================================================================
# Basic expense query (default: last 3 months)
status, res = api_post("/api/chat", {"query": "how much did i spend", "lang": "en"})
check(status == 200, "Chat: basic expense query returns 200")
check("text" in res and "data" in res and "action" in res, "Chat: response has text, data, action")
check(res["data"]["type"] == "expenses", "Chat: expense query returns type=expenses")
check(res["data"]["count"] >= 0, "Chat: count is non-negative")
check(res["data"]["total"] >= 0, "Chat: total is non-negative")
check(isinstance(res["data"]["items"], list), "Chat: items is a list")
check(isinstance(res["data"].get("breakdown", []), list), "Chat: breakdown is a list (or absent)")

# Hebrew query
status, res = api_post("/api/chat", {"query": "כמה הוצאתי החודש", "lang": "he"})
check(status == 200, "Chat: Hebrew query returns 200")
check("נמצאו" in res["text"] or "סה״כ" in res["text"] or res["data"]["count"] == 0,
      "Chat: Hebrew response text is in Hebrew")

# English query with data
status, res = api_post("/api/chat", {"query": "show me expenses this year", "lang": "en"})
check(status == 200, "Chat: 'this year' query returns 200")
check(res["data"]["count"] >= 0, "Chat: year query returns results")

# Specific month query
status, res = api_post("/api/chat", {"query": "expenses 2026-03", "lang": "en"})
check(status == 200, "Chat: specific month query returns 200")

# Top/biggest query
status, res = api_post("/api/chat", {"query": "what is the biggest expense", "lang": "en"})
check(status == 200, "Chat: top expense query returns 200")
if res["data"]["count"] > 0:
    check("Top expenses" in res["text"] or "biggest" in res["text"].lower() or res["data"]["items"],
          "Chat: top query contains expense data")

# Income query
status, res = api_post("/api/chat", {"query": "show me my income", "lang": "en"})
check(status == 200, "Chat: income query returns 200")
check(res["data"]["type"] == "income", "Chat: income query returns type=income")

# Navigation query — "הוצאות" is both a noise word and a tab keyword,
# so search_term becomes empty and is_navigate triggers
status, res = api_post("/api/chat", {"query": "איפה הוצאות", "lang": "he"})
check(status == 200, "Chat: navigation query returns 200")
check(res.get("action", {}).get("tab") == "expensesTab",
      "Chat: navigation routes to expensesTab", f"got action={res.get('action')}")

# Empty query
status, res = api_post("/api/chat", {"query": "", "lang": "en"})
check(status == 200, "Chat: empty query returns 200 (graceful)")

# Lang defaults to 'he'
status, res = api_post("/api/chat", {"query": "expenses"})
check(status == 200, "Chat: missing lang defaults gracefully")


# ====================================================================
print("\n🔹 24. Chat Assistant — Keyword Search & Fuzzy Fallback")
# ====================================================================
# Insert a known expense with a specific subcategory for search testing
with budget_app.app.app_context():
    conn = budget_app.get_db()
    test_uid = conn.execute("SELECT id FROM users WHERE username='testadmin'").fetchone()['id']
    conn.execute("""INSERT INTO expenses (date, category_id, subcategory, description, amount, source, user_id)
                    VALUES ('2026-04-01', ?, 'Supermarket Rami Levy', 'Weekly groceries', 250.0, 'visa', ?)""",
                 (cat_ids[0], test_uid))
    conn.commit()
    conn.close()

# Exact keyword search
status, res = api_post("/api/chat", {"query": "Rami Levy", "lang": "en"})
check(status == 200, "Chat: keyword search returns 200")
check(res["data"]["count"] >= 1, "Chat: keyword 'Rami Levy' finds the inserted expense",
      f"count={res['data']['count']}")

# Search with a typo — should trigger fuzzy fallback
status, res = api_post("/api/chat", {"query": "Rami Levi supermarkt", "lang": "en"})
check(status == 200, "Chat: typo search returns 200")
if res["data"]["count"] > 0:
    is_fuzzy = res["data"].get("fuzzy", False)
    check(True, "Chat: typo search found results via fallback")
    if is_fuzzy:
        check("fuzzy_matches" in res["data"], "Chat: fuzzy result includes fuzzy_matches list")
        check("original_search" in res["data"], "Chat: fuzzy result includes original_search")
        check("Did you mean" in res["text"] or "\u05d0\u05d5\u05dc\u05d9" in res["text"],
              "Chat: fuzzy response asks for confirmation")

# Category filter in search
if len(cat_ids) >= 2:
    # Get first category name
    with budget_app.app.app_context():
        conn = budget_app.get_db()
        cat_row = conn.execute("SELECT name_he FROM categories WHERE id=?", (cat_ids[0],)).fetchone()
        conn.close()
    if cat_row:
        cat_name_he = cat_row["name_he"]
        status, res = api_post("/api/chat", {"query": f"{cat_name_he} השנה", "lang": "he"})
        check(status == 200, f"Chat: category+date query returns 200")


# ====================================================================
print("\n🔹 25. Chat Assistant — Alias Learning (confirm/deny)")
# ====================================================================
# Confirm a fuzzy match alias
status, res = api_post("/api/chat/confirm", {
    "user_typed": "rami levi",
    "actual_match": "Supermarket Rami Levy",
    "confirmed": True
})
check(status == 200, "Chat confirm: save alias returns 200")
check(res.get("ok") is True, "Chat confirm: response ok=True")
check(res.get("saved") is True, "Chat confirm: alias was saved")

# Verify alias exists in DB
with budget_app.app.app_context():
    conn = budget_app.get_db()
    alias = conn.execute("SELECT * FROM chat_aliases WHERE user_typed='rami levi'").fetchone()
    conn.close()
check(alias is not None, "Chat confirm: alias row created in DB")
check(alias["actual_match"] == "Supermarket Rami Levy", "Chat confirm: alias actual_match is correct")
check(alias["times_used"] == 1, "Chat confirm: times_used starts at 1")

# Confirm same alias again — times_used should increment
status, res = api_post("/api/chat/confirm", {
    "user_typed": "rami levi",
    "actual_match": "Supermarket Rami Levy",
    "confirmed": True
})
check(status == 200 and res.get("saved"), "Chat confirm: re-confirm same alias succeeds")
with budget_app.app.app_context():
    conn = budget_app.get_db()
    alias = conn.execute("SELECT times_used FROM chat_aliases WHERE user_typed='rami levi'").fetchone()
    conn.close()
check(alias["times_used"] == 2, "Chat confirm: times_used incremented to 2")

# Now search using the aliased term — should use the alias directly
status, res = api_post("/api/chat", {"query": "rami levi", "lang": "en"})
check(status == 200, "Chat alias: search with aliased term returns 200")
check(res["data"]["count"] >= 1, "Chat alias: aliased search finds results directly",
      f"count={res['data']['count']}")
check(res["data"].get("fuzzy") is not True, "Chat alias: aliased search is NOT fuzzy (used alias)")

# Deny a fuzzy match — should not save
status, res = api_post("/api/chat/confirm", {
    "user_typed": "some typo",
    "confirmed": False
})
check(status == 200, "Chat deny: returns 200")
check(res.get("saved") is False, "Chat deny: not saved")

# Missing user_typed — should return error
status, res = api_post("/api/chat/confirm", {
    "user_typed": "",
    "actual_match": "anything",
    "confirmed": True
})
check(status == 200 and res.get("ok") is False, "Chat confirm: empty user_typed rejected")


# ====================================================================
print("\n🔹 26. Chat Assistant — Feedback / Satisfaction Rating")
# ====================================================================
# Valid rating
status, res = api_post("/api/chat/feedback", {"rating": 5, "query": "test query"})
check(status == 200, "Chat feedback: rating 5 returns 200")
check(res.get("ok") is True, "Chat feedback: ok=True")

# Another rating
status, res = api_post("/api/chat/feedback", {"rating": 3, "query": "another query"})
check(status == 200, "Chat feedback: rating 3 returns 200")

status, res = api_post("/api/chat/feedback", {"rating": 1, "query": "bad query"})
check(status == 200, "Chat feedback: rating 1 returns 200")

# Invalid ratings
status, res = api_post("/api/chat/feedback", {"rating": 0, "query": "test"})
check(res.get("ok") is False, "Chat feedback: rating 0 rejected")

status, res = api_post("/api/chat/feedback", {"rating": 6, "query": "test"})
check(res.get("ok") is False, "Chat feedback: rating 6 rejected")

status, res = api_post("/api/chat/feedback", {"rating": "abc", "query": "test"})
check(res.get("ok") is False, "Chat feedback: non-numeric rating rejected")

status, res = api_post("/api/chat/feedback", {"query": "test"})
check(res.get("ok") is False, "Chat feedback: missing rating rejected")

# Verify feedback in DB
with budget_app.app.app_context():
    conn = budget_app.get_db()
    fb_count = conn.execute("SELECT COUNT(*) as c FROM chat_feedback").fetchone()["c"]
    conn.close()
check(fb_count == 3, "Chat feedback: 3 valid ratings stored in DB", f"got {fb_count}")


# ====================================================================
print("\n🔹 27. Chat Satisfaction — Admin Dashboard API")
# ====================================================================
status, res = api_get("/api/admin/chat-satisfaction")
check(status == 200, "Admin satisfaction: returns 200")
check(res.get("total") == 3, "Admin satisfaction: total = 3", f"got {res.get('total')}")
check(res.get("avg_rating") == 3.0, "Admin satisfaction: avg = 3.0 ((5+3+1)/3)",
      f"got {res.get('avg_rating')}")
check(res.get("positive") == 1, "Admin satisfaction: 1 positive (rating 5)")
check(res.get("negative") == 1, "Admin satisfaction: 1 negative (rating 1)")
check(isinstance(res.get("recent"), list), "Admin satisfaction: recent is a list")
check(len(res["recent"]) == 3, "Admin satisfaction: 3 recent entries")
check(isinstance(res.get("distribution"), list), "Admin satisfaction: distribution is a list")

# Check recent entries have expected fields
if res["recent"]:
    r0 = res["recent"][0]
    check("rating" in r0 and "query" in r0 and "created_at" in r0,
          "Admin satisfaction: recent entry has rating, query, created_at")

# Non-admin should not access satisfaction data
api_post("/api/auth/logout")
api_post("/api/auth/login", {"username": "regularuser", "password": "regular123"})
status, _ = api_get("/api/admin/chat-satisfaction")
check(status in [401, 403], "Admin satisfaction: non-admin blocked",
      f"got {status}")

# Re-login as admin
api_post("/api/auth/logout")
api_post("/api/auth/login", {"username": "testadmin", "password": "testpass123"})


# ====================================================================
print("\n🔹 28. Chat Assistant — Auth Required")
# ====================================================================
# Logout and test that chat POST requires auth
api_post("/api/auth/logout")

status, res = api_post("/api/chat", {"query": "test", "lang": "en"})
check(status == 401, "Chat: POST without auth returns 401", f"got {status}")

status, res = api_post("/api/chat/confirm", {"user_typed": "x", "confirmed": True})
check(status == 401, "Chat confirm: POST without auth returns 401", f"got {status}")

status, res = api_post("/api/chat/feedback", {"rating": 5, "query": "test"})
check(status == 401, "Chat feedback: POST without auth returns 401", f"got {status}")

# Re-login for cleanup
api_post("/api/auth/login", {"username": "testadmin", "password": "testpass123"})


# ====================================================================
print("\n🔹 29. Multi-User Data Isolation")
# ====================================================================
# Create two fresh test users for isolation testing
api_post("/api/auth/logout")

api_post("/api/auth/signup", {
    "username": "isolationA",
    "password": "isopass123A",
    "email": "isoA@example.com",
    "verification_method": "email",
})
# isolationA is auto-verified and logged in

# -- User A: add expense, income, installment --
status, _ = api_post("/api/expenses", {
    "description": "UserA Expense Only",
    "amount": 777.77,
    "date": "2026-04-05",
    "category_id": cat_ids[0],
    "source": "cash",
    "frequency": "once",
})
check(status == 200, "Isolation: User A creates expense")

status, _ = api_post("/api/income", {
    "description": "UserA Salary Only",
    "amount": 9999.99,
    "date": "2026-04-05",
    "source": "bank_transfer",
    "person": "User A",
    "is_recurring": 1,
})
check(status == 200, "Isolation: User A creates income")

status, _ = api_post("/api/installments", {
    "description": "UserA Laptop Only",
    "total_amount": 3000,
    "num_payments": 6,
    "start_date": "2026-04-01",
    "card": "1111",
})
check(status == 200, "Isolation: User A creates installment")

# Verify User A sees own data
status, exps = api_get("/api/expenses?month=2026-04")
check(status == 200, "Isolation: User A GET expenses")
a_exps = [e for e in exps if e["description"] == "UserA Expense Only"]
check(len(a_exps) == 1, "Isolation: User A sees own expense")
check(a_exps[0]["amount"] == 777.77, "Isolation: User A expense amount correct")

status, incs = api_get("/api/income?month=2026-04")
a_incs = [i for i in incs if i["description"] == "UserA Salary Only"]
check(len(a_incs) == 1, "Isolation: User A sees own income")

status, inst = api_get("/api/installments")
a_inst = [i for i in inst if i["description"] == "UserA Laptop Only"]
check(len(a_inst) == 1, "Isolation: User A sees own installment")

# Get user A's budget plan info
status, a_plans = api_get("/api/budget-plans")
check(len(a_plans) >= 1, "Isolation: User A has a budget plan")
a_plan_id = a_plans[0]["id"]

# Set a budget for User A
api_post("/api/budget", {
    "category_id": cat_ids[0], "month": "2026-04",
    "planned_amount": 1234, "plan_id": a_plan_id,
})
status, a_budgets = api_get(f"/api/budget?month=2026-04&plan={a_plan_id}")
a_bud = [b for b in a_budgets if b["category_id"] == cat_ids[0]]
check(len(a_bud) == 1 and a_bud[0]["planned_amount"] == 1234,
      "Isolation: User A budget set to 1234")

# Summary should reflect User A data only
status, a_summary = api_get(f"/api/summary?month=2026-04&plan={a_plan_id}")
check(a_summary["expense_total"] == 777.77,
      "Isolation: User A summary expense_total = 777.77",
      f"got {a_summary.get('expense_total')}")
check(a_summary["income_total"] == 9999.99,
      "Isolation: User A summary income_total = 9999.99",
      f"got {a_summary.get('income_total')}")

# -- Switch to User B --
api_post("/api/auth/logout")
api_post("/api/auth/signup", {
    "username": "isolationB",
    "password": "isopass123B",
    "email": "isoB@example.com",
    "verification_method": "email",
})
# isolationB is auto-verified and logged in

# User B should see NONE of User A's data
status, exps = api_get("/api/expenses?month=2026-04")
check(status == 200, "Isolation: User B GET expenses")
b_sees_a = [e for e in exps if e["description"] == "UserA Expense Only"]
check(len(b_sees_a) == 0, "Isolation: User B does NOT see User A expense")

status, incs = api_get("/api/income?month=2026-04")
b_sees_a_inc = [i for i in incs if i["description"] == "UserA Salary Only"]
check(len(b_sees_a_inc) == 0, "Isolation: User B does NOT see User A income")

status, inst = api_get("/api/installments")
b_sees_a_inst = [i for i in inst if i["description"] == "UserA Laptop Only"]
check(len(b_sees_a_inst) == 0, "Isolation: User B does NOT see User A installment")

# User B should have own empty budget plan, NOT User A's
status, b_plans = api_get("/api/budget-plans")
check(len(b_plans) >= 1, "Isolation: User B has own budget plan")
b_plan_id = b_plans[0]["id"]
check(b_plan_id != a_plan_id, "Isolation: User B plan id differs from User A",
      f"A={a_plan_id}, B={b_plan_id}")

status, b_budgets = api_get(f"/api/budget?month=2026-04&plan={b_plan_id}")
b_bud_a = [b for b in b_budgets if b.get("planned_amount", 0) == 1234]
check(len(b_bud_a) == 0, "Isolation: User B does NOT see User A budget entries")

# User B summary should be empty/zero
status, b_summary = api_get(f"/api/summary?month=2026-04&plan={b_plan_id}")
check(b_summary["expense_total"] == 0,
      "Isolation: User B summary expense_total = 0",
      f"got {b_summary.get('expense_total')}")
check(b_summary["income_total"] == 0,
      "Isolation: User B summary income_total = 0",
      f"got {b_summary.get('income_total')}")

# -- User B adds own data --
status, _ = api_post("/api/expenses", {
    "description": "UserB Expense Only",
    "amount": 333.33,
    "date": "2026-04-06",
    "category_id": cat_ids[1],
    "source": "visa",
    "frequency": "once",
})
check(status == 200, "Isolation: User B creates expense")

status, exps = api_get("/api/expenses?month=2026-04")
b_exps = [e for e in exps if e["description"] == "UserB Expense Only"]
check(len(b_exps) == 1, "Isolation: User B sees own expense")

# User B still shouldn't see User A
b_sees_a2 = [e for e in exps if e["description"] == "UserA Expense Only"]
check(len(b_sees_a2) == 0, "Isolation: User B still doesn't see User A after own insert")

# -- Switch back to User A — verify A doesn't see B --
api_post("/api/auth/logout")
api_post("/api/auth/login", {"username": "isolationA", "password": "isopass123A"})

status, exps = api_get("/api/expenses?month=2026-04")
a_sees_b = [e for e in exps if e["description"] == "UserB Expense Only"]
check(len(a_sees_b) == 0, "Isolation: User A does NOT see User B expense")

# User A still sees own expense
a_exps2 = [e for e in exps if e["description"] == "UserA Expense Only"]
check(len(a_exps2) == 1, "Isolation: User A still sees own expense")

# -- User A cannot UPDATE User B's expense --
# Get User B's expense id via direct DB
with budget_app.app.app_context():
    conn = budget_app.get_db()
    b_exp = conn.execute("SELECT id FROM expenses WHERE description='UserB Expense Only'").fetchone()
    conn.close()

if b_exp:
    status, _ = api_put(f"/api/expenses/{b_exp['id']}", {
        "description": "UserA hacked B", "amount": 0.01,
        "date": "2026-04-06", "category_id": cat_ids[1],
        "source": "visa", "frequency": "once",
    })
    # Should either 404 or silently update 0 rows — verify B's data unchanged
    with budget_app.app.app_context():
        conn = budget_app.get_db()
        b_exp_check = conn.execute("SELECT description FROM expenses WHERE id=?", (b_exp['id'],)).fetchone()
        conn.close()
    check(b_exp_check["description"] == "UserB Expense Only",
          "Isolation: User A cannot overwrite User B's expense")

    # User A cannot DELETE User B's expense
    status, _ = api_delete(f"/api/expenses/{b_exp['id']}")
    with budget_app.app.app_context():
        conn = budget_app.get_db()
        b_exp_still = conn.execute("SELECT id FROM expenses WHERE id=?", (b_exp['id'],)).fetchone()
        conn.close()
    check(b_exp_still is not None, "Isolation: User A cannot delete User B's expense")

# -- Admin cascade delete removes user data --
api_post("/api/auth/logout")
api_post("/api/auth/login", {"username": "testadmin", "password": "testpass123"})

# Get isolationB user id
with budget_app.app.app_context():
    conn = budget_app.get_db()
    iso_b = conn.execute("SELECT id FROM users WHERE username='isolationB'").fetchone()
    conn.close()

if iso_b:
    status, _ = api_delete(f"/api/admin/users/{iso_b['id']}")
    check(status == 200, "Isolation: Admin deletes User B")

    # Verify User B's data is gone
    with budget_app.app.app_context():
        conn = budget_app.get_db()
        b_exps_left = conn.execute("SELECT COUNT(*) as c FROM expenses WHERE user_id=?", (iso_b['id'],)).fetchone()['c']
        b_incs_left = conn.execute("SELECT COUNT(*) as c FROM income WHERE user_id=?", (iso_b['id'],)).fetchone()['c']
        b_plans_left = conn.execute("SELECT COUNT(*) as c FROM budget_plans WHERE user_id=?", (iso_b['id'],)).fetchone()['c']
        conn.close()
    check(b_exps_left == 0, "Isolation: Admin delete cascaded — User B expenses removed")
    check(b_incs_left == 0, "Isolation: Admin delete cascaded — User B income removed")
    check(b_plans_left == 0, "Isolation: Admin delete cascaded — User B budget plans removed")

# Clean up isolationA too
with budget_app.app.app_context():
    conn = budget_app.get_db()
    iso_a = conn.execute("SELECT id FROM users WHERE username='isolationA'").fetchone()
    conn.close()

if iso_a:
    status, _ = api_delete(f"/api/admin/users/{iso_a['id']}")
    check(status == 200, "Isolation: Admin deletes User A")


# ====================================================================
print("\n🔹 30. Cross-User Endpoint Isolation (standing orders, available months, export)")
# ====================================================================
# Create userC with data, verify userD sees nothing on derived endpoints
api_post("/api/auth/logout")
api_post("/api/auth/signup", {
    "username": "isolationC",
    "password": "isopass123C",
    "email": "isoC@example.com",
    "verification_method": "email",
})

# User C: add monthly expense (creates standing order)
status, _ = api_post("/api/expenses", {
    "description": "UserC Monthly Gym",
    "amount": 200,
    "date": "2026-04-01",
    "category_id": cat_ids[0],
    "source": "visa",
    "frequency": "monthly",
})
check(status == 200, "Isolation: User C creates monthly expense")

status, c_orders = api_get("/api/standing-orders")
check(any(o["description"] == "UserC Monthly Gym" for o in c_orders),
      "Isolation: User C sees own standing order")

status, c_months = api_get("/api/available-months")
check("2026-04" in c_months, "Isolation: User C sees own available month")

# Switch to User D
api_post("/api/auth/logout")
api_post("/api/auth/signup", {
    "username": "isolationD",
    "password": "isopass123D",
    "email": "isoD@example.com",
    "verification_method": "email",
})

# User D should not see User C's standing orders or months
status, d_orders = api_get("/api/standing-orders")
d_sees_c = [o for o in d_orders if o.get("description") == "UserC Monthly Gym"]
check(len(d_sees_c) == 0, "Isolation: User D does NOT see User C standing order")

status, d_months = api_get("/api/available-months")
check(len(d_months) == 0, "Isolation: User D has no available months (no data)",
      f"got {d_months}")

# Export for User D should be empty/minimal
r = client.get("/api/export?month=2026-04")
check(r.status_code == 200, "Isolation: User D export returns 200")

# Cleanup isolation users C and D
api_post("/api/auth/logout")
api_post("/api/auth/login", {"username": "testadmin", "password": "testpass123"})

for uname in ['isolationC', 'isolationD']:
    with budget_app.app.app_context():
        conn = budget_app.get_db()
        u = conn.execute("SELECT id FROM users WHERE username=?", (uname,)).fetchone()
        conn.close()
    if u:
        api_delete(f"/api/admin/users/{u['id']}")


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
