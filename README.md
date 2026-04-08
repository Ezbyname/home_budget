# Home Budget Tracker

A family budget management app with Hebrew and English support. Track expenses, income, budgets, insurance, savings, and more — all from a clean web interface.

## Features

- **Dashboard** — monthly overview with charts, trends, and budget vs. actual
- **Expenses & Income** — add, edit, delete with category tagging and recurring support
- **Budget Plans** — up to 3 separate budget plans with per-plan settings
- **Insurance & Savings** — track financial products, installments, auto-detect from bank data
- **Insights** — heatmaps, burn rate, anomaly detection, weekly pulse, projections
- **AI Analysis** — smart tips and pattern recognition
- **Import/Export** — Excel import and export
- **Interactive Tutorial** — animated walkthrough for new users
- **Login System** — signup/login with admin panel for user and category management
- **Bilingual** — full Hebrew (RTL) and English support, switchable anytime

## Getting Started

### Windows (easiest)

1. Go to the [Latest Release](https://github.com/Ezbyname/home_budget/releases/latest)
2. Download **`expense-tracker.exe`**
3. Double-click to run — the app opens in your browser automatically

No Python or installation needed. Just download and run.

> **Build from source (optional):**
> ```bash
> pip install -r requirements.txt
> pip install pyinstaller
> pyinstaller --onefile --name expense-tracker --add-data "static;static" app.py
> ```

### Mac

1. Clone the repo:
   ```bash
   git clone https://github.com/Ezbyname/home_budget.git
   cd home_budget
   ```

2. Double-click **`start_mac.command`** — it installs dependencies and opens the app in your browser.

   Or run manually:
   ```bash
   pip3 install -r requirements.txt
   python3 app.py
   ```
   Then open `http://127.0.0.1:5000` in your browser.

### Linux

```bash
git clone https://github.com/Ezbyname/home_budget.git
cd home_budget
pip3 install -r requirements.txt
python3 app.py
```

Open `http://127.0.0.1:5000` in your browser.

## Requirements

- Python 3.10+
- Flask, xlrd, openpyxl (installed automatically via `requirements.txt`)

## Admin Setup

To create an admin user, place a file at `~/.budget_tracker_secrets.json`:

```json
{
    "ADMIN_EMAIL": "your@email.com",
    "ADMIN_PASSWORD": "your_password",
    "ADMIN_USERNAME": "admin"
}
```

The admin user is auto-created on first startup. The admin panel allows managing users and categories.

## Running Tests

```bash
python sanity_test.py
```

Runs 164 tests covering all API endpoints, auth flows, and data integrity.

## Tech Stack

- **Backend:** Flask, SQLite (WAL mode)
- **Frontend:** Bootstrap 5.3.3 (RTL), Chart.js, vanilla JS
- **Packaging:** PyInstaller (Windows exe), Inno Setup (installer)
