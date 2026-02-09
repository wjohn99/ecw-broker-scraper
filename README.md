# ECW Broker Scraper

Python tools to collect Express Car Wash (ECW) broker contact data into a Google Sheet using Playwright for browser automation and Pandas for data handling.

Each target website gets its **own scraper script** (for example, `ibba_scraper.py` for the IBBA directory).

---

### Setup

1. **Install dependencies:**

```bash
pip install -r requirements.txt
```

2. **Install Playwright browsers** (one-time):

```bash
playwright install
```

---

### IBBA Directory Scraper

File: `ibba_scraper.py`

This script:

- Navigates to the IBBA Directory (`https://www.ibba.org/find-a-business-broker/`).
- Runs a search (currently configured for a broad U.S. search) and applies the **Auto Related Businesses** specialty.
- Iterates through results and opens **View Profile** for each broker.
- Extracts:
  - Full Name
  - Phone Number
  - Location (City, State)
  - Company
  - Email Address
  - Source (Website URL)
  - Notes
- Uses shared helpers in `ecw_scraper_data.py` to:
  - Normalize missing fields to `"N/A"`.
  - Build a Pandas DataFrame with the exact SOP headers.
  - De-duplicate brokers found across pages/searches.
  - Save to CSV (default `ibba_ecw_brokers.csv`).

**Run it from the project root**

```bash
python ibba_scraper.py
```

You can open `ibba_scraper.py` and adjust:

- The starting city/zip and distance (search radius).
- Any selectors if IBBA updates their HTML.

- `ecw_scraper_google_sheets.py` is a helper for pushing any DataFrame to a Google Sheet via `gspread` and a Google Cloud service account.

---