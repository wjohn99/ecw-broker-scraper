# ECW Broker Scraper

Python scrapers that find Express Car Wash (ECW) brokers from multiple directories and push matches to a shared Google Sheet. Each site gets its own script and worksheet tab.

---

### Setup

```bash
pip install -r requirements.txt
playwright install
```

Create a Google Cloud service account with the Google Sheets & Drive API plugins, download the JSON key, and place it in the project root.

---

### Scrapers

#### IBBA — `ibba_scraper.py`

Scrapes the IBBA state directories (FL & NY). Opens each broker's "more details" profile, checks bio/specialties for ECW keywords, extracts contact info. Worksheet tab: **IBBA**.

```bash
python ibba_scraper.py
```

#### BizQuest — `bizquest_scraper.py`

Paginates through BizQuest broker directories (FL & NY). Opens each profile, checks Broker Bio tab for keywords, extracts from Company Info tab. Worksheet tab: **BizQuest**.

```bash
python bizquest_scraper.py # both states (clears sheet then appends)
python bizquest_scraper.py --fl # Florida only (appends sheet starting on next blank row)
python bizquest_scraper.py --ny # New York only (appends sheet starting on next blank row)
```

#### BusinessBroker.net — `businessbroker_scraper.py`

Scrapes BusinessBroker.net state pages (FL & NY). All brokers are on one page per state. Opens each profile, scrolls to load Sold Listings, checks full page for keywords. Worksheet tab: **BusinessBroker**.

```bash
python businessbroker_scraper.py # both states (clears sheet then appends)
python businessbroker_scraper.py --fl # Florida only (appends sheet starting on next blank row)
python businessbroker_scraper.py --ny # New York only (appends sheet starting on next blank row)
```

---

### Keywords

All scrapers use the same keyword list: **Car Wash, Express Car Wash, Express Wash, Tunnel Wash, Carwash, Conveyor Wash**.

### Columns

All scrapers produce the same columns: **Full Name, Phone Number, Location (City, State), Company, Email Address, URL, Notes**. No "Source" column — the worksheet tab name identifies the source.

---