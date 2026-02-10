from __future__ import annotations

import argparse
import re
import time
from typing import List, Optional, Set, Tuple
from urllib.parse import urljoin

from playwright.sync_api import Page, sync_playwright

from ecw_scraper_data import (
    BrokerContact,
    clean_contacts_dataframe,
    contacts_to_dataframe,
    save_to_csv,
)
from ecw_scraper_google_sheets import (
    append_row_to_google_sheet,
    clear_worksheet_data,
    upload_dataframe_to_google_sheet,
)

DIRECTORY_URLS: List[Tuple[str, str]] = [
    ("Florida", "https://www.businessbroker.net/brokers/florida.aspx?"),
    ("New York", "https://www.businessbroker.net/brokers/new-york.aspx?"),
]

ECW_KEYWORDS = [
    "express car wash",
    "express wash",
    "tunnel wash",
    "car wash",
    "carwash",
    "conveyor wash",
]

OUTPUT_CSV = "businessbroker_ecw_brokers.csv"
SHEET_ID = "1MMnxeTTlf9noOKmmvGEBl9xPinsNTd12ZXv7lBa6S9A"
WORKSHEET_NAME = "BusinessBroker"
SERVICE_ACCOUNT_JSON = "ecw-broker-scraper-ef955c25c30d.json"

_PROFILE_URL_RE = re.compile(r"/brokers/[a-z0-9][a-z0-9\-]+-\d+\.aspx", re.I)

_CITY_STATE_ONLY = re.compile(r"^[^,]+,\s*[A-Za-z]{2}$")

_US_STATE_ABBREV = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN", "mississippi": "MS",
    "missouri": "MO", "montana": "MT", "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY", "north carolina": "NC",
    "north dakota": "ND", "ohio": "OH", "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD", "tennessee": "TN",
    "texas": "TX", "utah": "UT", "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY", "district of columbia": "DC",
}

_RE_CITY_STATE_ZIP = re.compile(
    r"([A-Za-z][A-Za-z\s\.\-']+?),\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+(\d{5}(?:-\d{4})?)",
    re.I,
)
_RE_CITY_ST = re.compile(
    r"([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Za-z]{2})(?:\s+\d{5}|\s|$)"
)



def _safe_text(locator, default: str = "N/A") -> str:
    try:
        if locator is None or locator.count() == 0:
            return default
        text = locator.first.inner_text(timeout=5000).strip()
        return text or default
    except Exception:
        return default


def _normalize_state(state_raw: str) -> Optional[str]:
    s = (state_raw or "").strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()
    return _US_STATE_ABBREV.get(s.lower())


def _scroll_to_bottom(page: Page, steps: int = 5, pause: float = 0.5) -> None:
    try:
        for _ in range(steps):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(pause)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(1.0)
    except Exception:
        pass



def _get_profile_urls_from_state_page(page: Page, state_url: str) -> List[str]:
    urls: List[str] = []
    seen: Set[str] = set()

    try:
        search_btn = page.locator("input[value='SEARCH'], button:has-text('SEARCH')").first
        if search_btn.count() > 0:
            search_btn.click()
            time.sleep(2)
    except Exception:
        pass

    _scroll_to_bottom(page, steps=6, pause=0.8)

    links = page.locator("a[href*='/brokers/']")
    try:
        n = links.count()
    except Exception:
        return urls
    for i in range(n):
        try:
            href = links.nth(i).get_attribute("href")
            if not href:
                continue
            full = urljoin(state_url, href)
            if _PROFILE_URL_RE.search(full) and full not in seen:
                if "brokers.aspx" in full.lower() or "florida.aspx" in full.lower() or "new-york.aspx" in full.lower():
                    continue
                seen.add(full)
                urls.append(full)
        except Exception:
            continue
    return urls



def _profile_contains_ecw_keywords(page: Page) -> Tuple[bool, List[str]]:
    try:
        body = page.inner_text("body", timeout=10000) or ""
        lower = body.lower()
        found = [kw for kw in ECW_KEYWORDS if kw in lower]
        return (len(found) > 0, found)
    except Exception:
        return (False, [])


def _extract_name(page: Page) -> str:
    try:
        h1 = page.locator("h1").first
        if h1.count() > 0:
            text = h1.inner_text(timeout=5000).strip()
            if text:
                return text
    except Exception:
        pass
    return "N/A"


def _extract_company(page: Page) -> str:
    try:
        h2 = page.locator("h2").first
        if h2.count() > 0:
            text = h2.inner_text(timeout=5000).strip()
            lower = text.lower()
            if text and len(text) < 200 and "company overview" not in lower and "broker profile" not in lower and "services offered" not in lower and "areas served" not in lower:
                return text
    except Exception:
        pass
    try:
        company_label = page.get_by_text("Company", exact=False).first
        if company_label.count() > 0:
            sib = company_label.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                text = sib.first.inner_text(timeout=2000).strip()
                if text and len(text) < 200:
                    return text
    except Exception:
        pass
    return "N/A"


def _extract_phone(page: Page) -> str:
    try:
        tel = page.locator("a[href^='tel:']").first
        if tel.count() > 0:
            href = tel.get_attribute("href")
            if href and href.startswith("tel:"):
                num = href.replace("tel:", "").strip().split("?")[0].strip()
                if num and re.search(r"\d{3}", num):
                    return num
            text = tel.inner_text(timeout=2000).strip()
            if text and re.search(r"\d{3}", text):
                return text
    except Exception:
        pass
    try:
        body = page.inner_text("body", timeout=5000) or ""
        match = re.search(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}", body[:5000])
        if match:
            return match.group(0).strip()
    except Exception:
        pass
    return "N/A"


def _extract_location(page: Page) -> str:
    try:
        body = page.inner_text("body", timeout=5000) or ""
        body = body[:10000].replace("\u00a0", " ")
        text = " ".join(body.split())
    except Exception:
        return "N/A"

    for match in _RE_CITY_STATE_ZIP.finditer(text):
        city = match.group(1).strip()
        state_raw = match.group(2).strip()
        if not city or len(city) > 50 or re.match(r"^\d+$", city):
            continue
        state_abbrev = _normalize_state(state_raw)
        if state_abbrev:
            city_title = city.title() if city.isupper() else city
            out = f"{city_title}, {state_abbrev}"
            if _CITY_STATE_ONLY.match(out):
                return out

    for match in _RE_CITY_ST.finditer(text):
        city = match.group(1).strip()
        state = match.group(2).strip().upper()
        if not city or len(city) > 50 or re.match(r"^\d+$", city):
            continue
        city_title = city.title() if city.isupper() else city
        out = f"{city_title}, {state}"
        if _CITY_STATE_ONLY.match(out):
            return out

    return "N/A"


def _extract_email(page: Page) -> str:
    try:
        mailto = page.locator("a[href^='mailto:']").first
        if mailto.count() > 0:
            href = mailto.get_attribute("href")
            if href and "@" in href:
                return href.replace("mailto:", "").strip().split("?")[0].strip()
    except Exception:
        pass
    try:
        body = page.inner_text("body", timeout=3000) or ""
        match = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", body[:10000])
        if match:
            return match.group(0)
    except Exception:
        pass
    return "N/A"



def _scrape_state(
    page: Page,
    state_name: str,
    state_url: str,
    seen_urls: Set[str],
    *,
    sheet_id: Optional[str] = None,
    worksheet_name: str = "BusinessBroker",
    service_account_json: Optional[str] = None,
) -> List[BrokerContact]:
    contacts: List[BrokerContact] = []

    page.goto(state_url, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_load_state("load", timeout=30000)
    time.sleep(2)

    profile_urls = _get_profile_urls_from_state_page(page, state_url)
    print(f"  Found {len(profile_urls)} broker profiles on the page.")

    for profile_url in profile_urls:
        if profile_url in seen_urls:
            continue
        seen_urls.add(profile_url)

        try:
            for attempt in range(2):
                try:
                    page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_load_state("load", timeout=30000)
                    break
                except Exception as e:
                    if attempt == 0:
                        continue
                    raise e

            time.sleep(1)

            _scroll_to_bottom(page, steps=5, pause=0.5)
            time.sleep(1)

            has_keywords, keywords_found = _profile_contains_ecw_keywords(page)
            if not has_keywords:
                continue

            full_name = _extract_name(page)
            company = _extract_company(page)
            phone = _extract_phone(page)
            location = _extract_location(page)
            email = _extract_email(page)
            notes = "; ".join(keywords_found) if keywords_found else "N/A"

            contact = BrokerContact(
                full_name=full_name,
                phone_number=phone,
                location=location,
                company=company,
                email=email,
                source_url=profile_url,
                notes=notes,
            )
            contacts.append(contact)
            print(f"  + {full_name} ({company}) — keywords: {notes}")

            if sheet_id and service_account_json:
                try:
                    df_one = contacts_to_dataframe([contact])
                    row = df_one.astype(str).fillna("").values.tolist()[0]
                    append_row_to_google_sheet(
                        row,
                        sheet_id=sheet_id,
                        worksheet_name=worksheet_name,
                        service_account_json_path=service_account_json,
                    )
                except Exception as e:
                    print(f"  (Sheet append failed: {e})")

        except Exception as e:
            print(f"  Skip broker {profile_url[:80]}...: {e}")
            continue

    return contacts



def scrape_businessbroker_directory(
    regions: Optional[List[Tuple[str, str]]] = None,
) -> None:
    urls = regions if regions is not None else DIRECTORY_URLS
    all_contacts: List[BrokerContact] = []
    seen_urls: Set[str] = set()

    if SHEET_ID and regions is None:
        try:
            clear_worksheet_data(
                SHEET_ID,
                worksheet_name=WORKSHEET_NAME,
                service_account_json_path=SERVICE_ACCOUNT_JSON,
            )
            print(f"Cleared worksheet {WORKSHEET_NAME!r}; will append matches as we go.")
        except Exception as e:
            print(f"Could not clear sheet: {e}")
    elif SHEET_ID and regions is not None:
        print(f"Appending to existing worksheet {WORKSHEET_NAME!r} (next empty row).")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            page.set_default_timeout(30000)
            page.set_default_navigation_timeout(60000)

            for state_name, state_url in urls:
                print(f"Scraping: {state_name} — {state_url}")
                state_contacts = _scrape_state(
                    page,
                    state_name,
                    state_url,
                    seen_urls,
                    sheet_id=SHEET_ID or None,
                    worksheet_name=WORKSHEET_NAME,
                    service_account_json=SERVICE_ACCOUNT_JSON,
                )
                all_contacts.extend(state_contacts)
                print(f"  Collected {len(state_contacts)} ECW-matching brokers from {state_name}.")

            browser.close()
    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C). Saving what we have so far...")

    df = contacts_to_dataframe(all_contacts)
    df_clean = clean_contacts_dataframe(df)
    save_to_csv(df_clean, OUTPUT_CSV)
    print(f"Total ECW-matching brokers: {len(all_contacts)}. After de-dup: {len(df_clean)}. Saved: {OUTPUT_CSV}")

    if SHEET_ID and regions is None:
        print(f"Uploading final deduped list to worksheet {WORKSHEET_NAME!r}...")
        upload_dataframe_to_google_sheet(
            df_clean,
            sheet_id=SHEET_ID,
            worksheet_name=WORKSHEET_NAME,
            service_account_json_path=SERVICE_ACCOUNT_JSON,
        )
        print("Google Sheets upload completed.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrape BusinessBroker.net for ECW-matching brokers (FL & NY)."
    )
    parser.add_argument("--fl", action="store_true", help="Scrape Florida only.")
    parser.add_argument("--ny", action="store_true", help="Scrape New York only.")
    args = parser.parse_args()

    regions = None
    if args.fl and not args.ny:
        regions = [("Florida", "https://www.businessbroker.net/brokers/florida.aspx?")]
    elif args.ny and not args.fl:
        regions = [("New York", "https://www.businessbroker.net/brokers/new-york.aspx?")]

    scrape_businessbroker_directory(regions=regions)
