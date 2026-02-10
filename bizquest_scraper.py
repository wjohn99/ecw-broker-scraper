from __future__ import annotations

import argparse
import re
import time
from typing import List, Set, Tuple
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
    ("New York", "https://www.bizquest.com/new-york-business-brokers/"),
    ("Florida", "https://www.bizquest.com/florida-business-brokers/"),
]

ECW_KEYWORDS = [
    "express car wash",
    "express wash",
    "tunnel wash",
    "car wash",
    "carwash",
    "conveyor wash",
]

OUTPUT_CSV = "bizquest_ecw_brokers.csv"
SHEET_ID = "1MMnxeTTlf9noOKmmvGEBl9xPinsNTd12ZXv7lBa6S9A"
WORKSHEET_NAME = "BizQuest"
SERVICE_ACCOUNT_JSON = "ecw-broker-scraper-ef955c25c30d.json"

_PROFILE_PATH_RE = re.compile(r"/business-broker/[^/]+/[^/]+/BW\d+", re.I)

_CITY_STATE_ONLY = re.compile(r"^[^,]+,\s*[A-Za-z]{2}$")
_RE_CITY_ST = re.compile(
    r"([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Za-z]{2})(?:\s+\d{5}(?:-\d{4})?|\s+United States|\s|$)"
)
_STATE_ABBREV = {
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
_RE_CITY_FULL_STATE = re.compile(
    r"([A-Za-z][A-Za-z\s\.\-']+?),?\s*,\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s*(?=\d{5}(?:-\d{4})?|$|,)",
    re.I
)
_RE_CITY_STATE_BEFORE_ZIP = re.compile(
    r"([A-Za-z][A-Za-z\s\.\-']+?),\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+\d{5}(?:-\d{4})?",
    re.I
)


def _safe_text(locator, default: str = "N/A") -> str:
    try:
        if locator is None or locator.count() == 0:
            return default
        text = locator.first.inner_text(timeout=5000).strip()
        return text or default
    except Exception:
        return default


def _click_tab_by_role_or_text(page: Page, role_pattern: str, text_fallback: str) -> bool:
    try:
        el = page.get_by_role("tab", name=re.compile(role_pattern, re.I)).first
        if el.count() > 0:
            el.scroll_into_view_if_needed(timeout=3000)
            el.click()
            time.sleep(1.0)
            return True
    except Exception:
        pass
    try:
        el = page.get_by_text(text_fallback, exact=False).first
        if el.count() > 0:
            el.scroll_into_view_if_needed(timeout=3000)
            el.click()
            time.sleep(1.0)
            return True
    except Exception:
        pass
    return False


def _click_broker_bio_tab(page: Page) -> None:
    _click_tab_by_role_or_text(page, r"broker\s+bio", "Broker Bio")


def _click_company_info_tab(page: Page) -> None:
    _click_tab_by_role_or_text(page, r"company\s+info", "Company Info")


def _click_show_phone_number(page: Page) -> None:
    try:
        link = page.get_by_text("Show Phone Number", exact=False).first
        if link.count() > 0:
            link.scroll_into_view_if_needed(timeout=3000)
            link.click()
            time.sleep(0.8)
    except Exception:
        pass


def _profile_contains_ecw_keywords(page: Page) -> Tuple[bool, List[str]]:
    try:
        body = page.inner_text("body", timeout=8000) or ""
        lower = body.lower()
        found = [kw for kw in ECW_KEYWORDS if kw in lower]
        return (len(found) > 0, found)
    except Exception:
        return (False, [])


def _extract_location_from_profile(page: Page) -> str:
    def parse_city_state(text: str) -> str:
        if not text or len(text) > 2000:
            return "N/A"
        text = text.replace("\u00a0", " ")
        text = " ".join(text.split())

        for match in _RE_CITY_STATE_BEFORE_ZIP.finditer(text):
            city = match.group(1).strip()
            state_raw = match.group(2).strip()
            if not city or len(city) > 50 or re.match(r"^\d+$", city):
                continue
            state_lower = state_raw.lower()
            state_abbrev = _STATE_ABBREV.get(state_lower)
            if state_abbrev:
                out = f"{city}, {state_abbrev}"
                if _CITY_STATE_ONLY.match(out):
                    return out

        for match in _RE_CITY_ST.finditer(text):
            city = match.group(1).strip()
            state = match.group(2).strip()
            if len(state) != 2 or not city or len(city) > 50 or re.match(r"^\d+$", city):
                continue
            out = f"{city}, {state}"
            if _CITY_STATE_ONLY.match(out):
                return out
        for match in _RE_CITY_FULL_STATE.finditer(text):
            city = match.group(1).strip()
            state_raw = match.group(2).strip()
            if not city or len(city) > 50 or re.match(r"^\d+$", city):
                continue
            state_lower = state_raw.lower()
            state_abbrev = _STATE_ABBREV.get(state_lower)
            if state_abbrev:
                out = f"{city}, {state_abbrev}"
                if _CITY_STATE_ONLY.match(out):
                    return out
        for match in re.finditer(r"([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Za-z]{2})\b", text):
            city, state = match.group(1).strip(), match.group(2).strip()
            if len(state) == 2 and city and len(city) <= 50 and not re.match(r"^\d+$", city):
                out = f"{city}, {state}"
                if _CITY_STATE_ONLY.match(out):
                    return out
        return "N/A"

    for label in ["Address", "Location", "City", "Office"]:
        try:
            el = page.get_by_text(label, exact=False).first
            if el.count() == 0:
                continue
            parent = el.locator("xpath=..")
            if parent.count() > 0:
                block = parent.first.inner_text(timeout=3000).strip()
                if block:
                    loc = parse_city_state(block)
                    if loc != "N/A":
                        return loc
            sib = el.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                block = sib.first.inner_text(timeout=3000).strip()
                if block:
                    loc = parse_city_state(block)
                    if loc != "N/A":
                        return loc
        except Exception:
            continue
    try:
        body_text = page.evaluate(
            "() => (document.querySelector('main') || document.body || {}).innerText || ''"
        )
    except Exception:
        body_text = ""
    if not body_text or not isinstance(body_text, str):
        try:
            body_text = page.inner_text("body", timeout=3000) or ""
        except Exception:
            return "N/A"
    return parse_city_state(body_text[:50000])


def _extract_phone_from_profile(page: Page) -> str:
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
    return "N/A"


def _extract_email_from_profile(page: Page) -> str:
    try:
        mailto = page.locator("a[href^='mailto:']").first
        if mailto.count() > 0:
            href = mailto.get_attribute("href")
            if href and "@" in href:
                return href.replace("mailto:", "").strip().split("?")[0].strip()
    except Exception:
        pass
    return "N/A"


def _extract_company_from_profile(page: Page) -> str:
    def _is_company_like(text: str) -> bool:
        if not text or len(text) > 200:
            return False
        lower = text.lower()
        if lower.startswith("phone") or "show phone" in lower or lower == "share":
            return False
        if re.match(r"^[\d\s\-\(\)\.]+$", text.strip()):
            return False
        return True

    def _first_line(s: str) -> str:
        return s.split("\n")[0].strip() if s else ""

    try:
        heading = page.locator("h1").first
        if heading.count() > 0:
            next_el = heading.locator("xpath=following-sibling::*[1]")
            if next_el.count() > 0:
                t = _first_line(next_el.inner_text(timeout=2000).strip())
                if t and _is_company_like(t):
                    return t
    except Exception:
        pass
    try:
        heading = page.get_by_role("heading").first
        if heading.count() > 0:
            next_el = heading.locator("xpath=following-sibling::*[1]")
            if next_el.count() > 0:
                t = _first_line(next_el.inner_text(timeout=2000).strip())
                if t and _is_company_like(t):
                    return t
    except Exception:
        pass
    try:
        company_label = page.get_by_text("Company", exact=False).first
        if company_label.count() > 0:
            sib = company_label.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                t = _safe_text(sib)
                if t != "N/A" and _is_company_like(t):
                    return t
    except Exception:
        pass
    try:
        for sel in ["[class*='company']", "[class*='firm']", "[class*='brokerage']"]:
            el = page.locator(sel).first
            if el.count() > 0:
                t = _safe_text(el)
                if t != "N/A" and _is_company_like(t):
                    return t
    except Exception:
        pass
    return "N/A"


def _get_profile_urls_from_page(page: Page, base_url: str) -> Set[str]:
    urls: Set[str] = set()
    links = page.locator("a[href*='/business-broker/']")
    try:
        n = links.count()
    except Exception:
        return urls
    for i in range(n):
        try:
            href = links.nth(i).get_attribute("href")
            if not href:
                continue
            full = urljoin(base_url, href)
            if _PROFILE_PATH_RE.search(full):
                urls.add(full)
        except Exception:
            continue
    return urls


def _scroll_to_bottom(page: Page, steps: int = 3) -> None:
    try:
        for _ in range(steps):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(0.3)
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.5)
    except Exception:
        pass


def _directory_page_url(base_url: str, page_num: int) -> str:
    base = base_url.rstrip("/")
    if page_num <= 1:
        return base + "/" if not base_url.endswith("/") else base_url
    return f"{base}/page-{page_num}/"


def _get_all_profile_urls_from_directory(page: Page, directory_url: str) -> Set[str]:
    all_urls: Set[str] = set()
    max_pages = 60
    for page_num in range(1, max_pages + 1):
        page_url = _directory_page_url(directory_url, page_num)
        try:
            page.goto(page_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_load_state("load", timeout=20000)
            time.sleep(1)
            _scroll_to_bottom(page)
            time.sleep(0.5)
            before = len(all_urls)
            all_urls |= _get_profile_urls_from_page(page, page_url)
            added = len(all_urls) - before
            print(f"    Page {page_num}: {added} broker links (total {len(all_urls)})")
            if added == 0:
                break
        except Exception as e:
            print(f"    Page {page_num}: {e}")
            break
    return all_urls


def _scrape_directory_page(
    page: Page,
    region_name: str,
    directory_url: str,
    seen_urls: Set[str],
    *,
    sheet_id: str | None = None,
    worksheet_name: str = "BizQuest",
    service_account_json: str | None = None,
) -> List[BrokerContact]:
    contacts: List[BrokerContact] = []
    profile_urls = _get_all_profile_urls_from_directory(page, directory_url)
    profile_list = sorted(profile_urls)
    print(f"  Opening {len(profile_list)} broker profiles to check for ECW keywords...")
    for profile_url in profile_list:
        if profile_url in seen_urls:
            continue
        seen_urls.add(profile_url)
        try:
            page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_load_state("load", timeout=15000)
            time.sleep(1)
            _click_broker_bio_tab(page)
            time.sleep(0.6)
            has_keywords, keywords_found = _profile_contains_ecw_keywords(page)
            if not has_keywords:
                continue
            _click_company_info_tab(page)
            time.sleep(0.8)
            _click_show_phone_number(page)
            full_name = _safe_text(page.get_by_role("heading").first)
            if full_name == "N/A":
                full_name = _safe_text(page.locator("h1").first)
            company = _extract_company_from_profile(page)
            location = _extract_location_from_profile(page)
            phone = _extract_phone_from_profile(page)
            email = _extract_email_from_profile(page)
            notes = "; ".join(keywords_found) if keywords_found else "N/A"

            contacts.append(
                BrokerContact(
                    full_name=full_name,
                    phone_number=phone,
                    location=location,
                    company=company,
                    email=email,
                    source_url=profile_url,
                    notes=notes,
                )
            )
            print(f"  + {full_name} ({company}) — keywords: {notes}")
            if sheet_id and service_account_json:
                try:
                    df_one = contacts_to_dataframe([contacts[-1]])
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
            print(f"  Skip broker {profile_url[:60]}...: {e}")
            continue
    return contacts


def scrape_bizquest_directory(
    headless: bool = True,
    regions: List[Tuple[str, str]] | None = None,
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_default_timeout(30000)
        page.set_default_navigation_timeout(60000)

        for region_name, directory_url in urls:
            print(f"Scraping: {region_name} — {directory_url}")
            region_contacts = _scrape_directory_page(
                page,
                region_name,
                directory_url,
                seen_urls,
                sheet_id=SHEET_ID or None,
                worksheet_name=WORKSHEET_NAME,
                service_account_json=SERVICE_ACCOUNT_JSON,
            )
            all_contacts.extend(region_contacts)
            print(f"  Collected {len(region_contacts)} ECW-matching brokers from {region_name}.")

        browser.close()

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
    parser = argparse.ArgumentParser(description="Scrape BizQuest for ECW brokers.")
    parser.add_argument("--ny", action="store_true", help="Scrape New York only (~17 pages)")
    parser.add_argument("--fl", action="store_true", help="Scrape Florida only (~40 pages)")
    args = parser.parse_args()
    regions = None
    if args.ny and not args.fl:
        regions = [("New York", "https://www.bizquest.com/new-york-business-brokers/")]
    elif args.fl and not args.ny:
        regions = [("Florida", "https://www.bizquest.com/florida-business-brokers/")]
    scrape_bizquest_directory(headless=False, regions=regions)
