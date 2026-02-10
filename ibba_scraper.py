from __future__ import annotations

import re
from typing import List, Optional, Tuple
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

STATE_DIRECTORY_URLS: List[Tuple[str, str]] = [
    ("New York", "https://www.ibba.org/state/new-york/"),
    ("Florida", "https://www.ibba.org/state/florida/"),
]

ECW_KEYWORDS = [
    "Car Wash",
    "Express Wash",
    "Tunnel Wash",
    "Wash Concepts",
]

OUTPUT_CSV = "ibba_ecw_brokers.csv"
MAX_PROFILES_PER_STATE: Optional[int] = None
SHEET_ID = "1MMnxeTTlf9noOKmmvGEBl9xPinsNTd12ZXv7lBa6S9A"
WORKSHEET_NAME = "IBBA"
SERVICE_ACCOUNT_JSON = "ecw-broker-scraper-ef955c25c30d.json"

OUTPUT_HEADERS = {
    "Notes (URL Link)": "Notes (Keywords Found)",
}

def _safe_text(locator, default: str = "N/A") -> str:
    try:
        if locator is None or locator.count() == 0:
            return default
        text = locator.first.inner_text(timeout=5000).strip()
        return text or default
    except Exception:
        return default


def _safe_attr(locator, attr: str, default: Optional[str] = None) -> Optional[str]:
    try:
        if locator is None or locator.count() == 0:
            return default
        return locator.first.get_attribute(attr)
    except Exception:
        return default


def _profile_contains_ecw_keyword(bio_specialties: str) -> Tuple[bool, List[str]]:
    if not bio_specialties or bio_specialties == "N/A":
        return False, []
    text = bio_specialties.lower()
    found = [kw for kw in ECW_KEYWORDS if kw.lower() in text]
    return (len(found) > 0, found)


def _extract_email_from_profile(page: Page) -> str:
    try:
        mailto = page.locator("a[href^='mailto:']").first
        if mailto.count() > 0:
            href = mailto.get_attribute("href")
            if href and href.startswith("mailto:"):
                addr = href.replace("mailto:", "").strip().split("?")[0].strip()
                if addr and "@" in addr:
                    return addr
    except Exception:
        pass

    try:
        contact_btn = page.get_by_role("button", name=re.compile(r"contact", re.I)).first
        if contact_btn.count() > 0:
            contact_btn.click()
            page.wait_for_load_state("load", timeout=10000)
            mailto_after = page.locator("a[href^='mailto:']").first
            if mailto_after.count() > 0:
                href = mailto_after.get_attribute("href")
                if href and "@" in href:
                    return href.replace("mailto:", "").strip().split("?")[0].strip()
    except Exception:
        pass

    try:
        contact_link = page.get_by_role("link", name=re.compile(r"contact", re.I)).first
        if contact_link.count() > 0:
            contact_link.click()
            page.wait_for_load_state("load", timeout=10000)
            mailto_after = page.locator("a[href^='mailto:']").first
            if mailto_after.count() > 0:
                href = mailto_after.get_attribute("href")
                if href and "@" in href:
                    return href.replace("mailto:", "").strip().split("?")[0].strip()
    except Exception:
        pass

    try:
        body_text = page.locator("body").inner_text(timeout=5000)
        for pat in [
            r"[\w.\-+]+?\s*\[at\]\s*[\w.\-+]+\s*\[dot\]\s*\w+",
            r"[\w.\-+]+?\s*\(at\)\s*[\w.\-+]+\s*\(dot\)\s*\w+",
            r"[\w.\-+]+?\s*@\s*[\w.\-+]+\s*\.\s*\w+",
        ]:
            m = re.search(pat, body_text, re.I)
            if m:
                candidate = m.group(0).replace("[at]", "@").replace("[dot]", ".").replace("(at)", "@").replace("(dot)", ".").replace(" ", "")
                if "@" in candidate and "." in candidate:
                    return candidate
    except Exception:
        pass

    return "N/A"


def _extract_bio_specialties(page: Page) -> str:
    for label in ["Specialty", "Specialties", "Bio", "About", "Areas of Expertise"]:
        try:
            el = page.get_by_text(label, exact=False).first
            if el.count() == 0:
                continue
            parent = el.locator("xpath=..")
            if parent.count() > 0:
                text = parent.first.inner_text(timeout=3000).strip()
                if text and len(text) < 2000:
                    return text
        except Exception:
            continue
    try:
        block = page.locator("main p, main div, .content p, .profile p, [class*='bio']").first
        if block.count() > 0:
            return block.first.inner_text(timeout=3000).strip() or "N/A"
    except Exception:
        pass
    return "N/A"


_CITY_STATE_ONLY = re.compile(r"^[^,]+,\s*[A-Za-z]{2}$")
_RE_CITY_ST = re.compile(r"([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Za-z]{2})(?:\s+\d{5}(?:-\d{4})?|\s+United States|\s|$)")

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
_RE_CITY_STATE_BEFORE_ZIP = re.compile(
    r"([A-Za-z][A-Za-z\s\.\-']+?),\s*([A-Za-z]+(?:\s+[A-Za-z]+)*)\s+\d{5}(?:-\d{4})?",
    re.I,
)


def _normalize_state_to_abbrev(state_raw: str) -> Optional[str]:
    s = (state_raw or "").strip()
    if len(s) == 2 and s.isalpha():
        return s.upper()
    return _US_STATE_ABBREV.get(s.lower())


def _extract_location_from_profile(page: Page) -> str:
    try:
        page.wait_for_selector("img[src*='icon-location'], a[href^='tel:']", timeout=10000)
    except Exception:
        pass
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
    body_text = body_text[:50000].replace("\u00a0", " ")
    text = " ".join(body_text.split())
    cutoff = int(len(text) * 0.75)

    for match in _RE_CITY_STATE_BEFORE_ZIP.finditer(text):
        if match.start() > cutoff:
            break
        city = match.group(1).strip()
        state_raw = match.group(2).strip()
        if not city or len(city) > 50 or re.match(r"^\d+$", city):
            continue
        state_abbrev = _normalize_state_to_abbrev(state_raw)
        if state_abbrev:
            out = f"{city}, {state_abbrev}"
            if _CITY_STATE_ONLY.match(out):
                return out

    for match in _RE_CITY_ST.finditer(text):
        if match.start() > cutoff:
            break
        city = match.group(1).strip()
        state = match.group(2).strip()
        if len(state) != 2 or not city or len(city) > 50:
            continue
        if re.match(r"^\d+$", city):
            continue
        out = f"{city}, {state.upper()}"
        if _CITY_STATE_ONLY.match(out):
            return out
    for match in re.finditer(r"([A-Za-z][A-Za-z\s\.\-']+),\s*([A-Za-z]{2})\b", text[:cutoff]):
        city, state = match.group(1).strip(), match.group(2).strip()
        if len(state) == 2 and city and len(city) <= 50 and not re.match(r"^\d+$", city):
            out = f"{city}, {state.upper()}"
            if _CITY_STATE_ONLY.match(out):
                return out
    return "N/A"


_IBBA_HEADER_DIGITS = "8886864222"


def _normalize_phone_digits(s: str) -> str:
    return re.sub(r"\D", "", s) if s else ""


def _is_ibba_header(digits: str) -> bool:
    if not digits or len(digits) < 10:
        return False
    d = digits[1:] if len(digits) == 11 and digits.startswith("1") else digits
    return d == _IBBA_HEADER_DIGITS


def _extract_phone_from_profile(page: Page) -> str:
    try:
        all_tel = page.locator("a[href^='tel:']")
        n = all_tel.count()
        for i in range(n):
            node = all_tel.nth(i)
            href = node.get_attribute("href")
            if not href or not href.startswith("tel:"):
                continue
            num_raw = href.replace("tel:", "").strip().split("?")[0].strip()
            digits = _normalize_phone_digits(num_raw)
            if _is_ibba_header(digits):
                continue
            if len(digits) >= 10:
                text = node.inner_text(timeout=2000).strip()
                return text if (text and re.search(r"\d", text)) else num_raw
        for i in range(n):
            node = all_tel.nth(i)
            text = node.inner_text(timeout=2000).strip()
            if not text or not re.search(r"\d{3}", text):
                continue
            digits = _normalize_phone_digits(text)
            if _is_ibba_header(digits):
                continue
            if len(digits) >= 10:
                return text
    except Exception:
        pass
    return "N/A"


def _extract_company_from_profile(page: Page) -> str:
    icon_selectors = (
        "[class*='apartment'], [class*='fa-apartment'], "
        "[class*='building'], [class*='fa-building'], "
        "svg[class*='building'], svg[class*='apartment'], "
        "[class*='icon-apartment'], [class*='icon-building']"
    )
    try:
        icon = page.locator(icon_selectors).first
        if icon.count() > 0:
            next_sib = icon.locator("xpath=following-sibling::*[1]")
            if next_sib.count() > 0:
                text = next_sib.first.inner_text(timeout=2000).strip()
                if text and len(text) <= 200:
                    return text
            parent = icon.locator("xpath=..")
            if parent.count() > 0:
                raw = parent.first.inner_text(timeout=2000).strip()
                if raw and len(raw) <= 200:
                    for prefix in ("apartment", "building", "company"):
                        if raw.lower().startswith(prefix):
                            raw = raw[len(prefix):].strip()
                            break
                    if raw:
                        return raw
    except Exception:
        pass
    try:
        company = page.evaluate("""
            () => {
                const walk = (el) => {
                    const text = (el.innerText || '').trim();
                    const m = text.match(/^(apartment|building|company)\\s+(.+)$/i);
                    if (m && m[2].length > 0 && m[2].length < 200) return m[2].trim();
                    for (const c of el.children || []) { const r = walk(c); if (r) return r; }
                    return null;
                };
                return walk(document.body) || null;
            }
        """)
        if company:
            return company
    except Exception:
        pass
    try:
        company_label = page.get_by_text("Company", exact=False).first
        if company_label.count() > 0:
            sib = company_label.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                return sib.first.inner_text(timeout=2000).strip() or "N/A"
    except Exception:
        pass
    return "N/A"


def _get_listing_cards_and_links(page: Page):
    more_details = page.get_by_role("link", name=re.compile(r"more\s+details\s*»?", re.I))
    try:
        n = more_details.count()
    except Exception:
        n = 0
    for i in range(n):
        link = more_details.nth(i)
        try:
            card = link.locator("xpath=ancestor::*[self::div or self::article or self::li or self::section][position()<=4][1]")
            if card.count() == 0:
                card = link.locator("xpath=..")
            yield (card, link)
        except Exception:
            yield (None, link)


def _text_from_card(card, page: Page) -> str:
    if card is None or card.count() == 0:
        return ""
    try:
        return card.first.inner_text(timeout=5000).strip()
    except Exception:
        return ""


def _parse_listing_card_text(card_text: str) -> Tuple[str, str, str]:
    name, company, phone = "N/A", "N/A", "N/A"
    if not card_text:
        return name, company, phone
    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
    phone_match = re.search(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}", card_text)
    if phone_match:
        phone = phone_match.group(0).strip()
    if lines:
        name = lines[0]
    if len(lines) >= 2 and lines[1] != phone and not re.match(r"^\(?\d{3}\)?", lines[1]):
        company = lines[1]
    return name, company, phone


def _scrape_state_page(
    page: Page,
    state_name: str,
    state_url: str,
    max_profiles: Optional[int] = None,
    *,
    sheet_id: Optional[str] = None,
    worksheet_name: str = "IBBA",
    service_account_json: Optional[str] = None,
) -> List[BrokerContact]:
    contacts: List[BrokerContact] = []
    page.goto(state_url, wait_until="domcontentloaded", timeout=60000)
    try:
        page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=25000)
    except Exception:
        print(f"  No 'more details' links found on {state_url}; state page may have changed or be empty.")
        return contacts

    seen_urls = set()
    visited = 0
    for card, link in _get_listing_cards_and_links(page):
        if max_profiles is not None and visited >= max_profiles:
            break
        try:
            href = _safe_attr(link, "href")
            if not href:
                continue
            profile_url = urljoin(state_url, href)
            if profile_url in seen_urls:
                continue
            seen_urls.add(profile_url)
            visited += 1

            card_text = _text_from_card(card, page)
            list_name, list_company, list_phone = _parse_listing_card_text(card_text)

            _loaded = False
            for _attempt in range(2):
                try:
                    page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
                    page.wait_for_load_state("load", timeout=30000)
                    _loaded = True
                    break
                except Exception as goto_err:
                    if _attempt == 0:
                        continue
                    raise goto_err
            if not _loaded:
                raise RuntimeError("profile load failed")

            bio_specialties = _extract_bio_specialties(page)
            passes, keywords_found = _profile_contains_ecw_keyword(bio_specialties)
            if not passes:
                page.goto(state_url, wait_until="domcontentloaded", timeout=60000)
                page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=20000)
                continue

            email = _extract_email_from_profile(page)
            location = _extract_location_from_profile(page)
            full_name = list_name if list_name != "N/A" else _safe_text(page.get_by_role("heading").first)
            company = list_company if list_company != "N/A" else _safe_text(page.get_by_text("Company", exact=False).first.locator("xpath=following-sibling::*[1]"))
            phone = _extract_phone_from_profile(page)

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
                    df_one = contacts_to_dataframe([contact]).rename(columns=OUTPUT_HEADERS)
                    row = df_one.astype(str).fillna("").values.tolist()[0]
                    append_row_to_google_sheet(
                        row,
                        sheet_id=sheet_id,
                        worksheet_name=worksheet_name,
                        service_account_json_path=service_account_json,
                    )
                except Exception as e:
                    print(f"  (Sheet append failed: {e})")

            page.goto(state_url, wait_until="domcontentloaded", timeout=60000)
            page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=20000)
        except Exception as e:
            print(f"  Skip broker at {state_url}: {e}")
            for _ in range(2):
                try:
                    page.goto(state_url, wait_until="domcontentloaded", timeout=60000)
                    page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=25000)
                    break
                except Exception:
                    pass
            continue

    return contacts


def _save_and_upload_contacts(all_contacts: List[BrokerContact]) -> None:
    df = contacts_to_dataframe(all_contacts)
    df_clean = clean_contacts_dataframe(df)
    df_out = df_clean.rename(columns=OUTPUT_HEADERS)
    save_to_csv(df_out, OUTPUT_CSV)
    print(f"Total ECW-matching brokers: {len(all_contacts)}.")
    print(f"After de-duplication: {len(df_out)}.")
    print(f"Saved to: {OUTPUT_CSV}")
    if SHEET_ID:
        print(f"Uploading to Google Sheet {SHEET_ID!r}, worksheet {WORKSHEET_NAME!r}...")
        upload_dataframe_to_google_sheet(
            df_out,
            sheet_id=SHEET_ID,
            worksheet_name=WORKSHEET_NAME,
            service_account_json_path=SERVICE_ACCOUNT_JSON,
        )
        print("Google Sheets upload completed.")


def scrape_ibba_directory() -> None:
    all_contacts: List[BrokerContact] = []

    if SHEET_ID:
        try:
            clear_worksheet_data(
                SHEET_ID,
                worksheet_name=WORKSHEET_NAME,
                service_account_json_path=SERVICE_ACCOUNT_JSON,
            )
            print(f"Cleared worksheet {WORKSHEET_NAME!r}; will append matches as we go.")
        except Exception as e:
            print(f"Could not clear sheet: {e}")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            page = browser.new_page()
            page.set_default_timeout(30000)
            page.set_default_navigation_timeout(60000)

            for state_name, state_url in STATE_DIRECTORY_URLS:
                print(f"Scraping state: {state_name} — {state_url}")
                state_contacts = _scrape_state_page(
                    page,
                    state_name,
                    state_url,
                    max_profiles=MAX_PROFILES_PER_STATE,
                    sheet_id=SHEET_ID,
                    worksheet_name=WORKSHEET_NAME,
                    service_account_json=SERVICE_ACCOUNT_JSON,
                )
                all_contacts.extend(state_contacts)
                print(f"  Collected {len(state_contacts)} ECW-matching brokers from {state_name}.")

            browser.close()

        _save_and_upload_contacts(all_contacts)
    except KeyboardInterrupt:
        print("\nStopped by user (Ctrl+C). Saving and uploading what we have so far...")
        _save_and_upload_contacts(all_contacts)
        print("Done.")


if __name__ == "__main__":
    scrape_ibba_directory()
