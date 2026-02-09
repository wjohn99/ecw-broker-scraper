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
from ecw_scraper_google_sheets import upload_dataframe_to_google_sheet

STATE_DIRECTORY_URLS: List[Tuple[str, str]] = [
    ("New York", "https://www.ibba.org/state/new-york/"),
    ("Florida", "https://www.ibba.org/state/florida/"),
]

# Narrow step: keep only brokers whose Bio/Specialties contain at least one of these
ECW_KEYWORDS = [
    "Car Wash",
    "Express Wash",
    "Tunnel Wash",
    "Wash Concepts",
]

OUTPUT_CSV = "ibba_ecw_brokers.csv"
# Set to a profile URL to run a one-contact test (scrape that profile only, then save + upload to Google Sheet). None = normal full scrape.
TEST_SINGLE_PROFILE_URL: Optional[str] = "https://www.ibba.org/broker-profile/florida/miami/harry-caruso/"
# Set to a small number to limit profiles per state for quick runs. None = no limit.
MAX_PROFILES_PER_STATE: Optional[int] = None
SHEET_ID = "1MMnxeTTlf9noOKmmvGEBl9xPinsNTd12ZXv7lBa6S9A"
WORKSHEET_NAME = "ECW Brokers"
SERVICE_ACCOUNT_JSON = "ecw-broker-scraper-ef955c25c30d.json"

# Final output headers per SOP
OUTPUT_HEADERS = {
    "Source (Website)": "Source (URL)",
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
    """
    Narrow step: return (True, list of matched keywords) if bio/specialties
    contains at least one ECW keyword; otherwise (False, []).
    """
    if not bio_specialties or bio_specialties == "N/A":
        return False, []
    text = bio_specialties.lower()
    found = [kw for kw in ECW_KEYWORDS if kw.lower() in text]
    return (len(found) > 0, found)


def _extract_email_from_profile(page: Page) -> str:
    """
    Robust email extraction: mailto link first, then Contact button reveal,
    then common obfuscation patterns. Returns "N/A" if not found.
    """
    # 1) mailto: link
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

    # 2) Contact button / link that might reveal email
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

    # 3) Obfuscation patterns in page text (e.g. "name [at] domain [dot] com")
    try:
        body_text = page.locator("body").inner_text(timeout=5000)
        # [at] / [dot] / (at) / (dot)
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
    """Get Bio / Specialties text from profile page for keyword filtering."""
    # Common section labels
    for label in ["Specialty", "Specialties", "Bio", "About", "Areas of Expertise"]:
        try:
            el = page.get_by_text(label, exact=False).first
            if el.count() == 0:
                continue
            # Next sibling or nearby block
            parent = el.locator("xpath=..")
            if parent.count() > 0:
                text = parent.first.inner_text(timeout=3000).strip()
                if text and len(text) < 2000:
                    return text
        except Exception:
            continue
    try:
        # Fallback: first substantial paragraph or div
        block = page.locator("main p, main div, .content p, .profile p, [class*='bio']").first
        if block.count() > 0:
            return block.first.inner_text(timeout=3000).strip() or "N/A"
    except Exception:
        pass
    return "N/A"


def _extract_location_from_profile(page: Page) -> str:
    """City, State from profile page."""
    try:
        loc_label = page.get_by_text("Location", exact=False).first
        if loc_label.count() > 0:
            sib = loc_label.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                return sib.first.inner_text(timeout=2000).strip() or "N/A"
    except Exception:
        pass
    return "N/A"


def _extract_contact_from_profile_only(page: Page, profile_url: str) -> Optional[BrokerContact]:
    """
    Extract a single BrokerContact entirely from a profile page (no list page data).
    Returns None if the profile does not pass the ECW keyword filter.
    """
    bio_specialties = _extract_bio_specialties(page)
    passes, keywords_found = _profile_contains_ecw_keyword(bio_specialties)
    if not passes:
        return None
    full_name = _safe_text(page.get_by_role("heading").first)
    company = "N/A"
    try:
        company_label = page.get_by_text("Company", exact=False).first
        if company_label.count() > 0:
            sib = company_label.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                company = sib.first.inner_text(timeout=2000).strip() or "N/A"
    except Exception:
        pass
    phone = _safe_text(page.locator("a[href^='tel:']").first)
    location = _extract_location_from_profile(page)
    email = _extract_email_from_profile(page)
    notes = "; ".join(keywords_found) if keywords_found else "N/A"
    return BrokerContact(
        full_name=full_name,
        phone_number=phone,
        location=location,
        company=company,
        email=email,
        source_url=profile_url,
        notes=notes,
    )


def _get_listing_cards_and_links(page: Page):
    """
    On a state directory page, find all broker listing blocks that contain
    a "more details" link. Yields (card_locator, more_details_link_locator)
    for each listing. Uses auto-waiting via Playwright locators.
    """
    # Link that contains "more details" (with optional »)
    more_details = page.get_by_role("link", name=re.compile(r"more\s+details\s*»?", re.I))
    try:
        n = more_details.count()
    except Exception:
        n = 0
    for i in range(n):
        link = more_details.nth(i)
        try:
            # Parent listing container: walk up to a likely card/row (div, article, li, section)
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
    """
    Heuristic: from a block of listing text, try to get Full Name, Company, Phone.
    Name is often first line; phone often has digits/dashes; company may be second line or before phone.
    """
    name, company, phone = "N/A", "N/A", "N/A"
    if not card_text:
        return name, company, phone
    lines = [ln.strip() for ln in card_text.splitlines() if ln.strip()]
    # Phone: first token that looks like (xxx) xxx-xxxx or xxx-xxx-xxxx
    phone_match = re.search(r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}", card_text)
    if phone_match:
        phone = phone_match.group(0).strip()
    if lines:
        name = lines[0]
    if len(lines) >= 2 and lines[1] != phone and not re.match(r"^\(?\d{3}\)?", lines[1]):
        company = lines[1]
    return name, company, phone


def _scrape_state_page(
    page: Page, state_name: str, state_url: str, max_profiles: Optional[int] = None
) -> List[BrokerContact]:
    """
    Navigate to state directory, iterate listings, open each profile via "more details",
    extract list + profile data, and return only contacts that pass ECW keyword filter.
    If max_profiles is set, stop after visiting that many profiles per state (for quick tests).
    """
    contacts: List[BrokerContact] = []
    page.goto(state_url, wait_until="domcontentloaded", timeout=60000)
    # Don't use networkidle — IBBA page has ongoing traffic. Wait for list content instead.
    try:
        page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=25000)
    except Exception:
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

            # Open profile (more reliable than click for SPA)
            page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_load_state("load", timeout=15000)

            bio_specialties = _extract_bio_specialties(page)
            passes, keywords_found = _profile_contains_ecw_keyword(bio_specialties)
            if not passes:
                # Go back to state list for next listing
                page.goto(state_url, wait_until="domcontentloaded", timeout=30000)
                page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=20000)
                continue

            email = _extract_email_from_profile(page)
            location = _extract_location_from_profile(page)
            # Prefer list page name/company/phone; fallback to profile if we have them
            full_name = list_name if list_name != "N/A" else _safe_text(page.get_by_role("heading").first)
            company = list_company if list_company != "N/A" else _safe_text(page.get_by_text("Company", exact=False).first.locator("xpath=following-sibling::*[1]"))
            phone = list_phone if list_phone != "N/A" else _safe_text(page.locator("a[href^='tel:']").first)

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

            # Return to state list for next broker
            page.goto(state_url, wait_until="domcontentloaded", timeout=30000)
            page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=20000)
        except Exception as e:
            # Log and continue; avoid one bad listing killing the run
            print(f"  Skip broker at {state_url}: {e}")
            try:
                page.goto(state_url, wait_until="domcontentloaded", timeout=30000)
                page.get_by_role("link", name=re.compile(r"more\s+details", re.I)).first.wait_for(state="visible", timeout=20000)
            except Exception:
                pass
            continue

    return contacts


def _save_and_upload_contacts(all_contacts: List[BrokerContact]) -> None:
    """Build dataframe, dedupe, save CSV, upload to Google Sheet. Used on normal finish and on Ctrl+C."""
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


def run_single_profile_test(profile_url: str, headless: bool = False) -> None:
    """
    Scrape one profile URL, then save to CSV and upload to Google Sheet.
    Used to verify end-to-end (e.g. Harry Caruso) and test Google Sheets.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()
        page.set_default_timeout(30000)
        page.set_default_navigation_timeout(60000)
        page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_load_state("load", timeout=15000)
        contact = _extract_contact_from_profile_only(page, profile_url)
        browser.close()
    if contact is None:
        print("This profile did not match ECW keywords. No contact added.")
        return
    print(f"Extracted 1 contact: {contact.full_name}. Saving and uploading...")
    _save_and_upload_contacts([contact])
    print("Single-profile test done.")


def scrape_ibba_directory(headless: bool = True) -> None:
    """
    Wide-to-Narrow IBBA scraper for ECW brokers in NY and FL:
    - Scrape state directory pages (no search radius).
    - Open each broker via "more details »", extract list + profile data.
    - Keep only brokers whose Bio/Specialties contain an ECW keyword.
    - Deduplicate, save CSV, upload to Google Sheets with requested headers.
    - On Ctrl+C: stop scraping and still save/upload whatever was collected.
    """
    all_contacts: List[BrokerContact] = []

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless)
            page = browser.new_page()
            page.set_default_timeout(30000)
            page.set_default_navigation_timeout(60000)

            for state_name, state_url in STATE_DIRECTORY_URLS:
                print(f"Scraping state: {state_name} — {state_url}")
                state_contacts = _scrape_state_page(
                    page, state_name, state_url, max_profiles=MAX_PROFILES_PER_STATE
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
    if TEST_SINGLE_PROFILE_URL:
        print(f"Single-profile test: {TEST_SINGLE_PROFILE_URL}")
        run_single_profile_test(TEST_SINGLE_PROFILE_URL, headless=False)
    else:
        scrape_ibba_directory(headless=False)
