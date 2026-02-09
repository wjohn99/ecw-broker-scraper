from __future__ import annotations

from typing import List
from urllib.parse import urljoin

from playwright.sync_api import Page, Playwright, sync_playwright

from ecw_scraper_data import BrokerContact, clean_contacts_dataframe, contacts_to_dataframe, save_to_csv
from ecw_scraper_google_sheets import upload_dataframe_to_google_sheet

IBBA_SEARCH_URL = "https://www.ibba.org/find-a-business-broker/"

DEFAULT_CITY_OR_ZIP = "Dallas, TX"
DEFAULT_DISTANCE_LABEL = "250 Miles"

OUTPUT_CSV = "ibba_ecw_brokers.csv"

SHEET_ID = "1MMnxeTTlf9noOKmmvGEBl9xPinsNTd12ZXv7lBa6S9A" 
WORKSHEET_NAME = "ECW Brokers"
SERVICE_ACCOUNT_JSON = "ecw-broker-scraper-ef955c25c30d.json"


def _safe_text_from_locator(locator, default: str = "N/A") -> str:
    try:
        if locator is None:
            return default
        if locator.count() == 0:
            return default
        return locator.first.inner_text().strip() or default
    except Exception:
        return default


def _configure_main_search(page: Page) -> None:
    """
    On the IBBA search page:
    - Fill City/Zip
    - Set distance
    - Select the "Auto Related Businesses" Specialty Area
    - Submit the search
    """
    page.goto(IBBA_SEARCH_URL, wait_until="networkidle")

    # City/Zip (required)
    try:
        city_input = page.get_by_placeholder("City/Zip")
    except Exception:
        # Fallback: label-based lookup
        city_input = page.get_by_label("City/Zip", exact=False)
    city_input.fill(DEFAULT_CITY_OR_ZIP)

    # Distance select
    try:
        distance_select = page.get_by_label("Distance")
        distance_select.select_option(label=DEFAULT_DISTANCE_LABEL)
    except Exception:
        # If the above fails, you can adjust this selector using dev tools.
        pass

    # Specialty Areas → Auto Related Businesses
    #
    # IBBA uses a custom widget here; the exact structure can change, so this
    # block may need a small tweak. The general idea is:
    #   1) Click "Specialty Areas" control to open it.
    #   2) Click the "Auto Related Businesses" option.
    try:
        # Try a role-based approach first.
        specialty_control = page.get_by_text("Specialty Areas", exact=False)
        specialty_control.click()

        option = page.get_by_text("Auto Related Businesses", exact=False)
        option.click()
    except Exception:
        # If this doesn't work, open DevTools on the page and replace this
        # block with precise locators for the widget.
        pass

    # Submit search
    try:
        search_button = page.get_by_role("button", name="Find A Business Broker")
    except Exception:
        # Fallback if role lookup fails
        search_button = page.get_by_text("Find A Business Broker", exact=False)

    search_button.click()
    # Let Playwright wait until the network is idle and results have rendered.
    page.wait_for_load_state("networkidle")

    # Ensure results section is visible
    try:
        page.get_by_text("Results for your Search").wait_for(timeout=15000)
    except Exception:
        # If this text changes, you can remove or adjust this guard.
        pass


def _collect_profile_urls_from_results(page: Page) -> List[str]:
    """
    On a search results page, return absolute URLs for all "View Profile" links.
    """
    urls: List[str] = []

    links = page.get_by_role("link", name="View Profile")
    try:
        count = links.count()
    except Exception:
        return urls

    for i in range(count):
        try:
            href = links.nth(i).get_attribute("href")
        except Exception:
            href = None
        if href:
            urls.append(urljoin(page.url, href))

    return urls


def _go_to_next_results_page(page: Page) -> bool:
    """
    Try to advance to the next search results page.
    Returns True if navigation occurred, False otherwise.
    """
    # You may need to tweak this selector depending on IBBA's pagination UI.
    next_link = page.get_by_role("link", name="Next")
    try:
        if next_link.count() == 0:
            return False
    except Exception:
        return False

    try:
        next_link.first.click()
        page.wait_for_load_state("networkidle")
        return True
    except Exception:
        return False


def _extract_contact_from_profile(page: Page, profile_url: str) -> BrokerContact:
    """
    On an individual broker profile page, capture:
    - Full Name
    - Company
    - Location (City, State)
    - Phone Number
    - Email Address
    - Notes (e.g. designations / specialties)

    All fields gracefully fall back to "N/A".
    """
    page.goto(profile_url, wait_until="networkidle")

    # Full name: typically the main heading on the page.
    full_name = _safe_text_from_locator(page.get_by_role("heading"))

    # Email: most reliably via mailto: links.
    email = _safe_text_from_locator(page.locator("a[href^='mailto:']"))

    # Phone: often in a tel: link or labeled field.
    phone = _safe_text_from_locator(page.locator("a[href^='tel:']"))

    # Company and location (City, State):
    #
    # These vary by layout; this implementation is conservative and may need a
    # small tweak. We try some common patterns and then fall back to "N/A".
    company = "N/A"
    location = "N/A"

    try:
        # Try a label-based pattern like "Company" / "Location"
        company_label = page.get_by_text("Company", exact=False).first
        company = (
            company_label.locator("xpath=following-sibling::*[1]").inner_text().strip()
        )
    except Exception:
        pass

    try:
        location_label = page.get_by_text("Location", exact=False).first
        location = (
            location_label.locator("xpath=following-sibling::*[1]")
            .inner_text()
            .strip()
        )
    except Exception:
        pass

    # Notes: capture visible text for specialties / designations where possible.
    notes_parts: List[str] = []

    try:
        designation_block = page.get_by_text("Specialty", exact=False)
        notes_parts.append(_safe_text_from_locator(designation_block))
    except Exception:
        pass

    # Fallback: capture a small snippet from a specific section if you prefer.
    notes = "; ".join([p for p in notes_parts if p and p != "N/A"]) or "N/A"

    return BrokerContact(
        full_name=full_name,
        phone_number=phone,
        location=location,
        company=company,
        email=email,
        source_url=profile_url,
        notes=notes,
    )


def scrape_ibba_directory(headless: bool = True) -> None:
    """
    Main entry point:
    - Run an IBBA search filtered to "Auto Related Businesses"
    - Iterate through all result pages
    - Visit each "View Profile" page and collect broker contacts
    - De-duplicate and save to CSV
    """
    all_contacts: List[BrokerContact] = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        page = browser.new_page()

        _configure_main_search(page)

        # Iterate through all pages of results
        while True:
            profile_urls = _collect_profile_urls_from_results(page)
            for url in profile_urls:
                contact = _extract_contact_from_profile(page, url)
                all_contacts.append(contact)

            if not _go_to_next_results_page(page):
                break

        browser.close()

    # DataFrame creation, de-dup, and CSV output
    df = contacts_to_dataframe(all_contacts)
    df_clean = clean_contacts_dataframe(df)
    save_to_csv(df_clean, OUTPUT_CSV)

    print(f"Scraped {len(all_contacts)} raw contacts.")
    print(f"{len(df_clean)} contacts after de-duplication.")
    print(f"Saved CSV to: {OUTPUT_CSV}")

    # Optional: also push to Google Sheets if SHEET_ID is set.
    if SHEET_ID:
        print(
            f"Uploading {len(df_clean)} contacts to Google Sheet {SHEET_ID!r}, "
            f"worksheet {WORKSHEET_NAME!r}..."
        )
        upload_dataframe_to_google_sheet(
            df_clean,
            sheet_id=SHEET_ID,
            worksheet_name=WORKSHEET_NAME,
            service_account_json_path=SERVICE_ACCOUNT_JSON,
        )
        print("Google Sheets upload completed.")


if __name__ == "__main__":
    # Run non-headless by default so you can see what the browser is doing.
    scrape_ibba_directory(headless=False)

