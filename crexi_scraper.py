from __future__ import annotations

import argparse
import random
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
from ecw_scraper_google_sheets import append_row_to_google_sheet

DIRECTORY_URL = (
    "https://www.crexi.com/resources/find-a-broker/Florida/Special_Purpose/"
    "CCIM%2CSIOR%2CREALTOR%2CCREW%2CCRRP%2CICSC"
)

NY_DIRECTORY_URL = (
    "https://www.crexi.com/resources/find-a-broker/New_York/Special_Purpose/"
    "CCIM%2CSIOR%2CNAIOP%2CREALTOR%2CCREW%2CCRRP%2CICSC"
)

START_PAGE = 1
COLLECT_PAGES = 50
# NY directory: scrape only page 21 (directory ends at page 21)
NY_START_PAGE = 21
NY_COLLECT_PAGES = 1

KEYWORDS = [
    "express car wash",
    "express wash",
    "tunnel wash",
    "car wash",
    "carwash",
    "conveyor wash",
    "owner-user sale",
    "owner-user",
    "pad site",
    "retail pad site",
    "underperforming asset",
    "automotive real estate",
    "stand-alone building",
    "stand-alone retail",
    "service station",
    "gas station",
    "oil change",
    "lube center",
    "tire shop",
    "sale leaseback",
    "slb",
]

OUTPUT_CSV = "crexi_ecw_brokers.csv"
SHEET_ID = "1MMnxeTTlf9noOKmmvGEBl9xPinsNTd12ZXv7lBa6S9A"
WORKSHEET_NAME = "CRE Brokers"
SERVICE_ACCOUNT_JSON = "ecw-broker-scraper-ef955c25c30d.json"

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

_PHONE_PATTERNS = [
    r"\(?\d{3}\)?[\s.\-]?\d{3}[\s.\-]?\d{4}",
    r"\d{3}[\s.\-]\d{3}[\s.\-]\d{4}",
    r"\d{10}",
]

_EMAIL_PATTERN = r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}"

_BLOCKED_EMAILS = {"support@crexi.com"}
_BLOCKED_PHONE_DIGITS = {"8882730423"}

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


def _random_delay() -> None:
    time.sleep(random.uniform(2, 5))


def _apply_stealth_mode(page: Page) -> None:
    try:
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            window.chrome = { runtime: {} };
        """)
    except Exception:
        pass


def _is_cloudflare_challenge(page: Page) -> bool:
    try:
        url = page.url.lower()
        title = (page.title() or "").lower()
        if "challenge" in url or "cloudflare" in title or "cf-browser-verification" in url:
            return True
        body = page.inner_text("body", timeout=3000) or ""
        return "verify you are human" in body.lower() or "performing security verification" in body.lower()
    except Exception:
        return False


def _wait_for_cloudflare_pass(page: Page, max_wait_seconds: int = 120) -> bool:
    start = time.time()
    while (time.time() - start) < max_wait_seconds:
        if not _is_cloudflare_challenge(page):
            time.sleep(2)
            if not _is_cloudflare_challenge(page):
                return True
        time.sleep(2)
    return False


def _scroll_until_content_loaded(page: Page, max_scrolls: int = 10, pause: float = 0.3) -> None:
    try:
        prev_height = page.evaluate("document.body.scrollHeight")
        no_change_count = 0
        max_no_change = 2
        
        for _ in range(max_scrolls):
            page.evaluate("window.scrollBy(0, window.innerHeight)")
            time.sleep(pause)
            
            current_height = page.evaluate("document.body.scrollHeight")
            if current_height == prev_height:
                no_change_count += 1
                if no_change_count >= max_no_change:
                    break
            else:
                no_change_count = 0
                prev_height = current_height
        
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        time.sleep(0.5)
    except Exception:
        pass


def _click_read_more_if_exists(page: Page) -> None:
    try:
        read_more = page.get_by_text("Read more", exact=False).first
        if read_more.count() > 0:
            read_more.scroll_into_view_if_needed(timeout=3000)
            read_more.click()
            time.sleep(1.5)
    except Exception:
        pass


def _scroll_to_listings_sections(page: Page) -> None:
    try:
        active_listings = page.get_by_text("Active Listings", exact=False).first
        if active_listings.count() > 0:
            active_listings.scroll_into_view_if_needed(timeout=5000)
            time.sleep(0.5)
            _scroll_until_content_loaded(page, max_scrolls=5, pause=0.3)
    except Exception:
        pass
    
    try:
        sold_listings = page.get_by_text("Sold Listings", exact=False).first
        if sold_listings.count() > 0:
            sold_listings.scroll_into_view_if_needed(timeout=5000)
            time.sleep(0.5)
            _scroll_until_content_loaded(page, max_scrolls=5, pause=0.3)
    except Exception:
        pass
    
    try:
        listings_section = page.locator("[class*='listing'], [class*='property'], [data-testid*='listing']").first
        if listings_section.count() > 0:
            listings_section.scroll_into_view_if_needed(timeout=5000)
            time.sleep(0.5)
            _scroll_until_content_loaded(page, max_scrolls=5, pause=0.3)
    except Exception:
        pass
    
    _scroll_until_content_loaded(page, max_scrolls=3, pause=0.3)


def _extract_property_type_from_text(text: str) -> str:
    text_lower = text.lower()
    property_types = [
        "gas station", "former gas station", "gas station site",
        "car wash", "carwash", "express car wash", "tunnel wash",
        "retail", "retail property", "retail building",
        "office", "office building", "office space",
        "industrial", "warehouse", "distribution",
        "land", "vacant land", "development land",
        "restaurant", "qsr", "fast food",
        "automotive", "auto service", "auto repair",
        "special purpose", "specialty",
    ]
    for prop_type in property_types:
        if prop_type in text_lower:
            return prop_type.title()
    if "former" in text_lower:
        words = text_lower.split()
        idx = words.index("former")
        if idx + 1 < len(words):
            return f"Former {words[idx + 1].title()}"
    return ""


def _extract_listings_data(page: Page) -> Tuple[List[dict], List[dict]]:
    active_listings = []
    sold_listings = []
    try:
        body_text = page.inner_text("body", timeout=10000) or ""
        
        listing_cards = page.locator("[class*='listing'], [class*='property'], [class*='property-card'], [class*='listing-card'], [data-testid*='listing'], article, [role='article']")
        try:
            n = listing_cards.count()
        except Exception:
            n = 0
        
        for i in range(min(n, 100)):
            try:
                card = listing_cards.nth(i)
                title = _safe_text(card.locator("h2, h3, h4, h5, [class*='title'], [class*='name'], [class*='heading']"), "")
                description = _safe_text(card.locator("[class*='description'], [class*='overview'], [class*='summary'], [class*='details'], p"), "")
                listing_type_raw = _safe_text(card.locator("[class*='type'], [class*='category'], [class*='property-type']"), "")
                
                full_text_raw = (title + " " + description + " " + listing_type_raw).strip()
                if not full_text_raw:
                    continue
                
                full_text = full_text_raw.lower()
                property_type = _extract_property_type_from_text(full_text_raw)
                if not property_type and listing_type_raw:
                    property_type = listing_type_raw.strip()
                
                listing_data = {
                    "title": title.lower() if title else "",
                    "description": description.lower() if description else "",
                    "type": property_type if property_type else listing_type_raw.strip() if listing_type_raw else "",
                    "full_text": full_text
                }
                
                card_text = full_text
                parent = card.locator("xpath=..")
                try:
                    parent_text = _safe_text(parent, "").lower()
                    card_text += " " + parent_text
                except Exception:
                    pass
                
                if "sold" in card_text or "closed" in card_text or "transaction" in card_text:
                    sold_listings.append(listing_data)
                elif "active" in card_text or "available" in card_text or "for sale" in card_text or "for lease" in card_text:
                    active_listings.append(listing_data)
                else:
                    active_listings.append(listing_data)
            except Exception:
                continue
        
        if not active_listings and not sold_listings:
            text_lower = body_text.lower()
            sections_to_check = [
                ("active listings", "active"),
                ("active properties", "active"),
                ("sold listings", "sold"),
                ("sold properties", "sold"),
                ("closed transactions", "sold"),
            ]
            
            for section_text, section_type in sections_to_check:
                if section_text in text_lower:
                    try:
                        section_header = page.get_by_text(section_text, exact=False).first
                        if section_header.count() > 0:
                            section = section_header.locator("xpath=following::*[1]")
                            section_content = _safe_text(section, "")
                            if section_content and len(section_content) > 50:
                                prop_type = _extract_property_type_from_text(section_content)
                                listing_data = {
                                    "title": "",
                                    "description": section_content.lower(),
                                    "type": prop_type if prop_type else "",
                                    "full_text": section_content.lower()
                                }
                                if section_type == "sold":
                                    sold_listings.append(listing_data)
                                else:
                                    active_listings.append(listing_data)
                    except Exception:
                        pass
    except Exception:
        pass
    
    return active_listings, sold_listings


def _check_keywords_in_listings(active_listings: List[dict], sold_listings: List[dict]) -> Tuple[bool, List[str], Optional[str], Optional[str]]:
    found_keywords = []
    matched_listing_type = None
    match_source = None
    
    for listing in active_listings:
        full_text = listing.get("full_text", "")
        listing_type = listing.get("type", "")
        title = listing.get("title", "")
        description = listing.get("description", "")
        
        for kw in KEYWORDS:
            if kw.lower() in full_text:
                if kw not in found_keywords:
                    found_keywords.append(kw)
                if not matched_listing_type:
                    if listing_type:
                        matched_listing_type = listing_type
                    elif title:
                        prop_type = _extract_property_type_from_text(title)
                        if prop_type:
                            matched_listing_type = prop_type
                        else:
                            matched_listing_type = title[:50] if len(title) > 0 else "Property Listing"
                    elif description:
                        prop_type = _extract_property_type_from_text(description[:200])
                        if prop_type:
                            matched_listing_type = prop_type
                        else:
                            matched_listing_type = "Property Listing"
                    else:
                        matched_listing_type = "Property Listing"
                    match_source = "Active"
    
    for listing in sold_listings:
        full_text = listing.get("full_text", "")
        listing_type = listing.get("type", "")
        title = listing.get("title", "")
        description = listing.get("description", "")
        
        for kw in KEYWORDS:
            if kw.lower() in full_text:
                if kw not in found_keywords:
                    found_keywords.append(kw)
                if not matched_listing_type:
                    if listing_type:
                        matched_listing_type = listing_type
                    elif title:
                        prop_type = _extract_property_type_from_text(title)
                        if prop_type:
                            matched_listing_type = prop_type
                        else:
                            matched_listing_type = title[:50] if len(title) > 0 else "Property Listing"
                    elif description:
                        prop_type = _extract_property_type_from_text(description[:200])
                        if prop_type:
                            matched_listing_type = prop_type
                        else:
                            matched_listing_type = "Property Listing"
                    else:
                        matched_listing_type = "Property Listing"
                    match_source = "Sold"
    
    return (len(found_keywords) > 0, found_keywords, matched_listing_type, match_source)


def _profile_contains_keywords(page: Page) -> Tuple[bool, List[str], Optional[str], Optional[str], List[dict], List[dict]]:
    try:
        body = page.inner_text("body", timeout=10000) or ""
        lower = body.lower()
        bio_keywords = [kw for kw in KEYWORDS if kw.lower() in lower]
        
        _scroll_to_listings_sections(page)
        active_listings, sold_listings = _extract_listings_data(page)
        
        listing_match, listing_keywords, listing_type, match_source = _check_keywords_in_listings(active_listings, sold_listings)
        
        all_keywords = list(set(bio_keywords + listing_keywords))
        has_match = len(all_keywords) > 0
        
        if listing_match and listing_type:
            return (has_match, all_keywords, listing_type, match_source, active_listings, sold_listings)
        elif listing_match:
            return (has_match, all_keywords, "Property Listing", match_source, active_listings, sold_listings)
        else:
            return (has_match, all_keywords, None, None, active_listings, sold_listings)
    except Exception:
        return (False, [], None, None, [], [])


def _extract_phone_from_text(text: str) -> str:
    if not text:
        return "N/A"
    for pattern in _PHONE_PATTERNS:
        match = re.search(pattern, text)
        if match:
            phone = match.group(0).strip()
            digits = re.sub(r"\D", "", phone)
            if len(digits) == 11 and digits.startswith("1"):
                digits = digits[1:]
            if len(digits) == 10 and digits not in _BLOCKED_PHONE_DIGITS:
                return phone
    return "N/A"


def _extract_email_from_text(text: str) -> str:
    if not text:
        return "N/A"
    match = re.search(_EMAIL_PATTERN, text)
    if match:
        email = match.group(0).strip().lower()
        if email not in _BLOCKED_EMAILS:
            return match.group(0).strip()
    return "N/A"


def _extract_name(page: Page) -> str:
    try:
        h1 = page.locator("h1").first
        if h1.count() > 0:
            text = h1.inner_text(timeout=5000).strip()
            if text:
                return text
    except Exception:
        pass
    try:
        heading = page.get_by_role("heading").first
        if heading.count() > 0:
            text = heading.inner_text(timeout=5000).strip()
            if text:
                return text
    except Exception:
        pass
    return "N/A"


def _is_valid_company_candidate(text: str) -> bool:
    if not text or len(text) < 2 or len(text) > 200:
        return False
    lower = text.lower()
    skip = [
        "city", "state", "location", "address", "zip", "phone", "email", "@",
        "logo", "image", "photo", "picture", "profile", "broker", "view",
    ]
    return not any(x in lower for x in skip)


def _extract_company(page: Page) -> str:
    try:
        body_text = page.inner_text("body", timeout=5000) or ""
        lines = [line.strip() for line in body_text.split("\n") if line.strip()]

        for i, line in enumerate(lines):
            if _RE_CITY_STATE_ZIP.search(line) or _RE_CITY_ST.search(line):
                if i > 0:
                    company_candidate = lines[i - 1]
                    if _is_valid_company_candidate(company_candidate):
                        return company_candidate
                break

        location_element = None
        pin_icon = page.locator("[class*='pin'], [class*='location'], [class*='map'], svg[class*='pin'], svg[class*='location'], [data-testid*='location'], [aria-label*='location' i]").first
        if pin_icon.count() > 0:
            location_element = pin_icon
        else:
            location_div = page.locator("[class*='location'], [class*='address'], [class*='city']").first
            if location_div.count() > 0:
                location_element = location_div

        if location_element:
            try:
                parent = location_element.locator("xpath=..")
                if parent.count() > 0:
                    parent_text = parent.first.inner_text(timeout=3000).strip()
                    if parent_text:
                        plines = [l.strip() for l in parent_text.split("\n") if l.strip()]
                        for j, pline in enumerate(plines):
                            if _RE_CITY_STATE_ZIP.search(pline) or _RE_CITY_ST.search(pline):
                                if j > 0 and _is_valid_company_candidate(plines[j - 1]):
                                    return plines[j - 1]
                                break
            except Exception:
                pass

            try:
                prev = location_element.locator("xpath=preceding-sibling::*[1]")
                if prev.count() > 0:
                    text = prev.first.inner_text(timeout=2000).strip()
                    if _is_valid_company_candidate(text):
                        return text
            except Exception:
                pass

            try:
                parent = location_element.locator("xpath=..")
                if parent.count() > 0:
                    grandparent = parent.locator("xpath=..")
                    if grandparent.count() > 0:
                        gp_text = grandparent.first.inner_text(timeout=3000).strip()
                        if gp_text:
                            gplines = [l.strip() for l in gp_text.split("\n") if l.strip()]
                            for k, gpline in enumerate(gplines):
                                if _RE_CITY_STATE_ZIP.search(gpline) or _RE_CITY_ST.search(gpline):
                                    if k > 0 and _is_valid_company_candidate(gplines[k - 1]):
                                        return gplines[k - 1]
                                    break
            except Exception:
                pass

            try:
                parent = location_element.locator("xpath=..")
                if parent.count() > 0:
                    prev_block = parent.locator("xpath=preceding-sibling::*[1]")
                    if prev_block.count() > 0:
                        text = prev_block.first.inner_text(timeout=2000).strip()
                        first_line = text.split("\n")[0].strip() if text else ""
                        if _is_valid_company_candidate(first_line):
                            return first_line
                        if _is_valid_company_candidate(text):
                            return text
            except Exception:
                pass

        logo = page.locator("img[class*='logo'], img[alt*='logo' i], img[src*='logo'], [class*='company-logo'], [class*='logo'] img").first
        if logo.count() > 0:
            try:
                parent = logo.locator("xpath=..")
                if parent.count() > 0:
                    parent_text = parent.first.inner_text(timeout=3000).strip()
                    if parent_text:
                        for line in parent_text.split("\n"):
                            line = line.strip()
                            if _is_valid_company_candidate(line):
                                return line
            except Exception:
                pass
            try:
                next_sibling = logo.locator("xpath=following-sibling::*[1]")
                if next_sibling.count() > 0:
                    text = next_sibling.first.inner_text(timeout=2000).strip()
                    if _is_valid_company_candidate(text):
                        return text
            except Exception:
                pass

        company_label = page.get_by_text("Company", exact=False).first
        if company_label.count() > 0:
            try:
                sib = company_label.locator("xpath=following-sibling::*[1]")
                if sib.count() > 0:
                    text = sib.first.inner_text(timeout=2000).strip()
                    if text and len(text) < 200 and len(text) > 1:
                        return text
            except Exception:
                pass
    except Exception:
        pass
    return "N/A"


def _extract_location(page: Page) -> str:
    try:
        pin_icon = page.locator("[class*='pin'], [class*='location'], [class*='map'], svg[class*='pin'], svg[class*='location'], [data-testid*='location'], [aria-label*='location' i]").first
        if pin_icon.count() > 0:
            parent = pin_icon.locator("xpath=..")
            if parent.count() > 0:
                text = parent.first.inner_text(timeout=3000).strip()
                if text:
                    for match in _RE_CITY_STATE_ZIP.finditer(text):
                        city = match.group(1).strip()
                        state_raw = match.group(2).strip()
                        if not city or len(city) > 50:
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
                        if not city or len(city) > 50:
                            continue
                        city_title = city.title() if city.isupper() else city
                        out = f"{city_title}, {state}"
                        if _CITY_STATE_ONLY.match(out):
                            return out
            sib = pin_icon.locator("xpath=following-sibling::*[1]")
            if sib.count() > 0:
                text = sib.first.inner_text(timeout=2000).strip()
                if text:
                    for match in _RE_CITY_STATE_ZIP.finditer(text):
                        city = match.group(1).strip()
                        state_raw = match.group(2).strip()
                        if not city or len(city) > 50:
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
                        if not city or len(city) > 50:
                            continue
                        city_title = city.title() if city.isupper() else city
                        out = f"{city_title}, {state}"
                        if _CITY_STATE_ONLY.match(out):
                            return out
    except Exception:
        pass
    try:
        location_div = page.locator("[class*='location'], [class*='address'], [class*='city']").first
        if location_div.count() > 0:
            text = location_div.first.inner_text(timeout=3000).strip()
            if text:
                for match in _RE_CITY_STATE_ZIP.finditer(text):
                    city = match.group(1).strip()
                    state_raw = match.group(2).strip()
                    if not city or len(city) > 50:
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
                    if not city or len(city) > 50:
                        continue
                    city_title = city.title() if city.isupper() else city
                    out = f"{city_title}, {state}"
                    if _CITY_STATE_ONLY.match(out):
                        return out
    except Exception:
        pass
    try:
        body = page.inner_text("body", timeout=5000) or ""
        body = body[:10000].replace("\u00a0", " ")
        text = " ".join(body.split())
        for match in _RE_CITY_STATE_ZIP.finditer(text):
            city = match.group(1).strip()
            state_raw = match.group(2).strip()
            if not city or len(city) > 50:
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
            if not city or len(city) > 50:
                continue
            city_title = city.title() if city.isupper() else city
            out = f"{city_title}, {state}"
            if _CITY_STATE_ONLY.match(out):
                return out
    except Exception:
        pass
    return "N/A"


_PROFILE_SLUG_RE = re.compile(r"crexi\.com/profile/([a-z0-9\-]+)", re.I)


def _get_profile_urls_from_page(page: Page, base_url: str) -> List[str]:
    """Extract all broker profile URLs from the current page. Scrolls and waits to ensure all content is loaded."""
    # Scroll to load all content
    _scroll_until_content_loaded(page, max_scrolls=8, pause=0.4)
    time.sleep(1)
    
    all_profile_urls: List[str] = []
    seen: Set[str] = set()
    
    # Try multiple selectors to catch all profile links
    selectors = [
        "a[href*='/profile/']",
        "a[href*='crexi.com/profile']",
        "[href*='/profile/']",
    ]
    
    for selector in selectors:
        try:
            profile_links = page.locator(selector)
            n = profile_links.count()
            for i in range(n):
                try:
                    href = profile_links.nth(i).get_attribute("href")
                    if not href:
                        continue
                    full = urljoin(base_url, href)
                    full_lower = full.lower()
                    if "/profile/" not in full_lower:
                        continue
                    match = _PROFILE_SLUG_RE.search(full_lower)
                    if not match or not match.group(1) or len(match.group(1)) < 3:
                        continue
                    if full in seen:
                        continue
                    seen.add(full)
                    all_profile_urls.append(full)
                except Exception:
                    continue
        except Exception:
            continue
    
    # Remove duplicates while preserving order
    unique_urls = []
    seen_unique = set()
    for url in all_profile_urls:
        if url not in seen_unique:
            seen_unique.add(url)
            unique_urls.append(url)
    
    # Skip top 3 (usually header/nav links)
    skip_top_n = 3
    if len(unique_urls) > skip_top_n:
        return unique_urls[skip_top_n:]
    return unique_urls


def _has_next_page(page: Page) -> bool:
    try:
        next_btn = page.get_by_role("button", name=re.compile(r"next", re.I)).first
        if next_btn.count() > 0:
            disabled = next_btn.get_attribute("disabled")
            aria_disabled = next_btn.get_attribute("aria-disabled")
            if disabled is None and aria_disabled != "true":
                return True
    except Exception:
        pass
    try:
        next_link = page.get_by_role("link", name=re.compile(r"next", re.I)).first
        if next_link.count() > 0:
            return True
    except Exception:
        pass
    try:
        next_arrow = page.locator("[aria-label*='next' i], [aria-label*='Next' i]").first
        if next_arrow.count() > 0:
            disabled = next_arrow.get_attribute("disabled")
            aria_disabled = next_arrow.get_attribute("aria-disabled")
            if disabled is None and aria_disabled != "true":
                return True
    except Exception:
        pass
    return False


def _click_next_page(page: Page) -> bool:
    """Click Next button/link. Scrolls to bottom, waits, then tries multiple selectors."""
    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
    time.sleep(1.5)
    
    # Try button first
    try:
        next_btn = page.get_by_role("button", name=re.compile(r"next", re.I)).first
        if next_btn.count() > 0:
            disabled = next_btn.get_attribute("disabled")
            aria_disabled = next_btn.get_attribute("aria-disabled")
            if disabled is None and aria_disabled != "true":
                next_btn.scroll_into_view_if_needed(timeout=5000)
                time.sleep(0.5)
                next_btn.click()
                time.sleep(3)  # Wait longer for page to load
                return True
    except Exception:
        pass
    
    # Try link
    try:
        next_link = page.get_by_role("link", name=re.compile(r"next", re.I)).first
        if next_link.count() > 0:
            next_link.scroll_into_view_if_needed(timeout=5000)
            time.sleep(0.5)
            next_link.click()
            time.sleep(3)
            return True
    except Exception:
        pass
    
    # Try aria-label
    try:
        next_arrow = page.locator("[aria-label*='next' i], [aria-label*='Next' i]").first
        if next_arrow.count() > 0:
            disabled = next_arrow.get_attribute("disabled")
            aria_disabled = next_arrow.get_attribute("aria-disabled")
            if disabled is None and aria_disabled != "true":
                next_arrow.scroll_into_view_if_needed(timeout=5000)
                time.sleep(0.5)
                next_arrow.click()
                time.sleep(3)
                return True
    except Exception:
        pass
    
    # Try numbered page links (e.g. "22", "23" if we're on page 21)
    try:
        # Look for page number links in pagination
        page_links = page.locator("a[href*='page'], button[aria-label*='page']")
        count = page_links.count()
        for i in range(count):
            try:
                link = page_links.nth(i)
                text = link.inner_text(timeout=2000).strip()
                # If it's a number higher than current, try clicking
                if text.isdigit():
                    link.scroll_into_view_if_needed(timeout=3000)
                    time.sleep(0.3)
                    link.click()
                    time.sleep(3)
                    return True
            except Exception:
                continue
    except Exception:
        pass
    
    return False


def _collect_all_profile_urls(page: Page, directory_url: str, start_page: int = 1, max_pages: int = 10) -> List[str]:
    """Collect profile URLs by clicking Next through pages. Start on page 1; click Next until we reach start_page, then collect through end_page. No page reload (keeps Special Purpose filter)."""
    all_urls: List[str] = []
    seen: Set[str] = set()
    end_page = start_page + max_pages - 1
    page_num = 1
    consecutive_failures = 0
    max_failures = 2

    while page_num <= end_page:
        try:
            current_url = page.url
            _scroll_until_content_loaded(page, max_scrolls=5, pause=0.3)
            time.sleep(0.5)

            if page_num >= start_page:
                profile_urls = _get_profile_urls_from_page(page, current_url)
                if not profile_urls:
                    print(f"  Page {page_num}: no profile links found — continuing...")
                    # Don't break - maybe page is still loading, try Next anyway
                else:
                    new_count = 0
                    for u in profile_urls:
                        if u not in seen:
                            seen.add(u)
                            all_urls.append(u)
                            new_count += 1
                    print(f"  Page {page_num}: {len(profile_urls)} links ({new_count} new) — total collected: {len(all_urls)}")
                    consecutive_failures = 0  # Reset on success
            else:
                if page_num == 1 and start_page > 1:
                    print(f"  Advancing to page {start_page}: clicking Next (now on page {page_num})...")
                else:
                    print(f"  Advancing... (now on page {page_num}, target page {start_page})")

            if page_num >= end_page:
                print(f"  Reached end page {end_page}. Stopping collection.")
                break
            
            # Try to go to next page
            if not _has_next_page(page):
                consecutive_failures += 1
                print(f"  Page {page_num}: No Next button found (failure {consecutive_failures}/{max_failures})")
                if consecutive_failures >= max_failures:
                    print(f"  Stopping: {consecutive_failures} consecutive failures to find Next button.")
                    break
                time.sleep(2)  # Wait a bit and try again
                continue
            
            if not _click_next_page(page):
                consecutive_failures += 1
                print(f"  Page {page_num}: Failed to click Next (failure {consecutive_failures}/{max_failures})")
                if consecutive_failures >= max_failures:
                    print(f"  Stopping: {consecutive_failures} consecutive failures to click Next.")
                    break
                time.sleep(2)
                continue
            
            # Successfully clicked Next
            consecutive_failures = 0
            page_num += 1
            time.sleep(random.uniform(1.5, 2.5))
        except Exception as e:
            print(f"  Page {page_num}: {e}")
            consecutive_failures += 1
            if consecutive_failures >= max_failures:
                break
            time.sleep(2)
            continue

    return all_urls


def _scrape_directory(
    page: Page,
    directory_url: str,
    seen_urls: Set[str],
    *,
    start_page: int = START_PAGE,
    max_pages: int = COLLECT_PAGES,
    sheet_id: Optional[str] = None,
    worksheet_name: str = "CRE Brokers",
    service_account_json: Optional[str] = None,
) -> List[BrokerContact]:
    end_page = start_page + max_pages - 1
    print(f"  Phase 1: Collecting broker profile links from page {start_page} to {end_page}...")
    profile_urls = _collect_all_profile_urls(page, directory_url, start_page=start_page, max_pages=max_pages)
    print(f"  Collected {len(profile_urls)} profile URLs. Phase 2: Scraping each broker...")

    contacts: List[BrokerContact] = []
    for i, profile_url in enumerate(profile_urls):
        if profile_url in seen_urls:
            continue
        seen_urls.add(profile_url)
        n = i + 1
        print(f"  [{n}] Checking profile...")
        try:
            contact = _scrape_single_profile(
                page,
                profile_url,
                entry_num=n,
                sheet_id=sheet_id,
                worksheet_name=worksheet_name,
                service_account_json=service_account_json,
            )
            if contact:
                contacts.append(contact)
            _random_delay()
        except Exception as e:
            print(f"  [{n}] Skip: {e}")
            _random_delay()
        if n % 20 == 0:
            print(f"  — Progress: {n}/{len(profile_urls)} profiles processed.")

    return contacts


def _scrape_single_profile(
    page: Page,
    profile_url: str,
    *,
    entry_num: Optional[int] = None,
    sheet_id: Optional[str] = None,
    worksheet_name: str = "CRE Brokers",
    service_account_json: Optional[str] = None,
) -> Optional[BrokerContact]:
    try:
        response = page.goto(profile_url, wait_until="domcontentloaded", timeout=60000)
        if response:
            final_url = response.url
            if final_url != profile_url and "crexi.com/profile" not in final_url:
                print(f"  Redirected from {profile_url} to {final_url}")
                return None
        page.wait_for_load_state("load", timeout=45000)
        time.sleep(1.5)
        
        current_url = page.url
        if "crexi.com/profile" not in current_url:
            print(f"  Page redirected to {current_url}, skipping")
            return None

        _click_read_more_if_exists(page)
        time.sleep(0.8)

        has_keywords, keywords_found, listing_type, match_source, active_listings, sold_listings = _profile_contains_keywords(page)
        if not has_keywords:
            num_pre = f"  [{entry_num}] " if entry_num is not None else "  "
            print(f"{num_pre}No keywords found")
            return None

        full_name = _extract_name(page)
        company = _extract_company(page)
        location = _extract_location(page)

        bio_text = page.inner_text("body", timeout=5000) or ""
        all_listings_text = " ".join([l.get("full_text", "") for l in active_listings + sold_listings])
        combined_text = bio_text + " " + all_listings_text
        
        phone = _extract_phone_from_text(combined_text)
        if phone == "N/A":
            phone = _extract_phone_from_text(bio_text)
        email = _extract_email_from_text(combined_text)
        if email == "N/A":
            email = _extract_email_from_text(bio_text)

        notes_parts = []
        display_type = listing_type or ""
        if match_source:
            if display_type and display_type.lower() not in ("active listings", "active", "property listing", "listing match"):
                notes_parts.append(f"{match_source}: {display_type}")
            elif keywords_found:
                notes_parts.append(f"{match_source}: {keywords_found[0].title()}")
            else:
                notes_parts.append(f"{match_source} Listing Match")
        if keywords_found:
            notes_parts.append(f"keywords: {'; '.join(keywords_found)}")
        notes = " — ".join(notes_parts) if notes_parts else "N/A"

        contact = BrokerContact(
            full_name=full_name,
            phone_number=phone,
            location=location,
            company=company,
            email=email,
            source_url=profile_url,
            notes=notes,
        )
        num_pre = f"  [{entry_num}] " if entry_num is not None else "  "
        print(f"{num_pre}+ {full_name} ({company}) — keywords: {'; '.join(keywords_found) if keywords_found else 'N/A'}")
        print(f"    Phone: {phone}, Email: {email}, Location: {location}")

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
                print("  ✓ Added to Google Sheet")
            except Exception as e:
                print(f"  (Sheet append failed: {e})")

        return contact
    except Exception as e:
        print(f"  Error scraping {profile_url}: {e}")
        return None


def scrape_crexi_directory(test_url: Optional[str] = None, use_ny: bool = False) -> None:
    all_contacts: List[BrokerContact] = []
    seen_urls: Set[str] = set()

    if test_url:
        print(f"TEST MODE: Scraping single profile: {test_url}")
        try:
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(
                        headless=False,
                        channel="chrome",
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                except Exception:
                    browser = p.chromium.launch(
                        headless=False,
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                    timezone_id="America/New_York",
                )
                page = context.new_page()
                _apply_stealth_mode(page)
                page.set_default_timeout(30000)
                page.set_default_navigation_timeout(60000)
                
                page.goto(test_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)
                if _is_cloudflare_challenge(page):
                    print("  ⚠️  Cloudflare challenge detected. Solve the captcha in the browser.")
                    if not _wait_for_cloudflare_pass(page, max_wait_seconds=120):
                        browser.close()
                        return
                    time.sleep(2)
                
                contact = _scrape_single_profile(
                    page,
                    test_url,
                    sheet_id=SHEET_ID or None,
                    worksheet_name=WORKSHEET_NAME,
                    service_account_json=SERVICE_ACCOUNT_JSON,
                )
                if contact:
                    all_contacts.append(contact)

                browser.close()
        except KeyboardInterrupt:
            print("\nStopped by user (Ctrl+C).")
    else:
        if SHEET_ID:
            print(f"Appending to worksheet {WORKSHEET_NAME!r} from next empty row.")

        directory_url = NY_DIRECTORY_URL if use_ny else DIRECTORY_URL

        try:
            with sync_playwright() as p:
                try:
                    browser = p.chromium.launch(
                        headless=False,
                        channel="chrome",
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                except Exception:
                    browser = p.chromium.launch(
                        headless=False,
                        args=["--disable-blink-features=AutomationControlled"],
                    )
                context = browser.new_context(
                    viewport={"width": 1920, "height": 1080},
                    locale="en-US",
                    timezone_id="America/New_York",
                )
                page = context.new_page()
                _apply_stealth_mode(page)
                page.set_default_timeout(30000)
                page.set_default_navigation_timeout(60000)
                
                print(f"Opening directory: {directory_url}")
                page.goto(directory_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(2)
                if _is_cloudflare_challenge(page):
                    print("  ⚠️  Cloudflare challenge detected. Solve the captcha in the browser.")
                    if not _wait_for_cloudflare_pass(page, max_wait_seconds=120):
                        browser.close()
                        return
                    time.sleep(2)
                
                print("Waiting 10 seconds before starting to scrape so you can adjust filters if needed...")
                time.sleep(10)

                print("Scraping directory...")
                start_page = NY_START_PAGE if use_ny else START_PAGE
                max_pages = NY_COLLECT_PAGES if use_ny else COLLECT_PAGES
                contacts = _scrape_directory(
                    page,
                    directory_url,
                    seen_urls,
                    start_page=start_page,
                    max_pages=max_pages,
                    sheet_id=SHEET_ID or None,
                    worksheet_name=WORKSHEET_NAME,
                    service_account_json=SERVICE_ACCOUNT_JSON,
                )
                all_contacts.extend(contacts)
                print(f"  Collected {len(contacts)} matching brokers.")

                browser.close()
        except KeyboardInterrupt:
            print("\nStopped by user (Ctrl+C). Saving what we have so far...")

    if all_contacts:
        df = contacts_to_dataframe(all_contacts)
        df_clean = clean_contacts_dataframe(df)
        save_to_csv(df_clean, OUTPUT_CSV)
        print(f"Total matching brokers: {len(all_contacts)}. After de-dup: {len(df_clean)}. Saved: {OUTPUT_CSV}")
    else:
        print("No matching brokers found.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape Crexi broker directory")
    parser.add_argument("--test", type=str, help="Test with a single profile URL")
    parser.add_argument(
        "--ny",
        action="store_true",
        help="Use the New York Special Purpose broker directory instead of Florida",
    )
    args = parser.parse_args()
    scrape_crexi_directory(test_url=args.test, use_ny=args.ny)
