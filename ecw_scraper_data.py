from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from typing import List

import pandas as pd

_LOCATION_CITY_STATE = re.compile(r"^[^,]+,\s*[A-Za-z]{2}$")


@dataclass
class BrokerContact:
    full_name: str = ""
    phone_number: str = ""
    location: str = ""
    company: str = ""
    email: str = ""
    source_url: str = ""
    notes: str = ""


CSV_COLUMNS = [
    "Full Name",
    "Phone Number",
    "Location (City, State)",
    "Company",
    "Email Address",
    "URL",
    "Notes (URL Link)",
]


def contacts_to_dataframe(contacts: List[BrokerContact]) -> pd.DataFrame:
    def _na_or(value: str) -> str:
        if value is None:
            return "N/A"
        text = str(value).strip()
        return text if text else "N/A"

    def _location_city_state_only(loc: str) -> str:
        s = _na_or(loc)
        if s == "N/A" or len(s) > 80:
            return "N/A"
        return s if _LOCATION_CITY_STATE.match(s) else "N/A"

    records = []
    for c in contacts:
        records.append(
            {
                "Full Name": _na_or(c.full_name),
                "Phone Number": _na_or(c.phone_number),
                "Location (City, State)": _location_city_state_only(c.location),
                "Company": _na_or(c.company),
                "Email Address": _na_or(c.email),
                "URL": _na_or(c.source_url),
                "Notes (URL Link)": _na_or(c.notes),
            }
        )

    df = pd.DataFrame(records, columns=CSV_COLUMNS)
    return df


def clean_contacts_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    df = df.copy()
    for col in df.columns:
        df[col] = df[col].astype(str).str.strip()

    df["_key_with_phone"] = (
        df["Full Name"].str.lower().fillna("")
        + "|"
        + df["Phone Number"].str.replace(r"\D", "", regex=True).fillna("")
        + "|"
        + df["Company"].str.lower().fillna("")
    )
    df["_key_no_phone"] = (
        df["Full Name"].str.lower().fillna("")
        + "|"
        + df["Company"].str.lower().fillna("")
    )

    df["_dedup_key"] = df["_key_with_phone"]
    mask_no_phone = df["Phone Number"].str.strip() == ""
    df.loc[mask_no_phone, "_dedup_key"] = df.loc[mask_no_phone, "_key_no_phone"]

    df = df.drop_duplicates(subset="_dedup_key").drop(
        columns=["_key_with_phone", "_key_no_phone", "_dedup_key"]
    )
    return df


def save_to_csv(df: pd.DataFrame, path: str) -> None:
    df.to_csv(path, index=False)

