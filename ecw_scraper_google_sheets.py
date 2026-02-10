from __future__ import annotations

from typing import Optional

import pandas as pd

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    gspread = None
    Credentials = None


def upload_dataframe_to_google_sheet(
    df: pd.DataFrame,
    sheet_id: str,
    worksheet_name: str = "ECW Brokers",
    service_account_json_path: Optional[str] = None,
) -> None:
    if gspread is None or Credentials is None:
        raise RuntimeError(
            "gspread / google-auth are not installed. "
            "Either install them or disable Google Sheets upload."
        )

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]

    if service_account_json_path:
        creds = Credentials.from_service_account_file(
            service_account_json_path, scopes=scopes
        )
        client = gspread.authorize(creds)
    else:
        client = gspread.service_account()

    sh = client.open_by_key(sheet_id)

    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)

    worksheet.batch_clear(["A2:Z1000"])
    values = df.astype(str).fillna("").values.tolist()
    if values:
        worksheet.update("A2", values)


def clear_worksheet_data(
    sheet_id: str,
    worksheet_name: str = "ECW Brokers",
    service_account_json_path: Optional[str] = None,
) -> None:
    if gspread is None or Credentials is None:
        raise RuntimeError(
            "gspread / google-auth are not installed. "
            "Either install them or disable Google Sheets upload."
        )
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if service_account_json_path:
        creds = Credentials.from_service_account_file(
            service_account_json_path, scopes=scopes
        )
        client = gspread.authorize(creds)
    else:
        client = gspread.service_account()
    sh = client.open_by_key(sheet_id)
    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
    worksheet.batch_clear(["A2:Z1000"])


def append_row_to_google_sheet(
    row_values: list,
    sheet_id: str,
    worksheet_name: str = "ECW Brokers",
    service_account_json_path: Optional[str] = None,
) -> None:
    if gspread is None or Credentials is None:
        raise RuntimeError(
            "gspread / google-auth are not installed. "
            "Either install them or disable Google Sheets upload."
        )
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    if service_account_json_path:
        creds = Credentials.from_service_account_file(
            service_account_json_path, scopes=scopes
        )
        client = gspread.authorize(creds)
    else:
        client = gspread.service_account()
    sh = client.open_by_key(sheet_id)
    try:
        worksheet = sh.worksheet(worksheet_name)
    except gspread.WorksheetNotFound:
        worksheet = sh.add_worksheet(title=worksheet_name, rows=100, cols=20)
    row_str = [str(v) for v in row_values]
    worksheet.append_row(row_str, value_input_option="USER_ENTERED")

