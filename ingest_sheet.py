#!/usr/bin/env python3
"""
Ingest stock transactions from a downloaded sheet (CSV/XLSX), or from the live
Google Sheet, for a row range you supply.

Pipeline:  file/sheet -> read rows from:to -> normalise -> validate -> report
           -> (optional) write JSON/CSV -> (optional) commit to Postgres

Safe by default: without --commit nothing is written to the database.

Sheet layout this script expects (header names, in any order)
-------------------------------------------------------------
Stock Name | Symbol | Purchased Price (in USD - US stocks only) | Purchased Quantity
| Invested Amount | Current Price (in USD - US stocks only) | Current Amount
| Trend Since Purchase | P/L Amount | P/L Percentage
| Price 30 Days Ago | Trend since 30D | Price 90 Days Ago | Trend since 90D
| Price 6M Ago | Trend Since 6M | Price 1 Year Ago | Trend Since 1 Year
| Market Cap (in CR.) | Cap | Transaction Type (Default BUY)

Where each column goes
----------------------
transactions (first-class columns):
    Symbol -> instrument, Purchased Price -> price, Purchased Quantity -> quantity,
    Invested Amount -> invested_amount, Transaction Type -> type (blank = BUY)
transactions.sheet_snapshot (JSONB, values as of import time, not live):
    Current Price, Current Amount, Trend Since Purchase, P/L Amount, P/L Percentage
instruments (one row per stock, not per transaction):
    Stock Name -> name, Market Cap (in CR.) -> market_cap_cr, Cap -> cap_category
NOT imported (never touch the transactions table):
    Price 30 Days Ago, Trend since 30D, Price 90 Days Ago, Trend since 90D,
    Price 6M Ago, Trend Since 6M, Price 1 Year Ago, Trend Since 1 Year

Local file (recommended)
------------------------
In Google Sheets: File > Download > .csv (current tab) or .xlsx (whole workbook,
then pick the tab with --tab). Row numbers refer to rows in the file.

    pip install "psycopg[binary]"      # only needed for --commit
    pip install openpyxl               # only needed for .xlsx files

Live Google Sheet (optional)
----------------------------
    pip install google-api-python-client google-auth "psycopg[binary]"
Share the sheet (Viewer) with a service account and pass --credentials.

Examples
--------
    # Local CSV, dry run: rows 2..150
    python ingest_sheet.py --file stocks.csv --from-row 2 --to-row 150

    # Local XLSX, specific tab, commit to Postgres
    python ingest_sheet.py --file stocks.xlsx --tab "Purchases" --from-row 2 --to-row 150 \
        --db-url postgresql://user:pw@localhost/portfolio --commit

If your headers differ, map them:  --map ticker="GF Symbol" --map quantity="Qty"

Prices and currency
-------------------
* Purchased Price is "USD - US stocks only". If it is blank but Invested Amount is
  present, price is derived as invested_amount / quantity and flagged
  price_derived = true.
* price_currency is inferred: USD when a price was given AND the ticker's exchange
  is a US exchange (NASDAQ, NYSE, ...); otherwise --base-currency (default INR).
  A bare symbol with no exchange prefix (e.g. "AAPL") is treated as base currency
  and reported in the warnings.
* Invested Amount is stored as given, assumed to be in the base currency.

Idempotency
-----------
Each row gets a hash of (ticker, date if any, qty, price, invested amount, type)
plus an occurrence counter, so two genuinely identical buys stay as two rows and
re-running the same range inserts nothing new (ON CONFLICT DO NOTHING). The sheet
has no date column, so if you EDIT a row's values and re-import, it is treated as
a new transaction. Re-import the same range, or non-overlapping ranges.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import sys
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly"

# Header names are lower-cased, whitespace-collapsed and stripped of "(...)" notes
# before matching, so "Purchased Price (in USD - US stocks only)" -> "purchased price".
ALIASES: dict[str, set[str]] = {
    "name": {"stock name", "name", "stock", "company", "company name", "security"},
    "ticker": {"symbol", "ticker", "ticker symbol", "google finance ticker", "google finance symbol"},
    "price": {"purchased price", "purchase price", "buy price", "price"},
    "quantity": {"purchased quantity", "quantity", "qty", "shares", "units"},
    "invested_amount": {"invested amount", "invested", "amount invested"},
    "type": {"transaction type", "type", "side", "action"},
    "market_cap_cr": {"market cap", "market cap in cr", "market cap in cr."},
    "cap_category": {"cap", "cap category", "cap type"},
    # Optional: not in the current sheet, picked up automatically if you add one.
    "trade_date": {"date", "trade date", "purchase date", "purchased date", "buy date", "transaction date"},
    # Snapshot-only fields (stored in transactions.sheet_snapshot)
    "current_price": {"current price"},
    "current_amount": {"current amount"},
    "trend_since_purchase": {"trend since purchase"},
    "pl_amount": {"p/l amount", "pl amount", "p&l amount"},
    "pl_percentage": {"p/l percentage", "pl percentage", "p/l %", "p&l percentage"},
}
SNAPSHOT_NUMERIC = ("current_price", "current_amount", "pl_amount", "pl_percentage")
SNAPSHOT_TEXT = ("trend_since_purchase",)

# Columns deliberately NOT imported into the transactions table.
IGNORED_HEADERS = {
    "price 30 days ago", "trend since 30d",
    "price 90 days ago", "trend since 90d",
    "price 6m ago", "trend since 6m",
    "price 1 year ago", "trend since 1 year",
}

US_EXCHANGES = {"NASDAQ", "NYSE", "NYSEARCA", "NYSEAMERICAN", "NYSEMKT", "BATS", "OTCMKTS"}

DAY_FIRST = [
    "%d/%m/%Y", "%d-%m-%Y", "%d.%m.%Y", "%d/%m/%y", "%d-%m-%y",
    "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M", "%d-%m-%Y %H:%M:%S",
]
MONTH_FIRST = [
    "%m/%d/%Y", "%m-%d-%Y", "%m/%d/%y",
    "%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M", "%b %d, %Y", "%B %d, %Y",
]
UNAMBIGUOUS = [
    "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d",
    "%d %b %Y", "%d-%b-%Y", "%d %B %Y", "%d-%B-%Y", "%d %b %Y %H:%M:%S",
]

DDL = """
CREATE TABLE IF NOT EXISTS instruments (
  id BIGSERIAL PRIMARY KEY,
  exchange TEXT NOT NULL DEFAULT '',
  symbol TEXT NOT NULL,
  name TEXT,
  market_cap_cr NUMERIC(18,2),
  cap_category TEXT,
  UNIQUE (exchange, symbol)
);
CREATE TABLE IF NOT EXISTS transactions (
  id BIGSERIAL PRIMARY KEY,
  instrument_id BIGINT NOT NULL REFERENCES instruments(id),
  type TEXT NOT NULL CHECK (type IN ('BUY','SELL')),
  quantity NUMERIC(18,4) NOT NULL CHECK (quantity > 0),
  price NUMERIC(18,4) NOT NULL CHECK (price >= 0),
  price_derived BOOLEAN NOT NULL DEFAULT FALSE,
  price_currency TEXT,
  invested_amount NUMERIC(18,4),
  fees NUMERIC(18,4) NOT NULL DEFAULT 0,
  trade_date TIMESTAMP,
  notes TEXT,
  sheet_snapshot JSONB,
  source TEXT NOT NULL,
  source_row_hash TEXT UNIQUE,
  import_batch_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  deleted_at TIMESTAMPTZ
);
"""


@dataclass
class Txn:
    sheet_row: int
    name: Optional[str]
    exchange: Optional[str]
    symbol: str
    trade_type: str
    quantity: Decimal
    price: Decimal
    price_derived: bool
    price_currency: str
    invested_amount: Optional[Decimal]
    trade_date: Optional[datetime]
    market_cap_cr: Optional[Decimal]
    cap_category: Optional[str]
    snapshot: dict[str, str] = field(default_factory=dict)
    source_hash: str = ""


# --------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------

def norm_header(h: Any) -> str:
    s = str(h).lower()
    s = re.sub(r"\(.*?\)", "", s)
    return re.sub(r"\s+", " ", s).strip()


def parse_decimal(raw: Any, label: str) -> Optional[Decimal]:
    """Strict: blank -> None, garbage -> ValueError."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = re.sub(r"(?i)\b(rs|inr|usd)\.?", "", s)
    s = re.sub(r"[A-Za-z₹$€£,\s()%]", "", s)
    try:
        value = Decimal(s)
    except InvalidOperation:
        raise ValueError(f"{label}: cannot parse number from {raw!r}")
    return -value if negative else value


def lenient_decimal(raw: Any) -> Optional[Decimal]:
    """Lenient: anything unparseable (#N/A, 'Loading...') -> None."""
    try:
        return parse_decimal(raw, "")
    except ValueError:
        return None


def clean_text(raw: Any) -> Optional[str]:
    if raw is None:
        return None
    s = str(raw).strip()
    return None if not s or s.startswith("#") else s


def parse_datetime(raw: Any, month_first: bool) -> Optional[datetime]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    if re.fullmatch(r"\d{5}(\.\d+)?", s):  # Sheets/Excel serial date
        return datetime(1899, 12, 30) + timedelta(days=float(s))
    formats = UNAMBIGUOUS + (MONTH_FIRST + DAY_FIRST if month_first else DAY_FIRST + MONTH_FIRST)
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"trade_date: cannot parse date from {raw!r}")


def parse_type(raw: Any) -> str:
    s = (str(raw).strip().lower() if raw is not None else "")
    if s in ("", "buy", "b", "purchase", "bought"):  # blank defaults to BUY
        return "BUY"
    if s in ("sell", "s", "sale", "sold"):
        return "SELL"
    raise ValueError(f"type: expected BUY or SELL, got {raw!r}")


def split_ticker(raw: Any) -> tuple[Optional[str], str]:
    s = str(raw or "").strip()
    if not s:
        raise ValueError("ticker: missing")
    m = re.match(r"^([A-Za-z]+)\s*:\s*(.+)$", s)
    if m:
        return m.group(1).upper(), m.group(2).strip().upper()
    return None, s.upper()


def canon(d: Optional[Decimal]) -> str:
    return "" if d is None else format(d.normalize(), "f")


# --------------------------------------------------------------------------
# Core processing (pure functions, no I/O)
# --------------------------------------------------------------------------

def build_column_map(header: list[Any], overrides: dict[str, str]) -> dict[str, int]:
    normalized = [norm_header(h) for h in header]
    mapping: dict[str, int] = {}

    for fld, label in overrides.items():
        n = norm_header(label)
        if n not in normalized:
            raise SystemExit(f"--map {fld}: column {label!r} not found in header {list(header)}")
        mapping[fld] = normalized.index(n)

    for fld, names in ALIASES.items():
        if fld in mapping:
            continue
        for idx, h in enumerate(normalized):
            if h in names and idx not in mapping.values():
                mapping[fld] = idx
                break

    missing = [f for f in ("ticker", "quantity") if f not in mapping]
    if "price" not in mapping and "invested_amount" not in mapping:
        missing.append("price or invested_amount")
    if missing:
        raise SystemExit(
            f"Could not find column(s) for: {', '.join(missing)}.\n"
            f"Header seen: {list(header)}\n"
            f'Use --map, e.g. --map ticker="<your column name>"'
        )
    return mapping


def describe_other_columns(header: list[Any], mapping: dict[str, int]) -> tuple[list[str], list[str]]:
    """Return (ignored-by-design columns found, unrecognised columns)."""
    ignored, unknown = [], []
    mapped = set(mapping.values())
    for i, h in enumerate(header):
        if i in mapped or not str(h).strip():
            continue
        (ignored if norm_header(h) in IGNORED_HEADERS else unknown).append(str(h).strip())
    return ignored, unknown


def process_rows(
    rows: list[list[Any]], start_row: int, mapping: dict[str, int],
    month_first: bool, base_currency: str,
) -> tuple[list[Txn], list[tuple[int, str]]]:
    good: list[Txn] = []
    bad: list[tuple[int, str]] = []

    for offset, row in enumerate(rows):
        sheet_row = start_row + offset
        if not any(str(c).strip() for c in row):
            continue  # blank row

        def cell(fld: str) -> Any:
            i = mapping.get(fld)
            return row[i] if i is not None and i < len(row) else None

        try:
            exchange, symbol = split_ticker(cell("ticker"))

            qty = parse_decimal(cell("quantity"), "quantity")
            if qty is None or qty <= 0:
                raise ValueError(f"quantity: must be > 0, got {cell('quantity')!r}")

            price = parse_decimal(cell("price"), "price")
            invested = parse_decimal(cell("invested_amount"), "invested_amount")
            if invested is not None and invested < 0:
                raise ValueError(f"invested_amount: must be >= 0, got {cell('invested_amount')!r}")

            derived = False
            if price is None:
                if invested is None:
                    raise ValueError("price and invested_amount are both missing")
                price = (invested / qty).quantize(Decimal("0.0001"))
                derived = True
            if price < 0:
                raise ValueError(f"price: must be >= 0, got {cell('price')!r}")

            currency = "USD" if (not derived and exchange in US_EXCHANGES) else base_currency

            snapshot: dict[str, str] = {}
            for key in SNAPSHOT_NUMERIC:
                v = lenient_decimal(cell(key))
                if v is not None:
                    snapshot[key] = canon(v)
            for key in SNAPSHOT_TEXT:
                v = clean_text(cell(key))
                if v is not None:
                    snapshot[key] = v

            good.append(
                Txn(
                    sheet_row=sheet_row,
                    name=clean_text(cell("name")),
                    exchange=exchange,
                    symbol=symbol,
                    trade_type=parse_type(cell("type")),
                    quantity=qty,
                    price=price,
                    price_derived=derived,
                    price_currency=currency,
                    invested_amount=invested,
                    trade_date=parse_datetime(cell("trade_date"), month_first),
                    market_cap_cr=lenient_decimal(cell("market_cap_cr")),
                    cap_category=clean_text(cell("cap_category")),
                    snapshot=snapshot,
                )
            )
        except ValueError as e:
            bad.append((sheet_row, str(e)))

    # Deterministic hash + occurrence counter so identical rows stay distinct.
    seen: Counter[str] = Counter()
    for t in good:
        base = "|".join(
            [
                f"{t.exchange or ''}:{t.symbol}",
                t.trade_date.isoformat() if t.trade_date else "",
                canon(t.quantity),
                canon(t.price),
                canon(t.invested_amount),
                t.trade_type,
            ]
        )
        seen[base] += 1
        t.source_hash = hashlib.sha256(f"{base}|{seen[base]}".encode()).hexdigest()
    return good, bad


# --------------------------------------------------------------------------
# Readers: live Google Sheet and local file
# --------------------------------------------------------------------------

def quote_tab(tab: str) -> str:
    return "'" + tab.replace("'", "''") + "'"


def fetch_range(
    credentials_path: str, sheet_id: str, tab: str, header_row: int, from_row: int, to_row: int
) -> tuple[list[Any], list[list[Any]], int]:
    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise SystemExit("Missing deps. Run: pip install google-api-python-client google-auth")

    creds = service_account.Credentials.from_service_account_file(credentials_path, scopes=[SCOPE])
    svc = build("sheets", "v4", credentials=creds, cache_discovery=False)
    values = svc.spreadsheets().values()
    q = quote_tab(tab)

    h = values.get(spreadsheetId=sheet_id, range=f"{q}!{header_row}:{header_row}").execute()
    header = (h.get("values") or [[]])[0]
    if not header:
        raise SystemExit(f"Header row {header_row} is empty in tab {tab!r}.")

    resp = values.get(
        spreadsheetId=sheet_id,
        range=f"{q}!{from_row}:{to_row}",
        valueRenderOption="FORMATTED_VALUE",
    ).execute()
    rows = resp.get("values", [])

    start = from_row
    m = re.search(r"!\$?[A-Za-z]*\$?(\d+)", resp.get("range", ""))
    if m:
        start = int(m.group(1))  # trust the row the API says it started at
    return header, rows, start


def read_local_range(
    path: str, tab: Optional[str], header_row: int, from_row: int, to_row: int
) -> tuple[list[Any], list[list[Any]], int]:
    ext = os.path.splitext(path)[1].lower()
    if ext in (".csv", ".tsv", ".txt"):
        delim = "\t" if ext == ".tsv" else ","
        with open(path, newline="", encoding="utf-8-sig") as f:
            all_rows: list[list[Any]] = list(csv.reader(f, delimiter=delim))
    elif ext in (".xlsx", ".xlsm"):
        try:
            import openpyxl
        except ImportError:
            raise SystemExit("Missing dependency. Run: pip install openpyxl")
        wb = openpyxl.load_workbook(path, data_only=True)
        if tab:
            if tab not in wb.sheetnames:
                raise SystemExit(f"Tab {tab!r} not found. Available tabs: {wb.sheetnames}")
            ws = wb[tab]
        else:
            ws = wb[wb.sheetnames[0]]
            print(f"No --tab given; using first tab {ws.title!r}.")
        all_rows = [["" if c is None else c for c in r] for r in ws.iter_rows(values_only=True)]
        wb.close()
    else:
        raise SystemExit(f"Unsupported file type {ext!r}. Use .csv, .tsv or .xlsx.")

    if header_row > len(all_rows) or not any(str(h).strip() for h in all_rows[header_row - 1]):
        raise SystemExit(f"Header row {header_row} is empty or missing in {path!r}.")
    rows = all_rows[from_row - 1 : to_row]
    if not rows:
        raise SystemExit(f"File has only {len(all_rows)} rows; --from-row {from_row} is past the end.")
    return all_rows[header_row - 1], rows, from_row


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------

def write_output(path: str, txns: list[Txn]) -> None:
    records = [asdict(t) for t in txns]
    if path.lower().endswith(".csv"):
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(records[0].keys()) if records else [])
            w.writeheader()
            for r in records:
                r["snapshot"] = json.dumps(r["snapshot"], default=str)
                w.writerow({k: ("" if v is None else v) for k, v in r.items()})
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, default=str)


def print_report(
    good: list[Txn], bad: list[tuple[int, str]], ignored: list[str], unknown: list[str],
    preview: int = 10,
) -> None:
    buys = sum(1 for t in good if t.trade_type == "BUY")
    symbols = {(t.exchange or "", t.symbol) for t in good}
    print(f"\nValid rows: {len(good)}  (BUY {buys}, SELL {len(good) - buys})  |  unique tickers: {len(symbols)}")
    print(f"Invalid rows: {len(bad)}")
    if ignored:
        print(f"Not imported by design: {', '.join(ignored)}")
    if unknown:
        print(f"Unrecognised columns (not imported): {', '.join(unknown)}")

    warnings = []
    derived = [t.sheet_row for t in good if t.price_derived]
    if derived:
        warnings.append(f"{len(derived)} row(s) had no purchase price; derived from invested amount (rows {derived[:15]})")
    no_exch = [t.sheet_row for t in good if not t.exchange]
    if no_exch:
        warnings.append(f"{len(no_exch)} row(s) have no exchange prefix in the symbol, currency assumed to be base (rows {no_exch[:15]})")
    no_inv = [t.sheet_row for t in good if t.invested_amount is None]
    if no_inv:
        warnings.append(f"{len(no_inv)} row(s) have no invested amount (rows {no_inv[:15]})")
    for w in warnings:
        print(f"Warning: {w}")

    if bad:
        print("\nInvalid rows:")
        for row, msg in bad:
            print(f"  row {row}: {msg}")
    if good:
        print(f"\nPreview (first {min(preview, len(good))}):")
        print(f"  {'row':>5}  {'ticker':<18} {'type':<5} {'qty':>9} {'price':>11} {'ccy':<4} {'invested':>12}")
        for t in good[:preview]:
            tk = f"{t.exchange}:{t.symbol}" if t.exchange else t.symbol
            flag = "*" if t.price_derived else " "
            print(
                f"  {t.sheet_row:>5}  {tk:<18} {t.trade_type:<5} {canon(t.quantity):>9} "
                f"{canon(t.price):>10}{flag} {t.price_currency:<4} {canon(t.invested_amount) or '-':>12}"
            )
        if derived:
            print("  (* = price derived from invested amount)")


# --------------------------------------------------------------------------
# Postgres
# --------------------------------------------------------------------------

def commit_to_db(db_url: str, txns: list[Txn], source: str) -> tuple[int, int]:
    try:
        import psycopg
    except ImportError:
        raise SystemExit('Missing dependency. Run: pip install "psycopg[binary]"')

    batch_id = uuid.uuid4()
    captured_at = datetime.now(timezone.utc).isoformat()
    inserted = 0
    with psycopg.connect(db_url) as conn:  # one transaction; rolls back on error
        with conn.cursor() as cur:
            cur.execute(DDL)
            for t in txns:
                # Existing values win, so a re-import never clobbers later edits.
                cur.execute(
                    """
                    INSERT INTO instruments (exchange, symbol, name, market_cap_cr, cap_category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (exchange, symbol) DO UPDATE SET
                      name          = COALESCE(instruments.name, EXCLUDED.name),
                      market_cap_cr = COALESCE(instruments.market_cap_cr, EXCLUDED.market_cap_cr),
                      cap_category  = COALESCE(instruments.cap_category, EXCLUDED.cap_category)
                    RETURNING id
                    """,
                    (t.exchange or "", t.symbol, t.name, t.market_cap_cr, t.cap_category),
                )
                instrument_id = cur.fetchone()[0]
                snapshot = (
                    json.dumps({**t.snapshot, "captured_at": captured_at}) if t.snapshot else None
                )
                cur.execute(
                    """
                    INSERT INTO transactions
                      (instrument_id, type, quantity, price, price_derived, price_currency,
                       invested_amount, trade_date, sheet_snapshot,
                       source, source_row_hash, import_batch_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s, %s)
                    ON CONFLICT (source_row_hash) DO NOTHING
                    """,
                    (
                        instrument_id, t.trade_type, t.quantity, t.price, t.price_derived,
                        t.price_currency, t.invested_amount, t.trade_date, snapshot,
                        source, t.source_hash, batch_id,
                    ),
                )
                inserted += cur.rowcount
    print(f"\nImport batch {batch_id}")
    return inserted, len(txns) - inserted


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Ingest stock transactions from a downloaded sheet or live Google Sheet.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument("--file", help="Local .csv/.tsv/.xlsx file downloaded from the sheet")
    p.add_argument("--sheet-id", default=os.getenv("SHEET_ID"),
                   help="Live Google Sheet ID (used only when --file is not given)")
    p.add_argument("--tab", default=None,
                   help="Tab name (XLSX: default first tab; live sheet: default Sheet1; ignored for CSV)")
    p.add_argument("--from-row", type=int, required=True, help="First data row (1-based)")
    p.add_argument("--to-row", type=int, required=True, help="Last data row (inclusive)")
    p.add_argument("--header-row", type=int, default=1, help="Row containing column headers (default: 1)")
    p.add_argument("--credentials", default=os.getenv("GOOGLE_APPLICATION_CREDENTIALS"),
                   help="Service account JSON key path (live sheet only)")
    p.add_argument("--map", action="append", default=[], metavar='FIELD="Column name"',
                   help=f"Override column detection; repeatable. Fields: {', '.join(ALIASES)}")
    p.add_argument("--base-currency", default="INR",
                   help="Currency for non-US tickers and for invested amounts (default: INR)")
    p.add_argument("--month-first", action="store_true",
                   help="Treat ambiguous dates as MM/DD/YYYY (default is DD/MM/YYYY)")
    p.add_argument("--out", help="Write cleaned rows to this .json or .csv file")
    p.add_argument("--db-url", default=os.getenv("DATABASE_URL"), help="Postgres connection URL")
    p.add_argument("--commit", action="store_true", help="Write to Postgres (default is dry run)")
    p.add_argument("--skip-invalid", action="store_true",
                   help="With --commit, import valid rows even if some rows are invalid")
    return p.parse_args()


def _fail(msg: str) -> int:
    print(f"Error: {msg}", file=sys.stderr)
    return 2


def main() -> int:
    a = parse_args()

    if a.file:
        if not os.path.isfile(a.file):
            return _fail(f"File not found: {a.file}")
    else:
        if not a.sheet_id:
            return _fail("Provide --file, or --sheet-id (or SHEET_ID) for the live sheet.")
        if not a.credentials:
            return _fail("--credentials (or GOOGLE_APPLICATION_CREDENTIALS) is required for the live sheet.")
    if a.from_row < 1 or a.to_row < a.from_row:
        return _fail("Row range must satisfy 1 <= from-row <= to-row.")
    if a.from_row <= a.header_row:
        return _fail(f"--from-row ({a.from_row}) must be below the header row ({a.header_row}).")
    if a.commit and not a.db_url:
        return _fail("--commit requires --db-url (or DATABASE_URL).")

    overrides: dict[str, str] = {}
    for item in a.map:
        if "=" not in item:
            return _fail(f"Bad --map value {item!r}; expected FIELD=\"Column name\".")
        fld, label = item.split("=", 1)
        fld = fld.strip()
        if fld not in ALIASES:
            return _fail(f"Unknown field {fld!r}. Choose from: {', '.join(ALIASES)}")
        overrides[fld] = label.strip()

    if a.file:
        print(f"Reading {a.file} rows {a.from_row}-{a.to_row} ...")
        header, rows, start = read_local_range(a.file, a.tab, a.header_row, a.from_row, a.to_row)
        source = f"file:{os.path.basename(a.file)}"
    else:
        tab = a.tab or "Sheet1"
        print(f"Reading {tab!r} rows {a.from_row}-{a.to_row} ...")
        header, rows, start = fetch_range(a.credentials, a.sheet_id, tab, a.header_row, a.from_row, a.to_row)
        source = f"gsheet:{a.sheet_id}"

    mapping = build_column_map(header, overrides)
    print("Column mapping:", {f: str(header[i]).strip() for f, i in mapping.items()})

    good, bad = process_rows(rows, start, mapping, a.month_first, a.base_currency.upper())
    ignored, unknown = describe_other_columns(header, mapping)
    print_report(good, bad, ignored, unknown)

    if a.out:
        write_output(a.out, good)
        print(f"\nWrote {len(good)} cleaned row(s) to {a.out}")

    if not a.commit:
        print("\nDry run only. Re-run with --db-url ... --commit to write to Postgres.")
        return 1 if bad else 0

    if bad and not a.skip_invalid:
        return _fail(f"{len(bad)} invalid row(s). Fix them in the sheet, or pass --skip-invalid.")
    if not good:
        return _fail("Nothing valid to import.")

    inserted, skipped = commit_to_db(a.db_url, good, source)
    print(f"Inserted {inserted} new transaction(s); skipped {skipped} already imported.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
