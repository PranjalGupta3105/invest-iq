# Invest IQ

Invest IQ is a command-line stock transaction importer. It reads a selected row
range from a local CSV, TSV, or Excel workbook, or from a Google Sheet; validates
and previews the transactions; and can optionally write them to PostgreSQL.
Without `--commit`, the importer runs in dry-run mode and does not write to the
database.

## What it imports

The importer maps spreadsheet columns by their header names, regardless of
column order. It requires a ticker/symbol and quantity, plus either a purchase
price or an invested amount.

| Spreadsheet data | Destination |
| --- | --- |
| Symbol, purchase price, quantity, invested amount, transaction type, trade date | `transactions` |
| Stock name, market cap, cap category | `instruments` |
| Current price/amount, trend since purchase, P/L amount/percentage | Transaction `sheet_snapshot` JSON |

Historical comparison columns (such as “Price 30 Days Ago” and “Trend Since
6M”) are deliberately not imported. Unrecognised columns are reported but
ignored.

If transaction type is blank, it defaults to `BUY`. If purchase price is blank
but invested amount is present, price is derived as `invested amount / quantity`.
Prices on recognised US exchange tickers are treated as USD when entered in the
sheet; other prices use the configured base currency. Invested amounts are
assumed to use the base currency.

## Requirements

- Python 3.10 or newer
- `openpyxl` to read `.xlsx` and `.xlsm` workbooks
- `psycopg[binary]` to write to PostgreSQL
- For live Google Sheets: `google-api-python-client` and `google-auth`

Create and activate a virtual environment, then install the dependencies needed
for your workflow:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install openpyxl
```

For PostgreSQL imports, also install:

```bash
python -m pip install "psycopg[binary]"
```

For live Google Sheets, install:

```bash
python -m pip install google-api-python-client google-auth
```

## Local file import

Row numbers are 1-based and inclusive. `--header-row` identifies the header
row; `--from-row` and `--to-row` select the rows to process. The header row must
be before the first data row.

Start with a dry run:

```bash
python ingest_sheet.py \
  --file Stocks.xlsx \
  --tab Sheet1 \
  --header-row 2 \
  --from-row 6 \
  --to-row 105
```

The included `Stocks.xlsx` workbook has its column headers on row 2. Rows 3–5
are blank or category labels, so the example starts at row 6. Adjust the tab
and row range when using a different workbook.

To export the validated rows without writing to the database, add `--out`:

```bash
python ingest_sheet.py --file stocks.csv --from-row 2 --to-row 150 \
  --out cleaned-transactions.json
```

The output extension may be `.json` or `.csv`.

## Import to PostgreSQL

After checking the dry-run report, provide a PostgreSQL connection URL and add
`--commit`. The importer creates the `instruments` and `transactions` tables if
they do not already exist.

```bash
export DATABASE_URL='postgresql://USER:PASSWORD@HOST:5432/DATABASE'

python ingest_sheet.py \
  --file Stocks.xlsx \
  --tab Sheet1 \
  --header-row 2 \
  --from-row 6 \
  --to-row 105 \
  --commit
```

By default, the importer will not commit if any selected non-empty row is
invalid. `--skip-invalid` allows valid rows to be committed while invalid rows
are reported:

```bash
python ingest_sheet.py --file Stocks.xlsx --tab Sheet1 \
  --header-row 2 --from-row 6 --to-row 105 \
  --commit --skip-invalid
```

Keep database credentials out of source control and avoid putting real
credentials in shared command history or logs.

## Import from a live Google Sheet

Create a Google service account, share the spreadsheet with its email address
with Viewer access, and provide the service-account JSON file. The spreadsheet
ID can be passed as `--sheet-id` or through the `SHEET_ID` environment
variable. Credentials can be passed as `--credentials` or through
`GOOGLE_APPLICATION_CREDENTIALS`.

```bash
export SHEET_ID='YOUR_SPREADSHEET_ID'
export GOOGLE_APPLICATION_CREDENTIALS='/path/to/service-account.json'

python ingest_sheet.py --tab 'Purchases' \
  --header-row 2 --from-row 3 --to-row 150
```

Add `--db-url "$DATABASE_URL" --commit` only after reviewing a dry run.

## Column matching

Common header names such as `Symbol`, `Ticker`, `Purchased Price`,
`Purchased Quantity`, `Invested Amount`, and `Transaction Type` are recognised
automatically. Header matching ignores case, repeated whitespace, and
parenthetical notes (for example, `Purchased Price (in USD - US stocks only)`
matches `Purchased Price`).

Use `--map` when a required column uses a different name. Repeat the option for
each column:

```bash
python ingest_sheet.py --file stocks.csv --from-row 2 --to-row 150 \
  --map ticker="GF Symbol" --map quantity="Qty"
```

Supported mapping fields include `ticker`, `quantity`, `price`,
`invested_amount`, `name`, `type`, and `trade_date`, along with instrument and
snapshot fields.

## Useful options

| Option | Description |
| --- | --- |
| `--file` | Local `.csv`, `.tsv`, `.txt`, `.xlsx`, or `.xlsm` file |
| `--tab` | Workbook tab name; for a live sheet, tab name (defaults to `Sheet1`) |
| `--header-row` | 1-based header row (defaults to `1`) |
| `--from-row`, `--to-row` | Inclusive 1-based row range to process |
| `--map FIELD="Column name"` | Override a field's header match; repeatable |
| `--base-currency` | Currency for non-US prices and invested amounts (defaults to `INR`) |
| `--month-first` | Interpret ambiguous dates as month-first instead of day-first |
| `--out` | Write valid rows to a JSON or CSV file |
| `--db-url` | PostgreSQL connection URL; defaults to `DATABASE_URL` |
| `--commit` | Write validated transactions to PostgreSQL; otherwise dry run |
| `--skip-invalid` | With `--commit`, proceed with valid rows despite invalid rows |

Run `python ingest_sheet.py --help` for the complete command-line help.

## Duplicate handling

Each transaction receives a deterministic hash based on its ticker, date (if
present), quantity, price, invested amount, type, and occurrence among matching
rows. PostgreSQL ignores a transaction whose hash was already imported, so
re-importing the same unchanged rows avoids duplicate transactions. If a
transaction's values are edited, it receives a different hash and may be
inserted as a new transaction.

## Example query

[`query.sql`](./query.sql) contains a sample PostgreSQL query that summarizes
BUY quantities, average prices, and invested amounts by symbol for
non-USD-priced transactions.

## License

This project is available under the [MIT License](./LICENSE).
