#!/usr/bin/env python3
"""
Download product specification files listed in the most recent exported_specs*.xlsx.

Usage:
    python download_specs.py
    python download_specs.py --subfolder countryName
    python download_specs.py --subfolder providerName --delay 3 --output specs
"""

import argparse
import glob
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import openpyxl
import requests

# ── Constants ────────────────────────────────────────────────────────────────
SPEC_URL_COLUMN = "specification"
HEADER_ROW = 2          # Row 1 is blank; headers are on row 2
DATA_START_ROW = 3
DEFAULT_OUTPUT = "specs"
DEFAULT_DELAY = 2.0     # seconds between downloads
REQUEST_TIMEOUT = 30    # seconds per request


# ── Helpers ──────────────────────────────────────────────────────────────────

def find_latest_excel() -> Path:
    matches = sorted(glob.glob("exported_specs*.xlsx"))
    if not matches:
        sys.exit("ERROR: No exported_specs*.xlsx file found in the current directory.")
    return Path(matches[-1])


def read_rows(xlsx_path: Path, subfolder_col: str | None) -> list[dict]:
    """
    Return a list of dicts with keys: url, subfolder, filename.
    Deduplicates by URL (keeps first occurrence).
    """
    print(f"Reading {xlsx_path} …")
    wb = openpyxl.load_workbook(xlsx_path, read_only=True, data_only=True)
    ws = wb.active

    # Parse headers from the designated row
    headers = []
    for row in ws.iter_rows(min_row=HEADER_ROW, max_row=HEADER_ROW):
        headers = [str(c.value).strip() if c.value else "" for c in row]
    if SPEC_URL_COLUMN not in headers:
        wb.close()
        sys.exit(f"ERROR: Column '{SPEC_URL_COLUMN}' not found. Available: {headers}")
    if subfolder_col and subfolder_col not in headers:
        wb.close()
        sys.exit(f"ERROR: Column '{subfolder_col}' not found. Available: {headers}")

    url_idx = headers.index(SPEC_URL_COLUMN)
    sub_idx = headers.index(subfolder_col) if subfolder_col else None

    seen_urls: set[str] = set()
    rows: list[dict] = []

    for row in ws.iter_rows(min_row=DATA_START_ROW, values_only=True):
        url = row[url_idx] if len(row) > url_idx else None
        if not url or not str(url).startswith("http"):
            continue
        url = str(url).strip()
        if url in seen_urls:
            continue
        seen_urls.add(url)

        subfolder = ""
        if sub_idx is not None:
            val = row[sub_idx] if len(row) > sub_idx else None
            if val:
                # Sanitise value for use as a directory name
                subfolder = "".join(c if c.isalnum() or c in " _-" else "_" for c in str(val).strip())

        filename = Path(urlparse(url).path).name or f"spec_{len(rows)}.xlsx"
        rows.append({"url": url, "subfolder": subfolder, "filename": filename})

    wb.close()
    return rows


def download(url: str, dest: Path, session: requests.Session) -> bool:
    """Download url to dest. Returns True on success."""
    try:
        resp = session.get(url, timeout=REQUEST_TIMEOUT, stream=True)
        resp.raise_for_status()
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in resp.iter_content(chunk_size=65536):
                f.write(chunk)
        return True
    except requests.RequestException as exc:
        print(f"  FAIL  {exc}", file=sys.stderr)
        return False


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk-download product spec files from an exported_specs*.xlsx."
    )
    parser.add_argument(
        "--subfolder",
        metavar="COLUMN",
        default=None,
        help=(
            "Column whose value is used as a sub-directory inside --output. "
            "E.g. --subfolder countryName  ->  specs/Chile/Spec_xxx.xlsx"
        ),
    )
    parser.add_argument(
        "--output",
        metavar="DIR",
        default=DEFAULT_OUTPUT,
        help=f"Root output directory (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--delay",
        metavar="SECONDS",
        type=float,
        default=DEFAULT_DELAY,
        help=f"Sleep between downloads in seconds (default: {DEFAULT_DELAY})",
    )
    parser.add_argument(
        "--no-skip",
        action="store_true",
        help="Re-download files that already exist locally (default: skip them)",
    )
    parser.add_argument(
        "--test",
        metavar="N",
        type=int,
        default=None,
        help="Download only the first N specs (useful for testing)",
    )
    args = parser.parse_args()

    xlsx = find_latest_excel()
    rows = read_rows(xlsx, args.subfolder)

    if not rows:
        sys.exit("No downloadable rows found.")

    if args.test is not None:
        rows = rows[: args.test]

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    total = len(rows)
    skipped = downloaded = failed = 0

    print(f"\nFound {total} unique specs to download -> {output_dir}/")
    if args.subfolder:
        print(f"Subfolder column: '{args.subfolder}'")
    print(f"Delay between downloads: {args.delay}s\n")

    session = requests.Session()
    session.headers.update({"User-Agent": "spec-downloader/1.0"})

    for i, row in enumerate(rows, start=1):
        dest = output_dir / row["subfolder"] / row["filename"] if row["subfolder"] \
               else output_dir / row["filename"]

        prefix = f"[{i:>{len(str(total))}}/{total}]"

        if not args.no_skip and dest.exists():
            print(f"{prefix} SKIP  {dest}")
            skipped += 1
            continue

        print(f"{prefix} GET   {row['url']}")
        print(f"{'':>{len(prefix)+1}}->  {dest}")

        ok = download(row["url"], dest, session)
        if ok:
            downloaded += 1
        else:
            failed += 1

        if i < total:
            time.sleep(args.delay)

    print(f"\nDone. Downloaded: {downloaded}  Skipped: {skipped}  Failed: {failed}")


if __name__ == "__main__":
    main()
