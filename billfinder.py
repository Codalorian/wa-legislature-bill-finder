#!/usr/bin/env python3
import re
import csv
import time
import html
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

SESSION_FOLDERS = {
    # biennium label -> House-bill text directory
    "2023-24": "https://app.leg.wa.gov/documents/billdocs/2023-24/Htm/Bills/House%20Bills/",
    "2025-26": "https://app.leg.wa.gov/documents/billdocs/2025-26/Htm/Bills/House%20Bills/",
}

DEFAULT_YEARS = {2023, 2024, 2025}
KEYWORDS = [r"\bclimate\b"]  # adjust as needed

HEADERS = {"User-Agent": "WA-Leg-Scraper/1.0 (+github.com/you)"}  # be polite

def get_soup(url, timeout=30):
    r = requests.get(url, headers=HEADERS, timeout=timeout)
    r.raise_for_status()
    return BeautifulSoup(r.text, "html.parser")

def list_bill_files(biennium, base_url):
    # Directory listing contains anchors to e.g. 1181.htm, 1181-S.htm, etc.
    soup = get_soup(base_url)
    files = []
    for a in soup.select('a[href$=".htm"]'):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(base_url, href)  # safe join for relative URLs
        files.append(url)
    return files

YEAR_RE = re.compile(r"\b(20\d{2})\s+(?:Regular|1st|2nd|3rd)\s+Session", re.I)
BILLNUM_RE = re.compile(r"\bHOUSE\s+BILL\s+(\d{3,4})\b", re.I)
AN_ACT_RE = re.compile(r"\bAN\s+ACT\s+Relating\s+to[^\n\r]*", re.I)

def parse_bill_page(url):
    r = requests.get(url, headers=HEADERS, timeout=45)
    r.raise_for_status()
    text = r.text

    # Extract bill number and year
    year = None
    m = YEAR_RE.search(text)
    if m:
        year = int(m.group(1))

    bill_number = None
    n = BILLNUM_RE.search(text)
    if n:
        bill_number = n.group(1)
    else:
        # fallback: from filename
        mfn = re.search(r"/(\d{3,4})(?:[-\w]*)\.htm$", url)
        if mfn:
            bill_number = mfn.group(1)

    # Find a reasonable title line
    title = None
    t = AN_ACT_RE.search(text)
    if t:
        title = html.unescape(t.group(0)).strip()

    # keyword match (case-insensitive)
    lower = text.lower()
    matched = any(re.search(k, lower, re.I) for k in KEYWORDS)

    return {
        "year": year,
        "bill_number": bill_number,
        "title": title,
        "matched": matched,
        "bill_text_url": url,
    }

def bill_summary_url(bill_number, year):
    if not bill_number or not year:
        return None
    return f"https://app.leg.wa.gov/BillSummary/?BillNumber={bill_number}&Year={year}&Initiative=false"

def main(out_csv, years, max_workers, delay):
    rows = []
    seen = set()  # (biennium, year, bill_number)

    for biennium, base in SESSION_FOLDERS.items():
        print(f"Scanning {biennium} …")
        try:
            files = list_bill_files(biennium, base)
        except Exception as e:
            print(f"Failed to list {biennium}: {e}")
            continue

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(parse_bill_page, url): url for url in files}
            for fut in as_completed(futures):
                url = futures[fut]
                try:
                    info = fut.result()
                except Exception as e:
                    print(f"Error fetching {url}: {e}")
                    continue

                y = info["year"]
                b = info["bill_number"]
                if not (y and b):
                    continue
                if y not in years:
                    continue
                if not info["matched"]:
                    continue

                key = (biennium, y, b)
                if key in seen:
                    continue
                seen.add(key)

                title = info["title"] or ""
                summary = bill_summary_url(b, y)
                rows.append({
                    "biennium": biennium,
                    "year": y,
                    "bill_number": f"HB {b}",
                    "title": title,
                    "bill_text_url": info["bill_text_url"],
                    "bill_summary_url": summary or "",
                })
                if delay > 0:
                    time.sleep(delay)

    # sort by year then bill number
    rows.sort(key=lambda r: (r["year"], int(re.sub(r"\D", "", r["bill_number"]))))

    # write CSV
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["biennium", "year", "bill_number", "title", "bill_text_url", "bill_summary_url"])
        w.writeheader()
        w.writerows(rows)

    print(f"Wrote {len(rows)} results to {out_csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Search WA House bills for climate-related text (years 2023–2025).")
    ap.add_argument("--out", default="wa_house_climate_bills_2023_2025.csv", help="Output CSV filename")
    ap.add_argument("--years", default="2023,2024,2025", help="Comma-separated years to include")
    ap.add_argument("--workers", type=int, default=8, help="Max concurrent fetches")
    ap.add_argument("--delay", type=float, default=0.0, help="Optional delay (seconds) between successful matches")
    args = ap.parse_args()

    years = {int(y.strip()) for y in args.years.split(",") if y.strip()}
    main(args.out, years, args.workers, args.delay)
