#!/usr/bin/env python3
"""
Fetch every reel, consolidate by ItemCode,
export to CSV and push into SQLite with rolling
OldData/NewData tables and timestamps.
"""

import csv
import math
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime

import requests
from tqdm import tqdm

# ─── CONFIG ──────────────────────────────────────────────
BASE_URL = "http://localhost:8081/"
USERNAME  = "CHRISTIANS"
PASSWORD  = "ALLYGREEN2019!"
PAGECOUNT = 1000                       # max rows per API page
CSV_FILE  = "reel_quantities.csv"
DB_FILE   = "reels.db"
TIMEOUT   = 60                         # HTTP timeout (s)
# ─────────────────────────────────────────────────────────


# ─── API HELPERS ─────────────────────────────────────────
def login_get_token() -> str:
    url = f"{BASE_URL}?f=login&username={USERNAME}&password={PASSWORD}"
    xml = requests.get(url, timeout=TIMEOUT).text
    token = ET.fromstring(xml).findtext(".//token")
    if not token:
        sys.exit("❌  Login failed — token not found.")
    return token


def get_totfound(token: str) -> int:
    url = f"{BASE_URL}?f=V2_reel_getlist&pagestart=0&pagecount=1&tkn={token}"
    xml = requests.get(url, timeout=TIMEOUT).text
    return int(ET.fromstring(xml).attrib.get("totfound", 0))


def fetch_page_xml(token: str, start: int) -> str:
    url = (
        f"{BASE_URL}?f=V2_reel_getlist"
        f"&pagestart={start}&pagecount={PAGECOUNT}&tkn={token}"
    )
    return requests.get(url, timeout=TIMEOUT).text


def parse_reels(xml: str):
    """
    Yield (item_code, avail:int, total:int) for each reel.
    Tag names are case-normalised.
    """
    root = ET.fromstring(xml)
    reellist = root.find(".//reellist")
    if reellist is None:
        return

    for reel in reellist:
        tags = {c.tag.lower(): c.text for c in reel}

        item_code = tags.get("itemcode") or tags.get("code")
        qt_avail  = int(float(tags.get("qtavailable") or tags.get("availableqty") or 0))
        qt_total  = int(float(tags.get("qttotal")     or tags.get("quantity")     or 0))

        if item_code and qt_total:       # skip total == 0
            yield item_code, qt_avail, qt_total


# ─── SQLITE HELPER ──────────────────────────────────────
def push_to_sqlite(rows):
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()
    
    # Get the current time for this data pull
    current_timestamp = datetime.now().isoformat()

    # Ensure tables exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS NewData (
            item_code TEXT, available_quantity INTEGER, total_quantity INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS OldData (
            item_code TEXT, available_quantity INTEGER, total_quantity INTEGER
        )
    """)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Metadata (
            key TEXT PRIMARY KEY, value TEXT
        )
    """)

    # --- Rotate Timestamps ---
    # Get the timestamp of the current "NewData" before we overwrite it
    cur.execute("SELECT value FROM Metadata WHERE key = 'newDataTimestamp'")
    last_new_timestamp = cur.fetchone()
    
    # If a timestamp existed for NewData, it now becomes the OldData timestamp
    if last_new_timestamp:
        cur.execute(
            "INSERT OR REPLACE INTO Metadata (key, value) VALUES ('oldDataTimestamp', ?)",
            (last_new_timestamp[0],)
        )

    # --- Rotate Data Tables ---
    cur.execute("DELETE FROM OldData")
    cur.execute("INSERT INTO OldData SELECT * FROM NewData")
    cur.execute("DELETE FROM NewData")

    # Insert new consolidated data
    cur.executemany(
        "INSERT INTO NewData (item_code, available_quantity, total_quantity) VALUES (?, ?, ?)",
        rows
    )
    
    # Insert the new timestamp for the data we just inserted
    cur.execute(
        "INSERT OR REPLACE INTO Metadata (key, value) VALUES ('newDataTimestamp', ?)",
        (current_timestamp,)
    )

    conn.commit()
    conn.close()
    print(f"🧠  SQLite updated → {len(rows)} rows in NewData.")


# ─── MAIN WORKFLOW ─────────────────────────────────────
def main():
    token    = login_get_token()
    totfound = get_totfound(token)
    pages    = math.ceil(totfound / PAGECOUNT)

    print(f"🔢  Reels reported by server: {totfound}")

    # Download and consolidate
    consolidated = defaultdict(lambda: [0, 0])  # item_code → [avail_sum, total_sum]
    progress = tqdm(total=pages, desc="Downloading reels",
                    unit="page", dynamic_ncols=True)

    for p in range(pages):
        start = p * PAGECOUNT
        t0 = time.time()
        try:
            xml = fetch_page_xml(token, start)
            for item_code, avail, total in parse_reels(xml):
                consolidated[item_code][0] += avail
                consolidated[item_code][1] += total
        except Exception as exc:
            print(f"\n❌  Error on page {p} (start={start}): {exc}")
        finally:
            progress.set_postfix(last_page_sec=f"{time.time() - t0:.1f}")
            progress.update(1)

    progress.close()

    # Flatten dict → list
    rows = [(ic, avail, total) for ic, (avail, total) in consolidated.items()]

    # CSV export
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["item_code", "available_quantity", "total_quantity"])
        writer.writerows(rows)
    print(f"📄  CSV written → {CSV_FILE} ({len(rows)} rows)")

    # SQLite push
    push_to_sqlite(rows)

    print("✅  All done.")


if __name__ == "__main__":
    main()
