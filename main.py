#!/usr/bin/env python3
"""
Fetch every reel, consolidate by ItemCode,
export to CSV and push into SQLite with rolling
OldData/NewData tables and timestamps.

Then, commit and push the updated reels.db to GitHub.
"""

import csv
import math
import os
import sqlite3
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from datetime import datetime
from dotenv import load_dotenv

import requests
from tqdm import tqdm

load_dotenv()

# ─── CONFIG ──────────────────────────────────────────────
BASE_URL = "http://localhost:8081/"
USERNAME  = "CHRISTIANS"
PASSWORD  = "ALLYGREEN2019!"
PAGECOUNT = 1000
CSV_FILE  = "reel_quantities.csv"
DB_FILE   = "reels.db"
TIMEOUT   = 60

# ─── GIT CONFIG ──────────────────────────────────────────
# IMPORTANT: Set these as environment variables for security
GITHUB_USERNAME = os.getenv("GITHUB_USERNAME")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN") # Your Personal Access Token
GITHUB_REPO = os.getenv("GITHUB_REPO") # e.g., "my-username/datahub-prod"

# The script assumes it's running from within the git repository folder.
PROJECT_PATH = os.path.dirname(os.path.realpath(__file__))

# ─── GIT HELPERS ─────────────────────────────────────────
def run_command(command, working_dir):
    """Runs a shell command in a specified directory."""
    print(f"Running command: {' '.join(command)}")
    try:
        result = subprocess.run(
            command,
            cwd=working_dir,
            check=True,
            capture_output=True,
            text=True
        )
        print(f"Command successful.")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running command: {e.stderr}")
        return False

def git_sync_and_push():
    """Pulls latest changes, adds, commits, and pushes the reels.db file."""
    if not all([GITHUB_USERNAME, GITHUB_TOKEN, GITHUB_REPO]):
        print("❌ Git credentials not found in environment variables. Skipping push.")
        return

    commit_message = f"Data update (reels.db): {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    remote_url = f"https://{GITHUB_USERNAME}:{GITHUB_TOKEN}@github.com/{GITHUB_REPO}.git"
    branch_name = "master" # Or "master" if that's your branch name

    print("\n syncing with git...")

    # Always pull first
    if not run_command(["git", "pull", remote_url, branch_name], PROJECT_PATH):
        print("Failed to pull latest changes. Please check for conflicts.")
        return

    # Stage the database file
    if not run_command(["git", "add", DB_FILE], PROJECT_PATH): return

    # Commit with --allow-empty
    if not run_command(["git", "commit", "-m", commit_message, "--allow-empty"], PROJECT_PATH):
        print("Commit failed, continuing to push.")

    # Push to remote
    if not run_command(["git", "push", f"https://github.com/{GITHUB_REPO}.git"], PROJECT_PATH):
        print("Failed to push to remote repository.")
        return
    
    print("✅ Successfully pushed data update to GitHub.")

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
    root = ET.fromstring(xml)
    reellist = root.find(".//reellist")
    if reellist is None:
        return

    for reel in reellist:
        tags = {c.tag.lower(): c.text for c in reel}
        item_code = tags.get("itemcode") or tags.get("code")
        qt_avail  = int(float(tags.get("qtavailable") or tags.get("availableqty") or 0))
        qt_total  = int(float(tags.get("qttotal")     or tags.get("quantity")     or 0))
        if item_code and qt_total:
            yield item_code, qt_avail, qt_total

# ─── SQLITE HELPER ──────────────────────────────────────
def push_to_sqlite(rows):
    conn = sqlite3.connect(os.path.join(PROJECT_PATH, DB_FILE))
    cur  = conn.cursor()
    current_timestamp = datetime.now().isoformat()

    cur.execute("CREATE TABLE IF NOT EXISTS NewData (item_code TEXT, available_quantity INTEGER, total_quantity INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS OldData (item_code TEXT, available_quantity INTEGER, total_quantity INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS Metadata (key TEXT PRIMARY KEY, value TEXT)")

    cur.execute("SELECT value FROM Metadata WHERE key = 'newDataTimestamp'")
    last_new_timestamp = cur.fetchone()
    
    if last_new_timestamp:
        cur.execute("INSERT OR REPLACE INTO Metadata (key, value) VALUES ('oldDataTimestamp', ?)", (last_new_timestamp[0],))

    cur.execute("DELETE FROM OldData")
    cur.execute("INSERT INTO OldData SELECT * FROM NewData")
    cur.execute("DELETE FROM NewData")

    cur.executemany("INSERT INTO NewData (item_code, available_quantity, total_quantity) VALUES (?, ?, ?)", rows)
    cur.execute("INSERT OR REPLACE INTO Metadata (key, value) VALUES ('newDataTimestamp', ?)", (current_timestamp,))

    conn.commit()
    conn.close()
    print(f"🧠  SQLite updated → {len(rows)} rows in NewData.")

# ─── MAIN WORKFLOW ─────────────────────────────────────
def main():
    token    = login_get_token()
    totfound = get_totfound(token)
    pages    = math.ceil(totfound / PAGECOUNT)

    print(f"🔢  Reels reported by server: {totfound}")

    consolidated = defaultdict(lambda: [0, 0])
    progress = tqdm(total=pages, desc="Downloading reels", unit="page", dynamic_ncols=True)

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
    rows = [(ic, avail, total) for ic, (avail, total) in consolidated.items()]

    csv_path = os.path.join(PROJECT_PATH, CSV_FILE)
    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["item_code", "available_quantity", "total_quantity"])
        writer.writerows(rows)
    print(f"📄  CSV written → {CSV_FILE} ({len(rows)} rows)")

    push_to_sqlite(rows)

    # Automatically push the changes to Git
    git_sync_and_push()

    print("✅  All done.")

if __name__ == "__main__":
    main()