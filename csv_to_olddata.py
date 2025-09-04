#!/usr/bin/env python3
"""
Load a CSV (item_code, available_quantity, total_quantity)
into the OldData table of reels.db, replacing anything there.
Usage:
    python csv_to_olddata.py myfile.csv
"""

import csv
import sqlite3
import sys
from pathlib import Path

DB_FILE   = "reels.db"            # same DB your main script uses
TABLE     = "OldData"             # the table to overwrite
SCHEMA_SQL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    item_code TEXT,
    available_quantity INTEGER,
    total_quantity INTEGER
)
"""

def main(csv_path: Path):
    if not csv_path.is_file():
        sys.exit(f"❌ CSV not found: {csv_path}")

    # 1. connect / create DB
    conn = sqlite3.connect(DB_FILE)
    cur  = conn.cursor()

    # 2. ensure table exists
    cur.execute(SCHEMA_SQL)

    # 3. wipe existing data
    cur.execute(f"DELETE FROM {TABLE}")

    # 4. read CSV and insert rows
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = [
            (
                row["item_code"],
                int(row["available_quantity"]),
                int(row["total_quantity"]),
            )
            for row in reader
            if row["item_code"]
        ]

    cur.executemany(
        f"INSERT INTO {TABLE} (item_code, available_quantity, total_quantity) "
        f"VALUES (?, ?, ?)",
        rows
    )
    conn.commit()
    conn.close()

    print(f"✅ Imported {len(rows)} rows into {TABLE}.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python csv_to_olddata.py yourfile.csv")
    main(Path(sys.argv[1]))
