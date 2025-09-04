from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional

import pyodbc
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator
from zoneinfo import ZoneInfo
import tzdata

# -----------------------------------------------------------------------------
# Environment & Logging
# -----------------------------------------------------------------------------
load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("analytics-api")

# -----------------------------------------------------------------------------
# FastAPI app
# -----------------------------------------------------------------------------
app = FastAPI(title="Analytics API", version="0.0.3")

# -----------------------------------------------------------------------------
# Settings
# -----------------------------------------------------------------------------
SQL_SERVER_ADDRESS = os.getenv("SQL_SERVER_ADDRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE")
SQL_USER = os.getenv("SQL_USER")
SQL_PASSWORD = os.getenv("SQL_PASSWORD")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 17 for SQL Server")
SQL_ENCRYPT = os.getenv("SQL_ENCRYPT", "yes")
SQL_TRUST = os.getenv("SQL_TRUST", "yes")
SQL_TIMEOUT = int(os.getenv("SQL_TIMEOUT", "5"))

CONN_STR = (
    f"DRIVER={{{SQL_DRIVER}}};"
    f"SERVER={SQL_SERVER_ADDRESS};"
    f"DATABASE={SQL_DATABASE};"
    f"UID={SQL_USER};PWD={SQL_PASSWORD};"
    f"Encrypt={SQL_ENCRYPT};TrustServerCertificate={SQL_TRUST};"
    f"Connection Timeout={SQL_TIMEOUT};"
)

# -----------------------------------------------------------------------------
# Models
# -----------------------------------------------------------------------------
class IngestPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    stage_name: str = Field(min_length=1)
    barcodes: list[str] = Field(min_length=1)
    event_time: datetime

    @field_validator("barcodes")
    @classmethod
    def ensure_barcodes_nonempty(cls, v: list[str]) -> list[str]:
        cleaned = [b.strip() for b in v if b and b.strip()]
        if not cleaned:
            raise ValueError("at least one non-empty barcode is required")
        return cleaned

    @field_validator("event_time")
    @classmethod
    def localize_event_time(cls, dt: datetime) -> datetime:
        """Make tz-aware in America/Chicago if naive.
        Store/compare in UTC in DB if that's your standard.
        """
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=ZoneInfo("America/Chicago"))
        return dt

@dataclass(frozen=True)
class Stage:
    stage_name: str
    position: int

# -----------------------------------------------------------------------------
# DB helpers
# -----------------------------------------------------------------------------

def get_conn() -> pyodbc.Connection:
    # autocommit=True keeps logic simple for short transactions
    return pyodbc.connect(CONN_STR, autocommit=True)


def get_stage(conn: pyodbc.Connection, stage_name: str) -> Stage:
    sql = "SELECT stage_name, position FROM dbo.stages WHERE stage_name = ?"
    row = conn.cursor().execute(sql, stage_name).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail=f"Stage '{stage_name}' not found")
    return Stage(stage_name=row.stage_name, position=int(row.position))


def current_barcode_position(
    conn: pyodbc.Connection, barcode: str
) -> Optional[int]:
    """Return the current *position* (via join to stages) or None if not seen."""
    sql = (
        "SELECT s.position AS pos "
        "FROM dbo.barcodes b "
        "JOIN dbo.stages s ON s.stage_name = b.stage "
        "WHERE b.barcode = ?"
    )
    row = conn.cursor().execute(sql, barcode).fetchone()
    return int(row.pos) if row else None


def classify_barcode(
    existing_pos: Optional[int], target_pos: int
) -> Literal["new", "old", "same"]:
    if existing_pos is None:
        return "new"
    if existing_pos < target_pos:
        return "old"
    return "same"


def add_new_barcode(conn: pyodbc.Connection, barcode: str, stage: Stage) -> None:
    now = datetime.now(tz=ZoneInfo("America/Chicago")).isoformat()
    sql = (
        "INSERT INTO dbo.barcodes (barcode, stage, created_date, last_updated) "
        "VALUES (?, ?, ?, ?)"
    )
    conn.cursor().execute(sql, barcode, stage.stage_name, now, now)


def advance_barcode(conn: pyodbc.Connection, barcode: str, stage: Stage) -> None:
    now = datetime.now(tz=ZoneInfo("America/Chicago")).isoformat()
    sql = "UPDATE dbo.barcodes SET stage = ?, last_updated = ? WHERE barcode = ?"
    conn.cursor().execute(sql, stage.stage_name, now, barcode)


def add_event(
    conn: pyodbc.Connection, barcode: str, stage: Stage, event_time: datetime
) -> None:
    sql = "INSERT INTO dbo.events (barcode, stage, event_time) VALUES (?, ?, ?)"
    # Persist the *reported* event_time; FastAPI/Pydantic validator gives tz-aware
    conn.cursor().execute(sql, barcode, stage.stage_name, event_time.isoformat())


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.post("/ingest")
async def ingest(p: IngestPayload):
    try:
        with get_conn() as cx:
            stage = get_stage(cx, p.stage_name)

            processed = {"new": 0, "old": 0, "same": 0}
            for b in p.barcodes:
                pos = current_barcode_position(cx, b)
                cls = classify_barcode(pos, stage.position)

                if cls == "new":
                    add_new_barcode(cx, b, stage)
                elif cls == "old":
                    advance_barcode(cx, b, stage)
                else:
                    log.info("barcode %s already at or past stage '%s'", b, stage.stage_name)

                add_event(cx, b, stage, p.event_time)
                processed[cls] += 1

            return {
                "stage": stage.stage_name,
                "position": stage.position,
                "counts": processed,
                "message": "Finished processing barcodes.",
            }
    except HTTPException:
        # pass through HTTPExceptions like stage not found
        raise
    except pyodbc.Error as e:
        log.exception("Database error")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        log.exception("Unhandled error")
        raise HTTPException(status_code=500, detail=str(e))
    
    
@app.get("/analytics")
def get_analytics_data():
    """
    Endpoint to provide aggregated event data for the push script.
    This query groups all events by day and stage, counting only the
    unique barcodes at each stage per day to prevent duplicates.
    """
    sql = """
        SELECT
            CAST(event_time AS DATE) as event_date,
            stage,
            COUNT(DISTINCT barcode) as event_count
        FROM dbo.events
        GROUP BY CAST(event_time AS DATE), stage
        ORDER BY event_date, stage;
    """
    try:
        with get_conn() as cx:
            rows = cx.cursor().execute(sql).fetchall()
        
        # Format the data for easy JSON consumption
        results = [
            {
                "event_date": row.event_date.isoformat(),
                "stage": row.stage,
                "count": row.event_count,
            }
            for row in rows
        ]
        return results
    except pyodbc.Error as e:
        log.exception("Database error in /analytics")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")


@app.get("/stages")
def get_stages():
    # Simple implementation to make the route useful
    try:
        with get_conn() as cx:
            rows = cx.cursor().execute(
                "SELECT stage_name, position FROM dbo.stages ORDER BY position"
            ).fetchall()
        return [{"stage_name": r.stage_name, "position": int(r.position)} for r in rows]
    except pyodbc.Error as e:
        log.exception("Database error in /stages")
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
@app.get("/health")
def health():
    return {"ok": True}
