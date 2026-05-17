"""
database.py — Persistence layer.

Uses SQLite via the standard library so the package has zero mandatory
external dependencies.  To upgrade to Postgres/MySQL, swap the
connection logic inside _get_connection(); the rest of the module is
DB-agnostic SQL.

Configuration
-------------
Set the DB_PATH environment variable to override the default SQLite file
location (default: "leads.db" in the current working directory).

    export DB_PATH=/var/data/leads.db

Schema
------
leads
  run_id            TEXT  PRIMARY KEY
  ticket_id         TEXT
  session_id        TEXT
  name              TEXT
  phone             TEXT
  email             TEXT
  reason            TEXT
  event             TEXT
  csat              REAL
  language          TEXT
  frustration       TEXT
  lead_type         TEXT
  callback_tier     TEXT
  callback_intent   TEXT
  dispatch_queue    TEXT
  status            TEXT
  previous_ticket   TEXT
  processed_at      TEXT
  notified_at       TEXT
"""
from __future__ import annotations

import logging
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Generator, List, Optional

# TYPE_CHECKING guard breaks the circular import:
#   lead_pipeline → database → lead_pipeline
# At runtime this block is skipped; type checkers and IDEs still see the types.
if TYPE_CHECKING:
    from lead_pipeline import ProcessedLead

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DB path — read from env var so callers can override without touching code.
# Strips the "sqlite:///" scheme prefix that SQLAlchemy-style URLs use.
# ---------------------------------------------------------------------------
_DB_PATH = os.getenv("DB_PATH", "leads.db").replace("sqlite:///", "")

# Status constant — mirrors LeadStatus.NOTIFIED in lead_pipeline.py.
# Defined here as a plain string to avoid the circular import at runtime.
_STATUS_NOTIFIED = "Notified"


# ---------------------------------------------------------------------------
# Connection & schema bootstrap
# ---------------------------------------------------------------------------

@contextmanager
def _get_connection() -> Generator[sqlite3.Connection, None, None]:
    """Yield a thread-safe SQLite connection with WAL mode enabled."""
    conn = sqlite3.connect(_DB_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def initialise_db() -> None:
    """Create tables and indexes if they do not exist. Safe to call on every startup."""
    ddl = """
    CREATE TABLE IF NOT EXISTS leads (
        run_id          TEXT PRIMARY KEY,
        ticket_id       TEXT,
        session_id      TEXT,
        name            TEXT,
        phone           TEXT,
        email           TEXT,
        reason          TEXT,
        event           TEXT,
        csat            REAL,
        language        TEXT,
        frustration     TEXT,
        lead_type       TEXT,
        callback_tier   TEXT,
        callback_intent TEXT,
        dispatch_queue  TEXT,
        status          TEXT NOT NULL DEFAULT 'New',
        previous_ticket TEXT,
        processed_at    TEXT NOT NULL,
        notified_at     TEXT
    );
    CREATE INDEX IF NOT EXISTS idx_leads_email  ON leads(email);
    CREATE INDEX IF NOT EXISTS idx_leads_phone  ON leads(phone);
    CREATE INDEX IF NOT EXISTS idx_leads_status ON leads(status);
    CREATE INDEX IF NOT EXISTS idx_leads_queue  ON leads(dispatch_queue);
    """
    with _get_connection() as conn:
        conn.executescript(ddl)
    logger.info("Database initialised at %s", _DB_PATH)


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------

def upsert_lead(lead: "ProcessedLead") -> None:
    """
    Insert a new lead record, or replace it if the run_id already exists.
    This is idempotent — safe to call more than once for the same run.
    """
    r = lead.raw
    sql = """
    INSERT OR REPLACE INTO leads (
        run_id, ticket_id, session_id, name, phone, email,
        reason, event, csat, language, frustration,
        lead_type, callback_tier, callback_intent, dispatch_queue,
        status, previous_ticket, processed_at
    ) VALUES (
        :run_id, :ticket_id, :session_id, :name, :phone, :email,
        :reason, :event, :csat, :language, :frustration,
        :lead_type, :callback_tier, :callback_intent, :dispatch_queue,
        :status, :previous_ticket, :processed_at
    )
    """
    params = {
        "run_id":          lead.run_id,
        "ticket_id":       r.TICKET_ID,
        "session_id":      r.SESSION_ID,
        "name":            r.NAME,
        "phone":           r.PHONE,
        "email":           r.EMAIL,
        "reason":          r.REASON,
        "event":           r.EVENT,
        "csat":            r.CSAT,
        "language":        r.LANGUAGE,
        "frustration":     r.FRUSTRATION,
        "lead_type":       lead.lead_type,
        "callback_tier":   lead.callback_tier,
        "callback_intent": lead.callback_intent,
        "dispatch_queue":  lead.dispatch_queue,
        "status":          lead.status,
        "previous_ticket": lead.previous_ticket_id,
        "processed_at":    lead.processed_at.isoformat(),
    }
    with _get_connection() as conn:
        conn.execute(sql, params)
    logger.debug("Upserted lead run_id=%s ticket=%s", lead.run_id, r.TICKET_ID)


def mark_notified(run_ids: List[str]) -> None:
    """Bulk-update status → Notified and stamp notified_at."""
    if not run_ids:
        return
    now = datetime.now(timezone.utc).isoformat()
    placeholders = ",".join("?" * len(run_ids))
    sql = f"""
    UPDATE leads
       SET status      = '{_STATUS_NOTIFIED}',
           notified_at = ?
     WHERE run_id IN ({placeholders})
    """
    with _get_connection() as conn:
        conn.execute(sql, [now, *run_ids])
    logger.info("Marked %d lead(s) as Notified.", len(run_ids))


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------

def find_duplicate(
    phone: Optional[str],
    email: Optional[str],
    current_run_id: str,
) -> Optional[sqlite3.Row]:
    """
    Return the MOST RECENT existing lead that shares the same phone or email.
    Excludes the current run to avoid self-matching.

    Returns None if no duplicate exists.
    """
    if not phone and not email:
        return None

    conditions, params = [], []
    if phone:
        conditions.append("phone = ?")
        params.append(phone)
    if email:
        conditions.append("email = ?")
        params.append(email)

    where_clause = " OR ".join(conditions)
    params.append(current_run_id)

    sql = f"""
    SELECT run_id, ticket_id, status
      FROM leads
     WHERE ({where_clause})
       AND run_id != ?
     ORDER BY processed_at DESC
     LIMIT 1
    """
    with _get_connection() as conn:
        row = conn.execute(sql, params).fetchone()
    return row


def fetch_pending_by_queue(queue: str) -> List[sqlite3.Row]:
    """
    Fetch all leads in a given dispatch queue that have NOT yet been notified
    (status is New, Duplicate, or Recurring).
    """
    sql = """
    SELECT *
      FROM leads
     WHERE dispatch_queue = ?
       AND status != ?
     ORDER BY processed_at ASC
    """
    with _get_connection() as conn:
        rows = conn.execute(sql, [queue, _STATUS_NOTIFIED]).fetchall()
    return rows
