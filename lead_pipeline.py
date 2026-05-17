"""
lead_pipeline.py — Chatbot webhook receiver + full lead automation pipeline.

This is a single self-contained script.  Run it directly and it starts an
HTTP server that accepts POST requests from your deployed chatbot.  Every
incoming lead flows through an 8-step pipeline automatically — no manual
intervention required after startup.

Quick start
-----------
    # 1. Set environment variables (see SECTION 2 for the full list)
    export SMTP_HOST=smtp.gmail.com
    export SMTP_USER=you@gmail.com
    export SMTP_PASSWORD=your_app_password
    export SENDER_EMAIL=you@gmail.com
    export TEAM_EMAIL=team@yourcompany.com
    export WEBHOOK_SECRET=some_secret   # must match your chatbot's signing key
    export DB_PATH=leads.db             # SQLite file location (default: leads.db)

    # 2. Install dependencies
    pip install flask apscheduler

    # 3. Run
    python lead_pipeline.py                  # default: 0.0.0.0:8000
    python lead_pipeline.py --port 5000      # custom port
    python lead_pipeline.py --debug          # Flask debug mode

Webhook endpoint
----------------
    POST /webhook/lead
    Content-Type: application/json
    X-Hub-Signature-256: sha256=<hmac>   # optional; required if WEBHOOK_SECRET is set

    Accepts both the legacy flat format and the new chatbot nested format:

    {
        "event":      "callback_requested",
        "session_id": "35af1d19...",
        "callback_data": {
            "name":      "Prateek",
            "phone":     "7905317710",
            "email":     "rk0708090@gmail.com",
            "reason":    "Wholesale",
            "ticket_id": "TKT-5C8B1B95"
        },
        "frustration_score": 0,
        "language": "en"
    }

Other endpoints
---------------
    GET  /health          — liveness check; returns scheduler status
    GET  /leads/pending   — count of un-notified leads per queue (admin view)

Pipeline steps (fully automated)
---------------------------------
    1. Parse      — flatten callback_data, uppercase keys, alias frustration_score
    2. Clean      — normalise phone/email/CSAT/FRUSTRATION; drop malformed values
    3. Validate   — HARD_STOP (no contact info) or WARN (missing fields)
    4. Classify   — LeadType → CallbackTier → CallbackIntent
    5. Deduplicate — match phone/email against DB → New / Duplicate / Recurring
    6. Persist    — upsert to SQLite (idempotent; safe to replay)
    7. Auto-email — customer confirmation sent if EMAIL present and lead is valid
    8. Dispatch   — Immediate leads sent inline; others drained by background jobs

Background scheduler (starts automatically on first request)
-------------------------------------------------------------
    Query queue      — flushed every  8 h
    Escalation queue — flushed every 16 h
    Failed queue     — flushed every 24 h
"""

from __future__ import annotations

import atexit
import hashlib
import hmac
import logging
import re
import signal
import smtplib
import uuid
import socket
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, jsonify, request

import database as db
orig_getaddrinfo = socket.getaddrinfo
def getaddrinfo_ipv4(host, port, family=0, type=0, proto=0, flags=0):
    return orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
socket.getaddrinfo = getaddrinfo_ipv4
logging.basicConfig(
        level   = logging.INFO,
        format  = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S",
    )
logger = logging.getLogger(__name__)
logging.getLogger(__name__).addHandler(logging.NullHandler())


# ============================================================================
# SECTION 1 — ENUMS & MODELS
# ============================================================================

class LeadType(str, Enum):
    ESCALATION = "Escalation"
    CALLBACK   = "Callback"


class CallbackTier(str, Enum):
    SUCCESSFUL = "Successful"
    FAILED     = "Failed"


class CallbackIntent(str, Enum):
    IMMEDIATE = "Immediate"
    QUERY     = "Query"


class LeadStatus(str, Enum):
    NEW       = "New"
    DUPLICATE = "Duplicate"
    RECURRING = "Recurring"
    NOTIFIED  = "Notified"


class DispatchQueue(str, Enum):
    IMMEDIATE  = "immediate"    # real-time, per-case
    QUERY      = "query"        # batched every  8 h
    ESCALATION = "escalation"   # batched every 16 h
    FAILED     = "failed"       # batched every 24 h


@dataclass
class RawLead:
    """Raw incoming payload — all fields optional; validation runs later."""
    NAME:        Optional[str]   = None
    PHONE:       Optional[str]   = None
    EMAIL:       Optional[str]   = None
    REASON:      Optional[str]   = None
    EVENT:       Optional[str]   = None
    TICKET_ID:   Optional[str]   = None
    SESSION_ID:  Optional[str]   = None
    CSAT:        Optional[float] = None
    LANGUAGE:    Optional[str]   = None
    FRUSTRATION: Optional[int]   = None   # numeric score e.g. 0–10
    TURN_COUNT:  Optional[int]   = None   # conversation turn depth

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, data: dict) -> "RawLead":
        """
        Accepts both the legacy flat format and the new nested format:

        New format differences handled here
        ------------------------------------
        - callback_data (nested object) → flattened into the top level
        - lowercase keys (name, phone …) → uppercased to match field names
        - frustration_score (numeric, different key) → aliased to FRUSTRATION

        Steps
        -----
        1. Flatten callback_data into the top-level dict.
        2. Uppercase all keys so 'name' == 'NAME', 'phone' == 'PHONE', etc.
        3. Apply field aliases for keys that differ in name.
        4. Discard any remaining unknown keys.
        """
        # Step 1 — flatten callback_data into the top-level dict
        flat = {k: v for k, v in data.items() if k != "callback_data"}
        flat.update(data.get("callback_data") or {})

        # Step 2 — uppercase all keys so 'name' == 'NAME', 'phone' == 'PHONE' etc.
        flat = {k.upper(): v for k, v in flat.items()}

        # Step 3 — apply field aliases for keys that differ in name
        aliases = {
            "FRUSTRATION_SCORE": "FRUSTRATION",
            "TURN_COUNT":        "TURN_COUNT",   # already correct after uppercase
        }
        for src_key, dest_key in aliases.items():
            if src_key in flat and dest_key not in flat:
                flat[dest_key] = flat.pop(src_key)

        # Step 4 — keep only known fields
        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in flat.items() if k in known})


@dataclass
class ProcessedLead:
    """Fully processed lead with classification, dedup status, and metadata."""
    raw:             RawLead
    lead_type:       Optional[LeadType]       = None
    callback_tier:   Optional[CallbackTier]   = None
    callback_intent: Optional[CallbackIntent] = None
    status:          LeadStatus               = LeadStatus.NEW
    previous_ticket_id: Optional[str]         = None
    validation_errors:  List[str]             = field(default_factory=list)
    processed_at:    datetime                 = field(default_factory=datetime.utcnow)
    run_id:          str                      = field(default_factory=lambda: str(uuid.uuid4()))

    @property
    def is_valid(self) -> bool:
        return not self.validation_errors

    @property
    def dispatch_queue(self) -> Optional[DispatchQueue]:
        """Single source-of-truth for which queue this lead enters."""
        if self.lead_type == LeadType.ESCALATION:
            return DispatchQueue.ESCALATION
        if self.callback_tier == CallbackTier.FAILED:
            return DispatchQueue.FAILED
        if self.callback_intent == CallbackIntent.IMMEDIATE:
            return DispatchQueue.IMMEDIATE
        if self.callback_intent == CallbackIntent.QUERY:
            return DispatchQueue.QUERY
        return None

    @property
    def should_send_auto_email(self) -> bool:
        return bool(self.raw.EMAIL and self.is_valid)

    def summary(self) -> dict:
        return {
            "run_id":            self.run_id,
            "ticket_id":         self.raw.TICKET_ID,
            "lead_type":         self.lead_type,
            "callback_tier":     self.callback_tier,
            "callback_intent":   self.callback_intent,
            "dispatch_queue":    self.dispatch_queue,
            "status":            self.status,
            "previous_ticket":   self.previous_ticket_id,
            "processed_at":      self.processed_at.isoformat(),
            "validation_errors": self.validation_errors,
        }


# ============================================================================
# SECTION 2 — CONFIGURATION
# ============================================================================

import os

@dataclass(frozen=True)
class _EmailConfig:
    smtp_host:     str  = os.getenv("SMTP_HOST",     "smtp.gmail.com")
    smtp_port:     int  = int(os.getenv("SMTP_PORT", "587"))
    smtp_user:     str  = os.getenv("SMTP_USER",     "")
    smtp_password: str  = os.getenv("SMTP_PASSWORD", "")
    sender_email:  str  = os.getenv("SENDER_EMAIL",  "noreply@company.com")
    team_email:    str  = os.getenv("TEAM_EMAIL",     "team@company.com")
    use_tls:       bool = os.getenv("SMTP_TLS", "true").lower() == "true"


@dataclass(frozen=True)
class _SchedulerConfig:
    query_interval_hours:      int = int(os.getenv("QUERY_INTERVAL_H",       "8"))
    escalation_interval_hours: int = int(os.getenv("ESCALATION_INTERVAL_H", "16"))
    failed_interval_hours:     int = int(os.getenv("FAILED_INTERVAL_H",     "24"))


@dataclass(frozen=True)
class _ClassifierConfig:
    immediate_keywords: FrozenSet[str] = frozenset({
        "buy", "purchase", "order", "price", "pricing", "quote",
        "wholesale", "bulk", "negotiate", "negotiation", "deal",
        "discount", "offer", "payment", "invoice", "checkout",
        "upgrade", "subscription", "renew",
    })
    escalation_keywords: FrozenSet[str] = frozenset({
        "escalat", "complaint", "refund", "chargeback",
        "legal", "fraud", "urgent", "critical",
    })


@dataclass(frozen=True)
class _PhoneConfig:
    min_digits: int = 7
    max_digits: int = 15


@dataclass(frozen=True)
class _AppConfig:
    email:      _EmailConfig      = field(default_factory=_EmailConfig)
    scheduler:  _SchedulerConfig  = field(default_factory=_SchedulerConfig)
    classifier: _ClassifierConfig = field(default_factory=_ClassifierConfig)
    phone:      _PhoneConfig      = field(default_factory=_PhoneConfig)


CONFIG = _AppConfig()


# ============================================================================
# SECTION 3 — CLEANER
# ============================================================================

_DIGITS_ONLY = re.compile(r"\D")
_EMAIL_RE    = re.compile(r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$")


def _clean_str(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _clean_email(email: Optional[str]) -> Optional[str]:
    if email is None:
        return None
    normalised = email.strip().lower()
    if not _EMAIL_RE.match(normalised):
        logger.debug("Dropping malformed email: %s", email)
        return None
    return normalised


def _clean_phone(phone: Optional[str]) -> Optional[str]:
    if phone is None:
        return None
    has_plus = phone.strip().startswith("+")
    digits   = _DIGITS_ONLY.sub("", phone)
    n        = len(digits)
    cfg      = CONFIG.phone
    if not (cfg.min_digits <= n <= cfg.max_digits):
        logger.debug("Phone '%s' has %d digits — outside [%d,%d]; invalid.", phone, n, cfg.min_digits, cfg.max_digits)
        return None
    return ("+" + digits) if has_plus else digits


def _clean_csat(csat) -> Optional[float]:
    if csat is None:
        return None
    try:
        value = float(csat)
    except (TypeError, ValueError):
        logger.debug("Non-numeric CSAT '%s' — discarded.", csat)
        return None
    if not (0.0 <= value <= 10.0):
        logger.debug("CSAT %s out of range [0,10] — discarded.", value)
        return None
    return round(value, 2)


def _clean_frustration(value) -> Optional[int]:
    """
    Accepts both a plain integer (new payload: frustration_score: 0)
    and a string label (legacy payload: "low" / "medium" / "high").

    Numeric values are range-checked against [0, 10] and returned as int.
    String labels are mapped to a representative midpoint integer.
    Anything else is discarded and returns None.
    """
    if value is None:
        return None

    # --- numeric path (new payload sends an int or float) ---
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        score = int(value)
        if 0 <= score <= 10:
            return score
        logger.debug("Frustration score %s out of range [0,10] — discarded.", value)
        return None

    # --- string path (legacy labels or a numeric string like "7") ---
    if isinstance(value, str):
        label_map = {"low": 2, "medium": 5, "high": 8}
        stripped = value.strip().lower()
        if stripped in label_map:
            return label_map[stripped]
        try:
            score = int(stripped)
            if 0 <= score <= 10:
                return score
        except ValueError:
            pass
        logger.debug("Unrecognised FRUSTRATION value '%s' — discarded.", value)

    return None


def _clean(lead: RawLead) -> RawLead:
    """Return a new RawLead with all fields normalised. Original is never mutated."""
    return RawLead(
        NAME        = _clean_str(lead.NAME),
        PHONE       = _clean_phone(lead.PHONE),
        EMAIL       = _clean_email(lead.EMAIL),
        REASON      = _clean_str(lead.REASON),
        EVENT       = _clean_str(lead.EVENT),
        TICKET_ID   = _clean_str(lead.TICKET_ID),
        SESSION_ID  = _clean_str(lead.SESSION_ID),
        CSAT        = _clean_csat(lead.CSAT),
        LANGUAGE    = _clean_str(lead.LANGUAGE),
        FRUSTRATION = _clean_frustration(lead.FRUSTRATION),  # ← was _clean_str; now handles int
        TURN_COUNT  = lead.TURN_COUNT,
    )


# ============================================================================
# SECTION 4 — VALIDATOR
# ============================================================================

def _validate(lead: RawLead) -> Tuple[bool, List[str]]:
    """
    Returns (is_valid, errors).
    HARD_STOP errors block the lead; WARN errors are logged but non-blocking.
    """
    errors: List[str] = []

    if not any([lead.NAME, lead.PHONE, lead.EMAIL]):
        errors.append("HARD_STOP: No primary contact method (NAME, PHONE, EMAIL all absent).")

    if not lead.TICKET_ID:
        errors.append("WARN: TICKET_ID missing — traceability limited.")
    if not lead.REASON:
        errors.append("WARN: REASON missing — defaults to QUERY intent.")
    if not lead.EVENT:
        errors.append("WARN: EVENT missing — escalation detection skipped.")
    if lead.CSAT is None:
        errors.append("WARN: CSAT absent or unparseable.")

    if errors:
        logger.debug("Validation issues for TICKET_ID=%s: %s", lead.TICKET_ID, "; ".join(errors))

    is_valid = not any(e.startswith("HARD_STOP") for e in errors)
    return is_valid, errors


# ============================================================================
# SECTION 5 — CLASSIFIER
# ============================================================================

def _contains_any(text: Optional[str], keywords: FrozenSet[str]) -> bool:
    if not text:
        return False
    lower = text.lower()
    return any(kw in lower for kw in keywords)


def _classify(
    lead: RawLead,
) -> Tuple[LeadType, Optional[CallbackTier], Optional[CallbackIntent]]:
    """
    Returns (lead_type, callback_tier, callback_intent).
    callback_tier / callback_intent are None for Escalation leads.
    callback_intent is None for Failed Callback leads.
    """
    if _contains_any(lead.EVENT, CONFIG.classifier.escalation_keywords):
        logger.debug("TICKET_ID=%s → Escalation", lead.TICKET_ID)
        return LeadType.ESCALATION, None, None

    tier = CallbackTier.SUCCESSFUL if any([lead.NAME, lead.PHONE, lead.EMAIL]) else CallbackTier.FAILED

    if tier == CallbackTier.FAILED:
        logger.debug("TICKET_ID=%s → Callback / Failed", lead.TICKET_ID)
        return LeadType.CALLBACK, tier, None

    intent = (
        CallbackIntent.IMMEDIATE
        if _contains_any(lead.REASON, CONFIG.classifier.immediate_keywords)
        else CallbackIntent.QUERY
    )
    logger.debug("TICKET_ID=%s → Callback / Successful / %s", lead.TICKET_ID, intent)
    return LeadType.CALLBACK, tier, intent


# ============================================================================
# SECTION 6 — DEDUPLICATOR
# ============================================================================

def _deduplicate(lead: ProcessedLead) -> ProcessedLead:
    """
    Case A — prior is Notified     → mark new as RECURRING  + ref prior TICKET_ID
    Case B — prior is not Notified → mark new as DUPLICATE  + ref prior TICKET_ID
    No match                       → status stays NEW

    Edge cases:
    - No PHONE or EMAIL → skip check (no identifier to match on).
    - Re-entrant calls  → run_id exclusion in DB prevents self-matching.
    """
    r = lead.raw
    if not r.PHONE and not r.EMAIL:
        logger.debug("TICKET_ID=%s — dedup skipped: no PHONE or EMAIL.", r.TICKET_ID)
        return lead

    prior = db.find_duplicate(phone=r.PHONE, email=r.EMAIL, current_run_id=lead.run_id)
    if prior is None:
        logger.debug("TICKET_ID=%s — no duplicate found.", r.TICKET_ID)
        return lead

    if prior["status"] == LeadStatus.NOTIFIED:
        lead.status             = LeadStatus.RECURRING
        lead.previous_ticket_id = prior["ticket_id"]
        logger.info("TICKET_ID=%s → RECURRING (prior Notified: %s)", r.TICKET_ID, prior["ticket_id"])
    else:
        lead.status             = LeadStatus.DUPLICATE
        lead.previous_ticket_id = prior["ticket_id"]
        logger.info("TICKET_ID=%s → DUPLICATE (prior unnotified: %s)", r.TICKET_ID, prior["ticket_id"])

    return lead


# ============================================================================
# SECTION 7 — EMAIL SERVICE
# ============================================================================

def _send_smtp(to: str, subject: str, body_html: str) -> bool:
    """
    Send one email via SMTP/TLS or SSL.
    Returns True on success, False on any failure.
    Never raises — the pipeline must keep running regardless.
    """
    cfg = CONFIG.email
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = cfg.sender_email
    msg["To"]      = to
    msg.attach(MIMEText(body_html, "html"))

    try:
        # Increased timeout to 30s for slower cloud cold-starts
        timeout_secs = 30 
        
        # Automatically handle the difference between Port 465 (SSL) and 587 (TLS)
        if cfg.smtp_port == 465:
            server = smtplib.SMTP_SSL(cfg.smtp_host, cfg.smtp_port, timeout=timeout_secs)
        else:
            server = smtplib.SMTP(cfg.smtp_host, cfg.smtp_port, timeout=timeout_secs)
            if cfg.use_tls:
                server.starttls()
                
        with server:
            if cfg.smtp_user and cfg.smtp_password:
                server.login(cfg.smtp_user, cfg.smtp_password)
            server.sendmail(cfg.sender_email, [to], msg.as_string())
            
        logger.debug("Email sent → %s | %s", to, subject)
        return True
        
    except smtplib.SMTPRecipientsRefused:
        logger.error("Email rejected for recipient: %s", to)
    except smtplib.SMTPAuthenticationError:
        logger.error("SMTP authentication failed — check SMTP_USER / SMTP_PASSWORD.")
    except TimeoutError:
        logger.error("SMTP connection timed out for %s.", to)
    except Exception as exc:
        logger.error("Unexpected email error for %s: %s", to, exc)
        
    return False
def _lead_row_html(row: dict) -> str:
    cells = "".join(
        f"<td style='padding:4px 8px;border:1px solid #ddd'>{v or ''}</td>"
        for v in [
            row.get("ticket_id"),   row.get("name"),     row.get("phone"),
            row.get("email"),       row.get("reason"),   row.get("status"),
            row.get("callback_intent") or row.get("lead_type"),
            row.get("csat"),        row.get("language"), row.get("previous_ticket"),
        ]
    )
    return f"<tr>{cells}</tr>"


def _build_batch_html(queue_label: str, leads: List[dict]) -> str:
    headers = ["Ticket ID","Name","Phone","Email","Reason","Status","Type/Intent","CSAT","Language","Prior Ticket"]
    header_row = "".join(
        f"<th style='padding:4px 8px;background:#f5f5f5;border:1px solid #ddd'>{h}</th>"
        for h in headers
    )
    data_rows = "\n".join(_lead_row_html(dict(lead)) for lead in leads)
    return f"""
    <html><body>
    <h2>Lead Dispatch: {queue_label} Queue</h2>
    <p>Total leads: <strong>{len(leads)}</strong></p>
    <table style='border-collapse:collapse;font-family:sans-serif;font-size:13px'>
      <thead><tr>{header_row}</tr></thead>
      <tbody>{data_rows}</tbody>
    </table>
    </body></html>
    """


def _send_customer_confirmation(lead: ProcessedLead) -> bool:
    r       = lead.raw
    subject = f"We've received your request — Ticket #{r.TICKET_ID}"
    body    = f"""
    <html><body>
    <p>Hi {r.NAME or 'there'},</p>
    <p>Thank you for reaching out. Your request has been logged.</p>
    <p><strong>Ticket ID:</strong> {r.TICKET_ID}<br>
       <strong>Session ID:</strong> {r.SESSION_ID or 'N/A'}</p>
    <p>A member of our team will follow up with you shortly.</p>
    <p>— The Support Team</p>
    </body></html>
    """
    logger.info("Sending auto-confirm to %s (ticket %s)", r.EMAIL, r.TICKET_ID)
    return _send_smtp(r.EMAIL, subject, body)


def _dispatch_immediate(lead: ProcessedLead) -> bool:
    r       = lead.raw
    subject = f"[IMMEDIATE LEAD] {r.NAME or 'Unknown'} — Ticket #{r.TICKET_ID}"
    body    = _build_batch_html("Immediate", [lead.summary()])
    logger.info("Dispatching IMMEDIATE lead ticket=%s", r.TICKET_ID)
    return _send_smtp(CONFIG.email.team_email, subject, body)


def _dispatch_batch(queue_label: str, leads: List) -> bool:
    if not leads:
        logger.debug("Batch dispatch skipped — no pending leads for '%s'.", queue_label)
        return True
    subject = f"[{queue_label.upper()} BATCH] {len(leads)} lead(s) pending action"
    body    = _build_batch_html(queue_label, leads)
    logger.info("Dispatching %s batch: %d lead(s)", queue_label, len(leads))
    return _send_smtp(CONFIG.email.team_email, subject, body)


# ============================================================================
# SECTION 8 — SCHEDULER
# ============================================================================

_scheduler = BackgroundScheduler(timezone="UTC")


def _make_batch_job(queue: DispatchQueue) -> Callable:
    """Factory — returns a zero-arg job that drains one queue."""
    def job() -> None:
        label = queue.value.capitalize()
        logger.info("Running batch job: %s queue", label)
        leads = db.fetch_pending_by_queue(queue.value)
        if not leads:
            logger.info("%s batch: nothing to send.", label)
            return
        if _dispatch_batch(label, leads):
            run_ids = [row["run_id"] for row in leads]
            db.mark_notified(run_ids)
            logger.info("%s batch: %d lead(s) dispatched & marked Notified.", label, len(leads))
        else:
            # Do NOT mark Notified — leads stay pending for the next cycle
            logger.error("%s batch: dispatch FAILED — leads remain pending.", label)

    job.__name__ = f"batch_job_{queue.value}"
    return job


def _start_scheduler() -> None:
    cfg  = CONFIG.scheduler
    jobs = [
        (DispatchQueue.QUERY,      cfg.query_interval_hours),
        (DispatchQueue.ESCALATION, cfg.escalation_interval_hours),
        (DispatchQueue.FAILED,     cfg.failed_interval_hours),
    ]
    for queue, hours in jobs:
        _scheduler.add_job(
            func             = _make_batch_job(queue),
            trigger          = IntervalTrigger(hours=hours),
            id               = f"batch_{queue.value}",
            name             = f"Batch dispatch — {queue.value} (every {hours}h)",
            replace_existing = True,
            max_instances    = 1,     # prevent overlapping runs
            misfire_grace_time = 300, # 5-min grace window on late fire
        )
        logger.info("Scheduled: %s queue every %dh", queue.value, hours)
    _scheduler.start()
    logger.info("Background scheduler started (UTC).")


def stop_scheduler() -> None:
    """Call on application teardown."""
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")


# ============================================================================
# SECTION 9 — PIPELINE ORCHESTRATOR  (public API)
# ============================================================================

def initialise() -> None:
    """Call once on application startup. Idempotent."""
    db.initialise_db()
    _start_scheduler()


def process_lead(payload: Dict[str, Any]) -> ProcessedLead:
    """
    Process a single incoming lead payload end-to-end.

    Steps
    -----
    1. Parse      — build RawLead from dict, unknown keys discarded
    2. Clean      — normalise all fields
    3. Validate   — hard-stop check + soft warnings
    4. Classify   — LeadType → CallbackTier → CallbackIntent
    5. Deduplicate — Cases A & B against the database
    6. Persist    — upsert to DB (idempotent)
    7. Auto-email — customer confirmation if EMAIL present and lead is valid
    8. Dispatch   — immediate send for Immediate leads; others wait for batch
    """

    # 1. Parse
    lead = ProcessedLead(raw=RawLead.from_dict(payload))

    # 2. Clean
    lead.raw = _clean(lead.raw)

    # 3. Validate
    is_valid, errors = _validate(lead.raw)
    lead.validation_errors = errors

    # 4. Classify
    if is_valid:
        lead.lead_type, lead.callback_tier, lead.callback_intent = _classify(lead.raw)
    else:
        # Hard-stop leads are routed to Failed queue — never silently dropped
        lead.lead_type     = LeadType.CALLBACK
        lead.callback_tier = CallbackTier.FAILED
        logger.warning(
            "TICKET_ID=%s failed validation → Failed queue. Errors: %s",
            lead.raw.TICKET_ID, errors,
        )

    # 5. Deduplicate
    lead = _deduplicate(lead)

    # 6. Persist
    db.upsert_lead(lead)
    logger.info(
        "Lead persisted | ticket=%s | type=%s | queue=%s | status=%s",
        lead.raw.TICKET_ID, lead.lead_type, lead.dispatch_queue, lead.status,
    )

    # 7. Customer auto-confirmation
    if lead.should_send_auto_email:
        ok = _send_customer_confirmation(lead)
        if not ok:
            logger.warning(
                "Auto-confirm FAILED for TICKET_ID=%s EMAIL=%s — "
                "lead is saved; email can be retried manually.",
                lead.raw.TICKET_ID, lead.raw.EMAIL,
            )

    # 8. Immediate dispatch
    if lead.dispatch_queue == DispatchQueue.IMMEDIATE:
        if _dispatch_immediate(lead):
            db.mark_notified([lead.run_id])
            lead.status = LeadStatus.NOTIFIED
        else:
            logger.error(
                "Immediate dispatch FAILED for TICKET_ID=%s — "
                "lead remains in queue for manual follow-up.",
                lead.raw.TICKET_ID,
            )

    return lead


# ============================================================================
# SECTION 10 — WEBHOOK SERVER  (HTTP entry point from chatbot)
# ============================================================================

_flask_app = Flask(__name__)

# Set WEBHOOK_SECRET env var to enable HMAC-SHA256 signature verification.
# Your chatbot platform must sign requests with the same secret.
# Leave blank to disable verification (not recommended in production).
_WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")


def _verify_signature(body: bytes, sig_header: str) -> bool:
    """
    Validate the X-Hub-Signature-256 header sent by the chatbot.
    Returns True unconditionally when WEBHOOK_SECRET is not configured,
    so local development works without a secret.
    """
    if not _WEBHOOK_SECRET:
        return True
    expected = "sha256=" + hmac.new(
        _WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig_header or "")


# ── Liveness / health check ──────────────────────────────────────────────────

@_flask_app.route("/health", methods=["GET"])
def health():
    """
    Simple liveness probe.
    Returns HTTP 200 with scheduler status so load balancers and uptime
    monitors know the process is alive.
    """
    return jsonify({
        "status":            "ok",
        "scheduler_running": _scheduler.running,
        "db_path":           os.getenv("DB_PATH", "leads.db"),
    }), 200


# ── Admin: pending lead counts ────────────────────────────────────────────────

@_flask_app.route("/leads/pending", methods=["GET"])
def leads_pending():
    """
    Return the number of un-notified leads per queue.
    Protect this route with a reverse-proxy rule (nginx basic auth /
    IP allowlist) in production.
    """
    counts = {}
    for queue in DispatchQueue:
        rows = db.fetch_pending_by_queue(queue.value)
        counts[queue.value] = len(rows)
    return jsonify({"pending": counts}), 200


# ── Main webhook endpoint ─────────────────────────────────────────────────────

@_flask_app.route("/webhook/lead", methods=["POST"])
def webhook_lead():
    """
    Receive a lead payload from the deployed chatbot and run the full
    8-step pipeline automatically.

    Expected headers
    ----------------
    Content-Type: application/json
    X-Hub-Signature-256: sha256=<hmac>   (required when WEBHOOK_SECRET is set)

    Response codes
    --------------
    200  Lead processed successfully. Body contains the lead summary dict.
    400  Empty body or non-JSON content.
    401  HMAC signature mismatch.
    422  Lead failed hard validation but was saved to the Failed queue.
    500  Unexpected exception inside the pipeline.
    """

    # 1. Signature verification
    sig_header = request.headers.get("X-Hub-Signature-256", "")
    if not _verify_signature(request.data, sig_header):
        logger.warning(
            "Webhook: signature mismatch from %s — request rejected.",
            request.remote_addr,
        )
        return jsonify({"error": "invalid signature"}), 401

    # 2. Parse body
    payload = request.get_json(force=True, silent=True)
    if not payload:
        logger.warning("Webhook: empty or non-JSON body from %s.", request.remote_addr)
        return jsonify({"error": "request body must be valid JSON"}), 400

    logger.info(
        "Webhook: payload received from %s | event=%s | ticket=%s",
        request.remote_addr,
        payload.get("event", "—"),
        (payload.get("callback_data") or payload).get("ticket_id", "—"),
    )

    # 3. Run the full pipeline
    try:
        result = process_lead(payload)
    except Exception as exc:
        logger.exception("Webhook: unhandled exception in process_lead: %s", exc)
        return jsonify({
            "error":  "internal processing error",
            "detail": str(exc),
        }), 500

    # 4. Return summary — 422 if the lead failed hard validation
    summary     = result.summary()
    http_status = 422 if result.validation_errors and any(
        e.startswith("HARD_STOP") for e in result.validation_errors
    ) else 200

    return jsonify(summary), http_status


# ============================================================================
# SECTION 11 — GRACEFUL SHUTDOWN
# ============================================================================

def _on_shutdown(*_) -> None:
    """Stop the background scheduler cleanly on SIGTERM or process exit."""
    stop_scheduler()


atexit.register(_on_shutdown)
signal.signal(signal.SIGTERM, _on_shutdown)


# ============================================================================
# SECTION 12 — ENTRY POINT
# ============================================================================

def run_server(
    host:  str  = "0.0.0.0",
    port:  int  = 8000,
    debug: bool = False,
) -> None:
    """
    Initialise DB + scheduler, then start the Flask server.

    For production, use gunicorn instead:
        gunicorn "lead_pipeline:_flask_app" --bind 0.0.0.0:8000 --workers 2
    Call initialise() before gunicorn starts (e.g. in a startup hook or
    gunicorn's post_fork hook).
    """
    initialise()
    logger.info("Starting webhook server on %s:%d (debug=%s)", host, port, debug)
    try:
        # use_reloader=False prevents the scheduler starting twice when
        # Flask's debug reloader spawns a child process.
        _flask_app.run(host=host, port=port, debug=debug, use_reloader=False)
    finally:
        _on_shutdown()
# Called when gunicorn imports this module (--preload flag)
if os.getenv("RENDER"):
    initialise()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Lead pipeline webhook server — chatbot → automation workflow"
    )
    parser.add_argument(
        "--host",
        default = "0.0.0.0",
        help    = "Interface to bind (default: 0.0.0.0)",
    )
    parser.add_argument(
        "--port",
        type    = int,
        default = int(os.getenv("PORT", "8000")),
        help    = "Port to listen on (default: 8000, or $PORT env var)",
    )
    parser.add_argument(
        "--debug",
        action  = "store_true",
        help    = "Enable Flask debug mode (do not use in production)",
    )
    args = parser.parse_args()
    run_server(host=args.host, port=args.port, debug=args.debug)
