"""
lead_pipeline.py — Chatbot webhook receiver + full lead automation pipeline.

This script uses an HTTP Email API (Brevo) to bypass all cloud SMTP restrictions,
guaranteeing reliable email delivery without port blocking or connection timeouts.
"""

from __future__ import annotations

import atexit
import hashlib
import hmac
import logging
import re
import signal
import uuid
import urllib.request
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, FrozenSet, List, Optional, Tuple

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
from flask import Flask, jsonify, request

import database as db

# --- LOGGING CONFIGURATION FOR GUNICORN ---
logging.basicConfig(
    level   = logging.INFO,
    format  = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt = "%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logging.getLogger(__name__).addHandler(logging.NullHandler())
# ------------------------------------------

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
    IMMEDIATE  = "immediate"
    QUERY      = "query"
    ESCALATION = "escalation"
    FAILED     = "failed"

@dataclass
class RawLead:
    NAME:        Optional[str]   = None
    PHONE:       Optional[str]   = None
    EMAIL:       Optional[str]   = None
    REASON:      Optional[str]   = None
    EVENT:       Optional[str]   = None
    TICKET_ID:   Optional[str]   = None
    SESSION_ID:  Optional[str]   = None
    CSAT:        Optional[float] = None
    LANGUAGE:    Optional[str]   = None
    FRUSTRATION: Optional[int]   = None
    TURN_COUNT:  Optional[int]   = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items()}

    @classmethod
    def from_dict(cls, data: dict) -> "RawLead":
        flat = {k: v for k, v in data.items() if k != "callback_data"}
        flat.update(data.get("callback_data") or {})
        flat = {k.upper(): v for k, v in flat.items()}

        aliases = {
            "FRUSTRATION_SCORE": "FRUSTRATION",
            "TURN_COUNT":        "TURN_COUNT",
        }
        for src_key, dest_key in aliases.items():
            if src_key in flat and dest_key not in flat:
                flat[dest_key] = flat.pop(src_key)

        known = set(cls.__dataclass_fields__)
        return cls(**{k: v for k, v in flat.items() if k in known})

@dataclass
class ProcessedLead:
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
    email_api_key: str  = os.getenv("EMAIL_API_KEY", "")
    sender_email:  str  = os.getenv("SENDER_EMAIL",  "noreply@company.com")
    team_email:    str  = os.getenv("TEAM_EMAIL",    "team@company.com")

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
    if value is None: return None
    cleaned = value.strip()
    return cleaned or None

def _clean_email(email: Optional[str]) -> Optional[str]:
    if email is None: return None
    normalised = email.strip().lower()
    if not _EMAIL_RE.match(normalised):
        return None
    return normalised

def _clean_phone(phone: Optional[str]) -> Optional[str]:
    if phone is None: return None
    has_plus = phone.strip().startswith("+")
    digits   = _DIGITS_ONLY.sub("", phone)
    n        = len(digits)
    cfg      = CONFIG.phone
    if not (cfg.min_digits <= n <= cfg.max_digits):
        return None
    return ("+" + digits) if has_plus else digits

def _clean_csat(csat) -> Optional[float]:
    if csat is None: return None
    try:
        value = float(csat)
    except (TypeError, ValueError):
        return None
    if not (0.0 <= value <= 10.0):
        return None
    return round(value, 2)

def _clean_frustration(value) -> Optional[int]:
    if value is None: return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        score = int(value)
        if 0 <= score <= 10: return score
        return None
    if isinstance(value, str):
        label_map = {"low": 2, "medium": 5, "high": 8}
        stripped = value.strip().lower()
        if stripped in label_map: return label_map[stripped]
        try:
            score = int(stripped)
            if 0 <= score <= 10: return score
        except ValueError:
            pass
    return None

def _clean(lead: RawLead) -> RawLead:
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
        FRUSTRATION = _clean_frustration(lead.FRUSTRATION),
        TURN_COUNT  = lead.TURN_COUNT,
    )

# ============================================================================
# SECTION 4 — VALIDATOR
# ============================================================================

def _validate(lead: RawLead) -> Tuple[bool, List[str]]:
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
    if not text: return False
    lower = text.lower()
    return any(kw in lower for kw in keywords)

def _classify(
    lead: RawLead,
) -> Tuple[LeadType, Optional[CallbackTier], Optional[CallbackIntent]]:
    if _contains_any(lead.EVENT, CONFIG.classifier.escalation_keywords):
        return LeadType.ESCALATION, None, None

    tier = CallbackTier.SUCCESSFUL if any([lead.NAME, lead.PHONE, lead.EMAIL]) else CallbackTier.FAILED

    if tier == CallbackTier.FAILED:
        return LeadType.CALLBACK, tier, None

    intent = (
        CallbackIntent.IMMEDIATE
        if _contains_any(lead.REASON, CONFIG.classifier.immediate_keywords)
        else CallbackIntent.QUERY
    )
    return LeadType.CALLBACK, tier, intent

# ============================================================================
# SECTION 6 — DEDUPLICATOR
# ============================================================================

def _deduplicate(lead: ProcessedLead) -> ProcessedLead:
    r = lead.raw
    if not r.PHONE and not r.EMAIL:
        return lead

    prior = db.find_duplicate(phone=r.PHONE, email=r.EMAIL, current_run_id=lead.run_id)
    if prior is None:
        return lead

    if prior["status"] == LeadStatus.NOTIFIED:
        lead.status             = LeadStatus.RECURRING
        lead.previous_ticket_id = prior["ticket_id"]
    else:
        lead.status             = LeadStatus.DUPLICATE
        lead.previous_ticket_id = prior["ticket_id"]

    return lead

# ============================================================================
# SECTION 7 — API EMAIL SERVICE (ROCK-SOLID HTTP METHOD)
# ============================================================================

def _send_email(to: str, subject: str, body_html: str) -> bool:
    """
    Sends an email using the Brevo HTTP API via port 443.
    This bypasses all cloud SMTP restrictions and port blockages.
    """
    cfg = CONFIG.email
    if not cfg.email_api_key:
        logger.error("EMAIL_API_KEY is not set. Cannot send email.")
        return False

    payload = {
        "sender": {"email": cfg.sender_email, "name": "AdiShila Support"},
        "to": [{"email": to}],
        "subject": subject,
        "htmlContent": body_html
    }

    headers = {
        "accept": "application/json",
        "api-key": cfg.email_api_key,
        "content-type": "application/json"
    }

    try:
        req = urllib.request.Request(
            "https://api.brevo.com/v3/smtp/email",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=15) as response:
            if response.status in (200, 201, 202):
                logger.info("API Email sent successfully to %s", to)
                return True
            else:
                logger.error("API returned status %s", response.status)
                return False
    except Exception as exc:
        logger.error("Unexpected API email error for %s: %s", to, exc)
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
    logger.info("Sending API auto-confirm to %s (ticket %s)", r.EMAIL, r.TICKET_ID)
    return _send_email(r.EMAIL, subject, body)

def _dispatch_immediate(lead: ProcessedLead) -> bool:
    r       = lead.raw
    subject = f"[IMMEDIATE LEAD] {r.NAME or 'Unknown'} — Ticket #{r.TICKET_ID}"
    body    = _build_batch_html("Immediate", [lead.summary()])
    logger.info("Dispatching IMMEDIATE lead ticket=%s via API", r.TICKET_ID)
    return _send_email(CONFIG.email.team_email, subject, body)

def _dispatch_batch(queue_label: str, leads: List) -> bool:
    if not leads:
        return True
    subject = f"[{queue_label.upper()} BATCH] {len(leads)} lead(s) pending action"
    body    = _build_batch_html(queue_label, leads)
    logger.info("Dispatching %s batch via API: %d lead(s)", queue_label, len(leads))
    return _send_email(CONFIG.email.team_email, subject, body)

# ============================================================================
# SECTION 8 — SCHEDULER
# ============================================================================

_scheduler = BackgroundScheduler(timezone="UTC")

def _make_batch_job(queue: DispatchQueue) -> Callable:
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
            max_instances    = 1,     
            misfire_grace_time = 300, 
        )
        logger.info("Scheduled: %s queue every %dh", queue.value, hours)
    _scheduler.start()
    logger.info("Background scheduler started (UTC).")

def stop_scheduler() -> None:
    if _scheduler.running:
        _scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped.")

# ============================================================================
# SECTION 9 — PIPELINE ORCHESTRATOR
# ============================================================================

def initialise() -> None:
    db.initialise_db()
    _start_scheduler()

def process_lead(payload: Dict[str, Any]) -> ProcessedLead:
    lead = ProcessedLead(raw=RawLead.from_dict(payload))
    lead.raw = _clean(lead.raw)

    is_valid, errors = _validate(lead.raw)
    lead.validation_errors = errors

    if is_valid:
        lead.lead_type, lead.callback_tier, lead.callback_intent = _classify(lead.raw)
    else:
        lead.lead_type     = LeadType.CALLBACK
        lead.callback_tier = CallbackTier.FAILED
        logger.warning(
            "TICKET_ID=%s failed validation → Failed queue. Errors: %s",
            lead.raw.TICKET_ID, errors,
        )

    lead = _deduplicate(lead)
    db.upsert_lead(lead)
    
    logger.info(
        "Lead persisted | ticket=%s | type=%s | queue=%s | status=%s",
        lead.raw.TICKET_ID, lead.lead_type, lead.dispatch_queue, lead.status,
    )

    if lead.should_send_auto_email:
        ok = _send_customer_confirmation(lead)
        if not ok:
            logger.warning("Auto-confirm FAILED for TICKET_ID=%s EMAIL=%s", lead.raw.TICKET_ID, lead.raw.EMAIL)

    if lead.dispatch_queue == DispatchQueue.IMMEDIATE:
        if _dispatch_immediate(lead):
            db.mark_notified([lead.run_id])
            lead.status = LeadStatus.NOTIFIED
        else:
            logger.error("Immediate dispatch FAILED for TICKET_ID=%s", lead.raw.TICKET_ID)

    return lead

# ============================================================================
# SECTION 10 — WEBHOOK SERVER
# ============================================================================

_flask_app = Flask(__name__)
_WEBHOOK_SECRET: str = os.getenv("WEBHOOK_SECRET", "")

def _verify_signature(body: bytes, sig_header: str) -> bool:
    if not _WEBHOOK_SECRET:
        return True
    expected = "sha256=" + hmac.new(
        _WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig_header or "")

@_flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({
        "status":            "ok",
        "scheduler_running": _scheduler.running,
        "db_path":           os.getenv("DB_PATH", "leads.db"),
    }), 200

@_flask_app.route("/leads/pending", methods=["GET"])
def leads_pending():
    counts = {}
    for queue in DispatchQueue:
        rows = db.fetch_pending_by_queue(queue.value)
        counts[queue.value] = len(rows)
    return jsonify({"pending": counts}), 200

@_flask_app.route("/webhook/lead", methods=["POST"])
def webhook_lead():
    sig_header = request.headers.get("X-Hub-Signature-256", "")
    if not _verify_signature(request.data, sig_header):
        return jsonify({"error": "invalid signature"}), 401

    payload = request.get_json(force=True, silent=True)
    if not payload:
        return jsonify({"error": "request body must be valid JSON"}), 400

    logger.info(
        "Webhook: payload received from %s | event=%s | ticket=%s",
        request.remote_addr,
        payload.get("event", "—"),
        (payload.get("callback_data") or payload).get("ticket_id", "—"),
    )

    try:
        result = process_lead(payload)
    except Exception as exc:
        logger.exception("Webhook: unhandled exception in process_lead: %s", exc)
        return jsonify({"error": "internal processing error", "detail": str(exc)}), 500

    summary = result.summary()
    http_status = 422 if result.validation_errors and any(
        e.startswith("HARD_STOP") for e in result.validation_errors
    ) else 200

    return jsonify(summary), http_status

# ============================================================================
# SECTION 11 — GRACEFUL SHUTDOWN
# ============================================================================

def _on_shutdown(*_) -> None:
    stop_scheduler()

atexit.register(_on_shutdown)
signal.signal(signal.SIGTERM, _on_shutdown)

# ============================================================================
# SECTION 12 — ENTRY POINT
# ============================================================================

def run_server(host: str = "0.0.0.0", port: int = 8000, debug: bool = False) -> None:
    initialise()
    logger.info("Starting webhook server on %s:%d (debug=%s)", host, port, debug)
    try:
        _flask_app.run(host=host, port=port, debug=debug, use_reloader=False)
    finally:
        _on_shutdown()

if os.getenv("RENDER"):
    initialise()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Lead pipeline webhook server")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8000")))
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    run_server(host=args.host, port=args.port, debug=args.debug)
