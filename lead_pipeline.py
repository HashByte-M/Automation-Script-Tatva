# lead_pipeline.py — Stateless Google Sheets CRM with Smart Batching & Reorder Logic

"""
This script handles webhooks, logs to Google Sheets, sends immediate emails for urgent leads.
It features strict deduplication, silently routes CSAT scores, and enforces strict rules 
around when to send emails based on the presence of contact information and Ticket IDs.
"""

import hmac
import hashlib
import logging
import urllib.request
import json
import os
import gspread
import random
import string
from datetime import datetime
from flask import Flask, jsonify, request

# --- LOGGING CONFIGURATION ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ============================================================================
# CONFIGURATION
# ============================================================================

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
EMAIL_API_KEY  = os.getenv("EMAIL_API_KEY", "")
SENDER_EMAIL   = os.getenv("SENDER_EMAIL", "info@adishila.in")
TEAM_EMAIL     = os.getenv("TEAM_EMAIL", "team@adishila.in")
SUPPORT_PHONE  = "+91 86301 79867"

# Google Sheets Configuration
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID")

# Keywords for classification
IMMEDIATE_KEYWORDS = ["buy", "purchase", "order", "price", "pricing", "quote", "wholesale", "urgent"]

# Google Form — pre-fill base URL for pipeline stage updates
# Replace FORM_ID and ENTRY_TICKET_ID with your actual values from the form's pre-fill link
FORM_BASE_URL = "https://docs.google.com/forms/d/e/1FAIpQLSfbcez3LZvaLRXIioeO6CIMqmZhteULntiNgYx3Np23CCl0mQ/viewform?usp=pp_url&entry.215603920="

# Google Sheet Column Mapping (1-indexed for gspread)
COL_DATE        = 1
COL_TICKET      = 2
COL_NAME        = 3
COL_PHONE       = 4
COL_EMAIL       = 5
COL_REASON      = 6
COL_LANGUAGE    = 7
COL_STATUS      = 8
COL_PRIOR_TKT   = 9
COL_CSAT        = 10
COL_FRUSTRATION = 11
COL_INTENT      = 12
COL_PIPELINE    = 13
COL_COMMENTS    = 14

# ============================================================================
# GOOGLE SHEETS INTEGRATION (THE CRM)
# ============================================================================

def _get_sheet():
    """Authenticates and returns the main worksheet."""
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise ValueError("Missing Google Sheets credentials.")
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    try:
        return gc.open_by_key(GOOGLE_SHEET_ID).worksheet("Sheet 1")
    except Exception as e:
        if "404" in str(e) or "SpreadsheetNotFound" in str(type(e).__name__):
            logger.error("CRITICAL: Google Sheet 404 Not Found! Ensure GOOGLE_SHEET_ID is correct and the service account email is added as an Editor.")
        raise e

def generate_fallback_ticket_id() -> str:
    """Generates a random ticket ID for orphan webhooks."""
    p1 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    p2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"TKT-AUTO-{p1}-{p2}"

def check_for_duplicate(sheet, phone: str, email: str) -> str:
    """Returns the previous Ticket ID if a duplicate customer exists (Reorder), else empty string."""
    if not phone and not email:
        return ""
    try:
        records = sheet.get_all_records()
        for row in reversed(records):
            row_phone = str(row.get("PHONE", "") or row.get("Phone", ""))
            row_email = str(row.get("EMAIL", "") or row.get("Email", ""))
            if (phone and phone in row_phone) or (email and email.lower() == row_email.lower()):
                return str(row.get("TICKET_ID", "") or row.get("Ticket ID", ""))
    except Exception as e:
        logger.error(f"Error checking for duplicates in CRM: {e}")
    return ""

def append_to_google_sheet(lead_data: dict, prior_ticket: str, intent: str, status: str) -> None:
    try:
        sheet = _get_sheet()

        # Auto-derive the starting pipeline stage from the CRM status
        stage_map = {
            "Unreachable":            "Unreachable",
            "CSAT Only (No Contact)": "CSAT Only",
            "New":                    "Cold",
            "Recurring":              "Cold",
            "Notified (Immediate)":   "Cold",
            "Notified (Reorder)":     "Cold",
        }
        pipeline_stage = stage_map.get(status, "Cold")

        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            lead_data.get("TICKET_ID") or "N/A",
            lead_data.get("NAME") or "N/A",
            lead_data.get("PHONE") or "N/A",
            lead_data.get("EMAIL") or "N/A",
            lead_data.get("REASON") or "N/A",
            lead_data.get("LANGUAGE") or "N/A",
            status or "N/A",
            prior_ticket or "N/A",
            lead_data.get("CSAT") or "N/A",
            lead_data.get("FRUSTRATION_SCORE") or "N/A",
            intent or "N/A",
            pipeline_stage,  # Col 13 — auto-set on arrival
            ""               # Col 14 — Comments, blank on arrival, filled via Google Form
        ]
        sheet.append_row(row)
        logger.info(f"Added Ticket #{lead_data.get('TICKET_ID')} to CRM. Status: {status} | Pipeline: {pipeline_stage}")
    except Exception as e:
        logger.error(f"Failed to append to Google Sheets: {e}")

# ============================================================================
# EMAIL INTEGRATION (BREVO) & CUSTOM TEMPLATES
# ============================================================================

def send_brevo_email(to_email: str, subject: str, html_content: str) -> bool:
    if not EMAIL_API_KEY:
        logger.error("EMAIL_API_KEY is not set.")
        return False

    payload = {
        "sender": {"email": SENDER_EMAIL, "name": "AdiShila Support"},
        "to": [{"email": to_email}],
        "subject": subject,
        "htmlContent": html_content
    }
    headers = {
        "accept": "application/json",
        "api-key": EMAIL_API_KEY,
        "content-type": "application/json"
    }

    try:
        req = urllib.request.Request(
            "https://api.brevo.com/v3/smtp/email",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status in (200, 201, 202)
    except Exception as e:
        logger.error(f"Failed to send email to {to_email}: {e}")
        return False

def send_customer_confirmation(lead: dict):
    email = lead.get("EMAIL")
    ticket_id = lead.get("TICKET_ID")
    
    # Strict rule: Must have email and ticket ID
    if not email or not ticket_id or ticket_id == "N/A": 
        return
        
    name = lead.get("NAME", "Customer")
    phone = lead.get("PHONE") or "your registered contact number"
    date_str = datetime.now().strftime("%B %d, %Y")
    
    subject = f"We've Received Your Request – Ticket #{ticket_id} | AdiShila"
    body = f"""
    <div style="font-family: 'Inter', Arial, sans-serif; background-color: #0E0E0E; color: #FAF7F2; line-height: 1.6; max-width: 600px; margin: 0 auto; padding: 40px 30px; border-top: 4px solid #C8A96E;">
        
        <div style="text-align: center; margin-bottom: 40px;">
            <h1 style="font-family: 'Cormorant Garamond', Georgia, serif; color: #C8A96E; margin: 0; font-size: 32px; letter-spacing: 3px; font-weight: normal;">AdiShila</h1>
            <p style="color: #9A9286; font-size: 11px; letter-spacing: 2px; text-transform: uppercase; margin-top: 8px;">The Primordial Stone</p>
        </div>

        <p>Namaskaram {name},</p>
        <p>Thank you for reaching out to AdiShila. 🙏</p>
        <p>We have successfully received your callback request. Whether you are exploring our authentic Karelian Shungite for Vedic practices, Vastu correction, or personal wellness, your query is deeply important to us.</p>
        
        <div style="background-color: #1A1A1A; border: 1px solid rgba(200,169,110,0.2); padding: 25px; margin: 30px 0;">
            <h3 style="font-family: 'Cormorant Garamond', Georgia, serif; margin: 0 0 20px 0; font-size: 18px; font-weight: normal; color: #C8A96E; letter-spacing: 1px;">📋 Your Ticket Details</h3>
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <tr><td style="padding: 6px 0; width: 120px; color: #9A9286;">Ticket ID</td><td style="padding: 6px 0; color: #FAF7F2;">: <strong>#{ticket_id}</strong></td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Request Date</td><td style="padding: 6px 0; color: #FAF7F2;">: {date_str}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Status</td><td style="padding: 6px 0; color: #C8A96E;">: Under Review</td></tr>
            </table>
        </div>
        
        <p>Our dedicated support team will get in touch with you within the next 48 hours on {phone}.</p>
        <p>In the meantime, if you have any additional information to share, feel free to reply directly to this email quoting your Ticket ID.</p>
        <p>We appreciate your patience and look forward to assisting you on your wellness journey!</p>
        
        <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid rgba(200,169,110,0.1); font-size: 13px; color: #9A9286;">
            Warm regards,<br>
            <strong style="color: #C8A96E; font-size: 14px;">Customer Support Team</strong><br>
            AdiShila<br><br>
            📧 info@adishila.in<br>
            🌐 www.adishila.in<br>
            📞 {SUPPORT_PHONE}
        </div>
    </div>
    """
    send_brevo_email(email, subject, body)

def send_immediate_team_notification(lead: dict, prior_ticket: str, intent: str, display_status: str):
    ticket_id = lead.get("TICKET_ID", "N/A")
    date_str = datetime.now().strftime("%d-%b-%Y")
    
    subject_prefix = "REORDER" if "Reorder" in display_status else "URGENT ASSIGNMENT"
    subject = f"{subject_prefix} | Ticket #{ticket_id} | {intent} | {date_str}"

    # Pre-filled Google Form link — Ticket ID is baked in, team just picks stage + adds comment
    form_link = f"{FORM_BASE_URL}{ticket_id}"
    
    body = f"""
    <div style="font-family: 'Inter', Arial, sans-serif; background-color: #0E0E0E; color: #FAF7F2; line-height: 1.5; max-width: 650px; margin: 0 auto; padding: 30px;">
        
        <div style="margin-bottom: 25px; padding-bottom: 15px; border-bottom: 1px solid rgba(200,169,110,0.2);">
            <h1 style="font-family: 'Cormorant Garamond', Georgia, serif; color: #C8A96E; margin: 0; font-size: 24px; font-weight: normal;">AdiShila Internal Dispatch</h1>
            <p style="color: #9A9286; font-size: 12px; margin-top: 5px;">Automated Ticket Routing System</p>
        </div>

        <p>Dear Resolution Team,</p>
        <p>A new <strong style="color: #C8A96E;">high-priority</strong> callback request has been assigned to your queue. Please ensure contact is made within 48 hours.</p>
        
        <div style="background-color: #1A1A1A; border: 1px solid rgba(200,169,110,0.3); padding: 25px; margin: 25px 0;">
            <h3 style="margin: 0 0 15px 0; font-size: 14px; font-weight: normal; color: #C8A96E; letter-spacing: 2px; text-transform: uppercase; border-bottom: 1px solid rgba(200,169,110,0.1); padding-bottom: 10px;">📋 Customer Callback Details</h3>
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <tr><td style="padding: 6px 0; width: 140px; color: #9A9286;">Ticket ID</td><td style="padding: 6px 0; color: #FAF7F2;">: <strong>#{ticket_id}</strong></td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Name</td><td style="padding: 6px 0; color: #FAF7F2;">: {lead.get('NAME', 'N/A')}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Phone</td><td style="padding: 6px 0; color: #FAF7F2;">: {lead.get('PHONE', 'N/A')}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Email</td><td style="padding: 6px 0; color: #FAF7F2;">: {lead.get('EMAIL', 'N/A')}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Reason</td><td style="padding: 6px 0; color: #FAF7F2;">: {lead.get('REASON', 'N/A')}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Status</td><td style="padding: 6px 0; color: #E8D5A3; font-weight: bold;">: {display_status}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Type / Intent</td><td style="padding: 6px 0; color: #FAF7F2;">: {intent}</td></tr>
                <tr><td style="padding: 6px 0; color: #9A9286;">Prior Ticket ID</td><td style="padding: 6px 0; color: #FAF7F2;">: {prior_ticket or 'N/A'}</td></tr>
            </table>
        </div>
        
        <div style="background-color: rgba(200,169,110,0.05); padding: 15px; border-left: 3px solid #C8A96E; margin-bottom: 25px;">
            <p style="margin: 0 0 8px 0; font-weight: bold; color: #C8A96E; font-size: 13px;">⚠️ Action Required:</p>
            <ul style="margin: 0; padding-left: 20px; font-size: 13px; color: #9A9286;">
                <li style="margin-bottom: 5px;">If a Prior Ticket ID is listed, review interaction history before calling.</li>
                <li>After each interaction, update the pipeline stage and add your notes using the button below.</li>
            </ul>
        </div>

        <div style="text-align: center; margin: 30px 0; padding: 25px; background-color: #1A1A1A; border: 1px solid rgba(200,169,110,0.2);">
            <p style="color: #9A9286; font-size: 11px; letter-spacing: 2px; text-transform: uppercase; margin: 0 0 15px 0;">Update Pipeline Stage & Add Notes</p>
            <a href="{form_link}"
               style="display: inline-block; padding: 14px 32px; background-color: #C8A96E; color: #0E0E0E; font-weight: 700; text-decoration: none; font-size: 14px; letter-spacing: 1px;">
                📋 Update Ticket #{ticket_id}
            </a>
            <p style="color: #9A9286; font-size: 11px; margin: 12px 0 0 0;">Ticket ID is pre-filled — just select the stage and add your notes.</p>
        </div>
        
        <p style="font-size: 12px; color: #9A9286;">Regards,<br>Automated Dispatch System<br>AdiShila</p>
    </div>
    """
    send_brevo_email(TEAM_EMAIL, subject, body)

def send_batch_team_notification(leads: list) -> bool:
    if not leads:
        return True
        
    date_str = datetime.now().strftime("%d-%b-%Y")
    total_count = len(leads)
    
    subject = f"Daily Callback Assignment | {total_count} Tickets | {date_str}"
    
    data_rows = ""
    for row in leads:
        ticket_id = row.get('ticket_id', '')
        form_link = f"{FORM_BASE_URL}{ticket_id}"
        data_rows += f"""
        <tr style="border-bottom: 1px solid #3D3D3D;">
          <td style="padding: 12px 8px; color: #C8A96E; white-space: nowrap;">#{ticket_id}</td>
          <td style="padding: 12px 8px; color: #FAF7F2;">{row.get('name', 'N/A')}</td>
          <td style="padding: 12px 8px; color: #FAF7F2; white-space: nowrap;">{row.get('phone', 'N/A')}</td>
          <td style="padding: 12px 8px; color: #FAF7F2;">{row.get('email', 'N/A')}</td>
          <td style="padding: 12px 8px; color: #9A9286; font-size: 13px;">{row.get('reason', 'N/A')}</td>
          <td style="padding: 12px 8px; color: #E8D5A3;">{row.get('status', '')}</td>
          <td style="padding: 12px 8px; color: #FAF7F2;">{row.get('intent', 'Query')}</td>
          <td style="padding: 12px 8px; color: #FAF7F2;">{row.get('language', 'N/A')}</td>
          <td style="padding: 12px 8px; color: #9A9286; white-space: nowrap;">{row.get('previous_ticket') or 'N/A'}</td>
          <td style="padding: 12px 8px; text-align: center;">
            <a href="{form_link}"
               style="display: inline-block; padding: 6px 14px; background-color: #C8A96E; color: #0E0E0E; font-weight: 700; text-decoration: none; font-size: 11px; white-space: nowrap;">
              📋 Update
            </a>
          </td>
        </tr>
        """

    body = f"""
    <div style="font-family: 'Inter', Arial, sans-serif; background-color: #0E0E0E; color: #FAF7F2; line-height: 1.5; padding: 30px;">
        
        <div style="margin-bottom: 30px; padding-bottom: 15px; border-bottom: 1px solid rgba(200,169,110,0.2);">
            <h1 style="font-family: 'Cormorant Garamond', Georgia, serif; color: #C8A96E; margin: 0; font-size: 24px; font-weight: normal;">AdiShila Batch Dispatch</h1>
            <p style="color: #9A9286; font-size: 13px; margin-top: 5px;">Date: {date_str} | Total Assigned: <strong style="color: #C8A96E;">{total_count}</strong></p>
        </div>

        <p style="color: #E8E4DC;">Dear Resolution Team,</p>
        <p style="color: #9A9286; font-size: 14px;">Please find below the callback requests assigned to your queue for today. All customers must be contacted within 48 hours of their respective ticket creation time.</p>
        <p style="color: #9A9286; font-size: 14px;">After each interaction, click <strong style="color: #C8A96E;">📋 Update</strong> on the relevant row to update the pipeline stage and add your notes. The Ticket ID is pre-filled — no sheet access required.</p>
        
        <div style="overflow-x: auto; margin-top: 30px;">
            <table style="width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; min-width: 1000px; background-color: #1A1A1A; border: 1px solid #3D3D3D;">
                <thead>
                    <tr style="background-color: rgba(200,169,110,0.1); border-bottom: 2px solid #C8A96E;">
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Ticket ID</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Name</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Phone</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Email</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Reason</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Status</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Intent</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Language</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px;">Prior Ticket</th>
                        <th style="padding: 12px 8px; color: #C8A96E; font-weight: normal; letter-spacing: 1px; text-transform: uppercase; font-size: 11px; text-align: center;">Update CRM</th>
                    </tr>
                </thead>
                <tbody>{data_rows}</tbody>
            </table>
        </div>
        
        <p style="margin-top: 30px; font-size: 12px; color: #9A9286;">Regards,<br>Automated Dispatch System<br>AdiShila</p>
    </div>
    """
    logger.info(f"Dispatching batch email via API for {total_count} leads.")
    return send_brevo_email(TEAM_EMAIL, subject, body)

# ============================================================================
# WEBHOOK SERVER & ENDPOINTS
# ============================================================================

_flask_app = Flask(__name__)

def verify_signature(body: bytes, sig_header: str) -> bool:
    if not WEBHOOK_SECRET: return True
    expected = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_header or "")

@_flask_app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mode": "google_sheets_batching_reorders"}), 200

# --- NEW PING ROUTE ADDED HERE ---
@_flask_app.route("/ping", methods=["GET", "HEAD"])
def ping():
    """
    Extremely lightweight endpoint designed specifically for external cron services 
    to hit in order to keep the server awake. Returns a tiny payload to avoid 
    'output too large' errors.
    """
    return "OK", 200

@_flask_app.route("/webhook/lead", methods=["POST"])
def webhook_lead():
    if not verify_signature(request.data, request.headers.get("X-Hub-Signature-256", "")):
        return jsonify({"error": "invalid signature"}), 401

    payload = request.get_json(force=True, silent=True) or {}
    
    # Flatten format
    lead_data = {k: v for k, v in payload.items() if k != "callback_data"}
    lead_data.update(payload.get("callback_data") or {})
    lead_data = {k.upper(): v for k, v in lead_data.items()}
    
    # 1. Missing Ticket ID fallback
    original_ticket_id = str(lead_data.get("TICKET_ID", "")).strip()
    ticket_id = original_ticket_id
    
    if not ticket_id:
        ticket_id = generate_fallback_ticket_id()
        lead_data["TICKET_ID"] = ticket_id
        logger.info(f"Generated fallback Ticket ID: {ticket_id}")

    # 2. Determine Contactability and Payload Type
    phone_val = str(lead_data.get("PHONE", "")).strip()
    email_val = str(lead_data.get("EMAIL", "")).strip()
    has_contact = bool(phone_val or email_val)
    
    has_metrics = bool(lead_data.get("CSAT") or lead_data.get("FRUSTRATION_SCORE"))
    is_metrics_only = has_metrics and not has_contact

    # 3. Connect to sheet and process updates vs. new
    try:
        sheet = _get_sheet()
    except Exception as e:
        logger.error(f"Failed to connect to sheet during initial check: {e}")
        sheet = None

    if sheet and original_ticket_id:
        try:
            cell = sheet.find(original_ticket_id, in_column=COL_TICKET)
            # If we get here, the TICKET ALREADY EXISTS in the CRM.
            
            if has_metrics:
                # Silently append the CSAT score to the existing row
                csat = lead_data.get("CSAT")
                frust = lead_data.get("FRUSTRATION_SCORE")
                
                if csat: 
                    sheet.update_cell(cell.row, COL_CSAT, csat)
                    logger.info(f"Silently updated CSAT ({csat}) for existing Ticket #{original_ticket_id}")
                if frust: 
                    sheet.update_cell(cell.row, COL_FRUSTRATION, frust)
                    logger.info(f"Silently updated Frustration Score ({frust}) for existing Ticket #{original_ticket_id}")

            if is_metrics_only:
                # Mission accomplished. It was just a CSAT score, we updated it. Stop here.
                return jsonify({"status": "updated_existing", "message": "Metrics appended to existing row."}), 200
            else:
                # It's a full callback request, but the ID already exists. 
                # This means it's a TatvaBot retry (or a refresh glitch). Block it.
                logger.warning(f"Blocked webhook retry: Ticket #{original_ticket_id} is already in the CRM.")
                return jsonify({"status": "ignored", "reason": "duplicate_webhook"}), 200
                
        except Exception:
            # 'find' throws an exception if the cell is not found. 
            # This means it's a completely new ticket. Proceed normally.
            pass

    # 4. If we get here, the ticket is NEW (either generated, or provided but not in CRM).
    
    if is_metrics_only:
        # It's an orphan CSAT score (no contact info, no existing ticket).
        # We log it directly to the CRM and EXIT immediately so no emails are sent.
        append_to_google_sheet(lead_data, "", "CSAT Logged", "CSAT Only (No Contact)")
        return jsonify({"status": "csat_logged_silently", "message": "New row created for orphan metrics."}), 200

    # 5. Process normal Callback Request (Determine Intent & Status)
    reason = str(lead_data.get("REASON", "")).lower()
    intent = "Immediate" if any(kw in reason for kw in IMMEDIATE_KEYWORDS) else "Query"

    prior_ticket = ""
    if sheet:
        prior_ticket = check_for_duplicate(sheet, phone_val, email_val)

    if not has_contact:
        crm_status = "Unreachable"
        email_status = "Unreachable"
        logger.info(f"Ticket #{ticket_id} is unreachable. CRM logged, emails bypassed.")
    elif intent == "Immediate":
        if prior_ticket:
            crm_status = "Notified (Reorder)"
            email_status = "Reorder (High Priority)"
        else:
            crm_status = "Notified (Immediate)"
            email_status = "New (High Priority)"
    else:
        # intent == "Query"
        crm_status = "Recurring" if prior_ticket else "New"
        email_status = crm_status

    # 6. Save new lead to CRM
    append_to_google_sheet(lead_data, prior_ticket, intent, crm_status)

    # 7. Send Emails Strict Logic
    valid_email = bool(email_val)
    valid_ticket = bool(lead_data.get("TICKET_ID"))

    if valid_email and valid_ticket:
        send_customer_confirmation(lead_data)
        
    if intent == "Immediate" and has_contact:
        send_immediate_team_notification(lead_data, prior_ticket, intent, email_status)

    return jsonify({"status": "success", "intent": intent, "contactable": has_contact, "crm_status": crm_status}), 200

@_flask_app.route("/cron/batch", methods=["GET", "POST"])
def process_batches():
    """
    Endpoint triggered by an external cron service.
    Finds all un-notified leads in Google Sheets, sends a batch email, 
    and updates their status to 'Notified'.
    Ghost leads ('Unreachable') are intentionally ignored.
    """
    provided_key = request.args.get("key")
    if WEBHOOK_SECRET and provided_key != WEBHOOK_SECRET:
        return jsonify({"error": "Unauthorized"}), 401

    try:
        sheet = _get_sheet()
        records = sheet.get_all_records()
        
        pending_leads = []
        rows_to_update = []
        
        # Row 1 is headers, so data starts at Row 2
        for idx, row in enumerate(records, start=2):
            status = str(row.get("Status", row.get("STATUS", "")))
            
            # Grabs only standard priority items waiting for batching
            if status in ["New", "Recurring"]:
                pending_leads.append({
                    "ticket_id": row.get("Ticket ID", row.get("TICKET_ID", "")),
                    "name": row.get("Name", row.get("NAME", "")),
                    "phone": row.get("Phone", row.get("PHONE", "")),
                    "email": row.get("Email", row.get("EMAIL", "")),
                    "reason": row.get("Reason", row.get("REASON", "")),
                    "status": status,
                    "intent": row.get("Intent", row.get("INTENT", "Query")),
                    "language": row.get("Language", row.get("LANGUAGE", "en")),
                    "previous_ticket": row.get("Prior Ticket ID", row.get("PRIOR_TKT", ""))
                })
                rows_to_update.append(idx)

        # STRICT ZERO-REQUEST SAFEGUARD
        if not pending_leads:
            logger.info("Batch dispatcher ran: 0 new leads. Skipping team email.")
            return jsonify({
                "status": "no_pending_leads", 
                "message": "0 requests found, team not pinged"
            }), 200

        # Send the massive table email
        email_success = send_batch_team_notification(pending_leads)
        
        if email_success:
            for row_idx in rows_to_update:
                sheet.update_cell(row_idx, COL_STATUS, "Notified")
            
            logger.info(f"Successfully processed batch of {len(pending_leads)} leads.")
            return jsonify({"status": "batch_sent", "count": len(pending_leads)}), 200
        else:
            return jsonify({"error": "Failed to send batch email"}), 500

    except Exception as e:
        logger.error(f"Batch dispatch failed: {e}")
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.getenv("PORT", "8000"))
    _flask_app.run(host="0.0.0.0", port=port, debug=False)
