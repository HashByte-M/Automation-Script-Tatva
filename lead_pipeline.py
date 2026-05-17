"""
lead_pipeline.py — Stateless Google Sheets CRM with Smart Batching & Reorder Logic

This script handles webhooks, logs to Google Sheets, sends immediate emails for 
urgent leads (including high-priority returning customers), and holds low-priority 
leads. It safely ignores and silences any leads that have no contact information.
"""

import hmac
import hashlib
import logging
import urllib.request
import json
import os
import gspread
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
SENDER_EMAIL   = os.getenv("SENDER_EMAIL", "noreply@company.com")
TEAM_EMAIL     = os.getenv("TEAM_EMAIL", "team@company.com")
SUPPORT_PHONE  = "+91 86301 79867"

# Google Sheets Configuration
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS")
GOOGLE_SHEET_ID         = os.getenv("GOOGLE_SHEET_ID")

# Keywords for classification
IMMEDIATE_KEYWORDS = ["buy", "purchase", "order", "price", "pricing", "quote", "wholesale", "urgent"]

# Google Sheet Column Mapping (1-indexed for gspread)
COL_DATE        = 1
COL_TICKET      = 2
COL_NAME        = 3
COL_PHONE       = 4
COL_EMAIL       = 5
COL_REASON      = 6
COL_CSAT        = 7
COL_LANGUAGE    = 8
COL_FRUSTRATION = 9
COL_STATUS      = 10
COL_PRIOR_TKT   = 11
COL_INTENT      = 12  # Tracks if it's Immediate or Query

# ============================================================================
# GOOGLE SHEETS INTEGRATION (THE CRM)
# ============================================================================

def _get_sheet():
    """Authenticates and returns the main worksheet."""
    if not GOOGLE_CREDENTIALS_JSON or not GOOGLE_SHEET_ID:
        raise ValueError("Missing Google Sheets credentials.")
    creds_dict = json.loads(GOOGLE_CREDENTIALS_JSON)
    gc = gspread.service_account_from_dict(creds_dict)
    return gc.open_by_key(GOOGLE_SHEET_ID).sheet1

def check_for_duplicate(sheet, phone: str, email: str) -> str:
    """Returns the previous Ticket ID if a duplicate exists, else empty string."""
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
        logger.error(f"Error checking for duplicates: {e}")
    return ""

def update_csat_in_sheet(ticket_id: str, csat_score: float) -> bool:
    try:
        sheet = _get_sheet()
        cell = sheet.find(ticket_id, in_column=COL_TICKET)
        if cell:
            sheet.update_cell(cell.row, COL_CSAT, csat_score)
            logger.info(f"Successfully updated CSAT ({csat_score}) for Ticket #{ticket_id}")
            return True
    except Exception as e:
        logger.error(f"Failed to update CSAT in sheet: {e}")
    return False

def append_to_google_sheet(lead_data: dict, prior_ticket: str, intent: str, status: str) -> None:
    try:
        sheet = _get_sheet()
        row = [
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            lead_data.get("TICKET_ID", "N/A"),
            lead_data.get("NAME", ""),
            lead_data.get("PHONE", ""),
            lead_data.get("EMAIL", ""),
            lead_data.get("REASON", ""),
            lead_data.get("CSAT", ""),
            lead_data.get("LANGUAGE", "en"),
            lead_data.get("FRUSTRATION_SCORE", ""),
            status,
            prior_ticket,
            intent
        ]
        sheet.append_row(row)
        logger.info(f"Added Ticket #{lead_data.get('TICKET_ID')} to CRM. Status: {status}")
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
    if not email: return
    
    ticket_id = lead.get("TICKET_ID", "N/A")
    name = lead.get("NAME", "Customer")
    phone = lead.get("PHONE", "your registered contact number")
    date_str = datetime.now().strftime("%B %d, %Y")
    
    subject = f"We've Received Your Callback Request – Ticket #{ticket_id}"
    body = f"""
    <div style="font-family: Arial, sans-serif; color: #333; line-height: 1.6; max-width: 600px; margin: 0 auto;">
        <p>Dear {name},</p>
        <p>Thank you for reaching out to Adishila! 🙏</p>
        <p>We have successfully received your callback request, and we want you to know that your query is important to us.</p>
        
        <div style="border-top: 2px solid #ddd; border-bottom: 2px solid #ddd; padding: 15px 0; margin: 25px 0;">
            <h3 style="margin: 0 0 15px 0; font-size: 16px; font-weight: bold;">📋 Your Ticket Details</h3>
            <table style="width: 100%; border-collapse: collapse;">
                <tr><td style="padding: 4px 0; width: 120px; color: #555;">Ticket ID</td><td style="padding: 4px 0; font-weight: bold;">: #{ticket_id}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Request Date</td><td style="padding: 4px 0; font-weight: bold;">: {date_str}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Status</td><td style="padding: 4px 0; font-weight: bold;">: Under Review</td></tr>
            </table>
        </div>
        
        <p>Our dedicated support team will get in touch with you within the next 48 hours on your registered contact number: <strong>{phone}</strong> or email.</p>
        <p>In the meantime, if you have any additional information to share or wish to update your query, feel free to reply to this email quoting your Ticket ID.</p>
        <p>We appreciate your patience and look forward to assisting you!</p>
        
        <p style="margin-top: 30px;">
            Warm regards,<br><strong>Customer Support Team</strong><br>Adishila.in<br>
            📧 support@adishila.in<br>🌐 www.adishila.in<br>📞 {SUPPORT_PHONE}
        </p>
    </div>
    """
    send_brevo_email(email, subject, body)

def send_immediate_team_notification(lead: dict, prior_ticket: str, intent: str, display_status: str):
    ticket_id = lead.get("TICKET_ID", "N/A")
    date_str = datetime.now().strftime("%d-%b-%Y")
    
    # Highlight the subject line if it's a reorder to grab attention immediately
    subject_prefix = "REORDER" if "Reorder" in display_status else "Callback Assignment"
    subject = f"{subject_prefix} | Ticket #{ticket_id} | {intent} | {date_str}"
    
    body = f"""
    <div style="font-family: Arial, sans-serif; color: #333; line-height: 1.5; max-width: 650px;">
        <p>Dear Resolution Team,</p>
        <p>A new callback request has been assigned to your queue. Please find the customer details below and ensure contact is made within 48 hours.</p>
        
        <div style="background-color: #fcfcfc; border: 1px solid #ddd; padding: 20px; margin: 20px 0;">
            <h3 style="margin: 0 0 15px 0; font-size: 15px; font-weight: bold; border-bottom: 1px solid #ccc; padding-bottom: 10px;">📋 CUSTOMER CALLBACK DETAILS</h3>
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <tr><td style="padding: 4px 0; width: 140px; color: #555;">Ticket ID</td><td style="padding: 4px 0;">: <strong>#{ticket_id}</strong></td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Name</td><td style="padding: 4px 0;">: {lead.get('NAME', 'N/A')}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Phone</td><td style="padding: 4px 0;">: {lead.get('PHONE', 'N/A')}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Email</td><td style="padding: 4px 0;">: {lead.get('EMAIL', 'N/A')}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Reason</td><td style="padding: 4px 0;">: {lead.get('REASON', 'N/A')}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Status</td><td style="padding: 4px 0; color: #d35400; font-weight: bold;">: {display_status}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Type / Intent</td><td style="padding: 4px 0;">: {intent}</td></tr>
                <tr><td style="padding: 4px 0; color: #555;">Prior Ticket ID</td><td style="padding: 4px 0;">: {prior_ticket or 'N/A'}</td></tr>
            </table>
        </div>
        
        <p style="margin: 10px 0 5px 0; font-weight: bold;">⚠️ Action Required:</p>
        <ul style="margin-top: 0; padding-left: 20px;">
            <li style="margin-bottom: 5px;">If a Prior Ticket ID is listed, please review the previous interaction history before reaching out.</li>
            <li style="margin-bottom: 5px;">Update the ticket status in the Google Sheets CRM after every interaction.</li>
        </ul>
        <p>Regards,<br>Automated Dispatch System<br>Adishila.in</p>
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
        data_rows += f"""
        <tr>
          <td style="padding: 8px; border: 1px solid #ddd; white-space: nowrap;">#{row.get('ticket_id', '')}</td>
          <td style="padding: 8px; border: 1px solid #ddd;">{row.get('name', 'N/A')}</td>
          <td style="padding: 8px; border: 1px solid #ddd; white-space: nowrap;">{row.get('phone', 'N/A')}</td>
          <td style="padding: 8px; border: 1px solid #ddd;">{row.get('email', 'N/A')}</td>
          <td style="padding: 8px; border: 1px solid #ddd;">{row.get('reason', 'N/A')}</td>
          <td style="padding: 8px; border: 1px solid #ddd;">{row.get('status', '')}</td>
          <td style="padding: 8px; border: 1px solid #ddd;">{row.get('intent', 'Query')}</td>
          <td style="padding: 8px; border: 1px solid #ddd;">{row.get('language', 'N/A')}</td>
          <td style="padding: 8px; border: 1px solid #ddd; white-space: nowrap;">{row.get('previous_ticket') or 'N/A'}</td>
        </tr>
        """

    body = f"""
    <div style="font-family: Arial, sans-serif; color: #333; line-height: 1.5; max-width: 95%;">
        <p>Dear Resolution Team,</p>
        <p>Please find below the callback requests assigned to your queue for {date_str}. All customers must be contacted within 48 hours of their respective ticket creation time.</p>
        
        <div style="background-color: #fcfcfc; border: 1px solid #ddd; padding: 20px; margin: 20px 0; max-width: 400px;">
            <h3 style="margin: 0 0 15px 0; font-size: 15px; font-weight: bold; border-bottom: 1px solid #ccc; padding-bottom: 10px;">📊 ASSIGNMENT SUMMARY</h3>
            <table style="width: 100%; border-collapse: collapse; font-size: 14px;">
                <tr><td style="padding: 4px 0; color: #555;">Total New Tickets</td><td style="padding: 4px 0;">: <strong>{total_count}</strong></td></tr>
            </table>
        </div>
        
        <h3 style="margin: 25px 0 10px 0; font-size: 15px;">📋 CUSTOMER CALLBACK TABLE</h3>
        <div style="overflow-x: auto;">
            <table style="width: 100%; border-collapse: collapse; font-size: 13px; text-align: left; min-width: 900px;">
                <thead>
                    <tr style="background-color: #f5f5f5;">
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Ticket ID</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Name</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Phone</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Email</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Reason</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Status</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Intent</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Language</th>
                        <th style="padding: 10px 8px; border: 1px solid #ddd;">Prior Ticket</th>
                    </tr>
                </thead>
                <tbody>{data_rows}</tbody>
            </table>
        </div>
        <p>Regards,<br>Automated Dispatch System<br>Adishila.in</p>
    </div>
    """
    logger.info(f"Dispatching batch email via API for {total_count} leads.")
    return send_brevo_email(TEAM_EMAIL, subject, body)

# ============================================================================
# WEBHOOK SERVER & ENDPOINTS
# ============================================================================

app = Flask(__name__)

def verify_signature(body: bytes, sig_header: str) -> bool:
    if not WEBHOOK_SECRET: return True
    expected = "sha256=" + hmac.new(WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, sig_header or "")

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "mode": "google_sheets_batching_reorders"}), 200

@app.route("/webhook/lead", methods=["POST"])
def webhook_lead():
    if not verify_signature(request.data, request.headers.get("X-Hub-Signature-256", "")):
        return jsonify({"error": "invalid signature"}), 401

    payload = request.get_json(force=True, silent=True) or {}
    
    # Flatten format
    lead_data = {k: v for k, v in payload.items() if k != "callback_data"}
    lead_data.update(payload.get("callback_data") or {})
    lead_data = {k.upper(): v for k, v in lead_data.items()}
    
    ticket_id = lead_data.get("TICKET_ID")
    
    # Check if this is just a CSAT update coming 10 seconds later
    is_csat_only = lead_data.get("CSAT") is not None and not any([lead_data.get("NAME"), lead_data.get("PHONE"), lead_data.get("EMAIL")])
    if is_csat_only:
        update_csat_in_sheet(ticket_id, lead_data.get("CSAT"))
        return jsonify({"status": "csat_updated"}), 200

    # 1. Check Contactability (The "Ghost Lead" Check)
    has_contact = bool(lead_data.get("PHONE") or lead_data.get("EMAIL"))

    # 2. Determine Intent
    reason = str(lead_data.get("REASON", "")).lower()
    intent = "Immediate" if any(kw in reason for kw in IMMEDIATE_KEYWORDS) else "Query"

    # 3. Check Deduplication
    try:
        sheet = _get_sheet()
        prior_ticket = check_for_duplicate(sheet, lead_data.get("PHONE"), lead_data.get("EMAIL"))
    except Exception:
        prior_ticket = ""

    # 4. Determine Dynamic Statuses
    # We separate 'crm_status' (how the batcher reads it) from 'email_status' (what humans read).
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

    # 5. Save to CRM (All leads get logged, regardless of contactability)
    append_to_google_sheet(lead_data, prior_ticket, intent, crm_status)

    # 6. Send Emails (Only if contactable)
    if lead_data.get("EMAIL"):
        send_customer_confirmation(lead_data)
        
    if intent == "Immediate" and has_contact:
        send_immediate_team_notification(lead_data, prior_ticket, intent, email_status)

    return jsonify({"status": "success", "intent": intent, "contactable": has_contact, "crm_status": crm_status}), 200

@app.route("/cron/batch", methods=["GET", "POST"])
def process_batches():
    """
    Endpoint triggered by an external cron service.
    Finds all un-notified leads in Google Sheets, sends a batch email, 
    and updates their status to 'Notified'.
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
    app.run(host="0.0.0.0", port=port, debug=False)