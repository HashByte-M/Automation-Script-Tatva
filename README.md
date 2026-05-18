# 🚀 Lead Pipeline Automation

> **A stateless, serverless-friendly CRM engine** — turns raw webhook payloads from any chatbot or form into a fully-logged Google Sheets CRM, rich branded emails, smart deduplication, reorder detection, CSAT tracking, and a daily batch digest. Zero database. Zero message queue. Just Python, a spreadsheet, and an API call.

---

## 📖 Table of Contents

- [✨ What This Project Does](#-what-this-project-does)
- [🗺️ Automation Workflow](#️-automation-workflow)
- [🏗️ Architecture Overview](#️-architecture-overview)
- [📂 File Structure](#-file-structure)
- [⚙️ Configuration & Environment Variables](#️-configuration--environment-variables)
- [🔑 How Security Works — HMAC Signature Verification](#-how-security-works--hmac-signature-verification)
- [📊 Google Sheets as a CRM](#-google-sheets-as-a-crm)
- [🎫 Ticket ID System](#-ticket-id-system)
- [🧠 Smart Deduplication & Reorder Detection](#-smart-deduplication--reorder-detection)
- [🔢 CSAT & Frustration Score Handling](#-csat--frustration-score-handling)
- [🎯 Intent Classification](#-intent-classification)
- [📧 Email System — Brevo Integration](#-email-system--brevo-integration)
  - [Customer Confirmation Email](#-customer-confirmation-email)
  - [Immediate Team Notification](#-immediate-team-notification-urgent--reorder)
  - [Daily Batch Digest](#-daily-batch-digest)
- [🌐 API Endpoints](#-api-endpoints)
- [📬 Webhook Payload Format](#-webhook-payload-format)
- [🔄 Lead Status Reference](#-lead-status-reference)
- [🚀 Deployment](#-deployment)
- [🔒 Strict Email Rules — Why They Exist](#-strict-email-rules--why-they-exist)
- [🧪 Local Development](#-local-development)
- [📦 Dependencies](#-dependencies)
- [🪲 Logging](#-logging)
- [❓ FAQ](#-faq)

---

## ✨ What This Project Does

Businesses that receive customer leads through a chatbot, contact form, or any webhook-capable tool need a reliable way to capture, classify, and act on those leads — without paying for a full CRM platform or maintaining a database.

This pipeline **automatically**:

1. 🔐 **Verifies** every incoming webhook with HMAC-SHA256 — fake or tampered payloads are rejected immediately with a `401`.
2. 🗃️ **Logs** the lead into a Google Sheet acting as a lightweight CRM — no database required.
3. 🔁 **Deduplicates** — if the same phone/email is seen again, it's flagged as a reorder or returning customer.
4. 🤫 **Silently** routes CSAT and frustration scores onto the existing CRM row without triggering any emails.
5. 🧠 **Classifies** intent — is this lead ready to transact (`Immediate`) or just asking a question (`Query`)?
6. 📧 **Sends branded emails** — a confirmation to the customer, and an urgent alert to the team for hot leads.
7. ⏰ **Batches** standard-priority leads into a single daily digest email triggered by an external cron job.

It is designed to be **dropped into any business** with minimal configuration — just point it at your own Google Sheet, Brevo account, and webhook source.

---

## 🗺️ Automation Workflow

The diagram below (included as `Automation_Workflow.svg`) visualises every decision branch in the pipeline:

```
Webhook POST ──► HMAC Verify ──► Parse Payload ──► Detect Flags
                    │                                     │
                 401 Reject                     ┌─────────┼──────────┐
                                           has_contact  has_metrics  is_metrics_only
                                                │
                                   ┌────────────▼────────────┐
                                   │  CRM Lookup (ticket_id) │
                                   └────────┬────────────────┘
                                        Found?
                                   Yes ──────────────────► Update CSAT silently → 200
                                   No  ──────────────────► New Lead Flow
                                                                │
                                                    ┌───────────▼───────────┐
                                                    │  Duplicate Check      │
                                                    │  (phone / email scan) │
                                                    └───────────┬───────────┘
                                                           prior_ticket?
                                                        Yes ────────── Reorder
                                                        No  ────────── New
                                                                │
                                                    ┌───────────▼──────────────┐
                                                    │  Intent Classification   │
                                                    │  Immediate vs Query      │
                                                    └───────────┬──────────────┘
                                                                │
                                                    ┌───────────▼──────────────┐
                                                    │  Append to Google Sheet  │
                                                    └───────────┬──────────────┘
                                                                │
                                                     ┌──────────▼──────────┐
                                                     │    Email Routing    │
                                                     │  Customer + Team    │
                                                     └─────────────────────┘

Cron ──► /cron/batch ──► Scan Sheet for "New"/"Recurring" ──► Batch Email ──► Mark "Notified"
```

---

## 🏗️ Architecture Overview

```
┌──────────────────────────────────────────────────────────────┐
│          Any Webhook Source (chatbot, form, CRM trigger)     │
│             Fires POST /webhook/lead on customer action      │
└─────────────────────────┬────────────────────────────────────┘
                          │  HTTPS + X-Hub-Signature-256
                          ▼
┌──────────────────────────────────────────────────────────────┐
│               Flask App  (lead_pipeline.py)                  │
│                                                              │
│   ┌──────────────┐   ┌───────────────┐   ┌───────────────┐  │
│   │ HMAC Verify  │──►│ Payload Parse │──►│ CRM Lookup    │  │
│   └──────────────┘   └───────────────┘   │ gspread API   │  │
│                                          └───────┬───────┘  │
│                                                  │          │
│   ┌──────────────────────────────────────────────▼───────┐  │
│   │              Lead Processing Engine                  │  │
│   │  Deduplication · Intent · Status · Email Routing     │  │
│   └──────┬──────────────────┬────────────────────────────┘  │
│          │                  │                               │
│          ▼                  ▼                               │
│   ┌─────────────┐   ┌──────────────┐                       │
│   │ Google Sheet│   │ Brevo Email  │                       │
│   │   (CRM)     │   │   API        │                       │
│   └─────────────┘   └──────────────┘                       │
│                                                              │
│   ┌──────────────────────────────────────────────────────┐  │
│   │  GET/POST /cron/batch  (triggered by external cron)  │  │
│   │  Scans sheet → batch email → marks "Notified"        │  │
│   └──────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────┘
```

**Why this stack?**

- **Flask** — lightweight, runs anywhere, easy to deploy on Railway/Render/Fly.io. No magic, no boilerplate framework overhead.
- **Google Sheets** — the business team already knows it; no database training needed, no extra cost, real-time visibility for non-engineers.
- **Brevo** — generous free tier, excellent deliverability for transactional email, dead-simple REST API callable with only `urllib` (no SDK bloat).
- **`urllib` over `requests`** — zero extra dependency for the HTTP call; keeps the Docker image and install time lean.

---

## 📂 File Structure

```
.
├── lead_pipeline.py        # ← Entire application (single-file by design)
├── Automation_Workflow.svg # ← Visual flowchart of every decision branch
├── requirements.txt        # ← Python dependencies
├── .env.example            # ← Template for environment variables
└── README.md               # ← This file
```

> **Why a single file?** This service has one job. Splitting across many modules adds navigation overhead with no architectural benefit at this scale. Every function is grouped into clearly labelled sections with `# ===` banners for easy orientation.

---

## ⚙️ Configuration & Environment Variables

All secrets and configuration are loaded from the environment. **Never hard-code credentials.**

| Variable | Required | Description |
|---|---|---|
| `WEBHOOK_SECRET` | ✅ Yes | Shared secret for HMAC-SHA256 signature verification. Must match what your webhook source signs with. |
| `EMAIL_API_KEY` | ✅ Yes | Brevo (formerly Sendinblue) API key. Get it from your Brevo dashboard → SMTP & API. |
| `SENDER_EMAIL` | ⬜ Optional | From-address for outbound emails. |
| `TEAM_EMAIL` | ⬜ Optional | Where team notifications and batch digests are sent. |
| `GOOGLE_CREDENTIALS` | ✅ Yes | Full JSON content of your Google service account key file (as a single-line string). |
| `GOOGLE_SHEET_ID` | ✅ Yes | The alphanumeric ID from your Google Sheet URL: `docs.google.com/spreadsheets/d/`**`<THIS_PART>`**`/edit`. |
| `PORT` | ⬜ Optional | Port for Flask to bind to. Defaults to `8000`. Most PaaS platforms set this automatically. |

### 🔧 Setting up Google Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/) → **IAM & Admin → Service Accounts**.
2. Create a new service account and download the **JSON key file**.
3. Share your Google Sheet with the service account email (`...@project.iam.gserviceaccount.com`) as an **Editor**.
4. Paste the entire contents of the JSON key as the value of `GOOGLE_CREDENTIALS`.

```bash
# Example .env file
WEBHOOK_SECRET=your-super-secret-key
EMAIL_API_KEY=xkeysib-xxxxxxxxxxxxxxxxxxxx
SENDER_EMAIL=info@yourbusiness.com
TEAM_EMAIL=team@yourbusiness.com
GOOGLE_CREDENTIALS={"type":"service_account","project_id":"..."}
GOOGLE_SHEET_ID=1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms
PORT=8000
```

---

## 🔑 How Security Works — HMAC Signature Verification

Every webhook request must include a `X-Hub-Signature-256` header. The pipeline verifies it using Python's built-in `hmac` and `hashlib` modules before doing anything else.

```python
def verify_signature(body: bytes, sig_header: str) -> bool:
    if not WEBHOOK_SECRET: return True   # dev mode — no secret set
    expected = "sha256=" + hmac.new(
        WEBHOOK_SECRET.encode("utf-8"), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, sig_header or "")
```

**Why `hmac.compare_digest` instead of `==`?**
Standard string equality (`==`) is vulnerable to [timing attacks](https://en.wikipedia.org/wiki/Timing_attack) — an attacker can infer the correct signature one character at a time by measuring response times. `compare_digest` runs in constant time regardless of how many characters match, eliminating this attack vector entirely.

**Why skip verification when `WEBHOOK_SECRET` is empty?**
In local development you often don't want to compute and attach a signature. Leaving the secret unset disables verification — a deliberate dev convenience that is obvious to fix before going to production.

**What happens on failure?** The request is rejected immediately with `HTTP 401`. No data is read, no sheet is accessed, no email is sent.

---

## 📊 Google Sheets as a CRM

The Google Sheet is the single source of truth. There is no separate database.

### Column Layout

| Col # | Name | What It Stores |
|---|---|---|
| 1 | `DATE` | Timestamp when the row was created (`YYYY-MM-DD HH:MM:SS`) |
| 2 | `TICKET_ID` | Unique ticket identifier (from your webhook source or auto-generated) |
| 3 | `NAME` | Customer name |
| 4 | `PHONE` | Customer phone number |
| 5 | `EMAIL` | Customer email address |
| 6 | `REASON` | Free-text reason for the callback or inquiry |
| 7 | `LANGUAGE` | Language the customer communicated in |
| 8 | `STATUS` | Current ticket status (`New`, `Notified`, `Recurring`, etc.) |
| 9 | `PRIOR_TKT` | Previous ticket ID if this customer is a repeat contact |
| 10 | `CSAT` | Customer satisfaction score (silently appended post-interaction) |
| 11 | `FRUSTRATION` | Frustration score from the chatbot's sentiment analysis |
| 12 | `INTENT` | `Immediate` or `Query` |

### Why Google Sheets (not Postgres, Airtable, Notion, etc.)?

- ✅ **Zero ops** — no connection pooling, no migrations, no backups to configure.
- ✅ **Team-visible** — the support team can filter, sort, and update statuses directly without any custom admin UI.
- ✅ **Free** — at the call volumes of most small-to-mid businesses, the Sheets API quota is never a concern.
- ✅ **Exportable** — one click to CSV for any reporting or analytics tool.

The tradeoff is that Google Sheets is slower than a real database for large datasets and has API rate limits. This is deliberately acceptable for the lead volumes this pipeline targets. For high-volume use cases, the sheet layer can be swapped for any database by replacing the `_get_sheet()` and `append_to_google_sheet()` functions.

---

## 🎫 Ticket ID System

Every lead must have a ticket ID so it can be traced end-to-end across logs, the CRM, and all emails.

### Primary Path
Your webhook source generates and includes a `TICKET_ID` in its payload. This is the expected, normal flow.

### Fallback — Auto-Generated ID
If the webhook source fires without a `TICKET_ID` (e.g., an orphaned CSAT survey response), the pipeline auto-generates one:

```python
def generate_fallback_ticket_id() -> str:
    p1 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    p2 = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"TKT-AUTO-{p1}-{p2}"
    # Example output: TKT-AUTO-X7K2M-9QWA
```

**Why this format?**
- The `TKT-AUTO-` prefix makes auto-generated IDs instantly distinguishable from source-generated ones — a human reviewer immediately knows this was an edge case.
- The alphanumeric suffix provides enough entropy (~36⁹ ≈ 101 billion combinations) to make collisions practically impossible at any realistic scale.

---

## 🧠 Smart Deduplication & Reorder Detection

Every incoming lead is checked against the full CRM history by phone number **and** email address.

```python
def check_for_duplicate(sheet, phone: str, email: str) -> str:
    records = sheet.get_all_records()
    for row in reversed(records):   # ← reversed: finds the MOST RECENT match
        row_phone = str(row.get("PHONE", ""))
        row_email = str(row.get("EMAIL", ""))
        if (phone and phone in row_phone) or \
           (email and email.lower() == row_email.lower()):
            return str(row.get("TICKET_ID", ""))
    return ""
```

**Why iterate in reverse?** The most recent prior interaction is the most relevant for the support team. Scanning backwards always returns the latest historical ticket, not the oldest.

**What does a match mean?**
- If matched **and** intent is `Immediate` → status becomes `Notified (Reorder)` → team is alerted immediately.
- If matched **and** intent is `Query` → status becomes `Recurring` → included in next batch digest.
- The prior ticket ID is logged in column 9 so the agent has full context before contacting the customer.

**Why check both phone AND email with `OR` logic?** Customers often use a different email than before but keep the same phone number, and vice versa. Matching on either maximises the chance of correctly linking interactions from the same person.

---

## 🔢 CSAT & Frustration Score Handling

After a support interaction is resolved, the webhook source may fire a second payload for the same ticket carrying a satisfaction score (`CSAT`) and/or a frustration score (`FRUSTRATION_SCORE`).

### The Decision Tree

```
Incoming webhook with ticket_id X
    │
    ├─ Ticket X already exists in sheet?
    │   YES ──► Update CSAT / Frustration columns silently
    │            • No new row created
    │            • No emails sent
    │            • Return 200 immediately
    │
    └─ Ticket X not in sheet AND no contact info?
        ──► Create a new row with status "CSAT Only (No Contact)"
             • No emails sent (nothing to send to)
```

**Why silent updates?** CSAT scores are internal performance data. Sending the customer another email because their score was recorded would be confusing. Sending the team another alert for a ticket already in their queue would be noise.

**Why log orphan CSAT rows?** Even without contact info, the score is valid data. It goes into the sheet so analysts can track satisfaction trends across all interactions, including anonymous ones.

---

## 🎯 Intent Classification

Every new lead's `REASON` field is scanned for keywords that indicate high purchase or urgency intent:

```python
IMMEDIATE_KEYWORDS = [
    "buy", "purchase", "order", "price", "pricing",
    "quote", "wholesale", "urgent"
]

intent = "Immediate" if any(kw in reason for kw in IMMEDIATE_KEYWORDS) else "Query"
```

> 💡 **Customise this list** for your business domain. A SaaS company might add `"demo"`, `"trial"`, `"enterprise"`. A service business might add `"appointment"`, `"booking"`, `"contract"`.

| Intent | What It Means | Action Taken |
|---|---|---|
| `Immediate` | Customer is ready to transact or has an urgent need | Team notified **right now** via individual priority email |
| `Query` | Customer has a general question or information need | Ticket queued for the **daily batch email** |

**Why this distinction matters?** A customer saying "I need a pricing quote for an enterprise plan" should not wait until tomorrow's batch. The pipeline fast-tracks them to the team within minutes. A customer asking "How does your product work?" can reasonably wait for the next working day — reducing alert fatigue for the team.

---

## 📧 Email System — Brevo Integration

All emails are sent via the [Brevo transactional email API](https://developers.brevo.com/reference/sendtransactemail) using raw `urllib` — no SDK required.

```python
def send_brevo_email(to_email: str, subject: str, html_content: str) -> bool:
    payload = {
        "sender": {"email": SENDER_EMAIL, "name": "Your Business Support"},
        "to": [{"email": to_email}],
        "subject": subject,
        "htmlContent": html_content
    }
    # POST to https://api.brevo.com/v3/smtp/email
```

**Why `urllib` instead of `requests`?** The `requests` library is convenient but is an external dependency that must be installed, version-pinned, and maintained. For a single POST call, Python's built-in `urllib.request` does exactly the same job with zero additional packages.

---

### 📬 Customer Confirmation Email

Sent to the customer when **both** conditions are met:
- ✅ The lead has a **valid email address**
- ✅ The lead has a **valid Ticket ID** (not `N/A`)

**What it contains:**
- Personalised greeting using the customer's name
- Confirmation that their request was received
- Ticket ID, request date, and current status
- Estimated callback or response timeframe
- Your support contact details

**Why this email?** It sets expectations. Customers who receive a confirmation with a ticket number are far less likely to follow up asking whether their request was received — reducing duplicate load on the support team.

**Why the strict email + ticket ID gate?** A confirmation with no ticket number gives the customer nothing to reference in follow-up. Both conditions must be met for the email to have any real value.

---

### 🚨 Immediate Team Notification (Urgent & Reorder)

Sent to `TEAM_EMAIL` when intent is `Immediate` **and** the customer has at least one contact method.

```
Subject: URGENT ASSIGNMENT | Ticket #TKT-12345 | Immediate | 18-May-2026
```
or, for returning customers:
```
Subject: REORDER | Ticket #TKT-12345 | Immediate | 18-May-2026
```

**What it contains:**
- Full customer details (name, phone, email, reason)
- Ticket status and intent classification
- Prior Ticket ID (if a reorder) — so the agent reviews interaction history before contacting
- Explicit action checklist

**Why send immediately for `Immediate` and batch for `Query`?** If every ticket triggered an individual email, agents would tune it out. By routing only hot leads to immediate alerts and bundling the rest, the signal-to-noise ratio stays high and urgent customers get faster responses.

---

### 📅 Daily Batch Digest

Triggered by an HTTP call to `GET /cron/batch` — scheduled by an external cron service (e.g., cron-job.org, GitHub Actions schedule, Render cron jobs).

**What it does, step by step:**

1. Reads all rows from the Google Sheet.
2. Collects every row with status `New` or `Recurring`.
3. If there are **zero** such rows → returns `200` immediately. **No email is sent** (zero-email safeguard).
4. Builds a rich HTML table containing all pending leads.
5. Sends **one** email to `TEAM_EMAIL` with the full table.
6. Updates every processed row's status from `New`/`Recurring` to `Notified`.

**Why update status after the email, not before?** If the Brevo API call fails, the rows remain `New` and will be picked up by the next cron run. Updating first would mark them `Notified` even if the team never saw them.

**Why the zero-email safeguard?** On quiet days there may be no new leads. Sending an empty table trains the team to ignore the digest. Every digest sent should require action.

**Securing the cron endpoint** — pass the `WEBHOOK_SECRET` as a query parameter:
```
GET /cron/batch?key=your-super-secret-key
```

---

## 🌐 API Endpoints

| Method | Path | Auth | Description |
|---|---|---|---|
| `GET` | `/health` | None | Returns `{"status": "ok"}`. Use this for uptime monitoring and health checks. |
| `POST` | `/webhook/lead` | HMAC `X-Hub-Signature-256` | Main webhook receiver. Processes all incoming leads. |
| `GET` or `POST` | `/cron/batch` | `?key=WEBHOOK_SECRET` | Cron-triggered batch dispatcher. |

---

## 📬 Webhook Payload Format

The endpoint accepts a **flat** JSON object or a **nested** one with a `callback_data` key. Both are normalised to a flat uppercase dict internally.

### Flat format
```json
{
  "TICKET_ID": "TKT-98765",
  "NAME": "Jane Smith",
  "PHONE": "+1 555 000 1234",
  "EMAIL": "jane@example.com",
  "REASON": "I need a pricing quote for a bulk order",
  "LANGUAGE": "en",
  "CSAT": null,
  "FRUSTRATION_SCORE": null
}
```

### Nested format
```json
{
  "TICKET_ID": "TKT-98765",
  "callback_data": {
    "name": "Jane Smith",
    "phone": "+1 555 000 1234",
    "email": "jane@example.com",
    "reason": "I need a pricing quote for a bulk order"
  }
}
```

Both formats produce identical results — keys are flattened and upper-cased before any processing begins.

### CSAT-only payload (post-interaction survey)
```json
{
  "TICKET_ID": "TKT-98765",
  "CSAT": 4,
  "FRUSTRATION_SCORE": 2
}
```
This silently updates the existing row. No new row is created. No emails are sent.

---

## 🔄 Lead Status Reference

| Status | Meaning | Emails Sent |
|---|---|---|
| `New` | New lead, standard priority, awaiting next batch | ✅ Customer confirmation (if email + ticket valid) |
| `Recurring` | Returning customer, standard priority, awaiting batch | ✅ Customer confirmation |
| `Notified (Immediate)` | Hot new lead — team alerted immediately | ✅ Customer + ✅ Team (immediate) |
| `Notified (Reorder)` | Returning hot lead — team alerted immediately | ✅ Customer + ✅ Team (immediate) |
| `Notified` | Included in a batch digest that was successfully sent | None (batch covered the team; customer already confirmed) |
| `Unreachable` | No phone or email — cannot contact customer | ❌ None |
| `CSAT Only (No Contact)` | Orphan satisfaction score with no contact info | ❌ None |

---

## 🚀 Deployment

### Railway / Render / Fly.io (Recommended)

1. Push the repo to GitHub.
2. Connect your PaaS to the repo.
3. Set all environment variables from the table above in the dashboard.
4. Set the start command to:
   ```bash
   python lead_pipeline.py
   ```
5. Configure an external cron service (e.g., [cron-job.org](https://cron-job.org)) to call:
   ```
   GET https://your-app.example.com/cron/batch?key=YOUR_WEBHOOK_SECRET
   ```
   once per day at your preferred time.

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY lead_pipeline.py .
CMD ["python", "lead_pipeline.py"]
```

```bash
docker build -t lead-pipeline .
docker run -p 8000:8000 --env-file .env lead-pipeline
```

### systemd (VPS / bare metal)

```ini
[Unit]
Description=Lead Pipeline Automation
After=network.target

[Service]
WorkingDirectory=/opt/lead-pipeline
ExecStart=/usr/bin/python3 /opt/lead-pipeline/lead_pipeline.py
EnvironmentFile=/opt/lead-pipeline/.env
Restart=always

[Install]
WantedBy=multi-user.target
```

---

## 🔒 Strict Email Rules — Why They Exist

The pipeline enforces three hard rules before any email is dispatched:

**Rule 1 — Customer confirmation requires both a valid email AND a valid Ticket ID.**
A confirmation without a ticket number gives the customer nothing to quote when they follow up. A missing email makes delivery impossible. Both must be present.

**Rule 2 — Immediate team alert requires `Immediate` intent AND at least one contact method.**
Alerting the team about a customer they cannot reach wastes their time and erodes trust in the alert system. If there's no phone and no email, the alert is silently withheld.

**Rule 3 — Batch dispatch is skipped entirely when there are zero pending leads.**
An empty batch email trains the team to ignore the digest. Every digest sent should be actionable.

These rules make the system feel **predictable and trustworthy**. The team learns quickly: an immediate alert always has a contactable customer; a digest always has leads to work through; silence means nothing is pending, not a bug.

---

## 🧪 Local Development

```bash
# 1. Clone the repo
git clone https://github.com/your-org/lead-pipeline.git
cd lead-pipeline

# 2. Create a virtual environment
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Copy and fill in the env file
cp .env.example .env
# Edit .env with your credentials

# 5. Start the server
python lead_pipeline.py
# Flask starts on http://localhost:8000

# 6. Verify the health endpoint
curl http://localhost:8000/health
# → {"mode": "google_sheets_batching_reorders", "status": "ok"}

# 7. Send a test webhook (leave WEBHOOK_SECRET empty to skip signature in dev)
curl -X POST http://localhost:8000/webhook/lead \
  -H "Content-Type: application/json" \
  -d '{
    "TICKET_ID": "TEST-001",
    "NAME": "Test User",
    "PHONE": "+1 555 000 0000",
    "EMAIL": "test@example.com",
    "REASON": "I want to buy your product"
  }'

# 8. Trigger the batch cron manually
curl http://localhost:8000/cron/batch
```

---

## 📦 Dependencies

```
flask       # HTTP server and routing
gspread     # Google Sheets API client (handles OAuth, cell addressing, pagination)
```

Everything else — `hmac`, `hashlib`, `urllib`, `json`, `os`, `datetime`, `random`, `string`, `logging` — is Python standard library.

**`requirements.txt`**
```
flask>=3.0
gspread>=6.0
```

> `google-auth` is pulled in automatically as a transitive dependency of `gspread`.

---

## 🪲 Logging

The pipeline uses Python's `logging` module at `INFO` level with structured, timestamped output:

```
2026-05-18 14:32:01  INFO      __main__  Generated fallback Ticket ID: TKT-AUTO-X7K2M-9QWA
2026-05-18 14:32:02  INFO      __main__  Added Ticket #TKT-AUTO-X7K2M-9QWA to CRM. Status: Notified (Immediate)
2026-05-18 14:32:02  WARNING   __main__  Blocked webhook retry: Ticket #TKT-98765 is already in the CRM.
2026-05-18 14:32:03  INFO      __main__  Dispatching batch email via API for 7 leads.
2026-05-18 14:32:04  ERROR     __main__  Failed to send email to test@example.com: <reason>
```

All errors are caught and logged at `ERROR` level but **never crash the server** — a bad Brevo call or a transient Sheets timeout logs a warning and returns gracefully.

---

## ❓ FAQ

**Q: What if the webhook source sends the same payload twice (retry logic)?**
The CRM lookup catches it. If the Ticket ID already exists in the sheet, the duplicate is blocked and a `200` is returned — so the source stops retrying — with body `{"status": "ignored", "reason": "duplicate_webhook"}`.

**Q: What if Google Sheets is temporarily unavailable?**
The connection failure is caught and logged at `ERROR`. For a production deployment at higher volume, consider wrapping `append_to_google_sheet` in a retry loop with exponential backoff, or queuing failed writes to a local file.

**Q: Can I swap Brevo for a different email provider?**
Yes. Replace the body of `send_brevo_email` with a call to any transactional email API (SendGrid, Postmark, Resend, AWS SES, etc.). The rest of the pipeline is completely email-provider-agnostic.

**Q: Can I swap Google Sheets for a real database?**
Yes. Replace `_get_sheet()`, `append_to_google_sheet()`, `check_for_duplicate()`, and the sheet-reading logic in `process_batches()` with your database driver of choice. The pipeline logic above those functions remains unchanged.

**Q: How do I customise the intent keywords for my industry?**
Edit the `IMMEDIATE_KEYWORDS` list at the top of `lead_pipeline.py`. For example, a SaaS company might use `["demo", "trial", "enterprise", "pricing", "upgrade"]`. A services business might use `["booking", "appointment", "urgent", "contract"]`.

**Q: Can this handle multiple sheets for multiple products or regions?**
Currently it always opens `sheet1`. To support multiple sheets, parameterise the sheet name — accept it from the webhook payload or a query parameter — and pass it to `gc.open_by_key(GOOGLE_SHEET_ID).worksheet(sheet_name)`.

**Q: Is this GDPR-compliant?**
That depends on your data processing agreements and privacy policy, not this code. The pipeline stores only data the customer voluntarily submitted and does not share it with any third party beyond Google (Sheets) and Brevo (email delivery). Consult a legal professional for a full compliance assessment.

---

*Built to ensure no lead ever falls through the cracks — regardless of the business it runs in.*
