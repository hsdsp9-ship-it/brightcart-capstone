# BrightCart — Email Report Secrets Setup Guide

The `07_business_analytics_report` notebook emails the generated HTML report to
`hsdsp9@gmail.com` after every pipeline run. It reads SMTP credentials from the
Databricks Secret scope **`brightcart`**. You only need to do this setup **once**.

---

## What You Need

| Secret key | What to put here |
| --- | --- |
| `report_sender_email` | The Gmail address that **sends** the report (e.g. `harpalsingh031@gmail.com`) |
| `report_sender_password` | A **Gmail App Password** for that account (NOT your normal login password) |

---

## Step 1 — Generate a Gmail App Password

1. Open [https://myaccount.google.com/security](https://myaccount.google.com/security) in your browser.
2. Make sure **2-Step Verification** is turned ON. (App Passwords require 2FA.)
3. Go to [https://myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords).
4. Click **Create a new App Password**.
   - App name: `BrightCart Databricks`
5. Copy the generated 16-character password (e.g. `abcd efgh ijkl mnop`).
   Remove the spaces when using it: `abcdefghijklmnop`.

---

## Step 2 — Store the Secrets in Databricks

Open the **Databricks Web Terminal** (Compute → your cluster → Web Terminal)
or run these from any environment with the Databricks CLI configured:

```bash
# The scope is already created. Just add the two secret keys:

databricks secrets put-secret brightcart report_sender_email \
  --string-value "harpalsingh031@gmail.com"

databricks secrets put-secret brightcart report_sender_password \
  --string-value "abcdefghijklmnop"
```

Replace the values with your actual Gmail address and App Password.

> **Security note:** Secret values are encrypted at rest and never shown in
> logs or notebook output. Databricks only reveals that a key exists, not its value.

---

## Step 3 — Verify

Run this in any notebook to confirm the keys exist (values stay hidden):

```python
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
for s in w.secrets.list_secrets(scope="brightcart"):
    print(s.key)   # prints key names only, never values
```

Expected output:
```
report_sender_email
report_sender_password
```

---

## How It Works After Setup

Every time the full pipeline runs (GitHub push or daily 2 AM UTC schedule):

```
GitHub push to main
       ↓
 databricks bundle deploy
       ↓
 BrightCart Full Pipeline job starts
       ↓
  … bronze → silver → gold …
       ↓
 07_business_analytics_report notebook
   ├─ generates BRIGHTCART_REPORT_2024.html
   └─ reads secrets → sends email via Gmail SMTP
       ↓
 hsdsp9@gmail.com receives:
   Subject : BrightCart Daily Analytics Report — YYYY-MM-DD
   Body    : KPI summary table (rendered HTML)
   Attachment: BRIGHTCART_REPORT_YYYY-MM-DD.html (full charts)
```

---

## Troubleshooting

| Error | Fix |
| --- | --- |
| `SMTPAuthenticationError` | You used your Gmail login password instead of an App Password. Generate one at [myaccount.google.com/apppasswords](https://myaccount.google.com/apppasswords). |
| `Secret does not exist` | Run the `put-secret` commands in Step 2. |
| `Less secure app access` error | App Passwords bypass this — make sure you are using an App Password, not your account password. |
| Email in spam | Add the sender address to your Gmail contacts or mark the first email as "Not spam". |
