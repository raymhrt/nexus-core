from datetime import datetime, timezone
import os
import secrets
import sqlite3
import hashlib
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import stripe
import requests
from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="QuantCode Nexus Lead API")

stripe.api_key = os.getenv("STRIPE_API_KEY", "your_stripe_key_here")
ENDPOINT_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "your_webhook_secret_here")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Gmail SMTP Configuration
SMTP_EMAIL = os.getenv("SMTP_EMAIL")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD")

# Check for Render Postgres URL, fallback to local sqlite if not set
DATABASE_URL = os.getenv("DATABASE_URL")


def get_db():
    if DATABASE_URL:
        import psycopg2
        import psycopg2.extras
        db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(db_url, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    else:
        conn = sqlite3.connect("quantcode_nexus.db")
        conn.row_factory = sqlite3.Row
        return conn


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    if DATABASE_URL:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                api_key TEXT UNIQUE,
                active INT DEFAULT 1
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS b2b_leads (
                id SERIAL PRIMARY KEY,
                company_name TEXT,
                email TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
    else:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                api_key TEXT UNIQUE,
                active INTEGER DEFAULT 1
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS b2b_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT,
                email TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
    conn.commit()
    cursor.close()
    conn.close()


# Initialize database tables on startup
init_db()


def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("Telegram tokens not configured.")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
    }
    try:
        response = requests.post(url, json=payload, timeout=5)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send Telegram notification: {e}")


def send_email_via_gmail(to_email: str, api_key: str):
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        print("SMTP credentials not configured.")
        return

    subject = "Your QuantCode Nexus API Key is Here 🚀"
    html_content = f"""
        <h2>Welcome to QuantCode Nexus!</h2>
        <p>Thank you for subscribing. Your B2B lead API key has been generated and activated.</p>
        <p><strong>Your API Key:</strong> <code>{api_key}</code></p>
        <p>You can start making requests immediately using your <code>x-api-key</code> header against our endpoint:</p>
        <p><code>GET https://nexus-core-yfou.onrender.com/api/v1/leads</code></p>
        <br>
        <p>Happy building,<br>The QuantCode Nexus Team</p>
    """

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_EMAIL
    msg["To"] = to_email
    msg.attach(MIMEText(html_content, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(SMTP_EMAIL, SMTP_PASSWORD)
            server.sendmail(SMTP_EMAIL, to_email, msg.as_string())
        print(f"SUCCESS: Emailed API key to {to_email} via Gmail SMTP")
    except Exception as e:
        print(f"Failed to send email via Gmail SMTP: {e}")


@app.get("/")
async def read_index():
    return FileResponse("index.html")


@app.post("/create-checkout-session")
async def create_checkout_session(email: str):
    try:
        checkout_session = stripe.checkout.Session.create(
            customer_email=email,
            managed_payments={"enabled": False},
            line_items=[
                {
                    "price_data": {
                        "currency": "usd",
                        "product_data": {
                            "name": "QuantCode Nexus B2B Leads Access"
                        },
                        "unit_amount": 2900,  # $29.00
                        "recurring": {"interval": "month"},
                    },
                    "quantity": 1,
                }
            ],
            mode="subscription",
            success_url="https://nexus-core-yfou.onrender.com/docs?success=true",
            cancel_url="https://nexus-core-yfou.onrender.com/docs?canceled=true",
        )
        return {"checkout_url": checkout_session.url}
    except Exception as e:
        print(f"Stripe Checkout Error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/webhook")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")

    try:
        event = stripe.Webhook.construct_event(
            payload, sig_header, ENDPOINT_SECRET
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Webhook error: {str(e)}")

    if event.type == "checkout.session.completed":
        try:
            session = event.data.object
            customer_email = getattr(session, "customer_email", None)

            if not customer_email and hasattr(session, "customer_details"):
                details = session.customer_details
                if details:
                    customer_email = getattr(details, "email", None)

            if not customer_email and getattr(session, "customer", None):
                try:
                    cust = stripe.Customer.retrieve(session.customer)
                    customer_email = getattr(cust, "email", None)
                except Exception:
                    pass

            if customer_email:
                raw_api_key = f"qcn_{secrets.token_hex(16)}"
                hashed_key = hash_api_key(raw_api_key)
                
                conn = get_db()
                cursor = conn.cursor()
                
                if DATABASE_URL:
                    cursor.execute(
                        "INSERT INTO subscribers (email, api_key, active) VALUES (%s, %s, 1) ON CONFLICT (email) DO UPDATE SET api_key = EXCLUDED.api_key, active = 1",
                        (customer_email, hashed_key),
                    )
                else:
                    cursor.execute(
                        "INSERT OR REPLACE INTO subscribers (email, api_key, active) VALUES (?, ?, 1)",
                        (customer_email, hashed_key),
                    )
                
                conn.commit()
                cursor.close()
                conn.close()

                # 1. Send Telegram Notification Alert (with raw key for your visibility)
                alert_msg = (
                    f"🚀 *New B2B Subscription!* \n\n"
                    f"Customer: `{customer_email}`\n"
                    f"API Key Provisioned: `{raw_api_key}`"
                )
                send_telegram_alert(alert_msg)

                # 2. Automatically Email API Key to Buyer via Gmail SMTP
                send_email_via_gmail(customer_email, raw_api_key)

                print(
                    f"SUCCESS: Provisioned secure API key for {customer_email}"
                )
        except Exception as err:
            print(f"Webhook processing error: {err}")

    return {"status": "success"}


@app.get("/api/v1/leads")
async def get_b2b_leads(x_api_key: str = Header(...)):
    incoming_hash = hash_api_key(x_api_key)
    
    conn = get_db()
    cursor = conn.cursor()

    if DATABASE_URL:
        cursor.execute(
            "SELECT active FROM subscribers WHERE api_key = %s AND active = 1",
            (incoming_hash,),
        )
    else:
        cursor.execute(
            "SELECT active FROM subscribers WHERE api_key = ? AND active = 1",
            (incoming_hash,),
        )
    subscriber = cursor.fetchone()

    if not subscriber:
        cursor.close()
        conn.close()
        raise HTTPException(
            status_code=403, detail="Invalid or inactive API subscription key."
        )

    if DATABASE_URL:
        cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT 50")
    else:
        cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT 50")
        
    rows = cursor.fetchall()
    leads = [dict(row) for row in rows]
    cursor.close()
    conn.close()

    return {"status": "success", "count": len(leads), "leads": leads}