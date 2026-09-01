from datetime import datetime, timezone
import os
import secrets
import sqlite3
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
DB_PATH = "quantcode_nexus.db"


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            email TEXT PRIMARY KEY,
            api_key TEXT UNIQUE,
            active INTEGER DEFAULT 1
        )
    """
    )
    conn.execute(
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
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Failed to send Telegram notification: {e}")


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

    try:
        if event.type == "checkout.session.completed":
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
                api_key = f"qcn_{secrets.token_hex(16)}"
                conn = get_db()
                conn.execute(
                    "INSERT OR REPLACE INTO subscribers (email, api_key, active) VALUES (?, ?, 1)",
                    (customer_email, api_key),
                )
                conn.commit()
                conn.close()

                # Send Telegram Notification Alert
                alert_msg = (
                    f"🚀 *New B2B Subscription!* \n\n"
                    f"Customer: `{customer_email}`\n"
                    f"API Key Provisioned: `{api_key}`"
                )
                send_telegram_alert(alert_msg)
                print(
                    f"SUCCESS: Provisioned API key {api_key} for {customer_email}"
                )
    except Exception as err:
        print(f"Webhook processing error: {err}")

    return {"status": "success"}


@app.get("/api/v1/leads")
async def get_b2b_leads(x_api_key: str = Header(...)):
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute(
        "SELECT active FROM subscribers WHERE api_key = ? AND active = 1",
        (x_api_key,),
    )
    subscriber = cursor.fetchone()

    if not subscriber:
        conn.close()
        raise HTTPException(
            status_code=403, detail="Invalid or inactive API subscription key."
        )

    cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT 50")
    rows = cursor.fetchall()
    leads = [dict(row) for row in rows]
    conn.close()

    return {"status": "success", "count": len(leads), "leads": leads}