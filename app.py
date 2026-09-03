from datetime import datetime, timezone
import os
import secrets
import sqlite3
import hashlib
import time
import stripe
import requests
import redis
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from fastapi import FastAPI, Header, HTTPException, Request, Query, Response
from fastapi.responses import FileResponse
from dotenv import load_dotenv

load_dotenv()

# Initialize Sentry Error Monitoring
SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[FastApiIntegration()],
        traces_sample_rate=1.0,
    )

app = FastAPI(title="QuantCode Nexus Lead API")

stripe.api_key = os.getenv("STRIPE_API_KEY", "your_stripe_key_here")
ENDPOINT_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "your_webhook_secret_here")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# Resend HTTP Email Configuration
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "onboarding@resend.dev")

# Check for Render Postgres URL & Redis URL
DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

# Initialize Redis client for distributed rate limiting if available
redis_client = redis.from_url(REDIS_URL, decode_responses=True) if REDIS_URL else None


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
                active INT DEFAULT 1,
                stripe_customer_id TEXT,
                tier TEXT DEFAULT 'starter'
            )
        """
        )
        cursor.execute(
            """
            ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS stripe_customer_id TEXT;
            """
        )
        cursor.execute(
            """
            ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS tier TEXT DEFAULT 'starter';
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
                active INTEGER DEFAULT 1,
                stripe_customer_id TEXT,
                tier TEXT DEFAULT 'starter'
            )
        """
        )
        try:
            cursor.execute("ALTER TABLE subscribers ADD COLUMN stripe_customer_id TEXT;")
        except Exception:
            pass

        try:
            cursor.execute("ALTER TABLE subscribers ADD COLUMN tier TEXT DEFAULT 'starter';")
        except Exception:
            pass

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


init_db()


def check_rate_limit(api_key_hash: str, response: Response, max_requests: int = 30):
    window_seconds = 60
    current_time = int(time.time())
    current_minute = current_time // window_seconds
    
    if redis_client:
        redis_key = f"rate_limit:{api_key_hash}:{current_minute}"
        
        pipe = redis_client.pipeline()
        pipe.incr(redis_key, 1)
        pipe.ttl(redis_key)
        count, ttl = pipe.execute()
        
        if ttl == -1:
            redis_client.expire(redis_key, window_seconds)
            ttl = window_seconds

        remaining = max(0, max_requests - count)
        reset_time = (current_minute + 1) * window_seconds

        response.headers["X-RateLimit-Limit"] = str(max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(reset_time)

        if count > max_requests:
            raise HTTPException(
                status_code=429,
                detail=f"Rate limit exceeded. Maximum {max_requests} requests per minute allowed for your tier."
            )
    else:
        response.headers["X-RateLimit-Limit"] = str(max_requests)
        response.headers["X-RateLimit-Remaining"] = str(max_requests)
        response.headers["X-RateLimit-Reset"] = str((current_minute + 1) * window_seconds)


def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
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


def send_email_via_resend(to_email: str, api_key: str):
    if not RESEND_API_KEY:
        return

    url = "https://api.resend.com/emails"
    headers = {
        "Authorization": f"Bearer {RESEND_API_KEY}",
        "Content-Type": "application/json"
    }
    
    html_content = f"""
        <h2>Welcome to QuantCode Nexus!</h2>
        <p>Thank you for subscribing. Your B2B lead API key has been generated and activated.</p>
        <p><strong>Your API Key:</strong> <code>{api_key}</code></p>
        <p>You can start making requests immediately using your <code>x-api-key</code> header against our endpoint:</p>
        <p><code>GET https://nexus-core-yfou.onrender.com/api/v1/leads</code></p>
        <br>
        <p>Happy building,<br>The QuantCode Nexus Team</p>
    """

    payload = {
        "from": f"QuantCode Nexus <{SENDER_EMAIL}>",
        "to": [to_email],
        "subject": "Your QuantCode Nexus API Key is Here 🚀",
        "html": html_content
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"Resend API error: {str(e)}")


@app.get("/")
async def read_index():
    return FileResponse("index.html")


@app.get("/success")
async def success_page():
    return FileResponse("success.html")


@app.get("/dashboard")
async def dashboard_page():
    return FileResponse("dashboard.html")


@app.post("/create-checkout-session")
async def create_checkout_session(email: str, tier: str = "starter"):
    amount = 9900 if tier == "pro" else 2900
    plan_name = "QuantCode Nexus Pro B2B Leads" if tier == "pro" else "QuantCode Nexus Starter B2B Leads"
    
    try:
        checkout_session = stripe.checkout.Session.create(
            customer_email=email,
            managed_payments={"enabled": False},
            metadata={"tier": tier},
            line_items=[
                {
                    "price_data": {
                        "currency": "usd",
                        "product_data": {
                            "name": plan_name
                        },
                        "unit_amount": amount,
                        "recurring": {"interval": "month"},
                    },
                    "quantity": 1,
                }
            ],
            mode="subscription",
            success_url="https://nexus-core-yfou.onrender.com/success?session_id={CHECKOUT_SESSION_ID}",
            cancel_url="https://nexus-core-yfou.onrender.com/docs?canceled=true",
        )
        return {"checkout_url": checkout_session.url}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/create-portal-session")
async def create_portal_session(email: str):
    try:
        conn = get_db()
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT stripe_customer_id FROM subscribers WHERE email = %s", (email,))
        else:
            cursor.execute("SELECT stripe_customer_id FROM subscribers WHERE email = ?", (email,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()

        customer_id = row["stripe_customer_id"] if row else None

        if not customer_id:
            customers = stripe.Customer.list(email=email, limit=1)
            if not customers.data:
                raise HTTPException(status_code=404, detail="No active Stripe customer found for this email.")
            customer_id = customers.data[0].id

        portal_session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url="https://nexus-core-yfou.onrender.com/success",
        )
        return {"portal_url": portal_session.url}
    except Exception as e:
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

    event_type = event.type
    session = event.data.object

    if event_type == "checkout.session.completed":
        try:
            customer_email = getattr(session, "customer_email", None)
            customer_id = getattr(session, "customer", None)
            metadata = getattr(session, "metadata", {}) or {}
            tier = metadata.get("tier", "starter")

            if not customer_email and hasattr(session, "customer_details"):
                details = session.customer_details
                if details:
                    customer_email = getattr(details, "email", None)

            if customer_email:
                raw_api_key = f"qcn_{secrets.token_hex(16)}"
                hashed_key = hash_api_key(raw_api_key)
                
                conn = get_db()
                cursor = conn.cursor()
                
                if DATABASE_URL:
                    cursor.execute(
                        "INSERT INTO subscribers (email, api_key, active, stripe_customer_id, tier) VALUES (%s, %s, 1, %s, %s) ON CONFLICT (email) DO UPDATE SET api_key = EXCLUDED.api_key, active = 1, stripe_customer_id = EXCLUDED.stripe_customer_id, tier = EXCLUDED.tier",
                        (customer_email, hashed_key, customer_id, tier),
                    )
                else:
                    cursor.execute(
                        "INSERT OR REPLACE INTO subscribers (email, api_key, active, stripe_customer_id, tier) VALUES (?, ?, 1, ?, ?)",
                        (customer_email, hashed_key, customer_id, tier),
                    )
                
                conn.commit()
                cursor.close()
                conn.close()

                alert_msg = (
                    f"🚀 *New B2B Subscription ({tier.upper()})!* \n\n"
                    f"Customer: `{customer_email}`\n"
                    f"API Key Provisioned: `{raw_api_key}`"
                )
                send_telegram_alert(alert_msg)
                send_email_via_resend(customer_email, raw_api_key)
        except Exception as err:
            print(f"Webhook error: {err}")

    elif event_type in ["customer.subscription.deleted", "invoice.payment_failed"]:
        try:
            customer_id = getattr(session, "customer", None)
            if not customer_id and hasattr(session, "customer"):
                customer_id = session.customer

            if customer_id:
                conn = get_db()
                cursor = conn.cursor()
                if DATABASE_URL:
                    cursor.execute("UPDATE subscribers SET active = 0 WHERE stripe_customer_id = %s", (customer_id,))
                else:
                    cursor.execute("UPDATE subscribers SET active = 0 WHERE stripe_customer_id = ?", (customer_id,))
                conn.commit()
                cursor.close()
                conn.close()
        except Exception as err:
            print(f"Revocation error: {err}")

    return {"status": "success"}


@app.get("/api/v1/leads")
async def get_b2b_leads(
    response: Response,
    x_api_key: str = Header(...),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    company: str | None = Query(None)
):
    incoming_hash = hash_api_key(x_api_key)

    conn = get_db()
    cursor = conn.cursor()

    if DATABASE_URL:
        cursor.execute(
            "SELECT active, tier FROM subscribers WHERE api_key = %s AND active = 1",
            (incoming_hash,),
        )
    else:
        cursor.execute(
            "SELECT active, tier FROM subscribers WHERE api_key = ? AND active = 1",
            (incoming_hash,),
        )
    subscriber = cursor.fetchone()

    if not subscriber:
        cursor.close()
        conn.close()
        raise HTTPException(
            status_code=403, detail="Invalid or inactive API subscription key."
        )

    subscriber_tier = subscriber["tier"] if isinstance(subscriber, (dict, sqlite3.Row)) or hasattr(subscriber, "__getitem__") else subscriber[1]

    max_limit = 200 if subscriber_tier == "pro" else 50
    if limit > max_limit:
        cursor.close()
        conn.close()
        raise HTTPException(
            status_code=400,
            detail=f"Your '{subscriber_tier}' tier allows a maximum record limit of {max_limit} per request."
        )

    rate_limit_max = 120 if subscriber_tier == "pro" else 30
    check_rate_limit(incoming_hash, response=response, max_requests=rate_limit_max)

    if DATABASE_URL:
        if company:
            cursor.execute(
                "SELECT * FROM b2b_leads WHERE company_name ILIKE %s ORDER BY timestamp DESC LIMIT %s OFFSET %s",
                (f"%{company}%", limit, offset)
            )
        else:
            cursor.execute(
                "SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT %s OFFSET %s",
                (limit, offset)
            )
    else:
        if company:
            cursor.execute(
                "SELECT * FROM b2b_leads WHERE company_name LIKE ? ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (f"%{company}%", limit, offset)
            )
        else:
            cursor.execute(
                "SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT ? OFFSET ?",
                (limit, offset)
            )
        
    rows = cursor.fetchall()
    leads = [dict(row) for row in rows]
    cursor.close()
    conn.close()

    return {"status": "success", "tier": subscriber_tier, "count": len(leads), "limit": limit, "offset": offset, "leads": leads}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)