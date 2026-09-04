from datetime import datetime, timedelta, timezone
import os
import secrets
import sqlite3
import hashlib
import hmac
import time
import json
import uuid
import stripe
import requests
import redis
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from fastapi import FastAPI, Header, HTTPException, Request, Query, Response, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel
from typing import List, Optional
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
from google import genai

load_dotenv()

SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[FastApiIntegration()],
        traces_sample_rate=1.0,
    )

stripe.api_key = os.getenv("STRIPE_API_KEY", "your_stripe_key_here")
ENDPOINT_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "your_webhook_secret_here")
WEBHOOK_SIGNING_SECRET = os.getenv("WEBHOOK_SIGNING_SECRET", "nexus_sec_sig_default_99")
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "onboarding@resend.dev")

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

redis_client = None
if REDIS_URL:
    try:
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping()
    except Exception as e:
        print(f"Warning: Redis connection failed: {e}")
        redis_client = None

db_pool = None
if DATABASE_URL:
    try:
        db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        db_pool = pool.ThreadedConnectionPool(minconn=2, maxconn=20, dsn=db_url)
    except Exception as e:
        print(f"Warning: Database connection pool initialization failed: {e}")


def get_db():
    if db_pool:
        conn = db_pool.getconn()
        conn.cursor_factory = RealDictCursor
        return conn
    else:
        conn = sqlite3.connect("quantcode_nexus.db")
        conn.row_factory = sqlite3.Row
        return conn


def release_db(conn):
    if db_pool:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass
    else:
        conn.close()


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def generate_hmac_signature(payload_json: str) -> str:
    return hmac.new(
        WEBHOOK_SIGNING_SECRET.encode("utf-8"),
        payload_json.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def log_audit_event(email: str, action: str, details: str, ip_address: str = "127.0.0.1"):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                "INSERT INTO audit_logs (email, action, details, ip_address) VALUES (%s, %s, %s, %s)",
                (email, action, details, ip_address)
            )
        else:
            cursor.execute(
                "INSERT INTO audit_logs (email, action, details, ip_address) VALUES (?, ?, ?, ?)",
                (email, action, details, ip_address)
            )
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Audit log error: {e}")
    finally:
        release_db(conn)


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    if DATABASE_URL:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                active INT DEFAULT 1,
                stripe_customer_id TEXT,
                tier TEXT DEFAULT 'starter',
                reset_token TEXT,
                reset_expires_at TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id SERIAL PRIMARY KEY,
                email TEXT REFERENCES subscribers(email),
                key_hash TEXT UNIQUE,
                key_name TEXT DEFAULT 'Default',
                active INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS b2b_leads (
                id SERIAL PRIMARY KEY,
                company_name TEXT UNIQUE,
                email TEXT,
                industry TEXT DEFAULT 'SaaS / Tech',
                employee_count TEXT DEFAULT '10-50',
                linkedin_url TEXT DEFAULT '',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS industry TEXT DEFAULT 'SaaS / Tech';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS employee_count TEXT DEFAULT '10-50';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS linkedin_url TEXT DEFAULT '';")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_events (
                event_id TEXT PRIMARY KEY,
                processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriber_webhooks (
                id SERIAL PRIMARY KEY,
                email TEXT REFERENCES subscribers(email),
                webhook_url TEXT NOT NULL,
                active INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_logs (
                id SERIAL PRIMARY KEY,
                event_id TEXT,
                webhook_url TEXT NOT NULL,
                payload TEXT,
                status_code INT,
                success INT DEFAULT 0,
                error_message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_dlq (
                id SERIAL PRIMARY KEY,
                event_id TEXT,
                webhook_url TEXT NOT NULL,
                payload TEXT,
                error_message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_usage_history (
                id SERIAL PRIMARY KEY,
                email TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                email TEXT,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_timestamp ON b2b_leads(timestamp DESC);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_company ON b2b_leads(company_name);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_usage_email_time ON api_usage_history(email, timestamp);")
    else:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                active INTEGER DEFAULT 1,
                stripe_customer_id TEXT,
                tier TEXT DEFAULT 'starter',
                reset_token TEXT,
                reset_expires_at DATETIME
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                key_hash TEXT UNIQUE,
                key_name TEXT DEFAULT 'Default',
                active INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS b2b_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company_name TEXT UNIQUE,
                email TEXT,
                industry TEXT DEFAULT 'SaaS / Tech',
                employee_count TEXT DEFAULT '10-50',
                linkedin_url TEXT DEFAULT '',
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_events (
                event_id TEXT PRIMARY KEY,
                processed_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriber_webhooks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                webhook_url TEXT NOT NULL,
                active INTEGER DEFAULT 1,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                webhook_url TEXT NOT NULL,
                payload TEXT,
                status_code INT,
                success INTEGER DEFAULT 0,
                error_message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS webhook_dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT,
                webhook_url TEXT NOT NULL,
                payload TEXT,
                error_message TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_usage_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS audit_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_timestamp ON b2b_leads(timestamp DESC);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_company ON b2b_leads(company_name);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_usage_email_time ON api_usage_history(email, timestamp);")
    conn.commit()
    cursor.close()
    release_db(conn)


init_db()


def record_usage_hit(email: str):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO api_usage_history (email) VALUES (%s)", (email,))
        else:
            cursor.execute("INSERT INTO api_usage_history (email) VALUES (?)", (email,))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"Usage analytics record error: {e}")
    finally:
        release_db(conn)


def dispatch_outbound_webhooks(lead_data: dict):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, webhook_url FROM subscriber_webhooks WHERE active = 1")
        webhooks = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    event_id = f"evt_{uuid.uuid4()}"
    base_payload = {
        "event_id": event_id,
        "event": "lead.ingested",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": lead_data
    }

    for wh in webhooks:
        url = wh["webhook_url"] if isinstance(wh, dict) or hasattr(wh, "__keys__") else wh[1]
        success = 0
        status_code = None
        error_msg = None
        
        for attempt in range(1, 4):
            payload_data = {**base_payload, "attempt": attempt}
            payload_json = json.dumps(payload_data)
            signature = generate_hmac_signature(payload_json)
            headers = {
                "Content-Type": "application/json",
                "X-Nexus-Signature": signature
            }

            try:
                response = requests.post(url, data=payload_json, headers=headers, timeout=5)
                status_code = response.status_code
                if 200 <= response.status_code < 300:
                    success = 1
                    error_msg = None
                    break
                else:
                    error_msg = f"HTTP Error Status: {response.status_code}"
            except Exception as e:
                error_msg = str(e)
                status_code = 500
            
            time.sleep(2 * attempt)

        log_conn = get_db()
        try:
            log_cursor = log_conn.cursor()
            if DATABASE_URL:
                if success == 0:
                    log_cursor.execute(
                        "INSERT INTO webhook_dlq (event_id, webhook_url, payload, error_message) VALUES (%s, %s, %s, %s)",
                        (event_id, url, json.dumps(base_payload), error_msg)
                    )
                log_cursor.execute(
                    "INSERT INTO webhook_logs (event_id, webhook_url, payload, status_code, success, error_message) VALUES (%s, %s, %s, %s, %s, %s)",
                    (event_id, url, json.dumps(base_payload), status_code, success, error_msg)
                )
            else:
                if success == 0:
                    log_cursor.execute(
                        "INSERT INTO webhook_dlq (event_id, webhook_url, payload, error_message) VALUES (?, ?, ?, ?)",
                        (event_id, url, json.dumps(base_payload), error_msg)
                    )
                log_cursor.execute(
                    "INSERT INTO webhook_logs (event_id, webhook_url, payload, status_code, success, error_message) VALUES (?, ?, ?, ?, ?, ?)",
                    (event_id, url, json.dumps(base_payload), status_code, success, error_msg)
                )
            log_conn.commit()
            log_cursor.close()
        except Exception as log_err:
            print(f"Failed to log webhook delivery: {log_err}")
        finally:
            release_db(log_conn)


async def automated_lead_ingestion():
    if not ai_client:
        print("Gemini AI Client not initialized. Skipping automated ingestion.")
        return

    prompt = (
        "Generate a JSON list of 3 real, active B2B technology, SaaS, or AI companies. "
        "For each company, provide: "
        "company_name, contact email format (e.g. contact@domain.com), industry, "
        "employee_count (e.g. '51-200'), and linkedin_url. "
        "Return strictly valid JSON matching this schema: "
        '[{"company_name": "...", "email": "...", "industry": "...", "employee_count": "...", "linkedin_url": "..."}]'
    )
    
    try:
        response = ai_client.models.generate_content(
            model='gemini-2.5-flash',
            contents=prompt,
        )
        raw_text = response.text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:-3].strip()
        elif raw_text.startswith("```"):
            raw_text = raw_text[3:-3].strip()
            
        leads = json.loads(raw_text)
        
        conn = get_db()
        try:
            cursor = conn.cursor()
            for lead in leads:
                if DATABASE_URL:
                    cursor.execute(
                        "INSERT INTO b2b_leads (company_name, email, industry, employee_count, linkedin_url) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (company_name) DO NOTHING",
                        (lead["company_name"], lead["email"], lead["industry"], lead["employee_count"], lead["linkedin_url"])
                    )
                else:
                    cursor.execute(
                        "INSERT OR IGNORE INTO b2b_leads (company_name, email, industry, employee_count, linkedin_url) VALUES (?, ?, ?, ?, ?)",
                        (lead["company_name"], lead["email"], lead["industry"], lead["employee_count"], lead["linkedin_url"])
                    )
                
                if cursor.rowcount > 0:
                    dispatch_outbound_webhooks({
                        "company_name": lead["company_name"], 
                        "email": lead["email"], 
                        "industry": lead["industry"],
                        "employee_count": lead["employee_count"],
                        "linkedin_url": lead["linkedin_url"],
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
            conn.commit()
            cursor.close()
        finally:
            release_db(conn)
    except Exception as e:
        print(f"Gemini AI Lead Ingestion Error: {e}")


scheduler = AsyncIOScheduler()

if os.getenv("ENABLE_MOCK_LEEDS", "false").lower() == "true":
    scheduler.add_job(automated_lead_ingestion, "interval", hours=1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not ADMIN_SECRET_KEY:
        print("CRITICAL WARNING: ADMIN_SECRET_KEY environment variable is not configured!")
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="QuantCode Nexus Lead API", lifespan=lifespan)

app.add_middleware(
    TrustedHostMiddleware, 
    allowed_hosts=["nexus-core-yfou.onrender.com", "localhost", "127.0.0.1", "testserver"]
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://nexus-core-yfou.onrender.com", "http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "code": exc.status_code,
            "message": exc.detail,
            "path": request.url.path
        },
    )


@app.get("/health")
async def health_check():
    db_status = "ok"
    redis_status = "ok" if redis_client else "disabled"
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
    except Exception as e:
        db_status = f"error: {str(e)}"
    finally:
        release_db(conn)
    
    if redis_client:
        try:
            redis_client.ping()
        except Exception as e:
            redis_status = f"error: {str(e)}"
            
    is_healthy = (db_status == "ok" and (redis_status == "ok" or redis_status == "disabled"))
    if not is_healthy:
        raise HTTPException(status_code=503, detail={"status": "degraded", "database": db_status, "redis": redis_status})

    return {
        "status": "healthy",
        "database": db_status,
        "redis": redis_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def verify_api_key(x_api_key: str, request: Request):
    incoming_hash = hash_api_key(x_api_key)
    client_ip = request.client.host if request.client else "unknown"
    
    cached_data = None
    if redis_client:
        try:
            cached_data = redis_client.get(f"apikey_cache:{incoming_hash}")
        except Exception:
            pass

    if cached_data:
        sub_info = json.loads(cached_data)
        record_usage_hit(sub_info["email"])
        return {
            "email": sub_info["email"],
            "key_name": sub_info["key_name"],
            "tier": sub_info["tier"],
            "hash": incoming_hash,
            "ip": client_ip
        }

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                """
                SELECT k.email, k.key_name, s.active, s.tier 
                FROM api_keys k 
                JOIN subscribers s ON k.email = s.email 
                WHERE k.key_hash = %s AND k.active = 1 AND s.active = 1
                """,
                (incoming_hash,)
            )
        else:
            cursor.execute(
                """
                SELECT k.email, k.key_name, s.active, s.tier 
                FROM api_keys k 
                JOIN subscribers s ON k.email = s.email 
                WHERE k.key_hash = ? AND k.active = 1 AND s.active = 1
                """,
                (incoming_hash,)
            )
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)
    
    if not row:
        log_audit_event("unknown", "API_AUTH_FAILURE", "Invalid key hash attempt", client_ip)
        raise HTTPException(status_code=403, detail="Invalid or inactive API subscription key.")
    
    email = row["email"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[0]
    key_name = row["key_name"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[1]
    tier = row["tier"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[3]

    if redis_client:
        try:
            redis_client.setex(
                f"apikey_cache:{incoming_hash}",
                60,
                json.dumps({"email": email, "key_name": key_name, "tier": tier})
            )
        except Exception:
            pass

    record_usage_hit(email)

    return {
        "email": email,
        "key_name": key_name,
        "tier": tier,
        "hash": incoming_hash,
        "ip": client_ip
    }


def check_rate_limit(api_key_hash: str, response: Response, max_requests: int = 30):
    window_seconds = 60
    current_time = int(time.time())
    current_minute = current_time // window_seconds
    
    if redis_client:
        try:
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
                    detail=f"Rate limit exceeded. Maximum {max_requests} requests per minute allowed."
                )
            return
        except redis.RedisError as e:
            print(f"Redis rate limit error (failing open): {e}")

    response.headers["X-RateLimit-Limit"] = str(max_requests)
    response.headers["X-RateLimit-Remaining"] = str(max_requests)
    response.headers["X-RateLimit-Reset"] = str((current_minute + 1) * window_seconds)


def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"Telegram error: {e}")


def send_email_via_resend(to_email: str, api_key: str):
    if not RESEND_API_KEY:
        return
    url = "https://api.resend.com/emails"
    headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
    html_content = f"""
        <h2>Welcome to QuantCode Nexus!</h2>
        <p>Your B2B lead API key has been generated and activated.</p>
        <p><strong>Your API Key:</strong> <code>{api_key}</code></p>
        <p><a href="https://nexus-core-yfou.onrender.com/dashboard" style="background: #38bdf8; color: #0f172a; padding: 12px 20px; text-decoration: none; border-radius: 6px; display: inline-block; font-weight: bold;">Open Dashboard</a></p>
    """
    payload = {"from": f"QuantCode Nexus <{SENDER_EMAIL}>", "to": [to_email], "subject": "Your QuantCode Nexus API Key 🚀", "html": html_content}
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except Exception as e:
        print(f"Resend error: {e}")


def send_password_reset_email(to_email: str, reset_url: str):
    if not RESEND_API_KEY:
        return
    url = "https://api.resend.com/emails"
    headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
    html_content = f"""
        <h2>QuantCode Nexus API Key Reset</h2>
        <p>Click below to generate your replacement API key:</p>
        <p><a href="{reset_url}" style="background: #38bdf8; color: #0f172a; padding: 12px 20px; text-decoration: none; border-radius: 6px; display: inline-block; font-weight: bold;">Reset My API Key</a></p>
        <p><small>Link expires in 15 minutes.</small></p>
    """
    payload = {"from": f"QuantCode Nexus <{SENDER_EMAIL}>", "to": [to_email], "subject": "Reset your API Key", "html": html_content}
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except Exception as e:
        print(f"Resend reset error: {e}")


@app.get("/")
async def read_index():
    return FileResponse("index.html")

@app.get("/success")
async def success_page():
    return FileResponse("success.html")

@app.get("/dashboard")
async def dashboard_page():
    return FileResponse("dashboard.html")

@app.get("/reset-success")
async def reset_success_page():
    return FileResponse("reset_success.html")

@app.get("/terms")
async def terms_page():
    return FileResponse("terms.html")

@app.get("/privacy")
async def privacy_page():
    return FileResponse("privacy.html")


@app.get("/api/v1/admin/cleanup-webhooks")
async def cleanup_webhooks(admin_key: str):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM subscriber_webhooks WHERE webhook_url LIKE '%your-unique-url%';")
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "Placeholder webhooks cleaned up."}


@app.get("/api/v1/admin/clear-leads")
async def clear_leads(admin_key: str):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("TRUNCATE TABLE b2b_leads RESTART IDENTITY CASCADE;")
        else:
            cursor.execute("DELETE FROM b2b_leads;")
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "All historical leads cleared."}


@app.get("/api/v1/claim-session")
async def claim_session_key(session_id: str):
    try:
        session = stripe.checkout.Session.retrieve(session_id)
        customer_email = session.customer_email or (session.customer_details and session.customer_details.email)
        if not customer_email:
            raise HTTPException(status_code=400, detail="No email attached to this checkout session.")

        conn = get_db()
        try:
            cursor = conn.cursor()
            if DATABASE_URL:
                cursor.execute("SELECT k.key_name FROM api_keys k JOIN subscribers s ON k.email = s.email WHERE s.email = %s LIMIT 1", (customer_email,))
            else:
                cursor.execute("SELECT k.key_name FROM api_keys k JOIN subscribers s ON k.email = s.email WHERE s.email = ? LIMIT 1", (customer_email,))
            
            row = cursor.fetchone()
            if not row:
                raw_api_key = f"qcn_{secrets.token_hex(16)}"
                hashed_key = hash_api_key(raw_api_key)
                tier = session.metadata.get("tier", "starter") if session.metadata else "starter"
                
                if DATABASE_URL:
                    cursor.execute(
                        "INSERT INTO subscribers (email, active, stripe_customer_id, tier) VALUES (%s, 1, %s, %s) ON CONFLICT (email) DO UPDATE SET active = 1",
                        (customer_email, session.customer, tier)
                    )
                    cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (%s, %s, 'Primary Key')", (customer_email, hashed_key))
                else:
                    cursor.execute(
                        "INSERT OR REPLACE INTO subscribers (email, active, stripe_customer_id, tier) VALUES (?, 1, ?, ?)",
                        (customer_email, session.customer, tier)
                    )
                    cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (?, ?, 'Primary Key')", (customer_email, hashed_key))
                conn.commit()
                cursor.close()
                return {"status": "success", "email": customer_email, "api_key": raw_api_key, "note": "Key freshly generated and claimed."}

            cursor.close()
            return {"status": "success", "email": customer_email, "message": "Subscription active. Check your email or generate a new key in your dashboard."}
        finally:
            release_db(conn)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/reset-confirm")
async def confirm_key_reset(token: str, background_tasks: BackgroundTasks, request: Request):
    conn = get_db()
    try:
        cursor = conn.cursor()
        now = datetime.now(timezone.utc)
        
        if DATABASE_URL:
            cursor.execute("SELECT email FROM subscribers WHERE reset_token = %s AND reset_expires_at > %s", (token, now))
        else:
            cursor.execute("SELECT email FROM subscribers WHERE reset_token = ? AND reset_expires_at > ?", (token, now))
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=400, detail="Invalid or expired reset token.")

        email = row["email"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[0]
        new_raw_key = f"qcn_{secrets.token_hex(16)}"
        new_hashed_key = hash_api_key(new_raw_key)

        if DATABASE_URL:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (%s, %s, 'Reset Key')", (email, new_hashed_key))
            cursor.execute("UPDATE subscribers SET reset_token = NULL, reset_expires_at = NULL WHERE email = %s", (email,))
        else:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (?, ?, 'Reset Key')", (email, new_hashed_key))
            cursor.execute("UPDATE subscribers SET reset_token = NULL, reset_expires_at = NULL WHERE email = ?", (email,))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    log_audit_event(email, "KEY_RESET", "API key successfully reset via email token", request.client.host if request.client else "unknown")
    background_tasks.add_task(send_email_via_resend, email, new_raw_key)
    return FileResponse("reset_success.html")


@app.post("/api/v1/request-key-reset")
async def request_key_reset(email: str, background_tasks: BackgroundTasks, request: Request):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT active FROM subscribers WHERE email = %s", (email,))
        else:
            cursor.execute("SELECT active FROM subscribers WHERE email = ?", (email,))
        row = cursor.fetchone()
        
        if not row or (row["active"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[0]) == 0:
            cursor.close()
            return {"status": "success", "message": "If an active account exists, a reset link has been sent."}

        reset_token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

        if DATABASE_URL:
            cursor.execute("UPDATE subscribers SET reset_token = %s, reset_expires_at = %s WHERE email = %s", (reset_token, expires_at, email))
        else:
            cursor.execute("UPDATE subscribers SET reset_token = ?, reset_expires_at = ? WHERE email = ?", (reset_token, expires_at, email))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    log_audit_event(email, "KEY_RESET_REQUEST", "Requested password/key reset link", request.client.host if request.client else "unknown")
    reset_url = f"https://nexus-core-yfou.onrender.com/reset-confirm?token={reset_token}"
    background_tasks.add_task(send_password_reset_email, email, reset_url)
    return {"status": "success", "message": "If an active account exists, a reset link has been sent."}


@app.get("/api/v1/keys")
async def list_subscriber_keys(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT id, key_name, active, created_at FROM api_keys WHERE email = %s", (sub["email"],))
        else:
            cursor.execute("SELECT id, key_name, active, created_at FROM api_keys WHERE email = ?", (sub["email"],))
        keys = [dict(r) for r in cursor.fetchall()]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "keys": keys}


@app.post("/api/v1/keys")
async def create_subscriber_key(request: Request, key_name: str = "New Key", x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    raw_key = f"qcn_{secrets.token_hex(16)}"
    hashed_key = hash_api_key(raw_key)

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (%s, %s, %s)", (sub["email"], hashed_key, key_name))
        else:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (?, ?, ?)", (sub["email"], hashed_key, key_name))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    if redis_client:
        try:
            redis_client.delete(f"apikey_cache:{sub['hash']}")
        except Exception:
            pass

    log_audit_event(sub["email"], "KEY_CREATED", f"Created new API key labeled '{key_name}'", sub["ip"])
    return {"status": "success", "key_name": key_name, "api_key": raw_key, "message": "Save this key now. It will not be shown again."}


@app.delete("/api/v1/keys/{key_id}")
async def revoke_subscriber_key(key_id: int, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("UPDATE api_keys SET active = 0 WHERE id = %s AND email = %s", (key_id, sub["email"]))
        else:
            cursor.execute("UPDATE api_keys SET active = 0 WHERE id = ? AND email = ?", (key_id, sub["email"]))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    if redis_client:
        try:
            redis_client.delete(f"apikey_cache:{sub['hash']}")
        except Exception:
            pass

    log_audit_event(sub["email"], "KEY_REVOKED", f"Revoked API key ID {key_id}", sub["ip"])
    return {"status": "success", "message": f"API key ID {key_id} revoked."}


@app.get("/api/v1/analytics/usage")
async def get_usage_analytics_history(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                """
                SELECT TO_CHAR(timestamp, 'YYYY-MM-DD') as day, COUNT(*) as request_count
                FROM api_usage_history
                WHERE email = %s AND timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY TO_CHAR(timestamp, 'YYYY-MM-DD')
                ORDER BY day ASC
                """,
                (sub["email"],)
            )
        else:
            cursor.execute(
                """
                SELECT DATE(timestamp) as day, COUNT(*) as request_count
                FROM api_usage_history
                WHERE email = ? AND timestamp >= datetime('now', '-7 days')
                GROUP BY DATE(timestamp)
                ORDER BY day ASC
                """,
                (sub["email"],)
            )
        rows = cursor.fetchall()
        history = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "usage_history": history}


@app.post("/api/v1/webhooks")
async def register_subscriber_webhook(webhook_url: str, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO subscriber_webhooks (email, webhook_url) VALUES (%s, %s)", (sub["email"], webhook_url))
        else:
            cursor.execute("INSERT INTO subscriber_webhooks (email, webhook_url) VALUES (?, ?)", (sub["email"], webhook_url))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    log_audit_event(sub["email"], "WEBHOOK_REGISTERED", f"Registered destination URL: {webhook_url}", sub["ip"])
    return {"status": "success", "message": "Webhook URL registered successfully."}


@app.get("/api/v1/webhook-logs")
async def get_webhook_logs(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                """
                SELECT DISTINCT l.id, l.event_id, l.webhook_url, l.status_code, l.success, l.error_message, l.timestamp 
                FROM webhook_logs l
                JOIN subscriber_webhooks w ON l.webhook_url = w.webhook_url
                WHERE w.email = %s
                ORDER BY l.timestamp DESC LIMIT 20
                """,
                (sub["email"],)
            )
        else:
            cursor.execute(
                """
                SELECT DISTINCT l.id, l.event_id, l.webhook_url, l.status_code, l.success, l.error_message, l.timestamp 
                FROM webhook_logs l
                JOIN subscriber_webhooks w ON l.webhook_url = w.webhook_url
                WHERE w.email = ?
                ORDER BY l.timestamp DESC LIMIT 20
                """,
                (sub["email"],)
            )
        rows = cursor.fetchall()
        logs = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "delivery_logs": logs}


class LeadItem(BaseModel):
    company_name: str
    email: str
    industry: Optional[str] = "SaaS / Tech"
    employee_count: Optional[str] = "10-50"
    linkedin_url: Optional[str] = ""

class BatchLeadUpload(BaseModel):
    leads: List[LeadItem]


@app.post("/api/v1/admin/upload-leads")
async def admin_upload_leads(payload: BatchLeadUpload, background_tasks: BackgroundTasks, admin_key: str = Header(...)):
    admin_secret = os.getenv("ADMIN_SECRET_KEY")
    if not admin_secret or admin_key != admin_secret:
        raise HTTPException(status_code=403, detail="Unauthorized admin key.")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        count = 0
        for lead in payload.leads:
            if DATABASE_URL:
                cursor.execute(
                    "INSERT INTO b2b_leads (company_name, email, industry, employee_count, linkedin_url) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (company_name) DO NOTHING",
                    (lead.company_name, lead.email, lead.industry, lead.employee_count, lead.linkedin_url)
                )
            else:
                cursor.execute(
                    "INSERT OR IGNORE INTO b2b_leads (company_name, email, industry, employee_count, linkedin_url) VALUES (?, ?, ?, ?, ?)",
                    (lead.company_name, lead.email, lead.industry, lead.employee_count, lead.linkedin_url)
                )
            
            if cursor.rowcount > 0:
                count += 1
                background_tasks.add_task(
                    dispatch_outbound_webhooks,
                    {
                        "company_name": lead.company_name,
                        "email": lead.email,
                        "industry": lead.industry,
                        "employee_count": lead.employee_count,
                        "linkedin_url": lead.linkedin_url,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    }
                )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "imported_count": count}


@app.post("/create-checkout-session")
async def create_checkout_session(email: str, tier: str = "starter"):
    amount = 9900 if tier == "pro" else 2900
    plan_name = "QuantCode Nexus Pro B2B Leads" if tier == "pro" else "QuantCode Nexus Starter B2B Leads"
    try:
        checkout_session = stripe.checkout.Session.create(
            customer_email=email,
            managed_payments={"enabled": False},
            metadata={"tier": tier},
            line_items=[{
                "price_data": {
                    "currency": "usd",
                    "product_data": {"name": plan_name},
                    "unit_amount": amount,
                    "recurring": {"interval": "month"},
                },
                "quantity": 1,
            }],
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
        try:
            cursor = conn.cursor()
            if DATABASE_URL:
                cursor.execute("SELECT stripe_customer_id FROM subscribers WHERE email = %s", (email,))
            else:
                cursor.execute("SELECT stripe_customer_id FROM subscribers WHERE email = ?", (email,))
            row = cursor.fetchone()
            cursor.close()
        finally:
            release_db(conn)

        customer_id = row["stripe_customer_id"] if row else None
        if not customer_id:
            customers = stripe.Customer.list(email=email, limit=1)
            if not customers.data:
                raise HTTPException(status_code=404, detail="No active Stripe customer found.")
            customer_id = customers.data[0].id

        portal_session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url="https://nexus-core-yfou.onrender.com/dashboard",
        )
        return {"portal_url": portal_session.url}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/webhook")
async def stripe_webhook(request: Request, background_tasks: BackgroundTasks):
    payload = await request.body()
    sig_header = request.headers.get("stripe-signature")
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, ENDPOINT_SECRET)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

    event_id = event.id
    event_type = event.type
    session = event.data.object
    session_dict = session.to_dict() if hasattr(session, "to_dict") else dict(session)

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT event_id FROM webhook_events WHERE event_id = %s", (event_id,))
        else:
            cursor.execute("SELECT event_id FROM webhook_events WHERE event_id = ?", (event_id,))
        
        if cursor.fetchone():
            cursor.close()
            return {"status": "success", "note": "event already processed"}

        try:
            if DATABASE_URL:
                cursor.execute("INSERT INTO webhook_events (event_id) VALUES (%s)", (event_id,))
            else:
                cursor.execute("INSERT INTO webhook_events (event_id) VALUES (?)", (event_id,))
            conn.commit()
        except Exception:
            pass

        if event_type == "checkout.session.completed":
            try:
                customer_email = session_dict.get("customer_email")
                customer_id = session_dict.get("customer")
                metadata = session_dict.get("metadata", {}) or {}
                tier = metadata.get("tier", "starter")

                if not customer_email and session_dict.get("customer_details"):
                    details = session_dict.get("customer_details")
                    if isinstance(details, dict):
                        customer_email = details.get("email")

                if customer_email:
                    raw_api_key = f"qcn_{secrets.token_hex(16)}"
                    hashed_key = hash_api_key(raw_api_key)
                    
                    if DATABASE_URL:
                        cursor.execute(
                            "INSERT INTO subscribers (email, active, stripe_customer_id, tier) VALUES (%s, 1, %s, %s) ON CONFLICT (email) DO UPDATE SET active = 1, stripe_customer_id = EXCLUDED.stripe_customer_id, tier = EXCLUDED.tier",
                            (customer_email, customer_id, tier),
                        )
                        cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (%s, %s, 'Primary Key')", (customer_email, hashed_key))
                    else:
                        cursor.execute(
                            "INSERT OR REPLACE INTO subscribers (email, active, stripe_customer_id, tier) VALUES (?, 1, ?, ?)",
                            (customer_email, customer_id, tier),
                        )
                        cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (?, ?, 'Primary Key')", (customer_email, hashed_key))
                    conn.commit()

                    log_audit_event(customer_email, "SUBSCRIPTION_CREATED", f"New subscription created on tier {tier}")
                    background_tasks.add_task(send_telegram_alert, f"🚀 *New Subscription ({tier.upper()})!*\nCustomer: `{customer_email}`")
                    background_tasks.add_task(send_email_via_resend, customer_email, raw_api_key)
            except Exception as err:
                print(f"Webhook processing error: {err}")

        elif event_type in ["customer.subscription.deleted", "invoice.payment_failed"]:
            try:
                customer_id = session_dict.get("customer")
                if customer_id:
                    if DATABASE_URL:
                        cursor.execute("UPDATE subscribers SET active = 0 WHERE stripe_customer_id = %s", (customer_id,))
                    else:
                        cursor.execute("UPDATE subscribers SET active = 0 WHERE stripe_customer_id = ?", (customer_id,))
                    conn.commit()
            except Exception as err:
                print(f"Revocation error: {err}")

        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success"}


@app.get("/api/v1/leads")
async def get_b2b_leads(
    request: Request,
    response: Response,
    x_api_key: str = Header(...),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    company: str | None = Query(None)
):
    sub = verify_api_key(x_api_key, request)
    max_limit = 200 if sub["tier"] == "pro" else 50
    if limit > max_limit:
        raise HTTPException(status_code=400, detail=f"Your '{sub['tier']}' tier allows max {max_limit} records per request.")

    check_rate_limit(sub["hash"], response=response, max_requests=(120 if sub["tier"] == "pro" else 30))

    conn = get_db()
    try:
        cursor = conn.cursor()

        if DATABASE_URL:
            if company:
                cursor.execute("SELECT * FROM b2b_leads WHERE company_name ILIKE %s ORDER BY timestamp DESC LIMIT %s OFFSET %s", (f"%{company}%", limit, offset))
            else:
                cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT %s OFFSET %s", (limit, offset))
        else:
            if company:
                cursor.execute("SELECT * FROM b2b_leads WHERE company_name LIKE ? ORDER BY timestamp DESC LIMIT ? OFFSET ?", (f"%{company}%", limit, offset))
            else:
                cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))

        rows = cursor.fetchall()
        leads = [dict(row) for row in rows]
        cursor.close()
    finally:
        release_db(conn)

    return {"status": "success", "tier": sub["tier"], "count": len(leads), "limit": limit, "offset": offset, "leads": leads}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)