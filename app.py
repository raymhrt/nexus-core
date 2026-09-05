from datetime import datetime, timedelta, timezone
import os
import secrets
import sqlite3
import hashlib
import hmac
import time
import json
import uuid
import logging
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

# Setup Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("nexus-core")

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
        logger.warning(f"Redis connection failed: {e}")
        redis_client = None

db_pool = None
if DATABASE_URL:
    try:
        db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        db_pool = pool.ThreadedConnectionPool(minconn=2, maxconn=20, dsn=db_url)
    except Exception as e:
        logger.warning(f"Database connection pool initialization failed: {e}")


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
        logger.error(f"Audit log error: {e}")
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
                company_name TEXT,
                domain TEXT,
                email TEXT,
                industry TEXT DEFAULT 'SaaS / Tech',
                employee_count TEXT DEFAULT '10-50',
                linkedin_url TEXT DEFAULT '',
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS domain TEXT;")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS industry TEXT DEFAULT 'SaaS / Tech';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS employee_count TEXT DEFAULT '10-50';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS linkedin_url TEXT DEFAULT '';")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_b2b_leads_domain_unique ON b2b_leads (domain);")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_error_dlq (
                id SERIAL PRIMARY KEY,
                raw_payload TEXT,
                error_message TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_domain ON b2b_leads(domain);")
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
                company_name TEXT,
                domain TEXT UNIQUE,
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
            CREATE TABLE IF NOT EXISTS ai_error_dlq (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                raw_payload TEXT,
                error_message TEXT,
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
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_leads_domain ON b2b_leads(domain);")
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
        logger.error(f"Usage analytics record error: {e}")
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
            logger.error(f"Failed to log webhook delivery: {log_err}")
        finally:
            release_db(log_conn)


app = FastAPI(title="QuantCode Nexus Lead API")

app.add_middleware(TrustedHostMiddleware, allowed_hosts=["*"])
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}


class LeadItem(BaseModel):
    company_name: str
    domain: str
    email: str
    industry: Optional[str] = "SaaS / Tech"
    employee_count: Optional[str] = "10-50"
    linkedin_url: Optional[str] = ""

class BatchLeadUpload(BaseModel):
    leads: List[LeadItem]

class AIErrorDLQItem(BaseModel):
    raw_payload: str
    error_message: str


@app.post("/api/v1/admin/ai-dlq")
async def store_ai_error(payload: AIErrorDLQItem, admin_key: str = Header(None, alias="admin-key")):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO ai_error_dlq (raw_payload, error_message) VALUES (%s, %s)", (payload.raw_payload, payload.error_message))
        else:
            cursor.execute("INSERT INTO ai_error_dlq (raw_payload, error_message) VALUES (?, ?)", (payload.raw_payload, payload.error_message))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "AI parsing error stored in DLQ"}


@app.get("/api/v1/admin/ai-dlq")
async def get_ai_dlq_logs(admin_key: str = Header(None, alias="admin-key")):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT * FROM ai_error_dlq ORDER BY timestamp DESC LIMIT 20")
        else:
            cursor.execute("SELECT * FROM ai_error_dlq ORDER BY timestamp DESC LIMIT 20")
        rows = cursor.fetchall()
        logs = [dict(row) for row in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "dlq_errors": logs}


@app.post("/api/v1/admin/upload-leads")
async def admin_upload_leads(payload: BatchLeadUpload, background_tasks: BackgroundTasks, admin_key: str = Header(None, alias="admin-key")):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized admin key.")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        count = 0
        for lead in payload.leads:
            clean_domain = lead.domain.lower().strip().replace("https://", "").replace("http://", "").rstrip("/")
            if DATABASE_URL:
                cursor.execute(
                    "INSERT INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (domain) DO NOTHING",
                    (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url)
                )
            else:
                cursor.execute(
                    "INSERT OR IGNORE INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url) VALUES (?, ?, ?, ?, ?, ?)",
                    (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url)
                )
            
            if cursor.rowcount > 0:
                count += 1
                background_tasks.add_task(
                    dispatch_outbound_webhooks,
                    {
                        "company_name": lead.company_name,
                        "domain": clean_domain,
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


@app.get("/api/v1/leads")
async def get_b2b_leads(limit: int = 50, offset: int = 0):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT %s OFFSET %s", (limit, offset))
        else:
            cursor.execute("SELECT * FROM b2b_leads ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))
        rows = cursor.fetchall()
        leads = [dict(row) for row in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "count": len(leads), "leads": leads}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)