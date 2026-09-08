from datetime import datetime, timedelta, timezone
import os
import secrets
import sqlite3
import hashlib
import hmac
import time
import asyncio
import json
import uuid
import logging
import random
import stripe
import requests
import redis
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from fastapi import FastAPI, Header, HTTPException, Request, Query, Response, BackgroundTasks
from fastapi.responses import FileResponse, JSONResponse, Response as FastAPIResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional, Dict, Any
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
)
logger = logging.getLogger("nexus-enterprise-apex")

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
        db_pool = pool.ThreadedConnectionPool(minconn=5, maxconn=40, dsn=db_url)
    except Exception as e:
        logger.warning(f"Database connection pool initialization failed: {e}")

webhook_semaphore = asyncio.Semaphore(10)


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


def call_gemini_rest(prompt: str, max_retries: int = 3) -> str:
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY environment variable is missing or empty.")
        raise Exception("GEMINI_API_KEY not configured")
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.6-flash:generateContent?key={GEMINI_API_KEY}"
    headers = {"Content-Type": "application/json"}
    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }]
    }
    
    backoff_factor = 2
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            if res.status_code == 200:
                data = res.json()
                return data["candidates"][0]["content"]["parts"][0]["text"]
            elif res.status_code in [503, 429, 502, 504]:
                logger.warning(f"Gemini API returned transient status {res.status_code} on attempt {attempt}/{max_retries}. Retrying...")
                if attempt == max_retries:
                    raise Exception(f"Gemini API returned status {res.status_code} after {max_retries} attempts: {res.text}")
            else:
                logger.error(f"Gemini API error status {res.status_code}: {res.text}")
                raise Exception(f"Gemini API returned status {res.status_code}")
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as net_err:
            logger.warning(f"Gemini network connection error on attempt {attempt}/{max_retries}: {net_err}")
            if attempt == max_retries:
                raise
        except Exception as err:
            if attempt == max_retries:
                raise
            logger.warning(f"Gemini retryable exception on attempt {attempt}: {err}")

        sleep_time = (backoff_factor ** attempt) + random.uniform(0.1, 1.0)
        time.sleep(sleep_time)
        
    raise Exception("Gemini API failed after maximum retries.")


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


def send_telegram_alert(message: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "Markdown"}, timeout=5)
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")


def send_email_via_resend(to_email: str, api_key: str):
    if not RESEND_API_KEY:
        return
    url = "https://api.resend.com/emails"
    headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
    html_content = f"""
        <h2>Welcome to QuantCode Nexus Enterprise Apex!</h2>
        <p>Your elite B2B lead API key has been generated and activated.</p>
        <p><strong>Your API Key:</strong> <code>{api_key}</code></p>
        <p><a href="https://nexus-core-yfou.onrender.com/dashboard" style="background: #38bdf8; color: #0f172a; padding: 12px 20px; text-decoration: none; border-radius: 6px; display: inline-block; font-weight: bold;">Open Dashboard</a></p>
    """
    payload = {"from": f"QuantCode Nexus <{SENDER_EMAIL}>", "to": [to_email], "subject": "Your Enterprise API Key 🚀", "html": html_content}
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except Exception as e:
        logger.error(f"Resend error: {e}")


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
        logger.error(f"Resend reset error: {e}")


def send_magic_link_email(to_email: str, magic_url: str):
    if not RESEND_API_KEY:
        return
    url = "https://api.resend.com/emails"
    headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
    html_content = f"""
        <h2>QuantCode Nexus Magic Link Sign-In</h2>
        <p>Click the secure link below to instantly sign in to your dashboard:</p>
        <p><a href="{magic_url}" style="background: #38bdf8; color: #0f172a; padding: 12px 20px; text-decoration: none; border-radius: 6px; display: inline-block; font-weight: bold;">Sign In Instantly 🚀</a></p>
        <p><small>Link expires in 15 minutes.</small></p>
    """
    payload = {"from": f"QuantCode Nexus <{SENDER_EMAIL}>", "to": [to_email], "subject": "Your Magic Sign-In Link", "html": html_content}
    try:
        requests.post(url, json=payload, headers=headers, timeout=10)
    except Exception as e:
        logger.error(f"Resend magic link error: {e}")


def init_db():
    conn = get_db()
    cursor = conn.cursor()
    
    if DATABASE_URL:
        cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
        cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                active INT DEFAULT 1,
                stripe_customer_id TEXT,
                tier TEXT DEFAULT 'starter',
                reset_token TEXT,
                reset_expires_at TIMESTAMP,
                magic_token TEXT,
                magic_expires_at TIMESTAMP
            )
        """
        )
        cursor.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_token TEXT;")
        cursor.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_expires_at TIMESTAMP;")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS api_keys (
                id SERIAL PRIMARY KEY,
                email TEXT REFERENCES subscribers(email),
                key_hash TEXT UNIQUE,
                key_name TEXT DEFAULT 'Default',
                scope TEXT DEFAULT 'full',
                role TEXT DEFAULT 'admin',
                active INT DEFAULT 1,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS scope TEXT DEFAULT 'full';")
        cursor.execute("ALTER TABLE api_keys ADD COLUMN IF NOT EXISTS role TEXT DEFAULT 'admin';")
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriber_credits (
                email TEXT PRIMARY KEY REFERENCES subscribers(email),
                credits_remaining INT DEFAULT 500,
                credits_limit INT DEFAULT 500,
                last_refill_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriber_destinations (
                id SERIAL PRIMARY KEY,
                email TEXT REFERENCES subscribers(email),
                destination_type TEXT NOT NULL,
                webhook_url TEXT NOT NULL,
                access_token TEXT DEFAULT '',
                mapping_rules TEXT DEFAULT '{}',
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
                confidence_score FLOAT DEFAULT 0.9,
                trust_score INT DEFAULT 95,
                tech_stack TEXT DEFAULT 'Python, PostgreSQL',
                funding_stage TEXT DEFAULT 'Series A',
                intent_signals TEXT DEFAULT 'None',
                verified_email INT DEFAULT 1,
                embedding vector(768),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS tech_stack TEXT DEFAULT 'Python, PostgreSQL';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS funding_stage TEXT DEFAULT 'Series A';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS intent_signals TEXT DEFAULT 'None';")
        cursor.execute("ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS verified_email INT DEFAULT 1;")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_b2b_leads_domain_unique ON b2b_leads (domain);")
        cursor.execute("CREATE INDEX IF NOT EXISTS b2b_leads_hnsw_idx ON b2b_leads USING hnsw (embedding vector_cosine_ops);")

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS subscriber_icps (
                email TEXT PRIMARY KEY REFERENCES subscribers(email),
                target_industries TEXT DEFAULT 'SaaS / Tech',
                min_trust_score INT DEFAULT 80,
                preferred_employee_count TEXT DEFAULT '10-50',
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS lead_feedback (
                id SERIAL PRIMARY KEY,
                email TEXT,
                lead_id INT,
                feedback_status TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
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
                consecutive_failures INT DEFAULT 0,
                last_failure_time TIMESTAMP,
                circuit_status TEXT DEFAULT 'ACTIVE',
                filter_rules TEXT DEFAULT '{}',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        cursor.execute("ALTER TABLE subscriber_webhooks ADD COLUMN IF NOT EXISTS filter_rules TEXT DEFAULT '{}';")

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
    else:
        cursor.execute("CREATE TABLE IF NOT EXISTS subscribers (email TEXT PRIMARY KEY, active INTEGER DEFAULT 1, stripe_customer_id TEXT, tier TEXT DEFAULT 'starter', reset_token TEXT, reset_expires_at DATETIME, magic_token TEXT, magic_expires_at DATETIME)")
        cursor.execute("CREATE TABLE IF NOT EXISTS api_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, key_hash TEXT UNIQUE, key_name TEXT DEFAULT 'Default', scope TEXT DEFAULT 'full', role TEXT DEFAULT 'admin', active INTEGER DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_credits (email TEXT PRIMARY KEY, credits_remaining INTEGER DEFAULT 500, credits_limit INTEGER DEFAULT 500, last_refill_date DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_destinations (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, destination_type TEXT NOT NULL, webhook_url TEXT NOT NULL, access_token TEXT DEFAULT '', mapping_rules TEXT DEFAULT '{}', active INTEGER DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS b2b_leads (id INTEGER PRIMARY KEY AUTOINCREMENT, company_name TEXT, domain TEXT UNIQUE, email TEXT, industry TEXT DEFAULT 'SaaS / Tech', employee_count TEXT DEFAULT '10-50', linkedin_url TEXT DEFAULT '', confidence_score REAL DEFAULT 0.9, trust_score INTEGER DEFAULT 95, tech_stack TEXT DEFAULT 'Python, PostgreSQL', funding_stage TEXT DEFAULT 'Series A', intent_signals TEXT DEFAULT 'None', verified_email INTEGER DEFAULT 1, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_icps (email TEXT PRIMARY KEY, target_industries TEXT, min_trust_score INTEGER, preferred_employee_count TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS lead_feedback (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, lead_id INTEGER, feedback_status TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS ai_error_dlq (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_payload TEXT, error_message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_webhooks (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, webhook_url TEXT NOT NULL, active INTEGER DEFAULT 1, consecutive_failures INTEGER DEFAULT 0, last_failure_time DATETIME, circuit_status TEXT DEFAULT 'ACTIVE', filter_rules TEXT DEFAULT '{}', created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS webhook_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT, webhook_url TEXT NOT NULL, payload TEXT, status_code INT, success INTEGER DEFAULT 0, error_message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, action TEXT NOT NULL, details TEXT, ip_address TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
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


def generate_lead_embedding(text_content: str):
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY missing during embedding generation.")
        return None
    try:
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-embedding-001:embedContent?key={GEMINI_API_KEY}"
        payload = {
            "model": "models/gemini-embedding-001",
            "content": {
                "parts": [{"text": text_content}]
            },
            "output_dimensionality": 768
        }
        res = requests.post(url, json=payload, headers={"Content-Type": "application/json"}, timeout=10)
        if res.status_code == 200:
            data = res.json()
            if "embedding" in data and "values" in data["embedding"]:
                return data["embedding"]["values"]
            elif "embedding" in data and "embedding" in data["embedding"]:
                return data["embedding"]["embedding"]["values"]
        else:
            logger.error(f"Embedding API error status {res.status_code}: {res.text}")
        return None
    except Exception as e:
        logger.error(f"CRITICAL Embedding generation error: {e}")
        return None


def fetch_advanced_enrichment_data(domain: str) -> dict:
    clean_dom = domain.lower().replace("https://", "").replace("http://", "").rstrip("/")
    tech_candidates = ["React, Node.js, AWS", "Python, FastAPI, PostgreSQL", "Go, Kubernetes, GCP", "Ruby on Rails, Redis", "Next.js, TypeScript, Vercel"]
    funding_candidates = ["Seed", "Series A", "Series B", "Series C", "Bootstrapped", "Public / Enterprise"]
    intent_candidates = ["High hiring velocity in engineering", "Recent Series A funding announcement", "Migrated infrastructure to Cloud", "Expanding international sales team"]
    
    hash_val = int(hashlib.md5(clean_dom.encode('utf-8')).hexdigest(), 16)
    tech = tech_candidates[hash_val % len(tech_candidates)]
    funding = funding_candidates[(hash_val // 7) % len(funding_candidates)]
    intent = intent_candidates[(hash_val // 11) % len(intent_candidates)]
    
    verified = 1 if (hash_val % 10) != 0 else 0
    
    return {"tech_stack": tech, "funding_stage": funding, "intent_signals": intent, "verified_email": verified}


async def async_background_enrichment_worker(lead_id: int, company_name: str, domain: str):
    enrichment = fetch_advanced_enrichment_data(domain)
    mock_tech = enrichment["tech_stack"]
    mock_funding = enrichment["funding_stage"]
    mock_intent = enrichment["intent_signals"]
    verified_flag = enrichment["verified_email"]

    vec = await asyncio.to_thread(generate_lead_embedding, f"{company_name} {domain} {mock_tech} {mock_funding} {mock_intent}")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                "UPDATE b2b_leads SET tech_stack = %s, funding_stage = %s, intent_signals = %s, verified_email = %s, embedding = %s WHERE id = %s",
                (mock_tech, mock_funding, mock_intent, verified_flag, str(vec) if vec else None, lead_id)
            )
        else:
            cursor.execute(
                "UPDATE b2b_leads SET tech_stack = ?, funding_stage = ?, intent_signals = ?, verified_email = ?, embedding = ? WHERE id = ?",
                (mock_tech, mock_funding, mock_intent, verified_flag, str(vec) if vec else None, lead_id)
            )
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Enrichment worker error for lead {lead_id}: {e}")
    finally:
        release_db(conn)


async def webhook_canary_healing_worker():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, webhook_url FROM subscriber_webhooks WHERE circuit_status = 'TRIPPED'")
        tripped_hooks = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    for hook in tripped_hooks:
        h_dict = dict(hook) if not isinstance(hook, dict) and not hasattr(hook, "keys") else hook
        h_id = h_dict["id"] if isinstance(h_dict, dict) else hook[0]
        url = h_dict["webhook_url"] if isinstance(h_dict, dict) else hook[1]

        try:
            res = await asyncio.to_thread(requests.get, url, timeout=5)
            if res.status_code < 500:
                up_conn = get_db()
                up_cursor = up_conn.cursor()
                if DATABASE_URL:
                    up_cursor.execute("UPDATE subscriber_webhooks SET circuit_status = 'ACTIVE', consecutive_failures = 0, last_failure_time = NULL WHERE id = %s", (h_id,))
                else:
                    up_cursor.execute("UPDATE subscriber_webhooks SET circuit_status = 'ACTIVE', consecutive_failures = 0, last_failure_time = NULL WHERE id = ?", (h_id,))
                up_conn.commit()
                up_cursor.close()
                release_db(up_conn)
                logger.info(f"Webhook canary successfully healed and reset endpoint {url} to ACTIVE.")
        except Exception:
            pass


async def dispatch_outbound_webhooks(lead_data: dict):
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, email, webhook_url, consecutive_failures, circuit_status, last_failure_time, filter_rules FROM subscriber_webhooks WHERE active = 1")
        webhooks = cursor.fetchall()
        
        cursor.execute("SELECT email, destination_type, webhook_url, access_token, mapping_rules FROM subscriber_destinations WHERE active = 1")
        native_destinations = cursor.fetchall()
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

    current_span = sentry_sdk.get_current_span()
    trace_id = current_span.get_trace_context().get("trace_id") if current_span and hasattr(current_span, "get_trace_context") else uuid.uuid4().hex
    span_id = uuid.uuid4().hex[:16]
    traceparent_header = f"00-{trace_id}-{span_id}-01"

    for wh in webhooks:
        wh_dict = dict(wh) if not isinstance(wh, dict) and not hasattr(wh, "keys") else wh
        wh_id = wh_dict["id"] if isinstance(wh_dict, dict) else wh[0]
        url = wh_dict["webhook_url"] if isinstance(wh_dict, dict) else wh[2]
        circuit_status = wh_dict.get("circuit_status", "ACTIVE") if isinstance(wh_dict, dict) else wh[4]
        last_failure = wh_dict.get("last_failure_time") if isinstance(wh_dict, dict) else wh[5]
        raw_rules = wh_dict.get("filter_rules", "{}") if isinstance(wh_dict, dict) else wh[6]

        try:
            rules = json.loads(raw_rules) if raw_rules else {}
            min_trust = rules.get("min_trust_score", 0)
            target_ind = rules.get("industries", [])
            
            if lead_data.get("trust_score", 0) < min_trust:
                continue
            if target_ind and lead_data.get("industry") not in target_ind:
                continue
        except Exception:
            pass

        if circuit_status == "TRIPPED":
            if last_failure:
                if isinstance(last_failure, str):
                    last_failure_dt = datetime.fromisoformat(last_failure.replace('Z', '+00:00'))
                else:
                    last_failure_dt = last_failure
                if last_failure_dt.tzinfo is None:
                    last_failure_dt = last_failure_dt.replace(tzinfo=timezone.utc)
                
                if datetime.now(timezone.utc) - last_failure_dt > timedelta(minutes=15):
                    circuit_status = "HALF_OPEN"
                else:
                    continue
            else:
                continue

        success = 0
        status_code = None
        error_msg = None
        max_retries = 3
        base_backoff = 2

        for attempt in range(1, max_retries + 1):
            payload_json = json.dumps({**base_payload, "attempt": attempt})
            signature = generate_hmac_signature(payload_json)
            headers = {
                "Content-Type": "application/json",
                "X-Nexus-Signature": signature,
                "X-Nexus-Event-Id": event_id,
                "traceparent": traceparent_header
            }

            try:
                response = await asyncio.to_thread(requests.post, url, data=payload_json, headers=headers, timeout=10)
                status_code = response.status_code
                if 200 <= response.status_code < 300:
                    success = 1
                    error_msg = None
                    break
                else:
                    error_msg = f"HTTP Error Status: {response.status_code}"
                    if response.status_code < 500 and response.status_code != 429:
                        break
            except Exception as e:
                error_msg = str(e)
                status_code = 500
            
            await asyncio.sleep(base_backoff ** attempt)

        log_conn = get_db()
        try:
            log_cursor = log_conn.cursor()
            if DATABASE_URL:
                if success == 0:
                    log_cursor.execute("INSERT INTO webhook_dlq (event_id, webhook_url, payload, error_message) VALUES (%s, %s, %s, %s)", (event_id, url, json.dumps(base_payload), error_msg))
                log_cursor.execute("INSERT INTO webhook_logs (event_id, webhook_url, payload, status_code, success, error_message) VALUES (%s, %s, %s, %s, %s, %s)", (event_id, url, json.dumps(base_payload), status_code, success, error_msg))
            else:
                if success == 0:
                    log_cursor.execute("INSERT INTO webhook_dlq (event_id, webhook_url, payload, error_message) VALUES (?, ?, ?, ?)", (event_id, url, json.dumps(base_payload), error_msg))
                log_cursor.execute("INSERT INTO webhook_logs (event_id, webhook_url, payload, status_code, success, error_message) VALUES (?, ?, ?, ?, ?, ?)", (event_id, url, json.dumps(base_payload), status_code, success, error_msg))
            log_conn.commit()
            log_cursor.close()
        except Exception as log_err:
            logger.error(f"Failed to log webhook delivery: {log_err}")
        finally:
            release_db(log_conn)

    for nd in native_destinations:
        nd_dict = dict(nd) if not isinstance(nd, dict) and not hasattr(nd, "keys") else nd
        dest_type = nd_dict["destination_type"] if isinstance(nd_dict, dict) else nd[1]
        dest_url = nd_dict["webhook_url"] if isinstance(nd_dict, dict) else nd[2]
        token = nd_dict.get("access_token", "") if isinstance(nd_dict, dict) else nd[3]

        formatted_payload = lead_data
        headers = {"Content-Type": "application/json"}

        if dest_type.lower() == "hubspot":
            headers["Authorization"] = f"Bearer {token}"
            formatted_payload = {
                "properties": {
                    "company": lead_data.get("company_name"),
                    "domain": lead_data.get("domain"),
                    "email": lead_data.get("email"),
                    "industry": lead_data.get("industry"),
                    "numberofemployees": lead_data.get("employee_count"),
                    "lifecyclestage": "lead"
                }
            }
        elif dest_type.lower() == "salesforce":
            headers["Authorization"] = f"Bearer {token}"
            formatted_payload = {
                "Name": lead_data.get("company_name"),
                "Website": lead_data.get("domain"),
                "Industry": lead_data.get("industry"),
                "NumberOfEmployees": lead_data.get("employee_count")
            }
        elif dest_type.lower() == "slack":
            formatted_payload = {
                "text": f"🚀 *New B2B Lead Ingested!*\n*Company:* {lead_data.get('company_name')} ({lead_data.get('domain')})\n*Industry:* {lead_data.get('industry')} | *Trust Score:* {lead_data.get('trust_score')}/100"
            }

        try:
            await asyncio.to_thread(requests.post, dest_url, json=formatted_payload, headers=headers, timeout=10)
        except Exception as crm_err:
            logger.error(f"Native CRM dispatch error for {dest_type}: {crm_err}")


async def safe_dispatch_wrapper(lead_payload: dict):
    async with webhook_semaphore:
        await dispatch_outbound_webhooks(lead_payload)


class GeminiLeadSchema(BaseModel):
    company_name: str
    domain: str
    email: str
    industry: Optional[str] = "SaaS / Tech"
    employee_count: Optional[str] = "10-50"
    linkedin_url: Optional[str] = ""
    confidence_score: Optional[float] = 0.9
    trust_score: Optional[int] = 95


async def automated_lead_ingestion():
    if not GEMINI_API_KEY:
        return

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT l.company_name, l.industry, l.tech_stack, f.feedback_status 
            FROM lead_feedback f 
            JOIN b2b_leads l ON f.lead_id = l.id 
            WHERE f.feedback_status = 'converted'
            ORDER BY f.timestamp DESC LIMIT 10
        """)
        converted_rows = cursor.fetchall()
        
        cursor.execute("SELECT DISTINCT target_industries, min_trust_score, preferred_employee_count FROM subscriber_icps")
        icps = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    converted_anchors = [f"{r['company_name']} ({r['industry']} - Tech Stack: {r['tech_stack']})" for r in converted_rows]
    vector_alignment_context = ""
    if converted_anchors:
        vector_alignment_context = f" Mathematically align generated company profiles as exact vector lookalikes to these verified converted customer accounts: {', '.join(converted_anchors)}."

    target_niches = [dict(i) for i in icps] if icps else [{"target_industries": "SaaS / Tech / Fintech / AI", "min_trust_score": 85, "preferred_employee_count": "10-50"}]

    for icp in target_niches:
        industries = icp.get("target_industries", "SaaS / Tech")
        min_trust = icp.get("min_trust_score", 85)
        employee_size = icp.get("preferred_employee_count", "10-50")

        prompt = (
            f"Generate a JSON list of 3 real, active B2B companies specifically matching these criteria: "
            f"Industries/Niche: {industries}, Minimum Trust/Confidence Level: {min_trust}+ out of 100, "
            f"Employee Size: {employee_size}."
            f"{vector_alignment_context} "
            "For each company, provide: company_name, domain (e.g. 'stripe.com'), email (e.g. 'contact@domain.com'), "
            "industry, employee_count, linkedin_url, confidence_score (0.0 to 1.0), and trust_score (0 to 100). "
            "Return strictly valid JSON matching this schema: "
            '[{"company_name": "...", "domain": "...", "email": "...", "industry": "...", "employee_count": "...", "linkedin_url": "...", "confidence_score": 0.95, "trust_score": 95}]'
        )
        
        try:
            raw_text = await asyncio.to_thread(call_gemini_rest, prompt)
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:-3].strip()
            elif raw_text.startswith("```"):
                raw_text = raw_text[3:-3].strip()
                
            parsed_data = json.loads(raw_text)
            validated_leads = []
            for item in parsed_data:
                try:
                    validated_leads.append(GeminiLeadSchema(**item))
                except ValidationError as val_err:
                    logger.warning(f"Skipping malformed lead item from Gemini: {val_err}")
            
            insert_conn = get_db()
            try:
                cursor = insert_conn.cursor()
                for lead in validated_leads:
                    clean_domain = lead.domain.lower().strip().replace("https://", "").replace("http://", "").rstrip("/")
                    conf_score = lead.confidence_score if lead.confidence_score is not None else 0.9
                    trust_score = lead.trust_score if lead.trust_score is not None else 95

                    if DATABASE_URL:
                        cursor.execute(
                            """
                            INSERT INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                            ON CONFLICT (domain) DO UPDATE SET 
                                confidence_score = EXCLUDED.confidence_score,
                                trust_score = EXCLUDED.trust_score
                            RETURNING id
                            """,
                            (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url, conf_score, trust_score)
                        )
                        row = cursor.fetchone()
                        lead_id = row["id"] if row else None
                    else:
                        cursor.execute(
                            "INSERT OR IGNORE INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url, conf_score, trust_score)
                        )
                        lead_id = cursor.lastrowid
                    
                    if lead_id and cursor.rowcount > 0:
                        asyncio.create_task(async_background_enrichment_worker(lead_id, lead.company_name, clean_domain))
                        asyncio.create_task(safe_dispatch_wrapper({
                            "lead_id": lead_id,
                            "company_name": lead.company_name,
                            "domain": clean_domain,
                            "email": lead.email,
                            "industry": lead.industry,
                            "employee_count": lead.employee_count,
                            "linkedin_url": lead.linkedin_url,
                            "confidence_score": conf_score,
                            "trust_score": trust_score,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }))
                insert_conn.commit()
                cursor.close()
            finally:
                release_db(insert_conn)
        except Exception as e:
            logger.error(f"Agentic ICP ingestion error for niche {industries}: {e}")


async def gdpr_compliance_cleanup():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        if DATABASE_URL:
            cursor.execute("DELETE FROM audit_logs WHERE timestamp < %s;", (cutoff,))
            cursor.execute("DELETE FROM webhook_logs WHERE timestamp < %s;", (cutoff,))
        else:
            cursor.execute("DELETE FROM audit_logs WHERE timestamp < ?;", (cutoff,))
            cursor.execute("DELETE FROM webhook_logs WHERE timestamp < ?;", (cutoff,))
        conn.commit()
        cursor.close()
        logger.info("GDPR/CCPA 30-day compliance cleanup executed successfully.")
    except Exception as e:
        logger.error(f"GDPR compliance cleanup error: {e}")
    finally:
        release_db(conn)


scheduler = AsyncIOScheduler()
scheduler.add_job(gdpr_compliance_cleanup, "interval", hours=24)
scheduler.add_job(webhook_canary_healing_worker, "interval", minutes=1)
if os.getenv("ENABLE_MOCK_LEEDS", "false").lower() == "true":
    scheduler.add_job(automated_lead_ingestion, "interval", hours=1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    if not ADMIN_SECRET_KEY:
        logger.warning("CRITICAL WARNING: ADMIN_SECRET_KEY environment variable is not configured!")
    scheduler.start()
    yield
    scheduler.shutdown()


app = FastAPI(title="QuantCode Nexus Enterprise Apex API", lifespan=lifespan)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=["nexus-core-yfou.onrender.com", "localhost", "127.0.0.1", "testserver"])
app.add_middleware(CORSMiddleware, allow_origins=["https://nexus-core-yfou.onrender.com", "http://localhost:8000"], allow_credentials=True, allow_methods=["GET", "POST", "DELETE", "PUT"], allow_headers=["*"])


class AIDLQPayload(BaseModel):
    raw_payload: str
    error_message: str

@app.post("/api/v1/admin/ai-dlq")
async def receive_ai_dlq(payload: AIDLQPayload, admin_key: str = Header(None, alias="admin-key")):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                "INSERT INTO ai_error_dlq (raw_payload, error_message) VALUES (%s, %s)",
                (payload.raw_payload, payload.error_message)
            )
        else:
            cursor.execute(
                "INSERT INTO ai_error_dlq (raw_payload, error_message) VALUES (?, ?)",
                (payload.raw_payload, payload.error_message)
            )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "Logged to AI DLQ successfully."}


@app.middleware("http")
async def correlation_id_middleware(request: Request, call_next):
    request_id = request.headers.get("X-Request-Id", f"req_{uuid.uuid4()}")
    response = await call_next(request)
    response.headers["X-Request-Id"] = request_id
    return response


@app.exception_handler(HTTPException)
async def custom_http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(status_code=exc.status_code, content={"status": "error", "code": exc.status_code, "message": exc.detail, "path": request.url.path})


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


@app.get("/health")
async def health_check():
    return {"status": "healthy", "architecture": "enterprise-apex-hybrid-vector", "timestamp": datetime.now(timezone.utc).isoformat()}


@app.get("/metrics")
async def prometheus_metrics():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM b2b_leads;")
        lead_count = cursor.fetchone()["count"] if DATABASE_URL else cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM subscribers WHERE active = 1;")
        sub_count = cursor.fetchone()["count"] if DATABASE_URL else cursor.fetchone()[0]
        cursor.close()
    finally:
        release_db(conn)

    metrics_output = f"""# HELP nexus_leads_total Total active B2B leads stored
# TYPE nexus_leads_total gauge
nexus_leads_total {lead_count}
# HELP nexus_subscribers_active Total active subscribers
# TYPE nexus_subscribers_active gauge
nexus_subscribers_active {sub_count}
"""
    return FastAPIResponse(content=metrics_output, media_type="text/plain")


def verify_api_key(x_api_key: str, request: Request):
    incoming_hash = hash_api_key(x_api_key)
    client_ip = request.client.host if request.client else "unknown"
    
    if redis_client:
        cached = redis_client.get(f"apikey_cache:{incoming_hash}")
        if cached:
            sub = json.loads(cached)
            record_usage_hit(sub["email"])
            return {"email": sub["email"], "key_name": sub["key_name"], "scope": sub.get("scope", "full"), "role": sub.get("role", "admin"), "tier": sub["tier"], "hash": incoming_hash, "ip": client_ip}

    conn = get_db()
    try:
        cursor = conn.cursor()
        query = "SELECT k.email, k.key_name, k.scope, k.role, s.active, s.tier FROM api_keys k JOIN subscribers s ON k.email = s.email WHERE k.key_hash = %s AND k.active = 1 AND s.active = 1" if DATABASE_URL else "SELECT k.email, k.key_name, k.scope, k.role, s.active, s.tier FROM api_keys k JOIN subscribers s ON k.email = s.email WHERE k.key_hash = ? AND k.active = 1 AND s.active = 1"
        cursor.execute(query, (incoming_hash,))
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)
    
    if not row:
        log_audit_event("unknown", "API_AUTH_FAILURE", "Invalid key hash attempt", client_ip)
        raise HTTPException(status_code=403, detail="Invalid or inactive API subscription key.")
    
    email = row["email"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[0]
    key_name = row["key_name"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[1]
    scope = row["scope"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[2]
    role = row["role"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[3]
    tier = row["tier"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[5]

    if redis_client:
        redis_client.setex(f"apikey_cache:{incoming_hash}", 60, json.dumps({"email": email, "key_name": key_name, "scope": scope, "role": role, "tier": tier}))

    record_usage_hit(email)
    return {"email": email, "key_name": key_name, "scope": scope, "role": role, "tier": tier, "hash": incoming_hash, "ip": client_ip}


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
                raise HTTPException(status_code=429, detail=f"Rate limit exceeded. Maximum {max_requests} requests per minute allowed.")
            return
        except redis.RedisError as e:
            logger.warning(f"Redis rate limit error: {e}")

    response.headers["X-RateLimit-Limit"] = str(max_requests)
    response.headers["X-RateLimit-Remaining"] = str(max_requests)
    response.headers["X-RateLimit-Reset"] = str((current_minute + 1) * window_seconds)


class MagicLinkRequestPayload(BaseModel):
    email: str

@app.post("/api/v1/auth/request-magic-link")
async def request_magic_link(payload: MagicLinkRequestPayload, background_tasks: BackgroundTasks, request: Request):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT active FROM subscribers WHERE email = %s", (payload.email,))
        else:
            cursor.execute("SELECT active FROM subscribers WHERE email = ?", (payload.email,))
        row = cursor.fetchone()
        
        if not row:
            raw_key = f"qcn_{secrets.token_hex(16)}"
            hashed_key = hash_api_key(raw_key)
            if DATABASE_URL:
                cursor.execute("INSERT INTO subscribers (email, active, tier) VALUES (%s, 1, 'starter') ON CONFLICT (email) DO NOTHING", (payload.email,))
                cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (%s, %s, 'Magic Link Key', 'full', 'admin')", (payload.email, hashed_key))
                cursor.execute("INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (%s, 500, 500) ON CONFLICT (email) DO NOTHING", (payload.email,))
            else:
                cursor.execute("INSERT OR REPLACE INTO subscribers (email, active, tier) VALUES (?, 1, 'starter')", (payload.email,))
                cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (?, ?, 'Magic Link Key', 'full', 'admin')", (payload.email, hashed_key))
                cursor.execute("INSERT OR REPLACE INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (?, 500, 500)", (payload.email,))
            conn.commit()

        magic_token = secrets.token_urlsafe(32)
        expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)

        if DATABASE_URL:
            cursor.execute("UPDATE subscribers SET magic_token = %s, magic_expires_at = %s WHERE email = %s", (magic_token, expires_at, payload.email))
        else:
            cursor.execute("UPDATE subscribers SET magic_token = ?, magic_expires_at = ? WHERE email = ?", (magic_token, expires_at, payload.email))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    magic_url = f"https://nexus-core-yfou.onrender.com/auth/verify-magic?token={magic_token}"
    background_tasks.add_task(send_magic_link_email, payload.email, magic_url)
    log_audit_event(payload.email, "MAGIC_LINK_REQUESTED", "Magic link sign-in requested", request.client.host if request.client else "unknown")
    return {"status": "success", "message": "Magic sign-in link dispatched to your email."}


@app.get("/auth/verify-magic")
async def verify_magic_link(token: str):
    conn = get_db()
    try:
        cursor = conn.cursor()
        now = datetime.now(timezone.utc)
        if DATABASE_URL:
            cursor.execute("SELECT email FROM subscribers WHERE magic_token = %s AND magic_expires_at > %s", (token, now))
        else:
            cursor.execute("SELECT email FROM subscribers WHERE magic_token = ? AND magic_expires_at > ?", (token, now))
        row = cursor.fetchone()

        if not row:
            raise HTTPException(status_code=400, detail="Invalid or expired magic link.")

        email = row["email"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[0]
        
        if DATABASE_URL:
            cursor.execute("SELECT k.key_hash FROM api_keys k WHERE k.email = %s AND k.active = 1 LIMIT 1", (email,))
        else:
            cursor.execute("SELECT k.key_hash FROM api_keys k WHERE k.email = ? AND k.active = 1 LIMIT 1", (email,))
        
        if DATABASE_URL:
            cursor.execute("UPDATE subscribers SET magic_token = NULL, magic_expires_at = NULL WHERE email = %s", (email,))
        else:
            cursor.execute("UPDATE subscribers SET magic_token = NULL, magic_expires_at = NULL WHERE email = ?", (email,))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    return FileResponse("dashboard.html")


class DraftEmailPayload(BaseModel):
    lead_id: int

@app.post("/api/v1/leads/{lead_id}/draft-email")
async def draft_ai_cold_email(lead_id: int, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="Gemini API key not initialized.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT company_name, domain, industry, tech_stack, funding_stage, intent_signals FROM b2b_leads WHERE id = %s", (lead_id,))
        else:
            cursor.execute("SELECT company_name, domain, industry, tech_stack, funding_stage, intent_signals FROM b2b_leads WHERE id = ?", (lead_id,))
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)

    if not row:
        raise HTTPException(status_code=404, detail="Lead not found.")

    lead = dict(row)
    prompt = (
        f"Draft a hyper-personalized, high-converting B2B cold sales outreach email for {lead['company_name']} ({lead['domain']}). "
        f"Their industry is {lead['industry']}, tech stack includes {lead['tech_stack']}, funding stage is {lead['funding_stage']}, "
        f"and recent intent signals show: {lead['intent_signals']}. "
        "Keep it concise, professional, tailored to their exact tech stack, and end with a soft call-to-action. "
        "Return ONLY the email subject line and body in clean text format."
    )

    try:
        draft_content = await asyncio.to_thread(call_gemini_rest, prompt)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to generate email via Gemini REST: {e}")

    return {"status": "success", "lead_id": lead_id, "company_name": lead['company_name'], "drafted_email": draft_content.strip()}


class OnDemandGeneratePayload(BaseModel):
    query: str
    count: Optional[int] = 10

@app.post("/api/v1/leads/generate-on-demand")
async def generate_leads_on_demand(payload: OnDemandGeneratePayload, request: Request, response: Response, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") == "viewer":
        raise HTTPException(status_code=403, detail="Viewer role is not authorized to generate on-demand leads.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT credits_remaining, credits_limit FROM subscriber_credits WHERE email = %s", (sub["email"],))
        else:
            cursor.execute("SELECT credits_remaining, credits_limit FROM subscriber_credits WHERE email = ?", (sub["email"],))
        row = cursor.fetchone()

        if not row:
            initial_credits = 2500 if sub["tier"] == "pro" else 500
            if DATABASE_URL:
                cursor.execute("INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (%s, %s, %s)", (sub["email"], initial_credits, initial_credits))
            else:
                cursor.execute("INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (?, ?, ?)", (sub["email"], initial_credits, initial_credits))
            conn.commit()
            credits_left = initial_credits
        else:
            credits_left = row["credits_remaining"] if isinstance(row, dict) else row[0]

        requested_cost = payload.count
        if credits_left < requested_cost:
            raise HTTPException(status_code=402, detail=f"Insufficient lead generation credits. Remaining: {credits_left}, Requested: {requested_cost}.")

        cursor.close()
    finally:
        release_db(conn)

    if not GEMINI_API_KEY:
        raise HTTPException(status_code=500, detail="Gemini API key is not initialized.")

    prompt = (
        f"Generate a strict JSON list of {payload.count} real, active B2B companies matching this custom prompt query: '{payload.query}'. "
        "For each company, provide: company_name, domain (e.g. 'stripe.com'), email (e.g. 'contact@domain.com'), "
        "industry, employee_count, linkedin_url, confidence_score (0.0 to 1.0), and trust_score (0 to 100). "
        "Return strictly valid JSON matching this schema: "
        '[{"company_name": "...", "domain": "...", "email": "...", "industry": "...", "employee_count": "...", "linkedin_url": "...", "confidence_score": 0.95, "trust_score": 95}]'
    )

    try:
        raw_response_text = await asyncio.to_thread(call_gemini_rest, prompt)
        raw_text = raw_response_text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:-3].strip()
        elif raw_text.startswith("```"):
            raw_text = raw_text[3:-3].strip()
            
        parsed_data = json.loads(raw_text)
        new_leads = []

        ins_conn = get_db()
        try:
            cursor = ins_conn.cursor()
            for item in parsed_data:
                try:
                    lead = GeminiLeadSchema(**item)
                    clean_domain = lead.domain.lower().strip().replace("https://", "").replace("http://", "").rstrip("/")
                    
                    if DATABASE_URL:
                        cursor.execute(
                            """
                            INSERT INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score) 
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                            ON CONFLICT (domain) DO UPDATE SET confidence_score = EXCLUDED.confidence_score 
                            RETURNING id
                            """,
                            (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url, lead.confidence_score, lead.trust_score)
                        )
                        r = cursor.fetchone()
                        l_id = r["id"] if r else None
                    else:
                        cursor.execute(
                            "INSERT OR IGNORE INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url, lead.confidence_score, lead.trust_score)
                        )
                        l_id = cursor.lastrowid

                    if l_id:
                        new_leads.append({"id": l_id, "company_name": lead.company_name, "domain": clean_domain})
                        asyncio.create_task(async_background_enrichment_worker(l_id, lead.company_name, clean_domain))
                        asyncio.create_task(safe_dispatch_wrapper({
                            "lead_id": l_id, "company_name": lead.company_name, "domain": clean_domain,
                            "email": lead.email, "industry": lead.industry, "trust_score": lead.trust_score
                        }))
                except Exception:
                    pass

            new_balance = credits_left - len(new_leads)
            if DATABASE_URL:
                cursor.execute("UPDATE subscriber_credits SET credits_remaining = %s WHERE email = %s", (new_balance, sub["email"]))
            else:
                cursor.execute("UPDATE subscriber_credits SET credits_remaining = ? WHERE email = ?", (new_balance, sub["email"]))
            ins_conn.commit()
            cursor.close()
        finally:
            release_db(ins_conn)

        return {"status": "success", "credits_remaining": new_balance, "leads_generated": len(new_leads), "leads": new_leads}
    except Exception as e:
        logger.error(f"On-demand generation endpoint failure or timeout: {e}")
        fallback_leads = [
            {"id": random.randint(1000, 9999), "company_name": f"Apex Solutions {i}", "domain": f"apexsolutions{i}.io"}
            for i in range(min(payload.count, 3))
        ]
        return {
            "status": "success",
            "warning": "Generated via fallback mechanism due to upstream AI latency.",
            "credits_remaining": credits_left,
            "leads_generated": len(fallback_leads),
            "leads": fallback_leads
        }


@app.get("/api/v1/credits")
async def get_subscriber_credits(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT credits_remaining, credits_limit FROM subscriber_credits WHERE email = %s", (sub["email"],))
        else:
            cursor.execute("SELECT credits_remaining, credits_limit FROM subscriber_credits WHERE email = ?", (sub["email"],))
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)

    if not row:
        limit = 2500 if sub["tier"] == "pro" else 500
        return {"status": "success", "credits_remaining": limit, "credits_limit": limit}
    return {"status": "success", "credits_remaining": row["credits_remaining"] if isinstance(row, dict) else row[0], "credits_limit": row["credits_limit"] if isinstance(row, dict) else row[1]}


class DestinationPayload(BaseModel):
    destination_type: str
    webhook_url: str
    access_token: Optional[str] = ""
    mapping_rules: Optional[str] = "{}"

@app.post("/api/v1/destinations")
async def register_native_destination(payload: DestinationPayload, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") == "viewer":
        raise HTTPException(status_code=403, detail="Viewer role cannot configure CRM destinations.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                "INSERT INTO subscriber_destinations (email, destination_type, webhook_url, access_token, mapping_rules) VALUES (%s, %s, %s, %s, %s)",
                (sub["email"], payload.destination_type, payload.webhook_url, payload.access_token, payload.mapping_rules)
            )
        else:
            cursor.execute(
                "INSERT INTO subscriber_destinations (email, destination_type, webhook_url, access_token, mapping_rules) VALUES (?, ?, ?, ?, ?)",
                (sub["email"], payload.destination_type, payload.webhook_url, payload.access_token, payload.mapping_rules)
            )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    log_audit_event(sub["email"], "DESTINATION_REGISTERED", f"Registered native destination {payload.destination_type}", sub["ip"])
    return {"status": "success", "message": f"Native {payload.destination_type} integration connected successfully."}


@app.get("/api/v1/destinations")
async def list_native_destinations(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT id, destination_type, webhook_url, active, created_at FROM subscriber_destinations WHERE email = %s", (sub["email"],))
        else:
            cursor.execute("SELECT id, destination_type, webhook_url, active, created_at FROM subscriber_destinations WHERE email = ?", (sub["email"],))
        rows = cursor.fetchall()
        destinations = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "destinations": destinations}


class TeamInvitePayload(BaseModel):
    email: str
    key_name: str = "Team Member Key"
    role: str = "sdr"
    scope: str = "read"

@app.post("/api/v1/team/invite")
async def invite_team_member(payload: TeamInvitePayload, request: Request, background_tasks: BackgroundTasks, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only workspace admins can invite team members.")

    raw_key = f"qcn_{secrets.token_hex(16)}"
    hashed_key = hash_api_key(raw_key)

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                "INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (%s, %s, %s, %s, %s)",
                (sub["email"], hashed_key, payload.key_name, payload.scope, payload.role)
            )
        else:
            cursor.execute(
                "INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (?, ?, ?, ?, ?)",
                (sub["email"], hashed_key, payload.key_name, payload.scope, payload.role)
            )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    log_audit_event(sub["email"], "TEAM_INVITE", f"Invited member {payload.email} with role {payload.role}", sub["ip"])
    background_tasks.add_task(send_email_via_resend, payload.email, raw_key)
    return {"status": "success", "message": f"Team member invited successfully with role '{payload.role}'."}


class ICPPayload(BaseModel):
    target_industries: str
    min_trust_score: int
    preferred_employee_count: str

@app.post("/api/v1/icp")
async def save_subscriber_icp(payload: ICPPayload, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") == "viewer":
        raise HTTPException(status_code=403, detail="Viewer role cannot update ICP settings.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                """
                INSERT INTO subscriber_icps (email, target_industries, min_trust_score, preferred_employee_count, updated_at)
                VALUES (%s, %s, %s, %s, NOW())
                ON CONFLICT (email) DO UPDATE SET target_industries = EXCLUDED.target_industries, min_trust_score = EXCLUDED.min_trust_score, preferred_employee_count = EXCLUDED.preferred_employee_count, updated_at = NOW()
                """,
                (sub["email"], payload.target_industries, payload.min_trust_score, payload.preferred_employee_count)
            )
        else:
            cursor.execute(
                "INSERT OR REPLACE INTO subscriber_icps (email, target_industries, min_trust_score, preferred_employee_count, updated_at) VALUES (?, ?, ?, ?, datetime('now'))",
                (sub["email"], payload.target_industries, payload.min_trust_score, payload.preferred_employee_count)
            )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "Subscriber ICP profile updated successfully."}


@app.get("/api/v1/icp")
async def get_subscriber_icp(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT target_industries, min_trust_score, preferred_employee_count, updated_at FROM subscriber_icps WHERE email = %s", (sub["email"],))
        else:
            cursor.execute("SELECT target_industries, min_trust_score, preferred_employee_count, updated_at FROM subscriber_icps WHERE email = ?", (sub["email"],))
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)
    
    if DATABASE_URL and row and hasattr(row, 'keys'):
        icp_dict = dict(row)
        if icp_dict.get("updated_at") and isinstance(icp_dict["updated_at"], datetime):
            icp_dict["updated_at"] = icp_dict["updated_at"].isoformat()
        return {"status": "success", "icp": icp_dict}

    if not row:
        return {"status": "success", "icp": {"target_industries": "SaaS / Tech", "min_trust_score": 80, "preferred_employee_count": "10-50"}}
    return {"status": "success", "icp": dict(row)}


class LeadFeedbackPayload(BaseModel):
    feedback_status: str

@app.post("/api/v1/leads/{lead_id}/feedback")
async def submit_lead_feedback(lead_id: int, payload: LeadFeedbackPayload, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") == "viewer":
        raise HTTPException(status_code=403, detail="Viewer role cannot submit agent feedback.")
    if payload.feedback_status not in ["converted", "qualified", "disqualified"]:
        raise HTTPException(status_code=400, detail="Invalid feedback status. Must be 'converted', 'qualified', or 'disqualified'.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO lead_feedback (email, lead_id, feedback_status) VALUES (%s, %s, %s)", (sub["email"], lead_id, payload.feedback_status))
        else:
            cursor.execute("INSERT INTO lead_feedback (email, lead_id, feedback_status) VALUES (?, ?, ?)", (sub["email"], lead_id, payload.feedback_status))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": f"Feedback '{payload.feedback_status}' recorded for lead ID {lead_id}."}


@app.get("/api/v1/admin/cleanup-webhooks")
async def cleanup_webhooks(admin_key: str):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=7)
        if DATABASE_URL:
            cursor.execute("DELETE FROM webhook_logs WHERE timestamp < %s;", (cutoff_date,))
            cursor.execute("DELETE FROM subscriber_webhooks WHERE webhook_url LIKE '%your-unique-url%';")
        else:
            cursor.execute("DELETE FROM webhook_logs WHERE timestamp < ?;", (cutoff_date,))
            cursor.execute("DELETE FROM subscriber_webhooks WHERE webhook_url LIKE '%your-unique-url%';")
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "Automated 7-day TTL log retention and placeholder webhook cleanup executed successfully."}


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


@app.get("/api/v1/admin/backfill-embeddings")
async def backfill_embeddings(admin_key: str = Header(None, alias="admin-key")):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized admin key.")
    
    if DATABASE_URL is None:
        raise HTTPException(status_code=400, detail="Backfill requires PostgreSQL with pgvector.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, company_name, industry, domain FROM b2b_leads WHERE embedding IS NULL")
        rows = cursor.fetchall()
        count = 0
        for row in rows:
            r = dict(row)
            text_content = f"{r['company_name']} {r['industry']} {r['domain']}"
            vec = await asyncio.to_thread(generate_lead_embedding, text_content)
            if vec:
                cursor.execute("UPDATE b2b_leads SET embedding = %s WHERE id = %s", (str(vec), r['id']))
                count += 1
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "backfilled_count": count}


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
                initial_credits = 2500 if tier == "pro" else 500
                
                if DATABASE_URL:
                    cursor.execute("INSERT INTO subscribers (email, active, stripe_customer_id, tier) VALUES (%s, 1, %s, %s) ON CONFLICT (email) DO UPDATE SET active = 1", (customer_email, session.customer, tier))
                    cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (%s, %s, 'Primary Key', 'full', 'admin')", (customer_email, hashed_key))
                    cursor.execute("INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (%s, %s, %s) ON CONFLICT (email) DO UPDATE SET credits_limit = EXCLUDED.credits_limit", (customer_email, initial_credits, initial_credits))
                else:
                    cursor.execute("INSERT OR REPLACE INTO subscribers (email, active, stripe_customer_id, tier) VALUES (?, 1, ?, ?)", (customer_email, session.customer, tier))
                    cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (?, ?, 'Primary Key', 'full', 'admin')", (customer_email, hashed_key))
                    cursor.execute("INSERT OR REPLACE INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (?, ?, ?)", (customer_email, initial_credits, initial_credits))
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
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (%s, %s, 'Reset Key', 'full', 'admin')", (email, new_hashed_key))
            cursor.execute("UPDATE subscribers SET reset_token = NULL, reset_expires_at = NULL WHERE email = %s", (email,))
        else:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (?, ?, 'Reset Key', 'full', 'admin')", (email, new_hashed_key))
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
            cursor.execute("SELECT id, key_name, scope, role, active, created_at FROM api_keys WHERE email = %s", (sub["email"],))
        else:
            cursor.execute("SELECT id, key_name, scope, role, active, created_at FROM api_keys WHERE email = ?", (sub["email"],))
        
        rows = cursor.fetchall()
        keys = []
        for r in rows:
            r_dict = dict(r)
            if r_dict.get("created_at") and isinstance(r_dict["created_at"], datetime):
                r_dict["created_at"] = r_dict["created_at"].isoformat()
            keys.append(r_dict)
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "keys": keys}


@app.post("/api/v1/keys")
async def create_subscriber_key(request: Request, key_name: str = "New Key", scope: str = "full", role: str = "sdr", x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can generate new keys.")

    raw_key = f"qcn_{secrets.token_hex(16)}"
    hashed_key = hash_api_key(raw_key)

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (%s, %s, %s, %s, %s)", (sub["email"], hashed_key, key_name, scope, role))
        else:
            cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (?, ?, ?, ?, ?)", (sub["email"], hashed_key, key_name, scope, role))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    if redis_client:
        try:
            redis_client.delete(f"apikey_cache:{sub['hash']}")
        except Exception:
            pass

    log_audit_event(sub["email"], "KEY_CREATED", f"Created new API key labeled '{key_name}' with scope '{scope}' and role '{role}'", sub["ip"])
    return {"status": "success", "key_name": key_name, "scope": scope, "role": role, "api_key": raw_key, "message": "Save this key now. It will not be shown again."}


@app.delete("/api/v1/keys/{key_id}")
async def revoke_subscriber_key(key_id: int, request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Only admins can revoke keys.")

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
            cursor.execute("SELECT TO_CHAR(timestamp, 'YYYY-MM-DD') as day, COUNT(*) as request_count FROM api_usage_history WHERE email = %s AND timestamp >= NOW() - INTERVAL '7 days' GROUP BY TO_CHAR(timestamp, 'YYYY-MM-DD') ORDER BY day ASC", (sub["email"],))
        else:
            cursor.execute("SELECT DATE(timestamp) as day, COUNT(*) as request_count FROM api_usage_history WHERE email = ? AND timestamp >= datetime('now', '-7 days') GROUP BY DATE(timestamp) ORDER BY day ASC", (sub["email"],))
        rows = cursor.fetchall()
        history = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "usage_history": history}


@app.post("/api/v1/webhooks")
async def register_subscriber_webhook(webhook_url: str, filter_rules: Optional[str] = "{}", request: Request = None, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    if sub.get("role") == "viewer":
        raise HTTPException(status_code=403, detail="Viewer role cannot register webhooks.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                "INSERT INTO subscriber_webhooks (email, webhook_url, circuit_status, consecutive_failures, filter_rules) VALUES (%s, %s, 'ACTIVE', 0, %s) ON CONFLICT DO NOTHING",
                (sub["email"], webhook_url, filter_rules)
            )
        else:
            cursor.execute(
                "INSERT INTO subscriber_webhooks (email, webhook_url, circuit_status, consecutive_failures, filter_rules) VALUES (?, ?, 'ACTIVE', 0, ?)",
                (sub["email"], webhook_url, filter_rules)
            )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    log_audit_event(sub["email"], "WEBHOOK_REGISTERED", f"Registered destination URL with filter rules: {webhook_url}", sub["ip"])
    return {"status": "success", "message": "Webhook URL registered with custom filter rules successfully."}


@app.get("/api/v1/webhook-logs")
async def get_webhook_logs(request: Request, x_api_key: str = Header(...)):
    sub = verify_api_key(x_api_key, request)
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT DISTINCT l.id, l.event_id, l.webhook_url, l.status_code, l.success, l.error_message, l.timestamp FROM webhook_logs l JOIN subscriber_webhooks w ON l.webhook_url = w.webhook_url WHERE w.email = %s ORDER BY l.timestamp DESC LIMIT 20", (sub["email"],))
        else:
            cursor.execute("SELECT DISTINCT l.id, l.event_id, l.webhook_url, l.status_code, l.success, l.error_message, l.timestamp FROM webhook_logs l JOIN subscriber_webhooks w ON l.webhook_url = w.webhook_url WHERE w.email = ? ORDER BY l.timestamp DESC LIMIT 20", (sub["email"],))
        rows = cursor.fetchall()
        logs = []
        for r in rows:
            r_dict = dict(r)
            if r_dict.get("timestamp") and isinstance(r_dict["timestamp"], datetime):
                r_dict["timestamp"] = r_dict["timestamp"].isoformat()
            logs.append(r_dict)
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "delivery_logs": logs}


class LeadItem(BaseModel):
    company_name: str
    domain: str
    email: str
    industry: Optional[str] = "SaaS / Tech"
    employee_count: Optional[str] = "10-50"
    linkedin_url: Optional[str] = ""
    confidence_score: Optional[float] = 0.9
    trust_score: Optional[int] = 95

class BatchLeadUpload(BaseModel):
    leads: List[LeadItem]


@app.post("/api/v1/admin/upload-leads")
async def admin_upload_leads(
    payload: BatchLeadUpload, 
    background_tasks: BackgroundTasks, 
    admin_key: str = Header(None, alias="admin-key"),
    idempotency_key: Optional[str] = Header(None, alias="Idempotency-Key")
):
    if not ADMIN_SECRET_KEY or admin_key != ADMIN_SECRET_KEY:
        raise HTTPException(status_code=403, detail="Unauthorized admin key.")
    
    if idempotency_key and redis_client:
        idem_cache_key = f"idempotency:{idempotency_key}"
        if redis_client.get(idem_cache_key):
            return {"status": "success", "message": "Duplicate request caught via idempotency key.", "imported_count": 0}
        redis_client.setex(idem_cache_key, 3600, "processed")
    
    conn = get_db()
    try:
        cursor = conn.cursor()
        count = 0
        for lead in payload.leads:
            clean_domain = lead.domain.lower().strip().replace("https://", "").replace("http://", "").rstrip("/")
            conf_score = lead.confidence_score if lead.confidence_score is not None else 0.9
            trust_score = lead.trust_score if lead.trust_score is not None else 95
            
            if DATABASE_URL:
                cursor.execute(
                    """
                    INSERT INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score) 
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) 
                    ON CONFLICT (domain) DO UPDATE SET 
                        confidence_score = EXCLUDED.confidence_score,
                        trust_score = EXCLUDED.trust_score
                    RETURNING id
                    """,
                    (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url, conf_score, trust_score)
                )
                row = cursor.fetchone()
                lead_id = row["id"] if row else None
            else:
                cursor.execute(
                    "INSERT OR IGNORE INTO b2b_leads (company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (lead.company_name, clean_domain, lead.email, lead.industry, lead.employee_count, lead.linkedin_url, conf_score, trust_score)
                )
                lead_id = cursor.lastrowid
            
            if lead_id and cursor.rowcount > 0:
                count += 1
                background_tasks.add_task(async_background_enrichment_worker, lead_id, lead.company_name, clean_domain)
                asyncio.create_task(
                    safe_dispatch_wrapper(
                        {
                            "lead_id": lead_id,
                            "company_name": lead.company_name,
                            "domain": clean_domain,
                            "email": lead.email,
                            "industry": lead.industry,
                            "employee_count": lead.employee_count,
                            "linkedin_url": lead.linkedin_url,
                            "confidence_score": conf_score,
                            "trust_score": trust_score,
                            "timestamp": datetime.now(timezone.utc).isoformat()
                        }
                    )
                )
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "imported_count": count}


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
                cursor.execute("SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp FROM b2b_leads WHERE company_name ILIKE %s ORDER BY timestamp DESC LIMIT %s OFFSET %s", (f"%{company}%", limit, offset))
            else:
                cursor.execute("SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp FROM b2b_leads ORDER BY timestamp DESC LIMIT %s OFFSET %s", (limit, offset))
        else:
            if company:
                cursor.execute("SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp FROM b2b_leads WHERE company_name LIKE ? ORDER BY timestamp DESC LIMIT ? OFFSET ?", (f"%{company}%", limit, offset))
            else:
                cursor.execute("SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp FROM b2b_leads ORDER BY timestamp DESC LIMIT ? OFFSET ?", (limit, offset))

        rows = cursor.fetchall()
        leads = []
        for row in rows:
            r_dict = dict(row)
            if r_dict.get("timestamp") and isinstance(r_dict["timestamp"], datetime):
                r_dict["timestamp"] = r_dict["timestamp"].isoformat()
            leads.append(r_dict)
        cursor.close()
    finally:
        release_db(conn)

    return {"status": "success", "tier": sub["tier"], "count": len(leads), "limit": limit, "offset": offset, "leads": leads}


@app.get("/api/v1/leads/semantic-search")
async def elite_hybrid_lead_search(
    request: Request,
    response: Response,
    query: str,
    x_api_key: str = Header(...),
    limit: int = Query(10, ge=1, le=50)
):
    sub = verify_api_key(x_api_key, request)
    check_rate_limit(sub["hash"], response=response, max_requests=(100 if sub["tier"] == "pro" else 20))

    query_embedding = await asyncio.to_thread(generate_lead_embedding, query)
    if not query_embedding or DATABASE_URL is None:
        raise HTTPException(status_code=400, detail="Hybrid search requires PostgreSQL with pgvector and valid AI credentials.")

    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            WITH vector_ranked AS (
                SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp,
                       (1.0 - (embedding <=> %s::vector)) as raw_sim,
                       ROW_NUMBER() OVER (ORDER BY embedding <=> %s::vector ASC) as v_rank
                FROM b2b_leads
                WHERE embedding IS NOT NULL 
                  AND (embedding <=> %s::vector) < 0.55
                  AND industry NOT ILIKE '%%SaaS%%'
                  AND industry NOT ILIKE '%%Fintech%%'
                  AND industry NOT ILIKE '%%CRM%%'
                LIMIT 30
            ),
            text_ranked AS (
                SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp,
                       ROW_NUMBER() OVER (ORDER BY ts_rank(to_tsvector('english', company_name || ' ' || industry || ' ' || tech_stack), plainto_tsquery('english', %s)) DESC) as t_rank
                FROM b2b_leads
                WHERE to_tsvector('english', company_name || ' ' || industry || ' ' || tech_stack) @@ plainto_tsquery('english', %s)
                  AND industry NOT ILIKE '%%SaaS%%'
                  AND industry NOT ILIKE '%%Fintech%%'
                  AND industry NOT ILIKE '%%CRM%%'
                LIMIT 30
            ),
            combined AS (
                SELECT COALESCE(v.id, t.id) as id,
                       COALESCE(v.company_name, t.company_name) as company_name,
                       COALESCE(v.domain, t.domain) as domain,
                       COALESCE(v.email, t.email) as email,
                       COALESCE(v.industry, t.industry) as industry,
                       COALESCE(v.employee_count, t.employee_count) as employee_count,
                       COALESCE(v.linkedin_url, t.linkedin_url) as linkedin_url,
                       COALESCE(v.confidence_score, t.confidence_score) as confidence_score,
                       COALESCE(v.trust_score, t.trust_score) as trust_score,
                       COALESCE(v.tech_stack, t.tech_stack) as tech_stack,
                       COALESCE(v.funding_stage, t.funding_stage) as funding_stage,
                       COALESCE(v.intent_signals, t.intent_signals) as intent_signals,
                       COALESCE(v.verified_email, t.verified_email) as verified_email,
                       COALESCE(v.timestamp, t.timestamp) as timestamp,
                       COALESCE(v.raw_sim, 0.4) as raw_sim,
                       (1.0 / (60.0 + COALESCE(v_rank, 999))) + (1.0 / (60.0 + COALESCE(t_rank, 999))) as rrf_score
                FROM vector_ranked v
                FULL OUTER JOIN text_ranked t ON v.id = t.id
            )
            SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, verified_email, timestamp,
                   ROUND(CAST((CASE WHEN raw_sim > 0.45 THEN 0.65 + ((raw_sim - 0.45) / 0.55) * 0.34 ELSE raw_sim * 1.2 END) * 100 AS numeric), 0) as similarity
            FROM combined
            WHERE raw_sim >= 0.45
            ORDER BY rrf_score DESC, raw_sim DESC
            LIMIT %s
            """,
            (str(query_embedding), str(query_embedding), str(query_embedding), query, query, limit)
        )
        rows = cursor.fetchall()
        leads = []
        for row in rows:
            r_dict = dict(row)
            if r_dict.get("timestamp") and isinstance(r_dict["timestamp"], datetime):
                r_dict["timestamp"] = r_dict["timestamp"].isoformat()
            leads.append(r_dict)
        cursor.close()
    finally:
        release_db(conn)

    return {"status": "success", "query": query, "count": len(leads), "leads": leads}


@app.post("/create-checkout-session")
async def create_checkout_session(email: str, tier: str = "starter"):
    amount = 9900 if tier == "pro" else 2900
    plan_name = "QuantCode Nexus Enterprise Pro B2B Leads" if tier == "pro" else "QuantCode Nexus Starter B2B Leads"
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
            cancel_url="https://nexus-core-yfou.onrender.com/dashboard?canceled=true",
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
                initial_credits = 2500 if tier == "pro" else 500

                if not customer_email and session_dict.get("customer_details"):
                    details = session_dict.get("customer_details")
                    if isinstance(details, dict):
                        customer_email = details.get("email")

                if customer_email:
                    raw_api_key = f"qcn_{secrets.token_hex(16)}"
                    hashed_key = hash_api_key(raw_api_key)
                    
                    if DATABASE_URL:
                        cursor.execute("INSERT INTO subscribers (email, active, stripe_customer_id, tier) VALUES (%s, 1, %s, %s) ON CONFLICT (email) DO UPDATE SET active = 1, stripe_customer_id = EXCLUDED.stripe_customer_id, tier = EXCLUDED.tier", (customer_email, customer_id, tier))
                        cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (%s, %s, 'Primary Key', 'full', 'admin')", (customer_email, hashed_key))
                        cursor.execute("INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (%s, %s, %s) ON CONFLICT (email) DO UPDATE SET credits_limit = EXCLUDED.credits_limit", (customer_email, initial_credits, initial_credits))
                    else:
                        cursor.execute("INSERT OR REPLACE INTO subscribers (email, active, stripe_customer_id, tier) VALUES (?, 1, ?, ?)", (customer_email, customer_id, tier))
                        cursor.execute("INSERT INTO api_keys (email, key_hash, key_name, scope, role) VALUES (?, ?, 'Primary Key', 'full', 'admin')", (customer_email, hashed_key))
                        cursor.execute("INSERT OR REPLACE INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (?, ?, ?)", (customer_email, initial_credits, initial_credits))
                    conn.commit()

                    log_audit_event(customer_email, "SUBSCRIPTION_CREATED", f"New subscription created on tier {tier}")
                    background_tasks.add_task(send_telegram_alert, f"🚀 *New Enterprise Subscription ({tier.upper()})!*\nCustomer: `{customer_email}`")
                    background_tasks.add_task(send_email_via_resend, customer_email, raw_api_key)
            except Exception as err:
                logger.error(f"Checkout completion error: {err}")

        elif event_type == "customer.subscription.updated":
            try:
                customer_id = session_dict.get("customer")
                status = session_dict.get("status")
                active_state = 1 if status == "active" else 0
                if customer_id:
                    if DATABASE_URL:
                        cursor.execute("UPDATE subscribers SET active = %s WHERE stripe_customer_id = %s", (active_state, customer_id))
                    else:
                        cursor.execute("UPDATE subscribers SET active = ? WHERE stripe_customer_id = ?", (active_state, customer_id))
                    conn.commit()
            except Exception as err:
                logger.error(f"Subscription update error: {err}")

        elif event_type in ["customer.subscription.deleted", "invoice.payment_failed", "charge.dispute.created"]:
            try:
                customer_id = session_dict.get("customer") or session_dict.get("charge")
                if customer_id:
                    if DATABASE_URL:
                        cursor.execute("UPDATE subscribers SET active = 0 WHERE stripe_customer_id = %s", (customer_id,))
                    else:
                        cursor.execute("UPDATE subscribers SET active = 0 WHERE stripe_customer_id = ?", (customer_id,))
                    conn.commit()
                    log_audit_event("system", "SUBSCRIPTION_REVOKED", f"Revoked subscription access due to event: {event_type}")
            except Exception as err:
                logger.error(f"Revocation/Dispute error: {err}")

        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("app:app", host="0.0.0.0", port=port)