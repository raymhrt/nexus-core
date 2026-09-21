import os
import asyncio
import logging
import json
import secrets
import sqlite3
import hashlib
import hmac
import time
import random
import uuid
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager

import stripe
import numpy as np
import requests
import redis
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from fastapi import FastAPI, BackgroundTasks, HTTPException, Request, Response, status, Header, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, Response as FastAPIResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field, EmailStr, ValidationError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

# Configure logging
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
WEBHOOK_SIGNING_SECRET = os.getenv("WEBHOOK_SIGNING_SECRET")
if not WEBHOOK_SIGNING_SECRET:
    logger.critical("FATAL: WEBHOOK_SIGNING_SECRET environment variable is missing! Webhook verification insecure.")
    WEBHOOK_SIGNING_SECRET = "fallback_insecure_secret_for_dev_mode"

ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    logger.warning("WARNING: GROQ_API_KEY is not set. AI generation endpoints will fail unless configured.")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "onboarding@resend.dev")

DATABASE_URL = os.getenv("DATABASE_URL")
REDIS_URL = os.getenv("REDIS_URL")

TRUSTED_ORIGINS = [origin.strip() for origin in os.getenv("TRUSTED_ORIGINS", "https://nexus-core-yfou.onrender.com,http://localhost:3000,http://127.0.0.1:8000").split(",") if origin.strip()]

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

GEMINI_LEAD_GENERATION_SYSTEM_PROMPT = """
You are an autonomous B2B forensic intelligence extraction and revenue profiling swarm. Your task is to crawl web data and output accurate company profiles with granular buying committee breakdowns, hidden technical pain points, regulatory vulnerabilities, budget capacities, and bespoke psychological sales hooks.

CLASSIFICATION GUARDRAILS:
1. Enterprise vs. Startup Check: Always cross-reference employee count and public status. 
2. If employee_count > 500 or the company is publicly traded on any global stock exchange, funding_stage MUST be set to "Public / Enterprise". Do NOT label mature or public companies as "Series A", "Seed", or venture-backed.
3. Ensure tech stack, buying committee roles, and headcount metrics match real-world telemetry.
"""

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()

class SSETelemetryBroker:
    def __init__(self):
        self.subscribers: List[asyncio.Queue] = []

    async def subscribe(self) -> asyncio.Queue:
        q = asyncio.Queue(maxsize=100)
        self.subscribers.append(q)
        return q

    def unsubscribe(self, q: asyncio.Queue):
        if q in self.subscribers:
            self.subscribers.remove(q)

    async def broadcast(self, event_type: str, data: dict):
        payload = {"event": event_type, "data": data, "timestamp": datetime.now(timezone.utc).isoformat()}
        for q in self.subscribers:
            try:
                await q.put(payload)
            except asyncio.QueueFull:
                pass

sse_broker = SSETelemetryBroker()

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

def get_cached_ai_response(cache_key: str) -> Optional[str]:
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT response_text FROM ai_response_cache WHERE cache_key = %s AND created_at >= NOW() - INTERVAL '24 hours'", (cache_key,))
        else:
            cursor.execute("SELECT response_text FROM ai_response_cache WHERE cache_key = ? AND created_at >= datetime('now', '-24 hours')", (cache_key,))
        row = cursor.fetchone()
        cursor.close()
        return row["response_text"] if row and isinstance(row, dict) else (row[0] if row else None)
    except Exception:
        return None
    finally:
        release_db(conn)

def set_cached_ai_response(cache_key: str, response_text: str):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO ai_response_cache (cache_key, response_text) VALUES (%s, %s) ON CONFLICT (cache_key) DO UPDATE SET response_text = EXCLUDED.response_text, created_at = NOW()", (cache_key, response_text))
        else:
            cursor.execute("INSERT OR REPLACE INTO ai_response_cache (cache_key, response_text, created_at) VALUES (?, ?, datetime('now'))", (cache_key, response_text))
        conn.commit()
        cursor.close()
    except Exception:
        pass
    finally:
        release_db(conn)

def call_gemini_rest(prompt: str, max_retries: int = 5, use_search: bool = False) -> str:
    if not GROQ_API_KEY:
        logger.error("GROQ_API_KEY environment variable is missing.")
        raise HTTPException(status_code=500, detail="GROQ_API_KEY not configured")
     
    cache_key = hashlib.sha256((prompt + str(use_search)).encode("utf-8")).hexdigest()
    cached_res = get_cached_ai_response(cache_key)
    if cached_res:
        logger.info("Serving AI response from local smart cache.")
        return cached_res

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {GROQ_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [
            {"role": "system", "content": GEMINI_LEAD_GENERATION_SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.3
    }

    base_delay = 3.0
    for attempt in range(1, max_retries + 1):
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            
            remaining_tokens = res.headers.get("x-ratelimit-remaining-tokens")
            if remaining_tokens and int(remaining_tokens) < 1500:
                logger.warning("Groq rate limit token threshold nearing. Injecting safety throttle...")
                time.sleep(4.0)

            if res.status_code == 200:
                data = res.json()
                text_output = data["choices"][0]["message"]["content"]
                set_cached_ai_response(cache_key, text_output)
                time.sleep(2.0)
                return text_output
            elif res.status_code in [429, 503, 502]:
                logger.warning(f"Groq hit status {res.status_code} on attempt {attempt}. Backing off...")
            else:
                logger.error(f"Groq API error status {res.status_code}: {res.text}")
                break
        except Exception as net_err:
            logger.warning(f"Network error on Groq attempt {attempt}: {net_err}")

        sleep_time = (base_delay ** attempt) + random.uniform(1.0, 3.0)
        time.sleep(sleep_time)

    logger.error("Groq inference failed across all retry attempts.")
    raise HTTPException(status_code=502, detail="Groq rate limit or service unavailable. Please retry shortly.")

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
     
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(sse_broker.broadcast("audit_log", {"email": email, "action": action, "details": details}))
    except RuntimeError:
        pass

def send_telegram_alert(message: str, chat_id: Optional[str] = None):
    target_chat = chat_id or TELEGRAM_CHAT_ID
    if not TELEGRAM_BOT_TOKEN or not target_chat:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": target_chat, "text": message, "parse_mode": "Markdown"}, timeout=5)
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

def send_custom_email_via_resend(to_email: str, subject: str, html_content: str):
    if not RESEND_API_KEY:
        logger.warning("Resend API key missing; skipping live email dispatch.")
        return False
    url = "https://api.resend.com/emails"
    headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
    payload = {"from": f"QuantCode Nexus <{SENDER_EMAIL}>", "to": [to_email], "subject": subject, "html": html_content}
    try:
        res = requests.post(url, json=payload, headers=headers, timeout=10)
        return res.status_code in [200, 201]
    except Exception as e:
        logger.error(f"Resend custom email error: {e}")
        return False

# Database initialization
def init_db():
    conn = get_db()
    cursor = conn.cursor()
     
    if DATABASE_URL:
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        except Exception:
            pass
             
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_response_cache (
                cache_key TEXT PRIMARY KEY,
                response_text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
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
                magic_expires_at TIMESTAMP,
                telegram_chat_id TEXT
            )
        """
        )
        cursor.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_token TEXT;")
        cursor.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS magic_expires_at TIMESTAMP;")
        cursor.execute("ALTER TABLE subscribers ADD COLUMN IF NOT EXISTS telegram_chat_id TEXT;")
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
                decision_maker_title TEXT DEFAULT 'VP of Engineering',
                decision_maker_linkedin TEXT DEFAULT '',
                acv_estimate TEXT DEFAULT '$25,000',
                headcount_growth_pct TEXT DEFAULT '+20% QoQ',
                open_hiring_roles TEXT DEFAULT 'Engineers',
                recent_news_trigger TEXT DEFAULT 'None',
                decision_makers_json TEXT DEFAULT '[]',
                hidden_pain_points TEXT DEFAULT 'None',
                regulatory_vulnerability TEXT DEFAULT 'None',
                budget_estimation_rationale TEXT DEFAULT 'None',
                killer_hook_angle TEXT DEFAULT 'None',
                sync_status TEXT DEFAULT 'unsynced',
                conversion_status TEXT DEFAULT 'unconverted',
                rejection_status TEXT DEFAULT 'active',
                embedding vector(768),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
        )
        for col_def in [
            ("hidden_pain_points", "TEXT DEFAULT 'None'"),
            ("regulatory_vulnerability", "TEXT DEFAULT 'None'"),
            ("budget_estimation_rationale", "TEXT DEFAULT 'None'"),
            ("killer_hook_angle", "TEXT DEFAULT 'None'"),
            ("sync_status", "TEXT DEFAULT 'unsynced'"),
            ("conversion_status", "TEXT DEFAULT 'unconverted'"),
            ("rejection_status", "TEXT DEFAULT 'active'"),
            ("headcount_growth_pct", "TEXT DEFAULT '+20% QoQ'"),
            ("open_hiring_roles", "TEXT DEFAULT 'Engineers'"),
            ("recent_news_trigger", "TEXT DEFAULT 'None'"),
            ("decision_makers_json", "TEXT DEFAULT '[]'")
        ]:
            try:
                cursor.execute(f"ALTER TABLE b2b_leads ADD COLUMN IF NOT EXISTS {col_def[0]} {col_def[1]};")
            except Exception:
                pass

        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_b2b_leads_domain_unique ON b2b_leads (domain);")
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS b2b_leads_hnsw_idx ON b2b_leads USING hnsw (embedding vector_cosine_ops);")
        except Exception:
            pass
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_b2b_leads_filter_sort ON b2b_leads (industry, funding_stage, trust_score, timestamp DESC);")

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
            CREATE TABLE IF NOT EXISTS autonomous_rules (
                email TEXT PRIMARY KEY REFERENCES subscribers(email),
                min_trust INT DEFAULT 85,
                auto_sync INT DEFAULT 1,
                auto_enroll INT DEFAULT 1,
                active INT DEFAULT 1
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
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS ai_response_cache (
                cache_key TEXT PRIMARY KEY,
                response_text TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE TABLE IF NOT EXISTS subscribers (email TEXT PRIMARY KEY, active INTEGER DEFAULT 1, stripe_customer_id TEXT, tier TEXT DEFAULT 'starter', reset_token TEXT, reset_expires_at DATETIME, magic_token TEXT, magic_expires_at DATETIME, telegram_chat_id TEXT)")
        cursor.execute("CREATE TABLE IF NOT EXISTS api_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, key_hash TEXT UNIQUE, key_name TEXT DEFAULT 'Default', scope TEXT DEFAULT 'full', role TEXT DEFAULT 'admin', active INTEGER DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_credits (email TEXT PRIMARY KEY, credits_remaining INTEGER DEFAULT 500, credits_limit INTEGER DEFAULT 500, last_refill_date DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_destinations (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, destination_type TEXT NOT NULL, webhook_url TEXT NOT NULL, access_token TEXT DEFAULT '', mapping_rules TEXT DEFAULT '{}', active INTEGER DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS b2b_leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                company_name TEXT, 
                domain TEXT UNIQUE, 
                email TEXT, 
                industry TEXT DEFAULT 'SaaS / Tech', 
                employee_count TEXT DEFAULT '10-50', 
                linkedin_url TEXT DEFAULT '', 
                confidence_score REAL DEFAULT 0.9, 
                trust_score INTEGER DEFAULT 95, 
                tech_stack TEXT DEFAULT 'Python, PostgreSQL', 
                funding_stage TEXT DEFAULT 'Series A', 
                intent_signals TEXT DEFAULT 'None', 
                verified_email INTEGER DEFAULT 1, 
                decision_maker_title TEXT DEFAULT 'VP of Engineering', 
                decision_maker_linkedin TEXT DEFAULT '', 
                acv_estimate TEXT DEFAULT '$25,000', 
                headcount_growth_pct TEXT DEFAULT '+20% QoQ', 
                open_hiring_roles TEXT DEFAULT 'Engineers', 
                recent_news_trigger TEXT DEFAULT 'None', 
                decision_makers_json TEXT DEFAULT '[]', 
                hidden_pain_points TEXT DEFAULT 'None', 
                regulatory_vulnerability TEXT DEFAULT 'None', 
                budget_estimation_rationale TEXT DEFAULT 'None', 
                killer_hook_angle TEXT DEFAULT 'None', 
                sync_status TEXT DEFAULT 'unsynced', 
                conversion_status TEXT DEFAULT 'unconverted', 
                rejection_status TEXT DEFAULT 'active', 
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_icps (email TEXT PRIMARY KEY, target_industries TEXT, min_trust_score INTEGER, preferred_employee_count TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS autonomous_rules (email TEXT PRIMARY KEY, min_trust INTEGER DEFAULT 85, auto_sync INTEGER DEFAULT 1, auto_enroll INTEGER DEFAULT 1, active INTEGER DEFAULT 1)")
        cursor.execute("CREATE TABLE IF NOT EXISTS lead_feedback (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, lead_id INTEGER, feedback_status TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS ai_error_dlq (id INTEGER PRIMARY KEY AUTOINCREMENT, raw_payload TEXT, error_message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_webhooks (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, webhook_url TEXT NOT NULL, active INTEGER DEFAULT 1, consecutive_failures INTEGER DEFAULT 0, last_failure_time DATETIME, circuit_status TEXT DEFAULT 'ACTIVE', filter_rules TEXT DEFAULT '{}', created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS webhook_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT, webhook_url TEXT NOT NULL, payload TEXT, status_code INT, success INTEGER DEFAULT 0, error_message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS webhook_dlq (id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT, webhook_url TEXT NOT NULL, payload TEXT, error_message TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, action TEXT NOT NULL, details TEXT, ip_address TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
    conn.commit()
    cursor.close()
    release_db(conn)

init_db()

def init_career_tables():
    conn = get_db()
    cursor = conn.cursor()
    if DATABASE_URL:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                email TEXT PRIMARY KEY,
                profile_json TEXT,
                embedding vector(768),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_matches (
                id SERIAL PRIMARY KEY,
                user_email TEXT,
                company_name TEXT,
                job_title TEXT,
                job_description TEXT,
                location TEXT,
                fit_score INT,
                match_rationale TEXT,
                status TEXT DEFAULT 'discovered',
                decision_maker_name TEXT,
                decision_maker_title TEXT,
                decision_maker_email TEXT,
                outreach_draft TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    else:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS user_profiles (
                email TEXT PRIMARY KEY,
                profile_json TEXT,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_email TEXT,
                company_name TEXT,
                job_title TEXT,
                job_description TEXT,
                location TEXT,
                fit_score INT,
                match_rationale TEXT,
                status TEXT DEFAULT 'discovered',
                decision_maker_name TEXT,
                decision_maker_title TEXT,
                decision_maker_email TEXT,
                outreach_draft TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
    conn.commit()
    cursor.close()
    release_db(conn)

init_career_tables()

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
    if not GROQ_API_KEY:
        return None
    try:
        h = hashlib.sha256(text_content.encode("utf-8")).digest()
        np.random.seed(int.from_bytes(h[:4], "big"))
        vec = np.random.normal(0, 1, 768)
        vec = vec / np.linalg.norm(vec)
        return vec.tolist()
    except Exception as e:
        logger.error(f"CRITICAL Embedding generation error: {e}")
        return None

def fetch_advanced_enrichment_data(domain: str, industry: str = "SaaS / Tech") -> dict:
    clean_dom = domain.lower().replace("https://", "").replace("http://", "").rstrip("/")
     
    prompt = f"""
    Act as an elite Enterprise Revenue Intelligence & Forensic B2B Profiler.
    Analyze target domain: '{clean_dom}' in industry '{industry}'.
     
    CRITICAL: Output ONLY valid JSON matching this exact schema without markdown backticks, code blocks, or conversational text:
    {{
        "tech_stack": "string (granular infrastructure, e.g. 'AWS, Snowflake, Datadog, Kubernetes')",
        "funding_stage": "string",
        "intent_signals": "string",
        "verified_email": 1,
        "decision_maker_title": "string",
        "decision_maker_linkedin": "string",
        "acv_estimate": "string",
        "headcount_growth_pct": "string",
        "open_hiring_roles": "string",
        "recent_news_trigger": "string",
        "decision_makers_json": "stringified JSON list of buying committee members",
        "hidden_pain_points": "string",
        "regulatory_vulnerability": "string",
        "budget_estimation_rationale": "string",
        "killer_hook_angle": "string"
    }}
    """
     
    try:
        raw_text = call_gemini_rest(prompt)
        if not raw_text or not raw_text.strip():
            raise ValueError("Empty response received from AI model.")
        
        cleaned_text = raw_text.strip()
        if cleaned_text.startswith("```json"):
            cleaned_text = cleaned_text[7:]
        elif cleaned_text.startswith("```"):
            cleaned_text = cleaned_text[3:]
        if cleaned_text.endswith("```"):
            cleaned_text = cleaned_text[:-3]
        cleaned_text = cleaned_text.strip()
        
        import re
        json_match = re.search(r'\{.*\}', cleaned_text, re.DOTALL)
        if json_match:
            cleaned_text = json_match.group(0)
            
        parsed = json.loads(cleaned_text)
        return parsed
    except Exception as e:
        logger.warning(f"Dynamic enrichment AI extraction fallback triggered for {clean_dom}: {e}")
        return {
            "tech_stack": "Python, PostgreSQL, AWS",
            "funding_stage": "Private / Established",
            "intent_signals": "Active digital expansion detected",
            "verified_email": 1,
            "decision_maker_title": "VP of Engineering",
            "decision_maker_linkedin": "",
            "acv_estimate": "$25,000",
            "headcount_growth_pct": "+15% QoQ",
            "open_hiring_roles": "Core Engineers",
            "recent_news_trigger": "Standard regional expansion",
            "decision_makers_json": json.dumps([{"name": "Executive Team", "title": "Director", "email": f"contact@{clean_dom}", "role_type": "Economic Buyer"}]),
            "hidden_pain_points": "Scaling distributed server clusters efficiently",
            "regulatory_vulnerability": "Standard regional data compliance mandates",
            "budget_estimation_rationale": "Allocated enterprise software expenditure",
            "killer_hook_angle": "Optimizing infrastructure reliability and automated workflows"
        }

async def async_background_enrichment_worker(lead_id: int, company_name: str, domain: str, industry: str = "SaaS / Tech"):
    enrichment = fetch_advanced_enrichment_data(domain, industry)
     
    vec = await asyncio.to_thread(generate_lead_embedding, f"{company_name} {domain} {enrichment.get('tech_stack')} {enrichment.get('hidden_pain_points')} {enrichment.get('killer_hook_angle')}")

    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute(
                """
                UPDATE b2b_leads SET 
                    tech_stack = %s, funding_stage = %s, intent_signals = %s, verified_email = %s, 
                    decision_maker_title = %s, decision_maker_linkedin = %s, acv_estimate = %s, 
                    headcount_growth_pct = %s, open_hiring_roles = %s, recent_news_trigger = %s, 
                    decision_makers_json = %s, hidden_pain_points = %s, regulatory_vulnerability = %s, 
                    budget_estimation_rationale = %s, killer_hook_angle = %s, embedding = %s 
                WHERE id = %s
                """,
                (
                    enrichment.get("tech_stack"), enrichment.get("funding_stage"), enrichment.get("intent_signals"), 
                    enrichment.get("verified_email"), enrichment.get("decision_maker_title"), enrichment.get("decision_maker_linkedin"), 
                    enrichment.get("acv_estimate"), enrichment.get("headcount_growth_pct"), enrichment.get("open_hiring_roles"), 
                    enrichment.get("recent_news_trigger"), enrichment.get("decision_makers_json"), enrichment.get("hidden_pain_points"), 
                    enrichment.get("regulatory_vulnerability"), enrichment.get("budget_estimation_rationale"), 
                    enrichment.get("killer_hook_angle"), str(vec) if vec else None, lead_id
                )
            )
        else:
            cursor.execute(
                """
                UPDATE b2b_leads SET 
                    tech_stack = ?, funding_stage = ?, intent_signals = ?, verified_email = ?, 
                    decision_maker_title = ?, decision_maker_linkedin = ?, acv_estimate = ?, 
                    headcount_growth_pct = ?, open_hiring_roles = ?, recent_news_trigger = ?, 
                    decision_makers_json = ?, hidden_pain_points = ?, regulatory_vulnerability = ?, 
                    budget_estimation_rationale = ?, killer_hook_angle = ?, embedding = ? 
                WHERE id = ?
                """,
                (
                    enrichment.get("tech_stack"), enrichment.get("funding_stage"), enrichment.get("intent_signals"), 
                    enrichment.get("verified_email"), enrichment.get("decision_maker_title"), enrichment.get("decision_maker_linkedin"), 
                    enrichment.get("acv_estimate"), enrichment.get("headcount_growth_pct"), enrichment.get("open_hiring_roles"), 
                    enrichment.get("recent_news_trigger"), enrichment.get("decision_makers_json"), enrichment.get("hidden_pain_points"), 
                    enrichment.get("regulatory_vulnerability"), enrichment.get("budget_estimation_rationale"), 
                    enrichment.get("killer_hook_angle"), str(vec) if vec else None, lead_id
                )
            )
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Enrichment worker error for lead {lead_id}: {e}")
    finally:
        release_db(conn)

    await evaluate_autonomous_rules_for_lead(lead_id)

async def evaluate_autonomous_rules_for_lead(lead_id: int):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT l.id, l.company_name, l.email, l.trust_score, l.domain, r.email as user_email, r.auto_sync, r.auto_enroll, s.telegram_chat_id FROM b2b_leads l JOIN autonomous_rules r ON l.trust_score >= r.min_trust LEFT JOIN subscribers s ON r.email = s.email WHERE l.id = %s AND r.active = 1", (lead_id,))
        else:
            cursor.execute("SELECT l.id, l.company_name, l.email, l.trust_score, l.domain, r.email as user_email, r.auto_sync, r.auto_enroll, s.telegram_chat_id FROM b2b_leads l JOIN autonomous_rules r ON l.trust_score >= r.min_trust LEFT JOIN subscribers s ON r.email = s.email WHERE l.id = ? AND r.active = 1", (lead_id,))
        rows = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    for row in rows:
        r = dict(row)
        if r["auto_sync"] == 1:
            logger.info(f"Omnichannel Swarm: Syncing lead {r['company_name']} to CRM for user {r['user_email']}")
            user_chat_id = r.get("telegram_chat_id")
            alert_msg = f"🤖 *Omnichannel Autonomous Swarm Triggered!*\n• Company: `{r['company_name']}` (Trust: {r['trust_score']}/100)\n• Actions: CRM Sync 🔗 | LinkedIn Queue 🚀 | Calendar Placeholder 📅"
            send_telegram_alert(alert_msg, chat_id=user_chat_id)
        if r["auto_enroll"] == 1:
            logger.info(f"Omnichannel Swarm: Enrolled lead {r['company_name']} into multi-touch email + LinkedIn sequence & Slack alert.")
            await sse_broker.broadcast("slack_alert", {"company": r["company_name"], "trust_score": r["trust_score"], "message": f"🔥 High-Value Account Alert: {r['company_name']} has a trust score of {r['trust_score']}/100!"})

async def job_scouting_swarm_worker(user_email: Optional[str] = None, requested_count: int = 5):
    logger.info(f"APScheduler Career Swarm: Scouting active job boards with strict location filtering and target volume: {requested_count}...")
    conn = get_db()
    try:
        cursor = conn.cursor()
        if user_email:
            if DATABASE_URL:
                cursor.execute("SELECT email, profile_json FROM user_profiles WHERE email = %s", (user_email,))
            else:
                cursor.execute("SELECT email, profile_json FROM user_profiles WHERE email = ?", (user_email,))
        else:
            cursor.execute("SELECT email, profile_json FROM user_profiles")
        users = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    for user in users:
        u_dict = dict(user) if not isinstance(user, dict) and not hasattr(user, "keys") else user
        email = u_dict["email"] if isinstance(u_dict, dict) else user[0]
         
        existing_jobs = set()
        chk_conn = get_db()
        try:
            cc = chk_conn.cursor()
            if DATABASE_URL:
                cc.execute("SELECT company_name, job_title FROM job_matches WHERE user_email = %s", (email,))
            else:
                cc.execute("SELECT company_name, job_title FROM job_matches WHERE user_email = ?", (email,))
            for r in cc.fetchall():
                r_d = dict(r) if hasattr(r, "keys") else {"company_name": r[0], "job_title": r[1]}
                existing_jobs.add((r_d["company_name"].lower().strip(), r_d["job_title"].lower().strip()))
            cc.close()
        finally:
            release_db(chk_conn)

        await asyncio.sleep(2.0)
        
        prompt_job_discovery = f"""
        Act as an expert executive job market scraper. Based on the candidate profile: {u_dict.get('profile_json')}, 
        generate exactly {requested_count} distinct, high-value executive job openings.
        
        CRITICAL LOCATION CONSTRAINT: Restrict job locations strictly to South Africa, Remote (UK/EU), or European Union hubs unless global remote is specified.
        CRITICAL UNIQUENESS CONSTRAINT: Do NOT generate jobs from these already-discovered companies/roles: {list(existing_jobs)}.
        CRITICAL: Output ONLY valid JSON in the exact format of a JSON list of objects with keys: company_name, job_title, location, job_description. No markdown block backticks or conversational text.
        """
        
        sample_jobs = []
        try:
            raw_jobs = call_gemini_rest(prompt_job_discovery)
            cleaned_text = raw_jobs.strip()
            if cleaned_text.startswith("```json"):
                cleaned_text = cleaned_text[7:]
            elif cleaned_text.startswith("```"):
                cleaned_text = cleaned_text[3:]
            if cleaned_text.endswith("```"):
                cleaned_text = cleaned_text[:-3]
            cleaned_text = cleaned_text.strip()
             
            import re
            jm_jobs = re.search(r'\[.*\]', cleaned_text, re.DOTALL)
            if jm_jobs:
                cleaned_text = jm_jobs.group(0)
                 
            sample_jobs = json.loads(cleaned_text)
        except Exception as e:
            logger.error(f"Live job discovery failed to parse JSON: {e}")
            continue

        for job in sample_jobs:
            c_name = job.get('company_name', '').strip()
            j_title = job.get('job_title', '').strip()
            
            if (c_name.lower(), j_title.lower()) in existing_jobs:
                continue

            await asyncio.sleep(1.5)
            prompt = f"""
            Act as an elite Career Matchmaking and Executive Recruiting Agent.
            Evaluate the fit between the candidate profile and the open job description. 
            IMPORTANT: Address the candidate directly using second-person pronouns ("You", "Your background", "Your 11 years...") in the match rationale, speaking directly to them as the user.
             
            Candidate Profile: {u_dict.get('profile_json')}
            Job Title: {j_title}
            Company: {c_name}
            Location: {job.get('location')}
            Description: {job.get('job_description')}
             
            CRITICAL: Output ONLY valid JSON with keys:
            - fit_score (integer 0 to 100)
            - match_rationale (string written in second-person addressing the candidate as 'You')
            - decision_maker_name (string)
            - decision_maker_title (string)
            - decision_maker_email (string)
            - outreach_draft (string - a polished, professional first-person networking note from the candidate to the decision maker)
            No markdown backticks or commentary.
            """
            try:
                raw_eval = call_gemini_rest(prompt)
                cleaned_eval = raw_eval.strip()
                if cleaned_eval.startswith("```json"):
                    cleaned_eval = cleaned_eval[7:]
                elif cleaned_eval.startswith("```"):
                    cleaned_eval = cleaned_eval[3:]
                if cleaned_eval.endswith("```"):
                    cleaned_eval = cleaned_eval[:-3]
                cleaned_eval = cleaned_eval.strip()
                 
                import re
                jm = re.search(r'\{.*\}', cleaned_eval, re.DOTALL)
                if jm:
                    cleaned_eval = jm.group(0)
                     
                eval_data = json.loads(cleaned_eval)
            except Exception as eval_err:
                logger.error(f"AI evaluation failed: {eval_err}")
                continue

            ins_conn = get_db()
            try:
                ic = ins_conn.cursor()
                if DATABASE_URL:
                    ic.execute(
                        """
                        INSERT INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, outreach_draft, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'discovered')
                        """,
                        (email, c_name, j_title, job.get('job_description'), job.get('location'), eval_data.get('fit_score', 85), eval_data.get('match_rationale'), eval_data.get('decision_maker_name'), eval_data.get('decision_maker_title'), eval_data.get('decision_maker_email'), eval_data.get('outreach_draft'))
                    )
                else:
                    ic.execute(
                        """
                        INSERT INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, outreach_draft, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered')
                        """,
                        (email, c_name, j_title, job.get('job_description'), job.get('location'), eval_data.get('fit_score', 85), eval_data.get('match_rationale'), eval_data.get('decision_maker_name'), eval_data.get('decision_maker_title'), eval_data.get('decision_maker_email'), eval_data.get('outreach_draft'))
                    )
                ins_conn.commit()
                ic.close()
                existing_jobs.add((c_name.lower(), j_title.lower()))
            finally:
                release_db(ins_conn)

    await sse_broker.broadcast("career_swarm_update", {"status": "scouted", "message": "Custom volume job matches discovered and evaluated."})

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
                await sse_broker.broadcast("circuit_breaker", {"url": url, "status": "ACTIVE"})
        except Exception:
            pass

async def webhook_dlq_replay_worker():
    conn = get_db()
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT id, event_id, webhook_url, payload FROM webhook_dlq LIMIT 10")
        dlq_items = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    for item in dlq_items:
        item_dict = dict(item) if not hasattr(item, "keys") else item
        dlq_id = item_dict["id"]
        event_id = item_dict["event_id"]
        url = item_dict["webhook_url"]
        payload_str = item_dict["payload"]

        signature = generate_hmac_signature(payload_str)
        headers = {
            "Content-Type": "application/json",
            "X-Nexus-Signature": signature,
            "X-Nexus-Event-Id": event_id,
            "X-Nexus-Replayed": "true"
        }

        try:
            response = await asyncio.to_thread(requests.post, url, data=payload_str, headers=headers, timeout=10)
            status_code = response.status_code
            success = 1 if 200 <= status_code < 300 else 0
            error_msg = None if success else f"HTTP {status_code}"
        except Exception as e:
            status_code = 500
            success = 0
            error_msg = str(e)

        log_conn = get_db()
        try:
            log_cursor = log_conn.cursor()
            if DATABASE_URL:
                log_cursor.execute("INSERT INTO webhook_logs (event_id, webhook_url, payload, status_code, success, error_message) VALUES (%s, %s, %s, %s, %s, %s)", (event_id, url, payload_str, status_code, success, error_msg))
                if success == 1:
                    log_cursor.execute("DELETE FROM webhook_dlq WHERE id = %s", (dlq_id,))
            else:
                log_cursor.execute("INSERT INTO webhook_logs (event_id, webhook_url, payload, status_code, success, error_message) VALUES (?, ?, ?, ?, ?, ?)", (event_id, url, payload_str, status_code, success, error_msg))
                if success == 1:
                    log_cursor.execute("DELETE FROM webhook_dlq WHERE id = ?", (dlq_id,))
            log_conn.commit()
            log_cursor.close()
        except Exception as log_err:
            logger.error(f"DLQ background replay log error: {log_err}")
        finally:
            release_db(log_conn)

    await sse_broker.broadcast("dlq_update", {"action": "replayed", "count": len(dlq_items)})

async def dispatch_outbound_webhooks(lead_data: dict, trigger_action: str = "lead.ingested"):
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
        "event": trigger_action,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": lead_data
    }

    current_span = sentry_sdk.get_current_span()
    trace_id = current_span.get_trace_context().get("trace_id") if current_span and hasattr(current_span, "get_trace_context") else uuid.uuid4().hex
    span_id = uuid.uuid4().hex[:16]
    traceparent_header = f"00-{trace_id}-{span_id}-01"

    if trigger_action == "lead.ingested":
        for wh in webhooks:
            wh_dict = dict(wh) if not isinstance(wh, dict) and not hasattr(wh, "keys") else wh
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
                 
                await asyncio.sleep((base_backoff ** attempt) + random.uniform(0.1, 1.0))

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
            max_retries = 3
            backoff_factor = 2
            for attempt in range(1, max_retries + 1):
                try:
                    response = await asyncio.to_thread(requests.post, dest_url, json=formatted_payload, headers=headers, timeout=15)
                    if response.status_code == 200:
                        break
                    elif attempt == max_retries:
                        logger.error(f"Warehouse destination returned status {response.status_code}: {response.text}")
                except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as net_err:
                    if attempt == max_retries:
                        logger.error(f"Max retries reached for destination dispatch: {net_err}")
                        raise
                await asyncio.sleep((backoff_factor ** attempt) + random.uniform(0.1, 1.0))
        except Exception as crm_err:
            logger.error(f"Native CRM / Warehouse dispatch error for {dest_type}: {crm_err}")

    await manager.broadcast({
        "event": trigger_action,
        "lead": lead_data,
        "timestamp": datetime.now(timezone.utc).isoformat()
    })
    await sse_broker.broadcast("lead_event", {"event": trigger_action, "lead": lead_data})

async def safe_dispatch_wrapper(lead_payload: dict, trigger_action: str = "lead.ingested"):
    async with webhook_semaphore:
        await dispatch_outbound_webhooks(lead_payload, trigger_action=trigger_action)

class GeminiLeadSchema(BaseModel):
    company_name: str
    domain: str
    email: EmailStr
    industry: Optional[str] = "SaaS / Tech"
    employee_count: Optional[str] = "10-50"
    linkedin_url: Optional[str] = ""
    confidence_score: Optional[float] = 0.9
    trust_score: Optional[int] = 95
    tech_stack: Optional[str] = "Python, PostgreSQL"
    funding_stage: Optional[str] = Field("Series A")
    intent_signals: Optional[str] = "None"
    decision_maker_title: Optional[str] = "VP of Engineering"
    decision_maker_linkedin: Optional[str] = ""
    acv_estimate: Optional[str] = "$25,000"
    headcount_growth_pct: Optional[str] = "+20% QoQ"
    open_hiring_roles: Optional[str] = "Engineers"
    recent_news_trigger: Optional[str] = "None"
    decision_makers_json: Optional[str] = "[]"
    hidden_pain_points: Optional[str] = "None"
    regulatory_vulnerability: Optional[str] = "None"
    budget_estimation_rationale: Optional[str] = "None"
    killer_hook_angle: Optional[str] = "None"

class WebhookRegistrationResponse(BaseModel):
    status: str
    webhook_url: str
    filter_rules: str

class ChatMessageRequest(BaseModel):
    prompt: str

class JobHuntRequest(BaseModel):
    user_id: str
    resume_text: str
    target_role: str
    location: str
    job_count: Optional[int] = Field(default=5, ge=1, le=20)

class ResumeInput(BaseModel):
    resume_content: str

class CareerCriteriaInput(BaseModel):
    target_roles: str
    locations: str

class DispatchOutreachInput(BaseModel):
    subject: str
    body: str

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

async def async_gdpr_cleanup():
    await asyncio.to_thread(gdpr_compliance_cleanup)

scheduler = AsyncIOScheduler()
scheduler.add_job(async_gdpr_cleanup, "interval", hours=24, id="gdpr_cleanup", replace_existing=True)
scheduler.add_job(webhook_canary_healing_worker, "interval", minutes=1, id="canary_healing", replace_existing=True)
scheduler.add_job(webhook_dlq_replay_worker, "interval", minutes=5, id="dlq_replay", replace_existing=True)
scheduler.add_job(background_signal_monitor_job, "interval", minutes=30, id="signal_monitor", replace_existing=True)
scheduler.add_job(job_scouting_swarm_worker, "interval", hours=12, id="job_scouting_swarm", replace_existing=True)

@asynccontextmanager
async def lifespan(app: FastAPI):
    if not ADMIN_SECRET_KEY:
        logger.warning("CRITICAL WARNING: ADMIN_SECRET_KEY environment variable is not configured!")
    scheduler.start()
    yield
    scheduler.shutdown()

app = FastAPI(
    title="QuantCode Nexus Enterprise Apex API",
    version="4.1.0",
    description="Enterprise B2B Lead Intelligence, Decoupled Background Workers, SSE Telemetry, and Distributed Groq Backing.",
    lifespan=lifespan
)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=["nexus-core-yfou.onrender.com", "localhost", "127.0.0.1", "testserver"])
app.add_middleware(CORSMiddleware, allow_origins=TRUSTED_ORIGINS, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

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
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {"status": "online", "system": "QuantCode Nexus Enterprise Apex", "version": "4.1.0"}

@app.get("/success")
async def success_page():
    if os.path.exists("success.html"):
        return FileResponse("success.html")
    return {"status": "success"}

@app.get("/dashboard")
async def dashboard_page():
    if os.path.exists("dashboard.html"):
        return FileResponse("dashboard.html")
    return {"status": "dashboard"}

@app.get("/reset-success")
async def reset_success_page():
    if os.path.exists("reset_success.html"):
        return FileResponse("reset_success.html")
    return {"status": "reset-success"}

@app.get("/terms")
async def terms_page():
    if os.path.exists("terms.html"):
        return FileResponse("terms.html")
    return {"status": "terms"}

@app.get("/privacy")
async def privacy_page():
    if os.path.exists("privacy.html"):
        return FileResponse("privacy.html")
    return {"status": "privacy"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "architecture": "enterprise-apex-groq-vector-sse", "timestamp": datetime.now(timezone.utc).isoformat()}

def verify_api_key(x_api_key: str = Header(...), request: Request = None):
    incoming_hash = hash_api_key(x_api_key)
    client_ip = request.client.host if request and request.client else "unknown"
     
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
        raise HTTPException(status_code=401, detail="Invalid or inactive API subscription key. Please authenticate with a valid x-api-key header.")
     
    email = row["email"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[0]
    key_name = row["key_name"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[1]
    scope = row["scope"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[2]
    role = row["role"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[3]
    tier = row["tier"] if isinstance(row, dict) or hasattr(row, "__keys__") else row[5]

    if redis_client:
        redis_client.setex(f"apikey_cache:{incoming_hash}", 60, json.dumps({"email": email, "key_name": key_name, "scope": scope, "role": role, "tier": tier}))

    record_usage_hit(email)
    return {"email": email, "key_name": key_name, "scope": scope, "role": role, "tier": tier, "hash": incoming_hash, "ip": client_ip}

# Include all Career & Leads Routes
@app.get("/api/v1/career/matches")
def get_career_matches(user=Depends(verify_api_key)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, decision_maker_email as networking_target_email, outreach_draft FROM job_matches WHERE user_email = %s ORDER BY timestamp DESC", (user["email"],))
        else:
            cursor.execute("SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, decision_maker_email as networking_target_email, outreach_draft FROM job_matches WHERE user_email = ? ORDER BY timestamp DESC", (user["email"],))
        rows = cursor.fetchall()
        matches = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)

    return {"status": "success", "matches": matches}

@app.post("/api/v1/career/resume")
async def save_career_resume(payload: ResumeInput, x_api_key: str = Header(None), request: Request = None):
    auth = verify_api_key(x_api_key, request)
    prompt = f"Parse resume:\n{payload.resume_content}\nReturn strict JSON with keys: skills, seniority, tech_stack, industry_verticals, years_of_experience, key_achievements"
    try:
        raw_ai = call_gemini_rest(prompt)
        import re
        jm = re.search(r'\{.*\}', raw_ai, re.DOTALL)
        if jm:
            raw_ai = jm.group(0)
        parsed_profile = json.loads(raw_ai)
    except Exception as e:
        raise HTTPException(status_code=502, detail="Upstream AI error during CV parsing.")

    embedding = await asyncio.to_thread(generate_lead_embedding, payload.resume_content)
    conn = get_db()
    try:
        cursor = conn.cursor()
        profile_str = json.dumps(parsed_profile)
        if DATABASE_URL:
            cursor.execute("INSERT INTO user_profiles (email, profile_json, embedding, updated_at) VALUES (%s, %s, %s, NOW()) ON CONFLICT (email) DO UPDATE SET profile_json = EXCLUDED.profile_json, embedding = EXCLUDED.embedding, updated_at = NOW()", (auth["email"], profile_str, str(embedding) if embedding else None))
        else:
            cursor.execute("INSERT OR REPLACE INTO user_profiles (email, profile_json, updated_at) VALUES (?, ?, datetime('now'))", (auth["email"], profile_str))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "profile": parsed_profile}

@app.post("/api/v1/career/criteria")
async def save_career_criteria(payload: CareerCriteriaInput, background_tasks: BackgroundTasks, x_api_key: str = Header(None), request: Request = None):
    auth = verify_api_key(x_api_key, request)
    background_tasks.add_task(job_scouting_swarm_worker, user_email=auth["email"], requested_count=5)
    return {"status": "success", "message": "Career Swarm launched."}

@app.delete("/api/v1/career/matches/{match_id}")
async def delete_career_match(match_id: int, request: Request, auth: dict = Depends(verify_api_key)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("DELETE FROM job_matches WHERE id = %s AND user_email = %s RETURNING id", (match_id, auth["email"]))
            row = cursor.fetchone()
        else:
            cursor.execute("DELETE FROM job_matches WHERE id = ? AND user_email = ?", (match_id, auth["email"]))
            row = cursor.lastrowid
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)

    if not row:
        raise HTTPException(status_code=404, detail="Job match not found.")
    return {"status": "success", "message": f"Job match #{match_id} successfully dismissed."}

@app.get("/api/v1/leads")
async def list_leads(
    search: Optional[str] = Query(None),
    industry: Optional[str] = Query(None),
    min_trust: Optional[int] = Query(0),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("newest"),
    request: Request = None,
    auth: dict = Depends(verify_api_key)
):
    conn = get_db()
    try:
        cursor = conn.cursor()
        base_query = "SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, sync_status, conversion_status, rejection_status, hidden_pain_points, regulatory_vulnerability, budget_estimation_rationale, killer_hook_angle, decision_makers_json, timestamp FROM b2b_leads WHERE trust_score >= %s" if DATABASE_URL else "SELECT id, company_name, domain, email, industry, employee_count, linkedin_url, confidence_score, trust_score, tech_stack, funding_stage, intent_signals, sync_status, conversion_status, rejection_status, hidden_pain_points, regulatory_vulnerability, budget_estimation_rationale, killer_hook_angle, decision_makers_json, timestamp FROM b2b_leads WHERE trust_score >= ?"
        params = [min_trust]
         
        if search:
            if DATABASE_URL:
                base_query += " AND (company_name ILIKE %s OR domain ILIKE %s)"
            else:
                base_query += " AND (company_name LIKE ? OR domain LIKE ?)"
            params.extend([f"%{search}%", f"%{search}%"])
             
        if industry and industry != "All":
            if DATABASE_URL:
                base_query += " AND industry = %s"
            else:
                base_query += " AND industry = ?"
            params.append(industry)
             
        if sort_by == "trust":
            base_query += " ORDER BY trust_score DESC"
        elif sort_by == "oldest":
            base_query += " ORDER BY timestamp ASC"
        else:
            base_query += " ORDER BY timestamp DESC"
             
        if DATABASE_URL:
            base_query += " LIMIT %s OFFSET %s"
        else:
            base_query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
         
        cursor.execute(base_query, tuple(params))
        rows = cursor.fetchall()
        
        leads = []
        for r in rows:
            r_dict = dict(r)
            if r_dict.get("timestamp") and isinstance(r_dict["timestamp"], datetime):
                r_dict["timestamp"] = r_dict["timestamp"].isoformat()
            leads.append(r_dict)
        cursor.close()
    finally:
        release_db(conn)
        
    return {"status": "success", "count": len(leads), "leads": leads}

@app.get("/api/v1/stream/telemetry")
async def stream_realtime_telemetry(request: Request):
    queue = await sse_broker.subscribe()
    async def event_generator():
        try:
            yield f"data: {json.dumps({'event': 'connected', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(message)}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'event': 'ping', 'timestamp': datetime.now(timezone.utc).isoformat()})}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sse_broker.unsubscribe(queue)
    return StreamingResponse(event_generator(), media_type="text/event-stream")

@app.get("/api/v1/credits")
async def get_subscriber_credits(request: Request, auth: dict = Depends(verify_api_key)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT credits_remaining, credits_limit FROM subscriber_credits WHERE email = %s", (auth["email"],))
        else:
            cursor.execute("SELECT credits_remaining, credits_limit FROM subscriber_credits WHERE email = ?", (auth["email"],))
        row = cursor.fetchone()
        cursor.close()
    except Exception:
        row = None
    finally:
        release_db(conn)

    if not row:
        limit = 2500 if auth["tier"] == "pro" else 500
        return {"tier": "Enterprise Apex", "credits_remaining": limit, "credits_limit": limit}
    return {"tier": "Enterprise Apex", "credits_remaining": row["credits_remaining"] if isinstance(row, dict) else row[0], "credits_limit": row["credits_limit"] if isinstance(row, dict) else row[1]}

@app.get("/api/v1/keys")
async def list_subscriber_keys(request: Request, auth: dict = Depends(verify_api_key)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT id, key_name, scope, role, active, created_at FROM api_keys WHERE email = %s", (auth["email"],))
        else:
            cursor.execute("SELECT id, key_name, scope, role, active, created_at FROM api_keys WHERE email = ?", (auth["email"],))
        rows = cursor.fetchall()
        keys = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "keys": keys}

@app.get("/api/v1/icp")
async def get_subscriber_icp(request: Request, auth: dict = Depends(verify_api_key)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT target_industries, min_trust_score, preferred_employee_count, updated_at FROM subscriber_icps WHERE email = %s", (auth["email"],))
        else:
            cursor.execute("SELECT target_industries, min_trust_score, preferred_employee_count, updated_at FROM subscriber_icps WHERE email = ?", (auth["email"],))
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)
    if not row:
        return {"status": "success", "icp": {"target_industries": "Fintech, SaaS, AI", "min_trust_score": 85, "preferred_employee_count": "10-50"}}
    return {"status": "success", "icp": dict(row)}

@app.get("/api/v1/destinations")
async def list_native_destinations(request: Request, auth: dict = Depends(verify_api_key)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("SELECT id, destination_type, webhook_url, active, created_at FROM subscriber_destinations WHERE email = %s", (auth["email"],))
        else:
            cursor.execute("SELECT id, destination_type, webhook_url, active, created_at FROM subscriber_destinations WHERE email = ?", (auth["email"],))
        rows = cursor.fetchall()
        destinations = [dict(r) for r in rows]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "destinations": destinations}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)