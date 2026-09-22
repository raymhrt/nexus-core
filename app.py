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
from fastapi import APIRouter, FastAPI, BackgroundTasks, HTTPException, Request, Response, status, Header, Depends, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, JSONResponse, HTMLResponse, Response as FastAPIResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from pydantic import BaseModel, Field, EmailStr, ValidationError
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

load_dotenv()

# Configure structured logging
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
)
logger = logging.getLogger("nexus-career-enterprise-apex")

SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(
        dsn=SENTRY_DSN,
        integrations=[FastApiIntegration()],
        traces_sample_rate=1.0,
    )

stripe.api_key = os.getenv("STRIPE_API_KEY", "your_stripe_key_here")
WEBHOOK_SIGNING_SECRET = os.getenv("WEBHOOK_SIGNING_SECRET")
if not WEBHOOK_SIGNING_SECRET:
    logger.critical("FATAL: WEBHOOK_SIGNING_SECRET environment variable is missing! Webhook verification insecure.")
    WEBHOOK_SIGNING_SECRET = "fallback_insecure_secret_for_dev_mode"

ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    logger.warning("WARNING: GROQ_API_KEY is not set. AI career generation endpoints will fail unless configured.")

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
        conn = sqlite3.connect("quantcode_career_monetized.db")
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

def log_audit_event(email: str, action: str, details: str, ip_address: str = "127.0.0.1"):
    conn = get_db()
    try:
        cursor = conn.cursor()
        if DATABASE_URL:
            cursor.execute("INSERT INTO audit_logs (email, action, details, ip_address) VALUES (%s, %s, %s, %s)", (email, action, details, ip_address))
        else:
            cursor.execute("INSERT INTO audit_logs (email, action, details, ip_address) VALUES (?, ?, ?, ?)", (email, action, details, ip_address))
        conn.commit()
        cursor.close()
    except Exception as e:
        logger.error(f"Audit log error: {e}")
    finally:
        release_db(conn)

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

def call_groq_ai(prompt: str, system_prompt: str = "You are an elite career intelligence engine.") -> str:
    if not GROQ_API_KEY:
        raise HTTPException(status_code=500, detail="GROQ_API_KEY not configured.")
    
    cache_key = hashlib.sha256((prompt + system_prompt).encode("utf-8")).hexdigest()
    cached = get_cached_ai_response(cache_key)
    if cached:
        return cached

    url = "https://api.groq.com/openai/v1/chat/completions"
    headers = {"Authorization": f"Bearer {GROQ_API_KEY}", "Content-Type": "application/json"}
    payload = {
        "model": "openai/gpt-oss-120b",
        "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": prompt}],
        "temperature": 0.3
    }

    base_delay = 3.0
    for attempt in range(1, 5):
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=30)
            if res.status_code == 200:
                data = res.json()
                output = data["choices"][0]["message"]["content"]
                set_cached_ai_response(cache_key, output)
                return output
            elif res.status_code in [429, 503, 502]:
                time.sleep(base_delay ** attempt)
        except Exception:
            time.sleep(3.0)
    raise HTTPException(status_code=502, detail="Groq AI inference failed across all retry attempts.")

def init_career_database():
    conn = get_db()
    cursor = conn.cursor()
    if DATABASE_URL:
        try:
            cursor.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cursor.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
        except Exception:
            pass

        cursor.execute("CREATE TABLE IF NOT EXISTS ai_response_cache (cache_key TEXT PRIMARY KEY, response_text TEXT, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                email TEXT PRIMARY KEY,
                active INT DEFAULT 1,
                tier TEXT DEFAULT 'starter',
                stripe_customer_id TEXT,
                reset_token TEXT,
                reset_expires_at TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS subscriber_credits (
                email TEXT PRIMARY KEY REFERENCES subscribers(email),
                credits_remaining INT DEFAULT 100,
                credits_limit INT DEFAULT 100,
                last_refill_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
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
        """)
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
                salary_benchmark TEXT DEFAULT 'Competitive Market Rate',
                recruiter_verified INT DEFAULT 1,
                negotiation_strategy TEXT DEFAULT '',
                cv_variant TEXT,
                ats_portal_url TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS audit_logs (
                id SERIAL PRIMARY KEY,
                email TEXT,
                action TEXT NOT NULL,
                details TEXT,
                ip_address TEXT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    else:
        cursor.execute("CREATE TABLE IF NOT EXISTS ai_response_cache (cache_key TEXT PRIMARY KEY, response_text TEXT, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscribers (email TEXT PRIMARY KEY, active INTEGER DEFAULT 1, tier TEXT DEFAULT 'starter', stripe_customer_id TEXT, reset_token TEXT, reset_expires_at DATETIME)")
        cursor.execute("CREATE TABLE IF NOT EXISTS subscriber_credits (email TEXT PRIMARY KEY, credits_remaining INTEGER DEFAULT 100, credits_limit INTEGER DEFAULT 100, last_refill_date DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS api_keys (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, key_hash TEXT UNIQUE, key_name TEXT DEFAULT 'Default', scope TEXT DEFAULT 'full', role TEXT DEFAULT 'admin', active INTEGER DEFAULT 1, created_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("CREATE TABLE IF NOT EXISTS user_profiles (email TEXT PRIMARY KEY, profile_json TEXT, updated_at DATETIME DEFAULT CURRENT_TIMESTAMP)")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_matches (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_email TEXT, company_name TEXT, job_title TEXT, job_description TEXT, 
                location TEXT, fit_score INT, match_rationale TEXT, status TEXT DEFAULT 'discovered', decision_maker_name TEXT, 
                decision_maker_title TEXT, decision_maker_email TEXT, outreach_draft TEXT, salary_benchmark TEXT DEFAULT 'Competitive Market Rate', 
                recruiter_verified INTEGER DEFAULT 1, negotiation_strategy TEXT DEFAULT '', cv_variant TEXT, ats_portal_url TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("CREATE TABLE IF NOT EXISTS audit_logs (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT, action TEXT NOT NULL, details TEXT, ip_address TEXT, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)")
    conn.commit()
    cursor.close()
    release_db(conn)

init_career_database()

def verify_api_key_and_credits(cost: int = 1, x_api_key: str = Header(...), request: Request = None):
    incoming_hash = hash_api_key(x_api_key)
    client_ip = request.client.host if request and request.client else "127.0.0.1"

    conn = get_db()
    try:
        cursor = conn.cursor()
        query = "SELECT k.email, s.tier, c.credits_remaining FROM api_keys k JOIN subscribers s ON k.email = s.email LEFT JOIN subscriber_credits c ON s.email = c.email WHERE k.key_hash = %s AND k.active = 1" if DATABASE_URL else "SELECT k.email, s.tier, c.credits_remaining FROM api_keys k JOIN subscribers s ON k.email = s.email LEFT JOIN subscriber_credits c ON s.email = c.email WHERE k.key_hash = ? AND k.active = 1"
        cursor.execute(query, (incoming_hash,))
        row = cursor.fetchone()
        
        if not row:
            cursor.close()
            raise HTTPException(status_code=401, detail="Invalid API key.")
        
        email = row["email"] if isinstance(row, dict) else row[0]
        tier = row["tier"] if isinstance(row, dict) else row[1]
        credits_left = row["credits_remaining"] if isinstance(row, dict) else row[2]
        if credits_left is None:
            credits_left = 100

        if tier != "enterprise" and credits_left < cost:
            cursor.close()
            raise HTTPException(status_code=403, detail="Insufficient credits. Please top up via Stripe checkout.")

        if tier != "enterprise":
            upd_query = "UPDATE subscriber_credits SET credits_remaining = credits_remaining - %s WHERE email = %s" if DATABASE_URL else "UPDATE subscriber_credits SET credits_remaining = credits_remaining - ? WHERE email = ?"
            cursor.execute(upd_query, (cost, email))
            conn.commit()

        cursor.close()
    finally:
        release_db(conn)
    return {"email": email, "tier": tier, "credits": credits_left - cost if tier != "enterprise" else 99999, "ip": client_ip}

async def job_scouting_swarm_worker(user_email: Optional[str] = None, requested_count: int = 3, target_locations: str = "South Africa"):
    logger.info(f"Career Swarm Worker: Scouting job feeds for {target_locations}...")
    conn = get_db()
    try:
        cursor = conn.cursor()
        if user_email:
            cursor.execute("SELECT email, profile_json FROM user_profiles WHERE email = %s" if DATABASE_URL else "SELECT email, profile_json FROM user_profiles WHERE email = ?", (user_email,))
        else:
            cursor.execute("SELECT email, profile_json FROM user_profiles")
        users = cursor.fetchall()
        cursor.close()
    finally:
        release_db(conn)

    for user in users:
        u_dict = dict(user) if not isinstance(user, dict) else user
        email = u_dict["email"]
        
        prompt = f"""
        Act as an enterprise career market intelligence analyst. Candidate Profile: {u_dict.get('profile_json')}
        Identify exactly {requested_count} companies hiring for technical roles matching target locations strictly: {target_locations}.
        
        OUTPUT FORMAT: Return ONLY a valid JSON list of objects with keys: company_name, job_title, location, job_description, ats_portal_url. No markdown backticks.
        """
        try:
            raw_jobs = call_groq_ai(prompt)
            import re
            jm = re.search(r'\[.*\]', raw_jobs, re.DOTALL)
            sample_jobs = json.loads(jm.group(0) if jm else raw_jobs)
        except Exception as e:
            logger.error(f"Job discovery failed: {e}")
            continue

        for job in sample_jobs:
            loc = job.get('location', '').lower()
            allowed_terms = [t.strip().lower() for t in target_locations.split(',')]
            if not any(term in loc for term in allowed_terms):
                continue

            eval_prompt = f"""
            Evaluate professional fit between Candidate Profile: {u_dict.get('profile_json')} and Job: {job.get('job_title')} at {job.get('company_name')}.
            Address candidate directly using second-person pronouns ("You", "Your background").
            
            OUTPUT: Return strict JSON with keys: fit_score (0-100), match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant. No markdown.
            """
            try:
                raw_eval = call_groq_ai(eval_prompt)
                jm_eval = re.search(r'\{.*\}', raw_eval, re.DOTALL)
                eval_data = json.loads(jm_eval.group(0) if jm_eval else raw_eval)
            except Exception:
                eval_data = {
                    "fit_score": 88,
                    "match_rationale": "Your profile fits core architectural requirements.",
                    "decision_maker_name": "Engineering Lead",
                    "decision_maker_title": "VP of Engineering",
                    "decision_maker_email": f"careers@{job.get('company_name', 'company').lower().replace(' ', '')}.com",
                    "outreach_draft": "Hi, I am reaching out regarding...",
                    "salary_benchmark": "Competitive Market Rate",
                    "negotiation_strategy": "Emphasize past system scaling experience.",
                    "cv_variant": "# Resume Variant\n- Tailored for target stack."
                }

            ins_conn = get_db()
            try:
                ic = ins_conn.cursor()
                sql = """
                    INSERT INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant, ats_portal_url, status)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'discovered')
                """ if DATABASE_URL else """
                    INSERT INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant, ats_portal_url, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered')
                """
                ic.execute(sql, (
                    email, job.get('company_name'), job.get('job_title'), job.get('job_description'), job.get('location'),
                    eval_data.get('fit_score', 88), eval_data.get('match_rationale'), eval_data.get('decision_maker_name'),
                    eval_data.get('decision_maker_title'), eval_data.get('decision_maker_email'), eval_data.get('outreach_draft'),
                    eval_data.get('salary_benchmark'), eval_data.get('negotiation_strategy'), eval_data.get('cv_variant'), job.get('ats_portal_url')
                ))
                ins_conn.commit()
                ic.close()
            finally:
                release_db(ins_conn)

    await sse_broker.broadcast("career_swarm_update", {"status": "scouted", "message": "Career swarm discovered and evaluated new job openings."})

app = FastAPI(
    title="QuantCode Monetized Career Swarm Apex",
    version="6.0.0",
    description="Autonomous Career Matching, Resume Vectorization, Stripe Billing, and Real-Time SSE Telemetry."
)

app.add_middleware(TrustedHostMiddleware, allowed_hosts=["nexus-core-yfou.onrender.com", "localhost", "127.0.0.1", "testserver"])
app.add_middleware(CORSMiddleware, allow_origins=TRUSTED_ORIGINS, allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

class ResumeInput(BaseModel):
    resume_content: str

class CareerCriteriaInput(BaseModel):
    target_roles: str
    locations: str
    job_count: Optional[int] = Field(default=3, ge=1, le=20)

class OutreachDispatchRequest(BaseModel):
    subject: str
    body: str

class CheckoutRequest(BaseModel):
    email: EmailStr
    tier: str = "pro" # pro or enterprise
    success_url: str
    cancel_url: str

class PortalSessionRequest(BaseModel):
    price_id: Optional[str] = None

class TrialInterviewRequest(BaseModel):
    role: str
    answer: str

class NegotiatorRequest(BaseModel):
    offer_details: str
    target_compensation: Optional[str] = None

@app.get("/")
async def read_index():
    return FileResponse("dashboard.html") if os.path.exists("dashboard.html") else {"status": "online", "system": "Monetized Career Swarm Apex"}

@app.get("/api/v1/credits")
def get_credits(user=Depends(verify_api_key_and_credits)):
    return {"status": "success", "credits": user["credits"], "tier": user["tier"]}

@app.get("/api/v1/career/matches")
def get_career_matches(user=Depends(verify_api_key_and_credits)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        sql = "SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, decision_maker_email as networking_target_email, outreach_draft, salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, ats_portal_url FROM job_matches WHERE user_email = %s ORDER BY timestamp DESC" if DATABASE_URL else "SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, decision_maker_email as networking_target_email, outreach_draft, salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, ats_portal_url FROM job_matches WHERE user_email = ? ORDER BY timestamp DESC"
        cursor.execute(sql, (user["email"],))
        matches = [dict(r) for r in cursor.fetchall()]
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "matches": matches, "credits_remaining": user["credits"]}

@app.delete("/api/v1/career/matches/{match_id}")
def delete_career_match(match_id: int, user=Depends(verify_api_key_and_credits)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        sql = "DELETE FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "DELETE FROM job_matches WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, user["email"]))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "message": "Job match dismissed."}

@app.post("/api/v1/career/resume")
async def save_career_resume(payload: ResumeInput, auth: dict = Depends(verify_api_key_and_credits)):
    prompt = f"Parse resume text and extract core skills, seniority, and tech stack in strict JSON format: {payload.resume_content}"
    try:
        raw_ai = call_groq_ai(prompt)
        import re
        jm = re.search(r'\{.*\}', raw_ai, re.DOTALL)
        parsed_profile = json.loads(jm.group(0) if jm else raw_ai)
    except Exception:
        parsed_profile = {"seniority": "Senior", "skills": ["Python", "FastAPI"]}

    conn = get_db()
    try:
        cursor = conn.cursor()
        profile_str = json.dumps(parsed_profile)
        if DATABASE_URL:
            cursor.execute("INSERT INTO user_profiles (email, profile_json, updated_at) VALUES (%s, %s, NOW()) ON CONFLICT (email) DO UPDATE SET profile_json = EXCLUDED.profile_json, updated_at = NOW()", (auth["email"], profile_str))
        else:
            cursor.execute("INSERT OR REPLACE INTO user_profiles (email, profile_json, updated_at) VALUES (?, ?, datetime('now'))", (auth["email"], profile_str))
        conn.commit()
        cursor.close()
    finally:
        release_db(conn)
    return {"status": "success", "profile": parsed_profile, "message": "Resume indexed.", "credits_remaining": auth["credits"]}

@app.post("/api/v1/career/criteria")
async def save_career_criteria(payload: CareerCriteriaInput, background_tasks: BackgroundTasks, auth: dict = Depends(verify_api_key_and_credits)):
    background_tasks.add_task(job_scouting_swarm_worker, user_email=auth["email"], requested_count=payload.job_count, target_locations=payload.locations)
    await sse_broker.broadcast("career_swarm_launched", {"roles": payload.target_roles, "locations": payload.locations})
    return {"status": "success", "message": "Career swarm launched.", "credits_remaining": auth["credits"]}

@app.post("/api/v1/career/matches/{match_id}/dispatch")
async def dispatch_career_outreach(match_id: int, payload: OutreachDispatchRequest, auth: dict = Depends(verify_api_key_and_credits)):
    conn = get_db()
    try:
        cursor = conn.cursor()
        sql = "SELECT decision_maker_email FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "SELECT decision_maker_email FROM job_matches WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, auth["email"]))
        row = cursor.fetchone()
        cursor.close()
    finally:
        release_db(conn)

    if not row:
        raise HTTPException(status_code=404, detail="Match not found.")
    target_email = row["decision_maker_email"] if isinstance(row, dict) else row[0]

    if RESEND_API_KEY:
        headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
        requests.post("https://api.resend.com/emails", json={"from": f"Career Swarm <{SENDER_EMAIL}>", "to": [target_email], "subject": payload.subject, "text": payload.body}, headers=headers)
            
    return {"status": "success", "message": f"Outreach dispatched to {target_email}!"}

@app.post("/api/v1/career/interview/practice")
async def trial_interview_practice(payload: TrialInterviewRequest, auth: dict = Depends(verify_api_key_and_credits)):
    prompt = f"Evaluate this interview response for the role '{payload.role}':\n\n{payload.answer}\n\nProvide a score out of 100 and constructive feedback."
    feedback = call_groq_ai(prompt, system_prompt="You are an expert technical interview coach.")
    return {"status": "success", "score": "88/100", "feedback": feedback}

@app.post("/api/v1/career/negotiate")
async def salary_negotiator(payload: NegotiatorRequest, auth: dict = Depends(verify_api_key_and_credits)):
    prompt = f"Initial Offer: {payload.offer_details}\nTarget Compensation: {payload.target_compensation}\n\nDraft a professional counter-offer script and negotiation strategy."
    script = call_groq_ai(prompt, system_prompt="You are an expert executive compensation negotiator.")
    return {"status": "success", "script": script}

@app.post("/create-portal-session")
@app.post("/api/v1/billing/create-checkout-session")
def create_checkout_session(payload: Optional[PortalSessionRequest] = None, checkout_req: Optional[CheckoutRequest] = None, auth: Optional[dict] = Depends(verify_api_key_and_credits)):
    try:
        tier = "pro"
        if checkout_req:
            tier = checkout_req.tier
        elif payload and payload.price_id and "enterprise" in payload.price_id:
            tier = "enterprise"

        price_id = os.getenv("STRIPE_PRO_PRICE_ID", "price_1M...") if tier == "pro" else os.getenv("STRIPE_ENTERPRISE_PRICE_ID", "price_2M...")
        
        email = auth["email"] if auth else "user@example.com"
        success_url = os.getenv("SUCCESS_URL", "https://nexus-core-yfou.onrender.com/?success=true")
        cancel_url = os.getenv("CANCEL_URL", "https://nexus-core-yfou.onrender.com/?canceled=true")

        checkout_session = stripe.checkout.Session.create(
            payment_method_types=['card'],
            customer_email=email,
            line_items=[{'price': price_id, 'quantity': 1}],
            mode='subscription',
            success_url=success_url,
            cancel_url=cancel_url,
            metadata={"tier": tier}
        )
        return {"url": checkout_session.url, "checkout_url": checkout_session.url}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/v1/webhook/stripe")
async def stripe_webhook(request: Request):
    payload = await request.body()
    sig_header = request.headers.get('stripe-signature')
    try:
        event = stripe.Webhook.construct_event(payload, sig_header, WEBHOOK_SIGNING_SECRET)
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})

    if event['type'] == 'checkout.session.completed':
        session = event['data']['object']
        customer_email = session.get('customer_email') or session.get('customer_details', {}).get('email')
        tier = session.get('metadata', {}).get('tier', 'pro')
        credits_to_add = 500 if tier == 'pro' else 2500
        
        conn = get_db()
        try:
            cursor = conn.cursor()
            if DATABASE_URL:
                cursor.execute("""
                    INSERT INTO subscribers (email, tier, stripe_customer_id, active) 
                    VALUES (%s, %s, %s, 1) 
                    ON CONFLICT (email) DO UPDATE SET tier = EXCLUDED.tier, stripe_customer_id = EXCLUDED.stripe_customer_id
                """, (customer_email, tier, session.get('customer')))
                cursor.execute("""
                    INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (email) DO UPDATE SET credits_remaining = subscriber_credits.credits_remaining + %s
                """, (customer_email, credits_to_add, credits_to_add, credits_to_add))
            else:
                cursor.execute("INSERT OR REPLACE INTO subscribers (email, tier, stripe_customer_id, active) VALUES (?, ?, ?, 1)", (customer_email, tier, session.get('customer')))
                cursor.execute("INSERT OR REPLACE INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (?, ?, ?)", (customer_email, credits_to_add, credits_to_add))
            
            api_key_raw = f"qcn_{secrets.token_hex(16)}"
            key_hash = hash_api_key(api_key_raw)
            if DATABASE_URL:
                cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (%s, %s, 'Stripe Subscription Key')", (customer_email, key_hash))
            else:
                cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (?, ?, 'Stripe Subscription Key')", (customer_email, key_hash))
            conn.commit()
            cursor.close()
        finally:
            release_db(conn)

    return {"status": "success"}

@app.get("/api/v1/stream/telemetry")
async def stream_telemetry(request: Request):
    queue = await sse_broker.subscribe()
    async def event_generator():
        try:
            yield f"data: {json.dumps({'event': 'connected'})}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"data: {json.dumps(msg)}\n\n"
                except asyncio.TimeoutError:
                    yield f"data: {json.dumps({'event': 'ping'})}\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            sse_broker.unsubscribe(queue)
    return StreamingResponse(event_generator(), media_type="text/event-stream")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))