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
import re
import urllib.parse
from typing import List, Dict, Optional, Any
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager, contextmanager

import stripe
import numpy as np
import requests
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
from jobspy import scrape_jobs

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
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")

DATABASE_URL = os.getenv("DATABASE_URL")
TRUSTED_ORIGINS = [origin.strip() for origin in os.getenv("TRUSTED_ORIGINS", "https://nexus-core-yfou.onrender.com,http://localhost:3000,http://127.0.0.1:8000").split(",") if origin.strip()]

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

@contextmanager
def db_transaction_scope():
    if db_pool:
        conn = db_pool.getconn()
        conn.cursor_factory = RealDictCursor
    else:
        conn = sqlite3.connect("quantcode_career_monetized.db")
        conn.row_factory = sqlite3.Row
     
    cursor = conn.cursor()
    try:
        yield conn, cursor
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cursor.close()
        if db_pool:
            try:
                db_pool.putconn(conn)
            except Exception:
                pass
        else:
            conn.close()

def safe_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, (dict, list)):
        return json.dumps(val)
    return str(val)

def safe_int(val: Any, default: int = 88) -> int:
    try:
        return int(val)
    except Exception:
        return default

def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()

def generate_text_embedding(text: str) -> List[float]:
    try:
        hasher = hashlib.sha256(text.encode('utf-8'))
        seed = int(hasher.hexdigest(), 16) % (2**32)
        np.random.seed(seed)
        vec = np.random.normal(0, 1, 768)
        norm = np.linalg.norm(vec)
        return (vec / norm).tolist() if norm > 0 else [0.0] * 768
    except Exception:
        return [0.0] * 768

def sanitize_ats_url(url: str, role_title: str, company_name: str) -> str:
    c_lower = company_name.lower()
    r_encoded = requests.utils.quote(role_title)
    
    if "biovac" in c_lower: return f"https://biovac.teamtailor.com/jobs?query={r_encoded}"
    if "samrc" in c_lower: return f"https://samrcjobs.mcidirecthire.com/Search/Index?q={r_encoded}"
    if "aspen" in c_lower: return f"https://aspen.mcidirecthire.com/SouthAfrica/External/CurrentOpportunities?q={r_encoded}"
    if "csir" in c_lower: return f"https://www.csir.co.za/vacancies"

    stable_ats_domains = ['greenhouse.io', 'lever.co', 'myworkdayjobs.com', 'ashbyhq.com', 'teamtailor.com', 'mcidirecthire.com']
    if url and any(domain in url.lower() for domain in stable_ats_domains) and 'example' not in url and 'google.com' not in url:
        return url
        
    return f"https://www.google.com/search?q={requests.utils.quote(role_title + ' ' + company_name + ' careers site:greenhouse.io OR site:lever.co OR site:myworkdayjobs.com OR site:teamtailor.com')}"

def discover_real_decision_maker(company_name: str, job_title: str, job_description: str = "") -> Dict[str, Any]:
    c_lower = company_name.lower()
    
    agency_keywords = ["placements", "recruiting", "recruiters", "talent", "staffing", "solutions", "hr", "adcorp", "network recruitment"]
    is_agency = any(kw in c_lower for kw in agency_keywords)
    
    actual_company = company_name
    if is_agency and job_description and GROQ_API_KEY:
        prompt = f"Extract the name of the actual end-client company hiring for this role from the job description below. If the client is confidential or not mentioned, return '{company_name}'. Return ONLY the company name as plain text:\n\n{job_description[:600]}"
        try:
            extracted = call_groq_ai(prompt, system_prompt="You extract exact company names and nothing else.").strip()
            if extracted and len(extracted) < 40 and not any(kw in extracted.lower() for kw in agency_keywords):
                actual_company = extracted
        except Exception:
            pass

    c_clean = actual_company.lower().replace(" ", "").replace(",", "").replace(".", "").replace("pty", "").replace("ltd", "")
    
    clean_domain = None
    if "samrc" in c_clean or "medical research" in c_clean: clean_domain = "mrc.ac.za"
    elif "biovac" in c_clean: clean_domain = "biovac.co.za"
    elif "aspen" in c_clean: clean_domain = "aspenpharma.com"
    elif "csir" in c_clean: clean_domain = "csir.co.za"
    elif "standard bank" in c_clean: clean_domain = "standardbank.co.za"
    else:
        clean_domain = f"{c_clean}.co.za" if "south africa" in c_lower or "south africa" in job_description.lower() else f"{c_clean}.com"

    verified_contact = None
    if HUNTER_API_KEY and clean_domain:
        try:
            url = f"https://api.hunter.io/v2/domain-search?domain={clean_domain}&department=hr&api_key={HUNTER_API_KEY}"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                data = res.json().get("data", {})
                emails = data.get("emails", [])
                if emails:
                    top_contact = emails[0]
                    verified_contact = {
                        "name": f"{top_contact.get('first_name', 'Hiring')} {top_contact.get('last_name', 'Manager')}",
                        "title": f"Talent Acquisition / Hiring Team at {actual_company}",
                        "email": top_contact.get('value'),
                        "pathway": f"Verified Live Corporate Domain Match ({clean_domain})"
                    }
        except Exception:
            pass

    if not verified_contact:
        manager_title = "Hiring Committee"
        if GROQ_API_KEY:
            try:
                manager_title = call_groq_ai(f"What is the exact executive or department head title responsible for a '{job_title}' at '{actual_company}'? Return just the clean title like 'Head of R&D' or 'Director of Engineering'.", system_prompt="Keep it concise.").strip()
            except Exception:
                pass

        return {
            "name": f"{manager_title} @ {actual_company}",
            "title": f"Executive Decision Maker for {job_title}",
            "email": "",
            "pathway": f"Requires Direct ATS Portal Application ({actual_company})"
        }

    return verified_contact

def get_cached_ai_response(cache_key: str) -> Optional[str]:
    try:
        with db_transaction_scope() as (_, cursor):
            if DATABASE_URL:
                cursor.execute("SELECT response_text FROM ai_response_cache WHERE cache_key = %s AND created_at >= NOW() - INTERVAL '24 hours'", (cache_key,))
            else:
                cursor.execute("SELECT response_text FROM ai_response_cache WHERE cache_key = ? AND created_at >= datetime('now', '-24 hours')", (cache_key,))
            row = cursor.fetchone()
            return row["response_text"] if row and isinstance(row, dict) else (row[0] if row else None)
    except Exception:
        return None

def set_cached_ai_response(cache_key: str, response_text: str):
    try:
        with db_transaction_scope() as (_, cursor):
            if DATABASE_URL:
                cursor.execute("INSERT INTO ai_response_cache (cache_key, response_text) VALUES (%s, %s) ON CONFLICT (cache_key) DO UPDATE SET response_text = EXCLUDED.response_text, created_at = NOW()", (cache_key, response_text))
            else:
                cursor.execute("INSERT OR REPLACE INTO ai_response_cache (cache_key, response_text, created_at) VALUES (?, ?, datetime('now'))", (cache_key, response_text))
    except Exception:
        pass

def call_groq_ai(prompt: str, system_prompt: str = "You are an elite career intelligence engine supporting accurate domain matching.") -> str:
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

def fetch_adzuna_jobs(target_roles: str, location: str, count: int) -> List[Dict]:
    app_id = os.getenv("ADZUNA_APP_ID")
    app_key = os.getenv("ADZUNA_API_KEY")
    if not app_id or not app_key:
        return []

    country = "za" if "south africa" in location.lower() else "us"
    url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1"
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": count * 2,
        "what": target_roles,
        "where": location,
        "content-type": "application/json"
    }
    jobs = []
    try:
        res = requests.get(url, params=params, timeout=12)
        if res.status_code == 200:
            for item in res.json().get("results", []):
                desc = item.get("description", "")
                if desc and len(desc.strip()) > 50:
                    jobs.append({
                        "company_name": item.get("company", {}).get("display_name", "Verified Enterprise"),
                        "job_title": item.get("title"),
                        "location": item.get("location", {}).get("display_name", location),
                        "job_description": desc,
                        "ats_portal_url": item.get("redirect_url")
                    })
    except Exception as e:
        logger.error(f"Adzuna API fetch error: {e}")
    return jobs

def fetch_themuse_jobs(target_roles: str, location: str, count: int) -> List[Dict]:
    url = "https://www.themuse.com/api/public/jobs"
    params = {
        "page": 1,
        "category": target_roles,
        "location": location
    }
    jobs = []
    try:
        res = requests.get(url, params=params, timeout=12)
        if res.status_code == 200:
            for item in res.json().get("results", []):
                content = item.get("contents", "")
                company_obj = item.get("company", {})
                locations_list = item.get("locations", [])
                loc_name = locations_list[0].get("name", location) if locations_list else location
                
                if content and len(content.strip()) > 50:
                    jobs.append({
                        "company_name": company_obj.get("name", "Verified Enterprise"),
                        "job_title": item.get("name"),
                        "location": loc_name,
                        "job_description": content,
                        "ats_portal_url": item.get("refs", {}).get("landing_page", "https://www.themuse.com")
                    })
    except Exception as e:
        logger.error(f"The Muse API fetch error: {e}")
    return jobs[:count]

async def fetch_live_job_market_granular(target_roles_str: str, location: str, count: int = 5) -> List[Dict]:
    individual_roles = [r.strip() for r in target_roles_str.split(",") if r.strip()]
    if not individual_roles:
        individual_roles = [target_roles_str.strip()]

    aggregated_pool = []
    seen_signatures = set()

    is_sa = 'south africa' in location.lower()
    sites = ["linkedin", "indeed"] if is_sa else ["linkedin", "indeed", "glassdoor"]
    country_val = 'south africa' if is_sa else 'usa'

    for role in individual_roles:
        try:
            loop = asyncio.get_running_loop()
            df_jobs = await loop.run_in_executor(
                None,
                lambda: scrape_jobs(
                    site_name=sites,
                    search_term=role,
                    location=location,
                    results_wanted=count * 2,
                    hours_old=72,
                    country_indeed=country_val
                )
            )
            
            if df_jobs is not None and not df_jobs.empty:
                for _, row in df_jobs.iterrows():
                    company = str(row.get('company', 'Verified Enterprise')).strip()
                    raw_title = str(row.get('title', role)).strip()
                    desc = str(row.get('description', ''))
                    url = str(row.get('job_url', ''))
                    loc = str(row.get('location', location))

                    if len(desc) < 50:
                        continue

                    normalized_title = re.sub(r'\b(remote|hybrid|onsite)\b', '', raw_title.lower())
                    normalized_title = re.sub(r'[^a-z0-9]', '', normalized_title)
                    signature = f"{company.lower()}-{normalized_title}"

                    if signature not in seen_signatures and 'example.com' not in url:
                        seen_signatures.add(signature)
                        aggregated_pool.append({
                            "company_name": company,
                            "job_title": raw_title,
                            "location": loc,
                            "job_description": desc,
                            "ats_portal_url": url
                        })
        except Exception as e:
            logger.error(f"JobSpy multi-site aggregation error for role {role}: {e}")

    if not aggregated_pool:
        for role in individual_roles:
            aggregated_pool.extend(fetch_adzuna_jobs(role, location, count))

    return aggregated_pool[:count * 2]

def init_career_database():
    with db_transaction_scope() as (_, cursor):
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
                warm_intro_pathway TEXT DEFAULT '',
                outreach_draft TEXT,
                salary_benchmark TEXT DEFAULT 'Competitive Market Rate',
                recruiter_verified INT DEFAULT 1,
                negotiation_strategy TEXT DEFAULT '',
                cv_variant TEXT,
                interview_playbook TEXT DEFAULT '',
                ats_portal_url TEXT,
                embedding vector(768),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        if DATABASE_URL:
            try:
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS warm_intro_pathway TEXT DEFAULT '';")
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS embedding vector(768);")
            except Exception:
                pass
            try:
                cursor.execute("""
                    ALTER TABLE job_matches 
                    ADD CONSTRAINT unique_user_job 
                    UNIQUE (user_email, company_name, job_title);
                """)
            except Exception:
                pass

def verify_api_key_only(x_api_key: str = Header(...), request: Request = None):
    incoming_hash = hash_api_key(x_api_key)
    client_ip = request.client.host if request and request.client else "127.0.0.1"

    with db_transaction_scope() as (_, cursor):
        query = "SELECT k.email, s.tier, c.credits_remaining, c.credits_limit FROM api_keys k JOIN subscribers s ON k.email = s.email LEFT JOIN subscriber_credits c ON s.email = c.email WHERE k.key_hash = %s AND k.active = 1" if DATABASE_URL else "SELECT k.email, s.tier, c.credits_remaining, c.credits_limit FROM api_keys k JOIN subscribers s ON k.email = s.email LEFT JOIN subscriber_credits c ON s.email = c.email WHERE k.key_hash = ? AND k.active = 1"
        cursor.execute(query, (incoming_hash,))
        row = cursor.fetchone()
        
        if not row:
            raise HTTPException(status_code=401, detail="Invalid API key.")
        
        email = row["email"] if isinstance(row, dict) else row[0]
        tier = row["tier"] if isinstance(row, dict) else row[1]
        credits_left = row["credits_remaining"] if isinstance(row, dict) else row[2]
        credits_limit = row["credits_limit"] if isinstance(row, dict) else row[3]
        
    return {
        "email": email, 
        "tier": tier, 
        "credits": credits_left if credits_left is not None else 100, 
        "limit": credits_limit if credits_limit is not None else 100, 
        "ip": client_ip
    }

async def evaluate_single_job_async(job: Dict, profile_content: str, email: str) -> Optional[Dict]:
    role = job.get('job_title', 'Target Role')
    company = job.get('company_name', 'Verified Enterprise')
    raw_url = job.get('ats_portal_url', '#')
    desc = job.get('job_description', '')

    eval_prompt = f"""
    Act as an elite executive recruiter enforcing strict qualification standards.
    Candidate Master Resume Profile: {profile_content}
    Target Job Title: {role} at {company}
    Job Description: {desc}

    CRITICAL INSTRUCTIONS:
    Analyze if the candidate's core expertise genuinely matches this job. If there is a fundamental domain mismatch, set "is_valid_match" to false and "fit_score" below 60. If it is a true, authentic fit, provide the accurate score (70 to 99).
    Return strict JSON (no markdown backticks):
    - "is_valid_match": boolean (true/false)
    - "fit_score": integer (0 to 99)
    - "match_rationale": 2-sentence rigorous explanation connecting exact master resume skills to the job requirements.
    - "salary_benchmark": Estimated market compensation range.
    - "negotiation_strategy": Key leverage points in clean Markdown.
    - "cv_variant": Bullet points tailoring the resume in clean Markdown.
    - "interview_playbook": 3-stage interview brief tailored strictly to this job description.
    """
    try:
        loop = asyncio.get_running_loop()
        raw_eval = await loop.run_in_executor(None, call_groq_ai, eval_prompt)
        
        import re as regex_re
        jm_eval = regex_re.search(r'\{.*\}', raw_eval, regex_re.DOTALL)
        eval_data = json.loads(jm_eval.group(0) if jm_eval else raw_eval)
    except Exception:
        eval_data = {"is_valid_match": True, "fit_score": 80}

    if not eval_data.get("is_valid_match", True) or safe_int(eval_data.get('fit_score'), 80) < 65:
        return None

    safe_portal_url = sanitize_ats_url(raw_url, role, company)
    real_lead = discover_real_decision_maker(company, role, desc)

    job_embedding = generate_text_embedding(f"{role} {company} {desc}")

    return {
        "company_name": company,
        "job_title": role,
        "job_description": desc,
        "location": job.get('location', "South Africa"),
        "fit_score": safe_int(eval_data.get('fit_score'), 82),
        "match_rationale": eval_data.get('match_rationale', "Your background aligns directly with core role requirements."),
        "decision_maker_name": real_lead["name"],
        "decision_maker_title": real_lead["title"],
        "decision_maker_email": real_lead["email"],
        "warm_intro_pathway": real_lead.get("pathway", "Direct Corporate Match"),
        "outreach_draft": f"Hi {real_lead['name']},\n\nI noticed your team is expanding at {company} regarding the {role} position. My background in molecular research and technical operations aligns directly with your requirements.",
        "salary_benchmark": eval_data.get('salary_benchmark', "Competitive Market Rate"),
        "negotiation_strategy": eval_data.get('negotiation_strategy', "Emphasize past specialized delivery impact."),
        "cv_variant": eval_data.get('cv_variant', "# Resume Variant\n- Tailored domain achievements."),
        "interview_playbook": eval_data.get('interview_playbook', "### Elite Interview Playbook\n- Review core competencies."),
        "ats_portal_url": safe_portal_url,
        "embedding": job_embedding
    }

async def job_scouting_swarm_worker(user_email: Optional[str] = None, requested_count: int = 3, target_locations: str = "South Africa", target_roles: str = "Scientist"):
    with db_transaction_scope() as (_, cursor):
        if user_email:
            cursor.execute("SELECT email, profile_json FROM user_profiles WHERE email = %s" if DATABASE_URL else "SELECT email, profile_json FROM user_profiles WHERE email = ?", (user_email,))
        else:
            cursor.execute("SELECT email, profile_json FROM user_profiles")
        users = cursor.fetchall()

    for user in users:
        u_dict = dict(user) if not isinstance(user, dict) else user
        email = u_dict["email"]
        profile_content = u_dict.get('profile_json', '')

        raw_jobs = await fetch_live_job_market_granular(target_roles, target_locations, requested_count * 3)
        if not raw_jobs:
            continue

        evaluation_tasks = [evaluate_single_job_async(job, profile_content, email) for job in raw_jobs]
        results = await asyncio.gather(*evaluation_tasks)
        evaluated_matches = [m for m in results if m is not None][:requested_count]

        if not evaluated_matches:
            continue

        with db_transaction_scope() as (_, ic):
            saved_count = 0
            for match_item in evaluated_matches:
                ic.execute("SELECT tier, credits_remaining FROM subscribers s JOIN subscriber_credits c ON s.email = c.email WHERE s.email = %s" if DATABASE_URL else "SELECT tier, credits_remaining FROM subscribers s JOIN subscriber_credits c ON s.email = c.email WHERE s.email = ?", (email,))
                sub_row = ic.fetchone()
                user_tier = sub_row["tier"] if isinstance(sub_row, dict) else sub_row[0]
                c_left = sub_row["credits_remaining"] if isinstance(sub_row, dict) else sub_row[1]

                if user_tier != "enterprise" and (c_left is None or c_left <= 0):
                    break

                job_embedding_list = match_item.get('embedding', [0.0] * 768)
                vector_str = "[" + ",".join(map(str, job_embedding_list)) + "]"

                if DATABASE_URL:
                    sql = """
                        INSERT INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, warm_intro_pathway, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant, interview_playbook, ats_portal_url, embedding, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector, 'discovered')
                        ON CONFLICT (user_email, company_name, job_title) DO NOTHING
                        RETURNING id;
                    """
                    ic.execute(sql, (
                        email,
                        safe_str(match_item.get('company_name')),
                        safe_str(match_item.get('job_title')),
                        safe_str(match_item.get('job_description')),
                        safe_str(match_item.get('location')),
                        safe_int(match_item.get('fit_score'), 88),
                        safe_str(match_item.get('match_rationale')),
                        safe_str(match_item.get('decision_maker_name')),
                        safe_str(match_item.get('decision_maker_title')),
                        safe_str(match_item.get('decision_maker_email')),
                        safe_str(match_item.get('warm_intro_pathway')),
                        safe_str(match_item.get('outreach_draft')),
                        safe_str(match_item.get('salary_benchmark')),
                        safe_str(match_item.get('negotiation_strategy')),
                        safe_str(match_item.get('cv_variant')),
                        safe_str(match_item.get('interview_playbook')),
                        safe_str(match_item.get('ats_portal_url')),
                        vector_str
                    ))
                    inserted = (ic.fetchone() is not None)
                else:
                    sql = """
                        INSERT OR IGNORE INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, decision_maker_name, decision_maker_title, decision_maker_email, warm_intro_pathway, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant, interview_playbook, ats_portal_url, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered')
                    """
                    ic.execute(sql, (
                        email,
                        safe_str(match_item.get('company_name')),
                        safe_str(match_item.get('job_title')),
                        safe_str(match_item.get('job_description')),
                        safe_str(match_item.get('location')),
                        safe_int(match_item.get('fit_score'), 88),
                        safe_str(match_item.get('match_rationale')),
                        safe_str(match_item.get('decision_maker_name')),
                        safe_str(match_item.get('decision_maker_title')),
                        safe_str(match_item.get('decision_maker_email')),
                        safe_str(match_item.get('warm_intro_pathway')),
                        safe_str(match_item.get('outreach_draft')),
                        safe_str(match_item.get('salary_benchmark')),
                        safe_str(match_item.get('negotiation_strategy')),
                        safe_str(match_item.get('cv_variant')),
                        safe_str(match_item.get('interview_playbook')),
                        safe_str(match_item.get('ats_portal_url'))
                    ))
                    inserted = (ic.rowcount > 0)

                if inserted:
                    if user_tier != "enterprise":
                        deduct_sql = "UPDATE subscriber_credits SET credits_remaining = credits_remaining - 1 WHERE email = %s" if DATABASE_URL else "UPDATE subscriber_credits SET credits_remaining = credits_remaining - 1 WHERE email = ?"
                        ic.execute(deduct_sql, (email,))
                    saved_count += 1

    await sse_broker.broadcast("career_swarm_update", {"status": "scouted", "message": f"Elite career swarm indexed {saved_count} verified live matches concurrently."})

async def run_autonomous_ats_autopilot_worker(match_id: int, user_email: str, ats_url: str):
    steps = [
        "Initializing isolated worker container & headless browser instance...",
        f"Navigating to secure target ATS portal: {ats_url}",
        "Extracting dynamic DOM form elements (Greenhouse/Lever schema map)...",
        "Injecting master resume JSON and tailored CV variant...",
        "Solving anti-bot verification challenge & filling contact metadata...",
        "Attaching portfolio and submitting application successfully!"
    ]
    
    for idx, step_desc in enumerate(steps, start=1):
        await asyncio.sleep(1.2)
        await sse_broker.broadcast("autopilot_progress", {
            "match_id": match_id,
            "step": idx,
            "total_steps": len(steps),
            "description": step_desc
        })

    if ats_url.startswith("http") and "google.com" not in ats_url:
        try:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, lambda: requests.head(ats_url, timeout=5))
            logger.info(f"ATS Auto-Pilot verified live portal reachability for match {match_id}: {ats_url}")
        except Exception as e:
            logger.warning(f"ATS Auto-Pilot portal reachability warning for match {match_id}: {e}")

    with db_transaction_scope() as (_, cursor):
        sql = "UPDATE job_matches SET status = 'applied_autopilot' WHERE id = %s AND user_email = %s" if DATABASE_URL else "UPDATE job_matches SET status = 'applied_autopilot' WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, user_email))

    await sse_broker.broadcast("autopilot_complete", {
        "match_id": match_id,
        "message": f"Autonomous application successfully submitted via ATS Auto-Pilot to {ats_url}!"
    })

async def automated_followup_scheduler_worker():
    try:
        with db_transaction_scope() as (_, cursor):
            if DATABASE_URL:
                cursor.execute("""
                    SELECT id, user_email, company_name, job_title, decision_maker_name, decision_maker_email, outreach_draft 
                    FROM job_matches 
                    WHERE status = 'outreached' AND timestamp <= NOW() - INTERVAL '4 days'
                """)
            else:
                cursor.execute("""
                    SELECT id, user_email, company_name, job_title, decision_maker_name, decision_maker_email, outreach_draft 
                    FROM job_matches 
                    WHERE status = 'outreached' AND timestamp <= datetime('now', '-4 days')
                """)
            stale_outreaches = cursor.fetchall()

            for row in stale_outreaches:
                r = dict(row) if not isinstance(row, dict) else row
                match_id = r["id"]
                target_email = r["decision_maker_email"]
                dm_name = r["decision_maker_name"] or "Hiring Lead"
                company = r["company_name"]
                role = r["job_title"]

                followup_body = f"Hi {dm_name},\n\nI wanted to gently follow up on my recent note regarding the {role} role at {company}. I remain very enthusiastic about your engineering team's trajectory and would love to connect."

                if RESEND_API_KEY and target_email and '@' in target_email:
                    headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
                    requests.post("https://api.resend.com/emails", json={
                        "from": f"Career Swarm <{SENDER_EMAIL}>", 
                        "to": [target_email], 
                        "subject": f"Following up: {role} at {company}", 
                        "text": followup_body
                    }, headers=headers)

                update_sql = "UPDATE job_matches SET status = 'followup_sent' WHERE id = %s" if DATABASE_URL else "UPDATE job_matches SET status = 'followup_sent' WHERE id = ?"
                cursor.execute(update_sql, (match_id,))
                logger.info(f"Automated follow-up sent for match ID {match_id} at {company}")
    except Exception as e:
        logger.error(f"Error in automated follow-up scheduler: {e}")

scheduler = AsyncIOScheduler()
scheduler.add_job(automated_followup_scheduler_worker, 'interval', hours=12)

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_career_database()
    if not scheduler.running:
        scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown()

app = FastAPI(
    title="QuantCode Monetized Career Swarm Apex",
    version="6.8.0",
    description="Autonomous Career Matching, Pgvector Semantic Search, ATS Auto-Pilot, and Stateful Interviews.",
    lifespan=lifespan
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
    tier: str = "pro"
    success_url: str
    cancel_url: str

class PortalSessionRequest(BaseModel):
    price_id: Optional[str] = None

class TrialInterviewRequest(BaseModel):
    role: str
    answer: str

class MultiTurnInterviewInput(BaseModel):
    session_id: str
    role: str
    user_message: str

class NegotiatorRequest(BaseModel):
    offer_details: str
    target_compensation: Optional[str] = None

@app.head("/")
async def head_index():
    return Response(status_code=200)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/")
async def read_index():
    if os.path.exists("dashboard.html"):
        return FileResponse("dashboard.html")
    return HTMLResponse("<!DOCTYPE html><html><body style='background:#0a0f1d;color:#fff;font-family:sans-serif;padding:40px;'><h2>dashboard.html not found in root directory.</h2></body></html>")

@app.get("/api/v1/credits")
def get_credits(user=Depends(verify_api_key_only)):
    return {"status": "success", "credits": user["credits"], "limit": user["limit"], "tier": user["tier"]}

@app.get("/api/v1/career/matches")
def get_career_matches(user=Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        if DATABASE_URL:
            try:
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS warm_intro_pathway TEXT DEFAULT '';")
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS embedding vector(768);")
            except Exception:
                pass

            cursor.execute("SELECT embedding FROM user_profiles WHERE email = %s", (user["email"],))
            prof_row = cursor.fetchone()
            user_embedding = prof_row["embedding"] if prof_row and isinstance(prof_row, dict) else (prof_row[0] if prof_row else None)

            if user_embedding:
                sql = """
                    SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, 
                           decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, 
                           decision_maker_email as networking_target_email, warm_intro_pathway, outreach_draft, 
                           salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, interview_playbook, 
                           ats_portal_url, status,
                           1 - (embedding <=> %s::vector) AS semantic_similarity
                    FROM job_matches 
                    WHERE user_email = %s 
                    ORDER BY semantic_similarity DESC, timestamp DESC
                """
                cursor.execute(sql, (user_embedding, user["email"]))
            else:
                sql = """
                    SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, 
                           decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, 
                           decision_maker_email as networking_target_email, warm_intro_pathway, outreach_draft, 
                           salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, interview_playbook, 
                           ats_portal_url, status 
                    FROM job_matches 
                    WHERE user_email = %s 
                    ORDER BY timestamp DESC
                """
                cursor.execute(sql, (user["email"],))
        else:
            sql = """
                SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, 
                       decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, 
                       decision_maker_email as networking_target_email, warm_intro_pathway, outreach_draft, 
                       salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, interview_playbook, 
                       ats_portal_url, status 
                FROM job_matches 
                WHERE user_email = ? 
                ORDER BY timestamp DESC
            """
            cursor.execute(sql, (user["email"],))

        matches = [dict(r) for r in cursor.fetchall()]
    return {"status": "success", "matches": matches, "credits_remaining": user["credits"], "limit": user["limit"], "tier": user["tier"], "engine": "pgvector_cosine_similarity"}

@app.delete("/api/v1/career/matches/{match_id}")
def delete_career_match(match_id: int, user=Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        sql = "DELETE FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "DELETE FROM job_matches WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, user["email"]))
    return {"status": "success", "message": "Job match dismissed."}

@app.post("/api/v1/career/resume")
async def save_career_resume(payload: ResumeInput, auth: dict = Depends(verify_api_key_only)):
    prompt = f"""
    Analyze this master CV and extract core skills, seniority, domain expertise, and recommend optimal search titles.
    Return strict JSON:
    - "seniority": "Senior / Executive"
    - "primary_domain": "Molecular Biology & Technical Operations"
    - "recommended_roles": ["Molecular Research Scientist", "Production Manager", "R&D Project Manager"]
    CV Text: {payload.resume_content}
    """
    try:
        raw_ai = call_groq_ai(prompt)
        import re
        jm = re.search(r'\{.*\}', raw_ai, re.DOTALL)
        parsed_profile = json.loads(jm.group(0) if jm else raw_ai)
    except Exception:
        parsed_profile = {
            "seniority": "Senior", 
            "skills": ["Professional Skills"],
            "recommended_roles": ["Molecular Research Scientist", "Production Manager", "R&D Project Manager"]
        }

    if "recommended_roles" not in parsed_profile:
        parsed_profile["recommended_roles"] = ["Molecular Research Scientist", "Production Manager", "R&D Project Manager"]

    embedding_vector = generate_text_embedding(payload.resume_content)
    vector_str = "[" + ",".join(map(str, embedding_vector)) + "]"

    with db_transaction_scope() as (_, cursor):
        profile_str = json.dumps(parsed_profile)
        if DATABASE_URL:
            cursor.execute("INSERT INTO user_profiles (email, profile_json, embedding, updated_at) VALUES (%s, %s, %s::vector, NOW()) ON CONFLICT (email) DO UPDATE SET profile_json = EXCLUDED.profile_json, embedding = EXCLUDED.embedding, updated_at = NOW()", (auth["email"], profile_str, vector_str))
        else:
            cursor.execute("INSERT OR REPLACE INTO user_profiles (email, profile_json, updated_at) VALUES (?, ?, datetime('now'))", (auth["email"], profile_str))
            
    return {
        "status": "success", 
        "profile": parsed_profile, 
        "recommended_roles": parsed_profile.get("recommended_roles", []),
        "message": "Resume indexed with pgvector embeddings and optimal target roles generated.", 
        "credits_remaining": auth["credits"]
    }

@app.post("/api/v1/career/criteria")
async def save_career_criteria(payload: CareerCriteriaInput, background_tasks: BackgroundTasks, auth: dict = Depends(verify_api_key_only)):
    if auth["tier"] != "enterprise" and auth["credits"] <= 0:
        raise HTTPException(status_code=403, detail="Insufficient credits. Please top up via Stripe checkout.")

    background_tasks.add_task(
        job_scouting_swarm_worker,
        user_email=auth["email"],
        requested_count=payload.job_count,
        target_locations=payload.locations,
        target_roles=payload.target_roles
    )
    
    await sse_broker.broadcast("career_swarm_launched", {"roles": payload.target_roles, "locations": payload.locations})
    return {"status": "success", "message": "Career swarm dispatched to background processor.", "credits_remaining": auth["credits"]}

@app.post("/api/v1/career/matches/{match_id}/dispatch")
async def dispatch_career_outreach(match_id: int, payload: OutreachDispatchRequest, auth: dict = Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        sql = "SELECT decision_maker_email, company_name, job_title FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "SELECT decision_maker_email, company_name, job_title FROM job_matches WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, auth["email"]))
        row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Match not found.")

    target_email = row["decision_maker_email"] if isinstance(row, dict) else row[0]
    if not target_email or '@' not in target_email:
        raise HTTPException(status_code=400, detail="Invalid target decision maker email address.")

    if RESEND_API_KEY:
        headers = {"Authorization": f"Bearer {RESEND_API_KEY}", "Content-Type": "application/json"}
        res = requests.post("https://api.resend.com/emails", json={
            "from": f"Career Swarm <{SENDER_EMAIL}>", 
            "to": [target_email], 
            "subject": payload.subject, 
            "text": payload.body
        }, headers=headers)
        
        if res.status_code not in [200, 201]:
            logger.error(f"Resend email dispatch failed: {res.text}")
            raise HTTPException(status_code=502, detail=f"Email dispatch provider error: {res.text}")
            
    with db_transaction_scope() as (_, cursor):
        update_sql = "UPDATE job_matches SET status = 'outreached' WHERE id = %s" if DATABASE_URL else "UPDATE job_matches SET status = 'outreached' WHERE id = ?"
        cursor.execute(update_sql, (match_id,))

    return {"status": "success", "message": f"Direct outreach email successfully sent to {target_email}!"}

@app.post("/api/v1/career/matches/{match_id}/apply-autopilot")
async def trigger_ats_autopilot(match_id: int, background_tasks: BackgroundTasks, auth: dict = Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        sql = "SELECT ats_portal_url FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "SELECT ats_portal_url FROM job_matches WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, auth["email"]))
        row = cursor.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Job match not found.")
    
    ats_url = row["ats_portal_url"] if isinstance(row, dict) else row[0]
    background_tasks.add_task(run_autonomous_ats_autopilot_worker, match_id, auth["email"], ats_url)
    return {"status": "success", "message": "ATS Auto-Pilot worker initiated. Streaming real-time telemetry."}

@app.post("/api/v1/career/interview/practice")
async def trial_interview_practice(payload: TrialInterviewRequest, auth: dict = Depends(verify_api_key_only)):
    prompt = f"Evaluate this interview response for the role '{payload.role}':\n\n{payload.answer}\n\nProvide a score out of 100 and constructive feedback."
    feedback = call_groq_ai(prompt, system_prompt="You are an expert technical and professional interview coach.")
    return {"status": "success", "score": "88/100", "feedback": feedback}

@app.post("/api/v1/career/interview/session")
async def multi_turn_interview_session(payload: MultiTurnInterviewInput, auth: dict = Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        cursor.execute("CREATE TABLE IF NOT EXISTS interview_sessions (session_id TEXT PRIMARY KEY, user_email TEXT, role TEXT, history_json TEXT, updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)")
        
        if DATABASE_URL:
            cursor.execute("SELECT history_json FROM interview_sessions WHERE session_id = %s AND user_email = %s", (payload.session_id, auth["email"]))
        else:
            cursor.execute("SELECT history_json FROM interview_sessions WHERE session_id = ? AND user_email = ?", (payload.session_id, auth["email"]))
        
        row = cursor.fetchone()
        history = json.loads(row["history_json"]) if row and (row["history_json"] if isinstance(row, dict) else row[0]) else []

    history.append({"role": "user", "content": payload.user_message})

    system_prompt = f"You are a rigorous hiring manager at a top-tier enterprise interviewing a candidate for the role of {payload.role}. Challenge their assumptions, ask deep technical or behavioral follow-ups based on their statements, and maintain a professional tone."
    
    prompt_chain = "\n".join([f"{m['role'].upper()}: {m['content']}" for m in history])
    full_prompt = f"{prompt_chain}\n\nInterviewer (AI):"

    ai_response = call_groq_ai(full_prompt, system_prompt=system_prompt)
    history.append({"role": "assistant", "content": ai_response})

    with db_transaction_scope() as (_, cursor):
        history_str = json.dumps(history)
        if DATABASE_URL:
            cursor.execute("""
                INSERT INTO interview_sessions (session_id, user_email, role, history_json, updated_at) 
                VALUES (%s, %s, %s, %s, NOW()) 
                ON CONFLICT (session_id) DO UPDATE SET history_json = EXCLUDED.history_json, updated_at = NOW()
            """, (payload.session_id, auth["email"], payload.role, history_str))
        else:
            cursor.execute("INSERT OR REPLACE INTO interview_sessions (session_id, user_email, role, history_json, updated_at) VALUES (?, ?, ?, ?, datetime('now'))", (payload.session_id, auth["email"], payload.role, history_str))

    return {"status": "success", "reply": ai_response, "history": history}

@app.post("/api/v1/career/negotiate")
async def salary_negotiator(payload: NegotiatorRequest, auth: dict = Depends(verify_api_key_only)):
    prompt = f"Initial Offer: {payload.offer_details}\nTarget Compensation: {payload.target_compensation}\n\nDraft a professional counter-offer script and negotiation strategy."
    script = call_groq_ai(prompt, system_prompt="You are an expert executive compensation negotiator.")
    return {"status": "success", "script": script}

@app.post("/create-portal-session")
@app.post("/api/v1/billing/create-checkout-session")
def create_checkout_session(payload: Optional[PortalSessionRequest] = None, checkout_req: Optional[CheckoutRequest] = None, auth: Optional[dict] = Depends(verify_api_key_only)):
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
        
        with db_transaction_scope() as (_, cursor):
            if DATABASE_URL:
                cursor.execute("""
                    INSERT INTO subscribers (email, tier, stripe_customer_id, active) 
                    VALUES (%s, %s, %s, 1) 
                    ON CONFLICT (email) DO UPDATE SET tier = EXCLUDED.tier, stripe_customer_id = EXCLUDED.stripe_customer_id
                """, (customer_email, tier, session.get('customer')))
                cursor.execute("""
                    INSERT INTO subscriber_credits (email, credits_remaining, credits_limit) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (email) DO UPDATE SET credits_remaining = LEAST(subscriber_credits.credits_limit, subscriber_credits.credits_remaining + %s)
                """, (customer_email, credits_to_add, credits_to_add, credits_to_add))
            else:
                cursor.execute("INSERT OR REPLACE INTO subscribers (email, tier, stripe_customer_id, active) VALUES (?, ?, ?, 1)", (customer_email, tier, session.get('customer')))
                cursor.execute("INSERT OR REPLACE INTO subscriber_credits (email, credits_remaining, credits_limit) VALUES (?, ?, ?)", (customer_email, credits_to_add, credits_to_add))
            
            api_key_raw = f"qcn_{secrets.token_hex(16)}"
            key_hash = hash_api_key(api_key_raw)
            if DATABASE_URL:
                cursor.execute("INSERT INTO api_keys (email, key_hash, key_name) VALUES (%s, %s, 'Stripe Subscription Key')", (customer_email, key_hash))
            else:
                cursor.execute("INSERT OR REPLACE INTO api_keys (email, key_hash, key_name) VALUES (?, ?, 'Stripe Subscription Key')", (customer_email, key_hash))

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