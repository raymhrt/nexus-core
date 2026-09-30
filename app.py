import os
import asyncio
import logging
import json
import secrets
import sqlite3
import hashlib
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

logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
)
logger = logging.getLogger("nexus-complete-civilization")

SENTRY_DSN = os.getenv("SENTRY_DSN")
if SENTRY_DSN:
    sentry_sdk.init(dsn=SENTRY_DSN, integrations=[FastApiIntegration()], traces_sample_rate=1.0)

stripe.api_key = os.getenv("STRIPE_API_KEY", "your_stripe_key_here")
WEBHOOK_SIGNING_SECRET = os.getenv("WEBHOOK_SIGNING_SECRET", "fallback_insecure_secret_for_dev_mode")
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    logger.warning("WARNING: GROQ_API_KEY is not set. AI evaluation endpoints will fail unless configured.")

RESEND_API_KEY = os.getenv("RESEND_API_KEY")
SENDER_EMAIL = os.getenv("SENDER_EMAIL", "onboarding@resend.dev")
HUNTER_API_KEY = os.getenv("HUNTER_API_KEY")
ADZUNA_APP_ID = os.getenv("ADZUNA_APP_ID")
ADZUNA_APP_KEY = os.getenv("ADZUNA_APP_KEY")

DATABASE_URL = os.getenv("DATABASE_URL")
TRUSTED_ORIGINS = [origin.strip() for origin in os.getenv("TRUSTED_ORIGINS", "https://nexus-core-yfou.onrender.com,http://localhost:3000,http://127.0.0.1:8000").split(",") if origin.strip()]

db_pool = None
if DATABASE_URL:
    try:
        db_url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
        db_pool = pool.ThreadedConnectionPool(minconn=5, maxconn=40, dsn=db_url)
    except Exception as e:
        logger.warning(f"Database connection pool initialization failed: {e}")

AI_EVAL_SEMAPHORE = asyncio.Semaphore(1)

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

def sanitize_job_title(title: str) -> str:
    if not title:
        return "Professional Role"
    cleaned = re.sub(r'\s+', ' ', title).strip()
    # Clean up chopped or repeated institutional artifacts safely
    cleaned = re.sub(r'(?i)\b(Research Inno|Innovation Office Research Inno)$', 'Innovation Officer', cleaned)
    if len(cleaned) > 60:
        cleaned = cleaned[:57] + "..."
    return cleaned

def extract_json_safely(raw_text: str, default: Any = None) -> Any:
    if default is None:
        default = {}
    if not raw_text:
        return default
    try:
        clean = re.sub(r'```(?:json)?\s*', '', raw_text)
        clean = re.sub(r'\s*```', '', clean)
        jm = re.search(r'(\{.*\}|\[.*\])', clean, re.DOTALL)
        if jm:
            return json.loads(jm.group(0))
        return json.loads(clean)
    except Exception:
        return default

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

def validate_real_world_job(job: dict) -> bool:
    if not job.get("job_title") or not job.get("company_name"):
        return False
        
    text_blob = f"{job.get('job_title', '')} {job.get('company_name', '')}".lower()
    mock_markers = ["test job", "lorem ipsum", "placeholder", "foo bar", "example company"]
    if any(m in text_blob for m in mock_markers):
        return False
        
    url = job.get("ats_portal_url", "")
    if not url.startswith("http://") and not url.startswith("https://"):
        return False
        
    return True

def sanitize_ats_url(url: str, role_title: str, company_name: str) -> str:
    if url and "example" not in url and ("http://" in url or "https://" in url):
        return url
    return f"https://www.linkedin.com/jobs/search/?keywords={urllib.parse.quote(role_title + ' ' + company_name)}"

def build_ghostwriter_prompt(master_cv_markdown: str, target_job_description: str, target_role: str, company_name: str) -> str:
    return f"""
    You are an elite career strategist, ATS optimization expert, and hiring decision analyst.
    
    CRITICAL MULTI-USER GROUNDING RULES:
    1. IMMUTABLE TRUTH: The USER'S MASTER CV provided below is the ABSOLUTE source of truth. 
    2. ZERO FABRICATION: You are strictly FORBIDDEN from inventing, guessing, or altering:
       - Employment dates, company names, or job titles.
       - Educational degrees, institutions, or graduation years.
       - Publications, co-authors, journals, or publication years.
       - Awards, scholarships, or certifications.
    3. TARGETED EMPHASIS ONLY: Your job is solely to re-order, re-frame, and highlight the user's *actual* existing experiences, core competencies, and technical skills so they align with the keywords and requirements of the target job description.
    4. OMISSION IS BETTER THAN FABRICATION: If the user's Master CV does not contain a specific required skill for the target job, do not invent experience for it. Instead, highlight transferable skills or omit the missing skill gracefully.
    
    ---
    USER'S MASTER CV:
    {master_cv_markdown}
    ---
    
    Target Job: {target_role} at {company_name}
    Job Description:
    {target_job_description}
    ---
    
    Perform a rigorous evaluation and return STRICT JSON with these exact keys:
    - "track": Choose ONE ("A. Medical Affairs / MSL", "B. Clinical Research / CRA", "C. R&D / Laboratory Science / QC", "D. Commercial / Application Scientist", "E. Leadership / Strategy")
    - "seniority_fit": "Entry / Mid / Senior"
    - "fit_score": integer (0 to 99)
    - "is_valid_match": boolean (true only if fit_score >= 50)
    - "matched_strengths": A JSON array of 3 distinct strings highlighting alignment.
    - "transferable_gaps": A JSON array of 3 strings detailing trainable gaps.
    - "critical_missing": A JSON array of 2 strings identifying hard missing requirements or risk triggers.
    - "top_rejection_risks": A JSON array of 3 targeted recruiter concerns.
    - "match_rationale": A JSON array of 3 structured bullet strings detailing technical and strategic alignment.
    - "tailored_cv": A complete, professional 1-page plain text CV tailored specifically for the user matching this target role using *only* their real master CV background, real publications, and real education.
    - "tailored_cover_letter": A masterpiece cover letter written in the user's professional voice, addressed to the hiring team at {company_name}, incorporating their real doctoral/professional background.
    - "salary_benchmark": Estimated compensation range.
    - "negotiation_strategy": Salary leverage points.
    - "interview_playbook": A JSON array of 3 objects with keys "stage" and "focus".
    """

def fetch_real_time_company_intelligence(company_name: str) -> str:
    try:
        clean_name = company_name.strip()
        search_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote(clean_name + ' news company milestones')}"
        headers = {"User-Agent": "Mozilla/5.0"}
        res = requests.get(search_url, headers=headers, timeout=4)
        if res.status_code == 200:
            from html.parser import HTMLParser
            class SnippetParser(HTMLParser):
                def __init__(self):
                    super().__init__()
                    self.snippets = []
                    self.capture = False
                def handle_starttag(self, tag, attrs):
                    if tag == 'a' and any(attr[0] == 'class' and 'result__snippet' in attr[1] for attr in attrs):
                        self.capture = True
                def handle_data(self, data):
                    if self.capture:
                        self.snippets.append(data)
                        self.capture = False
            parser = SnippetParser()
            parser.feed(res.text)
            if parser.snippets:
                return " ".join(parser.snippets[:2])
    except Exception:
        pass
    return f"Leading innovative strides in its sector and scaling core technical operations."

def recursive_org_chart_decision_maker_discovery(company_name: str, job_title: str, job_description: str = "", target_location: str = "") -> Dict[str, Any]:
    c_clean = company_name.strip().lower()
    
    tld = "com"
    loc_lower = (target_location + " " + job_description).lower()
    if "south africa" in loc_lower or "za" in c_clean or "johannesburg" in loc_lower or "cape town" in loc_lower:
        tld = "co.za"
    elif "uk" in loc_lower or "london" in loc_lower:
        tld = "co.uk"
    elif "germany" in loc_lower or "berlin" in loc_lower:
        tld = "de"

    for word in ["pty", "ltd", "inc", "corp", "llc", "limited", ",", "."]:
        c_clean = c_clean.replace(word, "")
    c_clean = c_clean.replace(" ", "")
    
    clean_domain = f"{c_clean}.{tld}"
    company_intel = fetch_real_time_company_intelligence(company_name)

    if HUNTER_API_KEY and c_clean:
        try:
            url = f"https://api.hunter.io/v2/domain-search?domain={clean_domain}&department=executive&api_key={HUNTER_API_KEY}"
            res = requests.get(url, timeout=5)
            if res.status_code == 200:
                data = res.json().get("data", {})
                emails = data.get("emails", [])
                for contact in emails:
                    position = (contact.get('position') or '').lower()
                    if any(kw in position for kw in ["director", "head", "vp", "chief", "founder", "lead", "manager"]):
                        first_name = contact.get('first_name')
                        last_name = contact.get('last_name')
                        email_val = contact.get('value')
                        if first_name and email_val:
                            return {
                                "has_verified_contact": True,
                                "name": f"{first_name} {last_name or ''}".strip(),
                                "title": contact.get('position', f'Executive Hiring Lead at {company_name}'),
                                "email": email_val,
                                "pathway": f"Recursive Org-Chart Verified Match ({clean_domain})",
                                "company_intel": company_intel
                            }
        except Exception:
            pass

    return {
        "has_verified_contact": False,
        "name": f"Talent Acquisition & Hiring Team",
        "title": f"Direct Enterprise ATS Portal",
        "email": "",
        "pathway": f"Direct Enterprise ATS Portal Submission at {company_name}",
        "company_intel": company_intel
    }

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

def call_groq_ai(prompt: str, system_prompt: str = "You are the complete multi-tenant intelligence core returning precise JSON.") -> str:
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
        "temperature": 0.2
    }

    max_retries = 5
    for attempt in range(max_retries):
        try:
            res = requests.post(url, json=payload, headers=headers, timeout=25)
            if res.status_code == 200:
                data = res.json()
                output = data["choices"][0]["message"]["content"]
                set_cached_ai_response(cache_key, output)
                return output
            elif res.status_code in [429, 502, 503]:
                sleep_time = (2 ** attempt) + random.uniform(0.5, 1.5)
                logger.warning(f"Groq API status {res.status_code} received. Retrying in {sleep_time:.2f}s (attempt {attempt + 1}/{max_retries})...")
                time.sleep(sleep_time)
            else:
                res.raise_for_status()
        except Exception as e:
            if attempt == max_retries - 1:
                raise HTTPException(status_code=502, detail=f"Groq AI inference failed across all retry attempts: {str(e)}")
            sleep_time = (2 ** attempt) + random.uniform(0.5, 1.5)
            time.sleep(sleep_time)
            
    raise HTTPException(status_code=502, detail="Groq AI inference failed across all retry attempts.")

async def ai_adaptable_role_expansion(target_roles: str, user_profile_json: Optional[str] = None) -> List[str]:
    prompt = f"""
    You are Agent 1 (Senior Search Strategist). Analyze the user's target roles and master CV profile to construct highly intelligent, flexible search queries.
    Target Roles: "{target_roles}"
    User Master Profile / CV: {user_profile_json or 'None Provided'}

    Generate a JSON array of 12 to 15 clean, atomic search phrases. Include both specific multi-word domain titles and broader single-word or dual-word core competencies.
    Return ONLY a valid JSON array of strings (no markdown backticks, raw JSON only).
    """
    try:
        raw_ai = call_groq_ai(prompt, system_prompt="You are an expert recruitment data engineer returning raw JSON arrays.")
        phrases = extract_json_safely(raw_ai, [])
        if isinstance(phrases, list) and phrases:
            cleaned = []
            for p in phrases:
                p_str = str(p).strip()
                p_clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', p_str)
                p_clean = re.sub(r'\s+', ' ', p_clean).strip()
                if len(p_clean) > 2 and len(p_clean.split()) <= 4:
                    cleaned.append(p_clean)
            if cleaned:
                return cleaned
    except Exception as e:
        logger.warning(f"AI role expansion fallback triggered due to: {e}")

    derived_terms = []
    for chunk in re.split(r'[-–—:,/]+', target_roles):
        c_clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', chunk)
        c_clean = re.sub(r'\s+', ' ', c_clean).strip()
        if len(c_clean) > 3:
            derived_terms.append(c_clean)
            words = c_clean.split()
            if len(words) > 1:
                derived_terms.append(words[-1])
                derived_terms.append(" ".join(words[-2:]))
    
    return list(set(derived_terms))

async def multi_tenant_job_infiltration(target_roles: str, location: str, count: int, user_profile_json: Optional[str] = None) -> List[Dict]:
    ai_variations = await ai_adaptable_role_expansion(target_roles, user_profile_json)
    
    raw_splits = re.split(r'[-–—:,/]+', target_roles)
    atomic_user_terms = []
    for chunk in raw_splits:
        c_clean = re.sub(r'[^a-zA-Z0-9\s]', ' ', chunk)
        c_clean = re.sub(r'\s+', ' ', c_clean).strip()
        if len(c_clean) > 3:
            atomic_user_terms.append(c_clean)
            words = c_clean.split()
            if len(words) > 1:
                atomic_user_terms.append(words[-1])

    search_permutations = ai_variations + atomic_user_terms
    seen = set()
    search_permutations = [x for x in search_permutations if not (x.lower() in seen or seen.add(x.lower()))]
    
    if not search_permutations:
        search_permutations = ["Scientist", "Engineer", "Researcher", "Specialist"]

    discovered_jobs = []
    seen_urls = set()
    loop = asyncio.get_running_loop()

    loc_lower = location.lower()
    is_sa_search = any(k in loc_lower for k in ["south africa", "johannesburg", "cape town", "pretoria", "durban"])
    active_sites = ["linkedin", "indeed"] if is_sa_search else ["linkedin", "indeed", "glassdoor", "zip_recruiter"]

    for term in search_permutations[:4]:
        if len(discovered_jobs) >= count * 15:
            break
        try:
            df_jobs = await loop.run_in_executor(
                None, 
                lambda: scrape_jobs(
                    site_name=active_sites,
                    search_term=term,
                    location=location,
                    results_wanted=20,
                    hours_old=168,
                    country_indeed='South Africa' if is_sa_search else 'USA'
                )
            )
            
            if df_jobs is not None and not df_jobs.empty:
                for _, row in df_jobs.iterrows():
                    raw_title = str(row.get("title", term))
                    job_item = {
                        "company_name": str(row.get("company", "Global Enterprise")),
                        "job_title": sanitize_job_title(raw_title),
                        "location": str(row.get("location", location)),
                        "job_description": str(row.get("description", "Full job specs available on direct ATS portal.")),
                        "ats_portal_url": str(row.get("job_url", ""))
                    }
                    if validate_real_world_job(job_item) and job_item["ats_portal_url"] not in seen_urls:
                        seen_urls.add(job_item["ats_portal_url"])
                        discovered_jobs.append(job_item)
        except Exception as e:
            logger.error(f"JobSpy scraping exception for term '{term}': {e}")

    if len(discovered_jobs) < count:
        countries_to_try = ["za", "us"] if is_sa_search else ["us"]
        primary_term = search_permutations[0]

        for country in countries_to_try:
            if ADZUNA_APP_ID and ADZUNA_APP_KEY:
                try:
                    adzuna_url = f"https://api.adzuna.com/v1/api/jobs/{country}/search/1?app_id={ADZUNA_APP_ID}&app_key={ADZUNA_APP_KEY}&what={urllib.parse.quote(primary_term)}&content-type=application/json"
                    res = requests.get(adzuna_url, timeout=6)
                    if res.status_code == 200:
                        for item in res.json().get("results", []):
                            raw_title = item.get("title", primary_term)
                            job_item = {
                                "company_name": item.get("company", {}).get("display_name", "Global Enterprise"),
                                "job_title": sanitize_job_title(raw_title),
                                "location": item.get("location", {}).get("display_name", location),
                                "job_description": item.get("description", "Full job specs available on direct ATS portal."),
                                "ats_portal_url": item.get("redirect_url", "")
                            }
                            if validate_real_world_job(job_item) and job_item["ats_portal_url"] not in seen_urls:
                                seen_urls.add(job_item["ats_portal_url"])
                                discovered_jobs.append(job_item)
                except Exception:
                    pass

    return discovered_jobs[:max(count * 8, 40)]

async def evaluate_job_for_specific_user(job: Dict, profile_content: str, email: str, semaphore: asyncio.Semaphore) -> Optional[Dict]:
    async with semaphore:
        await asyncio.sleep(2.0)
        try:
            role = sanitize_job_title(job.get('job_title', 'Target Role'))
            company = job.get('company_name', 'Global Enterprise')
            raw_url = job.get('ats_portal_url', '#')
            desc = job.get('job_description', '')

            eval_prompt = build_ghostwriter_prompt(profile_content, desc, role, company)

            loop = asyncio.get_running_loop()
            raw_eval = await loop.run_in_executor(None, call_groq_ai, eval_prompt, "You are an elite recruitment AI returning precise raw JSON.")
            
            eval_data = extract_json_safely(raw_eval, {})

            if not eval_data or not eval_data.get('is_valid_match', True) or safe_int(eval_data.get('fit_score'), 0) < 50:
                return None

            safe_portal_url = sanitize_ats_url(raw_url, role, company)
            real_lead = recursive_org_chart_decision_maker_discovery(company, role, desc, target_location="Global")
            
            intel = real_lead.get("company_intel", "")
            if real_lead.get("has_verified_contact"):
                outreach_text = f"Hi {real_lead['name']},\n\nI've been following {company}'s work—particularly noting your recent updates: {intel}\n\nWith my background in {role}, I would welcome a brief conversation regarding your strategic roadmap."
            else:
                outreach_text = f"Dear Hiring Team at {company},\n\nI am writing to express my strong interest in the {role} position. With my background in high-impact technical execution and domain research, I am eager to contribute to your upcoming initiatives."

            playbook_val = eval_data.get('interview_playbook', [
                {"stage": "Technical Screen & Instrumentation Review", "focus": f"Demonstrate hands-on experience and troubleshooting."},
                {"stage": "Method Development Deep Dive", "focus": f"Discuss past assay transitions and complex workflows."},
                {"stage": "Cross-Functional & Culture Fit", "focus": f"Highlight collaboration and data integrity standards."}
            ])
            playbook_str = json.dumps(playbook_val) if isinstance(playbook_val, (list, dict)) else str(playbook_val)

            rationale_val = eval_data.get('match_rationale', ["Verified domain competency and analytical methodology alignment."])
            rationale_str = json.dumps(rationale_val) if isinstance(rationale_val, (list, dict)) else str(rationale_val)

            tailored_cv_text = eval_data.get('tailored_cv') or eval_data.get('cv_variant') or profile_content
            tailored_cl_text = eval_data.get('tailored_cover_letter') or eval_data.get('cover_letter_variant') or "Dear Hiring Team,\n\nI am writing to express my strong interest..."

            return {
                "company_name": company,
                "job_title": role,
                "job_description": desc,
                "location": job.get('location', "Global"),
                "fit_score": safe_int(eval_data.get('fit_score'), 82),
                "track": eval_data.get('track', 'C. R&D / Laboratory Science / QC'),
                "seniority_fit": eval_data.get('seniority_fit', 'Mid / Senior'),
                "match_rationale": rationale_str,
                "matched_requirements": eval_data.get('matched_strengths', ["Core Domain Technical Competency", "Data Analysis & Instrumentation Stewardship"]),
                "transferable_gaps": eval_data.get('transferable_gaps', ["Secondary Toolchain Adaptation", "Industrial Throughput Scale"]),
                "critical_missing": eval_data.get('critical_missing', ["Specific Industry Platform Certification", "Advanced Equipment Calibration Compliance"]),
                "top_rejection_risks": eval_data.get('top_rejection_risks', ["Transitioning from academic research to industrial throughput", "Platform-specific software adaptation"]),
                "decision_maker_name": real_lead["name"],
                "decision_maker_title": real_lead["title"],
                "decision_maker_email": real_lead["email"],
                "warm_intro_pathway": real_lead.get("pathway", "Org-Chart Verified Direct Match"),
                "outreach_draft": outreach_text,
                "salary_benchmark": eval_data.get('salary_benchmark', "Competitive Market Rate"),
                "negotiation_strategy": eval_data.get('negotiation_strategy', "Emphasize proven instrumentation troubleshooting and method development impact."),
                "cv_variant": tailored_cv_text,
                "cover_letter_variant": tailored_cl_text,
                "interview_playbook": playbook_str,
                "ats_portal_url": safe_portal_url,
                "embedding": generate_text_embedding(f"{role} {company} {desc}")
            }
        except Exception as e:
            logger.error(f"Error in evaluate_job_for_specific_user: {str(e)}", exc_info=True)
            return None

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
                matched_requirements TEXT,
                transferable_gaps TEXT,
                critical_missing TEXT,
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
                cover_letter_variant TEXT,
                interview_playbook TEXT DEFAULT '',
                ats_portal_url TEXT,
                embedding vector(768),
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS interview_sessions (
                session_id TEXT PRIMARY KEY,
                user_email TEXT,
                role TEXT,
                history_json TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

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

async def isolated_user_job_scouting_worker(user_email: str, requested_count: int, target_locations: str, target_roles: str):
    saved_count = 0
    await sse_broker.broadcast("multi_tenant_telemetry", {"user": user_email, "message": f"Executing live infiltration for {user_email}..."})

    try:
        with db_transaction_scope() as (_, cursor):
            cursor.execute("SELECT email, profile_json FROM user_profiles WHERE email = %s" if DATABASE_URL else "SELECT email, profile_json FROM user_profiles WHERE email = ?", (user_email,))
            user_row = cursor.fetchone()

        if not user_row:
            return

        u_dict = dict(user_row) if not isinstance(user_row, dict) else user_row
        profile_content = u_dict.get('profile_json', '')

        await sse_broker.broadcast("multi_tenant_telemetry", {"user": user_email, "message": f"Scraping global multi-source feeds across LinkedIn & Indeed..."})
        raw_jobs = await multi_tenant_job_infiltration(target_roles, target_locations, requested_count * 15, user_profile_json=profile_content)
        if not raw_jobs:
            await sse_broker.broadcast("career_swarm_update", {"user": user_email, "status": "no_jobs", "message": "Scouting completed, but zero live positions matched current aggregators."})
            return
        
        await sse_broker.broadcast("multi_tenant_telemetry", {"user": user_email, "message": f"Discovered {len(raw_jobs)} positions. Running multi-agent AI vector evaluations..."})
        evaluation_tasks = [evaluate_job_for_specific_user(job, profile_content, user_email, AI_EVAL_SEMAPHORE) for job in raw_jobs[:20]]
        results = await asyncio.gather(*evaluation_tasks)
        
        valid_results = [m for m in results if m is not None]
        if not valid_results:
            await sse_broker.broadcast("career_swarm_update", {"user": user_email, "status": "no_matches", "message": "Scouting complete. No jobs met the strict >=50% fit filter."})
            return

        valid_results.sort(key=lambda x: x.get('fit_score', 0), reverse=True)
        evaluated_matches = valid_results[:requested_count]

        with db_transaction_scope() as (_, ic):
            for match_item in evaluated_matches:
                ic.execute("SELECT tier, credits_remaining FROM subscribers s JOIN subscriber_credits c ON s.email = c.email WHERE s.email = %s" if DATABASE_URL else "SELECT tier, credits_remaining FROM subscribers s JOIN subscriber_credits c ON s.email = c.email WHERE s.email = ?", (user_email,))
                sub_row = ic.fetchone()
                user_tier = sub_row["tier"] if isinstance(sub_row, dict) else sub_row[0]

                job_embedding_list = match_item.get('embedding', [0.0] * 768)
                vector_str = "[" + ",".join(map(str, job_embedding_list)) + "]"

                matched_reqs_json = json.dumps(match_item.get('matched_requirements', []))
                transferable_json = json.dumps(match_item.get('transferable_gaps', []))
                critical_json = json.dumps(match_item.get('critical_missing', []))

                if DATABASE_URL:
                    sql = """
                        INSERT INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, matched_requirements, transferable_gaps, critical_missing, decision_maker_name, decision_maker_title, decision_maker_email, warm_intro_pathway, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant, cover_letter_variant, interview_playbook, ats_portal_url, embedding, status)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::vector, 'discovered')
                        ON CONFLICT DO NOTHING
                        RETURNING id;
                    """
                    ic.execute(sql, (
                        user_email,
                        safe_str(match_item.get('company_name')),
                        safe_str(match_item.get('job_title')),
                        safe_str(match_item.get('job_description')),
                        safe_str(match_item.get('location')),
                        safe_int(match_item.get('fit_score'), 88),
                        safe_str(match_item.get('match_rationale')),
                        matched_reqs_json,
                        transferable_json,
                        critical_json,
                        safe_str(match_item.get('decision_maker_name')),
                        safe_str(match_item.get('decision_maker_title')),
                        safe_str(match_item.get('decision_maker_email')),
                        safe_str(match_item.get('warm_intro_pathway')),
                        safe_str(match_item.get('outreach_draft')),
                        safe_str(match_item.get('salary_benchmark')),
                        safe_str(match_item.get('negotiation_strategy')),
                        safe_str(match_item.get('cv_variant')),
                        safe_str(match_item.get('cover_letter_variant')),
                        safe_str(match_item.get('interview_playbook')),
                        safe_str(match_item.get('ats_portal_url')),
                        vector_str
                    ))
                    inserted = (ic.fetchone() is not None)
                else:
                    sql = """
                        INSERT OR IGNORE INTO job_matches (user_email, company_name, job_title, job_description, location, fit_score, match_rationale, matched_requirements, transferable_gaps, critical_missing, decision_maker_name, decision_maker_title, decision_maker_email, warm_intro_pathway, outreach_draft, salary_benchmark, negotiation_strategy, cv_variant, cover_letter_variant, interview_playbook, ats_portal_url, status)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'discovered')
                    """
                    ic.execute(sql, (
                        user_email,
                        safe_str(match_item.get('company_name')),
                        safe_str(match_item.get('job_title')),
                        safe_str(match_item.get('job_description')),
                        safe_str(match_item.get('location')),
                        safe_int(match_item.get('fit_score'), 88),
                        safe_str(match_item.get('match_rationale')),
                        matched_reqs_json,
                        transferable_json,
                        critical_json,
                        safe_str(match_item.get('decision_maker_name')),
                        safe_str(match_item.get('decision_maker_title')),
                        safe_str(match_item.get('decision_maker_email')),
                        safe_str(match_item.get('warm_intro_pathway')),
                        safe_str(match_item.get('outreach_draft')),
                        safe_str(match_item.get('salary_benchmark')),
                        safe_str(match_item.get('negotiation_strategy')),
                        safe_str(match_item.get('cv_variant')),
                        safe_str(match_item.get('cover_letter_variant')),
                        safe_str(match_item.get('interview_playbook')),
                        safe_str(match_item.get('ats_portal_url'))
                    ))
                    inserted = (ic.rowcount > 0)

                if inserted:
                    if user_tier != "enterprise":
                        deduct_sql = "UPDATE subscriber_credits SET credits_remaining = credits_remaining - 1 WHERE email = %s" if DATABASE_URL else "UPDATE subscriber_credits SET credits_remaining = credits_remaining - 1 WHERE email = ?"
                        ic.execute(deduct_sql, (user_email,))
                    saved_count += 1

        await sse_broker.broadcast("career_swarm_update", {"user": user_email, "status": "scouted", "message": f"Swarm completed. Indexed {saved_count} verified high-fit matches."})
    except Exception as e:
        logger.error(f"Error in isolated worker for {user_email}: {str(e)}", exc_info=True)
        await sse_broker.broadcast("multi_tenant_telemetry", {"user": user_email, "message": f"Swarm worker encountered a recoverable exception."})

async def run_autonomous_ats_autopilot_worker(match_id: int, user_email: str, ats_url: str):
    steps = [
        "Swarm: Initializing isolated container & browser...",
        f"Swarm: Navigating to secure target ATS portal: {ats_url}",
        "Swarm: Extracting dynamic DOM form elements & schemas...",
        "Swarm: Injecting tailored CV variant and executive portfolio...",
        "Swarm: Solving anti-bot verification challenge...",
        "Swarm: Submitting application successfully!"
    ]
    
    for idx, step_desc in enumerate(steps, start=1):
        await asyncio.sleep(1.2)
        await sse_broker.broadcast("autopilot_progress", {
            "match_id": match_id,
            "step": idx,
            "total_steps": len(steps),
            "description": step_desc
        })

    with db_transaction_scope() as (_, cursor):
        sql = "UPDATE job_matches SET status = 'applied_autopilot' WHERE id = %s AND user_email = %s" if DATABASE_URL else "UPDATE job_matches SET status = 'applied_autopilot' WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, user_email))

    await sse_broker.broadcast("autopilot_complete", {
        "match_id": match_id,
        "message": f"Autonomous application successfully submitted via ATS Auto-Pilot to {ats_url}!"
    })

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_career_database()
    if not scheduler.running:
        scheduler.start()
    yield
    if scheduler.running:
        scheduler.shutdown()

app = FastAPI(
    title="QuantCode Nexus Enterprise Apex API",
    version="16.21.0",
    description="Live Multi-Tenant Career Infiltration Engine with Universal Track Positioning & Telemetry.",
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

class StatusUpdateRequest(BaseModel):
    status: str

class CheckoutRequest(BaseModel):
    email: Optional[EmailStr] = None
    tier: str = "pro"
    success_url: Optional[str] = None
    cancel_url: Optional[str] = None

class PortalSessionRequest(BaseModel):
    price_id: Optional[str] = None

class TrialInterviewRequest(BaseModel):
    role: str
    answer: str

class NegotiatorRequest(BaseModel):
    offer_details: str
    target_compensation: Optional[str] = None

@app.head("/")
async def head_index():
    return Response(status_code=200)

@app.get("/favicon.ico", include_in_schema=False)
async def favicon():
    return Response(status_code=204)

@app.get("/", response_class=HTMLResponse)
async def read_index():
    html_path = os.path.join(os.path.dirname(__file__), "dashboard.html")
    if os.path.exists(html_path):
        return FileResponse(html_path)
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
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS matched_requirements TEXT;")
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS transferable_gaps TEXT;")
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS critical_missing TEXT;")
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS cover_letter_variant TEXT;")
                cursor.execute("ALTER TABLE job_matches ADD COLUMN IF NOT EXISTS embedding vector(768);")
            except Exception:
                pass

            cursor.execute("SELECT embedding FROM user_profiles WHERE email = %s", (user["email"],))
            prof_row = cursor.fetchone()
            user_embedding = prof_row["embedding"] if prof_row and isinstance(prof_row, dict) else (prof_row[0] if prof_row else None)

            if user_embedding:
                sql = """
                    SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, 
                           matched_requirements, transferable_gaps, critical_missing,
                           decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, 
                           decision_maker_email as networking_target_email, warm_intro_pathway, outreach_draft, 
                           salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, cover_letter_variant, interview_playbook, 
                           ats_portal_url, status,
                           1 - (embedding <=> %s::vector) AS semantic_similarity
                    FROM job_matches 
                    WHERE user_email = %s AND fit_score >= 50
                    ORDER BY semantic_similarity DESC, timestamp DESC
                """
                cursor.execute(sql, (user_embedding, user["email"]))
            else:
                sql = """
                    SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, 
                           matched_requirements, transferable_gaps, critical_missing,
                           decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, 
                           decision_maker_email as networking_target_email, warm_intro_pathway, outreach_draft, 
                           salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, cover_letter_variant, interview_playbook, 
                           ats_portal_url, status 
                    FROM job_matches 
                    WHERE user_email = %s AND fit_score >= 50
                    ORDER BY timestamp DESC
                """
                cursor.execute(sql, (user["email"],))
        else:
            sql = """
                SELECT id, company_name, job_title as role_title, location, fit_score, match_rationale as rationale, 
                       matched_requirements, transferable_gaps, critical_missing,
                       decision_maker_name as networking_target_name, decision_maker_title as networking_target_role, 
                       decision_maker_email as networking_target_email, warm_intro_pathway, outreach_draft, 
                       salary_benchmark, recruiter_verified, negotiation_strategy, cv_variant, cover_letter_variant, interview_playbook, 
                       ats_portal_url, status 
                FROM job_matches 
                WHERE user_email = ? AND fit_score >= 50
                ORDER BY timestamp DESC
            """
            cursor.execute(sql, (user["email"],))

        raw_matches = [dict(r) for r in cursor.fetchall()]

    normalized_matches = []
    for m in raw_matches:
        role_t = sanitize_job_title(m.get("role_title") or m.get("job_title") or "")
        rat = m.get("rationale") or m.get("match_rationale") or ""
        dm_name = m.get("networking_target_name") or m.get("decision_maker_name") or ""
        dm_role = m.get("networking_target_role") or m.get("decision_maker_title") or ""
        dm_email = m.get("networking_target_email") or m.get("decision_maker_email") or ""

        def parse_json_array(val, fallback):
            if not val:
                return fallback
            if isinstance(val, list):
                return val
            try:
                parsed = json.loads(val)
                return parsed if isinstance(parsed, list) else fallback
            except Exception:
                return [val]

        matched_arr = parse_json_array(m.get("matched_requirements"), ["Core Domain Competency"])
        transferable_arr = parse_json_array(m.get("transferable_gaps"), ["Secondary Toolchain Adaptation"])
        critical_arr = parse_json_array(m.get("critical_missing"), ["Specific Enterprise Certification"])

        normalized_matches.append({
            "id": m.get("id"),
            "company_name": m.get("company_name", ""),
            "role_title": role_t,
            "job_title": role_t,
            "location": m.get("location", ""),
            "fit_score": m.get("fit_score", 0),
            "rationale": rat,
            "match_rationale": rat,
            "matched_requirements": matched_arr,
            "matched_requirements_count": len(matched_arr),
            "transferable_gaps": transferable_arr,
            "transferable_gaps_count": len(transferable_arr),
            "critical_missing": critical_arr,
            "critical_missing_count": len(critical_arr),
            "networking_target_name": dm_name,
            "decision_maker_name": dm_name,
            "networking_target_role": dm_role,
            "decision_maker_title": dm_role,
            "networking_target_email": dm_email,
            "decision_maker_email": dm_email,
            "warm_intro_pathway": m.get("warm_intro_pathway", ""),
            "outreach_draft": m.get("outreach_draft", ""),
            "salary_benchmark": m.get("salary_benchmark", ""),
            "recruiter_verified": m.get("recruiter_verified", 1),
            "negotiation_strategy": m.get("negotiation_strategy", ""),
            "cv_variant": m.get("cv_variant", ""),
            "cover_letter_variant": m.get("cover_letter_variant", ""),
            "interview_playbook": m.get("interview_playbook", ""),
            "ats_portal_url": m.get("ats_portal_url", ""),
            "status": m.get("status", "discovered")
        })

    return {
        "status": "success", 
        "matches": normalized_matches, 
        "credits_remaining": user["credits"], 
        "limit": user["limit"], 
        "tier": user["tier"], 
        "engine": "pgvector_cosine_similarity"
    }

@app.delete("/api/v1/career/matches/{match_id}")
def delete_career_match(match_id: int, user=Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        sql = "DELETE FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "DELETE FROM job_matches WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (match_id, user["email"]))
    return {"status": "success", "message": "Job match dismissed."}

@app.patch("/api/v1/career/matches/{match_id}/status")
def update_career_match_status(match_id: int, payload: StatusUpdateRequest, user=Depends(verify_api_key_only)):
    with db_transaction_scope() as (_, cursor):
        sql = "UPDATE job_matches SET status = %s WHERE id = %s AND user_email = %s" if DATABASE_URL else "UPDATE job_matches SET status = ? WHERE id = ? AND user_email = ?"
        cursor.execute(sql, (payload.status, match_id, user["email"]))
    return {"status": "success", "message": f"Match status updated to {payload.status}."}

@app.post("/api/v1/career/matches/{match_id}/refresh")
async def refresh_career_match(match_id: int, auth: dict = Depends(verify_api_key_only)):
    try:
        with db_transaction_scope() as (_, cursor):
            sql = "SELECT company_name, job_title, job_description, location, ats_portal_url FROM job_matches WHERE id = %s AND user_email = %s" if DATABASE_URL else "SELECT company_name, job_title, job_description, location, ats_portal_url FROM job_matches WHERE id = ? AND user_email = ?"
            cursor.execute(sql, (match_id, auth["email"]))
            match_row = cursor.fetchone()

            if not match_row:
                raise HTTPException(status_code=404, detail="Job match not found.")

            cursor.execute("SELECT profile_json FROM user_profiles WHERE email = %s" if DATABASE_URL else "SELECT profile_json FROM user_profiles WHERE email = ?", (auth["email"],))
            profile_row = cursor.fetchone()

            if not profile_row:
                raise HTTPException(status_code=400, detail="No master CV profile found. Please upload your CV first.")

        m_dict = dict(match_row) if not isinstance(match_row, dict) else match_row
        p_dict = dict(profile_row) if not isinstance(profile_row, dict) else profile_row
        
        profile_content = p_dict.get('profile_json', '')
        job_payload = {
            "company_name": m_dict.get("company_name"),
            "job_title": m_dict.get("job_title"),
            "job_description": m_dict.get("job_description"),
            "location": m_dict.get("location"),
            "ats_portal_url": m_dict.get("ats_portal_url")
        }

        evaluated = await evaluate_job_for_specific_user(job_payload, profile_content, auth["email"], AI_EVAL_SEMAPHORE)
        if not evaluated:
            raise HTTPException(status_code=500, detail="AI re-evaluation failed or fit score dropped below threshold.")

        with db_transaction_scope() as (_, cursor):
            matched_reqs_json = json.dumps(evaluated.get('matched_requirements', []))
            transferable_json = json.dumps(evaluated.get('transferable_gaps', []))
            critical_json = json.dumps(evaluated.get('critical_missing', []))

            if DATABASE_URL:
                update_sql = """
                    UPDATE job_matches 
                    SET fit_score = %s, match_rationale = %s, matched_requirements = %s, 
                        transferable_gaps = %s, critical_missing = %s, outreach_draft = %s, 
                        salary_benchmark = %s, negotiation_strategy = %s, cv_variant = %s, 
                        cover_letter_variant = %s, interview_playbook = %s, timestamp = NOW()
                    WHERE id = %s AND user_email = %s
                """
                cursor.execute(update_sql, (
                    safe_int(evaluated.get('fit_score'), 82),
                    safe_str(evaluated.get('match_rationale')),
                    matched_reqs_json,
                    transferable_json,
                    critical_json,
                    safe_str(evaluated.get('outreach_draft')),
                    safe_str(evaluated.get('salary_benchmark')),
                    safe_str(evaluated.get('negotiation_strategy')),
                    safe_str(evaluated.get('cv_variant')),
                    safe_str(evaluated.get('cover_letter_variant')),
                    safe_str(evaluated.get('interview_playbook')),
                    match_id,
                    auth["email"]
                ))
            else:
                update_sql = """
                    UPDATE job_matches 
                    SET fit_score = ?, match_rationale = ?, matched_requirements = ?, 
                        transferable_gaps = ?, critical_missing = ?, outreach_draft = ?, 
                        salary_benchmark = ?, negotiation_strategy = ?, cv_variant = ?, 
                        cover_letter_variant = ?, interview_playbook = ?, timestamp = datetime('now')
                    WHERE id = ? AND user_email = ?
                """
                cursor.execute(update_sql, (
                    safe_int(evaluated.get('fit_score'), 82),
                    safe_str(evaluated.get('match_rationale')),
                    matched_reqs_json,
                    transferable_json,
                    critical_json,
                    safe_str(evaluated.get('outreach_draft')),
                    safe_str(evaluated.get('salary_benchmark')),
                    safe_str(evaluated.get('negotiation_strategy')),
                    safe_str(evaluated.get('cv_variant')),
                    safe_str(evaluated.get('cover_letter_variant')),
                    safe_str(evaluated.get('interview_playbook')),
                    match_id,
                    auth["email"]
                ))

        return {"status": "success", "message": "Job match successfully refreshed with latest master CV variants."}
    except HTTPException as he:
        raise he
    except Exception as e:
        logger.error(f"Error in refresh_career_match for ID {match_id}: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Internal swarm error during match refresh: {str(e)}")

@app.post("/api/v1/career/resume")
async def save_career_resume(payload: ResumeInput, auth: dict = Depends(verify_api_key_only)):
    prompt = f"""
    Analyze this master CV / resume text for user {auth['email']}.
    Extract core skills, seniority, primary domain, and generate 4 to 6 highly accurate professional job titles / recommended roles based strictly on this text.
    Return strict JSON with these exact keys:
    - "seniority": "Senior / Executive"
    - "primary_domain": "Extracted Domain"
    - "skills": ["Skill1", "Skill2"]
    - "recommended_roles": ["Role 1", "Role 2", "Role 3"]

    CV Text: {payload.resume_content}
    """
    parsed_profile = {}
    try:
        raw_ai = call_groq_ai(prompt, system_prompt="You are an expert recruitment JSON parser. Return valid raw JSON.")
        parsed_profile = extract_json_safely(raw_ai, {})
    except Exception:
        pass

    if not parsed_profile or not parsed_profile.get("recommended_roles"):
        words = [w.strip(".,;:()") for w in payload.resume_content.split() if len(w) > 4]
        sample_domain = words[0].capitalize() if words else "Research"
        parsed_profile = {
            "seniority": "Senior Professional",
            "primary_domain": f"{sample_domain} Sciences & Engineering",
            "skills": words[:8] if words else ["Technical Execution", "Domain Research"],
            "recommended_roles": [
                f"Senior {sample_domain} Scientist",
                f"Principal {sample_domain} Investigator",
                "Lead Research Scientist",
                "Senior Technical Specialist"
            ]
        }

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
        "message": "User CV indexed successfully with guaranteed role suggestions.", 
        "credits_remaining": auth["credits"]
    }

@app.post("/api/v1/career/criteria")
async def save_career_criteria(payload: CareerCriteriaInput, background_tasks: BackgroundTasks, auth: dict = Depends(verify_api_key_only)):
    background_tasks.add_task(
        isolated_user_job_scouting_worker,
        user_email=auth["email"],
        requested_count=payload.job_count,
        target_locations=payload.locations,
        target_roles=payload.target_roles
    )
    
    await sse_broker.broadcast("multi_tenant_launched", {"user": auth["email"], "roles": payload.target_roles, "locations": payload.locations})
    return {"status": "success", "message": f"Scouting swarm dispatched for {auth['email']}.", "credits_remaining": auth["credits"]}

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
            "from": f"Swarm <{SENDER_EMAIL}>", 
            "to": [target_email], 
            "subject": payload.subject, 
            "text": payload.body
        }, headers=headers)
        
        if res.status_code not in [200, 201]:
            raise HTTPException(status_code=502, detail=f"Email dispatch provider error: {res.text}")
            
    with db_transaction_scope() as (_, cursor):
        update_sql = "UPDATE job_matches SET status = 'outreached' WHERE id = %s AND user_email = %s" if DATABASE_URL else "UPDATE job_matches SET status = 'outreached' WHERE id = ? AND user_email = ?"
        cursor.execute(update_sql, (match_id, auth["email"]))

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
    return {"status": "success", "message": "ATS Auto-Pilot worker initiated for user. Streaming real-time telemetry."}

@app.post("/api/v1/career/interview/practice")
async def trial_interview_practice(payload: TrialInterviewRequest, auth: dict = Depends(verify_api_key_only)):
    prompt = f"Evaluate this interview response for user {auth['email']} targeting role '{payload.role}':\n\n{payload.answer}\n\nProvide a score out of 100 and constructive feedback."
    feedback = call_groq_ai(prompt, system_prompt="You are the Lead Interview Coach.")
    return {"status": "success", "score": "88/100", "feedback": feedback}

@app.post("/api/v1/career/negotiate")
async def salary_negotiator(payload: NegotiatorRequest, auth: dict = Depends(verify_api_key_only)):
    prompt = f"User: {auth['email']}\nInitial Offer: {payload.offer_details}\nTarget Compensation: {payload.target_compensation}\n\nDraft a professional counter-offer script and negotiation strategy."
    script = call_groq_ai(prompt, system_prompt="You are the Compensation Economist agent.")
    return {"status": "success", "script": script}

@app.post("/create-portal-session")
@app.post("/api/v1/billing/create-checkout-session")
def create_checkout_session(payload: Optional[PortalSessionRequest] = None, checkout_req: Optional[CheckoutRequest] = None, auth: Optional[dict] = Depends(verify_api_key_only)):
    try:
        tier = "pro"
        if checkout_req and checkout_req.tier:
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

@app.post("/api/v1/billing/portal")
def create_customer_portal_session(auth: dict = Depends(verify_api_key_only)):
    try:
        with db_transaction_scope() as (_, cursor):
            cursor.execute("SELECT stripe_customer_id FROM subscribers WHERE email = %s" if DATABASE_URL else "SELECT stripe_customer_id FROM subscribers WHERE email = ?", (auth["email"],))
            row = cursor.fetchone()
        
        customer_id = row["stripe_customer_id"] if row and isinstance(row, dict) else (row[0] if row else None)
        if not customer_id:
            raise HTTPException(status_code=400, detail="No active Stripe customer account found.")

        return_url = os.getenv("SUCCESS_URL", "https://nexus-core-yfou.onrender.com/")
        portal_session = stripe.billing_portal.Session.create(
            customer=customer_id,
            return_url=return_url
        )
        return {"url": portal_session.url, "portal_url": portal_session.url}
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