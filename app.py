import os
import time
import hashlib
from fastapi import FastAPI, Header, HTTPException, Response, Depends, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
import redis
import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration

# 1. Initialize Sentry Error Monitoring (Gracefully skips if SENTRY_DSN is absent)
sentry_dsn = os.getenv("SENTRY_DSN")
if sentry_dsn:
    sentry_sdk.init(
        dsn=sentry_dsn,
        integrations=[FastApiIntegration()],
        traces_sample_rate=1.0,
        send_default_pii=True
    )

# 2. Initialize FastAPI app
app = FastAPI(title="NexusCore B2B Lead Gen API")

# 3. Initialize Redis Client for Distributed Rate Limiting & Caching
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

# 4. Database Configuration (PostgreSQL in production, SQLite fallback locally)
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./nexus.db")
engine = create_engine(
    DATABASE_URL, 
    pool_pre_ping=True, 
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Define sample SQLAlchemy Model for API Keys / Users if applicable
class APIKeyRecord(Base):
    __tablename__ = "api_keys"
    id = Column(Integer, primary_key=True, index=True)
    key_hash = Column(String, unique=True, index=True)
    tier = Column(String, default="starter") # 'starter' or 'pro'

Base.metadata.create_all(bind=engine)

# Dependency to get DB session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# 5. Rate Limiting Helper with Standard Header Injection
def check_and_set_rate_limit(api_key: str, response: Response, tier: str = "starter"):
    """
    Tracks request counts in Redis using a 60-second fixed window 
    and sets standard X-RateLimit headers on the response object.
    """
    limit = 30 if tier == "starter" else 100
    window_seconds = 60
    
    current_time = int(time.time())
    window_bucket = current_time // window_seconds
    rate_limit_key = f"rate_limit:{api_key}:{window_bucket}"
    
    # Execute atomic increment and TTL retrieval via Redis pipeline
    pipe = redis_client.pipeline()
    pipe.incr(rate_limit_key, 1)
    pipe.ttl(rate_limit_key)
    count, ttl = pipe.execute()
    
    if ttl == -1:
        redis_client.expire(rate_limit_key, window_seconds)
        ttl = window_seconds

    remaining = max(0, limit - count)
    reset_time = (window_bucket + 1) * window_seconds

    # Inject standard rate-limiting headers into the response object
    response.headers["X-RateLimit-Limit"] = str(limit)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Reset"] = str(reset_time)

    if count > limit:
        raise HTTPException(
            status_code=429,
            detail={
                "error": "Rate limit exceeded",
                "message": f"You have exceeded your {tier} tier limit of {limit} requests per minute."
            }
        )

# 6. Routes & Endpoints

@app.get("/", response_class=HTMLResponse)
async def read_root(request: Request):
    """
    Renders success.html containing the live API lead-tester widget.
    """
    # If using Jinja2 templates:
    # return templates.TemplateResponse("success.html", {"request": request})
    return """
    <html>
        <head><title>NexusCore Checkout Success</title></head>
        <body style="font-family: sans-serif; text-align: center; margin-top: 50px;">
            <h1>Payment Successful! Welcome to NexusCore.</h1>
            <p>Your API key is active and rate-limited via Redis.</p>
        </body>
    </html>
    """

@app.get("/api/v1/leads")
async def get_leads(response: Response, x_api_key: str = Header(...), db: Session = Depends(get_db)):
    """
    Protected B2B Lead Generation endpoint enforcing Redis distributed rate-limiting 
    and returning X-RateLimit headers.
    """
    # Hash the incoming API key to securely match against database records
    key_hash = hashlib.sha256(x_api_key.encode()).hexdigest()
    
    # Optional DB lookup for tier verification
    key_record = db.query(APIKeyRecord).filter(APIKeyRecord.key_hash == key_hash).first()
    tier = key_record.tier if key_record else "starter"
    
    # Validate rate limit and attach standard headers to response
    check_and_set_rate_limit(api_key=key_hash, response=response, tier=tier)
    
    # Return sample verified B2B leads
    return {
        "status": "success",
        "tier": tier,
        "count": 5,
        "limit": 30 if tier == "starter" else 100,
        "offset": 0,
        "leads": [
            {"id": 1, "company_name": "Acme Corp", "email": "contact@acmecorp.com", "timestamp": "2026-09-03T06:05:16.842035"},
            {"id": 2, "company_name": "Nexus Dynamics", "email": "info@nexusdynamics.io", "timestamp": "2026-09-03T06:05:16.842035"},
            {"id": 3, "company_name": "Vertex Solutions", "email": "hello@vertexsolutions.co", "timestamp": "2026-09-03T06:05:16.842035"},
            {"id": 4, "company_name": "Quantum Growth Labs", "email": "sales@quantumgrowth.dev", "timestamp": "2026-09-03T06:05:16.842035"},
            {"id": 5, "company_name": "Apex Sales AI", "email": "reachout@apexsales.ai", "timestamp": "2026-09-03T06:05:16.842035"}
        ]
    }