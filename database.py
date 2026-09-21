import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()

# Fallback or production DATABASE_URL
DATABASE_URL = os.getenv("DATABASE_URL")

# Handle Render postgres URL prefix compatibility if needed
if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# Initialize pool safely or handle local mock/fallback if running offline without Postgres
db_pool = None
if DATABASE_URL:
    try:
        db_pool = ThreadedConnectionPool(minconn=2, maxconn=20, dsn=DATABASE_URL)
    except Exception as e:
        print(f"⚠️ Warning: Could not initialize PostgreSQL connection pool: {e}")

def get_db():
    """Acquires a connection from the pool."""
    if not db_pool:
        raise RuntimeError("Database connection pool is not initialized. Please check your DATABASE_URL environment variable.")
    conn = db_pool.getconn()
    return conn

def release_db(conn):
    """Releases a connection back to the pool."""
    if conn and db_pool:
        try:
            db_pool.putconn(conn)
        except Exception:
            pass

def init_db():
    """Initializes PostgreSQL extensions (pgvector, pg_trgm) and core tables."""
    if not db_pool:
        print("⚠️ Skipping database initialization because connection pool is inactive.")
        return
        
    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Enable extensions (requires superuser or appropriate permissions on Render postgres)
            try:
                cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
                cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            except Exception as ext_err:
                print(f"Note on extensions: {ext_err}")
                conn.rollback()
            
            # Create Leads table with 768-dim vector column for Gemini embeddings
            cur.execute("""
                CREATE TABLE IF NOT EXISTS b2b_leads (
                    id SERIAL PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    domain TEXT UNIQUE NOT NULL,
                    industry TEXT,
                    employee_count TEXT,
                    tech_stack TEXT,
                    funding_stage TEXT,
                    trust_score INTEGER DEFAULT 80,
                    verified_email INTEGER DEFAULT 1,
                    intent_signals TEXT,
                    embedding vector(768),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            # Create HNSW Index for millisecond semantic similarity search
            try:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS b2b_leads_hnsw_idx 
                    ON b2b_leads USING hnsw (embedding vector_cosine_ops);
                """)
            except Exception as idx_err:
                print(f"Note on index creation: {idx_err}")
                conn.rollback()

            # Create API Keys & RBAC table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS api_keys (
                    id SERIAL PRIMARY KEY,
                    key_hash TEXT UNIQUE NOT NULL,
                    key_name TEXT NOT NULL,
                    role TEXT DEFAULT 'sdr',
                    scope TEXT DEFAULT 'full',
                    tier TEXT DEFAULT 'starter',
                    active INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create Audit Logs table for SOC 2 / GDPR compliance
            cur.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id SERIAL PRIMARY KEY,
                    action TEXT NOT NULL,
                    actor_key_hash TEXT,
                    ip_address TEXT,
                    payload TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)

            # Create Career Matches table for Tab 2 Career Swarm
            cur.execute("""
                CREATE TABLE IF NOT EXISTS career_matches (
                    id SERIAL PRIMARY KEY,
                    role_title TEXT NOT NULL,
                    company_name TEXT NOT NULL,
                    location TEXT,
                    fit_score INTEGER DEFAULT 90,
                    rationale TEXT,
                    cv_variant TEXT,
                    networking_target_name TEXT,
                    networking_target_role TEXT,
                    networking_target_email TEXT,
                    outreach_subject TEXT,
                    outreach_draft TEXT,
                    ats_portal_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            
            conn.commit()
    except Exception as e:
        print(f"Database initialization error: {e}")
        if conn:
            conn.rollback()
        raise e
    finally:
        release_db(conn)