import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/quantcode_nexus")

# Initialize Threaded Connection Pooling for production concurrency
db_pool = ThreadedConnectionPool(minconn=2, maxconn=20, dsn=DATABASE_URL)

def get_db():
    """Acquires a connection from the pool."""
    conn = db_pool.getconn()
    return conn

def release_db(conn):
    """Releases a connection back to the pool."""
    if conn:
        db_pool.putconn(conn)

def init_db():
    """Initializes PostgreSQL extensions (pgvector, pg_trgm) and core tables."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("CREATE EXTENSION IF NOT EXISTS vector;")
            cur.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm;")
            
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
            cur.execute("""
                CREATE INDEX IF NOT EXISTS b2b_leads_hnsw_idx 
                ON b2b_leads USING hnsw (embedding vector_cosine_ops);
            """)

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
            
            conn.commit()
    finally:
        release_db(conn)