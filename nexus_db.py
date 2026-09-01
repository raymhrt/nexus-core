import sqlite3
from contextlib import contextmanager

DB_NAME = "nexus_ledger.db"

@contextmanager
def get_db():
    conn = sqlite3.connect(DB_NAME)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

def init_db():
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id TEXT PRIMARY KEY,
                agent_id TEXT,
                vertical TEXT,
                amount REAL,
                timestamp TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project TEXT,
                author TEXT,
                timestamp TEXT
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_metrics (
                agent_id TEXT PRIMARY KEY,
                vertical TEXT,
                status TEXT,
                tasks_completed INTEGER
            )
        """)

if __name__ == "__main__":
    init_db()
    print("Database initialized with WAL mode successfully.")