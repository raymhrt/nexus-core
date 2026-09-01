from datetime import datetime, timezone
import sqlite3
import httpx

DB_PATH = "quantcode_nexus.db"


def init_db():
  conn = sqlite3.connect(DB_PATH)
  conn.execute("""
        CREATE TABLE IF NOT EXISTS b2b_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_name TEXT,
            url TEXT,
            founder_email TEXT,
            timestamp TEXT
        )
    """)
  conn.commit()
  conn.close()


def ingest_github_leads(query: str = "fastapi stars:>100"):
  init_db()
  url = f"https://api.github.com/search/repositories?q={query}&sort=updated&order=desc"
  headers = {"Accept": "application/vnd.github+json"}

  try:
    response = httpx.get(url, headers=headers, timeout=10.0)
    response.raise_for_status()
    data = response.json()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    inserted_count = 0
    for repo in data.get("items", []):
      repo_name = repo.get("name")
      repo_url = repo.get("html_url")
      owner = repo.get("owner", {})
      owner_login = owner.get("login", "unknown")
      founder_email = f"contact@{owner_login.lower()}dev.com"
      current_time = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

      cursor.execute("SELECT id FROM b2b_leads WHERE url = ?", (repo_url,))
      if not cursor.fetchone():
        cursor.execute(
            "INSERT INTO b2b_leads (repo_name, url, founder_email, timestamp)"
            " VALUES (?, ?, ?, ?)",
            (repo_name, repo_url, founder_email, current_time),
        )
        inserted_count += 1

    conn.commit()
    conn.close()
    print(
        f"Successfully ingested {inserted_count} new leads into {DB_PATH}."
    )

  except Exception as e:
    print(f"Ingestion error: {e}")


if __name__ == "__main__":
  ingest_github_leads()