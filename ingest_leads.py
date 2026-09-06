from datetime import datetime, timezone
import os
import sqlite3
import httpx
from google import genai

DB_PATH = "quantcode_nexus.db"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS b2b_leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo_name TEXT,
            url TEXT UNIQUE,
            founder_email TEXT,
            industry TEXT DEFAULT 'SaaS / Tech',
            employee_count TEXT DEFAULT '10-50',
            confidence_score REAL DEFAULT 0.9,
            timestamp TEXT
        )
    """
    )
    conn.commit()
    conn.close()


def generate_lead_details_with_ai(repo_name: str, owner_login: str):
    """Attempt generation across multiple active Flash models to bypass 503 capacity issues."""
    if not ai_client:
        return f"contact@{owner_login.lower()}dev.com", "SaaS / Tech", "10-50", 0.9

    candidate_models = ["gemini-3.7-flash", "gemini-3.5-flash", "gemini-3.5-flash-lite"]

    prompt = (
        f"For the GitHub repository '{repo_name}' by owner '{owner_login}', estimate a realistic corporate email format "
        f"(e.g. contact@domain.com), industry, employee count ('10-50', '51-200', etc.), and confidence_score float (0.0 to 1.0). "
        f"Return strictly valid JSON matching this schema: "
        f'{{"email": "...", "industry": "...", "employee_count": "...", "confidence_score": 0.95}}'
    )

    for model_name in candidate_models:
        try:
            response = ai_client.models.generate_content(
                model=model_name, contents=prompt
            )
            raw_text = response.text.strip()
            if raw_text.startswith("```json"):
                raw_text = raw_text[7:-3].strip()
            elif raw_text.startswith("```"):
                raw_text = raw_text[3:-3].strip()

            import json

            data = json.loads(raw_text)
            return (
                data.get("email", f"contact@{owner_login.lower()}dev.com"),
                data.get("industry", "SaaS / Tech"),
                data.get("employee_count", "10-50"),
                data.get("confidence_score", 0.9)
            )
        except Exception as e:
            print(
                f"Model {model_name} failed or unavailable: {e}. Trying next..."
            )

    return f"contact@{owner_login.lower()}dev.com", "SaaS / Tech", "10-50", 0.9


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

            founder_email, industry, employee_count, confidence_score = (
                generate_lead_details_with_ai(repo_name, owner_login)
            )
            current_time = datetime.now(timezone.utc).strftime(
                "%Y-%m-%d %H:%M:%S"
            )

            cursor.execute("SELECT id FROM b2b_leads WHERE url = ?", (repo_url,))
            if not cursor.fetchone():
                cursor.execute(
                    """
                    INSERT INTO b2b_leads (repo_name, url, founder_email, industry, employee_count, confidence_score, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        repo_name,
                        repo_url,
                        founder_email,
                        industry,
                        employee_count,
                        confidence_score,
                        current_time,
                    ),
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