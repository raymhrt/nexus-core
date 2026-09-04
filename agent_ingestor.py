import os
import json
import requests
from google import genai
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
RENDER_API_URL = os.getenv("RENDER_API_URL", "https://nexus-core-yfou.onrender.com/api/v1/admin/upload-leads")

ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

def run_ai_lead_agent():
    if not ai_client:
        print("Error: GEMINI_API_KEY is not configured.")
        return

    print("Generating real B2B leads via Gemini using gemini-3.6-flash...")
    prompt = (
        "Generate a JSON list of 3 real, active B2B technology, SaaS, or AI companies. "
        "For each company, provide: "
        "company_name, contact email format (e.g. contact@domain.com), industry, "
        "employee_count (e.g. '51-200'), and linkedin_url. "
        "Return strictly valid JSON matching this schema: "
        '[{"company_name": "...", "email": "...", "industry": "...", "employee_count": "...", "linkedin_url": "..."}]'
    )

    try:
        response = ai_client.models.generate_content(
            model='gemini-3.6-flash',
            contents=prompt,
        )
    except Exception as e:
        print(f"Gemini API generation failed: {e}")
        raise e

    raw_text = response.text.strip()
    if raw_text.startswith("```json"):
        raw_text = raw_text[7:-3].strip()
    elif raw_text.startswith("```"):
        raw_text = raw_text[3:-3].strip()

    try:
        leads_data = json.loads(raw_text)
    except json.JSONDecodeError as jde:
        print(f"Failed to parse JSON response from Gemini: {jde}")
        print(f"Raw text was: {raw_text}")
        return

    payload = {"leads": leads_data}
    headers = {
        "Content-Type": "application/json",
        "admin-key": ADMIN_SECRET_KEY if ADMIN_SECRET_KEY else ""
    }

    print(f"Pushing {len(leads_data)} leads to Render API: {RENDER_API_URL}")
    try:
        res = requests.post(RENDER_API_URL, json=payload, headers=headers, timeout=15)
        print(f"Response Status: {res.status_code}")
        print(f"Response Body: {res.text}")
    except Exception as err:
        print(f"Failed to transmit leads to Render API: {err}")

if __name__ == "__main__":
    run_ai_lead_agent()