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

# List of models to try sequentially if quota limits (429) are encountered
FALLBACK_MODELS = [
    "gemini-3.6-flash",
    "gemini-2.5-flash",
    "gemini-3.5-flash-lite"
]

def run_ai_lead_agent():
    if not ai_client:
        print("Error: GEMINI_API_KEY is not configured.")
        return

    prompt = (
        "Generate a JSON list of 3 real, active B2B technology, SaaS, or AI companies. "
        "For each company, provide: "
        "company_name, contact email format (e.g. contact@domain.com), industry, "
        "employee_count (e.g. '51-200'), and linkedin_url. "
        "Return strictly valid JSON matching this schema: "
        '[{"company_name": "...", "email": "...", "industry": "...", "employee_count": "...", "linkedin_url": "..."}]'
    )

    response = None
    success_model = None

    # Try each model in sequence
    for model_name in FALLBACK_MODELS:
        print(f"Attempting to generate leads using model: {model_name}...")
        try:
            response = ai_client.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            success_model = model_name
            print(f"Successfully generated content using {model_name}.")
            break
        except Exception as e:
            print(f"Model {model_name} failed: {e}")
            if "429" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                print(f"Quota exhausted for {model_name}, falling back to next model...")
                continue
            else:
                # If it's a different error (like a bad prompt structure), raise it immediately
                raise e

    if not response:
        print("Error: All fallback models exhausted their rate limits or failed.")
        return

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

    print(f"Pushing {len(leads_data)} leads to Render API using {success_model}: {RENDER_API_URL}")
    try:
        res = requests.post(RENDER_API_URL, json=payload, headers=headers, timeout=15)
        print(f"Response Status: {res.status_code}")
        print(f"Response Body: {res.text}")
    except Exception as err:
        print(f"Failed to transmit leads to Render API: {err}")

if __name__ == "__main__":
    run_ai_lead_agent()