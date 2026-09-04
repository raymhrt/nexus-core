import os
import time
import json
import requests
from google import genai
from google.genai.errors import ServerError

api_key = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not api_key:
    raise RuntimeError("No API key found!")

client = genai.Client(api_key=api_key)

API_URL = "https://nexus-core-yfou.onrender.com/api/v1/admin/upload-leads"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
print(f"DEBUG: ADMIN_SECRET_KEY loaded? {bool(ADMIN_SECRET_KEY)}", flush=True)

def run_ai_lead_agent():
    print("Generating leads via Gemini...", flush=True)
    prompt = """
    Act as an elite B2B lead generation researcher. Generate 3 realistic, high-value tech/SaaS companies 
    that match a target buyer profile (B2B SaaS, FinTech, or AI Infrastructure). 
    Provide valid company_name (string), email (string), industry (string), employee_count (string like '10-50'), and linkedin_url (string). 
    Ensure no fields are null or missing.
    Output strictly in valid JSON format matching this exact structure:
    {
      "leads": [
        {
          "company_name": "Acme Corp",
          "email": "contact@acme.io",
          "industry": "B2B SaaS",
          "employee_count": "10-50",
          "linkedin_url": "https://linkedin.com/company/acme"
        }
      ]
    }
    """

    response = None
    models_to_try = ["gemini-2.5-flash", "gemini-1.5-flash"]
    for model_name in models_to_try:
        print(f"Attempting generation with model: {model_name}", flush=True)
        for gemini_attempt in range(2):
            try:
                response = client.models.generate_content(model=model_name, contents=prompt)
                break
            except ServerError as se:
                print(f"Model {model_name} busy: {se}. Retrying...", flush=True)
                time.sleep(5)
        if response:
            break

    if not response:
        raise RuntimeError("All Gemini fallback models are unavailable.")

    lead_text = response.text.strip()
    if lead_text.startswith("```json"):
        lead_text = lead_text[7:]
    if lead_text.startswith("```"):
        lead_text = lead_text[3:]
    if lead_text.endswith("```"):
        lead_text = lead_text[:-3]
    lead_text = lead_text.strip()

    print(f"Cleaned JSON text from Gemini:\n{lead_text}", flush=True)

    try:
        lead_json = json.loads(lead_text)
    except Exception as e:
        print(f"JSON Parse Error: {e}", flush=True)
        raise

    if isinstance(lead_json, list):
        lead_json = {"leads": lead_json}
    elif "leads" not in lead_json:
        for k, v in lead_json.items():
            if isinstance(v, list):
                lead_json = {"leads": v}
                break

    headers = {
        "admin-key": ADMIN_SECRET_KEY or "",
        "Admin-Key": ADMIN_SECRET_KEY or ""
    }

    print("Sending payload to Render...", flush=True)
    for attempt in range(3):
        try:
            res = requests.post(API_URL, json=lead_json, headers=headers, timeout=30)
            print(f"Response Status Code: {res.status_code}", flush=True)
            print(f"FASTAPI VALIDATION ERROR BODY:\n{res.text}", flush=True)
            if res.status_code == 200:
                print("Success:", res.json(), flush=True)
                return
        except Exception as e:
            print(f"Attempt failed: {e}", flush=True)
        time.sleep(10)
    raise RuntimeError("Failed to reach Render API due to validation error.")

if __name__ == "__main__":
    run_ai_lead_agent()
