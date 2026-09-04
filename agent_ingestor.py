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

def run_ai_lead_agent():
    print("Generating leads via Gemini...", flush=True)
    prompt = """
    Act as an elite B2B lead generation researcher. Generate 3 realistic, high-value tech/SaaS companies 
    that match a target buyer profile (B2B SaaS, FinTech, or AI Infrastructure). 
    Provide real corporate domain patterns, industry, employee counts as a string (e.g., '10-50'), and LinkedIn URLs.
    Output strictly in valid JSON format matching this exact root structure:
    {
      "leads": [
        {
          "company_name": "...",
          "email": "...",
          "industry": "...",
          "employee_count": "...",
          "linkedin_url": "..."
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

    try:
        lead_json = json.loads(lead_text)
    except Exception as e:
        print(f"JSON Parse Error: {e}", flush=True)
        raise

    # Ensure dictionary wrapper has 'leads' key
    if isinstance(lead_json, list):
        lead_json = {"leads": lead_json}
    elif "leads" not in lead_json:
        # Find first list element if nested differently
        for k, v in lead_json.items():
            if isinstance(v, list):
                lead_json = {"leads": v}
                break

    headers = {"admin-key": ADMIN_SECRET_KEY or ""}

    print("Sending payload to Render...", flush=True)
    print(f"Payload keys: {list(lead_json.keys())}", flush=True)
    for attempt in range(3):
        try:
            res = requests.post(API_URL, json=lead_json, headers=headers, timeout=30)
            print(f"Response Status Code: {res.status_code}", flush=True)
            print(f"Response Body: {res.text}", flush=True)
            if res.status_code == 200:
                print("Success:", res.json(), flush=True)
                return
        except Exception as e:
            print(f"Attempt failed: {e}", flush=True)
        time.sleep(10)
    raise RuntimeError("Failed to reach Render API.")

if __name__ == "__main__":
    run_ai_lead_agent()
