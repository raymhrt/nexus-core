import os
import time
import json
import requests
from google import genai

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
    Provide valid company_name (string), email (string), industry (string), employee_count (string like '10-50'), and linkedin_url (string). 
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
    models_to_try = ["gemini-2.5-flash", "gemini-1.5-flash", "gemini-1.5-pro"]
    
    for model_name in models_to_try:
        print(f"Attempting generation with model: {model_name}", flush=True)
        for attempt in range(3):
            try:
                response = client.models.generate_content(
                    model=model_name,
                    contents=prompt,
                )
                if response and response.text:
                    break
            except Exception as e:
                print(f"Model {model_name} attempt {attempt + 1} failed: {e}. Retrying...", flush=True)
                time.sleep(5)
        if response and response.text:
            break

    if not response or not response.text:
        raise RuntimeError("All Gemini models are currently unavailable.")

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
        print(f"Raw text was: {lead_text}", flush=True)
        raise

    if isinstance(lead_json, list):
        lead_json = {"leads": lead_json}
    elif not isinstance(lead_json, dict) or "leads" not in lead_json:
        found_list = None
        for v in lead_json.values():
            if isinstance(v, list):
                found_list = v
                break
        lead_json = {"leads": found_list if found_list is not None else [lead_json]}

    normalized_leads = []
    for item in lead_json.get("leads", []):
        if not isinstance(item, dict):
            continue
        normalized_leads.append({
            "company_name": item.get("company_name") or item.get("company") or "Unknown Corp",
            "email": item.get("email") or "contact@unknown.io",
            "industry": item.get("industry") or "SaaS / Tech",
            "employee_count": str(item.get("employee_count") or "10-50"),
            "linkedin_url": item.get("linkedin_url") or item.get("linkedin") or ""
        })
    lead_json = {"leads": normalized_leads}

    headers = {
        "admin-key": ADMIN_SECRET_KEY or "",
        "Admin-Key": ADMIN_SECRET_KEY or ""
    }

    print("Sending JSON payload to Render...", flush=True)
    print(f"Payload: {json.dumps(lead_json, indent=2)}", flush=True)
    
    for attempt in range(3):
        try:
            # CRITICAL FIX: Use `json=lead_json` instead of `data=` to serialize as application/json
            res = requests.post(API_URL, json=lead_json, headers=headers, timeout=30)
            print(f"Response Status Code: {res.status_code}", flush=True)
            try:
                error_detail = json.dumps(res.json(), indent=2)
            except Exception:
                error_detail = res.text
            print(f"=== FASTAPI RESPONSE BODY ===\n{error_detail}\n=============================", flush=True)
            
            if res.status_code == 200:
                print("Success:", res.json(), flush=True)
                return
        except Exception as e:
            print(f"Attempt failed: {e}", flush=True)
        time.sleep(10)
    
    raise RuntimeError("Failed to reach live Render API due to validation error.")

if __name__ == "__main__":
    run_ai_lead_agent()
