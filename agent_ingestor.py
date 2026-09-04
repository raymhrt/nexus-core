import os
import time
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
    print("Generating leads via Gemini...")
    prompt = """
    Act as an elite B2B lead generation researcher. Generate 3 realistic, high-value tech/SaaS companies 
    that match a target buyer profile (B2B SaaS, FinTech, or AI Infrastructure). 
    Provide real corporate domain patterns, industry, employee counts as a string (e.g., '10-50'), and LinkedIn URLs.
    Output strictly in valid JSON format matching this exact structure:
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
        print(f"Attempting generation with model: {model_name}")
        for gemini_attempt in range(2):
            try:
                response = client.models.generate_content(model=model_name, contents=prompt)
                break
            except ServerError as se:
                print(f"Model {model_name} busy: {se}. Retrying...")
                time.sleep(5)
        if response:
            break

    if not response:
        raise RuntimeError("All Gemini fallback models are unavailable.")

    lead_payload = response.text.strip()
    if lead_payload.startswith("```json"):
        lead_payload = lead_payload[7:]
    if lead_payload.startswith("```"):
        lead_payload = lead_payload[3:]
    if lead_payload.endswith("```"):
        lead_payload = lead_payload[:-3]
    lead_payload = lead_payload.strip()

    headers = {"Content-Type": "application/json", "admin_key": ADMIN_SECRET_KEY}

    print("Sending payload to Render...")
    for attempt in range(3):
        try:
            res = requests.post(API_URL, data=lead_payload, headers=headers, timeout=30)
            print(f"Response Status Code: {res.status_code}")
            print(f"Response Body: {res.text}")
            if res.status_code == 200:
                print("Success:", res.json())
                return
        except Exception as e:
            print(f"Attempt failed: {e}")
        time.sleep(10)
    raise RuntimeError("Failed to reach Render API.")

if __name__ == "__main__":
    run_ai_lead_agent()
