import os
import time
import requests
from google import genai

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

API_URL = "https://nexus-core-yfou.onrender.com/api/v1/admin/upload-leads"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")

def run_ai_lead_agent():
    print("Generating leads via Gemini...")
    prompt = """
    Act as an elite B2B lead generation researcher. Generate 3 realistic, high-value tech/SaaS companies 
    that match a target buyer profile (B2B SaaS, FinTech, or AI Infrastructure). 
    Provide real corporate domain patterns, industry, employee counts, and LinkedIn URLs.
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

    response = client.models.generate_content(
        model="gemini-3.6-flash",
        contents=prompt,
    )

    lead_payload = response.text

    headers = {
        "Content-Type": "application/json",
        "admin_key": ADMIN_SECRET_KEY
    }

    print(f"Sending payload to Render...")
    for attempt in range(3):
        try:
            res = requests.post(API_URL, data=lead_payload, headers=headers, timeout=30)
            print(f"Response Status Code: {res.status_code}")
            if res.status_code == 200:
                print("Agent Ingestion Success:", res.json())
                return
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
        time.sleep(10)

    raise RuntimeError("Failed to reach live Render API after multiple retries.")

if __name__ == "__main__":
    run_ai_lead_agent()