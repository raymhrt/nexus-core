import os
import time
import requests
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

API_URL = "https://nexus-core-yfou.onrender.com/api/v1/admin/upload-leads"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")

def run_ai_lead_agent():
    print("Generating leads via OpenAI...")
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
    
    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    
    lead_payload = response.choices[0].message.content
    
    headers = {
        "Content-Type": "application/json",
        "admin_key": ADMIN_SECRET_KEY
    }
    
    print(f"Sending payload to Render (waking up server if sleeping)...")
    
    # Retry loop to handle Render free-tier cold starts
    for attempt in range(3):
        try:
            res = requests.post(API_URL, data=lead_payload, headers=headers, timeout=30)
            print(f"Response Status Code: {res.status_code}")
            
            if res.headers.get("content-type", "").startswith("application/json"):
                print("Agent Ingestion Success:", res.json())
                return
            else:
                print(f"Non-JSON response received (likely Render cold-start page): {res.text[:200]}")
        except Exception as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            
        print("Waiting 10 seconds for server to spin up...")
        time.sleep(10)
        
    raise RuntimeError("Failed to reach live Render API after multiple retries.")

if __name__ == "__main__":
    run_ai_lead_agent()