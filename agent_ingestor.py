import os
import requests
from openai import OpenAI

client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

API_URL = "https://nexus-core-yfou.onrender.com/api/v1/admin/upload-leads"
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")

def run_ai_lead_agent():
    # 1. Prompt your AI agent to discover or generate a verified batch of target B2B leads
    prompt = """
    Act as a B2B lead generation researcher. Generate 3 realistic, high-value tech/SaaS companies 
    that match a target buyer profile (B2B SaaS, FinTech, or AI Infrastructure). 
    Provide real corporate domain patterns, industry, employee counts, and LinkedIn URLs.
    Output strictly in valid JSON format matching this structure:
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
    
    # 2. Automatically push the AI-generated batch to your secure admin endpoint
    headers = {
        "Content-Type": "application/json",
        "admin_key": ADMIN_SECRET_KEY
    }
    
    res = requests.post(API_URL, data=lead_payload, headers=headers)
    print("Agent Ingestion Status:", res.status_code, res.json())

if __name__ == "__main__":
    run_ai_lead_agent()