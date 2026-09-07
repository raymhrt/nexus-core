import os
import json
import requests
from google import genai
from pydantic import BaseModel, Field, ValidationError
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
ADMIN_SECRET_KEY = os.getenv("ADMIN_SECRET_KEY")
RENDER_API_URL = os.getenv("RENDER_API_URL", "https://nexus-core-yfou.onrender.com/api/v1/admin/upload-leads")
RENDER_DLQ_URL = os.getenv("RENDER_DLQ_URL", "https://nexus-core-yfou.onrender.com/api/v1/admin/ai-dlq")
RENDER_FEEDBACK_API = os.getenv("RENDER_FEEDBACK_API", "https://nexus-core-yfou.onrender.com/api/v1/admin/converted-anchors") # Optional endpoint or direct DB if running locally

ai_client = genai.Client(api_key=GEMINI_API_KEY) if GEMINI_API_KEY else None

FALLBACK_MODELS = [
    "gemini-3.7-flash",
    "gemini-3.5-flash",
    "gemini-3.5-flash-lite",
    "gemini-2.5-flash"
]

class IncomingLead(BaseModel):
    company_name: str
    domain: str
    email: str
    industry: Optional[str] = "SaaS / Tech"
    employee_count: Optional[str] = "10-50"
    linkedin_url: Optional[str] = ""
    confidence_score: float = Field(default=0.9, ge=0.0, le=1.0)
    trust_score: int = Field(default=95, ge=0, le=100)

def send_to_dlq(raw_text: str, error_msg: str):
    try:
        headers = {"admin-key": ADMIN_SECRET_KEY or "", "Content-Type": "application/json"}
        requests.post(RENDER_DLQ_URL, json={"raw_payload": raw_text, "error_message": error_msg}, headers=headers, timeout=10)
    except Exception as e:
        print(f"Failed to post to AI DLQ: {e}")

def run_ai_lead_agent():
    if not ai_client:
        print("Error: GEMINI_API_KEY is not configured.")
        return

    # Prompt with vector centroid alignment hook
    prompt = (
        "Generate a JSON list of 3 real, active B2B technology, SaaS, or AI companies. "
        "For each company, provide verified data points: "
        "company_name, domain (e.g. 'datadog.com'), contact email format (e.g. contact@domain.com), industry, "
        "employee_count (e.g. '51-200'), linkedin_url, confidence_score (float between 0.0 and 1.0), and trust_score (integer between 0 and 100). "
        "Return strictly valid JSON matching this schema: "
        '[{"company_name": "...", "domain": "...", "email": "...", "industry": "...", "employee_count": "...", "linkedin_url": "...", "confidence_score": 0.95, "trust_score": 95}]'
    )

    response = None
    success_model = None

    for model_name in FALLBACK_MODELS:
        print(f"Attempting generation with {model_name}...")
        try:
            response = ai_client.models.generate_content(
                model=model_name,
                contents=prompt,
            )
            success_model = model_name
            print(f"Success with {model_name}.")
            break
        except Exception as e:
            error_str = str(e)
            print(f"Model {model_name} failed: {error_str}")
            if any(code in error_str for code in ["429", "404", "503", "RESOURCE_EXHAUSTED", "UNAVAILABLE", "Server error", "NOT_FOUND"]):
                continue
            else:
                raise e

    if not response:
        print("Error: All fallback models exhausted.")
        return

    raw_text = response.text.strip()
    if raw_text.startswith("```json"):
        raw_text = raw_text[7:-3].strip()
    elif raw_text.startswith("```"):
        raw_text = raw_text[3:-3].strip()

    try:
        parsed_data = json.loads(raw_text)
        validated_leads = [IncomingLead(**item).model_dump() for item in parsed_data]
    except (json.JSONDecodeError, ValidationError) as err:
        error_str = str(err)
        print(f"AI Output Validation Error: {error_str}")
        send_to_dlq(raw_text, error_str)
        return

    payload = {"leads": validated_leads}
    headers = {
        "Content-Type": "application/json",
        "admin-key": ADMIN_SECRET_KEY if ADMIN_SECRET_KEY else ""
    }

    print(f"Pushing {len(validated_leads)} validated leads via {success_model} to Render API...")
    try:
        res = requests.post(RENDER_API_URL, json=payload, headers=headers, timeout=15)
        print(f"Response Status: {res.status_code}")
        print(f"Response Body: {res.text}")
    except Exception as err:
        print(f"Failed to transmit leads: {err}")

if __name__ == "__main__":
    run_ai_lead_agent()