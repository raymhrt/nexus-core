import os
import pytest
from fastapi.testclient import TestClient

os.environ["ADMIN_SECRET_KEY"] = "test-secret"

from app import app, ADMIN_SECRET_KEY

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_unauthorized_lead_upload():
    response = client.post("/api/v1/admin/upload-leads", json={"leads": []}, headers={"admin-key": "wrong-key"})
    assert response.status_code == 403

def test_valid_lead_upload_and_idempotency():
    lead_data = {
        "leads": [
            {
                "company_name": "TestCorp",
                "domain": "testcorp.com",
                "email": "contact@testcorp.com",
                "industry": "SaaS",
                "employee_count": "10-50",
                "linkedin_url": "https://linkedin.com/company/testcorp"
            }
        ]
    }
    headers = {"admin-key": ADMIN_SECRET_KEY}
    
    # First upload
    res1 = client.post("/api/v1/admin/upload-leads", json=lead_data, headers=headers)
    assert res1.status_code == 200
    assert res1.json()["imported_count"] == 1

    # Second upload (Idempotency test via unique domain constraint)
    res2 = client.post("/api/v1/admin/upload-leads", json=lead_data, headers=headers)
    assert res2.status_code == 200
    assert res2.json()["imported_count"] == 0  # Ignored due to unique domain constraint