import pytest
from fastapi.testclient import TestClient
from app import app

client = TestClient(app)

def test_health_check():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "healthy"

def test_unauthorized_leads_access():
    response = client.get("/api/v1/leads", headers={"x-api-key": "invalid_key"})
    assert response.status_code == 403

def test_terms_page():
    response = client.get("/terms")
    assert response.status_code == 200