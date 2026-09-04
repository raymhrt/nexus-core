from fastapi.testclient import TestClient
from app import app, hash_api_key, get_db

client = TestClient(app)


def test_read_index():
    response = client.get("/")
    assert response.status_code == 200


def test_dashboard_route():
    response = client.get("/dashboard")
    assert response.status_code == 200


def test_leads_unauthorized():
    response = client.get("/api/v1/leads", headers={"x-api-key": "invalid_key"})
    assert response.status_code == 403


def test_admin_ingest_unauthorized():
    response = client.post("/api/v1/admin/ingest-lead", params={"company_name": "Test", "email": "test@test.com"})
    assert response.status_code == 422  # missing header