from fastapi import APIRouter, HTTPException
from database import get_db, release_db
from pydantic import BaseModel

router = APIRouter(prefix="/api/v1/career", tags=["Career Swarm"])

class CareerCriteriaRequest(BaseModel):
    target_roles: str
    locations: str
    job_quantity: int = 5

@router.get("/matches")
def get_career_matches():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, role_title, company_name, location, fit_score, rationale, 
                       cv_variant, networking_target_name, networking_target_role, 
                       networking_target_email, outreach_subject, outreach_draft, ats_portal_url, created_at 
                FROM career_matches ORDER BY id DESC;
            """)
            rows = cur.fetchall()
            
            matches = []
            for row in rows:
                matches.append({
                    "id": row[0],
                    "role_title": row[1],
                    "company_name": row[2],
                    "location": row[3],
                    "fit_score": row[4],
                    "rationale": row[5],
                    "cv_variant": row[6],
                    "networking_target_name": row[7],
                    "networking_target_role": row[8],
                    "networking_target_email": row[9],
                    "outreach_subject": row[10],
                    "outreach_draft": row[11],
                    "ats_portal_url": row[12],
                    "created_at": str(row[13])
                })
            return {"status": "success", "matches": matches}
    finally:
        release_db(conn)

@router.post("/criteria")
def save_career_criteria(payload: CareerCriteriaRequest):
    # Here you can trigger your swarm agent logic using payload.job_quantity
    return {
        "status": "success", 
        "message": f"Swarm launched successfully searching for {payload.job_quantity} jobs per batch across {payload.locations}!"
    }

@router.delete("/matches/{match_id}")
def delete_career_match(match_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM career_matches WHERE id = %s RETURNING id;", (match_id,))
            deleted = cur.fetchone()
            conn.commit()
            if not deleted:
                raise HTTPException(status_code=404, detail="Career match card not found")
            return {"status": "success", "message": f"Job match card #{match_id} dismissed successfully."}
    finally:
        release_db(conn)