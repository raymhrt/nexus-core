from fastapi import APIRouter, HTTPException
from database import get_db, release_db
import json

router = APIRouter(prefix="/api/v1", tags=["B2B Leads Engine"])

@router.get("/leads")
def get_leads(min_trust: float = None, industry: str = None, limit: int = 20):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            query = "SELECT id, company_name, domain, industry, employee_count, tech_stack, funding_stage, trust_score, verified_email, intent_signals, created_at FROM b2b_leads WHERE 1=1"
            params = []
            
            if min_trust is not None:
                query += " AND trust_score >= %s"
                params.append(min_trust)
            if industry:
                query += " AND industry ILIKE %s"
                params.append(f"%{industry}%")
                
            query += " ORDER BY id DESC LIMIT %s"
            params.append(limit)
            
            cur.execute(query, tuple(params))
            rows = cur.fetchall()
            
            leads = []
            for row in rows:
                leads.append({
                    "id": row[0],
                    "company_name": row[1],
                    "domain": row[2],
                    "industry": row[3],
                    "employee_count": row[4],
                    "tech_stack": row[5],
                    "funding_stage": row[6],
                    "trust_score": row[7],
                    "verified_email": row[8],
                    "intent_signals": row[9],
                    "created_at": str(row[10])
                })
                
            return {"status": "success", "tier": "enterprise", "leads": leads}
    finally:
        release_db(conn)

@router.delete("/leads/{lead_id}")
def delete_lead(lead_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM b2b_leads WHERE id = %s RETURNING id;", (lead_id,))
            deleted = cur.fetchone()
            conn.commit()
            if not deleted:
                raise HTTPException(status_code=404, detail="Lead not found")
            return {"status": "success", "message": f"Lead #{lead_id} deleted successfully."}
    finally:
        release_db(conn)