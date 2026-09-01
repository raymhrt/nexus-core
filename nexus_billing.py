import uuid
from datetime import datetime, timezone
from fastapi import FastAPI
from pydantic import BaseModel
from nexus_db import init_db, get_db
from nexus_notifier import send_alert

app = FastAPI(title="QuantCode Nexus Billing & Webhook Gateway")

@app.on_event("startup")
def startup_event():
    init_db()

class CheckoutRequest(BaseModel):
    repo_url: str

@app.post("/create-checkout-session")
def create_checkout_session(data: CheckoutRequest):
    tx_id = f"TX_{uuid.uuid4().hex[:8].upper()}"
    amount = 50.00
    timestamp = datetime.now(timezone.utc).isoformat()

    with get_db() as conn:
        conn.execute(
            "INSERT INTO transactions (id, agent_id, vertical, amount, timestamp) VALUES (?, ?, ?, ?, ?)",
            (tx_id, "Agent_Billing_01", "API-Billing", amount, timestamp)
        )

    alert_text = f"💳 *New Revenue Secured!*\n\n• *TxID:* `{tx_id}`\n• *Target:* `{data.repo_url}`\n• *Amount:* `${amount:.2f} USD`"
    send_alert("QuantCode Nexus Billing", alert_text)

    return {
        "status": "success",
        "tx_id": tx_id,
        "checkout_url": f"https://checkout.quantcode.nexus/pay/{tx_id}",
        "amount": amount
    }