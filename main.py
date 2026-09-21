from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from database import init_db
from routers import b2b_leads, career_swarm

# Initialize database tables and extensions on startup
try:
    init_db()
    print("✅ Database initialized successfully.")
except Exception as e:
    print(f"⚠️ Database initialization notice: {e}")

app = FastAPI(
    title="QuantCode Nexus Enterprise Apex | Dual-Persona Command Center",
    version="2.5.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include partitioned domain routers
app.include_router(b2b_leads.router)
app.include_router(career_swarm.router)

@app.get("/")
def root_health_check():
    return {"status": "online", "system": "QuantCode Nexus Enterprise Apex Modular Core"}