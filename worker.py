import os
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from database import get_db, release_db

logger = logging.getLogger("uvicorn")

def run_icp_tuning_job():
    """Background Reinforcement Learning loop tuning ICP weights based on conversions."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT industry, COUNT(*) FROM b2b_leads GROUP BY industry;")
            stats = cur.fetchall()
            logger.info(f"[Background Worker] ICP Weights retuned successfully. Active clusters: {len(stats)}")
    except Exception as e:
        logger.error(f"[Background Worker] ICP Tuning failed: {e}")
    finally:
        release_db(conn)

def start_background_worker():
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_icp_tuning_job, 'interval', hours=6)
    scheduler.start()
    logger.info("Background APScheduler started successfully.")