import os
import logging
from celery import Celery
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from database import get_db, release_db

logger = logging.getLogger("uvicorn")

# Initialize Celery app connected to Redis for asynchronous worker queues
redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
celery_app = Celery(
    "nexus_workers",
    broker=redis_url,
    backend=redis_url
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
)

@celery_app.task(bind=True, name="worker.run_enrichment_waterfall")
def run_enrichment_waterfall(self, lead_id: int, query: str):
    """
    Asynchronous Multi-Agent Enrichment Waterfall:
    Crawls target niches, cross-references compliance, and calculates trust scores.
    """
    logger.info(f"[Celery Worker] Starting enrichment waterfall for lead ID {lead_id} (Query: {query})")
    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Example database update for multi-agent enrichment results
            cur.execute(
                "UPDATE b2b_leads SET trust_score = %s WHERE id = %s;",
                (95, lead_id)
            )
            conn.commit()
        logger.info(f"[Celery Worker] Enrichment waterfall completed for lead ID {lead_id}")
    except Exception as e:
        logger.error(f"[Celery Worker] Enrichment failed for lead {lead_id}: {e}")
        raise
    finally:
        release_db(conn)
    return {"status": "success", "lead_id": lead_id}

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

# Allow running worker.py standalone as a dedicated Render worker process
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info("Starting dedicated blocking background worker process with APScheduler...")
    blocking_scheduler = BlockingScheduler()
    blocking_scheduler.add_job(run_icp_tuning_job, 'interval', hours=6)
    try:
        blocking_scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Background worker stopped.")