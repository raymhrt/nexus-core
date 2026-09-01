import time
import schedule
import subprocess
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

def job_run_nexus():
    logger.info("⏰ Scheduled Task Triggered: Running QuantCode Nexus Pipeline...")
    try:
        subprocess.run(["python", "quantcode_nexus.py"], check=True)
    except subprocess.CalledProcessError as e:
        logger.error(f"Pipeline execution failed: {e}")

# Schedule the multi-agent pipeline to run every 15 minutes
schedule.every(15).minutes.do(job_run_nexus)

logger.info("🕒 QuantCode Nexus Background Scheduler Initialized. Running every 15 minutes...")

if __name__ == "__main__":
    while True:
        schedule.run_pending()
        time.sleep(1)