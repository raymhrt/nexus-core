import time
from ingest_leads import ingest_github_leads

if __name__ == "__main__":
  print("Starting QuantCode Nexus Automated Ingestion Service...")
  while True:
    try:
      ingest_github_leads("fastapi stars:>100")
      print("Waiting 1 hour for next ingestion cycle...")
    except Exception as e:
      print(f"Loop error: {e}")
    time.sleep(3600)  # Sleep for 1 hour