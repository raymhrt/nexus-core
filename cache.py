import os
import redis
from fastapi import HTTPException, Request

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def check_rate_limit(api_key_hash: str, tier: str = "starter"):
    """Enforces sliding-window rate limits tied to Stripe subscription tiers."""
    limit = 120 if tier == "pro" else 30
    window = 60  # 1 minute window
    
    key = f"ratelimit:{api_key_hash}"
    current = redis_client.get(key)
    
    if current is None:
        redis_client.setex(key, window, 1)
        remaining = limit - 1
    else:
        current = int(current)
        if current >= limit:
            raise HTTPException(
                status_code=429, 
                detail=f"Rate limit exceeded. Tier '{tier}' allows {limit} requests per minute."
            )
        redis_client.incr(key)
        remaining = limit - current - 1

    return limit, remaining

def check_idempotency(key: str) -> bool:
    """Checks if an idempotency key has already been processed."""
    if not key:
        return False
    cache_key = f"idempotency:{key}"
    if redis_client.get(cache_key):
        return True
    redis_client.setex(cache_key, 3600, "locked")  # Lock for 1 hour
    return False