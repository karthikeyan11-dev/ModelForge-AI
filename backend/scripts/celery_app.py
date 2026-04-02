import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Upstash Redis Configuration (External Cloud)
# Upstash typically uses rediss:// for secure connections
REDIS_URL = os.getenv("CELERY_BROKER_URL") or os.getenv("REDIS_URL")

if not REDIS_URL:
    print("⚠️ WARNING: REDIS_URL/CELERY_BROKER_URL not found in .env. Celery may not connect.")

# Initialize Celery
# The include parameter points to the module where tasks are defined
celery_app = Celery(
    "ml_tasks",
    broker=REDIS_URL,
    backend=REDIS_URL,
    include=["scripts.tasks"]
)

# Celery Performance & Configuration Tuning
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    # Upstash/Cloud Redis specific optimizations
    broker_connection_retry_on_startup=True,
    redis_backend_health_check_interval=30,
    # Ensure tasks don't hang indefinitely
    task_time_limit=3600,  # 1 hour max
    task_soft_time_limit=3000,
    
    # Parallelism & Task Distribution Optimizations (Multi-tenant requirement)
    worker_prefetch_multiplier=1, # One task at a time per worker (prevents starvation)
    task_acks_late=True,          # Acknowledge only after completion
    worker_send_task_events=True, # For monitoring
    
    # Visibility timeout for long-running ML tasks
    broker_transport_options={
        "visibility_timeout": 43200,  # 12 hours
    }
)

if __name__ == "__main__":
    celery_app.start()
