# 🐳 Docker Deployment Guide

This guide provides instructions on how to set up and run the **AI Data Cleaning + AutoML Platform** using Docker and Docker Compose.

## 📋 Prerequisites

- [Docker](https://docs.docker.com/get-docker/) installed on your system.
- [Docker Compose](https://docs.docker.com/compose/install/) (included with Docker Desktop).
- Euri API Keys (from euron.one).

## 🚀 Quick Start

### 1. Configure Environment Variables
Create or update your `.env` file in the root directory. You can copy existing keys, but ensure the server hosts are set to `0.0.0.0` for Docker bindings:

```env
# API Keys (Required)
DB_EURI_API_KEY=your-key-here
UPLOAD_EURI_API_KEY=your-key-here
API_EURI_API_KEY=your-key-here
FALLBACK_EURI_API_KEY=your-key-here

# Database Configuration (Internal Docker Network)
DB_USER=postgres
DB_PASSWORD=admin
DB_NAME=demodb
DB_HOST=db
DB_PORT=5432

# Application Configuration
FASTAPI_HOST=0.0.0.0
FASTAPI_PORT=8000
FLASK_HOST=0.0.0.0
FLASK_PORT=5000
FLASK_DEBUG=False

# Upstash Redis (Optional - if not using local Redis)
UPSTASH_REDIS_REST_URL=your-upstash-url
UPSTASH_REDIS_REST_TOKEN=your-upstash-token
```

### 2. Build and Run
Run the following command to build the images and start all services:

```bash
docker-compose up --build
```

To run in the background (detached mode):

```bash
docker-compose up -d
```

### 3. Access the Platform
Once the containers are running, you can access the services:

- **Frontend (Flask UI):** [http://localhost:5000](http://localhost:5000)
- **Backend (FastAPI Docs):** [http://localhost:8000/docs](http://localhost:8000/docs)
- **PostgreSQL:** Access at `localhost:5432` from your host machine.

---

## 🏗️ Docker Services

| Service | Container Name | Port | Description |
|---------|----------------|------|-------------|
| **Backend** | `data-cleaning-backend` | 8000 | FastAPI server performing data cleaning and ML training. |
| **Worker** | `data-cleaning-worker` | - | Celery worker for background task processing. |
| **MLflow** | `data-cleaning-mlflow` | 5005 | Experiment tracking and model management server. |
| **Db** | `data-cleaning-db` | 5432 | PostgreSQL 15 database for persistent data storage. |
| **Redis** | `data-cleaning-redis` | 6379 | Local Redis instance for task broker and caching. |

## 💾 Persistence & Volumes

Data is persisted across container restarts using Docker volumes:

- `postgres-data`: Stores the actual database records.
- `./uploads`: Mounted to ensure uploaded files are shared between host and containers.
- `./models`: Stores trained ML models (`.joblib` or `.pkl` files).
- `./datasets`: Stores cleaned datasets for the AutoML pipeline.

## 🛠️ Common Operations

### View Logs
```bash
docker-compose logs -f
```

### Stop All Services
```bash
docker-compose down
```

### Clean Up (Remove volumes)
```bash
docker-compose down -v
```

### Access Backend Directly from Host
If you want to run a script on your host machine to clean data from the Docker backend, use `http://localhost:8000`.

---
> [!TIP]
> **Production Note:** For production deployments, change `DB_PASSWORD` and `FLASK_SECRET_KEY` in your `.env` file and ensure `FLASK_DEBUG` is set to `False`.
