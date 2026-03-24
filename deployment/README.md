# VMAN3 Deployment Guide

This directory contains the orchestration and configuration files for deploying the VMAN3 application suite (Frontend, Backend, Database, and Background Workers).

## 📋 Prerequisites
- **Docker** & **Docker Compose**
- **Git** (for code updates)
- **.env** file (use `.env_sample` as a template)

---

## 🚀 Deployment Modes

### 1. Development Mode (Recommended for Coding)
Enables **hot-reload** for both the Backend and Frontend. Changes made to the source code will reflect immediately.

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
```

### 2. Production Mode (Stable)
Uses pre-built images or optimized builds without hot-reload. Best for stability and performance.

```bash
docker compose -f docker-compose.yml up -d
```

---

## 🛠 Infrastructure Components

| Service | Image | Description |
| :--- | :--- | :--- |
| **nginx** | `nginx:alpine` | Reverse proxy handling traffic for all services. |
| **backend** | `backend` | FastAPI application serving the REST API. |
| **celery-worker** | `celery-worker` | Background task processor (CCVA, ODK Fetch). |
| **flower** | `mher/flower:master` | Monitoring UI for Celery tasks (at port `:5555`). |
| **arango-db** | `arangodb:3.11` | Primary NoSQL database. |
| **redis-vman3** | `redis:alpine` | Message broker for Celery and WebSocket pub/sub. |
| **frontend** | `frontend` | Angular application (accessible via Nginx). |

---

## ⚙️ Configuration (.env)

Ensure these variables are set correctly for your environment:

- `USE_CELERY=True`: Enables background processing for CCVA.
- `REDIS_URL`: `redis://redis-vman3:6379` (Internal Docker network).
- `ARANGODB_URL`: `http://arango-db:8529`.

---

## 🔦 Useful Commands

### Check Status
```bash
docker compose ps
```

### View Logs (Real-time)
```bash
docker compose logs -f backend         # Backend logs
docker compose logs -f celery-worker  # Worker logs 
```

### Restart Services (Force Recreate)
```bash
docker compose up -d --force-recreate
```

### Accessing Tools
- **Swagger UI**: `http://localhost:8080/vman/api/v1/docs`
- **ArangoDB UI**: `http://localhost:8529`
- **Flower UI**: `http://localhost:5555`

---
**Note**: For CCVA analysis, memory optimizations ("Fetch-in-Worker") are enabled by default through the `.env` and Celery worker settings.
