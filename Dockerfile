# Build context must be the parent vman3/ directory so COPY can reach vman_ml/.
# docker build -f backend/Dockerfile ..
# Or update docker-compose build.context to ".." and dockerfile to "backend/Dockerfile".

FROM --platform=linux/amd64 python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Thread limits — prevent SHAP/sklearn/OpenBLAS threading crashes in containers.
# These are safe for all sklearn/numpy workloads; parallelism via Celery workers
# instead.
ENV OMP_NUM_THREADS=1
ENV OPENBLAS_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1

WORKDIR /vman3

# ── Python dependencies ───────────────────────────────────────────────────────
COPY backend/requirements.txt backend/setup.py ./

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        build-essential \
        libpq-dev \
        gettext \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install -e . \
    && apt-get remove -y gcc libffi-dev build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── vman_ml package ───────────────────────────────────────────────────────────
# Copy the sibling vman_ml repo so the service can import it via sys.path.
# The fallback in vman_ml_service.py resolves Path("/vman3/vman_ml").
COPY vman_ml ./vman_ml/

# ── ML model artifact ─────────────────────────────────────────────────────────
# The model is checked in under backend/app/ccva/ml_models/.
COPY backend/app ./app/

# ── Pre-cache sentence-transformer model ──────────────────────────────────────
# Bakes the ~500 MB multilingual MiniLM model into the image so the first
# prediction does not require outbound internet access at runtime.
RUN python -c "\
import sys; sys.path.insert(0, '/vman3/vman_ml'); \
from sentence_transformers import SentenceTransformer; \
SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"

# ── Application source ────────────────────────────────────────────────────────
COPY backend/ .

EXPOSE 8080

# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]
