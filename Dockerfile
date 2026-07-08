FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Limit native threading — prevents sklearn/numpy/SHAP threading crashes in containers.
ENV OMP_NUM_THREADS=1
ENV OPENBLAS_NUM_THREADS=1
ENV MKL_NUM_THREADS=1
ENV NUMEXPR_NUM_THREADS=1

WORKDIR /vman3

# ── System deps + Python dependencies ────────────────────────────────────────
# git is required for pip to install vman_ml directly from GitHub.
COPY requirements.txt setup.py ./

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        git \
        libffi-dev \
        build-essential \
        libpq-dev \
        gettext \
    && pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install -e . \
    && apt-get remove -y gcc git libffi-dev build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Pre-cache sentence-transformer model ──────────────────────────────────────
# Bakes the ~500 MB multilingual MiniLM model into the image so the first
# prediction does not require outbound internet access at runtime.
RUN python -c "from sentence_transformers import SentenceTransformer; SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')"

# ── Application source ────────────────────────────────────────────────────────
COPY app ./app/

EXPOSE 8080
