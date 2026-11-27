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
# requirements.txt itself installs vman_ml/vman_dq from GitHub release
# tarballs rather than `git+https://...` - but vman_ml's own pyproject.toml
# still declares ITS vman_dq dependency as `git+https://...`, so pip still
# needs git to resolve that transitively when installing vman_ml. Removing
# git from this image (as a prior pass here did) breaks that resolution with
# "ERROR: Cannot find command 'git'", it just didn't show up until a build
# got far enough to reach it instead of dying earlier on a flaky connection.
COPY requirements.txt setup.py ./

# Each step below is its own RUN/layer rather than one combined chain, so a
# transient network failure (flaky connections drop apt/pip downloads often
# enough to matter here) only costs a retry of that one step on a rebuild,
# instead of re-downloading everything - apt packages, torch, and the rest -
# from scratch every time. apt gets a built-in retry count for the same
# reason; the requirements.txt install additionally loops itself and, unlike
# a bare `for` loop, fails the build (via the final `test`) if every attempt
# is exhausted - a bare loop exits 0 regardless, which silently produced an
# image with requirements.txt never actually installed (no celery, no
# vman_ml, nothing) the first time this was tried.
RUN apt-get update -o Acquire::Retries=5 \
    && apt-get install -y -o Acquire::Retries=5 --no-install-recommends \
        gcc \
        git \
        libffi-dev \
        build-essential \
        libpq-dev \
        gettext

RUN pip install --upgrade pip

RUN pip install --no-cache-dir --retries 10 --timeout 100 \
        torch --index-url https://download.pytorch.org/whl/cpu

RUN success=0; \
    for i in 1 2 3 4 5; do \
        if pip install --no-cache-dir --retries 10 --timeout 100 -r requirements.txt; then \
            success=1; break; \
        fi; \
        echo "requirements.txt install failed (attempt $i/5), retrying..."; \
        sleep 5; \
    done; \
    test "$success" = 1

RUN pip install -e .

# xgboost (pulled in transitively via vman_ml) depends on nvidia-nccl-cu12 for
# multi-GPU training even on this CPU-only deployment (torch above is already
# the CPU build) - nothing in this codebase runs on a GPU, and stripping it
# saves ~470MB per image with no functional loss.
RUN pip uninstall -y nvidia-nccl-cu12 \
    && apt-get remove -y gcc git libffi-dev build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Application source ────────────────────────────────────────────────────────
COPY app ./app/

EXPOSE 8080
