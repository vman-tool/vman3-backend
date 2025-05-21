FROM --platform=linux/amd64 python:3.10-slim-buster

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /vman3

COPY requirements.txt setup.py ./

COPY app ./app/

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
    && apt-get remove -y \
    gcc \
    libffi-dev \
    build-essential \
    && apt-get autoremove -y \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY . .

EXPOSE 8080

# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]


# FROM --platform=linux/amd64 python:3.10-slim-buster

# ENV PYTHONDONTWRITEBYTECODE=1
# ENV PYTHONUNBUFFERED=1

# WORKDIR /vman3

# # Create ccva_files directory with proper permissions
# RUN mkdir -p /app/ccva_files && chmod -R 777 /app/ccva_files

# COPY requirements.txt setup.py ./

# COPY app ./app/

# RUN apt-get update \
#     && apt-get install -y --no-install-recommends \
#     gcc \
#     libffi-dev \
#     build-essential \
#     libpq-dev \
#     gettext \
#     # Add these packages for file operations and debugging
#     procps \
#     vim \
#     && pip install --upgrade pip \
#     && pip install --no-cache-dir -r requirements.txt \
#     && pip install -e . \
#     && apt-get remove -y \
#     gcc \
#     libffi-dev \
#     build-essential \
#     && apt-get autoremove -y \
#     && apt-get clean \
#     && rm -rf /var/lib/apt/lists/*

# # Create symbolic link to make /app/ccva_files available in /vman3
# # RUN ln -s /app/ccva_files /vman3/ccva_files

# COPY . .

# # Health check to verify filesystem access
# HEALTHCHECK --interval=30s --timeout=3s \
#   CMD python -c "import os; os.makedirs('/app/ccva_files/test', exist_ok=True); open('/app/ccva_files/test/test.txt', 'w').write('test'); os.remove('/app/ccva_files/test/test.txt')" || exit 1

# EXPOSE 8080

# CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]