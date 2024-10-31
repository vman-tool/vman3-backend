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

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]