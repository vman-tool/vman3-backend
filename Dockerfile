# FROM python:3.9

# WORKDIR /app

# COPY requirements.txt .
# COPY settings.json .


# RUN pip install --no-cache-dir -r requirements.txt

# COPY app/ .
# # COPY .env .

# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]


#FROM python:3.10-slim-buster
FROM --platform=linux/amd64 python:3.10-slim-buster

COPY requirements.txt .

RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext  \
    build-essential curl nano git \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && pip install -r requirements.txt

WORKDIR /vman3

COPY . /vman3

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]