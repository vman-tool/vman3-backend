FROM --platform=linux/amd64 python:3.10-slim-buster

COPY requirements.txt .

RUN apt-get update \
    && apt-get install -y --no-install-recommends gettext  \
    build-essential curl nano git \
    libpq-dev libcups2-dev pkg-config libdbus-1-dev python3-dev\
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && pip install --no-cache-dir -r requirements.txt

WORKDIR /vman3

COPY . /vman3

EXPOSE 8080

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]