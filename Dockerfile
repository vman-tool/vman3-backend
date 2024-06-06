FROM python:3.9

WORKDIR /app

COPY requirements.txt .
COPY settings.json .


RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .
COPY .env .

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080", "--reload"]