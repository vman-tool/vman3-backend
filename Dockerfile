# Use an official Python runtime as a parent image with the amd64 platform
FROM --platform=linux/amd64 python:3.10-slim-buster

# Set environment variables for production
ENV PYTHONDONTWRITEBYTECODE=1  
ENV PYTHONUNBUFFERED=1  

# Install dependencies and system packages for production
COPY requirements.txt .

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    gettext \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get remove -y build-essential  # Remove unnecessary build tools to reduce image size

# Set the working directory in the container
WORKDIR /vman3

# Copy the application code
COPY . /vman3

# Expose the necessary port for the application
EXPOSE 8080

# Command to run the FastAPI app with gunicorn using uvicorn workers for production
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]