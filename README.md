# Vman3 API

This is the API for the Vman3 project. It is built using FastAPI and MongoDB.

## Prerequisites

- Python 3.9+
- Docker
- Docker Compose
- Git/ Git Flow

## Setup Instructions

### 1. Create and Activate a Virtual Environment

Create a virtual environment to manage your project dependencies.

```bash
# Create a virtual environment
python3 -m venv venv

# Activate the virtual environment (Linux/Mac)
source venv/bin/activate

# Activate the virtual environment (Windows)
venv\Scripts\activate
```

### 2. Run the following command to install the required dependencies:
```bash
pip3 install -r requirements.txt
# update .txt from venv
pip freeze > requirements.txt
```


### 3. Configure Environment Variables

Create a `.env` file in the root directory of the project and add the following environment variables:

```bash

MONGODB_URL=mongodb://localhost:27017
MONGODB_DB=vman3

DEFAULT_PROJECT_ID=1
ODK_API_URL=""
ODK_API_VERSION=v1
ODK_USERNAME=""
ODK_PASSWORD=""

```

### 4. add settings.json
# command copy the content of settings.json.example to settings.json

```bash

cp settings.json.example settings.json
    
```

### 5. Running the Application

Run the following command to start the FastAPI application:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

The application will be accessible at `http://localhost:8080/vman/api/v1`.







### 6. Running the Application with Docker Compose






After ensuring the above configurations, build and run the Docker containers:

```bash
docker-compose up --build
```