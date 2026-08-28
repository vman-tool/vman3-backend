# VMan3 API

This is the API for the Vman3 project. It is built using FastAPI and MongoDB.

## Prerequisites

- Python 3.9+
- Docker
- Docker Compose
- Git/ Git Flow

## Setup Instructions
```
Run 'git clone https://github.com/vman-tool/vman3' to copy the project local then,
Run 'cd vman3 ' to change to the project directory,

```
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

### 2.1 if you run in development environment, install vman_ml and vman_dq manually
```bash
.venv/bin/pip install -e ../vman_dq --no-deps
.venv/bin/pip install -e ../vman_ml --no-deps
```
These editable installs point the venv at your local working copies, so
changes to vman_dq/vman_ml take effect without reinstalling. Production and
Docker are unaffected — they install the git tags pinned in requirements.txt.

Install vman_dq first, and keep `--no-deps`: vman_ml declares
`vman_dq @ git+...@v1.2.1` plus sentence-transformers (which pulls torch),
so without the flag pip would re-resolve that whole stack and overwrite the
editable vman_dq you just installed.

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

### 4. Copy the sample environment file
# command copy the content of env_sample.example to .env

```
cp .env_sample .env
    
```

### 5. Running the arangodb database  with Docker Compose


After ensuring the above configurations, build and run the Docker containers:

```bash
docker compose up arangodb -d

```

### 6. Running the Application

Run the following command to start the FastAPI application:

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

**Note:** Make sure your virtual environment is activated. If `uvicorn` is not found, use:
```bash
python -m uvicorn app.main:app --host 0.0.0.0 --port 8080 --reload
```

The application will be accessible at `http://localhost:8080/vman/api/v1`.



### 7. Running the Application with Docker Compose

After ensuring the above configurations, build and run the Docker containers:

```bash
docker compose up --build
```

> **⚠️ Migration note — `arango-db` renamed to `arangodb` (July 2026)**
> If you previously had an `arango-db` container running, use `--remove-orphans` to
> clean up the old container before the renamed one starts:
> ```bash
> docker compose up -d --remove-orphans
> ```
> Data in the `vman3db` volume is preserved automatically.

## Running Tests

Unit tests use [pytest](https://docs.pytest.org/). Most tests run against a
lightweight fake ArangoDB (`tests/support/fakes.py`) instead of a real
database, so the whole suite runs in well under a second.

```bash
# Install test dependencies (installs requirements.txt too)
pip install -r requirements-dev.txt

# Run the full suite
pytest

# Run one file, or one test, verbosely
pytest tests/unit/ccva/test_interva_utils.py -v
pytest tests/unit/ccva/test_interva_utils.py::test_csmf_rejects_an_invalid_age_group -v

# With a coverage report
pytest --cov=app --cov-report=term-missing
```

Tests live under `tests/unit/`, mirroring `app/`'s package layout (e.g.
`app/ccva/services/...` is covered by `tests/unit/ccva/...`).
`tests/conftest.py` and `tests/support/fakes.py` hold shared fixtures — the
fake ArangoDB stand-ins (`FakeDB`/`FakeAQL`/`FakeCursor`) most tests build
on to check what AQL a function issued, or feed it canned results, without
touching a real database.

`test_performance.py` at the project root is a separate, manual diagnostic
script for checking live query performance against a real database — it's
not a unit test, and `pyproject.toml` scopes pytest's discovery to `tests/`
so a bare `pytest` run never picks it up.

The same suite runs in CI (`.github/workflows/main.yml`) on every push/PR
to `main`, before the Docker image is built.

## Development Guidelines

### 1. Asynchronous vs Synchronous Code
This project uses **FastAPI**, which is built on an asynchronous core. However, we use **ArangoDB** via `python-arango`, which is a synchronous library.

**CRITICAL RULE**: You must **never** call blocking synchronous code directly within an `async def` function. This will freeze the entire server event loop.

#### Correct Pattern (Using Threadpool)
Use `run_in_threadpool` to bridge synchronous calls:
```python
from fastapi.concurrency import run_in_threadpool

async def my_async_endpoint():
    # WRONG: Blocks server
    # cursor = db.aql.execute(query) 
    
    # CORRECT: Runs in separate thread
    def execute_query():
        return db.aql.execute(query)
        
    cursor = await run_in_threadpool(execute_query)
```

**When to use `run_in_threadpool`**:
-   Any Database Call (`db.aql.execute`, `collection.insert`, etc.)
-   File I/O
-   Heavy CPU computations

**When NOT to use it**:
-   Simple variable assignment
-   Should be native async libraries (like `httpx`)
