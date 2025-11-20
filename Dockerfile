# Lightweight container for running the FDA recalls pipeline with Prefect
FROM python:3.12-slim

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /app

# Copy only requirements first for caching
COPY requirements.txt ./

RUN python -m pip install --upgrade pip setuptools wheel \
    && pip install --no-cache-dir -r requirements.txt

# Copy project
COPY . /app

# Ensure scripts are executable
RUN chmod +x /app/scripts/*.py || true

# Default command: run the Prefect-backed pipeline with a small default
CMD ["python", "scripts/run_pipeline.py", "--max-records", "1000"]
