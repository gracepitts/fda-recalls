# Run the FDA Recalls pipeline in Docker

This guide runs the pipeline in an isolated container so Prefect/Pydantic version conflicts in your host environment won't block execution.

Prerequisites
- Docker (or Docker Desktop) installed and running
- docker-compose (optional, included in Docker Desktop)

Build and run using docker-compose (recommended)

```bash
# Build image and run the pipeline service; uses MAX_RECORDS env var
docker-compose up --build
```

To run with a different ingest size:

```bash
MAX_RECORDS=50000 docker-compose up --build
```

Run just the container once (no compose):

```bash
# Build image
docker build -t fda-recalls:latest .

# Run (mount current dir so you keep plots/data/logs locally)
docker run --rm -v "${PWD}:/app" -v "${PWD}/data:/app/data" -v "${PWD}/logs:/app/logs" -v "${PWD}/plots:/app/plots" fda-recalls:latest python scripts/run_pipeline.py --max-records 1000
```

Notes
- DuckDB is file-backed under `data/fda_recalls.duckdb` and will be persisted on your host via the volume mount.
- If you don't want to use Prefect orchestration inside the container you can pass `--no-prefect` to `scripts/run_pipeline.py`.
