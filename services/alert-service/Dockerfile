FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/alert-service/requirements.lock .
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r requirements.lock
COPY services/alert-service/ .
ENV PORT=8007
EXPOSE ${PORT}
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid appuser --create-home --home-dir /home/appuser appuser \
    && chown -R 10001:10001 /app
USER 10001
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:${PORT:-8007}/health')"
# One shared sample dir per container so the worker that binds the
# core_metrics port (9103) serves every uvicorn worker (shared/core_metrics.py).
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
CMD ["sh", "-c", "rm -rf \"$PROMETHEUS_MULTIPROC_DIR\" && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8007} --workers 2"]
