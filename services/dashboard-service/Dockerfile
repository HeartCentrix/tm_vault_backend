FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends gcc libpq-dev && rm -rf /var/lib/apt/lists/*
COPY shared/ /app/shared/
COPY services/dashboard-service/requirements.txt .
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r requirements.txt
COPY services/dashboard-service/ .
ENV PORT=8006
EXPOSE ${PORT}
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid appuser --create-home --home-dir /home/appuser appuser \
    && chown -R 10001:10001 /app
USER 10001
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:${PORT:-8006}/health')"
# One shared sample dir per container so the worker that binds the
# core_metrics port (9103) serves every uvicorn worker (shared/core_metrics.py).
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
CMD ["sh", "-c", "rm -rf \"$PROMETHEUS_MULTIPROC_DIR\" && uvicorn main:app --host 0.0.0.0 --port ${PORT:-8006} --workers 2"]
