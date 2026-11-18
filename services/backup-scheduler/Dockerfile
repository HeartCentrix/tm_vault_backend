FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app

# Copy shared library first
COPY shared /app/shared

# Copy service code + the services package marker so cross-service
# imports resolve (e.g. `from services.exports_cleanup import ...`
# used by the daily exports-cleanup cron — without these the cron
# silently fails every 24h with `No module named services.exports_cleanup`,
# and orphaned export ZIPs never get reaped).
COPY services/__init__.py /app/services/__init__.py
COPY services/exports_cleanup.py /app/services/exports_cleanup.py
COPY services/backup-scheduler /app/services/backup-scheduler

# Install dependencies
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/services/backup-scheduler/requirements.lock

ENV PYTHONPATH=/app
ENV PORT=8008

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.backup-scheduler.main:app --host 0.0.0.0 --port ${PORT:-8008}"]
