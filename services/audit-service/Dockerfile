FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app

COPY shared /app/shared
COPY services/audit-service /app/services/audit-service

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/services/audit-service/requirements.lock

ENV PYTHONPATH=/app
ENV PORT=8012

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.audit-service.main:app --host 0.0.0.0 --port ${PORT:-8012}"]
