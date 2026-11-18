FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app

# Copy shared library first
COPY shared /app/shared

# Copy service code
COPY services/delta-token /app/services/delta-token

# Install dependencies
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r /app/services/delta-token/requirements.lock

ENV PYTHONPATH=/app
ENV PORT=8010

EXPOSE ${PORT}

CMD ["sh", "-c", "python -m uvicorn services.delta-token.main:app --host 0.0.0.0 --port ${PORT:-8010}"]
