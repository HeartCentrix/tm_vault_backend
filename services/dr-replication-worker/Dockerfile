FROM python:3.11.18-trixie@sha256:55c475496e35ad8d898659c85f92b42d3b1c58f25f5d79023baf0840b0b4a462

WORKDIR /app

# Install build dependencies for compiling C extensions
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libffi-dev \
        libssl-dev \
        libpq-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY shared /app/shared
COPY services/dr-replication-worker /app/services/dr-replication-worker

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir --prefer-binary -r /app/services/dr-replication-worker/requirements.lock

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python", "services/dr-replication-worker/main.py"]
