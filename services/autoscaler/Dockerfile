FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app

COPY shared /app/shared
COPY services/autoscaler /app/services/autoscaler

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir --prefer-binary -r /app/services/autoscaler/requirements.lock

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/services/autoscaler/main.py"]
