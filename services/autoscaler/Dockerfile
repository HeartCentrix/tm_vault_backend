FROM python:3.11-slim

WORKDIR /app

COPY shared /app/shared
COPY services/autoscaler /app/services/autoscaler

RUN pip install --default-timeout=300 --retries 5 --no-cache-dir --prefer-binary -r /app/services/autoscaler/requirements.txt

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

CMD ["python3", "/app/services/autoscaler/main.py"]
