FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app

COPY requirements.base.lock .
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r requirements.base.lock

COPY shared/ ./shared/
COPY services/report-service/ ./services/report-service/

EXPOSE 8014

CMD ["sh", "-c", "uvicorn services.report-service.main:app --host 0.0.0.0 --port 8014"]
