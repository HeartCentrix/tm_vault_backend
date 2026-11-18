FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535
WORKDIR /app
COPY shared/ /app/shared/
COPY api-gateway/requirements.lock .
RUN pip install --default-timeout=300 --retries 5 --no-cache-dir -r requirements.lock
COPY api-gateway/ .
ENV PORT=8000
EXPOSE ${PORT}
RUN groupadd --system --gid 10001 appuser \
    && useradd --system --uid 10001 --gid appuser --create-home --home-dir /home/appuser appuser \
    && chown -R 10001:10001 /app
USER 10001
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 CMD python -c "import httpx; httpx.get('http://localhost:${PORT:-8000}/health')"
CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 2"]
