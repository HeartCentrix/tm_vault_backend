FROM python:3.11.18-slim-trixie@sha256:07d4824036af339fa1b6ef769cc99d9c10a141769538b52b3b597c84f7a7b535

WORKDIR /app
COPY requirements.base.lock /app/requirements.base.lock
RUN pip install --no-cache-dir -r requirements.base.lock

COPY . /app
ENV PYTHONPATH=/app

CMD ["python", "-m", "services.storage_toggle_worker.main"]
