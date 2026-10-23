FROM python:3.12.11-slim-bookworm
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PIP_NO_CACHE_DIR=1
WORKDIR /app
RUN groupadd --gid 10001 batchforge && useradd --uid 10001 --gid batchforge --no-create-home batchforge
COPY pyproject.toml requirements.txt ./
COPY backend ./backend
COPY worker ./worker
COPY frontend ./frontend
RUN pip install --require-hashes -r requirements.txt && pip install --no-deps . && chown -R batchforge:batchforge /app
USER 10001:10001
EXPOSE 8000
CMD ["uvicorn", "backend.app:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
