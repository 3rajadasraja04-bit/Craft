FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# App files
COPY . .

# Persistent data directory (mount volume here on Railway)
RUN mkdir -p /app/data

# Expose port (Railway provides $PORT)
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD curl -f http://localhost:${PORT:-8080}/api/health || exit 1

# Shell form so ${PORT} expands
CMD uvicorn web_app:app --host 0.0.0.0 --port ${PORT:-8080}