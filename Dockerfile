# ============================================================
# Dockerfile – Enterprise Fraud Detection FastAPI Service
# ============================================================
# Multi-stage build for a lean production image.
# Stage 1: install Python dependencies
# Stage 2: copy application code and run uvicorn
# ============================================================

# ---------- Stage 1: dependencies ----------
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build-essential for any compiled wheels (e.g. psycopg2-binary)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc libpq-dev && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ---------- Stage 2: runtime ----------
FROM python:3.11-slim AS runtime

# Runtime dependency for psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends libpq5 && \
    rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source and data into the image
WORKDIR /app
COPY src/  ./src/
COPY data/ ./data/

# The FastAPI app uses relative imports:
#   from schemas import ...
#   from database import ...
# Setting WORKDIR to the serving package directory makes those resolve.
WORKDIR /app/src/serving

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
