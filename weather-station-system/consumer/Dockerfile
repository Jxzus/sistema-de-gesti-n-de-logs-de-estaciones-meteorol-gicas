FROM python:3.13-slim

LABEL maintainer="Weather Station System"
LABEL description="Consumidor de datos meteorológicos - persiste en PostgreSQL"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    gcc \
    libc6-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

RUN adduser --disabled-password --gecos '' appuser
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=20s --retries=3 \
    CMD python -c "import psycopg2; import pika; print('OK')" || exit 1

CMD ["python", "-u", "consumer.py"]
