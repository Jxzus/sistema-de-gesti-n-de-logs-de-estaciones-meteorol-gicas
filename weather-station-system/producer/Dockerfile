FROM python:3.13-slim

LABEL maintainer="Weather Station System"
LABEL description="Productor de datos meteorológicos - publica en RabbitMQ"

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código fuente
COPY producer.py .

# Usuario no privilegiado
RUN adduser --disabled-password --gecos '' appuser
USER appuser

HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import pika; print('OK')" || exit 1

CMD ["python", "-u", "producer.py"]
