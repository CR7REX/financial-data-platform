FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y gcc libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    confluent-kafka==2.3.0 \
    python-schema-registry-client==2.5.0 \
    sqlalchemy==2.0.25 \
    psycopg2-binary==2.9.9 \
    pandas==2.1.4 \
    pyarrow==14.0.2 \
    structlog==24.1.0 \
    python-dotenv==1.0.0 \
    requests==2.31.0

COPY . .

CMD ["python", "processor.py"]