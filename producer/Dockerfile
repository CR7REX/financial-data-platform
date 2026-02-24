FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    confluent-kafka==2.3.0 \
    yfinance==0.2.36 \
    websocket-client==1.7.0 \
    avro==1.11.3 \
    python-schema-registry-client==2.5.0 \
    python-dotenv==1.0.0 \
    structlog==24.1.0 \
    pandas==2.1.4 \
    requests==2.31.0

COPY . .

CMD ["python", "producer.py"]