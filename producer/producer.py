#!/usr/bin/env python3
"""
Real-time market data producer
Streams stock prices from Yahoo Finance to Kafka
"""
import json
import time
import signal
import sys
from datetime import datetime, timezone
from typing import List

import yfinance as yf
import structlog
from confluent_kafka import Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

logger = structlog.get_logger()

# Configuration
KAFKA_BROKER = "redpanda:9092"
SCHEMA_REGISTRY_URL = "http://redpanda:8081"
TOPIC_NAME = "stock-prices"

# Stock symbols to track
SYMBOLS = ["SPY", "QQQ", "AAPL", "MSFT", "GOOGL", "TSLA", "META", "NVDA"]

# Avro schema for stock price data
STOCK_PRICE_SCHEMA = """
{
  "type": "record",
  "name": "StockPrice",
  "namespace": "com.financial",
  "fields": [
    {"name": "symbol", "type": "string"},
    {"name": "timestamp", "type": "long", "logicalType": "timestamp-millis"},
    {"name": "price", "type": "double"},
    {"name": "volume", "type": "long"},
    {"name": "change", "type": "double"},
    {"name": "change_percent", "type": "double"},
    {"name": "open", "type": ["null", "double"], "default": null},
    {"name": "high", "type": ["null", "double"], "default": null},
    {"name": "low", "type": ["null", "double"], "default": null},
    {"name": "prev_close", "type": ["null", "double"], "default": null}
  ]
}
"""


class StockPriceProducer:
    def __init__(self):
        self.running = True
        self.producer = None
        self.serializer = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal, stopping producer...")
        self.running = False
        
    def _delivery_report(self, err, msg):
        """Callback for Kafka delivery reports"""
        if err is not None:
            logger.error("Message delivery failed", error=str(err), topic=msg.topic())
        else:
            logger.debug("Message delivered", 
                        topic=msg.topic(), 
                        partition=msg.partition(),
                        offset=msg.offset())
    
    def connect(self):
        """Initialize Kafka producer with Avro serialization"""
        try:
            # Setup Schema Registry
            schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            
            # Create Avro serializer
            self.serializer = AvroSerializer(
                schema_registry_client,
                STOCK_PRICE_SCHEMA,
                lambda obj, ctx: obj  # Identity function - already a dict
            )
            
            # Setup Kafka producer
            conf = {
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': 'stock-price-producer',
                'retries': 3,
                'retry.backoff.ms': 1000,
                'acks': 'all',  # Ensure exactly-once semantics
                'enable.idempotence': True,
                'compression.type': 'lz4',
            }
            self.producer = Producer(conf)
            
            logger.info("Connected to Kafka", broker=KAFKA_BROKER)
            
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            raise
    
    def fetch_stock_data(self, symbol: str) -> dict:
        """Fetch real-time stock data from Yahoo Finance"""
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info
            history = ticker.history(period="1d", interval="1m")
            
            if history.empty:
                logger.warning("No data received", symbol=symbol)
                return None
            
            latest = history.iloc[-1]
            prev_close = info.get('previousClose', latest['Close'])
            change = latest['Close'] - prev_close
            change_percent = (change / prev_close) * 100 if prev_close else 0
            
            return {
                "symbol": symbol,
                "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "price": round(latest['Close'], 4),
                "volume": int(latest['Volume']),
                "change": round(change, 4),
                "change_percent": round(change_percent, 4),
                "open": round(latest['Open'], 4) if not pd.isna(latest['Open']) else None,
                "high": round(latest['High'], 4) if not pd.isna(latest['High']) else None,
                "low": round(latest['Low'], 4) if not pd.isna(latest['Low']) else None,
                "prev_close": round(prev_close, 4) if prev_close else None
            }
            
        except Exception as e:
            logger.error("Failed to fetch stock data", symbol=symbol, error=str(e))
            return None
    
    def send_to_kafka(self, data: dict):
        """Send stock data to Kafka with Avro serialization"""
        try:
            # Serialize to Avro
            serialized = self.serializer(
                data, 
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            )
            
            # Produce message with key for partitioning (by symbol)
            self.producer.produce(
                topic=TOPIC_NAME,
                key=data['symbol'].encode('utf-8'),
                value=serialized,
                callback=self._delivery_report
            )
            
            # Non-blocking poll for callbacks
            self.producer.poll(0)
            
        except Exception as e:
            logger.error("Failed to send to Kafka", error=str(e))
    
    def run(self, interval: int = 5):
        """Main loop - fetch and stream stock prices"""
        logger.info("Starting stock price producer", 
                   symbols=SYMBOLS, 
                   interval_seconds=interval)
        
        self.connect()
        
        while self.running:
            for symbol in SYMBOLS:
                if not self.running:
                    break
                    
                data = self.fetch_stock_data(symbol)
                if data:
                    self.send_to_kafka(data)
                    logger.info("Stock data fetched", 
                               symbol=symbol,
                               price=data['price'],
                               change=f"{data['change_percent']:+.2f}%")
                
                # Small delay between symbols to avoid rate limiting
                time.sleep(0.5)
            
            # Wait for next batch
            time.sleep(interval)
        
        # Flush remaining messages
        logger.info("Flushing remaining messages...")
        self.producer.flush(timeout=30)
        logger.info("Producer stopped")


def main():
    import pandas as pd  # Import here to avoid circular issues
    
    logger.info("Stock Price Producer starting...")
    producer = StockPriceProducer()
    
    try:
        producer.run(interval=10)  # Fetch every 10 seconds
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error("Unexpected error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()