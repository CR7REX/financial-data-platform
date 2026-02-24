#!/usr/bin/env python3
"""
Mock market data producer for demonstration
Generates simulated stock price movements for testing the pipeline
"""
import json
import time
import signal
import sys
import random
from datetime import datetime, timezone
from typing import List

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

# Stock symbols with base prices (for realistic simulation)
STOCKS = {
    "SPY": {"base": 595.0, "volatility": 0.002},
    "QQQ": {"base": 515.0, "volatility": 0.003},
    "AAPL": {"base": 235.0, "volatility": 0.004},
    "MSFT": {"base": 425.0, "volatility": 0.003},
    "GOOGL": {"base": 185.0, "volatility": 0.005},
    "TSLA": {"base": 345.0, "volatility": 0.008},
    "META": {"base": 595.0, "volatility": 0.004},
    "NVDA": {"base": 138.0, "volatility": 0.006},
}

# Current prices (will drift over time)
current_prices = {symbol: data["base"] for symbol, data in STOCKS.items()}

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


class MockStockPriceProducer:
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
                'acks': 'all',
                'enable.idempotence': True,
                'compression.type': 'lz4',
            }
            self.producer = Producer(conf)
            
            logger.info("Connected to Kafka", broker=KAFKA_BROKER)
            
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            raise
    
    def generate_price_data(self, symbol: str) -> dict:
        """Generate simulated stock price data with random walk"""
        try:
            stock_config = STOCKS[symbol]
            base_price = current_prices[symbol]
            volatility = stock_config["volatility"]
            
            # Random walk with mean reversion toward base price
            drift = (stock_config["base"] - base_price) * 0.001
            change_pct = random.gauss(drift, volatility)
            
            new_price = base_price * (1 + change_pct)
            current_prices[symbol] = new_price
            
            # Calculate change from previous close (simulated)
            prev_close = stock_config["base"]
            change = new_price - prev_close
            change_percent = (change / prev_close) * 100
            
            # OHLC based on the current price
            daily_high = new_price * (1 + abs(random.gauss(0, volatility)))
            daily_low = new_price * (1 - abs(random.gauss(0, volatility)))
            daily_open = stock_config["base"] * (1 + random.gauss(0, volatility * 0.5))
            
            # Random volume (more volume for bigger price moves)
            base_volume = 1000000
            volume = int(base_volume * (1 + abs(change_percent) * 10) * random.uniform(0.5, 1.5))
            
            return {
                "symbol": symbol,
                "timestamp": int(datetime.now(timezone.utc).timestamp() * 1000),
                "price": round(new_price, 4),
                "volume": volume,
                "change": round(change, 4),
                "change_percent": round(change_percent, 4),
                "open": round(daily_open, 4),
                "high": round(daily_high, 4),
                "low": round(daily_low, 4),
                "prev_close": round(prev_close, 4)
            }
            
        except Exception as e:
            logger.error("Failed to generate price data", symbol=symbol, error=str(e))
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
    
    def run(self, interval: int = 2):
        """Main loop - generate and stream stock prices"""
        logger.info("Starting MOCK stock price producer", 
                   symbols=list(STOCKS.keys()), 
                   interval_seconds=interval)
        
        self.connect()
        
        while self.running:
            for symbol in STOCKS.keys():
                if not self.running:
                    break
                    
                data = self.generate_price_data(symbol)
                if data:
                    self.send_to_kafka(data)
                    logger.info("MOCK stock data generated", 
                               symbol=symbol,
                               price=data['price'],
                               change=f"{data['change_percent']:+.2f}%")
                
                # Small delay between symbols
                time.sleep(0.1)
            
            # Wait for next batch
            time.sleep(interval)
        
        # Flush remaining messages
        logger.info("Flushing remaining messages...")
        self.producer.flush(timeout=30)
        logger.info("Producer stopped")


def main():
    logger.info("MOCK Stock Price Producer starting...")
    logger.info("Note: Using simulated data for demonstration purposes")
    producer = MockStockPriceProducer()
    
    try:
        producer.run(interval=2)  # Generate every 2 seconds
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error("Unexpected error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()