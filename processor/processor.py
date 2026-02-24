#!/usr/bin/env python3
"""
Stream processor for real-time stock prices
Consumes from Kafka, processes, and stores to PostgreSQL
"""
import json
import signal
import sys
from datetime import datetime, timezone
from typing import Dict, Optional

import structlog
from confluent_kafka import Consumer, KafkaError, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from sqlalchemy import create_engine, Column, String, Float, BigInteger, DateTime, Index
from sqlalchemy.orm import declarative_base, sessionmaker

logger = structlog.get_logger()

# Configuration
KAFKA_BROKER = "redpanda:9092"
SCHEMA_REGISTRY_URL = "http://redpanda:8081"
TOPIC_NAME = "stock-prices"
CONSUMER_GROUP = "stock-price-processor"
DATABASE_URL = "postgresql://airflow:airflow@postgres:5432/financial_data"

# Avro schema (must match producer)
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

Base = declarative_base()


class StockPrice(Base):
    """Stock price model for PostgreSQL"""
    __tablename__ = 'stock_prices'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    symbol = Column(String(10), nullable=False, index=True)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    price = Column(Float, nullable=False)
    volume = Column(BigInteger)
    change = Column(Float)
    change_percent = Column(Float)
    open_price = Column(Float)
    high = Column(Float)
    low = Column(Float)
    prev_close = Column(Float)
    processed_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    
    # Composite index for time-series queries
    __table_args__ = (
        Index('idx_symbol_timestamp', 'symbol', 'timestamp'),
    )


class StockPriceProcessor:
    def __init__(self):
        self.running = True
        self.consumer = None
        self.deserializer = None
        self.engine = None
        self.Session = None
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
    def _signal_handler(self, signum, frame):
        logger.info("Received shutdown signal, stopping processor...")
        self.running = False
    
    def connect_database(self):
        """Initialize database connection"""
        try:
            self.engine = create_engine(DATABASE_URL, pool_pre_ping=True)
            Base.metadata.create_all(self.engine)
            self.Session = sessionmaker(bind=self.engine)
            logger.info("Connected to database")
        except Exception as e:
            logger.error("Failed to connect to database", error=str(e))
            raise
    
    def connect_kafka(self):
        """Initialize Kafka consumer with Avro deserialization"""
        try:
            # Setup Schema Registry
            schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            
            # Create Avro deserializer
            self.deserializer = AvroDeserializer(
                schema_registry_client,
                STOCK_PRICE_SCHEMA,
                lambda data, ctx: data  # Returns dict directly
            )
            
            # Setup Kafka consumer
            conf = {
                'bootstrap.servers': KAFKA_BROKER,
                'group.id': CONSUMER_GROUP,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': False,  # Manual commit for exactly-once
                'max.poll.interval.ms': 300000,
            }
            self.consumer = Consumer(conf)
            self.consumer.subscribe([TOPIC_NAME])
            
            logger.info("Connected to Kafka", 
                       broker=KAFKA_BROKER,
                       topic=TOPIC_NAME,
                       group=CONSUMER_GROUP)
            
        except Exception as e:
            logger.error("Failed to connect to Kafka", error=str(e))
            raise
    
    def process_message(self, msg) -> Optional[StockPrice]:
        """Deserialize and transform Kafka message to DB model"""
        try:
            # Deserialize Avro
            data = self.deserializer(
                msg.value(),
                SerializationContext(TOPIC_NAME, MessageField.VALUE)
            )
            
            if not data:
                logger.warning("Empty message received")
                return None
            
            # Convert timestamp (milliseconds to datetime)
            timestamp = datetime.fromtimestamp(data['timestamp'] / 1000, tz=timezone.utc)
            
            # Create DB record
            return StockPrice(
                symbol=data['symbol'],
                timestamp=timestamp,
                price=data['price'],
                volume=data['volume'],
                change=data['change'],
                change_percent=data['change_percent'],
                open_price=data.get('open'),
                high=data.get('high'),
                low=data.get('low'),
                prev_close=data.get('prev_close')
            )
            
        except Exception as e:
            logger.error("Failed to process message", error=str(e))
            return None
    
    def save_to_database(self, records: list):
        """Batch insert records to PostgreSQL"""
        if not records:
            return
        
        session = self.Session()
        try:
            session.add_all(records)
            session.commit()
            logger.info("Saved records to database", count=len(records))
        except Exception as e:
            session.rollback()
            logger.error("Failed to save to database", error=str(e))
            raise
        finally:
            session.close()
    
    def run(self, batch_size: int = 100):
        """Main processing loop"""
        logger.info("Starting stream processor...")
        
        self.connect_database()
        self.connect_kafka()
        
        batch = []
        
        while self.running:
            try:
                # Poll for messages
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Flush batch if idle
                    if batch:
                        self.save_to_database(batch)
                        self.consumer.commit()
                        batch = []
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.debug("Reached end of partition")
                    else:
                        logger.error("Kafka error", error=str(msg.error()))
                    continue
                
                # Process message
                record = self.process_message(msg)
                if record:
                    batch.append(record)
                    
                    # Log every 10th message to avoid spam
                    if len(batch) % 10 == 0:
                        logger.info("Processing batch", 
                                   symbol=record.symbol,
                                   price=record.price)
                
                # Commit and save batch
                if len(batch) >= batch_size:
                    self.save_to_database(batch)
                    self.consumer.commit()
                    batch = []
                    
            except Exception as e:
                logger.error("Error in processing loop", error=str(e))
                # Don't crash, keep processing
                continue
        
        # Cleanup
        if batch:
            self.save_to_database(batch)
            self.consumer.commit()
        
        self.consumer.close()
        logger.info("Processor stopped")


def main():
    logger.info("Stock Price Processor starting...")
    processor = StockPriceProcessor()
    
    try:
        processor.run(batch_size=50)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error("Unexpected error", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()