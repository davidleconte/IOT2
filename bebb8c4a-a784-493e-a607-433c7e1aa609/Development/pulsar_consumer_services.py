import os
from pathlib import Path

# Create services directory structure for Pulsar consumer services
services_base = "services/stream-processing"
Path(services_base).mkdir(parents=True, exist_ok=True)

# 1. Raw Telemetry Ingestion Service
telemetry_service_code = """
# services/stream-processing/raw-telemetry-ingestion/src/main.py
import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pulsar import Client, Consumer, Message, ConsumerType
from pydantic import BaseModel, ValidationError, Field
from redis import Redis
from cassandra.cluster import Cluster, Session

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Schema validation models
class VesselTelemetrySchema(BaseModel):
    vessel_id: str = Field(..., pattern=r'^IMO\\d{7}$')
    timestamp: datetime
    latitude: float = Field(..., ge=-90, le=90)
    longitude: float = Field(..., ge=-180, le=180)
    speed: float = Field(..., ge=0, le=50)
    course: float = Field(..., ge=0, le=360)
    heading: Optional[float] = Field(None, ge=0, le=360)
    tenant_id: str
    sensor_id: str
    
    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}

class TelemetryIngestionService:
    def __init__(
        self,
        pulsar_url: str = 'pulsar://pulsar-broker:6650',
        redis_url: str = 'redis://redis:6379',
        cassandra_hosts: list = None,
    ):
        self.pulsar_client = Client(pulsar_url)
        self.redis_client = Redis.from_url(redis_url, decode_responses=True)
        self.cassandra_hosts = cassandra_hosts or ['cassandra-0', 'cassandra-1', 'cassandra-2']
        self.cassandra_session: Optional[Session] = None
        self.dedup_ttl = 3600  # 1 hour deduplication window
        
    def connect_cassandra(self, keyspace: str = 'fleet_telemetry'):
        cluster = Cluster(self.cassandra_hosts)
        self.cassandra_session = cluster.connect(keyspace)
        logger.info(f"Connected to Cassandra keyspace: {keyspace}")
        
    def check_duplicate(self, message_id: str) -> bool:
        \"\"\"Check if message already processed using Redis\"\"\"
        return self.redis_client.exists(f"processed:{message_id}")
    
    def mark_processed(self, message_id: str):
        \"\"\"Mark message as processed in Redis\"\"\"
        self.redis_client.setex(f"processed:{message_id}", self.dedup_ttl, '1')
    
    def validate_schema(self, data: Dict[str, Any]) -> tuple[bool, Optional[VesselTelemetrySchema], Optional[str]]:
        \"\"\"Validate message against schema\"\"\"
        try:
            validated_data = VesselTelemetrySchema(**data)
            return True, validated_data, None
        except ValidationError as e:
            return False, None, str(e)
    
    def route_to_tenant(self, tenant_id: str, data: VesselTelemetrySchema) -> str:
        \"\"\"Route message to tenant-specific processing based on tenant_id\"\"\"
        tenant_mappings = {
            'shipping-co-alpha': 'persistent://shipping-co-alpha/vessel-tracking/raw_telemetry',
            'logistics-beta': 'persistent://logistics-beta/vessel-tracking/raw_telemetry',
            'maritime-gamma': 'persistent://maritime-gamma/vessel-tracking/raw_telemetry',
        }
        return tenant_mappings.get(tenant_id, f'persistent://{tenant_id}/vessel-tracking/raw_telemetry')
    
    def send_to_dlq(self, message: Message, error_reason: str, error_type: str):
        \"\"\"Send failed message to DLQ with error metadata\"\"\"
        original_topic = message.topic_name()
        tenant_id = message.properties().get('tenant_id', 'unknown')
        
        dlq_topic = f"{original_topic}_dlq"
        
        try:
            dlq_producer = self.pulsar_client.create_producer(dlq_topic)
            dlq_producer.send(
                message.data(),
                properties={
                    **message.properties(),
                    'dlq_reason': error_reason,
                    'dlq_error_type': error_type,
                    'dlq_timestamp': datetime.utcnow().isoformat(),
                    'original_topic': original_topic,
                }
            )
            dlq_producer.close()
            logger.warning(f"Message sent to DLQ: {dlq_topic} - Reason: {error_reason}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def send_to_retry(self, message: Message, retry_count: int):
        \"\"\"Send message to retry topic with exponential backoff\"\"\"
        retry_delays = {1: '5s', 2: '1m', 3: '10m'}
        delay = retry_delays.get(retry_count, '10m')
        
        original_topic = message.topic_name()
        retry_topic = f"{original_topic}_retry_{delay}"
        
        try:
            retry_producer = self.pulsar_client.create_producer(retry_topic)
            retry_producer.send(
                message.data(),
                properties={
                    **message.properties(),
                    'retry_count': str(retry_count),
                    'retry_timestamp': datetime.utcnow().isoformat(),
                }
            )
            retry_producer.close()
            logger.info(f"Message sent to retry topic: {retry_topic} (attempt {retry_count})")
        except Exception as e:
            logger.error(f"Failed to send to retry: {e}")
    
    def ingest_to_hcd(self, data: VesselTelemetrySchema):
        \"\"\"Write validated telemetry to HyperConverged Database (Cassandra)\"\"\"
        if not self.cassandra_session:
            logger.error("Cassandra session not initialized")
            return False
        
        try:
            query = \"\"\"
            INSERT INTO raw_telemetry 
            (vessel_id, timestamp, latitude, longitude, speed, course, heading, tenant_id, sensor_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            \"\"\"
            self.cassandra_session.execute(
                query,
                (
                    data.vessel_id,
                    data.timestamp,
                    data.latitude,
                    data.longitude,
                    data.speed,
                    data.course,
                    data.heading,
                    data.tenant_id,
                    data.sensor_id,
                )
            )
            return True
        except Exception as e:
            logger.error(f"Failed to write to Cassandra: {e}")
            return False
    
    async def process_message(self, consumer: Consumer, message: Message):
        \"\"\"Main message processing pipeline\"\"\"
        try:
            # Extract message metadata
            message_id = message.properties().get('sequence_id', message.message_id().serialize().decode())
            tenant_id = message.properties().get('tenant_id', 'unknown')
            retry_count = int(message.properties().get('retry_count', '0'))
            
            # 1. Deduplication check
            if self.check_duplicate(message_id):
                logger.info(f"Duplicate message detected: {message_id}")
                consumer.acknowledge(message)
                return
            
            # 2. Schema validation
            data = json.loads(message.data().decode('utf-8'))
            is_valid, validated_data, error_msg = self.validate_schema(data)
            
            if not is_valid:
                logger.error(f"Schema validation failed: {error_msg}")
                self.send_to_dlq(message, error_msg, 'SCHEMA_VALIDATION_ERROR')
                consumer.acknowledge(message)
                return
            
            # 3. Tenant routing validation
            if tenant_id not in ['shipping-co-alpha', 'logistics-beta', 'maritime-gamma']:
                logger.error(f"Invalid tenant_id: {tenant_id}")
                self.send_to_dlq(message, f"Invalid tenant: {tenant_id}", 'TENANT_ROUTING_ERROR')
                consumer.acknowledge(message)
                return
            
            # 4. Write to HCD (online store)
            write_success = self.ingest_to_hcd(validated_data)
            
            if not write_success:
                # Retry logic
                if retry_count < 3:
                    self.send_to_retry(message, retry_count + 1)
                    consumer.acknowledge(message)
                else:
                    self.send_to_dlq(message, "Max retries exceeded", 'HCD_WRITE_ERROR')
                    consumer.acknowledge(message)
                return
            
            # 5. Mark as processed and forward to next stage
            self.mark_processed(message_id)
            
            # Forward to tenant-specific topic for feature computation
            tenant_topic = self.route_to_tenant(tenant_id, validated_data)
            producer = self.pulsar_client.create_producer(tenant_topic)
            producer.send(
                validated_data.json().encode('utf-8'),
                partition_key=validated_data.vessel_id,  # Key_Shared routing by vessel
                properties=message.properties()
            )
            producer.close()
            
            consumer.acknowledge(message)
            logger.info(f"Successfully processed message: {message_id} for vessel {validated_data.vessel_id}")
            
        except Exception as e:
            logger.error(f"Unexpected error processing message: {e}")
            consumer.negative_acknowledge(message)
    
    async def start(self, subscription_name: str = 'telemetry-ingestion'):
        \"\"\"Start consuming messages with Key_Shared subscription\"\"\"
        self.connect_cassandra()
        
        # Subscribe to raw telemetry topics for all tenants
        topics = [
            'persistent://shipping-co-alpha/vessel-tracking/tenant.shipping-co-alpha.vessel-tracking.raw',
            'persistent://logistics-beta/vessel-tracking/tenant.logistics-beta.vessel-tracking.raw',
            'persistent://maritime-gamma/vessel-tracking/tenant.maritime-gamma.vessel-tracking.raw',
        ]
        
        consumer = self.pulsar_client.subscribe(
            topics,
            subscription_name=subscription_name,
            consumer_type=ConsumerType.KeyShared,  # Per-vessel ordering
            receiver_queue_size=1000,
            max_total_receiver_queue_size_across_partitions=50000,
        )
        
        logger.info(f"Started telemetry ingestion service with Key_Shared subscription")
        logger.info(f"Subscribed to topics: {topics}")
        
        try:
            while True:
                message = consumer.receive()
                await self.process_message(consumer, message)
        except KeyboardInterrupt:
            logger.info("Shutting down service...")
        finally:
            consumer.close()
            self.pulsar_client.close()

if __name__ == '__main__':
    service = TelemetryIngestionService()
    asyncio.run(service.start())
"""

# Write raw telemetry ingestion service
telemetry_dir = f"{services_base}/raw-telemetry-ingestion/src"
Path(telemetry_dir).mkdir(parents=True, exist_ok=True)
with open(f"{telemetry_dir}/main.py", "w") as f:
    f.write(telemetry_service_code)

# Dockerfile for telemetry service
telemetry_dockerfile = """FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1

CMD ["python", "src/main.py"]
"""

with open(f"{services_base}/raw-telemetry-ingestion/Dockerfile", "w") as f:
    f.write(telemetry_dockerfile)

# Requirements for telemetry service
telemetry_requirements = """pulsar-client==3.4.0
pydantic==2.5.3
redis==5.0.1
cassandra-driver==3.29.0
"""

with open(f"{services_base}/raw-telemetry-ingestion/requirements.txt", "w") as f:
    f.write(telemetry_requirements)

print("✅ Raw Telemetry Ingestion Service Created")
print(f"   📂 Location: {services_base}/raw-telemetry-ingestion/")
print(f"   📄 Files: src/main.py, Dockerfile, requirements.txt")
print(f"   🔧 Features:")
print(f"      • Schema validation with Pydantic")
print(f"      • Deduplication check via Redis")
print(f"      • Tenant routing logic")
print(f"      • DLQ handling for schema errors")
print(f"      • Retry handling with exponential backoff")
print(f"      • HCD (Cassandra) writer for online store")
print(f"      • Key_Shared subscription for per-vessel ordering")
print()

telemetry_service_created = {
    "service": "raw-telemetry-ingestion",
    "path": f"{services_base}/raw-telemetry-ingestion/",
    "features": ["schema_validation", "deduplication", "tenant_routing", "dlq_handling", "retry_logic", "hcd_writer"]
}
