import os
from pathlib import Path

services_base = "services/stream-processing"

# 3. Anomaly Detection Integration Service (OpenSearch Ingest)
anomaly_service_code = """
# services/stream-processing/anomaly-detection/src/main.py
import asyncio
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pulsar import Client, Consumer, Message, ConsumerType
from opensearchpy import OpenSearch, helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AnomalyDetectionService:
    def __init__(
        self,
        pulsar_url: str = 'pulsar://pulsar-broker:6650',
        opensearch_host: str = 'opensearch-cluster-master',
        opensearch_port: int = 9200,
    ):
        self.pulsar_client = Client(pulsar_url)
        self.opensearch_client = OpenSearch(
            hosts=[{'host': opensearch_host, 'port': opensearch_port}],
            http_auth=('admin', 'admin'),  # Use proper secrets in production
            use_ssl=True,
            verify_certs=False,
        )
        
    def detect_anomalies(self, features: Dict[str, float]) -> tuple[bool, Optional[str], Optional[float]]:
        \"\"\"Simple anomaly detection logic (can be replaced with ML model)\"\"\"
        anomalies = []
        
        # Speed anomaly
        if features['avg_speed'] > 30 or features['avg_speed'] < 0.5:
            anomalies.append(f"Unusual speed: {features['avg_speed']:.2f} knots")
        
        # High speed variance
        if features['std_speed'] > 5.0:
            anomalies.append(f"High speed variance: {features['std_speed']:.2f}")
        
        # Excessive course changes
        if features['course_changes'] > 20:
            anomalies.append(f"Excessive course changes: {features['course_changes']}")
        
        if anomalies:
            return True, '; '.join(anomalies), max(features['std_speed'] / 5.0, features['course_changes'] / 20.0)
        
        return False, None, None
    
    def ingest_to_opensearch(self, vessel_id: str, tenant_id: str, features: Dict[str, float], 
                            is_anomaly: bool, anomaly_reason: Optional[str], severity: Optional[float]) -> bool:
        \"\"\"Ingest anomaly data to OpenSearch tenant-specific index\"\"\"
        index_name = f"{tenant_id}_anomalies"
        
        document = {
            'vessel_id': vessel_id,
            'tenant_id': tenant_id,
            'timestamp': datetime.utcnow().isoformat(),
            'features': features,
            'is_anomaly': is_anomaly,
            'anomaly_reason': anomaly_reason,
            'severity': severity,
        }
        
        try:
            self.opensearch_client.index(
                index=index_name,
                body=document,
                refresh=True,
            )
            return True
        except Exception as e:
            logger.error(f"Failed to ingest to OpenSearch: {e}")
            return False
    
    def send_to_dlq(self, message: Message, error_reason: str):
        \"\"\"Send failed message to DLQ\"\"\"
        dlq_topic = f"{message.topic_name()}_dlq"
        try:
            dlq_producer = self.pulsar_client.create_producer(dlq_topic)
            dlq_producer.send(
                message.data(),
                properties={
                    **message.properties(),
                    'dlq_reason': error_reason,
                    'dlq_timestamp': datetime.utcnow().isoformat(),
                }
            )
            dlq_producer.close()
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def send_to_retry(self, message: Message, retry_count: int):
        retry_delays = {1: '5s', 2: '1m', 3: '10m'}
        delay = retry_delays.get(retry_count, '10m')
        retry_topic = f"{message.topic_name()}_retry_{delay}"
        
        try:
            retry_producer = self.pulsar_client.create_producer(retry_topic)
            retry_producer.send(
                message.data(),
                properties={**message.properties(), 'retry_count': str(retry_count)}
            )
            retry_producer.close()
        except Exception as e:
            logger.error(f"Failed to send to retry: {e}")
    
    async def process_message(self, consumer: Consumer, message: Message):
        \"\"\"Process features and detect anomalies\"\"\"
        try:
            data = json.loads(message.data().decode('utf-8'))
            vessel_id = data['vessel_id']
            tenant_id = data['tenant_id']
            features = data['features']
            retry_count = int(message.properties().get('retry_count', '0'))
            
            # Detect anomalies
            is_anomaly, reason, severity = self.detect_anomalies(features)
            
            # Ingest to OpenSearch
            success = self.ingest_to_opensearch(vessel_id, tenant_id, features, is_anomaly, reason, severity)
            
            if not success:
                if retry_count < 3:
                    self.send_to_retry(message, retry_count + 1)
                else:
                    self.send_to_dlq(message, "Failed to ingest to OpenSearch")
                consumer.acknowledge(message)
                return
            
            consumer.acknowledge(message)
            
            if is_anomaly:
                logger.warning(f"Anomaly detected for vessel {vessel_id}: {reason}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            consumer.negative_acknowledge(message)
    
    async def start(self, subscription_name: str = 'anomaly-detection'):
        \"\"\"Start anomaly detection service\"\"\"
        topics = [
            'persistent://shipping-co-alpha/analytics/anomaly_detection',
            'persistent://logistics-beta/analytics/anomaly_detection',
            'persistent://maritime-gamma/analytics/anomaly_detection',
        ]
        
        consumer = self.pulsar_client.subscribe(
            topics,
            subscription_name=subscription_name,
            consumer_type=ConsumerType.KeyShared,
            receiver_queue_size=1000,
        )
        
        logger.info(f"Started anomaly detection service")
        
        try:
            while True:
                message = consumer.receive()
                await self.process_message(consumer, message)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
        finally:
            consumer.close()
            self.pulsar_client.close()

if __name__ == '__main__':
    service = AnomalyDetectionService()
    asyncio.run(service.start())
"""

# 4. Lakehouse Archival Service (Iceberg Writer)
lakehouse_service_code = """
# services/stream-processing/lakehouse-archival/src/main.py
import asyncio
import json
import logging
from typing import Dict, List
from datetime import datetime
from pulsar import Client, Consumer, Message, ConsumerType
from pyiceberg.catalog import load_catalog
from pyiceberg.table import Table
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LakehouseArchivalService:
    def __init__(
        self,
        pulsar_url: str = 'pulsar://pulsar-broker:6650',
        catalog_name: str = 'maritime_iceberg',
        batch_size: int = 100,
    ):
        self.pulsar_client = Client(pulsar_url)
        self.catalog = load_catalog(catalog_name)
        self.batch_size = batch_size
        self.batch_buffer = []
        
    def send_to_dlq(self, message: Message, error_reason: str):
        dlq_topic = f"{message.topic_name()}_dlq"
        try:
            dlq_producer = self.pulsar_client.create_producer(dlq_topic)
            dlq_producer.send(
                message.data(),
                properties={
                    **message.properties(),
                    'dlq_reason': error_reason,
                    'dlq_timestamp': datetime.utcnow().isoformat(),
                }
            )
            dlq_producer.close()
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def send_to_quarantine(self, messages: List[Message], error_reason: str):
        \"\"\"Send batch to quarantine topic for manual review\"\"\"
        quarantine_topic = f"{messages[0].topic_name()}_quarantine"
        try:
            quarantine_producer = self.pulsar_client.create_producer(quarantine_topic)
            for msg in messages:
                quarantine_producer.send(
                    msg.data(),
                    properties={
                        **msg.properties(),
                        'quarantine_reason': error_reason,
                        'quarantine_timestamp': datetime.utcnow().isoformat(),
                    }
                )
            quarantine_producer.close()
            logger.warning(f"Sent {len(messages)} messages to quarantine")
        except Exception as e:
            logger.error(f"Failed to send to quarantine: {e}")
    
    def append_to_iceberg(self, tenant_id: str, data_batch: List[Dict]) -> bool:
        \"\"\"Append batch to Iceberg table\"\"\"
        try:
            table_name = f"{tenant_id}.raw_telemetry_silver"
            table: Table = self.catalog.load_table(table_name)
            
            # Convert to DataFrame
            df = pd.DataFrame(data_batch)
            
            # Append to Iceberg
            table.append(df)
            
            logger.info(f"Appended {len(data_batch)} records to {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to append to Iceberg: {e}")
            return False
    
    async def flush_batch(self, consumer: Consumer, tenant_id: str):
        \"\"\"Flush accumulated batch to Iceberg\"\"\"
        if not self.batch_buffer:
            return
        
        messages = [item['message'] for item in self.batch_buffer]
        data_batch = [item['data'] for item in self.batch_buffer]
        
        success = self.append_to_iceberg(tenant_id, data_batch)
        
        if success:
            # Acknowledge all messages in batch
            for msg in messages:
                consumer.acknowledge(msg)
            logger.info(f"Flushed batch of {len(messages)} messages for tenant {tenant_id}")
        else:
            # Send batch to quarantine for manual review
            self.send_to_quarantine(messages, "Failed to write batch to Iceberg")
            for msg in messages:
                consumer.acknowledge(msg)
        
        self.batch_buffer.clear()
    
    async def process_message(self, consumer: Consumer, message: Message):
        \"\"\"Buffer messages and flush in batches\"\"\"
        try:
            data = json.loads(message.data().decode('utf-8'))
            tenant_id = data['tenant_id']
            
            # Add to batch buffer
            self.batch_buffer.append({
                'message': message,
                'data': data,
            })
            
            # Flush when batch size reached
            if len(self.batch_buffer) >= self.batch_size:
                await self.flush_batch(consumer, tenant_id)
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            self.send_to_dlq(message, str(e))
            consumer.acknowledge(message)
    
    async def start(self, subscription_name: str = 'lakehouse-archival'):
        \"\"\"Start lakehouse archival service\"\"\"
        topics = [
            'persistent://shipping-co-alpha/vessel-tracking/raw_telemetry',
            'persistent://logistics-beta/vessel-tracking/raw_telemetry',
            'persistent://maritime-gamma/vessel-tracking/raw_telemetry',
        ]
        
        consumer = self.pulsar_client.subscribe(
            topics,
            subscription_name=subscription_name,
            consumer_type=ConsumerType.KeyShared,
            receiver_queue_size=1000,
        )
        
        logger.info(f"Started lakehouse archival service")
        
        try:
            while True:
                message = consumer.receive(timeout_millis=5000)
                if message:
                    await self.process_message(consumer, message)
                else:
                    # Flush remaining batch on timeout
                    if self.batch_buffer:
                        tenant_id = self.batch_buffer[0]['data']['tenant_id']
                        await self.flush_batch(consumer, tenant_id)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            if self.batch_buffer:
                tenant_id = self.batch_buffer[0]['data']['tenant_id']
                await self.flush_batch(consumer, tenant_id)
        finally:
            consumer.close()
            self.pulsar_client.close()

if __name__ == '__main__':
    service = LakehouseArchivalService()
    asyncio.run(service.start())
"""

# Write anomaly detection service
anomaly_dir = f"{services_base}/anomaly-detection/src"
Path(anomaly_dir).mkdir(parents=True, exist_ok=True)
with open(f"{anomaly_dir}/main.py", "w") as f:
    f.write(anomaly_service_code)

with open(f"{services_base}/anomaly-detection/Dockerfile", "w") as f:
    f.write("""FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
ENV PYTHONUNBUFFERED=1
CMD ["python", "src/main.py"]
""")

with open(f"{services_base}/anomaly-detection/requirements.txt", "w") as f:
    f.write("""pulsar-client==3.4.0
opensearch-py==2.4.2
""")

# Write lakehouse archival service
lakehouse_dir = f"{services_base}/lakehouse-archival/src"
Path(lakehouse_dir).mkdir(parents=True, exist_ok=True)
with open(f"{lakehouse_dir}/main.py", "w") as f:
    f.write(lakehouse_service_code)

with open(f"{services_base}/lakehouse-archival/Dockerfile", "w") as f:
    f.write("""FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
ENV PYTHONUNBUFFERED=1
CMD ["python", "src/main.py"]
""")

with open(f"{services_base}/lakehouse-archival/requirements.txt", "w") as f:
    f.write("""pulsar-client==3.4.0
pyiceberg==0.6.0
pandas==2.1.4
""")

print("✅ Anomaly Detection Service Created")
print(f"   📂 Location: {services_base}/anomaly-detection/")
print(f"   🔧 Features:")
print(f"      • Anomaly detection logic (speed, variance, course changes)")
print(f"      • OpenSearch ingest to tenant-specific indices")
print(f"      • DLQ/retry handling")
print()

print("✅ Lakehouse Archival Service Created")
print(f"   📂 Location: {services_base}/lakehouse-archival/")
print(f"   🔧 Features:")
print(f"      • Batch writes to Iceberg tables")
print(f"      • Quarantine topic for failed batches")
print(f"      • Automatic batch flushing on timeout")
print()

services_created = {
    "anomaly_detection": f"{services_base}/anomaly-detection/",
    "lakehouse_archival": f"{services_base}/lakehouse-archival/",
}
