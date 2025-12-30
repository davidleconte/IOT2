import os
from pathlib import Path

services_base = "services/stream-processing"

# 5. DLQ Handler Service
dlq_handler_code = """
# services/stream-processing/dlq-handler/src/main.py
import asyncio
import json
import logging
from typing import Dict, Any
from datetime import datetime
from pulsar import Client, Consumer, Message, ConsumerType
from opensearchpy import OpenSearch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DLQHandlerService:
    \"\"\"Handles messages from Dead Letter Queue - logs, alerts, and monitors\"\"\"
    def __init__(
        self,
        pulsar_url: str = 'pulsar://pulsar-broker:6650',
        opensearch_host: str = 'opensearch-cluster-master',
        opensearch_port: int = 9200,
        alert_threshold: int = 10,
    ):
        self.pulsar_client = Client(pulsar_url)
        self.opensearch_client = OpenSearch(
            hosts=[{'host': opensearch_host, 'port': opensearch_port}],
            http_auth=('admin', 'admin'),
            use_ssl=True,
            verify_certs=False,
        )
        self.alert_threshold = alert_threshold
        self.dlq_counts = {}  # tenant -> count
        
    def log_to_opensearch(self, message: Message, metadata: Dict[str, Any]):
        \"\"\"Log DLQ message to OpenSearch for monitoring\"\"\"
        tenant_id = message.properties().get('tenant_id', 'unknown')
        index_name = f"{tenant_id}_dlq_logs"
        
        document = {
            'timestamp': datetime.utcnow().isoformat(),
            'tenant_id': tenant_id,
            'original_topic': metadata.get('original_topic'),
            'dlq_reason': metadata.get('dlq_reason'),
            'error_type': metadata.get('dlq_error_type'),
            'message_id': message.message_id().serialize().decode(),
            'message_properties': dict(message.properties()),
            'dlq_timestamp': metadata.get('dlq_timestamp'),
        }
        
        try:
            self.opensearch_client.index(
                index=index_name,
                body=document,
                refresh=True,
            )
            logger.info(f"Logged DLQ message to OpenSearch: {index_name}")
        except Exception as e:
            logger.error(f"Failed to log to OpenSearch: {e}")
    
    def alert_on_threshold(self, tenant_id: str):
        \"\"\"Send alert if DLQ count exceeds threshold\"\"\"
        self.dlq_counts[tenant_id] = self.dlq_counts.get(tenant_id, 0) + 1
        
        if self.dlq_counts[tenant_id] >= self.alert_threshold:
            # In production: send to PagerDuty, Slack, email, etc.
            logger.critical(f"ALERT: Tenant {tenant_id} has {self.dlq_counts[tenant_id]} DLQ messages (threshold: {self.alert_threshold})")
            
            # Send alert to monitoring topic
            alert_topic = f"persistent://{tenant_id}/system/alerts"
            try:
                alert_producer = self.pulsar_client.create_producer(alert_topic)
                alert_payload = {
                    'alert_type': 'DLQ_THRESHOLD_EXCEEDED',
                    'tenant_id': tenant_id,
                    'dlq_count': self.dlq_counts[tenant_id],
                    'threshold': self.alert_threshold,
                    'timestamp': datetime.utcnow().isoformat(),
                }
                alert_producer.send(json.dumps(alert_payload).encode('utf-8'))
                alert_producer.close()
            except Exception as e:
                logger.error(f"Failed to send alert: {e}")
            
            # Reset counter after alerting
            self.dlq_counts[tenant_id] = 0
    
    def store_for_manual_review(self, message: Message, metadata: Dict[str, Any]):
        \"\"\"Store DLQ message in a format suitable for manual review\"\"\"
        tenant_id = message.properties().get('tenant_id', 'unknown')
        review_topic = f"persistent://{tenant_id}/system/manual_review"
        
        try:
            review_producer = self.pulsar_client.create_producer(review_topic)
            review_payload = {
                'original_message': message.data().decode('utf-8'),
                'metadata': metadata,
                'properties': dict(message.properties()),
                'stored_at': datetime.utcnow().isoformat(),
                'requires_action': True,
            }
            review_producer.send(json.dumps(review_payload).encode('utf-8'))
            review_producer.close()
            logger.info(f"Stored message for manual review: {tenant_id}")
        except Exception as e:
            logger.error(f"Failed to store for review: {e}")
    
    async def process_message(self, consumer: Consumer, message: Message):
        \"\"\"Process DLQ message\"\"\"
        try:
            props = message.properties()
            tenant_id = props.get('tenant_id', 'unknown')
            
            metadata = {
                'original_topic': props.get('original_topic'),
                'dlq_reason': props.get('dlq_reason'),
                'dlq_error_type': props.get('dlq_error_type'),
                'dlq_timestamp': props.get('dlq_timestamp'),
            }
            
            # 1. Log to OpenSearch for monitoring
            self.log_to_opensearch(message, metadata)
            
            # 2. Alert if threshold exceeded
            self.alert_on_threshold(tenant_id)
            
            # 3. Store for manual review
            self.store_for_manual_review(message, metadata)
            
            consumer.acknowledge(message)
            
        except Exception as e:
            logger.error(f"Error processing DLQ message: {e}")
            consumer.negative_acknowledge(message)
    
    async def start(self, subscription_name: str = 'dlq-handler'):
        \"\"\"Start DLQ handler service\"\"\"
        # Subscribe to all DLQ topics
        dlq_pattern = 'persistent://.*/.*/_dlq'
        
        consumer = self.pulsar_client.subscribe(
            topics_pattern=dlq_pattern,
            subscription_name=subscription_name,
            consumer_type=ConsumerType.Shared,  # Multiple handlers can process DLQ
        )
        
        logger.info(f"Started DLQ handler service")
        
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
    service = DLQHandlerService()
    asyncio.run(service.start())
"""

# 6. Retry Processor Service
retry_processor_code = """
# services/stream-processing/retry-processor/src/main.py
import asyncio
import json
import logging
import time
from typing import Dict
from datetime import datetime
from pulsar import Client, Consumer, Message, ConsumerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RetryProcessorService:
    \"\"\"Processes retry topics and re-routes messages back to original processing\"\"\"
    def __init__(
        self,
        pulsar_url: str = 'pulsar://pulsar-broker:6650',
        max_retry_attempts: int = 3,
    ):
        self.pulsar_client = Client(pulsar_url)
        self.max_retry_attempts = max_retry_attempts
        
    def get_original_topic(self, retry_topic: str) -> str:
        \"\"\"Extract original topic from retry topic name\"\"\"
        # Format: persistent://tenant/namespace/topic_retry_5s
        parts = retry_topic.split('_retry_')
        return parts[0] if len(parts) > 0 else retry_topic
    
    def get_delay_seconds(self, delay_str: str) -> int:
        \"\"\"Convert delay string to seconds\"\"\"
        delay_map = {'5s': 5, '1m': 60, '10m': 600}
        return delay_map.get(delay_str, 60)
    
    def send_to_dlq(self, message: Message, reason: str):
        \"\"\"Send to DLQ after max retries exceeded\"\"\"
        original_topic = self.get_original_topic(message.topic_name())
        dlq_topic = f"{original_topic}_dlq"
        
        try:
            dlq_producer = self.pulsar_client.create_producer(dlq_topic)
            dlq_producer.send(
                message.data(),
                properties={
                    **message.properties(),
                    'dlq_reason': reason,
                    'dlq_timestamp': datetime.utcnow().isoformat(),
                    'max_retries_exceeded': 'true',
                }
            )
            dlq_producer.close()
            logger.warning(f"Max retries exceeded, sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    async def process_message(self, consumer: Consumer, message: Message):
        \"\"\"Process retry message with delay\"\"\"
        try:
            props = message.properties()
            retry_count = int(props.get('retry_count', '1'))
            retry_timestamp = props.get('retry_timestamp', datetime.utcnow().isoformat())
            
            # Extract delay from topic name
            topic_name = message.topic_name()
            delay_str = topic_name.split('_retry_')[-1] if '_retry_' in topic_name else '5s'
            delay_seconds = self.get_delay_seconds(delay_str)
            
            # Wait for retry delay
            retry_time = datetime.fromisoformat(retry_timestamp)
            elapsed = (datetime.utcnow() - retry_time).total_seconds()
            
            if elapsed < delay_seconds:
                wait_time = delay_seconds - elapsed
                logger.info(f"Waiting {wait_time:.2f}s before retry (attempt {retry_count})")
                await asyncio.sleep(wait_time)
            
            # Check if max retries exceeded
            if retry_count >= self.max_retry_attempts:
                self.send_to_dlq(message, "Max retry attempts exceeded")
                consumer.acknowledge(message)
                return
            
            # Send back to original topic for reprocessing
            original_topic = self.get_original_topic(topic_name)
            
            try:
                retry_producer = self.pulsar_client.create_producer(original_topic)
                retry_producer.send(
                    message.data(),
                    properties={
                        **message.properties(),
                        'retry_count': str(retry_count),
                        'last_retry_timestamp': datetime.utcnow().isoformat(),
                    }
                )
                retry_producer.close()
                
                consumer.acknowledge(message)
                logger.info(f"Reprocessing message (attempt {retry_count}) on topic {original_topic}")
                
            except Exception as e:
                logger.error(f"Failed to send retry message: {e}")
                consumer.negative_acknowledge(message)
            
        except Exception as e:
            logger.error(f"Error processing retry message: {e}")
            consumer.negative_acknowledge(message)
    
    async def start(self, subscription_name: str = 'retry-processor'):
        \"\"\"Start retry processor service\"\"\"
        # Subscribe to all retry topics
        retry_pattern = 'persistent://.*/.*/_retry_.*'
        
        consumer = self.pulsar_client.subscribe(
            topics_pattern=retry_pattern,
            subscription_name=subscription_name,
            consumer_type=ConsumerType.Shared,  # Multiple processors can handle retries
        )
        
        logger.info(f"Started retry processor service")
        
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
    service = RetryProcessorService()
    asyncio.run(service.start())
"""

# Write DLQ handler
dlq_dir = f"{services_base}/dlq-handler/src"
Path(dlq_dir).mkdir(parents=True, exist_ok=True)
with open(f"{dlq_dir}/main.py", "w") as f:
    f.write(dlq_handler_code)

with open(f"{services_base}/dlq-handler/Dockerfile", "w") as f:
    f.write("""FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
ENV PYTHONUNBUFFERED=1
CMD ["python", "src/main.py"]
""")

with open(f"{services_base}/dlq-handler/requirements.txt", "w") as f:
    f.write("""pulsar-client==3.4.0
opensearch-py==2.4.2
""")

# Write retry processor
retry_dir = f"{services_base}/retry-processor/src"
Path(retry_dir).mkdir(parents=True, exist_ok=True)
with open(f"{retry_dir}/main.py", "w") as f:
    f.write(retry_processor_code)

with open(f"{services_base}/retry-processor/Dockerfile", "w") as f:
    f.write("""FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ ./src/
ENV PYTHONUNBUFFERED=1
CMD ["python", "src/main.py"]
""")

with open(f"{services_base}/retry-processor/requirements.txt", "w") as f:
    f.write("""pulsar-client==3.4.0
""")

print("✅ DLQ Handler Service Created")
print(f"   📂 Location: {services_base}/dlq-handler/")
print(f"   🔧 Features:")
print(f"      • Logs DLQ messages to OpenSearch")
print(f"      • Alerts on threshold exceeded")
print(f"      • Stores messages for manual review")
print(f"      • Pattern-based DLQ topic subscription")
print()

print("✅ Retry Processor Service Created")
print(f"   📂 Location: {services_base}/retry-processor/")
print(f"   🔧 Features:")
print(f"      • Exponential backoff retry delays (5s, 1m, 10m)")
print(f"      • Re-routes to original topic after delay")
print(f"      • Sends to DLQ after max retries")
print(f"      • Pattern-based retry topic subscription")
print()

dlq_retry_services = {
    "dlq_handler": f"{services_base}/dlq-handler/",
    "retry_processor": f"{services_base}/retry-processor/",
}
