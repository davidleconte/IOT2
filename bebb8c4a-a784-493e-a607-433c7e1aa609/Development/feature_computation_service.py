import os
from pathlib import Path

# 2. Real-time Feature Computation Service
feature_service_code = """
# services/stream-processing/feature-computation/src/main.py
import asyncio
import json
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pulsar import Client, Consumer, Message, ConsumerType
from cassandra.cluster import Cluster, Session
from collections import deque
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class StreamingAggregator:
    \"\"\"Windowed aggregations for vessel telemetry features\"\"\"
    def __init__(self, window_minutes: int = 5):
        self.window = timedelta(minutes=window_minutes)
        self.vessel_windows = {}  # vessel_id -> deque of (timestamp, data)
    
    def add_datapoint(self, vessel_id: str, timestamp: datetime, data: Dict[str, float]):
        \"\"\"Add data point to vessel's window\"\"\"
        if vessel_id not in self.vessel_windows:
            self.vessel_windows[vessel_id] = deque()
        
        window = self.vessel_windows[vessel_id]
        window.append((timestamp, data))
        
        # Remove old data outside window
        cutoff_time = timestamp - self.window
        while window and window[0][0] < cutoff_time:
            window.popleft()
    
    def compute_features(self, vessel_id: str) -> Optional[Dict[str, float]]:
        \"\"\"Compute aggregated features for vessel\"\"\"
        if vessel_id not in self.vessel_windows or not self.vessel_windows[vessel_id]:
            return None
        
        window = self.vessel_windows[vessel_id]
        speeds = [data['speed'] for _, data in window]
        courses = [data['course'] for _, data in window]
        
        features = {
            'avg_speed': statistics.mean(speeds),
            'max_speed': max(speeds),
            'min_speed': min(speeds),
            'std_speed': statistics.stdev(speeds) if len(speeds) > 1 else 0.0,
            'course_changes': sum(1 for i in range(1, len(courses)) if abs(courses[i] - courses[i-1]) > 10),
            'data_points': len(speeds),
        }
        
        return features

class FeatureComputationService:
    def __init__(
        self,
        pulsar_url: str = 'pulsar://pulsar-broker:6650',
        cassandra_hosts: List[str] = None,
    ):
        self.pulsar_client = Client(pulsar_url)
        self.cassandra_hosts = cassandra_hosts or ['cassandra-0', 'cassandra-1', 'cassandra-2']
        self.cassandra_session: Optional[Session] = None
        self.aggregator = StreamingAggregator(window_minutes=5)
        
    def connect_cassandra(self, keyspace: str = 'feast_features'):
        cluster = Cluster(self.cassandra_hosts)
        self.cassandra_session = cluster.connect(keyspace)
        logger.info(f"Connected to Cassandra keyspace: {keyspace}")
    
    def send_to_dlq(self, message: Message, error_reason: str):
        \"\"\"Send failed message to DLQ\"\"\"
        original_topic = message.topic_name()
        dlq_topic = f"{original_topic}_dlq"
        
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
            logger.warning(f"Message sent to DLQ: {dlq_topic}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")
    
    def send_to_retry(self, message: Message, retry_count: int):
        \"\"\"Send message to retry topic\"\"\"
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
                }
            )
            retry_producer.close()
        except Exception as e:
            logger.error(f"Failed to send to retry: {e}")
    
    def write_features_to_feast(self, vessel_id: str, tenant_id: str, features: Dict[str, float], timestamp: datetime):
        \"\"\"Write computed features to Feast online store (Cassandra)\"\"\"
        if not self.cassandra_session:
            return False
        
        try:
            query = \"\"\"
            INSERT INTO vessel_operational_features 
            (vessel_id, tenant_id, feature_timestamp, avg_speed, max_speed, min_speed, 
             std_speed, course_changes, data_points)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            \"\"\"
            self.cassandra_session.execute(
                query,
                (
                    vessel_id,
                    tenant_id,
                    timestamp,
                    features['avg_speed'],
                    features['max_speed'],
                    features['min_speed'],
                    features['std_speed'],
                    features['course_changes'],
                    features['data_points'],
                )
            )
            return True
        except Exception as e:
            logger.error(f"Failed to write features to Feast: {e}")
            return False
    
    def forward_to_anomaly_detection(self, vessel_id: str, tenant_id: str, features: Dict[str, float]):
        \"\"\"Forward features to anomaly detection service via Pulsar\"\"\"
        anomaly_topic = f"persistent://{tenant_id}/analytics/anomaly_detection"
        
        try:
            producer = self.pulsar_client.create_producer(anomaly_topic)
            payload = {
                'vessel_id': vessel_id,
                'tenant_id': tenant_id,
                'features': features,
                'timestamp': datetime.utcnow().isoformat(),
            }
            producer.send(
                json.dumps(payload).encode('utf-8'),
                partition_key=vessel_id,
            )
            producer.close()
            logger.debug(f"Forwarded features to anomaly detection for {vessel_id}")
        except Exception as e:
            logger.error(f"Failed to forward to anomaly detection: {e}")
    
    async def process_message(self, consumer: Consumer, message: Message):
        \"\"\"Process telemetry and compute streaming features\"\"\"
        try:
            data = json.loads(message.data().decode('utf-8'))
            vessel_id = data['vessel_id']
            tenant_id = data['tenant_id']
            timestamp = datetime.fromisoformat(data['timestamp'])
            retry_count = int(message.properties().get('retry_count', '0'))
            
            # Add to streaming window
            telemetry_data = {
                'speed': data['speed'],
                'course': data['course'],
                'latitude': data['latitude'],
                'longitude': data['longitude'],
            }
            self.aggregator.add_datapoint(vessel_id, timestamp, telemetry_data)
            
            # Compute features
            features = self.aggregator.compute_features(vessel_id)
            
            if not features:
                consumer.acknowledge(message)
                return
            
            # Write to Feast online store
            write_success = self.write_features_to_feast(vessel_id, tenant_id, features, timestamp)
            
            if not write_success:
                if retry_count < 3:
                    self.send_to_retry(message, retry_count + 1)
                else:
                    self.send_to_dlq(message, "Failed to write features after retries")
                consumer.acknowledge(message)
                return
            
            # Forward to anomaly detection
            self.forward_to_anomaly_detection(vessel_id, tenant_id, features)
            
            consumer.acknowledge(message)
            logger.info(f"Computed features for vessel {vessel_id}: {features}")
            
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            consumer.negative_acknowledge(message)
    
    async def start(self, subscription_name: str = 'feature-computation'):
        \"\"\"Start feature computation service\"\"\"
        self.connect_cassandra()
        
        # Subscribe to tenant-specific raw telemetry topics
        topics = [
            'persistent://shipping-co-alpha/vessel-tracking/raw_telemetry',
            'persistent://logistics-beta/vessel-tracking/raw_telemetry',
            'persistent://maritime-gamma/vessel-tracking/raw_telemetry',
        ]
        
        consumer = self.pulsar_client.subscribe(
            topics,
            subscription_name=subscription_name,
            consumer_type=ConsumerType.KeyShared,  # Per-vessel ordering
            receiver_queue_size=1000,
        )
        
        logger.info(f"Started feature computation service with Key_Shared subscription")
        
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
    service = FeatureComputationService()
    asyncio.run(service.start())
"""

# Write feature computation service
feature_dir = f"{services_base}/feature-computation/src"
Path(feature_dir).mkdir(parents=True, exist_ok=True)
with open(f"{feature_dir}/main.py", "w") as f:
    f.write(feature_service_code)

# Dockerfile
feature_dockerfile = """FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

ENV PYTHONUNBUFFERED=1

CMD ["python", "src/main.py"]
"""

with open(f"{services_base}/feature-computation/Dockerfile", "w") as f:
    f.write(feature_dockerfile)

# Requirements
feature_requirements = """pulsar-client==3.4.0
cassandra-driver==3.29.0
"""

with open(f"{services_base}/feature-computation/requirements.txt", "w") as f:
    f.write(feature_requirements)

print("✅ Feature Computation Service Created")
print(f"   📂 Location: {services_base}/feature-computation/")
print(f"   📄 Files: src/main.py, Dockerfile, requirements.txt")
print(f"   🔧 Features:")
print(f"      • Streaming windowed aggregations (5-minute windows)")
print(f"      • Computes: avg_speed, max_speed, min_speed, std_speed, course_changes")
print(f"      • Writes features to Feast online store (Cassandra)")
print(f"      • Forwards to anomaly detection service")
print(f"      • DLQ/retry handling")
print(f"      • Key_Shared subscription for per-vessel ordering")
print()

feature_service_created = {
    "service": "feature-computation",
    "path": f"{services_base}/feature-computation/",
    "features": ["streaming_aggregations", "feast_writer", "anomaly_forwarding", "dlq_retry"]
}
