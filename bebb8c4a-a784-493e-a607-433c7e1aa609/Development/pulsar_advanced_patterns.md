# 🚀 Apache Pulsar - Advanced Patterns & Configuration

## Dead Letter Queue (DLQ) Pattern

### Purpose
Handle messages that fail processing after multiple retry attempts without blocking the main topic.

### Configuration
```yaml
# ops/pulsar/topics/vessel_telemetry_topic.yaml
subscription:
  name: vessel-processor
  type: Key_Shared
  
deadLetterPolicy:
  maxRedeliverCount: 3
  deadLetterTopic: persistent://tenant-{id}/maritime/vessel_telemetry_dlq
  initialSubscriptionName: dlq-processor

# DLQ Processing Strategy
dlqProcessor:
  enabled: true
  actions:
    - log_to_opensearch
    - alert_on_threshold
    - manual_review_after_24h
```

### Implementation
```python
# ops/pulsar/functions/dlq_handler.py
from pulsar import Consumer, Message

def process_dlq_message(message: Message):
    """Process failed messages from DLQ"""
    error_metadata = message.properties()
    original_topic = error_metadata.get('ORIGIN_TOPIC')
    failure_count = error_metadata.get('RECONSUMETIMES')
    
    # Log to OpenSearch for monitoring
    log_failure(message, failure_count)
    
    # Alert if threshold exceeded
    if failure_count >= 3:
        send_alert(f"Critical: Message failed {failure_count} times")
    
    # Store for manual review
    store_for_review(message)
```

---

## Retry Policies

### Exponential Backoff Strategy
```yaml
# ops/pulsar/namespaces/namespace_config.yaml
retryPolicy:
  type: exponential_backoff
  initialInterval: 1s
  multiplier: 2.0
  maxInterval: 60s
  maxRetries: 5

# Example progression: 1s → 2s → 4s → 8s → 16s → 32s
```

### Tenant-Specific Retry Configuration
```yaml
tenants:
  premium:
    retryPolicy:
      maxRetries: 10
      initialInterval: 500ms
  standard:
    retryPolicy:
      maxRetries: 5
      initialInterval: 2s
```

### Circuit Breaker Integration
```python
# Prevent cascading failures
class CircuitBreakerRetryPolicy:
    def __init__(self, failure_threshold=5, timeout=60):
        self.failure_count = 0
        self.failure_threshold = failure_threshold
        self.circuit_open = False
        
    def should_retry(self, error):
        if self.circuit_open:
            return False  # Fast fail
        
        self.failure_count += 1
        if self.failure_count >= self.failure_threshold:
            self.circuit_open = True
            schedule_reset(timeout)
            return False
        
        return True
```

---

## Message Deduplication

### Enable Broker-Level Deduplication
```yaml
# ops/pulsar/namespaces/namespace_config.yaml
namespace: tenant-{id}/maritime

deduplication:
  enabled: true
  # Deduplication window
  deduplicationSnapshotIntervalSeconds: 120
  
# Producer must set message ID
producerConfig:
  enableIdempotence: true
  sequenceIdGenerator: monotonic
```

### Producer Implementation
```python
# Idempotent producer with sequence IDs
from pulsar import Client, MessageId

client = Client('pulsar://pulsar-broker:6650')
producer = client.create_producer(
    topic='persistent://tenant-a/maritime/vessel_telemetry',
    producer_name='vessel-sensor-001',  # Stable producer name
    enable_batching=False,  # For strict ordering
)

# Send with unique sequence ID
message_id = f"{vessel_id}_{timestamp}_{sensor_type}"
producer.send(
    content=telemetry_data,
    properties={
        'sequence_id': message_id,
        'tenant_id': 'tenant-a',
        'vessel_id': vessel_id,
    },
    event_timestamp=int(time.time() * 1000)
)
```

### Consumer-Side Deduplication
```python
# For exactly-once semantics
class DeduplicatingConsumer:
    def __init__(self, redis_client):
        self.seen_messages = redis_client
        self.ttl = 3600  # 1 hour
        
    def process_message(self, message):
        msg_id = message.properties().get('sequence_id')
        
        # Check if already processed
        if self.seen_messages.exists(msg_id):
            message.acknowledge()
            return  # Skip duplicate
        
        # Process message
        result = process_telemetry(message.data())
        
        # Mark as processed
        self.seen_messages.setex(msg_id, self.ttl, '1')
        message.acknowledge()
```

---

## Key_Shared Subscription for Ordered Processing

### Why Key_Shared?
- **Problem**: Shared subscription = parallel processing but NO ordering guarantee
- **Solution**: Key_Shared = parallel processing WITH per-key ordering

### Configuration
```yaml
# ops/pulsar/topics/vessel_telemetry_topic.yaml
subscription:
  name: vessel-analytics
  type: Key_Shared
  
  keySharedPolicy:
    # Sticky: Same key always goes to same consumer
    mode: STICKY
    # Allow range-based key distribution
    allowOutOfOrderDelivery: false
```

### Producer Key Assignment
```python
# Partition by vessel_id for ordering
producer.send(
    content=telemetry_data,
    partition_key=vessel_id,  # Critical for key_shared
    properties={
        'tenant_id': tenant_id,
        'vessel_id': vessel_id,
    }
)

# All messages with same vessel_id go to same consumer
# Ordering guaranteed per vessel, parallel across vessels
```

### Consumer Implementation
```python
consumer = client.subscribe(
    topic='persistent://tenant-a/maritime/vessel_telemetry',
    subscription_name='vessel-analytics',
    subscription_type=pulsar.ConsumerType.KeyShared,
    
    # Configure key-shared behavior
    consumer_name=f'analytics-worker-{worker_id}',
    
    # Each consumer gets subset of keys
    receiver_queue_size=1000,
)

# Process messages - ordering preserved per key
while True:
    message = consumer.receive()
    vessel_id = message.properties().get('vessel_id')
    
    # All messages for vessel-123 processed in order
    process_vessel_telemetry(vessel_id, message.data())
    
    consumer.acknowledge(message)
```

---

## Multi-Tenant Topic Strategy

### Hierarchical Topic Structure
```
persistent://[tenant]/[namespace]/[topic]

Examples:
- persistent://tenant-a/maritime/vessel_telemetry
- persistent://tenant-a/maritime/voyage_events
- persistent://tenant-a/maritime/maintenance_alerts

- persistent://tenant-b/maritime/vessel_telemetry
- persistent://tenant-b/maritime/voyage_events
```

### Tenant Isolation Policies
```yaml
# ops/pulsar/tenants/tenant_policies.yaml
tenants:
  - name: tenant-a
    admin_roles: ['tenant-a-admin']
    allowed_clusters: ['pulsar-aws-us-east', 'pulsar-aws-eu-west']
    
    quotas:
      # Prevent noisy neighbor
      publishRateLimit: 1000  # msg/sec
      dispatchRateLimit: 5000  # msg/sec
      storageQuota: 1TB
      
  - name: tenant-b
    admin_roles: ['tenant-b-admin']
    allowed_clusters: ['pulsar-aws-us-east']
    
    quotas:
      publishRateLimit: 500
      dispatchRateLimit: 2000
      storageQuota: 500GB
```

---

## Monitoring & Observability

### Key Metrics
```yaml
# ops/monitoring/prometheus/rules.yaml
groups:
  - name: pulsar_tenant_monitoring
    rules:
      - alert: HighDLQRate
        expr: rate(pulsar_dlq_messages_total[5m]) > 10
        annotations:
          summary: "High DLQ rate for tenant {{ $labels.tenant }}"
          
      - alert: ConsumerLag
        expr: pulsar_subscription_back_log > 10000
        annotations:
          summary: "Consumer lag exceeds threshold"
          
      - alert: DeduplicationFailures
        expr: rate(pulsar_deduplication_failures[5m]) > 1
```

### Dashboard Metrics
- **DLQ Rate**: Messages/sec entering DLQ per tenant
- **Retry Distribution**: Histogram of retry attempts
- **Deduplication Hit Rate**: % of duplicate messages caught
- **Key Distribution**: Balance across consumers in Key_Shared
- **Throughput per Tenant**: Publish/consume rates with quotas