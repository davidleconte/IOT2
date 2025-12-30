import os
import json

# Create directories
os.makedirs("docs/runbook/scaling", exist_ok=True)
os.makedirs("docs/runbook/dlq-operations", exist_ok=True)

# Scaling Guidelines Runbook
scaling_guidelines = """# Scaling Guidelines: 4M → 8M msg/s

## Current Capacity
- Pulsar: 4M messages/second
- Cassandra: 50K writes/sec
- OpenSearch: 30K docs/sec indexing
- watsonx.data: 100K rows/sec ingestion

## Target Capacity
- Pulsar: 8M messages/second (2x)
- Cassandra: 100K writes/sec (2x)
- OpenSearch: 60K docs/sec (2x)
- watsonx.data: 200K rows/sec (2x)

## Pulsar Scaling (4M → 8M msg/s)

### Component Sizing
```yaml
# Current: 9 brokers, 6 BookKeeper, 3 ZooKeeper
# Target: 15 brokers, 10 BookKeeper, 5 ZooKeeper

brokers:
  replicas: 15  # was 9
  resources:
    requests: { cpu: "4", memory: "8Gi" }
    limits: { cpu: "8", memory: "16Gi" }

bookkeeper:
  replicas: 10  # was 6
  resources:
    requests: { cpu: "4", memory: "16Gi" }
    limits: { cpu: "8", memory: "32Gi" }
  storage: 500Gi  # per node, NVMe recommended

zookeeper:
  replicas: 5  # was 3 (always odd for quorum)
  resources:
    requests: { cpu: "2", memory: "4Gi" }
```

### Scaling Procedure
```bash
# Step 1: Scale ZooKeeper (if needed, requires downtime planning)
helm upgrade pulsar-operator ./helm/charts/pulsar-operator \\
  --set zookeeper.replicas=5 \\
  --reuse-values

# Wait for ZooKeeper quorum
kubectl wait --for=condition=Ready pods -l component=zookeeper -n pulsar --timeout=300s

# Step 2: Scale BookKeeper
helm upgrade pulsar-operator ./helm/charts/pulsar-operator \\
  --set bookkeeper.replicas=10 \\
  --reuse-values

# Wait and verify
kubectl get pods -n pulsar -l component=bookkeeper
bin/pulsar-admin bookies list

# Step 3: Scale Brokers
helm upgrade pulsar-operator ./helm/charts/pulsar-operator \\
  --set broker.replicas=15 \\
  --reuse-values

# Step 4: Rebalance load
bin/pulsar-admin brokers update-dynamic-config --config loadBalancerAutoBundleSplitEnabled --value true
bin/pulsar-admin namespaces split-bundle --bundle 0x00000000_0xffffffff persistent://shipping-co-alpha/vessel-tracking
```

## Cassandra Scaling (50K → 100K writes/sec)

### Component Sizing
```yaml
# Current: 9 nodes (3 per AZ)
# Target: 15 nodes (5 per AZ)

cassandra:
  replicas: 15  # was 9
  resources:
    requests: { cpu: "8", memory: "32Gi" }
    limits: { cpu: "16", memory: "64Gi" }
  storage: 1Ti  # per node, NVMe SSD required
```

### Scaling Procedure
```bash
# Step 1: Add nodes one AZ at a time
kubectl scale statefulset datastax-hcd --replicas=12 -n datastax  # +3 nodes (1 per AZ)

# Wait for bootstrap
kubectl exec -it datastax-hcd-0 -n datastax -- nodetool status

# Step 2: Run cleanup on existing nodes
for i in {0..8}; do
  kubectl exec -it datastax-hcd-$i -n datastax -- nodetool cleanup
done

# Step 3: Add remaining nodes
kubectl scale statefulset datastax-hcd --replicas=15 -n datastax

# Step 4: Balance tokens
kubectl exec -it datastax-hcd-0 -n datastax -- nodetool repair -full
```

## OpenSearch Scaling (30K → 60K docs/sec)

### Component Sizing
```yaml
# Current: 6 data nodes + 3 masters
# Target: 12 data nodes + 3 masters (masters unchanged)

opensearch-data:
  replicas: 12  # was 6
  resources:
    requests: { cpu: "4", memory: "16Gi" }
    limits: { cpu: "8", memory: "32Gi" }
  storage: 500Gi  # per node
```

### Scaling Procedure
```bash
# Step 1: Scale data nodes
kubectl scale statefulset opensearch-data --replicas=12 -n opensearch

# Step 2: Allow shard rebalancing
curl -X PUT "https://opensearch:9200/_cluster/settings" -H 'Content-Type: application/json' -d '{
  "transient": {
    "cluster.routing.allocation.cluster_concurrent_rebalance": "4"
  }
}'

# Step 3: Monitor rebalancing
watch -n 5 'curl -s "https://opensearch:9200/_cat/shards?v" | grep RELOCATING | wc -l'
```

## watsonx.data Scaling (100K → 200K rows/sec)

### Component Sizing
```yaml
# Presto workers and Spark executors
presto-worker:
  replicas: 20  # was 10
  resources:
    requests: { cpu: "8", memory: "32Gi" }

spark:
  dynamicAllocation:
    enabled: true
    minExecutors: 20  # was 10
    maxExecutors: 100  # was 50
```

### Scaling Procedure
```bash
# Scale Presto workers
kubectl scale deployment presto-worker --replicas=20 -n watsonx

# Update Spark config for dynamic allocation
kubectl edit configmap spark-defaults -n watsonx
# Set spark.dynamicAllocation.minExecutors=20
# Set spark.dynamicAllocation.maxExecutors=100
```

## Monitoring Post-Scale

### Pulsar Metrics
```bash
# Message rate per broker
bin/pulsar-admin broker-stats topics | jq '.[] | {msgRateIn, msgRateOut}'

# BookKeeper throughput
bin/pulsar-admin bookies list-bookies
```

### Cassandra Metrics
```bash
kubectl exec -it datastax-hcd-0 -n datastax -- nodetool tablestats | grep "Write Latency"
kubectl exec -it datastax-hcd-0 -n datastax -- nodetool tpstats
```

### OpenSearch Metrics
```bash
curl "https://opensearch:9200/_cat/indices?v&h=index,indexing.index_total,indexing.index_time"
curl "https://opensearch:9200/_nodes/stats/indices/indexing"
```

## Cost Impact
- Infrastructure cost increase: ~80% (less than 2x due to efficiency gains)
- Monthly: $45K → $81K (estimated)
- Per-tenant: $3K → $5.4K for typical workload

## Rollback Plan
If scaling causes issues:
1. Scale down gradually (reverse order)
2. Monitor for stability at each step
3. Rebalance shards/tokens after scale-down
"""

scaling_path = "docs/runbook/scaling/01_scaling_4m_to_8m_msgs.md"
with open(scaling_path, 'w') as f:
    f.write(scaling_guidelines)

print(f"✓ Created scaling guidelines: {scaling_path}")

# DLQ Operations Runbook
dlq_operations = """# DLQ/Retry/Quarantine Operations

## Overview
Procedures for managing Dead Letter Queues, retry logic, and quarantine queues.

## Architecture
- Retry levels: 5s, 1m, 10m (exponential backoff)
- DLQ: Final destination for unprocessable messages
- Quarantine: Data quality validation failures

## DLQ Message Inspection

### List Messages in DLQ
```bash
# Connect to Pulsar
TENANT="shipping-co-alpha"
NAMESPACE="vessel-tracking"
TOPIC="telemetry"

# Check DLQ stats
bin/pulsar-admin topics stats persistent://${TENANT}/${NAMESPACE}/${TOPIC}-DLQ

# Peek at DLQ messages
bin/pulsar-admin topics peek-messages persistent://${TENANT}/${NAMESPACE}/${TOPIC}-DLQ \\
  --count 10 --subscription dlq-inspection
```

### Analyze Failure Patterns
```bash
# Export DLQ messages for analysis
bin/pulsar-client consume persistent://${TENANT}/${NAMESPACE}/${TOPIC}-DLQ \\
  --subscription analysis-$(date +%Y%m%d) \\
  --num-messages 1000 > /tmp/dlq_messages.json

# Parse failure reasons
cat /tmp/dlq_messages.json | jq -r '.properties.exception_class' | sort | uniq -c
```

## Replay from DLQ

### Prerequisites
1. Root cause fixed (schema update, code fix, etc.)
2. Target system healthy
3. Approval from team lead

### Replay Procedure
```bash
# Step 1: Create replay subscription
bin/pulsar-admin topics create-subscription \\
  persistent://${TENANT}/${NAMESPACE}/${TOPIC}-DLQ \\
  --subscription replay-$(date +%Y%m%d%H%M)

# Step 2: Start replay consumer
# Option A: Direct replay to original topic
python ops/pulsar/replay_dlq.py \\
  --source-topic persistent://${TENANT}/${NAMESPACE}/${TOPIC}-DLQ \\
  --target-topic persistent://${TENANT}/${NAMESPACE}/${TOPIC} \\
  --subscription replay-$(date +%Y%m%d%H%M) \\
  --rate-limit 100  # messages per second

# Option B: Manual inspection and selective replay
bin/pulsar-client consume persistent://${TENANT}/${NAMESPACE}/${TOPIC}-DLQ \\
  --subscription manual-replay \\
  --listener-name manual-processor
```

### Replay Script
```python
# ops/pulsar/replay_dlq.py
import pulsar
import time
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--source-topic', required=True)
parser.add_argument('--target-topic', required=True)
parser.add_argument('--subscription', required=True)
parser.add_argument('--rate-limit', type=int, default=100)
args = parser.parse_args()

client = pulsar.Client('pulsar://pulsar-broker:6650')
consumer = client.subscribe(args.source_topic, args.subscription)
producer = client.create_producer(args.target_topic)

replayed = 0
rate_limiter = time.time()

while True:
    msg = consumer.receive(timeout_millis=5000)
    if msg is None:
        break
    
    # Replay to target topic
    producer.send(msg.data(), properties=msg.properties())
    consumer.acknowledge(msg)
    replayed += 1
    
    # Rate limiting
    if replayed % args.rate_limit == 0:
        elapsed = time.time() - rate_limiter
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
        rate_limiter = time.time()
    
    if replayed % 100 == 0:
        print(f"Replayed {replayed} messages")

print(f"Replay complete: {replayed} messages")
client.close()
```

## Retry Queue Management

### Monitor Retry Queues
```bash
# Check all retry levels
for retry in 5s 1m 10m; do
  echo "=== Retry $retry ==="
  bin/pulsar-admin topics stats persistent://${TENANT}/${NAMESPACE}/${TOPIC}-retry-${retry}
done
```

### Adjust Retry Delays
```bash
# Update retry configuration
kubectl edit configmap retry-processor-config -n streaming

# Restart retry processor
kubectl rollout restart deployment retry-processor -n streaming
```

## Quarantine Queue Operations

### Quarantine Inspection
```bash
# List quarantined messages (data quality failures)
curl -X GET "https://opensearch:9200/dq_quarantine_queue/_search?pretty" \\
  -H 'Content-Type: application/json' \\
  -d '{
    "query": {"match_all": {}},
    "size": 100,
    "sort": [{"@timestamp": "desc"}]
  }'
```

### Quarantine Resolution

**Option 1: Fix and Reprocess**
```bash
# Export quarantined data
elasticdump \\
  --input=https://opensearch:9200/dq_quarantine_queue \\
  --output=/tmp/quarantine_export.json \\
  --type=data

# Fix data offline (edit /tmp/quarantine_export.json)

# Resubmit to Pulsar
python ops/data-quality/resubmit_quarantine.py \\
  --input /tmp/quarantine_export.json \\
  --target persistent://${TENANT}/${NAMESPACE}/${TOPIC}
```

**Option 2: Discard Invalid Data**
```bash
# Mark as reviewed and discard
curl -X POST "https://opensearch:9200/dq_quarantine_queue/_update_by_query" \\
  -H 'Content-Type: application/json' \\
  -d '{
    "script": {
      "source": "ctx._source.status = \"discarded\"; ctx._source.reviewed_by = \"sre-team\"; ctx._source.reviewed_at = params.now",
      "params": {"now": "'$(date -u +%Y-%m-%dT%H:%M:%SZ)'"}
    },
    "query": {
      "terms": {"_id": ["id1", "id2", "id3"]}
    }
  }'
```

## Alert Thresholds
- DLQ growth rate > 100 msg/min → P2 alert
- Quarantine queue > 10,000 messages → P3 alert
- Retry queue saturation (> 1M messages) → P1 alert

## Scheduled Maintenance
- Weekly DLQ review: Every Monday 10:00 UTC
- Monthly quarantine cleanup: First of month
- Quarterly retry policy review

## Common Failure Patterns

### Schema Evolution Issues
- Root cause: Producer using old schema
- Resolution: Update producer, replay DLQ

### Transient Network Errors
- Root cause: Temporary connectivity loss
- Resolution: Verify retry mechanism working, no manual action needed

### Data Quality Violations
- Root cause: Invalid data from source system
- Resolution: Fix source system, update DQ schemas if needed, reprocess quarantine

## SLO Targets
- DLQ message age: < 24 hours before review
- Quarantine resolution time: < 72 hours
- Replay success rate: > 95%
"""

dlq_path = "docs/runbook/dlq-operations/01_dlq_retry_quarantine_ops.md"
with open(dlq_path, 'w') as f:
    f.write(dlq_operations)

print(f"✓ Created DLQ operations runbook: {dlq_path}")

scaling_dlq_summary = {
    "created_runbooks": [scaling_path, dlq_path],
    "topics": ["scaling_4m_to_8m", "dlq_operations", "retry_logic", "quarantine_management"],
    "next": "replay_reprocessing_runbook"
}

print(json.dumps(scaling_dlq_summary, indent=2))
