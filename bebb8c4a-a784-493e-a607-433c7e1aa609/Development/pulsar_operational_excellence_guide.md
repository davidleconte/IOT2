# Apache Pulsar Operational Excellence Guide

## High Availability Architecture

### Multi-AZ BookKeeper Ensemble Placement

Pulsar's BookKeeper layer ensures data durability through **rack-aware ensemble placement** across 3 availability zones:

- **Ensemble Size (E)**: 5 bookies
- **Write Quorum (W)**: 5 bookies  
- **Ack Quorum (A)**: 3 bookies
- **Minimum Racks**: 3 AZs required for write quorum

This configuration guarantees:
- **Data survives** loss of entire availability zone
- **Writes continue** with 3/5 bookies available
- **No data loss** with zone-aware placement

### Automated Recovery

**BookKeeper Auto-Recovery**:
- Detects under-replicated ledgers every hour
- Replicates at 100 MB/s with 20 concurrent operations
- Audits bookie health every 24 hours

**Broker Failover**:
- Heartbeat checks every 5 seconds
- Failover triggered after 3 missed heartbeats (15s)
- Bundle migration completes within 10 seconds

**Topic Auto-Recovery**:
- Checks topic health every 60 seconds
- Retries up to 5 times with exponential backoff
- Automatically restores topic ownership

### Load Balancing Strategy

**Broker Load Balancing**:
- **Strategy**: Least-loaded server placement
- **Shedding**: Automatic bundle unload when broker exceeds 85% capacity
- **Bundle splitting**: Automatic when topics exceed threshold
- **Failure domains**: 3 domains (one per AZ) with 3 brokers each

**Load Metrics**:
- CPU, memory, network bandwidth
- Topic ownership count (max 50k per broker)
- Bundle throughput (min 10 MB/s threshold)

## Scale Specifications

### Throughput Targets

| Metric | Sustained | Burst |
|--------|-----------|-------|
| Messages/sec | 4,000,000 | 8,000,000 |
| Throughput | 4 GB/s | 8 GB/s |
| Avg Message Size | 1 KB | 1 KB |
| P99 Latency | < 50 ms | < 100 ms |

### Resource Allocation

**Broker Pods** (9 replicas, 3 per AZ):
- CPU: 8-16 cores
- Memory: 32-64 GB
- JVM Heap: 32 GB
- Direct Memory: 32 GB
- Managed Ledger Cache: 8 GB

**BookKeeper Pods** (9 replicas, 3 per AZ):
- CPU: 8-16 cores
- Memory: 16-32 GB
- JVM Heap: 16 GB
- Journal Storage: 100 GB (fast SSD, 10k IOPS)
- Ledger Storage: 2 TB (standard SSD, 5k IOPS)

**Proxy Pods** (6 replicas):
- CPU: 4-8 cores
- Memory: 8-16 GB

### Performance Tuning

**Broker Configuration**:
```yaml
numIOThreads: 16
numWorkerThreads: 32
maxConcurrentLookupRequest: 50000
dispatchThrottlingRatePerTopicInMsg: 100000
managedLedgerCacheSizeMB: 8192
```

**BookKeeper Configuration**:
```yaml
journalMaxGroupWaitMSec: 1
journalFlushWhenQueueEmpty: true
dbStorage_writeCacheMaxSizeMb: 2048
dbStorage_rocksDB_blockCacheSize: 2GB
numAddWorkerThreads: 16
```

**Producer Settings**:
```yaml
batchingEnabled: true
batchingMaxMessages: 1000
batchingMaxPublishDelayMicros: 10000  # 10ms
compressionType: LZ4
```

**Consumer Settings**:
```yaml
receiverQueueSize: 10000
acknowledgmentGroupTime: 100  # ms
```

## Rolling Upgrade Procedures

### Pre-Upgrade Checklist

✅ Verify cluster health (all brokers/bookies running)  
✅ Check replication lag < 1 second  
✅ Ensure all topics have minimum replicas  
✅ Backup ZooKeeper metadata  
✅ Verify BookKeeper ensemble health  

### Upgrade Order

1. **BookKeeper Nodes** (one rack/AZ at a time)
2. **Pulsar Brokers** (one failure domain at a time)
3. **Pulsar Proxies**
4. **Function Workers**

### BookKeeper Upgrade Steps

For each bookie:
1. Drain traffic from bookie (set read-only mode)
2. Wait for under-replicated ledgers to heal
3. Upgrade bookie to new version
4. Verify bookie rejoins cluster
5. **Wait 60 seconds** before next bookie

### Broker Upgrade Steps

For each broker:
1. Enable graceful shutdown
2. Trigger bundle unload to migrate traffic
3. Wait for bundles to migrate (max 5 minutes)
4. Upgrade broker to new version
5. Verify broker health and topic ownership
6. **Wait 30 seconds** before next broker

### Rollback Strategy

- **Health check threshold**: Auto-rollback after 3 failures
- **Previous version**: Preserved for quick rollback
- **Automatic rollback**: Enabled by default

## Geo-Replication

### Cluster Setup

**Active-Active Replication** across 3 regions:
- `us-east` (primary)
- `us-west` (secondary)
- `eu-central` (DR region)

### Replication Configuration

```yaml
replication_clusters: [us-east, us-west, eu-central]
message_ttl_seconds: 259200  # 3 days
replication_rate_limit: 100 MB/s per namespace
```

### Per-Namespace Policies

```bash
# Enable geo-replication for namespace
bin/pulsar-admin namespaces set-clusters tenant/namespace \
  --clusters us-east,us-west

# Set replication rate limit
bin/pulsar-admin namespaces set-replicator-dispatch-rate tenant/namespace \
  --msg-dispatch-rate 10000 \
  --byte-dispatch-rate 10485760
```

### Monitoring Replication

Key metrics:
- `pulsar_replication_backlog` - Messages pending replication
- `pulsar_replication_rate_in` - Incoming replication rate
- `pulsar_replication_rate_out` - Outgoing replication rate
- `pulsar_replication_delay_seconds` - Replication lag

## Tiered Storage Offload

### Storage Tiers

| Tier | Retention | Storage | Use Case |
|------|-----------|---------|----------|
| **Hot** | 7 days | BookKeeper | Real-time access |
| **Warm** | 7-90 days | S3 Standard | Recent analytics |
| **Cold** | 90+ days | S3 Glacier | Long-term archive |

### Offload Configuration

**Hot Tier** (BookKeeper):
- Retention: 7 days or 1 TB per namespace
- Storage: Fast SSD (journals) + Standard SSD (ledgers)

**Warm Tier** (S3 Standard):
- Triggered when namespace exceeds 1 TB
- Deletion lag: 4 hours (keeps data accessible during migration)
- Block size: 64 MB chunks

**Cold Tier** (S3 Glacier):
- Automatic transition after 90 days
- Glacier Instant Retrieval for quick access
- Deep Archive after 365 days for compliance

### Enable Offload Per Namespace

```bash
# Set offload threshold
bin/pulsar-admin namespaces set-offload-threshold tenant/namespace \
  --size 1T

# Set offload deletion lag
bin/pulsar-admin namespaces set-offload-deletion-lag tenant/namespace \
  --lag 4h

# Configure S3 offload
bin/pulsar-admin namespaces set-offload-policies tenant/namespace \
  --driver aws-s3 \
  --bucket pulsar-offload-warm \
  --region us-east-1 \
  --offloadAfterElapsed 7d
```

### Cost Optimization

**BookKeeper Storage**:
- Hot data: ~$0.10/GB/month (fast SSD)

**S3 Tiered Storage**:
- Warm (S3 Standard): ~$0.023/GB/month
- Cold (Glacier Instant): ~$0.004/GB/month
- Archive (Glacier Deep): ~$0.001/GB/month

**Example**: 10 TB data retention
- Days 0-7 (1 TB): $100/month (BookKeeper)
- Days 7-90 (9 TB): $207/month (S3 Standard)
- Days 90+ (archived): $40/month (Glacier)

**Total**: ~$347/month vs $1,000/month (all BookKeeper)

## Monitoring and Alerting

### Critical Metrics

**Broker Health**:
- `pulsar_broker_connection_count`
- `pulsar_broker_topics_count`
- `pulsar_broker_in_bytes_total`
- `pulsar_broker_out_bytes_total`

**BookKeeper Health**:
- `bookkeeper_server_ADD_ENTRY_REQUEST`
- `bookkeeper_server_READ_ENTRY_REQUEST`
- `bookkeeper_journal_JOURNAL_SYNC`
- `bookkeeper_server_ledger_writable_dirs`

**Replication Health**:
- `pulsar_replication_backlog`
- `pulsar_replication_delay_seconds`

### Alert Rules

```yaml
- alert: BrokerHighCPU
  expr: rate(process_cpu_seconds_total{job="pulsar-broker"}[5m]) > 0.85
  for: 5m

- alert: BookKeeperDiskFull
  expr: (node_filesystem_avail_bytes{mountpoint="/pulsar/data"} / node_filesystem_size_bytes) < 0.15
  for: 10m

- alert: ReplicationLagHigh
  expr: pulsar_replication_delay_seconds > 60
  for: 5m
```

## Operational Runbooks

### Scenario: Broker Failure

1. **Detect**: Monitor alerts or health check failures
2. **Verify**: Check broker logs and metrics
3. **Action**: Kubernetes automatically restarts pod
4. **Validate**: Ensure topic bundles redistributed to other brokers
5. **Timeline**: < 30 seconds for automatic recovery

### Scenario: BookKeeper Disk Full

1. **Detect**: Disk usage alert fires
2. **Immediate**: Trigger tiered storage offload
3. **Short-term**: Manually offload old ledgers to S3
4. **Long-term**: Increase ledger volume size
5. **Timeline**: 1-4 hours depending on data size

### Scenario: Replication Lag Spike

1. **Detect**: Replication delay > 60 seconds
2. **Check**: Network connectivity between regions
3. **Verify**: Replication rate limits not exceeded
4. **Action**: Increase replication rate if needed
5. **Timeline**: 5-15 minutes to catch up

---

## Reference Files

- **HA Configurations**: `ops/pulsar/ha-config/`
- **Benchmark Configs**: `ops/pulsar/benchmark/`
- **Upgrade Procedures**: `ops/pulsar/ha-config/rolling_upgrade_procedures.yaml`
- **Geo-Replication**: `ops/pulsar/ha-config/geo_replication_config.yaml`
- **Tiered Storage**: `ops/pulsar/ha-config/tiered_storage_offload.yaml`
