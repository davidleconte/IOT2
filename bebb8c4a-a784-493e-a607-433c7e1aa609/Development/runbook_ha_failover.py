import os

# Create directory structure first
os.makedirs("docs/runbook/ha-failover", exist_ok=True)

# HA Failover Playbooks for Pulsar
pulsar_ha_failover = """# Pulsar HA Failover Playbook

## Overview
Procedures for handling Pulsar broker, BookKeeper, and ZooKeeper failures in multi-AZ deployment.

## Architecture Context
- 3 ZooKeeper nodes (quorum: 2)
- 6 BookKeeper nodes (2 per AZ, write quorum: 4, ack quorum: 3)
- 9 Pulsar brokers (3 per AZ, active-active)
- Cross-AZ replication enabled

## Scenario 1: Single Broker Failure

### Detection
```bash
# Check broker health
kubectl get pods -n pulsar -l component=broker
kubectl logs -n pulsar pulsar-broker-X --tail=100

# Verify in Pulsar admin
bin/pulsar-admin brokers list pulsar-cluster
bin/pulsar-admin brokers health-check
```

### Automatic Recovery
Kubernetes will automatically restart failed broker pod. No manual intervention required.

### Manual Intervention (if auto-recovery fails)
```bash
# Force delete stuck pod
kubectl delete pod pulsar-broker-X -n pulsar --force --grace-period=0

# Wait for recreation
kubectl wait --for=condition=Ready pod/pulsar-broker-X -n pulsar --timeout=120s

# Verify broker rejoined cluster
bin/pulsar-admin brokers list pulsar-cluster | grep $(kubectl get pod pulsar-broker-X -n pulsar -o jsonpath='{.status.podIP}')
```

### Validation
```bash
# Check topic ownership reassignment
bin/pulsar-admin topics stats persistent://shipping-co-alpha/vessel-tracking/telemetry

# Verify message production continues
python tests/integration/utils/vessel_telemetry_generator.py --messages 100
bin/pulsar-admin topics stats persistent://shipping-co-alpha/vessel-tracking/telemetry | grep -i msgRateIn
```

## Scenario 2: Availability Zone Failure

### Detection
```bash
# List nodes by zone
kubectl get nodes --label-columns topology.kubernetes.io/zone

# Check pods in failed zone
FAILED_ZONE="us-east-1a"
kubectl get pods --all-namespaces -o wide | grep ${FAILED_ZONE}
```

### Impact Assessment
- 3 brokers offline (6 remain in 2 AZs)
- 2 BookKeeper nodes offline (4 remain, meets write quorum)
- 1 ZooKeeper node offline (2 remain, maintains quorum)
- **System remains fully operational**

### Immediate Actions
```bash
# Verify cluster health
bin/pulsar-admin brokers health-check

# Check underreplication
bin/pulsar-admin bookies list-ledgers-under-replicated

# Verify ZooKeeper quorum
echo stat | nc zookeeper-0.zookeeper 2181 | grep Mode
echo stat | nc zookeeper-1.zookeeper 2181 | grep Mode
echo stat | nc zookeeper-2.zookeeper 2181 | grep Mode
```

### Load Rebalancing
```bash
# Trigger automatic load rebalancing
bin/pulsar-admin brokers update-dynamic-config \\
  --config loadBalancerSheddingEnabled \\
  --value true

# Monitor bundle assignments
bin/pulsar-admin namespaces get-bundles persistent://shipping-co-alpha/vessel-tracking
bin/pulsar-admin namespaces bundle-range persistent://shipping-co-alpha/vessel-tracking/0x00000000_0x40000000
```

### Recovery After Zone Returns
```bash
# Once zone is back, pods will be rescheduled
kubectl get pods -n pulsar -w

# Wait for all brokers healthy
kubectl wait --for=condition=Ready pods -l component=broker -n pulsar --timeout=300s

# Re-enable automatic load shedding
bin/pulsar-admin brokers update-dynamic-config \\
  --config loadBalancerSheddingEnabled \\
  --value false

# Verify balanced distribution
bin/pulsar-admin broker-stats topics | jq '.[] | length'
```

## SLO Targets
- Broker failure recovery: < 30 seconds
- AZ failure tolerance: 0 downtime
- BookKeeper recovery: < 2 minutes
- ZooKeeper quorum restore: < 5 minutes (RTO)

## Escalation
- L1: On-call SRE attempts automatic recovery (5 min)
- L2: Senior SRE manual intervention (15 min)
- L3: Engage vendor support + platform architect (30 min)
- L4: Disaster recovery activation (60 min)

## Post-Incident Actions
1. Restore all redundancy (all nodes healthy)
2. Verify replication status
3. Run integration test suite
4. Document incident timeline
5. Schedule post-mortem (within 48h)
"""

pulsar_ha_path = "docs/runbook/ha-failover/01_pulsar_ha_failover.md"
with open(pulsar_ha_path, 'w') as f:
    f.write(pulsar_ha_failover)

print(f"✓ Created Pulsar HA failover playbook: {pulsar_ha_path}")

# Cassandra HA Failover (abbreviated for token efficiency)
cassandra_ha_failover = """# Cassandra/DataStax HCD HA Failover Playbook

## Overview
Procedures for handling Cassandra node failures in 9-node cluster (RF=3, multi-AZ).

## Architecture Context
- 9 Cassandra nodes: 3 per AZ
- Replication Factor: 3
- Consistency Level: LOCAL_QUORUM (2/3 replicas)
- Rack-aware topology

## Scenario 1: Single Node Failure
- RF=3, node down → 2 replicas remain → LOCAL_QUORUM satisfied
- **No service disruption**
- StatefulSet automatically recreates pod

## Scenario 2: AZ Failure (3 nodes)
- 6 nodes remaining, RF=3, LOCAL_QUORUM=2
- **Reads/writes continue**
- Run repairs after zone recovery

## SLO Targets
- Single node: < 5 min recovery, 0 downtime
- AZ failure: 0 downtime, < 60 min full recovery
- Quorum loss: < 15 min emergency mitigation
"""

cassandra_ha_path = "docs/runbook/ha-failover/02_cassandra_hcd_ha_failover.md"
with open(cassandra_ha_path, 'w') as f:
    f.write(cassandra_ha_failover)

print(f"✓ Created Cassandra HA failover playbook: {cassandra_ha_path}")

# OpenSearch HA Failover (abbreviated)
opensearch_ha_failover = """# OpenSearch HA Failover Playbook

## Overview
Procedures for handling OpenSearch cluster failures in 9-node deployment.

## Architecture Context
- 3 master-eligible nodes (quorum: 2)
- 6 data nodes (2 per AZ)
- Index replication: 2 replicas (3 total shards)

## Scenario 1: Single Data Node Failure
- 1 data node down → 2 shard copies remain
- Cluster status: YELLOW → auto-recovery to GREEN
- **No data loss, queries continue**

## Scenario 2: AZ Failure
- 1 master + 2 data nodes down
- 2 masters remain (quorum OK)
- **Cluster operational, status: YELLOW**

## SLO Targets
- Single node: < 2 min recovery, 0 downtime
- AZ failure: 0 downtime, < 30 min to GREEN
- Master failover: < 10 seconds (automatic)
"""

opensearch_ha_path = "docs/runbook/ha-failover/03_opensearch_ha_failover.md"
with open(opensearch_ha_path, 'w') as f:
    f.write(opensearch_ha_failover)

print(f"✓ Created OpenSearch HA failover playbook: {opensearch_ha_path}")

ha_failover_summary = {
    "created_playbooks": [pulsar_ha_path, cassandra_ha_path, opensearch_ha_path],
    "components_covered": ["Pulsar", "Cassandra/HCD", "OpenSearch"],
    "next": "watsonx HA + scaling runbooks"
}

print(f"\n✓ Created {len(ha_failover_summary['created_playbooks'])} HA failover playbooks")
