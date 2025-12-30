import os

# Generate comprehensive testing guide covering generators, DQ, anomaly detection, and ML
testing_guide = """
# Step 3: Generator Testing & Data Quality Validation

## 🎯 Objective
Execute smoke tests, load tests, and stress tests using data generators. Validate DQ framework and anomaly detection.

## 📂 Location
- **Generators**: `tests/integration/utils/`
- **DQ Framework**: `services/data-quality/`
- **Tests**: `tests/integration/`

## 🏗️ Testing Sequence

### Phase 1: Smoke Testing with Generators (10 minutes)

#### 1.1 Configure Test Environment
```bash
cd tests/integration

# Review test configuration
cat test_config.json

# Key settings:
# - pulsar_service_url: Pulsar broker endpoint
# - cassandra_contact_points: Cassandra nodes
# - opensearch_url: OpenSearch cluster
# - tenant_ids: List of tenants to test
```

#### 1.2 Run Vessel Telemetry Generator (Smoke Test)
```bash
# Generate 100 telemetry messages per tenant
python -c "
from utils.vessel_telemetry_generator import VesselTelemetryGenerator
import json

with open('test_config.json') as f:
    config = json.load(f)

gen = VesselTelemetry Generator(config)

# Generate valid telemetry for smoke test
for tenant in ['shipping-co-alpha', 'logistics-beta', 'maritime-gamma']:
    messages = gen.generate_batch(tenant, count=100, failure_rate=0.0)
    print(f'Generated {len(messages)} messages for {tenant}')
    gen.publish_to_pulsar(tenant, messages)
"

# Expected output: 300 total messages (100 per tenant)
```

**Validation:**
```bash
# Check messages were consumed
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin topics stats persistent://shipping-co-alpha/vessel-tracking/telemetry

# Verify consumption by stream processors
kubectl logs -n navtor-dev deployment/raw-telemetry-ingestion --tail=50 | grep "Processed message"

# Check data landed in Cassandra
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- \
  cqlsh -e "SELECT COUNT(*) FROM shipping_co_alpha.vessel_telemetry WHERE date = '$(date +%Y-%m-%d)';"
```

**Validation Gate:**
- ✅ All 300 messages published successfully
- ✅ Stream consumers processed messages (check logs)
- ✅ Data visible in Cassandra tables
- ✅ No errors in DLQ topics

#### 1.3 Run Multi-Tenant Generator (Concurrent Test)
```bash
# Test concurrent ingestion from multiple tenants
python -c "
from utils.multi_tenant_generator import MultiTenantGenerator
import json

with open('test_config.json') as f:
    config = json.load(f)

gen = MultiTenantGenerator(config)

# Generate concurrent load
results = gen.generate_concurrent_load(
    tenants=['shipping-co-alpha', 'logistics-beta', 'maritime-gamma'],
    messages_per_tenant=200,
    concurrency=3
)

print('Concurrent generation results:')
for tenant, stats in results.items():
    print(f'  {tenant}: {stats[\"success\"]}/{stats[\"total\"]} messages')
"
```

**Validation Gate:**
- ✅ 600 total messages generated (200 x 3 tenants)
- ✅ All tenants processed concurrently
- ✅ No cross-tenant data leakage (verify isolation)

### Phase 2: Load Testing (15 minutes)

#### 2.1 Sustained Load Test (1000 msg/sec for 5 minutes)
```bash
# Run sustained load test
python tests/integration/run_load_test.py \
  --duration 300 \
  --rate 1000 \
  --tenants shipping-co-alpha logistics-beta maritime-gamma

# Monitor system metrics during test
kubectl top pods -n navtor-dev
kubectl top nodes
```

**Metrics to Track:**
- Consumer lag (should remain < 1000)
- CPU/Memory usage (should not exceed 80%)
- Error rate (should be < 0.1%)
- P95 latency (should be < 500ms)

**Validation Commands:**
```bash
# Check consumer lag
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin topics stats persistent://shipping-co-alpha/vessel-tracking/telemetry | \
  jq '.subscriptions[].msgBacklog'

# Check Pulsar backlog
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin topics stats-internal persistent://shipping-co-alpha/vessel-tracking/telemetry
```

**Validation Gate:**
- ✅ System sustained 1000 msg/sec for 5 minutes
- ✅ Consumer lag < 1000 messages
- ✅ No pod restarts or OOMKills
- ✅ All messages eventually processed

#### 2.2 Burst Load Test (5000 msg/sec for 1 minute)
```bash
# Test burst capacity
python tests/integration/run_load_test.py \
  --duration 60 \
  --rate 5000 \
  --burst true \
  --tenants shipping-co-alpha

# Observe autoscaling behavior
watch kubectl get hpa -n navtor-dev
```

**Validation Gate:**
- ✅ HPA scaled up deployments
- ✅ System recovered within 5 minutes post-burst
- ✅ No message loss

### Phase 3: Stress Testing with Failures (15 minutes)

#### 3.1 Inject Invalid Messages (DQ Validation)
```bash
# Generate batch with 20% invalid messages
python -c "
from utils.failure_injection_generator import FailureInjectionGenerator
import json

with open('test_config.json') as f:
    config = json.load(f)

gen = FailureInjectionGenerator(config)

# Inject various failure types
results = gen.inject_failures(
    tenant='shipping-co-alpha',
    count=500,
    failure_types={
        'missing_required_field': 0.10,  # 10% missing vessel_id
        'invalid_type': 0.05,             # 5% wrong data types
        'out_of_range': 0.05,             # 5% invalid ranges
        'malformed_json': 0.0             # 0% for this test
    }
)

print(f'Generated {results[\"total\"]} messages')
print(f'Valid: {results[\"valid\"]}, Invalid: {results[\"invalid\"]}')
"
```

**Validation - DQ Framework Should:**
- Route valid messages to processing pipeline
- Route invalid messages to DQ quarantine queue
- Log validation failures in DQ metrics index

**Check DQ Processing:**
```bash
# Verify DQ processor handled invalid messages
kubectl logs -n navtor-dev deployment/dq-processor --tail=100 | grep "Validation failed"

# Check quarantine queue depth
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin topics stats persistent://shipping-co-alpha/dq/quarantine

# Query DQ metrics in OpenSearch
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin -X GET "https://localhost:9200/dq_metrics/_search?pretty" \
  -H 'Content-Type: application/json' -d '{
    "query": {
      "bool": {
        "must": [
          {"term": {"tenant_id": "shipping-co-alpha"}},
          {"term": {"validation_outcome": "failed"}}
        ]
      }
    },
    "size": 0,
    "aggs": {
      "failure_types": {"terms": {"field": "failure_reason.keyword"}}
    }
  }'
```

**Validation Gate:**
- ✅ Valid messages processed normally
- ✅ Invalid messages routed to quarantine
- ✅ DQ metrics captured in OpenSearch
- ✅ Failure types correctly classified

#### 3.2 DQ Dashboard Validation
```bash
# Access DQ dashboards
kubectl port-forward -n opensearch svc/opensearch-dashboards 5601:5601 &

# Open browser to http://localhost:5601
# Navigate to dashboards:
# 1. DQ Validation Rates - Shows pass/fail rates per tenant
# 2. DQ Failure Analysis - Breakdown by failure type
# 3. DQ Quarantine Queue - Messages awaiting review
```

**Verify Dashboards Show:**
- ✅ Validation rate chart (should show ~80% pass rate)
- ✅ Failure type breakdown (missing_required_field: 50%, invalid_type: 25%, out_of_range: 25%)
- ✅ Quarantine queue visualization

### Phase 4: Anomaly Detection Validation (10 minutes)

#### 4.1 Inject Anomalous Patterns
```bash
# Generate telemetry with anomalies
python -c "
from utils.vessel_telemetry_generator import VesselTelemetryGenerator
import json

with open('test_config.json') as f:
    config = json.load(f)

gen = VesselTelemetryGenerator(config)

# Generate anomalous patterns for vessel VESSEL-001
anomalies = []

# Pattern 1: Fuel consumption spike (300% above normal)
anomalies.extend(gen.generate_anomaly(
    vessel_id='VESSEL-001',
    anomaly_type='fuel_spike',
    duration_minutes=30,
    severity=3.0
))

# Pattern 2: Engine temperature sustained high
anomalies.extend(gen.generate_anomaly(
    vessel_id='VESSEL-002',
    anomaly_type='engine_temp_high',
    duration_minutes=60,
    severity=1.5
))

# Pattern 3: Erratic speed changes
anomalies.extend(gen.generate_anomaly(
    vessel_id='VESSEL-003',
    anomaly_type='speed_oscillation',
    duration_minutes=45,
    severity=2.0
))

print(f'Generated {len(anomalies)} anomalous telemetry records')

# Publish to Pulsar
gen.publish_to_pulsar('shipping-co-alpha', anomalies)
"
```

#### 4.2 Verify Anomaly Detection
Wait 2-3 minutes for anomaly detection to process, then verify:

```bash
# Check anomaly detection logs
kubectl logs -n navtor-dev deployment/anomaly-detection --tail=100 | grep "Anomaly detected"

# Query detected anomalies in OpenSearch
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin -X GET "https://localhost:9200/shipping-co-alpha_anomalies/_search?pretty" \
  -H 'Content-Type: application/json' -d '{
    "query": {
      "range": {
        "timestamp": {
          "gte": "now-10m"
        }
      }
    },
    "sort": [{"anomaly_score": {"order": "desc"}}]
  }'

# Expected: 3 anomaly records for VESSEL-001, VESSEL-002, VESSEL-003
```

#### 4.3 Verify OpenSearch Anomaly Detectors
```bash
# Check built-in OpenSearch AD detectors
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin -X GET "https://localhost:9200/_plugins/_anomaly_detection/detectors/_search?pretty"

# Verify detectors for each tenant:
# - shipping-co-alpha_fuel_consumption_spikes
# - shipping-co-alpha_engine_multi_metric
# - shipping-co-alpha_vessel_behavior
```

**Validation Gate:**
- ✅ 3 anomalies detected and indexed
- ✅ Anomaly scores > threshold
- ✅ OpenSearch AD detectors running
- ✅ Anomaly dashboard shows recent detections

### Phase 5: Run Complete Integration Test Suite (20 minutes)

```bash
cd tests/integration

# Run all integration tests
python run_all_tests.py --verbose

# Tests include:
# ✅ multi_tenant_data_flow/test_tenant_isolation.py
# ✅ multi_tenant_data_flow/test_concurrent_ingestion.py
# ✅ pulsar_dlq_retry/test_dlq_routing.py
# ✅ pulsar_dlq_retry/test_retry_backoff.py
# ✅ per_vessel_ordering/test_key_shared_ordering.py
# ✅ anomaly_detection/test_anomaly_injection.py
# ✅ feature_store_materialization/test_feast_materialization.py
# ✅ inference_pipeline/test_batch_inference.py
# ✅ replay_test/test_subscription_rewind.py
```

**Expected Results:**
```
============================= test session starts ==============================
collected 47 items

test_tenant_isolation.py::test_pulsar_namespace_isolation PASSED        [  2%]
test_tenant_isolation.py::test_cassandra_keyspace_isolation PASSED      [  4%]
test_tenant_isolation.py::test_opensearch_index_isolation PASSED        [  6%]
test_concurrent_ingestion.py::test_concurrent_multi_tenant PASSED       [  8%]
test_dlq_routing.py::test_malformed_message_to_dlq PASSED               [ 10%]
test_dlq_routing.py::test_retry_exhaustion_to_quarantine PASSED         [ 12%]
test_retry_backoff.py::test_exponential_backoff PASSED                  [ 14%]
...
======================== 47 passed in 18.42s ===============================
```

**Validation Gate:**
- ✅ All 47 integration tests pass
- ✅ No test failures or errors
- ✅ Test coverage > 80%

## 📊 Expected Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Smoke Testing | 10 min | 10 min |
| Load Testing | 15 min | 25 min |
| Stress Testing | 15 min | 40 min |
| Anomaly Detection | 10 min | 50 min |
| Integration Suite | 20 min | **70 min** |

## ✅ Success Criteria

- [ ] Smoke tests pass for all tenants
- [ ] System sustains 1000 msg/sec load
- [ ] DQ framework routes invalid messages to quarantine
- [ ] Anomaly detection identifies injected anomalies
- [ ] All 47 integration tests pass
- [ ] No data corruption or cross-tenant leakage

---

**Next**: Proceed to [ML Training & Validation](#ml-training)
"""

# Write the guide
guide_path = 'docs/cookbook/03_generator_testing_dq_validation.md'

with open(guide_path, 'w') as f:
    f.write(testing_guide)

print(f"✅ Created: {guide_path}")
print(f"📄 Guide includes:")
print("   - Smoke, load, and stress testing procedures")
print("   - DQ validation with failure injection")
print("   - Anomaly detection validation")
print("   - Complete integration test suite (47 tests)")
print("   - Expected timeline: ~70 minutes")