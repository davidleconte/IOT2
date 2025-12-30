import os

# Generate Helm deployment guide with installation order
helm_guide = """
# Step 2: Helm Chart Deployment

## 🎯 Objective
Deploy application services and stream processing pipelines using Helm charts.

## 📂 Location
All Helm charts are in: `helm/`

## 🏗️ Helm Installation Sequence

### Prerequisites Check
```bash
# Verify Terraform infrastructure is healthy
kubectl get pods -n pulsar | grep Running | wc -l  # Should be 12+
kubectl get pods -n cassandra | grep Running | wc -l  # Should be 3+
kubectl get pods -n opensearch | grep Running | wc -l  # Should be 3+

# Ensure Helm is installed
helm version

# Add any required Helm repositories
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
```

### Phase 1: Parent Chart Deployment (15-20 minutes)

The parent chart orchestrates all subchart dependencies.

#### 1.1 Review Configuration
```bash
cd helm/navtor-fleet-guardian

# Review values for your environment (dev/staging/prod)
cat values/values-dev.yaml

# Key configurations to verify:
# - Image registry and tags
# - Resource requests/limits
# - Storage class names
# - Ingress hostnames
# - TLS certificate references
```

#### 1.2 Customize Environment Values
```bash
# Edit environment-specific values
vim values/values-dev.yaml

# Important sections:
# 1. Global settings (namespace, domain, registry)
# 2. Pulsar connection strings
# 3. Cassandra contact points
# 4. OpenSearch endpoints
# 5. S3/object storage for Iceberg
# 6. ML model registry (MLflow) URL
```

#### 1.3 Install Parent Chart (Dev Environment)
```bash
# Create namespace
kubectl create namespace navtor-dev

# Perform dry-run to validate
helm install navtor-fleet-guardian . \
  -f values/values-dev.yaml \
  --namespace navtor-dev \
  --dry-run --debug > helm-dry-run.yaml

# Review the rendered manifests
less helm-dry-run.yaml

# Install for real
helm install navtor-fleet-guardian . \
  -f values/values-dev.yaml \
  --namespace navtor-dev \
  --timeout 15m \
  --wait

# Monitor deployment
watch kubectl get pods -n navtor-dev
```

**Validation Gate:**
- ✅ Helm release status: deployed
- ✅ All pods transitioning to Running
- ✅ No ImagePullBackOff errors

### Phase 2: Tenant Provisioning (10 minutes)

#### 2.1 Run Tenant Setup Jobs
The Helm chart includes init jobs for tenant provisioning.

```bash
# Verify tenant setup jobs completed
kubectl get jobs -n navtor-dev | grep tenant-setup

# Check logs for each tenant
kubectl logs -n navtor-dev job/shipping-co-alpha-tenant-setup
kubectl logs -n navtor-dev job/logistics-beta-tenant-setup
kubectl logs -n navtor-dev job/maritime-gamma-tenant-setup
```

**Jobs should create:**
- Pulsar tenants and namespaces
- Cassandra keyspaces with tenant isolation
- OpenSearch indices and roles
- Feast feature views for each tenant

**Validation Commands:**
```bash
# Verify Pulsar tenants
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin tenants list

# Verify Cassandra keyspaces
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- \
  cqlsh -e "DESCRIBE KEYSPACES;" | grep -E "(shipping_co_alpha|logistics_beta|maritime_gamma)"

# Verify OpenSearch indices
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin https://localhost:9200/_cat/indices?v | grep -E "(shipping|logistics|maritime)"
```

**Validation Gate:**
- ✅ 3 Pulsar tenants created
- ✅ 3 Cassandra keyspaces created  
- ✅ 15+ OpenSearch indices created (5 per tenant)

### Phase 3: Seed Data Loading (5 minutes)

#### 3.1 Run Seed Data Job
```bash
# Check if seed data job is enabled
helm get values navtor-fleet-guardian -n navtor-dev | grep -A 5 seedData

# Verify seed data job completed
kubectl get job -n navtor-dev seed-data-job
kubectl logs -n navtor-dev job/seed-data-job --tail=100
```

**Seed data includes:**
- Sample vessel telemetry records (100 per tenant)
- Historical voyage data
- Maintenance event records
- Reference data (ports, vessel specs)

**Validation:**
```bash
# Query Cassandra for seed data
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- \
  cqlsh -e "SELECT COUNT(*) FROM shipping_co_alpha.vessel_telemetry;"

# Should return ~100 rows per tenant
```

**Validation Gate:**
- ✅ Seed data job completed successfully
- ✅ Data present in Cassandra tables
- ✅ Data present in OpenSearch indices

### Phase 4: Stream Processing Services (5 minutes)

#### 4.1 Verify Stream Processing Deployments
```bash
# List all stream processing services
kubectl get deployments -n navtor-dev | grep -E "(raw-telemetry|feature-computation|anomaly-detection|lakehouse-archival)"

# Expected deployments:
# - raw-telemetry-ingestion
# - feature-computation
# - anomaly-detection
# - lakehouse-archival
# - dlq-handler
# - retry-processor
```

#### 4.2 Check Service Connectivity
```bash
# Verify Pulsar consumers are subscribed
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin topics stats persistent://shipping-co-alpha/vessel-tracking/telemetry

# Should show active subscriptions for each service
```

**Validation Gate:**
- ✅ All 6 stream processing deployments Running
- ✅ Consumers subscribed to Pulsar topics
- ✅ No consumer lag (check metrics)

### Phase 5: Inference Services (5 minutes)

#### 5.1 Deploy Inference Chart
```bash
cd helm/inference-services

# Install inference services
helm install inference-services . \
  --namespace navtor-dev \
  --set batch.enabled=true \
  --set realtime.enabled=true \
  --wait

# Verify deployments
kubectl get deployments -n navtor-dev | grep inference
```

**Expected resources:**
- `realtime-inference` deployment (REST/gRPC API)
- `batch-inference-spark` CronJob for scheduled batch inference
- HPA for realtime inference autoscaling
- Ingress for external access

**Validation:**
```bash
# Test realtime inference endpoint
kubectl port-forward -n navtor-dev svc/realtime-inference 8080:8080 &

# Health check
curl http://localhost:8080/health

# Sample inference request
curl -X POST http://localhost:8080/predict \
  -H "Content-Type: application/json" \
  -d '{
    "vessel_id": "VESSEL-001",
    "features": {
      "speed": 12.5,
      "fuel_rate": 45.2,
      "engine_temp": 85.0
    }
  }'
```

**Validation Gate:**
- ✅ Realtime inference responding to health checks
- ✅ Batch inference CronJob scheduled
- ✅ HPA monitoring metrics

## 🔍 Post-Deployment Verification

### Verify All Helm Releases
```bash
# List all releases
helm list -n navtor-dev

# Expected output:
# NAME                    STATUS      CHART
# navtor-fleet-guardian   deployed    navtor-fleet-guardian-0.1.0
# inference-services      deployed    inference-services-0.1.0
```

### Run Helm Tests
```bash
# Run smoke tests included in Helm charts
helm test navtor-fleet-guardian -n navtor-dev --logs

# Tests verify:
# - Pulsar connectivity
# - Cassandra queries
# - OpenSearch API
# - Service-to-service communication
```

### Check Resource Utilization
```bash
# View resource usage
kubectl top nodes
kubectl top pods -n navtor-dev

# Ensure no pods are hitting resource limits
kubectl get pods -n navtor-dev -o json | \
  jq '.items[] | select(.status.containerStatuses[].state.waiting.reason == "CrashLoopBackOff") | .metadata.name'
```

## ⚠️ Common Issues & Troubleshooting

### Issue: Helm Install Timeout
**Symptoms:** Helm times out waiting for pods to be ready

**Solution:**
```bash
# Check which pods are not ready
kubectl get pods -n navtor-dev | grep -v Running

# Describe problematic pod
kubectl describe pod -n navtor-dev <pod-name>

# Check events
kubectl get events -n navtor-dev --sort-by='.lastTimestamp'

# Common causes:
# 1. Image pull errors (check imagePullSecrets)
# 2. Insufficient resources (scale down or add nodes)
# 3. Configuration errors (check ConfigMaps and Secrets)
```

### Issue: Tenant Provisioning Jobs Failing
**Symptoms:** Tenant setup jobs show Error or Failed status

**Solution:**
```bash
# Check job logs
kubectl logs -n navtor-dev job/<tenant>-tenant-setup --tail=100

# Common issues:
# - Cannot connect to Pulsar/Cassandra/OpenSearch (check endpoints in ConfigMap)
# - Permission denied (verify service accounts and RBAC)
# - Resource already exists (clean up and retry)

# Rerun failed job
kubectl delete job -n navtor-dev <tenant>-tenant-setup
helm upgrade navtor-fleet-guardian . -f values/values-dev.yaml -n navtor-dev
```

### Issue: Stream Processing Services Not Consuming
**Symptoms:** Consumer lag increasing, no processing happening

**Solution:**
```bash
# Check consumer logs
kubectl logs -n navtor-dev deployment/raw-telemetry-ingestion --tail=100

# Verify Pulsar subscription
kubectl exec -n pulsar pulsar-broker-0 -- \
  bin/pulsar-admin topics stats persistent://shipping-co-alpha/vessel-tracking/telemetry

# Check for:
# - Authentication errors
# - Topic doesn't exist
# - Consumer config mismatch (subscriptionType, subscriptionName)

# Restart consumer
kubectl rollout restart deployment/raw-telemetry-ingestion -n navtor-dev
```

## 📊 Expected Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Parent Chart Deploy | 20 min | 20 min |
| Tenant Provisioning | 10 min | 30 min |
| Seed Data | 5 min | 35 min |
| Stream Processing | 5 min | 40 min |
| Inference Services | 5 min | 45 min |
| Verification | 10 min | **55 min** |

## ✅ Success Criteria

- [ ] All Helm releases in 'deployed' status
- [ ] All pods Running (except completed Jobs)
- [ ] Helm tests pass
- [ ] Tenants provisioned in all systems
- [ ] Seed data loaded successfully
- [ ] Stream consumers active and processing
- [ ] Inference endpoints responding

---

**Next**: Proceed to [Data Quality Validation](#dq-validation)
"""

# Write the guide
guide_path = 'docs/cookbook/02_helm_deployment.md'

with open(guide_path, 'w') as f:
    f.write(helm_guide)

print(f"✅ Created: {guide_path}")
print(f"📄 Guide includes:")
print("   - 5 deployment phases with validation gates")
print("   - Tenant provisioning and seed data steps")
print("   - Stream processing verification")
print("   - Inference service testing")
print("   - Expected timeline: ~55 minutes")
