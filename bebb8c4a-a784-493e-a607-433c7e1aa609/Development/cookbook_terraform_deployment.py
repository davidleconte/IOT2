import os

# Generate comprehensive Terraform deployment guide
terraform_guide = """
# Step 1: Terraform Infrastructure Deployment

## 🎯 Objective
Deploy foundational infrastructure components using Terraform modules.

## 📂 Location
All Terraform code is in: `terraform/`

## 🏗️ Deployment Sequence

### Phase 1: Security Foundation (10-15 minutes)

#### 1.1 Deploy Vault for Secrets Management
```bash
cd terraform/examples

# Initialize Terraform
terraform init

# Review the Vault module plan
terraform plan -target=module.vault

# Deploy Vault
terraform apply -target=module.vault -auto-approve

# Verify Vault is running
kubectl get pods -n vault-system
kubectl logs -n vault-system -l app=vault --tail=50
```

**Validation Gate:**
- ✅ Vault pod is Running
- ✅ Vault is unsealed (check logs)
- ✅ Service endpoint is accessible

#### 1.2 Configure Sealed Secrets
```bash
# Deploy sealed-secrets controller
terraform apply -target=module.sealed_secrets -auto-approve

# Verify sealed-secrets controller
kubectl get pods -n kube-system -l name=sealed-secrets-controller

# Get public key for encrypting secrets
kubeseal --fetch-cert > pub-cert.pem
```

**Validation Gate:**
- ✅ Controller pod is Running
- ✅ Public certificate retrieved successfully

### Phase 2: Data Infrastructure (20-30 minutes)

#### 2.1 Deploy Apache Pulsar
```bash
# Deploy Pulsar operator and cluster
terraform apply -target=module.pulsar -auto-approve

# Wait for Pulsar to stabilize (5-10 minutes)
kubectl get pods -n pulsar

# Verify all Pulsar components are ready:
# - Zookeeper (3 replicas)
# - BookKeeper (3 replicas)
# - Broker (3 replicas)
# - Proxy (2 replicas)
```

**Validation Commands:**
```bash
# Check Pulsar cluster health
kubectl exec -n pulsar pulsar-broker-0 -- bin/pulsar-admin brokers healthcheck

# List brokers
kubectl exec -n pulsar pulsar-broker-0 -- bin/pulsar-admin brokers list pulsar-cluster

# Check BookKeeper ledgers
kubectl exec -n pulsar bookkeeper-0 -- bin/bookkeeper shell listledgers
```

**Validation Gate:**
- ✅ All Pulsar pods Running (12+ pods)
- ✅ Healthcheck passes
- ✅ Brokers are discoverable

#### 2.2 Deploy DataStax HCD (Cassandra)
```bash
# Deploy HCD cluster
terraform apply -target=module.datastax_hcd -auto-approve

# Wait for Cassandra nodes to join (10-15 minutes)
kubectl get cassandradatacenters -n cassandra

# Check nodetool status
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- nodetool status
```

**Validation Gate:**
- ✅ All Cassandra nodes Up and Normal (UN)
- ✅ Replication strategy configured
- ✅ System keyspaces present

#### 2.3 Deploy OpenSearch
```bash
# Deploy OpenSearch cluster
terraform apply -target=module.opensearch -auto-approve

# Wait for cluster formation (5-10 minutes)
kubectl get pods -n opensearch

# Check cluster health
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin https://localhost:9200/_cluster/health?pretty
```

**Validation Gate:**
- ✅ Cluster status: green
- ✅ All nodes joined
- ✅ Security plugin enabled

### Phase 3: ML & Analytics (15-20 minutes)

#### 3.1 Deploy Watsonx.data (Presto + Iceberg)
```bash
# Configure S3 credentials for Iceberg
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export S3_BUCKET="your-iceberg-bucket"

# Deploy Watsonx.data
terraform apply -target=module.watsonx_data -auto-approve

# Verify Presto coordinator
kubectl get pods -n watsonx-data
kubectl logs -n watsonx-data watsonx-presto-coordinator-0 --tail=100
```

**Validation Gate:**
- ✅ Presto coordinator Running
- ✅ Workers connected
- ✅ Iceberg catalog configured

#### 3.2 Deploy MLflow
```bash
# Deploy MLflow tracking server
terraform apply -target=module.mlflow -auto-approve

# Get MLflow UI URL
kubectl get ingress -n mlflow
```

**Validation Gate:**
- ✅ MLflow server accessible
- ✅ Backend store (PostgreSQL) connected
- ✅ Artifact store (S3) accessible

#### 3.3 Deploy Feast Feature Store
```bash
# Deploy Feast
terraform apply -target=module.feast -auto-approve

# Verify Feast services
kubectl get pods -n feast
kubectl logs -n feast feast-online-0 --tail=50
```

**Validation Gate:**
- ✅ Feast online store Running
- ✅ Connected to Cassandra/Redis backend

### Phase 4: Complete Deployment

```bash
# Apply all remaining resources
terraform apply -auto-approve

# Generate outputs
terraform output > terraform-outputs.txt
```

## 🔍 Post-Deployment Verification

### Verify All Services
```bash
# Check all namespaces
kubectl get pods --all-namespaces | grep -E "(pulsar|cassandra|opensearch|watsonx|feast|mlflow)"

# Get service endpoints
kubectl get svc --all-namespaces -o wide

# Check persistent volumes
kubectl get pv
kubectl get pvc --all-namespaces
```

### Verify Network Connectivity
```bash
# Test Pulsar broker
kubectl run -n pulsar pulsar-test --image=apachepulsar/pulsar:latest --rm -it -- \
  bin/pulsar-client produce persistent://public/default/test-topic --messages "hello"

# Test Cassandra
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- cqlsh -e "DESCRIBE KEYSPACES;"

# Test OpenSearch
kubectl port-forward -n opensearch svc/opensearch 9200:9200 &
curl -k -u admin:admin https://localhost:9200/_cat/nodes
```

## ⚠️ Common Issues & Troubleshooting

### Issue: Pulsar Pods CrashLooping
**Solution:**
```bash
# Check resource constraints
kubectl describe pod -n pulsar <pod-name>

# Increase memory/CPU in terraform/modules/k8s-pulsar/main.tf
# Reapply: terraform apply -target=module.pulsar
```

### Issue: Cassandra Nodes Not Joining
**Solution:**
```bash
# Check seed node DNS
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- nslookup cassandra-seed-service

# Restart nodes one by one
kubectl delete pod -n cassandra cassandra-dc1-default-sts-0
```

### Issue: OpenSearch Cluster Yellow/Red
**Solution:**
```bash
# Check shard allocation
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin https://localhost:9200/_cat/shards?v

# Retry failed shards
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -XPOST -u admin:admin https://localhost:9200/_cluster/reroute?retry_failed
```

## 📊 Expected Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Security Foundation | 15 min | 15 min |
| Data Infrastructure | 30 min | 45 min |
| ML & Analytics | 20 min | 65 min |
| Verification | 10 min | **75 min** |

## ✅ Success Criteria

- [ ] All Terraform modules applied successfully
- [ ] All pods in Running state (except completed jobs)
- [ ] Cluster health checks passing
- [ ] Service endpoints accessible
- [ ] No CrashLoopBackOff or Pending pods

---

**Next**: Proceed to [Helm Chart Deployment](#helm-deployment)
"""

# Write the guide
os.makedirs('docs/cookbook', exist_ok=True)
guide_path = 'docs/cookbook/01_terraform_deployment.md'

with open(guide_path, 'w') as f:
    f.write(terraform_guide)

print(f"✅ Created: {guide_path}")
print(f"📄 Guide includes:")
print("   - 4 deployment phases with validation gates")
print("   - Detailed kubectl commands for verification")
print("   - Troubleshooting for common issues")
print("   - Expected timeline: ~75 minutes")
print("   - Success criteria checklist")