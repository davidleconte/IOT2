import os
import json

# Create runbook directory structure
os.makedirs("docs/runbook/tenant-operations", exist_ok=True)
os.makedirs("docs/runbook/ha-failover", exist_ok=True)
os.makedirs("docs/runbook/scaling", exist_ok=True)
os.makedirs("docs/runbook/dlq-operations", exist_ok=True)
os.makedirs("docs/runbook/replay", exist_ok=True)
os.makedirs("docs/runbook/observability", exist_ok=True)
os.makedirs("docs/runbook/security", exist_ok=True)
os.makedirs("docs/runbook/dr-backup", exist_ok=True)
os.makedirs("docs/runbook/benchmarking", exist_ok=True)

# Tenant Onboarding Runbook
tenant_onboarding = """# Tenant Onboarding Runbook

## Overview
Complete procedure for onboarding new tenants to the NavTor Fleet Guardian platform.

## Prerequisites
- [ ] Tenant information collected (company name, contact details, vessel count)
- [ ] Resource quotas determined (storage, compute, message throughput)
- [ ] Security clearance and contracts signed
- [ ] Network connectivity confirmed

## Procedure

### 1. Create Tenant Identifier
```bash
# Use lowercase, hyphenated format
TENANT_ID="acme-shipping-co"
TENANT_NAME="Acme Shipping Co"
```

### 2. Provision Pulsar Resources
```bash
# Apply tenant configuration
kubectl apply -f - <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: tenant-${TENANT_ID}-config
  namespace: pulsar
data:
  tenant.yaml: |
    tenant: ${TENANT_ID}
    allowedClusters:
      - pulsar-cluster
    adminRoles:
      - tenant-${TENANT_ID}-admin
EOF

# Create Pulsar tenant
bin/pulsar-admin tenants create ${TENANT_ID} \\
  --allowed-clusters pulsar-cluster \\
  --admin-roles tenant-${TENANT_ID}-admin

# Create namespaces
for ns in vessel-tracking cargo port-operations analytics; do
  bin/pulsar-admin namespaces create persistent://${TENANT_ID}/${ns}
  
  # Set retention policy (7 days)
  bin/pulsar-admin namespaces set-retention persistent://${TENANT_ID}/${ns} \\
    --size 100G --time 7d
  
  # Enable deduplication
  bin/pulsar-admin namespaces set-deduplication persistent://${TENANT_ID}/${ns} \\
    --enable
  
  # Set message TTL (30 days)
  bin/pulsar-admin namespaces set-message-ttl persistent://${TENANT_ID}/${ns} \\
    --messageTTL 2592000
done

# Create DLQ and retry topics (automated via Helm)
helm upgrade --install ${TENANT_ID}-topics ./helm/navtor-fleet-guardian \\
  --set tenantId=${TENANT_ID} \\
  --set createTopics=true
```

### 3. Create Cassandra Keyspace and Tables
```bash
# Connect to Cassandra
kubectl exec -it datastax-hcd-0 -n datastax -- cqlsh

# Run schema creation
cqlsh> SOURCE '/ops/cassandra/schemas/${TENANT_ID}_schema.cql';

# Verify keyspace
cqlsh> DESCRIBE KEYSPACE ${TENANT_ID};
```

### 4. Provision OpenSearch Indices
```bash
# Create tenant indices
for index in vessel_telemetry anomalies engine_metrics incidents maintenance_events; do
  curl -X PUT "https://opensearch:9200/${TENANT_ID}_${index}" \\
    -H 'Content-Type: application/json' \\
    -d @/ops/opensearch/index-templates/${TENANT_ID}_${index}.json
done

# Create tenant role
curl -X PUT "https://opensearch:9200/_plugins/_security/api/roles/${TENANT_ID}_admin" \\
  -H 'Content-Type: application/json' \\
  -d @/ops/opensearch/roles/${TENANT_ID}_admin.json

# Create tenant user
curl -X PUT "https://opensearch:9200/_plugins/_security/api/internalusers/${TENANT_ID}_user" \\
  -H 'Content-Type: application/json' \\
  -d '{
    "password": "<generated_password>",
    "opendistro_security_roles": ["'${TENANT_ID}'_admin"]
  }'
```

### 5. Configure watsonx.data Catalog
```bash
# Create Iceberg namespace for tenant
presto --server presto-coordinator:8080 --catalog maritime_iceberg --execute "
CREATE SCHEMA IF NOT EXISTS maritime_iceberg.${TENANT_ID}
WITH (location = 's3a://lakehouse/${TENANT_ID}/');
"

# Create tenant tables (automated via migration)
python ops/watsonx-data/create_tenant_schema.py --tenant-id ${TENANT_ID}
```

### 6. Deploy Tenant-Specific Resources
```bash
# Create namespace
kubectl create namespace tenant-${TENANT_ID}

# Apply resource quotas
kubectl apply -f - <<EOF
apiVersion: v1
kind: ResourceQuota
metadata:
  name: ${TENANT_ID}-quota
  namespace: tenant-${TENANT_ID}
spec:
  hard:
    requests.cpu: "10"
    requests.memory: 20Gi
    limits.cpu: "20"
    limits.memory: 40Gi
    persistentvolumeclaims: "10"
EOF

# Deploy stream processing services for tenant
helm upgrade --install ${TENANT_ID}-services ./helm/stream-processing-services \\
  --namespace tenant-${TENANT_ID} \\
  --set tenantId=${TENANT_ID} \\
  --set pulsar.tenant=${TENANT_ID}
```

### 7. Configure Monitoring and Dashboards
```bash
# Create tenant OpenSearch dashboards
python ops/opensearch/create_tenant_dashboards.py --tenant-id ${TENANT_ID}

# Add tenant to metrics collection
kubectl edit configmap metrics-collector-config -n monitoring
# Add tenant to tenant_mapping.json

# Restart metrics collector
kubectl rollout restart deployment metrics-collector -n monitoring
```

### 8. Validation Checks
```bash
# Test message ingestion
python tests/integration/utils/vessel_telemetry_generator.py \\
  --tenant-id ${TENANT_ID} \\
  --messages 100

# Verify data flow
bin/pulsar-admin topics stats persistent://${TENANT_ID}/vessel-tracking/telemetry
cqlsh> SELECT COUNT(*) FROM ${TENANT_ID}.vessel_telemetry;
curl -X GET "https://opensearch:9200/${TENANT_ID}_vessel_telemetry/_count"

# Check feature store
feast feature-views list --project ${TENANT_ID}
```

### 9. Documentation and Handoff
- [ ] Generate tenant credentials document (store in Vault)
- [ ] Share API endpoints and connection strings
- [ ] Provide sample integration code
- [ ] Schedule onboarding call
- [ ] Add tenant to support system

## Rollback Procedure
If onboarding fails, execute in reverse order:
1. Remove monitoring dashboards
2. Delete K8s namespace and resources
3. Drop watsonx.data schemas
4. Remove OpenSearch indices and roles
5. Drop Cassandra keyspace
6. Delete Pulsar namespaces and tenant

## Estimated Time
- Standard onboarding: 30-45 minutes
- With validation: 60 minutes

## Support Contacts
- Platform Team: platform-ops@navtor.com
- On-call: +47-XXXXXXX
"""

tenant_onboarding_path = "docs/runbook/tenant-operations/01_tenant_onboarding.md"
with open(tenant_onboarding_path, 'w') as f:
    f.write(tenant_onboarding)

print(f"✓ Created tenant onboarding runbook: {tenant_onboarding_path}")

# Tenant Offboarding Runbook
tenant_offboarding = """# Tenant Offboarding Runbook

## Overview
Secure procedure for tenant removal including data retention compliance.

## Prerequisites
- [ ] Offboarding notice received (minimum 30 days)
- [ ] Data export completed (if requested)
- [ ] Legal/compliance approval obtained
- [ ] Final invoice generated

## Data Retention Policy
Per GDPR and maritime regulations:
- Operational data: 30 days post-termination
- Compliance data: 7 years (archived to cold storage)
- Personal data: Immediate deletion upon request

## Procedure

### 1. Notify Stakeholders
```bash
TENANT_ID="acme-shipping-co"
OFFBOARD_DATE="2025-02-01"

# Send notifications (automated)
python ops/tenant-management/notify_offboarding.py \\
  --tenant-id ${TENANT_ID} \\
  --date ${OFFBOARD_DATE}
```

### 2. Suspend Data Ingestion (T-7 days)
```bash
# Disable Pulsar namespace writes
for ns in vessel-tracking cargo port-operations analytics; do
  bin/pulsar-admin namespaces set-subscription-dispatch-rate \\
    persistent://${TENANT_ID}/${ns} \\
    --dispatch-msg-rate 0
done

# Update stream processing to reject messages
kubectl scale deployment ${TENANT_ID}-raw-telemetry-ingestion \\
  --replicas=0 -n tenant-${TENANT_ID}
```

### 3. Export Data (T-5 days)
```bash
# Export Pulsar messages to S3
bin/pulsar-admin topics offload persistent://${TENANT_ID}/vessel-tracking/telemetry \\
  --size-threshold 100M

# Export Cassandra data
cqlsh> COPY ${TENANT_ID}.vessel_telemetry TO '/exports/${TENANT_ID}/vessel_telemetry.csv';
cqlsh> COPY ${TENANT_ID}.alerts TO '/exports/${TENANT_ID}/alerts.csv';

# Export OpenSearch data
elasticdump \\
  --input=https://opensearch:9200/${TENANT_ID}_vessel_telemetry \\
  --output=/exports/${TENANT_ID}/opensearch_telemetry.json \\
  --type=data

# Export watsonx.data (Iceberg) tables
presto --server presto-coordinator:8080 --execute "
CALL system.export_table(
  schema => '${TENANT_ID}',
  table_name => 'vessel_performance',
  export_path => 's3a://exports/${TENANT_ID}/vessel_performance/'
);
"

# Create encrypted archive
tar czf ${TENANT_ID}_export_$(date +%Y%m%d).tar.gz /exports/${TENANT_ID}/
gpg --encrypt --recipient tenant-admin@example.com ${TENANT_ID}_export_*.tar.gz
```

### 4. Archive Compliance Data (T-3 days)
```bash
# Move compliance data to cold storage (7-year retention)
aws s3 sync s3://lakehouse/${TENANT_ID}/compliance/ \\
  s3://cold-storage-archive/${TENANT_ID}/compliance/ \\
  --storage-class GLACIER_DEEP_ARCHIVE

# Tag for retention policy
aws s3api put-object-tagging \\
  --bucket cold-storage-archive \\
  --key ${TENANT_ID}/compliance/ \\
  --tagging 'TagSet=[{Key=RetentionYears,Value=7},{Key=TenantId,Value='${TENANT_ID}'}]'
```

### 5. Delete Operational Data (T-Day)
```bash
# Delete Pulsar namespaces
for ns in vessel-tracking cargo port-operations analytics; do
  bin/pulsar-admin namespaces delete persistent://${TENANT_ID}/${ns}
done
bin/pulsar-admin tenants delete ${TENANT_ID}

# Drop Cassandra keyspace
cqlsh> DROP KEYSPACE ${TENANT_ID};

# Delete OpenSearch indices
curl -X DELETE "https://opensearch:9200/${TENANT_ID}_*"

# Delete watsonx.data schemas (non-compliance data)
presto --server presto-coordinator:8080 --execute "
DROP SCHEMA IF EXISTS maritime_iceberg.${TENANT_ID} CASCADE;
"

# Delete K8s resources
kubectl delete namespace tenant-${TENANT_ID}
helm uninstall ${TENANT_ID}-services
```

### 6. Remove Access and Monitoring
```bash
# Delete OpenSearch users and roles
curl -X DELETE "https://opensearch:9200/_plugins/_security/api/internalusers/${TENANT_ID}_user"
curl -X DELETE "https://opensearch:9200/_plugins/_security/api/roles/${TENANT_ID}_admin"

# Remove from monitoring
kubectl edit configmap metrics-collector-config -n monitoring
# Remove tenant from tenant_mapping.json

# Delete dashboards
curl -X DELETE "https://opensearch:9200/.kibana/_doc/dashboard:${TENANT_ID}_*"

# Revoke certificates
vault write pki/revoke serial_number=$(vault read -field=serial_number pki/cert/${TENANT_ID})
```

### 7. Cleanup Audit Trail
```bash
# Archive audit logs (7-year retention)
kubectl logs -n security audit-log-consumer --since=720h > /exports/${TENANT_ID}/audit_logs.txt
aws s3 cp /exports/${TENANT_ID}/audit_logs.txt \\
  s3://cold-storage-archive/${TENANT_ID}/audit/ \\
  --storage-class GLACIER_DEEP_ARCHIVE

# Remove from active audit indices (keep in archive)
curl -X POST "https://opensearch:9200/audit_events/_delete_by_query" \\
  -H 'Content-Type: application/json' \\
  -d '{
    "query": {
      "term": { "tenant_id": "'${TENANT_ID}'" }
    }
  }'
```

### 8. Final Validation
```bash
# Verify deletion
bin/pulsar-admin tenants list | grep ${TENANT_ID}  # Should be empty
cqlsh> DESCRIBE KEYSPACES;  # Should not include ${TENANT_ID}
curl -X GET "https://opensearch:9200/_cat/indices?v" | grep ${TENANT_ID}  # Should be empty

# Verify archive
aws s3 ls s3://cold-storage-archive/${TENANT_ID}/compliance/
aws s3 ls s3://exports/${TENANT_ID}_export_*.tar.gz.gpg
```

## Post-Offboarding
- [ ] Confirm data export delivered
- [ ] Document retention location
- [ ] Update tenant registry
- [ ] Close support tickets
- [ ] Archive contract documents

## Emergency Rollback
If offboarding must be reversed within 30 days:
```bash
# Restore from export
./ops/tenant-management/restore_from_export.sh ${TENANT_ID}

# Re-enable services
kubectl scale deployment ${TENANT_ID}-raw-telemetry-ingestion --replicas=3
bin/pulsar-admin namespaces set-subscription-dispatch-rate persistent://${TENANT_ID}/vessel-tracking --dispatch-msg-rate -1
```

## Estimated Time
- Full offboarding: 2-3 hours (spread over 7 days)
- Emergency offboarding: 4-6 hours

## Compliance Notes
- All deletions logged to immutable audit trail
- Archive locations documented in tenant registry
- Deletion certificates generated for GDPR compliance
"""

tenant_offboarding_path = "docs/runbook/tenant-operations/02_tenant_offboarding.md"
with open(tenant_offboarding_path, 'w') as f:
    f.write(tenant_offboarding)

print(f"✓ Created tenant offboarding runbook: {tenant_offboarding_path}")

runbook_tenant_summary = {
    "created_files": [tenant_onboarding_path, tenant_offboarding_path],
    "operations_covered": ["tenant_onboarding", "tenant_offboarding"],
    "next_runbooks": ["ha_failover", "scaling", "dlq_operations"]
}

print(json.dumps(runbook_tenant_summary, indent=2))
