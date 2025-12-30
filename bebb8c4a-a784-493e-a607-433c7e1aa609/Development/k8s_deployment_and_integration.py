import os

# Create K8s deployment manifests for all components
k8s_dir = "monitoring/k8s"
os.makedirs(k8s_dir, exist_ok=True)

# Metrics collector deployment
collector_deployment = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: metrics-collector
  namespace: monitoring
spec:
  replicas: 3
  selector:
    matchLabels:
      app: metrics-collector
  template:
    metadata:
      labels:
        app: metrics-collector
    spec:
      serviceAccountName: metrics-collector
      containers:
      - name: collector
        image: metrics-collector:latest
        env:
        - name: OPENSEARCH_HOST
          value: "opensearch:9200"
        - name: OPENSEARCH_USER
          valueFrom:
            secretKeyRef:
              name: opensearch-credentials
              key: username
        - name: OPENSEARCH_PASSWORD
          valueFrom:
            secretKeyRef:
              name: opensearch-credentials
              key: password
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        volumeMounts:
        - name: config
          mountPath: /app/config
      volumes:
      - name: config
        configMap:
          name: metrics-collector-config
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: metrics-collector
  namespace: monitoring
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: metrics-collector
rules:
- apiGroups: [""]
  resources: ["pods", "nodes", "services"]
  verbs: ["get", "list", "watch"]
- apiGroups: ["metrics.k8s.io"]
  resources: ["pods", "nodes"]
  verbs: ["get", "list"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: metrics-collector
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: metrics-collector
subjects:
- kind: ServiceAccount
  name: metrics-collector
  namespace: monitoring
"""

# Cost calculation engine deployment
cost_engine_deployment = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: cost-calculation-engine
  namespace: monitoring
spec:
  replicas: 2
  selector:
    matchLabels:
      app: cost-engine
  template:
    metadata:
      labels:
        app: cost-engine
    spec:
      containers:
      - name: engine
        image: cost-engine:latest
        env:
        - name: OPENSEARCH_HOST
          value: "opensearch:9200"
        - name: OPENSEARCH_INDEX
          value: "tenant-costs"
        resources:
          requests:
            memory: "256Mi"
            cpu: "250m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        volumeMounts:
        - name: pricing-config
          mountPath: /app/config
      volumes:
      - name: pricing-config
        configMap:
          name: pricing-models
"""

# Report generator deployment
report_generator_deployment = """apiVersion: apps/v1
kind: Deployment
metadata:
  name: report-generator
  namespace: monitoring
spec:
  replicas: 1
  selector:
    matchLabels:
      app: report-generator
  template:
    metadata:
      labels:
        app: report-generator
    spec:
      containers:
      - name: generator
        image: report-generator:latest
        env:
        - name: OPENSEARCH_HOST
          value: "opensearch:9200"
        - name: REPORT_OUTPUT_DIR
          value: "/reports"
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "1Gi"
            cpu: "1000m"
        volumeMounts:
        - name: reports
          mountPath: /reports
      volumes:
      - name: reports
        persistentVolumeClaim:
          claimName: report-storage
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: report-storage
  namespace: monitoring
spec:
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 20Gi
  storageClassName: standard
"""

# ConfigMaps
metrics_configmap = """apiVersion: v1
kind: ConfigMap
metadata:
  name: metrics-collector-config
  namespace: monitoring
data:
  collector_config.json: |
    {
      "sources": ["pulsar", "cassandra_hcd", "opensearch", "watsonx_data", "object_storage", "network"],
      "aggregation_window": "1h",
      "storage": {
        "backend": "opensearch",
        "index_pattern": "tenant-metrics-{YYYY.MM}",
        "retention_days": 395
      },
      "collection_workers": 3,
      "batch_size": 1000
    }
  tenant_mapping.json: |
    {
      "shipping-co-alpha": {
        "tenant_id": "shipping-co-alpha",
        "pulsar_tenant": "shipping-co-alpha",
        "cassandra_keyspaces": ["shipping_co_alpha"],
        "opensearch_indices": ["shipping-co-alpha-*"],
        "presto_users": ["shipping_alpha_user"],
        "s3_buckets": ["fleet-data-shipping-alpha"],
        "k8s_namespaces": ["shipping-alpha"]
      },
      "maritime-gamma": {
        "tenant_id": "maritime-gamma",
        "pulsar_tenant": "maritime-gamma",
        "cassandra_keyspaces": ["maritime_gamma"],
        "opensearch_indices": ["maritime-gamma-*"],
        "presto_users": ["maritime_gamma_user"],
        "s3_buckets": ["fleet-data-maritime-gamma"],
        "k8s_namespaces": ["maritime-gamma"]
      },
      "logistics-beta": {
        "tenant_id": "logistics-beta",
        "pulsar_tenant": "logistics-beta",
        "cassandra_keyspaces": ["logistics_beta"],
        "opensearch_indices": ["logistics-beta-*"],
        "presto_users": ["logistics_beta_user"],
        "s3_buckets": ["fleet-data-logistics-beta"],
        "k8s_namespaces": ["logistics-beta"]
      }
    }
"""

pricing_configmap = """apiVersion: v1
kind: ConfigMap
metadata:
  name: pricing-models
  namespace: monitoring
data:
  pricing_models.json: |
    {
      "pulsar": {
        "pricing_units": [
          {"metric": "topic_bytes_in", "rate": 0.09, "unit": "GB"},
          {"metric": "topic_bytes_out", "rate": 0.12, "unit": "GB"},
          {"metric": "topic_storage_size", "rate": 0.025, "unit": "GB"}
        ]
      },
      "cassandra_hcd": {
        "pricing_units": [
          {"metric": "read_operations", "rate": 0.00025, "unit": "operation"},
          {"metric": "write_operations", "rate": 0.00125, "unit": "operation"},
          {"metric": "storage_bytes", "rate": 0.10, "unit": "GB"}
        ]
      },
      "opensearch": {
        "pricing_units": [
          {"metric": "index_size_bytes", "rate": 0.024, "unit": "GB"},
          {"metric": "search_query_total", "rate": 0.0005, "unit": "query"}
        ]
      },
      "watsonx_data": {
        "pricing_units": [
          {"metric": "presto_scan_bytes", "rate": 5.0, "unit": "TB"},
          {"metric": "spark_job_duration", "rate": 0.10, "unit": "vcpu_hour"}
        ]
      }
    }
"""

# Save all K8s manifests
manifests = {
    "metrics-collector-deployment.yaml": collector_deployment,
    "cost-engine-deployment.yaml": cost_engine_deployment,
    "report-generator-deployment.yaml": report_generator_deployment,
    "metrics-collector-configmap.yaml": metrics_configmap,
    "pricing-configmap.yaml": pricing_configmap
}

manifest_files = []
for filename, content in manifests.items():
    filepath = os.path.join(k8s_dir, filename)
    with open(filepath, 'w') as f:
        f.write(content)
    manifest_files.append(filepath)

# Create integration script
integration_script = '''#!/bin/bash
# Integration script to wire metrics collection with cost calculation

set -e

echo "Setting up cross-tenant showback/chargeback system..."

# Create namespace
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

# Apply ConfigMaps
echo "Applying configurations..."
kubectl apply -f k8s/metrics-collector-configmap.yaml
kubectl apply -f k8s/pricing-configmap.yaml

# Create OpenSearch index templates
echo "Creating OpenSearch index templates..."
cat <<EOF | curl -X PUT "opensearch:9200/_index_template/tenant-metrics" -H 'Content-Type: application/json' -d @-
{
  "index_patterns": ["tenant-metrics-*"],
  "template": {
    "settings": {
      "number_of_shards": 3,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "tenant_id": {"type": "keyword"},
        "service_name": {"type": "keyword"},
        "metric_name": {"type": "keyword"},
        "value": {"type": "double"},
        "timestamp": {"type": "date"}
      }
    }
  }
}
EOF

cat <<EOF | curl -X PUT "opensearch:9200/_index_template/tenant-costs" -H 'Content-Type: application/json' -d @-
{
  "index_patterns": ["tenant-costs-*"],
  "template": {
    "settings": {
      "number_of_shards": 2,
      "number_of_replicas": 1
    },
    "mappings": {
      "properties": {
        "tenant_id": {"type": "keyword"},
        "service_name": {"type": "keyword"},
        "metric_name": {"type": "keyword"},
        "billable_value": {"type": "double"},
        "cost": {"type": "double"},
        "total_cost": {"type": "double"},
        "timestamp": {"type": "date"}
      }
    }
  }
}
EOF

# Deploy services
echo "Deploying services..."
kubectl apply -f k8s/metrics-collector-deployment.yaml
kubectl apply -f k8s/cost-engine-deployment.yaml
kubectl apply -f k8s/report-generator-deployment.yaml

# Wait for deployments
echo "Waiting for deployments to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/metrics-collector -n monitoring
kubectl wait --for=condition=available --timeout=300s deployment/cost-calculation-engine -n monitoring
kubectl wait --for=condition=available --timeout=300s deployment/report-generator -n monitoring

echo "Cross-tenant showback/chargeback system deployed successfully!"
echo "Dashboards available at: http://opensearch-dashboards:5601"
echo "Reports will be generated monthly in: /reports"
'''

integration_file = os.path.join(k8s_dir, "deploy.sh")
with open(integration_file, 'w') as f:
    f.write(integration_script)
os.chmod(integration_file, 0o755)

k8s_summary = {
    "deployments": 3,
    "configmaps": 2,
    "manifests": len(manifest_files) + 1,
    "components": [
        "Metrics Collector (3 replicas)",
        "Cost Calculation Engine (2 replicas)",
        "Report Generator (1 replica)"
    ],
    "files": manifest_files + [integration_file]
}

print("K8s Deployment and Integration Created")
print(f"Deployments: {k8s_summary['deployments']}")
print(f"ConfigMaps: {k8s_summary['configmaps']}")
print(f"Components: {', '.join(k8s_summary['components'])}")
print(f"Total files: {len(k8s_summary['files'])}")
