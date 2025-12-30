import os
import json

# Create HCD Cassandra chart with keyspace initialization
hcd_chart_dir = "helm/navtor-fleet-guardian/charts/datastax-hcd"
os.makedirs(f"{hcd_chart_dir}/templates", exist_ok=True)

hcd_chart_yaml = """apiVersion: v2
name: datastax-hcd
description: DataStax HCD Cassandra with tenant keyspace provisioning
version: 1.0.0
appVersion: "1.0.0"
"""

hcd_values_yaml = """cluster:
  name: maritime-cluster
  datacenter: dc1

size: 3

resources:
  requests:
    cpu: "8"
    memory: "32Gi"
  limits:
    cpu: "16"
    memory: "64Gi"

storage:
  size: 2Ti
  storageClass: fast-ssd

multiTenant:
  enabled: true
  keyspaceProvisioning:
    autoCreate: true
    replicationStrategy: NetworkTopologyStrategy
    replicationFactor: 3
"""

hcd_init_job = """{{- if .Values.multiTenant.enabled }}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "datastax-hcd.fullname" . }}-init-keyspaces
  annotations:
    "helm.sh/hook": post-install,post-upgrade
    "helm.sh/hook-weight": "5"
    "helm.sh/hook-delete-policy": before-hook-creation
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: init-keyspaces
        image: cassandra:4.1
        command:
        - /bin/bash
        - -c
        - |
          {{- range .Values.global.tenants }}
          echo "Creating keyspace for tenant {{ .id }}..."
          cqlsh {{ $.Values.cluster.name }}-{{ $.Values.cluster.datacenter }}-service -e "
          CREATE KEYSPACE IF NOT EXISTS {{ .id | replace "-" "_" }}
          WITH replication = {
            'class': '{{ $.Values.multiTenant.keyspaceProvisioning.replicationStrategy }}',
            'dc1': {{ $.Values.multiTenant.keyspaceProvisioning.replicationFactor }}
          };
          
          USE {{ .id | replace "-" "_" }};
          
          CREATE TABLE IF NOT EXISTS vessel_telemetry (
            vessel_id text,
            ts timestamp,
            lat double,
            lon double,
            speed double,
            heading double,
            fuel_rate double,
            engine_rpm int,
            PRIMARY KEY ((vessel_id), ts)
          ) WITH CLUSTERING ORDER BY (ts DESC);
          
          CREATE TABLE IF NOT EXISTS vessel_metadata (
            vessel_id text PRIMARY KEY,
            imo_number text,
            vessel_name text,
            vessel_type text,
            flag text,
            dwt double
          );
          
          CREATE TABLE IF NOT EXISTS alerts (
            alert_id uuid,
            vessel_id text,
            ts timestamp,
            alert_type text,
            severity text,
            message text,
            PRIMARY KEY ((vessel_id), ts, alert_id)
          ) WITH CLUSTERING ORDER BY (ts DESC);
          "
          {{- end }}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "datastax-hcd.fullname" . }}-feast-schema
  annotations:
    "helm.sh/hook": post-install,post-upgrade
    "helm.sh/hook-weight": "10"
    "helm.sh/hook-delete-policy": before-hook-creation
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: feast-schema
        image: cassandra:4.1
        command:
        - /bin/bash
        - -c
        - |
          cqlsh {{ $.Values.cluster.name }}-{{ $.Values.cluster.datacenter }}-service -e "
          CREATE KEYSPACE IF NOT EXISTS feast_online
          WITH replication = {
            'class': 'NetworkTopologyStrategy',
            'dc1': 3
          };
          
          USE feast_online;
          
          CREATE TABLE IF NOT EXISTS feature_values (
            entity_key text,
            feature_name text,
            feature_value blob,
            event_ts timestamp,
            created_ts timestamp,
            PRIMARY KEY ((entity_key), feature_name)
          );
          "
{{- end }}
"""

with open(f"{hcd_chart_dir}/Chart.yaml", "w") as f:
    f.write(hcd_chart_yaml)
with open(f"{hcd_chart_dir}/values.yaml", "w") as f:
    f.write(hcd_values_yaml)
with open(f"{hcd_chart_dir}/templates/init-jobs.yaml", "w") as f:
    f.write(hcd_init_job)

# OpenSearch chart with plugin installation
opensearch_chart_dir = "helm/navtor-fleet-guardian/charts/opensearch"
os.makedirs(f"{opensearch_chart_dir}/templates", exist_ok=True)

opensearch_chart_yaml = """apiVersion: v2
name: opensearch
description: OpenSearch with ML/KNN plugins and tenant index templates
version: 1.0.0
appVersion: "2.11.0"
"""

opensearch_values_yaml = """replicas:
  master: 3
  data: 6
  ingest: 3

resources:
  master:
    requests:
      cpu: "2"
      memory: "8Gi"
    limits:
      cpu: "4"
      memory: "16Gi"
  data:
    requests:
      cpu: "8"
      memory: "32Gi"
    limits:
      cpu: "16"
      memory: "64Gi"

storage:
  data:
    size: 2Ti
    storageClass: fast-ssd

plugins:
  - opensearch-ml
  - opensearch-knn
  - opensearch-anomaly-detection

multiTenant:
  enabled: true
  indexTemplateProvisioning: true
  rbacProvisioning: true
"""

opensearch_init_job = """{{- if .Values.multiTenant.enabled }}
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "opensearch.fullname" . }}-tenant-setup
  annotations:
    "helm.sh/hook": post-install,post-upgrade
    "helm.sh/hook-weight": "10"
    "helm.sh/hook-delete-policy": before-hook-creation
spec:
  template:
    spec:
      restartPolicy: OnFailure
      containers:
      - name: tenant-setup
        image: curlimages/curl:latest
        command:
        - /bin/sh
        - -c
        - |
          {{- range .Values.global.tenants }}
          # Create index templates for {{ .id }}
          curl -X PUT "opensearch-master:9200/_index_template/{{ .id }}_vessel_telemetry" \\
            -H 'Content-Type: application/json' -d'
          {
            "index_patterns": ["{{ .id }}_vessel_telemetry-*"],
            "template": {
              "settings": {
                "number_of_shards": 6,
                "number_of_replicas": 2,
                "index.codec": "best_compression"
              },
              "mappings": {
                "properties": {
                  "vessel_id": { "type": "keyword" },
                  "timestamp": { "type": "date" },
                  "location": { "type": "geo_point" },
                  "speed": { "type": "float" },
                  "fuel_rate": { "type": "float" }
                }
              }
            }
          }'
          
          # Create RBAC roles
          curl -X PUT "opensearch-master:9200/_plugins/_security/api/roles/{{ .id }}_admin" \\
            -H 'Content-Type: application/json' -d'
          {
            "cluster_permissions": ["cluster_composite_ops"],
            "index_permissions": [{
              "index_patterns": ["{{ .id }}_*"],
              "allowed_actions": ["indices_all"]
            }]
          }'
          {{- end }}
{{- end }}
"""

with open(f"{opensearch_chart_dir}/Chart.yaml", "w") as f:
    f.write(opensearch_chart_yaml)
with open(f"{opensearch_chart_dir}/values.yaml", "w") as f:
    f.write(opensearch_values_yaml)
with open(f"{opensearch_chart_dir}/templates/tenant-setup-job.yaml", "w") as f:
    f.write(opensearch_init_job)

# Feast chart
feast_chart_dir = "helm/navtor-fleet-guardian/charts/feast"
os.makedirs(f"{feast_chart_dir}/templates", exist_ok=True)

feast_chart_yaml = """apiVersion: v2
name: feast
description: Feast feature store with multi-tenant support
version: 1.0.0
appVersion: "0.34.0"
"""

feast_values_yaml = """server:
  replicas: 3
  resources:
    requests:
      cpu: "2"
      memory: "8Gi"
    limits:
      cpu: "4"
      memory: "16Gi"

online:
  provider: cassandra

offline:
  provider: spark

registry:
  provider: sql
  database: postgresql

multiTenant:
  enabled: true
  featureViewIsolation: true
"""

with open(f"{feast_chart_dir}/Chart.yaml", "w") as f:
    f.write(feast_chart_yaml)
with open(f"{feast_chart_dir}/values.yaml", "w") as f:
    f.write(feast_values_yaml)

# MLflow chart
mlflow_chart_dir = "helm/navtor-fleet-guardian/charts/mlflow"
os.makedirs(f"{mlflow_chart_dir}/templates", exist_ok=True)

mlflow_chart_yaml = """apiVersion: v2
name: mlflow
description: MLflow tracking server with multi-tenant experiment isolation
version: 1.0.0
appVersion: "2.9.0"
"""

mlflow_values_yaml = """server:
  replicas: 2
  resources:
    requests:
      cpu: "2"
      memory: "8Gi"
    limits:
      cpu: "4"
      memory: "16Gi"

tracking:
  backend: postgresql
  artifact:
    root: s3://maritime-mlflow/artifacts

registry:
  backend: postgresql

multiTenant:
  enabled: true
  experimentIsolation: true
"""

with open(f"{mlflow_chart_dir}/Chart.yaml", "w") as f:
    f.write(mlflow_chart_yaml)
with open(f"{mlflow_chart_dir}/values.yaml", "w") as f:
    f.write(mlflow_values_yaml)

# Watsonx chart
watsonx_chart_dir = "helm/navtor-fleet-guardian/charts/watsonx-data"
os.makedirs(f"{watsonx_chart_dir}/templates", exist_ok=True)

watsonx_chart_yaml = """apiVersion: v2
name: watsonx-data
description: Watsonx.data lakehouse with tenant schema provisioning
version: 1.0.0
appVersion: "1.0.0"
"""

watsonx_values_yaml = """presto:
  coordinator:
    replicas: 2
    resources:
      requests:
        cpu: "4"
        memory: "16Gi"
      limits:
        cpu: "8"
        memory: "32Gi"
  worker:
    replicas: 6
    resources:
      requests:
        cpu: "8"
        memory: "32Gi"
      limits:
        cpu: "16"
        memory: "64Gi"

storage:
  s3:
    bucket: maritime-lakehouse
    region: us-east-1
  iceberg:
    enabled: true
    catalogType: hive

multiTenant:
  enabled: true
  schemaProvisioning: true
"""

with open(f"{watsonx_chart_dir}/Chart.yaml", "w") as f:
    f.write(watsonx_chart_yaml)
with open(f"{watsonx_chart_dir}/values.yaml", "w") as f:
    f.write(watsonx_values_yaml)

charts_summary = {
    "charts_created": [
        "datastax-hcd",
        "opensearch",
        "feast",
        "mlflow",
        "watsonx-data"
    ],
    "init_jobs": {
        "cassandra": ["keyspace-init", "feast-schema"],
        "opensearch": ["tenant-setup"]
    },
    "tenant_provisioning": "Automated via Helm hooks"
}

print("✅ Created component charts with tenant provisioning")
print(f"   Charts: {', '.join(charts_summary['charts_created'])}")
print(f"   Init jobs: Cassandra={len(charts_summary['init_jobs']['cassandra'])}, OpenSearch={len(charts_summary['init_jobs']['opensearch'])}")
print(json.dumps(charts_summary, indent=2))
