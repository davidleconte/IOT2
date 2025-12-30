import os
import json

# Create parent umbrella chart for the NavTor Fleet Guardian platform
helm_parent_dir = "helm/navtor-fleet-guardian"
os.makedirs(helm_parent_dir, exist_ok=True)

# Parent Chart.yaml
parent_chart_yaml = """apiVersion: v2
name: navtor-fleet-guardian
description: Multi-tenant maritime IoT analytics platform with streaming, lakehouse, ML, and compliance
version: 1.0.0
appVersion: "1.0.0"
type: application
keywords:
  - maritime
  - iot
  - analytics
  - multi-tenant
  - streaming
  - lakehouse
  - ml
maintainers:
  - name: NavTor Platform Team
    email: platform@navtor.com
dependencies:
  - name: pulsar-operator
    version: "1.0.0"
    repository: "file://./charts/pulsar-operator"
    condition: pulsar.enabled
  - name: datastax-hcd
    version: "1.0.0"
    repository: "file://./charts/datastax-hcd"
    condition: cassandra.enabled
  - name: opensearch
    version: "1.0.0"
    repository: "file://./charts/opensearch"
    condition: opensearch.enabled
  - name: watsonx-data
    version: "1.0.0"
    repository: "file://./charts/watsonx-data"
    condition: watsonx.enabled
  - name: feast
    version: "1.0.0"
    repository: "file://./charts/feast"
    condition: feast.enabled
  - name: mlflow
    version: "1.0.0"
    repository: "file://./charts/mlflow"
    condition: mlflow.enabled
  - name: streaming-services
    version: "1.0.0"
    repository: "file://./charts/streaming-services"
    condition: streamingServices.enabled
"""

with open(f"{helm_parent_dir}/Chart.yaml", "w") as f:
    f.write(parent_chart_yaml)

# Parent values.yaml with multi-tenant configuration
parent_values_yaml = """# Global configuration for all charts
global:
  environment: production
  domain: navtor.io
  registry: registry.navtor.io
  imageTag: "1.0.0"
  storageClass: fast-ssd
  monitoring:
    enabled: true
    prometheus: true
    grafana: true
  security:
    tls:
      enabled: true
      certManager: true
    networkPolicies: true
    podSecurityPolicies: true
  tenants:
    - id: shipping-co-alpha
      name: "Shipping Co Alpha"
      region: us-east-1
      quota:
        cpu: "100"
        memory: "200Gi"
        storage: "5Ti"
    - id: logistics-beta
      name: "Logistics Beta"
      region: eu-west-1
      quota:
        cpu: "80"
        memory: "160Gi"
        storage: "3Ti"
    - id: maritime-gamma
      name: "Maritime Gamma"
      region: ap-southeast-1
      quota:
        cpu: "60"
        memory: "120Gi"
        storage: "2Ti"

# Component enablement
pulsar:
  enabled: true
  
cassandra:
  enabled: true
  
opensearch:
  enabled: true
  
watsonx:
  enabled: true
  
feast:
  enabled: true
  
mlflow:
  enabled: true
  
streamingServices:
  enabled: true

# Pulsar configuration
pulsar-operator:
  replicas: 3
  resources:
    bookkeeper:
      requests:
        cpu: "4"
        memory: "16Gi"
      limits:
        cpu: "8"
        memory: "32Gi"
    broker:
      requests:
        cpu: "2"
        memory: "8Gi"
      limits:
        cpu: "4"
        memory: "16Gi"
    zookeeper:
      requests:
        cpu: "1"
        memory: "4Gi"
      limits:
        cpu: "2"
        memory: "8Gi"
  persistence:
    bookkeeper:
      size: 1Ti
      storageClass: fast-ssd
  multiTenant:
    enabled: true
    isolation: namespace
    tenantProvisioning:
      autoCreate: true

# Cassandra configuration
datastax-hcd:
  cluster:
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

# OpenSearch configuration
opensearch:
  replicas:
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

# Watsonx.data configuration
watsonx-data:
  presto:
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

# Feast configuration
feast:
  server:
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

# MLflow configuration
mlflow:
  server:
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

# Streaming services configuration
streaming-services:
  services:
    - name: raw-telemetry-ingestion
      replicas: 6
      resources:
        requests:
          cpu: "2"
          memory: "4Gi"
        limits:
          cpu: "4"
          memory: "8Gi"
    - name: feature-computation
      replicas: 4
      resources:
        requests:
          cpu: "4"
          memory: "8Gi"
        limits:
          cpu: "8"
          memory: "16Gi"
    - name: anomaly-detection
      replicas: 4
      resources:
        requests:
          cpu: "4"
          memory: "8Gi"
        limits:
          cpu: "8"
          memory: "16Gi"
    - name: lakehouse-archival
      replicas: 3
      resources:
        requests:
          cpu: "2"
          memory: "4Gi"
        limits:
          cpu: "4"
          memory: "8Gi"
    - name: dlq-handler
      replicas: 2
      resources:
        requests:
          cpu: "1"
          memory: "2Gi"
        limits:
          cpu: "2"
          memory: "4Gi"
    - name: retry-processor
      replicas: 2
      resources:
        requests:
          cpu: "1"
          memory: "2Gi"
        limits:
          cpu: "2"
          memory: "4Gi"
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 20
    targetCPU: 70
    targetMemory: 80
"""

with open(f"{helm_parent_dir}/values.yaml", "w") as f:
    f.write(parent_values_yaml)

# Environment-specific values files
values_dev = """global:
  environment: development
  domain: dev.navtor.io
  imageTag: "latest"
  monitoring:
    enabled: false

pulsar-operator:
  replicas: 1
  resources:
    bookkeeper:
      requests:
        cpu: "1"
        memory: "4Gi"

datastax-hcd:
  size: 1
  resources:
    requests:
      cpu: "2"
      memory: "8Gi"

opensearch:
  replicas:
    master: 1
    data: 2
    ingest: 1

streaming-services:
  autoscaling:
    enabled: false
"""

values_staging = """global:
  environment: staging
  domain: staging.navtor.io
  imageTag: "stable"
  monitoring:
    enabled: true

pulsar-operator:
  replicas: 2

datastax-hcd:
  size: 2

opensearch:
  replicas:
    master: 2
    data: 3
    ingest: 2
"""

values_prod = """global:
  environment: production
  domain: navtor.io
  imageTag: "1.0.0"
  monitoring:
    enabled: true
    prometheus: true
    grafana: true
  security:
    tls:
      enabled: true
    networkPolicies: true
    podSecurityPolicies: true

# Use full production values from values.yaml
"""

env_dir = f"{helm_parent_dir}/values"
os.makedirs(env_dir, exist_ok=True)

with open(f"{env_dir}/values-dev.yaml", "w") as f:
    f.write(values_dev)

with open(f"{env_dir}/values-staging.yaml", "w") as f:
    f.write(values_staging)

with open(f"{env_dir}/values-prod.yaml", "w") as f:
    f.write(values_prod)

parent_chart_summary = {
    "parent_chart": helm_parent_dir,
    "files_created": [
        "Chart.yaml",
        "values.yaml",
        "values/values-dev.yaml",
        "values/values-staging.yaml", 
        "values/values-prod.yaml"
    ],
    "dependencies": 7,
    "tenants_configured": 3,
    "environments": ["dev", "staging", "prod"]
}

print("✅ Created parent umbrella chart: navtor-fleet-guardian")
print(f"   Dependencies: {parent_chart_summary['dependencies']}")
print(f"   Tenants: {parent_chart_summary['tenants_configured']}")
print(f"   Environments: {', '.join(parent_chart_summary['environments'])}")
print(json.dumps(parent_chart_summary, indent=2))
