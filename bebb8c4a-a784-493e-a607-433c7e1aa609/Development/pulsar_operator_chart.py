import os
import json

# Create Pulsar Operator chart with tenant CRD provisioning
pulsar_chart_dir = "helm/navtor-fleet-guardian/charts/pulsar-operator"
os.makedirs(f"{pulsar_chart_dir}/templates", exist_ok=True)

# Chart.yaml
chart_yaml = """apiVersion: v2
name: pulsar-operator
description: Apache Pulsar with tenant CRD provisioning
version: 1.0.0
appVersion: "3.0.0"
"""

with open(f"{pulsar_chart_dir}/Chart.yaml", "w") as f:
    f.write(chart_yaml)

# values.yaml
values_yaml = """replicas: 3

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
    
image:
  repository: apachepulsar/pulsar
  tag: 3.0.0
  pullPolicy: IfNotPresent
"""

with open(f"{pulsar_chart_dir}/values.yaml", "w") as f:
    f.write(values_yaml)

# Tenant CRD template
tenant_crd = """{{- if .Values.multiTenant.enabled }}
{{- range .Values.global.tenants }}
---
apiVersion: v1
kind: Namespace
metadata:
  name: {{ .id }}-pulsar
  labels:
    tenant: {{ .id }}
    app: pulsar
---
apiVersion: pulsar.apache.org/v1alpha1
kind: PulsarTenant
metadata:
  name: {{ .id }}
  namespace: {{ .id }}-pulsar
spec:
  adminRoles:
    - {{ .id }}-admin
  allowedClusters:
    - {{ $.Values.global.environment }}-cluster
  resourceQuota:
    cpu: {{ .quota.cpu | quote }}
    memory: {{ .quota.memory | quote }}
---
apiVersion: pulsar.apache.org/v1alpha1
kind: PulsarNamespace
metadata:
  name: {{ .id }}-vessel-tracking
  namespace: {{ .id }}-pulsar
spec:
  tenant: {{ .id }}
  namespace: vessel-tracking
  messageTTL: 604800
  retentionTime: 2592000
  retentionSize: 1099511627776
  backlogQuota:
    limit: 10995116277760
    policy: producer_exception
  replicationClusters:
    - {{ $.Values.global.environment }}-cluster
  deduplicationEnabled: true
---
apiVersion: pulsar.apache.org/v1alpha1
kind: PulsarNamespace
metadata:
  name: {{ .id }}-analytics
  namespace: {{ .id }}-pulsar
spec:
  tenant: {{ .id }}
  namespace: analytics
  messageTTL: 259200
  retentionTime: 604800
  retentionSize: 549755813888
  backlogQuota:
    limit: 5497558138880
    policy: producer_request_hold
  replicationClusters:
    - {{ $.Values.global.environment }}-cluster
  deduplicationEnabled: true
---
apiVersion: pulsar.apache.org/v1alpha1
kind: PulsarNamespace
metadata:
  name: {{ .id }}-cargo
  namespace: {{ .id }}-pulsar
spec:
  tenant: {{ .id }}
  namespace: cargo
  messageTTL: 604800
  retentionTime: 2592000
  retentionSize: 274877906944
  backlogQuota:
    limit: 2748779069440
    policy: producer_exception
  replicationClusters:
    - {{ $.Values.global.environment }}-cluster
---
apiVersion: pulsar.apache.org/v1alpha1
kind: PulsarNamespace
metadata:
  name: {{ .id }}-port-operations
  namespace: {{ .id }}-pulsar
spec:
  tenant: {{ .id }}
  namespace: port-operations
  messageTTL: 604800
  retentionTime: 2592000
  retentionSize: 274877906944
  backlogQuota:
    limit: 2748779069440
    policy: producer_exception
  replicationClusters:
    - {{ $.Values.global.environment }}-cluster
{{- end }}
{{- end }}
"""

with open(f"{pulsar_chart_dir}/templates/tenant-provisioning.yaml", "w") as f:
    f.write(tenant_crd)

# Pulsar cluster deployment
cluster_deployment = """apiVersion: pulsar.apache.org/v1alpha1
kind: PulsarCluster
metadata:
  name: {{ .Values.global.environment }}-cluster
  namespace: pulsar-system
spec:
  zookeeper:
    replicas: {{ .Values.replicas }}
    resources:
{{ toYaml .Values.resources.zookeeper | indent 6 }}
    persistence:
      data:
        storageClassName: {{ .Values.global.storageClass }}
        size: 100Gi
  bookkeeper:
    replicas: {{ .Values.replicas }}
    resources:
{{ toYaml .Values.resources.bookkeeper | indent 6 }}
    persistence:
      journal:
        storageClassName: {{ .Values.global.storageClass }}
        size: {{ .Values.persistence.bookkeeper.size }}
      ledgers:
        storageClassName: {{ .Values.global.storageClass }}
        size: {{ .Values.persistence.bookkeeper.size }}
  broker:
    replicas: {{ .Values.replicas }}
    resources:
{{ toYaml .Values.resources.broker | indent 6 }}
    config:
      brokerDeduplicationEnabled: "true"
      brokerDeduplicationMaxNumberOfProducers: "1000000"
      brokerDeduplicationEntriesInterval: "1000"
      maxUnackedMessagesPerConsumer: "50000"
      maxUnackedMessagesPerSubscription: "200000"
  proxy:
    replicas: {{ .Values.replicas }}
    resources:
      requests:
        cpu: "1"
        memory: "2Gi"
      limits:
        cpu: "2"
        memory: "4Gi"
  monitoring:
    prometheus: {{ .Values.global.monitoring.prometheus }}
    grafana: {{ .Values.global.monitoring.grafana }}
"""

with open(f"{pulsar_chart_dir}/templates/pulsar-cluster.yaml", "w") as f:
    f.write(cluster_deployment)

# Helpers template
helpers = """{{- define "pulsar-operator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "pulsar-operator.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "pulsar-operator.labels" -}}
helm.sh/chart: {{ include "pulsar-operator.name" . }}
{{ include "pulsar-operator.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "pulsar-operator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "pulsar-operator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
"""

with open(f"{pulsar_chart_dir}/templates/_helpers.tpl", "w") as f:
    f.write(helpers)

pulsar_chart_summary = {
    "chart": "pulsar-operator",
    "version": "1.0.0",
    "tenant_provisioning": "CRD-based auto-provisioning",
    "namespaces_per_tenant": 4,
    "templates": [
        "tenant-provisioning.yaml",
        "pulsar-cluster.yaml",
        "_helpers.tpl"
    ]
}

print("✅ Created Pulsar Operator chart with tenant CRDs")
print(f"   Chart: {pulsar_chart_summary['chart']}")
print(f"   Tenant provisioning: {pulsar_chart_summary['tenant_provisioning']}")
print(f"   Namespaces per tenant: {pulsar_chart_summary['namespaces_per_tenant']}")
print(json.dumps(pulsar_chart_summary, indent=2))
