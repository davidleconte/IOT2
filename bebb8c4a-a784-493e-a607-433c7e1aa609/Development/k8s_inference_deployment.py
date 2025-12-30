"""
Kubernetes Deployment Manifests for Inference Services
=======================================================
Helm charts for batch and real-time inference services with autoscaling.
Includes SparkApplication for batch and Deployment for real-time API.
"""

import os

k8s_inference_dir = f"{batch_inference_base}/k8s"
os.makedirs(f"{k8s_inference_dir}/batch", exist_ok=True)
os.makedirs(f"{k8s_inference_dir}/realtime", exist_ok=True)

# ========================================
# BATCH INFERENCE - SPARK APPLICATION
# ========================================

batch_spark_job = '''apiVersion: sparkoperator.k8s.io/v1beta2
kind: SparkApplication
metadata:
  name: batch-inference-job
  namespace: maritime-ml
spec:
  type: Python
  pythonVersion: "3"
  mode: cluster
  image: maritime/batch-inference:latest
  imagePullPolicy: Always
  mainApplicationFile: local:///app/batch_inference.py
  
  arguments:
    - "{{ .Values.tenant_id }}"
    - "{{ .Values.use_case }}"
    - "{{ .Values.start_date }}"
    - "{{ .Values.end_date }}"
    - "{{ .Values.model_version | default \\"latest\\" }}"
  
  sparkVersion: "3.4.0"
  restartPolicy:
    type: OnFailure
    onFailureRetries: 3
    onFailureRetryInterval: 10
    onSubmissionFailureRetries: 5
    onSubmissionFailureRetryInterval: 20
  
  driver:
    cores: 2
    coreLimit: "2000m"
    memory: "4g"
    labels:
      version: "3.4.0"
      app: batch-inference
      tenant: "{{ .Values.tenant_id }}"
    serviceAccount: spark-operator-sa
    env:
      - name: MLFLOW_TRACKING_URI
        value: "http://mlflow-service:5000"
      - name: AWS_ACCESS_KEY_ID
        valueFrom:
          secretKeyRef:
            name: s3-credentials
            key: access-key
      - name: AWS_SECRET_ACCESS_KEY
        valueFrom:
          secretKeyRef:
            name: s3-credentials
            key: secret-key
  
  executor:
    cores: 4
    instances: 8
    memory: "8g"
    labels:
      version: "3.4.0"
      app: batch-inference
    env:
      - name: MLFLOW_TRACKING_URI
        value: "http://mlflow-service:5000"
  
  # Iceberg + Spark configuration
  sparkConf:
    "spark.sql.catalog.maritime_iceberg": "org.apache.iceberg.spark.SparkCatalog"
    "spark.sql.catalog.maritime_iceberg.type": "hive"
    "spark.sql.catalog.maritime_iceberg.uri": "thrift://hive-metastore:9083"
    "spark.sql.catalog.maritime_iceberg.warehouse": "s3://maritime-lakehouse/warehouse"
    "spark.hadoop.fs.s3a.endpoint": "https://s3.us-east-1.amazonaws.com"
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem"
    "spark.sql.adaptive.enabled": "true"
    "spark.sql.adaptive.coalescePartitions.enabled": "true"
    "spark.kubernetes.allocation.batch.size": "8"
    "spark.dynamicAllocation.enabled": "false"
  
  deps:
    jars:
      - "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.4_2.12/1.4.2/iceberg-spark-runtime-3.4_2.12-1.4.2.jar"
      - "https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar"
'''

batch_spark_job_path = f"{k8s_inference_dir}/batch/batch-inference-spark-job.yaml"
with open(batch_spark_job_path, 'w') as f:
    f.write(batch_spark_job)

# Batch inference CronJob
batch_cronjob = '''apiVersion: batch/v1
kind: CronJob
metadata:
  name: scheduled-batch-inference
  namespace: maritime-ml
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM UTC
  concurrencyPolicy: Forbid
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      template:
        spec:
          serviceAccountName: spark-operator-sa
          containers:
          - name: spark-submit-trigger
            image: bitnami/kubectl:latest
            command:
            - /bin/bash
            - -c
            - |
              # Get yesterday's date range
              START_DATE=$(date -d "yesterday" +%Y-%m-%d)
              END_DATE=$(date +%Y-%m-%d)
              
              # Run batch inference for all tenants and use cases
              for TENANT in shipping-co-alpha logistics-beta maritime-gamma; do
                for USE_CASE in maintenance fuel eta anomaly; do
                  echo "Triggering batch inference: $TENANT / $USE_CASE"
                  
                  cat <<EOF | kubectl apply -f -
              apiVersion: sparkoperator.k8s.io/v1beta2
              kind: SparkApplication
              metadata:
                name: batch-inference-${TENANT}-${USE_CASE}-$(date +%s)
                namespace: maritime-ml
              spec:
                type: Python
                pythonVersion: "3"
                mode: cluster
                image: maritime/batch-inference:latest
                mainApplicationFile: local:///app/batch_inference.py
                arguments:
                  - "${TENANT}"
                  - "${USE_CASE}"
                  - "${START_DATE}"
                  - "${END_DATE}"
                  - "latest"
                sparkVersion: "3.4.0"
                restartPolicy:
                  type: Never
                driver:
                  cores: 2
                  memory: "4g"
                executor:
                  cores: 4
                  instances: 8
                  memory: "8g"
              EOF
                  
                  sleep 2
                done
              done
          restartPolicy: OnFailure
'''

batch_cronjob_path = f"{k8s_inference_dir}/batch/scheduled-batch-inference.yaml"
with open(batch_cronjob_path, 'w') as f:
    f.write(batch_cronjob)

# ========================================
# REAL-TIME INFERENCE - DEPLOYMENT
# ========================================

realtime_deployment = '''apiVersion: apps/v1
kind: Deployment
metadata:
  name: realtime-inference
  namespace: maritime-ml
  labels:
    app: realtime-inference
    tier: inference
spec:
  replicas: 3
  selector:
    matchLabels:
      app: realtime-inference
  template:
    metadata:
      labels:
        app: realtime-inference
        tier: inference
    spec:
      serviceAccountName: inference-sa
      containers:
      - name: inference-api
        image: maritime/realtime-inference:latest
        imagePullPolicy: Always
        ports:
        - containerPort: 8000
          name: http
          protocol: TCP
        env:
        - name: MLFLOW_TRACKING_URI
          value: "http://mlflow-service:5000"
        - name: FEAST_REPO_PATH
          value: "/app/feast_repo"
        - name: CASSANDRA_HOSTS
          value: "cassandra-node-1,cassandra-node-2,cassandra-node-3"
        - name: CASSANDRA_USERNAME
          valueFrom:
            secretKeyRef:
              name: cassandra-credentials
              key: username
        - name: CASSANDRA_PASSWORD
          valueFrom:
            secretKeyRef:
              name: cassandra-credentials
              key: password
        resources:
          requests:
            cpu: 1000m
            memory: 2Gi
          limits:
            cpu: 2000m
            memory: 4Gi
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
          timeoutSeconds: 5
          failureThreshold: 3
        readinessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5
          timeoutSeconds: 3
          failureThreshold: 2
        volumeMounts:
        - name: feast-config
          mountPath: /app/feast_repo
          readOnly: true
      volumes:
      - name: feast-config
        configMap:
          name: feast-feature-store-config
---
apiVersion: v1
kind: Service
metadata:
  name: realtime-inference-service
  namespace: maritime-ml
spec:
  type: ClusterIP
  selector:
    app: realtime-inference
  ports:
  - port: 8000
    targetPort: 8000
    protocol: TCP
    name: http
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: inference-sa
  namespace: maritime-ml
'''

realtime_deployment_path = f"{k8s_inference_dir}/realtime/realtime-inference-deployment.yaml"
with open(realtime_deployment_path, 'w') as f:
    f.write(realtime_deployment)

# HPA for real-time inference
realtime_hpa = '''apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: realtime-inference-hpa
  namespace: maritime-ml
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: realtime-inference
  minReplicas: 3
  maxReplicas: 20
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
  - type: Pods
    pods:
      metric:
        name: http_requests_per_second
      target:
        type: AverageValue
        averageValue: "1000"
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 50
        periodSeconds: 60
    scaleUp:
      stabilizationWindowSeconds: 60
      policies:
      - type: Percent
        value: 100
        periodSeconds: 30
      - type: Pods
        value: 5
        periodSeconds: 30
      selectPolicy: Max
'''

realtime_hpa_path = f"{k8s_inference_dir}/realtime/realtime-inference-hpa.yaml"
with open(realtime_hpa_path, 'w') as f:
    f.write(realtime_hpa)

# Ingress for external access
realtime_ingress = '''apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: realtime-inference-ingress
  namespace: maritime-ml
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  ingressClassName: nginx
  tls:
  - hosts:
    - inference.maritime-ml.example.com
    secretName: inference-tls
  rules:
  - host: inference.maritime-ml.example.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: realtime-inference-service
            port:
              number: 8000
'''

realtime_ingress_path = f"{k8s_inference_dir}/realtime/realtime-inference-ingress.yaml"
with open(realtime_ingress_path, 'w') as f:
    f.write(realtime_ingress)

# ConfigMap for Feast
feast_configmap = '''apiVersion: v1
kind: ConfigMap
metadata:
  name: feast-feature-store-config
  namespace: maritime-ml
data:
  feature_store.yaml: |
    project: maritime_vessel_ml
    provider: local
    registry:
      registry_type: file
      path: s3://maritime-feast-registry/registry.db
      cache_ttl_seconds: 60
    online_store:
      type: cassandra
      hosts:
        - cassandra-node-1
        - cassandra-node-2
        - cassandra-node-3
      keyspace: feast_online
      port: 9042
      protocol_version: 4
      read_consistency: LOCAL_ONE
      write_consistency: LOCAL_QUORUM
    entity_key_serialization_version: 2
'''

feast_configmap_path = f"{k8s_inference_dir}/realtime/feast-configmap.yaml"
with open(feast_configmap_path, 'w') as f:
    f.write(feast_configmap)

print("=" * 80)
print("KUBERNETES INFERENCE DEPLOYMENT MANIFESTS GENERATED")
print("=" * 80)
print(f"\nBatch Inference (Spark):")
print(f"  • {batch_spark_job_path}")
print(f"  • {batch_cronjob_path}")
print(f"    - SparkApplication for on-demand batch inference")
print(f"    - CronJob for scheduled daily inference")
print(f"\nReal-Time Inference (FastAPI):")
print(f"  • {realtime_deployment_path}")
print(f"  • {realtime_hpa_path}")
print(f"  • {realtime_ingress_path}")
print(f"  • {feast_configmap_path}")
print(f"\nAutoscaling Configuration:")
print(f"  • Min replicas: 3")
print(f"  • Max replicas: 20")
print(f"  • CPU target: 70%")
print(f"  • Memory target: 80%")
print(f"  • Scale-up: Fast (100% increase per 30s)")
print(f"  • Scale-down: Gradual (50% decrease per 60s, 5min stabilization)")
print(f"\nResource Allocation:")
print(f"  Batch Inference:")
print(f"    - Driver: 2 cores, 4GB RAM")
print(f"    - Executors: 8 × (4 cores, 8GB) = 32 cores, 64GB total")
print(f"  Real-Time Inference:")
print(f"    - Per pod: 1-2 cores, 2-4GB RAM")
print(f"    - Total (3-20 replicas): 3-40 cores, 6-80GB RAM")
print("=" * 80)

k8s_inference_summary = {
    "batch_files": [batch_spark_job_path, batch_cronjob_path],
    "realtime_files": [realtime_deployment_path, realtime_hpa_path, realtime_ingress_path, feast_configmap_path],
    "autoscaling": {
        "min_replicas": 3,
        "max_replicas": 20,
        "metrics": ["cpu", "memory", "requests_per_second"]
    }
}
