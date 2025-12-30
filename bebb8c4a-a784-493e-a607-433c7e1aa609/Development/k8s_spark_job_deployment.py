"""
Kubernetes Spark Job Deployment Manifests

Generates K8s manifests for deploying Spark training jobs on watsonx.data
with Gluten acceleration and resource management.
"""

import os
import yaml

# Create K8s deployment directory
k8s_spark_dir = "ml/training/k8s-spark-jobs"
os.makedirs(k8s_spark_dir, exist_ok=True)

# Spark job configuration for feature engineering
feature_eng_spark_job = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "maritime-feature-engineering",
        "namespace": "maritime-ml"
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "maritime/spark-gluten:3.5.0",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "s3a://maritime-ml/scripts/feature_engineering.py",
        "sparkVersion": "3.5.0",
        "restartPolicy": {
            "type": "OnFailure",
            "onFailureRetries": 3,
            "onFailureRetryInterval": 10,
            "onSubmissionFailureRetries": 5,
            "onSubmissionFailureRetryInterval": 20
        },
        "driver": {
            "cores": 2,
            "coreLimit": "2000m",
            "memory": "4g",
            "labels": {
                "version": "3.5.0",
                "app": "maritime-feature-engineering"
            },
            "serviceAccount": "spark-operator"
        },
        "executor": {
            "cores": 4,
            "instances": 5,
            "memory": "8g",
            "labels": {
                "version": "3.5.0",
                "app": "maritime-feature-engineering"
            }
        },
        "sparkConf": {
            "spark.sql.catalog.maritime_iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.maritime_iceberg.type": "hadoop",
            "spark.sql.catalog.maritime_iceberg.warehouse": "s3a://maritime-lakehouse/warehouse",
            "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            "spark.plugins": "io.glutenproject.GlutenPlugin",
            "spark.gluten.enabled": "true",
            "spark.gluten.sql.columnar.backend.lib": "velox",
            "spark.shuffle.manager": "org.apache.spark.shuffle.sort.ColumnarShuffleManager",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.iceberg.vectorization.enabled": "true",
            "spark.kubernetes.allocation.batch.size": "10",
            "spark.kubernetes.allocation.batch.delay": "1s"
        }
    }
}

# Spark job configuration for model training
model_training_spark_job = {
    "apiVersion": "sparkoperator.k8s.io/v1beta2",
    "kind": "SparkApplication",
    "metadata": {
        "name": "maritime-model-training",
        "namespace": "maritime-ml"
    },
    "spec": {
        "type": "Python",
        "pythonVersion": "3",
        "mode": "cluster",
        "image": "maritime/spark-mlflow:3.5.0",
        "imagePullPolicy": "Always",
        "mainApplicationFile": "s3a://maritime-ml/scripts/model_training.py",
        "sparkVersion": "3.5.0",
        "deps": {
            "packages": [
                "org.mlflow:mlflow-spark:2.9.2",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3"
            ]
        },
        "restartPolicy": {
            "type": "OnFailure",
            "onFailureRetries": 2,
            "onFailureRetryInterval": 10
        },
        "driver": {
            "cores": 2,
            "coreLimit": "2000m",
            "memory": "4g",
            "labels": {
                "version": "3.5.0",
                "app": "maritime-model-training"
            },
            "serviceAccount": "spark-operator",
            "env": [
                {
                    "name": "MLFLOW_TRACKING_URI",
                    "value": "http://mlflow-service:5000"
                }
            ]
        },
        "executor": {
            "cores": 4,
            "instances": 8,
            "memory": "8g",
            "labels": {
                "version": "3.5.0",
                "app": "maritime-model-training"
            }
        },
        "sparkConf": {
            "spark.sql.catalog.maritime_iceberg": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.maritime_iceberg.warehouse": "s3a://maritime-lakehouse/warehouse",
            "spark.plugins": "io.glutenproject.GlutenPlugin",
            "spark.gluten.enabled": "true",
            "spark.sql.adaptive.enabled": "true"
        }
    }
}

# CronJob for scheduled training
scheduled_training_cron = {
    "apiVersion": "batch/v1",
    "kind": "CronJob",
    "metadata": {
        "name": "maritime-scheduled-training",
        "namespace": "maritime-ml"
    },
    "spec": {
        "schedule": "0 2 * * 0",  # Weekly on Sunday at 2am
        "jobTemplate": {
            "spec": {
                "template": {
                    "spec": {
                        "serviceAccountName": "spark-operator",
                        "containers": [{
                            "name": "training-trigger",
                            "image": "maritime/spark-submit:latest",
                            "command": [
                                "/bin/bash",
                                "-c",
                                "spark-submit --deploy-mode cluster --master k8s://https://kubernetes.default.svc:443 s3a://maritime-ml/scripts/model_training.py"
                            ],
                            "env": [
                                {
                                    "name": "MLFLOW_TRACKING_URI",
                                    "value": "http://mlflow-service:5000"
                                }
                            ]
                        }],
                        "restartPolicy": "OnFailure"
                    }
                }
            }
        },
        "successfulJobsHistoryLimit": 3,
        "failedJobsHistoryLimit": 1
    }
}

# Save manifests
feature_eng_path = f"{k8s_spark_dir}/feature-engineering-spark-job.yaml"
with open(feature_eng_path, 'w') as f:
    yaml.dump(feature_eng_spark_job, f, default_flow_style=False, sort_keys=False)

training_path = f"{k8s_spark_dir}/model-training-spark-job.yaml"
with open(training_path, 'w') as f:
    yaml.dump(model_training_spark_job, f, default_flow_style=False, sort_keys=False)

scheduled_path = f"{k8s_spark_dir}/scheduled-training-cronjob.yaml"
with open(scheduled_path, 'w') as f:
    yaml.dump(scheduled_training_cron, f, default_flow_style=False, sort_keys=False)

# MLflow tracking configuration
mlflow_config = {
    "apiVersion": "v1",
    "kind": "ConfigMap",
    "metadata": {
        "name": "mlflow-config",
        "namespace": "maritime-ml"
    },
    "data": {
        "mlflow.conf": """
[mlflow]
tracking_uri = http://mlflow-service:5000
registry_uri = postgresql://mlflow:password@mlflow-db:5432/mlflow
default_artifact_root = s3://maritime-ml/mlflow-artifacts

[experiments]
prefix = /maritime_fleet_guardian

[model_registry]
default_tags = tenant_id,use_case,training_mode,feature_count
versioning = auto
"""
    }
}

mlflow_config_path = f"{k8s_spark_dir}/mlflow-config.yaml"
with open(mlflow_config_path, 'w') as f:
    yaml.dump(mlflow_config, f, default_flow_style=False, sort_keys=False)

print("=" * 80)
print("Kubernetes Spark Job Deployment Manifests Generated")
print("=" * 80)
print(f"Directory: {k8s_spark_dir}")
print("\nGenerated Files:")
print(f"  1. {feature_eng_path}")
print(f"     - Feature engineering with Gluten acceleration")
print(f"     - 5 executors with 4 cores and 8GB memory each")
print(f"  2. {training_path}")
print(f"     - Model training with MLflow integration")
print(f"     - 8 executors with 4 cores and 8GB memory each")
print(f"  3. {scheduled_path}")
print(f"     - Weekly scheduled training (Sunday 2am)")
print(f"  4. {mlflow_config_path}")
print(f"     - MLflow tracking configuration")
print("\nDeployment Commands:")
print("  kubectl apply -f ml/training/k8s-spark-jobs/")
print("  kubectl get sparkapplications -n maritime-ml")
print("  kubectl logs <pod-name> -n maritime-ml -f")
print("\nResource Allocation:")
print("  Feature Engineering: ~20 cores, ~40GB memory")
print("  Model Training: ~32 cores, ~64GB memory")
print("  Total Cluster Requirement: ~52 cores, ~104GB memory")
print("=" * 80)

k8s_deployment_summary = {
    "directory": k8s_spark_dir,
    "manifests": [
        "feature-engineering-spark-job.yaml",
        "model-training-spark-job.yaml",
        "scheduled-training-cronjob.yaml",
        "mlflow-config.yaml"
    ],
    "resource_requirements": {
        "feature_engineering": {
            "executors": 5,
            "cores_per_executor": 4,
            "memory_per_executor": "8g"
        },
        "model_training": {
            "executors": 8,
            "cores_per_executor": 4,
            "memory_per_executor": "8g"
        }
    },
    "schedule": "weekly_sunday_2am"
}
