# ML Training Pipeline - Complete Implementation Summary

## Overview
End-to-end ML training pipeline for Maritime Fleet Guardian using **watsonx.data Spark with Gluten**, **Feast offline store**, and **MLflow** for model tracking and registry.

## 🏗️ Architecture Components

### 1. **Feature Retrieval (Feast + Iceberg)**
- **Script**: `ml/training/feast-retrieval/feast_offline_retrieval.py`
- Retrieves historical features from Iceberg offline store via Feast
- Tenant-specific feature isolation
- Time-range based retrieval for training windows
- Support for all 4 use cases with targeted feature selection

### 2. **Feature Engineering (Spark + Gluten)**
- **Script**: `ml/training/spark-jobs/feature_engineering.py`
- Gluten/Velox acceleration for columnar processing
- Spark configuration with Iceberg catalog integration
- Engineered features for each use case:
  - **Predictive Maintenance**: engine_health_index, fuel_efficiency, engine_stress_score
  - **Fuel Optimization**: weather_resistance, fuel_per_nautical_mile, fuel_deviation
  - **ETA Prediction**: effective_speed, estimated_hours_remaining, speed_variability
  - **Anomaly Classification**: total_anomaly_score, anomaly_persistence

### 3. **Model Training (PySpark ML + MLflow)**
- **Script**: `ml/training/spark-jobs/model_training.py`
- Four trained models per tenant:
  1. **Predictive Maintenance** - GBT Classifier (accuracy, F1, precision, recall)
  2. **Fuel Optimization** - GBT Regressor (RMSE, MAE, R²)
  3. **ETA Prediction** - GBT Regressor (RMSE, MAE, R²)
  4. **Anomaly Classification** - Multi-class GBT Classifier (accuracy, F1)
- MLflow experiment tracking: `/maritime_fleet_guardian/{tenant}/{use_case}`
- Model registry with tenant metadata tagging

### 4. **Shared Model Training (Alternative Strategy)**
- **Script**: `ml/training/spark-jobs/shared_model_training.py`
- Single model trained across all tenants
- Tenant identification via one-hot encoded features
- Cross-tenant learning with isolation maintained
- Performance comparison framework (tenant-specific vs shared)

### 5. **Kubernetes Deployment**
- **Directory**: `ml/training/k8s-spark-jobs/`
- SparkApplication manifests for K8s Spark Operator
- Resource allocation:
  - Feature Engineering: 5 executors × 4 cores × 8GB = ~20 cores, ~40GB
  - Model Training: 8 executors × 4 cores × 8GB = ~32 cores, ~64GB
- Scheduled training: Weekly CronJob (Sunday 2am)
- MLflow configuration via ConfigMap

## 📊 Training Pipeline Flow

```
1. Feast Feature Retrieval
   └─> Read offline features from Iceberg (tenant-specific)
   
2. Spark Feature Engineering
   └─> Apply Gluten acceleration
   └─> Engineer domain-specific features
   └─> Write to Iceberg training tables
   
3. Model Training
   └─> Load engineered features
   └─> Train/test split (80/20)
   └─> Build ML pipelines (VectorAssembler → StandardScaler → Model)
   └─> Train models with cross-validation support
   
4. Model Evaluation
   └─> Compute metrics (accuracy, F1, RMSE, R², MAE)
   └─> Log to MLflow experiments
   └─> Per-tenant evaluation
   
5. Model Registration
   └─> Register to MLflow model registry
   └─> Tag with tenant metadata (tenant_id, use_case, training_mode)
   └─> Version control with artifact storage
```

## 🎯 Success Criteria - ✅ ACHIEVED

- ✅ **Feast offline store integration** - Feature retrieval from Iceberg via Feast SDK
- ✅ **Spark with Gluten acceleration** - Full Gluten/Velox configuration for columnar processing
- ✅ **Feature engineering pipeline** - Use-case specific transformations with Spark
- ✅ **Model training for 4 use cases** - Complete training scripts with evaluation
- ✅ **MLflow model registry** - Experiment tracking and model registration with metadata
- ✅ **Tenant-aware metadata** - tenant_id, training_mode, use_case tagging
- ✅ **Tenant-specific + shared models** - Both training strategies implemented
- ✅ **K8s deployment manifests** - Production-ready SparkApplication configs
- ✅ **Resource management** - Proper executor/memory allocation
- ✅ **Scheduled training** - CronJob for automated retraining

## 🚀 Deployment Instructions

### Prerequisites
```bash
# Install Spark Operator
kubectl apply -f https://github.com/GoogleCloudPlatform/spark-on-k8s-operator/releases/download/v1beta2-1.3.8/spark-operator.yaml

# Create namespace
kubectl create namespace maritime-ml
```

### Deploy Training Pipeline
```bash
# Apply all K8s manifests
kubectl apply -f ml/training/k8s-spark-jobs/

# Monitor SparkApplications
kubectl get sparkapplications -n maritime-ml
kubectl describe sparkapplication maritime-model-training -n maritime-ml

# View logs
kubectl logs -n maritime-ml -l app=maritime-model-training -f
```

### Trigger Manual Training
```bash
# Feature engineering
kubectl apply -f ml/training/k8s-spark-jobs/feature-engineering-spark-job.yaml

# Model training
kubectl apply -f ml/training/k8s-spark-jobs/model-training-spark-job.yaml
```

### Access MLflow UI
```bash
kubectl port-forward -n maritime-ml svc/mlflow-service 5000:5000
# Open http://localhost:5000
```

## 📈 Model Performance Tracking

All models tracked in MLflow with:
- **Parameters**: tenant_id, training_mode, use_case, model_type, hyperparameters
- **Metrics**: accuracy, F1, precision, recall (classification) | RMSE, MAE, R² (regression)
- **Artifacts**: Trained Spark ML pipeline models
- **Tags**: Tenant metadata, feature counts, training timestamps

## 🔧 Configuration Files Generated

| File | Purpose | Lines |
|------|---------|-------|
| `feast_offline_retrieval.py` | Feast feature retrieval | 120+ |
| `feature_engineering.py` | Spark feature engineering | 200+ |
| `model_training.py` | Model training with MLflow | 350+ |
| `shared_model_training.py` | Shared model strategy | 250+ |
| `feature-engineering-spark-job.yaml` | K8s SparkApplication | 70+ |
| `model-training-spark-job.yaml` | K8s SparkApplication | 80+ |
| `scheduled-training-cronjob.yaml` | K8s CronJob | 40+ |
| `mlflow-config.yaml` | MLflow ConfigMap | 30+ |

## 💡 Key Features

1. **Gluten Acceleration**: 2-3x faster query processing with Velox backend
2. **Iceberg Integration**: ACID transactions, time travel, schema evolution
3. **Multi-Tenancy**: Isolated training per tenant or shared models with flags
4. **Production-Ready**: K8s native deployment with proper resource management
5. **Experiment Tracking**: Full MLflow integration for reproducibility
6. **Automated Retraining**: Scheduled weekly model updates via CronJob

## 🎓 Training Strategies

### Tenant-Specific Models
- **Pros**: Best per-tenant performance, complete data isolation
- **Cons**: More infrastructure, separate deployments per tenant

### Shared Models with Tenant Flags
- **Pros**: Simplified deployment, cross-tenant learning, reduced infrastructure
- **Cons**: Potential performance tradeoff for some tenants

**Recommendation**: Start with shared models, migrate to tenant-specific if performance gaps > 5%

---

**Status**: ✅ Complete end-to-end training pipeline with Feast, Spark+Gluten, and MLflow integration
