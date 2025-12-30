import os

# Generate ML training and comparison workflow guide
ml_guide = """
# Step 4: ML Training Pipeline & Model Comparison

## 🎯 Objective
Execute ML training workflows using Feast feature store, Spark-based feature engineering, and MLflow for experiment tracking. Compare multiple model approaches.

## 📂 Location
- **Training Scripts**: `ml/training/`
- **Evaluation Framework**: `ml/evaluation/`
- **Feature Store**: `feast_repo/`

## 🏗️ ML Workflow Sequence

### Phase 1: Feature Store Preparation (10 minutes)

#### 1.1 Initialize Feast Repository
```bash
cd feast_repo

# Review feature definitions
ls -la features/

# Key feature views:
# - operational_features.py: Speed, fuel rate, engine metrics
# - environmental_features.py: Weather, sea conditions
# - aggregation_features.py: Rolling averages, cumulative stats
# - anomaly_features.py: Historical anomaly indicators
```

#### 1.2 Apply Feature Store Configuration
```bash
# Apply all feature views to Feast
feast apply

# Expected output:
# Created entity vessel
# Created entity voyage
# Created feature view operational_features
# Created feature view environmental_features
# Created feature view aggregation_features
# Created feature view anomaly_features
```

**Validation:**
```bash
# List registered features
feast feature-views list

# Verify online store connectivity
feast materialize-incremental $(date -u +"%Y-%m-%dT%H:%M:%S")

# Check Cassandra tables created
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- \
  cqlsh -e "DESCRIBE KEYSPACE feast_online;"
```

**Validation Gate:**
- ✅ All 4 feature views registered
- ✅ Materialization runs successfully
- ✅ Online store tables created in Cassandra

#### 1.3 Materialize Historical Features
```bash
# Materialize last 30 days for training
feast materialize-incremental $(date -d '30 days ago' -u +"%Y-%m-%dT%H:%M:%S") $(date -u +"%Y-%m-%dT%H:%M:%S")

# This populates offline store (Cassandra) with historical feature values
# Duration: ~5 minutes for 30 days of data
```

### Phase 2: Spark Feature Engineering (15 minutes)

#### 2.1 Deploy Spark Feature Engineering Job
```bash
cd ml/training/k8s-spark-jobs

# Review Spark job configuration
cat feature-engineering-spark-job.yaml

# Key parameters:
# - spark.executor.instances: 3
# - spark.executor.memory: 4g
# - spark.sql.adaptive.enabled: true
```

#### 2.2 Submit Feature Engineering Job
```bash
# Submit Spark job to Kubernetes
kubectl apply -f feature-engineering-spark-job.yaml -n navtor-dev

# Monitor job progress
kubectl get sparkapplications -n navtor-dev
kubectl logs -n navtor-dev -l spark-role=driver --tail=100 -f
```

**Job performs:**
- Reads raw telemetry from Cassandra
- Computes rolling window aggregations (1h, 6h, 24h)
- Calculates velocity, acceleration, fuel efficiency metrics
- Joins with weather and port proximity data
- Writes engineered features back to Cassandra

**Validation:**
```bash
# Check job completion
kubectl get sparkapplications -n navtor-dev feature-engineering -o jsonpath='{.status.applicationState.state}'
# Expected: COMPLETED

# Verify output table
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- \
  cqlsh -e "SELECT COUNT(*) FROM shipping_co_alpha.engineered_features;"
# Should return thousands of rows

# Check feature quality
kubectl exec -n cassandra cassandra-dc1-default-sts-0 -- \
  cqlsh -e "SELECT vessel_id, feature_timestamp, avg_fuel_rate_6h, speed_std_dev_1h FROM shipping_co_alpha.engineered_features LIMIT 10;"
```

**Validation Gate:**
- ✅ Spark job completed successfully
- ✅ Engineered features table populated
- ✅ No null values in critical features
- ✅ Feature statistics within expected ranges

### Phase 3: Model Training Workflows (30 minutes)

#### 3.1 Train Classic ML Baseline Models
```bash
cd ml/training/spark-jobs

# Run classical ML training (Random Forest, XGBoost)
spark-submit \
  --master k8s://https://kubernetes.default.svc \
  --deploy-mode cluster \
  --name model-training-baseline \
  --conf spark.kubernetes.namespace=navtor-dev \
  --conf spark.executor.instances=3 \
  model_training.py \
  --model_type baseline \
  --tenant shipping-co-alpha

# Monitor training
kubectl logs -n navtor-dev -l spark-app-name=model-training-baseline -f
```

**Models trained:**
1. **Random Forest** - Baseline ensemble
2. **XGBoost** - Gradient boosting baseline
3. **LightGBM** - Fast gradient boosting

**Training produces:**
- Model artifacts (saved to MLflow)
- Training metrics (accuracy, precision, recall, F1)
- Feature importance rankings
- Cross-validation results

#### 3.2 Train Deep Learning Models
```bash
cd ml/training/deep-learning

# Train LSTM + Transformer model for time-series anomaly detection
python lstm_transformer_anomaly.py \
  --tenant shipping-co-alpha \
  --epochs 50 \
  --batch_size 64 \
  --sequence_length 24 \
  --mlflow_tracking_uri http://mlflow.navtor-dev.svc:5000

# Expected duration: ~15 minutes
```

**Model architecture:**
- LSTM layers for temporal patterns
- Multi-head attention for long-range dependencies
- Fully connected layers for prediction

#### 3.3 Train CNN for Sensor Pattern Recognition
```bash
# Train 1D CNN for sensor pattern classification
python cnn_sensor_pattern.py \
  --tenant shipping-co-alpha \
  --epochs 30 \
  --sensor_types fuel_rate engine_temp speed \
  --mlflow_tracking_uri http://mlflow.navtor-dev.svc:5000

# Expected duration: ~8 minutes
```

#### 3.4 Train Hybrid Ensemble (DL + ML)
```bash
# Train hybrid ensemble combining DL and classical ML
python hybrid_ensemble.py \
  --tenant shipping-co-alpha \
  --base_models lstm_transformer random_forest xgboost \
  --meta_learner logistic_regression \
  --mlflow_tracking_uri http://mlflow.navtor-dev.svc:5000

# Expected duration: ~10 minutes
```

#### 3.5 Train Shared Multi-Tenant Model
```bash
# Train shared model with tenant-specific fine-tuning
python shared_model_training.py \
  --tenants shipping-co-alpha logistics-beta maritime-gamma \
  --shared_layers 3 \
  --tenant_specific_layers 1 \
  --epochs 40 \
  --mlflow_experiment shared_multi_tenant \
  --mlflow_tracking_uri http://mlflow.navtor-dev.svc:5000

# Expected duration: ~12 minutes
```

**Validation:**
```bash
# Check all models registered in MLflow
kubectl port-forward -n mlflow svc/mlflow 5000:5000 &

# Query MLflow API for registered models
curl http://localhost:5000/api/2.0/mlflow/registered-models/list | jq '.registered_models[].name'

# Expected models:
# - shipping-co-alpha-random-forest
# - shipping-co-alpha-xgboost
# - shipping-co-alpha-lightgbm
# - shipping-co-alpha-lstm-transformer
# - shipping-co-alpha-cnn-sensor
# - shipping-co-alpha-hybrid-ensemble
# - shared-multi-tenant-model
```

**Validation Gate:**
- ✅ All 7 models trained successfully
- ✅ Models registered in MLflow
- ✅ Training metrics logged
- ✅ Model artifacts stored in S3

### Phase 4: Model Evaluation & Comparison (20 minutes)

#### 4.1 Run Classic ML Evaluation
```bash
cd ml/evaluation/classic-ml

# Evaluate all classical ML models
python classic_ml_evaluator.py \
  --tenant shipping-co-alpha \
  --models random_forest xgboost lightgbm \
  --test_data_path s3://bucket/test_data/ \
  --output_path results/classic_ml_evaluation.json

# Metrics computed:
# - Accuracy, Precision, Recall, F1
# - ROC AUC, PR AUC
# - Confusion matrix
# - Feature importance
```

#### 4.2 Run Deep Learning Evaluation
```bash
# Evaluate deep learning models
python ../evaluation_orchestrator.py \
  --tenant shipping-co-alpha \
  --models lstm_transformer cnn_sensor hybrid_ensemble \
  --metrics accuracy f1 roc_auc latency \
  --output_path results/dl_evaluation.json
```

#### 4.3 Compare All Models
```bash
# Run comprehensive model comparison
python ../evaluation_orchestrator.py \
  --tenant shipping-co-alpha \
  --compare_all_models \
  --generate_report \
  --output_path results/model_comparison_report.html

# Report includes:
# - Performance metrics table
# - ROC curves for all models
# - Precision-Recall curves
# - Latency vs accuracy tradeoff
# - Model size comparison
# - Recommendation for production deployment
```

**View Results:**
```bash
# Open comparison report
python -m http.server 8000 --directory results/ &
# Navigate to http://localhost:8000/model_comparison_report.html
```

**Expected Rankings (typical):**
1. **Hybrid Ensemble** - Best overall performance (F1: 0.94, AUC: 0.96)
2. **LSTM Transformer** - Best for temporal patterns (F1: 0.92, AUC: 0.95)
3. **XGBoost** - Best classical ML (F1: 0.89, AUC: 0.92)
4. **Shared Multi-Tenant** - Good balance with lower training cost (F1: 0.88, AUC: 0.91)

#### 4.4 Evaluate OpenSearch ML Models
```bash
cd ml/evaluation/opensearch-models

# Evaluate OpenSearch built-in anomaly detection
python opensearch_ad_evaluator.py \
  --tenant shipping-co-alpha \
  --detector_ids fuel_consumption_spikes engine_multi_metric vessel_behavior \
  --ground_truth_path data/anomaly_labels.csv \
  --output_path results/opensearch_ad_evaluation.json

# Metrics:
# - True positive rate
# - False positive rate
# - Anomaly detection latency
```

**Validation Gate:**
- ✅ All models evaluated successfully
- ✅ Performance metrics within acceptable ranges
- ✅ Comparison report generated
- ✅ Production model candidate identified

### Phase 5: Model Deployment & Monitoring Setup (15 minutes)

#### 5.1 Promote Best Model to Production
```bash
# Based on evaluation, promote hybrid ensemble to production
curl -X POST http://localhost:5000/api/2.0/mlflow/registered-models/set-tag \
  -H "Content-Type: application/json" \
  -d '{
    "name": "shipping-co-alpha-hybrid-ensemble",
    "key": "stage",
    "value": "Production"
  }'

# Update inference service configuration
kubectl set env deployment/realtime-inference \
  -n navtor-dev \
  MODEL_NAME=shipping-co-alpha-hybrid-ensemble \
  MODEL_VERSION=latest \
  MODEL_STAGE=Production
```

#### 5.2 Configure Model Monitoring
```bash
cd ml/monitoring

# Deploy model monitoring service
python model_monitoring.py \
  --tenant shipping-co-alpha \
  --model_name shipping-co-alpha-hybrid-ensemble \
  --monitoring_interval 300 \
  --drift_threshold 0.1 \
  --alert_endpoint http://alertmanager.navtor-dev.svc:9093

# Monitoring tracks:
# - Prediction drift
# - Feature distribution drift
# - Model performance degradation
# - Latency and throughput
```

**Validation:**
```bash
# Check monitoring is active
kubectl logs -n navtor-dev deployment/model-monitoring --tail=50

# Query drift metrics
kubectl exec -n opensearch opensearch-master-0 -- \
  curl -k -u admin:admin -X GET "https://localhost:9200/model_monitoring_metrics/_search?pretty" \
  -H 'Content-Type: application/json' -d '{
    "query": {
      "bool": {
        "must": [
          {"term": {"model_name": "shipping-co-alpha-hybrid-ensemble"}},
          {"range": {"timestamp": {"gte": "now-1h"}}}
        ]
      }
    }
  }'
```

**Validation Gate:**
- ✅ Production model deployed to inference service
- ✅ Model monitoring active
- ✅ Drift detection configured
- ✅ Alerting enabled

## 📊 Expected Timeline

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Feature Store Prep | 10 min | 10 min |
| Spark Feature Engineering | 15 min | 25 min |
| Model Training | 30 min | 55 min |
| Evaluation & Comparison | 20 min | 75 min |
| Deployment & Monitoring | 15 min | **90 min** |

## ✅ Success Criteria

- [ ] Feast feature store initialized and materialized
- [ ] Spark feature engineering completed
- [ ] All 7 models trained successfully
- [ ] Models registered in MLflow
- [ ] Model comparison report generated
- [ ] Best model promoted to production
- [ ] Model monitoring active
- [ ] No training failures or errors

---

**Next**: Proceed to [Presto Query Execution](#presto-queries)
"""

# Write the guide
guide_path = 'docs/cookbook/04_ml_training_workflow.md'

with open(guide_path, 'w') as f:
    f.write(ml_guide)

print(f"✅ Created: {guide_path}")
print(f"📄 Guide includes:")
print("   - Feast feature store preparation")
print("   - Spark-based feature engineering")
print("   - Training 7 different model types")
print("   - Comprehensive model evaluation and comparison")
print("   - Production deployment and monitoring setup")
print("   - Expected timeline: ~90 minutes")