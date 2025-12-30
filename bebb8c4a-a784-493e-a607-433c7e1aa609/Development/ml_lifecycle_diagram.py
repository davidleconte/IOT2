from pathlib import Path

diagrams_dir = Path("docs/diagrams")

# ML Lifecycle Pipeline Diagram
ml_lifecycle_mermaid = """graph TB
    subgraph "Feature Engineering"
        RawData[Raw Telemetry Data<br/>Cassandra + watsonx.data<br/>Historical vessel data]
        FeastRetrieval[Feast Offline Retrieval<br/>Point-in-time correct joins<br/>Training dataset generation]
        SparkFE[Spark Feature Engineering<br/>Window aggregations<br/>Statistical features<br/>Domain transformations]
        FeatureDef[Feature Definitions<br/>operational_features.py<br/>environmental_features.py<br/>aggregation_features.py<br/>anomaly_features.py]
    end
    
    subgraph "Model Training"
        TrainingDataset[Training Dataset<br/>Parquet files<br/>Feature vectors + labels<br/>Train/Val/Test split]
        SparkTraining[PySpark ML Training<br/>GBTClassifier<br/>RandomForest<br/>Cross-validation<br/>Hyperparameter tuning]
        ModelEval[Model Evaluation<br/>Precision/Recall/F1<br/>ROC-AUC<br/>Feature importance<br/>Validation metrics]
    end
    
    subgraph "Model Registry"
        MLflowTracking[MLflow Tracking<br/>Experiment logging<br/>Metrics & artifacts<br/>Parameter tracking]
        MLflowRegistry[MLflow Model Registry<br/>Model versioning<br/>Stage transitions<br/>Staging → Production]
        ModelMetadata[Model Metadata<br/>Training timestamp<br/>Feature list<br/>Performance metrics<br/>Tenant flags]
    end
    
    subgraph "Feature Materialization"
        FeastMaterialization[Feast Materialization<br/>Offline → Online sync<br/>Scheduled batch jobs<br/>Incremental updates]
        OnlineStore[Feast Online Store<br/>Redis/Cassandra<br/>Low-latency serving<br/>< 10ms p99 latency]
    end
    
    subgraph "Model Deployment"
        ModelSelection[Model Selection Logic<br/>Tenant-specific models<br/>Shared baseline model<br/>A/B testing configuration]
        K8sDeployment[Kubernetes Deployment<br/>Real-time inference pods<br/>HPA: 3-20 replicas<br/>Resource limits enforced]
        BatchDeployment[Batch Inference Jobs<br/>Spark on K8s<br/>CronJob schedule<br/>Historical scoring]
    end
    
    subgraph "Inference Serving"
        RealtimeAPI[Real-time Inference API<br/>REST + gRPC endpoints<br/>Feature fetch + predict<br/>Response time < 50ms]
        BatchScoring[Batch Scoring Pipeline<br/>Large-scale predictions<br/>Historical analysis<br/>Scheduled execution]
    end
    
    subgraph "Model Monitoring"
        PredictionLog[Prediction Logging<br/>Input features<br/>Model output<br/>Inference timestamp<br/>tenant_id]
        DriftDetection[Drift Detection<br/>Feature distribution shift<br/>Prediction distribution<br/>Alert on significant drift]
        RetrainingTrigger[Retraining Trigger<br/>Performance degradation<br/>Drift threshold exceeded<br/>Scheduled weekly]
    end
    
    subgraph "Feedback Loop"
        GroundTruth[Ground Truth Labels<br/>Maintenance events<br/>Anomaly confirmations<br/>Expert annotations]
        ModelUpdate[Model Update Pipeline<br/>Retrain with new data<br/>Evaluate improvements<br/>Deploy if better]
    end
    
    %% Feature Engineering Flow
    RawData --> FeastRetrieval
    FeatureDef --> FeastRetrieval
    FeastRetrieval --> SparkFE
    SparkFE --> TrainingDataset
    
    %% Training Flow
    TrainingDataset --> SparkTraining
    SparkTraining --> ModelEval
    ModelEval --> MLflowTracking
    MLflowTracking --> MLflowRegistry
    MLflowRegistry --> ModelMetadata
    
    %% Feature Materialization
    FeatureDef --> FeastMaterialization
    FeastMaterialization --> OnlineStore
    
    %% Deployment Flow
    MLflowRegistry --> ModelSelection
    ModelSelection --> K8sDeployment & BatchDeployment
    
    %% Inference Flow
    K8sDeployment --> RealtimeAPI
    BatchDeployment --> BatchScoring
    OnlineStore --> RealtimeAPI
    RawData --> BatchScoring
    
    %% Monitoring Flow
    RealtimeAPI --> PredictionLog
    BatchScoring --> PredictionLog
    PredictionLog --> DriftDetection
    DriftDetection --> RetrainingTrigger
    
    %% Feedback Loop
    GroundTruth --> ModelUpdate
    RetrainingTrigger --> ModelUpdate
    ModelUpdate --> SparkTraining
    
    style RawData fill:#a1c9f4
    style FeatureDef fill:#a1c9f4
    style SparkFE fill:#ffb482
    style SparkTraining fill:#ffb482
    style TrainingDataset fill:#8de5a1
    style MLflowRegistry fill:#ff9f9b
    style ModelMetadata fill:#ff9f9b
    style OnlineStore fill:#d0bbff
    style RealtimeAPI fill:#ffd400
    style BatchScoring fill:#ffd400
    style DriftDetection fill:#f04438
    style RetrainingTrigger fill:#f04438
    style ModelUpdate fill:#17b26a
"""

# Save Mermaid source
ml_lifecycle_path = diagrams_dir / "04_ml_lifecycle.mmd"
with open(ml_lifecycle_path, 'w') as f:
    f.write(ml_lifecycle_mermaid)

print(f"✅ ML Lifecycle Pipeline Diagram")
print(f"   📄 Mermaid source: {ml_lifecycle_path}")
print(f"   📏 Size: {len(ml_lifecycle_mermaid)} characters")
print(f"\n🤖 ML Lifecycle Stages:")
print(f"   • Feature Engineering: Feast offline retrieval → Spark transformations")
print(f"   • Training: PySpark ML with cross-validation → MLflow tracking")
print(f"   • Registry: Model versioning with stage management (Staging/Production)")
print(f"   • Deployment: K8s real-time inference + Spark batch scoring")
print(f"   • Monitoring: Drift detection → automatic retraining triggers")
print(f"   • Feedback: Ground truth integration → continuous improvement")

ml_lifecycle_summary = {
    "diagram": "ML Lifecycle Pipeline",
    "file": str(ml_lifecycle_path),
    "stages": ["Feature Engineering", "Training", "Registry", "Deployment", "Monitoring", "Feedback"],
    "components": ["Feast", "Spark", "MLflow", "K8s", "Drift Detection"]
}
