"""
Comprehensive ML Evaluation Framework for OpenSearch and Classical ML Models
Compares: OpenSearch Anomaly Detection, OpenSearch ML, Classic ML, Deep Learning, Hybrid approaches
"""
import json
import os
from pathlib import Path

# Define evaluation directory structure
eval_base_dir = 'ml/evaluation'
os.makedirs(eval_base_dir, exist_ok=True)

# Evaluation configuration with all model types and metrics
eval_config = {
    "model_types": {
        "opensearch_anomaly_detection": {
            "name": "OpenSearch Anomaly Detection Plugin",
            "description": "Random Cut Forest (RCF) based anomaly detection from OpenSearch AD plugin",
            "category": "opensearch_native",
            "deployment_mode": "opensearch_plugin"
        },
        "opensearch_ml_plugin": {
            "name": "OpenSearch ML Plugin",
            "description": "ML Commons framework with RCF and custom model support",
            "category": "opensearch_native",
            "deployment_mode": "opensearch_plugin"
        },
        "classic_ml_baseline": {
            "name": "Classic ML Baseline",
            "description": "Isolation Forest, LOF, DBSCAN, One-Class SVM from scikit-learn",
            "category": "classical_ml",
            "deployment_mode": "batch_inference"
        },
        "deep_learning_lstm": {
            "name": "LSTM+Transformer",
            "description": "Deep learning time series anomaly detection with attention",
            "category": "deep_learning",
            "deployment_mode": "realtime_inference"
        },
        "deep_learning_cnn": {
            "name": "CNN Sensor Pattern",
            "description": "Convolutional neural networks for multi-sensor pattern recognition",
            "category": "deep_learning",
            "deployment_mode": "realtime_inference"
        },
        "hybrid_ensemble": {
            "name": "Hybrid Ensemble",
            "description": "Ensemble of OpenSearch AD + Classic ML + Deep Learning with voting",
            "category": "ensemble",
            "deployment_mode": "realtime_inference"
        }
    },
    "metrics": {
        "primary": ["recall", "precision", "f1_score", "pr_auc"],
        "secondary": ["roc_auc", "accuracy", "false_positive_rate", "false_negative_rate"],
        "operational": ["latency_ms", "throughput_tps", "cost_per_1k_inferences"],
        "business": ["detection_delay_minutes", "alert_fatigue_score"]
    },
    "test_scenarios": {
        "single_tenant": ["shipping-co-alpha"],
        "multi_tenant": ["shipping-co-alpha", "logistics-beta", "maritime-gamma"],
        "use_cases": [
            "fuel_consumption_anomaly",
            "engine_failure_prediction",
            "route_deviation_detection",
            "maintenance_alert_classification",
            "co2_emission_anomaly"
        ]
    },
    "evaluation_splits": {
        "train": "2024-01-01 to 2024-09-30",
        "validation": "2024-10-01 to 2024-11-30",
        "test": "2024-12-01 to 2024-12-30"
    }
}

# Write configuration
config_path = os.path.join(eval_base_dir, 'evaluation_config.json')
with open(config_path, 'w') as f:
    json.dump(eval_config, f, indent=2)

print("✅ ML Evaluation Framework Configuration")
print(f"📁 Directory: {eval_base_dir}")
print(f"📊 Model types: {len(eval_config['model_types'])}")
print(f"📈 Metrics tracked: {len(eval_config['metrics']['primary']) + len(eval_config['metrics']['secondary'])}")
print(f"🎯 Test scenarios: {len(eval_config['test_scenarios']['use_cases'])}")
print(f"Configuration saved to: {config_path}")
