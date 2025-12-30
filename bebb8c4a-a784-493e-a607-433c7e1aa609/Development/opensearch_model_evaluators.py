"""
OpenSearch Native Model Evaluators
Implements evaluation clients for OpenSearch AD Plugin and ML Plugin
"""
import os
import json

eval_dir = 'ml/evaluation/opensearch-models'
os.makedirs(eval_dir, exist_ok=True)

# OpenSearch Anomaly Detection Plugin Evaluator
opensearch_ad_evaluator = '''"""
OpenSearch Anomaly Detection Plugin Evaluator
Uses the native AD plugin with Random Cut Forest (RCF)
"""
import requests
import numpy as np
from typing import Dict, List, Tuple
from datetime import datetime
import time

class OpenSearchADEvaluator:
    """Evaluates OpenSearch AD plugin performance"""
    
    def __init__(self, opensearch_url: str, detector_configs: Dict):
        self.opensearch_url = opensearch_url
        self.detector_configs = detector_configs
        self.detectors = {}
        
    def create_detector(self, tenant: str, use_case: str, config: Dict) -> str:
        """Create anomaly detector for tenant/use_case"""
        detector_body = {
            "name": f"{tenant}_{use_case}_detector",
            "description": f"AD detector for {use_case} in {tenant}",
            "time_field": "timestamp",
            "indices": [f"{tenant}_*"],
            "feature_attributes": config.get("features", []),
            "filter_query": config.get("filter", {}),
            "detection_interval": {"period": {"interval": 1, "unit": "Minutes"}},
            "window_delay": {"period": {"interval": 1, "unit": "Minutes"}},
            "shingle_size": config.get("shingle_size", 8),
            "category_field": config.get("category_field")
        }
        
        response = requests.post(
            f"{self.opensearch_url}/_plugins/_anomaly_detection/detectors",
            json=detector_body,
            headers={"Content-Type": "application/json"}
        )
        
        if response.status_code == 200:
            detector_id = response.json()["_id"]
            self.detectors[f"{tenant}_{use_case}"] = detector_id
            return detector_id
        else:
            raise Exception(f"Failed to create detector: {response.text}")
    
    def start_detector(self, detector_id: str):
        """Start real-time detection"""
        response = requests.post(
            f"{self.opensearch_url}/_plugins/_anomaly_detection/detectors/{detector_id}/_start"
        )
        return response.status_code == 200
    
    def get_results(self, detector_id: str, start_time: int, end_time: int) -> List[Dict]:
        """Get detection results for time range"""
        query = {
            "query": {
                "bool": {
                    "must": [
                        {"range": {"data_start_time": {"gte": start_time, "lte": end_time}}}
                    ]
                }
            },
            "sort": [{"data_start_time": {"order": "asc"}}],
            "size": 10000
        }
        
        response = requests.post(
            f"{self.opensearch_url}/_plugins/_anomaly_detection/detectors/{detector_id}/results/_search",
            json=query
        )
        
        if response.status_code == 200:
            hits = response.json()["hits"]["hits"]
            return [hit["_source"] for hit in hits]
        return []
    
    def evaluate(self, tenant: str, use_case: str, ground_truth: Dict) -> Dict:
        """Evaluate detector against ground truth labels"""
        detector_key = f"{tenant}_{use_case}"
        detector_id = self.detectors.get(detector_key)
        
        if not detector_id:
            raise ValueError(f"Detector not found: {detector_key}")
        
        # Get predictions
        results = self.get_results(
            detector_id,
            int(ground_truth["start_time"]),
            int(ground_truth["end_time"])
        )
        
        # Extract predictions and scores
        predictions = []
        scores = []
        latencies = []
        
        for result in results:
            anomaly_grade = result.get("anomaly_grade", 0)
            confidence = result.get("confidence", 0)
            
            # Record latency (processing time)
            if "execution_start_time" in result and "execution_end_time" in result:
                latency = result["execution_end_time"] - result["execution_start_time"]
                latencies.append(latency)
            
            # Binary prediction (threshold at 0.5)
            predictions.append(1 if anomaly_grade > 0.5 else 0)
            scores.append(anomaly_grade)
        
        # Calculate metrics
        y_true = ground_truth["labels"][:len(predictions)]
        y_pred = np.array(predictions)
        
        tp = np.sum((y_true == 1) & (y_pred == 1))
        fp = np.sum((y_true == 0) & (y_pred == 1))
        fn = np.sum((y_true == 1) & (y_pred == 0))
        tn = np.sum((y_true == 0) & (y_pred == 0))
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        return {
            "model": "opensearch_anomaly_detection",
            "tenant": tenant,
            "use_case": use_case,
            "metrics": {
                "precision": float(precision),
                "recall": float(recall),
                "f1_score": float(f1),
                "true_positives": int(tp),
                "false_positives": int(fp),
                "false_negatives": int(fn),
                "true_negatives": int(tn)
            },
            "operational": {
                "avg_latency_ms": float(np.mean(latencies)) if latencies else None,
                "p95_latency_ms": float(np.percentile(latencies, 95)) if latencies else None,
                "p99_latency_ms": float(np.percentile(latencies, 99)) if latencies else None
            },
            "predictions_count": len(predictions)
        }
'''

# OpenSearch ML Plugin Evaluator
opensearch_ml_evaluator = '''"""
OpenSearch ML Plugin Evaluator
Uses ML Commons framework with RCF and custom models
"""
import requests
import numpy as np
from typing import Dict, List

class OpenSearchMLEvaluator:
    """Evaluates OpenSearch ML Plugin models"""
    
    def __init__(self, opensearch_url: str):
        self.opensearch_url = opensearch_url
        self.model_ids = {}
        
    def register_model(self, tenant: str, use_case: str, model_config: Dict) -> str:
        """Register ML model with ML Commons"""
        register_body = {
            "name": f"{tenant}_{use_case}_ml_model",
            "version": "1.0",
            "model_format": model_config.get("format", "TORCH_SCRIPT"),
            "model_config": {
                "model_type": "rcf",
                "embedding_dimension": model_config.get("embedding_dim", 10),
                "time_field": "timestamp",
                "anomaly_rate": model_config.get("anomaly_rate", 0.005)
            }
        }
        
        response = requests.post(
            f"{self.opensearch_url}/_plugins/_ml/models/_register",
            json=register_body
        )
        
        if response.status_code == 200:
            model_id = response.json()["model_id"]
            self.model_ids[f"{tenant}_{use_case}"] = model_id
            return model_id
        else:
            raise Exception(f"Failed to register model: {response.text}")
    
    def deploy_model(self, model_id: str) -> bool:
        """Deploy model for inference"""
        response = requests.post(
            f"{self.opensearch_url}/_plugins/_ml/models/{model_id}/_deploy"
        )
        return response.status_code == 200
    
    def predict(self, model_id: str, data: List[Dict]) -> List[Dict]:
        """Run inference on data"""
        predict_body = {
            "parameters": {
                "data": data
            }
        }
        
        response = requests.post(
            f"{self.opensearch_url}/_plugins/_ml/models/{model_id}/_predict",
            json=predict_body
        )
        
        if response.status_code == 200:
            return response.json()["inference_results"]
        return []
    
    def evaluate(self, tenant: str, use_case: str, test_data: List[Dict], 
                 ground_truth: np.ndarray) -> Dict:
        """Evaluate ML model against ground truth"""
        model_key = f"{tenant}_{use_case}"
        model_id = self.model_ids.get(model_key)
        
        if not model_id:
            raise ValueError(f"Model not found: {model_key}")
        
        # Run predictions
        start_time = time.time()
        results = self.predict(model_id, test_data)
        inference_time = time.time() - start_time
        
        # Extract predictions
        predictions = np.array([r.get("is_anomaly", 0) for r in results])
        scores = np.array([r.get("anomaly_score", 0) for r in results])
        
        # Calculate metrics
        tp = np.sum((ground_truth == 1) & (predictions == 1))
        fp = np.sum((ground_truth == 0) & (predictions == 1))
        fn = np.sum((ground_truth == 1) & (predictions == 0))
        tn = np.sum((ground_truth == 0) & (predictions == 0))
        
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1 = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
        
        return {
            "model": "opensearch_ml_plugin",
            "tenant": tenant,
            "use_case": use_case,
            "metrics": {
                "precision": float(precision),
                "recall": float(recall),
                "f1_score": float(f1),
                "true_positives": int(tp),
                "false_positives": int(fp),
                "false_negatives": int(fn),
                "true_negatives": int(tn)
            },
            "operational": {
                "total_inference_time_s": float(inference_time),
                "avg_latency_ms": float(inference_time * 1000 / len(test_data)),
                "throughput_tps": float(len(test_data) / inference_time)
            },
            "predictions_count": len(predictions)
        }
'''

# Write evaluators
ad_path = os.path.join(eval_dir, 'opensearch_ad_evaluator.py')
with open(ad_path, 'w') as f:
    f.write(opensearch_ad_evaluator)

ml_path = os.path.join(eval_dir, 'opensearch_ml_evaluator.py')
with open(ml_path, 'w') as f:
    f.write(opensearch_ml_evaluator)

# Requirements
requirements = '''requests>=2.31.0
numpy>=1.24.0
scikit-learn>=1.3.0
matplotlib>=3.7.0
'''

req_path = os.path.join(eval_dir, 'requirements.txt')
with open(req_path, 'w') as f:
    f.write(requirements)

opensearch_summary = {
    "evaluators": ["opensearch_ad_evaluator.py", "opensearch_ml_evaluator.py"],
    "features": [
        "OpenSearch AD Plugin with RCF",
        "ML Commons framework support",
        "Real-time detection evaluation",
        "Latency and throughput tracking",
        "Multi-tenant evaluation"
    ],
    "metrics_tracked": ["precision", "recall", "f1_score", "latency_ms", "throughput_tps"]
}

print("✅ OpenSearch Model Evaluators Created")
print(f"📁 Directory: {eval_dir}")
print(f"📄 Files: {len(opensearch_summary['evaluators']) + 1}")
print(f"📊 Features: {len(opensearch_summary['features'])}")
print(opensearch_summary)
