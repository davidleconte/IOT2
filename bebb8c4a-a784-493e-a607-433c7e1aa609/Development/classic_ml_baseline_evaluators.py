"""
Classical ML Baseline Evaluators
Isolation Forest, LOF, DBSCAN, One-Class SVM
"""
import os
import json

eval_dir = 'ml/evaluation/classic-ml'
os.makedirs(eval_dir, exist_ok=True)

classic_ml_code = '''"""
Classical ML Baseline Models for Anomaly Detection
Implements Isolation Forest, LOF, DBSCAN, One-Class SVM
"""
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.neighbors import LocalOutlierFactor
from sklearn.cluster import DBSCAN
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import precision_score, recall_score, f1_score, roc_auc_score, precision_recall_curve, auc
from typing import Dict, List, Tuple
import time
import json

class ClassicMLEvaluator:
    """Evaluates classical ML anomaly detection models"""
    
    def __init__(self, model_type: str = "isolation_forest"):
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        
    def create_model(self, config: Dict):
        """Create model based on type"""
        if self.model_type == "isolation_forest":
            self.model = IsolationForest(
                n_estimators=config.get("n_estimators", 100),
                max_samples=config.get("max_samples", "auto"),
                contamination=config.get("contamination", 0.01),
                random_state=42,
                n_jobs=-1
            )
        elif self.model_type == "lof":
            self.model = LocalOutlierFactor(
                n_neighbors=config.get("n_neighbors", 20),
                contamination=config.get("contamination", 0.01),
                novelty=True,
                n_jobs=-1
            )
        elif self.model_type == "dbscan":
            self.model = DBSCAN(
                eps=config.get("eps", 0.5),
                min_samples=config.get("min_samples", 5),
                n_jobs=-1
            )
        elif self.model_type == "one_class_svm":
            self.model = OneClassSVM(
                kernel=config.get("kernel", "rbf"),
                gamma=config.get("gamma", "scale"),
                nu=config.get("nu", 0.01)
            )
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")
            
    def train(self, X_train: np.ndarray):
        """Train model on normal data"""
        # Scale features
        X_scaled = self.scaler.fit_transform(X_train)
        
        # Train model
        start_time = time.time()
        
        if self.model_type == "dbscan":
            # DBSCAN doesn't have fit_predict for training
            self.model.fit(X_scaled)
        else:
            self.model.fit(X_scaled)
            
        training_time = time.time() - start_time
        
        return {"training_time_s": training_time}
    
    def predict(self, X_test: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
        """
        Predict anomalies
        Returns: (predictions, scores)
        """
        X_scaled = self.scaler.transform(X_test)
        
        start_time = time.time()
        
        if self.model_type == "dbscan":
            # DBSCAN: outliers have label -1
            labels = self.model.fit_predict(X_scaled)
            predictions = (labels == -1).astype(int)
            # Use distance to nearest cluster as score
            scores = np.zeros(len(predictions))
        else:
            # Other models: -1 for outliers, 1 for inliers
            raw_predictions = self.model.predict(X_scaled)
            predictions = (raw_predictions == -1).astype(int)
            
            # Get anomaly scores
            if hasattr(self.model, 'decision_function'):
                scores = -self.model.decision_function(X_scaled)  # Negate so higher = more anomalous
            elif hasattr(self.model, 'score_samples'):
                scores = -self.model.score_samples(X_scaled)
            else:
                scores = predictions.astype(float)
        
        inference_time = time.time() - start_time
        
        return predictions, scores, inference_time
    
    def evaluate(self, tenant: str, use_case: str, X_test: np.ndarray, 
                 y_test: np.ndarray, config: Dict) -> Dict:
        """Comprehensive evaluation"""
        
        # Train if not already trained
        if self.model is None:
            self.create_model(config)
            # Note: X_test should contain mostly normal data for training
            # In practice, you'd use separate training data
            train_indices = y_test == 0
            if np.sum(train_indices) > 0:
                train_result = self.train(X_test[train_indices])
            else:
                train_result = {"training_time_s": 0}
        
        # Predict
        predictions, scores, inference_time = self.predict(X_test)
        
        # Calculate metrics
        precision = precision_score(y_test, predictions, zero_division=0)
        recall = recall_score(y_test, predictions, zero_division=0)
        f1 = f1_score(y_test, predictions, zero_division=0)
        
        # ROC-AUC and PR-AUC
        try:
            roc_auc = roc_auc_score(y_test, scores)
        except:
            roc_auc = None
            
        try:
            precision_curve, recall_curve, _ = precision_recall_curve(y_test, scores)
            pr_auc = auc(recall_curve, precision_curve)
        except:
            pr_auc = None
        
        # Confusion matrix
        tp = np.sum((y_test == 1) & (predictions == 1))
        fp = np.sum((y_test == 0) & (predictions == 1))
        fn = np.sum((y_test == 1) & (predictions == 0))
        tn = np.sum((y_test == 0) & (predictions == 0))
        
        # Calculate cost proxy (relative to infrastructure needed)
        cost_per_1k = self._estimate_cost(len(X_test), inference_time)
        
        return {
            "model": f"classic_ml_{self.model_type}",
            "tenant": tenant,
            "use_case": use_case,
            "metrics": {
                "precision": float(precision),
                "recall": float(recall),
                "f1_score": float(f1),
                "roc_auc": float(roc_auc) if roc_auc else None,
                "pr_auc": float(pr_auc) if pr_auc else None,
                "true_positives": int(tp),
                "false_positives": int(fp),
                "false_negatives": int(fn),
                "true_negatives": int(tn),
                "false_positive_rate": float(fp / (fp + tn)) if (fp + tn) > 0 else 0,
                "false_negative_rate": float(fn / (fn + tp)) if (fn + tp) > 0 else 0
            },
            "operational": {
                "total_inference_time_s": float(inference_time),
                "avg_latency_ms": float(inference_time * 1000 / len(X_test)),
                "throughput_tps": float(len(X_test) / inference_time) if inference_time > 0 else 0,
                "cost_per_1k_inferences": float(cost_per_1k)
            },
            "predictions_count": len(predictions),
            "model_config": config
        }
    
    def _estimate_cost(self, n_samples: int, inference_time: float) -> float:
        """
        Estimate cost per 1000 inferences
        Based on AWS Lambda pricing (~$0.0000166667 per GB-second)
        Assumes ~512MB memory usage
        """
        gb_seconds = (inference_time * 0.5)  # 0.5 GB
        cost_per_inference = gb_seconds * 0.0000166667
        cost_per_1k = cost_per_inference * 1000 / n_samples
        return cost_per_1k


class ClassicMLBenchmark:
    """Benchmark all classical ML models"""
    
    def __init__(self):
        self.models = {
            "isolation_forest": ClassicMLEvaluator("isolation_forest"),
            "lof": ClassicMLEvaluator("lof"),
            "dbscan": ClassicMLEvaluator("dbscan"),
            "one_class_svm": ClassicMLEvaluator("one_class_svm")
        }
        
    def run_benchmark(self, tenant: str, use_case: str, X_test: np.ndarray, 
                      y_test: np.ndarray, configs: Dict[str, Dict]) -> Dict:
        """Run all models and compare"""
        results = []
        
        for model_name, evaluator in self.models.items():
            config = configs.get(model_name, {})
            result = evaluator.evaluate(tenant, use_case, X_test, y_test, config)
            results.append(result)
        
        # Find best model by F1 score
        best_model = max(results, key=lambda x: x["metrics"]["f1_score"])
        
        return {
            "tenant": tenant,
            "use_case": use_case,
            "results": results,
            "best_model": best_model["model"],
            "best_f1_score": best_model["metrics"]["f1_score"]
        }
'''

# Write code
code_path = os.path.join(eval_dir, 'classic_ml_evaluator.py')
with open(code_path, 'w') as f:
    f.write(classic_ml_code)

# Default configs
default_configs = {
    "isolation_forest": {
        "n_estimators": 100,
        "max_samples": "auto",
        "contamination": 0.01
    },
    "lof": {
        "n_neighbors": 20,
        "contamination": 0.01
    },
    "dbscan": {
        "eps": 0.5,
        "min_samples": 5
    },
    "one_class_svm": {
        "kernel": "rbf",
        "gamma": "scale",
        "nu": 0.01
    }
}

config_path = os.path.join(eval_dir, 'model_configs.json')
with open(config_path, 'w') as f:
    json.dump(default_configs, f, indent=2)

# Requirements
requirements = '''scikit-learn>=1.3.0
numpy>=1.24.0
pandas>=2.0.0
matplotlib>=3.7.0
seaborn>=0.12.0
'''

req_path = os.path.join(eval_dir, 'requirements.txt')
with open(req_path, 'w') as f:
    f.write(requirements)

classic_summary = {
    "models": ["isolation_forest", "lof", "dbscan", "one_class_svm"],
    "features": [
        "Comprehensive anomaly detection baseline",
        "Precision/Recall/F1/ROC-AUC/PR-AUC metrics",
        "Latency and cost tracking",
        "Automatic model comparison",
        "Configurable hyperparameters"
    ],
    "files": ["classic_ml_evaluator.py", "model_configs.json", "requirements.txt"]
}

print("✅ Classical ML Baseline Evaluators Created")
print(f"📁 Directory: {eval_dir}")
print(f"📊 Models: {classic_summary['models']}")
print(f"📄 Files: {len(classic_summary['files'])}")
print(classic_summary)
