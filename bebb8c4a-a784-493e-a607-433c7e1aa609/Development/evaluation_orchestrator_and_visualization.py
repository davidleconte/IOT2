"""
ML Evaluation Orchestrator and Visualization Dashboard
Runs all models, generates reports with confusion matrices, PR curves, comparison charts
"""
import os
import json

eval_orchestrator_dir = 'ml/evaluation/orchestrator'
os.makedirs(eval_orchestrator_dir, exist_ok=True)

# Main evaluation orchestrator
orchestrator_code = '''"""
ML Evaluation Orchestrator
Coordinates evaluation across all model types and generates comprehensive reports
"""
import numpy as np
import json
import os
from pathlib import Path
from typing import Dict, List
from datetime import datetime
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix, precision_recall_curve, roc_curve, auc

# Zerve color palette
ZERVE_DARK_BG = '#1D1D20'
ZERVE_PRIMARY_TEXT = '#fbfbff'
ZERVE_SECONDARY_TEXT = '#909094'
ZERVE_COLORS = ['#A1C9F4', '#FFB482', '#8DE5A1', '#FF9F9B', '#D0BBFF', 
                '#1F77B4', '#9467BD', '#8C564B', '#C49C94', '#E377C2']
ZERVE_HIGHLIGHT = '#ffd400'
ZERVE_SUCCESS = '#17b26a'
ZERVE_WARNING = '#f04438'

class MLEvaluationOrchestrator:
    """Orchestrates comprehensive ML model evaluation"""
    
    def __init__(self, output_dir: str = 'ml/evaluation/results'):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.results = []
        
    def run_evaluation(self, evaluator, model_name: str, tenant: str, 
                      use_case: str, test_data, ground_truth) -> Dict:
        """Run single model evaluation"""
        print(f"Evaluating {model_name} for {tenant}/{use_case}...")
        
        result = evaluator.evaluate(tenant, use_case, test_data, ground_truth)
        result['model_name'] = model_name
        result['timestamp'] = datetime.now().isoformat()
        
        self.results.append(result)
        return result
    
    def generate_confusion_matrix(self, result: Dict, save_path: str):
        """Generate confusion matrix visualization"""
        metrics = result['metrics']
        cm = np.array([
            [metrics['true_negatives'], metrics['false_positives']],
            [metrics['false_negatives'], metrics['true_positives']]
        ])
        
        fig, ax = plt.subplots(figsize=(8, 6), facecolor=ZERVE_DARK_BG)
        ax.set_facecolor(ZERVE_DARK_BG)
        
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', 
                   xticklabels=['Normal', 'Anomaly'],
                   yticklabels=['Normal', 'Anomaly'],
                   cbar_kws={'label': 'Count'},
                   ax=ax)
        
        ax.set_xlabel('Predicted', color=ZERVE_PRIMARY_TEXT, fontsize=12)
        ax.set_ylabel('Actual', color=ZERVE_PRIMARY_TEXT, fontsize=12)
        ax.set_title(f"Confusion Matrix - {result['model_name']}\\n{result['tenant']} / {result['use_case']}", 
                    color=ZERVE_PRIMARY_TEXT, fontsize=14, pad=20)
        
        ax.tick_params(colors=ZERVE_PRIMARY_TEXT)
        plt.setp(ax.get_xticklabels(), color=ZERVE_PRIMARY_TEXT)
        plt.setp(ax.get_yticklabels(), color=ZERVE_PRIMARY_TEXT)
        
        plt.tight_layout()
        plt.savefig(save_path, facecolor=ZERVE_DARK_BG, dpi=300)
        plt.close()
        
        return save_path
    
    def generate_pr_curve(self, y_true: np.ndarray, y_scores: np.ndarray, 
                         model_name: str, save_path: str):
        """Generate Precision-Recall curve"""
        precision, recall, thresholds = precision_recall_curve(y_true, y_scores)
        pr_auc = auc(recall, precision)
        
        fig, ax = plt.subplots(figsize=(10, 6), facecolor=ZERVE_DARK_BG)
        ax.set_facecolor(ZERVE_DARK_BG)
        
        ax.plot(recall, precision, color=ZERVE_COLORS[0], linewidth=2, 
               label=f'{model_name} (AUC = {pr_auc:.3f})')
        ax.fill_between(recall, precision, alpha=0.2, color=ZERVE_COLORS[0])
        
        ax.set_xlabel('Recall', color=ZERVE_PRIMARY_TEXT, fontsize=12)
        ax.set_ylabel('Precision', color=ZERVE_PRIMARY_TEXT, fontsize=12)
        ax.set_title('Precision-Recall Curve', color=ZERVE_PRIMARY_TEXT, fontsize=14, pad=20)
        ax.legend(loc='best', facecolor=ZERVE_DARK_BG, edgecolor=ZERVE_SECONDARY_TEXT,
                 labelcolor=ZERVE_PRIMARY_TEXT)
        ax.grid(True, alpha=0.2, color=ZERVE_SECONDARY_TEXT)
        ax.tick_params(colors=ZERVE_PRIMARY_TEXT)
        
        plt.tight_layout()
        plt.savefig(save_path, facecolor=ZERVE_DARK_BG, dpi=300)
        plt.close()
        
        return save_path
    
    def generate_comparison_dashboard(self, tenant: str, use_case: str):
        """Generate comprehensive comparison dashboard"""
        # Filter results for this tenant/use_case
        filtered_results = [r for r in self.results 
                          if r['tenant'] == tenant and r['use_case'] == use_case]
        
        if not filtered_results:
            return None
        
        # Create 2x2 subplot dashboard
        fig, axes = plt.subplots(2, 2, figsize=(16, 12), facecolor=ZERVE_DARK_BG)
        fig.suptitle(f'Model Comparison Dashboard\\n{tenant} / {use_case}', 
                    color=ZERVE_PRIMARY_TEXT, fontsize=16, y=0.98)
        
        for ax in axes.flat:
            ax.set_facecolor(ZERVE_DARK_BG)
        
        # 1. F1 Score Comparison
        ax1 = axes[0, 0]
        models = [r['model_name'] for r in filtered_results]
        f1_scores = [r['metrics']['f1_score'] for r in filtered_results]
        
        bars = ax1.barh(models, f1_scores, color=ZERVE_COLORS[:len(models)])
        ax1.set_xlabel('F1 Score', color=ZERVE_PRIMARY_TEXT, fontsize=11)
        ax1.set_title('F1 Score Comparison', color=ZERVE_PRIMARY_TEXT, fontsize=12, pad=10)
        ax1.tick_params(colors=ZERVE_PRIMARY_TEXT)
        ax1.grid(axis='x', alpha=0.2, color=ZERVE_SECONDARY_TEXT)
        plt.setp(ax1.get_yticklabels(), color=ZERVE_PRIMARY_TEXT, fontsize=9)
        plt.setp(ax1.get_xticklabels(), color=ZERVE_PRIMARY_TEXT)
        
        # Add value labels
        for i, (bar, score) in enumerate(zip(bars, f1_scores)):
            ax1.text(score + 0.01, bar.get_y() + bar.get_height()/2, 
                    f'{score:.3f}', va='center', color=ZERVE_PRIMARY_TEXT, fontsize=9)
        
        # 2. Precision vs Recall
        ax2 = axes[0, 1]
        precisions = [r['metrics']['precision'] for r in filtered_results]
        recalls = [r['metrics']['recall'] for r in filtered_results]
        
        for i, (model, prec, rec) in enumerate(zip(models, precisions, recalls)):
            ax2.scatter(rec, prec, s=200, color=ZERVE_COLORS[i], 
                       label=model, alpha=0.7, edgecolors=ZERVE_PRIMARY_TEXT)
        
        ax2.set_xlabel('Recall', color=ZERVE_PRIMARY_TEXT, fontsize=11)
        ax2.set_ylabel('Precision', color=ZERVE_PRIMARY_TEXT, fontsize=11)
        ax2.set_title('Precision vs Recall', color=ZERVE_PRIMARY_TEXT, fontsize=12, pad=10)
        ax2.legend(loc='best', facecolor=ZERVE_DARK_BG, edgecolor=ZERVE_SECONDARY_TEXT,
                  labelcolor=ZERVE_PRIMARY_TEXT, fontsize=8)
        ax2.grid(True, alpha=0.2, color=ZERVE_SECONDARY_TEXT)
        ax2.tick_params(colors=ZERVE_PRIMARY_TEXT)
        plt.setp(ax2.get_xticklabels(), color=ZERVE_PRIMARY_TEXT)
        plt.setp(ax2.get_yticklabels(), color=ZERVE_PRIMARY_TEXT)
        
        # 3. Latency Comparison
        ax3 = axes[1, 0]
        latencies = [r['operational'].get('avg_latency_ms', 0) for r in filtered_results]
        
        bars = ax3.bar(range(len(models)), latencies, color=ZERVE_COLORS[:len(models)])
        ax3.set_ylabel('Latency (ms)', color=ZERVE_PRIMARY_TEXT, fontsize=11)
        ax3.set_title('Average Latency', color=ZERVE_PRIMARY_TEXT, fontsize=12, pad=10)
        ax3.set_xticks(range(len(models)))
        ax3.set_xticklabels(models, rotation=45, ha='right', color=ZERVE_PRIMARY_TEXT, fontsize=9)
        ax3.tick_params(colors=ZERVE_PRIMARY_TEXT)
        ax3.grid(axis='y', alpha=0.2, color=ZERVE_SECONDARY_TEXT)
        plt.setp(ax3.get_yticklabels(), color=ZERVE_PRIMARY_TEXT)
        
        # Add value labels
        for bar, lat in zip(bars, latencies):
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2, height + max(latencies)*0.02,
                    f'{lat:.2f}', ha='center', va='bottom', 
                    color=ZERVE_PRIMARY_TEXT, fontsize=9)
        
        # 4. Cost Comparison
        ax4 = axes[1, 1]
        costs = [r['operational'].get('cost_per_1k_inferences', 0) for r in filtered_results]
        
        bars = ax4.bar(range(len(models)), costs, color=ZERVE_COLORS[:len(models)])
        ax4.set_ylabel('Cost per 1K inferences ($)', color=ZERVE_PRIMARY_TEXT, fontsize=11)
        ax4.set_title('Cost Comparison', color=ZERVE_PRIMARY_TEXT, fontsize=12, pad=10)
        ax4.set_xticks(range(len(models)))
        ax4.set_xticklabels(models, rotation=45, ha='right', color=ZERVE_PRIMARY_TEXT, fontsize=9)
        ax4.tick_params(colors=ZERVE_PRIMARY_TEXT)
        ax4.grid(axis='y', alpha=0.2, color=ZERVE_SECONDARY_TEXT)
        plt.setp(ax4.get_yticklabels(), color=ZERVE_PRIMARY_TEXT)
        
        # Add value labels
        for bar, cost in zip(bars, costs):
            height = bar.get_height()
            ax4.text(bar.get_x() + bar.get_width()/2, height + max(costs)*0.02,
                    f'${cost:.4f}', ha='center', va='bottom', 
                    color=ZERVE_PRIMARY_TEXT, fontsize=9)
        
        plt.tight_layout()
        save_path = self.output_dir / f'comparison_dashboard_{tenant}_{use_case}.png'
        plt.savefig(save_path, facecolor=ZERVE_DARK_BG, dpi=300)
        plt.close()
        
        return str(save_path)
    
    def generate_evaluation_report(self, tenant: str, use_case: str) -> Dict:
        """Generate comprehensive evaluation report with recommendations"""
        filtered_results = [r for r in self.results 
                          if r['tenant'] == tenant and r['use_case'] == use_case]
        
        if not filtered_results:
            return {"error": "No results found"}
        
        # Find best model by different criteria
        best_f1 = max(filtered_results, key=lambda x: x['metrics']['f1_score'])
        best_precision = max(filtered_results, key=lambda x: x['metrics']['precision'])
        best_recall = max(filtered_results, key=lambda x: x['metrics']['recall'])
        best_latency = min(filtered_results, key=lambda x: x['operational'].get('avg_latency_ms', float('inf')))
        best_cost = min(filtered_results, key=lambda x: x['operational'].get('cost_per_1k_inferences', float('inf')))
        
        report = {
            "tenant": tenant,
            "use_case": use_case,
            "evaluation_date": datetime.now().isoformat(),
            "models_evaluated": len(filtered_results),
            "best_performers": {
                "best_overall_f1": {
                    "model": best_f1['model_name'],
                    "f1_score": best_f1['metrics']['f1_score'],
                    "precision": best_f1['metrics']['precision'],
                    "recall": best_f1['metrics']['recall']
                },
                "best_precision": {
                    "model": best_precision['model_name'],
                    "precision": best_precision['metrics']['precision']
                },
                "best_recall": {
                    "model": best_recall['model_name'],
                    "recall": best_recall['metrics']['recall']
                },
                "fastest": {
                    "model": best_latency['model_name'],
                    "avg_latency_ms": best_latency['operational'].get('avg_latency_ms')
                },
                "most_cost_effective": {
                    "model": best_cost['model_name'],
                    "cost_per_1k": best_cost['operational'].get('cost_per_1k_inferences')
                }
            },
            "detailed_results": filtered_results,
            "recommendation": self._generate_recommendation(filtered_results)
        }
        
        # Save report
        report_path = self.output_dir / f'evaluation_report_{tenant}_{use_case}.json'
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report
    
    def _generate_recommendation(self, results: List[Dict]) -> Dict:
        """Generate deployment recommendations"""
        best_f1 = max(results, key=lambda x: x['metrics']['f1_score'])
        
        # Simple recommendation logic
        if best_f1['model_name'].startswith('opensearch'):
            deployment_strategy = "opensearch_native"
            rationale = "OpenSearch native solution provides good performance with minimal infrastructure overhead"
        elif best_f1['model_name'].startswith('deep_learning'):
            deployment_strategy = "realtime_inference"
            rationale = "Deep learning model offers best accuracy but requires dedicated inference infrastructure"
        elif best_f1['model_name'].startswith('hybrid'):
            deployment_strategy = "ensemble_voting"
            rationale = "Hybrid ensemble balances accuracy and reliability through model diversity"
        else:
            deployment_strategy = "batch_inference"
            rationale = "Classical ML provides good baseline performance with lower resource requirements"
        
        return {
            "recommended_model": best_f1['model_name'],
            "deployment_strategy": deployment_strategy,
            "rationale": rationale,
            "expected_f1_score": best_f1['metrics']['f1_score'],
            "expected_latency_ms": best_f1['operational'].get('avg_latency_ms'),
            "expected_cost_per_1k": best_f1['operational'].get('cost_per_1k_inferences')
        }
'''

# Write orchestrator
orchestrator_path = os.path.join(eval_orchestrator_dir, 'evaluation_orchestrator.py')
with open(orchestrator_path, 'w') as f:
    f.write(orchestrator_code)

# Example usage script
usage_script = '''"""
Example: Run Comprehensive ML Evaluation
"""
from evaluation_orchestrator import MLEvaluationOrchestrator
import numpy as np

# Initialize orchestrator
orchestrator = MLEvaluationOrchestrator(output_dir='ml/evaluation/results')

# Example: Load your test data and ground truth
# X_test = load_test_features()
# y_test = load_ground_truth_labels()

# Run evaluations for different models
# results = []

# OpenSearch AD Plugin
# from opensearch_ad_evaluator import OpenSearchADEvaluator
# os_ad = OpenSearchADEvaluator(opensearch_url="https://opensearch:9200", detector_configs={})
# result_ad = orchestrator.run_evaluation(os_ad, "opensearch_ad", "shipping-co-alpha", 
#                                         "fuel_consumption_anomaly", X_test, y_test)

# Classical ML - Isolation Forest
# from classic_ml_evaluator import ClassicMLEvaluator
# iso_forest = ClassicMLEvaluator("isolation_forest")
# result_if = orchestrator.run_evaluation(iso_forest, "classic_ml_isolation_forest",
#                                         "shipping-co-alpha", "fuel_consumption_anomaly", 
#                                         X_test, y_test)

# Generate visualizations
# orchestrator.generate_comparison_dashboard("shipping-co-alpha", "fuel_consumption_anomaly")

# Generate report
# report = orchestrator.generate_evaluation_report("shipping-co-alpha", "fuel_consumption_anomaly")
# print(f"Recommended model: {report['recommendation']['recommended_model']}")
'''

usage_path = os.path.join(eval_orchestrator_dir, 'example_usage.py')
with open(usage_path, 'w') as f:
    f.write(usage_script)

# Requirements
requirements = '''numpy>=1.24.0
matplotlib>=3.7.0
seaborn>=0.12.0
scikit-learn>=1.3.0
pandas>=2.0.0
'''

req_path = os.path.join(eval_orchestrator_dir, 'requirements.txt')
with open(req_path, 'w') as f:
    f.write(requirements)

orchestrator_summary = {
    "components": ["evaluation_orchestrator.py", "example_usage.py", "requirements.txt"],
    "capabilities": [
        "Orchestrate evaluation across all model types",
        "Generate confusion matrices with Zerve styling",
        "Create PR curves and comparison dashboards",
        "Comprehensive evaluation reports (JSON + PNG)",
        "Automatic model recommendations",
        "Multi-tenant and multi-use-case support"
    ],
    "visualizations": [
        "Confusion matrices",
        "Precision-Recall curves",
        "F1 score comparison charts",
        "Precision vs Recall scatter plots",
        "Latency comparison bars",
        "Cost comparison charts"
    ]
}

print("✅ ML Evaluation Orchestrator and Visualization Created")
print(f"📁 Directory: {eval_orchestrator_dir}")
print(f"📊 Capabilities: {len(orchestrator_summary['capabilities'])}")
print(f"📈 Visualizations: {len(orchestrator_summary['visualizations'])}")
print(orchestrator_summary)
