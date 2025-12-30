"""
Hybrid Ensemble: Deep Learning + Classic ML
Combines LSTM, Transformer, CNN with GBT for robust anomaly detection
with iterative improvement tracking
"""

import os

hybrid_ensemble_script = '''"""
Hybrid Ensemble Model - Combining Deep Learning and Classic ML
Stacked ensemble with LSTM, Transformer, CNN, and GBT for maritime anomaly detection
"""

import torch
import torch.nn as nn
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import precision_recall_fscore_support, average_precision_score, roc_curve, auc
from sklearn.model_selection import TimeSeriesSplit
from pyspark.ml.classification import GBTClassifier as SparkGBTClassifier
from pyspark.ml.feature import VectorAssembler
import mlflow
import mlflow.pytorch
import mlflow.sklearn
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

mlflow.set_tracking_uri("http://mlflow-service:5000")

class HybridEnsemble:
    """Stacked ensemble combining DL and ML models"""
    
    def __init__(self, lstm_model, transformer_model, cnn1d_model, device='cpu'):
        self.lstm_model = lstm_model
        self.transformer_model = transformer_model
        self.cnn1d_model = cnn1d_model
        self.device = device
        self.meta_classifier = None
        
        # Set models to eval mode
        self.lstm_model.eval()
        self.transformer_model.eval()
        self.cnn1d_model.eval()
    
    def extract_dl_features(self, sequences):
        """Extract predictions from all DL models as features"""
        
        sequences_tensor = torch.FloatTensor(sequences).to(self.device)
        
        with torch.no_grad():
            # LSTM predictions
            lstm_out, _ = self.lstm_model(sequences_tensor)
            lstm_probs = torch.softmax(lstm_out, dim=1).cpu().numpy()
            
            # Transformer predictions
            transformer_out = self.transformer_model(sequences_tensor)
            transformer_probs = torch.softmax(transformer_out, dim=1).cpu().numpy()
            
            # CNN predictions (need to transpose for CNN1D: batch, channels, length)
            cnn_input = sequences_tensor.transpose(1, 2)
            cnn_out = self.cnn1d_model(cnn_input)
            cnn_probs = torch.softmax(cnn_out, dim=1).cpu().numpy()
        
        # Combine all features: [lstm_prob_0, lstm_prob_1, transformer_prob_0, ...]
        dl_features = np.concatenate([lstm_probs, transformer_probs, cnn_probs], axis=1)
        return dl_features
    
    def train_meta_classifier(self, train_sequences, train_labels, val_sequences, val_labels):
        """Train meta-classifier on DL model outputs"""
        
        print("Extracting DL features for meta-classifier training...")
        train_dl_features = self.extract_dl_features(train_sequences)
        val_dl_features = self.extract_dl_features(val_sequences)
        
        # Train GBT meta-classifier
        self.meta_classifier = GradientBoostingClassifier(
            n_estimators=200,
            learning_rate=0.1,
            max_depth=5,
            subsample=0.8,
            random_state=42
        )
        
        print("Training meta-classifier...")
        self.meta_classifier.fit(train_dl_features, train_labels)
        
        # Evaluate
        val_preds = self.meta_classifier.predict(val_dl_features)
        val_probs = self.meta_classifier.predict_proba(val_dl_features)[:, 1]
        
        precision, recall, f1, _ = precision_recall_fscore_support(
            val_labels, val_preds, average='binary', zero_division=0
        )
        pr_auc = average_precision_score(val_labels, val_probs)
        
        return {
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "pr_auc": pr_auc
        }
    
    def predict(self, sequences):
        """Make predictions using ensemble"""
        dl_features = self.extract_dl_features(sequences)
        return self.meta_classifier.predict(dl_features)
    
    def predict_proba(self, sequences):
        """Get probability predictions"""
        dl_features = self.extract_dl_features(sequences)
        return self.meta_classifier.predict_proba(dl_features)

def compare_baselines_and_ensemble(
    test_sequences, test_labels, 
    lstm_model, transformer_model, cnn1d_model,
    gbt_baseline_model, ensemble, device, tenant_id
):
    """Compare baseline models with ensemble and show iterative improvement"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/model_comparison"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"comparison_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        results = {}
        
        # Baseline 1: LSTM
        print("\\nEvaluating LSTM baseline...")
        lstm_model.eval()
        with torch.no_grad():
            lstm_out, _ = lstm_model(torch.FloatTensor(test_sequences).to(device))
            lstm_probs = torch.softmax(lstm_out, dim=1)[:, 1].cpu().numpy()
            lstm_preds = (lstm_probs > 0.5).astype(int)
        
        lstm_metrics = calculate_metrics(test_labels, lstm_preds, lstm_probs)
        results['LSTM'] = lstm_metrics
        log_metrics_with_prefix(lstm_metrics, "lstm")
        
        # Baseline 2: Transformer
        print("Evaluating Transformer baseline...")
        transformer_model.eval()
        with torch.no_grad():
            trans_out = transformer_model(torch.FloatTensor(test_sequences).to(device))
            trans_probs = torch.softmax(trans_out, dim=1)[:, 1].cpu().numpy()
            trans_preds = (trans_probs > 0.5).astype(int)
        
        transformer_metrics = calculate_metrics(test_labels, trans_preds, trans_probs)
        results['Transformer'] = transformer_metrics
        log_metrics_with_prefix(transformer_metrics, "transformer")
        
        # Baseline 3: CNN
        print("Evaluating CNN baseline...")
        cnn1d_model.eval()
        with torch.no_grad():
            cnn_input = torch.FloatTensor(test_sequences).transpose(1, 2).to(device)
            cnn_out = cnn1d_model(cnn_input)
            cnn_probs = torch.softmax(cnn_out, dim=1)[:, 1].cpu().numpy()
            cnn_preds = (cnn_probs > 0.5).astype(int)
        
        cnn_metrics = calculate_metrics(test_labels, cnn_preds, cnn_probs)
        results['CNN'] = cnn_metrics
        log_metrics_with_prefix(cnn_metrics, "cnn")
        
        # Baseline 4: Simple average ensemble
        print("Evaluating average ensemble...")
        avg_probs = (lstm_probs + trans_probs + cnn_probs) / 3
        avg_preds = (avg_probs > 0.5).astype(int)
        avg_metrics = calculate_metrics(test_labels, avg_preds, avg_probs)
        results['Average_Ensemble'] = avg_metrics
        log_metrics_with_prefix(avg_metrics, "average_ensemble")
        
        # Final: Hybrid ensemble
        print("Evaluating hybrid ensemble...")
        ensemble_probs = ensemble.predict_proba(test_sequences)[:, 1]
        ensemble_preds = ensemble.predict(test_sequences)
        ensemble_metrics = calculate_metrics(test_labels, ensemble_preds, ensemble_probs)
        results['Hybrid_Ensemble'] = ensemble_metrics
        log_metrics_with_prefix(ensemble_metrics, "hybrid_ensemble")
        
        # Create comparison visualization
        create_comparison_plots(results, test_labels, {
            'LSTM': lstm_probs,
            'Transformer': trans_probs,
            'CNN': cnn_probs,
            'Average': avg_probs,
            'Hybrid': ensemble_probs
        })
        
        # Log improvement summary
        best_baseline_f1 = max(lstm_metrics['f1'], transformer_metrics['f1'], cnn_metrics['f1'])
        improvement = ((ensemble_metrics['f1'] - best_baseline_f1) / best_baseline_f1) * 100
        
        mlflow.log_metric("improvement_over_best_baseline_pct", improvement)
        mlflow.log_param("best_baseline_model", 
                        max(results.items(), key=lambda x: x[1]['f1'])[0])
        
        print("\\n" + "="*80)
        print("MODEL COMPARISON RESULTS")
        print("="*80)
        for model_name, metrics in results.items():
            print(f"{model_name:20s} | F1: {metrics['f1']:.4f} | "
                  f"Precision: {metrics['precision']:.4f} | "
                  f"Recall: {metrics['recall']:.4f} | PR-AUC: {metrics['pr_auc']:.4f}")
        print("="*80)
        print(f"Hybrid Ensemble Improvement: +{improvement:.2f}% over best baseline")
        print("="*80)
        
        return results

def calculate_metrics(y_true, y_pred, y_probs):
    """Calculate all evaluation metrics"""
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average='binary', zero_division=0
    )
    pr_auc = average_precision_score(y_true, y_probs)
    
    return {
        "precision": precision,
        "recall": recall,
        "f1": f1,
        "pr_auc": pr_auc
    }

def log_metrics_with_prefix(metrics, prefix):
    """Log metrics with model prefix"""
    for metric_name, value in metrics.items():
        mlflow.log_metric(f"{prefix}_{metric_name}", value)

def create_comparison_plots(results, y_true, all_probs):
    """Create comparison visualizations"""
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 12))
    fig.patch.set_facecolor('#1D1D20')
    
    # Colors from Zerve palette
    colors = ['#A1C9F4', '#FFB482', '#8DE5A1', '#FF9F9B', '#D0BBFF']
    
    # Plot 1: F1 Score Comparison
    ax = axes[0, 0]
    ax.set_facecolor('#1D1D20')
    models = list(results.keys())
    f1_scores = [results[m]['f1'] for m in models]
    bars = ax.bar(range(len(models)), f1_scores, color=colors[:len(models)])
    ax.set_xticks(range(len(models)))
    ax.set_xticklabels(models, rotation=45, ha='right', color='#fbfbff')
    ax.set_ylabel('F1 Score', color='#fbfbff')
    ax.set_title('F1 Score Comparison', color='#fbfbff', fontsize=14, fontweight='bold')
    ax.tick_params(colors='#fbfbff')
    ax.spines['bottom'].set_color('#fbfbff')
    ax.spines['left'].set_color('#fbfbff')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Plot 2: PR-AUC Comparison
    ax = axes[0, 1]
    ax.set_facecolor('#1D1D20')
    pr_aucs = [results[m]['pr_auc'] for m in models]
    ax.bar(range(len(models)), pr_aucs, color=colors[:len(models)])
    ax.set_xticks(range(len(models)))
    ax.set_xticklabels(models, rotation=45, ha='right', color='#fbfbff')
    ax.set_ylabel('PR-AUC', color='#fbfbff')
    ax.set_title('PR-AUC Comparison', color='#fbfbff', fontsize=14, fontweight='bold')
    ax.tick_params(colors='#fbfbff')
    ax.spines['bottom'].set_color('#fbfbff')
    ax.spines['left'].set_color('#fbfbff')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Plot 3: Precision-Recall Curves
    ax = axes[1, 0]
    ax.set_facecolor('#1D1D20')
    for i, (model_name, probs) in enumerate(all_probs.items()):
        precision, recall, _ = precision_recall_curve(y_true, probs)
        ax.plot(recall, precision, label=model_name, color=colors[i], linewidth=2)
    ax.set_xlabel('Recall', color='#fbfbff')
    ax.set_ylabel('Precision', color='#fbfbff')
    ax.set_title('Precision-Recall Curves', color='#fbfbff', fontsize=14, fontweight='bold')
    ax.legend(facecolor='#1D1D20', edgecolor='#fbfbff', labelcolor='#fbfbff')
    ax.tick_params(colors='#fbfbff')
    ax.spines['bottom'].set_color('#fbfbff')
    ax.spines['left'].set_color('#fbfbff')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    # Plot 4: Iterative Improvement
    ax = axes[1, 1]
    ax.set_facecolor('#1D1D20')
    stages = ['LSTM', 'Transformer', 'CNN', 'Avg Ensemble', 'Hybrid']
    stage_f1s = [
        results['LSTM']['f1'],
        results['Transformer']['f1'],
        results['CNN']['f1'],
        results['Average_Ensemble']['f1'],
        results['Hybrid_Ensemble']['f1']
    ]
    ax.plot(range(len(stages)), stage_f1s, marker='o', linewidth=2, 
            markersize=10, color='#A1C9F4', markerfacecolor='#ffd400')
    ax.set_xticks(range(len(stages)))
    ax.set_xticklabels(stages, rotation=45, ha='right', color='#fbfbff')
    ax.set_ylabel('F1 Score', color='#fbfbff')
    ax.set_title('Iterative Model Improvement', color='#fbfbff', fontsize=14, fontweight='bold')
    ax.tick_params(colors='#fbfbff')
    ax.spines['bottom'].set_color('#fbfbff')
    ax.spines['left'].set_color('#fbfbff')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    
    plt.tight_layout()
    
    # Save and log
    comparison_plot = plt.gcf()
    mlflow.log_figure(comparison_plot, "model_comparison.png")
    plt.close()

# Import precision_recall_curve
from sklearn.metrics import precision_recall_curve

if __name__ == "__main__":
    # This would be called after training individual models
    print("Hybrid Ensemble Training Pipeline")
    print("Requires pre-trained LSTM, Transformer, and CNN models")
    print("Combines predictions for superior performance")
'''

# Save script
hybrid_ensemble_path = f"{dl_script_dir}/hybrid_ensemble.py"
with open(hybrid_ensemble_path, 'w') as f:
    f.write(hybrid_ensemble_script)

print("=" * 80)
print("Hybrid Ensemble Deep Learning + Classic ML Generated")
print("=" * 80)
print(f"Script: {hybrid_ensemble_path}")
print(f"Size: {len(hybrid_ensemble_script)} characters")
print("\nEnsemble Strategy:")
print("  • Stage 1: Train individual DL models (LSTM, Transformer, CNN)")
print("  • Stage 2: Extract predictions as meta-features")
print("  • Stage 3: Train GBT meta-classifier on DL outputs")
print("  • Stage 4: Combine for robust predictions")
print("\nComparison Framework:")
print("  ✓ Individual model baselines (LSTM, Transformer, CNN)")
print("  ✓ Simple average ensemble baseline")
print("  ✓ Hybrid ensemble with stacking")
print("  ✓ Iterative improvement tracking")
print("\nVisualization:")
print("  • F1 score comparison across all models")
print("  • PR-AUC comparison")
print("  • Precision-Recall curves")
print("  • Iterative improvement progression")
print("\nBenefits:")
print("  • Robustness: Combines strengths of multiple architectures")
print("  • Interpretability: Can analyze individual model contributions")
print("  • Performance: Meta-learning optimizes ensemble weighting")
print("=" * 80)

hybrid_ensemble_summary = {
    "script_path": hybrid_ensemble_path,
    "architecture": "stacked_ensemble",
    "base_models": ["LSTM", "Transformer", "CNN1D"],
    "meta_classifier": "GradientBoostingClassifier",
    "metrics_tracked": ["precision", "recall", "f1", "pr_auc"],
    "visualizations": ["f1_comparison", "pr_auc_comparison", "pr_curves", "iterative_improvement"]
}
