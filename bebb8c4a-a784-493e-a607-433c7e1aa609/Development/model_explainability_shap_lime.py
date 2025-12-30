"""
Model Explainability with SHAP and LIME
Interpretability tools for deep learning anomaly detection models
"""

import os

explainability_script = '''"""
Model Explainability for Maritime Anomaly Detection
SHAP and LIME for deep learning model interpretability
"""

import torch
import numpy as np
import shap
from lime import lime_tabular
import matplotlib.pyplot as plt
import mlflow
from datetime import datetime

mlflow.set_tracking_uri("http://mlflow-service:5000")

class ModelExplainer:
    """Wrapper for model explainability with SHAP and LIME"""
    
    def __init__(self, model, model_type, device='cpu'):
        self.model = model
        self.model_type = model_type
        self.device = device
        self.model.eval()
        
        self.shap_explainer = None
        self.lime_explainer = None
    
    def _model_predict_wrapper(self, sequences):
        """Wrapper for model predictions compatible with SHAP/LIME"""
        
        if len(sequences.shape) == 2:
            # For flattened sequences, reshape
            sequences = sequences.reshape(sequences.shape[0], -1, 7)
        
        with torch.no_grad():
            sequences_tensor = torch.FloatTensor(sequences).to(self.device)
            
            if self.model_type == "LSTM":
                outputs, _ = self.model(sequences_tensor)
            elif self.model_type == "Transformer":
                outputs = self.model(sequences_tensor)
            elif self.model_type == "CNN1D":
                sequences_tensor = sequences_tensor.transpose(1, 2)
                outputs = self.model(sequences_tensor)
            
            probs = torch.softmax(outputs, dim=1).cpu().numpy()
        
        return probs
    
    def initialize_shap_explainer(self, background_data, num_samples=100):
        """Initialize SHAP explainer with background data"""
        
        print(f"Initializing SHAP explainer with {num_samples} background samples...")
        
        # Flatten sequences for SHAP
        background_flat = background_data[:num_samples].reshape(num_samples, -1)
        
        # Create SHAP explainer (DeepExplainer for neural networks)
        self.shap_explainer = shap.KernelExplainer(
            lambda x: self._model_predict_wrapper(x)[:, 1],  # Probability of anomaly class
            background_flat
        )
        
        print("SHAP explainer initialized")
    
    def initialize_lime_explainer(self, train_data, feature_names):
        """Initialize LIME explainer"""
        
        print("Initializing LIME explainer...")
        
        # Flatten training data
        train_flat = train_data.reshape(train_data.shape[0], -1)
        
        self.lime_explainer = lime_tabular.LimeTabularExplainer(
            train_flat,
            feature_names=feature_names,
            class_names=['Normal', 'Anomaly'],
            mode='classification'
        )
        
        print("LIME explainer initialized")
    
    def explain_with_shap(self, test_sequences, num_samples=10):
        """Generate SHAP explanations for test samples"""
        
        if self.shap_explainer is None:
            raise ValueError("SHAP explainer not initialized. Call initialize_shap_explainer first.")
        
        print(f"Generating SHAP explanations for {num_samples} samples...")
        
        # Flatten sequences
        test_flat = test_sequences[:num_samples].reshape(num_samples, -1)
        
        # Calculate SHAP values
        shap_values = self.shap_explainer.shap_values(test_flat)
        
        return shap_values, test_flat
    
    def explain_with_lime(self, test_sequence, instance_idx=0):
        """Generate LIME explanation for a single instance"""
        
        if self.lime_explainer is None:
            raise ValueError("LIME explainer not initialized. Call initialize_lime_explainer first.")
        
        print(f"Generating LIME explanation for instance {instance_idx}...")
        
        # Flatten sequence
        test_flat = test_sequence.reshape(1, -1)
        
        # Generate explanation
        explanation = self.lime_explainer.explain_instance(
            test_flat[0],
            lambda x: self._model_predict_wrapper(x),
            num_features=10,
            num_samples=500
        )
        
        return explanation
    
    def visualize_shap_summary(self, shap_values, features, feature_names):
        """Create SHAP summary plot"""
        
        fig = plt.figure(figsize=(12, 8))
        fig.patch.set_facecolor('#1D1D20')
        
        shap.summary_plot(
            shap_values, 
            features, 
            feature_names=feature_names,
            show=False,
            plot_size=(12, 8)
        )
        
        plt.title('SHAP Feature Importance for Anomaly Detection', 
                 color='#fbfbff', fontsize=14, fontweight='bold', pad=20)
        
        # Customize plot colors for Zerve theme
        ax = plt.gca()
        ax.set_facecolor('#1D1D20')
        ax.tick_params(colors='#fbfbff')
        ax.spines['bottom'].set_color('#fbfbff')
        ax.spines['left'].set_color('#fbfbff')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.xaxis.label.set_color('#fbfbff')
        ax.yaxis.label.set_color('#fbfbff')
        
        return fig
    
    def visualize_lime_explanation(self, explanation):
        """Create LIME explanation plot"""
        
        fig = plt.figure(figsize=(10, 6))
        fig.patch.set_facecolor('#1D1D20')
        
        # Get feature importance
        exp_list = explanation.as_list()
        features = [f[0] for f in exp_list]
        importances = [f[1] for f in exp_list]
        
        # Create bar plot
        colors = ['#17b26a' if x > 0 else '#f04438' for x in importances]
        
        ax = plt.gca()
        ax.set_facecolor('#1D1D20')
        ax.barh(range(len(features)), importances, color=colors)
        ax.set_yticks(range(len(features)))
        ax.set_yticklabels(features, color='#fbfbff')
        ax.set_xlabel('Feature Importance', color='#fbfbff', fontsize=12)
        ax.set_title('LIME Explanation: Feature Contributions to Anomaly Prediction',
                    color='#fbfbff', fontsize=14, fontweight='bold')
        ax.tick_params(colors='#fbfbff')
        ax.spines['bottom'].set_color('#fbfbff')
        ax.spines['left'].set_color('#fbfbff')
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.axvline(x=0, color='#fbfbff', linestyle='--', linewidth=0.5)
        
        plt.tight_layout()
        return fig
    
    def visualize_attention_weights(self, sequence, attention_weights):
        """Visualize attention weights for LSTM model"""
        
        if self.model_type != "LSTM":
            print("Attention visualization only available for LSTM model")
            return None
        
        fig, ax = plt.subplots(figsize=(12, 6))
        fig.patch.set_facecolor('#1D1D20')
        ax.set_facecolor('#1D1D20')
        
        # Plot attention heatmap
        attention_np = attention_weights.squeeze().cpu().numpy()
        
        im = ax.imshow(attention_np.T, cmap='YlOrRd', aspect='auto')
        
        ax.set_xlabel('Time Steps', color='#fbfbff', fontsize=12)
        ax.set_ylabel('Features', color='#fbfbff', fontsize=12)
        ax.set_title('LSTM Attention Weights Over Time',
                    color='#fbfbff', fontsize=14, fontweight='bold')
        
        feature_names = ['Engine RPM', 'Engine Temp', 'Fuel Rate', 'Speed', 
                        'Latitude', 'Longitude', 'Course']
        ax.set_yticks(range(len(feature_names)))
        ax.set_yticklabels(feature_names, color='#fbfbff')
        ax.tick_params(colors='#fbfbff')
        
        # Colorbar
        cbar = plt.colorbar(im, ax=ax)
        cbar.ax.yaxis.set_tick_params(color='#fbfbff')
        cbar.ax.yaxis.set_ticklabels(cbar.ax.yaxis.get_ticklabels(), color='#fbfbff')
        cbar.set_label('Attention Weight', color='#fbfbff')
        
        plt.tight_layout()
        return fig

def generate_feature_names(sequence_length=24, num_features=7):
    """Generate feature names for flattened sequences"""
    
    feature_base_names = [
        'engine_rpm', 'engine_temp', 'fuel_rate', 'speed',
        'latitude', 'longitude', 'course'
    ]
    
    feature_names = []
    for t in range(sequence_length):
        for fname in feature_base_names:
            feature_names.append(f"{fname}_t{t}")
    
    return feature_names

def explain_models_and_log(
    lstm_model, transformer_model, cnn1d_model,
    train_sequences, test_sequences, test_labels,
    device, tenant_id
):
    """Generate and log explainability artifacts for all models"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/model_explainability"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"explainability_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        
        feature_names = generate_feature_names(
            sequence_length=train_sequences.shape[1],
            num_features=train_sequences.shape[2]
        )
        
        # Find anomalous samples for explanation
        with torch.no_grad():
            lstm_out, attn = lstm_model(torch.FloatTensor(test_sequences[:100]).to(device))
            lstm_probs = torch.softmax(lstm_out, dim=1)[:, 1].cpu().numpy()
        
        anomaly_indices = np.where(lstm_probs > 0.5)[0][:5]
        
        # Explain LSTM
        print("\\n" + "="*80)
        print("Explaining LSTM Model")
        print("="*80)
        lstm_explainer = ModelExplainer(lstm_model, "LSTM", device)
        lstm_explainer.initialize_shap_explainer(train_sequences, num_samples=50)
        lstm_explainer.initialize_lime_explainer(train_sequences[:500], feature_names)
        
        lstm_shap_values, lstm_features = lstm_explainer.explain_with_shap(test_sequences, num_samples=50)
        lstm_shap_fig = lstm_explainer.visualize_shap_summary(lstm_shap_values, lstm_features, feature_names)
        mlflow.log_figure(lstm_shap_fig, "lstm_shap_summary.png")
        plt.close()
        
        if len(anomaly_indices) > 0:
            lstm_lime_exp = lstm_explainer.explain_with_lime(test_sequences[anomaly_indices[0:1]])
            lstm_lime_fig = lstm_explainer.visualize_lime_explanation(lstm_lime_exp)
            mlflow.log_figure(lstm_lime_fig, "lstm_lime_explanation.png")
            plt.close()
            
            # Attention visualization
            with torch.no_grad():
                seq_tensor = torch.FloatTensor(test_sequences[anomaly_indices[0]:anomaly_indices[0]+1]).to(device)
                _, attn_weights = lstm_model(seq_tensor)
            attn_fig = lstm_explainer.visualize_attention_weights(test_sequences[anomaly_indices[0]], attn_weights)
            if attn_fig:
                mlflow.log_figure(attn_fig, "lstm_attention_weights.png")
                plt.close()
        
        # Explain Transformer
        print("\\n" + "="*80)
        print("Explaining Transformer Model")
        print("="*80)
        transformer_explainer = ModelExplainer(transformer_model, "Transformer", device)
        transformer_explainer.initialize_lime_explainer(train_sequences[:500], feature_names)
        
        if len(anomaly_indices) > 0:
            trans_lime_exp = transformer_explainer.explain_with_lime(test_sequences[anomaly_indices[0:1]])
            trans_lime_fig = transformer_explainer.visualize_lime_explanation(trans_lime_exp)
            mlflow.log_figure(trans_lime_fig, "transformer_lime_explanation.png")
            plt.close()
        
        # Explain CNN
        print("\\n" + "="*80)
        print("Explaining CNN Model")
        print("="*80)
        cnn_explainer = ModelExplainer(cnn1d_model, "CNN1D", device)
        cnn_explainer.initialize_lime_explainer(train_sequences[:500], feature_names)
        
        if len(anomaly_indices) > 0:
            cnn_lime_exp = cnn_explainer.explain_with_lime(test_sequences[anomaly_indices[0:1]])
            cnn_lime_fig = cnn_explainer.visualize_lime_explanation(cnn_lime_exp)
            mlflow.log_figure(cnn_lime_fig, "cnn_lime_explanation.png")
            plt.close()
        
        mlflow.log_param("num_explained_samples", len(anomaly_indices))
        mlflow.log_param("explainability_methods", "SHAP,LIME,Attention")
        
        print("\\nExplainability artifacts logged to MLflow")

if __name__ == "__main__":
    print("Model Explainability Module")
    print("Provides SHAP and LIME explanations for DL models")
'''

# Save script
explainability_path = f"{dl_script_dir}/model_explainability.py"
with open(explainability_path, 'w') as f:
    f.write(explainability_script)

print("=" * 80)
print("Model Explainability with SHAP & LIME Generated")
print("=" * 80)
print(f"Script: {explainability_path}")
print(f"Size: {len(explainability_script)} characters")
print("\nExplainability Methods:")
print("  • SHAP (SHapley Additive exPlanations): Feature importance via game theory")
print("  • LIME (Local Interpretable Model-agnostic Explanations): Local approximations")
print("  • Attention Weights: LSTM attention mechanism visualization")
print("\nVisualizations:")
print("  ✓ SHAP summary plots showing global feature importance")
print("  ✓ LIME bar charts for individual prediction explanations")
print("  ✓ Attention heatmaps showing temporal focus")
print("\nMLflow Integration:")
print("  • All explanation figures logged as artifacts")
print("  • Explanations for detected anomalies")
print("  • Cross-model interpretability comparison")
print("\nUse Cases:")
print("  • Understanding why a vessel was flagged as anomalous")
print("  • Identifying which sensors contribute most to predictions")
print("  • Building trust with stakeholders through transparency")
print("=" * 80)

explainability_summary = {
    "script_path": explainability_path,
    "methods": ["SHAP", "LIME", "Attention"],
    "visualizations": ["shap_summary", "lime_explanation", "attention_heatmap"],
    "models_supported": ["LSTM", "Transformer", "CNN1D"]
}
