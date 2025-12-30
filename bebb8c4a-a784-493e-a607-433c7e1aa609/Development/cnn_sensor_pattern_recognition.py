"""
CNN for Sensor Pattern Recognition
Convolutional Neural Network for detecting patterns in maritime sensor data
"""

import os

cnn_sensor_script = '''"""
CNN Sensor Pattern Recognition for Maritime Telemetry
Detect spatial-temporal patterns in multi-sensor maritime data
"""

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
from sklearn.metrics import precision_recall_fscore_support, average_precision_score, confusion_matrix
from sklearn.model_selection import TimeSeriesSplit
import mlflow
import mlflow.pytorch
from datetime import datetime

mlflow.set_tracking_uri("http://mlflow-service:5000")

class SensorPatternDataset(Dataset):
    """Dataset for multi-channel sensor patterns"""
    
    def __init__(self, sensor_matrices, labels):
        """
        Args:
            sensor_matrices: (N, channels, height, width) format
            labels: (N,) anomaly labels
        """
        self.sensor_matrices = torch.FloatTensor(sensor_matrices)
        self.labels = torch.LongTensor(labels)
    
    def __len__(self):
        return len(self.sensor_matrices)
    
    def __getitem__(self, idx):
        return self.sensor_matrices[idx], self.labels[idx]

class CNN1D_SensorDetector(nn.Module):
    """1D CNN for time-series sensor patterns"""
    
    def __init__(self, num_channels=7, sequence_length=24, num_classes=2):
        super(CNN1D_SensorDetector, self).__init__()
        
        # Convolutional layers
        self.conv1 = nn.Sequential(
            nn.Conv1d(num_channels, 64, kernel_size=3, padding=1),
            nn.BatchNorm1d(64),
            nn.ReLU(),
            nn.MaxPool1d(kernel_size=2)
        )
        
        self.conv2 = nn.Sequential(
            nn.Conv1d(64, 128, kernel_size=3, padding=1),
            nn.BatchNorm1d(128),
            nn.ReLU(),
            nn.MaxPool1d(kernel_size=2)
        )
        
        self.conv3 = nn.Sequential(
            nn.Conv1d(128, 256, kernel_size=3, padding=1),
            nn.BatchNorm1d(256),
            nn.ReLU(),
            nn.AdaptiveAvgPool1d(1)
        )
        
        # Fully connected layers
        self.fc = nn.Sequential(
            nn.Linear(256, 128),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.3),
            nn.Linear(64, num_classes)
        )
    
    def forward(self, x):
        # x shape: (batch, channels, sequence_length)
        x = self.conv1(x)
        x = self.conv2(x)
        x = self.conv3(x)
        x = x.view(x.size(0), -1)  # Flatten
        x = self.fc(x)
        return x

class CNN2D_SensorDetector(nn.Module):
    """2D CNN for spatial-temporal sensor patterns"""
    
    def __init__(self, num_channels=1, num_classes=2):
        super(CNN2D_SensorDetector, self).__init__()
        
        # Multi-scale feature extraction
        self.conv_block1 = nn.Sequential(
            nn.Conv2d(num_channels, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.Conv2d(32, 32, kernel_size=3, padding=1),
            nn.BatchNorm2d(32),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2)
        )
        
        self.conv_block2 = nn.Sequential(
            nn.Conv2d(32, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.Conv2d(64, 64, kernel_size=3, padding=1),
            nn.BatchNorm2d(64),
            nn.ReLU(),
            nn.MaxPool2d(kernel_size=2)
        )
        
        self.conv_block3 = nn.Sequential(
            nn.Conv2d(64, 128, kernel_size=3, padding=1),
            nn.BatchNorm2d(128),
            nn.ReLU(),
            nn.AdaptiveAvgPool2d((1, 1))
        )
        
        # Classification head
        self.classifier = nn.Sequential(
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(0.4),
            nn.Linear(64, num_classes)
        )
    
    def forward(self, x):
        # x shape: (batch, 1, height, width)
        x = self.conv_block1(x)
        x = self.conv_block2(x)
        x = self.conv_block3(x)
        x = x.view(x.size(0), -1)
        x = self.classifier(x)
        return x

def prepare_1d_sensor_data(spark_df, sequence_length=24):
    """Prepare 1D multi-channel sensor data"""
    
    sensor_cols = [
        'engine_rpm', 'engine_temperature', 'fuel_consumption_rate',
        'speed_knots', 'latitude', 'longitude', 'course'
    ]
    
    data = spark_df.select(sensor_cols + ['vessel_id', 'timestamp', 'is_anomaly']).toPandas()
    data = data.sort_values(['vessel_id', 'timestamp'])
    
    sequences = []
    labels = []
    
    for vessel_id in data['vessel_id'].unique():
        vessel_data = data[data['vessel_id'] == vessel_id]
        features = vessel_data[sensor_cols].values
        anomaly_labels = vessel_data['is_anomaly'].values
        
        for i in range(len(features) - sequence_length):
            # Shape: (channels, sequence_length)
            seq = features[i:i+sequence_length].T
            label = anomaly_labels[i+sequence_length]
            sequences.append(seq)
            labels.append(label)
    
    return np.array(sequences), np.array(labels)

def prepare_2d_sensor_data(spark_df, window_size=24):
    """Prepare 2D spatial-temporal sensor matrices"""
    
    sensor_cols = [
        'engine_rpm', 'engine_temperature', 'fuel_consumption_rate',
        'speed_knots', 'latitude', 'longitude', 'course'
    ]
    
    data = spark_df.select(sensor_cols + ['vessel_id', 'timestamp', 'is_anomaly']).toPandas()
    data = data.sort_values(['vessel_id', 'timestamp'])
    
    matrices = []
    labels = []
    
    for vessel_id in data['vessel_id'].unique():
        vessel_data = data[data['vessel_id'] == vessel_id]
        features = vessel_data[sensor_cols].values
        anomaly_labels = vessel_data['is_anomaly'].values
        
        for i in range(len(features) - window_size):
            # Create 2D matrix: (sensors x time)
            matrix = features[i:i+window_size].T
            # Add channel dimension: (1, sensors, time)
            matrix = np.expand_dims(matrix, axis=0)
            label = anomaly_labels[i+window_size]
            matrices.append(matrix)
            labels.append(label)
    
    return np.array(matrices), np.array(labels)

def train_cnn_model(train_loader, val_loader, model, model_type, device, tenant_id):
    """Train CNN model with evaluation"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/deep_learning_cnn"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"cnn_{model_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        mlflow.log_param("model_type", f"CNN_{model_type}")
        mlflow.log_param("tenant_id", tenant_id)
        mlflow.log_param("learning_rate", 0.001)
        mlflow.log_param("batch_size", 64)
        
        model = model.to(device)
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.Adam(model.parameters(), lr=0.001, weight_decay=1e-5)
        scheduler = optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='max', factor=0.5, patience=5)
        
        best_val_f1 = 0.0
        epochs = 50
        
        for epoch in range(epochs):
            # Training phase
            model.train()
            train_loss = 0.0
            
            for inputs, labels in train_loader:
                inputs, labels = inputs.to(device), labels.to(device)
                
                optimizer.zero_grad()
                outputs = model(inputs)
                loss = criterion(outputs, labels)
                loss.backward()
                optimizer.step()
                
                train_loss += loss.item()
            
            # Validation phase
            model.eval()
            val_preds = []
            val_labels_list = []
            val_probs = []
            
            with torch.no_grad():
                for inputs, labels in val_loader:
                    inputs = inputs.to(device)
                    outputs = model(inputs)
                    probs = torch.softmax(outputs, dim=1)
                    preds = torch.argmax(outputs, dim=1)
                    
                    val_preds.extend(preds.cpu().numpy())
                    val_labels_list.extend(labels.numpy())
                    val_probs.extend(probs[:, 1].cpu().numpy())
            
            # Calculate metrics
            precision, recall, f1, _ = precision_recall_fscore_support(
                val_labels_list, val_preds, average='binary', zero_division=0
            )
            pr_auc = average_precision_score(val_labels_list, val_probs)
            
            scheduler.step(f1)
            
            if (epoch + 1) % 10 == 0:
                print(f"Epoch {epoch+1}/{epochs} - Loss: {train_loss/len(train_loader):.4f} "
                      f"Val F1: {f1:.4f} Precision: {precision:.4f} Recall: {recall:.4f} PR-AUC: {pr_auc:.4f}")
            
            if f1 > best_val_f1:
                best_val_f1 = f1
                mlflow.log_metric("best_val_f1", best_val_f1)
                mlflow.log_metric("best_val_precision", precision)
                mlflow.log_metric("best_val_recall", recall)
                mlflow.log_metric("best_val_pr_auc", pr_auc)
                
                # Log confusion matrix
                cm = confusion_matrix(val_labels_list, val_preds)
                mlflow.log_metric("true_negatives", int(cm[0, 0]))
                mlflow.log_metric("false_positives", int(cm[0, 1]))
                mlflow.log_metric("false_negatives", int(cm[1, 0]))
                mlflow.log_metric("true_positives", int(cm[1, 1]))
        
        # Register model
        mlflow.pytorch.log_model(
            model,
            "model",
            registered_model_name=f"maritime_cnn_{model_type}_anomaly_{tenant_id}"
        )
        
        print(f"\\nCNN {model_type} Training Complete - Best Val F1: {best_val_f1:.4f}")
        return model, {"f1": best_val_f1, "precision": precision, "recall": recall, "pr_auc": pr_auc}

if __name__ == "__main__":
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("CNN-SensorPatternRecognition").getOrCreate()
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    
    tenant_id = "shipping-co-alpha"
    
    # Load data
    df = spark.read.format("iceberg").load(f"maritime_iceberg.telemetry.{tenant_id}_vessel_telemetry")
    
    print("Training 1D CNN...")
    # Prepare 1D data
    sequences_1d, labels_1d = prepare_1d_sensor_data(df, sequence_length=24)
    
    # Time-series split
    tscv = TimeSeriesSplit(n_splits=3)
    for train_idx, val_idx in tscv.split(sequences_1d):
        train_seq, val_seq = sequences_1d[train_idx], sequences_1d[val_idx]
        train_labels, val_labels = labels_1d[train_idx], labels_1d[val_idx]
    
    train_dataset_1d = SensorPatternDataset(train_seq, train_labels)
    val_dataset_1d = SensorPatternDataset(val_seq, val_labels)
    
    train_loader_1d = DataLoader(train_dataset_1d, batch_size=64, shuffle=False)
    val_loader_1d = DataLoader(val_dataset_1d, batch_size=64, shuffle=False)
    
    cnn1d_model = CNN1D_SensorDetector(num_channels=7, sequence_length=24)
    cnn1d_model, cnn1d_metrics = train_cnn_model(
        train_loader_1d, val_loader_1d, cnn1d_model, "1D", device, tenant_id
    )
    
    print("\\nTraining 2D CNN...")
    # Prepare 2D data
    matrices_2d, labels_2d = prepare_2d_sensor_data(df, window_size=24)
    
    for train_idx, val_idx in tscv.split(matrices_2d):
        train_mat, val_mat = matrices_2d[train_idx], matrices_2d[val_idx]
        train_labels_2d, val_labels_2d = labels_2d[train_idx], labels_2d[val_idx]
    
    train_dataset_2d = SensorPatternDataset(train_mat, train_labels_2d)
    val_dataset_2d = SensorPatternDataset(val_mat, val_labels_2d)
    
    train_loader_2d = DataLoader(train_dataset_2d, batch_size=64, shuffle=False)
    val_loader_2d = DataLoader(val_dataset_2d, batch_size=64, shuffle=False)
    
    cnn2d_model = CNN2D_SensorDetector(num_channels=1)
    cnn2d_model, cnn2d_metrics = train_cnn_model(
        train_loader_2d, val_loader_2d, cnn2d_model, "2D", device, tenant_id
    )
    
    spark.stop()
'''

# Save script
cnn_sensor_path = f"{dl_script_dir}/cnn_sensor_pattern.py"
with open(cnn_sensor_path, 'w') as f:
    f.write(cnn_sensor_script)

print("=" * 80)
print("CNN Sensor Pattern Recognition Generated")
print("=" * 80)
print(f"Script: {cnn_sensor_path}")
print(f"Size: {len(cnn_sensor_script)} characters")
print("\nArchitectures:")
print("  • 1D CNN: Multi-channel temporal convolutions for time-series")
print("  • 2D CNN: Spatial-temporal pattern recognition")
print("\nFeatures:")
print("  ✓ Multi-scale feature extraction with pooling")
print("  ✓ Batch normalization for stable training")
print("  ✓ Learning rate scheduling based on validation performance")
print("  ✓ Confusion matrix logging for detailed analysis")
print("\nPattern Detection:")
print("  • Temporal patterns in individual sensors")
print("  • Spatial correlations across multiple sensors")
print("  • Multi-scale anomaly signatures")
print("=" * 80)

cnn_sensor_summary = {
    "script_path": cnn_sensor_path,
    "models": ["CNN1D", "CNN2D"],
    "pattern_types": ["temporal", "spatial-temporal"],
    "metrics": ["precision", "recall", "f1", "pr_auc", "confusion_matrix"]
}
