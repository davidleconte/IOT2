"""
Model Training Scripts with MLflow Integration

Generates PySpark training scripts for all 4 use cases with MLflow tracking,
evaluation, and model registry integration with tenant metadata.
"""

import os

# Model training script with MLflow
model_training_script = '''"""
Model Training Pipeline with MLflow Tracking and Registry
Trains models for all maritime use cases with experiment tracking
"""

from pyspark.sql import SparkSession
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient
from datetime import datetime
import json

# MLflow configuration
mlflow.set_tracking_uri("http://mlflow-service:5000")

def train_predictive_maintenance_model(spark, tenant_id, training_mode="tenant_specific"):
    """Train predictive maintenance classification model"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/predictive_maintenance"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"maintenance_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        # Log parameters
        mlflow.log_param("tenant_id", tenant_id)
        mlflow.log_param("training_mode", training_mode)
        mlflow.log_param("use_case", "predictive_maintenance")
        mlflow.log_param("model_type", "GBTClassifier")
        
        # Load engineered features
        df = spark.read.format("iceberg") \\
            .load(f"maritime_iceberg.training.{tenant_id}_maintenance_features")
        
        # Train/test split
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        # Feature columns
        feature_cols = [
            "engine_rpm", "engine_temperature", "fuel_consumption_rate", "speed_knots",
            "engine_health_index", "fuel_efficiency", "engine_stress_score",
            "fuel_consumption_24h_avg", "speed_24h_std"
        ]
        
        # Build pipeline
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features", 
                               withMean=True, withStd=True)
        classifier = GBTClassifier(labelCol="maintenance_needed", featuresCol="features",
                                  maxIter=100, maxDepth=5, seed=42)
        
        pipeline = Pipeline(stages=[assembler, scaler, classifier])
        
        # Train model
        model = pipeline.fit(train_df)
        
        # Evaluate
        predictions = model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="maintenance_needed", predictionCol="prediction")
        
        accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
        f1_score = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
        precision = evaluator.evaluate(predictions, {evaluator.metricName: "weightedPrecision"})
        recall = evaluator.evaluate(predictions, {evaluator.metricName: "weightedRecall"})
        
        # Log metrics
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("f1_score", f1_score)
        mlflow.log_metric("precision", precision)
        mlflow.log_metric("recall", recall)
        mlflow.log_metric("training_samples", train_df.count())
        mlflow.log_metric("test_samples", test_df.count())
        
        # Log model with tenant metadata
        mlflow.spark.log_model(
            model,
            "model",
            registered_model_name=f"maritime_maintenance_{tenant_id}",
            metadata={
                "tenant_id": tenant_id,
                "training_mode": training_mode,
                "use_case": "predictive_maintenance",
                "feature_count": len(feature_cols),
                "spark_version": spark.version,
                "trained_at": datetime.now().isoformat()
            }
        )
        
        print(f"Predictive Maintenance Model - Accuracy: {accuracy:.4f}, F1: {f1_score:.4f}")
        return model, {"accuracy": accuracy, "f1_score": f1_score}

def train_fuel_optimization_model(spark, tenant_id, training_mode="tenant_specific"):
    """Train fuel optimization regression model"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/fuel_optimization"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"fuel_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("tenant_id", tenant_id)
        mlflow.log_param("training_mode", training_mode)
        mlflow.log_param("use_case", "fuel_optimization")
        mlflow.log_param("model_type", "GBTRegressor")
        
        df = spark.read.format("iceberg") \\
            .load(f"maritime_iceberg.training.{tenant_id}_fuel_features")
        
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        feature_cols = [
            "fuel_consumption_rate", "speed_knots", "wind_speed", "wave_height",
            "sea_state", "weather_resistance", "fuel_per_nautical_mile",
            "fuel_deviation_from_avg", "fuel_consumption_7d_avg"
        ]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)
        regressor = GBTRegressor(labelCol="optimal_fuel_rate", featuresCol="features",
                                maxIter=100, maxDepth=5, seed=42)
        
        pipeline = Pipeline(stages=[assembler, scaler, regressor])
        model = pipeline.fit(train_df)
        
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(labelCol="optimal_fuel_rate", predictionCol="prediction")
        
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("training_samples", train_df.count())
        mlflow.log_metric("test_samples", test_df.count())
        
        mlflow.spark.log_model(
            model,
            "model",
            registered_model_name=f"maritime_fuel_optimization_{tenant_id}",
            metadata={
                "tenant_id": tenant_id,
                "training_mode": training_mode,
                "use_case": "fuel_optimization",
                "feature_count": len(feature_cols),
                "trained_at": datetime.now().isoformat()
            }
        )
        
        print(f"Fuel Optimization Model - RMSE: {rmse:.4f}, R2: {r2:.4f}")
        return model, {"rmse": rmse, "r2": r2}

def train_eta_prediction_model(spark, tenant_id, training_mode="tenant_specific"):
    """Train ETA prediction regression model"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/eta_prediction"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"eta_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("tenant_id", tenant_id)
        mlflow.log_param("training_mode", training_mode)
        mlflow.log_param("use_case", "eta_prediction")
        mlflow.log_param("model_type", "GBTRegressor")
        
        df = spark.read.format("iceberg") \\
            .load(f"maritime_iceberg.training.{tenant_id}_eta_features")
        
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        feature_cols = [
            "speed_knots", "distance_to_destination", "wind_speed", "current_speed",
            "effective_speed", "estimated_hours_remaining", "speed_variability",
            "speed_24h_avg"
        ]
        
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)
        regressor = GBTRegressor(labelCol="actual_eta_hours", featuresCol="features",
                                maxIter=100, maxDepth=5, seed=42)
        
        pipeline = Pipeline(stages=[assembler, scaler, regressor])
        model = pipeline.fit(train_df)
        
        predictions = model.transform(test_df)
        evaluator = RegressionEvaluator(labelCol="actual_eta_hours", predictionCol="prediction")
        
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        mlflow.log_metric("rmse", rmse)
        mlflow.log_metric("mae", mae)
        mlflow.log_metric("r2", r2)
        mlflow.log_metric("training_samples", train_df.count())
        
        mlflow.spark.log_model(
            model,
            "model",
            registered_model_name=f"maritime_eta_prediction_{tenant_id}",
            metadata={
                "tenant_id": tenant_id,
                "training_mode": training_mode,
                "use_case": "eta_prediction",
                "feature_count": len(feature_cols),
                "trained_at": datetime.now().isoformat()
            }
        )
        
        print(f"ETA Prediction Model - RMSE: {rmse:.4f}, R2: {r2:.4f}")
        return model, {"rmse": rmse, "r2": r2}

def train_anomaly_classification_model(spark, tenant_id, training_mode="tenant_specific"):
    """Train anomaly classification model"""
    
    experiment_name = f"/maritime_fleet_guardian/{tenant_id}/anomaly_classification"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"anomaly_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("tenant_id", tenant_id)
        mlflow.log_param("training_mode", training_mode)
        mlflow.log_param("use_case", "anomaly_classification")
        mlflow.log_param("model_type", "GBTClassifier")
        
        df = spark.read.format("iceberg") \\
            .load(f"maritime_iceberg.training.{tenant_id}_anomaly_features")
        
        train_df, test_df = df.randomSplit([0.8, 0.2], seed=42)
        
        feature_cols = [
            "fuel_anomaly_score", "speed_anomaly_score", "engine_anomaly_score",
            "consecutive_anomalies", "total_anomaly_score", "anomaly_persistence"
        ]
        
        label_indexer = StringIndexer(inputCol="anomaly_class", outputCol="label_indexed")
        assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
        scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                               withMean=True, withStd=True)
        classifier = GBTClassifier(labelCol="label_indexed", featuresCol="features",
                                  maxIter=100, maxDepth=5, seed=42)
        
        pipeline = Pipeline(stages=[label_indexer, assembler, scaler, classifier])
        model = pipeline.fit(train_df)
        
        predictions = model.transform(test_df)
        evaluator = MulticlassClassificationEvaluator(
            labelCol="label_indexed", predictionCol="prediction")
        
        accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
        f1_score = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
        
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("f1_score", f1_score)
        mlflow.log_metric("training_samples", train_df.count())
        
        mlflow.spark.log_model(
            model,
            "model",
            registered_model_name=f"maritime_anomaly_classification_{tenant_id}",
            metadata={
                "tenant_id": tenant_id,
                "training_mode": training_mode,
                "use_case": "anomaly_classification",
                "feature_count": len(feature_cols),
                "trained_at": datetime.now().isoformat()
            }
        )
        
        print(f"Anomaly Classification Model - Accuracy: {accuracy:.4f}, F1: {f1_score:.4f}")
        return model, {"accuracy": accuracy, "f1_score": f1_score}

# Main training orchestrator
if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("MaritimeFleetGuardian-ModelTraining") \\
        .config("spark.jars.packages", "org.mlflow:mlflow-spark:2.9.2") \\
        .getOrCreate()
    
    tenants = ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]
    
    for tenant in tenants:
        print(f"\\n{'='*80}")
        print(f"Training models for tenant: {tenant}")
        print(f"{'='*80}")
        
        # Train all models for this tenant
        maintenance_model, maintenance_metrics = train_predictive_maintenance_model(spark, tenant)
        fuel_model, fuel_metrics = train_fuel_optimization_model(spark, tenant)
        eta_model, eta_metrics = train_eta_prediction_model(spark, tenant)
        anomaly_model, anomaly_metrics = train_anomaly_classification_model(spark, tenant)
        
        print(f"\\nAll models trained successfully for {tenant}")
    
    spark.stop()
'''

# Save training script
training_script_path = f"{spark_script_dir}/model_training.py"
with open(training_script_path, 'w') as f:
    f.write(model_training_script)

print("=" * 80)
print("Model Training Pipeline with MLflow Generated")
print("=" * 80)
print(f"Script Location: {training_script_path}")
print(f"Script Size: {len(model_training_script)} characters")
print("\nTraining Pipeline Features:")
print("  ✓ MLflow experiment tracking per tenant and use case")
print("  ✓ Automated train/test split and evaluation")
print("  ✓ Model registry with tenant metadata tagging")
print("  ✓ Comprehensive metrics logging (accuracy, F1, RMSE, R2, MAE)")
print("  ✓ Cross-validation ready structure")
print("\nModels Trained:")
print("  1. Predictive Maintenance (GBT Classifier)")
print("  2. Fuel Optimization (GBT Regressor)")
print("  3. ETA Prediction (GBT Regressor)")
print("  4. Anomaly Classification (Multi-class GBT Classifier)")
print("\nMLflow Integration:")
print("  - Experiments: /maritime_fleet_guardian/{tenant}/{use_case}")
print("  - Registry: maritime_{use_case}_{tenant}")
print("  - Metadata: tenant_id, training_mode, feature_count, trained_at")
print("=" * 80)

training_pipeline_summary = {
    "script_path": training_script_path,
    "models": [
        {
            "name": "predictive_maintenance",
            "type": "classification",
            "algorithm": "GBTClassifier",
            "metrics": ["accuracy", "f1_score", "precision", "recall"]
        },
        {
            "name": "fuel_optimization",
            "type": "regression",
            "algorithm": "GBTRegressor",
            "metrics": ["rmse", "mae", "r2"]
        },
        {
            "name": "eta_prediction",
            "type": "regression",
            "algorithm": "GBTRegressor",
            "metrics": ["rmse", "mae", "r2"]
        },
        {
            "name": "anomaly_classification",
            "type": "multiclass_classification",
            "algorithm": "GBTClassifier",
            "metrics": ["accuracy", "f1_score"]
        }
    ],
    "mlflow_features": [
        "experiment_tracking",
        "model_registry",
        "tenant_metadata",
        "metrics_logging",
        "model_versioning"
    ]
}
