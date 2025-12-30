"""
Shared Model with Tenant Feature Flags

Implements shared model training approach where a single model is trained
on data from all tenants with tenant-specific feature flags for adaptation.
"""

import os

# Shared model training script
shared_model_script = '''"""
Shared Model Training with Tenant Feature Flags
Train single models on multi-tenant data with tenant identification features
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.evaluation import RegressionEvaluator, MulticlassClassificationEvaluator
import mlflow
import mlflow.spark
from datetime import datetime

mlflow.set_tracking_uri("http://mlflow-service:5000")

def train_shared_model_with_tenant_flags(spark, use_case, model_type="regression"):
    """
    Train a shared model across all tenants with tenant feature flags.
    
    This approach creates a single model that learns tenant-specific patterns
    through encoded tenant features, allowing for efficient model management
    and cross-tenant learning while maintaining tenant isolation.
    """
    
    experiment_name = f"/maritime_fleet_guardian/shared/{use_case}"
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"shared_{use_case}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"):
        mlflow.log_param("training_mode", "shared_with_tenant_flags")
        mlflow.log_param("use_case", use_case)
        mlflow.log_param("tenant_count", 3)
        
        # Load features from all tenants
        tenants = ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]
        
        all_tenant_data = []
        for tenant in tenants:
            tenant_df = spark.read.format("iceberg") \\
                .load(f"maritime_iceberg.training.{tenant}_{use_case}_features")
            
            # Add tenant identifier column
            tenant_df = tenant_df.withColumn("tenant_id", F.lit(tenant))
            all_tenant_data.append(tenant_df)
        
        # Union all tenant data
        combined_df = all_tenant_data[0]
        for df in all_tenant_data[1:]:
            combined_df = combined_df.union(df)
        
        print(f"Combined dataset size: {combined_df.count()} samples across {len(tenants)} tenants")
        mlflow.log_metric("total_training_samples", combined_df.count())
        
        # Encode tenant as features (one-hot encoding)
        tenant_indexer = StringIndexer(inputCol="tenant_id", outputCol="tenant_index")
        tenant_encoder = OneHotEncoder(inputCol="tenant_index", outputCol="tenant_features")
        
        # Example for predictive maintenance
        if use_case == "maintenance":
            feature_cols = [
                "engine_rpm", "engine_temperature", "fuel_consumption_rate", "speed_knots",
                "engine_health_index", "fuel_efficiency", "engine_stress_score",
                "fuel_consumption_24h_avg", "speed_24h_std"
            ]
            label_col = "maintenance_needed"
            
            assembler = VectorAssembler(
                inputCols=feature_cols + ["tenant_features"], 
                outputCol="features_raw"
            )
            scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                                   withMean=True, withStd=True)
            
            if model_type == "classification":
                model = GBTClassifier(labelCol=label_col, featuresCol="features",
                                     maxIter=150, maxDepth=6, seed=42)
            else:
                model = GBTRegressor(labelCol=label_col, featuresCol="features",
                                    maxIter=150, maxDepth=6, seed=42)
        
        # Build full pipeline
        pipeline = Pipeline(stages=[tenant_indexer, tenant_encoder, assembler, scaler, model])
        
        # Train/test split
        train_df, test_df = combined_df.randomSplit([0.8, 0.2], seed=42)
        
        # Train model
        shared_model = pipeline.fit(train_df)
        
        # Evaluate overall and per-tenant
        predictions = shared_model.transform(test_df)
        
        if model_type == "classification":
            evaluator = MulticlassClassificationEvaluator(
                labelCol=label_col, predictionCol="prediction")
            overall_accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "accuracy"})
            overall_f1 = evaluator.evaluate(predictions, {evaluator.metricName: "f1"})
            
            mlflow.log_metric("overall_accuracy", overall_accuracy)
            mlflow.log_metric("overall_f1_score", overall_f1)
            
            print(f"Shared Model - Overall Accuracy: {overall_accuracy:.4f}, F1: {overall_f1:.4f}")
            
            # Per-tenant evaluation
            for tenant in tenants:
                tenant_predictions = predictions.filter(F.col("tenant_id") == tenant)
                tenant_accuracy = evaluator.evaluate(tenant_predictions, {evaluator.metricName: "accuracy"})
                tenant_f1 = evaluator.evaluate(tenant_predictions, {evaluator.metricName: "f1"})
                
                mlflow.log_metric(f"{tenant}_accuracy", tenant_accuracy)
                mlflow.log_metric(f"{tenant}_f1_score", tenant_f1)
                
                print(f"  {tenant}: Accuracy={tenant_accuracy:.4f}, F1={tenant_f1:.4f}")
        
        else:  # regression
            evaluator = RegressionEvaluator(labelCol=label_col, predictionCol="prediction")
            overall_rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
            overall_r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
            
            mlflow.log_metric("overall_rmse", overall_rmse)
            mlflow.log_metric("overall_r2", overall_r2)
            
            print(f"Shared Model - Overall RMSE: {overall_rmse:.4f}, R2: {overall_r2:.4f}")
            
            # Per-tenant evaluation
            for tenant in tenants:
                tenant_predictions = predictions.filter(F.col("tenant_id") == tenant)
                tenant_rmse = evaluator.evaluate(tenant_predictions, {evaluator.metricName: "rmse"})
                tenant_r2 = evaluator.evaluate(tenant_predictions, {evaluator.metricName: "r2"})
                
                mlflow.log_metric(f"{tenant}_rmse", tenant_rmse)
                mlflow.log_metric(f"{tenant}_r2", tenant_r2)
                
                print(f"  {tenant}: RMSE={tenant_rmse:.4f}, R2={tenant_r2:.4f}")
        
        # Register shared model
        mlflow.spark.log_model(
            shared_model,
            "model",
            registered_model_name=f"maritime_{use_case}_shared",
            metadata={
                "training_mode": "shared_with_tenant_flags",
                "use_case": use_case,
                "tenant_count": len(tenants),
                "tenants": ",".join(tenants),
                "feature_count": len(feature_cols) + 2,  # +2 for tenant encoding
                "trained_at": datetime.now().isoformat()
            }
        )
        
        return shared_model

# Model comparison function
def compare_tenant_specific_vs_shared(spark, use_case):
    """
    Compare performance of tenant-specific models vs shared model.
    Helps determine optimal training strategy for each use case.
    """
    
    comparison_results = {
        "use_case": use_case,
        "tenant_specific": {},
        "shared_model": {},
        "recommendation": ""
    }
    
    client = mlflow.tracking.MlflowClient()
    
    # Get metrics for tenant-specific models
    tenants = ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]
    for tenant in tenants:
        model_name = f"maritime_{use_case}_{tenant}"
        try:
            versions = client.search_model_versions(f"name='{model_name}'")
            if versions:
                latest_version = versions[0]
                run = client.get_run(latest_version.run_id)
                comparison_results["tenant_specific"][tenant] = run.data.metrics
        except:
            pass
    
    # Get metrics for shared model
    model_name = f"maritime_{use_case}_shared"
    try:
        versions = client.search_model_versions(f"name='{model_name}'")
        if versions:
            latest_version = versions[0]
            run = client.get_run(latest_version.run_id)
            comparison_results["shared_model"] = run.data.metrics
    except:
        pass
    
    # Simple recommendation logic
    if comparison_results["tenant_specific"] and comparison_results["shared_model"]:
        avg_tenant_perf = sum([m.get("accuracy", m.get("r2", 0)) 
                               for m in comparison_results["tenant_specific"].values()]) / len(tenants)
        shared_perf = comparison_results["shared_model"].get("overall_accuracy", 
                                                             comparison_results["shared_model"].get("overall_r2", 0))
        
        if abs(avg_tenant_perf - shared_perf) < 0.05:  # Within 5%
            comparison_results["recommendation"] = "shared_model"
            comparison_results["reason"] = "Similar performance with simpler deployment"
        elif avg_tenant_perf > shared_perf:
            comparison_results["recommendation"] = "tenant_specific"
            comparison_results["reason"] = "Better per-tenant performance"
        else:
            comparison_results["recommendation"] = "shared_model"
            comparison_results["reason"] = "Better overall performance"
    
    return comparison_results

if __name__ == "__main__":
    spark = SparkSession.builder \\
        .appName("MaritimeFleetGuardian-SharedModelTraining") \\
        .getOrCreate()
    
    # Train shared models for all use cases
    print("Training shared models with tenant feature flags...")
    
    maintenance_shared = train_shared_model_with_tenant_flags(spark, "maintenance", "classification")
    fuel_shared = train_shared_model_with_tenant_flags(spark, "fuel", "regression")
    eta_shared = train_shared_model_with_tenant_flags(spark, "eta", "regression")
    anomaly_shared = train_shared_model_with_tenant_flags(spark, "anomaly", "classification")
    
    print("\\nComparing training strategies...")
    for use_case in ["maintenance", "fuel", "eta", "anomaly"]:
        comparison = compare_tenant_specific_vs_shared(spark, use_case)
        print(f"{use_case}: Recommend {comparison.get('recommendation', 'N/A')}")
    
    spark.stop()
'''

# Save shared model script
shared_script_path = f"{spark_script_dir}/shared_model_training.py"
with open(shared_script_path, 'w') as f:
    f.write(shared_model_script)

print("=" * 80)
print("Shared Model Training with Tenant Flags Generated")
print("=" * 80)
print(f"Script Location: {shared_script_path}")
print(f"Script Size: {len(shared_model_script)} characters")
print("\nShared Model Strategy:")
print("  ✓ Single model trained on multi-tenant data")
print("  ✓ Tenant identification via one-hot encoded features")
print("  ✓ Cross-tenant learning while maintaining isolation")
print("  ✓ Simplified deployment and model management")
print("  ✓ Per-tenant performance evaluation")
print("\nComparison Framework:")
print("  - Compares tenant-specific vs shared model performance")
print("  - Recommends optimal training strategy per use case")
print("  - Considers performance vs operational complexity tradeoffs")
print("\nDeployment Benefits:")
print("  - Single model endpoint vs multiple per-tenant endpoints")
print("  - Reduced storage and compute requirements")
print("  - Easier A/B testing and model updates")
print("  - Potential for better performance on small tenant datasets")
print("=" * 80)

shared_model_summary = {
    "script_path": shared_script_path,
    "strategy": "shared_with_tenant_flags",
    "benefits": [
        "simplified_deployment",
        "cross_tenant_learning",
        "reduced_infrastructure",
        "unified_model_management"
    ],
    "tenant_isolation": "one_hot_encoded_features",
    "comparison_metrics": [
        "per_tenant_performance",
        "overall_performance",
        "operational_complexity"
    ]
}
