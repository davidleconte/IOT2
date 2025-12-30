"""
Spark Feature Engineering Pipeline with Gluten Acceleration

Generates PySpark notebook/script for feature preprocessing and engineering
using Spark with Gluten for accelerated processing.
"""

import os

# PySpark feature engineering script
spark_feature_engineering = '''"""
Feature Engineering Pipeline - PySpark with Gluten Acceleration
Processes features retrieved from Feast for ML model training
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# Initialize Spark session with Gluten
def create_spark_session():
    """Create Spark session with Iceberg and Gluten configuration"""
    
    spark = SparkSession.builder \\
        .appName("MaritimeFleetGuardian-FeatureEngineering") \\
        .config("spark.sql.catalog.maritime_iceberg", "org.apache.iceberg.spark.SparkCatalog") \\
        .config("spark.sql.catalog.maritime_iceberg.type", "hadoop") \\
        .config("spark.sql.catalog.maritime_iceberg.warehouse", "s3a://maritime-lakehouse/warehouse") \\
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \\
        .config("spark.plugins", "io.glutenproject.GlutenPlugin") \\
        .config("spark.gluten.enabled", "true") \\
        .config("spark.gluten.sql.columnar.backend.lib", "velox") \\
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager") \\
        .config("spark.executor.memory", "8g") \\
        .config("spark.executor.cores", "4") \\
        .config("spark.driver.memory", "4g") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.iceberg.vectorization.enabled", "true") \\
        .getOrCreate()
    
    return spark

# Feature engineering functions
def engineer_predictive_maintenance_features(df):
    """Engineer features for predictive maintenance model"""
    
    df = df.withColumn("engine_health_index", 
                       (F.col("engine_temperature") / 100.0) * (F.col("engine_rpm") / 1000.0))
    
    df = df.withColumn("fuel_efficiency", 
                       F.col("speed_knots") / F.when(F.col("fuel_consumption_rate") > 0, 
                                                      F.col("fuel_consumption_rate")).otherwise(1.0))
    
    df = df.withColumn("engine_stress_score",
                       (F.col("engine_rpm") - F.col("aggregation_features__engine_rpm_24h_avg")) / 
                       F.when(F.col("aggregation_features__engine_rpm_24h_std") > 0,
                              F.col("aggregation_features__engine_rpm_24h_std")).otherwise(1.0))
    
    # Label: predict if maintenance needed in next 7 days (simulated)
    df = df.withColumn("maintenance_needed",
                       F.when((F.col("engine_health_index") > 8.0) | 
                              (F.col("engine_stress_score") > 2.5), 1).otherwise(0))
    
    return df

def engineer_fuel_optimization_features(df):
    """Engineer features for fuel optimization model"""
    
    df = df.withColumn("weather_resistance",
                       F.col("wind_speed") * 0.5 + F.col("wave_height") * 2.0)
    
    df = df.withColumn("fuel_per_nautical_mile",
                       F.col("fuel_consumption_rate") / F.when(F.col("speed_knots") > 0,
                                                                F.col("speed_knots")).otherwise(1.0))
    
    df = df.withColumn("fuel_deviation_from_avg",
                       (F.col("fuel_consumption_rate") - F.col("aggregation_features__fuel_consumption_7d_avg")) /
                       F.when(F.col("aggregation_features__fuel_consumption_7d_avg") > 0,
                              F.col("aggregation_features__fuel_consumption_7d_avg")).otherwise(1.0))
    
    # Target: optimal fuel consumption rate
    df = df.withColumn("optimal_fuel_rate",
                       F.col("aggregation_features__fuel_consumption_7d_avg") * 0.92)
    
    return df

def engineer_eta_prediction_features(df):
    """Engineer features for ETA prediction model"""
    
    df = df.withColumn("effective_speed",
                       F.col("speed_knots") - (F.col("wind_speed") * 0.1 + F.col("current_speed")))
    
    df = df.withColumn("estimated_hours_remaining",
                       F.col("distance_to_destination") / F.when(F.col("effective_speed") > 0,
                                                                   F.col("effective_speed")).otherwise(1.0))
    
    df = df.withColumn("speed_variability",
                       F.abs(F.col("speed_knots") - F.col("aggregation_features__speed_24h_avg")))
    
    # Target: actual arrival time (hours from now)
    df = df.withColumn("actual_eta_hours",
                       F.col("estimated_hours_remaining") * F.lit(1.1))  # Add 10% buffer
    
    return df

def engineer_anomaly_classification_features(df):
    """Engineer features for anomaly classification model"""
    
    df = df.withColumn("total_anomaly_score",
                       F.col("fuel_anomaly_score") + 
                       F.col("speed_anomaly_score") + 
                       F.col("engine_anomaly_score"))
    
    df = df.withColumn("anomaly_persistence",
                       F.col("consecutive_anomalies") / 10.0)
    
    # Multi-class label: normal, minor_anomaly, major_anomaly, critical
    df = df.withColumn("anomaly_class",
                       F.when(F.col("total_anomaly_score") < 2.0, "normal")
                       .when(F.col("total_anomaly_score") < 5.0, "minor_anomaly")
                       .when(F.col("total_anomaly_score") < 8.0, "major_anomaly")
                       .otherwise("critical"))
    
    return df

# ML Pipeline builder
def build_ml_pipeline(feature_cols, label_col, model_type="regression"):
    """Build ML pipeline with preprocessing steps"""
    
    # Vector assembler
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    
    # Standard scaler
    scaler = StandardScaler(inputCol="features_raw", outputCol="features", 
                           withMean=True, withStd=True)
    
    stages = [assembler, scaler]
    
    # Add model based on type
    if model_type == "regression":
        from pyspark.ml.regression import GBTRegressor
        model = GBTRegressor(labelCol=label_col, featuresCol="features", 
                           maxIter=100, maxDepth=5)
        stages.append(model)
    elif model_type == "classification":
        from pyspark.ml.classification import GBTClassifier
        # Need to index string labels first
        label_indexer = StringIndexer(inputCol=label_col, outputCol="label_indexed")
        model = GBTClassifier(labelCol="label_indexed", featuresCol="features",
                             maxIter=100, maxDepth=5)
        stages.extend([label_indexer, model])
    
    pipeline = Pipeline(stages=stages)
    return pipeline

# Main execution
if __name__ == "__main__":
    spark = create_spark_session()
    
    # Read features from Iceberg (after Feast materialization)
    tenant_id = "shipping-co-alpha"
    features_df = spark.read.format("iceberg") \\
        .load(f"maritime_iceberg.features.{tenant_id}_training_features")
    
    print(f"Loaded {features_df.count()} training samples")
    
    # Engineer features for each use case
    maintenance_df = engineer_predictive_maintenance_features(features_df)
    fuel_df = engineer_fuel_optimization_features(features_df)
    eta_df = engineer_eta_prediction_features(features_df)
    anomaly_df = engineer_anomaly_classification_features(features_df)
    
    # Write engineered features back to Iceberg
    maintenance_df.write.format("iceberg").mode("overwrite") \\
        .saveAsTable(f"maritime_iceberg.training.{tenant_id}_maintenance_features")
    
    fuel_df.write.format("iceberg").mode("overwrite") \\
        .saveAsTable(f"maritime_iceberg.training.{tenant_id}_fuel_features")
    
    eta_df.write.format("iceberg").mode("overwrite") \\
        .saveAsTable(f"maritime_iceberg.training.{tenant_id}_eta_features")
    
    anomaly_df.write.format("iceberg").mode("overwrite") \\
        .saveAsTable(f"maritime_iceberg.training.{tenant_id}_anomaly_features")
    
    print("Feature engineering completed successfully")
    spark.stop()
'''

# Save Spark feature engineering script
spark_script_dir = "ml/training/spark-jobs"
os.makedirs(spark_script_dir, exist_ok=True)

spark_fe_path = f"{spark_script_dir}/feature_engineering.py"
with open(spark_fe_path, 'w') as f:
    f.write(spark_feature_engineering)

print("=" * 80)
print("Spark Feature Engineering Pipeline Generated")
print("=" * 80)
print(f"Script Location: {spark_fe_path}")
print(f"Script Size: {len(spark_feature_engineering)} characters")
print("\nFeature Engineering Capabilities:")
print("  - Gluten/Velox acceleration for columnar processing")
print("  - Iceberg table read/write operations")
print("  - Use case specific feature transformations")
print("  - ML Pipeline construction with preprocessing")
print("\nEngineered Feature Types:")
print("  - Predictive Maintenance: engine_health_index, fuel_efficiency, engine_stress_score")
print("  - Fuel Optimization: weather_resistance, fuel_per_nautical_mile, fuel_deviation")
print("  - ETA Prediction: effective_speed, estimated_hours_remaining, speed_variability")
print("  - Anomaly Classification: total_anomaly_score, anomaly_persistence")
print("=" * 80)

spark_fe_summary = {
    "script_path": spark_fe_path,
    "acceleration": "Gluten with Velox backend",
    "storage_format": "Apache Iceberg",
    "engineered_features": {
        "predictive_maintenance": 4,
        "fuel_optimization": 4,
        "eta_prediction": 4,
        "anomaly_classification": 3
    }
}
