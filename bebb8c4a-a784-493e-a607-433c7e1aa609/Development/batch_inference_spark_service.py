"""
Batch Inference Service - Spark Jobs for Batch Predictions
===========================================================
Reads from Iceberg lakehouse, loads models from MLflow, writes predictions back to Iceberg.
Supports tenant-specific model routing and batch processing at scale.
"""

import os

batch_inference_base = "services/inference"
batch_dir = f"{batch_inference_base}/batch-inference"
os.makedirs(f"{batch_dir}/src", exist_ok=True)

# ========================================
# BATCH INFERENCE SPARK JOB
# ========================================

batch_inference_code = '''"""
Batch Inference Service using Spark
Reads data from Iceberg, loads models from MLflow, writes predictions back to Iceberg
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import PipelineModel
import mlflow
import mlflow.spark
from datetime import datetime, timedelta
import sys

mlflow.set_tracking_uri("http://mlflow-service:5000")

class BatchInferenceService:
    """
    Batch inference service for maritime ML models
    Processes large batches of historical data for predictions
    """
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.mlflow_client = mlflow.tracking.MlflowClient()
    
    def load_model_for_tenant(self, tenant_id: str, use_case: str, version: str = "latest"):
        """
        Load ML model from MLflow registry for specific tenant
        Supports tenant-specific and shared model routing
        """
        # Try tenant-specific model first
        tenant_model_name = f"maritime_{use_case}_{tenant_id}"
        
        try:
            if version == "latest":
                model_version = self.mlflow_client.get_latest_versions(tenant_model_name, stages=["Production"])
                if not model_version:
                    model_version = self.mlflow_client.get_latest_versions(tenant_model_name, stages=["None"])
                if model_version:
                    model_uri = f"models:/{tenant_model_name}/{model_version[0].version}"
                    print(f"Loading tenant-specific model: {model_uri}")
                    return mlflow.spark.load_model(model_uri), "tenant_specific"
            else:
                model_uri = f"models:/{tenant_model_name}/{version}"
                print(f"Loading tenant-specific model: {model_uri}")
                return mlflow.spark.load_model(model_uri), "tenant_specific"
        except Exception as e:
            print(f"Tenant-specific model not found: {e}")
        
        # Fallback to shared model
        shared_model_name = f"maritime_{use_case}_shared"
        try:
            if version == "latest":
                model_version = self.mlflow_client.get_latest_versions(shared_model_name, stages=["Production"])
                if not model_version:
                    model_version = self.mlflow_client.get_latest_versions(shared_model_name, stages=["None"])
                if model_version:
                    model_uri = f"models:/{shared_model_name}/{model_version[0].version}"
                    print(f"Loading shared model: {model_uri}")
                    return mlflow.spark.load_model(model_uri), "shared"
            else:
                model_uri = f"models:/{shared_model_name}/{version}"
                print(f"Loading shared model: {model_uri}")
                return mlflow.spark.load_model(model_uri), "shared"
        except Exception as e:
            raise RuntimeError(f"Failed to load model for {tenant_id}/{use_case}: {e}")
    
    def run_batch_inference(self, tenant_id: str, use_case: str, start_date: str, end_date: str, 
                           model_version: str = "latest"):
        """
        Run batch inference for a specific tenant and use case
        
        Args:
            tenant_id: Tenant identifier
            use_case: One of ['maintenance', 'fuel', 'eta', 'anomaly']
            start_date: Start date for batch processing (YYYY-MM-DD)
            end_date: End date for batch processing (YYYY-MM-DD)
            model_version: MLflow model version or 'latest'
        """
        print("=" * 80)
        print(f"BATCH INFERENCE: {tenant_id} / {use_case}")
        print(f"Date Range: {start_date} to {end_date}")
        print("=" * 80)
        
        # Load model from MLflow
        model, model_type = self.load_model_for_tenant(tenant_id, use_case, model_version)
        
        # Read processed features from Iceberg
        input_table = f"maritime_iceberg.{tenant_id}.processed_features"
        print(f"Reading from Iceberg: {input_table}")
        
        input_df = self.spark.read.format("iceberg") \\
            .load(input_table) \\
            .filter(
                (F.col("timestamp_utc") >= start_date) & 
                (F.col("timestamp_utc") <= end_date)
            )
        
        record_count = input_df.count()
        print(f"Input records: {record_count}")
        
        if record_count == 0:
            print("No data to process. Skipping inference.")
            return
        
        # Run predictions
        print(f"Running inference with {model_type} model...")
        predictions_df = model.transform(input_df)
        
        # Add metadata columns
        predictions_df = predictions_df \\
            .withColumn("inference_timestamp", F.current_timestamp()) \\
            .withColumn("model_version", F.lit(model_version)) \\
            .withColumn("model_type", F.lit(model_type)) \\
            .withColumn("use_case", F.lit(use_case))
        
        # Select output columns based on use case
        if use_case in ["maintenance", "anomaly"]:
            # Classification output
            output_df = predictions_df.select(
                "tenant_id", "vessel_id", "timestamp_utc",
                "prediction", "probability",
                "inference_timestamp", "model_version", "model_type", "use_case"
            )
        else:
            # Regression output (fuel, eta)
            output_df = predictions_df.select(
                "tenant_id", "vessel_id", "timestamp_utc",
                "prediction",
                "inference_timestamp", "model_version", "model_type", "use_case"
            )
        
        # Write predictions back to Iceberg
        output_table = f"maritime_iceberg.{tenant_id}.{use_case}_predictions"
        print(f"Writing predictions to Iceberg: {output_table}")
        
        output_df.writeTo(output_table) \\
            .using("iceberg") \\
            .tableProperty("write.format.default", "parquet") \\
            .tableProperty("write.parquet.compression-codec", "zstd") \\
            .partitionedBy("timestamp_utc") \\
            .append()
        
        prediction_count = output_df.count()
        print(f"✓ Wrote {prediction_count} predictions to {output_table}")
        
        # Log metrics
        print(f"\\nBatch Inference Summary:")
        print(f"  Tenant: {tenant_id}")
        print(f"  Use Case: {use_case}")
        print(f"  Model Type: {model_type}")
        print(f"  Records Processed: {record_count}")
        print(f"  Predictions Generated: {prediction_count}")
        print(f"  Date Range: {start_date} to {end_date}")
        print("=" * 80)


def main():
    """Main entry point for batch inference"""
    if len(sys.argv) < 5:
        print("Usage: python batch_inference.py <tenant_id> <use_case> <start_date> <end_date> [model_version]")
        print("Example: python batch_inference.py shipping-co-alpha maintenance 2024-01-01 2024-01-31 latest")
        sys.exit(1)
    
    tenant_id = sys.argv[1]
    use_case = sys.argv[2]
    start_date = sys.argv[3]
    end_date = sys.argv[4]
    model_version = sys.argv[5] if len(sys.argv) > 5 else "latest"
    
    # Initialize Spark
    spark = SparkSession.builder \\
        .appName(f"BatchInference-{tenant_id}-{use_case}") \\
        .getOrCreate()
    
    # Run batch inference
    service = BatchInferenceService(spark)
    service.run_batch_inference(tenant_id, use_case, start_date, end_date, model_version)
    
    spark.stop()


if __name__ == "__main__":
    main()
'''

batch_inference_path = f"{batch_dir}/src/batch_inference.py"
with open(batch_inference_path, 'w') as f:
    f.write(batch_inference_code)

# Dockerfile for batch inference
batch_dockerfile = '''FROM apache/spark-py:3.4.0

USER root

# Install Python dependencies
RUN pip install --no-cache-dir \\
    mlflow==2.9.2 \\
    pyspark==3.4.0 \\
    pyiceberg[spark]==0.5.1

# Copy batch inference script
COPY src/batch_inference.py /app/batch_inference.py

WORKDIR /app

USER 185
'''

batch_dockerfile_path = f"{batch_dir}/Dockerfile"
with open(batch_dockerfile_path, 'w') as f:
    f.write(batch_dockerfile)

# Requirements.txt
batch_requirements = '''mlflow==2.9.2
pyspark==3.4.0
pyiceberg[spark]==0.5.1
'''

batch_requirements_path = f"{batch_dir}/requirements.txt"
with open(batch_requirements_path, 'w') as f:
    f.write(batch_requirements)

print("=" * 80)
print("BATCH INFERENCE SERVICE GENERATED")
print("=" * 80)
print(f"Directory: {batch_dir}/")
print(f"  • batch_inference.py: {len(batch_inference_code)} characters")
print(f"  • Dockerfile: Spark 3.4.0 + MLflow + PyIceberg")
print(f"  • requirements.txt: Python dependencies")
print("\nCapabilities:")
print("  ✓ Reads from Iceberg lakehouse (processed features)")
print("  ✓ Loads models from MLflow registry")
print("  ✓ Tenant-specific model routing (fallback to shared)")
print("  ✓ Batch prediction processing")
print("  ✓ Writes predictions back to Iceberg")
print("  ✓ Partitioned by timestamp for efficient queries")
print("  ✓ ZSTD compression for storage efficiency")
print("\nUsage:")
print(f"  spark-submit batch_inference.py <tenant_id> <use_case> <start_date> <end_date> [version]")
print("=" * 80)

batch_inference_summary = {
    "directory": batch_dir,
    "service": "batch_inference",
    "files": [batch_inference_path, batch_dockerfile_path, batch_requirements_path],
    "capabilities": [
        "iceberg_read",
        "mlflow_model_loading",
        "tenant_routing",
        "batch_processing",
        "iceberg_write"
    ]
}
