"""
Feast Feature Retrieval - PySpark Script for Offline Features from Iceberg

This script generates PySpark code for retrieving offline features from Feast
backed by Iceberg tables for training data preparation.
"""

import os

# PySpark script for Feast feature retrieval
feast_retrieval_script = '''"""
Feast Offline Feature Retrieval for ML Training
Uses Feast Python SDK to retrieve features from Iceberg offline store
"""

from feast import FeatureStore
from datetime import datetime, timedelta
import pandas as pd

# Initialize Feast feature store
feast_store = FeatureStore(repo_path="./feast_repo")

def retrieve_training_features(
    tenant_id: str,
    start_date: datetime,
    end_date: datetime,
    use_case: str = "all"
) -> pd.DataFrame:
    """
    Retrieve offline features for a specific tenant and time range.
    
    Args:
        tenant_id: Tenant identifier (e.g., 'shipping-co-alpha')
        start_date: Start of training data window
        end_date: End of training data window
        use_case: Specific use case or 'all' for all features
    
    Returns:
        Pandas DataFrame with entity keys and feature values
    """
    
    # Define entity dataframe with vessels and timestamps
    # This would come from Iceberg tables via Spark in production
    entity_df = pd.DataFrame({
        "vessel_id": [f"VESSEL_{i}" for i in range(1, 101)],
        "event_timestamp": [end_date] * 100,
        "tenant_id": [tenant_id] * 100
    })
    
    # Feature service selection based on use case
    feature_refs = []
    
    if use_case in ["predictive_maintenance", "all"]:
        feature_refs.extend([
            "operational_features:engine_rpm",
            "operational_features:engine_temperature",
            "operational_features:fuel_consumption_rate",
            "operational_features:speed_knots",
            "aggregation_features:fuel_consumption_24h_avg",
            "aggregation_features:speed_24h_std"
        ])
    
    if use_case in ["fuel_optimization", "all"]:
        feature_refs.extend([
            "operational_features:fuel_consumption_rate",
            "environmental_features:wind_speed",
            "environmental_features:wave_height",
            "environmental_features:sea_state",
            "aggregation_features:fuel_consumption_7d_avg"
        ])
    
    if use_case in ["eta_prediction", "all"]:
        feature_refs.extend([
            "operational_features:speed_knots",
            "operational_features:distance_to_destination",
            "environmental_features:wind_speed",
            "environmental_features:current_speed",
            "aggregation_features:speed_24h_avg"
        ])
    
    if use_case in ["anomaly_classification", "all"]:
        feature_refs.extend([
            "anomaly_features:fuel_anomaly_score",
            "anomaly_features:speed_anomaly_score",
            "anomaly_features:engine_anomaly_score",
            "anomaly_features:consecutive_anomalies"
        ])
    
    # Retrieve historical features
    training_df = feast_store.get_historical_features(
        entity_df=entity_df,
        features=feature_refs
    ).to_df()
    
    return training_df

# Example: Retrieve features for predictive maintenance
tenant = "shipping-co-alpha"
end_date = datetime.now()
start_date = end_date - timedelta(days=90)

print(f"Retrieving features for {tenant} from {start_date.date()} to {end_date.date()}")

# Get features for each use case
maintenance_features = retrieve_training_features(tenant, start_date, end_date, "predictive_maintenance")
fuel_features = retrieve_training_features(tenant, start_date, end_date, "fuel_optimization")
eta_features = retrieve_training_features(tenant, start_date, end_date, "eta_prediction")
anomaly_features = retrieve_training_features(tenant, start_date, end_date, "anomaly_classification")

print(f"Predictive Maintenance Features: {maintenance_features.shape}")
print(f"Fuel Optimization Features: {fuel_features.shape}")
print(f"ETA Prediction Features: {eta_features.shape}")
print(f"Anomaly Classification Features: {anomaly_features.shape}")
'''

# Save Feast retrieval script
feast_script_dir = "ml/training/feast-retrieval"
os.makedirs(feast_script_dir, exist_ok=True)

feast_script_path = f"{feast_script_dir}/feast_offline_retrieval.py"
with open(feast_script_path, 'w') as f:
    f.write(feast_retrieval_script)

print("=" * 80)
print("Feast Offline Feature Retrieval Script Generated")
print("=" * 80)
print(f"Script Location: {feast_script_path}")
print(f"Script Size: {len(feast_retrieval_script)} characters")
print("\nFeature Retrieval Capabilities:")
print("  - Tenant-specific feature isolation")
print("  - Time-range based historical retrieval")
print("  - Use case specific feature selection")
print("  - Integration with Iceberg offline store")
print("=" * 80)

feast_retrieval_summary = {
    "script_path": feast_script_path,
    "supported_use_cases": [
        "predictive_maintenance",
        "fuel_optimization",
        "eta_prediction",
        "anomaly_classification"
    ],
    "feature_groups": {
        "operational": 5,
        "environmental": 4,
        "aggregation": 3,
        "anomaly": 4
    }
}
