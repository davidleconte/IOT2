import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

# Anomaly pattern clustering using K-means
# Feature engineering for anomaly characterization

np.random.seed(42)

# Create feature vectors for each anomaly
# Features: anomaly_score, confidence, hour_of_day, vessel_activity_level
anomaly_features = realtime_anomalies_filtered.copy()
anomaly_features['hour_of_day'] = anomaly_features['timestamp'].dt.hour
anomaly_features['day_of_week'] = anomaly_features['timestamp'].dt.dayofweek
anomaly_features['time_since_first'] = (
    anomaly_features['timestamp'] - anomaly_features['timestamp'].min()
).dt.total_seconds() / 3600  # hours since first anomaly

# Encode categorical feature_name
feature_encoding = {
    'temperature': 1, 'vibration': 2, 'pressure': 3, 
    'rpm': 4, 'fuel_consumption': 5
}
anomaly_features['feature_encoded'] = anomaly_features['feature_name'].map(feature_encoding)

# Select features for clustering
clustering_features = ['anomaly_score', 'confidence', 'anomaly_grade', 
                       'hour_of_day', 'feature_encoded', 'time_since_first']

X = anomaly_features[clustering_features].values

# Standardize features
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# K-means clustering with optimal k=4 (elbow method result)
optimal_k = 4
kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
anomaly_features['cluster'] = kmeans.fit_predict(X_scaled)

# PCA for visualization
pca = PCA(n_components=2)
X_pca = pca.fit_transform(X_scaled)
anomaly_features['pca1'] = X_pca[:, 0]
anomaly_features['pca2'] = X_pca[:, 1]

print("=" * 80)
print("Anomaly Pattern Clustering Analysis (K-Means)")
print("=" * 80)

print(f"\nNumber of clusters: {optimal_k}")
print(f"Total anomalies clustered: {len(anomaly_features)}")
print(f"PCA explained variance: {pca.explained_variance_ratio_.sum():.2%}")

# Analyze each cluster
print("\n" + "=" * 80)
print("CLUSTER CHARACTERISTICS")
print("=" * 80)

for cluster_id in range(optimal_k):
    cluster_data = anomaly_features[anomaly_features['cluster'] == cluster_id]
    
    print(f"\n{'─' * 80}")
    print(f"CLUSTER {cluster_id} - {len(cluster_data)} anomalies ({len(cluster_data)/len(anomaly_features)*100:.1f}%)")
    print(f"{'─' * 80}")
    
    print(f"  Avg Anomaly Score:     {cluster_data['anomaly_score'].mean():.3f}")
    print(f"  Avg Confidence:        {cluster_data['confidence'].mean():.3f}")
    print(f"  Most Common Feature:   {cluster_data['feature_name'].mode()[0]}")
    print(f"  Most Common Vessel:    {cluster_data['vessel_id'].mode()[0]}")
    print(f"  Peak Hour:             {cluster_data['hour_of_day'].mode()[0]}:00")
    print(f"  Vessels Affected:      {cluster_data['vessel_id'].nunique()}")
    
    # Cluster interpretation
    avg_score = cluster_data['anomaly_score'].mean()
    avg_conf = cluster_data['confidence'].mean()
    
    if avg_score > 0.85 and avg_conf > 0.90:
        pattern_type = "🔴 PERSISTENT HIGH-SEVERITY"
    elif avg_score > 0.80 and avg_conf > 0.85:
        pattern_type = "🟠 MODERATE PERSISTENT"
    elif avg_score < 0.80 and avg_conf < 0.85:
        pattern_type = "🟡 TRANSIENT LOW-SEVERITY"
    else:
        pattern_type = "⚪ MIXED PATTERN"
    
    print(f"  Pattern Type:          {pattern_type}")

# Cluster summary table
cluster_summary = anomaly_features.groupby('cluster').agg({
    'anomaly_score': ['count', 'mean', 'std'],
    'confidence': 'mean',
    'vessel_id': 'nunique',
    'feature_name': lambda x: x.mode()[0] if len(x) > 0 else 'unknown'
}).round(3)

cluster_summary.columns = ['Count', 'Avg_Score', 'Std_Score', 'Avg_Confidence', 
                           'Vessels', 'Top_Feature']

print("\n" + "=" * 80)
print("CLUSTER SUMMARY TABLE")
print("=" * 80)
print(cluster_summary.to_string())

# Pattern recurrence analysis
# Identify which clusters appear repeatedly (recurring patterns)
cluster_time_distribution = anomaly_features.groupby(['cluster', 'hour_of_day']).size().reset_index(name='occurrences')
recurring_patterns = cluster_time_distribution[cluster_time_distribution['occurrences'] >= 3]

print("\n" + "=" * 80)
print("RECURRING PATTERNS (3+ occurrences at same hour)")
print("=" * 80)
if len(recurring_patterns) > 0:
    print(recurring_patterns.to_string(index=False))
    print(f"\n✓ {len(recurring_patterns)} recurring patterns detected")
    print("  → These patterns suggest systematic issues requiring investigation")
else:
    print("No strong recurring patterns detected in this 24h window")

# Cluster center interpretation (in original feature space)
cluster_centers_original = scaler.inverse_transform(kmeans.cluster_centers_)
center_df = pd.DataFrame(
    cluster_centers_original,
    columns=clustering_features
).round(3)
center_df.index.name = 'Cluster'

print("\n" + "=" * 80)
print("CLUSTER CENTERS (Original Feature Space)")
print("=" * 80)
print(center_df.to_string())

# Presto query for clustering historical patterns
clustering_query = """
-- Presto Query: Cluster Historical Anomaly Patterns
-- Uses array_agg and statistical functions for pattern identification

WITH anomaly_vectors AS (
    SELECT 
        vessel_id,
        feature_name,
        date_trunc('hour', timestamp) as hour,
        AVG(anomaly_score) as avg_score,
        AVG(confidence) as avg_confidence,
        COUNT(*) as occurrence_count,
        STDDEV(anomaly_score) as score_variance
    FROM iceberg.fleet_analytics.anomaly_history
    WHERE timestamp >= CURRENT_DATE - INTERVAL '30' DAY
      AND confidence > 0.7
    GROUP BY vessel_id, feature_name, date_trunc('hour', timestamp)
),
pattern_classification AS (
    SELECT *,
        CASE 
            WHEN avg_score > 0.85 AND avg_confidence > 0.90 
                THEN 'persistent_high'
            WHEN avg_score > 0.80 AND avg_confidence > 0.85 
                THEN 'persistent_moderate'
            WHEN occurrence_count > 5 
                THEN 'recurring'
            ELSE 'transient'
        END as pattern_type
    FROM anomaly_vectors
)
SELECT 
    pattern_type,
    COUNT(*) as pattern_count,
    AVG(avg_score) as typical_severity,
    COUNT(DISTINCT vessel_id) as vessels_affected,
    array_agg(DISTINCT feature_name) as affected_features
FROM pattern_classification
GROUP BY pattern_type
ORDER BY pattern_count DESC;
"""

print("\n" + "=" * 80)
print("PRESTO CLUSTERING QUERY")
print("=" * 80)
print(clustering_query)