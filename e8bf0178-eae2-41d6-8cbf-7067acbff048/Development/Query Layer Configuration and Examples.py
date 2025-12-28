import pandas as pd

# Query layer configuration
query_layer_config = pd.DataFrame({
    'Query_Interface': [
        'OpenSearch Dashboards',
        'Presto CLI',
        'Presto JDBC',
        'Spark SQL',
        'Spark Notebooks'
    ],
    'Use_Cases': [
        'Real-time monitoring, operational dashboards, alerts',
        'Ad-hoc SQL queries, data exploration',
        'BI tool integration (Tableau, PowerBI, Looker)',
        'Complex analytics, batch processing',
        'ML workloads, feature engineering, model training'
    ],
    'Access_URL': [
        'http://localhost:5601',
        'presto-cli --server presto-coordinator:8080',
        'jdbc:presto://localhost:8081/iceberg',
        'spark-sql --master spark://spark-master:7077',
        'http://localhost:8082 (Spark UI)'
    ],
    'Data_Source': [
        'OpenSearch indices',
        'Iceberg tables via Presto',
        'Iceberg tables via Presto',
        'Iceberg tables',
        'Iceberg tables + external sources'
    ],
    'Best_For': [
        'Last 7 days, real-time alerts',
        'Historical analysis, aggregations',
        'Business reporting',
        'Large-scale transformations',
        'Predictive analytics'
    ]
})

print("=" * 100)
print("QUERY LAYER CONFIGURATION")
print("=" * 100)
print(query_layer_config.to_string(index=False))

# OpenSearch query examples
opensearch_queries = """
# OPENSEARCH QUERY EXAMPLES (via Dashboards or API)

# 1. Recent vehicle telemetry (last 1 hour)
GET /telemetry-*/_search
{
  "query": {
    "bool": {
      "must": [
        { "range": { "timestamp": { "gte": "now-1h" } } }
      ]
    }
  },
  "sort": [{ "timestamp": "desc" }],
  "size": 100
}

# 2. Critical alerts
GET /telemetry-*/_search
{
  "query": {
    "bool": {
      "must": [
        { "term": { "telemetry_type": "alert" } },
        { "term": { "severity": "critical" } },
        { "range": { "timestamp": { "gte": "now-24h" } } }
      ]
    }
  }
}

# 3. Vehicle geospatial query
GET /telemetry-*/_search
{
  "query": {
    "bool": {
      "filter": {
        "geo_bounding_box": {
          "location": {
            "top_left": { "lat": 42.0, "lon": -74.0 },
            "bottom_right": { "lat": 40.0, "lon": -72.0 }
          }
        }
      }
    }
  }
}

# 4. Aggregation - events per vehicle (last hour)
GET /telemetry-*/_search
{
  "size": 0,
  "query": {
    "range": { "timestamp": { "gte": "now-1h" } }
  },
  "aggs": {
    "vehicles": {
      "terms": { "field": "vehicle_id", "size": 100 },
      "aggs": {
        "event_count": { "value_count": { "field": "message_id" } }
      }
    }
  }
}
"""

# Presto SQL examples
presto_queries = """
-- PRESTO SQL QUERY EXAMPLES

-- 1. Historical vehicle mileage analysis (last 30 days)
SELECT 
    vehicle_id,
    DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP)) AS day,
    COUNT(*) AS event_count,
    AVG(CAST(JSON_EXTRACT(value, '$.speed') AS DOUBLE)) AS avg_speed,
    SUM(CAST(JSON_EXTRACT(value, '$.distance') AS DOUBLE)) AS total_distance
FROM iceberg.telemetry.raw_events
WHERE timestamp >= CURRENT_DATE - INTERVAL '30' DAY
  AND telemetry_type = 'gps'
GROUP BY vehicle_id, DATE_TRUNC('day', CAST(timestamp AS TIMESTAMP))
ORDER BY day DESC, vehicle_id;

-- 2. Fleet health summary
SELECT 
    vehicle_id,
    telemetry_type,
    COUNT(*) AS event_count,
    MIN(timestamp) AS first_seen,
    MAX(timestamp) AS last_seen
FROM iceberg.telemetry.raw_events
WHERE timestamp >= CURRENT_DATE - INTERVAL '7' DAY
GROUP BY vehicle_id, telemetry_type
ORDER BY vehicle_id, telemetry_type;

-- 3. Long-term trend analysis (monthly aggregates)
SELECT 
    DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)) AS month,
    telemetry_type,
    COUNT(*) AS total_events,
    COUNT(DISTINCT vehicle_id) AS unique_vehicles,
    AVG(DATEDIFF('second', LAG(timestamp) OVER (PARTITION BY vehicle_id ORDER BY timestamp), timestamp)) AS avg_interval_sec
FROM iceberg.telemetry.raw_events
WHERE timestamp >= CURRENT_DATE - INTERVAL '12' MONTH
GROUP BY DATE_TRUNC('month', CAST(timestamp AS TIMESTAMP)), telemetry_type
ORDER BY month DESC, telemetry_type;

-- 4. Cross-system query (join with operational data)
SELECT 
    t.vehicle_id,
    t.timestamp,
    t.telemetry_type,
    v.model,
    v.year,
    v.fleet_id
FROM iceberg.telemetry.raw_events t
JOIN iceberg.operational.vehicles v 
  ON t.vehicle_id = v.vehicle_id
WHERE t.timestamp >= CURRENT_DATE - INTERVAL '1' DAY
  AND v.fleet_id = 'fleet-001';
"""

# Spark SQL examples
spark_queries = """
# SPARK SQL QUERY EXAMPLES

# 1. ML Feature Engineering - Daily vehicle features
spark.sql(\"\"\"
    CREATE OR REPLACE TEMPORARY VIEW vehicle_daily_features AS
    SELECT 
        vehicle_id,
        DATE(timestamp) AS date,
        COUNT(*) AS event_count,
        AVG(CAST(get_json_object(value, '$.speed') AS DOUBLE)) AS avg_speed,
        STDDEV(CAST(get_json_object(value, '$.speed') AS DOUBLE)) AS speed_variance,
        MAX(CAST(get_json_object(value, '$.speed') AS DOUBLE)) AS max_speed,
        SUM(CAST(get_json_object(value, '$.distance') AS DOUBLE)) AS daily_distance,
        COUNT(DISTINCT telemetry_type) AS distinct_event_types
    FROM iceberg.telemetry.raw_events
    WHERE timestamp >= CURRENT_DATE - INTERVAL 90 DAYS
      AND telemetry_type IN ('gps', 'speed', 'odometer')
    GROUP BY vehicle_id, DATE(timestamp)
\"\"\")

# 2. Anomaly Detection Data Prep
df = spark.sql(\"\"\"
    SELECT 
        vehicle_id,
        timestamp,
        CAST(get_json_object(value, '$.engine_temp') AS DOUBLE) AS engine_temp,
        CAST(get_json_object(value, '$.oil_pressure') AS DOUBLE) AS oil_pressure,
        CAST(get_json_object(value, '$.rpm') AS DOUBLE) AS rpm
    FROM iceberg.telemetry.raw_events
    WHERE telemetry_type = 'engine_diagnostics'
      AND timestamp >= CURRENT_DATE - INTERVAL 30 DAYS
\"\"\")

# Apply anomaly detection model
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans

assembler = VectorAssembler(
    inputCols=['engine_temp', 'oil_pressure', 'rpm'],
    outputCol='features'
)
feature_df = assembler.transform(df)

kmeans = KMeans(k=5, seed=1)
model = kmeans.fit(feature_df)
anomalies = model.transform(feature_df)

# 3. Large-scale aggregation and write back to Iceberg
spark.sql(\"\"\"
    CREATE TABLE IF NOT EXISTS iceberg.analytics.vehicle_weekly_summary
    USING iceberg
    PARTITIONED BY (week)
    AS
    SELECT 
        DATE_TRUNC('week', timestamp) AS week,
        vehicle_id,
        COUNT(*) AS total_events,
        SUM(CAST(get_json_object(value, '$.distance') AS DOUBLE)) AS weekly_distance,
        AVG(CAST(get_json_object(value, '$.speed') AS DOUBLE)) AS avg_speed,
        COLLECT_SET(telemetry_type) AS event_types
    FROM iceberg.telemetry.raw_events
    WHERE timestamp >= CURRENT_DATE - INTERVAL 52 WEEKS
    GROUP BY DATE_TRUNC('week', timestamp), vehicle_id
\"\"\")
"""

print("\n" + "=" * 100)
print("OPENSEARCH QUERY EXAMPLES")
print("=" * 100)
print(opensearch_queries)

print("\n" + "=" * 100)
print("PRESTO SQL QUERY EXAMPLES")
print("=" * 100)
print(presto_queries)

print("\n" + "=" * 100)
print("SPARK SQL & ML QUERY EXAMPLES")
print("=" * 100)
print(spark_queries)

# Query routing decision matrix
routing_matrix = pd.DataFrame({
    'Scenario': [
        'Real-time dashboard (<1min old)',
        'Recent alerts (last 24h)',
        'Geospatial queries',
        'Historical analysis (>7 days)',
        'Cross-system joins',
        'Aggregations on months of data',
        'ML feature extraction',
        'Batch transformations',
        'Ad-hoc exploration',
        'BI tool integration'
    ],
    'Recommended_System': [
        'OpenSearch',
        'OpenSearch',
        'OpenSearch',
        'Presto/watsonx.data',
        'Presto/watsonx.data',
        'Presto/watsonx.data',
        'Spark/watsonx.data',
        'Spark/watsonx.data',
        'Presto/watsonx.data',
        'Presto/watsonx.data'
    ],
    'Rationale': [
        'Low latency, real-time indexing',
        'Fast text search, time-based filtering',
        'Native geo queries, spatial indexing',
        'Cost-effective storage, columnar format',
        'SQL joins, schema on read',
        'Columnar storage optimized for aggregations',
        'Python/Scala libraries, distributed compute',
        'Large-scale data processing',
        'Full SQL support, fast for analytics',
        'JDBC/ODBC standard interface'
    ]
})

print("\n" + "=" * 100)
print("QUERY ROUTING DECISION MATRIX")
print("=" * 100)
print(routing_matrix.to_string(index=False))
