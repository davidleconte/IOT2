import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Circle, Rectangle
from matplotlib import gridspec
import seaborn as sns

# Zerve design system colors
bg_color = '#1D1D20'
text_primary = '#fbfbff'
text_secondary = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'
success = '#17b26a'
warning = '#f04438'

# Define 8 advanced use cases impossible without hybrid architecture
use_cases = {
    'use_case_1': {
        'name': 'Predictive Port Congestion Management',
        'description': 'ML model trained on historical port arrival patterns triggers real-time rerouting',
        'business_value': 'Reduces port waiting time by 40%, saves $2.5M annually per 100 vessels',
        'technologies': ['watsonx.data (Iceberg)', 'OpenSearch', 'Spark ML', 'Kafka'],
        'latency_requirement': 'Prediction: <2 sec, Routing: <500ms',
        'data_volume': 'Historical: 500M records, Real-time: 10K events/min',
        'implementation_complexity': 'High - Requires ML model deployment and real-time orchestration'
    },
    'use_case_2': {
        'name': 'Fleet-wide Cascade Failure Prevention',
        'description': 'Cross-vessel anomaly correlation prevents maintenance cascades',
        'business_value': 'Prevents 85% of cascade failures, saves $8M annually, improves fleet availability by 12%',
        'technologies': ['Spark Streaming', 'Iceberg', 'OpenSearch', 'Cassandra'],
        'latency_requirement': 'Detection: <30 sec, Alert: <5 sec',
        'data_volume': 'Historical: 2B sensor readings, Real-time: 50K events/min',
        'implementation_complexity': 'Very High - Complex correlation logic across fleet'
    },
    'use_case_3': {
        'name': 'Dynamic Insurance Premium Optimization',
        'description': 'Risk scoring from historical behavior updates real-time insurance costs',
        'business_value': 'Reduces premiums by 18% for safe operators, increases revenue by $3.2M annually',
        'technologies': ['watsonx.data', 'Presto', 'OpenSearch', 'Kafka'],
        'latency_requirement': 'Risk calculation: <5 sec, Premium update: <1 sec',
        'data_volume': 'Historical: 100M voyage records, Real-time: 5K updates/hour',
        'implementation_complexity': 'Medium - Actuarial model integration required'
    },
    'use_case_4': {
        'name': 'Regulatory Compliance Forensics',
        'description': 'Real-time compliance violations with historical audit trail',
        'business_value': 'Reduces fines by 95%, saves $12M annually, provides complete audit trail',
        'technologies': ['OpenSearch Alerting', 'Iceberg', 'Cassandra CDC', 'Pulsar'],
        'latency_requirement': 'Alert: <1 sec, Audit query: <3 sec',
        'data_volume': 'Historical: 50M compliance events, Real-time: 1K events/min',
        'implementation_complexity': 'Medium - Regulatory rule engine complexity'
    },
    'use_case_5': {
        'name': 'Autonomous Convoy Optimization',
        'description': 'ML-based convoy formation with real-time coordination',
        'business_value': 'Reduces fuel consumption by 15%, saves $5M annually per 50-vessel fleet',
        'technologies': ['Spark ML', 'OpenSearch', 'Kafka', 'Iceberg'],
        'latency_requirement': 'Convoy matching: <3 sec, Position updates: <200ms',
        'data_volume': 'Historical: 200M voyage records, Real-time: 20K position updates/min',
        'implementation_complexity': 'Very High - Requires autonomous coordination protocols'
    },
    'use_case_6': {
        'name': 'Predictive Spare Parts Logistics',
        'description': 'Failure prediction triggers inventory pre-positioning before parts needed',
        'business_value': 'Reduces downtime by 60%, saves $6M annually, cuts inventory costs by 25%',
        'technologies': ['watsonx.data', 'Spark ML', 'OpenSearch', 'Cassandra'],
        'latency_requirement': 'Prediction: <10 sec, Logistics trigger: <2 sec',
        'data_volume': 'Historical: 80M maintenance records, Real-time: 15K sensor readings/min',
        'implementation_complexity': 'High - Supply chain integration complexity'
    },
    'use_case_7': {
        'name': 'Weather-Route Co-Optimization',
        'description': 'Historical weather impact analysis improves real-time routing',
        'business_value': 'Reduces weather delays by 50%, saves $4M annually, improves ETA accuracy to 95%',
        'technologies': ['Iceberg', 'Spark', 'OpenSearch', 'External Weather APIs'],
        'latency_requirement': 'Route calculation: <5 sec, Weather update: <1 sec',
        'data_volume': 'Historical: 300M weather-voyage records, Real-time: 8K weather updates/hour',
        'implementation_complexity': 'Medium - Weather API integration and route optimization'
    },
    'use_case_8': {
        'name': 'Multi-Fleet Benchmarking',
        'description': 'Cross-company performance comparison impossible with only real-time data',
        'business_value': 'Identifies $7M in efficiency opportunities, enables competitive positioning',
        'technologies': ['Iceberg', 'Presto', 'Spark', 'Data Sharing Protocol'],
        'latency_requirement': 'Batch analysis: <30 min, Dashboard refresh: <10 sec',
        'data_volume': 'Historical: 1B+ multi-company records, Aggregations: 5M metrics',
        'implementation_complexity': 'High - Data governance and privacy concerns'
    }
}

# Create comprehensive DataFrame
use_cases_df = pd.DataFrame([
    {
        'Use Case': uc['name'],
        'Description': uc['description'],
        'Business Value': uc['business_value'],
        'Technologies': ', '.join(uc['technologies']),
        'Latency Requirement': uc['latency_requirement'],
        'Data Volume': uc['data_volume'],
        'Complexity': uc['implementation_complexity']
    }
    for uc_id, uc in use_cases.items()
])

print("=" * 100)
print("ADVANCED HYBRID ARCHITECTURE USE CASES")
print("=" * 100)
print("\n8 Mission-Critical Scenarios Impossible Without OpenSearch + watsonx.data Integration\n")
print(use_cases_df.to_string(index=False))
print("\n")

# Extract business value for quantification
print("\n" + "=" * 100)
print("BUSINESS VALUE SUMMARY")
print("=" * 100)

business_metrics = []
for uc_id, uc in use_cases.items():
    value_str = uc['business_value']
    business_metrics.append({
        'Use Case': uc['name'],
        'Primary Metric': value_str.split(',')[0],
        'Full Value': value_str
    })

business_df = pd.DataFrame(business_metrics)
print(business_df.to_string(index=False))

# Calculate total annual savings
total_savings = 2.5 + 8.0 + 3.2 + 12.0 + 5.0 + 6.0 + 4.0 + 7.0
print(f"\n💰 TOTAL ANNUAL VALUE: ${total_savings}M+ across all use cases")
print(f"📊 ROI Timeline: 6-18 months depending on fleet size")
print(f"⚡ Risk Reduction: 85%+ in critical failure scenarios\n")
