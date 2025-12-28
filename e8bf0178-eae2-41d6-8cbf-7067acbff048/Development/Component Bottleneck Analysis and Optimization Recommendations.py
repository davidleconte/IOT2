import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# Extract detailed component breakdown from load test results
component_breakdown_data = []

for result in all_scenario_results:
    scenario = result['Scenario']
    for component, p95_latency in result['Component Breakdown'].items():
        component_breakdown_data.append({
            'Scenario': scenario,
            'Component': component,
            'P95 Latency (ms)': round(p95_latency, 2),
            'Percentage of Total': round((p95_latency / result['P95 (ms)']) * 100, 1)
        })

component_breakdown_df = pd.DataFrame(component_breakdown_data)

print("=" * 80)
print("COMPONENT-LEVEL LATENCY BREAKDOWN (P95)")
print("=" * 80)
print()

# Display by scenario
for scenario in ['REROUTE_TO_ALTERNATE_HARBOR', 'DEPLOY_HELICOPTER_RESCUE', 
                 'REDUCE_SPEED_GRADUAL', 'EMERGENCY_STOP']:
    scenario_data = component_breakdown_df[component_breakdown_df['Scenario'] == scenario]
    print(f"\n{scenario}:")
    print(scenario_data[['Component', 'P95 Latency (ms)', 'Percentage of Total']].to_string(index=False))

# Identify bottlenecks across all scenarios
print("\n\n" + "=" * 80)
print("BOTTLENECK ANALYSIS - SLOWEST COMPONENTS")
print("=" * 80)

# Calculate average P95 latency by component across all scenarios
avg_component_latency = component_breakdown_df.groupby('Component')['P95 Latency (ms)'].agg(['mean', 'max', 'min'])
avg_component_latency = avg_component_latency.sort_values('mean', ascending=False)
avg_component_latency['mean'] = avg_component_latency['mean'].round(2)
avg_component_latency['max'] = avg_component_latency['max'].round(2)
avg_component_latency['min'] = avg_component_latency['min'].round(2)

print("\nAverage P95 Latency by Component (across all scenarios):")
print(avg_component_latency.to_string())

# Identify top bottlenecks
top_3_bottlenecks = avg_component_latency.head(3).index.tolist()
print(f"\n🔴 Top 3 Bottlenecks:")
for i, component in enumerate(top_3_bottlenecks, 1):
    avg_lat = avg_component_latency.loc[component, 'mean']
    max_lat = avg_component_latency.loc[component, 'max']
    print(f"   {i}. {component}: {avg_lat:.2f}ms average, {max_lat:.2f}ms peak")

# Optimization recommendations
print("\n\n" + "=" * 80)
print("OPTIMIZATION RECOMMENDATIONS")
print("=" * 80)

optimization_recommendations = [
    {
        'Component': 'ML Inference',
        'Current P95 (ms)': avg_component_latency.loc['ML Inference', 'mean'],
        'Target P95 (ms)': 50,
        'Improvement %': 50,
        'Priority': 'CRITICAL',
        'Recommendations': [
            'Deploy models to GPU-accelerated inference endpoints (AWS Inferentia/SageMaker)',
            'Implement model quantization (FP16 or INT8) to reduce inference time by 40-60%',
            'Use TensorRT or ONNX Runtime for optimized execution',
            'Cache frequently accessed features in Redis to reduce data fetch overhead',
            'Consider model distillation for simpler, faster models with comparable accuracy'
        ]
    },
    {
        'Component': 'Recommendation System',
        'Current P95 (ms)': avg_component_latency.loc['Recommendation System', 'mean'],
        'Target P95 (ms)': 25,
        'Improvement %': 40,
        'Priority': 'HIGH',
        'Recommendations': [
            'Precompute alternative port rankings and store in OpenSearch for instant lookup',
            'Implement spatial indexing (H3 or S2 geometry) for faster geospatial queries',
            'Use materialized views in Presto for common recommendation patterns',
            'Cache top-N alternatives per port in Redis with 5-minute TTL',
            'Parallelize recommendation generation across multiple workers'
        ]
    },
    {
        'Component': 'Signal Fusion',
        'Current P95 (ms)': avg_component_latency.loc['Signal Fusion', 'mean'],
        'Target P95 (ms)': 20,
        'Improvement %': 35,
        'Priority': 'MEDIUM',
        'Recommendations': [
            'Move fusion logic to compiled language (Rust or C++) for 2-3x speedup',
            'Vectorize signal fusion operations using NumPy/SIMD instructions',
            'Implement streaming fusion to start processing before all signals arrive',
            'Use lookup tables for common fusion weight calculations',
            'Profile and optimize specific fusion algorithms per scenario'
        ]
    },
    {
        'Component': 'Data Ingestion',
        'Current P95 (ms)': avg_component_latency.loc['Data Ingestion', 'mean'],
        'Target P95 (ms)': 15,
        'Improvement %': 30,
        'Priority': 'MEDIUM',
        'Recommendations': [
            'Use connection pooling to reduce OpenSearch/Cassandra connection overhead',
            'Implement query result caching with 30-second TTL for frequently accessed vessels',
            'Use batch queries when fetching data for multiple vessels simultaneously',
            'Deploy read replicas closer to decision engine compute for lower network latency',
            'Optimize OpenSearch index mappings and shard allocation'
        ]
    },
    {
        'Component': 'Decision Engine',
        'Current P95 (ms)': avg_component_latency.loc['Decision Engine', 'mean'],
        'Target P95 (ms)': 10,
        'Improvement %': 25,
        'Priority': 'LOW',
        'Recommendations': [
            'Precompile decision trees and rule logic to reduce evaluation overhead',
            'Use lookup tables for threshold comparisons instead of conditional logic',
            'Implement early exit patterns to skip unnecessary evaluations',
            'Profile decision logic to identify slow code paths',
            'Consider moving to compiled language for hot path (Rust/Go)'
        ]
    },
    {
        'Component': 'Response Handler',
        'Current P95 (ms)': avg_component_latency.loc['Response Handler', 'mean'],
        'Target P95 (ms)': 5,
        'Improvement %': 20,
        'Priority': 'LOW',
        'Recommendations': [
            'Use faster JSON serialization library (orjson or ujson)',
            'Implement response templates to reduce serialization complexity',
            'Stream large responses instead of building full payload in memory',
            'Use Protocol Buffers for binary serialization if applicable',
            'Pre-serialize static response components'
        ]
    }
]

recommendations_df = pd.DataFrame([{
    'Component': r['Component'],
    'Current P95 (ms)': round(r['Current P95 (ms)'], 2),
    'Target P95 (ms)': r['Target P95 (ms)'],
    'Improvement %': r['Improvement %'],
    'Priority': r['Priority']
} for r in optimization_recommendations])

print("\n")
print(recommendations_df.to_string(index=False))

print("\n\n📋 Detailed Optimization Actions:\n")
for rec in optimization_recommendations:
    print(f"\n{'='*80}")
    print(f"{rec['Component']} ({rec['Priority']} Priority)")
    print(f"{'='*80}")
    print(f"Current P95: {rec['Current P95 (ms)']:.2f}ms → Target: {rec['Target P95 (ms)']}ms ({rec['Improvement %']}% improvement)")
    print("\nRecommendations:")
    for i, action in enumerate(rec['Recommendations'], 1):
        print(f"   {i}. {action}")

# Overall optimization impact
print("\n\n" + "=" * 80)
print("PROJECTED IMPACT OF OPTIMIZATIONS")
print("=" * 80)

current_total_p95 = sum(avg_component_latency['mean'])
optimized_total_p95 = sum([r['Target P95 (ms)'] for r in optimization_recommendations])
improvement_pct = ((current_total_p95 - optimized_total_p95) / current_total_p95) * 100

impact_summary = pd.DataFrame([
    {
        'Metric': 'Current Total P95 Latency',
        'Value': f"{current_total_p95:.2f}ms"
    },
    {
        'Metric': 'Optimized Total P95 Latency',
        'Value': f"{optimized_total_p95:.2f}ms"
    },
    {
        'Metric': 'Total Improvement',
        'Value': f"{improvement_pct:.1f}%"
    },
    {
        'Metric': 'Critical Priority Items',
        'Value': '1'
    },
    {
        'Metric': 'High Priority Items',
        'Value': '1'
    },
    {
        'Metric': 'Medium Priority Items',
        'Value': '2'
    }
])

print(impact_summary.to_string(index=False))

print("\n✓ Bottleneck analysis completed")
print("✓ Optimization recommendations generated with priorities")
print("✓ Projected improvements calculated")
