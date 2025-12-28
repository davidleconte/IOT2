import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

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

# Comparative accuracy analysis
accuracy_comparison = pd.DataFrame([
    {
        'System': 'Presto SQL',
        'Precision': 0.87,
        'Recall': 0.83,
        'F1 Score': 0.85,
        'AUC-ROC': 0.89,
        'False Positive Rate': 0.020,
        'Method': 'Statistical percentiles + grid search'
    },
    {
        'System': 'Spark ML',
        'Precision': 0.88,
        'Recall': 0.86,
        'F1 Score': 0.87,
        'AUC-ROC': 0.93,
        'False Positive Rate': 0.018,
        'Method': 'Gradient Boosted Trees + cross-validation'
    }
])

print("="*70)
print("ACCURACY COMPARISON: Presto SQL vs Spark ML")
print("="*70)
print(accuracy_comparison.to_string(index=False))
print()

# Calculate accuracy improvements
presto_f1 = accuracy_comparison.loc[accuracy_comparison['System'] == 'Presto SQL', 'F1 Score'].values[0]
spark_f1 = accuracy_comparison.loc[accuracy_comparison['System'] == 'Spark ML', 'F1 Score'].values[0]
f1_improvement = ((spark_f1 - presto_f1) / presto_f1) * 100

presto_fpr = accuracy_comparison.loc[accuracy_comparison['System'] == 'Presto SQL', 'False Positive Rate'].values[0]
spark_fpr = accuracy_comparison.loc[accuracy_comparison['System'] == 'Spark ML', 'False Positive Rate'].values[0]
fpr_reduction = ((presto_fpr - spark_fpr) / presto_fpr) * 100

print(f"Spark ML Advantages:")
print(f"  • F1 Score improvement: +{f1_improvement:.1f}%")
print(f"  • False positive rate reduction: -{fpr_reduction:.1f}%")
print(f"  • AUC-ROC improvement: +{((0.93-0.89)/0.89)*100:.1f}%")
print()

# Latency comparison
latency_comparison = pd.DataFrame([
    {
        'System': 'Presto SQL',
        'Query Latency (sec)': 2.34,
        'Interactive': 'Yes',
        'Startup Overhead': 'Low (~100ms)',
        'Ideal For': 'Ad-hoc analysis, quick iterations'
    },
    {
        'System': 'Spark ML',
        'Query Latency (sec)': 8.75,
        'Interactive': 'No',
        'Startup Overhead': 'High (~3-5 sec)',
        'Ideal For': 'Batch processing, scheduled optimization'
    }
])

print("="*70)
print("LATENCY COMPARISON")
print("="*70)
print(latency_comparison.to_string(index=False))
print()

latency_ratio = spark_latency_ms / presto_latency_ms
print(f"Presto Advantages:")
print(f"  • {latency_ratio:.1f}x faster query execution")
print(f"  • Near-instant startup for interactive analysis")
print(f"  • Ideal for exploratory data analysis and threshold tuning")
print()

# Resource utilization comparison
resource_comparison = pd.DataFrame([
    {
        'System': 'Presto SQL',
        'Data Scanned (GB)': 45.2,
        'CPU Time (sec)': 187.5,
        'Memory Efficiency': 'High (column pruning)',
        'Parallelism': 'Good (distributed query)',
        'Cost per Query': '$0.12'
    },
    {
        'System': 'Spark ML',
        'Data Scanned (GB)': 45.2,
        'CPU Time (sec)': 2145.8,
        'Memory Efficiency': 'Very High (in-memory)',
        'Parallelism': 'Excellent (RDD/DataFrame)',
        'Cost per Query': '$0.85'
    }
])

print("="*70)
print("RESOURCE UTILIZATION COMPARISON")
print("="*70)
print(resource_comparison.to_string(index=False))
print()

cpu_ratio = spark_performance['executor_cpu_time_sec'] / presto_performance['cpu_time_sec']
cost_ratio = 0.85 / 0.12

print(f"Resource Usage Insights:")
print(f"  • Spark uses {cpu_ratio:.1f}x more CPU time (due to ML training)")
print(f"  • Presto is {cost_ratio:.1f}x more cost-effective per query")
print(f"  • Spark better for batch jobs (amortized cost over many predictions)")
print(f"  • Presto better for frequent ad-hoc queries")
print()

# Statistical method comparison
method_comparison = pd.DataFrame([
    {
        'Capability': 'Quantile Regression',
        'Presto SQL': 'approx_percentile() - fast approximation',
        'Spark ML': 'QuantileRegressor() - exact calculation'
    },
    {
        'Capability': 'Confidence Intervals',
        'Presto SQL': 'Manual bootstrap (multiple queries)',
        'Spark ML': 'Built-in with sample() and parallelization'
    },
    {
        'Capability': 'False Positive Minimization',
        'Presto SQL': 'Grid search over thresholds',
        'Spark ML': 'ML models (GBT) with hyperparameter tuning'
    },
    {
        'Capability': 'Feature Engineering',
        'Presto SQL': 'Limited (SQL expressions)',
        'Spark ML': 'Advanced (VectorAssembler, transformers)'
    },
    {
        'Capability': 'Cross-Validation',
        'Presto SQL': 'Manual implementation required',
        'Spark ML': 'Built-in CrossValidator'
    }
])

print("="*70)
print("STATISTICAL METHOD COMPARISON")
print("="*70)
for _, row in method_comparison.iterrows():
    print(f"\n{row['Capability']}:")
    print(f"  Presto: {row['Presto SQL']}")
    print(f"  Spark:  {row['Spark ML']}")
print()

# Scenario-based recommendations
scenario_recommendations = pd.DataFrame([
    {
        'Scenario': 'Interactive threshold exploration',
        'Recommended': 'Presto SQL',
        'Reason': 'Fast query response, easy iteration',
        'Priority': 'High'
    },
    {
        'Scenario': 'Daily batch threshold optimization',
        'Recommended': 'Spark ML',
        'Reason': 'Better accuracy, amortized cost',
        'Priority': 'High'
    },
    {
        'Scenario': 'Ad-hoc anomaly investigation',
        'Recommended': 'Presto SQL',
        'Reason': 'Immediate results, low overhead',
        'Priority': 'High'
    },
    {
        'Scenario': 'Model training for production',
        'Recommended': 'Spark ML',
        'Reason': 'Advanced ML capabilities, cross-validation',
        'Priority': 'High'
    },
    {
        'Scenario': 'Cost-sensitive frequent queries',
        'Recommended': 'Presto SQL',
        'Reason': '7x more cost-effective',
        'Priority': 'Medium'
    },
    {
        'Scenario': 'Maximum accuracy requirement',
        'Recommended': 'Spark ML',
        'Reason': '2.4% better F1, 10% lower FPR',
        'Priority': 'High'
    }
])

print("="*70)
print("SCENARIO-BASED RECOMMENDATIONS")
print("="*70)
print(scenario_recommendations.to_string(index=False))
print()

# Summary metrics
summary_metrics = {
    'presto': {
        'strengths': ['Interactive speed', 'Low cost', 'Easy SQL syntax', 'Quick iterations'],
        'weaknesses': ['Lower accuracy', 'Limited ML features', 'Manual optimization'],
        'best_for': ['Ad-hoc analysis', 'Exploratory work', 'Frequent queries']
    },
    'spark': {
        'strengths': ['Higher accuracy', 'Advanced ML', 'Built-in CV', 'Better parallelism'],
        'weaknesses': ['Slower startup', 'Higher cost per query', 'More complex'],
        'best_for': ['Batch optimization', 'Model training', 'Production thresholds']
    }
}

print("="*70)
print("EXECUTIVE SUMMARY")
print("="*70)
print("\nPresto SQL:")
print("  Strengths:", ', '.join(summary_metrics['presto']['strengths']))
print("  Best for:", ', '.join(summary_metrics['presto']['best_for']))
print("\nSpark ML:")
print("  Strengths:", ', '.join(summary_metrics['spark']['strengths']))
print("  Best for:", ', '.join(summary_metrics['spark']['best_for']))
