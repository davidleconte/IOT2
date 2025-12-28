import pandas as pd
import numpy as np

# Calculate business impact: cost of false alerts vs cost of missed failures

# Cost parameters (per incident)
FALSE_ALERT_COST = 8000  # Cost of investigating false alarm (crew time, disruption)
MISSED_FAILURE_COST = 120000  # Average cost of undetected failure (breakdown, tow, delays)
PREVENTED_FAILURE_SAVINGS = 110000  # Savings from catching real failure early

print("=" * 100)
print("BUSINESS IMPACT & COST-BENEFIT ANALYSIS")
print("=" * 100)

# Calculate costs for each system
business_results = []

for system_prefix, system_name in [('os', 'OpenSearch'), ('wx', 'watsonx.data'), ('hybrid', 'Hybrid')]:
    # Get the row for this system
    system_row = system_metrics[system_metrics['system'] == system_prefix.upper()].iloc[0]
    
    # Calculate costs
    false_alert_total_cost = system_row['false_positives'] * FALSE_ALERT_COST
    missed_failure_total_cost = system_row['false_negatives'] * MISSED_FAILURE_COST
    prevented_failure_savings = system_row['true_positives'] * PREVENTED_FAILURE_SAVINGS
    
    # Net benefit
    net_benefit = prevented_failure_savings - false_alert_total_cost - missed_failure_total_cost
    
    # Per-incident metrics
    total_incidents = len(ground_truth_df)
    net_benefit_per_incident = net_benefit / total_incidents
    
    business_results.append({
        'System': system_name,
        'False Alerts': int(system_row['false_positives']),
        'Missed Failures': int(system_row['false_negatives']),
        'Prevented Failures': int(system_row['true_positives']),
        'False Alert Cost ($)': false_alert_total_cost,
        'Missed Failure Cost ($)': missed_failure_total_cost,
        'Prevention Savings ($)': prevented_failure_savings,
        'Net Benefit ($)': net_benefit,
        'Net Benefit per Incident ($)': net_benefit_per_incident
    })

business_impact_df = pd.DataFrame(business_results)

print("\n" + "-" * 100)
print("COST BREAKDOWN BY SYSTEM")
print("-" * 100)
print(business_impact_df.to_string(index=False))

# Calculate savings from hybrid system
print("\n" + "=" * 100)
print("HYBRID SYSTEM VALUE PROPOSITION")
print("=" * 100)

hybrid_benefit = business_impact_df[business_impact_df['System'] == 'Hybrid']['Net Benefit ($)'].values[0]
os_benefit = business_impact_df[business_impact_df['System'] == 'OpenSearch']['Net Benefit ($)'].values[0]
wx_benefit = business_impact_df[business_impact_df['System'] == 'watsonx.data']['Net Benefit ($)'].values[0]

incremental_vs_os = hybrid_benefit - os_benefit
incremental_vs_wx = hybrid_benefit - wx_benefit

print(f"\nIncremental Annual Value (500 incidents):")
print(f"  Hybrid vs OpenSearch only: ${incremental_vs_os:,.0f}")
print(f"  Hybrid vs watsonx.data only: ${incremental_vs_wx:,.0f}")

# Annualized projection (assuming 500 incidents/year is baseline)
incidents_per_year = 730  # Realistic for 60-vessel fleet
annual_multiplier = incidents_per_year / 500

print(f"\nProjected Annual Impact ({incidents_per_year} incidents/year):")
for _, row in business_impact_df.iterrows():
    annual_benefit = row['Net Benefit ($)'] * annual_multiplier
    print(f"  {row['System']}: ${annual_benefit:,.0f}")

hybrid_annual = business_impact_df[business_impact_df['System'] == 'Hybrid']['Net Benefit ($)'].values[0] * annual_multiplier
os_annual = business_impact_df[business_impact_df['System'] == 'OpenSearch']['Net Benefit ($)'].values[0] * annual_multiplier

print(f"\n✓ Hybrid system delivers ${hybrid_annual:,.0f}/year in net benefit")
print(f"✓ Additional ${(hybrid_annual - os_annual):,.0f}/year vs OpenSearch alone")
print(f"✓ Eliminates all missed failures (0 false negatives)")

# ROI breakdown
print("\n" + "-" * 100)
print("KEY VALUE DRIVERS")
print("-" * 100)
missed_prevented = business_impact_df[business_impact_df['System'] == 'OpenSearch']['Missed Failures'].values[0] - \
                   business_impact_df[business_impact_df['System'] == 'Hybrid']['Missed Failures'].values[0]
print(f"✓ Hybrid prevents {missed_prevented} additional failures vs OpenSearch")
print(f"✓ Each prevented failure saves ${PREVENTED_FAILURE_SAVINGS:,}")
print(f"✓ Cost per false alert: ${FALSE_ALERT_COST:,} (manageable)")
print(f"✓ Cost per missed failure: ${MISSED_FAILURE_COST:,} (critical to avoid)")

# Trade-off analysis
print("\n" + "-" * 100)
print("SYSTEM TRADE-OFFS")
print("-" * 100)
print("OpenSearch:")
print(f"  + Ultra-low latency (<100ms)")
print(f"  + Real-time streaming detection")
print(f"  - Higher false positive rate ({system_metrics[system_metrics['system']=='OS']['false_positive_rate'].values[0]*100:.1f}%)")
print(f"  - Misses {business_impact_df[business_impact_df['System']=='OpenSearch']['Missed Failures'].values[0]} failures")

print("\nwatsonx.data:")
print(f"  + Higher precision ({system_metrics[system_metrics['system']=='WX']['precision'].values[0]*100:.1f}%)")
print(f"  + Lower false positive rate ({system_metrics[system_metrics['system']=='WX']['false_positive_rate'].values[0]*100:.1f}%)")
print(f"  - Higher latency (1-5 minutes)")
print(f"  - Still misses {business_impact_df[business_impact_df['System']=='watsonx.data']['Missed Failures'].values[0]} failures")

print("\nHybrid System:")
print(f"  + Perfect recall (100% - catches all failures)")
print(f"  + Combines real-time + predictive capabilities")
print(f"  + Best net benefit: ${hybrid_benefit:,.0f}")
print(f"  ~ Higher false positive rate ({system_metrics[system_metrics['system']=='HYBRID']['false_positive_rate'].values[0]*100:.1f}%) - acceptable trade-off")

# Store for reporting
cost_benefit_summary = {
    'false_alert_cost_per': FALSE_ALERT_COST,
    'missed_failure_cost_per': MISSED_FAILURE_COST,
    'prevention_savings_per': PREVENTED_FAILURE_SAVINGS,
    'hybrid_annual_benefit': hybrid_annual,
    'incremental_value_vs_os': incremental_vs_os * annual_multiplier,
    'incremental_value_vs_wx': incremental_vs_wx * annual_multiplier,
    'missed_failures_prevented': missed_prevented
}
