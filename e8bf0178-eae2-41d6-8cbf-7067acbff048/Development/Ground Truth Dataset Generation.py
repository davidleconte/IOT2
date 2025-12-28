import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Ground Truth Dataset: Historical incidents with known outcomes
# This creates realistic training/evaluation data with labeled outcomes

np.random.seed(42)

# Generate 500 historical incidents over 1 year
n_incidents = 500
base_date = datetime.now() - timedelta(days=365)

ground_truth_data = []

for i in range(n_incidents):
    incident_time = base_date + timedelta(hours=np.random.randint(0, 365*24))
    vessel_id = f"VESSEL_{np.random.randint(1000, 9999)}"
    
    # Equipment failure scenarios
    equipment_temp = np.random.normal(75, 15)  # °C
    vibration = np.random.normal(5, 2)  # mm/s
    pressure = np.random.normal(100, 20)  # PSI
    
    # Determine actual outcome
    failure_score = (equipment_temp - 75)/15 + (vibration - 5)/2 + (pressure - 100)/20
    actual_failure = failure_score > 1.5
    
    # OpenSearch detected it (real-time RCF)
    os_detected = actual_failure and np.random.random() > 0.15  # 85% recall
    if not actual_failure:
        os_detected = np.random.random() < 0.12  # 12% false positive rate
    
    # watsonx.data detected it (historical ML model)
    wx_detected = actual_failure and np.random.random() > 0.10  # 90% recall
    if not actual_failure:
        wx_detected = np.random.random() < 0.08  # 8% false positive rate
    
    # Hybrid system (combines both)
    hybrid_detected = os_detected or wx_detected
    
    # Detection latency
    os_latency = np.random.uniform(50, 150) if os_detected else None  # ms
    wx_latency = np.random.uniform(60000, 300000) if wx_detected else None  # ms (1-5 min)
    hybrid_latency = min([l for l in [os_latency, wx_latency] if l is not None], default=None)
    
    # Business impact
    if actual_failure and not hybrid_detected:
        cost_impact = np.random.uniform(50000, 200000)  # Missed failure cost
        outcome_type = 'missed_detection'
    elif not actual_failure and hybrid_detected:
        cost_impact = np.random.uniform(5000, 15000)  # False alert cost
        outcome_type = 'false_positive'
    elif actual_failure and hybrid_detected:
        cost_impact = -np.random.uniform(40000, 180000)  # Prevented cost (savings)
        outcome_type = 'true_positive'
    else:
        cost_impact = 0
        outcome_type = 'true_negative'
    
    ground_truth_data.append({
        'incident_id': f"INC_{i:05d}",
        'timestamp': incident_time,
        'vessel_id': vessel_id,
        'equipment_temp': round(equipment_temp, 2),
        'vibration': round(vibration, 2),
        'pressure': round(pressure, 2),
        'failure_score': round(failure_score, 3),
        'actual_failure': actual_failure,
        'os_detected': os_detected,
        'wx_detected': wx_detected,
        'hybrid_detected': hybrid_detected,
        'os_latency_ms': os_latency,
        'wx_latency_ms': wx_latency,
        'hybrid_latency_ms': hybrid_latency,
        'cost_impact': round(cost_impact, 2),
        'outcome_type': outcome_type
    })

ground_truth_df = pd.DataFrame(ground_truth_data)

# Summary statistics
print("=" * 80)
print("GROUND TRUTH DATASET SUMMARY")
print("=" * 80)
print(f"\nTotal Incidents: {len(ground_truth_df)}")
print(f"Actual Failures: {ground_truth_df['actual_failure'].sum()} ({ground_truth_df['actual_failure'].mean()*100:.1f}%)")
print(f"Non-Failures: {(~ground_truth_df['actual_failure']).sum()} ({(~ground_truth_df['actual_failure']).mean()*100:.1f}%)")

print("\n" + "-" * 80)
print("OUTCOME DISTRIBUTION")
print("-" * 80)
print(ground_truth_df['outcome_type'].value_counts().to_string())

print("\n" + "-" * 80)
print("DETECTION RATES BY SYSTEM")
print("-" * 80)
actual_failures_df = ground_truth_df[ground_truth_df['actual_failure']]
print(f"OpenSearch Recall: {actual_failures_df['os_detected'].mean()*100:.1f}%")
print(f"watsonx.data Recall: {actual_failures_df['wx_detected'].mean()*100:.1f}%")
print(f"Hybrid System Recall: {actual_failures_df['hybrid_detected'].mean()*100:.1f}%")

non_failures_df = ground_truth_df[~ground_truth_df['actual_failure']]
print(f"\nOpenSearch False Positive Rate: {non_failures_df['os_detected'].mean()*100:.1f}%")
print(f"watsonx.data False Positive Rate: {non_failures_df['wx_detected'].mean()*100:.1f}%")
print(f"Hybrid System False Positive Rate: {non_failures_df['hybrid_detected'].mean()*100:.1f}%")

print("\n" + "-" * 80)
print("AVERAGE DETECTION LATENCY")
print("-" * 80)
print(f"OpenSearch: {ground_truth_df['os_latency_ms'].mean():.1f} ms")
print(f"watsonx.data: {ground_truth_df['wx_latency_ms'].mean()/1000:.1f} seconds")
print(f"Hybrid: {ground_truth_df['hybrid_latency_ms'].mean():.1f} ms")

print("\n" + "-" * 80)
print("SAMPLE RECORDS")
print("-" * 80)
print(ground_truth_df.head(10).to_string(index=False))
