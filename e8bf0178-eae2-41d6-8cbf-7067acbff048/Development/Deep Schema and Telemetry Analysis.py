import json
import pandas as pd
import numpy as np

# Re-load data for deep analysis
with open('events.json', 'r') as f:
    lines = f.readlines()
events_list = []
for line in lines:
    line = line.strip()
    if line:
        try:
            events_list.append(json.loads(line))
        except:
            pass

ovd_df = pd.read_csv('sample_ovd_log_abstract.csv')

with open('sea_report.json', 'r') as f:
    sea_reports = json.load(f)

print("=" * 80)
print("DEEP SCHEMA & TELEMETRY ANALYSIS")
print("=" * 80)

# EVENTS.JSON DETAILED SCHEMA EXTRACTION
print("\n1. EVENTS.JSON SCHEMA MAPPING")
print("-" * 80)

for idx, event in enumerate(events_list):
    print(f"\nEvent {idx + 1} Schema:")
    
    def analyze_structure(obj, prefix="", max_depth=3, current_depth=0):
        if current_depth > max_depth:
            return
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                value_type = type(value).__name__
                if isinstance(value, list) and len(value) > 0:
                    print(f"{prefix}{key}: list[{len(value)}] of {type(value[0]).__name__}")
                    if isinstance(value[0], dict) and current_depth < max_depth:
                        analyze_structure(value[0], prefix + "  " + key + "[0].", max_depth, current_depth + 1)
                elif isinstance(value, dict):
                    print(f"{prefix}{key}: dict with {len(value)} keys")
                    if current_depth < max_depth:
                        analyze_structure(value, prefix + "  " + key + ".", max_depth, current_depth + 1)
                else:
                    value_str = str(value)[:50] if value is not None else "null"
                    print(f"{prefix}{key}: {value_type} = {value_str}")
        elif isinstance(obj, list) and len(obj) > 0:
            print(f"{prefix}[list with {len(obj)} items]")
            analyze_structure(obj[0], prefix + "[0].", max_depth, current_depth + 1)
    
    analyze_structure(event, "  ", max_depth=2)

# SEA_REPORT.JSON DETAILED ANALYSIS
print("\n\n2. SEA_REPORT.JSON COMPREHENSIVE SCHEMA")
print("-" * 80)

if isinstance(sea_reports, list) and len(sea_reports) > 0:
    print(f"Total reports: {len(sea_reports)}")
    sample_report = sea_reports[0]
    
    print("\nTop-level structure:")
    for key, value in sample_report.items():
        value_type = type(value).__name__
        if isinstance(value, list):
            print(f"  {key}: list[{len(value)}]")
            if len(value) > 0:
                print(f"    └─ Item type: {type(value[0]).__name__}")
                if isinstance(value[0], dict):
                    print(f"       Keys: {list(value[0].keys())[:8]}")
        elif isinstance(value, dict):
            print(f"  {key}: dict with {len(value)} keys")
            print(f"    └─ Keys: {list(value.keys())[:8]}")
        else:
            value_str = str(value)[:60] if value is not None else "null"
            print(f"  {key}: {value_type} = {value_str}")

# OVD LOG TELEMETRY PATTERNS
print("\n\n3. OVD LOG TELEMETRY PATTERNS & OPERATIONAL EVENTS")
print("-" * 80)

print(f"\nDataset: {ovd_df.shape[0]} records from IMO {ovd_df['IMO'].iloc[0]}")
print(f"Voyage: {ovd_df['Voyage_From'].iloc[0]} → {ovd_df['Voyage_To'].iloc[0]}")
print(f"Date range: {ovd_df['Date_UTC'].iloc[0]} to {ovd_df['Date_UTC'].iloc[-1]}")

print("\nEvent types in log:")
event_counts = ovd_df['Event'].value_counts()
for event_type, count in event_counts.items():
    print(f"  • {event_type}: {count} occurrences")

print("\nTelemetry schema from OVD log:")
telemetry_fields = {
    'Temporal': ['Date_UTC', 'Time_UTC', 'Time_Since_Previous_Report'],
    'Positional': ['Voyage_From', 'Voyage_To', 'Distance'],
    'Cargo': ['Cargo_Mt'],
    'Fuel_Consumption': ['ME_Consumption_HFO', 'AE_Consumption_MGO'],
    'Fuel_Inventory': ['HFO_ROB', 'MGO_ROB'],
    'Event_Classification': ['Event']
}

for category, fields in telemetry_fields.items():
    print(f"\n  {category}:")
    for field in fields:
        if field in ovd_df.columns:
            dtype = ovd_df[field].dtype
            sample_val = ovd_df[field].iloc[0]
            print(f"    - {field}: {dtype} (e.g., {sample_val})")

# Calculate telemetry metrics
print("\n\n4. OPERATIONAL METRICS FROM TELEMETRY")
print("-" * 80)

total_distance = ovd_df['Distance'].sum()
total_hfo = ovd_df['ME_Consumption_HFO'].sum()
total_mgo = ovd_df['AE_Consumption_MGO'].sum()
avg_speed = total_distance / ovd_df['Time_Since_Previous_Report'].sum() if ovd_df['Time_Since_Previous_Report'].sum() > 0 else 0

print(f"Total distance: {total_distance} nm")
print(f"Total HFO consumption: {total_hfo:.1f} MT")
print(f"Total MGO consumption: {total_mgo:.1f} MT")
print(f"Average speed: {avg_speed:.1f} knots")
print(f"Fuel efficiency: {total_distance/total_hfo:.2f} nm/MT HFO" if total_hfo > 0 else "N/A")

# Event timing analysis
print("\nEvent timing patterns:")
ovd_df['Event_Hour'] = pd.to_datetime(ovd_df['Time_UTC'], format='%H:%M', errors='coerce').dt.hour
for idx, row in ovd_df.iterrows():
    print(f"  {row['Event']:12} at {row['Time_UTC']} - Distance since prev: {row['Distance']} nm")

telemetry_summary = pd.DataFrame({
    'Metric': ['Total Distance (nm)', 'Total HFO (MT)', 'Total MGO (MT)', 
               'Avg Speed (knots)', 'Fuel Efficiency (nm/MT)'],
    'Value': [total_distance, total_hfo, total_mgo, avg_speed, 
              total_distance/total_hfo if total_hfo > 0 else 0]
})

print("\n\nTelemetry Summary:")
print(telemetry_summary.to_string(index=False))
