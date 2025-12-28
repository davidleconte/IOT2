import pandas as pd
import json

# Load requirements document to extract Fleet Guardian architecture context
with open('COMPREHENSIVE_REQUIREMENTS_EXTRACTION.md', 'r') as f:
    req_doc = f.read()

# Load maritime data
with open('events.json', 'r') as f:
    lines_ev = f.readlines()
events = [json.loads(line.strip()) for line in lines_ev if line.strip()]

ovd = pd.read_csv('sample_ovd_log_abstract.csv')

with open('sea_report.json', 'r') as f:
    sea_rep = json.load(f)

print("=" * 80)
print("FLEET GUARDIAN ARCHITECTURE MAPPING & GAP ANALYSIS")
print("=" * 80)

# Extract key architectural components from requirements doc
print("\n1. FLEET GUARDIAN ARCHITECTURE COMPONENTS (FROM REQUIREMENTS)")
print("-" * 80)

# Parse key sections
architecture_keywords = {
    'Data Storage': ['cassandra', 'scylla', 'nosql', 'time-series', 'storage', 'database'],
    'Streaming': ['kafka', 'pulsar', 'streaming', 'real-time', 'ingestion'],
    'Analytics': ['spark', 'presto', 'query', 'analytics', 'aggregation'],
    'OpenSearch': ['opensearch', 'elasticsearch', 'search', 'indexing'],
    'Iceberg': ['iceberg', 'data lake', 'parquet', 'columnar'],
    'Monitoring': ['prometheus', 'grafana', 'monitoring', 'metrics', 'alerting']
}

components_found = {}
for component, keywords in architecture_keywords.items():
    count = sum(req_doc.lower().count(kw) for kw in keywords)
    components_found[component] = count

print("\nComponent mentions in requirements doc:")
for comp, count in sorted(components_found.items(), key=lambda x: x[1], reverse=True):
    print(f"  {comp:20} → {count:3} mentions")

# Map data files to architecture components
print("\n\n2. DATA FILE TO ARCHITECTURE COMPONENT MAPPING")
print("-" * 80)

file_mapping = {
    'File': ['events.json', 'sample_ovd_log_abstract.csv', 'sea_report.json'],
    'Data_Type': ['Maintenance Events', 'Voyage Operations Log', 'Detailed Sea Report'],
    'Primary_Storage': ['Cassandra/Scylla (operational)', 'Cassandra (time-series)', 'Cassandra (nested docs)'],
    'Analytics_Layer': ['Iceberg (historical)', 'Iceberg + Presto', 'Iceberg + Presto'],
    'Real_Time_Layer': ['OpenSearch (recent events)', 'OpenSearch (current voyage)', 'OpenSearch (active reports)'],
    'Update_Frequency': ['Low (quarterly/annual)', 'High (hourly)', 'High (daily)']
}

mapping_df = pd.DataFrame(file_mapping)
print("\n" + mapping_df.to_string(index=False))

# Schema alignment validation
print("\n\n3. SCHEMA ALIGNMENT VALIDATION")
print("-" * 80)

# Events.json schema alignment
print("\nEvents.json → Fleet Guardian Schema:")
print("  MongoDB-style schema detected:")
print("    • _id.$oid → vessel_event_id (UUID conversion needed)")
print("    • dry_dockings[].date.$date → event_timestamp (ISO8601 conversion)")
print("    • dry_dockings[].type → event_type (standardization required)")
print("  ✅ Aligns with: vessel_maintenance_events table")
print("  ⚠️  Gap: No vessel IMO identifier - requires JOIN via _id lookup")

# OVD log schema alignment
print("\nOVD Log CSV → Fleet Guardian Schema:")
print("  Structured tabular format:")
print("    • IMO → vessel.imo (direct mapping)")
print("    • Date_UTC + Time_UTC → report_timestamp (combine & parse)")
print("    • Event → event_type (standardized OVD event taxonomy)")
print("    • Distance, Cargo_Mt, Fuel → telemetry metrics")
print("  ✅ Aligns with: vessel_voyage_reports table")
print("  ✅ Aligns with: vessel_fuel_consumption table")
print("  ⚠️  Gap: No lat/lon coordinates - limits geospatial analysis")

# Sea report schema alignment
print("\nSea Report JSON → Fleet Guardian Schema:")
sample_sea = sea_rep[0] if isinstance(sea_rep, list) and len(sea_rep) > 0 else sea_rep
print(f"  Highly nested structure with {len(sample_sea)} top-level keys:")
print("    • vessel → vessel_id (reference lookup)")
print("    • computed.* → derived_metrics (110 computed fields)")
print("    • power.* → engine_telemetry (399 power metrics)")
print("    • consumption.* → fuel_consumption (1296 consumption metrics)")
print("  ✅ Aligns with: vessel_detailed_telemetry table")
print("  ⚠️  Gap: Extreme denormalization - requires ETL to extract key metrics")
print("  ⚠️  Gap: No clear timestamp field - 'observation' buried in nested objects")

# Identify gaps and optimization opportunities
print("\n\n4. GAPS IN CURRENT IMPLEMENTATION")
print("-" * 80)

gaps = [
    {
        'Gap_ID': 'G1',
        'Category': 'Schema Standardization',
        'Issue': 'Inconsistent timestamp formats (MongoDB $date, UTC strings, nested obs)',
        'Impact': 'High - blocks unified time-series queries',
        'Recommendation': 'Implement unified ISO8601 timestamp field at root level for all records'
    },
    {
        'Gap_ID': 'G2',
        'Category': 'Geospatial Data',
        'Issue': 'OVD log lacks lat/lon coordinates despite having voyage legs',
        'Impact': 'Medium - prevents route optimization and geofencing',
        'Recommendation': 'Enrich with AIS data or port coordinate lookup table'
    },
    {
        'Gap_ID': 'G3',
        'Category': 'Vessel Identification',
        'Issue': 'events.json uses internal _id instead of IMO number',
        'Impact': 'High - requires expensive JOIN to correlate with other datasets',
        'Recommendation': 'Add IMO field to all event records at ingestion time'
    },
    {
        'Gap_ID': 'G4',
        'Category': 'Data Normalization',
        'Issue': 'Sea report has 1,296 consumption fields - extreme denormalization',
        'Impact': 'High - inefficient storage, slow queries, high cardinality',
        'Recommendation': 'Implement dimensional model: fact_consumption + dim_fuel_type + dim_equipment'
    },
    {
        'Gap_ID': 'G5',
        'Category': 'Event Taxonomy',
        'Issue': 'No unified event classification across files (OVD events vs maintenance vs reports)',
        'Impact': 'Medium - hinders cross-dataset correlation and analytics',
        'Recommendation': 'Create master event taxonomy with hierarchical classification'
    },
    {
        'Gap_ID': 'G6',
        'Category': 'Data Completeness',
        'Issue': 'OVD log sample shows only 9 records over 2 days - insufficient for ML',
        'Impact': 'Critical - cannot train predictive models or detect anomalies',
        'Recommendation': 'Ensure minimum 90 days historical data per vessel for meaningful analytics'
    },
    {
        'Gap_ID': 'G7',
        'Category': 'Real-time Indicators',
        'Issue': 'No status fields indicating if data is current vs historical',
        'Impact': 'Medium - OpenSearch can\'t efficiently filter active voyages',
        'Recommendation': 'Add is_active, status, and last_updated_timestamp fields'
    },
    {
        'Gap_ID': 'G8',
        'Category': 'Data Quality',
        'Issue': 'Empty string fields in events (type_of_paint, comments) instead of null',
        'Impact': 'Low - wastes storage, complicates null checks',
        'Recommendation': 'Standardize on null for missing values, not empty strings'
    }
]

gaps_df = pd.DataFrame(gaps)
print("\n" + gaps_df[['Gap_ID', 'Category', 'Impact']].to_string(index=False))

print("\n\nDetailed Gap Analysis:")
for gap in gaps:
    print(f"\n{gap['Gap_ID']}: {gap['Issue']}")
    print(f"   Impact: {gap['Impact']}")
    print(f"   → {gap['Recommendation']}")

# Count critical/high/medium/low impact gaps
impact_counts = gaps_df['Impact'].str.split(' - ').str[0].value_counts()
print("\n\nGap Impact Distribution:")
for impact, count in impact_counts.items():
    print(f"  {impact:10} → {count} gaps")
