import json
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# Load all maritime data files
with open('COMPREHENSIVE_REQUIREMENTS_EXTRACTION.md', 'r') as f:
    requirements_doc = f.read()

# Load events.json with error handling
try:
    with open('events.json', 'r') as f:
        events_raw = f.read()
        # Try to parse as JSON
        events_data = json.loads(events_raw)
except json.JSONDecodeError as e:
    print(f"JSON decode error: {e}")
    # If it's multi-line JSON, try loading line by line
    with open('events.json', 'r') as f:
        lines = f.readlines()
    events_data = []
    for line in lines:
        line = line.strip()
        if line:
            try:
                events_data.append(json.loads(line))
            except:
                pass

ovd_log_df = pd.read_csv('sample_ovd_log_abstract.csv')

with open('sea_report.json', 'r') as f:
    sea_report_data = json.load(f)

# Analyze file structures and contents
print("=" * 80)
print("MARITIME DATA FILES COMPREHENSIVE ANALYSIS")
print("=" * 80)

# 1. COMPREHENSIVE_REQUIREMENTS_EXTRACTION.md Analysis
print("\n1. REQUIREMENTS DOCUMENT ANALYSIS")
print("-" * 80)
req_lines = requirements_doc.split('\n')
print(f"Total lines: {len(req_lines)}")
print(f"Total characters: {len(requirements_doc)}")

# Extract key sections
sections = [line for line in req_lines if line.startswith('#')]
print(f"\nDocument structure - {len(sections)} sections found:")
for section in sections[:15]:
    print(f"  • {section}")

# 2. events.json Analysis
print("\n\n2. EVENTS.JSON ANALYSIS")
print("-" * 80)
print(f"Type: {type(events_data)}")
if isinstance(events_data, list):
    print(f"List with {len(events_data)} items")
    if len(events_data) > 0:
        print(f"\nFirst event structure:")
        for key, value in events_data[0].items():
            print(f"  {key}: {type(value).__name__} = {str(value)[:100]}")
elif isinstance(events_data, dict):
    print(f"Top-level keys: {list(events_data.keys())}")
    for key, value in events_data.items():
        print(f"\n  {key}:")
        print(f"    - Type: {type(value).__name__}")
        if isinstance(value, (list, dict)):
            print(f"    - Length: {len(value)}")

# 3. sample_ovd_log_abstract.csv Analysis
print("\n\n3. OVD LOG CSV ANALYSIS")
print("-" * 80)
print(f"Shape: {ovd_log_df.shape}")
print(f"Columns: {list(ovd_log_df.columns)}")
print(f"\nData types:")
print(ovd_log_df.dtypes)
print(f"\nFirst 3 rows:")
print(ovd_log_df.head(3).to_string())

# 4. sea_report.json Analysis
print("\n\n4. SEA_REPORT.JSON ANALYSIS")
print("-" * 80)
print(f"Type: {type(sea_report_data)}")

if isinstance(sea_report_data, dict):
    print(f"Top-level keys: {list(sea_report_data.keys())}")
    
    for key, value in sea_report_data.items():
        print(f"\n  {key}:")
        if isinstance(value, list):
            print(f"    - Type: list with {len(value)} items")
            if len(value) > 0 and isinstance(value[0], dict):
                print(f"    - First item keys: {list(value[0].keys())}")
        elif isinstance(value, dict):
            print(f"    - Type: dict with {len(value)} keys")
            print(f"    - Keys: {list(value.keys())[:10]}")
        else:
            print(f"    - Type: {type(value).__name__}, Value: {str(value)[:100]}")

# Create summary of all files
files_summary_data = {
    'File': ['requirements_doc', 'events.json', 'ovd_log.csv', 'sea_report.json'],
    'Size_Bytes': [36822, 2291, 826, 121260],
    'Type': ['Markdown', 'JSON', 'CSV', 'JSON'],
    'Primary_Content': [
        'Architecture requirements & specs',
        'Event definitions/schemas',
        'Vessel operational data logs',
        'Maritime reporting data'
    ]
}

files_summary_df = pd.DataFrame(files_summary_data)

print("\n\n" + "=" * 80)
print("FILES SUMMARY")
print("=" * 80)
print(files_summary_df.to_string(index=False))
