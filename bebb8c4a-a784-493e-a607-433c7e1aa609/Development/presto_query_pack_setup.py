import os
import json

# Create directory structure for Presto C++ federated query pack
presto_queries_dir = 'ops/watsonx-data/presto-queries'
os.makedirs(f'{presto_queries_dir}/multi-source-joins', exist_ok=True)
os.makedirs(f'{presto_queries_dir}/json-extraction', exist_ok=True)
os.makedirs(f'{presto_queries_dir}/window-functions', exist_ok=True)
os.makedirs(f'{presto_queries_dir}/voyage-segmentation', exist_ok=True)
os.makedirs(f'{presto_queries_dir}/compliance-calculations', exist_ok=True)
os.makedirs(f'{presto_queries_dir}/performance-results', exist_ok=True)

# Query pack metadata
query_pack_metadata = {
    "name": "Navtor Maritime Presto C++ Federated Query Pack",
    "version": "1.0.0",
    "description": "Complex and highly complex federated queries demonstrating Presto C++ capabilities with watsonx.data",
    "target_engine": "Presto C++",
    "categories": [
        "multi-source-joins",
        "nested-json-extraction",
        "window-functions",
        "voyage-segmentation",
        "compliance-calculations"
    ],
    "data_sources": {
        "iceberg": "maritime_iceberg catalog with vessel telemetry, voyages, compliance data",
        "hcd": "Cassandra HCD via connector with operational features, maintenance data"
    },
    "tenant_aware": True,
    "resource_groups": ["tenant_alpha", "tenant_beta", "tenant_gamma"]
}

metadata_path = f'{presto_queries_dir}/query_pack_metadata.json'
with open(metadata_path, 'w') as f:
    json.dump(query_pack_metadata, f, indent=2)

print(f"Created query pack directory structure at: {presto_queries_dir}")
print(f"Query categories: {', '.join(query_pack_metadata['categories'])}")
print(f"Metadata saved to: {metadata_path}")
