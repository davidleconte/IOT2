import os
from pathlib import Path

# Create docs/diagrams/ directory structure
diagrams_dir = Path("docs/diagrams")
diagrams_dir.mkdir(parents=True, exist_ok=True)

# Comprehensive list of diagrams to generate
diagram_registry = {
    "openshift_multi_az_topology": "OpenShift Multi-AZ Deployment Topology",
    "multi_tenant_isolation": "Multi-Tenant Isolation Architecture",
    "end_to_end_dataflow": "End-to-End Data Flow with DLQ/Retry/Quarantine",
    "ml_lifecycle": "ML Lifecycle Pipeline (Features→Training→Registry→Inference)",
    "ha_configuration": "High Availability Configuration for All Components",
    "capacity_planning": "Capacity Planning for 4M msg/s Sustained Throughput"
}

print(f"✅ Created directory: {diagrams_dir}")
print(f"📊 Generating {len(diagram_registry)} comprehensive architecture diagrams\n")

for diagram_id, title in diagram_registry.items():
    print(f"  • {title}")

diagram_list = diagram_registry
