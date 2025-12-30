import json
import os

# Create comprehensive deployment guide
deployment_guide = """# OpenSearch Dashboards Deployment Guide
## Maritime IoT Control Tower - Dashboard Configuration

### Overview
This deployment package contains comprehensive OpenSearch Dashboards for the Maritime IoT Control Tower platform, providing:
- Real-time fleet visibility with GeoPoint visualizations
- Anomaly detection timelines with detector annotations
- Vector similarity analysis ('vessels behaving like this')
- Compliance & regulatory KPI monitoring
- Tenant-scoped access controls

### Package Contents

#### 1. Dashboards (ops/opensearch/dashboards/)
15 dashboards total - 5 dashboard types × 3 tenants:
- **Fleet Map Dashboards**: Real-time vessel positioning with GeoPoint visualization
- **Anomaly Timeline Dashboards**: Time-series anomaly detection with detector annotations
- **Vector Similarity Dashboards**: Similar vessel behavior analysis using vector search
- **Compliance KPI Dashboards**: Emissions, safety scores, regulatory alerts
- **Master Control Tower Dashboards**: Comprehensive operational overview

#### 2. Visualizations (ops/opensearch/visualizations/)
18 visualization artifacts including:
- **Index Patterns**: 6 patterns (2 per tenant: telemetry + anomalies)
- **GeoPoint Maps**: Fleet positioning visualizations
- **Timeline Charts**: Anomaly detection over time with threshold lines
- **Metric Visualizations**: KPI metrics (CO2, NOx, safety scores)
- **Data Tables**: Vector similarity search results

### Tenant Configuration
Three tenants configured with complete isolation:
- `shipping-co-alpha`
- `logistics-beta`
- `maritime-gamma`

Each tenant has dedicated:
- Index patterns for telemetry and anomaly data
- Dashboard compositions with tenant-scoped filters
- Visualizations referencing tenant-specific indices
- RBAC roles for access control (admin, analyst, writer)

### Deployment Instructions

#### Option 1: Manual Import via UI
1. Access OpenSearch Dashboards UI
2. Navigate to: Management → Stack Management → Saved Objects
3. Click "Import"
4. Upload NDJSON files from:
   - `ops/opensearch/visualizations/` (import these first for dependencies)
   - `ops/opensearch/dashboards/` (import dashboards second)
5. Select tenant space (if multi-tenancy enabled)
6. Resolve any ID conflicts (recommend "Check for existing objects")

#### Option 2: Automated Import via API
```bash
# Import index patterns and visualizations first
for tenant in shipping-co-alpha logistics-beta maritime-gamma; do
  curl -X POST "https://opensearch-host:5601/api/saved_objects/_import" \\
    -H "osd-xsrf: true" \\
    -H "securitytenant: ${tenant}" \\
    --form file=@"ops/opensearch/visualizations/${tenant}_vessel_telemetry_index_pattern.ndjson" \\
    --form file=@"ops/opensearch/visualizations/${tenant}_anomalies_index_pattern.ndjson"
done

# Import visualizations
for viz_file in ops/opensearch/visualizations/*_viz.ndjson; do
  curl -X POST "https://opensearch-host:5601/api/saved_objects/_import" \\
    -H "osd-xsrf: true" \\
    --form file=@"${viz_file}"
done

# Import dashboards
for dashboard_file in ops/opensearch/dashboards/*.ndjson; do
  curl -X POST "https://opensearch-host:5601/api/saved_objects/_import" \\
    -H "osd-xsrf: true" \\
    --form file=@"${dashboard_file}"
done
```

#### Option 3: Terraform/IaC Deployment
```hcl
resource "opensearch_dashboard_object" "fleet_dashboards" {
  for_each = fileset(path.module, "ops/opensearch/dashboards/*.ndjson")
  body     = file(each.value)
}
```

### Post-Deployment Verification

#### 1. Verify Index Patterns
```bash
curl -X GET "https://opensearch-host:5601/api/saved_objects/_find?type=index-pattern" \\
  -H "osd-xsrf: true"
```
Expected: 6 index patterns (2 per tenant)

#### 2. Verify Visualizations
Check that all visualizations are accessible and referencing correct index patterns.

#### 3. Verify Dashboards
Navigate to each dashboard and confirm:
- Panels load without errors
- Time range filters work correctly
- Tenant-scoped data is displayed
- GeoPoint maps render vessel positions
- Anomaly timelines show detector annotations

### Data Requirements

#### Required Indices
Dashboards expect the following index patterns to exist with data:

**Telemetry Indices**: `{tenant_id}_vessel_telemetry-*`
- Required fields: `@timestamp`, `vessel_id`, `location` (geo_point), `speed_knots`, `fuel_consumption_mt_per_day`, `engine_rpm`
- Optional fields: `imo_number`, `vessel_name`, `region`, `heading`

**Anomaly Indices**: `{tenant_id}_anomalies-*`
- Required fields: `detection_timestamp`, `anomaly_id`, `vessel_id`, `anomaly_type`, `severity`, `anomaly_grade`, `confidence_score`
- Optional fields: `detector_id`, `feature_data`, `similarity_score`, `pattern_type`

**Compliance Fields** (within telemetry or separate index):
- `engine_metrics.co2_emissions_mt`
- `engine_metrics.nox_emissions_kg`
- `compliance.safety_score`
- `compliance.regulatory_alerts`

### Access Control

Dashboards integrate with existing RBAC roles:
- **{tenant}_admin**: Full access to all dashboards and configurations
- **{tenant}_analyst**: Read access to all dashboards
- **{tenant}_writer**: Write access to indices, read access to dashboards

Ensure these roles are configured in OpenSearch Security plugin:
```bash
# Verify role mappings
curl -X GET "https://opensearch-host:9200/_plugins/_security/api/rolesmapping" \\
  -u admin:admin
```

### Customization Guide

#### Modifying Dashboards
1. Import dashboards to your OpenSearch instance
2. Edit via UI: Dashboard → Edit → Modify panels
3. Export modified dashboard: Management → Saved Objects → Export
4. Replace NDJSON file in repository

#### Adding New Tenants
1. Copy existing tenant dashboard files
2. Replace tenant ID throughout (e.g., `shipping-co-alpha` → `new-tenant-id`)
3. Update tenant name in dashboard titles
4. Import to OpenSearch

#### Customizing Visualizations
- **GeoPoint Maps**: Adjust map center, zoom level in visualization params
- **Anomaly Thresholds**: Modify `thresholdLine` value in timeline visualizations
- **KPI Goals**: Update metric color ranges in compliance visualizations
- **Time Ranges**: Adjust default time filters in dashboard `timeFrom`/`timeTo`

### Troubleshooting

#### Issue: "Index pattern not found"
**Solution**: Import index patterns first, then visualizations, then dashboards

#### Issue: "No data in dashboards"
**Solution**: 
1. Verify indices exist and contain data
2. Check time range filter (expand to larger window)
3. Confirm field mappings match expected types (especially `location` as geo_point)

#### Issue: "Visualization references not resolved"
**Solution**: Import visualizations before dashboards, as dashboards reference visualization IDs

#### Issue: "Tenant access denied"
**Solution**: Verify user has appropriate role mapping for tenant space

### Version Control & CI/CD

#### Git Workflow
```bash
# Track dashboard changes
git add ops/opensearch/dashboards/ ops/opensearch/visualizations/
git commit -m "feat: update fleet map dashboard with new KPIs"
```

#### Automated Deployment Pipeline
```yaml
# .github/workflows/deploy-dashboards.yml
name: Deploy OpenSearch Dashboards
on:
  push:
    paths:
      - 'ops/opensearch/dashboards/**'
      - 'ops/opensearch/visualizations/**'
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Import Dashboards
        run: |
          for file in ops/opensearch/dashboards/*.ndjson; do
            curl -X POST "$OPENSEARCH_DASHBOARDS_URL/api/saved_objects/_import" \\
              -H "osd-xsrf: true" \\
              -H "Authorization: Basic $OPENSEARCH_CREDENTIALS" \\
              --form file=@"$file"
          done
```

### Maintenance

#### Regular Updates
- **Weekly**: Review dashboard usage metrics
- **Monthly**: Update visualizations based on user feedback
- **Quarterly**: Audit tenant access controls

#### Backup Strategy
```bash
# Export all dashboards for backup
curl -X POST "https://opensearch-host:5601/api/saved_objects/_export" \\
  -H "osd-xsrf: true" \\
  -H "Content-Type: application/json" \\
  -d '{"type": ["dashboard", "visualization", "index-pattern"]}' \\
  > dashboards_backup_$(date +%Y%m%d).ndjson
```

### Support & Documentation
- OpenSearch Dashboards Documentation: https://opensearch.org/docs/latest/dashboards/
- Visualization Reference: https://opensearch.org/docs/latest/dashboards/visualize/viz-index/
- Security Plugin: https://opensearch.org/docs/latest/security/
"""

deployment_guide_path = "ops/opensearch/DASHBOARDS_DEPLOYMENT_GUIDE.md"
with open(deployment_guide_path, 'w') as f:
    f.write(deployment_guide)

# Create comprehensive summary
dashboard_summary = {
    "deployment_package": {
        "version": "1.0.0",
        "opensearch_version": "2.11.0",
        "created_date": "2025-12-30"
    },
    "dashboards": {
        "total": 15,
        "by_type": {
            "fleet_maps": 3,
            "anomaly_timelines": 3,
            "vector_similarity": 3,
            "compliance_kpi": 3,
            "master_control_tower": 3
        }
    },
    "visualizations": {
        "total": 18,
        "index_patterns": 6,
        "charts_and_metrics": 12
    },
    "tenants": [
        {
            "id": "shipping-co-alpha",
            "name": "Shipping Co Alpha",
            "dashboards": 5,
            "index_patterns": 2
        },
        {
            "id": "logistics-beta",
            "name": "Logistics Beta",
            "dashboards": 5,
            "index_patterns": 2
        },
        {
            "id": "maritime-gamma",
            "name": "Maritime Gamma",
            "dashboards": 5,
            "index_patterns": 2
        }
    ],
    "features": [
        "Real-time fleet positioning with GeoPoint visualization",
        "Anomaly detection timelines with detector annotations",
        "Vector similarity analysis for pattern matching",
        "Compliance KPI monitoring (CO2, NOx, safety scores)",
        "Tenant-scoped access controls and data isolation",
        "Time-range filtering and historical analysis",
        "Interactive maps with vessel tracking",
        "Regulatory alert monitoring and compliance status"
    ],
    "deployment": {
        "methods": ["Manual UI import", "Automated API import", "Terraform/IaC"],
        "import_order": ["Index patterns", "Visualizations", "Dashboards"],
        "required_indices": [
            "{tenant_id}_vessel_telemetry-*",
            "{tenant_id}_anomalies-*"
        ],
        "required_roles": [
            "{tenant}_admin",
            "{tenant}_analyst",
            "{tenant}_writer"
        ]
    },
    "files": {
        "dashboards": "ops/opensearch/dashboards/",
        "visualizations": "ops/opensearch/visualizations/",
        "deployment_guide": "ops/opensearch/DASHBOARDS_DEPLOYMENT_GUIDE.md",
        "summary": "ops/opensearch/dashboard_deployment_summary.json"
    }
}

summary_path = "ops/opensearch/dashboard_deployment_summary.json"
with open(summary_path, 'w') as f:
    json.dump(dashboard_summary, f, indent=2)

# List all generated files
all_dashboard_files = []
for root, dirs, files in os.walk("ops/opensearch"):
    for file in files:
        if file.endswith('.ndjson') and ('dashboards' in root or 'visualizations' in root):
            all_dashboard_files.append(os.path.join(root, file))

print("✅ OpenSearch Dashboards Deployment Package Complete!")
print(f"\n📦 Package Summary:")
print(f"  - Total Dashboards: 15")
print(f"  - Total Visualizations: 18 (including 6 index patterns)")
print(f"  - Tenants Configured: 3")
print(f"  - Total NDJSON Files: {len(all_dashboard_files)}")
print(f"\n📁 Key Files:")
print(f"  - Deployment Guide: {deployment_guide_path}")
print(f"  - Summary JSON: {summary_path}")
print(f"  - Dashboard Files: ops/opensearch/dashboards/ ({len([f for f in all_dashboard_files if 'dashboards' in f])} files)")
print(f"  - Visualization Files: ops/opensearch/visualizations/ ({len([f for f in all_dashboard_files if 'visualizations' in f])} files)")
print(f"\n🚀 Ready for Deployment:")
print(f"  1. Import index patterns from ops/opensearch/visualizations/")
print(f"  2. Import visualizations from ops/opensearch/visualizations/")
print(f"  3. Import dashboards from ops/opensearch/dashboards/")
print(f"  4. Follow deployment guide: {deployment_guide_path}")
print(f"\n✨ Dashboard Features:")
print(f"  ✓ Fleet maps with real-time GeoPoint vessel positions")
print(f"  ✓ Anomaly detection timelines with detector annotations")
print(f"  ✓ Vector similarity panels ('vessels behaving like this')")
print(f"  ✓ Compliance KPI dashboards (emissions, safety, alerts)")
print(f"  ✓ Tenant-scoped access controls")
print(f"  ✓ Version controlled NDJSON exports")
print(f"  ✓ Automated deployment ready")

final_summary = {
    "total_ndjson_files": len(all_dashboard_files),
    "dashboard_files": len([f for f in all_dashboard_files if 'dashboards' in f]),
    "visualization_files": len([f for f in all_dashboard_files if 'visualizations' in f]),
    "deployment_guide": deployment_guide_path,
    "summary_json": summary_path,
    "dashboard_summary": dashboard_summary
}
