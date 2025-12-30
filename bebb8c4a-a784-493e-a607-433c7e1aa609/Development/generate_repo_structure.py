import os
import json

# Define the complete navtor-fleet-guardian enterprise repository structure
repo_structure = {
    "terraform/": {
        "README.md": "# Terraform Infrastructure\n\nProvides IaC for multi-tenant AWS infrastructure with isolated VPCs, EKS clusters, and security controls.",
        "modules/": {
            "vpc/": {"main.tf": "", "variables.tf": "", "outputs.tf": ""},
            "eks/": {"main.tf": "", "variables.tf": "", "outputs.tf": ""},
            "rds/": {"main.tf": "", "variables.tf": "", "outputs.tf": ""},
            "msk/": {"main.tf": "", "variables.tf": "", "outputs.tf": ""},
            "opensearch/": {"main.tf": "", "variables.tf": "", "outputs.tf": ""},
            "s3/": {"main.tf": "", "variables.tf": "", "outputs.tf": ""},
        },
        "environments/": {
            "dev/": {"main.tf": "", "terraform.tfvars": ""},
            "staging/": {"main.tf": "", "terraform.tfvars": ""},
            "prod/": {"main.tf": "", "terraform.tfvars": ""},
        }
    },
    "helm/": {
        "README.md": "# Helm Charts\n\nKubernetes deployment charts with multi-tenant isolation, resource quotas, and network policies.",
        "navtor-fleet-guardian/": {
            "Chart.yaml": "",
            "values.yaml": "",
            "templates/": {
                "deployment.yaml": "",
                "service.yaml": "",
                "ingress.yaml": "",
                "configmap.yaml": "",
                "secret.yaml": "",
                "networkpolicy.yaml": "",
                "resourcequota.yaml": "",
            }
        }
    },
    "services/": {
        "README.md": "# Microservices\n\nDomain-driven microservices for fleet management, vessel tracking, analytics, and tenant isolation.",
        "fleet-management/": {"src/": {}, "Dockerfile": "", "requirements.txt": ""},
        "vessel-tracking/": {"src/": {}, "Dockerfile": "", "requirements.txt": ""},
        "analytics-service/": {"src/": {}, "Dockerfile": "", "requirements.txt": ""},
        "tenant-service/": {"src/": {}, "Dockerfile": "", "requirements.txt": ""},
        "auth-service/": {"src/": {}, "Dockerfile": "", "requirements.txt": ""},
    },
    "lakehouse/": {
        "README.md": "# Data Lakehouse Architecture\n\nDelta Lake tables with tenant partitioning, Iceberg catalog, and time-series optimization for maritime data.",
        "schemas/": {
            "vessel_telemetry.sql": "",
            "voyage_data.sql": "",
            "maintenance_logs.sql": "",
            "weather_data.sql": "",
        },
        "pipelines/": {
            "ingestion/": {"streaming_ingestion.py": "", "batch_ingestion.py": ""},
            "transformation/": {"data_quality.py": "", "enrichment.py": ""},
        }
    },
    "feast/": {
        "README.md": "# Feature Store (Feast)\n\nReal-time and batch features for ML models with tenant-aware feature serving and Redis online store.",
        "feature_repo/": {
            "feature_definitions.py": "",
            "data_sources.py": "",
            "feature_services.py": "",
        }
    },
    "ml/": {
        "README.md": "# ML Models\n\nVessel performance optimization, predictive maintenance, route optimization, and anomaly detection models.",
        "models/": {
            "vessel_performance/": {"train.py": "", "predict.py": "", "model.pkl": ""},
            "predictive_maintenance/": {"train.py": "", "predict.py": "", "model.pkl": ""},
            "route_optimization/": {"train.py": "", "predict.py": "", "model.pkl": ""},
            "anomaly_detection/": {"train.py": "", "predict.py": "", "model.pkl": ""},
        },
        "notebooks/": {}
    },
    "opensearch/": {
        "README.md": "# OpenSearch Configuration\n\nTenant-isolated indices, security policies, and anomaly detection dashboards for fleet monitoring.",
        "index_templates/": {
            "vessel_logs.json": "",
            "alerts.json": "",
            "metrics.json": "",
        },
        "dashboards/": {
            "fleet_overview.ndjson": "",
            "vessel_performance.ndjson": "",
            "anomaly_detection.ndjson": "",
        }
    },
    "ops/": {
        "pulsar/": {
            "README.md": "# Apache Pulsar Configuration\n\nAdvanced patterns: DLQ, retry policies, deduplication, key_shared ordering for tenant message isolation.",
            "tenants/": {"tenant_policies.yaml": ""},
            "namespaces/": {"namespace_config.yaml": ""},
            "topics/": {
                "vessel_telemetry_topic.yaml": "",
                "voyage_events_topic.yaml": "",
                "alerts_topic.yaml": "",
            },
            "functions/": {
                "data_enrichment.py": "",
                "anomaly_detector.py": "",
            }
        },
        "monitoring/": {
            "prometheus/": {"rules.yaml": ""},
            "grafana/": {"dashboards.json": ""},
        }
    },
    "docs/": {
        "README.md": "# Documentation\n\nComprehensive architecture documentation, multi-tenancy model, and deployment guides.",
    },
    ".github/": {
        "workflows/": {
            "ci.yaml": "",
            "cd.yaml": "",
        }
    },
    "scripts/": {
        "setup.sh": "",
        "deploy.sh": "",
    },
    "README.md": "# Navtor Fleet Guardian\n\nEnterprise multi-tenant maritime fleet management platform with real-time analytics and ML-powered insights.",
}

# Display structure summary
print("=" * 80)
print("NAVTOR FLEET GUARDIAN - ENTERPRISE REPOSITORY STRUCTURE")
print("=" * 80)
print("\n📁 Root Directory Structure:\n")

def count_structure(structure, depth=0):
    """Recursively count directories and files"""
    dirs = 0
    files = 0
    for key, value in structure.items():
        if isinstance(value, dict):
            dirs += 1
            sub_dirs, sub_files = count_structure(value, depth + 1)
            dirs += sub_dirs
            files += sub_files
        else:
            files += 1
    return dirs, files

total_dirs, total_files = count_structure(repo_structure)

for key in repo_structure.keys():
    if key.endswith('/'):
        print(f"  📂 {key}")
    else:
        print(f"  📄 {key}")

print(f"\n📊 Statistics:")
print(f"  • Total Directories: {total_dirs}")
print(f"  • Total Files: {total_files}")
print(f"  • Components: {len(repo_structure)}")

# Store structure for downstream blocks
repo_structure_data = repo_structure