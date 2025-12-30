import json
import yaml
from pathlib import Path

# Ensure directories exist from upstream block
opensearch_dirs = [
    'ops/opensearch/index-templates',
    'ops/opensearch/ilm-policies',
    'ops/opensearch/roles',
    'ops/opensearch/anomaly-detectors',
    'ops/opensearch/vector-indices'
]
for dir_path in opensearch_dirs:
    Path(dir_path).mkdir(parents=True, exist_ok=True)

# Generate tenant-prefixed index templates with GeoPoint mappings
index_templates = {}

for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    
    # Vessel telemetry index template with GeoPoint for vessel positions
    index_templates[f"{tenant_id}_vessel_telemetry"] = {
        "index_patterns": [f"{tenant_id}-vessel-telemetry-*"],
        "template": {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 2,
                "index.lifecycle.name": "telemetry_ilm",
                "index.lifecycle.rollover_alias": f"{tenant_id}-vessel-telemetry",
                "refresh_interval": "30s",
                "codec": "best_compression"
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "vessel_id": {"type": "keyword"},
                    "vessel_name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
                    "position": {"type": "geo_point"},
                    "speed": {"type": "float"},
                    "heading": {"type": "float"},
                    "draft": {"type": "float"},
                    "fuel_consumption": {"type": "float"},
                    "fuel_level": {"type": "float"},
                    "engine_rpm": {"type": "integer"},
                    "engine_temp": {"type": "float"},
                    "cargo_weight": {"type": "float"},
                    "weather_conditions": {"type": "text"},
                    "tenant_id": {"type": "keyword"}
                }
            }
        },
        "priority": 100,
        "version": 1,
        "_meta": {
            "description": f"Vessel telemetry data for tenant {tenant_id}"
        }
    }
    
    # Engine metrics index template for multi-metric anomaly detection
    index_templates[f"{tenant_id}_engine_metrics"] = {
        "index_patterns": [f"{tenant_id}-engine-metrics-*"],
        "template": {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2,
                "index.lifecycle.name": "telemetry_ilm",
                "index.lifecycle.rollover_alias": f"{tenant_id}-engine-metrics",
                "refresh_interval": "15s"
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "vessel_id": {"type": "keyword"},
                    "engine_id": {"type": "keyword"},
                    "rpm": {"type": "integer"},
                    "temperature": {"type": "float"},
                    "pressure": {"type": "float"},
                    "vibration": {"type": "float"},
                    "oil_pressure": {"type": "float"},
                    "coolant_temp": {"type": "float"},
                    "exhaust_temp": {"type": "float"},
                    "load_percentage": {"type": "float"},
                    "fuel_rate": {"type": "float"},
                    "tenant_id": {"type": "keyword"}
                }
            }
        },
        "priority": 100,
        "version": 1,
        "_meta": {
            "description": f"Engine metrics for multi-metric anomaly detection - tenant {tenant_id}"
        }
    }
    
    # Maintenance events index template for vector similarity search
    index_templates[f"{tenant_id}_maintenance_events"] = {
        "index_patterns": [f"{tenant_id}-maintenance-events-*"],
        "template": {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2,
                "index.lifecycle.name": "analytics_ilm",
                "index.lifecycle.rollover_alias": f"{tenant_id}-maintenance-events",
                "index.knn": True
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "vessel_id": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "component": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                    "description": {"type": "text"},
                    "description_embedding": {
                        "type": "knn_vector",
                        "dimension": 384,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 16
                            }
                        }
                    },
                    "resolution": {"type": "text"},
                    "cost": {"type": "float"},
                    "downtime_hours": {"type": "float"},
                    "technician": {"type": "keyword"},
                    "parts_replaced": {"type": "text"},
                    "tenant_id": {"type": "keyword"}
                }
            }
        },
        "priority": 100,
        "version": 1,
        "_meta": {
            "description": f"Maintenance events with vector embeddings for similarity search - tenant {tenant_id}"
        }
    }
    
    # Incidents index template for narrative similarity search
    index_templates[f"{tenant_id}_incidents"] = {
        "index_patterns": [f"{tenant_id}-incidents-*"],
        "template": {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2,
                "index.lifecycle.name": "analytics_ilm",
                "index.lifecycle.rollover_alias": f"{tenant_id}-incidents",
                "index.knn": True
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "incident_id": {"type": "keyword"},
                    "vessel_id": {"type": "keyword"},
                    "incident_type": {"type": "keyword"},
                    "severity": {"type": "keyword"},
                    "location": {"type": "geo_point"},
                    "narrative": {"type": "text"},
                    "narrative_embedding": {
                        "type": "knn_vector",
                        "dimension": 384,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 16
                            }
                        }
                    },
                    "injuries": {"type": "integer"},
                    "damage_cost": {"type": "float"},
                    "reported_by": {"type": "keyword"},
                    "status": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"}
                }
            }
        },
        "priority": 100,
        "version": 1,
        "_meta": {
            "description": f"Incident reports with narrative embeddings - tenant {tenant_id}"
        }
    }
    
    # Anomalies index template for storing detected anomalies
    index_templates[f"{tenant_id}_anomalies"] = {
        "index_patterns": [f"{tenant_id}-anomalies-*"],
        "template": {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2,
                "index.lifecycle.name": "analytics_ilm",
                "index.lifecycle.rollover_alias": f"{tenant_id}-anomalies",
                "index.knn": True
            },
            "mappings": {
                "properties": {
                    "timestamp": {"type": "date"},
                    "detector_id": {"type": "keyword"},
                    "detector_name": {"type": "keyword"},
                    "vessel_id": {"type": "keyword"},
                    "anomaly_grade": {"type": "float"},
                    "confidence": {"type": "float"},
                    "feature_data": {"type": "object"},
                    "anomaly_type": {"type": "keyword"},
                    "anomaly_embedding": {
                        "type": "knn_vector",
                        "dimension": 384,
                        "method": {
                            "name": "hnsw",
                            "space_type": "cosinesimil",
                            "engine": "nmslib",
                            "parameters": {
                                "ef_construction": 128,
                                "m": 16
                            }
                        }
                    },
                    "expected_value": {"type": "float"},
                    "actual_value": {"type": "float"},
                    "tenant_id": {"type": "keyword"}
                }
            }
        },
        "priority": 100,
        "version": 1,
        "_meta": {
            "description": f"Detected anomalies with embeddings for pattern similarity - tenant {tenant_id}"
        }
    }

# Save index templates
for template_name, template_config in index_templates.items():
    file_path = f"ops/opensearch/index-templates/{template_name}.json"
    with open(file_path, 'w') as f:
        json.dump(template_config, f, indent=2)

print("✓ Generated Index Templates:")
for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    print(f"  {tenant_id}:")
    print(f"    - vessel-telemetry (GeoPoint, telemetry_ilm)")
    print(f"    - engine-metrics (multi-metric AD, telemetry_ilm)")
    print(f"    - maintenance-events (384-dim vectors, analytics_ilm)")
    print(f"    - incidents (narratives + GeoPoint, analytics_ilm)")
    print(f"    - anomalies (pattern embeddings, analytics_ilm)")
print()

# Generate RBAC roles per tenant
rbac_roles = {}

for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    admin_roles = tenant['admin_roles']
    
    # Tenant admin role - full access to tenant indices
    rbac_roles[f"{tenant_id}_admin"] = {
        "cluster_permissions": [
            "cluster:monitor/*",
            "cluster:admin/opendistro/ad/*",
            "cluster:admin/opendistro/alerting/*"
        ],
        "index_permissions": [
            {
                "index_patterns": [f"{tenant_id}-*"],
                "allowed_actions": [
                    "indices:*"
                ]
            }
        ],
        "tenant_permissions": [
            {
                "tenant_patterns": [tenant_id],
                "allowed_actions": ["kibana_all_write"]
            }
        ]
    }
    
    # Tenant read-only analyst role
    rbac_roles[f"{tenant_id}_analyst"] = {
        "cluster_permissions": [
            "cluster:monitor/*"
        ],
        "index_permissions": [
            {
                "index_patterns": [f"{tenant_id}-*"],
                "allowed_actions": [
                    "indices:data/read/*",
                    "indices:admin/mappings/get",
                    "indices:monitor/*"
                ]
            }
        ],
        "tenant_permissions": [
            {
                "tenant_patterns": [tenant_id],
                "allowed_actions": ["kibana_all_read"]
            }
        ]
    }
    
    # Tenant writer role - for data ingestion services
    rbac_roles[f"{tenant_id}_writer"] = {
        "cluster_permissions": [],
        "index_permissions": [
            {
                "index_patterns": [f"{tenant_id}-*"],
                "allowed_actions": [
                    "indices:data/write/*",
                    "indices:admin/create"
                ]
            }
        ],
        "tenant_permissions": []
    }

# Save RBAC roles
for role_name, role_config in rbac_roles.items():
    file_path = f"ops/opensearch/roles/{role_name}.json"
    with open(file_path, 'w') as f:
        json.dump(role_config, f, indent=2)

print("✓ Generated RBAC Roles:")
for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    print(f"  {tenant_id}:")
    print(f"    - {tenant_id}_admin: full access to {tenant_id}-* indices")
    print(f"    - {tenant_id}_analyst: read-only access to {tenant_id}-* indices")
    print(f"    - {tenant_id}_writer: write access for ingestion services")
print()

# Store for downstream
opensearch_index_templates = index_templates
opensearch_rbac_roles = rbac_roles

print(f"✓ Created {len(index_templates)} tenant-prefixed index templates")
print(f"✓ Created {len(rbac_roles)} RBAC roles for tenant isolation")
print(f"✓ Configured GeoPoint mappings for vessel positions and incidents")
print(f"✓ Configured 384-dim knn_vector fields for similarity search")
