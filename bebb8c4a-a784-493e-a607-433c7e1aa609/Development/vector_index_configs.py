import json
from pathlib import Path

# Ensure directories exist
Path('ops/opensearch/vector-indices').mkdir(parents=True, exist_ok=True)

# Generate JVector/KNN index configurations for similarity search
vector_indices = {}

for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    
    # 1. Maintenance events similarity search configuration
    vector_indices[f"{tenant_id}_maintenance_similarity"] = {
        "index_name": f"{tenant_id}-maintenance-events",
        "description": f"Vector similarity search for maintenance events - {tenant_id}",
        "vector_field": "description_embedding",
        "dimension": 384,
        "similarity_metric": "cosinesimil",
        "index_method": "hnsw",
        "index_parameters": {
            "ef_construction": 128,
            "m": 16,
            "ef_search": 100
        },
        "use_cases": [
            "Find similar historical maintenance issues",
            "Match current problems to past resolutions",
            "Identify recurring maintenance patterns",
            "Recommend preventive actions based on similar events"
        ],
        "example_query": {
            "size": 10,
            "query": {
                "knn": {
                    "description_embedding": {
                        "vector": "[... 384-dimensional vector ...]",
                        "k": 10
                    }
                }
            },
            "_source": [
                "vessel_id",
                "event_type",
                "component",
                "severity",
                "description",
                "resolution",
                "cost",
                "downtime_hours",
                "timestamp"
            ]
        }
    }
    
    # 2. Incident narratives similarity search configuration
    vector_indices[f"{tenant_id}_incident_similarity"] = {
        "index_name": f"{tenant_id}-incidents",
        "description": f"Vector similarity search for incident narratives - {tenant_id}",
        "vector_field": "narrative_embedding",
        "dimension": 384,
        "similarity_metric": "cosinesimil",
        "index_method": "hnsw",
        "index_parameters": {
            "ef_construction": 128,
            "m": 16,
            "ef_search": 100
        },
        "use_cases": [
            "Find incidents with similar circumstances",
            "Identify common incident patterns",
            "Learn from past incident responses",
            "Predict incident outcomes based on similar cases"
        ],
        "example_query": {
            "size": 10,
            "query": {
                "bool": {
                    "must": [
                        {
                            "knn": {
                                "narrative_embedding": {
                                    "vector": "[... 384-dimensional vector ...]",
                                    "k": 10
                                }
                            }
                        }
                    ],
                    "filter": [
                        {
                            "term": {
                                "tenant_id": tenant_id
                            }
                        }
                    ]
                }
            },
            "_source": [
                "incident_id",
                "vessel_id",
                "incident_type",
                "severity",
                "location",
                "narrative",
                "injuries",
                "damage_cost",
                "status",
                "timestamp"
            ]
        }
    }
    
    # 3. Anomaly patterns similarity search configuration
    vector_indices[f"{tenant_id}_anomaly_similarity"] = {
        "index_name": f"{tenant_id}-anomalies",
        "description": f"Vector similarity search for anomaly patterns - {tenant_id}",
        "vector_field": "anomaly_embedding",
        "dimension": 384,
        "similarity_metric": "cosinesimil",
        "index_method": "hnsw",
        "index_parameters": {
            "ef_construction": 128,
            "m": 16,
            "ef_search": 100
        },
        "use_cases": [
            "Cluster similar anomalies",
            "Identify anomaly families",
            "Detect recurring anomaly patterns",
            "Root cause analysis via pattern matching"
        ],
        "example_query": {
            "size": 20,
            "query": {
                "bool": {
                    "must": [
                        {
                            "knn": {
                                "anomaly_embedding": {
                                    "vector": "[... 384-dimensional vector ...]",
                                    "k": 20
                                }
                            }
                        }
                    ],
                    "filter": [
                        {
                            "term": {
                                "tenant_id": tenant_id
                            }
                        },
                        {
                            "range": {
                                "confidence": {
                                    "gte": 0.7
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "by_detector": {
                    "terms": {
                        "field": "detector_name",
                        "size": 10
                    }
                },
                "by_vessel": {
                    "terms": {
                        "field": "vessel_id",
                        "size": 10
                    }
                }
            },
            "_source": [
                "detector_id",
                "detector_name",
                "vessel_id",
                "anomaly_grade",
                "confidence",
                "anomaly_type",
                "expected_value",
                "actual_value",
                "timestamp"
            ]
        }
    }

# Save vector index configurations
for index_name, index_config in vector_indices.items():
    file_path = f"ops/opensearch/vector-indices/{index_name}.json"
    with open(file_path, 'w') as f:
        json.dump(index_config, f, indent=2)

print("✓ Generated Vector Index Configurations:")
print()
for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    print(f"  {tenant_id}:")
    print(f"    - maintenance-similarity: 384-dim HNSW (cosinesimil)")
    print(f"      Use: Find similar historical maintenance issues")
    print(f"    - incident-similarity: 384-dim HNSW (cosinesimil)")
    print(f"      Use: Match incidents with similar narratives")
    print(f"    - anomaly-similarity: 384-dim HNSW (cosinesimil)")
    print(f"      Use: Cluster and analyze anomaly patterns")
print()

# Generate embedding pipeline documentation
embedding_pipeline = {
    "description": "Embedding generation pipeline for OpenSearch vector fields",
    "model_recommendation": "sentence-transformers/all-MiniLM-L6-v2 (384 dimensions)",
    "alternative_models": [
        "sentence-transformers/all-mpnet-base-v2 (768 dimensions)",
        "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2 (384 dimensions)"
    ],
    "implementation_options": [
        {
            "option": "OpenSearch ML Plugin",
            "description": "Deploy embedding model directly in OpenSearch cluster",
            "pros": ["Low latency", "No external dependencies", "Automatic embedding"],
            "cons": ["Resource intensive", "Model management overhead"]
        },
        {
            "option": "Ingest Pipeline with External Service",
            "description": "Use Lambda/Fargate to generate embeddings during ingestion",
            "pros": ["Flexible model choice", "Separate compute", "Easy updates"],
            "cons": ["Additional latency", "External dependency"]
        }
    ],
    "fields_to_embed": {
        "maintenance_events": {
            "field": "description",
            "target": "description_embedding",
            "preprocessing": "Concatenate: event_type, component, description, resolution"
        },
        "incidents": {
            "field": "narrative",
            "target": "narrative_embedding",
            "preprocessing": "Concatenate: incident_type, severity, narrative"
        },
        "anomalies": {
            "field": "feature_data",
            "target": "anomaly_embedding",
            "preprocessing": "JSON to text: detector_name, anomaly_type, feature_data, expected/actual values"
        }
    }
}

embedding_pipeline_path = "ops/opensearch/vector-indices/embedding_pipeline.json"
with open(embedding_pipeline_path, 'w') as f:
    json.dump(embedding_pipeline, f, indent=2)

# Store for downstream
opensearch_vector_indices = vector_indices
opensearch_embedding_pipeline = embedding_pipeline

print(f"✓ Created {len(vector_indices)} vector index configurations")
print(f"✓ Generated embedding pipeline documentation")
print(f"✓ Recommended model: sentence-transformers/all-MiniLM-L6-v2 (384-dim)")
print(f"✓ All vector indices use HNSW algorithm with cosine similarity")
