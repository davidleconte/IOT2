import os
import json
from datetime import datetime

# Directory for extended ML configurations
ml_config_dir = "ml/deployment/opensearch-ml"

# ===== ML Model Deployment Configurations =====
ml_model_deployment_configs = {
    "watsonx_vessel_anomaly_detector": {
        "model_group": "vessel_monitoring",
        "model_type": "ANOMALY_DETECTION",
        "algorithm": "XGBoost",
        "version": "1.0.0",
        "deployment": {
            "target_nodes": ["ml-node-1", "ml-node-2", "ml-node-3"],
            "deploy_to_all_nodes": True,
            "enable_auto_deploy": True
        },
        "model_config": {
            "embedding_dimension": 128,
            "input_features": [
                "fuel_consumption",
                "engine_temperature",
                "speed",
                "draft",
                "rpm",
                "sea_state"
            ],
            "output_type": "anomaly_score",
            "threshold": 0.75
        },
        "resource_limits": {
            "max_memory_mb": 2048,
            "max_cpu_cores": 2,
            "max_concurrent_predictions": 100
        },
        "mlflow_integration": {
            "model_registry_uri": "http://mlflow-server:5000",
            "model_name": "watsonx_vessel_anomaly_detector",
            "production_stage": "Production",
            "auto_sync_versions": True
        }
    },
    "watsonx_predictive_maintenance": {
        "model_group": "maintenance_prediction",
        "model_type": "CLASSIFICATION",
        "algorithm": "RandomForest",
        "version": "2.1.0",
        "deployment": {
            "target_nodes": ["ml-node-1", "ml-node-2"],
            "deploy_to_all_nodes": False,
            "enable_auto_deploy": True
        },
        "model_config": {
            "embedding_dimension": 64,
            "input_features": [
                "operating_hours",
                "vibration_level",
                "temperature",
                "oil_pressure",
                "component_age"
            ],
            "output_classes": ["normal", "warning", "critical"],
            "confidence_threshold": 0.8
        },
        "resource_limits": {
            "max_memory_mb": 1024,
            "max_cpu_cores": 1,
            "max_concurrent_predictions": 50
        },
        "mlflow_integration": {
            "model_registry_uri": "http://mlflow-server:5000",
            "model_name": "predictive_maintenance_rf",
            "production_stage": "Production",
            "auto_sync_versions": True
        }
    },
    "watsonx_fuel_efficiency_predictor": {
        "model_group": "fuel_optimization",
        "model_type": "REGRESSION",
        "algorithm": "LightGBM",
        "version": "1.3.0",
        "deployment": {
            "target_nodes": ["ml-node-1", "ml-node-2", "ml-node-3"],
            "deploy_to_all_nodes": True,
            "enable_auto_deploy": True
        },
        "model_config": {
            "embedding_dimension": 96,
            "input_features": [
                "vessel_speed",
                "draft",
                "sea_state",
                "wind_speed",
                "cargo_weight",
                "route_distance"
            ],
            "output_type": "fuel_consumption_tons_per_day",
            "prediction_interval": 0.95
        },
        "resource_limits": {
            "max_memory_mb": 1536,
            "max_cpu_cores": 2,
            "max_concurrent_predictions": 80
        },
        "mlflow_integration": {
            "model_registry_uri": "http://mlflow-server:5000",
            "model_name": "fuel_efficiency_lgbm",
            "production_stage": "Production",
            "auto_sync_versions": True
        }
    }
}

ml_deployment_config_path = os.path.join(ml_config_dir, "ml_model_deployment_configs.json")
with open(ml_deployment_config_path, 'w') as f:
    json.dump(ml_model_deployment_configs, f, indent=2)

print(f"Created ML model deployment configurations: {ml_deployment_config_path}")

# ===== Inference Pipeline Configurations =====
inference_pipelines = {
    "vessel_anomaly_detection_pipeline": {
        "pipeline_id": "vessel-anomaly-detection-v1",
        "description": "Real-time vessel anomaly detection using watsonx.ai model",
        "processors": [
            {
                "type": "text_embedding",
                "config": {
                    "model_id": "huggingface/sentence-transformers/all-MiniLM-L6-v2",
                    "field_map": {
                        "text_field": "maintenance_notes",
                        "embedding_field": "maintenance_notes_embedding"
                    }
                }
            },
            {
                "type": "ml_inference",
                "config": {
                    "model_id": "watsonx_vessel_anomaly_detector",
                    "input_fields": [
                        "fuel_consumption",
                        "engine_temperature",
                        "speed",
                        "draft",
                        "rpm",
                        "sea_state"
                    ],
                    "output_field": "anomaly_prediction",
                    "include_confidence": True,
                    "include_feature_importance": True
                }
            },
            {
                "type": "script",
                "config": {
                    "source": "ctx.is_anomaly = ctx.anomaly_prediction.anomaly_score > 0.75;",
                    "lang": "painless"
                }
            },
            {
                "type": "set",
                "config": {
                    "field": "prediction_timestamp",
                    "value": "{{_ingest.timestamp}}"
                }
            }
        ],
        "on_failure": [
            {
                "set": {
                    "field": "pipeline_error",
                    "value": "{{ _ingest.on_failure_message }}"
                }
            }
        ]
    },
    "maintenance_prediction_pipeline": {
        "pipeline_id": "maintenance-prediction-v1",
        "description": "Predictive maintenance classification pipeline",
        "processors": [
            {
                "type": "text_embedding",
                "config": {
                    "model_id": "huggingface/sentence-transformers/all-MiniLM-L6-v2",
                    "field_map": {
                        "text_field": "equipment_description",
                        "embedding_field": "equipment_embedding"
                    }
                }
            },
            {
                "type": "ml_inference",
                "config": {
                    "model_id": "watsonx_predictive_maintenance",
                    "input_fields": [
                        "operating_hours",
                        "vibration_level",
                        "temperature",
                        "oil_pressure",
                        "component_age"
                    ],
                    "output_field": "maintenance_prediction",
                    "include_confidence": True
                }
            },
            {
                "type": "script",
                "config": {
                    "source": """
                        if (ctx.maintenance_prediction.predicted_class == 'critical') {
                            ctx.alert_severity = 'high';
                            ctx.requires_immediate_action = true;
                        } else if (ctx.maintenance_prediction.predicted_class == 'warning') {
                            ctx.alert_severity = 'medium';
                            ctx.requires_immediate_action = false;
                        } else {
                            ctx.alert_severity = 'low';
                            ctx.requires_immediate_action = false;
                        }
                    """,
                    "lang": "painless"
                }
            }
        ]
    },
    "fuel_optimization_pipeline": {
        "pipeline_id": "fuel-optimization-v1",
        "description": "Fuel consumption prediction pipeline for route optimization",
        "processors": [
            {
                "type": "ml_inference",
                "config": {
                    "model_id": "watsonx_fuel_efficiency_predictor",
                    "input_fields": [
                        "vessel_speed",
                        "draft",
                        "sea_state",
                        "wind_speed",
                        "cargo_weight",
                        "route_distance"
                    ],
                    "output_field": "fuel_prediction",
                    "include_confidence": True
                }
            },
            {
                "type": "script",
                "config": {
                    "source": """
                        ctx.predicted_fuel_consumption = ctx.fuel_prediction.predicted_value;
                        ctx.fuel_cost_usd = ctx.predicted_fuel_consumption * ctx.fuel_price_per_ton;
                        ctx.efficiency_rating = ctx.predicted_fuel_consumption < ctx.baseline_consumption ? 'efficient' : 'inefficient';
                    """,
                    "lang": "painless"
                }
            }
        ]
    }
}

inference_pipelines_path = os.path.join(ml_config_dir, "inference_pipelines.json")
with open(inference_pipelines_path, 'w') as f:
    json.dump(inference_pipelines, f, indent=2)

print(f"Created inference pipeline configurations: {inference_pipelines_path}")

# ===== MLflow Integration Configuration =====
mlflow_integration_config = {
    "mlflow_server": {
        "tracking_uri": "http://mlflow-server:5000",
        "artifact_uri": "s3://mlflow-artifacts",
        "registry_uri": "http://mlflow-server:5000"
    },
    "model_sync": {
        "sync_interval_minutes": 15,
        "auto_deploy_production": True,
        "staging_approval_required": True,
        "version_retention_count": 5
    },
    "model_groups": [
        {
            "mlflow_model_name": "watsonx_vessel_anomaly_detector",
            "opensearch_model_group": "vessel_monitoring",
            "auto_deploy_stages": ["Production"],
            "deployment_target": "all_ml_nodes"
        },
        {
            "mlflow_model_name": "predictive_maintenance_rf",
            "opensearch_model_group": "maintenance_prediction",
            "auto_deploy_stages": ["Production"],
            "deployment_target": "subset_ml_nodes"
        },
        {
            "mlflow_model_name": "fuel_efficiency_lgbm",
            "opensearch_model_group": "fuel_optimization",
            "auto_deploy_stages": ["Production", "Staging"],
            "deployment_target": "all_ml_nodes"
        }
    ],
    "metadata_sync": {
        "sync_tags": True,
        "sync_metrics": True,
        "sync_params": True,
        "create_opensearch_index": True,
        "index_name": "mlflow_model_metadata"
    },
    "model_validation": {
        "validate_before_deploy": True,
        "test_data_sample_size": 100,
        "required_metrics": ["accuracy", "precision", "recall"],
        "minimum_accuracy": 0.85
    }
}

mlflow_integration_path = os.path.join(ml_config_dir, "mlflow_integration_config.json")
with open(mlflow_integration_path, 'w') as f:
    json.dump(mlflow_integration_config, f, indent=2)

print(f"Created MLflow integration configuration: {mlflow_integration_path}")

# ===== Watsonx.ai Deployment Examples =====
watsonx_deployment_examples = {
    "example_1_anomaly_detection": {
        "title": "Deploy Watsonx.ai Anomaly Detection Model",
        "description": "End-to-end deployment of vessel anomaly detection model from watsonx.ai to OpenSearch ML",
        "steps": [
            {
                "step": 1,
                "action": "Export model from watsonx.ai",
                "command": "watsonx model export --model-id vessel-anomaly-v1 --format onnx --output ./models/"
            },
            {
                "step": 2,
                "action": "Register model in MLflow",
                "code": """
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
model_uri = "./models/vessel-anomaly-v1.onnx"

# Register model
model_details = mlflow.register_model(
    model_uri=model_uri,
    name="watsonx_vessel_anomaly_detector",
    tags={
        "source": "watsonx.ai",
        "algorithm": "XGBoost",
        "trained_date": "2024-12-30"
    }
)

# Transition to production
client.transition_model_version_stage(
    name="watsonx_vessel_anomaly_detector",
    version=model_details.version,
    stage="Production"
)
                """
            },
            {
                "step": 3,
                "action": "Deploy to OpenSearch ML",
                "code": """
from opensearch_ml_deployment import OpenSearchMLDeployer

deployer = OpenSearchMLDeployer(
    opensearch_url="https://opensearch-cluster:9200",
    mlflow_tracking_uri="http://mlflow-server:5000"
)

# Get model from MLflow
model_metadata = deployer.get_model_from_mlflow(
    model_name="watsonx_vessel_anomaly_detector",
    stage="Production"
)

# Register and deploy
model_id = deployer.register_model_to_opensearch(
    model_metadata=model_metadata,
    model_config={"embedding_dimension": 128}
)

deployment_result = deployer.deploy_model(
    model_id=model_id,
    deployment_config={"deploy_to_all_nodes": True}
)

print(f"Model deployed: {model_id}")
                """
            },
            {
                "step": 4,
                "action": "Test inference",
                "curl_example": """
POST /_plugins/_ml/_predict
{
  "model_id": "watsonx_vessel_anomaly_detector",
  "input_data": {
    "fuel_consumption": 45.2,
    "engine_temperature": 87.5,
    "speed": 18.3,
    "draft": 12.4,
    "rpm": 115.0,
    "sea_state": 3
  }
}
                """
            }
        ]
    },
    "example_2_batch_inference": {
        "title": "Batch Inference with Watsonx.ai Model",
        "description": "Perform batch predictions on historical vessel data",
        "code": """
from opensearchpy import OpenSearch

client = OpenSearch(
    hosts=['opensearch-cluster:9200'],
    http_auth=('admin', 'admin'),
    use_ssl=True,
    verify_certs=False
)

# Create inference pipeline
pipeline_body = {
    "description": "Batch vessel anomaly detection",
    "processors": [
        {
            "ml_inference": {
                "model_id": "watsonx_vessel_anomaly_detector",
                "input_fields": ["fuel_consumption", "engine_temperature", "speed", "draft", "rpm", "sea_state"],
                "output_field": "anomaly_prediction"
            }
        }
    ]
}

client.ingest.put_pipeline(id="batch_anomaly_detection", body=pipeline_body)

# Reindex with inference
reindex_body = {
    "source": {
        "index": "vessel_telemetry_raw"
    },
    "dest": {
        "index": "vessel_telemetry_with_predictions",
        "pipeline": "batch_anomaly_detection"
    }
}

response = client.reindex(body=reindex_body, wait_for_completion=False)
print(f"Batch inference task: {response['task']}")
        """
    }
}

watsonx_examples_path = os.path.join(ml_config_dir, "watsonx_deployment_examples.json")
with open(watsonx_examples_path, 'w') as f:
    json.dump(watsonx_deployment_examples, f, indent=2)

print(f"Created watsonx.ai deployment examples: {watsonx_examples_path}")

# ===== Vector Similarity Search for Maintenance Text and Anomaly Embeddings =====
vector_similarity_config = {
    "maintenance_text_similarity": {
        "index_name": "maintenance_events_similarity",
        "mapping": {
            "properties": {
                "maintenance_id": {"type": "keyword"},
                "vessel_id": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "maintenance_type": {"type": "keyword"},
                "description": {"type": "text"},
                "description_embedding": {
                    "type": "knn_vector",
                    "dimension": 384,
                    "method": {
                        "name": "hnsw",
                        "space_type": "l2",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 16
                        }
                    }
                },
                "severity": {"type": "keyword"},
                "component": {"type": "keyword"},
                "resolution_time_hours": {"type": "float"}
            }
        },
        "embedding_pipeline": {
            "pipeline_id": "maintenance_text_embedding_pipeline",
            "processors": [
                {
                    "text_embedding": {
                        "model_id": "huggingface/sentence-transformers/all-MiniLM-L6-v2",
                        "field_map": {
                            "description": "description_embedding"
                        }
                    }
                }
            ]
        },
        "search_examples": [
            {
                "use_case": "Find similar maintenance issues",
                "query": {
                    "size": 10,
                    "query": {
                        "knn": {
                            "description_embedding": {
                                "vector": "<query_embedding>",
                                "k": 10
                            }
                        }
                    },
                    "_source": ["maintenance_id", "vessel_id", "description", "severity", "component"]
                }
            },
            {
                "use_case": "Hybrid search: semantic + filters",
                "query": {
                    "size": 10,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "knn": {
                                        "description_embedding": {
                                            "vector": "<query_embedding>",
                                            "k": 50
                                        }
                                    }
                                }
                            ],
                            "filter": [
                                {"term": {"vessel_id": "V-12345"}},
                                {"term": {"severity": "critical"}}
                            ]
                        }
                    }
                }
            }
        ]
    },
    "anomaly_embeddings_similarity": {
        "index_name": "anomaly_patterns_similarity",
        "mapping": {
            "properties": {
                "anomaly_id": {"type": "keyword"},
                "vessel_id": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "anomaly_type": {"type": "keyword"},
                "feature_vector": {
                    "type": "knn_vector",
                    "dimension": 128,
                    "method": {
                        "name": "hnsw",
                        "space_type": "cosinesimil",
                        "engine": "nmslib",
                        "parameters": {
                            "ef_construction": 256,
                            "m": 32
                        }
                    }
                },
                "anomaly_score": {"type": "float"},
                "affected_features": {"type": "keyword"},
                "severity": {"type": "keyword"}
            }
        },
        "embedding_pipeline": {
            "pipeline_id": "anomaly_feature_embedding_pipeline",
            "processors": [
                {
                    "ml_inference": {
                        "model_id": "watsonx_vessel_anomaly_detector",
                        "input_fields": ["fuel_consumption", "engine_temperature", "speed", "draft", "rpm", "sea_state"],
                        "output_field": "feature_vector",
                        "output_type": "embedding"
                    }
                }
            ]
        },
        "search_examples": [
            {
                "use_case": "Find similar anomaly patterns",
                "query": {
                    "size": 20,
                    "query": {
                        "knn": {
                            "feature_vector": {
                                "vector": "<anomaly_embedding>",
                                "k": 20
                            }
                        }
                    },
                    "_source": ["anomaly_id", "vessel_id", "anomaly_type", "anomaly_score", "severity"]
                }
            },
            {
                "use_case": "Cross-fleet anomaly detection",
                "query": {
                    "size": 10,
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "knn": {
                                        "feature_vector": {
                                            "vector": "<current_anomaly_embedding>",
                                            "k": 100
                                        }
                                    }
                                }
                            ],
                            "must_not": [
                                {"term": {"vessel_id": "current_vessel"}}
                            ],
                            "filter": [
                                {"range": {"anomaly_score": {"gte": 0.7}}}
                            ]
                        }
                    },
                    "aggs": {
                        "by_vessel": {
                            "terms": {"field": "vessel_id", "size": 5}
                        },
                        "by_type": {
                            "terms": {"field": "anomaly_type", "size": 10}
                        }
                    }
                }
            }
        ]
    },
    "combined_similarity_search": {
        "description": "Combined maintenance text + anomaly pattern similarity",
        "use_case": "Find vessels with similar operational issues",
        "query": {
            "size": 10,
            "query": {
                "script_score": {
                    "query": {"match_all": {}},
                    "script": {
                        "source": """
                            double text_score = cosineSimilarity(params.text_vector, 'description_embedding') + 1.0;
                            double anomaly_score = cosineSimilarity(params.anomaly_vector, 'feature_vector') + 1.0;
                            return (0.6 * text_score) + (0.4 * anomaly_score);
                        """,
                        "params": {
                            "text_vector": "<maintenance_text_embedding>",
                            "anomaly_vector": "<anomaly_feature_embedding>"
                        }
                    }
                }
            }
        }
    }
}

vector_similarity_path = os.path.join(ml_config_dir, "vector_similarity_config.json")
with open(vector_similarity_path, 'w') as f:
    json.dump(vector_similarity_config, f, indent=2)

print(f"Created vector similarity search configuration: {vector_similarity_path}")

ml_plugin_extension_summary = {
    "extended_configurations": {
        "ml_model_deployments": len(ml_model_deployment_configs),
        "inference_pipelines": len(inference_pipelines),
        "mlflow_integration": "enabled",
        "watsonx_examples": len(watsonx_deployment_examples),
        "vector_similarity_indices": len(vector_similarity_config) - 1
    },
    "files_created": [
        ml_deployment_config_path,
        inference_pipelines_path,
        mlflow_integration_path,
        watsonx_examples_path,
        vector_similarity_path
    ],
    "key_features": [
        "Multiple model deployment configs (anomaly detection, predictive maintenance, fuel optimization)",
        "Production-ready inference pipelines with text embeddings",
        "MLflow registry integration with auto-sync",
        "Watsonx.ai model deployment examples with code",
        "Vector similarity search for maintenance text (384-dim)",
        "Anomaly pattern similarity search (128-dim)",
        "Hybrid semantic + filter search capabilities",
        "Cross-fleet anomaly detection"
    ],
    "ml_capabilities": {
        "models": ["XGBoost Anomaly Detection", "RandomForest Classification", "LightGBM Regression"],
        "embedding_dimensions": [384, 128, 96, 64],
        "knn_algorithms": ["HNSW (nmslib)"],
        "distance_metrics": ["L2", "Cosine Similarity"],
        "pipeline_processors": ["text_embedding", "ml_inference", "script", "set"]
    }
}

print(f"\n=== OpenSearch ML Plugin Extension Complete ===")
print(f"ML Model Deployments: {ml_plugin_extension_summary['extended_configurations']['ml_model_deployments']}")
print(f"Inference Pipelines: {ml_plugin_extension_summary['extended_configurations']['inference_pipelines']}")
print(f"Vector Similarity Indices: {ml_plugin_extension_summary['extended_configurations']['vector_similarity_indices']}")
print(f"Files created: {len(ml_plugin_extension_summary['files_created'])}")
