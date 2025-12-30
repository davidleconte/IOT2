import json
from pathlib import Path

# Ensure directories exist
Path('ops/opensearch/anomaly-detectors').mkdir(parents=True, exist_ok=True)

# Generate anomaly detector configurations for each tenant
anomaly_detectors = {}

for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    
    # 1. Single-metric detector: Fuel consumption spikes
    anomaly_detectors[f"{tenant_id}_fuel_consumption_spikes"] = {
        "name": f"{tenant_id}-fuel-consumption-spikes",
        "description": f"Detects abnormal fuel consumption spikes for vessels in {tenant_id}",
        "time_field": "timestamp",
        "indices": [f"{tenant_id}-vessel-telemetry-*"],
        "feature_attributes": [
            {
                "feature_name": "fuel_consumption",
                "feature_enabled": True,
                "aggregation_query": {
                    "fuel_consumption_agg": {
                        "sum": {
                            "field": "fuel_consumption"
                        }
                    }
                }
            }
        ],
        "filter_query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "tenant_id": tenant_id
                        }
                    },
                    {
                        "range": {
                            "timestamp": {
                                "gte": "now-1h"
                            }
                        }
                    }
                ]
            }
        },
        "detection_interval": {
            "period": {
                "interval": 5,
                "unit": "MINUTES"
            }
        },
        "window_delay": {
            "period": {
                "interval": 1,
                "unit": "MINUTES"
            }
        },
        "shingle_size": 8,
        "category_field": ["vessel_id"],
        "result_index": f"{tenant_id}-anomalies"
    }
    
    # 2. Multi-metric detector: Correlated engine parameters
    anomaly_detectors[f"{tenant_id}_engine_multi_metric"] = {
        "name": f"{tenant_id}-engine-multi-metric",
        "description": f"Detects anomalies across correlated engine metrics (temp, pressure, vibration) for {tenant_id}",
        "time_field": "timestamp",
        "indices": [f"{tenant_id}-engine-metrics-*"],
        "feature_attributes": [
            {
                "feature_name": "engine_temperature",
                "feature_enabled": True,
                "aggregation_query": {
                    "temperature_agg": {
                        "avg": {
                            "field": "temperature"
                        }
                    }
                }
            },
            {
                "feature_name": "engine_pressure",
                "feature_enabled": True,
                "aggregation_query": {
                    "pressure_agg": {
                        "avg": {
                            "field": "pressure"
                        }
                    }
                }
            },
            {
                "feature_name": "engine_vibration",
                "feature_enabled": True,
                "aggregation_query": {
                    "vibration_agg": {
                        "avg": {
                            "field": "vibration"
                        }
                    }
                }
            },
            {
                "feature_name": "oil_pressure",
                "feature_enabled": True,
                "aggregation_query": {
                    "oil_pressure_agg": {
                        "avg": {
                            "field": "oil_pressure"
                        }
                    }
                }
            },
            {
                "feature_name": "exhaust_temp",
                "feature_enabled": True,
                "aggregation_query": {
                    "exhaust_temp_agg": {
                        "avg": {
                            "field": "exhaust_temp"
                        }
                    }
                }
            }
        ],
        "filter_query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "tenant_id": tenant_id
                        }
                    }
                ]
            }
        },
        "detection_interval": {
            "period": {
                "interval": 3,
                "unit": "MINUTES"
            }
        },
        "window_delay": {
            "period": {
                "interval": 1,
                "unit": "MINUTES"
            }
        },
        "shingle_size": 8,
        "category_field": ["vessel_id", "engine_id"],
        "result_index": f"{tenant_id}-anomalies"
    }
    
    # 3. Per-vessel behavior detector
    anomaly_detectors[f"{tenant_id}_vessel_behavior"] = {
        "name": f"{tenant_id}-vessel-behavior",
        "description": f"Detects deviations from normal vessel behavior patterns for {tenant_id}",
        "time_field": "timestamp",
        "indices": [f"{tenant_id}-vessel-telemetry-*"],
        "feature_attributes": [
            {
                "feature_name": "speed_pattern",
                "feature_enabled": True,
                "aggregation_query": {
                    "speed_agg": {
                        "avg": {
                            "field": "speed"
                        }
                    }
                }
            },
            {
                "feature_name": "fuel_efficiency",
                "feature_enabled": True,
                "aggregation_query": {
                    "fuel_efficiency_agg": {
                        "scripted_metric": {
                            "init_script": "state.fuel = []; state.distance = []",
                            "map_script": "state.fuel.add(doc['fuel_consumption'].value); state.distance.add(doc['speed'].value)",
                            "combine_script": "return [state.fuel.sum(), state.distance.sum()]",
                            "reduce_script": "double fuel = 0; double distance = 0; for (s in states) { fuel += s[0]; distance += s[1]; } return distance > 0 ? fuel / distance : 0"
                        }
                    }
                }
            },
            {
                "feature_name": "engine_load",
                "feature_enabled": True,
                "aggregation_query": {
                    "engine_load_agg": {
                        "avg": {
                            "field": "engine_rpm"
                        }
                    }
                }
            }
        ],
        "filter_query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "tenant_id": tenant_id
                        }
                    }
                ]
            }
        },
        "detection_interval": {
            "period": {
                "interval": 10,
                "unit": "MINUTES"
            }
        },
        "window_delay": {
            "period": {
                "interval": 2,
                "unit": "MINUTES"
            }
        },
        "shingle_size": 16,
        "category_field": ["vessel_id"],
        "result_index": f"{tenant_id}-anomalies"
    }
    
    # 4. Fleet-level patterns detector
    anomaly_detectors[f"{tenant_id}_fleet_patterns"] = {
        "name": f"{tenant_id}-fleet-patterns",
        "description": f"Detects fleet-wide anomalies and outlier vessels for {tenant_id}",
        "time_field": "timestamp",
        "indices": [f"{tenant_id}-vessel-telemetry-*"],
        "feature_attributes": [
            {
                "feature_name": "fleet_avg_fuel",
                "feature_enabled": True,
                "aggregation_query": {
                    "fleet_fuel_agg": {
                        "avg": {
                            "field": "fuel_consumption"
                        }
                    }
                }
            },
            {
                "feature_name": "fleet_avg_speed",
                "feature_enabled": True,
                "aggregation_query": {
                    "fleet_speed_agg": {
                        "avg": {
                            "field": "speed"
                        }
                    }
                }
            },
            {
                "feature_name": "active_vessel_count",
                "feature_enabled": True,
                "aggregation_query": {
                    "vessel_count_agg": {
                        "cardinality": {
                            "field": "vessel_id"
                        }
                    }
                }
            }
        ],
        "filter_query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "tenant_id": tenant_id
                        }
                    }
                ]
            }
        },
        "detection_interval": {
            "period": {
                "interval": 15,
                "unit": "MINUTES"
            }
        },
        "window_delay": {
            "period": {
                "interval": 2,
                "unit": "MINUTES"
            }
        },
        "shingle_size": 12,
        "result_index": f"{tenant_id}-anomalies"
    }
    
    # 5. Seasonal anomalies detector
    anomaly_detectors[f"{tenant_id}_seasonal_patterns"] = {
        "name": f"{tenant_id}-seasonal-patterns",
        "description": f"Detects seasonal anomalies and weather-related deviations for {tenant_id}",
        "time_field": "timestamp",
        "indices": [f"{tenant_id}-vessel-telemetry-*"],
        "feature_attributes": [
            {
                "feature_name": "hourly_fuel_pattern",
                "feature_enabled": True,
                "aggregation_query": {
                    "hourly_fuel_agg": {
                        "avg": {
                            "field": "fuel_consumption"
                        }
                    }
                }
            },
            {
                "feature_name": "daily_distance",
                "feature_enabled": True,
                "aggregation_query": {
                    "daily_distance_agg": {
                        "sum": {
                            "field": "speed"
                        }
                    }
                }
            }
        ],
        "filter_query": {
            "bool": {
                "filter": [
                    {
                        "term": {
                            "tenant_id": tenant_id
                        }
                    }
                ]
            }
        },
        "detection_interval": {
            "period": {
                "interval": 1,
                "unit": "HOURS"
            }
        },
        "window_delay": {
            "period": {
                "interval": 5,
                "unit": "MINUTES"
            }
        },
        "shingle_size": 24,
        "category_field": ["vessel_id"],
        "result_index": f"{tenant_id}-anomalies"
    }

# Save anomaly detector configurations
for detector_name, detector_config in anomaly_detectors.items():
    file_path = f"ops/opensearch/anomaly-detectors/{detector_name}.json"
    with open(file_path, 'w') as f:
        json.dump(detector_config, f, indent=2)

print("✓ Generated Anomaly Detector Configurations:")
print()
for tenant in opensearch_tenants:
    tenant_id = tenant['tenant_id']
    print(f"  {tenant_id}:")
    print(f"    - fuel-consumption-spikes: Single-metric (5min intervals)")
    print(f"    - engine-multi-metric: 5 correlated metrics (3min intervals)")
    print(f"    - vessel-behavior: Per-vessel patterns (10min intervals)")
    print(f"    - fleet-patterns: Fleet-wide anomalies (15min intervals)")
    print(f"    - seasonal-patterns: Hourly/daily seasonality (1hr intervals)")
print()

# Store for downstream
opensearch_anomaly_detectors = anomaly_detectors

print(f"✓ Created {len(anomaly_detectors)} anomaly detector configurations")
print(f"✓ Configured single-metric, multi-metric, per-vessel, fleet, and seasonal detectors")
print(f"✓ All detectors respect tenant isolation via index patterns and filters")
