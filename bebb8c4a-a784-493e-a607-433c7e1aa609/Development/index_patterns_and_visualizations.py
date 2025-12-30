import json
import os

# Directory for index patterns and visualizations
viz_dir = "ops/opensearch/visualizations"
os.makedirs(viz_dir, exist_ok=True)

tenant_configs = [
    {"id": "shipping-co-alpha", "name": "Shipping Co Alpha"},
    {"id": "logistics-beta", "name": "Logistics Beta"},
    {"id": "maritime-gamma", "name": "Maritime Gamma"}
]

visualization_definitions = []

# Index Patterns for each tenant
for tenant in tenant_configs:
    # Vessel Telemetry Index Pattern
    telemetry_index_pattern = {
        "version": "2.11.0",
        "objects": [{
            "id": f"{tenant['id']}_vessel_telemetry_pattern",
            "type": "index-pattern",
            "attributes": {
                "title": f"{tenant['id']}_vessel_telemetry-*",
                "timeFieldName": "@timestamp",
                "fields": json.dumps([
                    {"name": "vessel_id", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "imo_number", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "vessel_name", "type": "text", "searchable": True, "aggregatable": False},
                    {"name": "location", "type": "geo_point", "searchable": True, "aggregatable": True},
                    {"name": "latitude", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "longitude", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "speed_knots", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "heading", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "fuel_consumption_mt_per_day", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "engine_rpm", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "region", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "@timestamp", "type": "date", "searchable": True, "aggregatable": True}
                ])
            },
            "references": [],
            "migrationVersion": {"index-pattern": "2.11.0"}
        }]
    }
    visualization_definitions.append((f"{tenant['id']}_vessel_telemetry_index_pattern.ndjson", telemetry_index_pattern))
    
    # Anomalies Index Pattern
    anomalies_index_pattern = {
        "version": "2.11.0",
        "objects": [{
            "id": f"{tenant['id']}_anomalies_pattern",
            "type": "index-pattern",
            "attributes": {
                "title": f"{tenant['id']}_anomalies-*",
                "timeFieldName": "detection_timestamp",
                "fields": json.dumps([
                    {"name": "anomaly_id", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "vessel_id", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "detector_id", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "anomaly_type", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "severity", "type": "keyword", "searchable": True, "aggregatable": True},
                    {"name": "confidence_score", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "anomaly_grade", "type": "float", "searchable": True, "aggregatable": True},
                    {"name": "feature_data", "type": "object", "searchable": False, "aggregatable": False},
                    {"name": "detection_timestamp", "type": "date", "searchable": True, "aggregatable": True}
                ])
            },
            "references": [],
            "migrationVersion": {"index-pattern": "2.11.0"}
        }]
    }
    visualization_definitions.append((f"{tenant['id']}_anomalies_index_pattern.ndjson", anomalies_index_pattern))

# Fleet Map GeoPoint Visualization
for tenant in tenant_configs:
    fleet_map_viz = {
        "version": "2.11.0",
        "objects": [{
            "id": f"{tenant['id']}_fleet_map_geo",
            "type": "visualization",
            "attributes": {
                "title": f"{tenant['name']} Fleet Map",
                "description": "Real-time vessel positions with GeoPoint visualization",
                "visState": json.dumps({
                    "title": f"{tenant['name']} Fleet Map",
                    "type": "region_map",
                    "params": {
                        "addTooltip": True,
                        "colorSchema": "Yellow to Red",
                        "emsHotLink": None,
                        "isDesaturated": True,
                        "legendPosition": "bottomright",
                        "mapCenter": [0, 0],
                        "mapZoom": 2,
                        "outlineWeight": 1,
                        "showAllShapes": True,
                        "wms": {
                            "enabled": False,
                            "options": {"format": "image/png", "transparent": True}
                        }
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "schema": "metric",
                            "params": {}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "geohash_grid",
                            "schema": "segment",
                            "params": {
                                "field": "location",
                                "autoPrecision": True,
                                "precision": 2,
                                "useGeocentroid": True
                            }
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({}),
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": f"{tenant['id']}_vessel_telemetry_pattern",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": [{"id": f"{tenant['id']}_vessel_telemetry_pattern", "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}],
            "migrationVersion": {"visualization": "2.11.0"}
        }]
    }
    visualization_definitions.append((f"{tenant['id']}_fleet_map_geo_viz.ndjson", fleet_map_viz))

# Anomaly Timeline Visualizations
for tenant in tenant_configs:
    fuel_anomaly_timeline = {
        "version": "2.11.0",
        "objects": [{
            "id": f"{tenant['id']}_fuel_anomaly_timeline",
            "type": "visualization",
            "attributes": {
                "title": f"{tenant['name']} Fuel Consumption Anomalies Timeline",
                "description": "Time-series view of fuel consumption anomalies with detector annotations",
                "visState": json.dumps({
                    "title": f"{tenant['name']} Fuel Consumption Anomalies",
                    "type": "line",
                    "params": {
                        "type": "line",
                        "grid": {"categoryLines": False},
                        "categoryAxes": [{
                            "id": "CategoryAxis-1",
                            "type": "category",
                            "position": "bottom",
                            "show": True,
                            "style": {},
                            "scale": {"type": "linear"},
                            "labels": {"show": True, "truncate": 100},
                            "title": {}
                        }],
                        "valueAxes": [{
                            "id": "ValueAxis-1",
                            "name": "LeftAxis-1",
                            "type": "value",
                            "position": "left",
                            "show": True,
                            "style": {},
                            "scale": {"type": "linear", "mode": "normal"},
                            "labels": {"show": True, "rotate": 0, "filter": False, "truncate": 100},
                            "title": {"text": "Anomaly Grade"}
                        }],
                        "seriesParams": [{
                            "show": True,
                            "type": "line",
                            "mode": "normal",
                            "data": {"label": "Anomaly Grade", "id": "1"},
                            "valueAxis": "ValueAxis-1",
                            "drawLinesBetweenPoints": True,
                            "lineWidth": 2,
                            "showCircles": True
                        }],
                        "addTooltip": True,
                        "addLegend": True,
                        "legendPosition": "right",
                        "times": [],
                        "addTimeMarker": False,
                        "thresholdLine": {"show": True, "value": 0.7, "width": 1, "style": "dashed", "color": "#E7664C"}
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "anomaly_grade"}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "date_histogram",
                            "schema": "segment",
                            "params": {
                                "field": "detection_timestamp",
                                "timeRange": {"from": "now-7d", "to": "now"},
                                "useNormalizedOpenSearchInterval": True,
                                "scaleMetricValues": False,
                                "interval": "auto",
                                "drop_partials": False,
                                "min_doc_count": 1,
                                "extended_bounds": {}
                            }
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({}),
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": f"{tenant['id']}_anomalies_pattern",
                        "query": {"query": "anomaly_type:fuel_consumption_spikes", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": [{"id": f"{tenant['id']}_anomalies_pattern", "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}],
            "migrationVersion": {"visualization": "2.11.0"}
        }]
    }
    visualization_definitions.append((f"{tenant['id']}_fuel_anomaly_timeline_viz.ndjson", fuel_anomaly_timeline))

# Compliance KPI Metric Visualizations
for tenant in tenant_configs:
    # CO2 Emissions Metric
    co2_metric = {
        "version": "2.11.0",
        "objects": [{
            "id": f"{tenant['id']}_co2_emissions_metric",
            "type": "visualization",
            "attributes": {
                "title": f"{tenant['name']} CO2 Emissions",
                "description": "Total CO2 emissions in metric tons",
                "visState": json.dumps({
                    "title": f"{tenant['name']} CO2 Emissions",
                    "type": "metric",
                    "params": {
                        "addTooltip": True,
                        "addLegend": False,
                        "type": "metric",
                        "metric": {
                            "percentageMode": False,
                            "useRanges": False,
                            "colorSchema": "Green to Red",
                            "metricColorMode": "None",
                            "colorsRange": [{"from": 0, "to": 10000}],
                            "labels": {"show": True},
                            "invertColors": False,
                            "style": {
                                "bgFill": "#000",
                                "bgColor": False,
                                "labelColor": False,
                                "subText": "",
                                "fontSize": 60
                            }
                        }
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "sum",
                            "schema": "metric",
                            "params": {"field": "engine_metrics.co2_emissions_mt"}
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({}),
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": f"{tenant['id']}_vessel_telemetry_pattern",
                        "query": {"query": "", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": [{"id": f"{tenant['id']}_vessel_telemetry_pattern", "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}],
            "migrationVersion": {"visualization": "2.11.0"}
        }]
    }
    visualization_definitions.append((f"{tenant['id']}_co2_emissions_metric_viz.ndjson", co2_metric))

# Vector Similarity Table Visualization
for tenant in tenant_configs:
    similar_vessels_table = {
        "version": "2.11.0",
        "objects": [{
            "id": f"{tenant['id']}_similar_vessels_table",
            "type": "visualization",
            "attributes": {
                "title": f"{tenant['name']} Similar Vessels Table",
                "description": "Vessels with similar behavior patterns (vector similarity search results)",
                "visState": json.dumps({
                    "title": f"{tenant['name']} Similar Vessels",
                    "type": "table",
                    "params": {
                        "perPage": 10,
                        "showPartialRows": False,
                        "showMetricsAtAllLevels": False,
                        "showTotal": False,
                        "totalFunc": "sum",
                        "percentageCol": ""
                    },
                    "aggs": [
                        {
                            "id": "1",
                            "enabled": True,
                            "type": "count",
                            "schema": "metric",
                            "params": {}
                        },
                        {
                            "id": "2",
                            "enabled": True,
                            "type": "terms",
                            "schema": "bucket",
                            "params": {
                                "field": "vessel_id",
                                "orderBy": "1",
                                "order": "desc",
                                "size": 10,
                                "otherBucket": False,
                                "otherBucketLabel": "Other",
                                "missingBucket": False,
                                "missingBucketLabel": "Missing"
                            }
                        },
                        {
                            "id": "3",
                            "enabled": True,
                            "type": "avg",
                            "schema": "metric",
                            "params": {"field": "similarity_score"}
                        },
                        {
                            "id": "4",
                            "enabled": True,
                            "type": "terms",
                            "schema": "bucket",
                            "params": {
                                "field": "pattern_type",
                                "orderBy": "1",
                                "order": "desc",
                                "size": 5
                            }
                        }
                    ]
                }),
                "uiStateJSON": json.dumps({}),
                "kibanaSavedObjectMeta": {
                    "searchSourceJSON": json.dumps({
                        "index": f"{tenant['id']}_anomalies_pattern",
                        "query": {"query": "_exists_:similarity_score", "language": "kuery"},
                        "filter": []
                    })
                }
            },
            "references": [{"id": f"{tenant['id']}_anomalies_pattern", "name": "kibanaSavedObjectMeta.searchSourceJSON.index", "type": "index-pattern"}],
            "migrationVersion": {"visualization": "2.11.0"}
        }]
    }
    visualization_definitions.append((f"{tenant['id']}_similar_vessels_table_viz.ndjson", similar_vessels_table))

# Write all visualization NDJSON files
for filename, viz_config in visualization_definitions:
    viz_path = os.path.join(viz_dir, filename)
    with open(viz_path, 'w') as f:
        for obj in viz_config['objects']:
            json.dump(obj, f)
            f.write('\n')

print(f"✅ Created {len(visualization_definitions)} Index Patterns and Visualizations")
print(f"📁 Location: {viz_dir}/")
print(f"\n📊 Visualization Components:")
print(f"  - Index Patterns: {len(tenant_configs) * 2} (telemetry + anomalies per tenant)")
print(f"  - Fleet Map Geo Visualizations: {len(tenant_configs)}")
print(f"  - Anomaly Timeline Charts: {len(tenant_configs)}")
print(f"  - Compliance KPI Metrics: {len(tenant_configs)}")
print(f"  - Vector Similarity Tables: {len(tenant_configs)}")
print(f"\n✨ All visualizations use tenant-scoped index patterns")

index_patterns_viz_created = {
    "total_artifacts": len(visualization_definitions),
    "index_patterns": len(tenant_configs) * 2,
    "visualizations": len(visualization_definitions) - (len(tenant_configs) * 2),
    "tenants": [t['id'] for t in tenant_configs],
    "location": viz_dir
}
