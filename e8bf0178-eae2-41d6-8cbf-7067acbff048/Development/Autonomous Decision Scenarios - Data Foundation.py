import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import json

# Zerve color palette
bg_color = '#1D1D20'
text_primary = '#fbfbff'
text_secondary = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'
success = '#17b26a'
warning = '#f04438'

# ===== SCENARIO 1: REROUTE_TO_ALTERNATE_HARBOR =====
scenario_1_reroute = {
    'scenario_id': 'REROUTE_TO_ALTERNATE_HARBOR',
    'description': 'ML predicts port congestion/closure + real-time weather hazards → automated route recalculation',
    'trigger_conditions': [
        'Port congestion ML model predicts >80% probability of closure within 24h',
        'Weather hazard detected within 50nm of destination port',
        'Current vessel ETA < 48 hours from destination',
        'Alternative ports available within 200nm radius'
    ],
    'data_sources': {
        'ml_predictions': 'watsonx.data (Iceberg tables via Presto)',
        'real_time_weather': 'OpenSearch (weather hazards, storm tracks)',
        'port_status': 'OpenSearch (berth availability, congestion levels)',
        'vessel_telemetry': 'OpenSearch (current position, speed, course)',
        'historical_routes': 'watsonx.data (route performance, fuel consumption)'
    },
    'decision_logic': {
        'signal_fusion': 'Combine ML congestion score + weather severity + berth availability',
        'confidence_threshold': 0.75,
        'ranking_criteria': ['ETA adjustment', 'fuel_cost_delta', 'berth_availability_score', 'port_congestion_level']
    },
    'output': {
        'recommended_ports': 'List[Port] ranked by composite score',
        'route_recalculation': 'New waypoints with ETA and fuel estimate',
        'decision_rationale': 'Explainability factors with weights'
    }
}

# ===== SCENARIO 2: DEPLOY_HELICOPTER_RESCUE =====
scenario_2_helicopter = {
    'scenario_id': 'DEPLOY_HELICOPTER_RESCUE',
    'description': 'Critical anomaly (engine failure + medical emergency) triggers helicopter dispatch',
    'trigger_conditions': [
        'Critical equipment failure detected (engine, steering)',
        'Medical emergency signal (SOS button pressed)',
        'Vessel position > 100nm from nearest port',
        'Weather conditions allow helicopter operations (wind < 40 knots)'
    ],
    'data_sources': {
        'anomaly_detection': 'OpenSearch (real-time equipment telemetry)',
        'vessel_trajectory': 'ML model in watsonx.data (trajectory prediction)',
        'emergency_signals': 'OpenSearch (SOS alerts, crew communications)',
        'helicopter_availability': 'Operational database (fleet status)',
        'weather_conditions': 'OpenSearch (wind speed, visibility at intercept points)'
    },
    'decision_logic': {
        'signal_fusion': 'Critical equipment failure AND (medical emergency OR fire alarm)',
        'confidence_threshold': 0.90,
        'intercept_calculation': 'Predict vessel drift trajectory + optimize helicopter launch point'
    },
    'output': {
        'dispatch_decision': 'Boolean + urgency level (CRITICAL/HIGH)',
        'intercept_coordinates': 'Lat/Lon with ETA for helicopter',
        'alternative_vessels': 'Nearby vessels for potential assistance',
        'decision_rationale': 'Risk assessment + time criticality'
    }
}

# ===== SCENARIO 3: REDUCE_SPEED_GRADUAL =====
scenario_3_speed_reduction = {
    'scenario_id': 'REDUCE_SPEED_GRADUAL',
    'description': 'Fuel efficiency optimization ML detects suboptimal speed → gradual reduction recommendation',
    'trigger_conditions': [
        'Current speed > optimal speed by >2 knots',
        'Fuel consumption rate > historical baseline by >15%',
        'ETA slack available (can arrive 2-6 hours late without penalty)',
        'Weather/sea conditions stable (next 12 hours)'
    ],
    'data_sources': {
        'fuel_optimization_model': 'Spark ML model on watsonx.data',
        'real_time_consumption': 'OpenSearch (fuel flow sensors)',
        'speed_curves': 'watsonx.data (historical speed vs consumption)',
        'schedule_constraints': 'Operational database (contract penalties, port slots)',
        'weather_forecast': 'OpenSearch (sea state, wind predictions)'
    },
    'decision_logic': {
        'signal_fusion': 'Fuel efficiency score + schedule flexibility + weather stability',
        'confidence_threshold': 0.70,
        'optimization': 'Maximize fuel savings while maintaining ETA within acceptable window'
    },
    'output': {
        'recommended_speed': 'Target speed with gradual reduction profile (knots/hour)',
        'fuel_savings_estimate': 'Liters and $ savings over remaining voyage',
        'eta_impact': 'New ETA with delay minutes',
        'decision_rationale': 'Fuel savings vs schedule impact trade-off'
    }
}

# ===== SCENARIO 4: EMERGENCY_STOP =====
scenario_4_emergency_stop = {
    'scenario_id': 'EMERGENCY_STOP',
    'description': 'Multi-signal fusion triggers immediate stop with nearest safe anchorage calculation',
    'trigger_conditions': [
        'Collision risk detected (AIS data shows vessel on collision course)',
        'Equipment critical failure (loss of steering, propulsion)',
        'Hazard zone entry (restricted area, underwater obstacles)',
        'Crew override signal (emergency stop button)'
    ],
    'data_sources': {
        'ais_collision_detection': 'OpenSearch (real-time AIS data, TCPA/CPA calculations)',
        'equipment_status': 'OpenSearch (critical system health)',
        'geofencing': 'OpenSearch (hazard zones, restricted areas)',
        'anchorage_locations': 'watsonx.data (safe anchorage points with depth/holding)',
        'weather_safety': 'OpenSearch (current conditions at anchorage points)'
    },
    'decision_logic': {
        'signal_fusion': 'ANY critical signal OR (equipment failure AND collision risk)',
        'confidence_threshold': 0.95,
        'anchorage_selection': 'Nearest safe point considering depth, weather, holding ground'
    },
    'output': {
        'stop_decision': 'IMMEDIATE STOP command',
        'safe_anchorage': 'Coordinates with distance and ETA',
        'risk_factors': 'List of detected hazards with severity',
        'decision_rationale': 'Safety-first explanation with alternative actions'
    }
}

# Consolidate all scenarios
autonomous_scenarios = {
    'scenario_1': scenario_1_reroute,
    'scenario_2': scenario_2_helicopter,
    'scenario_3': scenario_3_speed_reduction,
    'scenario_4': scenario_4_emergency_stop
}

# Create summary DataFrame
scenarios_summary = pd.DataFrame([
    {
        'Scenario': 'REROUTE_TO_ALTERNATE_HARBOR',
        'Trigger Type': 'Predictive + Real-time',
        'Confidence Threshold': '75%',
        'Primary Data Source': 'watsonx.data + OpenSearch',
        'Decision Latency Target': '<500ms',
        'Operator Override': 'Yes'
    },
    {
        'Scenario': 'DEPLOY_HELICOPTER_RESCUE',
        'Trigger Type': 'Critical Multi-signal',
        'Confidence Threshold': '90%',
        'Primary Data Source': 'OpenSearch + ML Trajectory',
        'Decision Latency Target': '<500ms',
        'Operator Override': 'Yes (with audit)'
    },
    {
        'Scenario': 'REDUCE_SPEED_GRADUAL',
        'Trigger Type': 'Optimization',
        'Confidence Threshold': '70%',
        'Primary Data Source': 'Spark ML + OpenSearch',
        'Decision Latency Target': '<500ms',
        'Operator Override': 'Yes'
    },
    {
        'Scenario': 'EMERGENCY_STOP',
        'Trigger Type': 'Safety-Critical',
        'Confidence Threshold': '95%',
        'Primary Data Source': 'OpenSearch (Real-time)',
        'Decision Latency Target': '<200ms',
        'Operator Override': 'Yes (immediate)'
    }
])

print("=" * 80)
print("AUTONOMOUS DECISION SCENARIOS - OVERVIEW")
print("=" * 80)
print(scenarios_summary.to_string(index=False))
print("\n")

# Technical requirements across all scenarios
technical_requirements = pd.DataFrame([
    {'Component': 'Decision Engine', 'Requirement': 'Process signals in <500ms', 'Technology': 'Python FastAPI + async'},
    {'Component': 'Signal Fusion', 'Requirement': 'Combine OpenSearch + watsonx.data queries', 'Technology': 'Parallel query execution'},
    {'Component': 'ML Model Serving', 'Requirement': 'Real-time inference <100ms', 'Technology': 'TensorFlow Serving / ONNX'},
    {'Component': 'Explainability', 'Requirement': 'Generate decision rationale', 'Technology': 'SHAP / LIME'},
    {'Component': 'Audit Logging', 'Requirement': 'Log all decisions with full context', 'Technology': 'OpenSearch audit index'},
    {'Component': 'Operator Dashboard', 'Requirement': 'Real-time decision monitoring', 'Technology': 'Grafana + WebSocket'},
    {'Component': 'Fallback Mechanism', 'Requirement': 'Handle data source failures', 'Technology': 'Circuit breaker pattern'},
    {'Component': 'Override Capability', 'Requirement': 'Operator can accept/reject/modify', 'Technology': 'REST API + event bus'}
])

print("TECHNICAL REQUIREMENTS")
print("=" * 80)
print(technical_requirements.to_string(index=False))
print("\n")

print(f"✓ Defined {len(autonomous_scenarios)} autonomous decision scenarios")
print("✓ All scenarios integrate both OpenSearch (real-time) and watsonx.data (predictive)")
print("✓ Decision latency targets: <500ms (general), <200ms (emergency)")
print("✓ Confidence thresholds: 70-95% depending on scenario criticality")
