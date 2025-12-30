# Continue building task graph - ML infrastructure and microservices phases

# Phase 3: Feature Store & ML Infrastructure
ml_infrastructure_tasks = [
    {
        "id": "ml_001",
        "name": "deploy_feast_feature_store",
        "description": "Deploy Feast with Redis online store and S3 offline store for tenant-aware features",
        "phase": "ml_infrastructure",
        "dependencies": ["data_001", "infra_006"],
        "validation_gates": ["feast_deployed", "online_store_ready", "offline_store_ready", "tenant_features_registered"],
        "success_criteria": "Feast operational with tenant-aware feature serving",
        "resources": ["feast/feature_repo/"],
        "estimated_duration": "2.5h"
    },
    {
        "id": "ml_002",
        "name": "define_vessel_features",
        "description": "Define vessel performance, maintenance, and route optimization features",
        "phase": "ml_infrastructure",
        "dependencies": ["ml_001"],
        "validation_gates": ["features_defined", "feature_validation_passing", "serving_tested"],
        "success_criteria": "All vessel features registered and serving correctly",
        "resources": ["feast/feature_repo/feature_definitions.py"],
        "estimated_duration": "1.5h"
    },
    {
        "id": "ml_003",
        "name": "train_vessel_performance_model",
        "description": "Train ML model for vessel performance optimization with tenant-specific tuning",
        "phase": "ml_infrastructure",
        "dependencies": ["ml_002", "data_005"],
        "validation_gates": ["model_trained", "accuracy_threshold_met", "tenant_models_validated"],
        "success_criteria": "Vessel performance model operational with >85% accuracy",
        "resources": ["ml/models/vessel_performance/"],
        "estimated_duration": "4h"
    },
    {
        "id": "ml_004",
        "name": "train_predictive_maintenance_model",
        "description": "Train predictive maintenance model for equipment failure prediction",
        "phase": "ml_infrastructure",
        "dependencies": ["ml_002", "data_005"],
        "validation_gates": ["model_trained", "precision_threshold_met", "false_positive_rate_acceptable"],
        "success_criteria": "Predictive maintenance model operational with >80% precision",
        "resources": ["ml/models/predictive_maintenance/"],
        "estimated_duration": "4h"
    },
    {
        "id": "ml_005",
        "name": "train_route_optimization_model",
        "description": "Train route optimization model considering weather, fuel, and ETA",
        "phase": "ml_infrastructure",
        "dependencies": ["ml_002", "data_005"],
        "validation_gates": ["model_trained", "optimization_validated", "fuel_savings_demonstrated"],
        "success_criteria": "Route optimization model operational with measurable fuel savings",
        "resources": ["ml/models/route_optimization/"],
        "estimated_duration": "5h"
    },
    {
        "id": "ml_006",
        "name": "train_anomaly_detection_model",
        "description": "Train unsupervised anomaly detection model for fleet monitoring",
        "phase": "ml_infrastructure",
        "dependencies": ["ml_002", "data_005"],
        "validation_gates": ["model_trained", "anomaly_detection_tested", "alert_integration_working"],
        "success_criteria": "Anomaly detection operational with real-time alerting",
        "resources": ["ml/models/anomaly_detection/"],
        "estimated_duration": "3h"
    },
    {
        "id": "ml_007",
        "name": "deploy_model_serving",
        "description": "Deploy ML model serving infrastructure with A/B testing and monitoring",
        "phase": "ml_infrastructure",
        "dependencies": ["ml_003", "ml_004", "ml_005", "ml_006"],
        "validation_gates": ["models_deployed", "serving_endpoints_active", "monitoring_configured"],
        "success_criteria": "All ML models serving with <100ms latency",
        "resources": ["services/", "helm/"],
        "estimated_duration": "2h"
    }
]

# Phase 4: Microservices
microservices_tasks = [
    {
        "id": "svc_001",
        "name": "deploy_auth_service",
        "description": "Deploy authentication service with OAuth2, JWT, and tenant-aware RBAC",
        "phase": "microservices",
        "dependencies": ["infra_002", "infra_003"],
        "validation_gates": ["auth_deployed", "oauth_working", "rbac_enforced", "tenant_isolation_verified"],
        "success_criteria": "Auth service operational with multi-tenant RBAC",
        "resources": ["services/auth-service/", "helm/navtor-fleet-guardian/"],
        "estimated_duration": "2.5h"
    },
    {
        "id": "svc_002",
        "name": "deploy_tenant_service",
        "description": "Deploy tenant management service for onboarding, configuration, and billing",
        "phase": "microservices",
        "dependencies": ["svc_001"],
        "validation_gates": ["tenant_svc_deployed", "onboarding_working", "billing_integration_active"],
        "success_criteria": "Tenant service operational with automated onboarding",
        "resources": ["services/tenant-service/", "helm/navtor-fleet-guardian/"],
        "estimated_duration": "3h"
    },
    {
        "id": "svc_003",
        "name": "deploy_vessel_tracking_service",
        "description": "Deploy real-time vessel tracking service with AIS data processing",
        "phase": "microservices",
        "dependencies": ["svc_001", "data_003"],
        "validation_gates": ["tracking_svc_deployed", "ais_processing_active", "real_time_updates_working"],
        "success_criteria": "Vessel tracking operational with <5s latency",
        "resources": ["services/vessel-tracking/", "helm/navtor-fleet-guardian/"],
        "estimated_duration": "3h"
    },
    {
        "id": "svc_004",
        "name": "deploy_fleet_management_service",
        "description": "Deploy fleet management service for voyage planning and vessel assignments",
        "phase": "microservices",
        "dependencies": ["svc_001", "svc_003"],
        "validation_gates": ["fleet_svc_deployed", "voyage_planning_working", "assignments_automated"],
        "success_criteria": "Fleet management operational with automated planning",
        "resources": ["services/fleet-management/", "helm/navtor-fleet-guardian/"],
        "estimated_duration": "3.5h"
    },
    {
        "id": "svc_005",
        "name": "deploy_analytics_service",
        "description": "Deploy analytics service with ML model integration and real-time insights",
        "phase": "microservices",
        "dependencies": ["svc_001", "ml_007"],
        "validation_gates": ["analytics_svc_deployed", "ml_integration_working", "dashboards_active"],
        "success_criteria": "Analytics service operational with ML-powered insights",
        "resources": ["services/analytics-service/", "helm/navtor-fleet-guardian/"],
        "estimated_duration": "3h"
    }
]

print("=" * 100)
print("TASK GRAPH GENERATION - ML AND SERVICES PHASES")
print("=" * 100)
print(f"✅ ML infrastructure tasks: {len(ml_infrastructure_tasks)} tasks")
print(f"✅ Microservices tasks: {len(microservices_tasks)} tasks")
print(f"📊 New tasks total: {len(ml_infrastructure_tasks) + len(microservices_tasks)}")

# Store for next block
ml_and_services_tasks = ml_infrastructure_tasks + microservices_tasks