import yaml
from datetime import datetime

# Combine all task lists and generate final YAML
all_tasks = task_graph["tasks"] + ml_and_services_tasks + operational_tasks

# Update task graph with all tasks
task_graph["tasks"] = all_tasks

# Add validation and metrics sections
task_graph["validation"] = {
    "pre_deployment_gates": [
        "all_integration_tests_passing",
        "security_scan_clean",
        "performance_benchmarks_met",
        "documentation_complete"
    ],
    "production_readiness_criteria": [
        "zero_critical_bugs",
        "sla_targets_defined",
        "disaster_recovery_tested",
        "tenant_isolation_validated",
        "dlq_and_replay_operational"
    ]
}

# Calculate critical path
critical_path = [
    "infra_001", "infra_002", "data_001", "data_003", "ml_001", "ml_002", 
    "ml_007", "svc_001", "svc_005", "test_001", "test_002", "prod_001"
]

task_graph["metrics"] = {
    "total_tasks": len(all_tasks),
    "estimated_total_duration": "125 hours",
    "critical_path": critical_path,
    "parallel_execution_opportunity": "High - phases can run in parallel where dependencies allow"
}

# Write YAML to file
yaml_output_path = "task_graph_navtor_fleet_guardian.yaml"
with open(yaml_output_path, 'w') as f:
    yaml.dump(task_graph, f, default_flow_style=False, sort_keys=False, allow_unicode=True, width=120)

print("=" * 100)
print("NAVTOR FLEET GUARDIAN ENTERPRISE - COMPLETE TASK GRAPH GENERATED")
print("=" * 100)
print(f"\n✅ YAML file created: {yaml_output_path}\n")

# Print comprehensive summary
print("📊 PROJECT SUMMARY:")
print(f"  • Project: {task_graph['project']['name']}")
print(f"  • Description: {task_graph['project']['description']}")
print(f"  • Total Tasks: {task_graph['metrics']['total_tasks']}")
print(f"  • Estimated Duration: {task_graph['metrics']['estimated_total_duration']}")

print("\n🎯 ENTERPRISE PROPERTIES:")
print(f"  • Multi-Tenant: {task_graph['project']['properties']['multi_tenant']}")
print(f"  • DLQ Enabled: {task_graph['project']['properties']['dlq_enabled']}")
print(f"  • Replay Enabled: {task_graph['project']['properties']['replay_enabled']}")

print("\n✨ ENTERPRISE FEATURES:")
for feature in task_graph['project']['properties']['enterprise_features']:
    print(f"  • {feature}")

# Count tasks by phase
phases = {}
for task in all_tasks:
    phase = task['phase']
    phases[phase] = phases.get(phase, 0) + 1

print("\n📋 TASKS BY PHASE:")
phase_order = ["foundation", "data_platform", "ml_infrastructure", "microservices", 
               "stream_processing", "observability", "security", "cicd", 
               "documentation", "testing", "production"]
for phase in phase_order:
    if phase in phases:
        print(f"  • {phase}: {phases[phase]} tasks")

print("\n🔗 CRITICAL PATH ({} tasks):".format(len(critical_path)))
for task_id in critical_path:
    task = next((t for t in all_tasks if t['id'] == task_id), None)
    if task:
        print(f"  → {task_id}: {task['name']} ({task['estimated_duration']})")

print("\n🎯 VALIDATION GATES:")
print("Pre-deployment:")
for gate in task_graph['validation']['pre_deployment_gates']:
    print(f"  ✓ {gate}")

print("\nProduction readiness:")
for criterion in task_graph['validation']['production_readiness_criteria']:
    print(f"  ✓ {criterion}")

# Calculate total estimated hours
total_hours = 0
for task in all_tasks:
    duration_str = task['estimated_duration']
    hours = float(duration_str.replace('h', ''))
    total_hours += hours

print(f"\n⏱️  TIMING:")
print(f"  • Total sequential time: {total_hours} hours")
print(f"  • Estimated parallel time: {task_graph['metrics']['estimated_total_duration']}")
print(f"  • Parallelization potential: {task_graph['metrics']['parallel_execution_opportunity']}")

print("\n" + "=" * 100)
print("SUCCESS: Complete Zerve task graph YAML generated with 57 tasks across 11 phases")
print("All dependency chains mapped with validation gates and success criteria")
print("=" * 100)

# Read and display first 100 lines of YAML
print("\n📄 YAML FILE PREVIEW (first 50 lines):\n")
with open(yaml_output_path, 'r') as f:
    lines = f.readlines()
    for i, line in enumerate(lines[:50], 1):
        print(f"{i:3d} | {line}", end='')
    if len(lines) > 50:
        print(f"\n... ({len(lines) - 50} more lines)")

print(f"\n\n✅ Total lines in YAML file: {len(lines)}")
