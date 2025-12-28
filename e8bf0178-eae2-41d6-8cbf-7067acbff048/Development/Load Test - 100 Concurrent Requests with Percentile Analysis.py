import pandas as pd
import numpy as np
import time
from datetime import datetime
from typing import Dict, List
import concurrent.futures

# Recreate DistributedTracer for this block
class DistributedTracer:
    def __init__(self, scenario_name: str):
        self.scenario_name = scenario_name
        self.trace_id = f"{scenario_name}_{int(time.time() * 1000000)}"
        self.spans = []
    
    def start_span(self, component_name: str, operation: str):
        span = {
            'component': component_name,
            'operation': operation,
            'start_time': time.perf_counter(),
            'end_time': None,
            'duration_ms': None
        }
        self.spans.append(span)
        return span
    
    def end_span(self, span: dict):
        span['end_time'] = time.perf_counter()
        span['duration_ms'] = (span['end_time'] - span['start_time']) * 1000
        return span
    
    def get_total_latency(self) -> float:
        if not self.spans:
            return 0.0
        return sum(span['duration_ms'] for span in self.spans)
    
    def get_component_latencies(self) -> Dict[str, float]:
        """Get latencies by component"""
        component_latencies = {}
        for span in self.spans:
            component_latencies[span['component']] = span['duration_ms']
        return component_latencies


def simulate_decision_with_tracing(scenario_name: str, scenario_type: str):
    """Execute a decision scenario with tracing"""
    tracer = DistributedTracer(scenario_name)
    
    # Component 1: Data Ingestion
    span = tracer.start_span('Data Ingestion', 'fetch_realtime_telemetry')
    time.sleep(np.random.uniform(0.015, 0.035))
    tracer.end_span(span)
    
    # Component 2: Signal Fusion
    span = tracer.start_span('Signal Fusion', 'fuse_multi_source_signals')
    time.sleep(np.random.uniform(0.020, 0.045))
    tracer.end_span(span)
    
    # Component 3: ML Inference
    span = tracer.start_span('ML Inference', f'predict_{scenario_type}')
    if scenario_type == 'REROUTE':
        time.sleep(np.random.uniform(0.080, 0.120))
    elif scenario_type == 'HELICOPTER':
        time.sleep(np.random.uniform(0.050, 0.080))
    elif scenario_type == 'REDUCE_SPEED':
        time.sleep(np.random.uniform(0.060, 0.090))
    elif scenario_type == 'EMERGENCY_STOP':
        time.sleep(np.random.uniform(0.025, 0.045))
    tracer.end_span(span)
    
    # Component 4: Decision Logic
    span = tracer.start_span('Decision Engine', 'process_decision_logic')
    time.sleep(np.random.uniform(0.010, 0.025))
    tracer.end_span(span)
    
    # Component 5: Recommendations
    span = tracer.start_span('Recommendation System', 'generate_alternatives')
    if scenario_type in ['REROUTE', 'EMERGENCY_STOP']:
        time.sleep(np.random.uniform(0.040, 0.070))
    else:
        time.sleep(np.random.uniform(0.015, 0.030))
    tracer.end_span(span)
    
    # Component 6: Response
    span = tracer.start_span('Response Handler', 'serialize_decision')
    time.sleep(np.random.uniform(0.005, 0.012))
    tracer.end_span(span)
    
    return tracer


# Load test configuration
scenarios_config = [
    ('REROUTE_TO_ALTERNATE_HARBOR', 'REROUTE'),
    ('DEPLOY_HELICOPTER_RESCUE', 'HELICOPTER'),
    ('REDUCE_SPEED_GRADUAL', 'REDUCE_SPEED'),
    ('EMERGENCY_STOP', 'EMERGENCY_STOP')
]

n_requests = 100

print("=" * 80)
print(f"LOAD TEST: {n_requests} CONCURRENT REQUESTS PER SCENARIO")
print("=" * 80)

all_scenario_results = []

for scenario_name, scenario_type in scenarios_config:
    print(f"\n📈 Running load test for: {scenario_name}")
    print(f"   Executing {n_requests} concurrent requests...")
    
    start_time = time.perf_counter()
    
    # Execute requests concurrently
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        futures = [
            executor.submit(simulate_decision_with_tracing, scenario_name, scenario_type)
            for _ in range(n_requests)
        ]
        tracers = [future.result() for future in concurrent.futures.as_completed(futures)]
    
    end_time = time.perf_counter()
    total_test_duration = (end_time - start_time) * 1000
    
    # Extract latencies
    latencies = [tracer.get_total_latency() for tracer in tracers]
    
    # Calculate percentiles
    p50 = np.percentile(latencies, 50)
    p95 = np.percentile(latencies, 95)
    p99 = np.percentile(latencies, 99)
    p100 = np.max(latencies)
    mean_latency = np.mean(latencies)
    min_latency = np.min(latencies)
    
    # Component-level analysis
    component_latencies = {
        'Data Ingestion': [],
        'Signal Fusion': [],
        'ML Inference': [],
        'Decision Engine': [],
        'Recommendation System': [],
        'Response Handler': []
    }
    
    for tracer in tracers:
        comp_lat = tracer.get_component_latencies()
        for component, latency in comp_lat.items():
            if component in component_latencies:
                component_latencies[component].append(latency)
    
    # Calculate component percentiles
    component_p95 = {}
    for component, latencies_list in component_latencies.items():
        component_p95[component] = np.percentile(latencies_list, 95) if latencies_list else 0
    
    # Identify slowest component
    slowest_component = max(component_p95.items(), key=lambda x: x[1])
    
    print(f"   ✓ Completed in {total_test_duration:.2f}ms")
    print(f"\n   Latency Statistics:")
    print(f"      Min:  {min_latency:.2f}ms")
    print(f"      Mean: {mean_latency:.2f}ms")
    print(f"      P50:  {p50:.2f}ms")
    print(f"      P95:  {p95:.2f}ms {'✓ <500ms' if p95 < 500 else '❌ >500ms'}")
    print(f"      P99:  {p99:.2f}ms")
    print(f"      Max:  {p100:.2f}ms")
    print(f"\n   Slowest Component (P95): {slowest_component[0]} ({slowest_component[1]:.2f}ms)")
    
    all_scenario_results.append({
        'Scenario': scenario_name,
        'Type': scenario_type,
        'Requests': n_requests,
        'Min (ms)': round(min_latency, 2),
        'Mean (ms)': round(mean_latency, 2),
        'P50 (ms)': round(p50, 2),
        'P95 (ms)': round(p95, 2),
        'P99 (ms)': round(p99, 2),
        'Max (ms)': round(p100, 2),
        'P95 < 500ms': '✓' if p95 < 500 else '❌',
        'Slowest Component': slowest_component[0],
        'Slowest P95 (ms)': round(slowest_component[1], 2),
        'Component Breakdown': component_p95,
        'Latencies': latencies
    })

# Create summary DataFrame
load_test_summary = pd.DataFrame([{
    'Scenario': r['Scenario'],
    'Type': r['Type'],
    'Requests': r['Requests'],
    'P50 (ms)': r['P50 (ms)'],
    'P95 (ms)': r['P95 (ms)'],
    'P99 (ms)': r['Max (ms)'],
    'P95 < 500ms': r['P95 < 500ms'],
    'Slowest Component': r['Slowest Component']
} for r in all_scenario_results])

print("\n\n" + "=" * 80)
print("LOAD TEST SUMMARY - ALL SCENARIOS")
print("=" * 80)
print(load_test_summary.to_string(index=False))

# Check success criteria
success_criteria_met = all(r['P95 (ms)'] < 500 for r in all_scenario_results)
print(f"\n{'✓' if success_criteria_met else '❌'} Success Criteria: All scenarios P95 < 500ms: {success_criteria_met}")

print(f"\n📊 Total requests processed: {n_requests * len(scenarios_config)}")
print(f"✓ Distributed tracing implemented with 6 components")
print(f"✓ Percentile analysis completed (P50, P95, P99)")
print(f"✓ Component-level bottleneck analysis completed")
