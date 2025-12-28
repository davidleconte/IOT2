import pandas as pd
import numpy as np
import time
from datetime import datetime
import json
from typing import Dict, List, Tuple

# Distributed tracing instrumentation for decision engine
# Measures end-to-end latency with component-level breakdown

class DistributedTracer:
    """Distributed tracing for autonomous decision scenarios"""
    
    def __init__(self, scenario_name: str):
        self.scenario_name = scenario_name
        self.trace_id = f"{scenario_name}_{int(time.time() * 1000000)}"
        self.spans = []
        self.start_time = None
        self.end_time = None
    
    def start_span(self, component_name: str, operation: str):
        """Start a new span for a component"""
        span = {
            'span_id': f"{len(self.spans) + 1}",
            'component': component_name,
            'operation': operation,
            'start_time': time.perf_counter(),
            'end_time': None,
            'duration_ms': None
        }
        self.spans.append(span)
        return span
    
    def end_span(self, span: dict):
        """End a span and calculate duration"""
        span['end_time'] = time.perf_counter()
        span['duration_ms'] = (span['end_time'] - span['start_time']) * 1000
        return span
    
    def get_total_latency(self) -> float:
        """Calculate total end-to-end latency"""
        if not self.spans:
            return 0.0
        return sum(span['duration_ms'] for span in self.spans)
    
    def get_breakdown(self) -> pd.DataFrame:
        """Get component-level latency breakdown"""
        breakdown_data = []
        total = self.get_total_latency()
        
        for span in self.spans:
            breakdown_data.append({
                'Component': span['component'],
                'Operation': span['operation'],
                'Latency (ms)': round(span['duration_ms'], 2),
                'Percentage': round((span['duration_ms'] / total) * 100, 1) if total > 0 else 0
            })
        
        return pd.DataFrame(breakdown_data)


# Simulate realistic component latencies for each scenario
def simulate_decision_with_tracing(scenario_name: str, scenario_type: str) -> Tuple[Dict, DistributedTracer]:
    """Execute a decision scenario with full distributed tracing"""
    
    tracer = DistributedTracer(scenario_name)
    
    # Component 1: Data Ingestion (OpenSearch + Cassandra)
    span = tracer.start_span('Data Ingestion', 'fetch_realtime_telemetry')
    time.sleep(np.random.uniform(0.015, 0.035))  # 15-35ms realistic query latency
    tracer.end_span(span)
    
    # Component 2: Signal Fusion Engine
    span = tracer.start_span('Signal Fusion', 'fuse_multi_source_signals')
    time.sleep(np.random.uniform(0.020, 0.045))  # 20-45ms for fusion logic
    tracer.end_span(span)
    
    # Component 3: ML Model Inference
    span = tracer.start_span('ML Inference', f'predict_{scenario_type}')
    if scenario_type == 'REROUTE':
        time.sleep(np.random.uniform(0.080, 0.120))  # 80-120ms for congestion model
    elif scenario_type == 'HELICOPTER':
        time.sleep(np.random.uniform(0.050, 0.080))  # 50-80ms for emergency detection
    elif scenario_type == 'REDUCE_SPEED':
        time.sleep(np.random.uniform(0.060, 0.090))  # 60-90ms for fuel optimization
    elif scenario_type == 'EMERGENCY_STOP':
        time.sleep(np.random.uniform(0.025, 0.045))  # 25-45ms for collision detection (critical priority)
    tracer.end_span(span)
    
    # Component 4: Decision Logic Processing
    span = tracer.start_span('Decision Engine', 'process_decision_logic')
    time.sleep(np.random.uniform(0.010, 0.025))  # 10-25ms for decision rules
    tracer.end_span(span)
    
    # Component 5: Recommendation Generation
    span = tracer.start_span('Recommendation System', 'generate_alternatives')
    if scenario_type in ['REROUTE', 'EMERGENCY_STOP']:
        time.sleep(np.random.uniform(0.040, 0.070))  # 40-70ms for geospatial queries
    else:
        time.sleep(np.random.uniform(0.015, 0.030))  # 15-30ms for simpler recommendations
    tracer.end_span(span)
    
    # Component 6: Response Serialization
    span = tracer.start_span('Response Handler', 'serialize_decision')
    time.sleep(np.random.uniform(0.005, 0.012))  # 5-12ms for JSON serialization
    tracer.end_span(span)
    
    decision = {
        'scenario': scenario_name,
        'type': scenario_type,
        'trace_id': tracer.trace_id,
        'total_latency_ms': round(tracer.get_total_latency(), 2),
        'timestamp': datetime.now().isoformat()
    }
    
    return decision, tracer


print("=" * 80)
print("DISTRIBUTED TRACING INSTRUMENTATION - AUTONOMOUS DECISION SCENARIOS")
print("=" * 80)

# Test scenarios configuration
scenarios = [
    ('REROUTE_TO_ALTERNATE_HARBOR', 'REROUTE'),
    ('DEPLOY_HELICOPTER_RESCUE', 'HELICOPTER'),
    ('REDUCE_SPEED_GRADUAL', 'REDUCE_SPEED'),
    ('EMERGENCY_STOP', 'EMERGENCY_STOP')
]

# Single execution test
print("\n📊 Single Execution Latency Breakdown:\n")

single_exec_results = []
for scenario_name, scenario_type in scenarios:
    decision, tracer = simulate_decision_with_tracing(scenario_name, scenario_type)
    
    print(f"\n{'='*80}")
    print(f"Scenario: {scenario_name}")
    print(f"{'='*80}")
    print(f"Total Latency: {decision['total_latency_ms']:.2f}ms")
    print(f"Trace ID: {tracer.trace_id}")
    print("\nComponent Breakdown:")
    breakdown = tracer.get_breakdown()
    print(breakdown.to_string(index=False))
    
    single_exec_results.append({
        'scenario': scenario_name,
        'tracer': tracer,
        'decision': decision
    })

print("\n\n" + "=" * 80)
print("LOAD TEST: 100 CONCURRENT DECISION REQUESTS")
print("=" * 80)
print("\nSimulating 100 concurrent requests per scenario...")
print("Measuring P50, P95, P99 latencies...\n")
