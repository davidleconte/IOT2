import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle

# Zerve design system colors
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

# Define the three timestamp format problems
timestamp_formats = pd.DataFrame({
    'Source System': ['MongoDB', 'PostgreSQL', 'Cassandra'],
    'Format Type': ['ISODate', 'Unix Epoch (ms)', 'ISO8601 String'],
    'Example': [
        'ISODate("2024-01-15T14:30:00.000Z")',
        '1705329000000',
        '2024-01-15T14:30:00.000+00:00'
    ],
    'Current Issue': [
        'Wrapped in ISODate(), requires parsing',
        'Milliseconds since epoch, numeric type',
        'ISO string with timezone, text type'
    ],
    'JOIN Impact': [
        'Type mismatch in Presto',
        'Cannot join with timestamp types',
        'String comparison fails'
    ]
})

print("=" * 80)
print("TIMESTAMP NORMALIZATION CHALLENGE")
print("=" * 80)
print(timestamp_formats.to_string(index=False))
print()

# Define the unified normalization standard
normalization_standard = pd.DataFrame({
    'Property': [
        'Standard Format',
        'Data Type',
        'Timezone',
        'Precision',
        'Serialization',
        'Presto Type'
    ],
    'Specification': [
        'ISO 8601: YYYY-MM-DDTHH:mm:ss.SSSZ',
        'TIMESTAMP (millisecond precision)',
        'UTC (all timestamps normalized to UTC)',
        'Milliseconds (3 decimal places)',
        'AVRO: logical type "timestamp-millis"',
        'TIMESTAMP(3) WITH TIME ZONE'
    ],
    'Rationale': [
        'Universal standard, human-readable, sortable',
        'Proper temporal type for JOIN operations',
        'Eliminates timezone ambiguity',
        'Adequate for telemetry, matches source precision',
        'Efficient binary format, schema evolution support',
        'Native Presto type for timestamp operations'
    ]
})

print("UNIFIED NORMALIZATION STANDARD")
print("=" * 80)
print(normalization_standard.to_string(index=False))
print()

# Processing layer architecture
processing_architecture = {
    'Layer': 'Stream Processing',
    'Technology': 'Pulsar Functions',
    'Components': [
        {
            'name': 'Timestamp Normalizer Function',
            'language': 'Python',
            'deployment': 'Pulsar native function',
            'parallelism': 'Auto-scaled (matches topic partitions)'
        },
        {
            'name': 'Input Topics',
            'topics': [
                'persistent://fleet/cdc/mongodb-events',
                'persistent://fleet/cdc/postgres-events', 
                'persistent://fleet/cdc/cassandra-events'
            ]
        },
        {
            'name': 'Output Topic',
            'topic': 'persistent://fleet/normalized/telemetry-unified',
            'schema': 'Avro with timestamp-millis logical type'
        }
    ]
}

arch_df = pd.DataFrame([
    ['Input Layer', 'CDC Sources', 'MongoDB, PostgreSQL, Cassandra', '3 heterogeneous formats'],
    ['Processing Layer', 'Pulsar Function', 'Timestamp Normalizer (Python)', 'Auto-scaled, stateless'],
    ['Transformation', 'Format Detection', 'Detect source format via pattern matching', '<1ms overhead'],
    ['Transformation', 'Parse & Convert', 'Convert to UTC timestamp(3)', '<2ms overhead'],
    ['Transformation', 'Validation', 'Check range, timezone, precision', '<1ms overhead'],
    ['Output Layer', 'Normalized Topic', 'Unified Avro schema', 'timestamp-millis logical type'],
    ['Sink Layer', 'Iceberg Tables', 'TIMESTAMP(3) WITH TIME ZONE columns', 'Presto-compatible']
])
arch_df.columns = ['Layer', 'Component', 'Description', 'Performance']

print("STREAM PROCESSING ARCHITECTURE")
print("=" * 80)
print(arch_df.to_string(index=False))
print()

# Performance requirements
perf_requirements = pd.DataFrame({
    'Metric': [
        'Processing Latency',
        'Throughput',
        'CPU Overhead',
        'Memory Footprint',
        'Error Rate',
        'Backpressure Handling'
    ],
    'Target': [
        '<5ms per message',
        '50,000 msg/sec (sustained)',
        '<3% additional CPU',
        '<50MB per function instance',
        '<0.01% (invalid timestamps)',
        'Graceful degradation with DLQ'
    ],
    'Measurement': [
        'P99 latency via Pulsar metrics',
        'Messages processed/second',
        'CPU usage delta vs passthrough',
        'JVM/Python process memory',
        'Validation failures / total messages',
        'Consumer lag + DLQ message count'
    ]
})

print("PERFORMANCE REQUIREMENTS (Success Criteria)")
print("=" * 80)
print(perf_requirements.to_string(index=False))
print()
