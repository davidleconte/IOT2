import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

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

# CDC Lag Monitoring Dashboard Configuration
monitoring_config = pd.DataFrame({
    'Dashboard': [
        'CDC Agent Health',
        'Pulsar Topic Metrics',
        'Consumer Lag',
        'Iceberg Sink Performance',
        'Data Quality Checks',
        'End-to-End Pipeline'
    ],
    'Key_Metrics': [
        'Agent uptime, commitlog read rate, mutations sent, errors',
        'Producer count, msg rate in/out, backlog size, storage size',
        'Consumer lag (time), backlog messages, catchup rate',
        'Write throughput, latency P95/P99, failed writes, retries',
        'Row count validation, schema errors, DLQ messages',
        'End-to-end latency, data freshness, sync status'
    ],
    'Data_Source': [
        'CDC agent Prometheus endpoint (port 9091)',
        'Pulsar Admin API + Prometheus',
        'Pulsar subscription stats',
        'Connector Prometheus (port 8000)',
        'Iceberg metadata + validation queries',
        'Combined metrics from all sources'
    ],
    'Refresh_Interval': [
        '15 seconds',
        '30 seconds',
        '10 seconds (critical)',
        '15 seconds',
        '5 minutes',
        '1 minute'
    ],
    'Alert_Conditions': [
        'Agent down, error rate > 1%, lag > 5s',
        'No producers, backlog > 100k, rate drop > 50%',
        'Lag > 30 seconds, catchup rate < 0',
        'Throughput < 1k/sec, P95 > 10s, errors > 10/min',
        'Row count mismatch > 1%, schema errors > 0',
        'End-to-end latency > 60s, no data in 5 min'
    ]
})

print("=" * 100)
print("CDC MONITORING DASHBOARD CONFIGURATION")
print("=" * 100)
print(monitoring_config.to_string(index=False))

# Grafana dashboard JSON structure
grafana_dashboard = """
{
  "dashboard": {
    "title": "Cassandra CDC to Iceberg Pipeline",
    "tags": ["cdc", "cassandra", "pulsar", "iceberg"],
    "timezone": "browser",
    "panels": [
      {
        "id": 1,
        "title": "CDC Agent Status",
        "type": "stat",
        "targets": [
          {
            "expr": "up{job='cassandra-cdc-agent'}",
            "legendFormat": "{{instance}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "thresholds": {
              "steps": [
                {"value": 0, "color": "red"},
                {"value": 1, "color": "green"}
              ]
            }
          }
        }
      },
      {
        "id": 2,
        "title": "CDC Mutations Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(cdc_agent_mutations_sent_total[5m])",
            "legendFormat": "{{node}}"
          }
        ]
      },
      {
        "id": 3,
        "title": "Consumer Lag (Seconds)",
        "type": "gauge",
        "targets": [
          {
            "expr": "pulsar_subscription_back_log_ms / 1000",
            "legendFormat": "{{topic}}"
          }
        ],
        "fieldConfig": {
          "defaults": {
            "thresholds": {
              "steps": [
                {"value": 0, "color": "green"},
                {"value": 10, "color": "yellow"},
                {"value": 30, "color": "red"}
              ]
            },
            "max": 60
          }
        }
      },
      {
        "id": 4,
        "title": "Iceberg Sink Throughput",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(iceberg_sink_messages_processed_total[5m])",
            "legendFormat": "Processed"
          },
          {
            "expr": "rate(iceberg_sink_messages_failed_total[5m])",
            "legendFormat": "Failed"
          }
        ]
      },
      {
        "id": 5,
        "title": "Pulsar Topic Backlog",
        "type": "graph",
        "targets": [
          {
            "expr": "pulsar_subscription_back_log",
            "legendFormat": "{{topic}}"
          }
        ],
        "alert": {
          "conditions": [
            {
              "evaluator": {"type": "gt", "params": [100000]},
              "query": {"params": ["A", "5m", "now"]}
            }
          ]
        }
      },
      {
        "id": 6,
        "title": "Write Latency (P95)",
        "type": "graph",
        "targets": [
          {
            "expr": "histogram_quantile(0.95, rate(iceberg_sink_write_latency_seconds_bucket[5m]))",
            "legendFormat": "P95"
          },
          {
            "expr": "histogram_quantile(0.99, rate(iceberg_sink_write_latency_seconds_bucket[5m]))",
            "legendFormat": "P99"
          }
        ]
      }
    ]
  }
}
"""

print("\n" + "=" * 100)
print("GRAFANA DASHBOARD JSON TEMPLATE")
print("=" * 100)
print(grafana_dashboard)

# Validation test suite
validation_tests = pd.DataFrame({
    'Test_Name': [
        'End-to-End Latency Test',
        'Row Count Validation',
        'Schema Consistency Check',
        'Data Accuracy Test',
        'Failure Recovery Test',
        'Scale Test',
        'Zero Data Loss Test',
        'Lag Monitoring Test'
    ],
    'Test_Procedure': [
        'Insert record in Cassandra, measure time to appear in Iceberg',
        'Compare SELECT COUNT(*) between Cassandra and Iceberg per table',
        'Verify schema in Schema Registry matches Iceberg table schema',
        'Sample random records, validate field values match',
        'Kill CDC agent, verify automatic recovery and no lost messages',
        'Insert 10k records/sec for 1 hour, verify lag stays <30 sec',
        'Simulate all failure scenarios, verify Pulsar retention works',
        'Monitor consumer lag metric for 24 hours'
    ],
    'Success_Criteria': [
        'Latency < 30 seconds for 99% of records',
        'Row counts match within 1% (accounting for in-flight)',
        'All fields present, types match, compatible evolution',
        '100% match on sampled records',
        'Recovery < 30 seconds, zero messages lost',
        'Lag never exceeds 30 seconds, throughput sustained',
        'Zero data loss in all scenarios',
        'Lag stays below 30 seconds during normal operations'
    ],
    'Test_Duration': [
        '1 hour (continuous inserts)',
        '5 minutes',
        '10 minutes',
        '30 minutes (100 samples per table)',
        '1 hour',
        '1 hour',
        '4 hours (all scenarios)',
        '24 hours'
    ],
    'Automated': [
        'Yes (Python script)',
        'Yes (SQL queries)',
        'Yes (API calls)',
        'Yes (Python script)',
        'Partial (manual trigger)',
        'Yes (load generator)',
        'Partial (manual scenarios)',
        'Yes (alerting)'
    ]
})

print("\n" + "=" * 100)
print("CDC PIPELINE VALIDATION TEST SUITE")
print("=" * 100)
print(validation_tests.to_string(index=False))

# Validation script example
validation_script = """#!/usr/bin/env python3
\"\"\"
CDC Pipeline Validation Script
Tests end-to-end latency and row count consistency
\"\"\"

import time
import uuid
from datetime import datetime
from cassandra.cluster import Cluster
from pyiceberg.catalog import load_catalog
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CDCValidator:
    def __init__(self):
        # Cassandra connection
        self.cassandra_cluster = Cluster(['cassandra-node1'])
        self.cassandra_session = self.cassandra_cluster.connect('fleet_operational')
        
        # Iceberg catalog
        self.iceberg_catalog = load_catalog(
            'hive',
            **{
                'uri': 'thrift://hive-metastore:9083',
                'warehouse': 's3a://telemetry-data/warehouse'
            }
        )
    
    def test_end_to_end_latency(self, num_tests=100):
        \"\"\"Test latency from Cassandra insert to Iceberg availability\"\"\"
        latencies = []
        
        for i in range(num_tests):
            # Generate test record
            test_id = str(uuid.uuid4())
            insert_time = datetime.now()
            
            # Insert into Cassandra
            self.cassandra_session.execute(
                "INSERT INTO vehicles (vehicle_id, make, model, year, last_updated) "
                "VALUES (%s, %s, %s, %s, %s)",
                (test_id, 'Test', 'Validation', 2024, int(insert_time.timestamp() * 1000))
            )
            
            # Wait for record to appear in Iceberg
            max_wait = 60  # seconds
            found = False
            start_wait = time.time()
            
            while time.time() - start_wait < max_wait:
                iceberg_table = self.iceberg_catalog.load_table('operational.vehicles')
                scan = iceberg_table.scan(
                    row_filter=f"vehicle_id = '{test_id}'"
                ).to_arrow()
                
                if len(scan) > 0:
                    found = True
                    latency = time.time() - start_wait
                    latencies.append(latency)
                    logger.info(f"Test {i+1}: Latency = {latency:.2f}s")
                    break
                
                time.sleep(1)
            
            if not found:
                logger.error(f"Test {i+1}: Record not found after {max_wait}s")
        
        # Report results
        if latencies:
            p50 = sorted(latencies)[len(latencies)//2]
            p95 = sorted(latencies)[int(len(latencies)*0.95)]
            p99 = sorted(latencies)[int(len(latencies)*0.99)]
            
            logger.info(f"Latency P50: {p50:.2f}s, P95: {p95:.2f}s, P99: {p99:.2f}s")
            
            if p99 < 30:
                logger.info("✓ PASS: P99 latency < 30 seconds")
                return True
            else:
                logger.error("✗ FAIL: P99 latency >= 30 seconds")
                return False
    
    def test_row_count_consistency(self):
        \"\"\"Compare row counts between Cassandra and Iceberg\"\"\"
        tables = ['vehicles', 'maintenance_records', 'fleet_assignments', 'drivers', 'routes']
        
        for table in tables:
            # Count in Cassandra
            cassandra_count = self.cassandra_session.execute(
                f"SELECT COUNT(*) FROM {table}"
            ).one()[0]
            
            # Count in Iceberg
            iceberg_table = self.iceberg_catalog.load_table(f'operational.{table}')
            iceberg_count = len(iceberg_table.scan().to_arrow())
            
            # Calculate difference
            diff_pct = abs(cassandra_count - iceberg_count) / cassandra_count * 100
            
            logger.info(
                f"{table}: Cassandra={cassandra_count}, Iceberg={iceberg_count}, "
                f"Diff={diff_pct:.2f}%"
            )
            
            if diff_pct > 1:
                logger.warning(f"✗ Row count difference > 1% for {table}")
            else:
                logger.info(f"✓ Row count OK for {table}")


if __name__ == '__main__':
    validator = CDCValidator()
    
    logger.info("Starting CDC pipeline validation...")
    
    logger.info("Test 1: End-to-End Latency")
    validator.test_end_to_end_latency(num_tests=100)
    
    logger.info("Test 2: Row Count Consistency")
    validator.test_row_count_consistency()
    
    logger.info("Validation complete!")
"""

print("\n" + "=" * 100)
print("CDC VALIDATION SCRIPT (PYTHON)")
print("=" * 100)
print(validation_script)

# Create monitoring visualization
fig = plt.figure(figsize=(16, 10))
fig.patch.set_facecolor(bg_color)

# Title
fig.suptitle('CDC Monitoring Dashboard - Key Metrics & Alerts', 
             fontsize=18, color=text_primary, y=0.98, fontweight='bold')

# Create 3x3 grid for metric panels
gs = fig.add_gridspec(3, 3, hspace=0.4, wspace=0.3, 
                      left=0.08, right=0.95, top=0.93, bottom=0.08)

# Panel 1: CDC Agent Status
ax1 = fig.add_subplot(gs[0, 0])
ax1.set_facecolor(bg_color)
ax1.set_xlim(0, 10)
ax1.set_ylim(0, 10)
ax1.axis('off')
ax1.text(5, 8, 'CDC Agent Status', ha='center', fontsize=12, 
         color=text_primary, fontweight='bold')
# Simulate agent status
agent_status = FancyBboxPatch((2, 4), 6, 2, boxstyle="round,pad=0.1",
                              edgecolor=success, facecolor=success, alpha=0.3, linewidth=2)
ax1.add_patch(agent_status)
ax1.text(5, 5, 'HEALTHY', ha='center', fontsize=14, color=success, fontweight='bold')
ax1.text(5, 2.5, '3/3 Agents UP', ha='center', fontsize=9, color=text_secondary)

# Panel 2: Consumer Lag
ax2 = fig.add_subplot(gs[0, 1])
ax2.set_facecolor(bg_color)
ax2.set_xlim(0, 10)
ax2.set_ylim(0, 10)
ax2.axis('off')
ax2.text(5, 8, 'Consumer Lag', ha='center', fontsize=12, 
         color=text_primary, fontweight='bold')
# Gauge representation
_theta = np.linspace(0.75 * np.pi, 0.25 * np.pi, 100)
_x = 5 + 3 * np.cos(_theta)
_y = 4 + 3 * np.sin(_theta)
ax2.plot(_x, _y, color=text_secondary, linewidth=2)
# Needle for 12 seconds (good)
needle_angle = 0.75 * np.pi - (12/60) * 1.5 * np.pi
ax2.plot([5, 5 + 2.5 * np.cos(needle_angle)], 
         [4, 4 + 2.5 * np.sin(needle_angle)], 
         color=success, linewidth=3)
ax2.text(5, 1.5, '12 seconds', ha='center', fontsize=11, color=success, fontweight='bold')
ax2.text(5, 0.5, 'Target: <30s', ha='center', fontsize=8, color=text_secondary)

# Panel 3: Throughput
ax3 = fig.add_subplot(gs[0, 2])
ax3.set_facecolor(bg_color)
ax3.spines['top'].set_visible(False)
ax3.spines['right'].set_visible(False)
ax3.spines['left'].set_color(text_secondary)
ax3.spines['bottom'].set_color(text_secondary)
ax3.tick_params(colors=text_secondary, labelsize=8)
ax3.set_title('Message Throughput', fontsize=11, color=text_primary, pad=10)
time_points = np.arange(0, 60, 5)
throughput = 7000 + np.random.normal(0, 500, len(time_points))
ax3.plot(time_points, throughput, color=light_blue, linewidth=2)
ax3.axhline(y=5000, color=warning, linestyle='--', linewidth=1, alpha=0.5)
ax3.set_ylabel('Messages/sec', fontsize=9, color=text_secondary)
ax3.set_xlabel('Time (seconds)', fontsize=9, color=text_secondary)
ax3.grid(alpha=0.2, color=text_secondary)

# Panel 4: Backlog Size
ax4 = fig.add_subplot(gs[1, 0])
ax4.set_facecolor(bg_color)
ax4.spines['top'].set_visible(False)
ax4.spines['right'].set_visible(False)
ax4.spines['left'].set_color(text_secondary)
ax4.spines['bottom'].set_color(text_secondary)
ax4.tick_params(colors=text_secondary, labelsize=8)
ax4.set_title('Pulsar Topic Backlog', fontsize=11, color=text_primary, pad=10)
topics = ['vehicles', 'maintenance', 'assignments', 'drivers', 'routes']
backlog_values = [1200, 800, 450, 150, 600]
colors_list = [light_blue, orange, green, coral, lavender]
ax4.bar(topics, backlog_values, color=colors_list, alpha=0.8)
ax4.axhline(y=100000, color=warning, linestyle='--', linewidth=1, alpha=0.5, label='Alert threshold')
ax4.set_ylabel('Messages', fontsize=9, color=text_secondary)
ax4.tick_params(axis='x', rotation=45)
ax4.legend(fontsize=7, facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_secondary)

# Panel 5: Write Latency
ax5 = fig.add_subplot(gs[1, 1])
ax5.set_facecolor(bg_color)
ax5.spines['top'].set_visible(False)
ax5.spines['right'].set_visible(False)
ax5.spines['left'].set_color(text_secondary)
ax5.spines['bottom'].set_color(text_secondary)
ax5.tick_params(colors=text_secondary, labelsize=8)
ax5.set_title('Iceberg Write Latency', fontsize=11, color=text_primary, pad=10)
percentiles = ['P50', 'P75', 'P95', 'P99']
latency_values = [1.2, 2.5, 4.8, 7.2]
ax5.bar(percentiles, latency_values, color=[green, light_blue, orange, coral], alpha=0.8)
ax5.axhline(y=10, color=warning, linestyle='--', linewidth=1, alpha=0.5, label='Alert: >10s')
ax5.set_ylabel('Seconds', fontsize=9, color=text_secondary)
ax5.legend(fontsize=7, facecolor=bg_color, edgecolor=text_secondary, labelcolor=text_secondary)

# Panel 6: Error Rate
ax6 = fig.add_subplot(gs[1, 2])
ax6.set_facecolor(bg_color)
ax6.spines['top'].set_visible(False)
ax6.spines['right'].set_visible(False)
ax6.spines['left'].set_color(text_secondary)
ax6.spines['bottom'].set_color(text_secondary)
ax6.tick_params(colors=text_secondary, labelsize=8)
ax6.set_title('Error Rates', fontsize=11, color=text_primary, pad=10)
error_types = ['CDC Agent', 'Schema', 'Write', 'Network']
error_counts = [0, 0, 2, 1]
colors_err = [success if e == 0 else warning if e < 5 else coral for e in error_counts]
ax6.bar(error_types, error_counts, color=colors_err, alpha=0.8)
ax6.set_ylabel('Errors/min', fontsize=9, color=text_secondary)
ax6.tick_params(axis='x', rotation=45)

# Panel 7: Data Freshness
ax7 = fig.add_subplot(gs[2, 0])
ax7.set_facecolor(bg_color)
ax7.set_xlim(0, 10)
ax7.set_ylim(0, 10)
ax7.axis('off')
ax7.text(5, 8, 'Data Freshness', ha='center', fontsize=12, 
         color=text_primary, fontweight='bold')
freshness_box = FancyBboxPatch((1.5, 4), 7, 2.5, boxstyle="round,pad=0.1",
                               edgecolor=success, facecolor=bg_color, linewidth=2)
ax7.add_patch(freshness_box)
ax7.text(5, 5.5, 'Last Update: 3s ago', ha='center', fontsize=10, color=success)
ax7.text(5, 4.5, 'All tables synchronized', ha='center', fontsize=9, color=text_secondary)
ax7.text(5, 2, 'Target: <30s', ha='center', fontsize=8, color=text_secondary)

# Panel 8: Alert Status
ax8 = fig.add_subplot(gs[2, 1])
ax8.set_facecolor(bg_color)
ax8.set_xlim(0, 10)
ax8.set_ylim(0, 10)
ax8.axis('off')
ax8.text(5, 8, 'Active Alerts', ha='center', fontsize=12, 
         color=text_primary, fontweight='bold')
alert_box = FancyBboxPatch((1.5, 4), 7, 2.5, boxstyle="round,pad=0.1",
                           edgecolor=success, facecolor=bg_color, linewidth=2)
ax8.add_patch(alert_box)
ax8.text(5, 5.5, '0 Critical', ha='center', fontsize=10, color=success, fontweight='bold')
ax8.text(5, 4.5, '0 Warning', ha='center', fontsize=9, color=text_secondary)
ax8.text(5, 2, 'All systems nominal', ha='center', fontsize=8, color=success)

# Panel 9: Summary Status
ax9 = fig.add_subplot(gs[2, 2])
ax9.set_facecolor(bg_color)
ax9.set_xlim(0, 10)
ax9.set_ylim(0, 10)
ax9.axis('off')
ax9.text(5, 8, 'Pipeline Health', ha='center', fontsize=12, 
         color=text_primary, fontweight='bold')

# Traffic light status
light_y = 5.5
for status_label, status_color, status_y in [
    ('Capture', success, light_y),
    ('Transport', success, light_y - 1.2),
    ('Sink', success, light_y - 2.4)
]:
    status_circle = plt.Circle((3, status_y), 0.3, color=status_color, alpha=0.8)
    ax9.add_patch(status_circle)
    ax9.text(4, status_y, status_label, va='center', fontsize=9, color=text_primary)

ax9.text(5, 1, '✓ All components operational', ha='center', fontsize=8, color=success)

monitoring_fig = fig

print("\n" + "=" * 100)
print("CDC MONITORING DASHBOARD VISUALIZATION CREATED")
print("=" * 100)
print("Dashboard shows: Agent status, consumer lag, throughput, backlog, latency, errors, freshness, alerts")
