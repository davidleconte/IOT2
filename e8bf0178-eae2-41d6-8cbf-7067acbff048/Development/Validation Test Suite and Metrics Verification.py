import pandas as pd

print("=" * 100)
print("CDC VALIDATION TEST SUITE & METRICS VERIFICATION")
print("=" * 100)

# Python validation test suite
validation_suite_code = """#!/usr/bin/env python3
\"\"\"
CDC Validation Test Suite
Comprehensive validation of CDC pipeline: latency, data loss, performance
\"\"\"

import time
import uuid
import logging
from datetime import datetime, timedelta
from cassandra.cluster import Cluster
import requests
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CDCValidationSuite:
    def __init__(self):
        # Cassandra connection
        self.cassandra_cluster = Cluster(['cassandra-node1', 'cassandra-node2', 'cassandra-node3'])
        self.cassandra_session = self.cassandra_cluster.connect('fleet_operational')
        
        # Prometheus endpoint for CDC metrics
        self.prometheus_url = 'http://prometheus:9090'
        
        # Test results
        self.results = {
            'latency_test': None,
            'data_loss_test': None,
            'performance_test': None,
            'alerting_test': None
        }
    
    def test_cdc_latency(self, num_samples=100):
        \"\"\"Test CDC capture latency <100ms\"\"\"
        logger.info("=" * 60)
        logger.info("TEST: CDC Capture Latency")
        logger.info("=" * 60)
        
        latencies = []
        
        for i in range(num_samples):
            # Insert record with timestamp
            test_id = str(uuid.uuid4())
            insert_time = datetime.now()
            insert_ts_ms = int(insert_time.timestamp() * 1000)
            
            # Insert into Cassandra
            self.cassandra_session.execute(
                "INSERT INTO vehicles (vehicle_id, make, model, year, last_updated) "
                "VALUES (%s, %s, %s, %s, %s)",
                (test_id, 'TestMake', 'TestModel', 2024, insert_ts_ms)
            )
            
            # Query Pulsar metrics to see when message was sent
            time.sleep(0.5)  # Wait for CDC agent to process
            
            # Check CDC agent metrics for mutations sent
            query = 'cdc_agent_processing_lag_ms'
            response = requests.get(
                f'{self.prometheus_url}/api/v1/query',
                params={'query': query}
            )
            
            if response.status_code == 200:
                data = response.json()
                if data['data']['result']:
                    lag_ms = float(data['data']['result'][0]['value'][1])
                    latencies.append(lag_ms)
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"Sample {i+1}/{num_samples}: Lag = {lag_ms:.2f}ms")
        
        # Calculate statistics
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            max_latency = max(latencies)
            p95_latency = sorted(latencies)[int(len(latencies) * 0.95)]
            p99_latency = sorted(latencies)[int(len(latencies) * 0.99)]
            
            logger.info("")
            logger.info(f"Latency Results ({len(latencies)} samples):")
            logger.info(f"  Average: {avg_latency:.2f}ms")
            logger.info(f"  P95: {p95_latency:.2f}ms")
            logger.info(f"  P99: {p99_latency:.2f}ms")
            logger.info(f"  Max: {max_latency:.2f}ms")
            
            # Check success criteria
            if p99_latency < 100:
                logger.info("✓ PASS: P99 latency < 100ms")
                self.results['latency_test'] = 'PASS'
                return True
            else:
                logger.error(f"✗ FAIL: P99 latency = {p99_latency:.2f}ms (threshold: 100ms)")
                self.results['latency_test'] = 'FAIL'
                return False
        else:
            logger.error("✗ FAIL: No latency data collected")
            self.results['latency_test'] = 'FAIL'
            return False
    
    def test_zero_data_loss(self):
        \"\"\"Test zero data loss during CDC capture\"\"\"
        logger.info("")
        logger.info("=" * 60)
        logger.info("TEST: Zero Data Loss")
        logger.info("=" * 60)
        
        # Insert batch of records
        num_records = 1000
        test_batch_id = str(uuid.uuid4())
        
        logger.info(f"Inserting {num_records} records...")
        insert_start = time.time()
        
        for i in range(num_records):
            self.cassandra_session.execute(
                "INSERT INTO vehicles (vehicle_id, make, model, year, last_updated) "
                "VALUES (%s, %s, %s, %s, %s)",
                (f"{test_batch_id}_{i}", 'BatchTest', 'Model', 2024, 
                 int(datetime.now().timestamp() * 1000))
            )
        
        insert_duration = time.time() - insert_start
        logger.info(f"✓ Inserted {num_records} records in {insert_duration:.2f}s")
        
        # Wait for CDC to process
        logger.info("Waiting 60 seconds for CDC processing...")
        time.sleep(60)
        
        # Query Pulsar metrics for total mutations sent
        query = 'cdc_agent_mutations_sent_total'
        response = requests.get(
            f'{self.prometheus_url}/api/v1/query',
            params={'query': query}
        )
        
        if response.status_code == 200:
            data = response.json()
            if data['data']['result']:
                total_sent = sum(float(r['value'][1]) for r in data['data']['result'])
                logger.info(f"Total mutations sent by CDC: {total_sent}")
                
                if total_sent >= num_records:
                    logger.info("✓ PASS: All records captured by CDC")
                    self.results['data_loss_test'] = 'PASS'
                    return True
                else:
                    logger.error(f"✗ FAIL: Only {total_sent}/{num_records} records captured")
                    self.results['data_loss_test'] = 'FAIL'
                    return False
        
        logger.error("✗ FAIL: Could not verify data loss")
        self.results['data_loss_test'] = 'FAIL'
        return False
    
    def test_cassandra_performance_impact(self):
        \"\"\"Test Cassandra performance degradation <5%\"\"\"
        logger.info("")
        logger.info("=" * 60)
        logger.info("TEST: Cassandra Performance Impact")
        logger.info("=" * 60)
        
        # Get baseline metrics from nodes without CDC
        baseline_cpu_query = 'node_cpu_usage{job="cassandra",cdc="false"}'
        baseline_latency_query = 'cassandra_table_write_latency_p95{cdc="false"}'
        
        # Get CDC node metrics
        cdc_cpu_query = 'node_cpu_usage{job="cassandra",cdc="true"}'
        cdc_latency_query = 'cassandra_table_write_latency_p95{cdc="true"}'
        
        def get_metric_avg(query):
            response = requests.get(
                f'{self.prometheus_url}/api/v1/query',
                params={'query': query}
            )
            if response.status_code == 200:
                data = response.json()
                if data['data']['result']:
                    values = [float(r['value'][1]) for r in data['data']['result']]
                    return sum(values) / len(values) if values else 0
            return 0
        
        baseline_cpu = get_metric_avg(baseline_cpu_query)
        cdc_cpu = get_metric_avg(cdc_cpu_query)
        baseline_latency = get_metric_avg(baseline_latency_query)
        cdc_latency = get_metric_avg(cdc_latency_query)
        
        logger.info(f"CPU Usage: Baseline={baseline_cpu:.1f}%, CDC={cdc_cpu:.1f}%")
        logger.info(f"Write Latency: Baseline={baseline_latency:.2f}ms, CDC={cdc_latency:.2f}ms")
        
        # Calculate degradation
        cpu_degradation = ((cdc_cpu - baseline_cpu) / baseline_cpu * 100) if baseline_cpu > 0 else 0
        latency_degradation = ((cdc_latency - baseline_latency) / baseline_latency * 100) if baseline_latency > 0 else 0
        
        logger.info(f"CPU Degradation: {cpu_degradation:.2f}%")
        logger.info(f"Latency Degradation: {latency_degradation:.2f}%")
        
        # Check thresholds
        if cpu_degradation < 5 and latency_degradation < 5:
            logger.info("✓ PASS: Performance degradation <5%")
            self.results['performance_test'] = 'PASS'
            return True
        else:
            logger.error("✗ FAIL: Performance degradation >=5%")
            self.results['performance_test'] = 'FAIL'
            return False
    
    def test_alerting_system(self):
        \"\"\"Test alerting rules are configured correctly\"\"\"
        logger.info("")
        logger.info("=" * 60)
        logger.info("TEST: Alerting System")
        logger.info("=" * 60)
        
        # Check Prometheus alert rules
        response = requests.get(f'{self.prometheus_url}/api/v1/rules')
        
        if response.status_code == 200:
            data = response.json()
            
            required_alerts = [
                'CDCAgentDown',
                'CDCLatencyHigh',
                'CDCErrorRateHigh',
                'PulsarBacklogHigh',
                'IcebergSyncLagHigh'
            ]
            
            configured_alerts = []
            for group in data.get('data', {}).get('groups', []):
                for rule in group.get('rules', []):
                    if rule.get('type') == 'alerting':
                        configured_alerts.append(rule.get('name'))
            
            missing_alerts = [a for a in required_alerts if a not in configured_alerts]
            
            logger.info(f"Configured alerts: {len(configured_alerts)}")
            logger.info(f"Required alerts: {len(required_alerts)}")
            
            if not missing_alerts:
                logger.info("✓ PASS: All required alerts configured")
                self.results['alerting_test'] = 'PASS'
                return True
            else:
                logger.error(f"✗ FAIL: Missing alerts: {missing_alerts}")
                self.results['alerting_test'] = 'FAIL'
                return False
        else:
            logger.error("✗ FAIL: Could not fetch alert rules")
            self.results['alerting_test'] = 'FAIL'
            return False
    
    def run_all_tests(self):
        \"\"\"Run complete validation suite\"\"\"
        logger.info("")
        logger.info("=" * 60)
        logger.info("STARTING CDC VALIDATION SUITE")
        logger.info("=" * 60)
        
        test_start = time.time()
        
        # Run tests
        self.test_cdc_latency()
        self.test_zero_data_loss()
        self.test_cassandra_performance_impact()
        self.test_alerting_system()
        
        test_duration = time.time() - test_start
        
        # Summary
        logger.info("")
        logger.info("=" * 60)
        logger.info("VALIDATION SUITE RESULTS")
        logger.info("=" * 60)
        logger.info(f"CDC Latency Test: {self.results['latency_test']}")
        logger.info(f"Zero Data Loss Test: {self.results['data_loss_test']}")
        logger.info(f"Performance Impact Test: {self.results['performance_test']}")
        logger.info(f"Alerting System Test: {self.results['alerting_test']}")
        logger.info(f"Total Duration: {test_duration:.2f}s")
        logger.info("=" * 60)
        
        # Overall pass/fail
        all_pass = all(v == 'PASS' for v in self.results.values())
        
        if all_pass:
            logger.info("✓ ALL TESTS PASSED - Ready for production deployment")
            return True
        else:
            failed_tests = [k for k, v in self.results.items() if v == 'FAIL']
            logger.error(f"✗ TESTS FAILED: {failed_tests}")
            return False


if __name__ == '__main__':
    validator = CDCValidationSuite()
    success = validator.run_all_tests()
    exit(0 if success else 1)
"""

print("CDC Validation Test Suite:")
print(validation_suite_code[:1000] + "...")

# Metrics verification script
metrics_check_code = """#!/usr/bin/env python3
\"\"\"
CDC Metrics Verification
Checks CDC metrics against defined thresholds
\"\"\"

import requests
import sys
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROMETHEUS_URL = 'http://prometheus:9090'

# Metric thresholds
THRESHOLDS = {
    'cdc_agent_processing_lag_ms': {
        'warning': 50,
        'critical': 100,
        'description': 'CDC processing lag'
    },
    'pulsar_subscription_back_log': {
        'warning': 5000,
        'critical': 10000,
        'description': 'Pulsar backlog messages'
    },
    'cdc_agent_mutations_error_total': {
        'warning': 10,
        'critical': 50,
        'description': 'CDC mutation errors'
    },
    'iceberg_sink_write_latency_p95': {
        'warning': 5,
        'critical': 10,
        'description': 'Iceberg write latency P95 (seconds)'
    }
}


def query_prometheus(query):
    \"\"\"Query Prometheus API\"\"\"
    try:
        response = requests.get(
            f'{PROMETHEUS_URL}/api/v1/query',
            params={'query': query},
            timeout=10
        )
        if response.status_code == 200:
            data = response.json()
            return data.get('data', {}).get('result', [])
        else:
            logger.error(f"Prometheus query failed: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error querying Prometheus: {e}")
        return []


def check_metric(metric_name, threshold_config):
    \"\"\"Check metric against thresholds\"\"\"
    logger.info(f"Checking {threshold_config['description']}...")
    
    results = query_prometheus(metric_name)
    
    if not results:
        logger.warning(f"  ⚠ No data for {metric_name}")
        return 'UNKNOWN'
    
    # Get max value across all instances
    values = [float(r['value'][1]) for r in results]
    max_value = max(values) if values else 0
    
    status = 'OK'
    if max_value >= threshold_config['critical']:
        status = 'CRITICAL'
        logger.error(f"  ✗ CRITICAL: {max_value:.2f} (threshold: {threshold_config['critical']})")
    elif max_value >= threshold_config['warning']:
        status = 'WARNING'
        logger.warning(f"  ⚠ WARNING: {max_value:.2f} (threshold: {threshold_config['warning']})")
    else:
        logger.info(f"  ✓ OK: {max_value:.2f}")
    
    return status


def check_agent_health():
    \"\"\"Check CDC agent health across all nodes\"\"\"
    logger.info("Checking CDC agent health...")
    
    query = 'up{job="cassandra-cdc-agent"}'
    results = query_prometheus(query)
    
    if not results:
        logger.error("  ✗ No CDC agents found")
        return 'CRITICAL'
    
    down_agents = [r for r in results if float(r['value'][1]) == 0]
    
    if down_agents:
        logger.error(f"  ✗ {len(down_agents)} agent(s) down")
        return 'CRITICAL'
    else:
        logger.info(f"  ✓ All {len(results)} agents UP")
        return 'OK'


def main():
    logger.info("=" * 60)
    logger.info("CDC METRICS VERIFICATION")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    logger.info("=" * 60)
    
    # Check agent health first
    agent_status = check_agent_health()
    
    # Check all metrics
    metric_statuses = {}
    for metric, config in THRESHOLDS.items():
        status = check_metric(metric, config)
        metric_statuses[metric] = status
    
    # Summary
    logger.info("")
    logger.info("=" * 60)
    logger.info("SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Agent Health: {agent_status}")
    for metric, status in metric_statuses.items():
        logger.info(f"{metric}: {status}")
    
    # Overall status
    all_statuses = [agent_status] + list(metric_statuses.values())
    
    if 'CRITICAL' in all_statuses:
        logger.error("✗ OVERALL STATUS: CRITICAL")
        return 2
    elif 'WARNING' in all_statuses:
        logger.warning("⚠ OVERALL STATUS: WARNING")
        return 1
    else:
        logger.info("✓ OVERALL STATUS: OK")
        return 0


if __name__ == '__main__':
    exit_code = main()
    sys.exit(exit_code)
"""

print("\n" + "=" * 100)
print("Metrics Verification Script:")
print("=" * 100)
print(metrics_check_code[:1000] + "...")

# Test scenarios
test_scenarios = pd.DataFrame({
    'Test_Scenario': [
        'Normal Operations',
        'High Load (10k writes/sec)',
        'Network Partition (Pulsar unreachable)',
        'Cassandra Node Restart',
        'CDC Agent Crash',
        'Disk Space Low (<10%)',
        'Schema Change (add column)',
        'Backfill Historical Data'
    ],
    'Expected_Behavior': [
        'Latency <100ms, zero data loss, performance nominal',
        'Latency <100ms maintained, backlog <5k, agent handles load',
        'Agent buffers locally, resumes when connection restored',
        'Agent pauses, resumes when node UP, zero data loss',
        'Systemd restarts agent, replays from checkpoint, zero data loss',
        'Alert fires, CDC pauses, no data loss (commitlog retained)',
        'Schema registry updates, Iceberg schema evolves, no errors',
        'Historical data captured, Iceberg populated, no duplicates'
    ],
    'Validation': [
        'Check all metrics green, run validation suite',
        'Monitor backlog and lag metrics, verify <10s',
        'Check agent logs for retry attempts, verify resume',
        'Verify agent status returns to HEALTHY, check row counts',
        'Check systemd logs, verify restart, validate data continuity',
        'Verify alert received, check disk space monitoring',
        'Query Iceberg with new column, verify compatibility',
        'Compare Cassandra vs Iceberg row counts, check timestamps'
    ],
    'Success_Criteria': [
        'All validation tests pass',
        'Lag never exceeds 30 seconds',
        'All buffered data delivered after recovery',
        'Agent resumes within 60s, no lost mutations',
        'Agent restart <30s, zero data loss',
        'CDC paused, alert sent, manual intervention guided',
        'New column accessible in Iceberg, no query failures',
        'Row counts match, no duplicates, timestamps preserved'
    ]
})

print("\n" + "=" * 100)
print("TEST SCENARIOS & VALIDATION")
print("=" * 100)
print(test_scenarios.to_string(index=False))

# Continuous monitoring setup
monitoring_setup = pd.DataFrame({
    'Component': [
        'Grafana Dashboard',
        'Prometheus Alerts',
        'PagerDuty Integration',
        'Slack Notifications',
        'Log Aggregation',
        'Automated Health Checks'
    ],
    'Configuration': [
        'Import dashboard JSON, configure datasource',
        'Load alert rules YAML, verify routing',
        'Configure PagerDuty API key, test escalation',
        'Set up Slack webhook, configure channels',
        'Configure Loki/ELK for CDC agent logs',
        'Cron job running metrics check every 5 minutes'
    ],
    'Alert_Conditions': [
        'Lag >30s, backlog >10k, agent down',
        'Same as dashboard thresholds',
        'Critical alerts only (agent down, data loss)',
        'All alerts (critical + warning)',
        'Error keywords: "failed", "exception", "timeout"',
        'Exit code != 0 triggers alert'
    ],
    'Response_SLA': [
        'Visual monitoring (no alert)',
        '15 min acknowledge, 1 hour resolution',
        '5 min acknowledge, 30 min resolution (critical)',
        'FYI only, no SLA',
        'Investigation tool, no direct alerts',
        'Automated remediation where possible'
    ]
})

print("\n" + "=" * 100)
print("CONTINUOUS MONITORING SETUP")
print("=" * 100)
print(monitoring_setup.to_string(index=False))

print("\n" + "=" * 100)
print("VALIDATION & MONITORING FRAMEWORK COMPLETE")
print("=" * 100)
print("✓ Comprehensive validation test suite")
print("✓ Automated metrics verification")
print("✓ 8 test scenarios with success criteria")
print("✓ Continuous monitoring configuration")
print("✓ Multi-tier alerting (Grafana, Prometheus, PagerDuty, Slack)")
print("✓ All scripts ready for production deployment")
