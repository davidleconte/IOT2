import pandas as pd

print("=" * 100)
print("CDC PRODUCTION DEPLOYMENT - AUTOMATION SCRIPTS")
print("=" * 100)

# Node preparation script
node_preparation_script = """#!/bin/bash
# cassandra_cdc_node_prep.sh
# Prepares Cassandra node for CDC agent deployment

set -euo pipefail

NODE_NAME=${1:-$(hostname)}
CDC_RAW_DIR="/var/lib/cassandra/cdc_raw"
CASSANDRA_YAML="/etc/cassandra/cassandra.yaml"

echo "=========================================="
echo "Preparing Cassandra node: $NODE_NAME"
echo "=========================================="

# 1. Check prerequisites
echo "Step 1: Checking prerequisites..."

# Check if running as root
if [[ $EUID -ne 0 ]]; then
   echo "Error: This script must be run as root"
   exit 1
fi

# Check Cassandra is running
if ! systemctl is-active --quiet cassandra; then
    echo "Error: Cassandra service is not running"
    exit 1
fi

# Check disk space (need at least 20GB free)
AVAILABLE_GB=$(df /var/lib/cassandra | awk 'NR==2 {print int($4/1024/1024)}')
if [ "$AVAILABLE_GB" -lt 20 ]; then
    echo "Error: Insufficient disk space. Available: ${AVAILABLE_GB}GB, Required: 20GB"
    exit 1
fi
echo "✓ Disk space OK: ${AVAILABLE_GB}GB available"

# 2. Create CDC raw directory
echo "Step 2: Creating CDC raw directory..."
mkdir -p "$CDC_RAW_DIR"
chown cassandra:cassandra "$CDC_RAW_DIR"
chmod 750 "$CDC_RAW_DIR"
echo "✓ CDC raw directory created: $CDC_RAW_DIR"

# 3. Backup cassandra.yaml
echo "Step 3: Backing up cassandra.yaml..."
cp "$CASSANDRA_YAML" "${CASSANDRA_YAML}.backup.$(date +%Y%m%d_%H%M%S)"
echo "✓ Backup created"

# 4. Update cassandra.yaml for CDC
echo "Step 4: Updating cassandra.yaml for CDC..."

# Check if CDC is already enabled
if grep -q "^cdc_enabled: true" "$CASSANDRA_YAML"; then
    echo "⚠ CDC already enabled in cassandra.yaml"
else
    cat >> "$CASSANDRA_YAML" <<EOF

# CDC Configuration - Added by deployment script
cdc_enabled: true
cdc_raw_directory: $CDC_RAW_DIR
cdc_free_space_check_interval_ms: 250

# Commitlog settings for CDC
commitlog_sync: periodic
commitlog_sync_period_in_ms: 10000
commitlog_segment_size_in_mb: 32
EOF
    echo "✓ CDC configuration added to cassandra.yaml"
fi

# 5. Restart Cassandra (rolling restart - one node at a time)
echo "Step 5: Restarting Cassandra to apply CDC configuration..."
read -p "Restart Cassandra now? (yes/no): " CONFIRM
if [ "$CONFIRM" = "yes" ]; then
    systemctl restart cassandra
    echo "⏳ Waiting for Cassandra to restart..."
    sleep 30
    
    # Wait for node to be UP
    MAX_WAIT=120
    ELAPSED=0
    while [ $ELAPSED -lt $MAX_WAIT ]; do
        if nodetool status | grep -q "^UN.*$(hostname -i)"; then
            echo "✓ Cassandra restarted successfully"
            break
        fi
        sleep 5
        ELAPSED=$((ELAPSED + 5))
    done
    
    if [ $ELAPSED -ge $MAX_WAIT ]; then
        echo "Error: Cassandra failed to restart within ${MAX_WAIT}s"
        exit 1
    fi
else
    echo "⚠ Cassandra restart skipped - manual restart required"
fi

# 6. Verify CDC directory is being used
echo "Step 6: Verifying CDC configuration..."
if [ -d "$CDC_RAW_DIR" ] && [ "$(stat -c %U $CDC_RAW_DIR)" = "cassandra" ]; then
    echo "✓ CDC raw directory exists and has correct ownership"
else
    echo "Error: CDC raw directory validation failed"
    exit 1
fi

echo ""
echo "=========================================="
echo "Node preparation complete for: $NODE_NAME"
echo "=========================================="
echo "Next steps:"
echo "1. Deploy CDC agent binary and config"
echo "2. Enable CDC on tables"
echo "3. Start CDC agent service"
"""

# CDC agent installation script
agent_install_script = """#!/bin/bash
# install_cdc_agent.sh
# Installs DataStax CDC agent on Cassandra node

set -euo pipefail

NODE_NAME=${1:-$(hostname)}
AGENT_VERSION="1.5.0"
AGENT_BINARY_URL="https://releases.datastax.com/cdc-agent/cassandra-cdc-agent-${AGENT_VERSION}.tar.gz"
INSTALL_DIR="/opt/cassandra-cdc-agent"
CONFIG_DIR="/etc/cassandra-cdc-agent"
LOG_DIR="/var/log/cassandra-cdc-agent"

echo "=========================================="
echo "Installing CDC Agent on: $NODE_NAME"
echo "=========================================="

# 1. Download CDC agent
echo "Step 1: Downloading CDC agent v${AGENT_VERSION}..."
mkdir -p /tmp/cdc-agent-install
cd /tmp/cdc-agent-install
wget -q "$AGENT_BINARY_URL"
echo "✓ Downloaded CDC agent"

# 2. Extract and install
echo "Step 2: Installing CDC agent..."
tar -xzf "cassandra-cdc-agent-${AGENT_VERSION}.tar.gz"
mkdir -p "$INSTALL_DIR"
cp -r cassandra-cdc-agent-${AGENT_VERSION}/* "$INSTALL_DIR/"
chmod +x "$INSTALL_DIR/bin/cdc-agent"
echo "✓ CDC agent installed to $INSTALL_DIR"

# 3. Create config directory
echo "Step 3: Creating configuration directory..."
mkdir -p "$CONFIG_DIR"
mkdir -p "$LOG_DIR"
chown -R cassandra:cassandra "$LOG_DIR"
echo "✓ Config and log directories created"

# 4. Generate agent configuration
echo "Step 4: Generating cdc-agent.yaml..."
cat > "$CONFIG_DIR/cdc-agent.yaml" <<EOF
agent:
  contactPoints: ["cassandra-node1:9042", "cassandra-node2:9042", "cassandra-node3:9042"]
  localDc: datacenter1
  
  # Pulsar connection
  pulsarServiceUrl: pulsar://pulsar-broker:6650
  
  # Authentication
  auth:
    username: cassandra
    password: cassandra
  
  # CDC processing settings
  cdc:
    cdcRawDirectory: /var/lib/cassandra/cdc_raw
    pollIntervalMs: 1000
    errorCommitLogReprocessEnabled: true
    maxConcurrentProcessors: 4
    cdcConcurrentProcessors: 2
    schemaCacheRefreshMs: 60000
  
  # Topic mapping
  topicConfig:
    topicPrefix: data-
    topicFilter:
      - keyspace: fleet_operational
        tables: 
          - vehicles
          - maintenance_records
          - fleet_assignments
          - drivers
          - routes
    partitions: 3
    serializationFormat: AVRO
    schemaRegistry:
      enabled: true
      url: http://pulsar-broker:8001
  
  # Performance tuning
  performance:
    maxBatchSize: 1000
    flushIntervalMs: 5000
    maxBufferSize: 10000
    backPressureEnabled: true
    maxInflightMessages: 5000
  
  # Monitoring
  monitoring:
    metricsEnabled: true
    metricsPort: 9091
  
  # Logging
  logging:
    level: INFO
    logFile: /var/log/cassandra-cdc-agent/agent.log
    maxFileSize: 100MB
    maxBackupIndex: 10
EOF

echo "✓ Agent configuration generated"

# 5. Install systemd service
echo "Step 5: Installing systemd service..."
cat > /etc/systemd/system/cassandra-cdc-agent.service <<EOF
[Unit]
Description=DataStax CDC Agent for Apache Cassandra
After=cassandra.service
Requires=cassandra.service

[Service]
Type=simple
User=cassandra
Group=cassandra
Environment="JAVA_OPTS=-Xms2G -Xmx2G -XX:+UseG1GC"
ExecStart=$INSTALL_DIR/bin/cdc-agent --config $CONFIG_DIR/cdc-agent.yaml
Restart=on-failure
RestartSec=10s
LimitNOFILE=100000
LimitNPROC=32768
StandardOutput=journal
StandardError=journal
SyslogIdentifier=cassandra-cdc-agent

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable cassandra-cdc-agent
echo "✓ Systemd service installed and enabled"

# 6. Health check endpoint
echo "Step 6: Configuring health check..."
cat > "$INSTALL_DIR/bin/health_check.sh" <<'HEALTHEOF'
#!/bin/bash
# Health check for CDC agent

if systemctl is-active --quiet cassandra-cdc-agent; then
    if curl -s http://localhost:9091/metrics | grep -q "cdc_agent_mutations_sent"; then
        echo "HEALTHY"
        exit 0
    else
        echo "UNHEALTHY: Metrics not available"
        exit 1
    fi
else
    echo "UNHEALTHY: Service not running"
    exit 1
fi
HEALTHEOF

chmod +x "$INSTALL_DIR/bin/health_check.sh"
echo "✓ Health check script created"

echo ""
echo "=========================================="
echo "CDC Agent installation complete"
echo "=========================================="
echo "Configuration: $CONFIG_DIR/cdc-agent.yaml"
echo "Start service: sudo systemctl start cassandra-cdc-agent"
echo "Check status: sudo systemctl status cassandra-cdc-agent"
echo "View logs: journalctl -u cassandra-cdc-agent -f"
echo "Metrics: curl http://localhost:9091/metrics"
"""

# Table CDC enablement script
enable_cdc_script = """#!/bin/bash
# enable_cdc_tables.sh
# Enables CDC on specified Cassandra tables

set -euo pipefail

KEYSPACE="fleet_operational"
TABLES=("vehicles" "maintenance_records" "fleet_assignments" "drivers" "routes")

echo "=========================================="
echo "Enabling CDC on tables in keyspace: $KEYSPACE"
echo "=========================================="

# Function to enable CDC on a table
enable_cdc_on_table() {
    local table=$1
    echo "Enabling CDC on ${KEYSPACE}.${table}..."
    
    cqlsh -e "ALTER TABLE ${KEYSPACE}.${table} WITH cdc = true;" 2>&1
    
    if [ $? -eq 0 ]; then
        echo "✓ CDC enabled on ${KEYSPACE}.${table}"
    else
        echo "✗ Failed to enable CDC on ${KEYSPACE}.${table}"
        return 1
    fi
}

# Enable CDC on all tables
for table in "${TABLES[@]}"; do
    enable_cdc_on_table "$table"
    sleep 2
done

# Verify CDC is enabled
echo ""
echo "Verifying CDC status..."
cqlsh -e "SELECT keyspace_name, table_name, cdc FROM system_schema.tables WHERE keyspace_name = '${KEYSPACE}';"

echo ""
echo "=========================================="
echo "CDC enablement complete"
echo "=========================================="
"""

# Deployment orchestration script
orchestration_script = """#!/bin/bash
# deploy_cdc_canary.sh
# Orchestrates canary deployment of CDC across Cassandra cluster

set -euo pipefail

PHASE=${1:-"help"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Configuration
CANARY_NODE="cass-node-01"
PHASE2_NODES=("cass-node-01" "cass-node-02" "cass-node-03")
WAIT_TIME_NODE=86400  # 24 hours in seconds
WAIT_TIME_PHASE2=259200  # 72 hours in seconds

show_help() {
    cat <<EOF
CDC Canary Deployment Orchestrator

Usage: $0 <phase>

Phases:
  phase0    - Pre-deployment validation
  phase1    - Deploy to single canary node
  phase2    - Deploy to 3 nodes
  validate  - Run validation checks
  rollback  - Rollback deployment
  status    - Show deployment status

Example:
  $0 phase0
  $0 phase1
  $0 validate
EOF
}

phase0_predeployment() {
    echo "=========================================="
    echo "PHASE 0: Pre-Deployment Validation"
    echo "=========================================="
    
    # Check infrastructure
    echo "Checking Pulsar cluster..."
    if pulsar-admin brokers list | grep -q "pulsar-broker"; then
        echo "✓ Pulsar cluster operational"
    else
        echo "✗ Pulsar cluster not accessible"
        exit 1
    fi
    
    echo "Checking Iceberg sink connector..."
    if docker ps | grep -q "iceberg-sink"; then
        echo "✓ Iceberg sink running"
    else
        echo "✗ Iceberg sink not running"
        exit 1
    fi
    
    # Run validation test suite
    echo "Running validation test suite..."
    python3 "$SCRIPT_DIR/cdc_validation_suite.py"
    
    echo ""
    echo "✓ Phase 0 complete - ready for deployment"
}

phase1_single_node() {
    echo "=========================================="
    echo "PHASE 1: Single Node Deployment"
    echo "=========================================="
    
    NODE="$CANARY_NODE"
    
    # Prepare node
    echo "Preparing node: $NODE..."
    ssh "$NODE" "bash -s" < "$SCRIPT_DIR/cassandra_cdc_node_prep.sh"
    
    # Install CDC agent
    echo "Installing CDC agent on $NODE..."
    ssh "$NODE" "bash -s" < "$SCRIPT_DIR/install_cdc_agent.sh"
    
    # Enable CDC on tables
    echo "Enabling CDC on tables..."
    ssh "$NODE" "bash -s" < "$SCRIPT_DIR/enable_cdc_tables.sh"
    
    # Start CDC agent
    echo "Starting CDC agent on $NODE..."
    ssh "$NODE" "sudo systemctl start cassandra-cdc-agent"
    
    # Wait and verify
    sleep 30
    echo "Checking agent status..."
    ssh "$NODE" "systemctl status cassandra-cdc-agent"
    
    echo ""
    echo "✓ Phase 1 deployment complete on $NODE"
    echo "⏳ Monitor for 24 hours before proceeding to Phase 2"
    echo "   Run: $0 validate"
}

phase2_three_nodes() {
    echo "=========================================="
    echo "PHASE 2: Three Node Deployment"
    echo "=========================================="
    
    # Check Phase 1 was stable
    echo "Verifying Phase 1 stability..."
    if ! validate_deployment; then
        echo "✗ Phase 1 validation failed - cannot proceed to Phase 2"
        exit 1
    fi
    
    # Deploy to additional nodes
    for node in "${PHASE2_NODES[@]:1}"; do  # Skip first node (already deployed)
        echo "Deploying to $node..."
        
        ssh "$node" "bash -s" < "$SCRIPT_DIR/cassandra_cdc_node_prep.sh"
        ssh "$node" "bash -s" < "$SCRIPT_DIR/install_cdc_agent.sh"
        ssh "$node" "bash -s" < "$SCRIPT_DIR/enable_cdc_tables.sh"
        ssh "$node" "sudo systemctl start cassandra-cdc-agent"
        
        sleep 30
    done
    
    echo ""
    echo "✓ Phase 2 deployment complete on 3 nodes"
    echo "⏳ Monitor for 72 hours before full cluster rollout"
}

validate_deployment() {
    echo "Running deployment validation..."
    
    # Check agent health
    UNHEALTHY=0
    for node in "${PHASE2_NODES[@]}"; do
        if ssh "$node" "/opt/cassandra-cdc-agent/bin/health_check.sh" | grep -q "HEALTHY"; then
            echo "✓ $node: HEALTHY"
        else
            echo "✗ $node: UNHEALTHY"
            UNHEALTHY=$((UNHEALTHY + 1))
        fi
    done
    
    # Check metrics
    echo "Checking CDC metrics..."
    python3 "$SCRIPT_DIR/check_cdc_metrics.py"
    
    if [ $UNHEALTHY -eq 0 ]; then
        echo "✓ All nodes healthy"
        return 0
    else
        echo "✗ $UNHEALTHY nodes unhealthy"
        return 1
    fi
}

rollback_deployment() {
    echo "=========================================="
    echo "ROLLBACK: Disabling CDC"
    echo "=========================================="
    
    read -p "Confirm rollback (type 'ROLLBACK'): " CONFIRM
    if [ "$CONFIRM" != "ROLLBACK" ]; then
        echo "Rollback cancelled"
        exit 0
    fi
    
    # Stop agents on all deployed nodes
    for node in "${PHASE2_NODES[@]}"; do
        echo "Stopping CDC agent on $node..."
        ssh "$node" "sudo systemctl stop cassandra-cdc-agent" || true
    done
    
    # Disable CDC on tables
    echo "Disabling CDC on tables..."
    cqlsh -e "ALTER TABLE fleet_operational.vehicles WITH cdc = false;"
    cqlsh -e "ALTER TABLE fleet_operational.maintenance_records WITH cdc = false;"
    cqlsh -e "ALTER TABLE fleet_operational.fleet_assignments WITH cdc = false;"
    cqlsh -e "ALTER TABLE fleet_operational.drivers WITH cdc = false;"
    cqlsh -e "ALTER TABLE fleet_operational.routes WITH cdc = false;"
    
    echo ""
    echo "✓ Rollback complete"
}

show_status() {
    echo "=========================================="
    echo "CDC Deployment Status"
    echo "=========================================="
    
    for node in "${PHASE2_NODES[@]}"; do
        echo ""
        echo "Node: $node"
        echo "  Agent Status: $(ssh "$node" "systemctl is-active cassandra-cdc-agent" 2>/dev/null || echo "not deployed")"
        echo "  Health: $(ssh "$node" "/opt/cassandra-cdc-agent/bin/health_check.sh" 2>/dev/null || echo "N/A")"
    done
}

# Main execution
case "$PHASE" in
    phase0)
        phase0_predeployment
        ;;
    phase1)
        phase1_single_node
        ;;
    phase2)
        phase2_three_nodes
        ;;
    validate)
        validate_deployment
        ;;
    rollback)
        rollback_deployment
        ;;
    status)
        show_status
        ;;
    help|*)
        show_help
        exit 0
        ;;
esac
"""

print("Node Preparation Script:")
print(node_preparation_script[:500] + "...")
print("\nAgent Installation Script:")
print(agent_install_script[:500] + "...")
print("\nTable CDC Enablement Script:")
print(enable_cdc_script[:500] + "...")
print("\nOrchestration Script:")
print(orchestration_script[:500] + "...")

# Script summary
scripts_summary = pd.DataFrame({
    'Script': [
        'cassandra_cdc_node_prep.sh',
        'install_cdc_agent.sh',
        'enable_cdc_tables.sh',
        'deploy_cdc_canary.sh',
        'health_check.sh',
        'check_cdc_metrics.py'
    ],
    'Purpose': [
        'Prepares Cassandra node for CDC (directories, config, restart)',
        'Installs CDC agent binary, config, systemd service',
        'Enables CDC on all target tables via CQL',
        'Orchestrates canary deployment phases',
        'Health check endpoint for agent monitoring',
        'Validates CDC metrics against thresholds'
    ],
    'Usage': [
        'Run on each Cassandra node before agent install',
        'Run after node prep to install agent',
        'Run after agent install to enable CDC',
        'Run from control node to orchestrate deployment',
        'Called by monitoring systems',
        'Called during validation phases'
    ],
    'Execution_Time': ['10-15 min', '5 min', '2 min', 'Varies by phase', '<1 sec', '2-5 min']
})

print("\n" + "=" * 100)
print("DEPLOYMENT SCRIPTS SUMMARY")
print("=" * 100)
print(scripts_summary.to_string(index=False))

print("\n" + "=" * 100)
print("DEPLOYMENT AUTOMATION READY")
print("=" * 100)
print("✓ Node preparation script with safety checks")
print("✓ CDC agent installation with systemd service")
print("✓ Table CDC enablement with verification")
print("✓ Orchestration script for canary phases")
print("✓ Health check and metrics validation")
print("✓ Rollback automation included")
