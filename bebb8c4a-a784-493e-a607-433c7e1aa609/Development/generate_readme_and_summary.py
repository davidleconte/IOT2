from pathlib import Path
import json

diagrams_dir = Path("docs/diagrams")

# Comprehensive README for diagrams
readme_content = """# Architecture Diagrams - NAVTOR Fleet Guardian

This directory contains comprehensive architecture diagrams documenting the complete system topology, data flows, and operational characteristics of the NAVTOR Fleet Guardian platform.

## 📊 Diagram Suite Overview

### 1. OpenShift Multi-AZ Deployment Topology
**File:** `01_openshift_multi_az_topology.mmd`

Visualizes the complete OpenShift cluster deployment across 3 Availability Zones:
- **Control Plane:** 3 master nodes with etcd quorum and API servers
- **Worker Nodes:** 9 worker nodes (3 per AZ) hosting:
  - Pulsar brokers and Cassandra nodes
  - OpenSearch data nodes and stream processing services
  - Feast/MLflow infrastructure and inference services
- **Storage:** Zone-isolated EBS volumes with cross-zone replication
- **External Services:** IBM watsonx.data (multi-region) and AWS S3 (cross-region replication)
- **Network:** AWS Network Load Balancer with cross-AZ traffic distribution

**Key Features:**
- Complete redundancy across 3 availability zones
- Raft consensus for master nodes
- Gossip protocol for Cassandra coordination
- Zone-isolated storage with data synchronization

---

### 2. Multi-Tenant Isolation Architecture
**File:** `02_multi_tenant_isolation.mmd`

Demonstrates comprehensive isolation across 3 tenants (shipping-co-alpha, logistics-beta, maritime-gamma):
- **Network Isolation:** Kubernetes namespaces with NetworkPolicies and ResourceQuotas
- **Data Isolation:**
  - Pulsar: Tenant-level namespaces with 1M msg/s quotas per tenant
  - Cassandra: Keyspace-level isolation with row-level tenant_id enforcement
  - OpenSearch: Tenant-specific indices with document-level security
- **Authentication:** JWT tokens with tenant_id claims
- **Authorization:** Multi-layer RBAC (K8s, Pulsar, Cassandra, OpenSearch)
- **Audit:** Complete logging of all tenant operations

**Isolation Layers:**
1. Network (K8s namespaces + policies)
2. Data (tenant/keyspace/index isolation)
3. Authentication (JWT with tenant claims)
4. Authorization (RBAC at all levels)
5. Audit (comprehensive operation logging)

---

### 3. End-to-End Data Flow with DLQ/Retry/Quarantine
**File:** `03_end_to_end_dataflow.mmd`

Complete data flow from ingestion to persistence, processing, and inference:

**Success Path:**
- Vessel sensors (1000 vessels × 4 msg/s) → Ingress API (REST/gRPC with TLS + JWT)
- → Pulsar primary topic (128 partitions, KeyShared subscription)
- → Stream processing (validation, feature computation, anomaly detection)
- → Persistence (Cassandra, OpenSearch, watsonx.data)
- → Feature store (Feast online/offline) → ML inference (real-time/batch)

**Error Handling:**
- **Retry Chain:** 5s → 1m → 10m exponential backoff delays
- **Quarantine Topic:** Max retries exceeded, manual intervention required
- **DLQ (Dead Letter Queue):** Schema violations, malformed messages, permanent failures
- **DLQ Processor:** Alert generation, message inspection, replay capability

**Monitoring:**
- Prometheus metrics at every stage
- Alert Manager for DLQ thresholds and quarantine notifications
- PagerDuty integration for critical issues

---

### 4. ML Lifecycle Pipeline
**File:** `04_ml_lifecycle.mmd`

Complete machine learning lifecycle from features to production inference:

**1. Feature Engineering:**
- Raw data from Cassandra + watsonx.data
- Feast offline retrieval with point-in-time correct joins
- Spark feature engineering (window aggregations, statistical features)
- Feature definitions (operational, environmental, aggregation, anomaly)

**2. Model Training:**
- Training dataset generation (Parquet with train/val/test split)
- PySpark ML training (GBT, RandomForest with cross-validation)
- Model evaluation (precision/recall/F1, ROC-AUC, feature importance)
- MLflow tracking (experiments, metrics, parameters)

**3. Model Registry:**
- MLflow model versioning
- Stage transitions (Staging → Production)
- Model metadata (training timestamp, features, metrics, tenant flags)

**4. Deployment:**
- Tenant-specific model selection with A/B testing
- Kubernetes real-time inference (HPA: 3-20 replicas)
- Spark batch inference (CronJob scheduled)

**5. Monitoring & Feedback:**
- Prediction logging (features, outputs, timestamps)
- Drift detection (feature/prediction distribution shifts)
- Automatic retraining triggers
- Ground truth integration for continuous improvement

---

### 5. High Availability Configuration
**File:** `05_ha_configuration.mmd`

Detailed HA configuration for all platform components:

**Apache Pulsar:**
- 3 brokers (8 vCPU, 32GB RAM) active-active across AZs
- BookKeeper ensemble: 3 bookies, write quorum=2, ack quorum=2
- ZooKeeper quorum: 3 nodes, failover < 30s
- Features: RF=3, geo-replication ready, auto-failover, deduplication

**DataStax HCD (Cassandra):**
- 9 nodes (3 per AZ): 16 vCPU, 64GB RAM, 1TB NVMe SSD
- Replication Factor: 3, NetworkTopologyStrategy
- Quorum reads/writes, gossip protocol
- Hinted handoff, automated repair

**OpenSearch:**
- 3 master nodes (4 vCPU, 16GB) for quorum and split-brain prevention
- 6 data nodes (8 vCPU, 32GB, 500GB SSD) across AZs
- Index replicas: 2, shard allocation awareness
- ILM policies, snapshots to S3, rolling upgrades

**watsonx.data:**
- Multi-region replication (primary + secondary)
- RTO < 1 hour, RPO < 15 minutes
- Presto coordinators: 3, workers: 10+
- Iceberg ACID transactions, time travel

**MLflow:**
- Stateless app servers (2-5 replicas) across AZs
- PostgreSQL HA with synchronous replication
- S3 artifact store with versioning
- Load balanced, auto-failover

**Feast:**
- Redis cluster: 3 masters + 3 replicas with Sentinel
- Offline store inherits HA from watsonx.data
- Stateless registry, multi-replica serving

**Network:**
- AWS Network Load Balancer with cross-AZ distribution
- Route53 DNS with health-based and latency-based routing
- Automatic failover policies

---

### 6. Capacity Planning for 4M msg/s Sustained Throughput
**File:** `06_capacity_planning.mmd`

Comprehensive capacity planning for 4 million messages/second sustained:

**Workload Specification:**
- 1000 vessels × 4 msg/s = 4M msg/s sustained
- 3 tenants, average message size: 2KB
- Peak throughput: 6M msg/s burst capability

**Pulsar Capacity:**
- Ingress: 10 GB/s (8 GB/s data + protocol overhead)
- 3 brokers: 8 vCPU, 32GB RAM, 10 Gbps NIC each
- 128 partitions per topic (31,250 msg/s per partition)
- 3 BookKeeper bookies: 16 vCPU, 64GB RAM, 2TB SSD, 50K IOPS each
- Retention: 7 days hot + 30 days warm

**Cassandra Capacity:**
- 9 nodes: 16 vCPU, 64GB RAM, 1TB NVMe (4× SSDs per node)
- Write throughput: 5.5M writes/s (telemetry + features + metadata)
- Storage: 158 TB/day compressed (3:1), 4.7 PB for 30-day retention
- IOPS: 16.5M IOPS total (1.83M per node)

**OpenSearch Capacity:**
- 3 master nodes: 4 vCPU, 16GB RAM
- 6 data nodes: 8 vCPU, 32GB RAM, 500GB SSD
- Ingestion: 160K docs/s (anomalies + alerts + logs)
- Storage: 138 TB/day with replicas, 4.1 PB hot (30-day)
- ILM: Move to warm after 7 days, delete after 90 days

**watsonx.data Capacity:**
- Archive rate: 691 TB/day (96 batches × 7.2 TB)
- Compressed storage: 230 TB/day (3:1 Snappy compression)
- 1-year retention: 84 PB in S3 Intelligent-Tiering
- Presto: 3 coordinators (8 vCPU, 32GB), 10 workers (16 vCPU, 64GB)
- Query latency: < 30s for 1TB scan

**Network Capacity:**
- Ingress: 10 GB/s, Inter-service: 20 GB/s, Egress: 5 GB/s
- Total sustained: 35 GB/s, Peak: 50 GB/s
- Per-node: 10 Gbps minimum, 25 Gbps for data-heavy nodes

**ML Infrastructure:**
- Real-time inference: 100K predictions/s (10-50 pods, HPA-managed)
- Batch inference: 10M predictions/hour (5 Spark executors)
- Feature retrieval: < 10ms p99, Prediction: < 50ms p99
- Feast online: 6 Redis nodes, MLflow: 3 app servers

**Cost Estimate:**
- Compute: ~$5,000/month (200 vCPU, 800 GB RAM)
- Storage: ~$165,000/month (5 PB total: S3 + EBS)
- Network: ~$50,000/month (2.6 PB/month transfer)
- **Total: ~$220,000/month (~$2.64M/year)**

---

## 🎨 Diagram Format

All diagrams are provided in **Mermaid** format (`.mmd` files), which can be:
- Rendered in GitHub, GitLab, and most modern documentation platforms
- Converted to SVG/PNG using Mermaid CLI: `mmdc -i diagram.mmd -o diagram.svg`
- Edited in Mermaid Live Editor: https://mermaid.live/
- Integrated into documentation with Mermaid plugins

---

## 🎨 Color Scheme (Zerve Design System)

All diagrams use the official Zerve color palette:
- **#A1C9F4** (Light Blue) - Control plane, ingestion layer
- **#FFB482** (Orange) - Data storage, Cassandra
- **#8DE5A1** (Green) - Processing services, OpenSearch
- **#FF9F9B** (Coral) - ML components, watsonx
- **#D0BBFF** (Lavender) - Feature store, persistence
- **#FFD400** (Yellow) - Network, load balancers, inference
- **#17B26A** (Success Green) - Monitoring, feedback loops
- **#F04438** (Warning Red) - Error handling, DLQ, quarantine

---

## 📦 Diagram Generation

All diagrams were auto-generated using Python scripts that ensure:
- Consistency across the entire diagram suite
- Accurate representation of infrastructure specifications
- Adherence to Zerve design system
- Professional quality suitable for stakeholder presentations

---

## 🚀 Usage Recommendations

### For Technical Teams:
- Use these diagrams to understand system topology and data flows
- Reference capacity planning for infrastructure provisioning
- Leverage HA configurations for deployment setup

### For Architecture Reviews:
- Present OpenShift topology and multi-tenant isolation
- Discuss data flow patterns and error handling strategies
- Review ML lifecycle for model governance

### For Stakeholder Presentations:
- Use capacity planning for budget discussions
- Show HA configuration for reliability assurance
- Present end-to-end dataflow for system comprehension

### For Documentation:
- Embed diagrams in technical documentation
- Export to SVG/PNG for presentations
- Use as reference in runbooks and operational guides

---

## 📝 Maintenance

These diagrams should be updated when:
- Infrastructure specifications change
- New components are added to the platform
- Capacity requirements are adjusted
- HA configurations are modified
- Data flow patterns evolve

**Last Updated:** 2025-12-30
**Maintainer:** Architecture Team
**Review Cadence:** Quarterly or with major infrastructure changes
"""

# Save README
readme_path = diagrams_dir / "README.md"
with open(readme_path, 'w') as f:
    f.write(readme_content)

# Generate comprehensive summary JSON
diagram_suite_summary = {
    "diagram_suite": "NAVTOR Fleet Guardian Architecture Diagrams",
    "generated_date": "2025-12-30",
    "total_diagrams": 6,
    "diagrams": [
        {
            "id": 1,
            "name": "OpenShift Multi-AZ Deployment Topology",
            "file": "01_openshift_multi_az_topology.mmd",
            "description": "Complete 3-AZ OpenShift deployment with control plane, worker nodes, storage, and external services",
            "key_features": ["3 AZs", "3 masters + 9 workers", "Zone-isolated storage", "Multi-region external services"]
        },
        {
            "id": 2,
            "name": "Multi-Tenant Isolation Architecture",
            "file": "02_multi_tenant_isolation.mmd",
            "description": "Comprehensive isolation across 3 tenants with 5-layer security model",
            "key_features": ["Network isolation", "Data isolation", "JWT authentication", "Multi-layer RBAC", "Audit logging"]
        },
        {
            "id": 3,
            "name": "End-to-End Data Flow with DLQ/Retry/Quarantine",
            "file": "03_end_to_end_dataflow.mmd",
            "description": "Complete data pipeline from ingestion to inference with comprehensive error handling",
            "key_features": ["4M msg/s throughput", "3-stage retry chain", "DLQ processing", "Quarantine topic", "Monitoring"]
        },
        {
            "id": 4,
            "name": "ML Lifecycle Pipeline",
            "file": "04_ml_lifecycle.mmd",
            "description": "Full ML lifecycle from feature engineering to production inference with feedback loop",
            "key_features": ["Feature engineering", "Training pipeline", "MLflow registry", "Inference serving", "Drift detection"]
        },
        {
            "id": 5,
            "name": "High Availability Configuration",
            "file": "05_ha_configuration.mmd",
            "description": "Detailed HA setup for all platform components with specific configurations",
            "key_features": ["Pulsar HA", "Cassandra RF=3", "OpenSearch replicas", "watsonx multi-region", "MLflow HA"]
        },
        {
            "id": 6,
            "name": "Capacity Planning for 4M msg/s",
            "file": "06_capacity_planning.mmd",
            "description": "Comprehensive capacity planning with resource calculations and cost estimates",
            "key_features": ["4M msg/s sustained", "Resource sizing", "Storage calculations", "Network planning", "Cost estimate"]
        }
    ],
    "total_size_kb": sum([
        len(openshift_topology_mermaid),
        len(multi_tenant_isolation_mermaid),
        len(dataflow_mermaid),
        len(ml_lifecycle_mermaid),
        len(ha_configuration_mermaid),
        len(capacity_planning_mermaid)
    ]) / 1024,
    "export_instructions": {
        "mermaid_cli": "mmdc -i diagram.mmd -o diagram.svg",
        "mermaid_live": "https://mermaid.live/",
        "formats": ["SVG", "PNG", "PDF (via SVG)"]
    },
    "color_scheme": "Zerve Design System",
    "usage": ["Technical documentation", "Architecture reviews", "Stakeholder presentations", "Deployment guides"]
}

# Save summary JSON
summary_path = diagrams_dir / "DIAGRAM_SUITE_SUMMARY.json"
with open(summary_path, 'w') as f:
    json.dump(diagram_suite_summary, f, indent=2)

print(f"✅ Complete Diagram Suite Generated")
print(f"   📄 README: {readme_path}")
print(f"   📊 Summary JSON: {summary_path}")
print(f"\n📈 Diagram Suite Statistics:")
print(f"   • Total diagrams: 6")
print(f"   • Total size: {diagram_suite_summary['total_size_kb']:.1f} KB")
print(f"   • Format: Mermaid (.mmd)")
print(f"   • Color scheme: Zerve Design System")
print(f"\n🎯 All Required Diagrams Complete:")
print(f"   ✓ OpenShift Multi-AZ Topology")
print(f"   ✓ Multi-Tenant Isolation Architecture")
print(f"   ✓ End-to-End Data Flow (DLQ/Retry/Quarantine)")
print(f"   ✓ ML Lifecycle Pipeline")
print(f"   ✓ High Availability Configuration")
print(f"   ✓ Capacity Planning (4M msg/s)")

final_summary = diagram_suite_summary
