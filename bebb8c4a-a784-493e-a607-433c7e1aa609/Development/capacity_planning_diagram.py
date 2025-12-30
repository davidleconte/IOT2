from pathlib import Path

diagrams_dir = Path("docs/diagrams")

# Capacity Planning for 4M msg/s Diagram
capacity_planning_mermaid = """graph TB
    subgraph "Workload Specification"
        WorkloadSpec[4 Million messages/second sustained<br/>1000 vessels × 4 msg/s each<br/>3 tenants<br/>Avg message size: 2KB<br/>Peak throughput: 6M msg/s burst]
    end
    
    subgraph "Pulsar Capacity Planning"
        PulsarIngress[Ingress Capacity<br/>4M msg/s × 2KB = 8 GB/s<br/>+ Protocol overhead = 10 GB/s]
        
        PulsarBrokers[Broker Configuration<br/>3 brokers × 10 Gbps NIC<br/>Total capacity: 30 Gbps = 3.75 GB/s per broker<br/>Target utilization: 70%<br/>Effective: 2.6 GB/s per broker = 7.8 GB/s total]
        
        PulsarPartitions[Topic Partitioning<br/>128 partitions per topic<br/>31,250 msg/s per partition<br/>Load balanced across brokers]
        
        PulsarBookKeeper[BookKeeper Storage<br/>3 bookies, ensemble=3, write quorum=2<br/>Write throughput: 8 GB/s × 2 replicas = 16 GB/s<br/>SSD IOPS: 50,000 per bookie<br/>Retention: 7 days hot + 30 days warm]
        
        PulsarCompute[Compute Resources<br/>3 brokers: 8 vCPU, 32GB RAM each<br/>3 bookies: 16 vCPU, 64GB RAM, 2TB SSD each<br/>3 ZK nodes: 4 vCPU, 16GB RAM each]
    end
    
    subgraph "Cassandra Capacity Planning"
        CassandraWrites[Write Throughput<br/>Raw telemetry: 4M writes/s<br/>+ Features: 1M writes/s<br/>+ Metadata: 0.5M writes/s<br/>Total: 5.5M writes/s]
        
        CassandraNodes[Node Configuration<br/>9 nodes (3 per AZ)<br/>611K writes/s per node<br/>16 vCPU, 64GB RAM, 1TB NVMe per node]
        
        CassandraStorage[Storage Calculation<br/>Avg row size: 1KB<br/>5.5M writes/s × 1KB = 5.5 GB/s<br/>RF=3: 16.5 GB/s total writes<br/>Daily data: 475 TB/day raw<br/>With compression (3:1): 158 TB/day<br/>30-day retention: 4.7 PB (compressed)]
        
        CassandraIOPS[IOPS Requirements<br/>5.5M writes/s × RF3 = 16.5M IOPS<br/>Per node: 1.83M IOPS<br/>NVMe SSDs: 500K IOPS each<br/>4 SSDs per node recommended]
    end
    
    subgraph "OpenSearch Capacity Planning"
        OpenSearchIngestion[Ingestion Rate<br/>Anomalies: 50K/s<br/>Alerts: 10K/s<br/>Logs: 100K/s<br/>Total: 160K docs/s]
        
        OpenSearchNodes[Cluster Configuration<br/>3 master nodes: 4 vCPU, 16GB each<br/>6 data nodes: 8 vCPU, 32GB, 500GB SSD each<br/>Shards: 5 primaries + 10 replicas per index<br/>Target shard size: 20-30 GB]
        
        OpenSearchStorage[Storage Planning<br/>160K docs/s × 5KB avg = 800 MB/s<br/>Daily: 69 TB/day<br/>Replicas (2×): 138 TB/day<br/>30-day hot: 4.1 PB<br/>ILM: Move to warm after 7 days<br/>Delete after 90 days]
    end
    
    subgraph "watsonx.data Capacity Planning"
        WatsonXIngestion[Archive Rate<br/>Historical data: 4M msg/s<br/>Batch window: 15 minutes<br/>Batch size: 3.6B messages = 7.2 TB per batch<br/>Daily batches: 96<br/>Daily archive: 691 TB]
        
        WatsonXStorage[Storage Configuration<br/>S3 object storage<br/>Iceberg table format<br/>Snappy compression (3:1)<br/>Daily compressed: 230 TB<br/>1-year retention: 84 PB<br/>S3 Intelligent-Tiering enabled]
        
        WatsonXCompute[Query Compute<br/>Presto coordinators: 3 × 8 vCPU, 32GB<br/>Presto workers: 10 × 16 vCPU, 64GB<br/>Gluten acceleration enabled<br/>Target query latency: < 30s for 1TB scan]
    end
    
    subgraph "Network Capacity Planning"
        NetworkBandwidth[Network Bandwidth<br/>Ingress: 10 GB/s<br/>Inter-service: 20 GB/s<br/>Egress (queries): 5 GB/s<br/>Total: 35 GB/s sustained<br/>Peak: 50 GB/s]
        
        NetworkTopology[Network Topology<br/>10 Gbps per node minimum<br/>25 Gbps for data-heavy nodes<br/>Cross-AZ: 10 Gbps provisioned<br/>VPC peering for watsonx]
    end
    
    subgraph "ML Infrastructure Capacity"
        MLInference[Inference Throughput<br/>Real-time: 100K predictions/s<br/>Batch: 10M predictions/hour<br/>Feature retrieval: < 10ms p99<br/>Prediction latency: < 50ms p99]
        
        MLCompute[Compute Resources<br/>Real-time inference: 10-50 pods (HPA)<br/>4 vCPU, 16GB per pod<br/>Batch inference: 5 Spark executors<br/>8 vCPU, 32GB per executor<br/>Feast online: 6 Redis nodes<br/>MLflow: 3 app servers]
    end
    
    subgraph "Total Infrastructure Cost Estimate"
        ComputeCost[Compute<br/>~200 vCPU total<br/>~800 GB RAM total<br/>Est: $5,000/month]
        
        StorageCost[Storage<br/>5 PB total across all systems<br/>S3: $115,000/month<br/>EBS: $50,000/month<br/>Est: $165,000/month]
        
        NetworkCost[Network<br/>35 GB/s × 2.6 PB/month transfer<br/>Est: $50,000/month]
        
        TotalCost[Total Estimated Cost<br/>$220,000/month<br/>$2.64M/year]
    end
    
    %% Connections
    WorkloadSpec --> PulsarIngress
    PulsarIngress --> PulsarBrokers --> PulsarPartitions
    PulsarPartitions --> PulsarBookKeeper
    PulsarBrokers --> PulsarCompute
    
    PulsarPartitions --> CassandraWrites
    CassandraWrites --> CassandraNodes --> CassandraStorage
    CassandraStorage --> CassandraIOPS
    
    PulsarPartitions --> OpenSearchIngestion
    OpenSearchIngestion --> OpenSearchNodes --> OpenSearchStorage
    
    PulsarPartitions --> WatsonXIngestion
    WatsonXIngestion --> WatsonXStorage
    WatsonXStorage --> WatsonXCompute
    
    PulsarBrokers --> NetworkBandwidth
    CassandraNodes --> NetworkBandwidth
    OpenSearchNodes --> NetworkBandwidth
    NetworkBandwidth --> NetworkTopology
    
    CassandraNodes --> MLInference
    MLInference --> MLCompute
    
    PulsarCompute --> ComputeCost
    CassandraNodes --> ComputeCost
    OpenSearchNodes --> ComputeCost
    MLCompute --> ComputeCost
    
    CassandraStorage --> StorageCost
    OpenSearchStorage --> StorageCost
    WatsonXStorage --> StorageCost
    
    NetworkBandwidth --> NetworkCost
    
    ComputeCost --> TotalCost
    StorageCost --> TotalCost
    NetworkCost --> TotalCost
    
    style WorkloadSpec fill:#ffd400
    style PulsarBrokers fill:#a1c9f4
    style CassandraNodes fill:#ffb482
    style OpenSearchNodes fill:#8de5a1
    style WatsonXStorage fill:#ff9f9b
    style NetworkBandwidth fill:#d0bbff
    style MLInference fill:#f7b6d2
    style TotalCost fill:#17b26a
"""

# Save Mermaid source
capacity_path = diagrams_dir / "06_capacity_planning.mmd"
with open(capacity_path, 'w') as f:
    f.write(capacity_planning_mermaid)

print(f"✅ Capacity Planning Diagram for 4M msg/s")
print(f"   📄 Mermaid source: {capacity_path}")
print(f"   📏 Size: {len(capacity_planning_mermaid)} characters")
print(f"\n📊 Capacity Planning Summary:")
print(f"   • Workload: 4M msg/s sustained, 6M msg/s peak burst")
print(f"   • Pulsar: 3 brokers (8 vCPU, 32GB), 128 partitions, 10 GB/s ingress")
print(f"   • Cassandra: 9 nodes (16 vCPU, 64GB, 1TB NVMe), 5.5M writes/s, 4.7 PB storage")
print(f"   • OpenSearch: 3 masters + 6 data nodes, 160K docs/s, 4.1 PB hot storage")
print(f"   • watsonx.data: 691 TB/day archive, 84 PB yearly, 10 Presto workers")
print(f"   • Network: 35 GB/s sustained, 50 GB/s peak, 25 Gbps per data node")
print(f"   • ML: 100K predictions/s real-time, 10M/hour batch")
print(f"   • Cost Estimate: ~$220K/month (~$2.64M/year)")

capacity_summary = {
    "diagram": "Capacity Planning for 4M msg/s",
    "file": str(capacity_path),
    "throughput": "4M msg/s sustained, 6M msg/s peak",
    "infrastructure": {
        "Pulsar": "3 brokers, 10 GB/s ingress",
        "Cassandra": "9 nodes, 5.5M writes/s, 4.7 PB",
        "OpenSearch": "9 nodes, 160K docs/s, 4.1 PB",
        "watsonx": "691 TB/day, 84 PB/year",
        "Network": "35 GB/s sustained"
    },
    "cost_estimate": "$220K/month"
}
