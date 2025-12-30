from pathlib import Path

diagrams_dir = Path("docs/diagrams")

# High Availability Configuration Diagram
ha_configuration_mermaid = """graph TB
    subgraph "Apache Pulsar HA Configuration"
        subgraph "Pulsar Brokers"
            PB1[Broker 1 - AZ1<br/>8 vCPU, 32GB RAM<br/>Active-Active]
            PB2[Broker 2 - AZ2<br/>8 vCPU, 32GB RAM<br/>Active-Active]
            PB3[Broker 3 - AZ3<br/>8 vCPU, 32GB RAM<br/>Active-Active]
        end
        
        subgraph "BookKeeper Ensemble"
            BK1[Bookie 1 - AZ1<br/>Ensemble: 3<br/>Write Quorum: 2<br/>Ack Quorum: 2]
            BK2[Bookie 2 - AZ2<br/>SSD storage<br/>Journal + Ledger]
            BK3[Bookie 3 - AZ3<br/>Auto-recovery enabled]
        end
        
        subgraph "ZooKeeper Ensemble"
            ZK1[ZK 1 - AZ1<br/>Quorum: 3<br/>Leader election]
            ZK2[ZK 2 - AZ2<br/>Metadata store<br/>Configuration sync]
            ZK3[ZK 3 - AZ3<br/>Failover < 30s]
        end
        
        PulsarHA[Pulsar HA Features:<br/>• Topic replication RF=3<br/>• Geo-replication ready<br/>• Auto failover brokers<br/>• Message deduplication<br/>• Subscription cursor persistence]
    end
    
    subgraph "DataStax HCD HA Configuration"
        subgraph "Cassandra Nodes AZ1"
            C1[Node 1 - AZ1<br/>16 vCPU, 64GB RAM<br/>1TB NVMe SSD]
            C2[Node 2 - AZ1<br/>Rack-aware topology]
            C3[Node 3 - AZ1<br/>vnodes: 128]
        end
        
        subgraph "Cassandra Nodes AZ2"
            C4[Node 4 - AZ2<br/>16 vCPU, 64GB RAM]
            C5[Node 5 - AZ2<br/>Rack-aware topology]
            C6[Node 6 - AZ2<br/>vnodes: 128]
        end
        
        subgraph "Cassandra Nodes AZ3"
            C7[Node 7 - AZ3<br/>16 vCPU, 64GB RAM]
            C8[Node 8 - AZ3<br/>Rack-aware topology]
            C9[Node 9 - AZ3<br/>vnodes: 128]
        end
        
        CassandraHA[Cassandra HA Features:<br/>• Replication Factor: 3<br/>• NetworkTopologyStrategy<br/>• Quorum reads/writes<br/>• Gossip protocol<br/>• Hinted handoff<br/>• Repair automation]
    end
    
    subgraph "OpenSearch HA Configuration"
        subgraph "Master Nodes"
            OSM1[Master 1 - AZ1<br/>4 vCPU, 16GB RAM<br/>Master-eligible]
            OSM2[Master 2 - AZ2<br/>Quorum: 2 of 3]
            OSM3[Master 3 - AZ3<br/>Split-brain prevention]
        end
        
        subgraph "Data Nodes"
            OSD1[Data 1 - AZ1<br/>8 vCPU, 32GB RAM<br/>Hot data tier]
            OSD2[Data 2 - AZ2<br/>Index shards<br/>Replicas: 2]
            OSD3[Data 3 - AZ3<br/>Auto shard allocation]
            OSD4[Data 4 - AZ1<br/>Warm data tier]
            OSD5[Data 5 - AZ2<br/>ILM policies]
            OSD6[Data 6 - AZ3<br/>Snapshot to S3]
        end
        
        OpenSearchHA[OpenSearch HA Features:<br/>• Index replicas: 2<br/>• Shard allocation awareness<br/>• Cross-cluster replication<br/>• Snapshot lifecycle<br/>• Circuit breakers<br/>• Rolling upgrades]
    end
    
    subgraph "watsonx.data HA Configuration"
        WatsonXPrimary[Primary Region<br/>Presto coordinators: 3<br/>Workers: 10+<br/>S3 object storage]
        WatsonXSecondary[Secondary Region<br/>Active-Passive<br/>Cross-region replication<br/>RTO: < 1 hour<br/>RPO: < 15 minutes]
        
        WatsonXHA[watsonx.data HA Features:<br/>• Multi-region replication<br/>• Iceberg table format<br/>• ACID transactions<br/>• Time travel<br/>• Metadata versioning]
    end
    
    subgraph "MLflow HA Configuration"
        MLflowApp1[MLflow Server 1 - AZ1<br/>Backend: PostgreSQL HA<br/>Artifact: S3 versioned]
        MLflowApp2[MLflow Server 2 - AZ2<br/>Load balanced<br/>Stateless design]
        MLflowApp3[MLflow Server 3 - AZ3<br/>Auto-scaling: 2-5]
        
        MLflowDB[PostgreSQL HA<br/>Primary + Replica<br/>Synchronous replication<br/>Auto-failover]
        
        MLflowHA[MLflow HA Features:<br/>• Stateless app servers<br/>• DB replication<br/>• S3 artifact durability<br/>• Multi-AZ deployment]
    end
    
    subgraph "Feast HA Configuration"
        FeastOnlineRedis[Redis Cluster<br/>3 masters + 3 replicas<br/>Sentinel mode<br/>Auto-failover]
        FeastOfflineStore[watsonx.data<br/>Inherits HA from lakehouse<br/>Distributed queries]
        
        FeastHA[Feast HA Features:<br/>• Redis cluster mode<br/>• Offline store HA from lakehouse<br/>• Stateless registry<br/>• Multi-replica serving]
    end
    
    subgraph "Network & Load Balancing"
        NLB[AWS Network Load Balancer<br/>Cross-AZ<br/>Health checks<br/>Auto-failover]
        DNS[Route53 DNS<br/>Health-based routing<br/>Latency-based routing<br/>Failover policies]
    end
    
    %% Connections showing HA relationships
    NLB --> PB1 & PB2 & PB3
    PB1 & PB2 & PB3 --> BK1 & BK2 & BK3
    BK1 & BK2 & BK3 --> ZK1 & ZK2 & ZK3
    
    C1 & C2 & C3 -.Gossip.-> C4 & C5 & C6
    C4 & C5 & C6 -.Gossip.-> C7 & C8 & C9
    
    OSM1 & OSM2 & OSM3 --> OSD1 & OSD2 & OSD3 & OSD4 & OSD5 & OSD6
    
    WatsonXPrimary -.Replication.-> WatsonXSecondary
    
    DNS --> NLB
    NLB --> MLflowApp1 & MLflowApp2 & MLflowApp3
    MLflowApp1 & MLflowApp2 & MLflowApp3 --> MLflowDB
    
    style PB1 fill:#a1c9f4
    style PB2 fill:#a1c9f4
    style PB3 fill:#a1c9f4
    style C1 fill:#ffb482
    style C4 fill:#ffb482
    style C7 fill:#ffb482
    style OSM1 fill:#8de5a1
    style OSM2 fill:#8de5a1
    style OSM3 fill:#8de5a1
    style OSD1 fill:#d0bbff
    style OSD2 fill:#d0bbff
    style OSD3 fill:#d0bbff
    style WatsonXPrimary fill:#ff9f9b
    style WatsonXSecondary fill:#ff9f9b
    style NLB fill:#ffd400
    style DNS fill:#ffd400
    style MLflowDB fill:#17b26a
"""

# Save Mermaid source
ha_config_path = diagrams_dir / "05_ha_configuration.mmd"
with open(ha_config_path, 'w') as f:
    f.write(ha_configuration_mermaid)

print(f"✅ High Availability Configuration Diagram")
print(f"   📄 Mermaid source: {ha_config_path}")
print(f"   📏 Size: {len(ha_configuration_mermaid)} characters")
print(f"\n🔧 HA Configurations:")
print(f"   • Pulsar: 3 brokers, ensemble=3, write quorum=2, ack quorum=2")
print(f"   • Cassandra: 9 nodes across 3 AZs, RF=3, quorum consistency")
print(f"   • OpenSearch: 3 master + 6 data nodes, replicas=2, shard allocation awareness")
print(f"   • watsonx.data: Multi-region replication, RTO < 1hr, RPO < 15min")
print(f"   • MLflow: Stateless app servers (2-5 replicas), PostgreSQL HA with sync replication")
print(f"   • Feast: Redis cluster with Sentinel, offline HA from lakehouse")
print(f"   • Network: AWS NLB + Route53 with health-based routing")

ha_config_summary = {
    "diagram": "High Availability Configuration",
    "file": str(ha_config_path),
    "components": {
        "Pulsar": "3 brokers + BookKeeper ensemble + ZK quorum",
        "Cassandra": "9 nodes, RF=3, NetworkTopologyStrategy",
        "OpenSearch": "3 masters + 6 data nodes, replicas=2",
        "watsonx": "Multi-region, RTO<1hr, RPO<15min",
        "MLflow": "Stateless app + PostgreSQL HA",
        "Feast": "Redis cluster + lakehouse HA"
    }
}
