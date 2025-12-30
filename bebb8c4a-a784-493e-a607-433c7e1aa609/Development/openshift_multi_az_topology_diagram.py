from pathlib import Path

# Ensure directory exists
diagrams_dir = Path("docs/diagrams")
diagrams_dir.mkdir(parents=True, exist_ok=True)

# OpenShift Multi-AZ Deployment Topology Diagram
openshift_topology_mermaid = """graph TB
    subgraph "OpenShift Multi-AZ Cluster"
        subgraph "Availability Zone 1 (AZ1)"
            subgraph "Control Plane AZ1"
                Master1[Master Node 1<br/>etcd + API Server]
            end
            subgraph "Worker Nodes AZ1"
                Worker1A[Worker Node 1A<br/>Pulsar Broker<br/>Cassandra Node]
                Worker1B[Worker Node 1B<br/>OpenSearch Data<br/>Stream Processing]
                Worker1C[Worker Node 1C<br/>Feast/MLflow<br/>Inference Services]
            end
        end
        
        subgraph "Availability Zone 2 (AZ2)"
            subgraph "Control Plane AZ2"
                Master2[Master Node 2<br/>etcd + API Server]
            end
            subgraph "Worker Nodes AZ2"
                Worker2A[Worker Node 2A<br/>Pulsar Broker<br/>Cassandra Node]
                Worker2B[Worker Node 2B<br/>OpenSearch Data<br/>Stream Processing]
                Worker2C[Worker Node 2C<br/>Feast/MLflow<br/>Inference Services]
            end
        end
        
        subgraph "Availability Zone 3 (AZ3)"
            subgraph "Control Plane AZ3"
                Master3[Master Node 3<br/>etcd + API Server]
            end
            subgraph "Worker Nodes AZ3"
                Worker3A[Worker Node 3A<br/>Pulsar Broker<br/>Cassandra Node]
                Worker3B[Worker Node 3B<br/>OpenSearch Data<br/>Stream Processing]
                Worker3C[Worker Node 3C<br/>Feast/MLflow<br/>Inference Services]
            end
        end
    end
    
    subgraph "Persistent Storage"
        EBS1[EBS Volumes AZ1<br/>Cassandra/OpenSearch/Pulsar]
        EBS2[EBS Volumes AZ2<br/>Cassandra/OpenSearch/Pulsar]
        EBS3[EBS Volumes AZ3<br/>Cassandra/OpenSearch/Pulsar]
    end
    
    subgraph "Network Layer"
        LB[AWS Network Load Balancer<br/>Cross-AZ Traffic Distribution]
        Ingress[OpenShift Router/Ingress<br/>TLS Termination]
    end
    
    subgraph "External Services"
        WatsonX[IBM watsonx.data<br/>Lakehouse Storage<br/>Multi-Region Replication]
        S3[AWS S3<br/>MLflow Artifact Store<br/>Cross-Region Replication]
    end
    
    %% Connections
    LB --> Ingress
    Ingress --> Worker1A & Worker2A & Worker3A
    Ingress --> Worker1B & Worker2B & Worker3B
    Ingress --> Worker1C & Worker2C & Worker3C
    
    Worker1A --> EBS1
    Worker2A --> EBS2
    Worker3A --> EBS3
    
    Worker1B --> WatsonX
    Worker2B --> WatsonX
    Worker3B --> WatsonX
    
    Worker1C --> S3
    Worker2C --> S3
    Worker3C --> S3
    
    Master1 -.Raft Consensus.-> Master2 & Master3
    Master2 -.Raft Consensus.-> Master3
    
    Worker1A -.Gossip Protocol.-> Worker2A & Worker3A
    
    style Master1 fill:#a1c9f4
    style Master2 fill:#a1c9f4
    style Master3 fill:#a1c9f4
    style Worker1A fill:#ffb482
    style Worker2A fill:#ffb482
    style Worker3A fill:#ffb482
    style Worker1B fill:#8de5a1
    style Worker2B fill:#8de5a1
    style Worker3B fill:#8de5a1
    style Worker1C fill:#ff9f9b
    style Worker2C fill:#ff9f9b
    style Worker3C fill:#ff9f9b
    style LB fill:#ffd400
    style WatsonX fill:#d0bbff
    style S3 fill:#d0bbff
"""

# Save Mermaid source
openshift_topology_path = diagrams_dir / "01_openshift_multi_az_topology.mmd"
with open(openshift_topology_path, 'w') as f:
    f.write(openshift_topology_mermaid)

print(f"✅ OpenShift Multi-AZ Topology Diagram")
print(f"   📄 Mermaid source: {openshift_topology_path}")
print(f"   📏 Size: {len(openshift_topology_mermaid)} characters")
print(f"\n🎯 Topology Features:")
print(f"   • 3 Availability Zones with complete redundancy")
print(f"   • 3 Master nodes (etcd quorum) + 9 Worker nodes")
print(f"   • Cross-AZ replication for Pulsar, Cassandra, OpenSearch")
print(f"   • Zone-isolated EBS volumes with cross-zone data sync")
print(f"   • Multi-region external storage (watsonx.data, S3)")

openshift_diagram_summary = {
    "diagram": "OpenShift Multi-AZ Topology",
    "file": str(openshift_topology_path),
    "zones": 3,
    "master_nodes": 3,
    "worker_nodes": 9,
    "features": ["Cross-AZ HA", "Zone-isolated storage", "Multi-region external"]
}
