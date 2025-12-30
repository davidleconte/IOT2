from pathlib import Path

diagrams_dir = Path("docs/diagrams")

# Multi-Tenant Isolation Architecture Diagram
multi_tenant_isolation_mermaid = """graph TB
    subgraph "Tenant: shipping-co-alpha"
        subgraph "Network Isolation"
            NS_Alpha[Kubernetes Namespace<br/>shipping-co-alpha<br/>NetworkPolicies + ResourceQuotas]
        end
        
        subgraph "Pulsar Tenant Alpha"
            PulsarTenant_Alpha[Pulsar Tenant: shipping-co-alpha<br/>Isolated Namespaces<br/>Quota: 1M msg/s]
            Topics_Alpha[vessel-tracking<br/>cargo<br/>port-operations<br/>analytics]
        end
        
        subgraph "Cassandra Keyspace Alpha"
            CassKS_Alpha[Keyspace: shipping_co_alpha<br/>RF=3, Quorum Consistency<br/>Row-level tenant_id enforcement]
            Tables_Alpha[telemetry<br/>metadata<br/>alerts<br/>feature_store]
        end
        
        subgraph "OpenSearch Tenant Alpha"
            OS_Tenant_Alpha[OpenSearch Tenant<br/>Isolated indices<br/>Document-level security]
            Indices_Alpha[vessel_telemetry<br/>anomalies<br/>engine_metrics<br/>incidents]
        end
        
        subgraph "Authentication & Authorization"
            JWT_Alpha[JWT Token<br/>tenant_id: shipping-co-alpha<br/>roles: admin, analyst, writer]
            RBAC_Alpha[RBAC Policies<br/>K8s ServiceAccount<br/>Pulsar/Cassandra/OS Roles]
        end
    end
    
    subgraph "Tenant: logistics-beta"
        subgraph "Network Isolation Beta"
            NS_Beta[Kubernetes Namespace<br/>logistics-beta<br/>NetworkPolicies + ResourceQuotas]
        end
        
        subgraph "Pulsar Tenant Beta"
            PulsarTenant_Beta[Pulsar Tenant: logistics-beta<br/>Isolated Namespaces<br/>Quota: 1M msg/s]
        end
        
        subgraph "Cassandra Keyspace Beta"
            CassKS_Beta[Keyspace: logistics_beta<br/>RF=3, Quorum Consistency]
        end
        
        subgraph "OpenSearch Tenant Beta"
            OS_Tenant_Beta[OpenSearch Tenant<br/>Isolated indices]
        end
        
        subgraph "Authentication Beta"
            JWT_Beta[JWT Token<br/>tenant_id: logistics-beta]
            RBAC_Beta[RBAC Policies]
        end
    end
    
    subgraph "Tenant: maritime-gamma"
        subgraph "Network Isolation Gamma"
            NS_Gamma[Kubernetes Namespace<br/>maritime-gamma<br/>NetworkPolicies + ResourceQuotas]
        end
        
        subgraph "Pulsar Tenant Gamma"
            PulsarTenant_Gamma[Pulsar Tenant: maritime-gamma<br/>Isolated Namespaces<br/>Quota: 1M msg/s]
        end
        
        subgraph "Cassandra Keyspace Gamma"
            CassKS_Gamma[Keyspace: maritime_gamma<br/>RF=3, Quorum Consistency]
        end
        
        subgraph "OpenSearch Tenant Gamma"
            OS_Tenant_Gamma[OpenSearch Tenant<br/>Isolated indices]
        end
        
        subgraph "Authentication Gamma"
            JWT_Gamma[JWT Token<br/>tenant_id: maritime-gamma]
            RBAC_Gamma[RBAC Policies]
        end
    end
    
    subgraph "Shared Infrastructure (Multi-Tenant)"
        SharedPulsar[Apache Pulsar Cluster<br/>3 Brokers Across AZs<br/>Tenant-level isolation]
        SharedCassandra[DataStax HCD Cluster<br/>9 Nodes Across AZs<br/>Keyspace-level isolation]
        SharedOpenSearch[OpenSearch Cluster<br/>6 Data Nodes<br/>Tenant-level isolation]
        SharedWatsonX[IBM watsonx.data<br/>Lakehouse<br/>Schema-level isolation]
        SharedMLFlow[MLflow Registry<br/>Experiment-level tagging<br/>tenant_id tracking]
        SharedFeast[Feast Feature Store<br/>Tenant-aware retrieval]
    end
    
    subgraph "Isolation Enforcement Points"
        APIGateway[API Gateway<br/>JWT Validation<br/>Tenant extraction]
        ServiceMesh[Istio Service Mesh<br/>mTLS + Authorization Policies]
        AuditLog[Audit Log System<br/>All tenant operations logged]
    end
    
    %% Connections
    APIGateway --> JWT_Alpha & JWT_Beta & JWT_Gamma
    JWT_Alpha --> RBAC_Alpha --> NS_Alpha
    JWT_Beta --> RBAC_Beta --> NS_Beta
    JWT_Gamma --> RBAC_Gamma --> NS_Gamma
    
    NS_Alpha --> PulsarTenant_Alpha --> SharedPulsar
    NS_Beta --> PulsarTenant_Beta --> SharedPulsar
    NS_Gamma --> PulsarTenant_Gamma --> SharedPulsar
    
    NS_Alpha --> CassKS_Alpha --> SharedCassandra
    NS_Beta --> CassKS_Beta --> SharedCassandra
    NS_Gamma --> CassKS_Gamma --> SharedCassandra
    
    NS_Alpha --> OS_Tenant_Alpha --> SharedOpenSearch
    NS_Beta --> OS_Tenant_Beta --> SharedOpenSearch
    NS_Gamma --> OS_Tenant_Gamma --> SharedOpenSearch
    
    SharedPulsar & SharedCassandra & SharedOpenSearch --> AuditLog
    
    style NS_Alpha fill:#a1c9f4
    style NS_Beta fill:#ffb482
    style NS_Gamma fill:#8de5a1
    style SharedPulsar fill:#ff9f9b
    style SharedCassandra fill:#d0bbff
    style SharedOpenSearch fill:#f7b6d2
    style APIGateway fill:#ffd400
    style AuditLog fill:#17b26a
"""

# Save Mermaid source
multi_tenant_path = diagrams_dir / "02_multi_tenant_isolation.mmd"
with open(multi_tenant_path, 'w') as f:
    f.write(multi_tenant_isolation_mermaid)

print(f"✅ Multi-Tenant Isolation Architecture Diagram")
print(f"   📄 Mermaid source: {multi_tenant_path}")
print(f"   📏 Size: {len(multi_tenant_isolation_mermaid)} characters")
print(f"\n🔒 Isolation Layers:")
print(f"   • Network: Kubernetes namespaces + NetworkPolicies")
print(f"   • Data: Pulsar tenants, Cassandra keyspaces, OpenSearch tenants")
print(f"   • Authentication: JWT tokens with tenant_id claims")
print(f"   • Authorization: RBAC at K8s, Pulsar, Cassandra, OpenSearch levels")
print(f"   • Audit: Complete logging of all tenant operations")

multi_tenant_diagram_summary = {
    "diagram": "Multi-Tenant Isolation Architecture",
    "file": str(multi_tenant_path),
    "tenants": 3,
    "isolation_layers": ["Network", "Data", "Authentication", "Authorization", "Audit"]
}
