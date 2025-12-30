# 🏗️ Navtor Fleet Guardian - Enterprise Architecture

## Multi-Tenancy Model

### Tenant Isolation Strategy
The platform implements a **hybrid multi-tenancy model** combining:

1. **Namespace-Level Isolation** (Kubernetes)
   - Each tenant gets dedicated Kubernetes namespace
   - Resource quotas and limit ranges enforced per tenant
   - Network policies prevent cross-tenant communication
   - RBAC policies scoped to tenant namespaces

2. **Data-Level Isolation** (Lakehouse & Databases)
   - **Hard Partitioning**: Tenant ID as partition key in Delta Lake
   - **Row-Level Security**: PostgreSQL RLS policies filter by tenant_id
   - **Index Separation**: OpenSearch tenant-specific indices with aliases
   - **Feature Store**: Feast features tagged with tenant metadata

3. **Message-Level Isolation** (Pulsar)
   - Dedicated Pulsar tenants per customer
   - Namespace-level quotas and ACLs
   - Topic isolation with tenant prefix: `tenant-{id}/namespace/topic`
   - Key_shared subscription ensures ordering within tenant

### Tenant Onboarding Flow
```
New Tenant → Provision Infrastructure → Create Namespaces → Configure Security → Deploy Services → Validate Isolation
```

---

## Technology Stack Rationale

| Component | Technology | Rationale |
|-----------|-----------|-----------|
| **Container Orchestration** | Amazon EKS | Managed Kubernetes with AWS integration, multi-AZ HA, auto-scaling |
| **Message Streaming** | Apache Pulsar | Multi-tenancy native, geo-replication, tiered storage, guaranteed ordering |
| **Data Lakehouse** | Delta Lake on S3 | ACID transactions, time travel, schema evolution, cost-effective storage |
| **Feature Store** | Feast | Open-source, Redis online store for low-latency, Spark for batch |
| **Search & Analytics** | OpenSearch | Real-time logs, dashboards, anomaly detection, tenant isolation |
| **ML Infrastructure** | Amazon SageMaker | Managed training/inference, auto-scaling endpoints, model registry |
| **IaC** | Terraform | Multi-cloud support, state management, modular architecture |
| **Service Mesh** | Istio | mTLS, traffic management, observability, fault injection |

---

## Security Model

### Authentication & Authorization
- **OAuth 2.0 / OpenID Connect**: User authentication via AWS Cognito
- **JWT Tokens**: Tenant ID embedded in claims, validated at service mesh
- **Service-to-Service**: mTLS via Istio, certificate rotation automated
- **API Gateway**: Rate limiting per tenant, AWS WAF protection

### Data Protection
- **Encryption at Rest**: S3 SSE-KMS, RDS encryption, EBS encrypted volumes
- **Encryption in Transit**: TLS 1.3 for all communications, mTLS in mesh
- **Secret Management**: AWS Secrets Manager with IAM policies, rotation enabled
- **Key Management**: AWS KMS with separate CMKs per tenant for critical data

### Compliance Considerations
- **GDPR**: Right to erasure via tombstone markers, data portability through APIs
- **SOC 2 Type II**: Audit logging to CloudWatch Logs, immutable audit trail in S3
- **ISO 27001**: Access reviews, security policies, incident response procedures
- **IMO Standards**: Maritime-specific data retention, vessel identification standards

---

## Deployment Topology

### Production Architecture (Multi-Region)

```
Region: eu-west-1 (Primary)
├── VPC (10.0.0.0/16)
│   ├── Public Subnets (NAT, ALB)
│   ├── Private Subnets (EKS nodes, RDS)
│   └── Isolated Subnets (Data lakehouse)
├── EKS Cluster (3 node groups)
│   ├── System: t3.large (monitoring, ingress)
│   ├── Application: c5.2xlarge (services)
│   └── ML: p3.2xlarge (inference)
├── Amazon MSK (Pulsar alternative)
├── RDS PostgreSQL (Multi-AZ)
├── OpenSearch (3 master, 6 data nodes)
└── S3 (Delta Lake tables)

Region: us-east-1 (DR/Geo-replication)
└── [Mirror of primary with Pulsar geo-replication]
```

### Environment Strategy
- **Development**: Single-region, reduced capacity, shared resources
- **Staging**: Production-like, blue/green deployments, load testing
- **Production**: Multi-region, auto-scaling, 99.95% SLA target