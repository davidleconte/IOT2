import os

# Create example tenant deployment and comprehensive README
examples_dir = "terraform/examples"
os.makedirs(examples_dir, exist_ok=True)

# Example multi-tenant deployment
example_main_tf = """
# Example Multi-Tenant Infrastructure Deployment
# This demonstrates how to use all modules together with proper tenant isolation

terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
    helm = {
      source  = "hashicorp/helm"
      version = "~> 2.11"
    }
  }
}

# Define tenants
locals {
  tenants = ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]
}

# Create tenant namespaces
resource "kubernetes_namespace" "tenants" {
  for_each = toset(local.tenants)

  metadata {
    name = each.value
    labels = {
      "tenant"                 = each.value
      "multi-tenant"           = "true"
      "app.kubernetes.io/managed-by" = "terraform"
    }
  }
}

# Create tenant service accounts
resource "kubernetes_service_account" "tenant_sa" {
  for_each = toset(local.tenants)

  metadata {
    name      = "${each.value}-sa"
    namespace = kubernetes_namespace.tenants[each.value].metadata[0].name
  }
}

# Resource quotas per tenant
resource "kubernetes_resource_quota" "tenant_quota" {
  for_each = toset(local.tenants)

  metadata {
    name      = "${each.value}-quota"
    namespace = kubernetes_namespace.tenants[each.value].metadata[0].name
  }

  spec {
    hard = {
      "requests.cpu"    = "20"
      "requests.memory" = "64Gi"
      "requests.storage" = "500Gi"
      "persistentvolumeclaims" = "50"
      "pods" = "100"
      "services" = "50"
    }
  }
}

# Network policies for tenant isolation
resource "kubernetes_network_policy" "tenant_isolation" {
  for_each = toset(local.tenants)

  metadata {
    name      = "tenant-isolation"
    namespace = kubernetes_namespace.tenants[each.value].metadata[0].name
  }

  spec {
    pod_selector {}

    policy_types = ["Ingress", "Egress"]

    # Deny all ingress by default except from same namespace
    ingress {
      from {
        pod_selector {}
      }
    }

    # Allow egress to infrastructure services
    egress {
      to {
        namespace_selector {
          match_labels = {
            "multi-tenant" = "true"
          }
        }
      }
    }

    # Allow DNS
    egress {
      to {
        namespace_selector {
          match_labels = {
            "name" = "kube-system"
          }
        }
      }
      ports {
        protocol = "UDP"
        port     = "53"
      }
    }

    # Allow internet egress (adjust as needed)
    egress {
      to {
        ip_block {
          cidr = "0.0.0.0/0"
          except = ["169.254.169.254/32"] # Block metadata service
        }
      }
    }
  }
}

# Deploy Pulsar cluster
module "pulsar" {
  source = "../modules/k8s-pulsar"

  namespace          = "pulsar"
  tenant_namespaces  = local.tenants
  storage_class      = "gp3"
  
  zookeeper_replicas = 3
  bookkeeper_replicas = 3
  broker_replicas = 3
  
  tenant_cpu_quota    = "8000m"
  tenant_memory_quota = "32Gi"
}

# Deploy DataStax HCD
module "datastax" {
  source = "../modules/k8s-datastax-hcd"

  namespace          = "datastax"
  tenant_namespaces  = local.tenants
  storage_class      = "gp3"
  
  cassandra_replicas = 3
}

# Deploy OpenSearch
module "opensearch" {
  source = "../modules/k8s-opensearch"

  namespace          = "opensearch"
  tenant_namespaces  = local.tenants
  storage_class      = "gp3"
  
  opensearch_replicas = 3
}

# Deploy watsonx.data
module "watsonx_data" {
  source = "../modules/k8s-watsonx-data"

  namespace         = "watsonx-data"
  tenant_namespaces = local.tenants
  
  worker_replicas = 3
}

# Deploy MLflow
module "mlflow" {
  source = "../modules/k8s-mlflow"

  namespace         = "mlflow"
  tenant_namespaces = local.tenants
  
  mlflow_replicas = 2
}

# Deploy Feast
module "feast" {
  source = "../modules/k8s-feast"

  namespace         = "feast"
  tenant_namespaces = local.tenants
  
  feast_replicas = 2
}

# Deploy Vault
module "vault" {
  source = "../modules/k8s-vault"

  namespace         = "vault"
  tenant_namespaces = local.tenants
  storage_class     = "gp3"
  
  vault_replicas = 3
}

# Deploy Sealed Secrets
module "sealed_secrets" {
  source = "../modules/k8s-sealed-secrets"

  namespace = "sealed-secrets"
}
"""

example_outputs_tf = """
output "tenant_namespaces" {
  description = "List of tenant namespaces created"
  value       = [for ns in kubernetes_namespace.tenants : ns.metadata[0].name]
}

output "pulsar_broker_endpoint" {
  description = "Pulsar broker endpoint"
  value       = module.pulsar.broker_service
}

output "datastax_endpoint" {
  description = "DataStax Cassandra endpoint"
  value       = module.datastax.service_endpoint
}

output "opensearch_endpoint" {
  description = "OpenSearch endpoint"
  value       = module.opensearch.service_endpoint
}

output "watsonx_coordinator_endpoint" {
  description = "watsonx.data Presto coordinator endpoint"
  value       = module.watsonx_data.coordinator_endpoint
}

output "mlflow_endpoint" {
  description = "MLflow tracking server endpoint"
  value       = module.mlflow.service_endpoint
}

output "feast_endpoint" {
  description = "Feast registry endpoint"
  value       = module.feast.registry_endpoint
}

output "vault_endpoint" {
  description = "Vault service endpoint"
  value       = module.vault.vault_service
}

output "sealed_secrets_controller" {
  description = "Sealed Secrets controller endpoint"
  value       = module.sealed_secrets.controller_service
}
"""

# Comprehensive README
readme_content = """# Kubernetes Multi-Tenant Infrastructure Modules

Production-grade Terraform modules for deploying a multi-tenant Kubernetes infrastructure with proper security boundaries, RBAC, network policies, and secrets management.

## 📦 Modules

### Core Infrastructure

#### 1. **Pulsar Cluster** (`k8s-pulsar`)
Apache Pulsar cluster with tenant isolation for real-time messaging and event streaming.

**Components:**
- ZooKeeper StatefulSet (3 replicas)
- BookKeeper StatefulSet (3 replicas) 
- Broker StatefulSet (3 replicas)
- Tenant-aware network policies
- Resource quotas per tenant
- RBAC with service accounts

**Features:**
- Broker deduplication enabled
- Persistent storage with PVCs
- Health probes and readiness checks
- Tenant isolation via network policies

#### 2. **DataStax HCD** (`k8s-datastax-hcd`)
DataStax Hyper-Converged Database (Cassandra) for distributed data storage.

**Components:**
- K8ssandra operator deployment
- Cassandra 4.1.3 with 3 replicas
- Password authentication and authorization
- Tenant credentials management

**Features:**
- PasswordAuthenticator and CassandraAuthorizer
- Per-tenant secrets with random passwords
- Network policy isolation
- Configurable storage and resources

#### 3. **OpenSearch** (`k8s-opensearch`)
OpenSearch cluster with Anomaly Detection and JVector plugins for search and analytics.

**Components:**
- OpenSearch StatefulSet (3 replicas)
- Headless service for cluster discovery
- ConfigMap for cluster configuration

**Features:**
- Anomaly Detection plugin enabled
- JVector plugin for vector search
- Raft-based cluster coordination
- Tenant isolation via network policies
- Health and readiness probes

#### 4. **watsonx.data** (`k8s-watsonx-data`)
IBM watsonx.data with Presto C++ query engine and Iceberg catalog for lakehouse analytics.

**Components:**
- Presto Coordinator deployment
- Presto Worker deployment (3 replicas)
- Iceberg catalog configuration

**Features:**
- Presto C++ for high-performance queries
- Iceberg catalog with Hive metastore
- Parquet file format with Snappy compression
- Tenant-aware network policies

#### 5. **MLflow Registry** (`k8s-mlflow`)
MLflow model registry for ML lifecycle management.

**Components:**
- MLflow server deployment (2 replicas)
- PostgreSQL backend store
- S3 artifact storage

**Features:**
- Model versioning and tracking
- Health and readiness probes
- Tenant isolation
- Configurable backend and artifact storage

#### 6. **Feast Feature Store** (`k8s-feast`)
Feast feature store for ML feature management and serving.

**Components:**
- Feast registry deployment (2 replicas)
- Redis online store deployment
- gRPC service for feature serving

**Features:**
- SQL registry (PostgreSQL)
- Redis for online feature serving
- Tenant-aware network policies
- gRPC health probes

### Security & Secrets Management

#### 7. **HashiCorp Vault** (`k8s-vault`)
Vault for dynamic secrets management and encryption.

**Components:**
- Vault HA with Raft storage (3 replicas)
- Vault injector for sidecar injection
- Kubernetes auth backend configuration

**Features:**
- High availability with Raft storage
- Per-tenant policies and roles
- Kubernetes authentication
- Secrets injection via sidecar
- ClusterRole for token review

#### 8. **Sealed Secrets** (`k8s-sealed-secrets`)
Bitnami Sealed Secrets for GitOps-friendly encrypted secrets.

**Components:**
- Sealed Secrets controller
- ClusterRole for secret management

**Features:**
- Encrypt secrets for safe Git storage
- Namespace-scoped encryption
- Automatic decryption in cluster
- Metrics with ServiceMonitor

## 🚀 Quick Start

### Prerequisites

```bash
# Required tools
terraform >= 1.5
kubectl >= 1.27
kubeseal >= 0.24 (for sealed secrets)
```

### Basic Deployment

1. **Clone and navigate to examples:**

```bash
cd terraform/examples
```

2. **Initialize Terraform:**

```bash
terraform init
```

3. **Review the plan:**

```bash
terraform plan
```

4. **Apply the configuration:**

```bash
terraform apply
```

This will deploy:
- 3 tenant namespaces (shipping-co-alpha, logistics-beta, maritime-gamma)
- All infrastructure modules with tenant isolation
- Network policies and resource quotas
- Secrets management infrastructure

### Module Usage

Each module can be used independently:

```hcl
module "pulsar" {
  source = "./modules/k8s-pulsar"

  namespace          = "pulsar"
  tenant_namespaces  = ["tenant-a", "tenant-b"]
  storage_class      = "gp3"
  
  zookeeper_replicas  = 3
  bookkeeper_replicas = 3
  broker_replicas     = 3
}
```

## 🔒 Security Features

### Network Policies

All modules implement tenant-aware network policies:

- **Ingress:** Only from authorized tenant namespaces
- **Egress:** Controlled access to infrastructure services
- **Isolation:** Tenants cannot access each other's traffic

### RBAC

Comprehensive RBAC with:

- Service accounts per component
- Roles with least-privilege access
- ClusterRoles for cross-namespace operations
- RoleBindings scoped to namespaces

### Secrets Management

Two-tier secrets strategy:

1. **Vault:** Dynamic secrets, database credentials, PKI
2. **Sealed Secrets:** GitOps-friendly encrypted secrets

### Resource Quotas

Per-tenant quotas prevent resource exhaustion:

- CPU and memory limits
- Storage quotas
- Pod and service limits

## 📊 Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Tenant Namespaces                     │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐       │
│  │ Tenant A   │  │ Tenant B   │  │ Tenant C   │       │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘       │
└────────┼────────────────┼────────────────┼──────────────┘
         │                │                │
    Network Policies (Isolation)
         │                │                │
┌────────┴────────────────┴────────────────┴──────────────┐
│              Infrastructure Services                     │
│  ┌─────────┐ ┌──────────┐ ┌──────────┐ ┌────────────┐ │
│  │ Pulsar  │ │ DataStax │ │OpenSearch│ │ watsonx    │ │
│  └─────────┘ └──────────┘ └──────────┘ └────────────┘ │
│  ┌─────────┐ ┌──────────┐ ┌──────────┐                │
│  │ MLflow  │ │  Feast   │ │  Vault   │                │
│  └─────────┘ └──────────┘ └──────────┘                │
└──────────────────────────────────────────────────────────┘
```

## 🔧 Configuration

### Tenant Customization

Add or modify tenants in `examples/main.tf`:

```hcl
locals {
  tenants = ["your-tenant-1", "your-tenant-2"]
}
```

### Resource Sizing

Adjust resources per module:

```hcl
module "pulsar" {
  source = "../modules/k8s-pulsar"
  
  broker_cpu    = "4000m"
  broker_memory = "16Gi"
  broker_replicas = 5
}
```

### Storage Classes

Configure storage per environment:

```hcl
storage_class = "gp3"          # AWS EBS
storage_class = "standard-rwo"  # GCP Persistent Disk
storage_class = "managed-premium" # Azure Disk
```

## 📝 Outputs

After deployment, Terraform outputs all service endpoints:

```bash
terraform output

# Example outputs:
pulsar_broker_endpoint = "pulsar-broker.pulsar.svc.cluster.local:6650"
opensearch_endpoint = "opensearch.opensearch.svc.cluster.local:9200"
vault_endpoint = "vault.vault.svc.cluster.local:8200"
```

## 🧪 Validation

### Verify Deployments

```bash
# Check all namespaces
kubectl get namespaces

# Check Pulsar cluster
kubectl get statefulsets -n pulsar
kubectl get pods -n pulsar

# Check network policies
kubectl get networkpolicies --all-namespaces

# Check resource quotas
kubectl get resourcequotas --all-namespaces
```

### Test Tenant Isolation

```bash
# Try to access from one tenant to another (should fail)
kubectl run test -n tenant-a --rm -it --image=busybox -- \
  wget -O- http://service.tenant-b.svc.cluster.local
```

## 🛠️ Operations

### Scaling

Scale any component:

```bash
kubectl scale statefulset pulsar-broker -n pulsar --replicas=5
```

Or update Terraform variables and reapply.

### Monitoring

All services expose Prometheus metrics:

```bash
kubectl port-forward -n pulsar svc/pulsar-broker 8080:8080
curl http://localhost:8080/metrics
```

### Backup and Recovery

Vault supports automated snapshots:

```bash
kubectl exec -n vault vault-0 -- vault operator raft snapshot save backup.snap
```

## 🐛 Troubleshooting

### Common Issues

**1. Pods stuck in Pending:**
```bash
kubectl describe pod <pod-name> -n <namespace>
# Check for resource constraints or storage issues
```

**2. Network policy blocking traffic:**
```bash
kubectl describe networkpolicy -n <namespace>
# Verify ingress/egress rules match your requirements
```

**3. Vault unsealing:**
```bash
kubectl exec -n vault vault-0 -- vault operator init
kubectl exec -n vault vault-0 -- vault operator unseal <key>
```

## 📚 Additional Resources

- [Apache Pulsar Documentation](https://pulsar.apache.org/docs/)
- [DataStax K8ssandra](https://docs.k8ssandra.io/)
- [OpenSearch Documentation](https://opensearch.org/docs/)
- [HashiCorp Vault on Kubernetes](https://www.vaultproject.io/docs/platform/k8s)
- [Sealed Secrets](https://github.com/bitnami-labs/sealed-secrets)

## 🤝 Contributing

This is a production-ready infrastructure template. Customize for your specific requirements:

1. Adjust resource quotas
2. Modify network policies
3. Add additional tenants
4. Configure monitoring and logging
5. Implement backup strategies

## 📄 License

MIT License - Use freely in your projects.

## ✅ Success Criteria

- ✅ Terraform applies cleanly without errors
- ✅ All pods reach Running state
- ✅ Network policies enforce tenant isolation
- ✅ Resource quotas prevent resource exhaustion
- ✅ Secrets management operational (Vault + Sealed Secrets)
- ✅ RBAC configured with least-privilege access
- ✅ Service endpoints accessible from tenant namespaces
- ✅ Health probes passing for all services
"""

# Write example files
with open(f"{examples_dir}/main.tf", "w") as f:
    f.write(example_main_tf)

with open(f"{examples_dir}/outputs.tf", "w") as f:
    f.write(example_outputs_tf)

# Write README at terraform root
with open("terraform/README.md", "w") as f:
    f.write(readme_content)

print("✅ Multi-Tenant Example Created")
print(f"   📂 Location: {examples_dir}/")
print(f"   📄 Files: main.tf, outputs.tf")
print(f"\n✅ Comprehensive README Created")
print(f"   📂 Location: terraform/README.md")
print(f"   📄 Size: {len(readme_content)} characters")

print("\n" + "="*80)
print("🎉 TERRAFORM INFRASTRUCTURE COMPLETE")
print("="*80)
print("\n📦 Modules Created:")
print("   1. k8s-pulsar - Apache Pulsar cluster with tenant isolation")
print("   2. k8s-datastax-hcd - DataStax Cassandra deployment")
print("   3. k8s-opensearch - OpenSearch with Anomaly Detection & JVector")
print("   4. k8s-watsonx-data - Presto C++ with Iceberg catalog")
print("   5. k8s-mlflow - MLflow model registry")
print("   6. k8s-feast - Feast feature store with Redis")
print("   7. k8s-vault - HashiCorp Vault for secrets management")
print("   8. k8s-sealed-secrets - Sealed Secrets for GitOps")

print("\n🔒 Security Features:")
print("   ✅ Network policies for tenant isolation")
print("   ✅ RBAC with service accounts and roles")
print("   ✅ Resource quotas per tenant")
print("   ✅ Secrets management (Vault + Sealed Secrets)")
print("   ✅ Network segmentation between tenants")

print("\n📚 Documentation:")
print("   • Comprehensive README with usage examples")
print("   • Example multi-tenant deployment")
print("   • Troubleshooting guide")
print("   • Architecture diagrams")

summary_data = {
    "modules_created": 8,
    "example_files": 2,
    "documentation": "terraform/README.md",
    "total_terraform_files": 27  # 8 modules * 3 files + 2 example files + 1 README
}
