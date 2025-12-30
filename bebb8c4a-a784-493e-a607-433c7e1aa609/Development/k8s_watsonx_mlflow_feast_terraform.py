import os

# Create watsonx.data, MLflow, and Feast modules
watsonx_dir = "terraform/modules/k8s-watsonx-data"
mlflow_dir = "terraform/modules/k8s-mlflow"
feast_dir = "terraform/modules/k8s-feast"

for _dir in [watsonx_dir, mlflow_dir, feast_dir]:
    os.makedirs(_dir, exist_ok=True)

# watsonx.data with Presto C++ and Iceberg catalog
watsonx_main_tf = """
# watsonx.data with Presto C++ and Iceberg Catalog
terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

resource "kubernetes_namespace" "watsonx" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "watsonx-data"
      "multi-tenant"           = "true"
    }
  }
}

resource "kubernetes_service_account" "watsonx" {
  metadata {
    name      = "watsonx-data"
    namespace = kubernetes_namespace.watsonx.metadata[0].name
  }
}

# Presto C++ Coordinator
resource "kubernetes_deployment" "presto_coordinator" {
  metadata {
    name      = "presto-coordinator"
    namespace = kubernetes_namespace.watsonx.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        "app.kubernetes.io/name"      = "presto"
        "app.kubernetes.io/component" = "coordinator"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name"      = "presto"
          "app.kubernetes.io/component" = "coordinator"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.watsonx.metadata[0].name

        container {
          name  = "presto"
          image = var.presto_image

          port {
            name           = "http"
            container_port = 8080
          }

          env {
            name  = "PRESTO_DISCOVERY_URI"
            value = "http://presto-coordinator:8080"
          }
          env {
            name  = "PRESTO_NODE_TYPE"
            value = "coordinator"
          }

          resources {
            requests = {
              cpu    = var.coordinator_cpu
              memory = var.coordinator_memory
            }
            limits = {
              cpu    = var.coordinator_cpu
              memory = var.coordinator_memory
            }
          }

          volume_mount {
            name       = "catalog-config"
            mount_path = "/etc/presto/catalog"
          }

          liveness_probe {
            http_get {
              path = "/v1/info"
              port = 8080
            }
            initial_delay_seconds = 60
            period_seconds        = 30
          }
        }

        volume {
          name = "catalog-config"
          config_map {
            name = kubernetes_config_map.iceberg_catalog.metadata[0].name
          }
        }
      }
    }
  }
}

# Presto Workers
resource "kubernetes_deployment" "presto_worker" {
  metadata {
    name      = "presto-worker"
    namespace = kubernetes_namespace.watsonx.metadata[0].name
  }

  spec {
    replicas = var.worker_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name"      = "presto"
        "app.kubernetes.io/component" = "worker"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name"      = "presto"
          "app.kubernetes.io/component" = "worker"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.watsonx.metadata[0].name

        container {
          name  = "presto"
          image = var.presto_image

          env {
            name  = "PRESTO_DISCOVERY_URI"
            value = "http://presto-coordinator:8080"
          }
          env {
            name  = "PRESTO_NODE_TYPE"
            value = "worker"
          }

          resources {
            requests = {
              cpu    = var.worker_cpu
              memory = var.worker_memory
            }
            limits = {
              cpu    = var.worker_cpu
              memory = var.worker_memory
            }
          }

          volume_mount {
            name       = "catalog-config"
            mount_path = "/etc/presto/catalog"
          }
        }

        volume {
          name = "catalog-config"
          config_map {
            name = kubernetes_config_map.iceberg_catalog.metadata[0].name
          }
        }
      }
    }
  }

  depends_on = [kubernetes_deployment.presto_coordinator]
}

# Iceberg Catalog Configuration
resource "kubernetes_config_map" "iceberg_catalog" {
  metadata {
    name      = "iceberg-catalog-config"
    namespace = kubernetes_namespace.watsonx.metadata[0].name
  }

  data = {
    "iceberg.properties" = <<-EOF
      connector.name=iceberg
      iceberg.catalog.type=hive
      hive.metastore.uri=thrift://hive-metastore:9083
      iceberg.file-format=PARQUET
      iceberg.compression-codec=SNAPPY
      iceberg.table.base-location=s3a://watsonx-data/iceberg
    EOF
  }
}

# Service for Coordinator
resource "kubernetes_service" "presto_coordinator" {
  metadata {
    name      = "presto-coordinator"
    namespace = kubernetes_namespace.watsonx.metadata[0].name
  }

  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name"      = "presto"
      "app.kubernetes.io/component" = "coordinator"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
  }
}

# Network Policy
resource "kubernetes_network_policy" "watsonx_isolation" {
  metadata {
    name      = "watsonx-tenant-isolation"
    namespace = kubernetes_namespace.watsonx.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "presto"
      }
    }

    policy_types = ["Ingress", "Egress"]

    dynamic "ingress" {
      for_each = var.tenant_namespaces
      content {
        from {
          namespace_selector {
            match_labels = {
              "tenant" = ingress.value
            }
          }
        }
        ports {
          protocol = "TCP"
          port     = "8080"
        }
      }
    }

    ingress {
      from {
        pod_selector {}
      }
    }

    egress {
      to {
        pod_selector {}
      }
    }

    egress {
      ports {
        protocol = "TCP"
        port     = "9083"
      }
    }

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
  }
}
"""

watsonx_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for watsonx.data"
  type        = string
  default     = "watsonx-data"
}

variable "presto_image" {
  description = "Presto C++ Docker image"
  type        = string
  default     = "prestodb/presto:0.285"
}

variable "coordinator_cpu" {
  description = "CPU for coordinator"
  type        = string
  default     = "2000m"
}

variable "coordinator_memory" {
  description = "Memory for coordinator"
  type        = string
  default     = "8Gi"
}

variable "worker_replicas" {
  description = "Number of worker replicas"
  type        = number
  default     = 3
}

variable "worker_cpu" {
  description = "CPU for workers"
  type        = string
  default     = "4000m"
}

variable "worker_memory" {
  description = "Memory for workers"
  type        = string
  default     = "16Gi"
}

variable "tenant_namespaces" {
  description = "List of tenant namespaces"
  type        = list(string)
  default     = []
}
"""

watsonx_outputs_tf = """
output "namespace" {
  value = kubernetes_namespace.watsonx.metadata[0].name
}

output "coordinator_endpoint" {
  value = "${kubernetes_service.presto_coordinator.metadata[0].name}.${kubernetes_namespace.watsonx.metadata[0].name}.svc.cluster.local:8080"
}
"""

# MLflow Registry Module
mlflow_main_tf = """
# MLflow Model Registry
terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

resource "kubernetes_namespace" "mlflow" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "mlflow"
      "multi-tenant"           = "true"
    }
  }
}

resource "kubernetes_service_account" "mlflow" {
  metadata {
    name      = "mlflow"
    namespace = kubernetes_namespace.mlflow.metadata[0].name
  }
}

resource "kubernetes_deployment" "mlflow" {
  metadata {
    name      = "mlflow"
    namespace = kubernetes_namespace.mlflow.metadata[0].name
  }

  spec {
    replicas = var.mlflow_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name" = "mlflow"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name" = "mlflow"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.mlflow.metadata[0].name

        container {
          name  = "mlflow"
          image = var.mlflow_image

          port {
            name           = "http"
            container_port = 5000
          }

          env {
            name  = "MLFLOW_BACKEND_STORE_URI"
            value = var.backend_store_uri
          }
          env {
            name  = "MLFLOW_ARTIFACT_ROOT"
            value = var.artifact_root
          }

          resources {
            requests = {
              cpu    = var.mlflow_cpu
              memory = var.mlflow_memory
            }
            limits = {
              cpu    = var.mlflow_cpu
              memory = var.mlflow_memory
            }
          }

          liveness_probe {
            http_get {
              path = "/health"
              port = 5000
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }

          readiness_probe {
            http_get {
              path = "/health"
              port = 5000
            }
            initial_delay_seconds = 10
            period_seconds        = 5
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "mlflow" {
  metadata {
    name      = "mlflow"
    namespace = kubernetes_namespace.mlflow.metadata[0].name
  }

  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name" = "mlflow"
    }

    port {
      name        = "http"
      port        = 5000
      target_port = 5000
    }
  }
}

resource "kubernetes_network_policy" "mlflow_isolation" {
  metadata {
    name      = "mlflow-tenant-isolation"
    namespace = kubernetes_namespace.mlflow.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "mlflow"
      }
    }

    policy_types = ["Ingress", "Egress"]

    dynamic "ingress" {
      for_each = var.tenant_namespaces
      content {
        from {
          namespace_selector {
            match_labels = {
              "tenant" = ingress.value
            }
          }
        }
        ports {
          protocol = "TCP"
          port     = "5000"
        }
      }
    }

    ingress {
      from {
        pod_selector {}
      }
    }

    egress {
      to {
        pod_selector {}
      }
    }

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
  }
}
"""

mlflow_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for MLflow"
  type        = string
  default     = "mlflow"
}

variable "mlflow_image" {
  description = "MLflow Docker image"
  type        = string
  default     = "ghcr.io/mlflow/mlflow:v2.9.0"
}

variable "mlflow_replicas" {
  description = "Number of MLflow replicas"
  type        = number
  default     = 2
}

variable "mlflow_cpu" {
  description = "CPU for MLflow"
  type        = string
  default     = "1000m"
}

variable "mlflow_memory" {
  description = "Memory for MLflow"
  type        = string
  default     = "4Gi"
}

variable "backend_store_uri" {
  description = "Backend store URI (PostgreSQL)"
  type        = string
  default     = "postgresql://mlflow:password@postgres:5432/mlflow"
}

variable "artifact_root" {
  description = "Artifact root location (S3)"
  type        = string
  default     = "s3://mlflow-artifacts/"
}

variable "tenant_namespaces" {
  description = "List of tenant namespaces"
  type        = list(string)
  default     = []
}
"""

mlflow_outputs_tf = """
output "namespace" {
  value = kubernetes_namespace.mlflow.metadata[0].name
}

output "service_endpoint" {
  value = "${kubernetes_service.mlflow.metadata[0].name}.${kubernetes_namespace.mlflow.metadata[0].name}.svc.cluster.local:5000"
}
"""

# Feast Feature Store Module
feast_main_tf = """
# Feast Feature Store
terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

resource "kubernetes_namespace" "feast" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "feast"
      "multi-tenant"           = "true"
    }
  }
}

resource "kubernetes_service_account" "feast" {
  metadata {
    name      = "feast"
    namespace = kubernetes_namespace.feast.metadata[0].name
  }
}

# Feast Registry Deployment
resource "kubernetes_deployment" "feast_registry" {
  metadata {
    name      = "feast-registry"
    namespace = kubernetes_namespace.feast.metadata[0].name
  }

  spec {
    replicas = var.feast_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name"      = "feast"
        "app.kubernetes.io/component" = "registry"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name"      = "feast"
          "app.kubernetes.io/component" = "registry"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.feast.metadata[0].name

        container {
          name  = "feast"
          image = var.feast_image

          port {
            name           = "grpc"
            container_port = 6565
          }

          env {
            name  = "FEAST_REGISTRY_TYPE"
            value = "sql"
          }
          env {
            name  = "FEAST_REGISTRY_PATH"
            value = var.registry_path
          }
          env {
            name  = "FEAST_ONLINE_STORE_TYPE"
            value = "redis"
          }
          env {
            name  = "FEAST_REDIS_HOST"
            value = "redis.${kubernetes_namespace.feast.metadata[0].name}.svc.cluster.local"
          }
          env {
            name  = "FEAST_REDIS_PORT"
            value = "6379"
          }

          resources {
            requests = {
              cpu    = var.feast_cpu
              memory = var.feast_memory
            }
            limits = {
              cpu    = var.feast_cpu
              memory = var.feast_memory
            }
          }

          liveness_probe {
            grpc {
              port = 6565
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }
      }
    }
  }
}

# Redis for online store
resource "kubernetes_deployment" "redis" {
  metadata {
    name      = "redis"
    namespace = kubernetes_namespace.feast.metadata[0].name
  }

  spec {
    replicas = 1

    selector {
      match_labels = {
        "app.kubernetes.io/name" = "redis"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name" = "redis"
        }
      }

      spec {
        container {
          name  = "redis"
          image = "redis:7.2-alpine"

          port {
            name           = "redis"
            container_port = 6379
          }

          resources {
            requests = {
              cpu    = "500m"
              memory = "2Gi"
            }
            limits = {
              cpu    = "1000m"
              memory = "4Gi"
            }
          }
        }
      }
    }
  }
}

resource "kubernetes_service" "feast_registry" {
  metadata {
    name      = "feast-registry"
    namespace = kubernetes_namespace.feast.metadata[0].name
  }

  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name"      = "feast"
      "app.kubernetes.io/component" = "registry"
    }

    port {
      name        = "grpc"
      port        = 6565
      target_port = 6565
    }
  }
}

resource "kubernetes_service" "redis" {
  metadata {
    name      = "redis"
    namespace = kubernetes_namespace.feast.metadata[0].name
  }

  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name" = "redis"
    }

    port {
      name        = "redis"
      port        = 6379
      target_port = 6379
    }
  }
}

resource "kubernetes_network_policy" "feast_isolation" {
  metadata {
    name      = "feast-tenant-isolation"
    namespace = kubernetes_namespace.feast.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "feast"
      }
    }

    policy_types = ["Ingress", "Egress"]

    dynamic "ingress" {
      for_each = var.tenant_namespaces
      content {
        from {
          namespace_selector {
            match_labels = {
              "tenant" = ingress.value
            }
          }
        }
        ports {
          protocol = "TCP"
          port     = "6565"
        }
      }
    }

    ingress {
      from {
        pod_selector {}
      }
    }

    egress {
      to {
        pod_selector {}
      }
    }

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
  }
}
"""

feast_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for Feast"
  type        = string
  default     = "feast"
}

variable "feast_image" {
  description = "Feast Docker image"
  type        = string
  default     = "feastdev/feature-server:0.35.0"
}

variable "feast_replicas" {
  description = "Number of Feast replicas"
  type        = number
  default     = 2
}

variable "feast_cpu" {
  description = "CPU for Feast"
  type        = string
  default     = "1000m"
}

variable "feast_memory" {
  description = "Memory for Feast"
  type        = string
  default     = "4Gi"
}

variable "registry_path" {
  description = "Registry path (SQL connection string)"
  type        = string
  default     = "postgresql://feast:password@postgres:5432/feast"
}

variable "tenant_namespaces" {
  description = "List of tenant namespaces"
  type        = list(string)
  default     = []
}
"""

feast_outputs_tf = """
output "namespace" {
  value = kubernetes_namespace.feast.metadata[0].name
}

output "registry_endpoint" {
  value = "${kubernetes_service.feast_registry.metadata[0].name}.${kubernetes_namespace.feast.metadata[0].name}.svc.cluster.local:6565"
}

output "redis_endpoint" {
  value = "${kubernetes_service.redis.metadata[0].name}.${kubernetes_namespace.feast.metadata[0].name}.svc.cluster.local:6379"
}
"""

# Write all files
modules_data = {
    watsonx_dir: {
        "main.tf": watsonx_main_tf,
        "variables.tf": watsonx_variables_tf,
        "outputs.tf": watsonx_outputs_tf
    },
    mlflow_dir: {
        "main.tf": mlflow_main_tf,
        "variables.tf": mlflow_variables_tf,
        "outputs.tf": mlflow_outputs_tf
    },
    feast_dir: {
        "main.tf": feast_main_tf,
        "variables.tf": feast_variables_tf,
        "outputs.tf": feast_outputs_tf
    }
}

for _module_dir, files in modules_data.items():
    for filename, content in files.items():
        with open(f"{_module_dir}/{filename}", "w") as f:
            f.write(content)

print("✅ watsonx.data Terraform Module Created")
print(f"   📂 Location: {watsonx_dir}/")
print(f"   🔧 Features: Presto C++ coordinator/workers, Iceberg catalog, tenant isolation")

print("\n✅ MLflow Registry Terraform Module Created")
print(f"   📂 Location: {mlflow_dir}/")
print(f"   🔧 Features: MLflow tracking server, PostgreSQL backend, S3 artifacts")

print("\n✅ Feast Feature Store Terraform Module Created")
print(f"   📂 Location: {feast_dir}/")
print(f"   🔧 Features: Feast registry, Redis online store, tenant isolation")

all_modules = {
    "watsonx_data": watsonx_dir,
    "mlflow": mlflow_dir,
    "feast": feast_dir
}
