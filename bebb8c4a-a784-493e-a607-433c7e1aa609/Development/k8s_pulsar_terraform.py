import os

# Create Kubernetes Pulsar module with tenant isolation
pulsar_module_dir = "terraform/modules/k8s-pulsar"
os.makedirs(pulsar_module_dir, exist_ok=True)

# Main Terraform configuration for Pulsar cluster
pulsar_main_tf = """
# Apache Pulsar Cluster with Tenant Isolation
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

# Namespace for Pulsar cluster
resource "kubernetes_namespace" "pulsar" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name"    = "pulsar"
      "app.kubernetes.io/part-of" = "navtor-fleet-guardian"
      "multi-tenant"              = "true"
    }
  }
}

# ServiceAccount for Pulsar components
resource "kubernetes_service_account" "pulsar" {
  metadata {
    name      = "pulsar"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
    labels = {
      "app.kubernetes.io/name" = "pulsar"
    }
  }
}

# RBAC Role for Pulsar
resource "kubernetes_role" "pulsar" {
  metadata {
    name      = "pulsar"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "services", "endpoints", "configmaps", "secrets"]
    verbs      = ["get", "list", "watch", "create", "update", "patch"]
  }

  rule {
    api_groups = ["apps"]
    resources  = ["statefulsets", "deployments"]
    verbs      = ["get", "list", "watch"]
  }
}

# RBAC RoleBinding
resource "kubernetes_role_binding" "pulsar" {
  metadata {
    name      = "pulsar"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.pulsar.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.pulsar.metadata[0].name
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }
}

# ZooKeeper StatefulSet
resource "kubernetes_stateful_set" "zookeeper" {
  metadata {
    name      = "pulsar-zookeeper"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
    labels = {
      "app.kubernetes.io/name"      = "zookeeper"
      "app.kubernetes.io/component" = "zookeeper"
    }
  }

  spec {
    service_name = "pulsar-zookeeper"
    replicas     = var.zookeeper_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name"      = "zookeeper"
        "app.kubernetes.io/component" = "zookeeper"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name"      = "zookeeper"
          "app.kubernetes.io/component" = "zookeeper"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.pulsar.metadata[0].name

        container {
          name  = "zookeeper"
          image = var.zookeeper_image

          port {
            name           = "client"
            container_port = 2181
          }
          port {
            name           = "follower"
            container_port = 2888
          }
          port {
            name           = "leader"
            container_port = 3888
          }

          env {
            name  = "PULSAR_MEM"
            value = var.zookeeper_memory
          }

          resources {
            requests = {
              cpu    = var.zookeeper_cpu
              memory = var.zookeeper_memory
            }
            limits = {
              cpu    = var.zookeeper_cpu
              memory = var.zookeeper_memory
            }
          }

          volume_mount {
            name       = "data"
            mount_path = "/pulsar/data"
          }

          liveness_probe {
            exec {
              command = ["/bin/bash", "-c", "echo ruok | nc localhost 2181"]
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "data"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = var.storage_class
        resources {
          requests = {
            storage = var.zookeeper_storage
          }
        }
      }
    }
  }
}

# ZooKeeper Service
resource "kubernetes_service" "zookeeper" {
  metadata {
    name      = "pulsar-zookeeper"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }

  spec {
    cluster_ip = "None"
    selector = {
      "app.kubernetes.io/name"      = "zookeeper"
      "app.kubernetes.io/component" = "zookeeper"
    }

    port {
      name        = "client"
      port        = 2181
      target_port = 2181
    }
    port {
      name        = "follower"
      port        = 2888
      target_port = 2888
    }
    port {
      name        = "leader"
      port        = 3888
      target_port = 3888
    }
  }
}

# BookKeeper StatefulSet
resource "kubernetes_stateful_set" "bookkeeper" {
  metadata {
    name      = "pulsar-bookkeeper"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
    labels = {
      "app.kubernetes.io/name"      = "bookkeeper"
      "app.kubernetes.io/component" = "bookkeeper"
    }
  }

  spec {
    service_name = "pulsar-bookkeeper"
    replicas     = var.bookkeeper_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name"      = "bookkeeper"
        "app.kubernetes.io/component" = "bookkeeper"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name"      = "bookkeeper"
          "app.kubernetes.io/component" = "bookkeeper"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.pulsar.metadata[0].name

        init_container {
          name  = "wait-zookeeper"
          image = "busybox:1.35"
          command = [
            "sh", "-c",
            "until nc -z pulsar-zookeeper-0.pulsar-zookeeper.${kubernetes_namespace.pulsar.metadata[0].name}.svc.cluster.local 2181; do echo waiting for zookeeper; sleep 2; done"
          ]
        }

        container {
          name  = "bookkeeper"
          image = var.bookkeeper_image

          port {
            name           = "client"
            container_port = 3181
          }

          env {
            name  = "PULSAR_MEM"
            value = var.bookkeeper_memory
          }
          env {
            name  = "zkServers"
            value = "pulsar-zookeeper-0.pulsar-zookeeper:2181"
          }

          resources {
            requests = {
              cpu    = var.bookkeeper_cpu
              memory = var.bookkeeper_memory
            }
            limits = {
              cpu    = var.bookkeeper_cpu
              memory = var.bookkeeper_memory
            }
          }

          volume_mount {
            name       = "journal"
            mount_path = "/pulsar/data/bookkeeper/journal"
          }
          volume_mount {
            name       = "ledgers"
            mount_path = "/pulsar/data/bookkeeper/ledgers"
          }

          liveness_probe {
            exec {
              command = ["/bin/bash", "-c", "bin/bookkeeper shell bookiesanity"]
            }
            initial_delay_seconds = 60
            period_seconds        = 30
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "journal"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = var.storage_class
        resources {
          requests = {
            storage = var.bookkeeper_journal_storage
          }
        }
      }
    }

    volume_claim_template {
      metadata {
        name = "ledgers"
      }
      spec {
        access_modes       = ["ReadWriteOnce"]
        storage_class_name = var.storage_class
        resources {
          requests = {
            storage = var.bookkeeper_ledger_storage
          }
        }
      }
    }
  }

  depends_on = [kubernetes_stateful_set.zookeeper]
}

# BookKeeper Service
resource "kubernetes_service" "bookkeeper" {
  metadata {
    name      = "pulsar-bookkeeper"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }

  spec {
    cluster_ip = "None"
    selector = {
      "app.kubernetes.io/name"      = "bookkeeper"
      "app.kubernetes.io/component" = "bookkeeper"
    }

    port {
      name        = "client"
      port        = 3181
      target_port = 3181
    }
  }
}

# Broker StatefulSet with Tenant Isolation
resource "kubernetes_stateful_set" "broker" {
  metadata {
    name      = "pulsar-broker"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
    labels = {
      "app.kubernetes.io/name"      = "broker"
      "app.kubernetes.io/component" = "broker"
    }
  }

  spec {
    service_name = "pulsar-broker"
    replicas     = var.broker_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name"      = "broker"
        "app.kubernetes.io/component" = "broker"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name"      = "broker"
          "app.kubernetes.io/component" = "broker"
        }
        annotations = {
          "prometheus.io/scrape" = "true"
          "prometheus.io/port"   = "8080"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.pulsar.metadata[0].name

        init_container {
          name  = "wait-bookkeeper"
          image = "busybox:1.35"
          command = [
            "sh", "-c",
            "until nc -z pulsar-bookkeeper-0.pulsar-bookkeeper.${kubernetes_namespace.pulsar.metadata[0].name}.svc.cluster.local 3181; do echo waiting for bookkeeper; sleep 2; done"
          ]
        }

        container {
          name  = "broker"
          image = var.broker_image

          port {
            name           = "http"
            container_port = 8080
          }
          port {
            name           = "pulsar"
            container_port = 6650
          }

          env {
            name  = "PULSAR_MEM"
            value = var.broker_memory
          }
          env {
            name  = "zookeeperServers"
            value = "pulsar-zookeeper-0.pulsar-zookeeper:2181"
          }
          env {
            name  = "configurationStoreServers"
            value = "pulsar-zookeeper-0.pulsar-zookeeper:2181"
          }
          env {
            name  = "brokerDeduplicationEnabled"
            value = "true"
          }

          resources {
            requests = {
              cpu    = var.broker_cpu
              memory = var.broker_memory
            }
            limits = {
              cpu    = var.broker_cpu
              memory = var.broker_memory
            }
          }

          liveness_probe {
            http_get {
              path = "/admin/v2/brokers/health"
              port = 8080
            }
            initial_delay_seconds = 60
            period_seconds        = 30
          }

          readiness_probe {
            http_get {
              path = "/admin/v2/brokers/ready"
              port = 8080
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }
      }
    }
  }

  depends_on = [kubernetes_stateful_set.bookkeeper]
}

# Broker Service
resource "kubernetes_service" "broker" {
  metadata {
    name      = "pulsar-broker"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
    labels = {
      "app.kubernetes.io/name" = "broker"
    }
  }

  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name"      = "broker"
      "app.kubernetes.io/component" = "broker"
    }

    port {
      name        = "http"
      port        = 8080
      target_port = 8080
    }
    port {
      name        = "pulsar"
      port        = 6650
      target_port = 6650
    }
  }
}

# Network Policy for Pulsar - Tenant Isolation
resource "kubernetes_network_policy" "pulsar_isolation" {
  metadata {
    name      = "pulsar-tenant-isolation"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "broker"
      }
    }

    policy_types = ["Ingress", "Egress"]

    # Allow ingress from tenant namespaces
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
          port     = "6650"
        }
        ports {
          protocol = "TCP"
          port     = "8080"
        }
      }
    }

    # Allow internal Pulsar component communication
    ingress {
      from {
        pod_selector {
          match_labels = {
            "app.kubernetes.io/part-of" = "pulsar"
          }
        }
      }
    }

    # Allow egress to ZooKeeper and BookKeeper
    egress {
      to {
        pod_selector {
          match_labels = {
            "app.kubernetes.io/part-of" = "pulsar"
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
  }
}

# Resource Quota per Tenant
resource "kubernetes_resource_quota" "tenant_quota" {
  for_each = toset(var.tenant_namespaces)

  metadata {
    name      = "pulsar-tenant-${each.value}-quota"
    namespace = kubernetes_namespace.pulsar.metadata[0].name
  }

  spec {
    hard = {
      "requests.cpu"    = var.tenant_cpu_quota
      "requests.memory" = var.tenant_memory_quota
      "persistentvolumeclaims" = var.tenant_storage_quota
    }
  }
}
"""

# Variables file
pulsar_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for Pulsar"
  type        = string
  default     = "pulsar"
}

variable "storage_class" {
  description = "Storage class for persistent volumes"
  type        = string
  default     = "gp3"
}

# ZooKeeper Configuration
variable "zookeeper_replicas" {
  description = "Number of ZooKeeper replicas"
  type        = number
  default     = 3
}

variable "zookeeper_image" {
  description = "ZooKeeper Docker image"
  type        = string
  default     = "apachepulsar/pulsar:3.1.1"
}

variable "zookeeper_cpu" {
  description = "CPU allocation for ZooKeeper"
  type        = string
  default     = "500m"
}

variable "zookeeper_memory" {
  description = "Memory allocation for ZooKeeper"
  type        = string
  default     = "2Gi"
}

variable "zookeeper_storage" {
  description = "Storage size for ZooKeeper"
  type        = string
  default     = "20Gi"
}

# BookKeeper Configuration
variable "bookkeeper_replicas" {
  description = "Number of BookKeeper replicas"
  type        = number
  default     = 3
}

variable "bookkeeper_image" {
  description = "BookKeeper Docker image"
  type        = string
  default     = "apachepulsar/pulsar:3.1.1"
}

variable "bookkeeper_cpu" {
  description = "CPU allocation for BookKeeper"
  type        = string
  default     = "1000m"
}

variable "bookkeeper_memory" {
  description = "Memory allocation for BookKeeper"
  type        = string
  default     = "4Gi"
}

variable "bookkeeper_journal_storage" {
  description = "Storage size for BookKeeper journal"
  type        = string
  default     = "50Gi"
}

variable "bookkeeper_ledger_storage" {
  description = "Storage size for BookKeeper ledgers"
  type        = string
  default     = "100Gi"
}

# Broker Configuration
variable "broker_replicas" {
  description = "Number of Broker replicas"
  type        = number
  default     = 3
}

variable "broker_image" {
  description = "Broker Docker image"
  type        = string
  default     = "apachepulsar/pulsar:3.1.1"
}

variable "broker_cpu" {
  description = "CPU allocation for Broker"
  type        = string
  default     = "2000m"
}

variable "broker_memory" {
  description = "Memory allocation for Broker"
  type        = string
  default     = "8Gi"
}

# Tenant Configuration
variable "tenant_namespaces" {
  description = "List of tenant namespaces that can access Pulsar"
  type        = list(string)
  default     = []
}

variable "tenant_cpu_quota" {
  description = "CPU quota per tenant"
  type        = string
  default     = "4000m"
}

variable "tenant_memory_quota" {
  description = "Memory quota per tenant"
  type        = string
  default     = "16Gi"
}

variable "tenant_storage_quota" {
  description = "Storage quota per tenant (number of PVCs)"
  type        = string
  default     = "10"
}
"""

# Outputs file
pulsar_outputs_tf = """
output "namespace" {
  description = "Pulsar namespace"
  value       = kubernetes_namespace.pulsar.metadata[0].name
}

output "broker_service" {
  description = "Pulsar broker service endpoint"
  value       = "${kubernetes_service.broker.metadata[0].name}.${kubernetes_namespace.pulsar.metadata[0].name}.svc.cluster.local:6650"
}

output "broker_http_service" {
  description = "Pulsar broker HTTP endpoint"
  value       = "${kubernetes_service.broker.metadata[0].name}.${kubernetes_namespace.pulsar.metadata[0].name}.svc.cluster.local:8080"
}

output "zookeeper_service" {
  description = "ZooKeeper service endpoint"
  value       = "${kubernetes_service.zookeeper.metadata[0].name}.${kubernetes_namespace.pulsar.metadata[0].name}.svc.cluster.local:2181"
}

output "bookkeeper_service" {
  description = "BookKeeper service endpoint"
  value       = "${kubernetes_service.bookkeeper.metadata[0].name}.${kubernetes_namespace.pulsar.metadata[0].name}.svc.cluster.local:3181"
}
"""

# Write files
with open(f"{pulsar_module_dir}/main.tf", "w") as f:
    f.write(pulsar_main_tf)

with open(f"{pulsar_module_dir}/variables.tf", "w") as f:
    f.write(pulsar_variables_tf)

with open(f"{pulsar_module_dir}/outputs.tf", "w") as f:
    f.write(pulsar_outputs_tf)

print("✅ Kubernetes Pulsar Terraform Module Created")
print(f"   📂 Location: {pulsar_module_dir}/")
print(f"   📄 Files: main.tf, variables.tf, outputs.tf")
print(f"\n🔧 Features:")
print(f"   • ZooKeeper StatefulSet (3 replicas)")
print(f"   • BookKeeper StatefulSet (3 replicas)")
print(f"   • Broker StatefulSet (3 replicas)")
print(f"   • Tenant-aware network policies")
print(f"   • Resource quotas per tenant")
print(f"   • RBAC with service accounts")
print(f"   • Persistent storage with PVCs")

k8s_pulsar_files = {
    "main.tf": f"{pulsar_module_dir}/main.tf",
    "variables.tf": f"{pulsar_module_dir}/variables.tf",
    "outputs.tf": f"{pulsar_module_dir}/outputs.tf"
}
