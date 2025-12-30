import os

# Create DataStax HCD and OpenSearch modules
datastax_dir = "terraform/modules/k8s-datastax-hcd"
opensearch_dir = "terraform/modules/k8s-opensearch"
os.makedirs(datastax_dir, exist_ok=True)
os.makedirs(opensearch_dir, exist_ok=True)

# DataStax HCD Main Terraform
datastax_main_tf = """
# DataStax HCD (Hyper-Converged Database) for Cassandra
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

resource "kubernetes_namespace" "datastax" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "datastax-hcd"
      "multi-tenant"           = "true"
    }
  }
}

resource "kubernetes_service_account" "datastax" {
  metadata {
    name      = "datastax-hcd"
    namespace = kubernetes_namespace.datastax.metadata[0].name
  }
}

# Deploy using Helm chart
resource "helm_release" "datastax_hcd" {
  name       = "datastax-hcd"
  repository = "https://datastax.github.io/charts"
  chart      = "k8ssandra-operator"
  namespace  = kubernetes_namespace.datastax.metadata[0].name
  version    = var.chart_version

  values = [
    yamlencode({
      cassandra = {
        version = var.cassandra_version
        datacenters = [{
          name = "dc1"
          size = var.cassandra_replicas
          storageConfig = {
            cassandraDataVolumeClaimSpec = {
              storageClassName = var.storage_class
              accessModes      = ["ReadWriteOnce"]
              resources = {
                requests = {
                  storage = var.cassandra_storage
                }
              }
            }
          }
          resources = {
            requests = {
              cpu    = var.cassandra_cpu
              memory = var.cassandra_memory
            }
            limits = {
              cpu    = var.cassandra_cpu
              memory = var.cassandra_memory
            }
          }
          config = {
            cassandraYaml = {
              authenticator                = "PasswordAuthenticator"
              authorizer                   = "CassandraAuthorizer"
              num_tokens                   = 256
              commitlog_sync               = "periodic"
              concurrent_reads             = 32
              concurrent_writes            = 32
              concurrent_counter_writes    = 32
            }
          }
        }]
      }
      serviceAccount = {
        create = false
        name   = kubernetes_service_account.datastax.metadata[0].name
      }
    })
  ]

  depends_on = [kubernetes_namespace.datastax]
}

# Network Policy for tenant isolation
resource "kubernetes_network_policy" "datastax_isolation" {
  metadata {
    name      = "datastax-tenant-isolation"
    namespace = kubernetes_namespace.datastax.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "cassandra"
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
          port     = "9042"
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

# Secrets for credentials
resource "kubernetes_secret" "datastax_credentials" {
  for_each = toset(var.tenant_namespaces)

  metadata {
    name      = "datastax-${each.value}-credentials"
    namespace = kubernetes_namespace.datastax.metadata[0].name
  }

  data = {
    username = base64encode("tenant_${each.value}")
    password = base64encode(random_password.tenant_password[each.value].result)
  }

  type = "Opaque"
}

resource "random_password" "tenant_password" {
  for_each = toset(var.tenant_namespaces)

  length  = 32
  special = true
}
"""

datastax_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for DataStax HCD"
  type        = string
  default     = "datastax"
}

variable "storage_class" {
  description = "Storage class for persistent volumes"
  type        = string
  default     = "gp3"
}

variable "chart_version" {
  description = "K8ssandra operator chart version"
  type        = string
  default     = "1.10.0"
}

variable "cassandra_version" {
  description = "Cassandra version"
  type        = string
  default     = "4.1.3"
}

variable "cassandra_replicas" {
  description = "Number of Cassandra replicas"
  type        = number
  default     = 3
}

variable "cassandra_cpu" {
  description = "CPU allocation for Cassandra"
  type        = string
  default     = "2000m"
}

variable "cassandra_memory" {
  description = "Memory allocation for Cassandra"
  type        = string
  default     = "8Gi"
}

variable "cassandra_storage" {
  description = "Storage size for Cassandra data"
  type        = string
  default     = "200Gi"
}

variable "tenant_namespaces" {
  description = "List of tenant namespaces"
  type        = list(string)
  default     = []
}
"""

datastax_outputs_tf = """
output "namespace" {
  description = "DataStax namespace"
  value       = kubernetes_namespace.datastax.metadata[0].name
}

output "service_endpoint" {
  description = "Cassandra service endpoint"
  value       = "datastax-hcd-dc1-service.${kubernetes_namespace.datastax.metadata[0].name}.svc.cluster.local:9042"
}
"""

# OpenSearch Main Terraform with Anomaly Detection and JVector
opensearch_main_tf = """
# OpenSearch with Anomaly Detection and JVector plugins
terraform {
  required_version = ">= 1.5"
  required_providers {
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = "~> 2.23"
    }
  }
}

resource "kubernetes_namespace" "opensearch" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "opensearch"
      "multi-tenant"           = "true"
    }
  }
}

resource "kubernetes_service_account" "opensearch" {
  metadata {
    name      = "opensearch"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }
}

resource "kubernetes_role" "opensearch" {
  metadata {
    name      = "opensearch"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  rule {
    api_groups = [""]
    resources  = ["pods", "services", "endpoints", "configmaps"]
    verbs      = ["get", "list", "watch"]
  }
}

resource "kubernetes_role_binding" "opensearch" {
  metadata {
    name      = "opensearch"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "Role"
    name      = kubernetes_role.opensearch.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.opensearch.metadata[0].name
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }
}

# ConfigMap for OpenSearch configuration
resource "kubernetes_config_map" "opensearch_config" {
  metadata {
    name      = "opensearch-config"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  data = {
    "opensearch.yml" = <<-EOF
      cluster.name: navtor-opensearch
      network.host: 0.0.0.0
      bootstrap.memory_lock: true
      discovery.type: zen
      discovery.seed_hosts:
        - opensearch-0.opensearch-headless
        - opensearch-1.opensearch-headless
        - opensearch-2.opensearch-headless
      cluster.initial_master_nodes:
        - opensearch-0
        - opensearch-1
        - opensearch-2
      
      # Security plugin
      plugins.security.ssl.http.enabled: false
      plugins.security.disabled: false
      plugins.security.allow_default_init_securityindex: true
      
      # Anomaly Detection plugin
      plugins.anomaly_detection.enabled: true
      
      # JVector plugin for vector search
      plugins.jvector.enabled: true
    EOF
  }
}

# OpenSearch StatefulSet
resource "kubernetes_stateful_set" "opensearch" {
  metadata {
    name      = "opensearch"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  spec {
    service_name = "opensearch-headless"
    replicas     = var.opensearch_replicas

    selector {
      match_labels = {
        "app.kubernetes.io/name" = "opensearch"
      }
    }

    template {
      metadata {
        labels = {
          "app.kubernetes.io/name" = "opensearch"
        }
      }

      spec {
        service_account_name = kubernetes_service_account.opensearch.metadata[0].name
        
        init_container {
          name  = "sysctl"
          image = "busybox:1.35"
          command = ["sh", "-c", "sysctl -w vm.max_map_count=262144"]
          security_context {
            privileged = true
          }
        }

        container {
          name  = "opensearch"
          image = var.opensearch_image

          port {
            name           = "http"
            container_port = 9200
          }
          port {
            name           = "transport"
            container_port = 9300
          }

          env {
            name  = "cluster.name"
            value = "navtor-opensearch"
          }
          env {
            name = "node.name"
            value_from {
              field_ref {
                field_path = "metadata.name"
              }
            }
          }
          env {
            name  = "OPENSEARCH_JAVA_OPTS"
            value = "-Xms${var.opensearch_heap} -Xmx${var.opensearch_heap}"
          }
          env {
            name  = "DISABLE_INSTALL_DEMO_CONFIG"
            value = "true"
          }

          resources {
            requests = {
              cpu    = var.opensearch_cpu
              memory = var.opensearch_memory
            }
            limits = {
              cpu    = var.opensearch_cpu
              memory = var.opensearch_memory
            }
          }

          volume_mount {
            name       = "data"
            mount_path = "/usr/share/opensearch/data"
          }
          volume_mount {
            name       = "config"
            mount_path = "/usr/share/opensearch/config/opensearch.yml"
            sub_path   = "opensearch.yml"
          }

          liveness_probe {
            tcp_socket {
              port = 9200
            }
            initial_delay_seconds = 60
            period_seconds        = 30
          }

          readiness_probe {
            http_get {
              path   = "/_cluster/health"
              port   = 9200
              scheme = "HTTP"
            }
            initial_delay_seconds = 30
            period_seconds        = 10
          }
        }

        volume {
          name = "config"
          config_map {
            name = kubernetes_config_map.opensearch_config.metadata[0].name
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
            storage = var.opensearch_storage
          }
        }
      }
    }
  }
}

# Headless service for StatefulSet
resource "kubernetes_service" "opensearch_headless" {
  metadata {
    name      = "opensearch-headless"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  spec {
    cluster_ip = "None"
    selector = {
      "app.kubernetes.io/name" = "opensearch"
    }

    port {
      name = "http"
      port = 9200
    }
    port {
      name = "transport"
      port = 9300
    }
  }
}

# ClusterIP service
resource "kubernetes_service" "opensearch" {
  metadata {
    name      = "opensearch"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  spec {
    type = "ClusterIP"
    selector = {
      "app.kubernetes.io/name" = "opensearch"
    }

    port {
      name        = "http"
      port        = 9200
      target_port = 9200
    }
  }
}

# Network Policy for tenant isolation
resource "kubernetes_network_policy" "opensearch_isolation" {
  metadata {
    name      = "opensearch-tenant-isolation"
    namespace = kubernetes_namespace.opensearch.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "opensearch"
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
          port     = "9200"
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

opensearch_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for OpenSearch"
  type        = string
  default     = "opensearch"
}

variable "storage_class" {
  description = "Storage class for persistent volumes"
  type        = string
  default     = "gp3"
}

variable "opensearch_image" {
  description = "OpenSearch Docker image with plugins"
  type        = string
  default     = "opensearchproject/opensearch:2.11.0"
}

variable "opensearch_replicas" {
  description = "Number of OpenSearch replicas"
  type        = number
  default     = 3
}

variable "opensearch_cpu" {
  description = "CPU allocation for OpenSearch"
  type        = string
  default     = "2000m"
}

variable "opensearch_memory" {
  description = "Memory allocation for OpenSearch"
  type        = string
  default     = "8Gi"
}

variable "opensearch_heap" {
  description = "JVM heap size for OpenSearch"
  type        = string
  default     = "4g"
}

variable "opensearch_storage" {
  description = "Storage size for OpenSearch data"
  type        = string
  default     = "100Gi"
}

variable "tenant_namespaces" {
  description = "List of tenant namespaces"
  type        = list(string)
  default     = []
}
"""

opensearch_outputs_tf = """
output "namespace" {
  description = "OpenSearch namespace"
  value       = kubernetes_namespace.opensearch.metadata[0].name
}

output "service_endpoint" {
  description = "OpenSearch service endpoint"
  value       = "${kubernetes_service.opensearch.metadata[0].name}.${kubernetes_namespace.opensearch.metadata[0].name}.svc.cluster.local:9200"
}

output "headless_service" {
  description = "OpenSearch headless service for StatefulSet"
  value       = "${kubernetes_service.opensearch_headless.metadata[0].name}.${kubernetes_namespace.opensearch.metadata[0].name}.svc.cluster.local"
}
"""

# Write DataStax files
with open(f"{datastax_dir}/main.tf", "w") as f:
    f.write(datastax_main_tf)
with open(f"{datastax_dir}/variables.tf", "w") as f:
    f.write(datastax_variables_tf)
with open(f"{datastax_dir}/outputs.tf", "w") as f:
    f.write(datastax_outputs_tf)

# Write OpenSearch files
with open(f"{opensearch_dir}/main.tf", "w") as f:
    f.write(opensearch_main_tf)
with open(f"{opensearch_dir}/variables.tf", "w") as f:
    f.write(opensearch_variables_tf)
with open(f"{opensearch_dir}/outputs.tf", "w") as f:
    f.write(opensearch_outputs_tf)

print("✅ DataStax HCD Terraform Module Created")
print(f"   📂 Location: {datastax_dir}/")
print(f"   🔧 Features: K8ssandra operator, Cassandra 4.1, tenant credentials")

print("\n✅ OpenSearch Terraform Module Created")
print(f"   📂 Location: {opensearch_dir}/")
print(f"   🔧 Features: Anomaly Detection plugin, JVector plugin, tenant isolation")

terraform_modules_created = {
    "datastax": datastax_dir,
    "opensearch": opensearch_dir
}
