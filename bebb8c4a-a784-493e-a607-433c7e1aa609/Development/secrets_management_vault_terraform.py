import os

# Create secrets management modules - Vault and Sealed Secrets
vault_dir = "terraform/modules/k8s-vault"
sealed_secrets_dir = "terraform/modules/k8s-sealed-secrets"
os.makedirs(vault_dir, exist_ok=True)
os.makedirs(sealed_secrets_dir, exist_ok=True)

# HashiCorp Vault Module
vault_main_tf = """
# HashiCorp Vault for Secrets Management
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

resource "kubernetes_namespace" "vault" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "vault"
      "security"               = "true"
    }
  }
}

resource "kubernetes_service_account" "vault" {
  metadata {
    name      = "vault"
    namespace = kubernetes_namespace.vault.metadata[0].name
  }
}

# Vault Helm deployment
resource "helm_release" "vault" {
  name       = "vault"
  repository = "https://helm.releases.hashicorp.com"
  chart      = "vault"
  namespace  = kubernetes_namespace.vault.metadata[0].name
  version    = var.vault_version

  values = [
    yamlencode({
      server = {
        serviceAccount = {
          create = false
          name   = kubernetes_service_account.vault.metadata[0].name
        }
        ha = {
          enabled  = true
          replicas = var.vault_replicas
          raft = {
            enabled = true
            setNodeId = true
            config = <<-EOF
              ui = true
              listener "tcp" {
                tls_disable = 0
                address = "[::]:8200"
                cluster_address = "[::]:8201"
                tls_cert_file = "/vault/userconfig/vault-server-tls/tls.crt"
                tls_key_file = "/vault/userconfig/vault-server-tls/tls.key"
              }
              storage "raft" {
                path = "/vault/data"
              }
              service_registration "kubernetes" {}
            EOF
          }
        }
        dataStorage = {
          enabled      = true
          size         = var.vault_storage
          storageClass = var.storage_class
        }
        resources = {
          requests = {
            cpu    = var.vault_cpu
            memory = var.vault_memory
          }
          limits = {
            cpu    = var.vault_cpu
            memory = var.vault_memory
          }
        }
      }
      injector = {
        enabled = true
        replicas = 2
        resources = {
          requests = {
            cpu    = "250m"
            memory = "256Mi"
          }
          limits = {
            cpu    = "500m"
            memory = "512Mi"
          }
        }
      }
      ui = {
        enabled = true
        serviceType = "ClusterIP"
      }
    })
  ]

  depends_on = [kubernetes_namespace.vault]
}

# Vault policies for tenants
resource "kubernetes_config_map" "vault_policies" {
  metadata {
    name      = "vault-tenant-policies"
    namespace = kubernetes_namespace.vault.metadata[0].name
  }

  data = {
    for tenant in var.tenant_namespaces :
    "${tenant}-policy.hcl" => <<-EOF
      # Policy for tenant: ${tenant}
      path "secret/data/${tenant}/*" {
        capabilities = ["create", "read", "update", "delete", "list"]
      }
      
      path "secret/metadata/${tenant}/*" {
        capabilities = ["list"]
      }
      
      path "database/creds/${tenant}-*" {
        capabilities = ["read"]
      }
      
      path "pki/issue/${tenant}-role" {
        capabilities = ["create", "update"]
      }
    EOF
  }
}

# Kubernetes auth config
resource "kubernetes_config_map" "vault_k8s_auth" {
  metadata {
    name      = "vault-k8s-auth-config"
    namespace = kubernetes_namespace.vault.metadata[0].name
  }

  data = {
    "k8s-auth.sh" = <<-EOF
      #!/bin/sh
      # Enable Kubernetes auth
      vault auth enable kubernetes
      
      # Configure Kubernetes auth
      vault write auth/kubernetes/config \
        kubernetes_host="https://$KUBERNETES_PORT_443_TCP_ADDR:443" \
        kubernetes_ca_cert=@/var/run/secrets/kubernetes.io/serviceaccount/ca.crt \
        token_reviewer_jwt=@/var/run/secrets/kubernetes.io/serviceaccount/token
      
      # Create roles for each tenant
      ${join("\n", [for tenant in var.tenant_namespaces : <<-ROLE
      vault write auth/kubernetes/role/${tenant} \
        bound_service_account_names=${tenant}-sa \
        bound_service_account_namespaces=${tenant} \
        policies=${tenant}-policy \
        ttl=24h
      ROLE
      ])}
    EOF
  }
}

# RBAC for Vault
resource "kubernetes_cluster_role" "vault_auth" {
  metadata {
    name = "vault-auth"
  }

  rule {
    api_groups = [""]
    resources  = ["serviceaccounts"]
    verbs      = ["get"]
  }

  rule {
    api_groups = [""]
    resources  = ["serviceaccounts/token"]
    verbs      = ["create"]
  }
}

resource "kubernetes_cluster_role_binding" "vault_auth" {
  metadata {
    name = "vault-auth"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.vault_auth.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.vault.metadata[0].name
    namespace = kubernetes_namespace.vault.metadata[0].name
  }
}

# Network Policy
resource "kubernetes_network_policy" "vault_isolation" {
  metadata {
    name      = "vault-network-policy"
    namespace = kubernetes_namespace.vault.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "vault"
      }
    }

    policy_types = ["Ingress", "Egress"]

    # Allow ingress from all tenant namespaces
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
          port     = "8200"
        }
      }
    }

    # Allow internal Vault communication
    ingress {
      from {
        pod_selector {}
      }
      ports {
        protocol = "TCP"
        port     = "8200"
      }
      ports {
        protocol = "TCP"
        port     = "8201"
      }
    }

    # Allow egress to Kubernetes API
    egress {
      to {
        namespace_selector {
          match_labels = {
            "name" = "kube-system"
          }
        }
      }
    }

    # Allow internal communication
    egress {
      to {
        pod_selector {}
      }
    }

    # DNS
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

vault_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for Vault"
  type        = string
  default     = "vault"
}

variable "storage_class" {
  description = "Storage class for persistent volumes"
  type        = string
  default     = "gp3"
}

variable "vault_version" {
  description = "Vault Helm chart version"
  type        = string
  default     = "0.27.0"
}

variable "vault_replicas" {
  description = "Number of Vault replicas for HA"
  type        = number
  default     = 3
}

variable "vault_cpu" {
  description = "CPU allocation for Vault"
  type        = string
  default     = "1000m"
}

variable "vault_memory" {
  description = "Memory allocation for Vault"
  type        = string
  default     = "2Gi"
}

variable "vault_storage" {
  description = "Storage size for Vault data"
  type        = string
  default     = "10Gi"
}

variable "tenant_namespaces" {
  description = "List of tenant namespaces"
  type        = list(string)
  default     = []
}
"""

vault_outputs_tf = """
output "namespace" {
  value = kubernetes_namespace.vault.metadata[0].name
}

output "vault_service" {
  value = "vault.${kubernetes_namespace.vault.metadata[0].name}.svc.cluster.local:8200"
}

output "vault_ui_service" {
  value = "vault-ui.${kubernetes_namespace.vault.metadata[0].name}.svc.cluster.local:8200"
}
"""

# Sealed Secrets Module
sealed_secrets_main_tf = """
# Bitnami Sealed Secrets for encrypted secrets in Git
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

resource "kubernetes_namespace" "sealed_secrets" {
  metadata {
    name = var.namespace
    labels = {
      "app.kubernetes.io/name" = "sealed-secrets"
      "security"               = "true"
    }
  }
}

resource "kubernetes_service_account" "sealed_secrets" {
  metadata {
    name      = "sealed-secrets-controller"
    namespace = kubernetes_namespace.sealed_secrets.metadata[0].name
  }
}

# Sealed Secrets Controller
resource "helm_release" "sealed_secrets" {
  name       = "sealed-secrets"
  repository = "https://bitnami-labs.github.io/sealed-secrets"
  chart      = "sealed-secrets"
  namespace  = kubernetes_namespace.sealed_secrets.metadata[0].name
  version    = var.sealed_secrets_version

  values = [
    yamlencode({
      serviceAccount = {
        create = false
        name   = kubernetes_service_account.sealed_secrets.metadata[0].name
      }
      resources = {
        requests = {
          cpu    = var.controller_cpu
          memory = var.controller_memory
        }
        limits = {
          cpu    = var.controller_cpu
          memory = var.controller_memory
        }
      }
      metrics = {
        serviceMonitor = {
          enabled = true
        }
      }
    })
  ]

  depends_on = [kubernetes_namespace.sealed_secrets]
}

# RBAC for Sealed Secrets
resource "kubernetes_cluster_role" "sealed_secrets" {
  metadata {
    name = "sealed-secrets-controller"
  }

  rule {
    api_groups = [""]
    resources  = ["secrets"]
    verbs      = ["get", "list", "watch", "create", "update", "patch"]
  }

  rule {
    api_groups = [""]
    resources  = ["events"]
    verbs      = ["create", "patch"]
  }

  rule {
    api_groups = ["bitnami.com"]
    resources  = ["sealedsecrets"]
    verbs      = ["get", "list", "watch"]
  }

  rule {
    api_groups = ["bitnami.com"]
    resources  = ["sealedsecrets/status"]
    verbs      = ["update"]
  }
}

resource "kubernetes_cluster_role_binding" "sealed_secrets" {
  metadata {
    name = "sealed-secrets-controller"
  }

  role_ref {
    api_group = "rbac.authorization.k8s.io"
    kind      = "ClusterRole"
    name      = kubernetes_cluster_role.sealed_secrets.metadata[0].name
  }

  subject {
    kind      = "ServiceAccount"
    name      = kubernetes_service_account.sealed_secrets.metadata[0].name
    namespace = kubernetes_namespace.sealed_secrets.metadata[0].name
  }
}

# Network Policy for controller
resource "kubernetes_network_policy" "sealed_secrets" {
  metadata {
    name      = "sealed-secrets-controller"
    namespace = kubernetes_namespace.sealed_secrets.metadata[0].name
  }

  spec {
    pod_selector {
      match_labels = {
        "app.kubernetes.io/name" = "sealed-secrets"
      }
    }

    policy_types = ["Ingress", "Egress"]

    # Allow ingress from all namespaces (for decryption requests)
    ingress {
      ports {
        protocol = "TCP"
        port     = "8080"
      }
    }

    # Allow egress to Kubernetes API
    egress {
      to {
        namespace_selector {
          match_labels = {
            "name" = "kube-system"
          }
        }
      }
    }

    # DNS
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

# ConfigMap with usage instructions
resource "kubernetes_config_map" "sealed_secrets_usage" {
  metadata {
    name      = "sealed-secrets-usage"
    namespace = kubernetes_namespace.sealed_secrets.metadata[0].name
  }

  data = {
    "README.md" = <<-EOF
      # Sealed Secrets Usage
      
      ## Installation
      Install kubeseal CLI:
      ```bash
      wget https://github.com/bitnami-labs/sealed-secrets/releases/download/v0.24.0/kubeseal-0.24.0-linux-amd64.tar.gz
      tar xfz kubeseal-0.24.0-linux-amd64.tar.gz
      sudo install -m 755 kubeseal /usr/local/bin/kubeseal
      ```
      
      ## Create Sealed Secret
      ```bash
      # Create a secret
      kubectl create secret generic mysecret --dry-run=client --from-literal=password=mypassword -o yaml | \
        kubeseal -o yaml > mysealedsecret.yaml
      
      # Apply the sealed secret
      kubectl apply -f mysealedsecret.yaml
      ```
      
      ## Tenant-specific secrets
      For tenant isolation, use namespace-scoped sealed secrets:
      ```bash
      kubeseal --scope namespace-wide < secret.yaml > sealedsecret.yaml
      ```
    EOF
  }
}
"""

sealed_secrets_variables_tf = """
variable "namespace" {
  description = "Kubernetes namespace for Sealed Secrets"
  type        = string
  default     = "sealed-secrets"
}

variable "sealed_secrets_version" {
  description = "Sealed Secrets Helm chart version"
  type        = string
  default     = "2.13.2"
}

variable "controller_cpu" {
  description = "CPU allocation for controller"
  type        = string
  default     = "250m"
}

variable "controller_memory" {
  description = "Memory allocation for controller"
  type        = string
  default     = "256Mi"
}
"""

sealed_secrets_outputs_tf = """
output "namespace" {
  value = kubernetes_namespace.sealed_secrets.metadata[0].name
}

output "controller_service" {
  value = "sealed-secrets-controller.${kubernetes_namespace.sealed_secrets.metadata[0].name}.svc.cluster.local:8080"
}
"""

# Write Vault files
with open(f"{vault_dir}/main.tf", "w") as f:
    f.write(vault_main_tf)
with open(f"{vault_dir}/variables.tf", "w") as f:
    f.write(vault_variables_tf)
with open(f"{vault_dir}/outputs.tf", "w") as f:
    f.write(vault_outputs_tf)

# Write Sealed Secrets files
with open(f"{sealed_secrets_dir}/main.tf", "w") as f:
    f.write(sealed_secrets_main_tf)
with open(f"{sealed_secrets_dir}/variables.tf", "w") as f:
    f.write(sealed_secrets_variables_tf)
with open(f"{sealed_secrets_dir}/outputs.tf", "w") as f:
    f.write(sealed_secrets_outputs_tf)

print("✅ HashiCorp Vault Terraform Module Created")
print(f"   📂 Location: {vault_dir}/")
print(f"   🔧 Features: HA Raft storage, K8s auth, tenant policies, network isolation")

print("\n✅ Sealed Secrets Terraform Module Created")
print(f"   📂 Location: {sealed_secrets_dir}/")
print(f"   🔧 Features: Bitnami sealed secrets controller, GitOps-ready, usage docs")

secrets_modules = {
    "vault": vault_dir,
    "sealed_secrets": sealed_secrets_dir
}
