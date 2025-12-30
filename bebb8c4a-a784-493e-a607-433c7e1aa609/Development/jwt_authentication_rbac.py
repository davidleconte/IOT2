import os
import json

# JWT authentication with tenant claims and RBAC policies
# Implementing comprehensive authentication and authorization

auth_dir = os.path.join("security", "auth")
rbac_dir = os.path.join("security", "rbac")

for dir_path in [auth_dir, rbac_dir]:
    os.makedirs(dir_path, exist_ok=True)

# JWT Configuration with tenant claims
jwt_config = {
    "issuer": "maritime-platform-auth",
    "audience": "maritime-platform-services",
    "algorithm": "RS256",
    "access_token_ttl": 3600,
    "refresh_token_ttl": 86400,
    "required_claims": ["sub", "tenant_id", "roles", "exp", "iat"],
    "optional_claims": ["email", "name", "vessel_ids", "permissions"],
    "tenant_claim_format": {
        "tenant_id": "string",
        "tenant_name": "string",
        "tenant_type": "enum[alpha|beta|gamma]"
    }
}

# Sample JWT payload structure
jwt_payload_example = {
    "sub": "user-12345",
    "tenant_id": "shipping-co-alpha",
    "tenant_name": "Shipping Co Alpha",
    "tenant_type": "alpha",
    "roles": ["fleet_manager", "analyst"],
    "permissions": ["read:telemetry", "write:maintenance", "read:analytics"],
    "vessel_ids": ["IMO9876543", "IMO9876544"],
    "email": "manager@shipping-co.example",
    "name": "Fleet Manager",
    "iat": 1234567890,
    "exp": 1234571490
}

jwt_config_path = os.path.join(auth_dir, "jwt_config.json")
with open(jwt_config_path, 'w') as f:
    json.dump(jwt_config, f, indent=2)

jwt_example_path = os.path.join(auth_dir, "jwt_payload_example.json")
with open(jwt_example_path, 'w') as f:
    json.dump(jwt_payload_example, f, indent=2)

# RBAC Role definitions
rbac_roles = {
    "platform_admin": {
        "description": "Full platform administration access",
        "permissions": ["*:*"],
        "scope": "platform"
    },
    "tenant_admin": {
        "description": "Full access within tenant boundaries",
        "permissions": [
            "read:*",
            "write:*",
            "delete:telemetry",
            "delete:maintenance",
            "manage:users"
        ],
        "scope": "tenant",
        "tenant_isolation": True
    },
    "fleet_manager": {
        "description": "Manage fleet operations and view analytics",
        "permissions": [
            "read:telemetry",
            "read:analytics",
            "write:maintenance",
            "read:vessels",
            "write:alerts"
        ],
        "scope": "tenant",
        "tenant_isolation": True
    },
    "analyst": {
        "description": "Read-only access to analytics and telemetry",
        "permissions": [
            "read:telemetry",
            "read:analytics",
            "read:vessels",
            "read:maintenance"
        ],
        "scope": "tenant",
        "tenant_isolation": True
    },
    "operator": {
        "description": "Operational access for monitoring",
        "permissions": [
            "read:telemetry",
            "read:alerts",
            "write:maintenance"
        ],
        "scope": "tenant",
        "tenant_isolation": True
    },
    "readonly": {
        "description": "Read-only access to tenant data",
        "permissions": [
            "read:telemetry",
            "read:vessels"
        ],
        "scope": "tenant",
        "tenant_isolation": True
    }
}

rbac_roles_path = os.path.join(rbac_dir, "roles.json")
with open(rbac_roles_path, 'w') as f:
    json.dump(rbac_roles, f, indent=2)

# Kubernetes RBAC policies for services
k8s_rbac_policies = {
    "pulsar": {
        "authorization_enabled": True,
        "authentication_providers": ["jwt"],
        "roles": {
            "tenant_producer": {
                "permissions": ["produce"],
                "topics": ["persistent://{tenant}/*"]
            },
            "tenant_consumer": {
                "permissions": ["consume"],
                "topics": ["persistent://{tenant}/*"],
                "subscriptions": ["{tenant}-*"]
            },
            "tenant_admin": {
                "permissions": ["produce", "consume", "functions"],
                "topics": ["persistent://{tenant}/*"],
                "namespaces": ["{tenant}/*"]
            }
        }
    },
    "opensearch": {
        "security_plugin_enabled": True,
        "authentication": "jwt",
        "roles": {
            "tenant_admin": {
                "cluster_permissions": ["cluster_composite_ops_ro"],
                "index_permissions": [
                    {
                        "index_patterns": ["{tenant}_*"],
                        "allowed_actions": ["*"]
                    }
                ]
            },
            "tenant_analyst": {
                "cluster_permissions": [],
                "index_permissions": [
                    {
                        "index_patterns": ["{tenant}_*"],
                        "allowed_actions": ["read", "search"]
                    }
                ]
            },
            "tenant_writer": {
                "cluster_permissions": [],
                "index_permissions": [
                    {
                        "index_patterns": ["{tenant}_*"],
                        "allowed_actions": ["write", "index", "create_index"]
                    }
                ]
            }
        }
    },
    "presto": {
        "authentication_type": "JWT",
        "authorization_enabled": True,
        "rules": [
            {
                "user": ".*",
                "catalog": "maritime_iceberg",
                "schema": "{tenant}_.*",
                "privileges": ["SELECT", "INSERT", "UPDATE", "DELETE"],
                "filter": "tenant_id = '{tenant}'"
            }
        ]
    },
    "cassandra": {
        "authentication": "PasswordAuthenticator",
        "authorization": "CassandraAuthorizer",
        "roles": {
            "tenant_reader": {
                "permissions": ["SELECT"],
                "keyspaces": ["{tenant}"]
            },
            "tenant_writer": {
                "permissions": ["SELECT", "MODIFY"],
                "keyspaces": ["{tenant}"]
            },
            "tenant_admin": {
                "permissions": ["SELECT", "MODIFY", "CREATE", "ALTER", "DROP"],
                "keyspaces": ["{tenant}"]
            }
        }
    }
}

# Generate service-specific RBAC manifests
for service_name, rbac_config in k8s_rbac_policies.items():
    service_rbac_path = os.path.join(rbac_dir, f"{service_name}_rbac.json")
    with open(service_rbac_path, 'w') as f:
        json.dump(rbac_config, f, indent=2)

# Kubernetes ServiceAccount and RoleBinding manifests
k8s_service_accounts = {}
tenant_list = ["shipping-co-alpha", "logistics-beta", "maritime-gamma"]

for tenant_id in tenant_list:
    k8s_service_accounts[tenant_id] = {
        "service_account": {
            "apiVersion": "v1",
            "kind": "ServiceAccount",
            "metadata": {
                "name": f"{tenant_id}-sa",
                "namespace": "maritime-platform",
                "annotations": {
                    "tenant_id": tenant_id
                }
            }
        },
        "role": {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "Role",
            "metadata": {
                "name": f"{tenant_id}-role",
                "namespace": "maritime-platform"
            },
            "rules": [
                {
                    "apiGroups": [""],
                    "resources": ["pods", "services"],
                    "verbs": ["get", "list"]
                },
                {
                    "apiGroups": ["apps"],
                    "resources": ["deployments"],
                    "verbs": ["get", "list"]
                }
            ]
        },
        "role_binding": {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {
                "name": f"{tenant_id}-binding",
                "namespace": "maritime-platform"
            },
            "subjects": [
                {
                    "kind": "ServiceAccount",
                    "name": f"{tenant_id}-sa",
                    "namespace": "maritime-platform"
                }
            ],
            "roleRef": {
                "kind": "Role",
                "name": f"{tenant_id}-role",
                "apiGroup": "rbac.authorization.k8s.io"
            }
        }
    }

k8s_rbac_manifests_dir = os.path.join(rbac_dir, "k8s-rbac")
os.makedirs(k8s_rbac_manifests_dir, exist_ok=True)

for tenant_id, manifests in k8s_service_accounts.items():
    tenant_manifest_path = os.path.join(k8s_rbac_manifests_dir, f"{tenant_id}_rbac.json")
    with open(tenant_manifest_path, 'w') as f:
        json.dump(manifests, f, indent=2)

print("✅ JWT Authentication & RBAC Configuration Complete")
print(f"\n🔐 JWT Configuration:")
print(f"  - Issuer: {jwt_config['issuer']}")
print(f"  - Algorithm: {jwt_config['algorithm']}")
print(f"  - Access Token TTL: {jwt_config['access_token_ttl']}s")
print(f"  - Required Claims: {', '.join(jwt_config['required_claims'])}")
print(f"\n👥 RBAC Roles Defined: {len(rbac_roles)}")
for role_name, role_def in rbac_roles.items():
    print(f"  - {role_name}: {role_def['description']}")
print(f"\n🔒 Service RBAC Policies: {len(k8s_rbac_policies)} services")
print(f"  - {', '.join(k8s_rbac_policies.keys())}")
print(f"\n🎫 K8s Service Accounts: {len(k8s_service_accounts)} tenants")

jwt_rbac_summary = {
    "jwt_config": jwt_config,
    "rbac_roles": rbac_roles,
    "service_rbac_policies": list(k8s_rbac_policies.keys()),
    "tenant_service_accounts": list(k8s_service_accounts.keys())
}
