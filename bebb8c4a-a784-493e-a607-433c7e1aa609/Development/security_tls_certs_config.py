import os
import json

# TLS and mutual TLS configuration for all services
# Comprehensive security layer implementation

security_base = "security"
tls_dir = os.path.join(security_base, "tls")
certs_dir = os.path.join(tls_dir, "certs")
ca_dir = os.path.join(tls_dir, "ca")
policies_dir = os.path.join(security_base, "policies")

for dir_path in [security_base, tls_dir, certs_dir, ca_dir, policies_dir]:
    os.makedirs(dir_path, exist_ok=True)

# Root CA configuration for the platform
root_ca_config = {
    "ca_name": "maritime-platform-root-ca",
    "validity_days": 3650,
    "key_algorithm": "RSA",
    "key_size": 4096,
    "common_name": "Maritime IoT Platform Root CA",
    "organization": "Maritime Platform",
    "organizational_unit": "Security",
    "country": "US",
    "usage": ["cert_signing", "crl_signing"]
}

# Service-specific TLS configurations
service_tls_configs = {
    "pulsar": {
        "mutual_tls_enabled": True,
        "tls_port": 6651,
        "https_port": 8443,
        "cert_validity_days": 365,
        "components": ["broker", "bookkeeper", "zookeeper", "proxy"],
        "cert_requirements": {
            "key_algorithm": "RSA",
            "key_size": 2048,
            "signature_algorithm": "SHA256WithRSA"
        }
    },
    "cassandra": {
        "mutual_tls_enabled": True,
        "native_transport_port_ssl": 9042,
        "cert_validity_days": 365,
        "components": ["node", "client"],
        "internode_encryption": "all",
        "client_encryption_enabled": True
    },
    "opensearch": {
        "mutual_tls_enabled": True,
        "https_port": 9200,
        "cert_validity_days": 365,
        "components": ["node", "client", "admin"],
        "node_to_node_encryption": True,
        "rest_layer_tls": True
    },
    "presto": {
        "mutual_tls_enabled": False,
        "https_port": 8443,
        "cert_validity_days": 365,
        "internal_communication_tls": True
    },
    "watsonx": {
        "mutual_tls_enabled": False,
        "https_port": 443,
        "cert_validity_days": 365
    },
    "mlflow": {
        "mutual_tls_enabled": False,
        "https_port": 5000,
        "cert_validity_days": 365
    },
    "feast": {
        "mutual_tls_enabled": False,
        "grpc_port": 6566,
        "cert_validity_days": 365
    }
}

# Certificate manager script for automated cert generation
cert_manager_script = """#!/bin/bash
# Certificate Generation and Management Script
# Generates all required TLS certificates for the maritime platform

set -e

CA_DIR="./ca"
CERTS_DIR="./certs"
VALIDITY_DAYS=365

# Generate Root CA
generate_root_ca() {
    echo "Generating Root CA..."
    openssl genrsa -out $CA_DIR/ca-key.pem 4096
    openssl req -new -x509 -days 3650 -key $CA_DIR/ca-key.pem \\
        -out $CA_DIR/ca-cert.pem \\
        -subj "/CN=Maritime IoT Platform Root CA/O=Maritime Platform/OU=Security/C=US"
}

# Generate service certificate
generate_service_cert() {
    local service=$1
    local component=$2
    local cn="${service}-${component}"
    
    echo "Generating certificate for ${cn}..."
    
    # Generate private key
    openssl genrsa -out $CERTS_DIR/${cn}-key.pem 2048
    
    # Generate CSR
    openssl req -new -key $CERTS_DIR/${cn}-key.pem \\
        -out $CERTS_DIR/${cn}.csr \\
        -subj "/CN=${cn}.maritime-platform.svc.cluster.local/O=Maritime Platform/C=US"
    
    # Sign with CA
    openssl x509 -req -in $CERTS_DIR/${cn}.csr \\
        -CA $CA_DIR/ca-cert.pem -CAkey $CA_DIR/ca-key.pem \\
        -CAcreateserial -out $CERTS_DIR/${cn}-cert.pem \\
        -days $VALIDITY_DAYS -sha256
    
    # Clean up CSR
    rm $CERTS_DIR/${cn}.csr
}

# Generate tenant-specific client certificates
generate_tenant_cert() {
    local tenant=$1
    
    echo "Generating client certificate for tenant ${tenant}..."
    
    openssl genrsa -out $CERTS_DIR/tenant-${tenant}-key.pem 2048
    openssl req -new -key $CERTS_DIR/tenant-${tenant}-key.pem \\
        -out $CERTS_DIR/tenant-${tenant}.csr \\
        -subj "/CN=tenant-${tenant}/O=Maritime Platform/OU=Tenants/C=US"
    
    openssl x509 -req -in $CERTS_DIR/tenant-${tenant}.csr \\
        -CA $CA_DIR/ca-cert.pem -CAkey $CA_DIR/ca-key.pem \\
        -CAcreateserial -out $CERTS_DIR/tenant-${tenant}-cert.pem \\
        -days $VALIDITY_DAYS -sha256
    
    rm $CERTS_DIR/tenant-${tenant}.csr
}

# Main execution
mkdir -p $CA_DIR $CERTS_DIR

if [ ! -f "$CA_DIR/ca-cert.pem" ]; then
    generate_root_ca
fi

# Generate Pulsar certificates
for component in broker bookkeeper zookeeper proxy; do
    generate_service_cert "pulsar" "$component"
done

# Generate Cassandra certificates
for component in node client; do
    generate_service_cert "cassandra" "$component"
done

# Generate OpenSearch certificates
for component in node client admin; do
    generate_service_cert "opensearch" "$component"
done

# Generate other service certificates
for service in presto watsonx mlflow feast; do
    generate_service_cert "$service" "server"
done

# Generate tenant certificates
for tenant in shipping-co-alpha logistics-beta maritime-gamma; do
    generate_tenant_cert "$tenant"
done

echo "Certificate generation complete!"
echo "Certificates stored in: $CERTS_DIR"
echo "Root CA stored in: $CA_DIR"
"""

cert_manager_path = os.path.join(tls_dir, "generate_certs.sh")
with open(cert_manager_path, 'w') as f:
    f.write(cert_manager_script)

# Kubernetes TLS secret manifests for each service
k8s_tls_secrets = {
    "pulsar-tls": {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "pulsar-tls-certs",
            "namespace": "pulsar"
        },
        "type": "kubernetes.io/tls",
        "data": {
            "ca.crt": "<<BASE64_ENCODED_CA_CERT>>",
            "tls.crt": "<<BASE64_ENCODED_BROKER_CERT>>",
            "tls.key": "<<BASE64_ENCODED_BROKER_KEY>>"
        }
    },
    "cassandra-tls": {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "cassandra-tls-certs",
            "namespace": "cassandra"
        },
        "type": "kubernetes.io/tls",
        "data": {
            "ca.crt": "<<BASE64_ENCODED_CA_CERT>>",
            "tls.crt": "<<BASE64_ENCODED_NODE_CERT>>",
            "tls.key": "<<BASE64_ENCODED_NODE_KEY>>",
            "client.crt": "<<BASE64_ENCODED_CLIENT_CERT>>",
            "client.key": "<<BASE64_ENCODED_CLIENT_KEY>>"
        }
    },
    "opensearch-tls": {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": "opensearch-tls-certs",
            "namespace": "opensearch"
        },
        "type": "kubernetes.io/tls",
        "data": {
            "ca.crt": "<<BASE64_ENCODED_CA_CERT>>",
            "node.crt": "<<BASE64_ENCODED_NODE_CERT>>",
            "node.key": "<<BASE64_ENCODED_NODE_KEY>>",
            "admin.crt": "<<BASE64_ENCODED_ADMIN_CERT>>",
            "admin.key": "<<BASE64_ENCODED_ADMIN_KEY>>"
        }
    }
}

k8s_secrets_dir = os.path.join(security_base, "k8s-secrets")
os.makedirs(k8s_secrets_dir, exist_ok=True)

for secret_name, secret_config in k8s_tls_secrets.items():
    secret_path = os.path.join(k8s_secrets_dir, f"{secret_name}.yaml")
    with open(secret_path, 'w') as f:
        json.dump(secret_config, f, indent=2)

print("✅ TLS Configuration Complete")
print(f"Root CA Config: {root_ca_config}")
print(f"\n📁 Generated Files:")
print(f"  - Certificate manager: {cert_manager_path}")
print(f"  - K8s TLS secrets: {len(k8s_tls_secrets)} manifests")
print(f"\n🔐 Services with Mutual TLS:")
for service, config in service_tls_configs.items():
    if config.get('mutual_tls_enabled'):
        print(f"  - {service}: {', '.join(config['components'])}")

tls_configuration = {
    "root_ca": root_ca_config,
    "services": service_tls_configs,
    "cert_manager_script": cert_manager_path,
    "k8s_secrets": list(k8s_tls_secrets.keys())
}
