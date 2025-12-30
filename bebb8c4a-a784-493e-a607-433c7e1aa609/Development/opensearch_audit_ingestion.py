import os
import json

# OpenSearch audit log ingestion with retention policies
# Consuming from Pulsar audit topics and indexing to OpenSearch

opensearch_audit_dir = os.path.join("security", "audit", "opensearch")
consumers_dir = os.path.join(opensearch_audit_dir, "consumers")

for dir_path in [opensearch_audit_dir, consumers_dir]:
    os.makedirs(dir_path, exist_ok=True)

# OpenSearch index templates for audit logs
audit_index_templates = {
    "audit_events": {
        "index_patterns": ["audit-events-*"],
        "template": {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 2,
                "index.lifecycle.name": "audit_events_policy",
                "index.lifecycle.rollover_alias": "audit-events"
            },
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "event_type": {"type": "keyword"},
                    "event_name": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "severity": {"type": "keyword"},
                    "user_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "ip_address": {"type": "ip"},
                    "service_name": {"type": "keyword"},
                    "action": {"type": "keyword"},
                    "resource_type": {"type": "keyword"},
                    "resource_id": {"type": "keyword"},
                    "result": {"type": "keyword"},
                    "details": {"type": "object", "enabled": True},
                    "compliance_tags": {"type": "keyword"}
                }
            }
        }
    },
    "audit_authentication": {
        "index_patterns": ["audit-authentication-*"],
        "template": {
            "settings": {
                "number_of_shards": 2,
                "number_of_replicas": 2,
                "index.lifecycle.name": "audit_auth_policy",
                "index.lifecycle.rollover_alias": "audit-authentication"
            },
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "ip_address": {"type": "ip"},
                    "result": {"type": "keyword"},
                    "details": {"type": "object"}
                }
            }
        }
    },
    "audit_data_access": {
        "index_patterns": ["audit-data-access-*"],
        "template": {
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 2,
                "index.lifecycle.name": "audit_data_access_policy",
                "index.lifecycle.rollover_alias": "audit-data-access"
            },
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "user_id": {"type": "keyword"},
                    "tenant_id": {"type": "keyword"},
                    "resource_type": {"type": "keyword"},
                    "resource_id": {"type": "keyword"},
                    "action": {"type": "keyword"},
                    "result": {"type": "keyword"},
                    "details": {"type": "object"},
                    "compliance_tags": {"type": "keyword"}
                }
            }
        }
    }
}

# ILM policies for audit log retention
ilm_policies = {
    "audit_events_policy": {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "7d",
                            "max_size": "50gb"
                        }
                    }
                },
                "warm": {
                    "min_age": "30d",
                    "actions": {
                        "readonly": {}
                    }
                },
                "delete": {
                    "min_age": "90d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    },
    "audit_auth_policy": {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "7d",
                            "max_size": "30gb"
                        }
                    }
                },
                "warm": {
                    "min_age": "30d",
                    "actions": {
                        "readonly": {}
                    }
                },
                "delete": {
                    "min_age": "90d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    },
    "audit_data_access_policy": {
        "policy": {
            "phases": {
                "hot": {
                    "min_age": "0ms",
                    "actions": {
                        "rollover": {
                            "max_age": "7d",
                            "max_size": "50gb"
                        }
                    }
                },
                "warm": {
                    "min_age": "90d",
                    "actions": {
                        "readonly": {}
                    }
                },
                "delete": {
                    "min_age": "365d",
                    "actions": {
                        "delete": {}
                    }
                }
            }
        }
    }
}

# Save index templates
templates_dir = os.path.join(opensearch_audit_dir, "index-templates")
os.makedirs(templates_dir, exist_ok=True)

for template_name, template_config in audit_index_templates.items():
    template_path = os.path.join(templates_dir, f"{template_name}_template.json")
    with open(template_path, 'w') as f:
        json.dump(template_config, f, indent=2)

# Save ILM policies
ilm_dir = os.path.join(opensearch_audit_dir, "ilm-policies")
os.makedirs(ilm_dir, exist_ok=True)

for policy_name, policy_config in ilm_policies.items():
    policy_path = os.path.join(ilm_dir, f"{policy_name}.json")
    with open(policy_path, 'w') as f:
        json.dump(policy_config, f, indent=2)

# Pulsar consumer service for audit log ingestion
audit_consumer_service = """
import json
import logging
from typing import Dict, Any
from pulsar import Client, Consumer
from opensearchpy import OpenSearch, helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AuditLogIngestionService:
    def __init__(
        self,
        pulsar_url: str = "pulsar://pulsar-broker:6650",
        opensearch_hosts: list = ["opensearch-node:9200"]
    ):
        self.pulsar_client = Client(pulsar_url)
        self.opensearch_client = OpenSearch(
            hosts=opensearch_hosts,
            http_auth=('admin', 'admin'),
            use_ssl=True,
            verify_certs=False
        )
        self.consumers = {}
        
    def create_consumer(self, topic: str, subscription: str) -> Consumer:
        consumer = self.pulsar_client.subscribe(
            topic=f"persistent://public/default/{topic}",
            subscription_name=subscription,
            consumer_type="Shared"
        )
        return consumer
    
    def index_to_opensearch(self, index_name: str, document: Dict[str, Any]):
        try:
            self.opensearch_client.index(
                index=index_name,
                body=document,
                id=document.get('event_id')
            )
            logger.info(f"Indexed audit event {document.get('event_id')} to {index_name}")
        except Exception as e:
            logger.error(f"Failed to index document: {e}")
    
    def consume_audit_events(self, topic: str, index_alias: str):
        consumer = self.create_consumer(topic, f"{topic}-opensearch-consumer")
        logger.info(f"Starting consumption from {topic} to {index_alias}")
        
        while True:
            try:
                msg = consumer.receive(timeout_millis=5000)
                try:
                    event = json.loads(msg.data().decode('utf-8'))
                    self.index_to_opensearch(index_alias, event)
                    consumer.acknowledge(msg)
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
                    consumer.negative_acknowledge(msg)
            except Exception as e:
                logger.debug(f"No message received: {e}")
    
    def start_all_consumers(self):
        import threading
        
        consumers_config = [
            ("platform.audit.events", "audit-events"),
            ("platform.audit.authentication", "audit-authentication"),
            ("platform.audit.data_access", "audit-data-access"),
            ("platform.audit.security", "audit-events")
        ]
        
        threads = []
        for topic, index_alias in consumers_config:
            thread = threading.Thread(
                target=self.consume_audit_events,
                args=(topic, index_alias),
                daemon=True
            )
            thread.start()
            threads.append(thread)
            logger.info(f"Started consumer thread for {topic}")
        
        for thread in threads:
            thread.join()
    
    def close(self):
        self.pulsar_client.close()
        self.opensearch_client.close()

if __name__ == "__main__":
    service = AuditLogIngestionService()
    try:
        service.start_all_consumers()
    except KeyboardInterrupt:
        logger.info("Shutting down audit log ingestion service")
        service.close()
"""

consumer_service_path = os.path.join(consumers_dir, "audit_log_consumer.py")
with open(consumer_service_path, 'w') as f:
    f.write(audit_consumer_service)

# Dockerfile for the consumer service
dockerfile_content = """FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \\
    pulsar-client==3.4.0 \\
    opensearch-py==2.4.2

COPY audit_log_consumer.py .

CMD ["python", "audit_log_consumer.py"]
"""

dockerfile_path = os.path.join(consumers_dir, "Dockerfile")
with open(dockerfile_path, 'w') as f:
    f.write(dockerfile_content)

# Kubernetes deployment for the consumer
k8s_deployment = {
    "apiVersion": "apps/v1",
    "kind": "Deployment",
    "metadata": {
        "name": "audit-log-consumer",
        "namespace": "maritime-platform"
    },
    "spec": {
        "replicas": 2,
        "selector": {
            "matchLabels": {
                "app": "audit-log-consumer"
            }
        },
        "template": {
            "metadata": {
                "labels": {
                    "app": "audit-log-consumer"
                }
            },
            "spec": {
                "containers": [{
                    "name": "consumer",
                    "image": "maritime-platform/audit-log-consumer:latest",
                    "env": [
                        {
                            "name": "PULSAR_URL",
                            "value": "pulsar://pulsar-broker:6650"
                        },
                        {
                            "name": "OPENSEARCH_HOSTS",
                            "value": "opensearch-node:9200"
                        }
                    ]
                }]
            }
        }
    }
}

k8s_deployment_path = os.path.join(opensearch_audit_dir, "k8s_deployment.json")
with open(k8s_deployment_path, 'w') as f:
    json.dump(k8s_deployment, f, indent=2)

print("✅ OpenSearch Audit Ingestion Configuration Complete")
print(f"\n📑 Index Templates: {len(audit_index_templates)}")
for template_name in audit_index_templates.keys():
    print(f"  - {template_name}")
print(f"\n⏰ ILM Policies: {len(ilm_policies)}")
for policy_name, policy_config in ilm_policies.items():
    retention_days = policy_config['policy']['phases']['delete']['min_age']
    print(f"  - {policy_name}: {retention_days} retention")
print(f"\n📝 Generated Files:")
print(f"  - Consumer service: {consumer_service_path}")
print(f"  - Dockerfile: {dockerfile_path}")
print(f"  - K8s deployment: {k8s_deployment_path}")

opensearch_ingestion_summary = {
    "index_templates": list(audit_index_templates.keys()),
    "ilm_policies": list(ilm_policies.keys()),
    "consumer_service": consumer_service_path,
    "deployment": k8s_deployment_path
}
