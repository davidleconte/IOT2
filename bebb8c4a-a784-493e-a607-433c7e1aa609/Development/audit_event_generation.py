import os
import json
import uuid
from datetime import datetime

# Audit event generation and streaming to Pulsar audit topics
# Comprehensive audit logging for compliance and security

audit_dir = os.path.join("security", "audit")
events_dir = os.path.join(audit_dir, "events")
schemas_dir = os.path.join(audit_dir, "schemas")

for dir_path in [audit_dir, events_dir, schemas_dir]:
    os.makedirs(dir_path, exist_ok=True)

# Audit event types and schemas
audit_event_types = {
    "authentication": {
        "event_name": "auth.login",
        "severity": "INFO",
        "fields": ["user_id", "tenant_id", "ip_address", "user_agent", "success", "failure_reason"],
        "retention_days": 90
    },
    "authorization": {
        "event_name": "authz.access",
        "severity": "INFO",
        "fields": ["user_id", "tenant_id", "resource_type", "resource_id", "action", "allowed", "deny_reason"],
        "retention_days": 90
    },
    "data_access": {
        "event_name": "data.access",
        "severity": "INFO",
        "fields": ["user_id", "tenant_id", "data_type", "operation", "record_count", "query_pattern"],
        "retention_days": 365,
        "compliance": ["GDPR"]
    },
    "data_modification": {
        "event_name": "data.modify",
        "severity": "WARNING",
        "fields": ["user_id", "tenant_id", "data_type", "operation", "record_ids", "before_hash", "after_hash"],
        "retention_days": 2555,
        "compliance": ["GDPR", "SOX"]
    },
    "data_deletion": {
        "event_name": "data.delete",
        "severity": "WARNING",
        "fields": ["user_id", "tenant_id", "data_type", "record_ids", "deletion_reason", "retention_override"],
        "retention_days": 2555,
        "compliance": ["GDPR"]
    },
    "admin_action": {
        "event_name": "admin.action",
        "severity": "WARNING",
        "fields": ["admin_user_id", "tenant_id", "action_type", "target_resource", "changes"],
        "retention_days": 2555
    },
    "security_event": {
        "event_name": "security.event",
        "severity": "CRITICAL",
        "fields": ["event_type", "source_ip", "user_id", "tenant_id", "threat_level", "details"],
        "retention_days": 2555
    },
    "compliance_event": {
        "event_name": "compliance.event",
        "severity": "INFO",
        "fields": ["compliance_type", "tenant_id", "event_description", "status"],
        "retention_days": 2555,
        "compliance": ["GDPR", "SOX", "HIPAA"]
    }
}

# Pulsar audit topic configuration
audit_topics_config = {
    "platform.audit.events": {
        "description": "Platform-level audit events",
        "partitions": 3,
        "retention_policy": "time",
        "retention_time_hours": 2160,  # 90 days
        "schema_type": "JSON",
        "deduplication_enabled": False
    },
    "platform.audit.authentication": {
        "description": "Authentication audit events",
        "partitions": 2,
        "retention_policy": "time",
        "retention_time_hours": 2160,
        "schema_type": "JSON"
    },
    "platform.audit.data_access": {
        "description": "Data access audit events for GDPR compliance",
        "partitions": 3,
        "retention_policy": "time",
        "retention_time_hours": 8760,  # 365 days
        "schema_type": "JSON",
        "compliance_tag": "GDPR"
    },
    "platform.audit.security": {
        "description": "Security-related audit events",
        "partitions": 2,
        "retention_policy": "time",
        "retention_time_hours": 61320,  # 7 years
        "schema_type": "JSON",
        "alert_enabled": True
    }
}

audit_topics_path = os.path.join(audit_dir, "pulsar_audit_topics.json")
with open(audit_topics_path, 'w') as f:
    json.dump(audit_topics_config, f, indent=2)

# Audit event schema (Avro-style for Pulsar)
audit_event_schema = {
    "type": "record",
    "name": "AuditEvent",
    "namespace": "com.maritime.platform.audit",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "event_name", "type": "string"},
        {"name": "timestamp", "type": "long"},
        {"name": "severity", "type": {"type": "enum", "name": "Severity", "symbols": ["INFO", "WARNING", "ERROR", "CRITICAL"]}},
        {"name": "user_id", "type": ["null", "string"], "default": None},
        {"name": "tenant_id", "type": ["null", "string"], "default": None},
        {"name": "ip_address", "type": ["null", "string"], "default": None},
        {"name": "service_name", "type": "string"},
        {"name": "action", "type": "string"},
        {"name": "resource_type", "type": ["null", "string"], "default": None},
        {"name": "resource_id", "type": ["null", "string"], "default": None},
        {"name": "result", "type": {"type": "enum", "name": "Result", "symbols": ["SUCCESS", "FAILURE", "DENIED"]}},
        {"name": "details", "type": {"type": "map", "values": "string"}},
        {"name": "compliance_tags", "type": {"type": "array", "items": "string"}}
    ]
}

schema_path = os.path.join(schemas_dir, "audit_event_schema.json")
with open(schema_path, 'w') as f:
    json.dump(audit_event_schema, f, indent=2)

# Python audit event generator service
audit_service_code = """
import json
import uuid
import time
from typing import Dict, Any, Optional
from pulsar import Client, Producer

class AuditEventGenerator:
    def __init__(self, pulsar_url: str = "pulsar://pulsar-broker:6650"):
        self.client = Client(pulsar_url)
        self.producers = {}
        
    def _get_producer(self, topic: str) -> Producer:
        if topic not in self.producers:
            self.producers[topic] = self.client.create_producer(
                topic=f"persistent://public/default/{topic}",
                producer_name=f"audit-generator-{uuid.uuid4().hex[:8]}"
            )
        return self.producers[topic]
    
    def generate_audit_event(
        self,
        event_type: str,
        event_name: str,
        severity: str,
        user_id: Optional[str],
        tenant_id: Optional[str],
        service_name: str,
        action: str,
        result: str,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        ip_address: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        compliance_tags: Optional[list] = None
    ) -> Dict[str, Any]:
        event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_name": event_name,
            "timestamp": int(time.time() * 1000),
            "severity": severity,
            "user_id": user_id,
            "tenant_id": tenant_id,
            "ip_address": ip_address,
            "service_name": service_name,
            "action": action,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "result": result,
            "details": details or {},
            "compliance_tags": compliance_tags or []
        }
        return event
    
    def send_audit_event(self, event: Dict[str, Any], topic: str = "platform.audit.events"):
        producer = self._get_producer(topic)
        producer.send(json.dumps(event).encode('utf-8'))
        
    def log_authentication(self, user_id: str, tenant_id: str, ip_address: str, success: bool, failure_reason: Optional[str] = None):
        event = self.generate_audit_event(
            event_type="authentication",
            event_name="auth.login",
            severity="INFO",
            user_id=user_id,
            tenant_id=tenant_id,
            ip_address=ip_address,
            service_name="auth-service",
            action="login",
            result="SUCCESS" if success else "FAILURE",
            details={"failure_reason": failure_reason} if failure_reason else {}
        )
        self.send_audit_event(event, "platform.audit.authentication")
    
    def log_data_access(self, user_id: str, tenant_id: str, data_type: str, operation: str, record_count: int):
        event = self.generate_audit_event(
            event_type="data_access",
            event_name="data.access",
            severity="INFO",
            user_id=user_id,
            tenant_id=tenant_id,
            service_name="data-service",
            action=operation,
            resource_type=data_type,
            result="SUCCESS",
            details={"record_count": str(record_count)},
            compliance_tags=["GDPR"]
        )
        self.send_audit_event(event, "platform.audit.data_access")
    
    def log_security_event(self, event_type: str, source_ip: str, user_id: str, tenant_id: str, threat_level: str, details: Dict[str, Any]):
        event = self.generate_audit_event(
            event_type="security_event",
            event_name="security.event",
            severity="CRITICAL",
            user_id=user_id,
            tenant_id=tenant_id,
            ip_address=source_ip,
            service_name="security-monitor",
            action=event_type,
            result="DENIED",
            details={**details, "threat_level": threat_level}
        )
        self.send_audit_event(event, "platform.audit.security")
    
    def close(self):
        for producer in self.producers.values():
            producer.close()
        self.client.close()
"""

audit_service_path = os.path.join(audit_dir, "audit_event_generator.py")
with open(audit_service_path, 'w') as f:
    f.write(audit_service_code)

# Example audit events
example_events = {
    "authentication_success": {
        "event_id": str(uuid.uuid4()),
        "event_type": "authentication",
        "event_name": "auth.login",
        "timestamp": int(datetime.now().timestamp() * 1000),
        "severity": "INFO",
        "user_id": "user-123",
        "tenant_id": "shipping-co-alpha",
        "ip_address": "192.168.1.100",
        "service_name": "auth-service",
        "action": "login",
        "result": "SUCCESS",
        "details": {"user_agent": "Mozilla/5.0"},
        "compliance_tags": []
    },
    "data_access": {
        "event_id": str(uuid.uuid4()),
        "event_type": "data_access",
        "event_name": "data.access",
        "timestamp": int(datetime.now().timestamp() * 1000),
        "severity": "INFO",
        "user_id": "user-123",
        "tenant_id": "shipping-co-alpha",
        "ip_address": "192.168.1.100",
        "service_name": "opensearch-api",
        "action": "SELECT",
        "resource_type": "vessel_telemetry",
        "resource_id": "IMO9876543",
        "result": "SUCCESS",
        "details": {"record_count": "150", "query_time_ms": "45"},
        "compliance_tags": ["GDPR"]
    },
    "security_threat": {
        "event_id": str(uuid.uuid4()),
        "event_type": "security_event",
        "event_name": "security.event",
        "timestamp": int(datetime.now().timestamp() * 1000),
        "severity": "CRITICAL",
        "user_id": "unknown",
        "tenant_id": None,
        "ip_address": "203.0.113.45",
        "service_name": "api-gateway",
        "action": "unauthorized_access_attempt",
        "result": "DENIED",
        "details": {"threat_level": "HIGH", "attack_type": "SQL_INJECTION"},
        "compliance_tags": []
    }
}

examples_path = os.path.join(events_dir, "example_audit_events.json")
with open(examples_path, 'w') as f:
    json.dump(example_events, f, indent=2)

print("✅ Audit Event Generation Configuration Complete")
print(f"\n📊 Audit Event Types: {len(audit_event_types)}")
for event_type, config in audit_event_types.items():
    print(f"  - {event_type}: {config['event_name']} (retention: {config['retention_days']} days)")
print(f"\n📨 Pulsar Audit Topics: {len(audit_topics_config)}")
for topic_name, config in audit_topics_config.items():
    print(f"  - {topic_name}: {config['description']}")
print(f"\n📝 Generated Files:")
print(f"  - Audit service: {audit_service_path}")
print(f"  - Event schema: {schema_path}")
print(f"  - Example events: {examples_path}")

audit_generation_summary = {
    "event_types": audit_event_types,
    "pulsar_topics": audit_topics_config,
    "event_schema": audit_event_schema,
    "example_events": example_events
}
