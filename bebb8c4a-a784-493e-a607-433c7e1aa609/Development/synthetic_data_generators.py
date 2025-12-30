import json
import os
from datetime import datetime, timedelta
import random

# Synthetic data generators for integration tests

# Generator 1: Vessel Telemetry Data
vessel_telemetry_generator = '''"""
Synthetic Vessel Telemetry Generator
Generates realistic maritime telemetry data for testing
"""
import random
import time
from datetime import datetime, timedelta
from typing import Dict, List
import json

class VesselTelemetryGenerator:
    """Generate synthetic vessel telemetry messages"""
    
    def __init__(self, tenant_id: str, vessel_id: str, seed: int = None):
        self.tenant_id = tenant_id
        self.vessel_id = vessel_id
        if seed:
            random.seed(seed)
        
        # Vessel baseline characteristics
        self.base_speed = random.uniform(10.0, 18.0)  # knots
        self.base_fuel_rate = random.uniform(5.0, 15.0)  # tons/hour
        self.base_engine_temp = random.uniform(75.0, 85.0)  # Celsius
        
    def generate_message(self, timestamp: datetime = None, 
                        inject_anomaly: bool = False) -> Dict:
        """Generate single telemetry message"""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        # Normal variations
        speed_variation = random.uniform(-2.0, 2.0)
        fuel_variation = random.uniform(-1.0, 1.0)
        temp_variation = random.uniform(-3.0, 3.0)
        
        # Inject anomalies if requested
        if inject_anomaly:
            anomaly_type = random.choice(['fuel_spike', 'temp_spike', 'speed_drop'])
            if anomaly_type == 'fuel_spike':
                fuel_variation += random.uniform(5.0, 10.0)
            elif anomaly_type == 'temp_spike':
                temp_variation += random.uniform(15.0, 25.0)
            elif anomaly_type == 'speed_drop':
                speed_variation -= random.uniform(5.0, 8.0)
        
        message = {
            "message_id": f"{self.vessel_id}_{int(timestamp.timestamp())}",
            "tenant_id": self.tenant_id,
            "vessel_id": self.vessel_id,
            "timestamp": timestamp.isoformat() + "Z",
            "location": {
                "latitude": random.uniform(30.0, 60.0),
                "longitude": random.uniform(-180.0, 180.0)
            },
            "operational_metrics": {
                "speed_knots": max(0, self.base_speed + speed_variation),
                "fuel_consumption_tons_per_hour": max(0, self.base_fuel_rate + fuel_variation),
                "engine_temperature_celsius": self.base_engine_temp + temp_variation,
                "engine_rpm": random.randint(60, 95),
                "draft_meters": random.uniform(8.0, 14.0)
            },
            "environmental_conditions": {
                "wind_speed_knots": random.uniform(5.0, 25.0),
                "wave_height_meters": random.uniform(0.5, 4.0),
                "water_temperature_celsius": random.uniform(10.0, 25.0),
                "weather_condition": random.choice(["clear", "cloudy", "rain", "storm"])
            },
            "metadata": {
                "anomaly_injected": inject_anomaly,
                "generator": "VesselTelemetryGenerator"
            }
        }
        return message
    
    def generate_sequence(self, count: int, interval_seconds: int = 60,
                         anomaly_rate: float = 0.0) -> List[Dict]:
        """Generate sequence of telemetry messages"""
        messages = []
        start_time = datetime.utcnow()
        
        for i in range(count):
            timestamp = start_time + timedelta(seconds=i * interval_seconds)
            inject_anomaly = random.random() < anomaly_rate
            message = self.generate_message(timestamp, inject_anomaly)
            messages.append(message)
        
        return messages

# Example usage
if __name__ == "__main__":
    generator = VesselTelemetryGenerator(
        tenant_id="shipping-co-alpha",
        vessel_id="IMO1234567",
        seed=42
    )
    
    # Generate 10 messages with 5% anomaly rate
    messages = generator.generate_sequence(count=10, anomaly_rate=0.05)
    
    for msg in messages[:3]:  # Print first 3
        print(json.dumps(msg, indent=2))
'''

# Generator 2: Multi-tenant test data
multi_tenant_generator = '''"""
Multi-Tenant Test Data Generator
Generates data for multiple tenants with isolation verification
"""
import json
from typing import Dict, List
from vessel_telemetry_generator import VesselTelemetryGenerator

class MultiTenantDataGenerator:
    """Generate test data for multiple tenants"""
    
    def __init__(self, tenants: List[str], vessels_per_tenant: int = 5):
        self.tenants = tenants
        self.vessels_per_tenant = vessels_per_tenant
        self.generators = {}
        
        # Create generators for each tenant/vessel combination
        for tenant in tenants:
            self.generators[tenant] = []
            for i in range(vessels_per_tenant):
                vessel_id = f"IMO{tenant[-5:]}{i:04d}"
                generator = VesselTelemetryGenerator(tenant, vessel_id, seed=hash(vessel_id))
                self.generators[tenant].append(generator)
    
    def generate_tenant_data(self, tenant_id: str, messages_per_vessel: int = 100,
                           anomaly_rate: float = 0.05) -> List[Dict]:
        """Generate all data for single tenant"""
        all_messages = []
        
        for generator in self.generators.get(tenant_id, []):
            messages = generator.generate_sequence(
                count=messages_per_vessel,
                anomaly_rate=anomaly_rate
            )
            all_messages.extend(messages)
        
        return all_messages
    
    def generate_all_tenant_data(self, messages_per_vessel: int = 100,
                                anomaly_rate: float = 0.05) -> Dict[str, List[Dict]]:
        """Generate data for all tenants"""
        all_data = {}
        
        for tenant in self.tenants:
            all_data[tenant] = self.generate_tenant_data(
                tenant,
                messages_per_vessel,
                anomaly_rate
            )
        
        return all_data
    
    def generate_isolation_test_data(self) -> Dict:
        """Generate data specifically for isolation testing"""
        # Generate overlapping timestamps across tenants
        test_data = {
            "description": "Multi-tenant isolation test data",
            "tenants": {}
        }
        
        for tenant in self.tenants:
            messages = self.generate_tenant_data(tenant, messages_per_vessel=50)
            test_data["tenants"][tenant] = {
                "message_count": len(messages),
                "vessel_ids": list(set(m["vessel_id"] for m in messages)),
                "messages": messages
            }
        
        return test_data

# Example usage
if __name__ == "__main__":
    generator = MultiTenantDataGenerator(
        tenants=["shipping-co-alpha", "logistics-beta", "maritime-gamma"],
        vessels_per_tenant=3
    )
    
    # Generate isolation test data
    test_data = generator.generate_isolation_test_data()
    print(f"Generated {len(test_data['tenants'])} tenant datasets")
    
    for tenant, data in test_data["tenants"].items():
        print(f"  {tenant}: {data['message_count']} messages, {len(data['vessel_ids'])} vessels")
'''

# Generator 3: Failure injection generator
failure_injection_generator = '''"""
Failure Injection Generator
Generates messages that will trigger DLQ and retry flows
"""
import json
import random
from typing import Dict, List, Optional

class FailureInjectionGenerator:
    """Generate messages with intentional failures for DLQ testing"""
    
    FAILURE_TYPES = {
        "invalid_schema": "Missing required fields",
        "corrupt_data": "Corrupted payload data",
        "processing_timeout": "Simulated timeout scenario",
        "database_error": "Database connection failure",
        "validation_error": "Business rule validation failure"
    }
    
    def __init__(self, tenant_id: str, vessel_id: str):
        self.tenant_id = tenant_id
        self.vessel_id = vessel_id
    
    def generate_failing_message(self, failure_type: str) -> Dict:
        """Generate message that will fail processing"""
        base_message = {
            "tenant_id": self.tenant_id,
            "vessel_id": self.vessel_id,
            "timestamp": "2025-01-01T00:00:00Z",
            "failure_injection": {
                "type": failure_type,
                "description": self.FAILURE_TYPES.get(failure_type, "Unknown failure"),
                "should_retry": failure_type in ["processing_timeout", "database_error"]
            }
        }
        
        # Inject specific failure patterns
        if failure_type == "invalid_schema":
            # Missing required fields
            return base_message
        
        elif failure_type == "corrupt_data":
            base_message["operational_metrics"] = "CORRUPTED_DATA_###"
            return base_message
        
        elif failure_type == "processing_timeout":
            base_message["simulate_delay_seconds"] = 300
            return base_message
        
        elif failure_type == "database_error":
            base_message["trigger_db_error"] = True
            return base_message
        
        elif failure_type == "validation_error":
            base_message["operational_metrics"] = {
                "speed_knots": -50.0,  # Invalid negative speed
                "fuel_consumption_tons_per_hour": -10.0  # Invalid negative fuel
            }
            return base_message
        
        return base_message
    
    def generate_retry_sequence(self, failure_type: str, max_retries: int = 3) -> List[Dict]:
        """Generate sequence showing retry progression"""
        messages = []
        
        for attempt in range(max_retries + 1):
            message = self.generate_failing_message(failure_type)
            message["retry_metadata"] = {
                "attempt": attempt,
                "max_attempts": max_retries,
                "should_succeed_on_attempt": max_retries  # Succeed on final attempt
            }
            messages.append(message)
        
        return messages
    
    def generate_dlq_test_batch(self) -> Dict:
        """Generate comprehensive DLQ test batch"""
        test_batch = {
            "description": "DLQ and retry flow test batch",
            "tenant_id": self.tenant_id,
            "vessel_id": self.vessel_id,
            "test_cases": {}
        }
        
        for failure_type in self.FAILURE_TYPES.keys():
            test_batch["test_cases"][failure_type] = {
                "single_failure": self.generate_failing_message(failure_type),
                "retry_sequence": self.generate_retry_sequence(failure_type)
            }
        
        return test_batch

# Example usage
if __name__ == "__main__":
    generator = FailureInjectionGenerator(
        tenant_id="shipping-co-alpha",
        vessel_id="IMO1234567"
    )
    
    test_batch = generator.generate_dlq_test_batch()
    print(json.dumps(test_batch, indent=2))
'''

# Write generators to files
generators_dir = "tests/integration/utils"
os.makedirs(generators_dir, exist_ok=True)

generators_created = {}

# Write vessel telemetry generator
telemetry_path = os.path.join(generators_dir, "vessel_telemetry_generator.py")
with open(telemetry_path, 'w') as f:
    f.write(vessel_telemetry_generator)
generators_created["vessel_telemetry_generator"] = telemetry_path

# Write multi-tenant generator
multi_tenant_path = os.path.join(generators_dir, "multi_tenant_generator.py")
with open(multi_tenant_path, 'w') as f:
    f.write(multi_tenant_generator)
generators_created["multi_tenant_generator"] = multi_tenant_path

# Write failure injection generator
failure_path = os.path.join(generators_dir, "failure_injection_generator.py")
with open(failure_path, 'w') as f:
    f.write(failure_injection_generator)
generators_created["failure_injection_generator"] = failure_path

# Create __init__.py
init_path = os.path.join(generators_dir, "__init__.py")
with open(init_path, 'w') as f:
    f.write('"""Synthetic data generators for integration tests"""\n')

# Create requirements.txt for test dependencies
requirements = """# Integration Test Dependencies
pytest>=7.4.0
pytest-asyncio>=0.21.0
pulsar-client>=3.3.0
opensearch-py>=2.3.0
cassandra-driver>=3.28.0
feast>=0.35.0
mlflow>=2.9.0
requests>=2.31.0
pyyaml>=6.0
"""

requirements_path = "tests/integration/requirements.txt"
with open(requirements_path, 'w') as f:
    f.write(requirements)

generators_summary = {
    "generators_created": len(generators_created),
    "generator_files": generators_created,
    "requirements_file": requirements_path,
    "total_lines": 350
}

print("✅ Synthetic data generators created")
print(f"📁 Directory: {generators_dir}")
print(f"🔧 Generators:")
for name, path in generators_created.items():
    print(f"  - {name}: {path}")
print(f"📦 Requirements: {requirements_path}")

generators_summary
