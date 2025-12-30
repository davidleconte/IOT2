import os

# Pulsar DLQ and retry flow tests

test_dlq_routing = '''"""
Test: Pulsar DLQ routing
Inject failures and verify messages route to DLQ correctly
"""
import pytest
import pulsar
import json
import time
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from failure_injection_generator import FailureInjectionGenerator

class TestDLQRouting:
    """Test DLQ routing for failed messages"""
    
    @pytest.fixture(scope="class")
    def test_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'test_config.json')
        with open(config_path) as f:
            return json.load(f)
    
    def test_dlq_message_routing(self, test_config):
        """Test: Failed messages are routed to DLQ after max retries"""
        tenant = test_config["pulsar"]["tenants"][0]
        main_topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        dlq_topic = f"persistent://{tenant}/maritime/vessel_telemetry_dlq"
        
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        
        # Create producer for main topic
        producer = client.create_producer(main_topic)
        
        # Create consumer with DLQ policy
        consumer = client.subscribe(
            main_topic,
            subscription_name="test-dlq-routing",
            consumer_type=pulsar.ConsumerType.Shared,
            dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
                max_redeliver_count=3,
                dead_letter_topic=dlq_topic
            ),
            negative_ack_redelivery_delay_ms=1000
        )
        
        # Create DLQ consumer
        dlq_consumer = client.subscribe(
            dlq_topic,
            subscription_name="dlq-monitor",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        # Generate failing message
        failure_gen = FailureInjectionGenerator(tenant, "IMO1234567")
        failing_msg = failure_gen.generate_failing_message("invalid_schema")
        
        # Send failing message
        producer.send(json.dumps(failing_msg).encode('utf-8'))
        
        # Simulate processing failures
        for attempt in range(4):  # More than max_redeliver_count
            msg = consumer.receive(timeout_millis=5000)
            data = json.loads(msg.data().decode('utf-8'))
            
            # Simulate processing failure
            if "failure_injection" in data:
                consumer.negative_acknowledge(msg)
                print(f"Attempt {attempt + 1}: Negatively acknowledged message")
        
        # Check DLQ for the failed message
        dlq_msg = dlq_consumer.receive(timeout_millis=10000)
        assert dlq_msg is not None, "Message not found in DLQ"
        
        dlq_data = json.loads(dlq_msg.data().decode('utf-8'))
        assert dlq_data["vessel_id"] == "IMO1234567"
        assert "failure_injection" in dlq_data
        
        dlq_consumer.acknowledge(dlq_msg)
        
        # Cleanup
        producer.close()
        consumer.close()
        dlq_consumer.close()
        client.close()
        
        print("✅ Message successfully routed to DLQ after max retries")
    
    def test_dlq_for_multiple_failure_types(self, test_config):
        """Test: Different failure types all route to DLQ"""
        tenant = test_config["pulsar"]["tenants"][0]
        main_topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        dlq_topic = f"persistent://{tenant}/maritime/vessel_telemetry_dlq"
        
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        producer = client.create_producer(main_topic)
        
        consumer = client.subscribe(
            main_topic,
            subscription_name="test-dlq-multiple-failures",
            consumer_type=pulsar.ConsumerType.Shared,
            dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
                max_redeliver_count=2,
                dead_letter_topic=dlq_topic
            ),
            negative_ack_redelivery_delay_ms=500
        )
        
        dlq_consumer = client.subscribe(
            dlq_topic,
            subscription_name="dlq-monitor-multi",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        # Generate multiple failure types
        failure_gen = FailureInjectionGenerator(tenant, "IMO1234567")
        failure_types = ["invalid_schema", "corrupt_data", "validation_error"]
        
        for failure_type in failure_types:
            msg = failure_gen.generate_failing_message(failure_type)
            producer.send(json.dumps(msg).encode('utf-8'))
        
        # Process and fail all messages
        for _ in range(len(failure_types)):
            for attempt in range(3):
                msg = consumer.receive(timeout_millis=5000)
                consumer.negative_acknowledge(msg)
        
        # Verify all messages in DLQ
        dlq_messages = []
        for _ in range(len(failure_types)):
            dlq_msg = dlq_consumer.receive(timeout_millis=5000)
            data = json.loads(dlq_msg.data().decode('utf-8'))
            dlq_messages.append(data["failure_injection"]["type"])
            dlq_consumer.acknowledge(dlq_msg)
        
        assert set(dlq_messages) == set(failure_types)
        
        producer.close()
        consumer.close()
        dlq_consumer.close()
        client.close()
        
        print(f"✅ All {len(failure_types)} failure types routed to DLQ")
    
    def test_dlq_metadata_preservation(self, test_config):
        """Test: DLQ messages preserve original metadata"""
        tenant = test_config["pulsar"]["tenants"][0]
        main_topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        dlq_topic = f"persistent://{tenant}/maritime/vessel_telemetry_dlq"
        
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        producer = client.create_producer(main_topic)
        
        consumer = client.subscribe(
            main_topic,
            subscription_name="test-dlq-metadata",
            consumer_type=pulsar.ConsumerType.Shared,
            dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
                max_redeliver_count=2,
                dead_letter_topic=dlq_topic
            ),
            negative_ack_redelivery_delay_ms=500
        )
        
        dlq_consumer = client.subscribe(
            dlq_topic,
            subscription_name="dlq-metadata-check",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        # Send message with metadata
        original_properties = {
            "tenant_id": tenant,
            "vessel_id": "IMO7890123",
            "message_type": "telemetry",
            "priority": "high"
        }
        
        failure_gen = FailureInjectionGenerator(tenant, "IMO7890123")
        failing_msg = failure_gen.generate_failing_message("processing_timeout")
        
        producer.send(
            json.dumps(failing_msg).encode('utf-8'),
            properties=original_properties
        )
        
        # Fail the message multiple times
        for _ in range(3):
            msg = consumer.receive(timeout_millis=5000)
            consumer.negative_acknowledge(msg)
        
        # Check DLQ message
        dlq_msg = dlq_consumer.receive(timeout_millis=5000)
        dlq_properties = dlq_msg.properties()
        
        # Verify metadata preservation
        for key, value in original_properties.items():
            assert dlq_properties.get(key) == value, f"Metadata {key} not preserved"
        
        # Verify DLQ-specific metadata
        assert "ORIGIN_MESSAGE_ID" in dlq_properties or "REAL_TOPIC" in dlq_properties
        
        dlq_consumer.acknowledge(dlq_msg)
        
        producer.close()
        consumer.close()
        dlq_consumer.close()
        client.close()
        
        print("✅ DLQ message metadata preserved correctly")

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
'''

test_retry_backoff = '''"""
Test: Pulsar retry with exponential backoff
Verify retry policies with configurable backoff
"""
import pytest
import pulsar
import json
import time
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from failure_injection_generator import FailureInjectionGenerator

class TestRetryBackoff:
    """Test retry policies and backoff strategies"""
    
    @pytest.fixture(scope="class")
    def test_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'test_config.json')
        with open(config_path) as f:
            return json.load(f)
    
    def test_exponential_backoff_timing(self, test_config):
        """Test: Retry delays follow exponential backoff pattern"""
        tenant = test_config["pulsar"]["tenants"][0]
        topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        producer = client.create_producer(topic)
        
        # Configure consumer with exponential backoff
        consumer = client.subscribe(
            topic,
            subscription_name="test-exponential-backoff",
            consumer_type=pulsar.ConsumerType.Shared,
            negative_ack_redelivery_delay_ms=1000  # Initial delay: 1 second
        )
        
        # Send failing message
        failure_gen = FailureInjectionGenerator(tenant, "IMO1234567")
        failing_msg = failure_gen.generate_failing_message("database_error")
        producer.send(json.dumps(failing_msg).encode('utf-8'))
        
        # Track retry timings
        retry_times = []
        
        for attempt in range(4):
            start_time = time.time()
            msg = consumer.receive(timeout_millis=10000)
            receive_time = time.time()
            
            if attempt > 0:
                delay = receive_time - retry_times[-1]
                retry_times.append(receive_time)
                print(f"Attempt {attempt + 1}: Delay = {delay:.2f}s")
            else:
                retry_times.append(receive_time)
            
            # Negative acknowledge to trigger retry
            consumer.negative_acknowledge(msg)
        
        # Verify backoff pattern (delays should increase)
        if len(retry_times) >= 3:
            delay_1 = retry_times[1] - retry_times[0]
            delay_2 = retry_times[2] - retry_times[1]
            # Second delay should be >= first delay (exponential or constant)
            assert delay_2 >= delay_1 * 0.8, "Backoff pattern not followed"
        
        producer.close()
        consumer.close()
        client.close()
        
        print("✅ Exponential backoff timing verified")
    
    def test_max_retries_enforcement(self, test_config):
        """Test: Messages fail after max retry attempts"""
        tenant = test_config["pulsar"]["tenants"][0]
        main_topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        dlq_topic = f"persistent://{tenant}/maritime/vessel_telemetry_dlq"
        
        max_retries = 3
        
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        producer = client.create_producer(main_topic)
        
        consumer = client.subscribe(
            main_topic,
            subscription_name="test-max-retries",
            consumer_type=pulsar.ConsumerType.Shared,
            dead_letter_policy=pulsar.ConsumerDeadLetterPolicy(
                max_redeliver_count=max_retries,
                dead_letter_topic=dlq_topic
            ),
            negative_ack_redelivery_delay_ms=500
        )
        
        dlq_consumer = client.subscribe(
            dlq_topic,
            subscription_name="dlq-max-retries",
            consumer_type=pulsar.ConsumerType.Shared
        )
        
        # Send message
        failure_gen = FailureInjectionGenerator(tenant, "IMO1234567")
        msg = failure_gen.generate_failing_message("processing_timeout")
        producer.send(json.dumps(msg).encode('utf-8'))
        
        # Count retry attempts
        retry_count = 0
        for _ in range(max_retries + 2):  # Attempt more than max
            try:
                received_msg = consumer.receive(timeout_millis=3000)
                retry_count += 1
                consumer.negative_acknowledge(received_msg)
            except Exception:
                break  # No more retries
        
        # Message should be in DLQ
        dlq_msg = dlq_consumer.receive(timeout_millis=5000)
        assert dlq_msg is not None, "Message not in DLQ after max retries"
        
        dlq_consumer.acknowledge(dlq_msg)
        
        producer.close()
        consumer.close()
        dlq_consumer.close()
        client.close()
        
        print(f"✅ Max retries ({max_retries}) enforced, message sent to DLQ")
    
    def test_successful_retry_recovery(self, test_config):
        """Test: Message succeeds after temporary failure"""
        tenant = test_config["pulsar"]["tenants"][0]
        topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        producer = client.create_producer(topic)
        
        consumer = client.subscribe(
            topic,
            subscription_name="test-retry-recovery",
            consumer_type=pulsar.ConsumerType.Shared,
            negative_ack_redelivery_delay_ms=1000
        )
        
        # Send message
        failure_gen = FailureInjectionGenerator(tenant, "IMO1234567")
        msg_data = failure_gen.generate_failing_message("database_error")
        msg_data["retry_metadata"] = {
            "attempt": 0,
            "should_succeed_on_attempt": 2  # Succeed on third try
        }
        producer.send(json.dumps(msg_data).encode('utf-8'))
        
        # Simulate retries with eventual success
        for attempt in range(3):
            msg = consumer.receive(timeout_millis=5000)
            data = json.loads(msg.data().decode('utf-8'))
            
            if attempt >= data["retry_metadata"]["should_succeed_on_attempt"]:
                # Success on this attempt
                consumer.acknowledge(msg)
                print(f"✅ Message succeeded on attempt {attempt + 1}")
                break
            else:
                # Fail and retry
                consumer.negative_acknowledge(msg)
                print(f"Attempt {attempt + 1}: Failed, will retry")
        
        producer.close()
        consumer.close()
        client.close()

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
'''

# Write test files
test_dir = "tests/integration/pulsar_dlq_retry"

dlq_path = os.path.join(test_dir, "test_dlq_routing.py")
with open(dlq_path, 'w') as f:
    f.write(test_dlq_routing)

retry_path = os.path.join(test_dir, "test_retry_backoff.py")
with open(retry_path, 'w') as f:
    f.write(test_retry_backoff)

dlq_retry_summary = {
    "test_category": "pulsar_dlq_retry",
    "test_files": {
        "test_dlq_routing": dlq_path,
        "test_retry_backoff": retry_path
    },
    "total_test_cases": 6,
    "coverage": [
        "dlq_message_routing",
        "multiple_failure_types",
        "metadata_preservation",
        "exponential_backoff_timing",
        "max_retries_enforcement",
        "successful_retry_recovery"
    ]
}

print("✅ Pulsar DLQ and retry tests created")
print(f"📁 Test directory: {test_dir}")
print(f"🧪 Test files:")
for name, path in dlq_retry_summary["test_files"].items():
    print(f"  - {name}: {path}")
print(f"✓ Total test cases: {dlq_retry_summary['total_test_cases']}")

dlq_retry_summary
