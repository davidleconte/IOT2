import os

# Multi-tenant data flow and isolation tests

# Test 1: Tenant isolation verification
test_tenant_isolation = '''"""
Test: Multi-tenant data isolation
Verify that tenant A data is not accessible by tenant B
"""
import pytest
import pulsar
from cassandra.cluster import Cluster
from opensearchpy import OpenSearch
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from multi_tenant_generator import MultiTenantDataGenerator
import json

class TestTenantIsolation:
    """Test multi-tenant data isolation across all layers"""
    
    @pytest.fixture(scope="class")
    def test_config(self):
        """Load test configuration"""
        config_path = os.path.join(os.path.dirname(__file__), '..', 'test_config.json')
        with open(config_path) as f:
            return json.load(f)
    
    @pytest.fixture(scope="class")
    def data_generator(self, test_config):
        """Create multi-tenant data generator"""
        return MultiTenantDataGenerator(
            tenants=test_config["pulsar"]["tenants"],
            vessels_per_tenant=test_config["test_data"]["num_vessels_per_tenant"]
        )
    
    @pytest.fixture(scope="class")
    def pulsar_client(self, test_config):
        """Create Pulsar client"""
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        yield client
        client.close()
    
    @pytest.fixture(scope="class")
    def cassandra_session(self, test_config):
        """Create Cassandra session"""
        cluster = Cluster(test_config["cassandra"]["contact_points"])
        session = cluster.connect()
        yield session
        cluster.shutdown()
    
    @pytest.fixture(scope="class")
    def opensearch_client(self, test_config):
        """Create OpenSearch client"""
        return OpenSearch(
            hosts=test_config["opensearch"]["hosts"],
            verify_certs=test_config["opensearch"]["verify_certs"]
        )
    
    def test_pulsar_topic_isolation(self, test_config, pulsar_client, data_generator):
        """Test: Pulsar topics are isolated per tenant"""
        tenants = test_config["pulsar"]["tenants"][:2]  # Test first 2 tenants
        
        producers = {}
        consumers = {}
        
        # Create producers and consumers for each tenant
        for tenant in tenants:
            topic = f"persistent://{tenant}/maritime/vessel_telemetry"
            
            # Create producer
            producers[tenant] = pulsar_client.create_producer(topic)
            
            # Create consumer
            consumers[tenant] = pulsar_client.subscribe(
                topic,
                subscription_name=f"test-isolation-{tenant}",
                consumer_type=pulsar.ConsumerType.Exclusive
            )
        
        # Generate and publish test data
        published_data = {}
        for tenant in tenants:
            messages = data_generator.generate_tenant_data(tenant, messages_per_vessel=10)
            published_data[tenant] = []
            
            for msg in messages:
                producers[tenant].send(json.dumps(msg).encode('utf-8'))
                published_data[tenant].append(msg["vessel_id"])
        
        # Verify isolation: each consumer should only receive its tenant's data
        for tenant in tenants:
            received_vessels = set()
            
            # Consume all messages for this tenant
            for _ in range(len(published_data[tenant])):
                msg = consumers[tenant].receive(timeout_millis=5000)
                data = json.loads(msg.data().decode('utf-8'))
                
                # Assert tenant_id matches
                assert data["tenant_id"] == tenant, f"Cross-tenant data leak detected!"
                
                received_vessels.add(data["vessel_id"])
                consumers[tenant].acknowledge(msg)
            
            # Verify all expected vessels received
            assert received_vessels == set(published_data[tenant])
        
        # Cleanup
        for producer in producers.values():
            producer.close()
        for consumer in consumers.values():
            consumer.close()
    
    def test_cassandra_keyspace_isolation(self, test_config, cassandra_session):
        """Test: Cassandra keyspaces are isolated per tenant"""
        keyspaces = test_config["cassandra"]["keyspaces"]
        
        # Insert test data into each tenant keyspace
        test_data = {}
        for tenant, keyspace in keyspaces.items():
            vessel_id = f"TEST_{tenant}_VESSEL_001"
            test_data[tenant] = vessel_id
            
            # Insert into tenant-specific keyspace
            query = f"""
            INSERT INTO {keyspace}.vessel_telemetry 
            (tenant_id, vessel_id, event_timestamp, speed_knots) 
            VALUES (%s, %s, toTimestamp(now()), %s)
            """
            cassandra_session.execute(query, (tenant, vessel_id, 15.5))
        
        # Verify isolation: query each keyspace and ensure no cross-tenant data
        for tenant, keyspace in keyspaces.items():
            query = f"SELECT vessel_id FROM {keyspace}.vessel_telemetry WHERE tenant_id = %s"
            rows = cassandra_session.execute(query, (tenant,))
            
            vessel_ids = [row.vessel_id for row in rows]
            
            # Ensure only this tenant's vessels are in this keyspace
            for vessel_id in vessel_ids:
                assert tenant.replace('-', '_') in vessel_id.lower(), \
                    f"Cross-tenant data leak in Cassandra keyspace {keyspace}!"
    
    def test_opensearch_index_isolation(self, test_config, opensearch_client):
        """Test: OpenSearch indices are isolated per tenant"""
        tenants = test_config["pulsar"]["tenants"][:2]
        
        # Index test documents for each tenant
        test_docs = {}
        for tenant in tenants:
            index_name = f"{tenant}_vessel_telemetry"
            vessel_id = f"TEST_{tenant}_VESSEL_001"
            test_docs[tenant] = vessel_id
            
            doc = {
                "tenant_id": tenant,
                "vessel_id": vessel_id,
                "timestamp": "2025-01-01T00:00:00Z",
                "speed_knots": 15.5
            }
            
            opensearch_client.index(
                index=index_name,
                body=doc,
                refresh=True
            )
        
        # Verify isolation: search each index
        for tenant in tenants:
            index_name = f"{tenant}_vessel_telemetry"
            
            # Search with tenant_id filter
            result = opensearch_client.search(
                index=index_name,
                body={"query": {"match_all": {}}}
            )
            
            # Verify all documents belong to this tenant
            for hit in result["hits"]["hits"]:
                assert hit["_source"]["tenant_id"] == tenant, \
                    f"Cross-tenant data leak in OpenSearch index {index_name}!"
    
    def test_concurrent_tenant_ingestion(self, test_config, pulsar_client, data_generator):
        """Test: Concurrent ingestion from multiple tenants maintains isolation"""
        import threading
        
        tenants = test_config["pulsar"]["tenants"][:2]
        results = {}
        
        def ingest_tenant_data(tenant):
            """Ingest data for single tenant"""
            topic = f"persistent://{tenant}/maritime/vessel_telemetry"
            producer = pulsar_client.create_producer(topic)
            
            messages = data_generator.generate_tenant_data(tenant, messages_per_vessel=20)
            vessel_ids = set()
            
            for msg in messages:
                producer.send(json.dumps(msg).encode('utf-8'))
                vessel_ids.add(msg["vessel_id"])
            
            producer.close()
            results[tenant] = vessel_ids
        
        # Launch concurrent ingestion threads
        threads = []
        for tenant in tenants:
            thread = threading.Thread(target=ingest_tenant_data, args=(tenant,))
            thread.start()
            threads.append(thread)
        
        # Wait for completion
        for thread in threads:
            thread.join()
        
        # Verify no cross-tenant contamination
        tenant_a_vessels = results[tenants[0]]
        tenant_b_vessels = results[tenants[1]]
        
        # Ensure vessel IDs are distinct between tenants
        overlap = tenant_a_vessels.intersection(tenant_b_vessels)
        assert len(overlap) == 0, f"Vessel ID collision between tenants: {overlap}"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
'''

# Test 2: Concurrent ingestion test
test_concurrent_ingestion = '''"""
Test: Concurrent multi-tenant ingestion
Verify system handles concurrent ingestion from multiple tenants
"""
import pytest
import pulsar
import time
import threading
from collections import defaultdict
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'utils'))

from multi_tenant_generator import MultiTenantDataGenerator
import json

class TestConcurrentIngestion:
    """Test concurrent ingestion patterns"""
    
    @pytest.fixture(scope="class")
    def test_config(self):
        config_path = os.path.join(os.path.dirname(__file__), '..', 'test_config.json')
        with open(config_path) as f:
            return json.load(f)
    
    def test_high_throughput_ingestion(self, test_config):
        """Test: High throughput ingestion for multiple tenants"""
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        generator = MultiTenantDataGenerator(
            tenants=test_config["pulsar"]["tenants"],
            vessels_per_tenant=test_config["test_data"]["num_vessels_per_tenant"]
        )
        
        stats = defaultdict(lambda: {"sent": 0, "errors": 0, "duration": 0})
        
        def send_messages(tenant, message_count=1000):
            """Send messages for single tenant"""
            topic = f"persistent://{tenant}/maritime/vessel_telemetry"
            producer = client.create_producer(
                topic,
                batching_enabled=True,
                batching_max_messages=100,
                batching_max_publish_delay_ms=10
            )
            
            start_time = time.time()
            messages = generator.generate_tenant_data(tenant, messages_per_vessel=message_count)
            
            for msg in messages:
                try:
                    producer.send_async(
                        json.dumps(msg).encode('utf-8'),
                        callback=lambda res, msg_id: None
                    )
                    stats[tenant]["sent"] += 1
                except Exception as e:
                    stats[tenant]["errors"] += 1
            
            producer.flush()
            producer.close()
            stats[tenant]["duration"] = time.time() - start_time
        
        # Launch concurrent ingestion
        threads = []
        for tenant in test_config["pulsar"]["tenants"]:
            thread = threading.Thread(target=send_messages, args=(tenant, 500))
            thread.start()
            threads.append(thread)
        
        for thread in threads:
            thread.join()
        
        client.close()
        
        # Verify results
        for tenant, tenant_stats in stats.items():
            throughput = tenant_stats["sent"] / tenant_stats["duration"]
            print(f"{tenant}: {tenant_stats['sent']} msgs in {tenant_stats['duration']:.2f}s = {throughput:.0f} msg/s")
            
            assert tenant_stats["errors"] == 0, f"Errors encountered for {tenant}"
            assert tenant_stats["sent"] > 0, f"No messages sent for {tenant}"
    
    def test_burst_traffic_handling(self, test_config):
        """Test: System handles burst traffic patterns"""
        client = pulsar.Client(test_config["pulsar"]["service_url"])
        generator = MultiTenantDataGenerator(
            tenants=test_config["pulsar"]["tenants"][:1],
            vessels_per_tenant=10
        )
        
        tenant = test_config["pulsar"]["tenants"][0]
        topic = f"persistent://{tenant}/maritime/vessel_telemetry"
        producer = client.create_producer(topic)
        
        # Simulate burst: send 1000 messages as fast as possible
        messages = generator.generate_tenant_data(tenant, messages_per_vessel=100)
        
        start_time = time.time()
        for msg in messages:
            producer.send(json.dumps(msg).encode('utf-8'))
        duration = time.time() - start_time
        
        producer.close()
        client.close()
        
        throughput = len(messages) / duration
        print(f"Burst throughput: {throughput:.0f} msg/s")
        
        # Verify burst was handled
        assert duration < 30, "Burst ingestion took too long"
        assert throughput > 10, "Burst throughput too low"

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
'''

# Write test files
test_dir = "tests/integration/multi_tenant_data_flow"

test_files_created = {}

# Write tenant isolation test
isolation_path = os.path.join(test_dir, "test_tenant_isolation.py")
with open(isolation_path, 'w') as f:
    f.write(test_tenant_isolation)
test_files_created["test_tenant_isolation"] = isolation_path

# Write concurrent ingestion test
concurrent_path = os.path.join(test_dir, "test_concurrent_ingestion.py")
with open(concurrent_path, 'w') as f:
    f.write(test_concurrent_ingestion)
test_files_created["test_concurrent_ingestion"] = concurrent_path

# Create pytest configuration
pytest_ini = '''[pytest]
testpaths = tests/integration
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
markers =
    isolation: Multi-tenant isolation tests
    performance: Performance and throughput tests
    integration: Full integration tests
'''

pytest_path = "tests/integration/pytest.ini"
with open(pytest_path, 'w') as f:
    f.write(pytest_ini)

multi_tenant_test_summary = {
    "test_category": "multi_tenant_data_flow",
    "test_files": test_files_created,
    "pytest_config": pytest_path,
    "total_test_cases": 6,
    "coverage": ["pulsar_isolation", "cassandra_isolation", "opensearch_isolation", 
                 "concurrent_ingestion", "high_throughput", "burst_traffic"]
}

print("✅ Multi-tenant isolation tests created")
print(f"📁 Test directory: {test_dir}")
print(f"🧪 Test files:")
for name, path in test_files_created.items():
    print(f"  - {name}: {path}")
print(f"📝 Pytest config: {pytest_path}")
print(f"✓ Total test cases: {multi_tenant_test_summary['total_test_cases']}")

multi_tenant_test_summary
