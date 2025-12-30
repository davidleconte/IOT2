import os

# Create metrics collector service that pulls metrics from Prometheus endpoints
metrics_collector_code = '''"""
Metrics Collector Service
Collects resource consumption metrics from multiple services and attributes them to tenants
"""
import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any
import aiohttp
from prometheus_client.parser import text_string_to_metric_families
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MetricsCollector:
    """Collects metrics from Prometheus endpoints and attributes to tenants"""
    
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        with open('monitoring/metrics-collection/metrics_sources.json', 'r') as f:
            self.sources = json.load(f)
        
        with open('monitoring/metrics-collection/tenant_mapping.json', 'r') as f:
            self.tenant_mapping = json.load(f)
        
        self.session = None
    
    async def initialize(self):
        """Initialize HTTP session"""
        self.session = aiohttp.ClientSession()
    
    async def close(self):
        """Close HTTP session"""
        if self.session:
            await self.session.close()
    
    async def collect_prometheus_metrics(self, endpoint: str) -> List[Dict]:
        """Collect metrics from Prometheus endpoint"""
        try:
            async with self.session.get(endpoint) as response:
                if response.status == 200:
                    text = await response.text()
                    return self._parse_prometheus_metrics(text)
                else:
                    logger.error(f"Failed to collect from {endpoint}: {response.status}")
                    return []
        except Exception as e:
            logger.error(f"Error collecting from {endpoint}: {e}")
            return []
    
    def _parse_prometheus_metrics(self, metrics_text: str) -> List[Dict]:
        """Parse Prometheus text format metrics"""
        parsed_metrics = []
        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                parsed_metrics.append({
                    'metric_name': sample.name,
                    'labels': sample.labels,
                    'value': sample.value,
                    'timestamp': datetime.utcnow().isoformat()
                })
        return parsed_metrics
    
    async def collect_pulsar_metrics(self) -> Dict[str, List[Dict]]:
        """Collect Pulsar topic metrics and attribute to tenants"""
        source = self.sources['pulsar']
        raw_metrics = await self.collect_prometheus_metrics(source['prometheus_endpoint'])
        
        tenant_metrics = {tenant: [] for tenant in self.tenant_mapping.keys()}
        
        for metric_def in source['metrics']:
            matching_metrics = [m for m in raw_metrics if m['metric_name'] == metric_def['metric']]
            
            for metric in matching_metrics:
                tenant_label = metric['labels'].get('tenant')
                if tenant_label in tenant_metrics:
                    tenant_metrics[tenant_label].append({
                        'metric': metric_def['name'],
                        'value': metric['value'],
                        'unit': metric_def['unit'],
                        'labels': metric['labels'],
                        'timestamp': metric['timestamp']
                    })
        
        return tenant_metrics
    
    async def collect_cassandra_metrics(self) -> Dict[str, List[Dict]]:
        """Collect Cassandra/HCD metrics and attribute to tenants"""
        source = self.sources['cassandra_hcd']
        raw_metrics = await self.collect_prometheus_metrics(source['prometheus_endpoint'])
        
        tenant_metrics = {tenant: [] for tenant in self.tenant_mapping.keys()}
        
        for metric_def in source['metrics']:
            matching_metrics = [m for m in raw_metrics if m['metric_name'] == metric_def['metric']]
            
            for metric in matching_metrics:
                keyspace = metric['labels'].get('keyspace')
                
                # Map keyspace to tenant
                for tenant, mapping in self.tenant_mapping.items():
                    if keyspace in mapping['cassandra_keyspaces']:
                        tenant_metrics[tenant].append({
                            'metric': metric_def['name'],
                            'value': metric['value'],
                            'unit': metric_def['unit'],
                            'labels': metric['labels'],
                            'timestamp': metric['timestamp']
                        })
                        break
        
        return tenant_metrics
    
    async def collect_opensearch_metrics(self) -> Dict[str, List[Dict]]:
        """Collect OpenSearch metrics and attribute to tenants"""
        source = self.sources['opensearch']
        raw_metrics = await self.collect_prometheus_metrics(source['prometheus_endpoint'])
        
        tenant_metrics = {tenant: [] for tenant in self.tenant_mapping.keys()}
        
        for metric_def in source['metrics']:
            matching_metrics = [m for m in raw_metrics if m['metric_name'] == metric_def['metric']]
            
            for metric in matching_metrics:
                index = metric['labels'].get('index', '')
                
                # Map index pattern to tenant
                for tenant, mapping in self.tenant_mapping.items():
                    for pattern in mapping['opensearch_indices']:
                        pattern_prefix = pattern.replace('*', '')
                        if index.startswith(pattern_prefix):
                            tenant_metrics[tenant].append({
                                'metric': metric_def['name'],
                                'value': metric['value'],
                                'unit': metric_def['unit'],
                                'labels': metric['labels'],
                                'timestamp': metric['timestamp']
                            })
                            break
        
        return tenant_metrics
    
    async def collect_watsonx_metrics(self) -> Dict[str, List[Dict]]:
        """Collect watsonx.data/Presto and Spark metrics"""
        source = self.sources['watsonx_data']
        raw_metrics = await self.collect_prometheus_metrics(source['prometheus_endpoint'])
        
        tenant_metrics = {tenant: [] for tenant in self.tenant_mapping.keys()}
        
        for metric_def in source['metrics']:
            matching_metrics = [m for m in raw_metrics if m['metric_name'] == metric_def['metric']]
            
            for metric in matching_metrics:
                # For Presto, map by user
                user = metric['labels'].get('user')
                tenant_id = metric['labels'].get('tenant_id')
                
                for tenant, mapping in self.tenant_mapping.items():
                    if user in mapping.get('presto_users', []) or tenant_id == mapping['tenant_id']:
                        tenant_metrics[tenant].append({
                            'metric': metric_def['name'],
                            'value': metric['value'],
                            'unit': metric_def['unit'],
                            'labels': metric['labels'],
                            'timestamp': metric['timestamp']
                        })
                        break
        
        return tenant_metrics
    
    async def collect_all_metrics(self) -> Dict[str, Dict[str, List[Dict]]]:
        """Collect metrics from all sources"""
        logger.info("Starting metrics collection cycle")
        
        results = await asyncio.gather(
            self.collect_pulsar_metrics(),
            self.collect_cassandra_metrics(),
            self.collect_opensearch_metrics(),
            self.collect_watsonx_metrics(),
            return_exceptions=True
        )
        
        all_metrics = {
            'pulsar': results[0] if not isinstance(results[0], Exception) else {},
            'cassandra': results[1] if not isinstance(results[1], Exception) else {},
            'opensearch': results[2] if not isinstance(results[2], Exception) else {},
            'watsonx': results[3] if not isinstance(results[3], Exception) else {}
        }
        
        logger.info(f"Collected metrics from {len(all_metrics)} sources")
        return all_metrics


async def main():
    """Main collection loop"""
    collector = MetricsCollector('monitoring/metrics-collection/collector_config.json')
    await collector.initialize()
    
    try:
        while True:
            metrics = await collector.collect_all_metrics()
            # Store metrics (implementation in storage module)
            logger.info(f"Collection cycle complete")
            await asyncio.sleep(60)  # Collect every minute
    finally:
        await collector.close()


if __name__ == '__main__':
    asyncio.run(main())
'''

# Create service directory
service_dir = "monitoring/metrics-collection/services"
os.makedirs(service_dir, exist_ok=True)

collector_file = os.path.join(service_dir, "metrics_collector.py")
with open(collector_file, 'w') as f:
    f.write(metrics_collector_code)

# Create requirements file
requirements = """aiohttp==3.9.1
prometheus-client==0.19.0
opensearch-py==2.4.2
python-json-logger==2.0.7
"""

requirements_file = os.path.join(service_dir, "requirements.txt")
with open(requirements_file, 'w') as f:
    f.write(requirements)

# Create Dockerfile
dockerfile_content = """FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY metrics_collector.py .

CMD ["python", "metrics_collector.py"]
"""

dockerfile_path = os.path.join(service_dir, "Dockerfile")
with open(dockerfile_path, 'w') as f:
    f.write(dockerfile_content)

collector_service_summary = {
    "service": "metrics_collector",
    "files_created": [collector_file, requirements_file, dockerfile_path],
    "features": [
        "Async metrics collection from Prometheus endpoints",
        "Tenant attribution for all services",
        "Parallel collection from multiple sources",
        "Error handling and logging"
    ]
}

print("Metrics Collector Service Created")
print(f"Service: {collector_service_summary['service']}")
print(f"Files: {len(collector_service_summary['files_created'])}")
print(f"Features: {len(collector_service_summary['features'])}")
