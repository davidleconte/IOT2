import os
import json

# Create pricing models for all services
pricing_models = {
    "pulsar": {
        "description": "Apache Pulsar pricing based on throughput and storage",
        "pricing_units": [
            {
                "metric": "topic_bytes_in",
                "rate": 0.09,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Ingress data per GB"
            },
            {
                "metric": "topic_bytes_out",
                "rate": 0.12,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Egress data per GB"
            },
            {
                "metric": "topic_storage_size",
                "rate": 0.025,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Storage per GB-month"
            },
            {
                "metric": "topic_messages_in",
                "rate": 0.0000005,
                "unit": "message",
                "billing_unit": "monthly",
                "description": "Message processing per million messages"
            }
        ]
    },
    "cassandra_hcd": {
        "description": "DataStax HCD pricing based on operations and storage",
        "pricing_units": [
            {
                "metric": "read_operations",
                "rate": 0.00025,
                "unit": "operation",
                "billing_unit": "monthly",
                "description": "Read operation per 1000 ops"
            },
            {
                "metric": "write_operations",
                "rate": 0.00125,
                "unit": "operation",
                "billing_unit": "monthly",
                "description": "Write operation per 1000 ops"
            },
            {
                "metric": "storage_bytes",
                "rate": 0.10,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Storage per GB-month"
            }
        ]
    },
    "opensearch": {
        "description": "OpenSearch pricing based on storage and queries",
        "pricing_units": [
            {
                "metric": "index_size_bytes",
                "rate": 0.024,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Index storage per GB-month"
            },
            {
                "metric": "search_query_total",
                "rate": 0.0005,
                "unit": "query",
                "billing_unit": "monthly",
                "description": "Search query per 1000 queries"
            },
            {
                "metric": "query_latency_ms",
                "rate": 0.0001,
                "unit": "compute_second",
                "billing_unit": "monthly",
                "description": "Query compute time per second"
            }
        ]
    },
    "watsonx_data": {
        "description": "watsonx.data pricing for Presto queries and Spark jobs",
        "pricing_units": [
            {
                "metric": "presto_scan_bytes",
                "rate": 5.0,
                "unit": "TB",
                "billing_unit": "monthly",
                "description": "Presto data scanned per TB"
            },
            {
                "metric": "presto_query_count",
                "rate": 0.01,
                "unit": "query",
                "billing_unit": "monthly",
                "description": "Query execution per query"
            },
            {
                "metric": "spark_job_duration",
                "rate": 0.10,
                "unit": "vcpu_hour",
                "billing_unit": "monthly",
                "description": "Spark job compute per vCPU-hour",
                "conversion_factor": {"from": "seconds", "to": "hours", "divisor": 3600}
            },
            {
                "metric": "spark_executor_memory",
                "rate": 0.0112,
                "unit": "GB_hour",
                "billing_unit": "monthly",
                "description": "Spark memory usage per GB-hour"
            }
        ]
    },
    "object_storage": {
        "description": "S3-compatible object storage pricing",
        "pricing_units": [
            {
                "metric": "storage_gb_months",
                "rate": 0.023,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Storage per GB-month"
            },
            {
                "metric": "api_requests",
                "rate": 0.0004,
                "unit": "request",
                "billing_unit": "monthly",
                "description": "API requests per 1000 requests"
            }
        ]
    },
    "network": {
        "description": "Network egress pricing",
        "pricing_units": [
            {
                "metric": "egress_bytes",
                "rate": 0.09,
                "unit": "GB",
                "billing_unit": "monthly",
                "description": "Network egress per GB"
            }
        ],
        "tiers": [
            {"min": 0, "max": 10240, "rate": 0.09},
            {"min": 10240, "max": 51200, "rate": 0.085},
            {"min": 51200, "max": 153600, "rate": 0.07},
            {"min": 153600, "max": float('inf'), "rate": 0.05}
        ]
    }
}

# Create cost calculation engine
cost_engine_code = '''"""
Cost Calculation Engine
Applies pricing models to collected metrics and calculates tenant costs
"""
import json
from typing import Dict, List, Any
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CostCalculationEngine:
    """Calculates costs from metrics using pricing models"""
    
    def __init__(self, pricing_config_path: str):
        with open(pricing_config_path, 'r') as f:
            self.pricing_models = json.load(f)
    
    def convert_bytes_to_gb(self, bytes_value: float) -> Decimal:
        """Convert bytes to GB"""
        return Decimal(str(bytes_value)) / Decimal('1073741824')
    
    def convert_bytes_to_tb(self, bytes_value: float) -> Decimal:
        """Convert bytes to TB"""
        return Decimal(str(bytes_value)) / Decimal('1099511627776')
    
    def apply_tiered_pricing(self, value: Decimal, tiers: List[Dict]) -> Decimal:
        """Apply tiered pricing structure"""
        total_cost = Decimal('0')
        remaining = value
        
        for tier in sorted(tiers, key=lambda x: x['min']):
            tier_min = Decimal(str(tier['min']))
            tier_max = Decimal(str(tier['max']))
            tier_rate = Decimal(str(tier['rate']))
            
            if remaining <= 0:
                break
            
            tier_size = tier_max - tier_min
            billable = min(remaining, tier_size)
            
            total_cost += billable * tier_rate
            remaining -= billable
        
        return total_cost
    
    def calculate_pulsar_costs(self, metrics: List[Dict]) -> Dict[str, Any]:
        """Calculate Pulsar costs"""
        model = self.pricing_models['pulsar']
        costs = {}
        
        for metric in metrics:
            metric_name = metric['metric']
            value = Decimal(str(metric['value']))
            
            # Find pricing unit
            pricing_unit = next((p for p in model['pricing_units'] if p['metric'] == metric_name), None)
            if not pricing_unit:
                continue
            
            # Convert to billable units
            if pricing_unit['unit'] == 'GB':
                billable_value = self.convert_bytes_to_gb(float(value))
            elif pricing_unit['unit'] == 'message':
                billable_value = value / Decimal('1000000')  # Per million
            else:
                billable_value = value / Decimal('1000')  # Per thousand
            
            cost = billable_value * Decimal(str(pricing_unit['rate']))
            costs[metric_name] = {
                'raw_value': float(value),
                'billable_value': float(billable_value),
                'unit': pricing_unit['unit'],
                'rate': pricing_unit['rate'],
                'cost': float(cost.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))
            }
        
        return costs
    
    def calculate_cassandra_costs(self, metrics: List[Dict]) -> Dict[str, Any]:
        """Calculate Cassandra/HCD costs"""
        model = self.pricing_models['cassandra_hcd']
        costs = {}
        
        for metric in metrics:
            metric_name = metric['metric']
            value = Decimal(str(metric['value']))
            
            pricing_unit = next((p for p in model['pricing_units'] if p['metric'] == metric_name), None)
            if not pricing_unit:
                continue
            
            if pricing_unit['unit'] == 'GB':
                billable_value = self.convert_bytes_to_gb(float(value))
            else:
                billable_value = value / Decimal('1000')
            
            cost = billable_value * Decimal(str(pricing_unit['rate']))
            costs[metric_name] = {
                'raw_value': float(value),
                'billable_value': float(billable_value),
                'unit': pricing_unit['unit'],
                'rate': pricing_unit['rate'],
                'cost': float(cost.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))
            }
        
        return costs
    
    def calculate_opensearch_costs(self, metrics: List[Dict]) -> Dict[str, Any]:
        """Calculate OpenSearch costs"""
        model = self.pricing_models['opensearch']
        costs = {}
        
        for metric in metrics:
            metric_name = metric['metric']
            value = Decimal(str(metric['value']))
            
            pricing_unit = next((p for p in model['pricing_units'] if p['metric'] == metric_name), None)
            if not pricing_unit:
                continue
            
            if pricing_unit['unit'] == 'GB':
                billable_value = self.convert_bytes_to_gb(float(value))
            elif pricing_unit['unit'] == 'query':
                billable_value = value / Decimal('1000')
            else:
                billable_value = value
            
            cost = billable_value * Decimal(str(pricing_unit['rate']))
            costs[metric_name] = {
                'raw_value': float(value),
                'billable_value': float(billable_value),
                'unit': pricing_unit['unit'],
                'rate': pricing_unit['rate'],
                'cost': float(cost.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))
            }
        
        return costs
    
    def calculate_watsonx_costs(self, metrics: List[Dict]) -> Dict[str, Any]:
        """Calculate watsonx.data costs"""
        model = self.pricing_models['watsonx_data']
        costs = {}
        
        for metric in metrics:
            metric_name = metric['metric']
            value = Decimal(str(metric['value']))
            
            pricing_unit = next((p for p in model['pricing_units'] if p['metric'] == metric_name), None)
            if not pricing_unit:
                continue
            
            # Unit conversions
            if pricing_unit['unit'] == 'TB':
                billable_value = self.convert_bytes_to_tb(float(value))
            elif pricing_unit['unit'] == 'vcpu_hour':
                billable_value = value / Decimal('3600')  # seconds to hours
            elif pricing_unit['unit'] == 'GB_hour':
                billable_value = self.convert_bytes_to_gb(float(value))
            else:
                billable_value = value
            
            cost = billable_value * Decimal(str(pricing_unit['rate']))
            costs[metric_name] = {
                'raw_value': float(value),
                'billable_value': float(billable_value),
                'unit': pricing_unit['unit'],
                'rate': pricing_unit['rate'],
                'cost': float(cost.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))
            }
        
        return costs
    
    def calculate_network_costs(self, metrics: List[Dict]) -> Dict[str, Any]:
        """Calculate network egress costs with tiered pricing"""
        model = self.pricing_models['network']
        costs = {}
        
        for metric in metrics:
            metric_name = metric['metric']
            value = Decimal(str(metric['value']))
            
            billable_value = self.convert_bytes_to_gb(float(value))
            
            # Apply tiered pricing
            if 'tiers' in model:
                cost = self.apply_tiered_pricing(billable_value, model['tiers'])
            else:
                pricing_unit = model['pricing_units'][0]
                cost = billable_value * Decimal(str(pricing_unit['rate']))
            
            costs[metric_name] = {
                'raw_value': float(value),
                'billable_value': float(billable_value),
                'unit': 'GB',
                'cost': float(cost.quantize(Decimal('0.000001'), rounding=ROUND_HALF_UP))
            }
        
        return costs
    
    def calculate_tenant_costs(self, tenant_id: str, tenant_metrics: Dict[str, List[Dict]]) -> Dict[str, Any]:
        """Calculate total costs for a tenant across all services"""
        tenant_costs = {
            'tenant_id': tenant_id,
            'timestamp': datetime.utcnow().isoformat(),
            'services': {},
            'total_cost': Decimal('0')
        }
        
        # Calculate costs per service
        if 'pulsar' in tenant_metrics:
            tenant_costs['services']['pulsar'] = self.calculate_pulsar_costs(tenant_metrics['pulsar'])
        
        if 'cassandra' in tenant_metrics:
            tenant_costs['services']['cassandra'] = self.calculate_cassandra_costs(tenant_metrics['cassandra'])
        
        if 'opensearch' in tenant_metrics:
            tenant_costs['services']['opensearch'] = self.calculate_opensearch_costs(tenant_metrics['opensearch'])
        
        if 'watsonx' in tenant_metrics:
            tenant_costs['services']['watsonx'] = self.calculate_watsonx_costs(tenant_metrics['watsonx'])
        
        if 'network' in tenant_metrics:
            tenant_costs['services']['network'] = self.calculate_network_costs(tenant_metrics['network'])
        
        # Calculate total
        total = Decimal('0')
        for service_costs in tenant_costs['services'].values():
            for metric_cost in service_costs.values():
                total += Decimal(str(metric_cost['cost']))
        
        tenant_costs['total_cost'] = float(total.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP))
        
        return tenant_costs


if __name__ == '__main__':
    engine = CostCalculationEngine('monitoring/cost-calculation/pricing_models.json')
    logger.info("Cost calculation engine initialized")
'''

# Save files
cost_dir = "monitoring/cost-calculation"
os.makedirs(cost_dir, exist_ok=True)

pricing_file = os.path.join(cost_dir, "pricing_models.json")
with open(pricing_file, 'w') as f:
    json.dump(pricing_models, f, indent=2)

engine_file = os.path.join(cost_dir, "cost_engine.py")
with open(engine_file, 'w') as f:
    f.write(cost_engine_code)

pricing_summary = {
    "services": len(pricing_models),
    "total_pricing_units": sum(len(m['pricing_units']) for m in pricing_models.values()),
    "files": [pricing_file, engine_file],
    "features": [
        "Service-specific pricing models",
        "Tiered pricing support",
        "Unit conversion (bytes to GB/TB)",
        "Decimal precision for accurate calculations",
        "Per-tenant cost aggregation"
    ]
}

print("Pricing Model Engine Created")
print(f"Services with pricing: {pricing_summary['services']}")
print(f"Total pricing units: {pricing_summary['total_pricing_units']}")
print(f"Features: {len(pricing_summary['features'])}")
