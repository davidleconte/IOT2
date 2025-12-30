import os
import json

# Create OpenSearch dashboards for cost visualization
dashboards_dir = "monitoring/dashboards"
os.makedirs(dashboards_dir, exist_ok=True)

# Tenant cost overview dashboard
tenant_cost_dashboard = {
    "title": "Tenant Cost Overview",
    "description": "Real-time cost tracking per tenant across all services",
    "visualizations": [
        {
            "id": "total_cost_by_tenant",
            "type": "bar_chart",
            "title": "Total Monthly Cost by Tenant",
            "query": {
                "index": "tenant-costs-*",
                "aggregation": {
                    "field": "tenant_id",
                    "metric": "sum",
                    "value_field": "total_cost"
                }
            }
        },
        {
            "id": "cost_breakdown_by_service",
            "type": "stacked_bar_chart",
            "title": "Cost Breakdown by Service",
            "query": {
                "index": "tenant-costs-*",
                "aggregation": {
                    "field": "tenant_id",
                    "sub_aggregation": {
                        "field": "service_name",
                        "metric": "sum",
                        "value_field": "service_cost"
                    }
                }
            }
        },
        {
            "id": "cost_trend_timeline",
            "type": "line_chart",
            "title": "Cost Trend Over Time",
            "query": {
                "index": "tenant-costs-*",
                "time_field": "timestamp",
                "aggregation": {
                    "date_histogram": "1d",
                    "metric": "sum",
                    "value_field": "total_cost",
                    "group_by": "tenant_id"
                }
            }
        },
        {
            "id": "top_cost_drivers",
            "type": "table",
            "title": "Top Cost Drivers by Tenant",
            "query": {
                "index": "tenant-costs-*",
                "fields": ["tenant_id", "service_name", "metric_name", "cost"],
                "sort": {"field": "cost", "order": "desc"},
                "size": 20
            }
        }
    ]
}

# Service-specific cost dashboard
service_cost_dashboard = {
    "title": "Service Cost Analysis",
    "description": "Detailed cost breakdown per service with usage metrics",
    "visualizations": [
        {
            "id": "pulsar_costs",
            "type": "metric_cards",
            "title": "Pulsar Costs",
            "metrics": [
                {"label": "Total Pulsar Cost", "field": "services.pulsar.total"},
                {"label": "Ingress Cost", "field": "services.pulsar.topic_bytes_in.cost"},
                {"label": "Storage Cost", "field": "services.pulsar.topic_storage_size.cost"}
            ]
        },
        {
            "id": "cassandra_costs",
            "type": "metric_cards",
            "title": "Cassandra/HCD Costs",
            "metrics": [
                {"label": "Total HCD Cost", "field": "services.cassandra.total"},
                {"label": "Read Ops Cost", "field": "services.cassandra.read_operations.cost"},
                {"label": "Storage Cost", "field": "services.cassandra.storage_bytes.cost"}
            ]
        },
        {
            "id": "opensearch_costs",
            "type": "pie_chart",
            "title": "OpenSearch Cost Distribution",
            "query": {
                "index": "tenant-costs-*",
                "filter": {"term": {"service_name": "opensearch"}},
                "aggregation": {
                    "field": "metric_name",
                    "metric": "sum",
                    "value_field": "cost"
                }
            }
        },
        {
            "id": "watsonx_costs",
            "type": "bar_chart",
            "title": "watsonx.data Cost by Tenant",
            "query": {
                "index": "tenant-costs-*",
                "filter": {"term": {"service_name": "watsonx"}},
                "aggregation": {
                    "field": "tenant_id",
                    "metric": "sum",
                    "value_field": "cost"
                }
            }
        }
    ]
}

dashboard_files = []
for dash_name, dash_config in [
    ("tenant_cost_overview", tenant_cost_dashboard),
    ("service_cost_analysis", service_cost_dashboard)
]:
    dash_file = os.path.join(dashboards_dir, f"{dash_name}.json")
    with open(dash_file, 'w') as f:
        json.dump(dash_config, f, indent=2)
    dashboard_files.append(dash_file)

# Create monthly report generator
report_generator_code = '''"""
Monthly Cost Report Generator
Generates CSV and PDF reports of tenant costs
"""
import json
import csv
from datetime import datetime, timedelta
from typing import Dict, List, Any
import logging
from opensearchpy import OpenSearch
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
from reportlab.lib.units import inch

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MonthlyReportGenerator:
    """Generates monthly cost reports in CSV and PDF formats"""
    
    def __init__(self, opensearch_config: Dict):
        self.os_client = OpenSearch(
            hosts=[opensearch_config['host']],
            http_auth=(opensearch_config['user'], opensearch_config['password']),
            use_ssl=True,
            verify_certs=True
        )
        self.index_pattern = "tenant-costs-*"
    
    def query_monthly_costs(self, year: int, month: int) -> Dict[str, Any]:
        """Query costs for a specific month"""
        start_date = datetime(year, month, 1)
        if month == 12:
            end_date = datetime(year + 1, 1, 1)
        else:
            end_date = datetime(year, month + 1, 1)
        
        query = {
            "size": 0,
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": start_date.isoformat(),
                                    "lt": end_date.isoformat()
                                }
                            }
                        }
                    ]
                }
            },
            "aggs": {
                "by_tenant": {
                    "terms": {"field": "tenant_id.keyword", "size": 100},
                    "aggs": {
                        "total_cost": {"sum": {"field": "total_cost"}},
                        "by_service": {
                            "terms": {"field": "service_name.keyword"},
                            "aggs": {
                                "service_cost": {"sum": {"field": "service_cost"}},
                                "by_metric": {
                                    "terms": {"field": "metric_name.keyword"},
                                    "aggs": {
                                        "metric_cost": {"sum": {"field": "cost"}},
                                        "metric_value": {"sum": {"field": "billable_value"}}
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        response = self.os_client.search(index=self.index_pattern, body=query)
        return response['aggregations']['by_tenant']['buckets']
    
    def generate_csv_report(self, year: int, month: int, output_path: str):
        """Generate CSV report"""
        logger.info(f"Generating CSV report for {year}-{month:02d}")
        
        cost_data = self.query_monthly_costs(year, month)
        
        with open(output_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header
            writer.writerow([
                'Tenant ID',
                'Service',
                'Metric',
                'Billable Value',
                'Unit',
                'Cost (USD)',
                'Month'
            ])
            
            # Data rows
            for tenant_bucket in cost_data:
                tenant_id = tenant_bucket['key']
                
                for service_bucket in tenant_bucket['by_service']['buckets']:
                    service_name = service_bucket['key']
                    
                    for metric_bucket in service_bucket['by_metric']['buckets']:
                        metric_name = metric_bucket['key']
                        metric_value = metric_bucket['metric_value']['value']
                        metric_cost = metric_bucket['metric_cost']['value']
                        
                        writer.writerow([
                            tenant_id,
                            service_name,
                            metric_name,
                            f"{metric_value:.4f}",
                            "",  # Unit would come from metadata
                            f"{metric_cost:.2f}",
                            f"{year}-{month:02d}"
                        ])
        
        logger.info(f"CSV report saved to {output_path}")
    
    def generate_pdf_report(self, year: int, month: int, output_path: str):
        """Generate PDF report"""
        logger.info(f"Generating PDF report for {year}-{month:02d}")
        
        cost_data = self.query_monthly_costs(year, month)
        
        doc = SimpleDocTemplate(output_path, pagesize=letter)
        styles = getSampleStyleSheet()
        story = []
        
        # Title
        title_style = ParagraphStyle(
            'CustomTitle',
            parent=styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#1D1D20'),
            spaceAfter=30
        )
        title = Paragraph(f"Monthly Cost Report - {year}-{month:02d}", title_style)
        story.append(title)
        story.append(Spacer(1, 0.2*inch))
        
        # Summary section
        summary_style = styles['Heading2']
        story.append(Paragraph("Executive Summary", summary_style))
        story.append(Spacer(1, 0.1*inch))
        
        # Calculate totals
        total_cost = sum(t['total_cost']['value'] for t in cost_data)
        summary_text = f"Total Platform Cost: ${total_cost:,.2f}<br/>Number of Tenants: {len(cost_data)}"
        story.append(Paragraph(summary_text, styles['Normal']))
        story.append(Spacer(1, 0.3*inch))
        
        # Per-tenant breakdown
        story.append(Paragraph("Tenant Cost Breakdown", summary_style))
        story.append(Spacer(1, 0.1*inch))
        
        for tenant_bucket in cost_data:
            tenant_id = tenant_bucket['key']
            tenant_total = tenant_bucket['total_cost']['value']
            
            # Tenant header
            tenant_header = Paragraph(
                f"<b>{tenant_id}</b> - Total: ${tenant_total:,.2f}",
                styles['Heading3']
            )
            story.append(tenant_header)
            story.append(Spacer(1, 0.1*inch))
            
            # Service breakdown table
            table_data = [['Service', 'Metric', 'Cost (USD)']]
            
            for service_bucket in tenant_bucket['by_service']['buckets']:
                service_name = service_bucket['key']
                service_cost = service_bucket['service_cost']['value']
                
                # Add service total
                table_data.append([
                    Paragraph(f"<b>{service_name}</b>", styles['Normal']),
                    '',
                    Paragraph(f"<b>${service_cost:,.2f}</b>", styles['Normal'])
                ])
                
                # Add metrics
                for metric_bucket in service_bucket['by_metric']['buckets']:
                    metric_name = metric_bucket['key']
                    metric_cost = metric_bucket['metric_cost']['value']
                    
                    table_data.append([
                        '',
                        metric_name,
                        f"${metric_cost:,.2f}"
                    ])
            
            # Create table
            table = Table(table_data, colWidths=[2*inch, 3*inch, 1.5*inch])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1D1D20')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (2, 0), (2, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 12),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#F5F5F5')])
            ]))
            
            story.append(table)
            story.append(Spacer(1, 0.3*inch))
            story.append(PageBreak())
        
        # Build PDF
        doc.build(story)
        logger.info(f"PDF report saved to {output_path}")
    
    def generate_reports(self, year: int, month: int, output_dir: str):
        """Generate both CSV and PDF reports"""
        csv_path = os.path.join(output_dir, f"cost_report_{year}_{month:02d}.csv")
        pdf_path = os.path.join(output_dir, f"cost_report_{year}_{month:02d}.pdf")
        
        self.generate_csv_report(year, month, csv_path)
        self.generate_pdf_report(year, month, pdf_path)
        
        return {"csv": csv_path, "pdf": pdf_path}


if __name__ == '__main__':
    config = {
        'host': 'opensearch:9200',
        'user': 'admin',
        'password': 'admin'
    }
    
    generator = MonthlyReportGenerator(config)
    
    # Generate report for current month
    now = datetime.now()
    generator.generate_reports(now.year, now.month, '/reports')
'''

reports_dir = "monitoring/reports"
os.makedirs(reports_dir, exist_ok=True)

report_gen_file = os.path.join(reports_dir, "report_generator.py")
with open(report_gen_file, 'w') as f:
    f.write(report_generator_code)

# Create report scheduler
scheduler_code = '''"""
Automated Report Scheduler
Generates monthly reports automatically on the 1st of each month
"""
import schedule
import time
from datetime import datetime, timedelta
from report_generator import MonthlyReportGenerator
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_monthly_report():
    """Generate report for previous month"""
    now = datetime.now()
    last_month = now - timedelta(days=now.day)
    
    logger.info(f"Generating monthly report for {last_month.year}-{last_month.month:02d}")
    
    config = {
        'host': 'opensearch:9200',
        'user': 'admin',
        'password': 'admin'
    }
    
    generator = MonthlyReportGenerator(config)
    reports = generator.generate_reports(last_month.year, last_month.month, '/reports')
    
    logger.info(f"Reports generated: {reports}")

# Schedule to run on 1st of each month at 2 AM
schedule.every().month.at("02:00").do(generate_monthly_report)

if __name__ == '__main__':
    logger.info("Report scheduler started")
    while True:
        schedule.run_pending()
        time.sleep(3600)  # Check every hour
'''

scheduler_file = os.path.join(reports_dir, "report_scheduler.py")
with open(scheduler_file, 'w') as f:
    f.write(scheduler_code)

# Create requirements
report_requirements = """opensearch-py==2.4.2
reportlab==4.0.7
schedule==1.2.0
"""

report_req_file = os.path.join(reports_dir, "requirements.txt")
with open(report_req_file, 'w') as f:
    f.write(report_requirements)

dashboard_report_summary = {
    "dashboards": len([tenant_cost_dashboard, service_cost_dashboard]),
    "visualizations": len(tenant_cost_dashboard['visualizations']) + len(service_cost_dashboard['visualizations']),
    "report_formats": ["CSV", "PDF"],
    "automation": "Monthly scheduled reports",
    "files_created": dashboard_files + [report_gen_file, scheduler_file, report_req_file]
}

print("Cost Dashboards and Reports Created")
print(f"Dashboards: {dashboard_report_summary['dashboards']}")
print(f"Visualizations: {dashboard_report_summary['visualizations']}")
print(f"Report formats: {', '.join(dashboard_report_summary['report_formats'])}")
print(f"Files created: {len(dashboard_report_summary['files_created'])}")
