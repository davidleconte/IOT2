import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import numpy as np

# Zerve design system colors
bg_color = '#1D1D20'
primary_text = '#fbfbff'
secondary_text = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'

# Create figure with dark background
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(18, 8))
fig.patch.set_facecolor(bg_color)

for ax in [ax1, ax2]:
    ax.set_facecolor(bg_color)
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')

# ========== Diagram 1: Real-Time Data Flow ==========
ax1.text(5, 9.5, 'Real-Time Maritime Data Flow', 
         ha='center', fontsize=16, weight='bold', color=primary_text)

# Vessel layer
vessel_box = FancyBboxPatch((0.5, 7.5), 2, 1, boxstyle="round,pad=0.1", 
                            edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2)
ax1.add_patch(vessel_box)
ax1.text(1.5, 8, 'Vessel IoT\nSensors', ha='center', va='center', 
         fontsize=10, weight='bold', color=primary_text)

# Pulsar ingestion
pulsar_box = FancyBboxPatch((3.5, 7.5), 2, 1, boxstyle="round,pad=0.1",
                            edgecolor=orange, facecolor=orange, alpha=0.3, linewidth=2)
ax1.add_patch(pulsar_box)
ax1.text(4.5, 8, 'Apache Pulsar\n(Multi-Tenant)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# Stream processing
stream_box = FancyBboxPatch((6.5, 7.5), 2.5, 1, boxstyle="round,pad=0.1",
                            edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax1.add_patch(stream_box)
ax1.text(7.75, 8, 'Stream Processing\n(Pulsar Functions)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# Delta Lake
delta_box = FancyBboxPatch((1, 5.5), 2, 1, boxstyle="round,pad=0.1",
                           edgecolor=lavender, facecolor=lavender, alpha=0.3, linewidth=2)
ax1.add_patch(delta_box)
ax1.text(2, 6, 'Delta Lake\n(S3)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# Feast Feature Store
feast_box = FancyBboxPatch((4, 5.5), 2, 1, boxstyle="round,pad=0.1",
                           edgecolor=coral, facecolor=coral, alpha=0.3, linewidth=2)
ax1.add_patch(feast_box)
ax1.text(5, 6, 'Feast\nFeature Store', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# OpenSearch
opensearch_box = FancyBboxPatch((7, 5.5), 2, 1, boxstyle="round,pad=0.1",
                                edgecolor=highlight, facecolor=highlight, alpha=0.3, linewidth=2)
ax1.add_patch(opensearch_box)
ax1.text(8, 6, 'OpenSearch\nAnalytics', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# ML Models
ml_box = FancyBboxPatch((2, 3.5), 2.5, 1, boxstyle="round,pad=0.1",
                        edgecolor=light_blue, facecolor=light_blue, alpha=0.3, linewidth=2)
ax1.add_patch(ml_box)
ax1.text(3.25, 4, 'ML Models\n(SageMaker)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# Microservices
services_box = FancyBboxPatch((5.5, 3.5), 3, 1, boxstyle="round,pad=0.1",
                              edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax1.add_patch(services_box)
ax1.text(7, 4, 'Microservices Layer\n(Fleet, Tracking, Analytics)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# API Gateway
api_box = FancyBboxPatch((3, 1.5), 4, 1, boxstyle="round,pad=0.1",
                         edgecolor=orange, facecolor=orange, alpha=0.3, linewidth=2)
ax1.add_patch(api_box)
ax1.text(5, 2, 'API Gateway + Service Mesh (Istio)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# Tenant Apps
tenant_box = FancyBboxPatch((3, 0.2), 4, 0.8, boxstyle="round,pad=0.1",
                            edgecolor=lavender, facecolor=lavender, alpha=0.3, linewidth=2)
ax1.add_patch(tenant_box)
ax1.text(5, 0.6, 'Tenant Applications (Web/Mobile)', ha='center', va='center',
         fontsize=10, weight='bold', color=primary_text)

# Arrows for data flow
arrows_1 = [
    ((2.5, 8), (3.5, 8)),
    ((5.5, 8), (6.5, 8)),
    ((7.75, 7.5), (2, 6.5)),
    ((7.75, 7.5), (5, 6.5)),
    ((7.75, 7.5), (8, 6.5)),
    ((2, 5.5), (3.25, 4.5)),
    ((5, 5.5), (3.25, 4.5)),
    ((8, 5.5), (7, 4.5)),
    ((3.25, 3.5), (5, 2.5)),
    ((7, 3.5), (5, 2.5)),
    ((5, 1.5), (5, 1)),
]

for start, end in arrows_1:
    arrow = FancyArrowPatch(start, end, arrowstyle='->', 
                           mutation_scale=20, linewidth=2, 
                           color=secondary_text, alpha=0.6)
    ax1.add_patch(arrow)

# ========== Diagram 2: Multi-Tenant Isolation ==========
ax2.text(5, 9.5, 'Multi-Tenant Isolation Architecture', 
         ha='center', fontsize=16, weight='bold', color=primary_text)

# Tenant A
tenant_a = FancyBboxPatch((0.5, 6), 4, 2.5, boxstyle="round,pad=0.1",
                          edgecolor=light_blue, facecolor=light_blue, alpha=0.2, linewidth=3)
ax2.add_patch(tenant_a)
ax2.text(2.5, 8.2, 'Tenant A', ha='center', fontsize=12, weight='bold', color=light_blue)
ax2.text(2.5, 7.5, '• K8s Namespace: tenant-a\n• Pulsar: tenant-a/maritime/*\n• Delta: partition=tenant_a\n• OpenSearch: tenant-a-*', 
         ha='center', va='top', fontsize=9, color=primary_text)

# Tenant B
tenant_b = FancyBboxPatch((5.5, 6), 4, 2.5, boxstyle="round,pad=0.1",
                          edgecolor=coral, facecolor=coral, alpha=0.2, linewidth=3)
ax2.add_patch(tenant_b)
ax2.text(7.5, 8.2, 'Tenant B', ha='center', fontsize=12, weight='bold', color=coral)
ax2.text(7.5, 7.5, '• K8s Namespace: tenant-b\n• Pulsar: tenant-b/maritime/*\n• Delta: partition=tenant_b\n• OpenSearch: tenant-b-*', 
         ha='center', va='top', fontsize=9, color=primary_text)

# Shared infrastructure layer
shared_box = FancyBboxPatch((1, 3.5), 8, 1.5, boxstyle="round,pad=0.1",
                            edgecolor=green, facecolor=green, alpha=0.3, linewidth=2)
ax2.add_patch(shared_box)
ax2.text(5, 4.7, 'Shared Infrastructure Layer', ha='center', fontsize=11, weight='bold', color=primary_text)
ax2.text(5, 4.2, 'EKS Cluster | Pulsar Cluster | S3 Buckets | RDS | OpenSearch Domain', 
         ha='center', fontsize=9, color=primary_text)

# Security layer
security_box = FancyBboxPatch((1.5, 1.5), 7, 1.2, boxstyle="round,pad=0.1",
                              edgecolor=highlight, facecolor=highlight, alpha=0.3, linewidth=2)
ax2.add_patch(security_box)
ax2.text(5, 2.5, 'Security & Isolation Controls', ha='center', fontsize=11, weight='bold', color=primary_text)
ax2.text(5, 2, 'Network Policies | RBAC | Row-Level Security | Message ACLs | Encryption', 
         ha='center', fontsize=9, color=primary_text)

# Control plane
control_box = FancyBboxPatch((2.5, 0.3), 5, 0.8, boxstyle="round,pad=0.1",
                             edgecolor=lavender, facecolor=lavender, alpha=0.3, linewidth=2)
ax2.add_patch(control_box)
ax2.text(5, 0.7, 'Control Plane (Tenant Management, Provisioning, Monitoring)', 
         ha='center', fontsize=9, weight='bold', color=primary_text)

plt.tight_layout()
print("✅ Data flow diagrams generated successfully")
print("\n📊 Diagram 1: Real-time maritime data flow from vessels through Pulsar to analytics")
print("📊 Diagram 2: Multi-tenant isolation architecture with shared infrastructure")

data_flow_fig = fig