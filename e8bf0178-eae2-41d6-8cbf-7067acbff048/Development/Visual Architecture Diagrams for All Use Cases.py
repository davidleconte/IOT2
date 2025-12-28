import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch, Rectangle
import numpy as np

# Zerve colors
bg_color = '#1D1D20'
text_primary = '#fbfbff'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'

# Create comprehensive architecture visualization
arch_fig = plt.figure(figsize=(20, 28))
arch_fig.patch.set_facecolor(bg_color)

# Grid layout: 4 rows x 2 columns for 8 use cases
positions = [
    (0, 3), (1, 3), (0, 2), (1, 2),  # Row 1 and 2
    (0, 1), (1, 1), (0, 0), (1, 0)   # Row 3 and 4
]

use_case_configs = [
    {
        'title': 'UC1: Port Congestion',
        'historical': ['500M port\narrivals', 'ML Training\nSpark'],
        'realtime': ['Kafka\nVessel pos.', 'OpenSearch\nCurrent state'],
        'hybrid': 'Predictive\nRouting\n<2s',
        'value': '$2.5M/yr',
        'color': light_blue
    },
    {
        'title': 'UC2: Cascade Failure',
        'historical': ['2B sensor\nreadings', 'Cross-vessel\npatterns'],
        'realtime': ['OpenSearch\nAnomalies', 'Fleet\ncorrelation'],
        'hybrid': 'Preventive\nAlerts\n<45s',
        'value': '$8M/yr',
        'color': coral
    },
    {
        'title': 'UC3: Insurance Premium',
        'historical': ['100M voyages', 'Risk model\ntraining'],
        'realtime': ['OpenSearch\nBehavior', 'Real-time\nrisk'],
        'hybrid': 'Dynamic\nPricing\n<5s',
        'value': '$3.2M/yr',
        'color': green
    },
    {
        'title': 'UC4: Compliance',
        'historical': ['Audit trail\n7 years', 'Iceberg\nforensics'],
        'realtime': ['OpenSearch\nViolations', 'Instant\nalerts'],
        'hybrid': 'Compliance\nProof\n<3s',
        'value': '$12M/yr',
        'color': highlight
    },
    {
        'title': 'UC5: Convoy Optim.',
        'historical': ['200M routes', 'Formation\nalgorithm'],
        'realtime': ['20K pos/min', 'OpenSearch\ncoord.'],
        'hybrid': 'Convoy\nMatching\n<3s',
        'value': '$5M/yr',
        'color': lavender
    },
    {
        'title': 'UC6: Spare Parts',
        'historical': ['80M maint.\nrecords', 'Failure\nprediction'],
        'realtime': ['15K sensors', 'OpenSearch\nmonitoring'],
        'hybrid': 'Pre-position\nParts\n<10s',
        'value': '$6M/yr',
        'color': orange
    },
    {
        'title': 'UC7: Weather Routing',
        'historical': ['300M weather\nvoyages', 'Impact\nmodel'],
        'realtime': ['8K updates/hr', 'OpenSearch\nweather'],
        'hybrid': 'Route\nOptim.\n<5s',
        'value': '$4M/yr',
        'color': green
    },
    {
        'title': 'UC8: Benchmarking',
        'historical': ['1B+ multi-co.\nrecords', 'Cross-fleet\nanalytics'],
        'realtime': ['Interactive\ndashboards', 'OpenSearch\nquery'],
        'hybrid': 'Performance\nInsights\n<10s',
        'value': '$7M/yr',
        'color': light_blue
    }
]

for idx, (col, row) in enumerate(positions):
    config = use_case_configs[idx]
    ax = plt.subplot2grid((4, 2), (row, col))
    ax.set_xlim(0, 10)
    ax.set_ylim(0, 10)
    ax.axis('off')
    ax.set_facecolor(bg_color)
    
    # Title
    ax.text(5, 9.5, config['title'], ha='center', va='top', 
            fontsize=11, fontweight='bold', color=text_primary)
    
    # Historical layer (left)
    hist_box = FancyBboxPatch((0.5, 5), 3.5, 3, 
                              boxstyle="round,pad=0.1", 
                              edgecolor=config['color'], 
                              facecolor=bg_color, 
                              linewidth=2)
    ax.add_patch(hist_box)
    ax.text(2.25, 7.5, 'Historical', ha='center', fontsize=9, 
            color=config['color'], fontweight='bold')
    ax.text(2.25, 6.7, config['historical'][0], ha='center', 
            fontsize=7, color=text_primary)
    ax.text(2.25, 5.8, config['historical'][1], ha='center', 
            fontsize=7, color=text_primary)
    
    # Real-time layer (right)
    rt_box = FancyBboxPatch((6, 5), 3.5, 3,
                            boxstyle="round,pad=0.1",
                            edgecolor=config['color'],
                            facecolor=bg_color,
                            linewidth=2)
    ax.add_patch(rt_box)
    ax.text(7.75, 7.5, 'Real-time', ha='center', fontsize=9,
            color=config['color'], fontweight='bold')
    ax.text(7.75, 6.7, config['realtime'][0], ha='center',
            fontsize=7, color=text_primary)
    ax.text(7.75, 5.8, config['realtime'][1], ha='center',
            fontsize=7, color=text_primary)
    
    # Arrows connecting to hybrid
    arrow1 = FancyArrowPatch((4, 6.5), (4.8, 3.5),
                            arrowstyle='->', mutation_scale=15,
                            color=config['color'], linewidth=2)
    ax.add_patch(arrow1)
    
    arrow2 = FancyArrowPatch((6, 6.5), (5.2, 3.5),
                            arrowstyle='->', mutation_scale=15,
                            color=config['color'], linewidth=2)
    ax.add_patch(arrow2)
    
    # Hybrid solution (center bottom)
    hybrid_box = FancyBboxPatch((3, 1), 4, 2.5,
                                boxstyle="round,pad=0.1",
                                edgecolor=highlight,
                                facecolor=bg_color,
                                linewidth=2.5)
    ax.add_patch(hybrid_box)
    ax.text(5, 2.8, 'Hybrid Solution', ha='center', fontsize=9,
            color=highlight, fontweight='bold')
    ax.text(5, 2.2, config['hybrid'], ha='center', fontsize=8,
            color=text_primary)
    ax.text(5, 1.4, config['value'], ha='center', fontsize=9,
            color=green, fontweight='bold')

plt.suptitle('8 Advanced Use Cases: Hybrid Architecture Patterns', 
             fontsize=16, fontweight='bold', color=text_primary, y=0.995)

plt.tight_layout(rect=[0, 0, 1, 0.99])
print("✅ Architecture diagrams for all 8 use cases created")
print("Each diagram shows: Historical layer (left), Real-time layer (right), Hybrid solution (center)")
print("Color-coded by use case complexity and value\n")

arch_visualization = arch_fig
