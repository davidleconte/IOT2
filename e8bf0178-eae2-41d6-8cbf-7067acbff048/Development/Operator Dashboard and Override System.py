import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime, timedelta
import json

# Zerve design system colors
bg_color = '#1D1D20'
text_primary = '#fbfbff'
text_secondary = '#909094'
light_blue = '#A1C9F4'
orange = '#FFB482'
green = '#8DE5A1'
coral = '#FF9F9B'
lavender = '#D0BBFF'
highlight = '#ffd400'
success_color = '#17b26a'
warning_color = '#f04438'

# ===== OPERATOR DASHBOARD ARCHITECTURE =====
dashboard_architecture = {
    'components': [
        {
            'name': 'Real-time Decision Feed',
            'description': 'Live stream of autonomous decisions with confidence scores',
            'technology': 'WebSocket + React',
            'refresh_rate': '< 1 second'
        },
        {
            'name': 'Decision Rationale Panel',
            'description': 'Explainability visualization showing factor contributions',
            'technology': 'SHAP waterfall charts',
            'refresh_rate': 'On decision event'
        },
        {
            'name': 'Override Controls',
            'description': 'Accept/Reject/Modify decision with reason dropdown',
            'technology': 'REST API + Event Bus',
            'refresh_rate': 'On-demand'
        },
        {
            'name': 'Vessel Fleet Map',
            'description': 'Geospatial view with decision markers',
            'technology': 'Mapbox GL + OpenSearch geo queries',
            'refresh_rate': '5 seconds'
        },
        {
            'name': 'Performance Metrics',
            'description': 'Decision latency, accuracy, override rate',
            'technology': 'Grafana dashboards',
            'refresh_rate': '30 seconds'
        },
        {
            'name': 'Audit Log Viewer',
            'description': 'Full decision history with search/filter',
            'technology': 'OpenSearch + Kibana',
            'refresh_rate': 'On-demand'
        }
    ]
}

dashboard_df = pd.DataFrame(dashboard_architecture['components'])

# ===== OVERRIDE CAPABILITY IMPLEMENTATION =====
override_system_code = '''
class OperatorOverrideSystem:
    """
    System for operators to review, accept, reject, or modify autonomous decisions
    All overrides are logged to audit trail with reason codes
    """
    
    def __init__(self, event_bus, audit_logger):
        self.event_bus = event_bus
        self.audit_logger = audit_logger
        self.pending_decisions = {}
        
    async def present_decision_to_operator(self, decision: DecisionOutput) -> dict:
        """
        Present decision to operator with full context and override options
        """
        override_request = {
            'decision_id': decision.audit_log_id,
            'scenario_id': decision.scenario_id,
            'vessel_id': decision.vessel_id,
            'timestamp': datetime.utcnow().isoformat(),
            'decision': decision.decision,
            'confidence': decision.confidence,
            'rationale': decision.rationale,
            'recommendations': decision.recommendations,
            'override_options': {
                'ACCEPT': 'Execute decision as recommended',
                'REJECT': 'Cancel decision and maintain current state',
                'MODIFY': 'Modify decision parameters',
                'DEFER': 'Require additional operator review'
            },
            'timeout_seconds': 300,  # 5 minute timeout for operator response
            'auto_execute_on_timeout': decision.confidence > 0.90  # High confidence = auto-execute
        }
        
        self.pending_decisions[decision.audit_log_id] = override_request
        await self.event_bus.publish('decision.operator_review', override_request)
        
        return override_request
    
    async def process_operator_override(self, decision_id: str, action: str, 
                                       reason_code: str, operator_id: str,
                                       modified_params: dict = None) -> dict:
        """
        Process operator override decision with audit logging
        """
        if decision_id not in self.pending_decisions:
            raise ValueError(f"Decision {decision_id} not found in pending queue")
        
        original_decision = self.pending_decisions[decision_id]
        
        override_result = {
            'decision_id': decision_id,
            'original_decision': original_decision['decision'],
            'override_action': action,
            'reason_code': reason_code,
            'operator_id': operator_id,
            'timestamp': datetime.utcnow().isoformat(),
            'modified_params': modified_params
        }
        
        # Log to audit trail
        await self.audit_logger.log_override(override_result)
        
        # Execute based on override action
        if action == 'ACCEPT':
            await self._execute_decision(original_decision)
        elif action == 'REJECT':
            await self._cancel_decision(original_decision, reason_code)
        elif action == 'MODIFY':
            await self._execute_modified_decision(original_decision, modified_params)
        elif action == 'DEFER':
            # Escalate to senior operator or decision committee
            await self._escalate_decision(original_decision, operator_id)
        
        # Publish override event
        await self.event_bus.publish('decision.override_processed', override_result)
        
        # Remove from pending queue
        del self.pending_decisions[decision_id]
        
        return override_result
    
    async def get_override_statistics(self, time_range_hours: int = 24) -> dict:
        """
        Get override statistics for performance monitoring
        """
        stats = await self.audit_logger.query_override_stats(time_range_hours)
        
        return {
            'total_decisions': stats['total_decisions'],
            'auto_executed': stats['auto_executed'],
            'operator_accepted': stats['operator_accepted'],
            'operator_rejected': stats['operator_rejected'],
            'operator_modified': stats['operator_modified'],
            'override_rate': stats['override_rate'],
            'average_response_time_seconds': stats['avg_response_time'],
            'top_rejection_reasons': stats['top_rejection_reasons']
        }
'''

# ===== FALLBACK MECHANISMS =====
fallback_mechanisms = pd.DataFrame([
    {
        'Failure Scenario': 'OpenSearch unavailable',
        'Fallback Strategy': 'Use cached data (5 min TTL) + degraded mode flag',
        'Impact': 'Real-time signals stale, confidence score reduced by 20%',
        'Recovery': 'Circuit breaker re-attempts every 30s'
    },
    {
        'Failure Scenario': 'watsonx.data query timeout',
        'Fallback Strategy': 'Skip historical analysis, use real-time only',
        'Impact': 'Reduced context, confidence score capped at 75%',
        'Recovery': 'Retry with shorter time window'
    },
    {
        'Failure Scenario': 'ML model inference failure',
        'Fallback Strategy': 'Use rule-based heuristics',
        'Impact': 'Lower accuracy, flag decision as "degraded"',
        'Recovery': 'Model health check + reload'
    },
    {
        'Failure Scenario': 'Decision latency > 500ms',
        'Fallback Strategy': 'Stream partial results, complete async',
        'Impact': 'Operator sees preliminary decision immediately',
        'Recovery': 'Update UI when full analysis completes'
    },
    {
        'Failure Scenario': 'Network partition',
        'Fallback Strategy': 'Local decision cache + queue for sync',
        'Impact': 'Offline operation with limited context',
        'Recovery': 'Sync decisions when connectivity restored'
    },
    {
        'Failure Scenario': 'Operator not responding',
        'Fallback Strategy': 'Auto-execute if confidence > 90%, else escalate',
        'Impact': 'High-confidence decisions proceed automatically',
        'Recovery': 'Notify operator post-execution'
    }
])

# ===== AUDIT LOGGING SCHEMA =====
audit_log_schema = {
    'index_name': 'decision_audit',
    'fields': [
        {'name': 'timestamp', 'type': 'date', 'description': 'Decision timestamp'},
        {'name': 'decision_id', 'type': 'keyword', 'description': 'Unique decision ID'},
        {'name': 'scenario_id', 'type': 'keyword', 'description': 'Scenario type'},
        {'name': 'vessel_id', 'type': 'keyword', 'description': 'Target vessel'},
        {'name': 'decision', 'type': 'keyword', 'description': 'Decision action'},
        {'name': 'confidence', 'type': 'float', 'description': 'Confidence score'},
        {'name': 'rationale', 'type': 'nested', 'description': 'Explainability factors'},
        {'name': 'recommendations', 'type': 'object', 'description': 'Decision recommendations'},
        {'name': 'input_signals', 'type': 'object', 'description': 'All input signals'},
        {'name': 'processing_time_ms', 'type': 'integer', 'description': 'Processing latency'},
        {'name': 'override_action', 'type': 'keyword', 'description': 'Operator override (if any)'},
        {'name': 'override_reason', 'type': 'text', 'description': 'Override reason'},
        {'name': 'operator_id', 'type': 'keyword', 'description': 'Operator who overrode'},
        {'name': 'execution_status', 'type': 'keyword', 'description': 'Executed/Rejected/Modified'},
        {'name': 'outcome', 'type': 'object', 'description': 'Actual outcome of decision'}
    ]
}

audit_schema_df = pd.DataFrame(audit_log_schema['fields'])

# ===== MOCK OPERATOR DASHBOARD VISUALIZATION =====
operator_dashboard_fig = plt.figure(figsize=(18, 12), facecolor=bg_color)
operator_dashboard_fig.suptitle('Autonomous Decision System - Operator Dashboard', 
                               fontsize=22, color=text_primary, weight='bold', y=0.98)

# Dashboard layout (3x3 grid)
dashboard_gs = plt.GridSpec(3, 3, figure=operator_dashboard_fig, hspace=0.35, wspace=0.35,
                           left=0.05, right=0.95, top=0.93, bottom=0.05)

# 1. Real-time Decision Feed (top-left)
ax_feed = operator_dashboard_fig.add_subplot(dashboard_gs[0, 0])
ax_feed.set_xlim(0, 10)
ax_feed.set_ylim(0, 10)
ax_feed.axis('off')
ax_feed.set_title('Real-Time Decision Feed', color=text_primary, fontsize=14, weight='bold', pad=10)

recent_decisions = [
    {'time': '14:32:15', 'vessel': 'MV_PACIFIC_001', 'scenario': 'REROUTE', 'status': 'PENDING', 'conf': 0.82},
    {'time': '14:31:48', 'vessel': 'MV_ATLANTIC_045', 'scenario': 'SPEED_REDUCTION', 'status': 'ACCEPTED', 'conf': 0.78},
    {'time': '14:30:22', 'vessel': 'MV_CARGO_112', 'scenario': 'EMERGENCY_STOP', 'status': 'EXECUTED', 'conf': 0.95},
]

y_pos_feed = 9
for dec in recent_decisions:
    status_color_decision = highlight if dec['status'] == 'PENDING' else success_color if dec['status'] in ['ACCEPTED', 'EXECUTED'] else warning_color
    ax_feed.add_patch(mpatches.FancyBboxPatch((0.2, y_pos_feed - 0.8), 9.6, 1.5, 
                                              boxstyle="round,pad=0.1", 
                                              facecolor=status_color_decision, alpha=0.15, 
                                              edgecolor=status_color_decision, linewidth=1.5))
    ax_feed.text(0.5, y_pos_feed, dec['time'], color=text_secondary, fontsize=9, va='center')
    ax_feed.text(2, y_pos_feed, dec['vessel'], color=text_primary, fontsize=10, weight='bold', va='center')
    ax_feed.text(5.5, y_pos_feed, dec['scenario'], color=text_primary, fontsize=9, va='center')
    ax_feed.text(8, y_pos_feed, f"{dec['conf']:.0%}", color=text_primary, fontsize=10, va='center')
    y_pos_feed -= 2.5

# 2. Decision Rationale (top-middle) - Waterfall chart concept
ax_rationale = operator_dashboard_fig.add_subplot(dashboard_gs[0, 1])
ax_rationale.set_facecolor(bg_color)
ax_rationale.set_title('Decision Rationale - REROUTE (MV_PACIFIC_001)', 
                       color=text_primary, fontsize=14, weight='bold', pad=10)

factors = ['Baseline', 'ML Congestion\n(+34%)', 'Weather\n(+21%)', 'Berth Avail\n(+8%)', 
           'Schedule\n(+2%)', 'Final Score']
values = [0, 0.34, 0.21, 0.08, 0.02, 0]
cumulative = [0, 0.34, 0.55, 0.63, 0.65, 0.65]
colors_waterfall = [text_secondary, light_blue, orange, green, lavender, highlight]

for i in range(len(factors)):
    if i == 0:
        ax_rationale.bar(i, 0, color=colors_waterfall[i], edgecolor=text_primary, linewidth=1.5, width=0.6)
    elif i < len(factors) - 1:
        ax_rationale.bar(i, values[i], bottom=cumulative[i-1], 
                        color=colors_waterfall[i], edgecolor=text_primary, linewidth=1.5, width=0.6)
        ax_rationale.text(i, cumulative[i] + 0.02, f"+{values[i]:.0%}", 
                         ha='center', va='bottom', color=text_primary, fontsize=9, weight='bold')
    else:
        ax_rationale.bar(i, cumulative[i-1], color=colors_waterfall[i], 
                        edgecolor=text_primary, linewidth=2, width=0.6)
        ax_rationale.text(i, cumulative[i-1] + 0.02, f"{cumulative[i-1]:.0%}", 
                         ha='center', va='bottom', color=text_primary, fontsize=11, weight='bold')

ax_rationale.set_xticks(range(len(factors)))
ax_rationale.set_xticklabels(factors, color=text_primary, fontsize=9)
ax_rationale.set_ylabel('Confidence Contribution', color=text_primary, fontsize=10)
ax_rationale.tick_params(colors=text_primary)
ax_rationale.spines['bottom'].set_color(text_secondary)
ax_rationale.spines['left'].set_color(text_secondary)
ax_rationale.spines['top'].set_visible(False)
ax_rationale.spines['right'].set_visible(False)
ax_rationale.set_ylim(0, 0.8)
ax_rationale.grid(axis='y', alpha=0.2, color=text_secondary)

# 3. Override Controls (top-right)
ax_override = operator_dashboard_fig.add_subplot(dashboard_gs[0, 2])
ax_override.set_xlim(0, 10)
ax_override.set_ylim(0, 10)
ax_override.axis('off')
ax_override.set_title('Override Controls', color=text_primary, fontsize=14, weight='bold', pad=10)

button_y = 8.5
button_configs = [
    {'label': 'ACCEPT', 'color': success_color},
    {'label': 'REJECT', 'color': warning_color},
    {'label': 'MODIFY', 'color': orange},
    {'label': 'DEFER', 'color': text_secondary}
]

for btn in button_configs:
    ax_override.add_patch(mpatches.FancyBboxPatch((1, button_y - 0.6), 8, 1.2, 
                                                  boxstyle="round,pad=0.1", 
                                                  facecolor=btn['color'], alpha=0.3, 
                                                  edgecolor=btn['color'], linewidth=2))
    ax_override.text(5, button_y, btn['label'], color=text_primary, fontsize=12, 
                    weight='bold', ha='center', va='center')
    button_y -= 2

ax_override.text(5, 1.5, 'Reason: Port capacity updated', color=text_secondary, 
                fontsize=9, ha='center', style='italic')

# 4. Processing Latency Chart (middle-left)
ax_latency = operator_dashboard_fig.add_subplot(dashboard_gs[1, 0])
ax_latency.set_facecolor(bg_color)
ax_latency.set_title('Decision Processing Latency (Last Hour)', 
                    color=text_primary, fontsize=14, weight='bold', pad=10)

time_points_latency = pd.date_range(end=datetime.now(), periods=20, freq='3min')
latency_values = np.random.normal(280, 60, 20)
latency_values = np.clip(latency_values, 150, 450)

ax_latency.plot(range(len(time_points_latency)), latency_values, 
               color=light_blue, linewidth=2.5, marker='o', markersize=5)
ax_latency.axhline(500, color=warning_color, linestyle='--', linewidth=2, label='Target: 500ms')
ax_latency.fill_between(range(len(time_points_latency)), latency_values, 500, 
                        where=(latency_values <= 500), color=success_color, alpha=0.2)

ax_latency.set_xlabel('Time', color=text_primary, fontsize=10)
ax_latency.set_ylabel('Latency (ms)', color=text_primary, fontsize=10)
ax_latency.tick_params(colors=text_primary)
ax_latency.legend(loc='upper right', facecolor=bg_color, edgecolor=text_secondary, 
                 labelcolor=text_primary, fontsize=9)
ax_latency.spines['bottom'].set_color(text_secondary)
ax_latency.spines['left'].set_color(text_secondary)
ax_latency.spines['top'].set_visible(False)
ax_latency.spines['right'].set_visible(False)
ax_latency.grid(alpha=0.2, color=text_secondary)

# 5. Decision Accuracy & Override Rate (middle-center)
ax_accuracy = operator_dashboard_fig.add_subplot(dashboard_gs[1, 1])
ax_accuracy.set_facecolor(bg_color)
ax_accuracy.set_title('Decision Accuracy & Override Rate (24h)', 
                     color=text_primary, fontsize=14, weight='bold', pad=10)

metrics_data = ['Accepted\n(87%)', 'Rejected\n(8%)', 'Modified\n(5%)']
metrics_values = [87, 8, 5]
metrics_colors = [success_color, warning_color, orange]

wedges, texts = ax_accuracy.pie(metrics_values, colors=metrics_colors, startangle=90, 
                                wedgeprops={'edgecolor': bg_color, 'linewidth': 2})

for i, (wedge, label, value) in enumerate(zip(wedges, metrics_data, metrics_values)):
    angle = (wedge.theta2 - wedge.theta1) / 2 + wedge.theta1
    x = np.cos(np.radians(angle))
    y = np.sin(np.radians(angle))
    ax_accuracy.text(x * 0.6, y * 0.6, label, ha='center', va='center', 
                    color=text_primary, fontsize=11, weight='bold')

ax_accuracy.text(0, -1.5, 'Total Decisions: 1,247', ha='center', 
                color=text_primary, fontsize=11, weight='bold')

# 6. Scenario Performance (middle-right)
ax_scenarios = operator_dashboard_fig.add_subplot(dashboard_gs[1, 2])
ax_scenarios.set_facecolor(bg_color)
ax_scenarios.set_title('Scenario Performance (24h)', color=text_primary, fontsize=14, weight='bold', pad=10)

scenarios_perf = ['REROUTE', 'HELICOPTER', 'SPEED_REDUCE', 'EMERGENCY']
avg_confidence = [0.82, 0.91, 0.78, 0.95]
count_decisions = [45, 3, 28, 2]

x_pos_scenarios = np.arange(len(scenarios_perf))
bars_conf = ax_scenarios.bar(x_pos_scenarios, avg_confidence, color=[light_blue, coral, green, warning_color], 
                            edgecolor=text_primary, linewidth=1.5, alpha=0.7)

for i, (bar, count) in enumerate(zip(bars_conf, count_decisions)):
    height = bar.get_height()
    ax_scenarios.text(bar.get_x() + bar.get_width()/2, height + 0.02, 
                     f"{height:.0%}\n({count})", ha='center', va='bottom', 
                     color=text_primary, fontsize=9, weight='bold')

ax_scenarios.set_xticks(x_pos_scenarios)
ax_scenarios.set_xticklabels(scenarios_perf, color=text_primary, fontsize=9, rotation=15, ha='right')
ax_scenarios.set_ylabel('Avg Confidence', color=text_primary, fontsize=10)
ax_scenarios.tick_params(colors=text_primary)
ax_scenarios.spines['bottom'].set_color(text_secondary)
ax_scenarios.spines['left'].set_color(text_secondary)
ax_scenarios.spines['top'].set_visible(False)
ax_scenarios.spines['right'].set_visible(False)
ax_scenarios.set_ylim(0, 1.1)
ax_scenarios.grid(axis='y', alpha=0.2, color=text_secondary)

# 7-9. System Status panels (bottom row)
status_panels = [
    {'title': 'Data Sources', 'items': [
        ('OpenSearch', 'HEALTHY', success_color, '98ms avg'),
        ('watsonx.data', 'HEALTHY', success_color, '145ms avg'),
        ('ML Models', 'HEALTHY', success_color, '62ms avg')
    ]},
    {'title': 'Circuit Breakers', 'items': [
        ('OpenSearch CB', 'CLOSED', success_color, '0 trips'),
        ('Presto CB', 'CLOSED', success_color, '0 trips'),
        ('ML Inference CB', 'CLOSED', success_color, '0 trips')
    ]},
    {'title': 'Active Alerts', 'items': [
        ('High latency spike', 'WARNING', highlight, '13:45'),
        ('Model drift detected', 'INFO', light_blue, '12:30'),
        ('No critical alerts', 'INFO', success_color, '')
    ]}
]

for panel_idx, panel_config in enumerate(status_panels):
    ax_status = operator_dashboard_fig.add_subplot(dashboard_gs[2, panel_idx])
    ax_status.set_xlim(0, 10)
    ax_status.set_ylim(0, 10)
    ax_status.axis('off')
    ax_status.set_title(panel_config['title'], color=text_primary, fontsize=14, weight='bold', pad=10)
    
    y_status = 8.5
    for item_name, status, color, detail in panel_config['items']:
        ax_status.add_patch(mpatches.Circle((1, y_status), 0.3, color=color, alpha=0.8))
        ax_status.text(2, y_status, item_name, color=text_primary, fontsize=10, va='center')
        ax_status.text(6, y_status, status, color=color, fontsize=9, weight='bold', va='center')
        ax_status.text(8.5, y_status, detail, color=text_secondary, fontsize=8, va='center')
        y_status -= 2.5

plt.tight_layout()

print("=" * 80)
print("OPERATOR DASHBOARD & OVERRIDE SYSTEM")
print("=" * 80)
print("\nDashboard Components:")
print(dashboard_df.to_string(index=False))
print("\n")

print("FALLBACK MECHANISMS")
print("=" * 80)
print(fallback_mechanisms.to_string(index=False))
print("\n")

print("AUDIT LOG SCHEMA")
print("=" * 80)
print(audit_schema_df.to_string(index=False))
print("\n")

print("✓ Real-time decision feed with WebSocket updates")
print("✓ SHAP-based explainability waterfall charts")
print("✓ Accept/Reject/Modify/Defer override controls")
print("✓ Performance monitoring: latency, accuracy, override rate")
print("✓ System health monitoring with circuit breakers")
print("✓ Comprehensive audit logging to OpenSearch")
