# Watsonx.orchestrate Workflow Implementation

## Overview
This implementation provides two production-ready watsonx.orchestrate workflows for the maritime platform, demonstrating human-in-the-loop decision-making, comprehensive audit logging, and deep integration with platform components (OpenSearch, Pulsar, Feast, MLflow).

## Workflows Implemented

### 1. Anomaly Triage Workflow (`anomaly-triage-hitl-v1`)
**Purpose**: Automated anomaly detection and triage with human approval gates

**Key Features**:
- 10 workflow steps with 3 HITL decision points
- OpenSearch integration for anomaly detection and alert management
- Feast feature enrichment for context
- MLflow model inference for classification
- Conditional branching based on human decisions (approve, reject, escalate, investigate)
- Real-time notifications via Pulsar

**Workflow Steps**:
1. **Detect Anomalies** - Query OpenSearch for unreviewed anomalies
2. **Enrich with Vessel Data** - Fetch features from Feast
3. **Classify and Prioritize** - ML-based severity classification via MLflow
4. **Human Review Decision** ⚠️ HITL - Fleet manager reviews and decides action
5. **Execute Approved Actions** - Create alerts, notify operators
6. **Update Anomaly Status** - Mark as reviewed in OpenSearch
7. **Workflow Completion** - Log completion to Pulsar
8. **Handle Rejection** - Mark as false positive
9. **Escalate to Senior** ⚠️ HITL - Senior management review
10. **Assign to Analyst** - Create investigation task

**HITL Decision Points**:
- **Step 4**: Primary review by fleet manager/operations lead (60 min timeout)
- **Step 9**: Escalation to senior management (120 min timeout)
- Investigation assignment with analyst selection

**Audit Trail**: All steps log to `persistent://{{tenant}}/audit/workflow-events` and `persistent://{{tenant}}/audit/hitl-decisions`

---

### 2. Compliance Report Approval Workflow (`compliance-report-approval-v1`)
**Purpose**: Generate and submit maritime compliance reports with regulatory approval workflow

**Key Features**:
- 9 workflow steps with 3 HITL decision points
- Multi-index OpenSearch data retrieval (4 parallel queries)
- Automated compliance metric calculation
- PDF report generation
- Electronic signature collection
- Regulatory authority submission (IMO, EU MRV)
- Amendment workflow

**Workflow Steps**:
1. **Retrieve Compliance Data** - Parallel queries to 4 OpenSearch indices
2. **Generate Compliance Metrics** - Calculate KPIs and detect violations
3. **Generate Report Draft** - Create PDF with charts and tables
4. **Compliance Officer Review** ⚠️ HITL - Review and approve/reject (8 hour timeout)
5. **Submit to Authorities** - Automated submission to IMO, EU MRV APIs
6. **Apply Amendments** - Regenerate report with changes
7. **Handle Rejection** - Notify data quality team
8. **Escalate to Legal** ⚠️ HITL - Legal/CEO review for complex issues (24 hour timeout)
9. **Archive and Complete** - Store in OpenSearch compliance registry

**Data Sources**:
- `{{tenant}}_vessel_telemetry` - Emissions and fuel data
- `{{tenant}}_incidents` - Violations and non-compliance events
- `{{tenant}}_maintenance_events` - Inspection completion
- `audit_data_access` - GDPR data access logs

**HITL Decision Points**:
- **Step 4**: Compliance officer review with electronic signature
- **Step 8**: Legal/senior management escalation for complex cases

**Compliance Coverage**: IMO DCS, EU MRV, SOx/NOx emissions, GDPR, ballast water

---

## Audit Trail Infrastructure

### OpenSearch Indices
Created for comprehensive audit logging:

1. **`audit_workflow_events`** - All workflow execution events
2. **`audit_hitl_decisions`** - Human decision records with signatures
3. **`audit_compliance_submissions`** - Regulatory submission tracking
4. **`audit_data_access`** - GDPR-compliant data access logs

### Pulsar Audit Topics
Real-time event streaming:

1. **`persistent://{{tenant}}/audit/workflow-events`** - Workflow lifecycle events
2. **`persistent://{{tenant}}/audit/hitl-decisions`** - HITL decision audit trail
3. **`persistent://{{tenant}}/audit/compliance-submissions`** - Submission confirmations

### Audit Queries (6 predefined)

#### 1. Workflow Execution History
- Last 30 days of workflow runs
- Aggregations by workflow type, status, duration
- Timeline visualization

#### 2. HITL Decisions Audit
- All human decisions with reviewer, timestamp, duration
- Approval rates by reviewer and workflow
- 7-year retention for compliance
- CSV export format

#### 3. Anomaly Triage Effectiveness
- Completion rate, avg anomalies processed
- Decision distribution (approve/reject/escalate)
- Action execution tracking

#### 4. Compliance Submission Tracking
- Submissions by authority (IMO, EU MRV)
- On-time submission monitoring
- Alert triggers for late/failed submissions

#### 5. GDPR Data Access Audit
- Personal data access events from workflows
- Access patterns by user and workflow
- Breach detection (unauthorized access threshold: 5)

#### 6. Workflow Performance Monitoring
- Step-by-step duration analysis (avg, max, p95)
- OpenSearch query performance
- HITL timeout tracking
- SLA compliance (anomaly triage: 30 min, compliance: 2 hours)

---

## Platform Integration

### OpenSearch
- **Data Retrieval**: Multi-index parallel queries with aggregations
- **Alert Creation**: Automated alert indexing from workflow actions
- **Status Updates**: Update-by-query for bulk status changes
- **Compliance Registry**: Archive workflow results and reports

### Pulsar
- **Audit Logging**: All workflow events streamed to audit topics
- **Notifications**: Operator alerts and compliance notifications
- **Workflow Completion**: Results published to analytics topics
- **DLQ Integration**: Failed actions routed to appropriate topics

### Feast
- **Feature Enrichment**: Online feature retrieval for anomaly context
- **Vessel Metadata**: Operational and historical vessel features
- **Feature Services**: `vessel_enrichment_service` for real-time lookup

### MLflow
- **Model Inference**: Anomaly classification using production models
- **Model Registry**: `models:/anomaly-classifier-{{tenant}}/Production`
- **Multi-tenant**: Tenant-specific model versions

---

## Approval Forms

### Anomaly Triage Form
- **Fields**: Anomaly table, decision radio buttons, action checkboxes, notes, analyst assignment
- **Approval Roles**: `fleet_manager`, `operations_lead`
- **Timeout**: 60 minutes → escalate
- **Conditional Fields**: Actions visible only on approve, analyst dropdown on investigate

### Compliance Report Form
- **Fields**: Document viewer, metrics dashboard, decision radio, amendments checklist, authority selection, e-signature
- **Approval Roles**: `compliance_officer`, `dpo`
- **Timeout**: 8 hours → escalate
- **E-signature**: Required with hash recording for non-repudiation

### Escalation Forms
- Both workflows include escalation paths to senior management/legal
- Extended timeouts (2-24 hours)
- Read-only context display
- Simplified decision options

---

## Error Handling & Retry

### Global Policies
- **Max Retries**: 3
- **Backoff Multiplier**: 2x (5s → 10s → 20s)
- **Audit Failure Action**: `continue_and_log` (never block workflow on audit failures)

### Step-Level Handling
- **OpenSearch Query Failures**: Retry 3x with exponential backoff
- **HITL Timeouts**: Auto-escalate to next approval level
- **Submission Failures**: Retry and alert compliance officer

---

## Security & Compliance

### GDPR
- 7-year audit retention
- Personal data access tracking
- Breach detection and alerting
- Right to access/rectification/erasure support

### Non-Repudiation
- Electronic signatures with hash recording
- Immutable audit trail in Pulsar
- Reviewer identity and timestamp tracking

### Multi-Tenancy
- Tenant-aware workflows with `{{tenant}}` templating
- Isolated data access via tenant-scoped indices
- Tenant-specific model versions

---

## Deployment

### Files Created
```
ops/watsonx-orchestrate/
├── anomaly_triage_hitl_workflow.json       # Anomaly workflow definition
├── compliance_report_approval_workflow.json # Compliance workflow definition
└── audit-queries/
    ├── workflow_execution_history.json
    ├── hitl_decisions_audit.json
    ├── anomaly_triage_effectiveness.json
    ├── compliance_submission_tracking.json
    ├── gdpr_data_access_audit.json
    ├── workflow_performance_monitoring.json
    └── query_executor.py                    # Query execution script
```

### Prerequisites
- Watsonx.orchestrate instance configured
- OpenSearch cluster with audit indices
- Pulsar cluster with audit topics provisioned
- Feast online store deployed
- MLflow model registry with `anomaly-classifier-{{tenant}}` models

### Configuration
Set environment variables:
- `OPENSEARCH_HOST`: OpenSearch endpoint
- `PULSAR_BROKER`: Pulsar broker URL
- `IMO_SUBMISSION_API`: IMO DCS API endpoint
- `EU_THETIS_API`: EU THETIS MRV API endpoint

### Workflow Registration
```bash
# Register workflows with watsonx.orchestrate
curl -X POST https://watsonx-orchestrate-api/v1/workflows \
  -H "Content-Type: application/json" \
  -d @ops/watsonx-orchestrate/anomaly_triage_hitl_workflow.json

curl -X POST https://watsonx-orchestrate-api/v1/workflows \
  -H "Content-Type: application/json" \
  -d @ops/watsonx-orchestrate/compliance_report_approval_workflow.json
```

### Query Executor Setup
```bash
cd ops/watsonx-orchestrate/audit-queries
pip install opensearch-py pulsar-client
python query_executor.py
```

---

## Success Criteria ✅

✅ **Functional orchestrate workflow**: Two production workflows implemented  
✅ **HITL decision points**: 3 in anomaly triage, 3 in compliance (6 total)  
✅ **Audit logging to Pulsar**: All steps log to audit topics  
✅ **OpenSearch integration**: Data retrieval (4 indices), alerting, status updates  
✅ **Integration with platform**: Feast, MLflow, Pulsar, OpenSearch fully integrated  
✅ **Workflow definitions**: Complete JSON definitions with all integrations  
✅ **Approval forms**: Comprehensive forms with conditional fields and e-signature  
✅ **Audit trail queries**: 6 predefined queries with OpenSearch + Pulsar  

---

## Monitoring & Operations

### Metrics to Track
- Workflow completion rate
- Average HITL review time
- Timeout frequency
- Approval vs rejection rate
- OpenSearch query performance
- Compliance submission success rate

### Alerting
- Late compliance submissions
- Failed regulatory submissions
- Workflow timeouts exceeding SLA
- GDPR breach detection
- Unauthorized data access

### Performance SLAs
- Anomaly triage: 30 minutes end-to-end
- Compliance report: 2 hours end-to-end
- OpenSearch queries: <5 seconds
- HITL timeout: 8 hours (compliance), 1 hour (anomaly)
