# Fleet Guardian Implementation Guide: Hybrid Architecture

## Overview

This comprehensive implementation guide provides step-by-step instructions for deploying a hybrid OpenSearch + watsonx.data architecture for the Fleet Guardian telemetry platform.

## Architecture Summary

The implementation adds **watsonx.data** capabilities to the existing OpenSearch infrastructure, creating a dual-path system:

- **Real-time Path**: OpenSearch for operational monitoring, alerting, and dashboards
- **Analytics Path**: watsonx.data (Iceberg + Presto + Spark) for historical analysis, ML, and long-term storage

## Key Components

### 1. Service Architecture
- Existing OpenSearch services (maintained)
- New watsonx.data integration layer
- Dual-sink telemetry consumer
- Query federation layer

### 2. Deployment Configuration
- Podman Compose orchestration
- Container networking and service discovery
- Volume management and persistence
- Resource allocation

### 3. Data Pipeline
- Pulsar/Kafka message consumption
- Parallel writes to OpenSearch and watsonx.data
- Data format conversion (JSON → Parquet)
- Schema management

### 4. Query Layer
- OpenSearch Dashboards for operational views
- Presto CLI/JDBC for SQL analytics
- Spark notebooks for ML workloads
- Query routing based on use case

### 5. Operational Store Integration
- HCD/Cassandra CDC pipeline
- Scheduled exports to watsonx.data
- Data enrichment workflows

## Implementation Phases

1. **Phase 1**: Deploy watsonx.data services
2. **Phase 2**: Implement dual-sink consumer
3. **Phase 3**: Configure query layer
4. **Phase 4**: Integrate operational stores
5. **Phase 5**: Migration and validation
