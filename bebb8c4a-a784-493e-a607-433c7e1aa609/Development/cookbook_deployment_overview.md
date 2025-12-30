# Navtor Fleet Guardian - Deployment Cookbook

## 📚 Overview

This cookbook provides complete step-by-step instructions to deploy and validate the entire Navtor Fleet Guardian platform from scratch. Designed for new teams, this guide enables you to:

- Deploy all infrastructure components (Pulsar, Cassandra, OpenSearch, Watsonx)
- Configure multi-tenant isolation
- Set up ML training and inference pipelines
- Run comprehensive validation tests
- Verify data quality frameworks
- Execute end-to-end workflows

## 🎯 Purpose

This cookbook is your single source of truth for:
- **Initial deployment** of the platform
- **Validation** that all components work correctly
- **Training** new team members on the architecture
- **Reference** during troubleshooting

## 📋 Prerequisites

Before starting, ensure you have:

### 1. Infrastructure Access
- ✅ OpenShift/Kubernetes cluster (v1.24+)
- ✅ Cluster admin privileges
- ✅ kubectl/oc CLI configured
- ✅ Helm 3.x installed
- ✅ Terraform 1.5+ installed

### 2. IBM Subscriptions & Credentials
- ✅ Watsonx.data license and credentials
- ✅ Watsonx.ai API keys
- ✅ DataStax HCD license
- ✅ IBM Cloud account (if using managed services)

### 3. External Services
- ✅ S3-compatible object storage (for Iceberg tables)
- ✅ Container registry access (DockerHub, Quay.io, or private)
- ✅ DNS management for ingress routes

### 4. Development Tools
- ✅ Python 3.9+ with pip
- ✅ Java 11+ (for Spark jobs)
- ✅ Git CLI
- ✅ jq (for JSON processing)

### 5. Network Requirements
- ✅ Outbound HTTPS access for pulling images
- ✅ Internal cluster networking configured
- ✅ LoadBalancer or Ingress controller configured

### 6. Resource Quotas
Minimum cluster resources:
- **CPU**: 64 cores
- **Memory**: 256 GB
- **Storage**: 2 TB (PVs for stateful services)

## 🗂️ Repository Structure

```
navtor-fleet-guardian/
├── terraform/          # Infrastructure as Code
├── helm/              # Kubernetes deployments
├── ops/               # Operational configs (Pulsar, OpenSearch, Cassandra)
├── services/          # Stream processing microservices
├── ml/                # ML training and inference
├── security/          # RBAC, TLS, audit
├── tests/             # Integration test suite
└── docs/              # Documentation and diagrams
```

## 📖 How to Use This Cookbook

1. **Read prerequisites** - Ensure all requirements are met
2. **Follow deployment sequence** - Execute steps in order
3. **Validate each stage** - Run validation tests after each major step
4. **Refer to troubleshooting** - Check runbooks if issues arise
5. **Document customizations** - Keep notes on environment-specific changes

## ⚠️ Important Notes

- **Sequential execution required** - Components have dependencies
- **Validation gates** - Do not proceed if validation fails
- **Environment-specific values** - Replace placeholder credentials
- **Backup before production** - Test in dev/staging first
- **Security first** - Never commit credentials to Git

## 📞 Support & Resources

- **Architecture Diagrams**: `docs/diagrams/`
- **Runbooks**: `docs/runbook/`
- **Integration Tests**: `tests/integration/`
- **Troubleshooting**: Contact platform team

---

**Next**: Proceed to [Infrastructure Deployment](#terraform-deployment)