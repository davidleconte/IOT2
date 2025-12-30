from pathlib import Path

diagrams_dir = Path("docs/diagrams")

# End-to-End Data Flow with DLQ/Retry/Quarantine Diagram
dataflow_mermaid = """graph LR
    subgraph "Ingestion Layer"
        VesselSensor[Vessel Sensors<br/>AIS, Engine, GPS<br/>1000 vessels @ 4msg/s]
        IngressAPI[Ingress API<br/>REST + gRPC<br/>TLS + JWT Auth]
    end
    
    subgraph "Apache Pulsar - Primary Topic"
        PrimaryTopic[persistent://tenant/vessel-tracking/telemetry<br/>Partitioned: 128<br/>KeyShared Subscription]
    end
    
    subgraph "Stream Processing Services"
        RawIngestion[Raw Ingestion Service<br/>Schema validation<br/>Deduplication check<br/>tenant_id enforcement]
        FeatureComp[Feature Computation<br/>Real-time aggregations<br/>Rolling windows 1m/5m/15m]
        AnomalyDet[Anomaly Detection<br/>Streaming ML inference<br/>Anomaly score generation]
    end
    
    subgraph "Error Handling - Retry Chain"
        Retry5s[Retry Topic - 5s delay<br/>Transient failures<br/>Max attempts: 3]
        Retry1m[Retry Topic - 1m delay<br/>Service unavailability<br/>Max attempts: 2]
        Retry10m[Retry Topic - 10m delay<br/>Downstream backpressure<br/>Max attempts: 1]
        Quarantine[Quarantine Topic<br/>Manual intervention required<br/>Alerts triggered]
    end
    
    subgraph "Dead Letter Queue"
        DLQ[DLQ Topic<br/>Schema violations<br/>Malformed messages<br/>Permanent failures]
        DLQProcessor[DLQ Handler Service<br/>Alert generation<br/>Message inspection<br/>Replay capability]
    end
    
    subgraph "Data Persistence Layer"
        Cassandra[(DataStax HCD<br/>Timeseries data<br/>Quorum writes<br/>RF=3)]
        OpenSearch[(OpenSearch<br/>Full-text search<br/>Anomaly indices<br/>Vector similarity)]
        WatsonX[(watsonx.data<br/>Iceberg tables<br/>Historical archive<br/>Analytics queries)]
    end
    
    subgraph "Feature Store & ML"
        FeastOnline[Feast Online Store<br/>Redis/Cassandra<br/>Low-latency retrieval<br/>< 10ms p99]
        FeastOffline[Feast Offline Store<br/>watsonx.data<br/>Training datasets<br/>Point-in-time joins]
        MLRegistry[MLflow Model Registry<br/>Model versioning<br/>A/B testing<br/>Rollback support]
    end
    
    subgraph "Inference Layer"
        RealtimeInf[Real-time Inference<br/>REST API<br/>Feast feature fetch<br/>Model prediction]
        BatchInf[Batch Inference<br/>Spark jobs<br/>Historical scoring<br/>Scheduled CronJobs]
    end
    
    subgraph "Monitoring & Observability"
        Metrics[Prometheus Metrics<br/>Message rates<br/>Latencies<br/>Error rates]
        Alerts[Alert Manager<br/>DLQ threshold alerts<br/>Quarantine notifications<br/>PagerDuty integration]
    end
    
    %% Data Flow - Success Path
    VesselSensor -->|4M msg/s| IngressAPI -->|Produce| PrimaryTopic
    PrimaryTopic -->|Consume KeyShared| RawIngestion
    RawIngestion -->|Success| FeatureComp & Cassandra
    FeatureComp -->|Computed features| FeastOnline & AnomalyDet
    AnomalyDet -->|Anomalies detected| OpenSearch
    RawIngestion -->|Archive| WatsonX
    
    %% Data Flow - Error Handling
    RawIngestion -.Transient error.-> Retry5s
    FeatureComp -.Service unavailable.-> Retry1m
    AnomalyDet -.Backpressure.-> Retry10m
    
    Retry5s -.Retry failed.-> Retry1m
    Retry1m -.Retry failed.-> Retry10m
    Retry10m -.Max retries exceeded.-> Quarantine
    
    RawIngestion -.Schema violation.-> DLQ
    DLQ --> DLQProcessor --> Alerts
    Quarantine --> Alerts
    
    %% Feature Store to Inference
    FeastOnline --> RealtimeInf
    FeastOffline --> BatchInf
    MLRegistry --> RealtimeInf & BatchInf
    
    %% Results back to storage
    RealtimeInf --> OpenSearch
    BatchInf --> WatsonX
    
    %% Monitoring
    PrimaryTopic & RawIngestion & FeatureComp & AnomalyDet --> Metrics
    DLQ & Quarantine --> Alerts
    
    style VesselSensor fill:#a1c9f4
    style PrimaryTopic fill:#ffb482
    style RawIngestion fill:#8de5a1
    style FeatureComp fill:#8de5a1
    style AnomalyDet fill:#8de5a1
    style Retry5s fill:#ff9f9b
    style Retry1m fill:#ff9f9b
    style Retry10m fill:#ff9f9b
    style Quarantine fill:#f04438
    style DLQ fill:#f04438
    style Cassandra fill:#d0bbff
    style OpenSearch fill:#d0bbff
    style WatsonX fill:#d0bbff
    style FeastOnline fill:#1f77b4
    style FeastOffline fill:#1f77b4
    style RealtimeInf fill:#ffd400
    style BatchInf fill:#ffd400
    style Alerts fill:#17b26a
"""

# Save Mermaid source
dataflow_path = diagrams_dir / "03_end_to_end_dataflow.mmd"
with open(dataflow_path, 'w') as f:
    f.write(dataflow_mermaid)

print(f"✅ End-to-End Data Flow Diagram")
print(f"   📄 Mermaid source: {dataflow_path}")
print(f"   📏 Size: {len(dataflow_mermaid)} characters")
print(f"\n🔄 Data Flow Paths:")
print(f"   • Success Path: Ingestion → Processing → Persistence → Feature Store → Inference")
print(f"   • Retry Chain: 5s → 1m → 10m delays with exponential backoff")
print(f"   • DLQ: Schema violations and permanent failures")
print(f"   • Quarantine: Max retry exhaustion, requires manual intervention")
print(f"   • Monitoring: Metrics collection and alerting at every stage")

dataflow_diagram_summary = {
    "diagram": "End-to-End Data Flow with DLQ/Retry/Quarantine",
    "file": str(dataflow_path),
    "throughput": "4M msg/s sustained",
    "error_handling": ["Retry 5s", "Retry 1m", "Retry 10m", "Quarantine", "DLQ"]
}
