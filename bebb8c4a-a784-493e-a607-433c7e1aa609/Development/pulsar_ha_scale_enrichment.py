import os
import yaml
import json
from pathlib import Path

# Base directory
pulsar_dir = "ops/pulsar"

# ===== HIGH AVAILABILITY SPECIFICATIONS =====

# Multi-AZ placement policy for BookKeeper ensembles
bookkeeper_placement_policy = {
    "apiVersion": "bookkeeper.apache.org/v1alpha1",
    "kind": "BookKeeperPlacementPolicy",
    "metadata": {
        "name": "multi-az-rack-aware",
        "namespace": "pulsar"
    },
    "spec": {
        "rackAwarenessEnabled": True,
        "ensemblePlacementPolicy": "org.apache.bookkeeper.client.RackawareEnsemblePlacementPolicy",
        "ensembleSize": 5,  # Total number of bookies to write to
        "writeQuorum": 5,   # Number of bookies to wait for write ack
        "ackQuorum": 3,     # Minimum acks needed for success
        "minNumRacksPerWriteQuorum": 3,  # Spread across 3 AZs minimum
        "networkTopology": {
            "zone1": ["/az-us-east-1a"],
            "zone2": ["/az-us-east-1b"],
            "zone3": ["/az-us-east-1c"]
        }
    }
}

# BookKeeper ensemble configuration
bookkeeper_ha_config = {
    "ensemblePlacementPolicy": {
        "description": "Multi-AZ rack-aware placement for data durability",
        "policy": "RackawareEnsemblePlacementPolicy",
        "ensembleSize": 5,
        "writeQuorum": 5,
        "ackQuorum": 3,
        "minNumRacksPerWriteQuorum": 3,
        "rackAwarenessEnabled": True,
        "isolation_groups": {
            "az1": ["bookie-0", "bookie-1"],
            "az2": ["bookie-2", "bookie-3"],
            "az3": ["bookie-4", "bookie-5"]
        }
    },
    "autoRecovery": {
        "enabled": True,
        "replicationRate": 100,  # MB/s
        "maxConcurrentReplication": 20,
        "auditorPeriodicCheckInterval": 3600,  # 1 hour in seconds
        "auditorPeriodicBookieCheckInterval": 86400  # 24 hours
    },
    "journalSync": {
        "flushWhenQueueEmpty": True,
        "maxGroupWaitInNanos": 200000,  # 200 microseconds
        "journalSyncData": True
    },
    "storage": {
        "journalDirectories": ["/pulsar/data/bookkeeper/journal"],
        "ledgerDirectories": ["/pulsar/data/bookkeeper/ledgers"],
        "storageClass": "fast-ssd",  # Use premium SSDs for journals
        "ledgerStorageClass": "standard-ssd"
    }
}

# Broker load balancing configuration for HA
broker_load_balancing = {
    "loadBalancerEnabled": True,
    "loadBalancerAutoBundleSplitEnabled": True,
    "loadBalancerAutoUnloadSplitBundlesEnabled": True,
    "loadBalancerSheddingEnabled": True,
    "loadBalancerSheddingIntervalMinutes": 1,
    "loadBalancerSheddingGracePeriodMinutes": 5,
    "loadBalancerBrokerMaxTopics": 50000,
    "loadBalancerBrokerUnderloadedThresholdPercentage": 50,
    "loadBalancerBrokerOverloadedThresholdPercentage": 85,
    "loadBalancerBundleUnloadMinThroughputThreshold": 10485760,  # 10 MB/s
    "loadBalancerPlacementStrategy": "leastLoadedServer",
    "brokerServicePurgeInactiveFrequencyInSeconds": 60,
    "failureDomain": {
        "enabled": True,
        "domains": {
            "domain-az1": ["broker-0", "broker-1", "broker-2"],
            "domain-az2": ["broker-3", "broker-4", "broker-5"],
            "domain-az3": ["broker-6", "broker-7", "broker-8"]
        }
    }
}

# Automated recovery configurations
automated_recovery = {
    "topicAutoRecovery": {
        "enabled": True,
        "checkIntervalSeconds": 60,
        "maxRetries": 5,
        "backoffMultiplier": 2.0
    },
    "brokerFailover": {
        "enabled": True,
        "heartbeatIntervalSeconds": 5,
        "maxMissedHeartbeats": 3,
        "failoverDelaySeconds": 10
    },
    "zkSessionRecovery": {
        "sessionTimeoutMs": 30000,
        "connectionTimeoutMs": 10000,
        "retryPolicy": {
            "maxRetries": 10,
            "baseSleepTimeMs": 1000,
            "maxSleepTimeMs": 60000
        }
    },
    "metadataStoreFailover": {
        "operationTimeoutSeconds": 30,
        "sessionTimeoutSeconds": 30,
        "allowReadOnlyOperations": True
    }
}

# Rolling upgrade procedures
rolling_upgrade_config = {
    "strategy": "RollingUpdate",
    "maxUnavailable": 1,
    "maxSurge": 1,
    "procedure": {
        "pre_upgrade_checks": [
            "Verify cluster health",
            "Check replication lag < 1s",
            "Ensure all topics have min replicas",
            "Backup ZooKeeper metadata",
            "Verify BookKeeper ensemble health"
        ],
        "upgrade_order": [
            "1. BookKeeper nodes (one rack at a time)",
            "2. Pulsar brokers (one failure domain at a time)",
            "3. Pulsar proxies",
            "4. Function workers"
        ],
        "per_component_steps": {
            "bookkeeper": [
                "Drain traffic from bookie",
                "Wait for under-replicated ledgers to heal",
                "Upgrade bookie",
                "Verify bookie rejoins cluster",
                "Wait 60s before next bookie"
            ],
            "broker": [
                "Enable graceful shutdown",
                "Trigger bundle unload",
                "Wait for bundles to migrate (max 5 min)",
                "Upgrade broker",
                "Verify broker health and topic ownership",
                "Wait 30s before next broker"
            ]
        },
        "rollback_strategy": {
            "enabled": True,
            "healthCheckFailureThreshold": 3,
            "autoRollbackEnabled": True,
            "preservePreviousVersion": True
        }
    }
}

# ===== SCALE SPECIFICATIONS (4M sustained, 8M burst) =====

# Benchmark configurations
benchmark_config = {
    "target_throughput": {
        "sustained_msg_per_sec": 4000000,
        "burst_msg_per_sec": 8000000,
        "avg_message_size_bytes": 1024,
        "sustained_throughput_mbps": 4000,  # 4 GB/s
        "burst_throughput_mbps": 8000       # 8 GB/s
    },
    "performance_tuning": {
        "broker": {
            "numIOThreads": 16,
            "numWorkerThreads": 32,
            "numHttpServerThreads": 16,
            "maxConcurrentLookupRequest": 50000,
            "maxConcurrentTopicLoadRequest": 5000,
            "brokerDeleteInactiveTopicsEnabled": False,
            "dispatchThrottlingRatePerTopicInMsg": 100000,
            "dispatchThrottlingRatePerTopicInByte": 104857600,  # 100 MB/s per topic
            "maxUnackedMessagesPerConsumer": 50000,
            "maxUnackedMessagesPerSubscription": 200000,
            "managedLedgerDefaultEnsembleSize": 5,
            "managedLedgerDefaultWriteQuorum": 5,
            "managedLedgerDefaultAckQuorum": 3,
            "managedLedgerMaxEntriesPerLedger": 50000,
            "managedLedgerMinLedgerRolloverTimeMinutes": 10,
            "managedLedgerMaxLedgerRolloverTimeMinutes": 240,
            "managedLedgerCacheSizeMB": 8192,
            "managedLedgerCacheEvictionWatermark": 0.9,
            "brokerServicePort": 6650,
            "webServicePort": 8080
        },
        "bookkeeper": {
            "journalMaxGroupWaitMSec": 1,
            "journalBufferedWritesThreshold": 524288,  # 512 KB
            "journalBufferedEntriesThreshold": 500,
            "journalFlushWhenQueueEmpty": True,
            "journalAdaptiveGroupWrites": True,
            "dbStorage_writeCacheMaxSizeMb": 2048,
            "dbStorage_readAheadCacheMaxSizeMb": 1024,
            "dbStorage_rocksDB_blockCacheSize": 2147483648,  # 2 GB
            "numAddWorkerThreads": 16,
            "numReadWorkerThreads": 16,
            "numJournalCallbackThreads": 8,
            "nettyMaxFrameSizeBytes": 5242880  # 5 MB
        },
        "producer": {
            "batchingEnabled": True,
            "batchingMaxMessages": 1000,
            "batchingMaxPublishDelayMicros": 10000,  # 10ms
            "compressionType": "LZ4",
            "maxPendingMessages": 10000,
            "blockIfQueueFull": True,
            "sendTimeoutMs": 30000
        },
        "consumer": {
            "receiverQueueSize": 10000,
            "maxTotalReceiverQueueSizeAcrossPartitions": 500000,
            "acknowledgmentGroupTime": 100,  # ms
            "negativeAckRedeliveryDelay": 60000  # 1 min
        }
    },
    "resource_allocation": {
        "broker_pods": {
            "replicas": 9,  # 3 per AZ
            "resources": {
                "requests": {
                    "cpu": "8",
                    "memory": "32Gi"
                },
                "limits": {
                    "cpu": "16",
                    "memory": "64Gi"
                }
            },
            "jvm_options": "-Xms32g -Xmx32g -XX:MaxDirectMemorySize=32g -XX:+UseG1GC -XX:MaxGCPauseMillis=10"
        },
        "bookkeeper_pods": {
            "replicas": 9,  # 3 per AZ
            "resources": {
                "requests": {
                    "cpu": "8",
                    "memory": "16Gi"
                },
                "limits": {
                    "cpu": "16",
                    "memory": "32Gi"
                }
            },
            "storage": {
                "journal": {
                    "size": "100Gi",
                    "storageClass": "fast-ssd",
                    "iops": 10000
                },
                "ledgers": {
                    "size": "2Ti",
                    "storageClass": "standard-ssd",
                    "iops": 5000
                }
            },
            "jvm_options": "-Xms16g -Xmx16g -XX:MaxDirectMemorySize=16g -XX:+UseG1GC"
        },
        "proxy_pods": {
            "replicas": 6,
            "resources": {
                "requests": {
                    "cpu": "4",
                    "memory": "8Gi"
                },
                "limits": {
                    "cpu": "8",
                    "memory": "16Gi"
                }
            }
        }
    },
    "benchmark_scenarios": [
        {
            "name": "sustained_4m_msg_sec",
            "producers": 100,
            "topics": 1000,
            "partitions_per_topic": 10,
            "message_size": 1024,
            "rate_per_producer": 40000,
            "duration_minutes": 60,
            "expected_p99_latency_ms": 50
        },
        {
            "name": "burst_8m_msg_sec",
            "producers": 200,
            "topics": 1000,
            "partitions_per_topic": 10,
            "message_size": 1024,
            "rate_per_producer": 40000,
            "duration_minutes": 5,
            "expected_p99_latency_ms": 100
        }
    ]
}

# ===== GEO-REPLICATION SETUP =====

geo_replication_config = {
    "enabled": True,
    "clusters": [
        {
            "name": "us-east",
            "serviceUrl": "pulsar://pulsar-broker.us-east.svc.cluster.local:6650",
            "brokerServiceUrl": "pulsar://pulsar-broker.us-east.svc.cluster.local:6650",
            "authPlugin": "org.apache.pulsar.client.impl.auth.AuthenticationToken"
        },
        {
            "name": "us-west",
            "serviceUrl": "pulsar://pulsar-broker.us-west.svc.cluster.local:6650",
            "brokerServiceUrl": "pulsar://pulsar-broker.us-west.svc.cluster.local:6650",
            "authPlugin": "org.apache.pulsar.client.impl.auth.AuthenticationToken"
        },
        {
            "name": "eu-central",
            "serviceUrl": "pulsar://pulsar-broker.eu-central.svc.cluster.local:6650",
            "brokerServiceUrl": "pulsar://pulsar-broker.eu-central.svc.cluster.local:6650",
            "authPlugin": "org.apache.pulsar.client.impl.auth.AuthenticationToken"
        }
    ],
    "replication_policy": {
        "replication_clusters": ["us-east", "us-west", "eu-central"],
        "message_ttl_seconds": 259200,  # 3 days
        "replication_rate_limit_bytes_per_second": 104857600,  # 100 MB/s per namespace
        "dispatch_rate": {
            "dispatchThrottlingRatePerReplicatorInMsg": 10000,
            "dispatchThrottlingRatePerReplicatorInByte": 10485760  # 10 MB/s
        }
    },
    "namespace_replication_setup": {
        "tenant/namespace": {
            "replication_clusters": ["us-east", "us-west"],
            "messageTTL": 259200,
            "retentionTime": 7,
            "retentionSize": 1099511627776  # 1 TB
        }
    }
}

# ===== TIERED STORAGE OFFLOAD POLICIES =====

tiered_storage_config = {
    "enabled": True,
    "driver": "aws-s3",
    "offload_policies": {
        "hot_tier": {
            "description": "Recent data kept in BookKeeper",
            "retention_time_minutes": 10080,  # 7 days
            "retention_size_mb": 1048576  # 1 TB
        },
        "warm_tier": {
            "description": "Offloaded to S3 Standard",
            "offload_threshold_bytes": 1099511627776,  # 1 TB
            "offload_deletion_lag_ms": 14400000,  # 4 hours
            "s3_bucket": "pulsar-offload-warm",
            "s3_region": "us-east-1",
            "s3_storage_class": "STANDARD"
        },
        "cold_tier": {
            "description": "Long-term archive in S3 Glacier",
            "offload_after_days": 90,
            "s3_bucket": "pulsar-offload-cold",
            "s3_region": "us-east-1",
            "s3_storage_class": "GLACIER_INSTANT_RETRIEVAL",
            "lifecycle_policy": {
                "transition_to_glacier_deep_archive_days": 365
            }
        }
    },
    "managed_ledger_offload_config": {
        "managedLedgerOffloadDriver": "aws-s3",
        "managedLedgerOffloadBucket": "pulsar-offload-warm",
        "managedLedgerOffloadRegion": "us-east-1",
        "managedLedgerOffloadServiceEndpoint": "https://s3.us-east-1.amazonaws.com",
        "managedLedgerOffloadMaxBlockSizeInBytes": 67108864,  # 64 MB
        "managedLedgerOffloadPrefetchRounds": 1,
        "managedLedgerOffloadThresholdInBytes": 1099511627776,  # 1 TB
        "managedLedgerOffloadDeletionLagInMillis": 14400000,  # 4 hours
        "managedLedgerOffloadAutoTriggerSizeThresholdBytes": 1099511627776,
        "s3ManagedLedgerOffloadCredentialId": "AWS_ACCESS_KEY_ID",
        "s3ManagedLedgerOffloadCredentialSecret": "AWS_SECRET_ACCESS_KEY",
        "s3ManagedLedgerOffloadRole": "arn:aws:iam::ACCOUNT:role/PulsarS3OffloadRole",
        "s3ManagedLedgerOffloadRoleSessionName": "pulsar-s3-offload"
    },
    "namespace_offload_policies": {
        "shipping-co-alpha/vessel-tracking": {
            "offloadThresholdInBytes": 1099511627776,
            "offloadDeletionLagInMillis": 14400000
        },
        "logistics-beta/analytics": {
            "offloadThresholdInBytes": 2199023255552,  # 2 TB
            "offloadDeletionLagInMillis": 28800000  # 8 hours
        }
    }
}

# Save all HA configurations
ha_dir = Path(pulsar_dir) / "ha-config"
ha_dir.mkdir(parents=True, exist_ok=True)

# Save BookKeeper placement policy
placement_policy_path = ha_dir / "bookkeeper_placement_policy.yaml"
with open(placement_policy_path, 'w') as f:
    yaml.dump(bookkeeper_placement_policy, f, default_flow_style=False, sort_keys=False)

# Save BookKeeper HA config
bookkeeper_ha_path = ha_dir / "bookkeeper_ha_config.yaml"
with open(bookkeeper_ha_path, 'w') as f:
    yaml.dump(bookkeeper_ha_config, f, default_flow_style=False, sort_keys=False)

# Save broker load balancing
broker_lb_path = ha_dir / "broker_load_balancing.yaml"
with open(broker_lb_path, 'w') as f:
    yaml.dump(broker_load_balancing, f, default_flow_style=False, sort_keys=False)

# Save automated recovery
recovery_path = ha_dir / "automated_recovery.yaml"
with open(recovery_path, 'w') as f:
    yaml.dump(automated_recovery, f, default_flow_style=False, sort_keys=False)

# Save rolling upgrade procedures
upgrade_path = ha_dir / "rolling_upgrade_procedures.yaml"
with open(upgrade_path, 'w') as f:
    yaml.dump(rolling_upgrade_config, f, default_flow_style=False, sort_keys=False)

# Save benchmark configurations
benchmark_dir = Path(pulsar_dir) / "benchmark"
benchmark_dir.mkdir(parents=True, exist_ok=True)

benchmark_path = benchmark_dir / "performance_benchmark_config.yaml"
with open(benchmark_path, 'w') as f:
    yaml.dump(benchmark_config, f, default_flow_style=False, sort_keys=False)

# Save geo-replication config
geo_rep_path = ha_dir / "geo_replication_config.yaml"
with open(geo_rep_path, 'w') as f:
    yaml.dump(geo_replication_config, f, default_flow_style=False, sort_keys=False)

# Save tiered storage config
tiered_storage_path = ha_dir / "tiered_storage_offload.yaml"
with open(tiered_storage_path, 'w') as f:
    yaml.dump(tiered_storage_config, f, default_flow_style=False, sort_keys=False)

pulsar_ha_summary = {
    "high_availability": {
        "multi_az_placement": str(placement_policy_path),
        "bookkeeper_ensemble_config": str(bookkeeper_ha_path),
        "broker_load_balancing": str(broker_lb_path),
        "automated_recovery": str(recovery_path),
        "rolling_upgrades": str(upgrade_path),
        "features": [
            "Multi-AZ rack-aware ensemble placement (3 AZs, ensemble=5, ack=3)",
            "Automated bookie recovery with replication rate limiting",
            "Broker load balancing with failure domain awareness",
            "Graceful rolling upgrades with zero downtime",
            "Automatic topic and broker failover"
        ]
    },
    "scale_specifications": {
        "throughput_targets": {
            "sustained": "4M msg/sec (4 GB/s)",
            "burst": "8M msg/sec (8 GB/s)"
        },
        "benchmark_config": str(benchmark_path),
        "resource_allocation": {
            "brokers": "9 pods, 8-16 CPU, 32-64 GB RAM each",
            "bookies": "9 pods, 8-16 CPU, 16-32 GB RAM, 100GB journal + 2TB ledger storage",
            "proxies": "6 pods, 4-8 CPU, 8-16 GB RAM"
        },
        "performance_tuning": {
            "broker_threads": "16 IO + 32 worker threads",
            "cache_size": "8 GB managed ledger cache",
            "batching": "1000 messages per batch, 10ms delay",
            "compression": "LZ4"
        }
    },
    "geo_replication": {
        "enabled": True,
        "clusters": ["us-east", "us-west", "eu-central"],
        "config_file": str(geo_rep_path),
        "features": [
            "Active-active replication across 3 regions",
            "Per-namespace replication policies",
            "Rate limiting: 100 MB/s per namespace",
            "3-day message TTL for replicated data"
        ]
    },
    "tiered_storage": {
        "enabled": True,
        "config_file": str(tiered_storage_path),
        "tiers": {
            "hot": "7 days in BookKeeper (up to 1 TB)",
            "warm": "Offload to S3 Standard after 1 TB",
            "cold": "Glacier Instant Retrieval after 90 days"
        },
        "offload_settings": {
            "threshold": "1 TB per namespace",
            "deletion_lag": "4 hours",
            "block_size": "64 MB"
        }
    },
    "files_created": [
        str(placement_policy_path),
        str(bookkeeper_ha_path),
        str(broker_lb_path),
        str(recovery_path),
        str(upgrade_path),
        str(benchmark_path),
        str(geo_rep_path),
        str(tiered_storage_path)
    ]
}

print("✅ Pulsar HA, Scale, and Operational Excellence Configurations Created")
print(f"\n📁 HA Configurations: {ha_dir}")
print(f"   - Multi-AZ BookKeeper placement policy")
print(f"   - Ensemble HA config (E=5, W=5, A=3, 3 AZs)")
print(f"   - Broker load balancing with failure domains")
print(f"   - Automated recovery for topics and brokers")
print(f"   - Rolling upgrade procedures")
print(f"   - Geo-replication setup (3 regions)")
print(f"   - Tiered storage offload policies (Hot/Warm/Cold)")
print(f"\n📁 Benchmark Configurations: {benchmark_dir}")
print(f"   - 4M msg/sec sustained throughput target")
print(f"   - 8M msg/sec burst capacity")
print(f"   - Resource allocation: 9 brokers + 9 bookies")
print(f"   - Performance tuning parameters")
print(f"\n🎯 Key HA Features:")
print(f"   - Multi-AZ deployment across 3 availability zones")
print(f"   - Rack-aware ensemble placement")
print(f"   - Automated failover and recovery")
print(f"   - Zero-downtime rolling upgrades")
print(f"   - Geo-replication for disaster recovery")
print(f"   - Tiered storage for cost optimization")

pulsar_ha_summary
