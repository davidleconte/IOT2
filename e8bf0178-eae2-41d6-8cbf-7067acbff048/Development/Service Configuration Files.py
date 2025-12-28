import pandas as pd

# Presto Coordinator Configuration
presto_coordinator_config = """# config.properties
coordinator=true
node-scheduler.include-coordinator=false
http-server.http.port=8080
query.max-memory=2GB
query.max-memory-per-node=1GB
discovery-server.enabled=true
discovery.uri=http://presto-coordinator:8080

# node.properties
node.environment=production
node.id=presto-coordinator-1
node.data-dir=/opt/presto/data

# jvm.config
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError

# log.properties
com.facebook.presto=INFO

# catalog/iceberg.properties
connector.name=iceberg
hive.metastore.uri=thrift://hive-metastore:9083
iceberg.catalog.type=hive
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.ssl.enabled=false
"""

presto_worker_config = """# config.properties
coordinator=false
http-server.http.port=8080
query.max-memory=2GB
query.max-memory-per-node=1GB
discovery.uri=http://presto-coordinator:8080

# node.properties
node.environment=production
node.id=presto-worker-1
node.data-dir=/opt/presto/data

# jvm.config
-server
-Xmx4G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError

# log.properties
com.facebook.presto=INFO

# catalog/iceberg.properties
connector.name=iceberg
hive.metastore.uri=thrift://hive-metastore:9083
iceberg.catalog.type=hive
hive.s3.endpoint=http://minio:9000
hive.s3.path-style-access=true
hive.s3.aws-access-key=minioadmin
hive.s3.aws-secret-key=minioadmin
hive.s3.ssl.enabled=false
"""

# Spark Configuration
spark_defaults = """# spark-defaults.conf
spark.master=spark://spark-master:7077
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=hive
spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083
spark.sql.catalog.iceberg.io-impl=org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.iceberg.warehouse=s3a://telemetry-data/warehouse
spark.hadoop.fs.s3a.endpoint=http://minio:9000
spark.hadoop.fs.s3a.access.key=minioadmin
spark.hadoop.fs.s3a.secret.key=minioadmin
spark.hadoop.fs.s3a.path.style.access=true
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.connection.ssl.enabled=false
spark.eventLog.enabled=true
spark.eventLog.dir=/opt/spark/spark-events
spark.history.fs.logDirectory=/opt/spark/spark-events
"""

# Hive Metastore Configuration
hive_site = """<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:postgresql://postgres-hms:5432/metastore</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>org.postgresql.Driver</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>hive</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>hivepassword</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>s3a://telemetry-data/warehouse</value>
  </property>
  <property>
    <name>fs.s3a.endpoint</name>
    <value>http://minio:9000</value>
  </property>
  <property>
    <name>fs.s3a.access.key</name>
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>minioadmin</value>
  </property>
  <property>
    <name>fs.s3a.path.style.access</name>
    <value>true</value>
  </property>
</configuration>
"""

print("=" * 100)
print("PRESTO COORDINATOR CONFIGURATION")
print("=" * 100)
print(presto_coordinator_config)

print("\n" + "=" * 100)
print("PRESTO WORKER CONFIGURATION")
print("=" * 100)
print(presto_worker_config)

print("\n" + "=" * 100)
print("SPARK DEFAULTS CONFIGURATION")
print("=" * 100)
print(spark_defaults)

print("\n" + "=" * 100)
print("HIVE METASTORE CONFIGURATION (hive-site.xml)")
print("=" * 100)
print(hive_site)

# Configuration files summary
config_files = pd.DataFrame({
    'Component': ['Presto Coordinator', 'Presto Worker', 'Spark', 'Hive Metastore'],
    'Config_Files': [
        'config.properties, node.properties, jvm.config, catalog/iceberg.properties',
        'config.properties, node.properties, jvm.config, catalog/iceberg.properties',
        'spark-defaults.conf',
        'hive-site.xml'
    ],
    'Key_Settings': [
        'Coordinator role, discovery server, Iceberg catalog, S3 connection',
        'Worker role, connects to coordinator, Iceberg catalog, S3 connection',
        'Iceberg extensions, HMS catalog, S3 connection, event logging',
        'PostgreSQL backend, S3 warehouse, MinIO endpoint'
    ],
    'Location': [
        './presto-config/coordinator/',
        './presto-config/worker/',
        'Mount to /opt/spark/conf/',
        'Mount to /opt/hive/conf/'
    ]
})

print("\n" + "=" * 100)
print("CONFIGURATION FILES SUMMARY")
print("=" * 100)
print(config_files.to_string(index=False))
