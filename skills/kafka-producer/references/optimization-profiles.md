# Kafka Producer Optimization Profiles

This document contains specific producer configurations for different optimization goals when working with Confluent Cloud.

## Overview

Confluent Cloud emphasizes that "there is no one-size-fits-all recommendation" for producer configuration. The right settings depend on your use case, enabled features, and data characteristics.

**Service Goals Framework**: Choose your primary objective, understanding that these involve trade-offs:
- **Throughput**: Data movement rate
- **Latency**: Message transit time
- **Durability**: Message persistence guarantees
- **Availability**: System reliability
- **Freight**: Cost-optimized high-volume streaming

You cannot maximize all simultaneously - prioritize what matters most for your use case.

---

## Throughput Optimization

**Use case**: Batch processing, ETL pipelines, high-volume data ingestion where some latency is acceptable.

### Key Configurations

```properties
# Batching - allow larger batches for efficiency
batch.size=65536                # 64 KB (increase based on message size)
linger.ms=100                   # Wait up to 100ms to fill batches
buffer.memory=67108864          # 64 MB buffer for unsent messages

# Compression - reduce network usage
compression.type=lz4            # Fast compression (alternatives: snappy, zstd)

# Acknowledgments - reduced durability for speed
acks=1                          # Only leader acknowledges (faster than acks=all)

# Partitioning
partitioner.class=org.apache.kafka.clients.producer.internals.DefaultPartitioner
```

### Trade-offs
- **Gain**: Higher throughput, lower network costs, better batch efficiency
- **Lose**: Increased latency (messages wait in buffers), slightly lower durability (acks=1)

### When to Use
- Logs aggregation
- Clickstream analytics
- Batch ETL processes
- Any scenario where processing in larger chunks is acceptable

---

## Latency Optimization

**Use case**: Real-time alerts, user-facing features, trading systems where every millisecond matters.

### Key Configurations

```properties
# Immediate sends - no waiting for batches
linger.ms=0                     # Send immediately when data is available

# Acknowledgments - faster response
acks=1                          # Leader-only acknowledgment (acks=0 for absolute minimum latency)

# No compression - save CPU cycles
compression.type=none           # Skip compression overhead

# Batching - still happens automatically but smaller
batch.size=16384                # Smaller batches (16 KB)
```

### Trade-offs
- **Gain**: Minimal message delivery time, immediate sends
- **Lose**: Lower throughput, more network requests, higher broker CPU usage

### When to Use
- Real-time notifications
- Fraud detection alerts
- Trading applications
- Live dashboards
- Interactive user experiences

### Note
Confluent Cloud clusters are already configured for low latency by default. Most producer defaults favor responsiveness over throughput.

---

## Durability Optimization

**Use case**: Financial transactions, audit logs, compliance data, any scenario where message loss is unacceptable.

### Key Configurations

```properties
# Maximum durability guarantees
acks=all                        # Wait for all in-sync replicas (same as acks=-1)
enable.idempotence=true         # Prevent duplicates, guarantee ordering
min.insync.replicas=2           # Require at least 2 replicas (set on broker/topic)

# Retries and timeouts
retries=2147483647              # MAX_INT - retry indefinitely
delivery.timeout.ms=120000      # 2 minutes total timeout
request.timeout.ms=30000        # 30 seconds per request

# Ordering guarantees
max.in.flight.requests.per.connection=5  # With idempotence, maintains order
```

### Trade-offs
- **Gain**: Zero message loss, exactly-once semantics, guaranteed ordering
- **Lose**: Higher latency, lower throughput, increased broker load

### When to Use
- Payment processing
- Financial transactions
- Audit trails
- Compliance-critical data
- Healthcare records
- Legal documents

### Important Notes
- `enable.idempotence=true` automatically sets safe defaults for retries and in-flight requests
- Use with `acks=all` and `min.insync.replicas=2` for maximum durability
- On topic level, set `replication.factor=3` with `min.insync.replicas=2`

---

## Availability Optimization

**Use case**: Systems that must remain operational even during partial failures, resilient architectures.

### Key Configurations

```properties
# Acknowledgment strategy
acks=all                        # Ensure replication before success

# Replica requirements - balanced for availability
min.insync.replicas=1           # Lower threshold tolerates more failures

# Timeouts - allow time for recovery
request.timeout.ms=30000        # 30 seconds
delivery.timeout.ms=120000      # 2 minutes

# Retries - handle transient failures
retries=2147483647              # Retry indefinitely
retry.backoff.ms=100            # Wait between retries
```

### Trade-offs
- **Gain**: System continues operating during replica failures
- **Lose**: Lower durability guarantees (with min.insync.replicas=1)

### When to Use
- High-availability services
- Systems with strict uptime SLAs
- Distributed systems across multiple zones
- Applications that prefer stale data over no data

### Balancing Act
Availability and durability often conflict. With `min.insync.replicas=1`, you tolerate more failures but risk data loss. Adjust based on your specific requirements.

---

## Freight Optimization

**Use case**: Confluent Freight clusters - cost-optimized, high-volume streaming where cost efficiency is paramount.

### Key Configurations

```properties
# REQUIRED: Disable idempotence for Freight clusters
enable.idempotence=false        # Mandatory for Freight

# Batching - maximize batch sizes
linger.ms=100                   # Allow batches to fill
batch.size=1048576              # 1 MB batches
buffer.memory=524288000         # 500 MB buffer

# Partitioning - disable adaptive for predictability
partitioner.adaptive.partitioning.enable=false

# Metadata
metadata.max.age.ms=60000       # 60 seconds refresh interval
```

### Trade-offs
- **Gain**: Lowest cost per GB, optimized for cross-zone efficiency
- **Lose**: No idempotence guarantees (potential duplicates), batch latency

### When to Use
- Log aggregation at massive scale
- Data lake ingestion
- Backup/archival streams
- Any high-volume, cost-sensitive workload

### Important Notes
- Freight clusters are designed for throughput over guarantees
- `enable.idempotence=false` is required - this differs from standard Kafka best practices
- Occasional duplicates are acceptable in this model
- Zone alignment and batching reduce cross-zone network costs

---

## Balanced Profile (Default)

**Use case**: General-purpose applications where you want reasonable performance across all dimensions.

### Key Configurations

```properties
# Balanced batching
batch.size=16384                # 16 KB
linger.ms=10                    # 10ms wait time
buffer.memory=33554432          # 32 MB

# Standard compression
compression.type=lz4            # Good balance of speed and compression ratio

# Durability with reasonable performance
acks=all                        # Full durability
enable.idempotence=true         # Prevent duplicates

# Standard timeouts
delivery.timeout.ms=120000      # 2 minutes
request.timeout.ms=30000        # 30 seconds

# Retries
retries=2147483647              # Retry until delivery timeout
```

### When to Use
- Most applications without extreme requirements
- Prototyping and development
- When you're unsure which profile to choose
- Applications that need "good enough" across all dimensions

---

## Additional Universal Configurations

These apply to all profiles and should be set appropriately:

```properties
# Client identification
client.id=<your-app-name>       # Helps with debugging and monitoring

# Security (Confluent Cloud)
security.protocol=SASL_SSL      # Required for Confluent Cloud
sasl.mechanism=PLAIN            # Authentication mechanism
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<API_KEY>" password="<API_SECRET>";

# Serialization (when using Schema Registry)
key.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer
schema.registry.url=<SCHEMA_REGISTRY_URL>
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=<SR_API_KEY>:<SR_API_SECRET>
```

---

## Benchmarking and Tuning

Before finalizing your configuration:

1. **Establish baseline**: Use default settings first, measure performance
2. **Apply profile**: Implement the appropriate optimization profile
3. **Test with realistic data**: Ensure mock data matches production characteristics
4. **Iterate**: Adjust parameters based on observed metrics
5. **Monitor**: Use JMX metrics and Confluent Cloud monitoring

**Key metrics to watch**:
- Producer throughput (records/sec, MB/sec)
- Average latency (end-to-end message time)
- Error rate and retry rate
- Buffer utilization (memory)
- Batch sizes and compression ratios

Remember: "there is no one-size-fits-all" - these profiles are starting points. Fine-tune based on your specific environment, message sizes, partition counts, and business requirements.
