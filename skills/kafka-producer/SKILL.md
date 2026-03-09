---
name: kafka-producer
description: Guidelines for creating Kafka producer applications for Confluent Cloud with Schema Registry integration. Trigger this skill when the user wants to create, build, or set up a Kafka producer, Kafka client, streaming application, data pipeline, or any application that sends data to Kafka/Confluent Cloud. Use this when you see keywords like "kafka producer", "confluent producer", "kafka client", "send to kafka", "publish to kafka", "streaming app", "real-time pipeline", "produce messages", "kafka application", or when the user mentions sending/publishing data to topics.
---

# Kafka Producer for Confluent Cloud

This skill helps you create production-ready Kafka producer applications that work with Confluent Cloud and enforce schema usage via Schema Registry. All producers default to Avro serialization unless the user specifies otherwise.

## Workflow

### Step 1: Gather Requirements

Ask the user these questions if not already clear from context:

1. **Programming language** - Which language should the producer be written in?
   - Python
   - Java
   - JavaScript/Node.js
   - .NET/C#
   - Go
   - C++

2. **Optimization profile** - What's the primary goal for this producer?
   - **Throughput**: Maximize data volume (batch processing, ETL pipelines)
   - **Latency**: Minimize message delivery time (real-time alerts, user-facing features)
   - **Durability**: Guarantee no message loss (financial transactions, audit logs)
   - **Availability**: Maximize uptime tolerance (resilient systems)
   - **Freight**: Cost-optimized high-volume streaming (Confluent Freight clusters)
   - **Balanced**: General-purpose with reasonable defaults

3. **Additional context**:
   - Topic name(s) to produce to
   - Schema definition (if they have one) or data structure
   - Any specific requirements (ordering guarantees, transactional semantics, etc.)

### Step 2: Set Up Environment Configuration

Provide a `.env` file template for storing credentials securely:

```bash
# Kafka Cluster Configuration
BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
SASL_USERNAME=<CLUSTER_API_KEY>
SASL_PASSWORD=<CLUSTER_API_SECRET>

# Schema Registry Configuration
SCHEMA_REGISTRY_URL=https://psrc-xxxxx.region.provider.confluent.cloud
SCHEMA_REGISTRY_API_KEY=<SR_API_KEY>
SCHEMA_REGISTRY_API_SECRET=<SR_API_SECRET>

# Application Configuration (optional)
TOPIC_NAME=your-topic-name
CLIENT_ID=your-app-name
```

Explain to the user:
- These credentials come from Confluent Cloud console
- The `.env` file should NEVER be committed to version control (add to `.gitignore`)
- Use a library to load environment variables (python: `python-dotenv`, Node.js: `dotenv`, Java: consider system properties or config libraries)

### Step 3: Install Dependencies

Based on the chosen language, read the appropriate reference file and provide:
- Installation commands (pip, maven, npm, go get, etc.)
- Configuration file examples (requirements.txt, pom.xml, package.json, go.mod, etc.)
- Required libraries for Kafka client, Schema Registry, and Avro serialization

See language-specific references:
- `references/python-producer.md` for Python
- `references/java-producer.md` for Java
- `references/javascript-producer.md` for JavaScript/Node.js
- `references/dotnet-producer.md` for .NET/C#
- `references/go-producer.md` for Go

### Step 4: Create Schema

If the user doesn't have a schema yet, help them create an Avro schema based on their data structure.

**Example Avro Schema:**
```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "created_at", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

Save this as `schemas/user.avsc` or similar.

For schema evolution and compatibility modes, consult `references/schema-registry.md`.

### Step 5: Apply Optimization Profile

Based on the user's chosen optimization profile, read `references/optimization-profiles.md` and apply the recommended producer configurations.

Each profile involves trade-offs:
- **Throughput**: Larger batches, compression, tolerates higher latency
- **Latency**: Immediate sends, minimal batching, no compression
- **Durability**: `acks=all`, idempotence, retries
- **Availability**: Replica awareness, timeout configuration
- **Freight**: Large batches, disabled idempotence, zone-aware partitioning
- **Balanced**: Moderate settings across all dimensions

### Step 6: Generate Producer Code

Create a complete, production-ready producer that includes:

1. **Configuration loading** from .env file
2. **Producer initialization** with optimization profile settings
3. **Schema Registry integration** with Avro serializer
4. **Error handling**:
   - Delivery callbacks to track success/failure
   - Retry logic for transient errors
   - Logging for debugging
5. **Graceful shutdown**:
   - Flush pending messages before exit
   - Close producer properly
6. **Example usage** showing how to send messages

Use the language-specific patterns from the reference files and the optimization configurations from `references/optimization-profiles.md`.

### Step 7: Add Supporting Files

Provide additional files as needed:
- **README.md**: Setup instructions, how to run, configuration options
- **.gitignore**: Ensure `.env` is excluded
- **Sample data/test script**: Help the user test the producer
- **Error handling guide**: Common errors and how to resolve them

## Key Principles

**Always enforce schema usage**: Every producer should use Schema Registry with Avro serialization unless explicitly requested otherwise. This ensures data quality and enables schema evolution.

**Explain trade-offs**: When discussing optimization profiles, help the user understand what they're gaining and losing with each configuration choice. There's no one-size-fits-all solution.

**Production-ready defaults**: Include proper error handling, logging, graceful shutdown, and monitoring considerations from the start. Don't create toy examples.

**Security first**: Credentials must come from environment variables or secure configuration files, never hardcoded.

**Schema evolution awareness**: When helping with schemas, mention compatibility modes (BACKWARD, FORWARD, FULL) and how they affect the ability to evolve schemas over time. See `references/schema-registry.md` for details.

## Testing Guidance

After creating the producer, guide the user to:
1. Test with a single message first
2. Verify the message appears in Confluent Cloud console
3. Check Schema Registry to confirm schema was registered
4. Test error scenarios (invalid data, network issues)
5. Load test if throughput/latency requirements are critical

Remind them that Confluent Cloud provides metrics and monitoring - they should set up alerts for producer errors, lag, and throughput.
