# Java Kafka Producer Guide

Complete guide for creating Kafka producers in Java using the Confluent Kafka client with Schema Registry and Avro serialization.

## Dependencies

### Maven (pom.xml)

```xml
<project>
    <properties>
        <java.version>11</java.version>
        <kafka.version>7.6.0-ccs</kafka.version>
        <confluent.version>7.6.0</confluent.version>
    </properties>

    <repositories>
        <repository>
            <id>confluent</id>
            <url>https://packages.confluent.io/maven/</url>
        </repository>
    </repositories>

    <dependencies>
        <!-- Kafka Clients -->
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-clients</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- Schema Registry and Avro -->
        <dependency>
            <groupId>io.confluent</groupId>
            <artifactId>kafka-avro-serializer</artifactId>
            <version>${confluent.version}</version>
        </dependency>

        <!-- Environment variable loading -->
        <dependency>
            <groupId>io.github.cdimascio</groupId>
            <artifactId>dotenv-java</artifactId>
            <version>3.0.0</version>
        </dependency>

        <!-- Logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>2.0.9</version>
        </dependency>
    </dependencies>
</project>
```

### Gradle (build.gradle)

```gradle
plugins {
    id 'java'
    id 'application'
}

repositories {
    mavenCentral()
    maven {
        url "https://packages.confluent.io/maven/"
    }
}

dependencies {
    implementation 'org.apache.kafka:kafka-clients:7.6.0-ccs'
    implementation 'io.confluent:kafka-avro-serializer:7.6.0'
    implementation 'io.github.cdimascio:dotenv-java:3.0.0'
    implementation 'org.slf4j:slf4j-simple:2.0.9'
}

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
}
```

---

## Avro Schema

Create `src/main/avro/User.avsc`:

```json
{
  "type": "record",
  "name": "User",
  "namespace": "com.example.models",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "createdAt", "type": "long", "logicalType": "timestamp-millis"}
  ]
}
```

### Generate Java Classes from Avro

Add Avro Maven plugin:

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.avro</groupId>
            <artifactId>avro-maven-plugin</artifactId>
            <version>1.11.3</version>
            <executions>
                <execution>
                    <phase>generate-sources</phase>
                    <goals>
                        <goal>schema</goal>
                    </goals>
                    <configuration>
                        <sourceDirectory>${project.basedir}/src/main/avro/</sourceDirectory>
                        <outputDirectory>${project.basedir}/src/main/java/</outputDirectory>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

Run: `mvn generate-sources` to generate `com.example.models.User.java`

---

## Complete Producer Implementation

### Configuration Loader

```java
package com.example.kafka;

import io.github.cdimascio.dotenv.Dotenv;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

public class ProducerConfig {

    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    public static Properties getProducerProperties(OptimizationProfile profile) {
        Properties props = new Properties();

        // Connection settings
        props.put("bootstrap.servers", getEnv("BOOTSTRAP_SERVERS"));
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"" + getEnv("SASL_USERNAME") + "\" " +
            "password=\"" + getEnv("SASL_PASSWORD") + "\";");

        // Client identification
        try {
            props.put("client.id", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            props.put("client.id", getEnv("CLIENT_ID", "java-producer"));
        }

        // Serialization
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");

        // Schema Registry
        props.put("schema.registry.url", getEnv("SCHEMA_REGISTRY_URL"));
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info",
            getEnv("SCHEMA_REGISTRY_API_KEY") + ":" + getEnv("SCHEMA_REGISTRY_API_SECRET"));

        // Apply optimization profile
        applyOptimizationProfile(props, profile);

        return props;
    }

    private static void applyOptimizationProfile(Properties props, OptimizationProfile profile) {
        switch (profile) {
            case THROUGHPUT:
                props.put("batch.size", 65536);
                props.put("linger.ms", 100);
                props.put("compression.type", "lz4");
                props.put("buffer.memory", 67108864);
                props.put("acks", "1");
                break;

            case LATENCY:
                props.put("linger.ms", 0);
                props.put("acks", "1");
                props.put("compression.type", "none");
                props.put("batch.size", 16384);
                break;

            case DURABILITY:
                props.put("acks", "all");
                props.put("enable.idempotence", true);
                props.put("retries", Integer.MAX_VALUE);
                props.put("max.in.flight.requests.per.connection", 5);
                props.put("delivery.timeout.ms", 120000);
                break;

            case FREIGHT:
                props.put("enable.idempotence", false);
                props.put("linger.ms", 100);
                props.put("batch.size", 1048576);
                props.put("buffer.memory", 524288000);
                props.put("partitioner.adaptive.partitioning.enable", false);
                props.put("metadata.max.age.ms", 60000);
                break;

            case BALANCED:
            default:
                props.put("acks", "all");
                props.put("enable.idempotence", true);
                props.put("batch.size", 16384);
                props.put("linger.ms", 10);
                props.put("compression.type", "lz4");
                props.put("buffer.memory", 33554432);
                break;
        }
    }

    private static String getEnv(String key) {
        String value = dotenv.get(key);
        if (value == null) {
            throw new IllegalStateException("Missing required environment variable: " + key);
        }
        return value;
    }

    private static String getEnv(String key, String defaultValue) {
        String value = dotenv.get(key);
        return value != null ? value : defaultValue;
    }

    public enum OptimizationProfile {
        THROUGHPUT,
        LATENCY,
        DURABILITY,
        AVAILABILITY,
        FREIGHT,
        BALANCED
    }
}
```

### Producer Implementation with Error Handling

```java
package com.example.kafka;

import com.example.models.User;
import io.github.cdimascio.dotenv.Dotenv;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class UserProducer implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(UserProducer.class);
    private static final Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

    private final KafkaProducer<String, User> producer;
    private final String topicName;
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong failureCount = new AtomicLong(0);

    public UserProducer(ProducerConfig.OptimizationProfile profile) {
        this.producer = new KafkaProducer<>(ProducerConfig.getProducerProperties(profile));
        this.topicName = dotenv.get("TOPIC_NAME", "users");
        logger.info("UserProducer initialized with profile: {}", profile);
    }

    /**
     * Produce a user message asynchronously with callback.
     */
    public void produceAsync(User user) {
        ProducerRecord<String, User> record = new ProducerRecord<>(
            topicName,
            user.getId().toString(),
            user
        );

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    failureCount.incrementAndGet();
                    logger.error("Failed to produce message for user {}: {}",
                        user.getId(), exception.getMessage(), exception);
                    // Handle error:
                    // - Store in dead letter queue
                    // - Trigger alert
                    // - Log to monitoring system
                } else {
                    successCount.incrementAndGet();
                    logger.info("Message delivered: topic={}, partition={}, offset={}",
                        metadata.topic(), metadata.partition(), metadata.offset());
                }
            }
        });
    }

    /**
     * Produce a user message synchronously.
     * Blocks until acknowledgment is received.
     */
    public RecordMetadata produceSync(User user) throws ExecutionException, InterruptedException {
        ProducerRecord<String, User> record = new ProducerRecord<>(
            topicName,
            user.getId().toString(),
            user
        );

        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(); // Block until complete
            successCount.incrementAndGet();
            logger.info("Message delivered synchronously: topic={}, partition={}, offset={}",
                metadata.topic(), metadata.partition(), metadata.offset());
            return metadata;
        } catch (ExecutionException | InterruptedException e) {
            failureCount.incrementAndGet();
            logger.error("Failed to produce message synchronously for user {}: {}",
                user.getId(), e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Flush all pending messages.
     * Blocks until all messages are sent or timeout is reached.
     */
    public void flush() {
        logger.info("Flushing pending messages...");
        producer.flush();
        logger.info("Flush complete. Success: {}, Failures: {}",
            successCount.get(), failureCount.get());
    }

    /**
     * Get producer metrics.
     */
    public ProducerMetrics getMetrics() {
        return new ProducerMetrics(successCount.get(), failureCount.get());
    }

    @Override
    public void close() {
        logger.info("Closing producer...");
        flush();
        producer.close(30, TimeUnit.SECONDS);
        logger.info("Producer closed. Final stats - Success: {}, Failures: {}",
            successCount.get(), failureCount.get());
    }

    public static class ProducerMetrics {
        private final long successCount;
        private final long failureCount;

        public ProducerMetrics(long successCount, long failureCount) {
            this.successCount = successCount;
            this.failureCount = failureCount;
        }

        public long getSuccessCount() { return successCount; }
        public long getFailureCount() { return failureCount; }

        @Override
        public String toString() {
            return String.format("ProducerMetrics{success=%d, failures=%d}",
                successCount, failureCount);
        }
    }
}
```

### Example Application

```java
package com.example;

import com.example.kafka.ProducerConfig;
import com.example.kafka.UserProducer;
import com.example.models.User;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        // Use try-with-resources for automatic cleanup
        try (UserProducer producer = new UserProducer(ProducerConfig.OptimizationProfile.DURABILITY)) {

            // Create sample users
            User user1 = User.newBuilder()
                .setId("user-001")
                .setEmail("alice@example.com")
                .setCreatedAt(System.currentTimeMillis())
                .build();

            User user2 = User.newBuilder()
                .setId("user-002")
                .setEmail("bob@example.com")
                .setCreatedAt(System.currentTimeMillis())
                .build();

            // Produce asynchronously
            logger.info("Producing messages...");
            producer.produceAsync(user1);
            producer.produceAsync(user2);

            // Optional: produce synchronously for critical messages
            // RecordMetadata metadata = producer.produceSync(user1);

            // Flush to ensure all messages are sent
            producer.flush();

            // Print metrics
            logger.info("Final metrics: {}", producer.getMetrics());

        } catch (Exception e) {
            logger.error("Error in application: {}", e.getMessage(), e);
            System.exit(1);
        }
    }
}
```

---

## Advanced Patterns

### Retry with Exponential Backoff

```java
public class RetryableProducer extends UserProducer {

    private static final int MAX_RETRIES = 3;
    private static final long INITIAL_BACKOFF_MS = 1000;

    public RetryableProducer(ProducerConfig.OptimizationProfile profile) {
        super(profile);
    }

    public boolean produceWithRetry(User user) {
        int attempt = 0;
        while (attempt < MAX_RETRIES) {
            try {
                produceSync(user);
                return true;
            } catch (Exception e) {
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    logger.error("All {} retry attempts failed for user {}", MAX_RETRIES, user.getId());
                    return false;
                }

                long backoff = INITIAL_BACKOFF_MS * (long) Math.pow(2, attempt - 1);
                logger.warn("Attempt {} failed, retrying in {}ms: {}", attempt, backoff, e.getMessage());

                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }
}
```

### Batch Processing

```java
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class BatchProducer extends UserProducer {

    public BatchProducer(ProducerConfig.OptimizationProfile profile) {
        super(profile);
    }

    /**
     * Produce multiple users in batch.
     */
    public CompletableFuture<Void> produceBatch(List<User> users) {
        List<CompletableFuture<RecordMetadata>> futures = users.stream()
            .map(user -> CompletableFuture.supplyAsync(() -> {
                try {
                    return produceSync(user);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to produce user " + user.getId(), e);
                }
            }))
            .collect(Collectors.toList());

        return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
}
```

---

## Error Handling

### Custom Exception Handling

```java
public class ProducerException extends Exception {
    private final String userId;
    private final String topic;

    public ProducerException(String message, String userId, String topic, Throwable cause) {
        super(message, cause);
        this.userId = userId;
        this.topic = topic;
    }

    public String getUserId() { return userId; }
    public String getTopic() { return topic; }
}

// Usage in producer
public void produceWithExceptionHandling(User user) throws ProducerException {
    try {
        produceSync(user);
    } catch (Exception e) {
        throw new ProducerException(
            "Failed to produce user message",
            user.getId().toString(),
            topicName,
            e
        );
    }
}
```

---

## Testing

### Unit Test Example

```java
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.*;

class UserProducerTest {

    private UserProducer producer;

    @BeforeEach
    void setUp() {
        producer = new UserProducer(ProducerConfig.OptimizationProfile.BALANCED);
    }

    @AfterEach
    void tearDown() {
        producer.close();
    }

    @Test
    void testProduceUser() {
        User testUser = User.newBuilder()
            .setId("test-001")
            .setEmail("test@example.com")
            .setCreatedAt(System.currentTimeMillis())
            .build();

        assertDoesNotThrow(() -> {
            RecordMetadata metadata = producer.produceSync(testUser);
            assertNotNull(metadata);
            assertEquals("users", metadata.topic());
        });
    }

    @Test
    void testMetrics() {
        User testUser = User.newBuilder()
            .setId("test-002")
            .setEmail("metrics@example.com")
            .setCreatedAt(System.currentTimeMillis())
            .build();

        producer.produceAsync(testUser);
        producer.flush();

        ProducerMetrics metrics = producer.getMetrics();
        assertTrue(metrics.getSuccessCount() > 0);
    }
}
```

---

## Best Practices

1. **Use try-with-resources**: Ensures proper cleanup via `AutoCloseable`
2. **Always flush before shutdown**: Prevents message loss
3. **Handle callbacks properly**: Monitor success and failure cases
4. **Use appropriate profiles**: Choose optimization profile based on use case
5. **Log comprehensively**: Include context (user ID, topic, partition) in logs
6. **Monitor metrics**: Track success/failure rates via JMX or custom metrics
7. **Handle retries carefully**: Implement exponential backoff for transient errors
8. **Use connection pooling**: Reuse producer instances (they're thread-safe)
9. **Configure timeouts**: Balance between retry duration and responsiveness
10. **Secure credentials**: Never hardcode, always use environment variables or secure vaults

---

## Common Errors

### "Broker: Topic not found"
- **Solution**: Create topic in Confluent Cloud first, or enable auto.create.topics

### "Authentication failed"
- **Solution**: Verify SASL credentials in .env file

### "Schema being registered is incompatible"
- **Solution**: Check Schema Registry compatibility mode, adjust schema

### "TimeoutException"
- **Solution**: Increase `delivery.timeout.ms` or check network connectivity

### "BufferExhaustedException"
- **Solution**: Increase `buffer.memory` or call `flush()` more frequently
