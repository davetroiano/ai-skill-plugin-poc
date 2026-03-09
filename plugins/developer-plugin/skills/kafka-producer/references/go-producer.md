# Go Kafka Producer Guide

Complete guide for creating Kafka producers in Go using confluent-kafka-go with Schema Registry and Avro serialization.

## Installation

```bash
go get github.com/confluentinc/confluent-kafka-go/v2/kafka
go get github.com/confluentinc/confluent-kafka-go/v2/schemaregistry
go get github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde
go get github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro
go get github.com/joho/godotenv
```

### go.mod

```go
module github.com/example/kafka-producer

go 1.21

require (
    github.com/confluentinc/confluent-kafka-go/v2 v2.3.0
    github.com/joho/godotenv v1.5.1
)
```

---

## Producer Implementation

### Configuration

```go
package main

import (
    "fmt"
    "os"
    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
    "github.com/joho/godotenv"
)

type OptimizationProfile string

const (
    Throughput  OptimizationProfile = "throughput"
    Latency     OptimizationProfile = "latency"
    Durability  OptimizationProfile = "durability"
    Freight     OptimizationProfile = "freight"
    Balanced    OptimizationProfile = "balanced"
)

func GetProducerConfig(profile OptimizationProfile) *kafka.ConfigMap {
    _ = godotenv.Load()

    config := &kafka.ConfigMap{
        "bootstrap.servers": os.Getenv("BOOTSTRAP_SERVERS"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     os.Getenv("SASL_USERNAME"),
        "sasl.password":     os.Getenv("SASL_PASSWORD"),
        "client.id":         getEnv("CLIENT_ID", "go-producer"),
    }

    applyOptimizationProfile(config, profile)

    return config
}

func applyOptimizationProfile(config *kafka.ConfigMap, profile OptimizationProfile) {
    switch profile {
    case Throughput:
        config.SetKey("batch.size", 65536)
        config.SetKey("linger.ms", 100)
        config.SetKey("compression.type", "lz4")
        config.SetKey("acks", 1)

    case Latency:
        config.SetKey("linger.ms", 0)
        config.SetKey("acks", 1)
        config.SetKey("compression.type", "none")
        config.SetKey("batch.size", 16384)

    case Durability:
        config.SetKey("acks", "all")
        config.SetKey("enable.idempotence", true)
        config.SetKey("retries", 2147483647)
        config.SetKey("max.in.flight.requests.per.connection", 5)

    case Freight:
        config.SetKey("enable.idempotence", false)
        config.SetKey("linger.ms", 100)
        config.SetKey("batch.size", 1048576)
        config.SetKey("partitioner.adaptive.partitioning.enable", false)

    case Balanced:
        config.SetKey("acks", "all")
        config.SetKey("enable.idempotence", true)
        config.SetKey("batch.size", 16384)
        config.SetKey("linger.ms", 10)
        config.SetKey("compression.type", "lz4")
    }
}

func GetSchemaRegistryConfig() schemaregistry.Config {
    return schemaregistry.NewConfig(os.Getenv("SCHEMA_REGISTRY_URL"),
        schemaregistry.WithBasicAuth(
            os.Getenv("SCHEMA_REGISTRY_API_KEY"),
            os.Getenv("SCHEMA_REGISTRY_API_SECRET")))
}

func getEnv(key, defaultValue string) string {
    value := os.Getenv(key)
    if value == "" {
        return defaultValue
    }
    return value
}
```

### Avro Schema and User Type

```go
package main

type User struct {
    ID        string `avro:"id"`
    Email     string `avro:"email"`
    CreatedAt int64  `avro:"createdAt"`
}

const userSchemaString = `{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    {"name": "id", "type": "string"},
    {"name": "email", "type": "string"},
    {"name": "createdAt", "type": "long", "logicalType": "timestamp-millis"}
  ]
}`
```

### Producer Implementation

```go
package main

import (
    "context"
    "fmt"
    "sync/atomic"
    "time"

    "github.com/confluentinc/confluent-kafka-go/v2/kafka"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
    "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/avro"
)

type UserProducer struct {
    producer      *kafka.Producer
    serializer    *avro.GenericSerializer
    topicName     string
    successCount  int64
    failureCount  int64
    deliveryChan  chan kafka.Event
}

func NewUserProducer(profile OptimizationProfile) (*UserProducer, error) {
    config := GetProducerConfig(profile)

    producer, err := kafka.NewProducer(config)
    if err != nil {
        return nil, fmt.Errorf("failed to create producer: %w", err)
    }

    srConfig := GetSchemaRegistryConfig()
    srClient, err := schemaregistry.NewClient(srConfig)
    if err != nil {
        return nil, fmt.Errorf("failed to create schema registry client: %w", err)
    }

    serializer, err := avro.NewGenericSerializer(srClient, serde.ValueSerde, avro.NewSerializerConfig())
    if err != nil {
        return nil, fmt.Errorf("failed to create serializer: %w", err)
    }

    topicName := getEnv("TOPIC_NAME", "users")

    up := &UserProducer{
        producer:     producer,
        serializer:   serializer,
        topicName:    topicName,
        deliveryChan: make(chan kafka.Event, 10000),
    }

    // Start delivery report handler
    go up.handleDeliveryReports()

    fmt.Printf("UserProducer initialized with profile: %s\n", profile)

    return up, nil
}

func (up *UserProducer) handleDeliveryReports() {
    for e := range up.deliveryChan {
        switch ev := e.(type) {
        case *kafka.Message:
            if ev.TopicPartition.Error != nil {
                atomic.AddInt64(&up.failureCount, 1)
                fmt.Printf("❌ Delivery failed: %v\n", ev.TopicPartition.Error)
            } else {
                atomic.AddInt64(&up.successCount, 1)
                fmt.Printf("✅ Message delivered: topic=%s, partition=%d, offset=%d\n",
                    *ev.TopicPartition.Topic, ev.TopicPartition.Partition, ev.TopicPartition.Offset)
            }
        }
    }
}

func (up *UserProducer) ProduceAsync(user User) error {
    // Serialize the user
    payload, err := up.serializer.Serialize(up.topicName, &user)
    if err != nil {
        return fmt.Errorf("failed to serialize user: %w", err)
    }

    // Produce message
    err = up.producer.Produce(&kafka.Message{
        TopicPartition: kafka.TopicPartition{
            Topic:     &up.topicName,
            Partition: kafka.PartitionAny,
        },
        Key:   []byte(user.ID),
        Value: payload,
    }, up.deliveryChan)

    if err != nil {
        atomic.AddInt64(&up.failureCount, 1)
        return fmt.Errorf("failed to produce message: %w", err)
    }

    return nil
}

func (up *UserProducer) Flush(timeout time.Duration) int {
    fmt.Println("Flushing pending messages...")
    remaining := up.producer.Flush(int(timeout.Milliseconds()))
    fmt.Printf("Flush complete. Success: %d, Failures: %d\n",
        atomic.LoadInt64(&up.successCount), atomic.LoadInt64(&up.failureCount))
    return remaining
}

func (up *UserProducer) GetMetrics() (success, failures int64) {
    return atomic.LoadInt64(&up.successCount), atomic.LoadInt64(&up.failureCount)
}

func (up *UserProducer) Close() {
    fmt.Println("Closing producer...")
    up.Flush(30 * time.Second)
    up.producer.Close()
    close(up.deliveryChan)
    success, failures := up.GetMetrics()
    fmt.Printf("Producer closed. Final stats - Success: %d, Failures: %d\n", success, failures)
}
```

### Example Application

```go
package main

import (
    "fmt"
    "time"
)

func main() {
    producer, err := NewUserProducer(Durability)
    if err != nil {
        fmt.Printf("Error creating producer: %v\n", err)
        return
    }
    defer producer.Close()

    // Create sample users
    users := []User{
        {
            ID:        "user-001",
            Email:     "alice@example.com",
            CreatedAt: time.Now().UnixMilli(),
        },
        {
            ID:        "user-002",
            Email:     "bob@example.com",
            CreatedAt: time.Now().UnixMilli(),
        },
    }

    // Produce messages
    for _, user := range users {
        err := producer.ProduceAsync(user)
        if err != nil {
            fmt.Printf("Error producing message: %v\n", err)
            continue
        }
        fmt.Printf("Queued message for user %s\n", user.ID)
    }

    // Wait for delivery
    producer.Flush(10 * time.Second)

    success, failures := producer.GetMetrics()
    fmt.Printf("Successfully produced %d messages, %d failures\n", success, failures)
}
```

---

## Error Handling with Retry

```go
func (up *UserProducer) ProduceWithRetry(user User, maxRetries int) error {
    var err error
    backoff := 1 * time.Second

    for attempt := 0; attempt < maxRetries; attempt++ {
        err = up.ProduceAsync(user)
        if err == nil {
            return nil
        }

        if attempt < maxRetries-1 {
            fmt.Printf("Attempt %d failed, retrying in %v: %v\n", attempt+1, backoff, err)
            time.Sleep(backoff)
            backoff *= 2
        }
    }

    return fmt.Errorf("all %d retry attempts failed: %w", maxRetries, err)
}
```

---

## Best Practices

1. **Handle delivery reports**: Monitor via delivery channel
2. **Flush before shutdown**: Prevents message loss
3. **Use goroutines wisely**: Don't spawn excessive goroutines
4. **Configure timeouts**: Balance retries and responsiveness
5. **Secure credentials**: Use environment variables
6. **Monitor metrics**: Track success/failure with atomic counters
7. **Close resources**: Always defer Close() calls

---

## Common Errors

- **Authentication failed**: Verify SASL credentials
- **Schema incompatible**: Check Schema Registry compatibility
- **Connection timeout**: Verify bootstrap servers
- **Queue full**: Increase buffer or flush more frequently
