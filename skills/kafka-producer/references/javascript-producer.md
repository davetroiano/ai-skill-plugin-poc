# JavaScript/Node.js Kafka Producer Guide

Complete guide for creating Kafka producers in JavaScript/Node.js using Confluent's kafka-javascript client with Schema Registry and Avro serialization.

## Installation

### Using npm

```bash
npm install @confluentinc/kafka-javascript
npm install dotenv
```

### Using yarn

```bash
yarn add @confluentinc/kafka-javascript
yarn add dotenv
```

### Package.json

```json
{
  "name": "kafka-producer-app",
  "version": "1.0.0",
  "type": "module",
  "scripts": {
    "start": "node src/producer.js",
    "test": "node src/test-producer.js"
  },
  "dependencies": {
    "@confluentinc/kafka-javascript": "^1.0.0",
    "dotenv": "^16.0.0"
  },
  "engines": {
    "node": ">=18.0.0"
  }
}
```

**Note**: Set `"type": "module"` to use ES6 imports, or use CommonJS with `require()`.

---

## Producer Configuration

### Loading Configuration from .env

```javascript
import { config } from 'dotenv';
config();

function getProducerConfig(optimizationProfile = 'balanced') {
    const baseConfig = {
        // Connection
        'bootstrap.servers': process.env.BOOTSTRAP_SERVERS,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': process.env.SASL_USERNAME,
        'sasl.password': process.env.SASL_PASSWORD,
        'client.id': process.env.CLIENT_ID || 'nodejs-producer',

        // Schema Registry
        'schema.registry.url': process.env.SCHEMA_REGISTRY_URL,
        'basic.auth.user.info': `${process.env.SCHEMA_REGISTRY_API_KEY}:${process.env.SCHEMA_REGISTRY_API_SECRET}`,
    };

    // Apply optimization profile
    const profileConfigs = getProfileConfig(optimizationProfile);
    return { ...baseConfig, ...profileConfigs };
}

function getProfileConfig(profile) {
    const profiles = {
        throughput: {
            'batch.size': 65536,
            'linger.ms': 100,
            'compression.type': 'lz4',
            'buffer.memory': 67108864,
            'acks': 1,
        },
        latency: {
            'linger.ms': 0,
            'acks': 1,
            'compression.type': 'none',
            'batch.size': 16384,
        },
        durability: {
            'acks': 'all',
            'enable.idempotence': true,
            'retries': 2147483647,
            'max.in.flight.requests.per.connection': 5,
            'delivery.timeout.ms': 120000,
        },
        freight: {
            'enable.idempotence': false,
            'linger.ms': 100,
            'batch.size': 1048576,
            'buffer.memory': 524288000,
            'partitioner.adaptive.partitioning.enable': false,
            'metadata.max.age.ms': 60000,
        },
        balanced: {
            'acks': 'all',
            'enable.idempotence': true,
            'batch.size': 16384,
            'linger.ms': 10,
            'compression.type': 'lz4',
            'buffer.memory': 33554432,
        },
    };

    return profiles[profile] || profiles.balanced;
}
```

---

## Complete Producer Implementation

### Avro Schema Definition

```javascript
const userSchema = {
    type: 'record',
    name: 'User',
    namespace: 'com.example',
    fields: [
        { name: 'id', type: 'string' },
        { name: 'email', type: 'string' },
        { name: 'createdAt', type: 'long', logicalType: 'timestamp-millis' }
    ]
};
```

### Producer Class with Schema Registry

```javascript
import { Kafka, AvroSerializer } from '@confluentinc/kafka-javascript';
import { config as loadEnv } from 'dotenv';
loadEnv();

class UserProducer {
    constructor(optimizationProfile = 'balanced') {
        this.config = getProducerConfig(optimizationProfile);
        this.topicName = process.env.TOPIC_NAME || 'users';

        // Initialize Kafka producer
        this.producer = new Kafka().producer(this.config);

        // Initialize Avro serializer for values
        this.avroSerializer = new AvroSerializer(this.config, userSchema);

        // Metrics
        this.metrics = {
            success: 0,
            failures: 0,
            pending: 0
        };

        console.log(`UserProducer initialized with profile: ${optimizationProfile}`);
    }

    async connect() {
        await this.producer.connect();
        console.log('Producer connected');
    }

    async produceAsync(userData) {
        try {
            this.metrics.pending++;

            // Serialize the value using Avro
            const value = await this.avroSerializer.serialize(userData);

            // Send message
            const result = await this.producer.send({
                topic: this.topicName,
                messages: [{
                    key: userData.id,
                    value: value,
                }],
            });

            // Handle success
            this.metrics.success++;
            this.metrics.pending--;

            const { partition, offset } = result[0];
            console.log(`✅ Message delivered: topic=${this.topicName}, partition=${partition}, offset=${offset}`);

            return result;

        } catch (error) {
            this.metrics.failures++;
            this.metrics.pending--;
            console.error(`❌ Failed to produce message for user ${userData.id}:`, error);
            throw error;
        }
    }

    async produceBatch(userDataArray) {
        try {
            const messages = await Promise.all(
                userDataArray.map(async (userData) => ({
                    key: userData.id,
                    value: await this.avroSerializer.serialize(userData),
                }))
            );

            const result = await this.producer.send({
                topic: this.topicName,
                messages: messages,
            });

            this.metrics.success += userDataArray.length;
            console.log(`✅ Batch of ${userDataArray.length} messages delivered`);

            return result;

        } catch (error) {
            this.metrics.failures += userDataArray.length;
            console.error('❌ Failed to produce batch:', error);
            throw error;
        }
    }

    async flush() {
        console.log('Flushing pending messages...');
        await this.producer.flush();
        console.log(`Flush complete. Success: ${this.metrics.success}, Failures: ${this.metrics.failures}`);
    }

    getMetrics() {
        return { ...this.metrics };
    }

    async disconnect() {
        console.log('Disconnecting producer...');
        await this.flush();
        await this.producer.disconnect();
        console.log(`Producer disconnected. Final stats - Success: ${this.metrics.success}, Failures: ${this.metrics.failures}`);
    }
}

export default UserProducer;
```

---

## Usage Examples

### Basic Usage

```javascript
import UserProducer from './producer.js';

async function main() {
    const producer = new UserProducer('durability');

    try {
        await producer.connect();

        // Single message
        const user = {
            id: 'user-001',
            email: 'alice@example.com',
            createdAt: Date.now()
        };

        await producer.produceAsync(user);

        // Flush before exit
        await producer.flush();

    } catch (error) {
        console.error('Error:', error);
        process.exit(1);
    } finally {
        await producer.disconnect();
    }
}

main();
```

### Batch Production

```javascript
async function produceBatchExample() {
    const producer = new UserProducer('throughput');

    try {
        await producer.connect();

        // Create multiple users
        const users = Array.from({ length: 100 }, (_, i) => ({
            id: `user-${String(i).padStart(3, '0')}`,
            email: `user${i}@example.com`,
            createdAt: Date.now()
        }));

        // Send in batch
        await producer.produceBatch(users);

        console.log('Batch production complete');

    } finally {
        await producer.disconnect();
    }
}
```

---

## Error Handling Patterns

### Retry Logic with Exponential Backoff

```javascript
class RetryableProducer extends UserProducer {
    constructor(optimizationProfile = 'balanced', maxRetries = 3) {
        super(optimizationProfile);
        this.maxRetries = maxRetries;
        this.initialBackoff = 1000; // 1 second
    }

    async produceWithRetry(userData) {
        for (let attempt = 0; attempt < this.maxRetries; attempt++) {
            try {
                return await this.produceAsync(userData);
            } catch (error) {
                if (attempt === this.maxRetries - 1) {
                    console.error(`All ${this.maxRetries} retry attempts failed for user ${userData.id}`);
                    throw error;
                }

                const backoff = this.initialBackoff * Math.pow(2, attempt);
                console.warn(`Attempt ${attempt + 1} failed, retrying in ${backoff}ms:`, error.message);
                await this.sleep(backoff);
            }
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
```

### Dead Letter Queue Pattern

```javascript
class ProducerWithDLQ extends UserProducer {
    constructor(optimizationProfile = 'balanced') {
        super(optimizationProfile);
        this.dlqTopic = `${this.topicName}-dlq`;
        this.failedMessages = [];
    }

    async produceWithDLQ(userData) {
        try {
            return await this.produceAsync(userData);
        } catch (error) {
            console.error(`Message failed, sending to DLQ:`, error);

            // Send to dead letter queue
            await this.sendToDLQ(userData, error);

            // Store locally for inspection
            this.failedMessages.push({
                userData,
                error: error.message,
                timestamp: Date.now()
            });
        }
    }

    async sendToDLQ(userData, error) {
        try {
            const dlqMessage = {
                originalMessage: userData,
                error: error.message,
                failedAt: Date.now()
            };

            await this.producer.send({
                topic: this.dlqTopic,
                messages: [{
                    key: userData.id,
                    value: JSON.stringify(dlqMessage),
                }],
            });

            console.log(`Sent failed message to DLQ: ${this.dlqTopic}`);
        } catch (dlqError) {
            console.error('Failed to send to DLQ:', dlqError);
        }
    }

    getFailedMessages() {
        return [...this.failedMessages];
    }
}
```

---

## Event Emitter Pattern for Monitoring

```javascript
import { EventEmitter } from 'events';

class MonitoredProducer extends EventEmitter {
    constructor(optimizationProfile = 'balanced') {
        super();
        this.producer = new UserProducer(optimizationProfile);
    }

    async connect() {
        await this.producer.connect();
        this.emit('connected');
    }

    async produceAsync(userData) {
        this.emit('producing', userData);

        try {
            const result = await this.producer.produceAsync(userData);
            this.emit('success', { userData, result });
            return result;
        } catch (error) {
            this.emit('error', { userData, error });
            throw error;
        }
    }

    async disconnect() {
        await this.producer.disconnect();
        this.emit('disconnected');
    }
}

// Usage with event listeners
const monitoredProducer = new MonitoredProducer('balanced');

monitoredProducer.on('connected', () => {
    console.log('Producer connected event');
});

monitoredProducer.on('success', ({ userData, result }) => {
    console.log(`Success event for user ${userData.id}`);
});

monitoredProducer.on('error', ({ userData, error }) => {
    console.error(`Error event for user ${userData.id}:`, error);
});
```

---

## Graceful Shutdown

```javascript
class GracefulProducer extends UserProducer {
    constructor(optimizationProfile = 'balanced') {
        super(optimizationProfile);
        this.setupShutdownHandlers();
    }

    setupShutdownHandlers() {
        const shutdown = async (signal) => {
            console.log(`\nReceived ${signal}, shutting down gracefully...`);

            try {
                await this.disconnect();
                console.log('Shutdown complete');
                process.exit(0);
            } catch (error) {
                console.error('Error during shutdown:', error);
                process.exit(1);
            }
        };

        process.on('SIGINT', () => shutdown('SIGINT'));
        process.on('SIGTERM', () => shutdown('SIGTERM'));
    }
}
```

---

## Testing

### Test Script

```javascript
// test-producer.js
import UserProducer from './producer.js';

async function runTests() {
    console.log('Starting producer tests...\n');

    const producer = new UserProducer('balanced');

    try {
        // Test 1: Connection
        console.log('Test 1: Connecting...');
        await producer.connect();
        console.log('✅ Connection test passed\n');

        // Test 2: Single message
        console.log('Test 2: Producing single message...');
        const testUser = {
            id: 'test-001',
            email: 'test@example.com',
            createdAt: Date.now()
        };
        await producer.produceAsync(testUser);
        console.log('✅ Single message test passed\n');

        // Test 3: Batch
        console.log('Test 3: Producing batch...');
        const batchUsers = Array.from({ length: 5 }, (_, i) => ({
            id: `batch-${i}`,
            email: `batch${i}@example.com`,
            createdAt: Date.now()
        }));
        await producer.produceBatch(batchUsers);
        console.log('✅ Batch test passed\n');

        // Test 4: Metrics
        console.log('Test 4: Checking metrics...');
        const metrics = producer.getMetrics();
        console.log('Metrics:', metrics);
        console.log('✅ Metrics test passed\n');

        console.log('All tests passed! 🎉');

    } catch (error) {
        console.error('❌ Test failed:', error);
        process.exit(1);
    } finally {
        await producer.disconnect();
    }
}

runTests();
```

---

## TypeScript Support

### Type Definitions

```typescript
// types.ts
export interface UserData {
    id: string;
    email: string;
    createdAt: number;
}

export interface ProducerMetrics {
    success: number;
    failures: number;
    pending: number;
}

export type OptimizationProfile =
    | 'throughput'
    | 'latency'
    | 'durability'
    | 'availability'
    | 'freight'
    | 'balanced';
```

### TypeScript Producer

```typescript
// producer.ts
import { Kafka, Producer, AvroSerializer } from '@confluentinc/kafka-javascript';
import { UserData, ProducerMetrics, OptimizationProfile } from './types';

export class UserProducer {
    private producer: Producer;
    private avroSerializer: AvroSerializer;
    private topicName: string;
    private metrics: ProducerMetrics;

    constructor(optimizationProfile: OptimizationProfile = 'balanced') {
        // Implementation...
    }

    async connect(): Promise<void> {
        await this.producer.connect();
    }

    async produceAsync(userData: UserData): Promise<any> {
        // Implementation...
    }

    async disconnect(): Promise<void> {
        await this.flush();
        await this.producer.disconnect();
    }

    getMetrics(): ProducerMetrics {
        return { ...this.metrics };
    }
}
```

---

## Best Practices

1. **Always await connect()**: Ensure producer is connected before sending
2. **Flush before shutdown**: Prevent message loss on application exit
3. **Handle errors comprehensively**: Use try/catch and consider DLQ patterns
4. **Use appropriate profiles**: Choose based on your use case
5. **Implement graceful shutdown**: Handle SIGINT/SIGTERM signals
6. **Monitor metrics**: Track success/failure rates
7. **Serialize properly**: Use Avro serializer for Schema Registry integration
8. **Reuse producers**: Don't create new instances per message
9. **Configure timeouts**: Balance between retries and responsiveness
10. **Use environment variables**: Never hardcode credentials

---

## Common Errors

### "Connection error"
- **Solution**: Verify `BOOTSTRAP_SERVERS` and network connectivity

### "Authentication failed"
- **Solution**: Check `SASL_USERNAME` and `SASL_PASSWORD` in .env

### "Schema registration failed"
- **Solution**: Verify Schema Registry credentials and URL

### "Message timeout"
- **Solution**: Increase `delivery.timeout.ms` or check broker health

### "Buffer full"
- **Solution**: Increase `buffer.memory` or call `flush()` more frequently
