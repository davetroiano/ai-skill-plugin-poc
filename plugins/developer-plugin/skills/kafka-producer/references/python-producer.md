# Python Kafka Producer Guide

Complete guide for creating Kafka producers in Python using confluent-kafka-python with Schema Registry and Avro serialization.

## Installation

### Using pip

```bash
pip install confluent-kafka
pip install python-dotenv
pip install avro-python3  # For working with Avro schemas
```

### Requirements file (requirements.txt)

```text
confluent-kafka[avro]>=2.3.0
python-dotenv>=1.0.0
```

Install with:
```bash
pip install -r requirements.txt
```

The `[avro]` extra includes the Schema Registry serializers.

---

## Producer Configuration Pattern

### Loading Configuration from .env

```python
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Load environment variables
load_dotenv()

def get_producer_config():
    """Build producer configuration from environment variables."""
    return {
        'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': os.getenv('SASL_USERNAME'),
        'sasl.password': os.getenv('SASL_PASSWORD'),
        'client.id': os.getenv('CLIENT_ID', 'python-producer'),

        # Add optimization profile settings here
        # See references/optimization-profiles.md for specific configurations
    }

def get_schema_registry_config():
    """Build Schema Registry configuration from environment variables."""
    return {
        'url': os.getenv('SCHEMA_REGISTRY_URL'),
        'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
    }
```

---

## Complete Producer Implementation

### Basic Producer with Avro Serialization

```python
import os
import socket
from dotenv import load_dotenv
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

# Load environment variables
load_dotenv()

# Avro schema definition
user_schema_str = """
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
"""

class UserProducer:
    def __init__(self):
        # Producer configuration
        self.producer_config = {
            'bootstrap.servers': os.getenv('BOOTSTRAP_SERVERS'),
            'security.protocol': 'SASL_SSL',
            'sasl.mechanism': 'PLAIN',
            'sasl.username': os.getenv('SASL_USERNAME'),
            'sasl.password': os.getenv('SASL_PASSWORD'),
            'client.id': socket.gethostname(),

            # Optimization settings (example: durability profile)
            'acks': 'all',
            'enable.idempotence': True,
            'retries': 2147483647,
            'max.in.flight.requests.per.connection': 5,
        }

        # Schema Registry configuration
        sr_config = {
            'url': os.getenv('SCHEMA_REGISTRY_URL'),
            'basic.auth.user.info': f"{os.getenv('SCHEMA_REGISTRY_API_KEY')}:{os.getenv('SCHEMA_REGISTRY_API_SECRET')}"
        }

        # Initialize Schema Registry client
        self.schema_registry_client = SchemaRegistryClient(sr_config)

        # Create Avro serializer for value
        self.avro_serializer = AvroSerializer(
            self.schema_registry_client,
            user_schema_str,
            to_dict=self.user_to_dict
        )

        # String serializer for key
        self.string_serializer = StringSerializer('utf_8')

        # Create producer
        self.producer = Producer(self.producer_config)

        self.topic = os.getenv('TOPIC_NAME', 'users')

    @staticmethod
    def user_to_dict(user, ctx):
        """Convert user object to dictionary for serialization."""
        return {
            'id': user['id'],
            'email': user['email'],
            'created_at': user['created_at']
        }

    def delivery_report(self, err, msg):
        """
        Delivery callback - called once for each produced message.
        Handles success and error cases.
        """
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
            # Add your error handling logic here:
            # - Log to monitoring system
            # - Store failed messages for retry
            # - Alert operations team
        else:
            print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def produce_user(self, user_data):
        """
        Produce a user message to Kafka.

        Args:
            user_data: Dictionary with user information
        """
        try:
            # Serialize key and value
            key = self.string_serializer(user_data['id'])
            value = self.avro_serializer(
                user_data,
                SerializationContext(self.topic, MessageField.VALUE)
            )

            # Produce message
            self.producer.produce(
                topic=self.topic,
                key=key,
                value=value,
                on_delivery=self.delivery_report
            )

            # Trigger delivery report callbacks
            self.producer.poll(0)

        except Exception as e:
            print(f'Error producing message: {e}')
            raise

    def flush(self, timeout=30):
        """
        Wait for all messages to be delivered.

        Args:
            timeout: Maximum time to wait in seconds
        """
        remaining = self.producer.flush(timeout)
        if remaining > 0:
            print(f'⚠️  Warning: {remaining} messages still in queue after flush timeout')

    def close(self):
        """Gracefully close the producer."""
        print('Flushing pending messages...')
        self.flush()
        print('Producer closed.')


# Example usage
if __name__ == '__main__':
    producer = UserProducer()

    try:
        # Example user data
        users = [
            {
                'id': 'user-001',
                'email': 'alice@example.com',
                'created_at': 1678886400000
            },
            {
                'id': 'user-002',
                'email': 'bob@example.com',
                'created_at': 1678972800000
            }
        ]

        # Produce messages
        for user in users:
            producer.produce_user(user)
            print(f'Queued message for user {user["id"]}')

        # Wait for all messages to be delivered
        producer.flush()
        print(f'Successfully produced {len(users)} messages')

    except KeyboardInterrupt:
        print('Interrupted by user')
    except Exception as e:
        print(f'Error: {e}')
    finally:
        producer.close()
```

---

## Error Handling Patterns

### Retry Logic with Exponential Backoff

```python
import time
from typing import Optional

class RetryableProducer(UserProducer):
    def __init__(self, max_retries=3, initial_backoff=1.0):
        super().__init__()
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.failed_messages = []

    def delivery_report_with_retry(self, err, msg):
        """Enhanced delivery callback with retry tracking."""
        if err is not None:
            print(f'❌ Message delivery failed: {err}')
            # Store failed message for retry
            self.failed_messages.append({
                'key': msg.key(),
                'value': msg.value(),
                'topic': msg.topic(),
                'error': str(err)
            })
        else:
            print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def produce_with_retry(self, user_data):
        """Produce with automatic retry on failure."""
        for attempt in range(self.max_retries):
            try:
                self.produce_user(user_data)
                self.producer.poll(0.1)  # Give time for delivery reports
                return True
            except Exception as e:
                if attempt < self.max_retries - 1:
                    backoff = self.initial_backoff * (2 ** attempt)
                    print(f'Attempt {attempt + 1} failed: {e}. Retrying in {backoff}s...')
                    time.sleep(backoff)
                else:
                    print(f'All {self.max_retries} attempts failed for user {user_data.get("id")}')
                    return False
```

### Monitoring and Metrics

```python
from dataclasses import dataclass
from datetime import datetime

@dataclass
class ProducerMetrics:
    messages_sent: int = 0
    messages_failed: int = 0
    messages_pending: int = 0
    last_send_time: Optional[datetime] = None

class MonitoredProducer(UserProducer):
    def __init__(self):
        super().__init__()
        self.metrics = ProducerMetrics()

    def delivery_report_with_metrics(self, err, msg):
        """Delivery callback that tracks metrics."""
        if err is not None:
            self.metrics.messages_failed += 1
            print(f'❌ Message delivery failed: {err}')
        else:
            self.metrics.messages_sent += 1
            print(f'✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

    def produce_user(self, user_data):
        """Produce with metrics tracking."""
        super().produce_user(user_data)
        self.metrics.messages_pending += 1
        self.metrics.last_send_time = datetime.now()

    def get_metrics(self):
        """Return current producer metrics."""
        # Get queue length from producer
        queue_length = len(self.producer)
        self.metrics.messages_pending = queue_length
        return self.metrics

    def print_metrics(self):
        """Print formatted metrics."""
        metrics = self.get_metrics()
        print(f"""
Producer Metrics:
  Messages sent: {metrics.messages_sent}
  Messages failed: {metrics.messages_failed}
  Messages pending: {metrics.messages_pending}
  Last send: {metrics.last_send_time}
        """)
```

---

## Loading Schema from File

```python
def load_avro_schema(schema_file_path):
    """Load Avro schema from a .avsc file."""
    with open(schema_file_path, 'r') as f:
        return f.read()

# Usage
schema_str = load_avro_schema('schemas/user.avsc')
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    to_dict=lambda obj, ctx: obj  # If obj is already a dict
)
```

---

## Async Producer (Experimental)

For high-throughput scenarios, confluent-kafka-python offers async support:

```python
from confluent_kafka import Producer
import asyncio

class AsyncUserProducer:
    def __init__(self):
        # Same config as synchronous producer
        self.producer = Producer(get_producer_config())
        self.topic = os.getenv('TOPIC_NAME')

    async def produce_async(self, user_data):
        """Produce message asynchronously."""
        loop = asyncio.get_event_loop()

        # Run produce in thread pool to not block event loop
        await loop.run_in_executor(
            None,
            self.producer.produce,
            self.topic,
            value=str(user_data)
        )

        # Poll in executor as well
        await loop.run_in_executor(None, self.producer.poll, 0)

    async def flush_async(self):
        """Flush asynchronously."""
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, self.producer.flush, 30)

# Usage
async def main():
    producer = AsyncUserProducer()

    # Produce multiple messages concurrently
    tasks = [
        producer.produce_async({'id': f'user-{i}', 'email': f'user{i}@example.com'})
        for i in range(100)
    ]
    await asyncio.gather(*tasks)
    await producer.flush_async()

# Run
asyncio.run(main())
```

---

## Best Practices

1. **Always use delivery callbacks**: Track success/failure of each message
2. **Flush before shutdown**: Call `flush()` before closing to ensure all messages are sent
3. **Poll regularly**: Call `poll()` to trigger delivery callbacks
4. **Handle exceptions**: Wrap produce calls in try/except blocks
5. **Monitor queue depth**: Use `len(producer)` to check pending message count
6. **Use connection pooling**: Reuse producer instances rather than creating new ones per message
7. **Configure timeouts appropriately**: Balance between retry duration and application responsiveness
8. **Log errors comprehensively**: Include message context for debugging failed deliveries

---

## Common Errors and Solutions

### Error: "Local: Queue full"
- **Cause**: Producer buffer is full (`buffer.memory` exceeded)
- **Solution**: Increase `buffer.memory` or call `poll()` more frequently

### Error: "Authentication failed"
- **Cause**: Invalid API credentials
- **Solution**: Verify `SASL_USERNAME` and `SASL_PASSWORD` in .env file

### Error: "Schema being registered is incompatible"
- **Cause**: Schema evolution violates compatibility rules
- **Solution**: Check Schema Registry compatibility mode, adjust schema

### Error: "Message timed out"
- **Cause**: `delivery.timeout.ms` exceeded
- **Solution**: Increase timeout or check network/broker connectivity

---

## Testing the Producer

```python
# test_producer.py
def test_producer():
    """Simple test to verify producer works."""
    producer = UserProducer()

    test_user = {
        'id': 'test-001',
        'email': 'test@example.com',
        'created_at': int(time.time() * 1000)
    }

    try:
        producer.produce_user(test_user)
        producer.flush(timeout=10)
        print('✅ Test passed: Message produced successfully')
        return True
    except Exception as e:
        print(f'❌ Test failed: {e}')
        return False
    finally:
        producer.close()

if __name__ == '__main__':
    test_producer()
```
